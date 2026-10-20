# Web edges: what doctor detects and how the flip works

The operator-facing half is Step 2 of the [README walkthrough](../README.md#start-here--the-operator-walkthrough).
This page is the detail behind it: what `pulse-cutover doctor` detects, which setups are handled, and how the flip
works on nginx and HAProxy. The generated scripts themselves are described in
[REFERENCE.md](REFERENCE.md#installsh-internals).

## `pulse-cutover doctor` — detect, don't assume

The 22 recorded ceremonies ran on boxes we built. Your box is not that box —
so the tooling **detects** instead of assuming. `doctor` is a strictly
read-only survey (no restarts, no writes, no flips):

```sh
pulse-cutover doctor          # human table
pulse-cutover doctor --json   # machine JSON (what install.sh consumes)
```

What it detects:

- **host** — OS/version/arch/kernel, RAM, CPU, free disk per relevant mount,
  systemd/docker presence, virtualization;
- **nodeos** — native binary or docker container (both work), the systemd
  unit(s) that exec or docker-wrap it, chain API address, version
  (`server_version_string`), chain_id + head, whether the producer_api
  answers (`/v1/producer/paused` probe: 200 = enabled, 404 = missing plugin,
  401/403 = restricted), state-history plugin on/off;
- **web edge** — nginx, haproxy, apache or caddy. For nginx: the full
  `server_name -> location -> proxy_pass/upstream` map from `nginx -T`
  (this is what the flip templater uses), plus TLS cert paths and expiry.
  For haproxy: the full `frontend (binds, TLS, ACL path rules) -> backend ->
  server` map from haproxy.cfg, per-backend server counts (multi-nodeos
  load balancing is detected, not assumed away), the *admin socket* (a
  `stats socket ... level admin` line — its presence selects the zero-reload
  flip strategy), and whether socat/nc is installed to drive it. If BOTH
  nginx and haproxy are running with routes, doctor reports both maps and
  the verdict asks you to declare `flip.edge` in the manifest;
- **history stack** — legacy Hyperion under pm2 or systemd, its /v2 health;
  Elasticsearch version + heap;
- **PulseVM target** — metalgo binary/node, plugins dir, staged
  pulse-cutover services;
- **ports** — the ceremony's port plan (9650/9651/8899/80/9200/7000/7010/7019)
  vs what is actually listening, and who owns the conflict.

It ends with a per-mode verdict: **READY**, **NEEDS** (a precise list — e.g.
"producer_api_plugin (localhost-bound)"), or **UNSUPPORTED** (a precise
reason — e.g. caddy on the public edge, kube-managed nodeos) plus a pointer
at `pulse-cutover report` so unsupported setups become supported ones.


### Sample output

Recorded with doctor **v0.2.0** on a single-box rehearsal (August 2026, trimmed); today's output has more checks but
the same shape:

```
pulse-cutover doctor v0.2.0 — api-cutover-test (2026-08-21T08:31:45Z)

HOST
  os                     Ubuntu 24.04 (x86_64, Linux 6.8.0-137-generic)
  cpu                    8 x AMD EPYC-Genoa Processor
  ram                    15.2 GB
  disk /                 265 GB free / 300 GB
  systemd / docker       yes / yes

NODEOS (source chain — yours, untouched)
  runtime                docker container `nodeos` (nodeos:5.0.3)
  chain api              http://127.0.0.1:8888
  version                v5.0.3
  chain_id               71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd
  head                   401616025
  producer_api           enabled
  state_history          no

WEB EDGE (nginx)
  version                nginx/1.24.0 (Ubuntu)
  route _                /v1/chain/ -> 127.0.0.1:8888 (upstream pulse_v1_backend)
  ...

VERDICTS
  bp        READY
  api       READY
  hyperion  READY
```

### Supported setups matrix

"Detected & handled" means doctor recognises the setup and install.sh generates
the stop/flip scripts for it. It is not the same as qualified: the rehearsed
combinations are nginx and HAProxy edges (native and docker) on Ubuntu 20.04–24.04
with systemd- or script-managed nodeos. Apache, CDN/load-balancer rules in front of
the box, direct-origin access, IPv6 paths and WebSocket/streaming endpoints have
not been qualified and need an adapter plus a rehearsal.

| dimension | detected & handled | detected, NOT yet handled (UNSUPPORTED + explain) |
|---|---|---|
| nodeos runtime | native (systemd unit or bare pid) · docker container | kubernetes-managed |
| nodeos stop/start | manifest `stop_cmd` · derived `systemctl stop <unit>` · derived `docker stop <container>` · **script-managed** (no unit — doctor classifies the parent chain: screen/tmux/cron/nohup-orphan/shell, reports pid + config + data dir + where stdout/stderr go; install.sh generates a reviewed graceful-SIGTERM stop script, and a `[CHANGE]` start placeholder you point at your own start script — we cannot guess it, so we say so) | native nodeos whose management doctor cannot classify at all, in api mode without a manifest `stop_cmd` (NEEDS) |
| public edge | nginx (any layout: named upstreams, direct proxy_pass, TLS server blocks, multiple domains) · haproxy (frontends/listens, TLS binds, named + anonymous ACL path rules, multi-server backends) · nginx AND haproxy together (declare `flip.edge`) · no web server (managed nginx layout staged) | apache · caddy |
| nginx flip | templated byte-exact from the detected `server_name -> proxy_pass` map; refuses if no /v1 route points at your nodeos | hand-minified configs may degrade to fewer detected routes — doctor shows what it saw |
| haproxy flip (socket) | admin-level `stats socket` present: the gateway is pre-staged as a `disabled` server at INSTALL time (the one and only reload), and the ceremony flip is a transactional `enable/disable server` on the runtime socket — zero reloads at H, response-checked, instantly revertible | multi-server backend without a drain decision (NEEDS — see HAProxy notes) |
| haproxy flip (reload) | no admin socket: same pre-staged server; the flip swaps the `disabled` markers in haproxy.cfg, `haproxy -c` validates, then one graceful reload (native `systemctl reload` or docker `SIGHUP`) | same multi-server rule |
| haproxy runtime | native (systemd unit) · docker container (validate via `docker exec`, reload via `SIGHUP`) | — |
| history | legacy Hyperion (pm2 or systemd) noted; hyperion mode flips the detected /v2 route (nginx or haproxy) | no detectable /v2 route in hyperion mode (refuses with reason) |
| OS | Ubuntu 20.04 / 22.04 / 24.04 (20.04 note: the distro's Node 10 is too old for the gateway/federator — install.sh checks for Node ≥ 14 and prints the exact NodeSource one-liner if missing; all staged unit files use systemd-245-era directives only) | anything else (UNSUPPORTED — tell us via `report`) |

## HAProxy notes

HAProxy is common front-of-house for Antelope API providers (several nodeos
boxes balanced in one backend, TLS termination, path routing), so it gets
the same first-class treatment as nginx — and, when the admin socket exists,
a *better* flip than a reload:

**How the flip works.** At **install time** (days before the event),
install.sh adds one line to the backend that fronts your nodeos:

```
server pulsevm-gw 127.0.0.1:8899 check disabled # pulse-cutover: staged flip target, ...
```

validates (`haproxy -c`) and does **one graceful reload**. That is the only
reload in the whole procedure — from then on both servers exist in haproxy,
with the gateway parked in maintenance. At H the generated flip script:

- **runtime-socket strategy** (preferred — picked automatically when the
  config has a UNIX `stats socket ... level admin`): sends
  `enable server <backend>/pulsevm-gw`, checks haproxy's answer (an empty
  response means OK; anything else aborts *before touching your nodeos
  entry*), then `disable server <backend>/<your-nodeos>` — zero reloads,
  takes effect immediately, reverts the same way. The script then persists
  the same swap into haproxy.cfg (validated, not reloaded) so a later
  haproxy restart doesn't quietly fall back to the retired nodeos.
- **cfg-reload strategy** (fallback when there is no admin socket): swaps
  which of the two server lines carries `disabled`, validates with
  `haproxy -c`, then one graceful reload. Note a graceful reload is
  *asynchronous* — the old worker keeps answering until the new one takes
  the listeners — which is fine in a ceremony (the agent's health gate polls
  the public URL for several consecutive good answers) but worth knowing
  when testing by hand. Want the better strategy? Add
  `stats socket /run/haproxy/admin.sock mode 660 level admin` to your
  `global` section and re-run install.sh.

**Multi-server backends (the drain decision).** If the backend that fronts
your nodeos balances **several active servers**, doctor's verdict is NEEDS,
not READY: a single-box ceremony flips only *this box's* backend entry, and
proceeding silently would leave the other servers answering from the old
chain after the cut. Decide first, then re-run doctor:

- **drain**: mark every server that must not take post-cut traffic
  `disabled` in the backend (haproxy's own maintenance mechanism — doctor
  counts only non-`disabled` servers), or
- **fleet flip**: run the ceremony on each balanced box and coordinate the
  flips — every box runs its own install + flip against its own entry.

`backup` servers count as active on purpose: after a flip, failover to a
backup nodeos would silently serve the retired chain.

**Docker-run haproxy** is detected and handled (validate via `docker exec`,
reload via `SIGHUP` to the master). One gotcha the live test hit: bind-mount
the config **directory**, not the single file — `sed -i`/`mv` replace the
file's inode, and a single-file mount pins the container to the old inode so
every reload re-reads stale config. install.sh catches this: after staging
it asks the running haproxy (via the admin socket) whether the staged server
actually exists, and refuses with the fix if not. The containerized test rig
we validate with is in `examples/haproxy-test/`.

**Two edges at once.** A box running both nginx and haproxy (e.g. haproxy
terminating TLS in front of nginx, or a half-migrated setup) makes doctor
report **both** route maps and demand `flip.edge` in the manifest — it will
not guess which edge your users actually reach. `"edge": "auto"` (the
default) only auto-picks when exactly one edge routes /v1 to your nodeos.
Real doctor output from the dual-edge validation box (trimmed):

```
WEB EDGE (nginx)
  version                nginx/1.24.0 (Ubuntu)
  route _                /v1/chain/ -> 127.0.0.1:8888 (upstream pulse_loop_backend)
  ...

WEB EDGE (haproxy)
  state                  running (docker container `pulse-haproxy`)
  version                HAProxy version 2.9.15-e872a3f 2025/03/21 - https://haproxy.org/
  config                 /etc/haproxy/haproxy.cfg
  route fe_pulse_test    *:8081 [if path_beg /v1] -> be_v1 { nodeos 127.0.0.1:8888 }
  route fe_pulse_test    *:8081 [default] -> be_v1 { nodeos 127.0.0.1:8888 }
  admin socket           /run/haproxy/admin.sock (level admin) — zero-reload runtime flip via socat

VERDICTS
  api       NEEDS
            NEEDS: two web edges detected (nginx AND haproxy are both running with
            routes) — set flip.edge = "nginx" or "haproxy" in the manifest so the
            ceremony flips the edge your users actually reach
```
