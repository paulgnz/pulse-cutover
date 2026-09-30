# Connect your node to Cutover Mission Control

**For block producers. Takes about 2 minutes. Safe on a live producing node.**

This installs a small reporter (the *beacon*) that tells
[Cutover Mission Control](https://control-rehearsal.protonnz.com) whether your node is ready for a future
PulseVM cutover. The running beacon only reads local state; it **does not** touch nodeos, its config, your keys or
block production, and it opens no ports. (Installing it writes the binary, a config, a token and a systemd
service; see below.)

---

## 1. Run one command on your node

Log in to the server that runs your **producer** nodeos, then run:

```bash
curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh | sudo bash
```

It works out everything itself: mainnet or testnet (from your chain), your producer account (from
`producer-name` in your nodeos `config.ini`), and your nodeos API address.

> Want to look before you install? Add `-s -- --dry-run` to the end. It shows what it found and your readiness
> checklist, and changes nothing:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh | sudo bash -s -- --dry-run
> ```

## 2. Send one line

The last thing it prints looks like this:

```
  ✓ Done. Last step: send this ONE line to the mission-control operator (it is only a hash):

      network=testnet producer=youraccount token_sha256=3f9c…e21a
```

Send that line to the operator (Telegram or email). It is only a **hash** of your token. The token itself stays
on your server except that the beacon sends it, over HTTPS, with each report: that is how mission control knows
the report is yours. Re-running the installer later (to upgrade) keeps the same token, so there is nothing to send
again unless you change the producer or network.

## 3. Watch your node

Once you're approved, your node appears on the dashboard with its readiness checks:
`https://control-rehearsal.protonnz.com/testnet/youraccount`

A few red items are expected today (for example "validator not running" until the Metal node is set up).
That list is a **preparation** checklist. A fully green list does not authorize a cutover: release, validator
admission, funding and routing are checked separately.

## 4. Set up your Metal node

One more command installs metalgo and prints your NodeID, BLS key and proof of possession:
[METAL-QUICKSTART.md](METAL-QUICKSTART.md).

## 5. API and Hyperion operators: serve /v1 through the edge

If your node serves a public `/v1` (install mode `api`) or `/v1` + `/v2` history (mode `hyperion`), set
`"gateway": {"mode": "edge"}` in the ceremony manifest with PulseVM v1.0.0+ (the coordinator pins
`artifacts.edge`). After the cut your `/v1` URL keeps answering the Leap 5 endpoints dapps call: the node's own
API where it has one, translations for the rest, and a clear 501 where PulseVM cannot answer yet. In `hyperion`
mode `/v1/history` and `/v2` go through the federator, which reads balances and permissions from the new chain
and uses the old and new Hyperion indexes only to find accounts. What each endpoint does:
[V1-COVERAGE.md](V1-COVERAGE.md).

Before your source nodeos stops, capture two facts the edge serves from the old chain:

```bash
node tools/capture-static.mjs http://127.0.0.1:8888 /etc/pulse-cutover/static
```

---

## If something goes wrong

| You see | Do this |
|---|---|
| `could not find producer-name` | This box isn't your producer, or nodeos uses a custom config path. Add your account: `… \| sudo bash -s -- --producer youraccount` |
| `could not reach nodeos chain API` | Your nodeos listens somewhere else. Add it: `… \| sudo bash -s -- --api http://127.0.0.1:8888` |
| `unknown chain` | Not XPR mainnet or testnet. Add `--network <name>` |
| `checksum mismatch` | Stop. Don't install. Tell the operator. |
| Anything else | Run with `--dry-run` and send the output to the operator. |

**Remove it any time:**

```bash
curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh | sudo bash -s -- --uninstall
```

---

<details>
<summary>What exactly does it install?</summary>

- `/usr/local/bin/pulse-cutover`: the prebuilt binary from the
  [GitHub release](https://github.com/paulgnz/pulse-cutover/releases), checked against its sha256.
- `/etc/pulse-cutover/beacon.toml`: a readiness config for reporting only. **Never run `pulse-cutover run` with it.**
  From v0.5.0-rc.5 it is marked `profile = "readiness"` and the ceremony commands (`run`, `loop`, `await`)
  refuse it. Configs written by rc.4 and earlier lacked that guard; re-run the installer to upgrade.
- `/etc/pulse-cutover/beacon.token`: a random token made on your server (root and the beacon's service user only).
- `/etc/pulse-cutover/beacon.instance`: this server's stable instance id (kept across upgrades).
- `/var/lib/pulse-beacon`: the beacon's own state dir, the **only** place it can write.
- `pulse-beacon.service`: a systemd service, running as the unprivileged `pulse-beacon` user when it can read
  what it needs (otherwise root limited to read-only file access), with a read-only filesystem. The ceremony's
  directory `/var/lib/pulse-cutover` (journal, lock, staged snapshot) stays root-owned and read-only to the beacon,
  so the observer can never alter ceremony evidence.

Everything is downloaded, checksum-verified and test-run in a temp dir before the box changes (`--dry-run` writes
nothing outside it). The update itself is a transaction: if anything fails before the new beacon is confirmed
running, the previous binary, config, token, instance id and service are restored and the previous beacon restarted.
The report URL must be HTTPS (plain http only to localhost); it is parsed, so `localhost.example.org` or
`localhost@example.org` count as remote.

What the beacon reads: your nodeos `get_info`, whether the producer API answers locally, whether a Metal
validator service is running, and free disk. What it sends (the dashboard is public, so it is kept minimal):
pass/fail for each check with a short verdict (e.g. "83 GB free", "running"), head/LIB, your account, a node
label you choose (default: its role; use `--node` to tell several servers of the same role apart), your Metal
node's public identity (NodeID, BLS public key, version, peers), and the pulse-cutover version. Not sent: private
keys, hostnames, file paths or config contents. If a ceremony step fails, only a short error class is sent (paths,
URLs and IP addresses stripped, and mission control redacts again); the full error stays in your local journal.
Mission control does see the IP address your reports come from; it uses it only for two reachability checks
against that address: port 9651, and whether your producer API answers from the internet (below).
</details>

**Producer API must not be public.** Anyone who can reach `/v1/producer/*` can `resume` a producer the ceremony
paused at H (and pause, greylist, take snapshots). Mission control checks this for you: it sends the read-only
`POST /v1/producer/paused` to your server's own IP on :8888 and :80 and to your bp.json endpoints that resolve to
that IP (never anywhere else, no redirects followed), at most every 10 minutes. The result is the
**Producer API private** setup check; the dashboard shows only pass/fail and a count, and the beacon logs the open
URLs locally. Fix, with producer and API on the same box: in your nginx server blocks add
`location ^~ /v1/producer { return 403; }`, and if nodeos also listens publicly on :8888, bind it to
`http-server-address = 127.0.0.1:8889` and let nginx serve :8888 with the same rule (then point the beacon's
`rpc_url` and `producer_api_url` at `http://127.0.0.1:8889`).
