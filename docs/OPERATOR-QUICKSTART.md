# Connect your node to Cutover Mission Control

**For block producers. Takes about 2 minutes. Safe on a live producing node.**

This installs a small reporter (the *beacon*) that tells
[Cutover Mission Control](https://control-rehearsal.protonnz.com) whether your node is ready for a future
PulseVM cutover. The running beacon only reads local state; it **does not** touch nodeos, its config, your keys or
block production, and it opens no ports. (Installing it writes the binary, a config, a token and a systemd
service; see below.)

---

> **Before a real cut** (not needed for the beacon): your ceremony's `on_freeze` hook must close **every** write
> path into your producer's nodeos (public and private APIs, relays, bots, other direct clients), and the head
> block number your endpoints report will step back by a few hundred once at the flip (the discarded burn-off
> blocks; see [EXCHANGES.md](EXCHANGES.md)). Tell your API users and exchanges.

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

## 6. During a ceremony: read the fleet verdict, not only your own state

Each agent decides from its own observations, so one event can end with different states on different BPs (the
5-BP rehearsal on the upstream stack ended every run with some BPs LIVE and some HALTED on the same new chain).
Since rc.23 mission control shows one **fleet verdict** for the current event, under the network switch, computed
from the event's signed roster and the beacons' reports (`fleet` in `GET /api/status`):

| Verdict | Means | What you do |
|---|---|---|
| `PENDING` | no roster member has started chain creation or ignition | nothing; follow your agent |
| `LIVE` | at least `quorum` roster members are LIVE on **one** target chain, with the same first block after H | if your box is HALTED or STRANDED on that chain, recover onto it (`unhalt` + `run`, or `join`; see below) |
| `DEGRADED` | a target chain may be running, but no quorum is LIVE on one chain yet | do not reopen writes by hand; wait for LIVE or for the coordinator |
| `SPLIT` (red) | a roster member resumed the old chain after others started chain creation or ignition, the old chain's head moved after every member had paused and someone had started chain creation, or members report different target chains / different blocks after H / different blocks at one height | stop: do not reopen writes anywhere until the coordinator resolves it; the alarm names the BPs |
| `ABORTED` | every reporting member aborted before chain creation | the old chain continues; wait for a new event |

A `SPLIT` stays red (latched) for that event even if the reports that showed it change later. Only the
mission-control operator clears it, once the split is resolved, from a shell on the mission-control host:
`curl -X POST 'http://127.0.0.1:8787/api/admin/clear-split?net=testnet'` (its own port; accepted on loopback only,
never through the public proxy). If the condition still holds, it latches again. Details: `control/README.md`.

How it is computed: for each roster member (its pinned instance, if the event names one) the freshest report for this
event; LIVE members are grouped by target chain (Metal blockchain id, else chain id) and by the id of the first block
after the cut, which every member of one chain shares and any fork does not. Each beacon reports its target's head,
head block id and that first-block id (`ceremony.target`) once its target may be running. The verdict is
relay-reported and unsigned: it is evidence for people, not an authorization; the agents' own gates still decide.

The old chain is judged two ways (rc.24), neither with a guessed burn-off length (the old chain keeps making empty
blocks past H until H is final and the producers pause, a few hundred on XPR, and that lag is not fixed):

- **Pause-head bound.** When every member that reached SNAPSHOTTED published its pause head (`head_at_pause`), a
  source head more than 12 blocks above the highest one while a member is past chain creation is a split. With any
  pause head missing (an older beacon) this rule is skipped.
- **Movement.** Once every roster member has reached SNAPSHOTTED (all producers paused) and one is past chain
  creation, the relay records each member's source head from then on (first and highest). If it moves more than 12
  blocks, a producer resumed the old chain: split. A member that went silent before reaching SNAPSHOTTED does not
  hold this rule back forever: it also applies once every member that is still reporting has reached SNAPSHOTTED and
  at least `quorum` members are past chain creation. Before that, a slow BP's producer may still be making burn-off
  blocks and the head moving is normal.

What your agent does with the same view (rc.23, coordinated events only):

- **Before chain creation.** If your ceremony has to stop after writes froze (fleet timeout, a failed check, the
  relay unreachable) it resumes the old chain only when its **resume guard** passes: the relay answers, no roster
  member is missing, identity-conflicted or was ever reported past chain creation, and either fewer than `quorum`
  other members are still in the ceremony (fresh reports, not ABORTED / STRANDED: the event cannot reach its quorum
  without this node), or the coordinator signed an abort and every member has a report for the event. It checks
  twice: after the first pass it withdraws its own VERIFIED report (abort intent) and waits until the relay shows that
  (or the report is too old for any gate), then re-checks. That is evidence from unsigned relay reports, not proof
  that no peer ignites. A producer-mode event must carry a roster (`await` refuses one without). Otherwise it ends
  **STRANDED**: sealed like HALTED, the source stays paused, writes
  stay frozen, `on_halt` pages you. Put your `[beacon] producer` in the ceremony config so the agent can recognize
  its own entry in the roster. From STRANDED: if the verdict is `LIVE`, run
  `pulse-cutover join --config <the event's ceremony config> --event <id>` (it checks your verified artifacts
  against the LIVE members' and that your source took nothing after H, then tracks and ignites their chain);
  otherwise `pulse-cutover rollback --config …` re-runs the guard and resumes the old chain only if it passes
  (`--force-stranded --i-understand` records a fleet-wide decision instead, journaled with the fleet view it
  overrides).
- **After ignition.** A local symptom (a block gap in the sustained-LIVE window, a slow first block, a failing
  `post_ignite` / `on_live` hook) no longer halts at once while a quorum of the roster reports the same target chain
  with a common block after H and a moving head: the ceremony shows **degraded**, retries hooks with backoff (they
  must be safe to re-run) and keeps waiting up to `[coordination] degraded_patience_secs` (default 900). It halts
  when the fleet view stops vouching, when patience runs out, or when you create `operator-halt` next to the
  journal. It never resumes the old chain.

### Block producers on PulseVM: no rotation

There is no alphabetical 12-block rotation after the cut. Snowman / ProposerVM give validators stake-weighted
proposal windows and blocks are built on demand; a block's `producer` field is the producer name configured on the
node that built it (PulseVM v1.0.0 `controller.rs` lines 2210–2236), and in rehearsals every validator shares one
name and key (MetalBlockchain/pulsevm#107), so all blocks show the same producer. Missed-block trackers,
`unpaid_blocks`-based pay and tools that assume a rotation will report nonsense. Details: [EXCHANGES.md §5](EXCHANGES.md#5-block-producers-after-the-cut-no-12-block-rotation).

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
