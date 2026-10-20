# Design: shadow cutovers

**Status: implemented and hardened after an adversarial review (r30 shadow-fix), not yet rehearsed.** Test-ladder rung 5 in
[HOW-IT-WORKS.md](HOW-IT-WORKS.md#7-how-it-gets-proven-before-mainnet); risk-register rows D-6 and G-8.

A **shadow cutover** runs the full ceremony (freeze, snapshot at exactly H, convert, fleet gate, chain
creation, ignition, LIVE) on a **private** network, starting from real XPR mainnet state and keeping the
real mainnet chain_id. It is the closest thing to a mainnet cut that leaves mainnet untouched, and it can be
repeated until it is boring.

Without this mode the agent refuses it: every rehearsal relaxation is refused for the mainnet chain_id, and
an ignition is refused for mainnet while the mainnet prerequisites (UPSTREAM-ASKS) are open. Those refusals protect a
real cut and stay exactly as they are. Shadow mode is a separate, explicit path with its own checks.

## 1. Threat model

| # | Threat | Why it matters |
|---|---|---|
| T1 | **Signatures cross over.** The shadow chain has the mainnet chain_id, so a transaction signed for the shadow is also valid on mainnet, and the reverse. | Anyone who signs on the shadow with a real key has signed a mainnet transaction; anyone who reaches a public shadow endpoint can be tricked into it. |
| T2 | **The shadow is pointed at real mainnet.** A shadow config used on a real producer would run the relaxed path against mainnet. | Freeze, pause and an ignition with relaxed gates on the real chain. |
| T3 | **A shadow is mistaken for a real cut** (or its evidence for real evidence) on mission control, in a report or a journal. | False confidence; a shadow's fleet reports counted for a real event. |
| T4 | **A real cut quietly inherits a shadow relaxation** (config copied from the shadow). | The production refusals exist for the real cut. |
| T5 | **The shadow passes because it was relaxed**, and the pass is read as "mainnet is ready". | The point of the shadow is the true result. |
| T6 | **Operator-supplied proof.** Any identity check that trusts an endpoint, key list or flag the operator chose can be satisfied by a stub. | The review showed a free-text metalgo URL plus the real producer keys passed every check on a real node. |
| T7 | **The config changes mid-ceremony.** A real ceremony's journal resumed, joined, rolled back or unhalted with a shadow config (or the reverse). | The relaxed path would act on a real ceremony's state. |
| T8 | **A failed identity check acts on the source.** The ordinary abort resumes the producer and reopens writes. | A shadow config pointed at a real, already-paused mainnet producer would unpause it while its peers ignite. |

## 2. What shadow mode allows

Only with `[ceremony] shadow = true` **and** a complete `[shadow]` section:

```toml
[ceremony]
chain_id = "384da888…"          # required: a shadow pins the chain it imitates
shadow = true

[source]
rpc_url = "http://127.0.0.1:8888"            # rpc_url and producer_api_url: the SAME node (scheme, host, port)
producer_api_url = "http://127.0.0.1:8888"

[target]
metal_network_id = 4242                      # required: a PRIVATE Metal network (never 1 or 5)
rpc_url = "http://127.0.0.1:9650/ext/bc/{blockchain_id}/rpc"   # required: a metalgo /ext/ URL

[coordination]                               # required in producer mode (signed event, roster, quorum >= 2)
network = "shadow-fleet-a"                   # must start with "shadow"

[beacon]
network = "shadow-fleet-a"                   # must start with "shadow"

[shadow]
label = "shadow-2026-11-04-a"   # required: shown on every record, report and board
producer_keys = ["PUB_K1_…"]    # required: the shadow source's block-signing keys
i_understand_signatures_are_valid_on_mainnet = true   # required
```

With that, and only after the runtime checks in §3 pass:

- the source and target may carry the **XPR mainnet chain_id**;
- rehearsal relaxations (`ceremony.rehearsal = true`, the compare allowlist, the target chain_id change) are
  **accepted** for the mainnet chain_id, each journaled and labelled as a **shadow override**;
- the mainnet ignition refusal (`ignite_pending_reasons`) becomes a journaled warning, so the ceremony goes
  on to create and ignite the target. Every gate after that runs unchanged (lineage, chain_id, LIVE).

Nothing else is relaxed. A shadow with no relaxations must pass the production profile like a real cut.

### The shadow source

A private nodeos restored from a mainnet snapshot cannot produce: the scheduled producers' keys belong to
the real producers. The supported source is therefore a **re-keyed** copy: the mainnet snapshot with the
producer schedule's signing keys rewritten to shadow keys (the same technique the fleet rehearsals used on
testnet state). Its state equals mainnet at the snapshot except those keys, and its block ids diverge from
mainnet's from the first block it produces. `[shadow] producer_keys` declares those shadow keys.

## 3. What the agent enforces

**At config load** (every command that can change anything; `status` and the beacon load it too):

1. `[shadow]` without `shadow = true`, or `shadow = true` without `[shadow]`, is an error (no silent no-op).
2. `label` non-empty; `producer_keys` non-empty; the acknowledgement is `true`; `ceremony.chain_id` set.
3. `target.metal_network_id` set and **not** a public Metal network (1, 5).
4. `target.rpc_url` is a metalgo `/ext/` URL: the network proof asks the metalgo **in front of that URL** and no
   other (there is no separate, operator-chosen info endpoint; T6).
5. `source.rpc_url` and `source.producer_api_url` are the **same API base** (scheme, host, port **and path**, no query
   or fragment, parsed strictly): the identity is read on the node whose producer the ceremony pauses, and a
   path-routing proxy cannot serve the reads for one path and forward the mutations for another (T6).
6. `[beacon] network` and `[coordination] network` start with `shadow`: shadow evidence never lands on a real
   board and never counts for a real event. Mission control also refuses a shadow report on any board whose
   configured chain_id is XPR mainnet or whose id does not start with `shadow`, judged from `networks.json`, not
   from the report.
7. `[flip] public_url`, when set, is a **private** address (loopback, RFC 1918, link-local, IPv6 unique-local, or a
   `.local` / `.internal` / `.test` / `.localhost` / `.lan` / `.home.arpa` name), parsed with the same URL parser the
   HTTP client uses; backslashes and userinfo count as public (ambiguous authorities).
8. **Producer mode keeps the multi-party controls of a real cut**: a mainnet-chain_id shadow needs `[coordination]`
   at load and, at ARM, a signed event, a roster and `fleet_quorum` ≥ 2 (the D-10 rule). Only an API-mode shadow
   may run alone.

**The identity proof** (`shadow::identity_problems`), four independent parts, every one fail-closed:

9. **Target**: the metalgo behind `target.rpc_url` reports `info.getNetworkID` equal to the configured private id.
10. **Source**: its active producer schedule signs only with the declared `producer_keys`.
11. **Public mainnet** (a pinned list of public XPR mainnet API endpoints in `shadow::PUBLIC_XPR_MAINNET_APIS`;
    not configurable, T6): every endpoint that answers as XPR mainnet (by its `get_info` chain_id) is asked; the
    proof fails if **any** of them lists a declared shadow key in mainnet's active, pending or proposed producer
    schedule, or serves **the source's own block id** at the source's LIB. An endpoint without a head, or merely
    behind the source's LIB, compares nothing (a lagging or syncing endpoint proves nothing). The "far ahead" rule is
    judged on the **highest** head any answering mainnet endpoint reports (round 3): only when even that is more than
    a day of blocks (`FAR_AHEAD_BLOCKS` = 172,800) behind the source's LIB does the gap count as "not mainnet", so one
    stuck endpoint cannot vouch for a source a healthy endpoint would expose; anything closer needs a real `get_block`
    comparison. If no
    endpoint can answer either question, the proof fails: **a shadow run needs outbound HTTPS to at least one of those
    endpoints**.

A real mainnet node can pass 10 only by declaring the real keys, which 11 refuses; it can never pass the block-id half
of 11, whatever is declared.

12. **Producer binding** (round 4, T6): the head block id the **producer API itself** reports
    (`/v1/producer/get_integrity_hash`) must be the block the chain API serves at that height (one node behind both
    paths), and must not be public mainnet's block at that height (judged as in 11). This closes a proxy that serves
    the chain API from the shadow and forwards the producer API to a real producer. It runs with the proof and
    **again right before every pause and resume**; an unreadable answer is a failure.

The proof runs **at ARM, right before the write freeze, right before chain creation / ignition** (also on every
resume or `join` that reaches that step) and, for the target half, **again after ignition** before IGNITED. Only
the positive result of the check right before creation **in the same run** (`shadow_verified`) lets the shadow past
the mainnet ignition refusal; the config flag alone never does.

**A failed proof touches nothing (T8).** Before chain creation it ends ABORTED **without** resuming the producer,
running any hook or unstaging anything, and journals the rollback as complete-and-empty, so a later `rollback`
resumes nothing either; a producer this agent paused stays paused and an operator checks by hand which chain that
node belongs to. After chain creation or ignition it HALTS (sealed), like every failure past the point of no return.

The refusal is **authoritative from its first durable record** (a `shadow_refused` evidence record written before
anything else): journal replay recovers it, so a crash before the ABORTED or `rollback_done` record cannot reopen a
rollback; `run` and `rollback` finish the refusal's records without touching anything, and `join` refuses. It is final
for that journal: a new shadow starts on a fresh journal.

**Nothing touches the source of an unproven shadow (round 3).** Every source-side mutation in the state machine
(pausing or resuming the producer, every hook except the paging-only `on_halt`, moving the staged snapshot aside)
goes through one gate: on a shadow journal it first requires the full identity proof (parts 9–12) **in this run**,
whatever the entry point (`run` retrying a STRANDED pause, `rollback` and `strand`, `join` re-running `on_freeze`,
`unhalt` followed by `run`). A failed proof refuses through the path below; a test fails the build if a raw
pause, resume or hook call appears outside the gate.

**No abort resumes an unproven shadow.** Every abort and operator rollback of a shadow journal that would resume the
source first re-proves the identity (parts 9–12) in the same run, unless this run already proved it; otherwise it
goes through the refusal above. (A resumed run at VERIFIED, for example, can fail an ordinary preflight check before
the step that re-proves the identity; that failure no longer reaches a resume.)

**`loop` is gated too (round 5).** The rehearsal harness runs `[loop] reset_cmd` (service commands on the source)
before any ceremony exists, so every reset first passes `looper::loop_gate`: `ceremony.chain_id` must be pinned in the
config, real XPR mainnet is refused unless the config is a shadow, a source that answers must report the pinned chain,
a shadow's identity proof (parts 9–12) must pass right before **every** reset, and no ceremony or drill may hold this
box's journal or drill lock. A refused gate runs nothing. The repo-wide scan also treats `run_shell` (every free-form
shell command) as a mutation that needs the gate or an explicit `// ungated:` reason, and it now scans everything but
`#[cfg(test)]` **modules**, with comments and string literals blanked and calls split across lines matched.

**The chain about to be ignited must be on the proven network.** Right before ignition the agent asks the proven
private metalgo (`platform.getBlockchainStatus` under `target.rpc_url`'s base) about the journaled blockchain id (or
the id in `target.rpc_url`); anything but a known status refuses (upstream backend: after creation, so it halts).

**Round 6.** A shadow `loop` refuses `reset_cmd` when the source does not answer (`reset_when_source_down` is refused
for the mainnet chain_id a shadow carries), and every ceremony, shadow or not, undoes only what its own journal did:
a shadow refused at ARM never resumes a producer it did not pause.

### Manual recovery after a refused shadow

The agent never resumes a source after a shadow refusal, by design: the refusal means it could not prove that the node
is not a real producer. For a legitimate shadow refused after the freeze (for example all public endpoints were
unreachable for a moment), the operator:

1. checks by hand that the source node is the re-keyed shadow source (its `get_producer_schedule` keys are the shadow
   keys; its block at LIB is not mainnet's);
2. resumes it with `curl -X POST <producer_api>/v1/producer/resume` and reopens writes with the `on_abort` command;
3. starts the next shadow on a fresh journal.

**The journal binds the identity (T7).** The ARMED record carries `"shadow": {label, metal_network_id,
producer_keys_sha256}` (or `null` for a real ceremony). `run`, `join`, `rollback` and `unhalt` refuse when the current
config's identity differs: a real ceremony's journal can never take the shadow path, a shadow's never the real one,
and a shadow's label, network and keys are fixed at ARM.

**Labelling (T3):** a `SHADOW` evidence record says it is not a cutover; every shadow override is journaled as such;
`status` prints `ceremony: SHADOW <label>`; the beacon sends `shadow` in its summary and a failing `shadow` setup
check (red on mission control, like a rehearsal); the board shows SHADOW.

**Real cut stays as it was (T4):** a config without `shadow = true` gets every refusal unchanged. A shadow config
cannot be a real cut: its target must report a private network id, its coordination and beacon networks are shadow
boards, and public mainnet must contradict its source.

**True result (T5):** a shadow verifies with the real gate unless the operator adds the compare allowlist; then the
VERIFIED record says it passed under a shadow override. Mission control and `report` show the override list. The
`HOW-IT-WORKS` exit criteria ask for shadow runs **without** overrides.

## 4. What is only documented (not enforced)

- **Nobody signs on a shadow with a real key** (T1). The agent never signs user transactions, so it cannot
  check this; it is a written rule, and `tools/shadow-replay` (if used) only forwards transactions mainnet
  already executed, which mainnet's own dedupe then rejects if they leak back.
- **No other public exposure.** The agent checks its own flip URL only. Operators must keep the target RPC,
  any hyperion `/v2`, metalgo's HTTP port and the source's API off the internet.
- **The re-keying** of the source is done outside the agent; the agent only checks its result (rules 10, 11).
- A shadow source peering with real mainnet nodes is not detected beyond rules 10 and 11 (a re-keyed chain diverges
  from mainnet at its first block, so real peers would reject it, but operators should not peer it).
- **`create_chain_cmd` and `ignite_cmd` are operator hooks.** The agent proves the network of the metalgo it reads
  the target through (before creation and after ignition); a hook that creates the chain on another network would
  fail the lineage check on the private metalgo and halt, but the chain it created elsewhere is the operator's to
  retire.
- **The pinned public endpoints are trusted** to tell the truth about mainnet; DNS or TLS interception of all of them
  could fake an answer. Any one honest endpoint refuses a real node.

## 5. Open questions

- Traffic replay (`tools/shadow-replay`, a parallel track) needs the target to accept already-signed mainnet
  transactions whose TAPOS references blocks the shadow never had; until PulseVM enforces TAPOS (#108) they
  are accepted, after it they will be rejected unless the shadow seeds mainnet's block summaries.
