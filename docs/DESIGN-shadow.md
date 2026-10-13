# Design: shadow cutovers

**Status: implemented (minimum safe version), not yet rehearsed.** Test-ladder rung 5 in
[HOW-IT-WORKS.md](HOW-IT-WORKS.md#7-how-it-gets-proven-before-mainnet); risk-register rows D-6 and G-8.

A **shadow cutover** runs the full ceremony (freeze, snapshot at exactly H, convert, fleet gate, chain
creation, ignition, LIVE) on a **private** network, starting from real XPR mainnet state and keeping the
real mainnet chain_id. It is the closest thing to a mainnet cut that leaves mainnet untouched, and it can be
repeated until it is boring.

Without this mode the agent refuses it: every rehearsal relaxation is refused for the mainnet chain_id, and
an ignition is refused for mainnet while the same-chain-id prerequisites are open. Those refusals protect a
real cut and stay exactly as they are. Shadow mode is a separate, explicit path with its own checks.

## 1. Threat model

| # | Threat | Why it matters |
|---|---|---|
| T1 | **Signatures cross over.** The shadow chain has the mainnet chain_id, so a transaction signed for the shadow is also valid on mainnet, and the reverse. | Anyone who signs on the shadow with a real key has signed a mainnet transaction; anyone who reaches a public shadow endpoint can be tricked into it. |
| T2 | **The shadow is pointed at real mainnet.** A shadow config used on a real producer would run the relaxed path against mainnet. | Freeze, pause and an ignition with relaxed gates on the real chain. |
| T3 | **A shadow is mistaken for a real cut** (or its evidence for real evidence) on mission control, in a report or a journal. | False confidence; a shadow's fleet reports counted for a real event. |
| T4 | **A real cut quietly inherits a shadow relaxation** (config copied from the shadow). | The production refusals exist for the real cut. |
| T5 | **The shadow passes because it was relaxed**, and the pass is read as "mainnet is ready". | The point of the shadow is the true result. |

## 2. What shadow mode allows

Only with `[ceremony] shadow = true` **and** a complete `[shadow]` section:

```toml
[ceremony]
chain_id = "384da888…"          # required: a shadow pins the chain it imitates
shadow = true

[target]
metal_network_id = 4242         # required: a PRIVATE Metal network (never 1 or 5)

[shadow]
label = "shadow-2026-11-04-a"   # required: shown on every record, report and board
producer_keys = ["PUB_K1_…"]    # required: the shadow source's block-signing keys
i_understand_signatures_are_valid_on_mainnet = true   # required
# metal_info_url = "http://127.0.0.1:9650"  # where to ask info.getNetworkID (default: target.rpc_url's origin)
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
4. `[beacon] network` and `[coordination] network` are not `"mainnet"`: shadow evidence never lands on the
   mainnet board or counts for a mainnet event.
5. `[flip] public_url`, when set, is a **private** address: loopback, RFC 1918, link-local, IPv6 unique-local,
   or a `.local` / `.internal` / `.test` / `.localhost` / `.lan` / `.home.arpa` name. A shadow is never served on a
   public URL by the agent's own flip.

**At ARM and again right before the write freeze** (fail-closed: anything unreadable refuses):

6. The target metalgo reports `info.getNetworkID` equal to `target.metal_network_id` (so the agent is really
   talking to the private network it was told about).
7. The source's **active producer schedule** signs only with keys listed in `[shadow] producer_keys`. Real
   mainnet's schedule signs with the real producers' keys, so a shadow config on a real mainnet node refuses
   here before anything freezes (T2).

**Labelling (T3):** the ARMED record carries `"shadow": {label, metal_network_id}`; a `SHADOW` evidence record
says it is not a cutover; every shadow override is journaled as such; `status` prints
`ceremony: SHADOW <label>`; the beacon sends `shadow` in its summary and a failing `shadow` setup check (red
on mission control, like a rehearsal); the board shows SHADOW.

**Real cut stays as it was (T4):** a config without `shadow = true` gets every refusal unchanged. A shadow
config cannot be a real cut: it requires a private network id, which a real cut's target never has, and
the beacon network is not mainnet.

**True result (T5):** a shadow verifies with the real gate unless the operator adds the compare allowlist;
then the VERIFIED record says it passed under a shadow override. Mission control and `report` show the
override list. The `HOW-IT-WORKS` exit criteria ask for shadow runs **without** overrides.

## 4. What is only documented (not enforced)

- **Nobody signs on a shadow with a real key** (T1). The agent never signs user transactions, so it cannot
  check this; it is a written rule, and `tools/shadow-replay` (if used) only forwards transactions mainnet
  already executed, which mainnet's own dedupe then rejects if they leak back.
- **No other public exposure.** The agent checks its own flip URL only. Operators must keep the target RPC,
  any hyperion `/v2`, metalgo's HTTP port and the source's API off the internet.
- **The re-keying** of the source is done outside the agent; the agent only checks its result (rule 7).
- A shadow source peering with real mainnet nodes is not detected beyond rule 7 (a re-keyed chain diverges
  from mainnet at its first block, so real peers would reject it, but operators should not peer it).

## 5. Open questions

- Should mission control refuse shadow reports outright on a network named `mainnet` (today the agent
  refuses to send them; the relay does not check)?
- Traffic replay (`tools/shadow-replay`, a parallel track) needs the target to accept already-signed mainnet
  transactions whose TAPOS references blocks the shadow never had; until PulseVM enforces TAPOS (#108) they
  are accepted, after it they will be rejected unless the shadow seeds mainnet's block summaries.
