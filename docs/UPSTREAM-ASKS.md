# Upstream asks: what the cutover needs from PulseVM and MetalGo

The cutover agent can freeze, cut, convert, verify and ignite, but some properties a mainnet cutover depends on can
only come from the chain software itself. This page lists them as plain technical items. Each one gives the problem,
why the cutover needs it, an acceptance test that would show it is done, and its status. The risks they close are
in [RISK-REGISTER.md](RISK-REGISTER.md); the runs behind the measured numbers are in [EVIDENCE.md](EVIDENCE.md).

*Last updated 2026-10-20 (pulse-cutover v0.5.0-rc.33). Source references are to PulseVM `main` at `bd5cd618` and
the v1.0.0 release unless stated otherwise; MetalGo references are to the Metal fork as of 2026-10-19. Tags:
**measured** (seen in a recorded run), **source-read** (read in the code, not run), **inferred** (reasoned, not
observed). Issue and PR numbers are PulseVM's unless marked.*

## Summary

| ID | Ask | Repo | Closes | Status (2026-10-20) |
|---|---|---|---|---|
| [U-1](#u-1-a-genesis-settable-chain_id) | A chain_id set in the migration genesis, independent of MetalGo's blockchain ID | PulseVM | A-9, A-26 | open (related: #108) |
| [U-2](#u-2-a-content-based-state-root-and-a-deterministic-free-list) | A state root computed from content, and a deterministic, persisted free list | PulseVM | B-15, C-8 | open, no issue yet |
| [U-3](#u-3-no-full-arena-hash-per-accept-and-no-blocking-work-on-the-grpc-runtime-128) | No full-arena hash on every accept, no blocking work on the gRPC runtime | PulseVM | B-16 | open (#128) |
| [U-4](#u-4-cpu-rescale-at-import) | CPU limits rescaled at import into the units the target bills in | PulseVM | B-1, A-25 (born frozen) | open (#106, #77) |
| [U-5](#u-5-schedule-gated-signing-and-per-producer-identity) | Per-producer signing identity, with the schedule deciding who may sign | PulseVM | B-2, C-7 | open (#107, #122) |
| [U-6](#u-6-a-legacy-chain_id-diagnostic) | Say "signed for the retired chain" when a signature was made for the old chain_id | PulseVM | A-26 (support load) | open, no issue yet |
| [U-7](#u-7-tapos-enforcement) | TAPOS enforcement (Leap parity) | PulseVM | A-15 | open, no issue yet |
| [U-8](#u-8-configurable-rpcchainvm-keepalive) | A configurable rpcchainvm keepalive | MetalGo | B-16 (margin) | open, no issue yet |

U-1, U-2 and U-3 come first: U-1 because a new chain_id has to be published to wallets and exchanges weeks before H;
U-2 because every design that compares state across nodes depends on it; U-3 because it halts validators today.

---

## U-1. A genesis-settable chain_id

**Problem (source-read).** PulseVM's Antelope chain_id, the one `get_info` reports and signatures are recovered
against, is the 32-byte blockchain ID MetalGo hands to the plugin at `initialize` (`crates/pulsevm/src/main.rs`,
`initialize`; the controller stores it as `self.chain_id`). The chain_id derived from the genesis is computed too, but
a mismatch only logs a warning (`controller.rs`, around `genesis-derived chain id … does not match`). The blockchain
ID is the id of the P-Chain transaction that creates the chain; that transaction embeds the genesis, and the migration
genesis embeds the checksum of the checkpoint of the state at H.

**Why the cutover needs it.** The mainnet cutover moves to a new chain_id (decided 2026-10-19). Wallet store builds,
SDK releases, exchange signer and HSM configs, registries and BP configuration all need the final id before H. Today it
exists only after the chain is created, from a genesis that cannot exist before H, so it cannot be published in
advance. pulse-cutover rc.33 already pins `target_chain_id` in the signed event and refuses the source id on mainnet;
it cannot pin a value the chain will actually use.

**Acceptance test.**
1. A migration-genesis field (part of the hashed genesis) sets the chain_id. Two chains created from genesis files that
   differ only in the checkpoint report the **same**, pre-chosen chain_id in `get_info`, each with its own blockchain ID.
2. A transaction signed for the chosen id is accepted; the same transaction signed for the blockchain ID or for the
   XPR source id is refused.
3. A migrated chain refuses a genesis whose chosen id equals the XPR source chain_id.
4. `get_info` reports both the chain_id and the blockchain ID.

**Status.** Open. #108 covers chain-id identity on migrated chains, not a settable id.

## U-2. A content-based state root and a deterministic free list

**Problem.** The arena state root hashes each table's rows and then the whole blob arena, dead spans included
(source-read: `pulsevm_arena` `table.rs` `hash_state`). The free list (`free`, a map from length to offsets) is not
persisted and not rebuilt on load (source-read). Two consequences, both reproduced in unit tests on v1.0.0
(measured): undoing a trial execution leaves its bytes in reused free space and reverses the free-list order, so the
same block lands at different offsets on a node that ran the trial than on one that did not; and a restart loses the
free list, so new data is appended instead of reusing freed space. On a 7-validator fleet under light load
(about 2.6 tx/s) the roots diverged within 5 blocks of load starting, with no restarts, and every node had its own
root about 35 blocks later; table contents stayed identical (measured, 946 heights × 7 nodes,
[EVIDENCE](EVIDENCE.md#cross-validator-state-roots-on-pulsevm-v100-roots-diverge-under-load)).

**Why the cutover needs it.** A state root that differs per node with identical content cannot be compared across
nodes. That blocks state sync for validators joining after the cut (restarted nodes logged "no acceptable summaries
found"), attested or fleet-signed roots, a cut taken from a live follower, and any go/no-go check of the form "every
validator reports the same root".

**Acceptance test.**
1. Unit: the same block applied through the producer path (with an undone trial execution first) and through the
   validator path gives equal roots.
2. Unit: a node restarted between two blocks and a node that was not give equal roots after the second block.
3. Fleet: per-block roots identical on every validator, at least one of them arm64, over at least 10,000 post-cut
   blocks under load.
4. The root is exposed per height over RPC (today it exists only as a debug log line).

**Status.** Open, no issue yet. Changing the root changes the state-summary id, so it likely needs a protocol version
gate (inferred).

## U-3. No full-arena hash per accept, and no blocking work on the gRPC runtime (#128)

**Problem (source-read).** `main` sets the logger to the most verbose level at startup (`main.rs`,
`set_level_filter(LevelFilter::All)`), so the debug guard in `block_accept` is always true and every accepted block
hashes the entire arena (`controller.rs`, `arena state root at block …`). That work runs inline on the tokio runtime
that also answers MetalGo's rpcchainvm keepalive, whose ping interval and timeout are fixed at 30 s and 10 s (U-8).
#128 reports about 12.8 s per accept on mainnet state (reported upstream, not measured here).

**Measured.** On the 8-BP fleet (testnet state, v1.0.0), the slowest box halted with "keepalive ping failed to receive
ACK" in **6 of the 7 rc.32 LIVE runs** ([EVIDENCE](EVIDENCE.md#8-bp-fleet-rc32-fleet-step3-2026-10-16)).

**Why the cutover needs it.** The LIVE gate requires 60 s of progress with no gap over 20 s on every BP. A validator
that halts on the keepalive fails it, and on mainnet-sized state the per-accept cost is larger on every box, not only
the slowest (inferred from the reported 12.8 s).

**Acceptance test.**
1. The default log level is not debug, and is configurable; no full-arena hash runs per accept unless explicitly
   requested.
2. `build_block`, `block_verify` and `block_accept` run their controller work off the gRPC runtime, so keepalive pings
   are answered while a block is being processed.
3. On mainnet state, on the hardware the roster will use: 0 keepalive halts over at least 1,000 blocks, with accept
   latency recorded per block.

**Status.** Open (#128).

## U-4. CPU rescale at import

**Problem (source-read).** PulseVM bills CPU in points (`POINTS_PER_US = 38_000`,
`pulsevm_core/src/chain/config/mod.rs`). The import carries XPR's resource limits, which are in microseconds, so
imported accounts and the block budget are read in the wrong unit: #106 reports one transfer costing about 97,000
"µs", so accounts run dry within seconds. A straight multiplication overflows the `u32` receipt fields (#77).
pulse-cutover's `import_cpu_scale` is ignored on the upstream path, and the rehearsals ran with a patch that is not in
any release.

**Why the cutover needs it.** Without it the target is unusable for ordinary accounts minutes after LIVE. It also
decides what a migrated chain does with a frozen source row: if the source was frozen with CPU=0 `setparams`, the
row is imported as it is and the target is born frozen unless the import rewrites it (RISK-REGISTER A-25).

**Acceptance test.**
1. A consensus-pinned scale in the migration genesis is applied at import to `global_property` and the resource-limit
   rows, saturating below `u32`.
2. On mainnet state, right after ignition, a sample of ordinary accounts (including the heaviest CPU users) can push a
   transfer, and receipts fit their fields.
3. The unit of `get_account.cpu_limit` is documented for wallets.
4. Which value wins at ignition, the imported row or the genesis, is specified, so a frozen row is unfrozen on purpose.

**Status.** Open (#106, #77).

## U-5. Schedule-gated signing and per-producer identity

**Problem (source-read).** At boot the producer schedule is seeded with one entry: this node's configured
`producer_name` with the genesis `initial_key` (`controller.rs`, "Seed the active producer schedule from genesis").
Every validator therefore needs the same name and the same private key at ignition, and the migrated system
contract's first election can produce a schedule no validator can sign for, which halts the chain permanently
(reproduced twice on the 5-BP fleet; #122).

**Why the cutover needs it.** Every rehearsal so far shared one producer key. A mainnet cutover cannot hand every
producer the same signing key, and a routine `rmvproducer` or vote change must not be able to stop the chain.

**Acceptance test.**
1. The initial schedule is seeded from the imported `eosio/producers` (names and registered keys); each validator
   signs with its own key.
2. Removing a scheduled producer after LIVE (`rmvproducer`, a vote change) does not halt the chain.
3. A validator outside the DPoS schedule does not leave a gap in block production (or the rule for its windows is
   specified and measured).
4. A refusal to produce is logged at a visible level with the reason.

**Status.** Open (#107, #122). The design (whether validator weights follow the schedule, or any validator may build
and the schedule decides signing and pay only) is not settled.

## U-6. A legacy chain_id diagnostic

**Problem (inferred).** After the cut, a transaction signed for the retired XPR chain_id recovers a different public
key than the signer's, so it fails as an ordinary authorization error. Users, wallets and exchanges see "missing
authority" with no hint that the cause is the chain_id.

**Why the cutover needs it.** With a new chain_id, every client that still has the old id cached or hard-coded fails
this way on the first day. A precise error turns a support incident into a one-line fix. It is off the consensus path.

**Acceptance test.** A transaction signed for the XPR source chain_id is rejected with an error that names the cause:
"signed for the retired chain (chain_id …); sign for … instead". A transaction with a genuinely wrong key still gets
the ordinary authorization error.

**Status.** Open, no issue yet. The pulse-cutover edge can add the same message in front of the node.

## U-7. TAPOS enforcement

**Problem (source-read).** `main` parses the `block_summary` rows in the snapshot reader, but nothing on the
transaction input path checks `ref_block_num` / `ref_block_prefix` against recent blocks.

**Why the cutover needs it.** With a new chain_id, TAPOS is no longer the replay defence (the new id is: RISK-REGISTER
A-26); it is Leap parity inside the new chain. It matters for transactions referencing blocks around the cut, and it is
a precondition for any dual-id grace window. TAPOS alone cannot stop replay under a shared id: the 32-bit block prefix
can be ground by whoever builds the blocks, and a malicious RPC needs no grinding.

**Acceptance test.**
1. A 65,536-slot block-summary ring is written on accept and rebuilt from the block log on restart.
2. A transaction with a wrong `ref_block_prefix` is refused at input; one referencing a recent block is accepted.
3. Whether the slots up to H are imported from the checkpoint is specified, and a transaction referencing a block just
   before H behaves as specified after the cut.

**Status.** Open, no issue and no owner yet (#108 is chain-id identity, not TAPOS).

## U-8. Configurable rpcchainvm keepalive

**Problem (source-read).** MetalGo's rpcchainvm client fixes the keepalive at a 30 s ping and a 10 s timeout
(`vms/rpcchainvm/grpcutils/client.go`, `defaultClientKeepAliveTime` / `defaultClientKeepAliveTimeOut`). A plugin that
misses one ping is stopped.

**Why the cutover needs it.** U-3 is the real fix; a configurable keepalive gives operators margin while a slow
accept is being fixed or diagnosed, and a warning on the first missed ping makes the failure visible before it halts.

**Acceptance test.** Node flags (or VM config) for the keepalive time and timeout, defaults unchanged; a WARN log line on
the first missed ping before the plugin is stopped; a plugin that blocks for longer than the default survives with a
raised timeout.

**Status.** Open, no issue yet (MetalGo).
