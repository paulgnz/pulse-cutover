# Loop-harness reference deployment (api-cutover rehearsal, 2026-08-21)

The exact scripts used for the N-run repeatable rehearsal on the api-cutover-test box:

- `setup.sh` — one-time: metalgo-local unit (LOCAL network 12345, sybil protection off
  — the tmpnet pattern), dedicated nginx listener :8080 + upstream swap files,
  gateway-loop :8898, `ceremony-loop.toml` template.
- `reset.sh` — per-iteration (`[loop].reset_cmd`): source nodeos back up (still
  live-syncing the real XPR testnet), staged snapshot removed (R12), public loop URL
  reverted, local network wiped, fresh subnet+chain created (`createLocal.cjs`, ewoq
  key), and the one-line nginx proxy on 127.0.0.1:9657 rewritten with the fresh
  blockchainID so the ceremony config's target rpc_url NEVER changes.
- `createLocal.cjs` — createSubnet + createChain on the local P-chain.

Gotchas encoded here:
- metalgo 1.13.5 does not register aliases-file entries on the HTTP router (chain log
  aliased, /ext/bc/<alias>/rpc 404s) → the 9657 nginx indirection instead.
- metaljs `buildCreateChainTx` treats the vmID argument as a VM NAME:
  on-chain vmID = cb58(ascii(name) zero-padded to 32 bytes). Name your plugin file
  after the ON-CHAIN vmID (read it back from platform.getBlockchains).
