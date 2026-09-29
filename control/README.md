# Cutover Mission Control

A live readiness and ceremony board for every network you care about: XPR mainnet (first), testnet, and
rehearsal networks. Deployed for the rehearsals at **https://control-rehearsal.protonnz.com**.

![Mission control, mainnet view](../docs/media/mission-control-mainnet.png)

Run 6 of the 5-BP rehearsal, as seen on mission control (time-lapse):

![Run 6 time-lapse](../docs/media/mission-control-run6.gif)

| What you see | Where it comes from |
|---|---|
| Head, LIB, blocks/s, who is producing, block propagation arcs on the globe | public RPC of each network (read-only, polled every 3 s) |
| Every **active** producer (e.g. 37 on mainnet), vote rank, scheduled vs standby | `eosio::producers` (`is_active`) + `get_producer_schedule` |
| Names, logos, cities, map positions | each producer's `<url>/chains.json` → that chain's `bp.json` (falls back to `/bp.json`) |
| Per-producer readiness checks, ceremony state, agreement of cut id / snapshot hash / fingerprints / state digest | **opt-in beacons**: `pulse-cutover beacon` on each producer's box |

Nothing on this board can change a chain or a node. It reads public data and accepts reports from
producers who choose to send them.

```mermaid
flowchart LR
    classDef ext fill:#1f2937,stroke:#9ca3af,color:#f9fafb
    classDef mc fill:#5b21b6,stroke:#c4b5fd,color:#fff
    classDef bp fill:#065f46,stroke:#6ee7b7,color:#fff
    R1["public RPC<br/>mainnet · testnet · rehearsal"]:::ext -- "get_info · producers · schedule" --> S["mission control<br/>control/server.js"]:::mc
    B1["producer.com/chains.json → bp.json"]:::ext -- "names · logos · locations" --> S
    BP["pulse-cutover beacon<br/>(each producer, opt-in)"]:::bp -- "HTTPS + bearer token<br/>readiness · journal evidence" --> S
    S -- "GET /api/status" --> UI["dashboard<br/>desktop + phone"]:::mc
```

## Run it

```sh
NETWORKS=control/networks.json TOKENS=/etc/pulse-control/tokens.json COORD_FILE=/var/lib/pulse-control/coord.json \
  PORT=8787 node control/server.js
# behind nginx/caddy with TLS; it binds 127.0.0.1 only. The proxy MUST overwrite X-Real-IP with the client address
# (nginx: proxy_set_header X-Real-IP $remote_addr;) — /api/reach trusts that header only from loopback.
node --test control/test/*.test.mjs   # offline test suite (MC_OFFLINE=1, random port, temp config)
```

- **Networks:** `control/networks.json` lists each network's id, label, expected chain_id and public RPC URLs
  (tried in order). `priority` orders the tabs. A network with `"static_producers": true` + `"geo"` (the rehearsal) skips
  the producers table and uses the given locations.
- **Tokens:** `tokens.json` maps `sha256(token)` → `{ "network": "...", "producer": "..." }`. The server never stores a token,
  only its hash, and accepts a report only for the network and producer the token is bound to.
  Issue one: `t=$(openssl rand -hex 32)`; give `t` to the producer (file, mode 600); add `sha256(t)` here. The file is
  re-read automatically.
- **Coordination** (`COORD_FILE`): signed event/arm/abort messages are persisted atomically and survive restarts, with
  a history. While an event is active and not aborted, a *different* event is refused (409); abort it first.
- **Tokens file reload** is validated before it replaces the current set (a bad edit keeps the old set and logs);
  deleting the file revokes every token.
- No dependencies, Node ≥ 18. The page loads fonts from Google Fonts and the globe's land outline from jsDelivr
  (`world-atlas`); without them it still works, with a plain globe.

## Report from a producer (the beacon)

Add to the ceremony config and run `pulse-cutover beacon --config ceremony.toml` (e.g. as a systemd service):

```toml
[beacon]
url = "https://control.example/api/report"
producer = "protonnz"             # your account, as in the producer schedule
network = "testnet"               # the id in networks.json
token_file = "/etc/pulse-cutover/beacon.token"
interval_secs = 5
```

The beacon is read-only on the box. It checks the source API, producer API, chain_id, declared H, freeze lead,
hooks (exist and executable), that no snapshot is pre-staged, that the validator is running, and free disk. It
summarizes the journal into the evidence every producer must agree on. `--once` prints one report without sending it.

![Phone layout](../docs/media/mission-control-phone.png)

## Security model (what the public dashboard will and won't publish)

The dashboard is public. The server treats every beacon report and every producer-controlled URL as untrusted:

- **Reports** are validated against a strict schema (types, enums for ceremony states and roles, length caps) and
  rejected with 400 otherwise. Only an allow-listed projection is stored and published (`/api/status`, `/api/node`):
  unknown fields are dropped, check details are redacted (paths, URLs, IPs, key-like strings removed), and
  `ceremony.last_error` is reduced to a short redacted hint (the full error stays in the operator's local journal).
  Reports older than 5 min, more than 2 min in the future, or not newer than the last one from the same token
  (replay) are refused. Each token may send at most one report per 3 s (burst 5).
- **Identity:** servers are keyed by token hash (one token = one server). The `node` label is display only; two
  servers with the same label are both kept (`api`, `api (2)`). A token whose `instance_id` changes is flagged as
  used on two machines. A server is "silent" after max(3 × its interval, 45 s).
- **Outbound fetches** to producer-supplied URLs (bp.json, chains.json, logos, API/Hyperion/AtomicAssets probes,
  p2p TCP probes, known endpoints) resolve DNS first and refuse loopback, private, link-local, CGNAT, multicast,
  documentation and other non-public ranges (IPv4 and IPv6, incl. mapped/NAT64/6to4 forms); redirects are followed
  manually (max 3) and re-validated; responses are size-capped (logos 512 KB); at most 16 probes run at once.
  Residual risk: DNS can change between our check and fetch's own connect (rebinding); acceptable for a read-only
  board, documented here.
- **/api/reach** dials only the caller's own public IP literal, only port 9651, once per 5 s per IP, 20 in flight.
- **Agreement** is roster-based: the denominator is the producer schedule (or the configured set), stale reports are
  excluded and named, and roster members without a value are listed as missing. Agreement is equality, not
  correctness (a non-zero "transactions after the cut" is flagged even if everyone agrees).
- **Manifest** (`/api/manifest`) is preparation metadata only (`"status": "preparation metadata, not a release
  authorization"`): unsigned, not bound to an event.
- **Routing:** the page is served only for app routes (`/<net>`, `/<net>/<producer>`, `/<net>/<producer>/<server>`,
  `/<net>/<producer>/endpoint/<host[/path]>`); anything else is a 404. Malformed percent-encoding is a 400, and any
  handler error returns 500 without taking the process down.
- The globe's block arcs are illustrative animation, not measured propagation.
