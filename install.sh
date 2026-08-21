#!/usr/bin/env bash
# pulse-cutover installer — one command prepares a node for the cutover ceremony.
#
#   ./install.sh --mode api      --manifest ceremony.json  # API provider (nodeos serving /v1)
#   ./install.sh --mode bp       --manifest ceremony.json  # block producer (co-located nodeos)
#   ./install.sh --mode hyperion --manifest ceremony.json  # API provider + /v2 history continuity
#                                                          # (api mode + hyperion-rs + federating router)
#
# Discipline (modeled on the proven BP installer, wiki/36):
#   - everything determinism-critical is PINNED in the manifest + sha256-verified, fail-closed;
#   - tarballs are extracted SAFELY (--no-same-owner into a staging dir — a upstream release
#     tarball once chown'd /root to uid 1001; never again);
#   - idempotent: re-running re-verifies and converges, it does not duplicate;
#   - R12 stage-path hygiene: the staged snapshot path must NOT exist at install/arm time;
#   - refuses politely, with reasons, when the box isn't ready.
#
# What it does NOT do: touch your running nodeos, flip any traffic, or start a ceremony.
# The ceremony is cutover.sh's job, at H, and the only user-visible commitment (the /v1
# URL flip) happens after the new chain is verified and serving (R5 ratchet).
set -euo pipefail

MODE=""
MANIFEST=""
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE="$2"; shift 2;;
    --manifest) MANIFEST="$2"; shift 2;;
    *) echo "unknown arg: $1"; exit 2;;
  esac
done
[ -n "$MODE" ] || { echo "usage: ./install.sh --mode api|bp|hyperion --manifest ceremony.json"; exit 2; }
[ "$MODE" = "api" ] || [ "$MODE" = "bp" ] || [ "$MODE" = "hyperion" ] || { echo "ABORT: --mode must be api, bp or hyperion"; exit 2; }
# hyperion mode IS api mode (same /v1 machinery) + the /v2 history stack.
API_LIKE=false
if [ "$MODE" = "api" ] || [ "$MODE" = "hyperion" ]; then API_LIKE=true; fi
MANIFEST="${MANIFEST:-ceremony.json}"
[ -f "$MANIFEST" ] || { echo "ABORT: manifest $MANIFEST not found"; exit 2; }
[ "$(id -u)" = 0 ] || { echo "ABORT: run as root"; exit 2; }

echo "== pulse-cutover installer (mode: $MODE) =="
export DEBIAN_FRONTEND=noninteractive
command -v jq >/dev/null || { apt-get update -qq >/dev/null; apt-get install -y -qq jq >/dev/null; }

mget(){ jq -er "$1" "$MANIFEST"; }
mget_opt(){ jq -r "$1 // empty" "$MANIFEST"; }

# ---------- read manifest ----------
M_MODE=$(mget '.mode')
[ "$M_MODE" = "$MODE" ] || { echo "ABORT: manifest declares mode '$M_MODE' but --mode $MODE given"; exit 1; }
CHAIN_ID=$(mget '.ceremony.chain_id')
CPU_SCALE=$(mget '.ceremony.import_cpu_scale')
SRC_RPC=$(mget '.source.rpc_url')
SRC_PROD=$(mget '.source.producer_api_url')
SUBNET=$(mget '.target.subnet_id'); BID=$(mget '.target.blockchain_id'); VMID=$(mget '.target.vm_id')
NETWORK=$(mget '.target.network_id')
WORK=$(mget '.paths.work_dir')
STAGED="$WORK/snapshot-cut.bin"

# ---------- preflight: refuse politely, with reasons ----------
problems=()
. /etc/os-release || true
case "${VERSION_ID:-}" in 22.04|24.04) ;; *) problems+=("Ubuntu 22.04/24.04 required (found ${PRETTY_NAME:-unknown})");; esac
RAM_MB=$(free -m | awk '/Mem:/{print $2}')
RAM_GB=$(( (RAM_MB + 512) / 1024 ))
DISK_GB=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
# MB-based check: a "16 GB" box reports ~15.6 GB usable — free -g rounding
# must not fail it (dogfood finding on the cpx42 rehearsal box).
MIN_RAM_MB=15000; [ "$MODE" = "api" ] && MIN_RAM_MB=7500  # hyperion/bp need headroom (ES / chainbase)
[ "${RAM_MB:-0}" -ge "$MIN_RAM_MB" ] || problems+=("need >=$((MIN_RAM_MB/1000))GB RAM for $MODE mode (found ${RAM_GB}GB)")
[ "${DISK_GB:-0}" -ge 50 ] || problems+=("need >=50GB free disk (found ${DISK_GB}GB)")

# The source nodeos is YOURS — we detect it, we never install or touch it.
SRC_INFO=$(curl -s -m5 "$SRC_RPC/v1/chain/get_info" || true)
if [ -z "$SRC_INFO" ]; then
  problems+=("no nodeos answering at $SRC_RPC — an existing, synced $($API_LIKE && echo 'API node' || echo 'producer node') is a prerequisite")
else
  GOT_CHAIN=$(echo "$SRC_INFO" | jq -r '.chain_id // empty')
  [ "$GOT_CHAIN" = "$CHAIN_ID" ] || problems+=("nodeos at $SRC_RPC serves chain_id ${GOT_CHAIN:-none}, manifest expects $CHAIN_ID")
fi
PAUSED=$(curl -s -m5 -X POST "$SRC_PROD/v1/producer/paused" || true)
case "$PAUSED" in true|false) ;; *) problems+=("producer_api not reachable at $SRC_PROD (needed for create_snapshot; enable eosio::producer_api_plugin, localhost-bound — R10)");; esac
# R10: the producer_api must NOT be public.
if [ -n "$PAUSED" ]; then
  PUB_IP=$(curl -fsSL -m5 https://api.ipify.org 2>/dev/null || true)
  if [ -n "$PUB_IP" ]; then
    EXPOSED=$(curl -s -m3 -X POST "http://$PUB_IP:8888/v1/producer/paused" 2>/dev/null || true)
    case "$EXPOSED" in true|false) problems+=("producer_api is PUBLICLY reachable on $PUB_IP:8888 — it can stop your chain; bind it to localhost (R10)");; esac
  fi
fi
# R12: stage-path hygiene — a stale snapshot pins the target chain to the WRONG cut.
if [ -e "$STAGED" ]; then
  problems+=("staged snapshot $STAGED already exists (stale ceremony? R12) — remove it AND re-create the target chain if it ever initialized from it")
fi
if [ ${#problems[@]} -gt 0 ]; then
  echo ""
  echo "This box is not ready. Nothing was installed. Reasons:"
  for p in "${problems[@]}"; do echo "  - $p"; done
  exit 1
fi
echo "preflight: ok (${RAM_GB}GB RAM, ${DISK_GB}GB disk, nodeos serving $CHAIN_ID, producer_api localhost-only)"

# ---------- pinned, verified artifacts ----------
fetch_verify(){ # url sha dest
  local url="$1" sha="$2" dest="$3"
  if [ -f "$dest" ] && echo "$sha  $dest" | sha256sum -c - >/dev/null 2>&1; then
    echo "  ok (cached): $(basename "$dest")"; return
  fi
  echo "  fetch $(basename "$dest") ..."
  curl -fsSL "$url" -o "$dest.tmp"
  echo "$sha  $dest.tmp" | sha256sum -c - >/dev/null || { echo "ABORT: SHA256 mismatch on $dest (fail-closed)"; rm -f "$dest.tmp"; exit 1; }
  mv "$dest.tmp" "$dest"
  echo "  verified: $(basename "$dest")"
}
# Safe tarball extraction: NEVER extract an untrusted tarball over / or $HOME.
# (A pulsevm release tarball carrying uid-1001 ownership once chown'd /root.)
extract_safe(){ # tarball stagedir
  mkdir -p "$2"
  tar -xzf "$1" -C "$2" --no-same-owner --no-same-permissions
}

mkdir -p "$WORK" /opt/pulsevm/plugins /opt/metalgo /etc/pulse-cutover /opt/pulse-cutover
echo "artifacts:"
fetch_verify "$(mget '.artifacts.agent.url')"  "$(mget '.artifacts.agent.sha256')"  /usr/local/bin/pulse-cutover
chmod +x /usr/local/bin/pulse-cutover
fetch_verify "$(mget '.artifacts.plugin.url')" "$(mget '.artifacts.plugin.sha256')" "/opt/pulsevm/plugins/$VMID"
chmod +x "/opt/pulsevm/plugins/$VMID"
if [ ! -x /opt/metalgo/metalgo ] || ! echo "$(mget '.artifacts.metalgo.sha256')  /opt/metalgo/metalgo" | sha256sum -c - >/dev/null 2>&1; then
  fetch_verify "$(mget '.artifacts.metalgo.url')" "$(mget '.artifacts.metalgo.sha256')" /opt/metalgo/metalgo
  chmod +x /opt/metalgo/metalgo
else
  echo "  ok (present): metalgo"
fi

# ---------- metalgo node (tracks the target subnet; chain waits for the snapshot) ----------
PUBIP=$(curl -fsSL -m5 https://api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}')
mkdir -p /etc/metalgo/chains/"$BID" /root/.metalgo/configs
STAKING_DIR=$(mget_opt '.target.staking_dir')
{
  echo '{'
  echo "  \"network-id\": \"$NETWORK\","
  echo '  "plugin-dir": "/opt/pulsevm/plugins",'
  echo "  \"track-subnets\": \"$SUBNET\","
  echo '  "chain-config-dir": "/etc/metalgo/chains",'
  echo '  "data-dir": "/var/lib/metalgo",'
  echo '  "log-dir": "/var/log/metalgo",'
  [ -n "$STAKING_DIR" ] && echo "  \"staking-tls-cert-file\": \"$STAKING_DIR/staker.crt\","
  [ -n "$STAKING_DIR" ] && echo "  \"staking-tls-key-file\": \"$STAKING_DIR/staker.key\","
  [ -n "$STAKING_DIR" ] && echo "  \"staking-signer-key-file\": \"$STAKING_DIR/signer.key\","
  echo '  "http-host": "127.0.0.1",'
  echo '  "http-port": 9650,'
  echo '  "staking-port": 9651,'
  echo "  \"public-ip\": \"$PUBIP\","
  echo '  "log-level": "info"'
  echo '}'
} > /root/.metalgo/configs/node.json
# Chain config: producer pair per R11 (the genesis initial_key must pair with
# producer_key), snapshot_path DECLARED but the file ABSENT until VERIFIED (R12).
cat > "/etc/metalgo/chains/$BID/config.json" <<JSON
{
  "producer_name": "$(mget '.target.producer_name')",
  "producer_key": "$(mget '.target.producer_key')",
  "snapshot_path": "$STAGED",
  "import_cpu_scale": $CPU_SCALE
}
JSON
cat > /etc/systemd/system/metalgo-pulse.service <<UNIT
[Unit]
Description=metalgo + PulseVM (cutover target, subnet $SUBNET)
After=network-online.target
[Service]
ExecStart=/opt/metalgo/metalgo --config-file=/root/.metalgo/configs/node.json
Restart=always
RestartSec=5
LimitNOFILE=32768
TimeoutStopSec=180
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now metalgo-pulse >/dev/null 2>&1

# ---------- api mode: REST gateway + nginx flip machinery ----------
if $API_LIKE; then
  command -v node >/dev/null || { apt-get install -y -qq nodejs >/dev/null; }
  command -v nginx >/dev/null || { apt-get install -y -qq nginx >/dev/null; }
  mkdir -p /opt/pulse-gateway
  fetch_verify "$(mget '.artifacts.gateway.url')" "$(mget '.artifacts.gateway.sha256')" /opt/pulse-gateway/server.js
  cat > /etc/systemd/system/pulse-gateway.service <<UNIT
[Unit]
Description=PulseVM /v1/chain REST compatibility gateway
After=network.target
[Service]
Environment=UPSTREAM=http://127.0.0.1:9650/ext/bc/$BID/rpc
Environment=PORT=8899
ExecStart=$(command -v node) /opt/pulse-gateway/server.js
Restart=always
RestartSec=3
[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  # restart (not just enable --now): a re-run may have changed the unit's
  # UPSTREAM (new target chain) and a running service would keep the old env.
  systemctl enable pulse-gateway >/dev/null 2>&1
  systemctl restart pulse-gateway

  # nginx: ONLY /v1/chain is public (the producer_api must never be — R10).
  # The upstream indirection file is what the flip swaps: 8888 (nodeos) -> 8899 (gateway).
  NODEOS_PORT=$(echo "$SRC_RPC" | sed -E 's|.*:([0-9]+).*|\1|')
  cat > /etc/nginx/conf.d/pulse-cutover-upstream.conf <<CONF
# Managed by pulse-cutover. The ceremony's flip swaps this single line.
upstream pulse_v1_backend { server 127.0.0.1:$NODEOS_PORT; }
CONF
  cat > /etc/nginx/sites-available/pulse-v1 <<'CONF'
server {
    listen 80 default_server;
    server_name _;
    location /v1/chain/ {
        proxy_pass http://pulse_v1_backend;
        proxy_http_version 1.1;
        proxy_set_header Host localhost;
        proxy_read_timeout 30s;
    }
    location / { return 404 '{"error":"only /v1/chain is served here"}'; }
}
CONF
  rm -f /etc/nginx/sites-enabled/default
  ln -sf /etc/nginx/sites-available/pulse-v1 /etc/nginx/sites-enabled/pulse-v1
  nginx -t >/dev/null && systemctl reload nginx

  cat > /opt/pulse-cutover/flip-to-pulsevm.sh <<FLIP
#!/usr/bin/env bash
# The ONLY user-visible commitment of the ceremony (R5 ratchet): swap the
# public /v1 upstream from nodeos to the PulseVM gateway. Reads never gap:
# nginx reload is connection-graceful and nodeos is still up behind us.
set -e
sed -i 's|server 127.0.0.1:$NODEOS_PORT;|server 127.0.0.1:8899;|' /etc/nginx/conf.d/pulse-cutover-upstream.conf
nginx -t && nginx -s reload
echo flipped-to-pulsevm
FLIP
  cat > /opt/pulse-cutover/flip-revert.sh <<FLIP
#!/usr/bin/env bash
set -e
sed -i 's|server 127.0.0.1:8899;|server 127.0.0.1:$NODEOS_PORT;|' /etc/nginx/conf.d/pulse-cutover-upstream.conf
nginx -t && nginx -s reload
echo reverted-to-nodeos
FLIP
  chmod +x /opt/pulse-cutover/flip-*.sh
fi

# ---------- hyperion mode: /v2 history stack (ES + hyperion-rs + federator) ----------
if [ "$MODE" = "hyperion" ]; then
  command -v docker >/dev/null || problems_hyp="docker is required for the Elasticsearch container (install docker.io / docker-ce first)"
  [ -z "${problems_hyp:-}" ] || { echo "ABORT: $problems_hyp"; exit 1; }
  LEGACY_URL=$(mget '.hyperion.legacy_url')
  ES_HEAP=$(mget_opt '.hyperion.es_heap'); ES_HEAP=${ES_HEAP:-2g}
  ES_IMAGE=$(mget_opt '.hyperion.es_image'); ES_IMAGE=${ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:8.17.4}
  SHIP_WS=$(mget_opt '.hyperion.ship_ws'); SHIP_WS=${SHIP_WS:-ws://127.0.0.1:9090}

  # Elasticsearch: single-node container, heap capped (16 GB box budget:
  # nodeos + metalgo + dual-import verify + ES must coexist through the
  # ceremony; nodeos retires at the end).
  if ! docker inspect pulse-es >/dev/null 2>&1; then
    docker run -d --name pulse-es --restart unless-stopped \
      -p 127.0.0.1:9200:9200 \
      -e discovery.type=single-node -e xpack.security.enabled=false \
      -e "ES_JAVA_OPTS=-Xms$ES_HEAP -Xmx$ES_HEAP" \
      -v pulse-esdata:/usr/share/elasticsearch/data \
      "$ES_IMAGE" >/dev/null
    echo "  started: pulse-es ($ES_IMAGE, heap $ES_HEAP)"
  else
    docker start pulse-es >/dev/null 2>&1 || true
    echo "  ok (present): pulse-es"
  fi

  # hyperion-rs (pinned binary) + config against the NEW chain's RPC + SHiP.
  mkdir -p /opt/hyperion /etc/hyperion
  fetch_verify "$(mget '.artifacts.hyperion.url')" "$(mget '.artifacts.hyperion.sha256')" /opt/hyperion/hyperion
  chmod +x /opt/hyperion/hyperion
  cat > /etc/hyperion/config.toml <<HYP
# hyperion-rs against the post-cut PulseVM chain (staged by install.sh)
[chain]
name = "xpr"
http = "http://127.0.0.1:9650/ext/bc/$BID/rpc"
ship = "$SHIP_WS"
api = "pulsevm"
system_account = "eosio"

[indexer]
start_block = 0
stop_block = 0
fetch_block = true
fetch_traces = true
fetch_deltas = true
max_messages_in_flight = 128
batch_size = 200
flush_interval_ms = 500
skip_actions = []

[elasticsearch]
url = "http://127.0.0.1:9200"
user = ""
pass = ""
shards = 1
replicas = 0

[api]
listen = "127.0.0.1:7000"
max_limit = 1000
HYP
  for svc in indexer api; do
    cat > /etc/systemd/system/hyperion-$svc.service <<UNIT
[Unit]
Description=hyperion-rs $svc (post-cut PulseVM history)
After=network-online.target docker.service
[Service]
ExecStart=/opt/hyperion/hyperion $svc -c /etc/hyperion/config.toml
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
UNIT
  done
  # NOT enabled/started here: the ceremony's [hyperion].start_cmd starts them
  # after IGNITED, when the new chain's SHiP exists.

  # Federating history router (+ legacy passthrough for the pre-flip /v2).
  mkdir -p /opt/hyperion-federator
  fetch_verify "$(mget '.artifacts.federator.url')" "$(mget '.artifacts.federator.sha256')" /opt/hyperion-federator/server.js
  cat > /etc/systemd/system/hyperion-federator.service <<UNIT
[Unit]
Description=hyperion federating history router (/v2 boundary: legacy pre-cut, hyperion-rs post-cut)
After=network.target
[Service]
Environment=LOCAL=http://127.0.0.1:7000
Environment=LEGACY=$LEGACY_URL
Environment=BOUNDARY_FILE=/etc/pulse-cutover/boundary.json
Environment=PORT=7010
Environment=PASSTHROUGH_PORT=7019
ExecStart=$(command -v node) /opt/hyperion-federator/server.js
Restart=always
RestartSec=3
[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable hyperion-federator >/dev/null 2>&1
  systemctl restart hyperion-federator  # env (LEGACY/ports) may have changed on re-run

  # nginx: add the public /v2 surface. Pre-flip upstream = the legacy
  # passthrough ("the /v2 you already had" — point it at your own old
  # Hyperion instead if you kept one); the ceremony's v2 flip swaps it to
  # the federating router.
  cat > /etc/nginx/conf.d/pulse-cutover-v2-upstream.conf <<CONF
# Managed by pulse-cutover. The ceremony's /v2 flip swaps this single line.
upstream pulse_v2_backend { server 127.0.0.1:7019; }
CONF
  cat > /etc/nginx/sites-available/pulse-v1 <<'CONF'
server {
    listen 80 default_server;
    server_name _;
    location /v1/chain/ {
        proxy_pass http://pulse_v1_backend;
        proxy_http_version 1.1;
        proxy_set_header Host localhost;
        proxy_read_timeout 30s;
    }
    location /v2/ {
        proxy_pass http://pulse_v2_backend;
        proxy_http_version 1.1;
        proxy_read_timeout 30s;
    }
    location /v1/history/ {
        proxy_pass http://pulse_v2_backend;
        proxy_http_version 1.1;
        proxy_read_timeout 30s;
    }
    location / { return 404 '{"error":"only /v1/chain, /v1/history and /v2 are served here"}'; }
}
CONF
  nginx -t >/dev/null && systemctl reload nginx

  cat > /opt/pulse-cutover/flip-v2.sh <<'FLIP'
#!/usr/bin/env bash
# The /v2 half of the flip stage: public history moves to the federating
# router (pre-cut = legacy, post-cut = local hyperion-rs). Same single-line
# upstream swap discipline as /v1.
set -e
sed -i 's|server 127.0.0.1:7019;|server 127.0.0.1:7010;|' /etc/nginx/conf.d/pulse-cutover-v2-upstream.conf
nginx -t && nginx -s reload
echo flipped-v2-to-federator
FLIP
  cat > /opt/pulse-cutover/flip-v2-revert.sh <<'FLIP'
#!/usr/bin/env bash
set -e
sed -i 's|server 127.0.0.1:7010;|server 127.0.0.1:7019;|' /etc/nginx/conf.d/pulse-cutover-v2-upstream.conf
nginx -t && nginx -s reload
echo reverted-v2-to-passthrough
FLIP
  chmod +x /opt/pulse-cutover/flip-v2*.sh
fi

# ---------- ceremony.toml (the agent's manifest) ----------
FH=$(mget '.ceremony.freeze_height')
FM=$(mget_opt '.ceremony.freeze_margin')
SIM=$(mget_opt '.ceremony.simulate_freeze'); SIM=${SIM:-false}
STOP_CMD=$(mget_opt '.source.stop_cmd')
START_CMD=$(mget_opt '.source.start_cmd')
GOLDENS=$(mget_opt '.snapshot.golden_roots')
EXPECTED_SHA=$(mget_opt '.snapshot.expected_sha256')
{
  echo "journal_path = \"$WORK/journal.jsonl\""
  echo 'poll_ms = 250'
  echo ''
  echo '[ceremony]'
  echo "mode = \"$($API_LIKE && echo api || echo producer)\""
  echo "freeze_height = $FH"
  [ -n "$FM" ] && echo "freeze_margin = $FM"
  [ "$SIM" = "true" ] && echo 'simulate_freeze = true'
  echo "chain_id = \"$CHAIN_ID\""
  echo "import_cpu_scale = $CPU_SCALE"
  echo ''
  echo '[source]'
  echo "rpc_url = \"$SRC_RPC\""
  echo "producer_api_url = \"$SRC_PROD\""
  echo 'snapshot_timeout_secs = 900'
  [ -n "$STOP_CMD" ] && echo "stop_cmd = \"$STOP_CMD\""
  [ -n "$START_CMD" ] && echo "start_cmd = \"$START_CMD\""
  echo ''
  echo '[snapshot]'
  echo "staged_path = \"$STAGED\""
  SNAP_FROM=$(mget_opt '.source.snapshot_path_map_from'); SNAP_TO=$(mget_opt '.source.snapshot_path_map_to')
  [ -n "$SNAP_FROM" ] && echo "path_map_from = \"$SNAP_FROM\"" && echo "path_map_to = \"$SNAP_TO\""
  if [ -n "$GOLDENS" ]; then echo "golden_roots = \"$GOLDENS\""; else echo "capture_roots = \"$WORK/captured-roots.txt\""; fi
  [ -n "$EXPECTED_SHA" ] && echo "expected_sha256 = \"$EXPECTED_SHA\""
  echo ''
  echo '[target]'
  echo 'metalgo_unit = "metalgo-pulse"'
  echo "rpc_url = \"http://127.0.0.1:9650/ext/bc/$BID/rpc\""
  echo 'quorum_timeout_secs = 900'
  echo 'auto_rollback = true'
  if $API_LIKE; then
    echo ''
    echo '[flip]'
    echo 'cmd = "/opt/pulse-cutover/flip-to-pulsevm.sh"'
    echo "public_url = \"http://$(mget '.flip.public_host')\""
    echo 'revert_cmd = "/opt/pulse-cutover/flip-revert.sh"'
  fi
  if [ "$MODE" = "hyperion" ]; then
    echo ''
    echo '[hyperion]'
    echo 'start_cmd = "systemctl start hyperion-indexer hyperion-api"'
    echo 'health_url = "http://127.0.0.1:7000/v2/health"'
    echo 'hydration_timeout_secs = 900'
    echo 'boundary_path = "/etc/pulse-cutover/boundary.json"'
    echo 'flip_cmd = "/opt/pulse-cutover/flip-v2.sh"'
    echo 'revert_cmd = "/opt/pulse-cutover/flip-v2-revert.sh"'
    echo "public_health_url = \"http://$(mget '.flip.public_host')/v2/health\""
  fi
} > /etc/pulse-cutover/ceremony.toml
cp "$MANIFEST" /etc/pulse-cutover/ceremony.json

NODEID=$(curl -s -m5 http://127.0.0.1:9650/ext/info -X POST -H 'content-type:application/json' -d '{"jsonrpc":"2.0","id":1,"method":"info.getNodeID"}' | jq -r '.result.nodeID // "(metalgo still starting)"')
echo ""
echo "============================================================"
echo " ARMED-READY (mode: $MODE) — installed, verified, staged."
echo ""
echo " Source:  nodeos at $SRC_RPC serving $CHAIN_ID (untouched)"
echo " Target:  metalgo-pulse tracking subnet $SUBNET"
echo "          NodeID $NODEID"
echo "          chain $BID: waiting for the verified snapshot at"
echo "          $STAGED (absent by design — R12)"
$API_LIKE && echo " Public:  nginx /v1/chain -> nodeos:${NODEOS_PORT:-8888} (flip staged, NOT flipped)"
[ "$MODE" = "hyperion" ] && echo " History: nginx /v2 -> legacy passthrough ($(mget '.hyperion.legacy_url')); federator + hyperion-rs staged"
echo ""
echo " What happens at H (run: ./cutover.sh --manifest ceremony.json):"
echo "   1. agent watches the source chain to the freeze height"
echo "   2. snapshot via your nodeos producer_api at ~finality"
echo "   3. sha256 + dual-import 19-table fingerprint verification"
echo "   4. PulseVM ignites from the verified snapshot (same chain_id)"
if [ "$MODE" = "hyperion" ]; then
  echo "   5. hyperion-rs starts + hydrates against the new chain's SHiP"
  echo "   6. /v1 flips to PulseVM AND /v2 flips to the federating router"
  echo "      + health checks on both                <- only visible step"
  echo "   7. YOUR stop command retires nodeos (reads never gapped;"
  echo "      /v2 keeps its memory — pre-cut rows from the legacy Hyperion)"
elif $API_LIKE; then
  echo "   5. /v1 URL flips to PulseVM + health check   <- only visible step"
  echo "   6. YOUR stop command retires nodeos (reads never gapped)"
else
  echo "   5. LIVE gate: head advances past the cut, then traffic hooks"
fi
echo "============================================================"
