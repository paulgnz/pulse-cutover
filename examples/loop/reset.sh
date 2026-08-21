#!/usr/bin/env bash
# loop reset: return BOTH sides to a pre-ARM state for the next ceremony.
# Source: nodeos restarted (if the previous run stopped it), still syncing the
# LIVE XPR testnet — every iteration cuts at a fresh, real LIB.
# Target: the local metalgo network is wiped and a fresh subnet+chain created
# (an ignited PulseVM chain cannot be re-ignited; local network = no P-chain
# churn). The chain gets a FIXED alias "pulseloop" via the persisted aliases
# file, so the ceremony config's target rpc_url never changes.
set -euo pipefail
L=/root/api-cutover/loop

# --- source side back up + serving ---
systemctl start nodeos 2>/dev/null || true
for i in $(seq 1 90); do
  curl -s -m2 http://127.0.0.1:8888/v1/chain/get_info | grep -q head_block_num && break
  sleep 2
done
curl -s -m2 http://127.0.0.1:8888/v1/chain/get_info | grep -q head_block_num || { echo "reset: nodeos did not come back"; exit 1; }

# --- R12: staged snapshot must not exist pre-ARM ---
rm -f "$L/snapshot-cut.bin" "$L/captured-roots.txt"

# --- public loop URL back to nodeos ---
sed -i 's|server 127.0.0.1:8898;|server 127.0.0.1:8888;|' /etc/nginx/conf.d/pulse-loop-upstream.conf
nginx -t >/dev/null 2>&1 && nginx -s reload

# --- fresh local network ---
systemctl stop metalgo-local 2>/dev/null || true
rm -rf /root/metalgo-local/data /root/metalgo-local/log
rm -rf /etc/metalgo-local/chains
mkdir -p /root/metalgo-local /etc/metalgo-local/chains
echo '{}' > /root/metalgo-local/aliases.json

write_node_config(){ # $1 = track-subnets value ("" = none)
  {
    echo '{'
    echo '  "network-id": "local",'
    echo '  "sybil-protection-enabled": false,'
    echo '  "plugin-dir": "/opt/pulsevm/plugins",'
    [ -n "$1" ] && echo "  \"track-subnets\": \"$1\","
    echo '  "chain-config-dir": "/etc/metalgo-local/chains",'
    echo '  "chain-aliases-file": "/root/metalgo-local/aliases.json",'
    echo '  "data-dir": "/root/metalgo-local/data",'
    echo '  "log-dir": "/root/metalgo-local/log",'
    echo '  "http-host": "127.0.0.1",'
    echo '  "http-port": 9655,'
    echo '  "staking-port": 9656,'
    echo '  "public-ip": "127.0.0.1",'
    echo '  "log-level": "warn"'
    echo '}'
  } > /root/metalgo-local/node.json
}
wait_p(){
  for i in $(seq 1 120); do
    curl -s -m2 -X POST -H 'content-type:application/json' \
      -d '{"jsonrpc":"2.0","id":1,"method":"info.isBootstrapped","params":{"chain":"P"}}' \
      http://127.0.0.1:9655/ext/info 2>/dev/null | grep -q '"isBootstrapped":true' && return 0
    sleep 1
  done
  echo "reset: local P-chain did not bootstrap"; exit 1
}

write_node_config ""
systemctl start metalgo-local
wait_p
node "$L/createLocal.cjs" > "$L/create.out" 2>&1 || { echo "reset: createLocal failed"; cat "$L/create.out"; exit 1; }
SUBNET=$(grep '^SUBNET=' "$L/create.out" | cut -d= -f2)
BID=$(grep '^BID=' "$L/create.out" | cut -d= -f2)
[ -n "$SUBNET" ] && [ -n "$BID" ] || { echo "reset: no subnet/chain ids"; cat "$L/create.out"; exit 1; }

# Stable target URL: metalgo 1.13.5 does not register aliases-file entries on
# the HTTP router (404 on /ext/bc/<alias>/rpc even though the chain log IS
# aliased), so a one-line nginx proxy on 127.0.0.1:9657 is rewritten with the
# fresh blockchainID each iteration — the ceremony config never changes.
echo "server { listen 127.0.0.1:9657; location / { proxy_pass http://127.0.0.1:9655/ext/bc/$BID/rpc; proxy_set_header Host localhost; } }" > /etc/nginx/conf.d/pulse-loop-target.conf
nginx -t >/dev/null 2>&1 && nginx -s reload

mkdir -p "/etc/metalgo-local/chains/$BID"
cat > "/etc/metalgo-local/chains/$BID/config.json" <<JSON
{
  "producer_name": "eosio",
  "producer_key": "PVT_K1_<dev-producer-key-pairing-with-genesis-initial_key>",
  "snapshot_path": "$L/snapshot-cut.bin",
  "import_cpu_scale": 143
}
JSON
printf '{"%s": ["pulseloop"]}\n' "$BID" > /root/metalgo-local/aliases.json
write_node_config "$SUBNET"
systemctl restart metalgo-local
wait_p
echo "reset-done subnet=$SUBNET bid=$BID"
