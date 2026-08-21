#!/usr/bin/env bash
# One-time loop-mode setup on the box: metalgo-local unit, dedicated nginx
# listener on :8080 (the headline :80 setup is left exactly as the ceremony
# ended it), gateway-loop on :8898 pointed at the FIXED chain alias.
set -euo pipefail
L=/root/api-cutover/loop
mkdir -p "$L" /root/metalgo-local /etc/metalgo-local/chains
cp /root/api-cutover/genesis.json "$L/genesis.json" 2>/dev/null || true

cat > /etc/systemd/system/metalgo-local.service <<'UNIT'
[Unit]
Description=metalgo LOCAL network (loop-harness target substrate)
After=network.target
[Service]
ExecStart=/opt/metalgo/metalgo --config-file=/root/metalgo-local/node.json
Restart=always
RestartSec=3
LimitNOFILE=32768
TimeoutStopSec=120
[Install]
WantedBy=multi-user.target
UNIT

cat > /etc/systemd/system/gateway-loop.service <<UNIT
[Unit]
Description=PulseVM /v1/chain gateway (loop harness, fixed chain alias)
After=network.target
[Service]
Environment=UPSTREAM=http://127.0.0.1:9655/ext/bc/pulseloop/rpc
Environment=PORT=8898
ExecStart=$(command -v node) /opt/pulse-gateway/server.js
Restart=always
RestartSec=3
[Install]
WantedBy=multi-user.target
UNIT

cat > /etc/nginx/conf.d/pulse-loop-upstream.conf <<'CONF'
# Managed by the loop harness. flip-loop.sh swaps this single line.
upstream pulse_loop_backend { server 127.0.0.1:8888; }
CONF
cat > /etc/nginx/sites-available/pulse-loop <<'CONF'
server {
    listen 8080;
    server_name _;
    location /v1/chain/ {
        proxy_pass http://pulse_loop_backend;
        proxy_http_version 1.1;
        proxy_set_header Host localhost;
        proxy_read_timeout 30s;
    }
}
CONF
ln -sf /etc/nginx/sites-available/pulse-loop /etc/nginx/sites-enabled/pulse-loop
nginx -t >/dev/null && systemctl reload nginx

cat > "$L/flip-loop.sh" <<'FLIP'
#!/usr/bin/env bash
set -e
sed -i 's|server 127.0.0.1:8888;|server 127.0.0.1:8898;|' /etc/nginx/conf.d/pulse-loop-upstream.conf
nginx -t && nginx -s reload
echo flipped-loop
FLIP
cat > "$L/revert-loop.sh" <<'FLIP'
#!/usr/bin/env bash
set -e
sed -i 's|server 127.0.0.1:8898;|server 127.0.0.1:8888;|' /etc/nginx/conf.d/pulse-loop-upstream.conf
nginx -t && nginx -s reload
echo reverted-loop
FLIP
chmod +x "$L/flip-loop.sh" "$L/revert-loop.sh" "$L/reset.sh"

cat > "$L/ceremony-loop.toml" <<TOML
journal_path = "$L/journal.jsonl"
poll_ms = 250

[ceremony]
mode = "api"
freeze_height = 0
freeze_margin = 60
simulate_freeze = true
chain_id = "71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd"
import_cpu_scale = 143

[source]
rpc_url = "http://127.0.0.1:8888"
producer_api_url = "http://127.0.0.1:8888"
snapshot_timeout_secs = 900
stop_cmd = "systemctl stop nodeos"
start_cmd = "systemctl start nodeos"

[snapshot]
staged_path = "$L/snapshot-cut.bin"
path_map_from = "/data/snapshots"
path_map_to = "/root/nodeos/data/snapshots"
capture_roots = "$L/captured-roots.txt"

[target]
metalgo_unit = "metalgo-local"
rpc_url = "http://127.0.0.1:9655/ext/bc/pulseloop/rpc"
quorum_timeout_secs = 600
auto_rollback = true

[flip]
cmd = "$L/flip-loop.sh"
public_url = "http://127.0.0.1:8080"
revert_cmd = "$L/revert-loop.sh"
health_polls = 4
head_tolerance = 8
health_timeout_secs = 180

[loop]
reset_cmd = "$L/reset.sh"
settle_secs = 3
metrics_path = "$L/metrics.jsonl"
TOML

systemctl daemon-reload
systemctl enable gateway-loop metalgo-local >/dev/null 2>&1 || true
systemctl start gateway-loop
echo "loop setup done: $L/ceremony-loop.toml, reset at $L/reset.sh"
