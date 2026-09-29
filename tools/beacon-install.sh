#!/usr/bin/env bash
# beacon-install.sh — connect this producer's box to Cutover Mission Control (read-only readiness beacon).
#
#   curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh \
#     | sudo bash -s -- --network testnet --producer <your-account>
#
# What it does (and does NOT do):
#   + downloads the static pulse-cutover binary from the GitHub release and verifies its sha256
#   + runs `pulse-cutover doctor` (read-only) to find your nodeos chain API and chain_id
#   + writes a readiness-only config to /etc/pulse-cutover/beacon.toml (no ceremony can run from it:
#     no freeze height, no hooks, no target chain)
#   + generates a beacon token ON THIS BOX (/etc/pulse-cutover/beacon.token, mode 600) and prints
#     only its sha256 — send that hash to the mission-control operator; the token never leaves the box
#   + starts the `pulse-beacon` systemd service
#   - never touches nodeos, its config, its keys, or block production; never opens a port
#
# Options: --network <id> --producer <account> [--url https://control-rehearsal.protonnz.com]
#          [--version v0.5.0-rc.1] [--api http://127.0.0.1:8888] [--producer-api <url>]
#          [--snapshots-dir <dir>] [--interval 10] [--dry-run] [--uninstall]
set -euo pipefail
NETWORK=""; PRODUCER=""; URL="https://control-rehearsal.protonnz.com"; VERSION="v0.5.0-rc.1"
API=""; PAPI=""; SNAPDIR=""; INTERVAL=10; DRY=0; UNINSTALL=0
while [ $# -gt 0 ]; do case "$1" in
  --network) NETWORK=$2; shift 2;; --producer) PRODUCER=$2; shift 2;; --url) URL=$2; shift 2;;
  --version) VERSION=$2; shift 2;; --api) API=$2; shift 2;; --producer-api) PAPI=$2; shift 2;;
  --snapshots-dir) SNAPDIR=$2; shift 2;; --interval) INTERVAL=$2; shift 2;;
  --dry-run) DRY=1; shift;; --uninstall) UNINSTALL=1; shift;;
  *) echo "unknown option $1" >&2; exit 2;; esac; done
say() { printf '\033[1m==>\033[0m %s\n' "$*"; }
[ "$(id -u)" = 0 ] || { echo "run as root (sudo)"; exit 1; }
if [ "$UNINSTALL" = 1 ]; then
  systemctl disable --now pulse-beacon 2>/dev/null || true; rm -f /etc/systemd/system/pulse-beacon.service
  systemctl daemon-reload; say "beacon removed (kept /etc/pulse-cutover for your records)"; exit 0
fi
[ -n "$NETWORK" ] && [ -n "$PRODUCER" ] || { echo "need --network and --producer" >&2; exit 2; }
[[ "$PRODUCER" =~ ^[a-z1-5.]{1,12}$ ]] || { echo "--producer must be an Antelope account name" >&2; exit 2; }

ARCH=$(uname -m); case "$ARCH" in x86_64) T=x86_64-unknown-linux-musl;; aarch64|arm64) T=aarch64-unknown-linux-musl;; *) echo "unsupported arch $ARCH"; exit 1;; esac
BIN=/usr/local/bin/pulse-cutover; REL="https://github.com/paulgnz/pulse-cutover/releases/download/$VERSION"
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
say "downloading pulse-cutover $VERSION ($T)"
curl -fsSL -o "$TMP/pulse-cutover-$T" "$REL/pulse-cutover-$T"
curl -fsSL -o "$TMP/sha256sums.txt" "$REL/sha256sums.txt"
(cd "$TMP" && sha256sum -c sha256sums.txt --ignore-missing) || { echo "checksum mismatch: refusing to install"; exit 1; }
[ "$DRY" = 1 ] || install -m 755 "$TMP/pulse-cutover-$T" "$BIN"
B=$([ "$DRY" = 1 ] && echo "$TMP/pulse-cutover-$T" || echo "$BIN"); chmod +x "$B"

say "surveying this box (read-only doctor)"
VAR=/var/lib/pulse-cutover; ETC=/etc/pulse-cutover
if [ "$DRY" = 1 ]; then VAR=$TMP/var; ETC=$TMP/etc; fi
mkdir -p "$VAR"
"$B" doctor --json > "$VAR/doctor.json" 2>/dev/null || true
jqget() { command -v jq >/dev/null && jq -r "$1 // empty" "$VAR/doctor.json" 2>/dev/null || true; }
[ -n "$API" ] || API=$(jqget '.nodeos.chain_api_url'); [ -n "$API" ] || API="http://127.0.0.1:8888"
[ -n "$PAPI" ] || PAPI="$API"
INFO=$(curl -fsS -m 5 -X POST "$API/v1/chain/get_info" -d '{}' || true)
CHAIN_ID=$(printf '%s' "$INFO" | sed -n 's/.*"chain_id":"\([0-9a-f]\{64\}\)".*/\1/p')
HEAD=$(printf '%s' "$INFO" | sed -n 's/.*"head_block_num":\([0-9]*\).*/\1/p')
[ -n "$CHAIN_ID" ] || { echo "could not reach nodeos chain API at $API (use --api)"; exit 1; }
if [ -z "$SNAPDIR" ]; then
  CFG=$(jqget '.nodeos.config_dir'); [ -n "$CFG" ] && SNAPDIR=$(sed -n 's/^\s*snapshots-dir\s*=\s*//p' "$CFG/config.ini" 2>/dev/null | tail -1)
  [ -n "$SNAPDIR" ] || SNAPDIR=/var/lib/pulse-cutover/snapshots
fi
say "nodeos: $API · chain ${CHAIN_ID:0:16}… · head $HEAD · snapshots $SNAPDIR"

mkdir -p "$ETC" && chmod 750 "$ETC"
if [ ! -s "$ETC/beacon.token" ]; then (umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ETC/beacon.token"); fi
HASH=$(tr -d '\n' < "$ETC/beacon.token" | sha256sum | cut -d' ' -f1)

cat > "$ETC/beacon.toml" <<TOML
# Readiness-only config for pulse-cutover beacon (written by beacon-install.sh).
# No ceremony can run from this file: freeze height is unset (0) and no hooks or target chain exist yet.
journal_path = "/var/lib/pulse-cutover/journal.jsonl"
poll_ms = 1000

[ceremony]
mode = "producer"
freeze_height = 0
freeze_margin = 420
freeze_strategy = "schedule_at_h"
chain_id = "$CHAIN_ID"
import_cpu_scale = 143

[source]
rpc_url = "$API"
producer_api_url = "$PAPI"

[snapshot]
staged_path = "/var/lib/pulse-cutover/snapshot-cut.bin"
dir = "$SNAPDIR"

[target]
metalgo_unit = "metalgo"
rpc_url = "http://127.0.0.1:9650/ext/bc/NOT-CONFIGURED/rpc"

[beacon]
url = "${URL%/}/api/report"
producer = "$PRODUCER"
network = "$NETWORK"
token_file = "/etc/pulse-cutover/beacon.token"
interval_secs = $INTERVAL
TOML
chmod 640 "$ETC/beacon.toml"

say "first report (printed, not sent):"
sed 's|^url = .*|url = ""|' "$ETC/beacon.toml" > "$TMP/once.toml"
"$B" beacon --config "$TMP/once.toml" --once | sed -n '/"checks"/,/\]/p' | grep -E '"name"|"ok"|"detail"' | paste - - - | sed 's/  */ /g' | head -20 || true

if [ "$DRY" = 1 ]; then say "dry run: nothing installed or written outside a temp dir (token hash would be $HASH)"; exit 0; fi
cat > /etc/systemd/system/pulse-beacon.service <<UNIT
[Unit]
Description=pulse-cutover beacon (read-only readiness reporter → mission control)
After=network-online.target
[Service]
ExecStart=$BIN beacon --config /etc/pulse-cutover/beacon.toml
Restart=always
RestartSec=10
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=/var/lib/pulse-cutover
PrivateTmp=yes
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload; systemctl enable --now pulse-beacon >/dev/null 2>&1; systemctl restart pulse-beacon
say "beacon running: $(systemctl is-active pulse-beacon)"
echo
echo "  Send this to the mission-control operator to authorize your beacon:"
echo "    network=$NETWORK producer=$PRODUCER token_sha256=$HASH"
echo "  Dashboard: ${URL%/}/?net=$NETWORK&p=$PRODUCER"
echo "  Remove any time: sudo bash beacon-install.sh --uninstall"
