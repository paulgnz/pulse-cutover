#!/usr/bin/env bash
# beacon-install.sh — connect one server to Cutover Mission Control (readiness beacon).
#
#   curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh | sudo bash
#
# Network (mainnet/testnet) is detected from the node's chain_id and the producer account from
# `producer-name` in nodeos' config.ini; pass --network / --producer to override.
#
# What the INSTALLER writes: the pulse-cutover binary (/usr/local/bin/pulse-cutover), a readiness config
# (/etc/pulse-cutover/beacon.toml), a token and an instance id (/etc/pulse-cutover/beacon.token, beacon.instance),
# a doctor report (/var/lib/pulse-cutover/doctor.json), the beacon's own state dir (/var/lib/pulse-beacon) and the
# `pulse-beacon` systemd service. The beacon can write ONLY its state dir: the ceremony directory
# /var/lib/pulse-cutover (journal, lock, staged snapshot) stays root-owned and is read-only to the beacon.
# What the running BEACON does: reads local state (nodeos get_info, Metal node info, service status, free disk)
# and reports it to mission control every few seconds. It never touches nodeos, its config, keys or block
# production, and opens no port.
#
# Token: generated on this box. You enroll it by sending only its sha256 to the mission-control operator.
# The beacon then sends the token itself (over HTTPS) with every report, which is how mission control knows
# the report is yours.
#
# Everything is downloaded, verified and checked BEFORE anything on the box changes; a failed update restores
# the previous binary and config.
#
# Options: --network <mainnet|testnet> --producer <account> --node <label> --role <producer|api|history>
#          [--url https://control-rehearsal.protonnz.com] [--version vX.Y.Z] [--api http://127.0.0.1:8888]
#          [--producer-api <url>] [--snapshots-dir <dir>] [--interval 10] [--force] [--dry-run] [--yes] [--uninstall]
set -euo pipefail

VERSION_DEFAULT="v0.5.0-rc.21"
URL_DEFAULT="https://control-rehearsal.protonnz.com"
ETC=/etc/pulse-cutover; VAR=/var/lib/pulse-cutover; STATE=/var/lib/pulse-beacon; BIN=/usr/local/bin/pulse-cutover
# Version of an installed pulse-cutover binary: rc.16+ prints it; older builds only embed the string.
bin_version() {
  [ -x "$1" ] || return 0
  local v; v=$("$1" --version 2>/dev/null | sed -n 's/^pulse-cutover \([0-9][0-9A-Za-z.+-]*\)$/\1/p' | head -1)
  [ -z "$v" ] || printf 'v%s' "$v"
}
# Pre-rc.16 binaries have no --version: ask mission control what this beacon last reported.
reported_version() {
  [ -n "${URL:-}" ] && [ -n "${NETWORK:-}" ] && [ -n "${PRODUCER:-}" ] && [ -n "${NODE:-}" ] || return 0
  local v; v=$(curl -fsS -m 5 "${URL%/}/api/node/$NETWORK/$PRODUCER/$NODE" 2>/dev/null | jstdin report.agent_version)
  [ -z "$v" ] || printf 'v%s' "${v#v}"
}
# Other pulse-cutover services on this box (e.g. `await`) keep running the binary they started with.
# Never restarted here: an await may be driving a ceremony. Listed so the operator can restart them.
other_units() {
  systemctl list-units --type=service --state=active --no-legend --plain 2>/dev/null | awk '{print $1}' | while read -r u; do
    [ "$u" = pulse-beacon.service ] && continue
    if systemctl show -p ExecStart --value "$u" 2>/dev/null | grep -q "$BIN"; then echo "$u"; fi
  done
  return 0
}
UNIT=/etc/systemd/system/pulse-beacon.service; SVC_USER=pulse-beacon
MAINNET_CHAIN=384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0
TESTNET_CHAIN=71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd

say() { printf '\033[1m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[!]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

# ---- pure helpers (unit-tested in tools/test/run.sh) --------------------------------------------------
# The bearer token travels with every report, so the report URL must be HTTPS (plain http only to this host).
# Parsed, not pattern-matched: `http://localhost.example.org` and `http://localhost:80@example.org` are remote.
check_url() {
  local why
  why=$(python3 - "$1" <<'PY2'
import sys, urllib.parse
u = sys.argv[1]
try: p = urllib.parse.urlsplit(u); host = p.hostname; p.port
except Exception as e: print(f"not a valid URL ({e.__class__.__name__})"); sys.exit(1)
if p.username is not None or p.password is not None or "@" in p.netloc: print("URL must not contain user info (user@host)"); sys.exit(1)
if not host: print("URL has no host"); sys.exit(1)
if p.scheme == "https": sys.exit(0)
if p.scheme == "http" and host in ("localhost", "127.0.0.1", "::1"): sys.exit(0)
print("must be https:// (plain http only to localhost / 127.0.0.1 / [::1])"); sys.exit(1)
PY2
  ) || die "mission-control URL rejected: $why (the beacon token is sent with every report): got '$1'"
}
# existing_ancestor PATH → PATH itself or its nearest existing parent (free space is measured there when the
# snapshots dir does not exist yet).
existing_ancestor() { local p=$1; while [ -n "$p" ] && [ "$p" != / ] && [ ! -e "$p" ]; do p=$(dirname "$p"); done; printf '%s' "${p:-/}"; }
check_producer() { [[ "$1" =~ ^[a-z1-5.]{1,12}$ ]] || die "--producer must be an Antelope account name (got '$1')"; }
mode_for_role() { [ "$1" = producer ] && echo producer || echo api; }
# toml_read FILE → shell-quoted OLD_<section>_<key>=value lines for the simple `key = value` TOML this script
# writes (works on Python 3.8, which has no tomllib).
toml_read() {
  python3 - "$1" <<'PY'
import re, shlex, sys
sec = ""
for line in open(sys.argv[1], encoding="utf-8"):
    line = line.strip()
    if not line or line.startswith("#"): continue
    m = re.match(r"^\[([A-Za-z0-9_.-]+)\]$", line)
    if m: sec = m.group(1).replace(".", "_"); continue
    m = re.match(r'^([A-Za-z0-9_]+)\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^#\s]+))', line)
    if m:
        k = f"OLD_{sec}_{m.group(1)}" if sec else f"OLD_{m.group(1)}"
        v = m.group(2) if m.group(2) is not None else m.group(3)
        print(f"{k}={shlex.quote(v)}")
PY
}
jfile() { python3 -c 'import json,sys
try: v=json.load(open(sys.argv[1]))
except Exception: sys.exit(0)
for k in sys.argv[2].split("."):
  v=v.get(k) if isinstance(v,dict) else None
if v is not None: print(v)' "$1" "$2"; }
jstdin() { python3 -c 'import json,sys
try: v=json.load(sys.stdin)
except Exception: sys.exit(0)
for k in sys.argv[1].split("."):
  v=v.get(k) if isinstance(v,dict) else None
if v is not None: print(v)' "$1"; }
token_hash() { [ -s "$1" ] && tr -d '\n' < "$1" | sha256sum | cut -d' ' -f1 || true; }

parse_args() {
  NETWORK=""; PRODUCER=""; NODE=""; ROLE=""; URL=""; VERSION=""; API=""; PAPI=""; SNAPDIR=""; INTERVAL=""
  DRY=0; UNINSTALL=0; FORCE=0; YES=0
  while [ $# -gt 0 ]; do case "$1" in
    --network) NETWORK=${2:-}; shift 2;; --producer) PRODUCER=${2:-}; shift 2;; --node) NODE=${2:-}; shift 2;;
    --role) ROLE=${2:-}; shift 2;; --url) URL=${2:-}; shift 2;; --version) VERSION=${2:-}; shift 2;;
    --api) API=${2:-}; shift 2;; --producer-api) PAPI=${2:-}; shift 2;; --snapshots-dir) SNAPDIR=${2:-}; shift 2;;
    --interval) INTERVAL=${2:-}; shift 2;; --force) FORCE=1; shift;;
    --dry-run) DRY=1; shift;; --uninstall) UNINSTALL=1; shift;; --yes|-y) YES=1; shift;;
    *) die "unknown option $1";; esac; done
  case "$ROLE" in ""|producer|api|history) ;; *) die "--role must be producer, api or history";; esac
  case "$NETWORK" in ""|mainnet|testnet) ;; *) die "--network must be mainnet or testnet";; esac
  [ -z "$INTERVAL" ] || [[ "$INTERVAL" =~ ^[0-9]+$ ]] || die "--interval must be a number of seconds"
  [ -z "$NODE" ] || [[ "$NODE" =~ ^[A-Za-z0-9._-]{1,48}$ ]] || die "--node must be 1-48 of A-Z a-z 0-9 . _ -"
}

# restore_previous: put back binary, config, token, instance id and unit as they were before this run.
APPLYING=0; RESTORED=0; RETIRED=""; OLD_FOUND=0
restore_previous() {
  RESTORED=1; APPLYING=0
  warn "the update did not complete: restoring the previous beacon"
  systemctl stop pulse-beacon 2>/dev/null || true
  local f
  for f in "$BIN" "$ETC/beacon.toml" "$ETC/beacon.instance" "$UNIT"; do
    if [ -f "$f.prev" ]; then mv -f "$f.prev" "$f"; else [ "$OLD_FOUND" = 1 ] || rm -f "$f"; fi
    rm -f "$f.new"
  done
  # token: if this run retired the enrolled token, bring it back; if it replaced one, restore the old bytes
  if [ -n "$RETIRED" ] && [ -f "$RETIRED" ]; then mv -f "$RETIRED" "$ETC/beacon.token"
  elif [ -f "$ETC/beacon.token.prev" ]; then mv -f "$ETC/beacon.token.prev" "$ETC/beacon.token"; fi
  systemctl daemon-reload 2>/dev/null || true
  if [ "$OLD_FOUND" = 1 ] && [ -f "$UNIT" ]; then
    systemctl restart pulse-beacon 2>/dev/null || true; sleep 2
    RESTORE_MSG="rolled back to the previous beacon (service: $(systemctl is-active pulse-beacon 2>/dev/null); token hash $(token_hash "$ETC/beacon.token" | cut -c1-12)…, unchanged)"
  else
    systemctl disable --now pulse-beacon >/dev/null 2>&1 || true
    RESTORE_MSG="install failed: nothing is running; the partial install was removed"
  fi
}
on_exit() {
  local rc=$?
  if [ "$APPLYING" = 1 ] && [ "$RESTORED" = 0 ]; then
    restore_previous
    printf '\033[31m[x]\033[0m update failed (exit %s). %s. Send the lines above to the operator.\n' "$rc" "$RESTORE_MSG" >&2
    rc=1
  fi
  [ -n "${TMP:-}" ] && rm -rf "$TMP"
  exit $rc
}

uninstall() {
  systemctl disable --now pulse-beacon 2>/dev/null || true; rm -f "$UNIT"
  systemctl daemon-reload; say "beacon removed (kept $ETC for your records; the token there is still enrolled until the operator revokes it)"
}
# beacon_unit RUNAS BIN ETC STATE → the systemd unit text. Same sandbox whether the beacon runs as its own user
# or (fallback, when that user can't read the journal/snapshots) as root limited to CAP_DAC_READ_SEARCH.
# Network: AF_INET/AF_INET6 for reports to mission control and local RPCs; AF_UNIX because `systemctl is-active`
# talks to systemd over D-Bus. No IP allow-list: mission control's address can change and the beacon also polls
# local nodeos/metalgo; the report destination is pinned by the https-only URL check instead.
beacon_unit() {
  local runas=$1 bin=$2 etc=$3 state=$4
  cat <<UNIT
[Unit]
Description=pulse-cutover beacon (readiness reporter → mission control)
After=network-online.target
[Service]
$( if [ "$runas" = root ]; then printf 'CapabilityBoundingSet=CAP_DAC_READ_SEARCH\nAmbientCapabilities=\n'; else printf 'User=%s\n' "$runas"; fi )
ExecStart=$bin beacon --config $etc/beacon.toml
Restart=always
RestartSec=10
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=read-only
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
ProtectClock=yes
ProtectHostname=yes
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallArchitectures=native
SystemCallFilter=@system-service
StateDirectory=pulse-beacon
ReadWritePaths=$state
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
[Install]
WantedBy=multi-user.target
UNIT
}


main() {
  parse_args "$@"
  [ "$(id -u)" = 0 ] || die "run as root (sudo)"
  command -v python3 >/dev/null || die "python3 is required (it ships with every supported Ubuntu)"
  if [ "$UNINSTALL" = 1 ]; then uninstall; exit 0; fi

  # ---- re-run = upgrade: every existing value is kept unless a flag overrides it ------------------------
  OLD_FOUND=0; local CHANGED_OWNER=0 HASH_BEFORE=""
  JOURNAL=""; STAGED=""; TARGET_RPC=""
  if [ -f "$ETC/beacon.toml" ]; then
    OLD_FOUND=1; eval "$(toml_read "$ETC/beacon.toml")"
    local old_prod=${OLD_beacon_producer:-} old_net=${OLD_beacon_network:-}
    [ -n "$PRODUCER" ] || PRODUCER=$old_prod;             [ -n "$NETWORK" ] || NETWORK=$old_net
    [ -n "$NODE" ] || NODE=${OLD_beacon_node:-};          [ -n "$ROLE" ] || ROLE=${OLD_beacon_role:-}
    [ -n "$INTERVAL" ] || INTERVAL=${OLD_beacon_interval_secs:-}
    [ -n "$API" ] || API=${OLD_source_rpc_url:-};         [ -n "$PAPI" ] || PAPI=${OLD_source_producer_api_url:-}
    [ -n "$SNAPDIR" ] || SNAPDIR=${OLD_snapshot_dir:-}
    JOURNAL=${OLD_journal_path:-}; STAGED=${OLD_snapshot_staged_path:-}; TARGET_RPC=${OLD_target_rpc_url:-}
    if [ -z "$URL" ] && [ -n "${OLD_beacon_url:-}" ]; then URL=${OLD_beacon_url%/api/report}; fi
    if { [ -n "$old_prod" ] && [ "$PRODUCER" != "$old_prod" ]; } || { [ -n "$old_net" ] && [ "$NETWORK" != "$old_net" ]; }; then
      CHANGED_OWNER=1; warn "producer/network changed ($old_prod@$old_net → $PRODUCER@$NETWORK): a NEW token will be generated and must be enrolled"
    fi
    say "existing beacon found: upgrading in place (producer ${PRODUCER:-?}, node ${NODE:-?}); existing settings kept"
  fi
  HASH_BEFORE=$(token_hash "$ETC/beacon.token")
  URL=${URL:-$URL_DEFAULT}; VERSION=${VERSION:-$VERSION_DEFAULT}; INTERVAL=${INTERVAL:-10}
  JOURNAL=${JOURNAL:-$VAR/journal.jsonl}; STAGED=${STAGED:-$VAR/snapshot-cut.bin}
  TARGET_RPC=${TARGET_RPC:-http://127.0.0.1:9650/ext/bc/NOT-CONFIGURED/rpc}
  check_url "$URL"

  # ---- download + verify + survey, all in a temp dir (nothing on the box changes yet) --------------------
  local ARCH T REL TMP B CURRENT
  ARCH=$(uname -m); case "$ARCH" in x86_64) T=x86_64-unknown-linux-musl;; aarch64|arm64) T=aarch64-unknown-linux-musl;; *) die "unsupported arch $ARCH";; esac
  REL="https://github.com/paulgnz/pulse-cutover/releases/download/$VERSION"
  TMP=$(mktemp -d); trap on_exit EXIT
  CURRENT=$(bin_version "$BIN"); [ -n "$CURRENT" ] || CURRENT=$(reported_version)
  if [ -z "$CURRENT" ]; then say "installing pulse-cutover $VERSION ($T)"
  elif [ "$CURRENT" = "$VERSION" ]; then say "current: $CURRENT → already the target version; reinstalling $VERSION"
  else say "current: $CURRENT → upgrading to: $VERSION"; fi
  say "downloading pulse-cutover $VERSION ($T)"
  curl -fsSL -o "$TMP/pulse-cutover-$T" "$REL/pulse-cutover-$T" || die "download failed: $REL/pulse-cutover-$T"
  curl -fsSL -o "$TMP/sha256sums.txt" "$REL/sha256sums.txt" || die "download failed: $REL/sha256sums.txt"
  grep -q " pulse-cutover-$T\$" "$TMP/sha256sums.txt" || die "sha256sums.txt has no entry for pulse-cutover-$T: refusing"
  (cd "$TMP" && grep " pulse-cutover-$T\$" sha256sums.txt | sha256sum -c --quiet) || die "checksum mismatch: refusing to install"
  B="$TMP/pulse-cutover-$T"; chmod +x "$B"
  "$B" help >/dev/null 2>&1 || die "the downloaded binary does not run here"

  say "surveying this box (read-only doctor)"
  if ! "$B" doctor --json > "$TMP/doctor.json" 2>"$TMP/doctor.err" || ! python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$TMP/doctor.json" 2>/dev/null; then
    [ "$FORCE" = 1 ] || die "doctor failed ($(head -c 200 "$TMP/doctor.err")): re-run with --force to install anyway, or send the output to the operator"
    warn "doctor failed; continuing because of --force"; echo '{}' > "$TMP/doctor.json"
  fi
  [ -n "$API" ] || API=$(jfile "$TMP/doctor.json" nodeos.chain_api_url); API=${API:-http://127.0.0.1:8888}
  [ -n "$PAPI" ] || PAPI="$API"
  local INFO CHAIN_ID HEAD
  INFO=$(curl -fsS -m 5 -X POST "$API/v1/chain/get_info" -d '{}' 2>/dev/null || true)
  CHAIN_ID=$(printf '%s' "$INFO" | jstdin chain_id); HEAD=$(printf '%s' "$INFO" | jstdin head_block_num)
  [[ "$CHAIN_ID" =~ ^[0-9a-f]{64}$ ]] || die "could not reach a nodeos chain API at $API (use --api)"
  local CHAIN_NET=""
  case "$CHAIN_ID" in "$MAINNET_CHAIN") CHAIN_NET=mainnet;; "$TESTNET_CHAIN") CHAIN_NET=testnet;; esac
  if [ -z "$NETWORK" ]; then [ -n "$CHAIN_NET" ] || die "unknown chain ${CHAIN_ID:0:16}…: pass --network"; NETWORK=$CHAIN_NET
  elif [ -n "$CHAIN_NET" ] && [ "$NETWORK" != "$CHAIN_NET" ]; then die "--network $NETWORK does not match this node's chain ($CHAIN_NET)"; fi

  # nodeos config: the doctor's effective config path (honours --config/--config-dir), then process args
  local CFGFILE CFG_PRODUCER="" f
  CFGFILE=$(jfile "$TMP/doctor.json" nodeos.config_path)
  for f in "$CFGFILE" $(ps -o args= -C nodeos 2>/dev/null | grep -o -- '--config-dir[= ][^ ]*' | awk '{print $NF}' | sed 's/.*=//; s|$|/config.ini|'); do
    [ -n "$f" ] && [ -f "$f" ] || continue
    CFG_PRODUCER=$(sed -n 's/^\s*producer-name\s*=\s*\([a-z1-5.]\{1,12\}\).*/\1/p' "$f" | head -1)
    [ -n "$SNAPDIR" ] || SNAPDIR=$(sed -n 's/^\s*snapshots-dir\s*=\s*//p' "$f" | tail -1)
    [ -n "$CFG_PRODUCER" ] && break
  done
  if [ -z "$SNAPDIR" ]; then   # nodeos default: <data-dir>/snapshots
    local DD; DD=$(ps -o args= -C nodeos 2>/dev/null | grep -o -- '--data-dir[= ][^ ]*' | head -1 | awk '{print $NF}' | sed 's/.*=//' || true)
    [ -n "$DD" ] && [ -d "$DD" ] && SNAPDIR="$DD/snapshots"
  fi
  SNAPDIR=${SNAPDIR:-$VAR/snapshots}
  [ -n "$PRODUCER" ] || PRODUCER=$CFG_PRODUCER
  [ -n "$PRODUCER" ] || die "could not find producer-name in the nodeos config: pass --producer <your-account>"
  check_producer "$PRODUCER"
  if [ -z "$ROLE" ]; then
    if [ -n "$CFG_PRODUCER" ]; then ROLE=producer
    elif curl -fsS -m3 http://127.0.0.1:7000/v2/health 2>/dev/null | grep -q '"health"'; then ROLE=history
    else ROLE=api; fi
  fi
  local MODE; MODE=$(mode_for_role "$ROLE")
  [ -n "$NODE" ] || NODE=$ROLE   # public label; the hostname is never sent
  local MG_UNIT=${OLD_target_metalgo_unit:-}
  # A re-run keeps the saved unit only while it still exists (e.g. a node moved from metalgo-local to metalgo).
  if [ -n "$MG_UNIT" ] && ! systemctl list-unit-files --type=service --no-legend 2>/dev/null | awk '{print $1}' | grep -qx "$MG_UNIT.service"; then
    warn "saved Metal service '$MG_UNIT' no longer exists: detecting again"
    MG_UNIT=""
  fi
  if [ -z "$MG_UNIT" ]; then
    local units; units=$(systemctl list-unit-files --type=service --no-legend 2>/dev/null | awk '{print $1}' | grep -iE '^(metalgo|avalanchego)[^ ]*\.service$' | sed 's/\.service$//' || true)
    MG_UNIT=$(printf '%s\n' "$units" | grep -x metalgo || printf '%s\n' "$units" | head -1)
    [ "$(printf '%s\n' "$units" | grep -c .)" -gt 1 ] && warn "several Metal services found ($(echo $units)): using '$MG_UNIT'"
  fi
  MG_UNIT=${MG_UNIT:-metalgo}
  say "server label: $NODE · role $ROLE · ceremony mode $MODE"
  [ "$OLD_FOUND" = 1 ] || say "tip: several servers of the same role under one producer? give each its own label with --node <name>"
  say "nodeos: $API · chain ${CHAIN_ID:0:16}… ($NETWORK) · head $HEAD · producer $PRODUCER · snapshots $SNAPDIR · Metal unit $MG_UNIT"

  # ---- build the config in the temp dir. Readiness profile when the binary supports it. -------------------
  write_config() {   # $1 = with profile line (1/0)
    cat <<TOML
# Readiness-only config for the pulse-cutover beacon (written by beacon-install.sh $VERSION).
# It is for reporting only. Do not run a ceremony with it.
journal_path = "$JOURNAL"
poll_ms = 1000

[ceremony]
$( [ "$1" = 1 ] && echo 'profile = "readiness"' )
mode = "$MODE"
freeze_height = 0
$( [ "$1" = 1 ] || printf 'freeze_margin = 420\nfreeze_strategy = "schedule_at_h"' )
chain_id = "$CHAIN_ID"
import_cpu_scale = 143

[source]
rpc_url = "$API"
producer_api_url = "$PAPI"

[snapshot]
staged_path = "$STAGED"
dir = "$SNAPDIR"

[target]
metalgo_unit = "$MG_UNIT"
rpc_url = "$TARGET_RPC"

[beacon]
url = "${URL%/}/api/report"
producer = "$PRODUCER"
network = "$NETWORK"
node = "$NODE"
role = "$ROLE"
token_file = "$ETC/beacon.token"
interval_secs = $INTERVAL
TOML
  }
  # probe: does this binary understand `profile = "readiness"` (then no ceremony can run from the file)?
  local PROFILE=1
  mkdir -p "$TMP/probe-journal"
  probe_cfg() { sed "s|^url = .*|url = \"\"|; s|^token_file = .*|token_file = \"/dev/null\"|; s|^journal_path = .*|journal_path = \"$TMP/probe-journal/journal.jsonl\"|"; }
  write_config 1 | probe_cfg > "$TMP/probe.toml"
  if "$B" beacon --config "$TMP/probe.toml" --once > "$TMP/probe.out" 2>&1; then :
  elif grep -qi "profile" "$TMP/probe.out"; then
    PROFILE=0
    # Releases without the readiness profile also refuse `mode = "api"` without a [flip] section, so the
    # config stays in producer mode (as before) until the beacon is upgraded.
    [ "$MODE" = producer ] || { warn "pulse-cutover $VERSION needs mode = \"producer\" in a beacon config; the role is still reported as '$ROLE'"; MODE=producer; }
    warn "pulse-cutover $VERSION cannot mark its config readiness-only. NEVER run 'pulse-cutover run' with $ETC/beacon.toml; upgrade the beacon when a newer release is out."
  fi
  write_config "$PROFILE" > "$TMP/beacon.toml"
  # The exact file that will be installed must load and produce a report, or nothing is installed.
  probe_cfg < "$TMP/beacon.toml" > "$TMP/once.toml"
  if ! "$B" beacon --config "$TMP/once.toml" --once > "$TMP/once.out" 2>"$TMP/once.err"; then
    [ "$FORCE" = 1 ] || die "the beacon cannot run with the generated config: $(tail -c 300 "$TMP/once.err") (nothing was changed; --force installs anyway)"
    warn "the beacon cannot run with the generated config; continuing because of --force"
  fi

  say "first report (printed, not sent):"
  python3 -c 'import json,sys
try: r=json.load(open(sys.argv[1]))
except Exception: sys.exit(0)
for c in r.get("checks",[]): print("   ", "ok " if c.get("ok") else "-- ", c.get("name"), "·", c.get("detail"))' "$TMP/once.out" || true

  # ---- the plan: everything detected, every change, everything left alone. Shown before anything changes. --
  local MG_STATE PAPI_NOTE OTHERS_NOW
  MG_STATE=$(systemctl is-active "$MG_UNIT" 2>/dev/null || true); MG_STATE=${MG_STATE:-not found}
  case "$PAPI" in http://127.0.0.1*|http://localhost*|http://\[::1\]*) PAPI_NOTE="local ✓";; *) PAPI_NOTE="NOT local: keep /v1/producer off the internet";; esac
  OTHERS_NOW=$(other_units | tr '\n' ' ')
  echo
  echo "  pulse-cutover beacon installer   ${CURRENT:-(not installed)} → $VERSION"
  echo
  echo "  Found"
  printf '    %-11s %s\n' chain "XPR $NETWORK (${CHAIN_ID:0:8}…) · head $HEAD" \
    nodeos "API $API · producer API $PAPI ($PAPI_NOTE)" \
    producer "$PRODUCER$( [ -n "$CFG_PRODUCER" ] && echo ' (from the nodeos config)')" \
    server "label '$NODE' · role $ROLE · ceremony mode $MODE" \
    snapshots "$SNAPDIR" \
    metal "service $MG_UNIT: $MG_STATE"
  if [ "$OLD_FOUND" = 1 ]; then printf '    %-11s %s\n' existing "beacon ${CURRENT:-?} · token and settings kept"; fi
  [ -n "$OTHERS_NOW" ] && printf '    %-11s %s\n' also "$OTHERS_NOW(left running)"
  echo
  echo "  Plan"
  echo "    1. install /usr/local/bin/pulse-cutover $VERSION   (downloaded and sha256-checked already)"
  echo "    2. write $ETC/beacon.toml   (readiness-only: it cannot run a ceremony)"
  [ "$OLD_FOUND" = 1 ] || echo "    3. create the beacon's token and the 'pulse-beacon' service user"
  echo "    $( [ "$OLD_FOUND" = 1 ] && echo 3 || echo 4 ). (re)start the pulse-beacon service   (reports readiness every ${INTERVAL}s to ${URL%/})"
  echo "    If any step fails, the previous binary, config and service are restored."
  echo
  echo "  Won't touch: nodeos or its config, keys, metalgo, nginx, firewall."
  echo
  if [ "$DRY" = 1 ]; then say "dry run: nothing installed or written outside a temp dir"; exit 0; fi
  if [ "$YES" != 1 ] && [ -t 1 ] && { : </dev/tty; } 2>/dev/null; then
    local ANS; read -r -p "  Proceed? [Y/n] " ANS </dev/tty || ANS=n
    case "$ANS" in n*|N*) die "cancelled: nothing was changed";; esac
  fi

  # ---- apply: transactional. From the first change until the new beacon is confirmed running, ANY failure
  # (install, chown, systemctl, restart, or the beacon not staying up) restores the previous binary, config,
  # unit, token and instance id and restarts the previous beacon. ---------------------------------------------
  mkdir -p "$ETC"; chmod 750 "$ETC"
  local f
  for f in "$BIN" "$ETC/beacon.toml" "$ETC/beacon.token" "$ETC/beacon.instance" "$UNIT"; do
    rm -f "$f.prev"; [ -f "$f" ] && cp -p "$f" "$f.prev"
  done
  APPLYING=1
  RETIRED=""
  if [ "$CHANGED_OWNER" = 1 ] || [ ! -s "$ETC/beacon.token" ]; then
    if [ -s "$ETC/beacon.token" ]; then RETIRED="$ETC/beacon.token.retired-$(date -u +%Y%m%dT%H%M%SZ)"; mv -f "$ETC/beacon.token" "$RETIRED"; fi
    (umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ETC/beacon.token")
  fi
  install -m 755 "$B" "$BIN.new"; mv -f "$BIN.new" "$BIN"
  install -m 640 "$TMP/beacon.toml" "$ETC/beacon.toml.new"; mv -f "$ETC/beacon.toml.new" "$ETC/beacon.toml"
  mkdir -p "$VAR"; cp -f "$TMP/doctor.json" "$VAR/doctor.json" 2>/dev/null || true

  # Instance id: stable per server. rc.5 kept it next to the journal ($VAR); it now lives with the token, so the
  # beacon never has to write into the ceremony directory. An existing id is carried over, never regenerated.
  if [ ! -s "$ETC/beacon.instance" ]; then
    if [ -s "$VAR/beacon.instance" ] && grep -qE '^[0-9a-f]{32}$' "$VAR/beacon.instance"; then cp "$VAR/beacon.instance" "$ETC/beacon.instance"
    else head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ETC/beacon.instance"; echo >> "$ETC/beacon.instance"; fi
  fi

  # Observer isolation: the ceremony directory is root's (rc.5 handed it to the beacon user; take it back).
  chown root:root "$VAR"; chmod 755 "$VAR"
  [ -f "$VAR/beacon.instance" ] && chown root:root "$VAR/beacon.instance"
  local RUNAS=root
  id "$SVC_USER" >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin "$SVC_USER"
  chown root:"$SVC_USER" "$ETC" "$ETC/beacon.toml" "$ETC/beacon.token" "$ETC/beacon.instance"
  chmod 640 "$ETC/beacon.token" "$ETC/beacon.toml" "$ETC/beacon.instance"; chmod 750 "$ETC"
  # Unprivileged when that user can read what the beacon reads: token, the ceremony journal (if one exists),
  # and the snapshots dir (free-space check). Otherwise root, limited to reading files.
  if runuser -u "$SVC_USER" -- test -r "$ETC/beacon.token" 2>/dev/null \
     && runuser -u "$SVC_USER" -- test -x "$(dirname "$JOURNAL")" 2>/dev/null \
     && { [ ! -e "$JOURNAL" ] || runuser -u "$SVC_USER" -- test -r "$JOURNAL" 2>/dev/null; } \
     && runuser -u "$SVC_USER" -- df -P "$(existing_ancestor "$SNAPDIR")" >/dev/null 2>&1; then RUNAS=$SVC_USER
  else
    warn "the beacon user cannot read the ceremony journal or snapshots dir: running the beacon as root with read-only file access (CAP_DAC_READ_SEARCH only)"
    chmod 600 "$ETC/beacon.token"; chown root:root "$ETC/beacon.token"
  fi
  beacon_unit "$RUNAS" "$BIN" "$ETC" "$STATE" > "$UNIT.new"
  mv -f "$UNIT.new" "$UNIT"
  ${FAULT_BEFORE_RESTART:-true}   # test hook: FAULT_BEFORE_RESTART=false forces a failure mid-apply
  systemctl daemon-reload; systemctl enable pulse-beacon >/dev/null 2>&1; systemctl restart pulse-beacon
  sleep 3
  systemctl is-active --quiet pulse-beacon || { journalctl -u pulse-beacon -n 15 --no-pager 2>/dev/null | sed 's/^/    /' || true; false; }
  APPLYING=0   # committed
  rm -f "$BIN.prev" "$ETC/beacon.toml.prev" "$ETC/beacon.instance.prev" "$UNIT.prev"
  [ -z "$RETIRED" ] && rm -f "$ETC/beacon.token.prev"
  say "beacon running as $RUNAS: $(systemctl is-active pulse-beacon)"

  local HASH; HASH=$(token_hash "$ETC/beacon.token")
  echo
  if [ -n "$HASH_BEFORE" ] && [ "$HASH_BEFORE" = "$HASH" ]; then
    if [ -n "$CURRENT" ] && [ "$CURRENT" != "$VERSION" ]; then
      echo "  ✓ Upgraded from $CURRENT to $VERSION. Same token as before, so there is nothing to send: your page updates within 10 seconds."
    else
      echo "  ✓ On $VERSION. Same token as before, so there is nothing to send: your page updates within 10 seconds."
    fi
    echo "      ${URL%/}/$NETWORK/$PRODUCER/$NODE"
    local OTHERS; OTHERS=$(other_units | tr '\n' ' ')
    if [ -n "$OTHERS" ]; then
      echo
      echo "  ! Still running the previous binary: $OTHERS"
      echo "    Restart when no ceremony is in progress on this box: sudo systemctl restart $OTHERS"
    fi
    exit 0
  fi
  echo "  ✓ Done. Last step: send this ONE line to the mission-control operator (it is only a hash of your token):"
  echo
  echo "      network=$NETWORK producer=$PRODUCER token_sha256=$HASH"
  echo
  echo "  Then watch your server here: ${URL%/}/$NETWORK/$PRODUCER/$NODE"
  echo "  To remove it later:  curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh | sudo bash -s -- --uninstall"
}

[ "${BEACON_INSTALL_SOURCED:-0}" = 1 ] || main "$@"
