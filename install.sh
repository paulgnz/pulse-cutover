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
# (One announced exception on a haproxy edge: staging adds a DISABLED gateway server to
# your backend and gracefully reloads haproxy once, at install time — traffic unchanged,
# and it is exactly what makes the ceremony flip itself reload-free.)
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
[ -n "$MODE" ] || {
  echo "usage: ./install.sh --mode api|bp|hyperion --manifest ceremony.json"
  echo "  --mode bp        you are a block producer"
  echo "  --mode api       you serve a public /v1 RPC URL"
  echo "  --mode hyperion  you serve /v1 AND /v2 history (Hyperion)"
  echo "  (unsure? see 'Which mode am I?' in the README)"
  exit 2
}
[ "$MODE" = "api" ] || [ "$MODE" = "bp" ] || [ "$MODE" = "hyperion" ] || { echo "ABORT: --mode must be api, bp or hyperion (got '$MODE')"; exit 2; }
# hyperion mode IS api mode (same /v1 machinery) + the /v2 history stack.
API_LIKE=false
if [ "$MODE" = "api" ] || [ "$MODE" = "hyperion" ]; then API_LIKE=true; fi
MANIFEST="${MANIFEST:-ceremony.json}"
[ -f "$MANIFEST" ] || {
  echo "ABORT: manifest '$MANIFEST' not found. The manifest is the ceremony.json describing"
  echo "       the event — in a real event the coordinator publishes it; for a rehearsal ask"
  echo "       in the Telegram group for the current bundle, or see examples/ in the repo."
  exit 2
}
[ "$(id -u)" = 0 ] || { echo "ABORT: this installer stages system services, so it must run as root: sudo ./install.sh --mode $MODE --manifest $MANIFEST"; exit 2; }

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

# ---------- pinned, verified artifacts (helpers) ----------
fetch_verify(){ # url sha dest
  local url="$1" sha="$2" dest="$3"
  if [ -f "$dest" ] && echo "$sha  $dest" | sha256sum -c - >/dev/null 2>&1; then
    echo "  ok (cached): $(basename "$dest")"; return
  fi
  echo "  fetch $(basename "$dest") ..."
  curl -fsSL "$url" -o "$dest.tmp"
  echo "$sha  $dest.tmp" | sha256sum -c - >/dev/null || {
    echo "ABORT: the downloaded $(basename "$dest") does not match the sha256 pinned in the manifest."
    echo "       Nothing was installed. Do NOT fetch it from anywhere else — tell the ceremony"
    echo "       coordinator (bad mirror, stale manifest, or tampering)."
    rm -f "$dest.tmp"; exit 1
  }
  mv "$dest.tmp" "$dest"
  echo "  verified: $(basename "$dest")"
}
# Safe tarball extraction: NEVER extract an untrusted tarball over / or $HOME.
# (A pulsevm release tarball carrying uid-1001 ownership once chown'd /root.)
extract_safe(){ # tarball stagedir
  mkdir -p "$2"
  tar -xzf "$1" -C "$2" --no-same-owner --no-same-permissions
}

# The gateway + federator are modern Node scripts (optional chaining `?.`,
# nullish `??`): they need Node >= 14. The distro nodejs does NOT always
# qualify — Ubuntu 20.04 ships Node 10 and 22.04 ships Node 12 — so verify
# the actual major version instead of trusting `apt-get install nodejs`.
node_major(){ command -v node >/dev/null && node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0; }
ensure_node(){
  if [ "$(node_major)" -ge 14 ]; then return; fi
  apt-get install -y -qq nodejs >/dev/null 2>&1 || true
  if [ "$(node_major)" -ge 14 ]; then return; fi
  echo "ABORT: the /v1 gateway (and federator) need Node.js >= 14; this box has $(command -v node >/dev/null && node --version || echo none)."
  echo "       Ubuntu 20.04/22.04's apt nodejs is too old. Install a current LTS, e.g.:"
  echo "         curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash - && sudo apt-get install -y nodejs"
  echo "       then re-run this installer (re-running is always safe)."
  exit 1
}

# ---------- haproxy flip helpers (used when the detected edge is haproxy) ----------
# Both use the globals resolved in the edge section below:
#   HAP_CFG (config path), HAP_VALIDATE / HAP_RELOAD (per-runtime commands),
#   ADMIN_SOCK + HAP_STRATEGY (runtime-socket | cfg-reload).

# Stage a pulse-cutover server entry, state `disabled`, into a haproxy
# backend. This cfg edit happens at INSTALL time and is followed by the ONE
# haproxy reload of the whole procedure — days before the event, not at H.
# The ceremony flip itself then only toggles server states: via the runtime
# socket (zero reload) or by swapping the `disabled` markers (cfg fallback).
hap_stage_server(){ # backend_name server_name addr
  local b="$1" name="$2" addr="$3"
  if grep -qE "^[[:space:]]*server[[:space:]]+$name[[:space:]]" "$HAP_CFG"; then
    echo "  ok (staged): server $name already present in $HAP_CFG"
    return
  fi
  cp "$HAP_CFG" "$HAP_CFG.pulse-bak"   # restore point for THIS edit only
  awk -v b="$b" -v line="    server $name $addr check disabled # pulse-cutover: staged flip target, enabled only by the ceremony flip" '
    { print }
    ($1=="backend" || $1=="listen") && $2==b { print line }
  ' "$HAP_CFG" > "$HAP_CFG.pulse-tmp"
  mv "$HAP_CFG.pulse-tmp" "$HAP_CFG"
  if ! eval "$HAP_VALIDATE"; then
    cp "$HAP_CFG.pulse-bak" "$HAP_CFG"
    echo "ABORT: staging 'server $name' into backend '$b' made $HAP_CFG invalid;"
    echo "       the original file was restored. Check .haproxy.routes in doctor.json."
    exit 1
  fi
  eval "$HAP_RELOAD"
  echo "  staged: server $name $addr (disabled) in backend '$b'"
  echo "          ^ that was the ONE haproxy reload (install time); the ceremony flip needs none"
  # Prove the RUNNING haproxy picked the staged server up — a reload that
  # silently read stale config (classic: docker single-FILE bind mount pins
  # the old inode) would otherwise only surface at H, as a failed flip.
  if [ -n "${ADMIN_SOCK:-}" ] && [ -S "${ADMIN_SOCK:-/nonexistent}" ]; then
    sleep 1
    if ! echo "show servers state $b" | socat stdio "$ADMIN_SOCK" | awk -v s="$name" '$4==s{found=1} END{exit !found}'; then
      echo "ABORT: haproxy reloaded but the running process does not list $b/$name."
      echo "       If haproxy runs in docker, bind-mount the config DIRECTORY, not the single"
      echo "       file — a file mount pins the old inode and every reload re-reads stale config"
      echo "       (see examples/haproxy-test/compose.yml). Fix the mount and re-run."
      exit 1
    fi
    echo "  verified: running haproxy lists $b/$name (in MAINT until the ceremony flip)"
  fi
}

# Generate the flip + revert scripts for one backend swap (nodeos server ->
# staged pulse-cutover server). Strategy comes from $HAP_STRATEGY.
hap_gen_flip(){ # backend old_srv new_srv new_addr flip_path revert_path ok_msg back_msg marker
  local b="$1" old="$2" new="$3" addr="$4" flip="$5" revert="$6" okmsg="$7" backmsg="$8" marker="$9"
  # The operator's own server line, byte-exact — the generated sed patterns
  # must match it verbatim (same discipline as the nginx templater).
  local oldline
  oldline=$(awk -v b="$b" -v s="$old" '
    ($1=="backend" || $1=="listen") && $2==b { inb=1; next }
    /^[a-zA-Z]/ { inb=0 }
    inb && $1=="server" && $2==s { sub(/[[:space:]]+$/, ""); print; exit }
  ' "$HAP_CFG")
  [ -n "$oldline" ] || { echo "ABORT: could not find 'server $old' inside backend '$b' in $HAP_CFG"; exit 1; }

  if [ "$HAP_STRATEGY" = "runtime-socket" ]; then
    cat > "$flip" <<FLIP
#!/usr/bin/env bash
# GENERATED by install.sh from the DETECTED haproxy map (doctor.json).
# Swaps backend '$b' from your nodeos ($old) to the pulse-cutover target ($new).
#
# Strategy: RUNTIME SOCKET (stats socket, level admin) — ZERO reload, and
# fail-safe: a successful runtime command returns empty output, so each step
# is checked before the next. Order matters: enable the pre-staged (disabled)
# '$new' server FIRST and abort if haproxy rejects it — at that point NOTHING
# has changed and nodeos is still serving. Only then disable '$old'.
# The same swap is then persisted into haproxy.cfg (config is only read at
# reload/restart) so a later haproxy restart keeps the flipped state; that
# edit is validated but NOT reloaded — the socket already made it live.
set -e
out=\$(echo "enable server $b/$new" | socat stdio $ADMIN_SOCK | tr -d '[:space:]')
if [ -n "\$out" ]; then
  echo "ABORT: haproxy refused 'enable server $b/$new': \$out — nothing was changed, nodeos is still serving. Was haproxy reloaded since install staged the server?"
  exit 1
fi
out=\$(echo "disable server $b/$old" | socat stdio $ADMIN_SOCK | tr -d '[:space:]')
if [ -n "\$out" ]; then
  echo "WARN: haproxy refused 'disable server $b/$old': \$out — BOTH servers are active now; run the revert script, or disable it by hand"
fi
if ! grep -q 'pulse-cutover: flipped-$marker' $HAP_CFG; then
  sed -i 's|server $new $addr check disabled #|server $new $addr check #|' $HAP_CFG
  sed -i 's|$oldline\$|$oldline disabled # pulse-cutover: flipped-$marker|' $HAP_CFG
  $HAP_VALIDATE || echo "WARN: cfg persistence edit failed validation — the LIVE flip is done (socket), but fix $HAP_CFG before any haproxy restart"
fi
echo $okmsg
FLIP
    cat > "$revert" <<FLIP
#!/usr/bin/env bash
# GENERATED revert: swap backend '$b' back to your nodeos ($old) — the exact
# mirror of the flip, via the same admin socket + cfg persistence edit.
# Same fail-safe order: enable $old first (abort if refused — nothing has
# changed), only then disable $new.
set -e
out=\$(echo "enable server $b/$old" | socat stdio $ADMIN_SOCK | tr -d '[:space:]')
if [ -n "\$out" ]; then
  echo "ABORT: haproxy refused 'enable server $b/$old': \$out — nothing was changed"
  exit 1
fi
out=\$(echo "disable server $b/$new" | socat stdio $ADMIN_SOCK | tr -d '[:space:]')
if [ -n "\$out" ]; then
  echo "WARN: haproxy refused 'disable server $b/$new': \$out — BOTH servers are active now; disable it by hand"
fi
sed -i 's|$oldline disabled # pulse-cutover: flipped-$marker|$oldline|' $HAP_CFG
sed -i 's|server $new $addr check #|server $new $addr check disabled #|' $HAP_CFG
$HAP_VALIDATE || echo "WARN: cfg revert edit failed validation — the LIVE revert is done (socket), but fix $HAP_CFG before any haproxy restart"
echo $backmsg
FLIP
  else
    cat > "$flip" <<FLIP
#!/usr/bin/env bash
# GENERATED by install.sh from the DETECTED haproxy map (doctor.json).
# Swaps backend '$b' from your nodeos ($old) to the pulse-cutover target ($new).
#
# Strategy: CFG EDIT + VALIDATE + GRACEFUL RELOAD (no admin-level stats socket
# was found — add 'stats socket /run/haproxy/admin.sock mode 660 level admin'
# to the global section to get the zero-reload strategy instead).
# The edit only swaps which server carries the 'disabled' marker; haproxy -c
# validates BEFORE the reload, and the reload is hitless (the master hands
# live connections over to a fresh worker).
set -e
sed -i 's|server $new $addr check disabled #|server $new $addr check #|' $HAP_CFG
grep -q 'pulse-cutover: flipped-$marker' $HAP_CFG || sed -i 's|$oldline\$|$oldline disabled # pulse-cutover: flipped-$marker|' $HAP_CFG
$HAP_VALIDATE
$HAP_RELOAD
echo $okmsg
FLIP
    cat > "$revert" <<FLIP
#!/usr/bin/env bash
# GENERATED revert: swap backend '$b' back to your nodeos ($old) — the exact
# mirror of the flip: swap the 'disabled' markers back, validate, reload.
set -e
sed -i 's|$oldline disabled # pulse-cutover: flipped-$marker|$oldline|' $HAP_CFG
sed -i 's|server $new $addr check #|server $new $addr check disabled #|' $HAP_CFG
$HAP_VALIDATE
$HAP_RELOAD
echo $backmsg
FLIP
  fi
  chmod +x "$flip" "$revert"
}

mkdir -p "$WORK" /opt/pulsevm/plugins /opt/metalgo /etc/pulse-cutover /opt/pulse-cutover

# ---------- the agent first: doctor drives everything after this ----------
# Three ways to the binary, in preference order:
#   1. manifest-pinned artifact (ceremony-grade: URL + sha256, fail-closed);
#   2. GitHub release binary — STATIC musl build, so it runs on any glibc
#      (the bookworm-built binary failing on a glibc-2.31 box is why),
#      verified against the release's sha256sums.txt;
#   3. cargo build from this checkout (slowest; needs rustup + build deps).
install_agent_release(){
  local arch tag base sums bin
  case "$(uname -m)" in
    x86_64) arch=x86_64;;
    aarch64|arm64) arch=aarch64;;
    *) echo "  release binaries cover x86_64/aarch64 only (this box: $(uname -m))"; return 1;;
  esac
  bin="pulse-cutover-$arch-unknown-linux-musl"
  tag=$(mget_opt '.artifacts.agent_release_tag')
  if [ -n "$tag" ]; then
    base="https://github.com/paulgnz/pulse-cutover/releases/download/$tag"
  else
    base="https://github.com/paulgnz/pulse-cutover/releases/latest/download"
  fi
  echo "  fetching release binary $bin (${tag:-latest}) ..."
  curl -fsSL "$base/sha256sums.txt" -o /tmp/pulse-cutover.sums || { echo "  no release sums reachable"; return 1; }
  curl -fsSL "$base/$bin" -o /tmp/pulse-cutover.bin || { echo "  no release binary for $arch"; return 1; }
  sums=$(grep " $bin\$" /tmp/pulse-cutover.sums | awk '{print $1}')
  [ -n "$sums" ] || { echo "  sha256sums.txt has no entry for $bin"; return 1; }
  echo "$sums  /tmp/pulse-cutover.bin" | sha256sum -c - >/dev/null || {
    echo "  ABORT-worthy: release binary does not match its published sha256 — not installing it"
    rm -f /tmp/pulse-cutover.bin; return 1
  }
  install -m 0755 /tmp/pulse-cutover.bin /usr/local/bin/pulse-cutover
  rm -f /tmp/pulse-cutover.bin /tmp/pulse-cutover.sums
  echo "  verified + installed: pulse-cutover ($bin, sha256 $sums)"
}
install_agent_source(){
  local repo; repo=$(cd "$(dirname "$0")" && pwd)
  command -v cargo >/dev/null || { echo "  no cargo either — install rustup (see README Step 1) or add artifacts.agent to the manifest"; return 1; }
  echo "  building from source in $repo (this takes a few minutes) ..."
  (cd "$repo" && cargo build --release) || return 1
  install -m 0755 "$repo/target/release/pulse-cutover" /usr/local/bin/pulse-cutover
  echo "  built + installed: pulse-cutover (source build)"
}
echo "artifacts:"
AGENT_URL=$(mget_opt '.artifacts.agent.url')
if [ -n "$AGENT_URL" ]; then
  fetch_verify "$AGENT_URL" "$(mget '.artifacts.agent.sha256')" /usr/local/bin/pulse-cutover
else
  echo "  (manifest pins no agent binary — trying the GitHub release, then source)"
  install_agent_release || install_agent_source || {
    echo "ABORT: could not install the pulse-cutover agent by any path."
    exit 1
  }
fi
chmod +x /usr/local/bin/pulse-cutover

# ---------- doctor: DETECT the box, never assume ----------
# Read-only survey: how nodeos runs (native/docker + unit), the live nginx
# server_name -> upstream map, legacy hyperion/ES, metalgo, port plan.
# install.sh CONSUMES this JSON below: flip scripts are templated from the
# DETECTED routes, stop/start defaults from the detected unit/container,
# and the per-mode verdict gates the install.
DOC="$WORK/doctor.json"
echo "doctor: surveying this box (read-only) ..."
pulse-cutover doctor --json > "$DOC" || { echo "ABORT: doctor survey failed"; exit 1; }
VERDICT=$(jq -r --arg m "$MODE" '.verdicts[$m].status' "$DOC")
if [ "$VERDICT" = "UNSUPPORTED" ]; then
  echo ""
  echo "This setup is not supported yet (doctor verdict: UNSUPPORTED). Nothing was staged. Reasons:"
  jq -r --arg m "$MODE" '.verdicts[$m].unsupported[]' "$DOC" | sed 's/^/  - /'
  echo ""
  echo "Run 'pulse-cutover report' and share the bundle — that is exactly how setups get added."
  exit 1
fi
if [ "$VERDICT" = "NEEDS" ]; then
  # A need the manifest (or this installer itself) satisfies is not a gap:
  #   - stop_cmd:        the manifest may declare it explicitly;
  #   - flip.edge:       doctor flags "two web edges" — the manifest resolves it;
  #   - socat:           installed by this script when the socket flip is chosen;
  #   - script-managed:  this installer generates the reviewed stop/start pair
  #                      (graceful-SIGTERM stop + [CHANGE] start placeholder).
  M_STOP=$(mget_opt '.source.stop_cmd')
  M_EDGE=$(mget_opt '.flip.edge')
  NEEDS=$(jq -r --arg m "$MODE" '.verdicts[$m].needs[]' "$DOC")
  REMAINING=""
  while IFS= read -r n; do
    [ -z "$n" ] && continue
    if [ -n "$M_STOP" ] && echo "$n" | grep -q "stop_cmd"; then continue; fi
    if [ -n "$M_EDGE" ] && echo "$n" | grep -q "flip.edge"; then continue; fi
    if echo "$n" | grep -q "socat"; then continue; fi
    if echo "$n" | grep -q "script-managed"; then continue; fi
    REMAINING="$REMAINING  - $n\n"
  done <<< "$NEEDS"
  if [ -n "$REMAINING" ]; then
    echo ""
    echo "This box is not ready (doctor verdict: NEEDS). Nothing was staged. Missing:"
    printf "%b" "$REMAINING"
    echo "Full survey: $DOC (or run 'pulse-cutover doctor' for the table)"
    exit 1
  fi
fi
echo "doctor: verdict for $MODE mode is READY ($DOC)"

# ---------- manifest-vs-live preflight (doctor checked the box; this checks YOUR manifest) ----------
problems=()
# The source nodeos is YOURS — we detect it, we never install or touch it.
SRC_INFO=$(curl -s -m5 "$SRC_RPC/v1/chain/get_info" || true)
if [ -z "$SRC_INFO" ]; then
  problems+=("no nodeos answering at $SRC_RPC — is it running? (systemctl status <your-unit> / docker ps). If it listens on another address, set source.rpc_url in $MANIFEST")
else
  GOT_CHAIN=$(echo "$SRC_INFO" | jq -r '.chain_id // empty')
  [ "$GOT_CHAIN" = "$CHAIN_ID" ] || problems+=("nodeos at $SRC_RPC serves chain_id ${GOT_CHAIN:-none}, manifest expects $CHAIN_ID")
fi
PAUSED=$(curl -s -m5 -X POST "$SRC_PROD/v1/producer/paused" || true)
case "$PAUSED" in true|false) ;; *) problems+=("producer_api not answering at $SRC_PROD — the ceremony takes its snapshot through it. Fix: add 'plugin = eosio::producer_api_plugin' to your nodeos config and restart nodeos. Keep it bound to 127.0.0.1 — it can pause your chain (R10)");; esac
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
  echo ""
  echo "Fix the above and re-run this installer (re-running is always safe)."
  exit 1
fi
echo "preflight: ok (nodeos serving $CHAIN_ID, producer_api localhost-only)"

# ---------- source stop/start: manifest first, else DERIVED from detection ----------
STOP_CMD=$(mget_opt '.source.stop_cmd')
START_CMD=$(mget_opt '.source.start_cmd')
if $API_LIKE && [ -z "$STOP_CMD" ]; then
  NRUNTIME=$(jq -r '.nodeos.runtime // "unknown"' "$DOC")
  NCONTAINER=$(jq -r '.nodeos.container_name // empty' "$DOC")
  NUNIT=$(jq -r '.nodeos.systemd_units[0] // empty' "$DOC")
  NMGR=$(jq -r '.nodeos.script_manager // empty' "$DOC")
  NPID=$(jq -r '.nodeos.pid // empty' "$DOC")
  NBIN=$(jq -r '.nodeos.binary // "nodeos"' "$DOC")
  NDISC=$(jq -r '.nodeos.config_path // .nodeos.data_dir // .nodeos.binary // "nodeos"' "$DOC")
  if [ "$NRUNTIME" = "docker" ] && [ -n "$NCONTAINER" ]; then
    STOP_CMD="docker stop $NCONTAINER"; START_CMD=${START_CMD:-"docker start $NCONTAINER"}
    echo "source stop/start: derived from detected docker container '$NCONTAINER'"
  elif [ -n "$NUNIT" ]; then
    STOP_CMD="systemctl stop $NUNIT"; START_CMD=${START_CMD:-"systemctl start $NUNIT"}
    echo "source stop/start: derived from detected systemd unit '$NUNIT'"
  elif [ "$NRUNTIME" = "native" ] && [ -n "$NMGR" ]; then
    # Script-managed nodeos (the classic Antelope BP pattern: a start script
    # under screen/tmux/nohup, no unit). The STOP side is derivable — a
    # graceful SIGTERM to the exact process doctor identified, plus a
    # wait-for-exit loop (Leap flushes chainbase on SIGTERM; anything harder
    # risks a dirty database). The START side is NOT derivable: only the
    # operator's own start script knows how this nodeos comes up, so we
    # stage a clearly-marked placeholder they must edit.
    cat > /opt/pulse-cutover/source-stop.sh <<STOP
#!/usr/bin/env bash
# GENERATED by install.sh for a script-managed nodeos (no unit, no container;
# detected parent: $NMGR). Review before the ceremony.
# Graceful stop: SIGTERM to the nodeos whose command line matches BOTH the
# detected binary and its config/data path, then wait for it to exit.
set -e
PID=""
for p in \$(pgrep -x nodeos 2>/dev/null || pgrep -f nodeos); do
  cl=\$(tr '\\0' ' ' < "/proc/\$p/cmdline" 2>/dev/null || true)
  case "\$cl" in *"$NBIN"*"$NDISC"*|*"$NDISC"*"$NBIN"*|*"$NBIN"*) PID=\$p; break;; esac
done
[ -n "\$PID" ] || { echo "nodeos not running (nothing matching '$NBIN' + '$NDISC') — treating as already stopped"; exit 0; }
echo "stopping nodeos pid \$PID (SIGTERM, graceful — chainbase flush can take a while)"
kill -TERM "\$PID"
for i in \$(seq 1 120); do
  kill -0 "\$PID" 2>/dev/null || { echo "nodeos stopped after \${i}s"; exit 0; }
  sleep 1
done
echo "ERROR: nodeos (pid \$PID) still running after 120s. NOT escalating to SIGKILL"
echo "       (a killed Leap leaves a dirty chainbase). Investigate, then stop it yourself."
exit 1
STOP
    cat > /opt/pulse-cutover/source-start.sh <<'START'
#!/usr/bin/env bash
# [CHANGE] pulse-cutover cannot know how YOUR script-managed nodeos starts —
# only your own start script does. Replace the two lines below with it, e.g.:
#   exec screen -dmS nodeos /opt/xpr/start.sh
# or whatever launched the nodeos that doctor detected. Until you do, an
# aborted ceremony CANNOT bring your nodeos back automatically (the abort is
# still safe — the flip is reverted first — but nodeos stays down until you
# start it by hand).
echo "[CHANGE] edit /opt/pulse-cutover/source-start.sh: point it at YOUR nodeos start script" >&2
exit 1
START
    chmod +x /opt/pulse-cutover/source-stop.sh /opt/pulse-cutover/source-start.sh
    STOP_CMD=/opt/pulse-cutover/source-stop.sh
    if [ -z "$START_CMD" ]; then START_CMD=/opt/pulse-cutover/source-start.sh; fi
    echo "source stop/start: nodeos is SCRIPT-MANAGED (parent: $NMGR, pid ${NPID:-?})"
    echo "  generated: /opt/pulse-cutover/source-stop.sh   (graceful SIGTERM + wait — REVIEW it)"
    echo "  generated: /opt/pulse-cutover/source-start.sh  ([CHANGE] placeholder — EDIT it to"
    echo "             call your own start script; needed only if an aborted ceremony must"
    echo "             restart nodeos automatically)"
  else
    echo "ABORT: api mode retires nodeos after the flip, but the manifest has no source.stop_cmd"
    echo "       and doctor found neither a systemd unit, a docker container, nor a"
    echo "       classifiable script-managed nodeos process."
    echo "       Declare source.stop_cmd/start_cmd in the manifest."
    exit 1
  fi
fi

# ---------- remaining pinned artifacts ----------
echo "artifacts (target stack):"
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

# ---------- api mode: REST gateway + web-edge flip machinery ----------
if $API_LIKE; then
  ensure_node
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

  # Web-edge flip machinery. Which edge, then which layout:
  #   edge nginx, detected — the box ALREADY routes /v1 to nodeos through
  #              nginx (real API providers): template flip/revert from the
  #              DETECTED server_name -> proxy_pass map in doctor.json,
  #              byte-exact against the operator's own config files. nginx is
  #              untouched until the ceremony's flip stage.
  #   edge nginx, managed — fresh side-box with no web routes at all: stage
  #              our own nginx site with a single-line upstream indirection
  #              (the recorded-22-runs layout).
  #   edge haproxy — the box routes /v1 to nodeos through haproxy: stage the
  #              gateway as a disabled server in the SAME backend (one reload,
  #              now — not at H), and flip via the admin socket (preferred,
  #              zero reload) or cfg-edit + validate + graceful reload.
  NODEOS_PORT=$(echo "$SRC_RPC" | sed -E 's|.*:([0-9]+).*|\1|')
  GW="127.0.0.1:8899"
  # Detected /v1 routes whose backend is the source nodeos port — per edge.
  V1_MATCHES=$(jq -r --arg p ":$NODEOS_PORT" '
    [ .web.routes[]
      | select((.location | startswith("/v1")) or .location == "/")
      | select(any(.backends[]?; endswith($p))) ]
    | .[] | [ (if .upstream_name then "upstream" else "direct" end),
              (.upstream_name // .file), .proxy_pass,
              (.backends[0] // ""), (.server_names | join(" ")) ]
    | @tsv' "$DOC" | sort -u)
  ROUTE_COUNT=$(jq -r '.web.routes | length' "$DOC")
  HAP_RUNNING=$(jq -r '.haproxy.running // false' "$DOC")
  HAP_V1_COUNT=$(jq -r --arg p ":$NODEOS_PORT" '
    [ .haproxy.routes[]?
      | select(any(.servers[]?; (.addr | endswith($p)) and (.disabled | not))) ]
    | length' "$DOC")

  # ---------- which edge does the ceremony flip? ----------
  # manifest .flip.edge: "nginx" | "haproxy" | "auto" (default). auto picks
  # the edge that actually routes /v1 to the manifest's nodeos; if BOTH do,
  # doctor cannot know which one the public URL reaches — declare it.
  EDGE=$(mget_opt '.flip.edge'); EDGE=${EDGE:-auto}
  case "$EDGE" in
    nginx|haproxy)
      echo "flip edge: $EDGE (declared by flip.edge in $MANIFEST)";;
    auto)
      if [ "$HAP_RUNNING" = "true" ] && [ "${HAP_V1_COUNT:-0}" -gt 0 ] && [ -n "$V1_MATCHES" ]; then
        echo "ABORT: BOTH nginx and haproxy route /v1 to the nodeos on :$NODEOS_PORT — the installer"
        echo "       cannot know which edge your public URL reaches. Set \"edge\": \"nginx\" or"
        echo "       \"haproxy\" under .flip in $MANIFEST and re-run."
        exit 1
      elif [ "$HAP_RUNNING" = "true" ] && [ "${HAP_V1_COUNT:-0}" -gt 0 ]; then
        EDGE=haproxy
        echo "flip edge: haproxy (auto-detected — it is the edge routing /v1 to your nodeos)"
      else
        EDGE=nginx   # includes the no-web-server box: the managed nginx layout below
      fi;;
    *)
      echo "ABORT: flip.edge must be \"nginx\", \"haproxy\" or \"auto\" (got '$EDGE')"; exit 2;;
  esac
  [ "$EDGE" = "nginx" ] && { command -v nginx >/dev/null || apt-get install -y -qq nginx >/dev/null; }

  if [ "$EDGE" = "haproxy" ]; then
    # ---------- haproxy flip machinery ----------
    HAP_CFG=$(jq -r '.haproxy.cfg_path // empty' "$DOC")
    if [ -z "$HAP_CFG" ]; then
      echo "ABORT: flip.edge=haproxy but doctor found no readable haproxy config (checked the"
      echo "       process's -f path and /etc/haproxy/haproxy.cfg — run as root?). See $DOC (.haproxy)."
      exit 1
    fi
    ADMIN_SOCK=$(jq -r '.haproxy.admin_socket // empty' "$DOC")
    HAP_RT=$(jq -r '.haproxy.runtime // "unknown"' "$DOC")
    HAP_UNIT=$(jq -r '.haproxy.systemd_unit // empty' "$DOC")
    HAP_CONT=$(jq -r '.haproxy.container_name // empty' "$DOC")
    HAP_CCFG=$(jq -r '.haproxy.container_cfg_path // empty' "$DOC")
    # Validate/reload through the detected runtime: a containerized haproxy
    # has no host binary — validate via docker exec, reload via SIGHUP to the
    # master process (graceful, same as systemctl reload).
    if [ "$HAP_RT" = "docker" ] && [ -n "$HAP_CONT" ]; then
      HAP_VALIDATE="docker exec $HAP_CONT haproxy -c -f ${HAP_CCFG:-/usr/local/etc/haproxy/haproxy.cfg} >/dev/null"
      HAP_RELOAD="docker kill -s HUP $HAP_CONT >/dev/null"
    else
      HAP_VALIDATE="haproxy -c -f $HAP_CFG >/dev/null"
      HAP_RELOAD="systemctl reload ${HAP_UNIT:-haproxy}"
    fi

    # The /v1 surface: the first haproxy route whose backend reaches the
    # source nodeos (active, non-disabled server on the manifest's port).
    HAP_V1=$(jq -r --arg p ":$NODEOS_PORT" '
      [ .haproxy.routes[]
        | select(any(.servers[]?; (.addr | endswith($p)) and (.disabled | not))) ]
      | first // empty
      | . as $r
      | ($r.servers | map(select((.addr | endswith($p)) and (.disabled | not))) | first) as $s
      | [ $r.backend, $s.name, $s.addr,
          ($r.servers | map(select(.disabled | not)) | length),
          ($r.frontend + " " + ($r.binds | join(",")) + " [" + $r.rule + "]") ]
      | @tsv' "$DOC")
    if [ -z "$HAP_V1" ]; then
      echo "ABORT: flip.edge=haproxy but no haproxy route reaches the nodeos on :$NODEOS_PORT."
      echo "       doctor's map is in $DOC (.haproxy.routes). Point source.rpc_url at the nodeos"
      echo "       your haproxy actually fronts, or run 'pulse-cutover report' and share the"
      echo "       bundle so we can support this layout."
      exit 1
    fi
    IFS=$'\t' read -r HB HSRV HADDR HACTIVE HSURF <<< "$HAP_V1"
    if [ "$HACTIVE" -gt 1 ]; then
      # doctor already gates on this; belt-and-braces for hand-edited doctor.json.
      echo "ABORT: haproxy backend '$HB' balances $HACTIVE active servers — a single-box ceremony"
      echo "       flips only THIS box's entry. Decide the drain strategy first (README, HAProxy"
      echo "       notes): mark the servers that must not take post-cut traffic 'disabled', or"
      echo "       coordinate a fleet flip."
      exit 1
    fi
    LAYOUT=detected
    echo "public /v1 map DETECTED (haproxy): $HSURF -> $HB { $HSRV $HADDR }"
    # Strategy: admin socket when it exists AND is a live socket file;
    # otherwise cfg-edit + validate + graceful reload.
    if [ -n "$ADMIN_SOCK" ] && [ -S "$ADMIN_SOCK" ]; then
      HAP_STRATEGY="runtime-socket"
      command -v socat >/dev/null || { apt-get update -qq >/dev/null; apt-get install -y -qq socat >/dev/null; }
    else
      HAP_STRATEGY="cfg-reload"
      [ -n "$ADMIN_SOCK" ] && echo "  note: cfg declares stats socket $ADMIN_SOCK but no such socket exists on this host — falling back to the cfg-edit + reload flip"
    fi
    echo "  flip strategy: $HAP_STRATEGY (the flip will swap exactly $HB/$HSRV <-> $HB/pulsevm-gw, nothing else)"
    hap_stage_server "$HB" pulsevm-gw "$GW"
    hap_gen_flip "$HB" "$HSRV" pulsevm-gw "$GW" \
      /opt/pulse-cutover/flip-to-pulsevm.sh /opt/pulse-cutover/flip-revert.sh \
      flipped-to-pulsevm reverted-to-nodeos v1
  elif [ -n "$V1_MATCHES" ]; then
    LAYOUT=detected
    FLIP=/opt/pulse-cutover/flip-to-pulsevm.sh
    REVERT=/opt/pulse-cutover/flip-revert.sh
    {
      echo '#!/usr/bin/env bash'
      echo '# GENERATED by install.sh from the DETECTED nginx map (doctor.json).'
      echo '# The ONLY user-visible commitment of the ceremony (R5 ratchet): swap the'
      echo '# public /v1 upstream(s) from nodeos to the PulseVM gateway. Reads never gap:'
      echo '# nginx reload is connection-graceful and nodeos is still up behind us.'
      echo 'set -e'
    } > "$FLIP"
    { echo '#!/usr/bin/env bash'; echo '# GENERATED revert: swap the public /v1 back to nodeos.'; echo 'set -e'; } > "$REVERT"
    echo "public /v1 map DETECTED (the flip will swap exactly these, nothing else):"
    while IFS=$'\t' read -r KIND WHERE PASS BACKEND NAMES; do
      [ -n "$KIND" ] || continue
      if [ "$KIND" = "upstream" ]; then
        UPFILE=$(grep -RslE "upstream[[:space:]]+$WHERE[[:space:]]*\{" /etc/nginx 2>/dev/null | head -1)
        [ -n "$UPFILE" ] || { echo "ABORT: route uses upstream '$WHERE' but no file in /etc/nginx defines it"; exit 1; }
        echo "  ${NAMES:-_}: proxy_pass $PASS -> upstream $WHERE { $BACKEND } [$UPFILE]"
        echo "sed -i 's|server $BACKEND;|server $GW;|' $UPFILE" >> "$FLIP"
        echo "sed -i 's|server $GW;|server $BACKEND;|' $UPFILE" >> "$REVERT"
      else
        NEWPASS=${PASS/$BACKEND/$GW}
        echo "  ${NAMES:-_}: $PASS -> $NEWPASS [$WHERE]"
        echo "sed -i 's|proxy_pass $PASS;|proxy_pass $NEWPASS;|' $WHERE" >> "$FLIP"
        echo "sed -i 's|proxy_pass $NEWPASS;|proxy_pass $PASS;|' $WHERE" >> "$REVERT"
      fi
    done <<< "$V1_MATCHES"
    { echo 'nginx -t && nginx -s reload'; echo 'echo flipped-to-pulsevm'; } >> "$FLIP"
    { echo 'nginx -t && nginx -s reload'; echo 'echo reverted-to-nodeos'; } >> "$REVERT"
    chmod +x "$FLIP" "$REVERT"
  elif [ "${ROUTE_COUNT:-0}" -gt 0 ]; then
    # nginx has routes, but none proxy /v1 to the nodeos this manifest names.
    # Guessing which domain to hijack would be worse than refusing.
    echo "ABORT: nginx serves $ROUTE_COUNT route(s) but none proxy /v1 (or /) to the nodeos on :$NODEOS_PORT."
    echo "       doctor's map is in $DOC (.web.routes). Either add the /v1 route nginx-side,"
    echo "       point source.rpc_url at the nodeos your nginx actually fronts, or run"
    echo "       'pulse-cutover report' and share the bundle so we can support this layout."
    exit 1
  else
    LAYOUT=managed
    echo "no existing /v1 routes detected — staging the managed nginx layout"
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

  # Public /v2 surface, per edge/layout:
  #   haproxy  — same machinery as the /v1 flip: stage the federating router
  #              as a disabled server in the detected /v2 backend (reload now,
  #              not at H), flip via socket or cfg-edit+reload.
  #   detected — template the /v2 flip from the DETECTED nginx /v2 (or
  #              /v1/history) route: whatever it proxies today (your legacy
  #              Hyperion / old ES stack) is the pre-flip upstream, and the
  #              flip swaps it to the federating router. Your site files stay
  #              untouched until the flip stage.
  #   managed  — stage our own nginx site: pre-flip upstream = the legacy
  #              passthrough ("the /v2 you already had"); the v2 flip swaps
  #              the single upstream line.
  FED="127.0.0.1:7010"
  if [ "${EDGE:-nginx}" = "haproxy" ]; then
    # The /v2 surface: the first haproxy route whose rule mentions the
    # history paths (/v2 or /v1/history).
    HAP_V2=$(jq -r '
      [ .haproxy.routes[]?
        | select((.rule | test("/v2")) or (.rule | test("/v1/history"))) ]
      | first // empty
      | . as $r
      | ($r.servers | map(select(.disabled | not)) | first) as $s
      | [ $r.backend, ($s.name // ""), ($s.addr // ""),
          ($r.servers | map(select(.disabled | not)) | length),
          ($r.frontend + " [" + $r.rule + "]") ]
      | @tsv' "$DOC")
    if [ -z "$HAP_V2" ]; then
      echo "ABORT: hyperion mode needs a public /v2 (or /v1/history) route to flip, but doctor"
      echo "       found none in your haproxy map ($DOC .haproxy.routes). Add the route your"
      echo "       users already call, or run 'pulse-cutover report' so we can support this layout."
      exit 1
    fi
    IFS=$'\t' read -r HB2 HSRV2 HADDR2 HACTIVE2 HSURF2 <<< "$HAP_V2"
    if [ -z "$HSRV2" ]; then
      echo "ABORT: haproxy backend '$HB2' (the /v2 route) has no active server line to swap."
      exit 1
    fi
    if [ "$HACTIVE2" -gt 1 ]; then
      echo "ABORT: haproxy backend '$HB2' balances $HACTIVE2 active servers — decide the /v2 drain"
      echo "       strategy first (README, HAProxy notes), same rule as the /v1 backend."
      exit 1
    fi
    echo "public /v2 map DETECTED (haproxy): $HSURF2 -> $HB2 { $HSRV2 $HADDR2 }"
    hap_stage_server "$HB2" pulsevm-fed "$FED"
    hap_gen_flip "$HB2" "$HSRV2" pulsevm-fed "$FED" \
      /opt/pulse-cutover/flip-v2.sh /opt/pulse-cutover/flip-v2-revert.sh \
      flipped-v2-to-federator reverted-v2 v2
  elif [ "${LAYOUT:-managed}" = "detected" ]; then
    V2_MATCHES=$(jq -r '
      [ .web.routes[]
        | select((.location | startswith("/v2")) or (.location | startswith("/v1/history"))) ]
      | .[] | [ (if .upstream_name then "upstream" else "direct" end),
                (.upstream_name // .file), .proxy_pass,
                (.backends[0] // ""), (.server_names | join(" ")) ]
      | @tsv' "$DOC" | sort -u)
    if [ -z "$V2_MATCHES" ]; then
      echo "ABORT: hyperion mode needs a public /v2 (or /v1/history) route to flip, but doctor"
      echo "       found none in your nginx map ($DOC .web.routes). Add the route your users"
      echo "       already call, or run 'pulse-cutover report' so we can support this layout."
      exit 1
    fi
    FLIP2=/opt/pulse-cutover/flip-v2.sh
    REVERT2=/opt/pulse-cutover/flip-v2-revert.sh
    {
      echo '#!/usr/bin/env bash'
      echo '# GENERATED by install.sh from the DETECTED nginx map (doctor.json).'
      echo '# The /v2 half of the flip stage: public history moves to the federating'
      echo '# router (pre-cut = legacy, post-cut = local hyperion-rs).'
      echo 'set -e'
    } > "$FLIP2"
    { echo '#!/usr/bin/env bash'; echo '# GENERATED revert: swap /v2 back to its pre-ceremony upstream.'; echo 'set -e'; } > "$REVERT2"
    echo "public /v2 map DETECTED (the v2 flip will swap exactly these):"
    while IFS=$'\t' read -r KIND WHERE PASS BACKEND NAMES; do
      [ -n "$KIND" ] || continue
      if [ "$KIND" = "upstream" ]; then
        UPFILE=$(grep -RslE "upstream[[:space:]]+$WHERE[[:space:]]*\{" /etc/nginx 2>/dev/null | head -1)
        [ -n "$UPFILE" ] || { echo "ABORT: route uses upstream '$WHERE' but no file in /etc/nginx defines it"; exit 1; }
        echo "  ${NAMES:-_}: proxy_pass $PASS -> upstream $WHERE { $BACKEND } [$UPFILE]"
        echo "sed -i 's|server $BACKEND;|server $FED;|' $UPFILE" >> "$FLIP2"
        echo "sed -i 's|server $FED;|server $BACKEND;|' $UPFILE" >> "$REVERT2"
      else
        NEWPASS=${PASS/$BACKEND/$FED}
        echo "  ${NAMES:-_}: $PASS -> $NEWPASS [$WHERE]"
        echo "sed -i 's|proxy_pass $PASS;|proxy_pass $NEWPASS;|' $WHERE" >> "$FLIP2"
        echo "sed -i 's|proxy_pass $NEWPASS;|proxy_pass $PASS;|' $WHERE" >> "$REVERT2"
      fi
    done <<< "$V2_MATCHES"
    { echo 'nginx -t && nginx -s reload'; echo 'echo flipped-v2-to-federator'; } >> "$FLIP2"
    { echo 'nginx -t && nginx -s reload'; echo 'echo reverted-v2'; } >> "$REVERT2"
    chmod +x "$FLIP2" "$REVERT2"
  else
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

  cat > /opt/pulse-cutover/hyperion-start.sh <<'HYPSTART'
#!/usr/bin/env bash
# Started by the ceremony with the FIRST POST-CUT BLOCK as $1. hyperion-rs on
# an imported chain must index from cut+1: start_block=0 requests the SHiP
# stream from block 1, which this chain cannot serve (no pre-cut blocks), and
# the stream stays silent forever while health looks merely idle (R21).
set -e
FIRST=${1:?usage: hyperion-start.sh <first-post-cut-block>}
sed -i "s/^start_block = .*/start_block = $FIRST/" /etc/hyperion/config.toml
systemctl restart hyperion-indexer hyperion-api
echo "hyperion-rs indexing from $FIRST"
HYPSTART
  chmod +x /opt/pulse-cutover/hyperion-start.sh
fi

# ---------- ceremony.toml (the agent's manifest) ----------
# STOP_CMD/START_CMD were resolved above: manifest value, else derived from
# the doctor-detected unit/container.
FH=$(mget '.ceremony.freeze_height')
FM=$(mget_opt '.ceremony.freeze_margin')
SIM=$(mget_opt '.ceremony.simulate_freeze'); SIM=${SIM:-false}
GOLDENS=$(mget_opt '.snapshot.golden_roots')
EXPECTED_SHA=$(mget_opt '.snapshot.expected_sha256')
PRESCAN=$(mget_opt '.snapshot.prescan_path')
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
  [ -n "$PRESCAN" ] && echo "prescan_path = \"$PRESCAN\""
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
    echo 'start_cmd = "/opt/pulse-cutover/hyperion-start.sh {first_post_cut_block}"'
    echo 'health_url = "http://127.0.0.1:7000/v2/health"'
    echo 'hydration_timeout_secs = 900'
    echo 'boundary_path = "/etc/pulse-cutover/boundary.json"'
    echo 'flip_cmd = "/opt/pulse-cutover/flip-v2.sh"'
    echo 'revert_cmd = "/opt/pulse-cutover/flip-v2-revert.sh"'
    echo "public_health_url = \"http://$(mget '.flip.public_host')/v2/health\""
  fi
} > /etc/pulse-cutover/ceremony.toml
cp "$MANIFEST" /etc/pulse-cutover/ceremony.json

# ---------- stubbed-intrinsic preflight (advisory, never a gate) ----------
# When a rehearsal/pre-downloaded snapshot is staged, print + save the
# at-risk contract table now: which deployed contracts reference host
# functions PulseVM stubs (they load, but TRAP if the import is ever
# CALLED). The agent re-runs this scan on the ACTUAL cut snapshot during the
# ceremony and journals it either way.
if [ -n "$PRESCAN" ] && [ -f "$PRESCAN" ]; then
  echo ""
  echo "== stubbed-intrinsic preflight (advisory — install continues regardless) =="
  pulse-cutover scan-contracts "$PRESCAN" | tee "$WORK/scan-contracts.txt" || true
fi

NODEID=$(curl -s -m5 http://127.0.0.1:9650/ext/info -X POST -H 'content-type:application/json' -d '{"jsonrpc":"2.0","id":1,"method":"info.getNodeID"}' | jq -r '.result.nodeID // "(metalgo still starting)"')
echo ""
echo "============================================================"
echo " ARMED-READY (mode: $MODE) — installed, verified, staged."
echo ""
echo " Doctor:  survey + per-mode verdicts in $WORK/doctor.json"
$API_LIKE && echo "          flip scripts templated from the ${LAYOUT:-managed} ${EDGE:-nginx} layout${HAP_STRATEGY:+ ($HAP_STRATEGY flip)}"
[ -n "$STOP_CMD" ] && echo "          source stop: $STOP_CMD"
echo " Source:  nodeos at $SRC_RPC serving $CHAIN_ID (untouched)"
echo " Target:  metalgo-pulse tracking subnet $SUBNET"
echo "          NodeID $NODEID"
echo "          chain $BID: waiting for the verified snapshot at"
echo "          $STAGED"
echo "          (absent by design until VERIFIED — a stale file would pin the wrong cut, R12)"
$API_LIKE && echo " Public:  ${EDGE:-nginx} /v1 -> nodeos:${NODEOS_PORT:-8888} (flip staged, NOT flipped)"
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
