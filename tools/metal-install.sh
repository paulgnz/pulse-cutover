#!/usr/bin/env bash
# metal-install.sh — install a Metal (metalgo) node next to your XPR nodeos and print your NodeID.
#
#   curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/metal-install.sh | sudo bash
#
# What it does:
#   + reads the network manifest from mission control (which Metal network, pinned metalgo version + sha256,
#     and the PulseVM subnet/chain IDs once they exist) — nobody types IDs by hand
#   + checks the box (RAM, disk, ports, glibc) and picks how to install:
#       official binary (glibc >= 2.34) · official Docker image (if Docker is present) · build from source (--build)
#   + runs metalgo as the `metalgo` systemd service, API on 127.0.0.1 only, staking port 9651 open
#   + waits for your node identity and prints: NodeID, BLS public key, proof of possession
#   + backs up your staking keys to a root-only archive and tells you to copy it off the box
#   - never stakes, funds, registers or changes nodeos; never prints private keys
#
# Options: [--metal tahoe|mainnet] [--manifest-url URL] [--build] [--method binary|docker|build] [--dry-run] [--uninstall]
# Re-running is safe: it keeps the node identity, re-applies the pinned version and prints the IDs again.
set -euo pipefail
METAL=""; MANIFEST_URL=""; METHOD=""; BUILD=0; DRY=0; UNINSTALL=0
CONTROL="https://control-rehearsal.protonnz.com"
while [ $# -gt 0 ]; do case "$1" in
  --metal) METAL=$2; shift 2;; --manifest-url) MANIFEST_URL=$2; shift 2;; --build) BUILD=1; shift;; --method) METHOD=$2; shift 2;;
  --dry-run) DRY=1; shift;; --uninstall) UNINSTALL=1; shift;;
  *) echo "unknown option $1" >&2; exit 2;; esac; done
say() { printf '\033[1m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[!]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[x]\033[0m %s\n' "$*" >&2; exit 1; }
[ "$(id -u)" = 0 ] || die "run as root (sudo)"
DATA=/var/lib/metalgo; ETC=/etc/metalgo

if [ "$UNINSTALL" = 1 ]; then
  systemctl disable --now metalgo 2>/dev/null || true; rm -f /etc/systemd/system/metalgo.service; systemctl daemon-reload
  say "metalgo service removed. Your node identity and chain data are KEPT in $DATA (delete them yourself only after backing up $DATA/staking)."
  exit 0
fi

# ---- which Metal network: from the XPR chain this box runs, unless given ------------------------------
if [ -z "$METAL" ]; then
  CID=$(curl -fsS -m5 -X POST http://127.0.0.1:8888/v1/chain/get_info -d '{}' 2>/dev/null | sed -n 's/.*"chain_id":"\([0-9a-f]*\)".*/\1/p' || true)
  case "$CID" in
    384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0) XPR=mainnet; METAL=mainnet;;
    71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd) XPR=testnet; METAL=tahoe;;
    *) die "could not tell which XPR network this box runs: pass --metal tahoe (testnet) or --metal mainnet";;
  esac
else XPR=$([ "$METAL" = mainnet ] && echo mainnet || echo testnet); fi

# ---- manifest (pins) ---------------------------------------------------------------------------------
[ -n "$MANIFEST_URL" ] || MANIFEST_URL="$CONTROL/api/manifest/$XPR"
M=$(curl -fsS -m10 "$MANIFEST_URL" 2>/dev/null || true)
jqm() { printf '%s' "$M" | sed -n "s/.*\"$1\":\"\\([^\"]*\\)\".*/\\1/p" | head -1; }
if [ -n "$M" ]; then
  VERSION=$(jqm metalgo_version); NETID=$(printf '%s' "$M" | sed -n 's/.*"network_id":\([0-9]*\).*/\1/p' | head -1)
  SHA_AMD=$(jqm sha256_linux_amd64); SHA_ARM=$(jqm sha256_linux_arm64); IMAGE=$(jqm docker_image)
  SUBNET=$(jqm subnet_id); CHAIN=$(jqm blockchain_id)
  say "manifest: $MANIFEST_URL"
else
  warn "mission control unreachable — using built-in pins"
  case "$METAL" in tahoe) VERSION=v1.14.2-tahoe; NETID=5;; mainnet) VERSION=v1.13.5; NETID=1;; esac
  SHA_AMD=""; SHA_ARM=""; IMAGE="metalblockchain/metalgo:$VERSION"; SUBNET=""; CHAIN=""
fi
[ -n "$VERSION" ] && [ -n "$NETID" ] || die "manifest has no metalgo version/network id"
say "XPR $XPR → Metal $METAL (network-id $NETID) · metalgo $VERSION"

# ---- preflight ---------------------------------------------------------------------------------------
ARCH=$(uname -m); case "$ARCH" in x86_64) A=amd64; SHA=$SHA_AMD;; aarch64|arm64) A=arm64; SHA=$SHA_ARM;; *) die "unsupported arch $ARCH";; esac
RAM=$(awk '/MemTotal/{printf "%d", $2/1024/1024}' /proc/meminfo)
mkdir -p "$DATA" 2>/dev/null || true
FREE=$(df -Pk "$DATA" | awk 'NR==2{printf "%d", $4/1024/1024}')
GLIBC=$(ldd --version 2>/dev/null | head -1 | grep -o '[0-9]\+\.[0-9]\+$' || echo 0)
say "box: ${RAM} GB RAM · ${FREE} GB free for $DATA · glibc $GLIBC · $ARCH"
[ "$RAM" -ge 8 ] || warn "less than 8 GB RAM: metalgo next to nodeos will be tight"
[ "$FREE" -ge 100 ] || warn "less than 100 GB free: the primary network (P/X/C chains) can outgrow this"
systemctl is-active --quiet metalgo && say "metalgo already installed: upgrading/re-applying in place (identity kept)" || for p in 9650 9651; do ss -ltn "sport = :$p" 2>/dev/null | grep -q LISTEN && die "port $p is already in use (another metalgo?)"; done

if [ -n "$METHOD" ]; then :
elif awk "BEGIN{exit !($GLIBC >= 2.34)}"; then METHOD=binary
elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then METHOD=docker
elif [ "$BUILD" = 1 ]; then METHOD=build
else
  die "this OS has glibc $GLIBC; metalgo needs glibc 2.34+, i.e. Ubuntu 22.04 or newer.
    Recommended: upgrade this server to Ubuntu 22.04/24.04 (20.04 is out of standard support), then re-run.
    Stopgaps if you can't upgrade yet:
      - install Docker and re-run (uses the official image $IMAGE), or
      - re-run with --build (compiles metalgo here at lowest CPU priority, ~10 min; best effort)"
fi
say "install method: $METHOD"
[ "$DRY" = 1 ] && { say "dry run: nothing installed"; exit 0; }

# ---- install metalgo ---------------------------------------------------------------------------------
systemctl stop metalgo 2>/dev/null || true
id metalgo >/dev/null 2>&1 || useradd --system --home "$DATA" --shell /usr/sbin/nologin metalgo
mkdir -p "$DATA"/{db,logs,plugins,chains} "$ETC"; chown -R metalgo:metalgo "$DATA"
BIN=/usr/local/bin/metalgo
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
case "$METHOD" in
  binary)
    URL="https://github.com/MetalBlockchain/metalgo/releases/download/$VERSION/metalgo-linux-$A-$VERSION.tar.gz"
    curl -fsSL -o "$TMP/mg.tgz" "$URL"
    if [ -n "$SHA" ]; then echo "$SHA  $TMP/mg.tgz" | sha256sum -c --quiet || die "checksum mismatch for $URL: refusing to install"; else warn "no pinned checksum in manifest"; fi
    tar xzf "$TMP/mg.tgz" -C "$TMP"; install -m 755 "$TMP"/metalgo-*/metalgo "$BIN";;
  build)
    say "building metalgo $VERSION from source (nice 19; nodeos keeps priority)…"
    export DEBIAN_FRONTEND=noninteractive; apt-get install -y -qq git build-essential >/dev/null
    GOV=$(curl -fsSL "https://raw.githubusercontent.com/MetalBlockchain/metalgo/$VERSION/go.mod" | sed -n "s/^go //p"); curl -fsSL -o "$TMP/go.tgz" "https://go.dev/dl/go$GOV.linux-$A.tar.gz"; rm -rf /usr/local/go-metal; mkdir -p /usr/local/go-metal
    tar xzf "$TMP/go.tgz" -C /usr/local/go-metal --strip-components=1
    git clone -q --depth 1 --branch "$VERSION" https://github.com/MetalBlockchain/metalgo "$TMP/src"
    (cd "$TMP/src" && PATH=/usr/local/go-metal/bin:$PATH GOFLAGS=-buildvcs=false nice -n 19 ionice -c3 ./scripts/build.sh >"$TMP/build.log" 2>&1) || { tail -20 "$TMP/build.log"; die "build failed"; }
    install -m 755 "$TMP/src/build/metalgo" "$BIN";;
  docker) docker pull -q "$IMAGE" >/dev/null;;
esac

# Advertise the box's IPv4 (auto-resolution can pick IPv6, which many peers can't reach). Override: METAL_PUBLIC_IP=…
PUB4=${METAL_PUBLIC_IP:-$(curl -4 -fsS -m5 https://api.ipify.org 2>/dev/null || curl -4 -fsS -m5 https://ifconfig.me 2>/dev/null || true)}
printf '%s' "$PUB4" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' || PUB4=""
cat > "$ETC/config.json" <<J
{
  "network-id": "$NETID",
  "data-dir": "$DATA",
  "db-dir": "$DATA/db",
  "log-dir": "$DATA/logs",
  "plugin-dir": "$DATA/plugins",
  "chain-config-dir": "$DATA/chains",
  "http-host": "127.0.0.1",
  "http-port": 9650,
  "staking-port": 9651,
  $( [ -n "$PUB4" ] && printf '"public-ip": "%s"' "$PUB4" || printf '"public-ip-resolution-service": "opendns"' )$( [ -n "$SUBNET" ] && printf ',\n  "track-subnets": "%s"' "$SUBNET" )
}
J
if [ "$METHOD" = docker ]; then
  EXEC="/usr/bin/docker run --rm --name metalgo --network host -v $DATA:$DATA -v $ETC:$ETC --user $(id -u metalgo):$(id -g metalgo) $IMAGE /metalgo/build/metalgo --config-file=$ETC/config.json"
  PRE="ExecStartPre=-/usr/bin/docker rm -f metalgo"; USERLINE=""
else EXEC="$BIN --config-file=$ETC/config.json"; PRE=""; USERLINE="User=metalgo"; fi
cat > /etc/systemd/system/metalgo.service <<U
[Unit]
Description=Metal node (metalgo $VERSION, $METAL)
After=network-online.target
[Service]
$USERLINE
$PRE
ExecStart=$EXEC
Restart=always
RestartSec=5
LimitNOFILE=65536
Nice=5
[Install]
WantedBy=multi-user.target
U
command -v ufw >/dev/null && ufw status 2>/dev/null | grep -q "Status: active" && ufw allow 9651/tcp >/dev/null
systemctl daemon-reload; systemctl enable --now metalgo >/dev/null 2>&1; systemctl restart metalgo

# ---- identity ----------------------------------------------------------------------------------------
say "starting metalgo and waiting for the node identity…"
ID=""
for i in $(seq 1 90); do
  ID=$(curl -fsS -m3 -X POST -H 'content-type:application/json' -d '{"jsonrpc":"2.0","id":1,"method":"info.getNodeID"}' http://127.0.0.1:9650/ext/info 2>/dev/null || true)
  printf '%s' "$ID" | grep -q NodeID && break; sleep 2
done
printf '%s' "$ID" | grep -q NodeID || die "metalgo did not answer; check: journalctl -u metalgo -n 50"
NODEID=$(printf '%s' "$ID" | sed -n 's/.*"nodeID":"\(NodeID-[^"]*\)".*/\1/p')
BLSPUB=$(printf '%s' "$ID" | sed -n 's/.*"publicKey":"\(0x[0-9a-f]*\)".*/\1/p')
BLSPOP=$(printf '%s' "$ID" | sed -n 's/.*"proofOfPossession":"\(0x[0-9a-f]*\)".*/\1/p')

# ---- back up the identity (staking TLS cert/key + BLS signer key) -------------------------------------
BK=/root/metalgo-identity-$NODEID.tar.gz
for i in $(seq 1 20); do [ -f "$DATA/staking/signer.key" ] && break; sleep 1; done
(umask 077; tar czf "$BK" -C "$DATA" staking)
BKSHA=$(sha256sum "$BK" | cut -d' ' -f1)
cat > "$ETC/identity.txt" <<T
NodeID:              $NODEID
BLS public key:      $BLSPUB
Proof of possession: $BLSPOP
Metal network:       $METAL (network-id $NETID)
Backup:              $BK (sha256 $BKSHA)
Keys to back up:     $DATA/staking/staker.key, staker.crt (NodeID) and signer.key (BLS), all in the backup above
T
chmod 644 "$ETC/identity.txt"

PUBIP=$(curl -fsS -m3 -X POST -H 'content-type:application/json' -d '{"jsonrpc":"2.0","id":1,"method":"info.getNodeIP"}' http://127.0.0.1:9650/ext/info 2>/dev/null | sed -n 's/.*"ip":"\([^"]*\)".*/\1/p')
NOTE=$(printf '%s' "$M" | sed -n 's/.*"upgrades_note":"\([^"]*\)".*/\1/p')

echo
echo "  ✓ Metal node installed and running ($METHOD, metalgo $VERSION, $METAL)"
echo
echo "  ── Your validator identity (PUBLIC: share these to register) ───────────────────────────────"
echo "  NodeID               $NODEID"
echo "  BLS public key       $BLSPUB"
echo "  Proof of possession  $BLSPOP"
echo "  Advertised address   ${PUBIP:-unknown}   (staking port 9651/tcp must be reachable from the internet)"
echo "  saved in $ETC/identity.txt"
echo
echo "  ── BACK UP YOUR KEYS NOW (PRIVATE: never share, never commit) ──────────────────────────────"
echo "  $DATA/staking/staker.key + staker.crt   → your NodeID. Lose them and you get a new NodeID."
echo "  $DATA/staking/signer.key                → your BLS key. Lose it and you must re-register a new key + PoP."
echo "  All three are in:  $BK"
echo "                     (root-only, sha256 $BKSHA)"
echo "  Copy it OFF this server (password manager / encrypted storage):"
echo "      scp root@<this-server>:$BK ."
echo "  Chain data is NOT needed in the backup: it re-syncs. Never run two nodes with the same keys at once."
echo
echo "  ── What you will need to register as a PulseVM validator ───────────────────────────────────"
echo "  1. The NodeID, BLS public key and proof of possession above."
echo "  2. A Metal P-Chain address you control ($( [ "$METAL" = mainnet ] && echo 'P-metal1…' || echo 'P-tahoe1…')): it receives any unused validator balance"
echo "     and can disable the validator. Use a key you already back up; not a key on this server."
echo "  3. METAL on the P-Chain to prepay the validator's continuous fee (the amount is announced with the event)."
echo "  Nothing is registered or spent by this script."
echo
if [ -n "$CHAIN" ]; then echo "  Tracking PulseVM subnet $SUBNET (chain $CHAIN)."; else echo "  No PulseVM chain is published for XPR $XPR yet: this node syncs the Metal $METAL primary network for now."; fi
[ -n "$NOTE" ] && echo "  Note: $NOTE"
echo "  Upgrades: re-run this same command. It keeps your keys and installs the version the manifest pins."
echo "  Health:   curl -s 127.0.0.1:9650/ext/health | head -c 300"
echo "  Logs:     journalctl -u metalgo -f"
