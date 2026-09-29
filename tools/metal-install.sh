#!/usr/bin/env bash
# metal-install.sh — install a Metal (metalgo) node next to your XPR nodeos and print your NodeID.
#
#   curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/metal-install.sh | sudo bash
#
# What it does:
#   + reads the network manifest from mission control (which Metal network, pinned metalgo version + sha256,
#     and the PulseVM subnet/chain IDs once they exist) — nobody types IDs by hand
#   + checks the box (RAM, disk, ports, glibc) and picks how to install:
#       official binary (glibc >= 2.34) · native Ubuntu 20.04 build of the same tag (glibc 2.31-2.33)
#       · official Docker image · build from source (--build)
#   + runs metalgo as the `metalgo` systemd service, API on 127.0.0.1 only, staking port 9651 open
#   + waits for your node identity and prints: NodeID, BLS public key, proof of possession
#   + backs up your staking keys to a root-only archive and tells you to copy it off the box
#   - never stakes, funds, registers or changes nodeos; never prints private keys
#
# Options: [--check] (re-test port 9651 + print identity; changes nothing) [--open-port] (also add the local firewall rule)
#          [--metal tahoe|mainnet] [--manifest-url URL] [--build] [--method binary|compat|docker|build] [--dry-run] [--uninstall]
# Re-running is safe: it keeps the node identity, re-applies the pinned version and prints the IDs again.
set -euo pipefail
METAL=""; MANIFEST_URL=""; METHOD=""; CHECK=0; OPEN_PORT=0; BUILD=0; DRY=0; UNINSTALL=0
while [ $# -gt 0 ]; do case "$1" in
  --metal) METAL=$2; shift 2;; --manifest-url) MANIFEST_URL=$2; shift 2;; --build) BUILD=1; shift;; --method) METHOD=$2; shift 2;;
  --dry-run) DRY=1; shift;; --check) CHECK=1; shift;; --open-port) OPEN_PORT=1; shift;; --uninstall) UNINSTALL=1; shift;;
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

G='\033[32m'; Y='\033[33m'; B='\033[1m'; N='\033[0m'
CONTROL="https://control-rehearsal.protonnz.com"

# ---- port 9651: test from outside; if closed, work out WHICH firewall blocks it ----------------------
# Mission control dials back to this server's IP on 9651 only. If that fails, look at what this box can see:
# is metalgo listening publicly, and do ufw / firewalld / iptables / nftables here drop it? If every local
# layer allows it, the block is upstream (provider firewall / security group), which no script here can change.
port_diag() {
  REACH=""
  for _ in 1 2; do   # one retry: mission control rate-limits to one probe per 5 s per IP
    REACH=$(curl -4 -fsS -m10 "$CONTROL/api/reach" 2>/dev/null | sed -n 's/.*"reachable":\(true\|false\).*/\1/p' || true)
    [ -n "$REACH" ] && break; sleep 6
  done
  CAUSE=""; FIX=""; FIX2=""
  [ "$REACH" = false ] || return 0
  LISTEN=$(ss -ltnH "sport = :9651" 2>/dev/null | awk '{print $4}' | head -1 || true)
  if [ -z "$LISTEN" ]; then
    CAUSE="metalgo is not listening on port 9651 (it may still be starting, or crashed)"
    FIX="sudo systemctl restart metalgo && journalctl -u metalgo -n 50"; return 0
  fi
  case "$LISTEN" in 127.*|"[::1]"*)
    CAUSE="metalgo only listens on $LISTEN (localhost), not on the public interface"
    FIX="remove any staking-host / listen override from $ETC/config.json, then: sudo systemctl restart metalgo"; return 0;;
  esac
  if command -v ufw >/dev/null && ufw status 2>/dev/null | grep -q "Status: active"; then
    if ! ufw status 2>/dev/null | grep -qE "^9651(/tcp)?[[:space:]].*ALLOW"; then
      CAUSE="ufw (this server's firewall) is on and has no rule for 9651"
      FIX="sudo ufw allow 9651/tcp"; return 0
    fi
  fi
  if command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
    if ! firewall-cmd --list-ports 2>/dev/null | grep -qw "9651/tcp"; then
      CAUSE="firewalld (this server's firewall) is running and does not allow 9651"
      FIX="sudo firewall-cmd --permanent --add-port=9651/tcp && sudo firewall-cmd --reload"; return 0
    fi
  fi
  if command -v iptables >/dev/null; then
    RULES=$(iptables -S INPUT 2>/dev/null || true)
    POL=$(printf '%s\n' "$RULES" | sed -n 's/^-P INPUT //p')
    if ! printf '%s\n' "$RULES" | grep -qE -- "--dport 9651 .*-j ACCEPT" && \
       { [ "$POL" = DROP ] || printf '%s\n' "$RULES" | grep -qE -- "-j (DROP|REJECT)"; }; then
      CAUSE="iptables on this server drops inbound ports that aren't listed, and 9651 isn't listed"
      FIX="sudo iptables -I INPUT -p tcp --dport 9651 -j ACCEPT"
      if command -v netfilter-persistent >/dev/null; then FIX2="make it survive reboots: sudo netfilter-persistent save"
      else FIX2="make it survive reboots: add the same rule to whatever loads your firewall at boot (e.g. /etc/iptables/rules.v4)"; fi
      return 0
    fi
  fi
  if command -v nft >/dev/null && nft list ruleset 2>/dev/null | grep -qE "hook input .*policy drop" && \
     ! nft list ruleset 2>/dev/null | grep -q "dport 9651"; then
    CAUSE="nftables (this server's firewall) drops inbound by default and has no rule for 9651"
    FIX="add 'tcp dport 9651 accept' to the input chain in /etc/nftables.conf, then: sudo systemctl reload nftables"; return 0
  fi
  # Name the provider only when the cloud's own metadata service says who it is (link-local, never leaves the box).
  md() { curl -fsS -m2 "$@" 2>/dev/null || true; }
  PROV=""; WHERE=""
  if md http://169.254.169.254/hetzner/v1/metadata/instance-id | grep -q '[0-9]'; then PROV="Hetzner"; WHERE="Hetzner Console > Firewalls (or the server's Firewalls tab)"
  elif md http://169.254.169.254/v1.json | grep -q '"instanceid"'; then PROV="Vultr"; WHERE="Vultr > Network > Firewall (the firewall group attached to this instance)"
  elif md http://169.254.169.254/metadata/v1/id | grep -q '[0-9]'; then PROV="DigitalOcean"; WHERE="DigitalOcean > Networking > Firewalls"
  elif TOK=$(md -X PUT -H "X-aws-ec2-metadata-token-ttl-seconds: 60" http://169.254.169.254/latest/api/token) && [ -n "$TOK" ]; then PROV="AWS"; WHERE="EC2 > this instance > Security > Security groups (inbound rules)"
  elif md -H "Metadata-Flavor: Google" http://169.254.169.254/computeMetadata/v1/instance/id | grep -q '[0-9]'; then PROV="Google Cloud"; WHERE="VPC network > Firewall"
  fi
  if [ -n "$PROV" ]; then
    CAUSE="nothing on this server blocks 9651, so the block is in front of it: the $PROV firewall"
    FIX="allow inbound TCP 9651 from anywhere (0.0.0.0/0) to this server in $WHERE"
  else
    CAUSE="nothing on this server blocks 9651, so the block is in front of it: your hosting provider's firewall or network"
    FIX="ask your hosting provider (or use their control panel) to allow inbound TCP 9651 from anywhere to this server"
  fi
}

# --open-port: add the local rule for whichever firewall is active (never touches the provider's).
open_port_local() {
  if command -v ufw >/dev/null && ufw status 2>/dev/null | grep -q "Status: active"; then
    ufw allow 9651/tcp >/dev/null && say "ufw: allowed 9651/tcp"
  elif command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
    firewall-cmd --permanent --add-port=9651/tcp >/dev/null && firewall-cmd --reload >/dev/null && say "firewalld: allowed 9651/tcp"
  elif command -v iptables >/dev/null; then
    iptables -C INPUT -p tcp --dport 9651 -j ACCEPT 2>/dev/null || iptables -I INPUT -p tcp --dport 9651 -j ACCEPT
    say "iptables: allowed 9651/tcp"
    if command -v netfilter-persistent >/dev/null; then netfilter-persistent save >/dev/null 2>&1 && say "iptables: rule saved for reboots"
    else warn "iptables rule is live but not saved for reboots: add it to your boot-time firewall rules"; fi
  fi
}

print_port() {
  if [ "$REACH" = true ]; then printf "  ${G}✓${N} 1. Staking port 9651 is reachable from the internet. Nothing to do.\n"
  elif [ "$REACH" = false ]; then
    printf "  ${Y}✗${N} 1. ${B}Port 9651/tcp is closed to the internet.${N} Other nodes can't connect in (you only have outbound peers).\n"
    echo "       Why:  $CAUSE"
    echo "       Fix:  $FIX"
    [ -n "$FIX2" ] && echo "             $FIX2"
    echo "       Then re-check (changes nothing):"
    echo "         curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/metal-install.sh | sudo bash -s -- --check"
  else printf "  ${Y}?${N} 1. Could not test port 9651 (mission control unreachable). Make sure inbound TCP 9651 is open.\n"; fi
}

if [ "$CHECK" = 1 ]; then
  systemctl is-active --quiet metalgo || die "metalgo is not running here (install it first: run without --check)"
  [ "$OPEN_PORT" = 1 ] && open_port_local
  port_diag
  echo
  [ -f "$ETC/identity.txt" ] && sed 's/^/  /' "$ETC/identity.txt" && echo
  PEERS=$(curl -fsS -m3 -X POST -H 'content-type:application/json' -d '{"jsonrpc":"2.0","id":1,"method":"info.peers"}' http://127.0.0.1:9650/ext/info 2>/dev/null | sed -n 's/.*"numPeers":"\([0-9]*\)".*/\1/p' || true)
  PB=$(curl -fsS -m3 -X POST -H 'content-type:application/json' -d '{"jsonrpc":"2.0","id":1,"method":"info.isBootstrapped","params":{"chain":"P"}}' http://127.0.0.1:9650/ext/info 2>/dev/null | sed -n 's/.*"isBootstrapped":\(true\|false\).*/\1/p' || true)
  echo "  metalgo: $(systemctl is-active metalgo) · peers ${PEERS:-?} · P-Chain synced: ${PB:-?}"
  echo
  print_port
  echo
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
  COMPAT_URL=$(jqm compat_glibc231_url); COMPAT_SHA=$(jqm compat_glibc231_sha256)
  say "manifest: $MANIFEST_URL"
else
  warn "mission control unreachable — using built-in pins"
  case "$METAL" in tahoe) VERSION=v1.14.2-tahoe; NETID=5;; mainnet) VERSION=v1.13.5; NETID=1;; esac
  SHA_AMD=""; SHA_ARM=""; IMAGE="metalblockchain/metalgo:$VERSION"; SUBNET=""; CHAIN=""; COMPAT_URL=""; COMPAT_SHA=""
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
elif [ "$A" = amd64 ] && [ -n "$COMPAT_URL" ] && awk "BEGIN{exit !($GLIBC >= 2.31)}"; then METHOD=compat
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
  compat)
    # Same metalgo source tag, compiled on Ubuntu 20.04 so it runs natively on glibc 2.31
    # (reproduce with tools/build-metalgo-glibc231.sh). Checksum pinned in the manifest.
    curl -fsSL -o "$TMP/mg.tgz" "$COMPAT_URL"
    echo "$COMPAT_SHA  $TMP/mg.tgz" | sha256sum -c --quiet || die "checksum mismatch for $COMPAT_URL: refusing to install"
    tar xzf "$TMP/mg.tgz" -C "$TMP"; install -m 755 "$TMP"/metalgo-*/metalgo "$BIN"
    "$BIN" --version >/dev/null || die "the glibc 2.31 build does not run here";;
  build)
    say "building metalgo $VERSION from source (nice 19; nodeos keeps priority)…"
    export DEBIAN_FRONTEND=noninteractive; apt-get install -y -qq git build-essential >/dev/null
    GOV=$(curl -fsSL "https://raw.githubusercontent.com/MetalBlockchain/metalgo/$VERSION/go.mod" | sed -n "s/^go //p"); curl -fsSL -o "$TMP/go.tgz" "https://go.dev/dl/go$GOV.linux-$A.tar.gz"; rm -rf /usr/local/go-metal; mkdir -p /usr/local/go-metal
    tar xzf "$TMP/go.tgz" -C /usr/local/go-metal --strip-components=1
    git clone -q --depth 1 --branch "$VERSION" https://github.com/MetalBlockchain/metalgo "$TMP/src"
    (cd "$TMP/src" && PATH=/usr/local/go-metal/bin:$PATH GOFLAGS=-buildvcs=false CGO_LDFLAGS=-ldl nice -n 19 ionice -c3 ./scripts/build.sh >"$TMP/build.log" 2>&1) || { tail -20 "$TMP/build.log"; die "build failed"; }
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

PUBIP=$(curl -fsS -m3 -X POST -H 'content-type:application/json' -d '{"jsonrpc":"2.0","id":1,"method":"info.getNodeIP"}' http://127.0.0.1:9650/ext/info 2>/dev/null | sed -n 's/.*"ip":"\([^"]*\)".*/\1/p' || true)
NOTE=$(printf '%s' "$M" | sed -n 's/.*"upgrades_note":"\([^"]*\)".*/\1/p' || true)
PRODUCER=$(sed -n 's/^producer *= *"\([a-z1-5.]*\)".*/\1/p' /etc/pulse-cutover/beacon.toml 2>/dev/null | head -1 || true)
[ -n "$PRODUCER" ] || PRODUCER=youraccount

# Is the staking port reachable from the internet? Mission control dials back to THIS server's IP on 9651 only.
sleep 3
[ "$OPEN_PORT" = 1 ] && open_port_local
port_diag

# Put a copy of the key archive where the sudo user can scp it (root-only /root is awkward to copy from).
BK_USER=""
if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ]; then
  UH=$(getent passwd "$SUDO_USER" | cut -d: -f6)
  if [ -n "$UH" ] && [ -d "$UH" ]; then BK_USER="$UH/$(basename "$BK")"; install -m 600 -o "$SUDO_USER" "$BK" "$BK_USER"; fi
fi

# Machine-readable identity for agents and tooling (public values only).
cat > "$ETC/identity.json" <<JSON
{"schema":"metal-identity-v1","xpr_network":"$XPR","producer":"$PRODUCER","metal_network":"$METAL","network_id":$NETID,
 "metalgo_version":"$VERSION","install_method":"$METHOD","node_id":"$NODEID","bls_public_key":"$BLSPUB",
 "bls_proof_of_possession":"$BLSPOP","staking_address":"${PUBIP:-}","staking_port_reachable":${REACH:-null},
 "key_backup":"$BK","key_backup_sha256":"$BKSHA"}
JSON
chmod 644 "$ETC/identity.json"

echo
printf "  ${G}✓${N} ${B}Metal node installed and running${N} ($METHOD, metalgo $VERSION, $METAL)\n"
echo
echo "  ── Your validator identity (public) ───────────────────────────────────────────────────────"
echo "  NodeID               $NODEID"
echo "  BLS public key       $BLSPUB"
echo "  Proof of possession  $BLSPOP"
echo "  Staking address      ${PUBIP:-unknown}"
echo "  (also in $ETC/identity.txt and, for scripts/agents, $ETC/identity.json)"
echo
printf "  ${B}── NEXT STEPS ──────────────────────────────────────────────────────────────────────────────${N}\n"
echo
print_port
echo
printf "  ${Y}!${N} 2. ${B}Back up your keys off this server.${N} They ARE your validator; chain data is not needed.\n"
echo "       staker.key + staker.crt = your NodeID · signer.key = your BLS key  (all three in one archive)"
if [ -n "$BK_USER" ]; then
  echo "       From your own computer, run:"
  echo "         scp $SUDO_USER@${PUBIP%:*}:$BK_USER ."
  echo "       then store it in your password manager / encrypted storage, and delete the copy in $UH:"
  echo "         rm $BK_USER"
else
  echo "       From your own computer, run:"
  echo "         scp root@${PUBIP%:*}:$BK ."
fi
echo "       archive sha256: $BKSHA"
echo "       Never run a second node with these keys while this one is running."
echo
echo "  → 3. Let it sync. Check progress (true = synced):"
echo "         curl -s -X POST -H content-type:application/json -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"info.isBootstrapped\",\"params\":{\"chain\":\"P\"}}' 127.0.0.1:9650/ext/info"
echo
echo "  → 4. Send this ONE line to the mission-control operator (all public values):"
echo
echo "       metal network=$XPR producer=$PRODUCER node_id=$NODEID bls=$BLSPUB pop=$BLSPOP"
echo
echo "  → 5. Later, to register as a validator (announced with the event) you will also need:"
echo "       • a Metal P-Chain address you control ($( [ "$METAL" = mainnet ] && echo 'P-metal1…' || echo 'P-tahoe1…')), from a key you already back up, not on this server"
echo "       • METAL on the P-Chain to prepay the validator's continuous fee"
echo "       This script never registers, stakes or spends anything."
echo
echo "  ── Good to know ────────────────────────────────────────────────────────────────────────────"
if [ -n "$CHAIN" ]; then echo "  Tracking PulseVM subnet $SUBNET (chain $CHAIN)."; else echo "  No PulseVM chain is published for XPR $XPR yet: this node syncs the Metal $METAL primary network for now."; fi
[ -n "$NOTE" ] && echo "  $NOTE"
echo "  Upgrade: re-run this same command (keys are kept) · Health: curl -s 127.0.0.1:9650/ext/health | head -c 300"
echo "  Logs: journalctl -u metalgo -f · Your beacon on mission control now shows \"Metal validator: running\"."
