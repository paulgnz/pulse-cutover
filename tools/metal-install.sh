#!/usr/bin/env bash
# metal-install.sh — prepare a Metal (metalgo) node next to your XPR nodeos and print its identity.
#
#   curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/metal-install.sh | sudo bash
#
# What it does:
#   + reads the signed-off network manifest from mission control (which Metal network, pinned metalgo version
#     + sha256, and the PulseVM subnet/chain IDs once they exist). No manifest, no install (fail closed).
#   + checks the box (RAM, disk, ports, glibc, an existing metalgo it did not install) and picks a method:
#       official binary (glibc >= 2.34) · Ubuntu 20.04 build of the same tag (glibc 2.31-2.33)
#       · official Docker image pinned by digest · build from source (--build, pinned commit + Go checksum)
#   + downloads and verifies EVERYTHING before touching a running node, then swaps atomically, starts it and
#     checks the runtime identity / network / version; on any failure it restores the previous binary+config
#   + runs metalgo as the `metalgo` systemd service, API on 127.0.0.1 only
#   + prints NodeID, BLS public key, proof of possession; backs up and VERIFIES the identity keys
#   - never stakes, funds, registers, or changes nodeos; never prints private keys; only opens a firewall port
#     when you pass --open-port
#
# The result is a *prepared* Metal node. Being a registered/funded validator, or ready for a cutover, are separate
# steps with their own evidence.
#
# Options:
#   --check                re-test port 9651 and print the LIVE identity (changes nothing)
#   --open-port            also add the 9651 rule to this server's own firewall (ufw / firewalld / iptables)
#   --metal tahoe|mainnet  which Metal network (default: from this box's XPR chain id)
#   --manifest-url URL     manifest to use (default: mission control)
#   --allow-unpinned       manifest unreachable: fall back to the pins built into this script (testnet only)
#   --method binary|compat|docker|build   force an install method;  --build = --method build
#   --adopt                take over a metalgo service this script did not install (only /var/lib/metalgo layout)
#   --dry-run              show what would happen; writes nothing
#   --uninstall            remove the service this script installed (keys and chain data are kept)
set -euo pipefail

CONTROL="https://control-rehearsal.protonnz.com"
DATA=/var/lib/metalgo; ETC=/etc/metalgo; BIN=/usr/local/bin/metalgo; UNIT=/etc/systemd/system/metalgo.service
MARKER=$ETC/.installed-by-pulse-cutover
MAINNET_CHAIN=384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0
TESTNET_CHAIN=71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd

G='\033[32m'; Y='\033[33m'; B='\033[1m'; N='\033[0m'
say() { printf '\033[1m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[!]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

# ---- arguments ---------------------------------------------------------------------------------------
parse_args() {
  METAL=""; MANIFEST_URL=""; METHOD=""; CHECK=0; OPEN_PORT=0; DRY=0; UNINSTALL=0; ADOPT=0; ALLOW_UNPINNED=0
  while [ $# -gt 0 ]; do case "$1" in
    --metal) METAL=${2:-}; shift 2;; --manifest-url) MANIFEST_URL=${2:-}; shift 2;;
    --build) [ -z "$METHOD" ] || [ "$METHOD" = build ] || die "conflicting flags: --build and --method $METHOD"; METHOD=build; shift;;
    --method) [ -z "$METHOD" ] || [ "$METHOD" = "${2:-}" ] || die "conflicting flags: --method given twice"; METHOD=${2:-}; shift 2;;
    --dry-run) DRY=1; shift;; --check) CHECK=1; shift;; --open-port) OPEN_PORT=1; shift;; --uninstall) UNINSTALL=1; shift;;
    --adopt) ADOPT=1; shift;; --allow-unpinned) ALLOW_UNPINNED=1; shift;;
    *) die "unknown option $1";; esac; done
  check_flags
}
check_flags() {
  case "$METAL" in ""|tahoe|mainnet) ;; *) die "--metal must be tahoe or mainnet (got '$METAL')";; esac
  case "$METHOD" in ""|binary|compat|docker|build) ;; *) die "--method must be binary, compat, docker or build (got '$METHOD')";; esac
  if [ "$CHECK" = 1 ] && [ "$DRY" = 1 ] && [ "$OPEN_PORT" = 1 ]; then die "conflicting flags: --check --open-port --dry-run (--open-port changes the firewall; --dry-run promises no changes)"; fi
  if [ "$DRY" = 1 ] && [ "$OPEN_PORT" = 1 ]; then die "conflicting flags: --open-port changes the firewall; --dry-run promises no changes"; fi
  if [ "$UNINSTALL" = 1 ] && { [ "$CHECK" = 1 ] || [ "$DRY" = 1 ] || [ "$OPEN_PORT" = 1 ] || [ -n "$METHOD" ]; }; then die "conflicting flags: --uninstall takes no other options"; fi
  if [ "$CHECK" = 1 ] && { [ -n "$METHOD" ] || [ "$ADOPT" = 1 ] || [ "$ALLOW_UNPINNED" = 1 ]; }; then die "conflicting flags: --check only re-tests; it does not install"; fi
  if [ "$ALLOW_UNPINNED" = 1 ] && [ "$METAL" = mainnet ]; then die "--allow-unpinned is refused on mainnet: mainnet installs only from the published manifest"; fi
  return 0
}

# ---- small JSON helpers (python3 is present on every supported Ubuntu) ---------------------------------
# jget PATH  < json   → prints the value at a dotted path ('' if missing/null). Never evals anything.
jget() { python3 -c 'import json,sys
p=sys.argv[1].split(".")
try: v=json.load(sys.stdin)
except Exception: sys.exit(0)
for k in p:
  if isinstance(v,dict) and k in v: v=v[k]
  else: sys.exit(0)
if v is None: sys.exit(0)
print(json.dumps(v) if isinstance(v,(dict,list)) else ("true" if v is True else "false" if v is False else v))' "$1"; }
info_rpc() { curl -fsS -m4 -X POST -H 'content-type:application/json' -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\"${2:+,\"params\":$2}}" http://127.0.0.1:9650/ext/info 2>/dev/null || true; }

# manifest_validate FILE XPR_NET EXPECTED_CHAIN_ID METAL
# Structural validation of the manifest. On success prints `KEY=value` lines (shell-quoted) for eval; on any
# problem prints the reason to stderr and returns 1.
manifest_validate() {
  python3 - "$@" <<'PY'
import json, re, shlex, sys
f, xpr, cid, metal = sys.argv[1:5]
def bad(m): print(m, file=sys.stderr); sys.exit(1)
try: m = json.load(open(f))
except Exception as e: bad(f"manifest is not valid JSON ({e.__class__.__name__})")
if not isinstance(m, dict): bad("manifest is not a JSON object")
NET = {"tahoe": 5, "mainnet": 1}
def s(k, req=True):
    v = m.get(k)
    if v is None:
        if req: bad(f"manifest field '{k}' is missing")
        return ""
    if not isinstance(v, str): bad(f"manifest field '{k}' must be a string")
    return v
hexre = lambda n: re.compile(r"^[0-9a-f]{%d}$" % n)
if s("xpr_network") != xpr: bad(f"manifest is for XPR {m.get('xpr_network')!r}, this box runs XPR {xpr!r}")
mcid = s("xpr_chain_id")
if not hexre(64).match(mcid): bad("manifest xpr_chain_id is not a 64-hex chain id")
if cid and mcid != cid: bad(f"manifest xpr_chain_id {mcid[:12]}… does not match this node's chain id {cid[:12]}…")
net = s("network")
if net not in NET: bad(f"manifest network {net!r} is not an installable Metal network (tahoe, mainnet)")
if metal and net != metal: bad(f"manifest is for Metal {net!r} but --metal {metal!r} was requested")
nid = m.get("network_id")
if nid != NET[net]: bad(f"manifest network_id {nid!r} does not match Metal {net!r} (expected {NET[net]})")
ver = s("metalgo_version")
if not re.match(r"^v\d+\.\d+\.\d+(-[a-z0-9.]+)?$", ver): bad(f"manifest metalgo_version {ver!r} is not a release tag")
out = {"MF_NETWORK": net, "MF_NETID": str(nid), "MF_VERSION": ver, "MF_CHAIN_ID": mcid}
for k, env in [("sha256_linux_amd64", "MF_SHA_AMD"), ("sha256_linux_arm64", "MF_SHA_ARM"), ("compat_glibc231_sha256", "MF_COMPAT_SHA")]:
    v = s(k, False)
    if v and not hexre(64).match(v): bad(f"manifest {k} is not a sha256")
    out[env] = v
cu = s("compat_glibc231_url", False)
if cu and not cu.startswith("https://"): bad("manifest compat_glibc231_url must be https")
if cu and not out["MF_COMPAT_SHA"]: bad("manifest has compat_glibc231_url without compat_glibc231_sha256")
out["MF_COMPAT_URL"] = cu
img, dig = s("docker_image", False), s("docker_image_digest", False)
if dig and not re.match(r"^sha256:[0-9a-f]{64}$", dig): bad("manifest docker_image_digest must be sha256:<64 hex>")
out["MF_IMAGE"] = img.split(":")[0] if img else "metalblockchain/metalgo"; out["MF_IMAGE_DIGEST"] = dig
commit = s("metalgo_commit", False)
if commit and not hexre(40).match(commit): bad("manifest metalgo_commit must be a 40-hex git commit")
out["MF_COMMIT"] = commit
proto = m.get("rpcchainvm_protocol")
out["MF_PROTOCOL"] = str(proto) if isinstance(proto, int) else ""
b58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{40,60}$")
for k, env in [("subnet_id", "MF_SUBNET"), ("blockchain_id", "MF_CHAIN"), ("vm_id", "MF_VM")]:
    v = s(k, False)
    if v and not b58.match(v): bad(f"manifest {k} is not a Metal (cb58) id")
    out[env] = v
out["MF_NOTE"] = s("upgrades_note", False)[:300]
for k, v in out.items(): print(f"{k}={shlex.quote(v)}")
PY
}

# Pins built into this script, used only with --allow-unpinned on testnet when the manifest is unreachable.
# They are still checksum-pinned; they just may be older than the published manifest.
builtin_manifest() {
  cat <<J
{"xpr_network":"testnet","xpr_chain_id":"$TESTNET_CHAIN","network":"tahoe","network_id":5,
 "metalgo_version":"v1.14.2-tahoe","rpcchainvm_protocol":45,
 "sha256_linux_amd64":"7732b814168d6c34bca209a4429ae73b3fb22629dd7e2f286e3d85ca35c90b2a",
 "sha256_linux_arm64":"99ff38889599e401c3fbcc8226e3fa92db20da27f959317ad1dfee83a99ed45b",
 "compat_glibc231_url":"https://github.com/paulgnz/pulse-cutover/releases/download/metalgo-glibc2.31-1/metalgo-linux-amd64-v1.14.2-tahoe-glibc2.31.tar.gz",
 "compat_glibc231_sha256":"59777f62ae12e79a90bbec7effe2a38db59992d98683f8add37e9f68b063fdb2"}
J
}

# version_matches "metalgo/1.14.2 [..]" v1.14.2-tahoe   → 0 when the numeric release matches
version_matches() { local got want; got=$(printf '%s' "$1" | sed -n 's|^metalgo/\([0-9][0-9.]*\).*|\1|p'); want=$(printf '%s' "$2" | sed -n 's|^v\([0-9][0-9.]*\).*|\1|p'); [ -n "$got" ] && [ "$got" = "$want" ]; }

# ---- port 9651: test from outside; if closed, give the LIKELY cause ----------------------------------
# Mission control dials back to the address it sees this request come from, on 9651 only. That is usually,
# but not always, the address metalgo advertises (NAT, IPv6 and multi-homed hosts differ). Local firewall
# inspection is a heuristic: it names the most likely layer, not a proof.
port_diag() {
  REACH=""
  for _ in 1 2; do   # one retry: mission control rate-limits to one probe per 5 s per IP
    REACH=$(curl -4 -fsS -m10 "$CONTROL/api/reach" 2>/dev/null | jget reachable || true)
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
      FIX="sudo ufw allow 9651/tcp   (or re-run this script with --open-port)"; return 0
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
    CAUSE="no local firewall rule found blocking 9651, so the likely cause is in front of this server: the $PROV firewall"
    FIX="allow inbound TCP 9651 from anywhere (0.0.0.0/0) to this server in $WHERE"
  else
    CAUSE="no local firewall rule found blocking 9651, so the likely cause is in front of this server: your hosting provider's firewall, NAT or network"
    FIX="ask your hosting provider (or use their control panel) to allow inbound TCP 9651 from anywhere to this server"
  fi
}

# --open-port: add the local rule for whichever firewall is active (never touches the provider's).
open_port_local() {
  if command -v ufw >/dev/null && ufw status 2>/dev/null | grep -q "Status: active"; then
    ufw allow 9651/tcp >/dev/null && say "ufw: allowed 9651/tcp"
  elif command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
    firewall-cmd --permanent --add-port=9651/tcp >/dev/null && firewall-cmd --reload >/dev/null && say "firewalld: allowed 9651/tcp"
  elif command -v nft >/dev/null && nft list ruleset 2>/dev/null | grep -qE "hook input .*policy drop"; then
    warn "nftables-only firewall: not changed automatically. Add 'tcp dport 9651 accept' to your input chain."
  elif command -v iptables >/dev/null; then
    iptables -C INPUT -p tcp --dport 9651 -j ACCEPT 2>/dev/null || iptables -I INPUT -p tcp --dport 9651 -j ACCEPT
    say "iptables: allowed 9651/tcp"
    if command -v netfilter-persistent >/dev/null; then netfilter-persistent save >/dev/null 2>&1 && say "iptables: rule saved for reboots"
    else warn "iptables rule is live but not saved for reboots: add it to your boot-time firewall rules"; fi
  fi
}

print_port() {
  if [ "$REACH" = true ]; then printf "  ${G}✓${N} 1. Staking port 9651 answered from the internet (tested on the address mission control sees). Nothing to do.\n"
  elif [ "$REACH" = false ]; then
    printf "  ${Y}✗${N} 1. ${B}Port 9651/tcp did not answer from the internet${N} (tested on the address mission control sees).\n"
    echo "       Other nodes can't connect in (you only have outbound peers)."
    echo "       Likely cause:  $CAUSE"
    echo "       Fix:           $FIX"
    [ -n "$FIX2" ] && echo "                      $FIX2"
    echo "       Then re-check (changes nothing):"
    echo "         curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/metal-install.sh | sudo bash -s -- --check"
  else printf "  ${Y}?${N} 1. Could not test port 9651 (mission control unreachable). Make sure inbound TCP 9651 is open.\n"; fi
}

# ---- live identity -----------------------------------------------------------------------------------
read_live_identity() {
  local id; id=$(info_rpc info.getNodeID)
  NODEID=$(printf '%s' "$id" | jget result.nodeID)
  BLSPUB=$(printf '%s' "$id" | jget result.nodePOP.publicKey)
  BLSPOP=$(printf '%s' "$id" | jget result.nodePOP.proofOfPossession)
}
identity_complete() {
  printf '%s' "$NODEID" | grep -qE '^NodeID-[1-9A-HJ-NP-Za-km-z]{20,}$' &&
  printf '%s' "$BLSPUB" | grep -qE '^0x[0-9a-f]{96}$' &&
  printf '%s' "$BLSPOP" | grep -qE '^0x[0-9a-f]{192}$'
}

# ---- ownership --------------------------------------------------------------------------------------
existing_unit() { [ -f "$UNIT" ] || systemctl cat metalgo.service >/dev/null 2>&1; }
# data-dir of an existing metalgo service (from its --data-dir flag or config file); '' if unknown
existing_data_dir() {
  local exec cfg dd; exec=$(systemctl show -p ExecStart --value metalgo 2>/dev/null || true)
  dd=$(printf '%s' "$exec" | grep -o -- '--data-dir[= ][^ ;]*' | head -1 | sed 's/--data-dir[= ]//' || true)
  if [ -z "$dd" ]; then
    cfg=$(printf '%s' "$exec" | grep -o -- '--config-file[= ][^ ;]*' | head -1 | sed 's/--config-file[= ]//' || true)
    [ -n "$cfg" ] && [ -f "$cfg" ] && dd=$(jget data-dir < "$cfg" || true)
  fi
  printf '%s' "${dd:-$HOME/.metalgo}"
}

uninstall() {
  if [ ! -f "$MARKER" ]; then
    existing_unit && die "the metalgo service here was not installed by this script ($MARKER missing): not removing it. Remove it with the tool that installed it."
    say "nothing to uninstall"; exit 0
  fi
  systemctl disable --now metalgo 2>/dev/null || true; rm -f "$UNIT"; systemctl daemon-reload
  rm -f "$MARKER"
  say "metalgo service removed. Your node identity and chain data are KEPT in $DATA (delete them yourself only after backing up $DATA/staking)."
}

do_check() {
  systemctl is-active --quiet metalgo || die "metalgo is not running here (install it first: run without --check)"
  [ "$OPEN_PORT" = 1 ] && open_port_local
  port_diag
  read_live_identity
  local ver net peers pb
  ver=$(info_rpc info.getNodeVersion | jget result.version); net=$(info_rpc info.getNetworkID | jget result.networkID)
  peers=$(info_rpc info.peers | jget result.numPeers); pb=$(info_rpc info.isBootstrapped '{"chain":"P"}' | jget result.isBootstrapped)
  echo
  echo "  Live identity (queried now from the running node):"
  echo "  NodeID               ${NODEID:-unknown}"
  echo "  BLS public key       ${BLSPUB:-unknown}"
  echo "  Proof of possession  ${BLSPOP:-unknown}"
  echo "  metalgo              ${ver:-?} · network-id ${net:-?} · peers ${peers:-?} · P-Chain synced: ${pb:-?}"
  identity_complete || warn "the node did not return a complete identity (NodeID + BLS key + PoP)"
  [ -f "$ETC/identity.txt" ] && echo "  (the record of the last install is in $ETC/identity.txt)"
  echo
  print_port
  echo
}

# ---- staging (no changes to the running node) --------------------------------------------------------
stage_artifact() {   # → $STAGE/metalgo (native methods) or pulls the pinned image (docker)
  local url sha
  case "$METHOD" in
    binary)
      sha=$([ "$A" = amd64 ] && echo "$MF_SHA_AMD" || echo "$MF_SHA_ARM")
      [ -n "$sha" ] || die "manifest has no sha256 for linux-$A: refusing to install an unpinned binary"
      url="https://github.com/MetalBlockchain/metalgo/releases/download/$MF_VERSION/metalgo-linux-$A-$MF_VERSION.tar.gz"
      curl -fsSL -o "$STAGE/mg.tgz" "$url" || die "download failed: $url"
      echo "$sha  $STAGE/mg.tgz" | sha256sum -c --quiet || die "checksum mismatch for $url: refusing to install"
      tar xzf "$STAGE/mg.tgz" -C "$STAGE"; install -m 755 "$STAGE"/metalgo-*/metalgo "$STAGE/metalgo";;
    compat)
      [ -n "$MF_COMPAT_URL" ] && [ -n "$MF_COMPAT_SHA" ] || die "manifest has no pinned Ubuntu 20.04 build"
      curl -fsSL -o "$STAGE/mg.tgz" "$MF_COMPAT_URL" || die "download failed: $MF_COMPAT_URL"
      echo "$MF_COMPAT_SHA  $STAGE/mg.tgz" | sha256sum -c --quiet || die "checksum mismatch for $MF_COMPAT_URL: refusing to install"
      tar xzf "$STAGE/mg.tgz" -C "$STAGE"; install -m 755 "$STAGE"/metalgo-*/metalgo "$STAGE/metalgo";;
    build) stage_build;;
    docker)
      [ -n "$MF_IMAGE_DIGEST" ] || die "the manifest pins no docker_image_digest: refusing a mutable image tag. Use another method."
      IMAGE_REF="$MF_IMAGE@$MF_IMAGE_DIGEST"
      docker pull -q "$IMAGE_REF" >/dev/null || die "docker pull failed: $IMAGE_REF"
      docker run --rm --entrypoint /metalgo/build/metalgo "$IMAGE_REF" --version > "$STAGE/version.txt" 2>/dev/null || die "the pinned image does not run"
      ;;
  esac
  if [ "$METHOD" != docker ]; then "$STAGE/metalgo" --version > "$STAGE/version.txt" 2>/dev/null || die "the staged metalgo binary does not run on this host"; fi
  version_matches "$(cat "$STAGE/version.txt")" "$MF_VERSION" || die "staged binary reports '$(head -1 "$STAGE/version.txt")', expected $MF_VERSION"
  if [ -n "$MF_PROTOCOL" ] && ! grep -q "rpcchainvm=$MF_PROTOCOL" "$STAGE/version.txt"; then die "staged binary does not speak plugin protocol $MF_PROTOCOL: $(head -1 "$STAGE/version.txt")"; fi
  say "staged and verified: $(head -1 "$STAGE/version.txt")"
}

stage_build() {
  say "building metalgo $MF_VERSION from source (nice 19; nodeos keeps priority)…"
  command -v git >/dev/null && command -v gcc >/dev/null || die "--build needs git and build-essential installed first (apt-get install git build-essential)"
  local commit gov gosha
  commit=$(git ls-remote https://github.com/MetalBlockchain/metalgo "refs/tags/$MF_VERSION^{}" "refs/tags/$MF_VERSION" | awk 'NR==1{print $1}')
  [ -n "$commit" ] || die "tag $MF_VERSION not found upstream"
  local deref; deref=$(git ls-remote https://github.com/MetalBlockchain/metalgo "refs/tags/$MF_VERSION^{}" | awk '{print $1}')
  [ -n "$deref" ] && commit=$deref
  if [ -n "$MF_COMMIT" ] && [ "$commit" != "$MF_COMMIT" ]; then die "tag $MF_VERSION resolves to $commit but the manifest pins $MF_COMMIT: refusing"; fi
  gov=$(curl -fsSL "https://raw.githubusercontent.com/MetalBlockchain/metalgo/$commit/go.mod" | sed -n "s/^go //p")
  [ -n "$gov" ] || die "could not read the Go version from go.mod at $commit"
  gosha=$(curl -fsSL "https://go.dev/dl/?mode=json&include=all" | python3 -c 'import json,sys
v,f=sys.argv[1],sys.argv[2]
for r in json.load(sys.stdin):
  for x in r.get("files",[]):
    if x.get("filename")==f: print(x["sha256"]); sys.exit()' "go$gov" "go$gov.linux-$A.tar.gz")
  [ -n "$gosha" ] || die "go.dev does not list a checksum for go$gov.linux-$A.tar.gz"
  curl -fsSL -o "$STAGE/go.tgz" "https://go.dev/dl/go$gov.linux-$A.tar.gz"
  echo "$gosha  $STAGE/go.tgz" | sha256sum -c --quiet || die "Go toolchain checksum mismatch: refusing"
  mkdir -p "$STAGE/go"; tar xzf "$STAGE/go.tgz" -C "$STAGE/go" --strip-components=1
  git clone -q https://github.com/MetalBlockchain/metalgo "$STAGE/src" && git -C "$STAGE/src" -c advice.detachedHead=false checkout -q "$commit"
  [ "$(git -C "$STAGE/src" rev-parse HEAD)" = "$commit" ] || die "source checkout is not at $commit"
  (cd "$STAGE/src" && PATH="$STAGE/go/bin:$PATH" GOFLAGS=-buildvcs=false CGO_LDFLAGS=-ldl nice -n 19 ionice -c3 ./scripts/build.sh >"$STAGE/build.log" 2>&1) || { tail -20 "$STAGE/build.log"; die "build failed"; }
  install -m 755 "$STAGE/src/build/metalgo" "$STAGE/metalgo"
  say "built from commit $commit with go$gov (sha256 $gosha)"
}

stage_config() {   # → $STAGE/config.json, $STAGE/metalgo.service
  # Advertise the box's IPv4 (auto-resolution can pick IPv6, which many peers can't reach). Override: METAL_PUBLIC_IP=…
  local pub4; pub4=${METAL_PUBLIC_IP:-$(curl -4 -fsS -m5 https://api.ipify.org 2>/dev/null || curl -4 -fsS -m5 https://ifconfig.me 2>/dev/null || true)}
  printf '%s' "$pub4" | grep -qE '^[0-9]{1,3}(\.[0-9]{1,3}){3}$' || pub4=""
  python3 - "$MF_NETID" "$DATA" "$pub4" "$MF_SUBNET" > "$STAGE/config.json" <<'PY'
import json, sys
nid, data, pub4, subnet = sys.argv[1:5]
c = {"network-id": nid, "data-dir": data, "db-dir": f"{data}/db", "log-dir": f"{data}/logs",
     "plugin-dir": f"{data}/plugins", "chain-config-dir": f"{data}/chains",
     "http-host": "127.0.0.1", "http-port": 9650, "staking-port": 9651}
if pub4: c["public-ip"] = pub4
else: c["public-ip-resolution-service"] = "opendns"
if subnet: c["track-subnets"] = subnet
print(json.dumps(c, indent=2))
PY
  local exec pre userline
  if [ "$METHOD" = docker ]; then
    exec="/usr/bin/docker run --rm --name metalgo --network host -v $DATA:$DATA -v $ETC:$ETC --user $(id -u metalgo 2>/dev/null || echo 999):$(id -g metalgo 2>/dev/null || echo 999) --entrypoint /metalgo/build/metalgo $IMAGE_REF --config-file=$ETC/config.json"
    pre="ExecStartPre=-/usr/bin/docker rm -f metalgo"; userline=""
  else exec="$BIN --config-file=$ETC/config.json"; pre=""; userline="User=metalgo"; fi
  cat > "$STAGE/metalgo.service" <<U
[Unit]
Description=Metal node (metalgo $MF_VERSION, $MF_NETWORK) — installed by pulse-cutover metal-install.sh
After=network-online.target
[Service]
$userline
$pre
ExecStart=$exec
Restart=always
RestartSec=5
LimitNOFILE=65536
Nice=5
[Install]
WantedBy=multi-user.target
U
}

# ---- swap + verify + rollback -------------------------------------------------------------------------
HAD_PREV=0
swap_in() {
  HAD_PREV=0
  if existing_unit || [ -f "$BIN" ]; then
    HAD_PREV=1
    [ -f "$BIN" ] && cp -p "$BIN" "$BIN.prev"
    [ -f "$ETC/config.json" ] && cp -p "$ETC/config.json" "$ETC/config.json.prev"
    [ -f "$UNIT" ] && cp -p "$UNIT" "$UNIT.prev"
  fi
  systemctl stop metalgo 2>/dev/null || true
  [ "$METHOD" = docker ] || install -m 755 "$STAGE/metalgo" "$BIN.new"
  [ "$METHOD" = docker ] || mv -f "$BIN.new" "$BIN"
  install -m 644 "$STAGE/config.json" "$ETC/config.json.new" && mv -f "$ETC/config.json.new" "$ETC/config.json"
  install -m 644 "$STAGE/metalgo.service" "$UNIT.new" && mv -f "$UNIT.new" "$UNIT"
  systemctl daemon-reload; systemctl enable metalgo >/dev/null 2>&1; systemctl start metalgo
}
rollback() {
  warn "$1"
  systemctl stop metalgo 2>/dev/null || true
  if [ "$HAD_PREV" = 1 ]; then
    [ -f "$BIN.prev" ] && mv -f "$BIN.prev" "$BIN"
    [ -f "$ETC/config.json.prev" ] && mv -f "$ETC/config.json.prev" "$ETC/config.json"
    [ -f "$UNIT.prev" ] && mv -f "$UNIT.prev" "$UNIT"
    systemctl daemon-reload; systemctl start metalgo 2>/dev/null || true
    die "upgrade failed and was rolled back: the previous metalgo is running again ($(systemctl is-active metalgo 2>/dev/null)). Nothing else changed. Details: journalctl -u metalgo -n 50"
  fi
  systemctl disable metalgo >/dev/null 2>&1 || true
  die "install failed: metalgo is stopped and disabled. Details: journalctl -u metalgo -n 50"
}
verify_runtime() {   # PREV_NODEID set when an identity existed before this run
  say "starting metalgo and verifying it…"
  NODEID=""
  for _ in $(seq 1 90); do read_live_identity; [ -n "$NODEID" ] && break; sleep 2; done
  [ -n "$NODEID" ] || rollback "metalgo did not answer within 3 minutes"
  local ver net
  ver=$(info_rpc info.getNodeVersion | jget result.version); net=$(info_rpc info.getNetworkID | jget result.networkID)
  version_matches "$ver" "$MF_VERSION" || rollback "running node reports '$ver', expected $MF_VERSION"
  [ "$net" = "$MF_NETID" ] || rollback "running node is on network-id '$net', expected $MF_NETID"
  if [ -n "$PREV_NODEID" ] && [ "$NODEID" != "$PREV_NODEID" ]; then rollback "NodeID changed ($PREV_NODEID → $NODEID): the staking identity was not preserved"; fi
  for _ in $(seq 1 20); do identity_complete && break; sleep 1; read_live_identity; done
  identity_complete || rollback "the node did not return a complete identity (NodeID + BLS public key + proof of possession)"
  rm -f "$BIN.prev.keep"; [ -f "$BIN.prev" ] && mv -f "$BIN.prev" "$BIN.prev.keep" 2>/dev/null || true
  RUN_VERSION=$ver
}

# ---- identity backup (verified) -----------------------------------------------------------------------
backup_identity() {
  local f ts tmpx; ts=$(date -u +%Y%m%dT%H%M%SZ)
  for f in staker.crt staker.key signer.key; do [ -s "$DATA/staking/$f" ] || die "identity file $DATA/staking/$f is missing: cannot back up a complete identity (metalgo keeps running; fix and re-run)"; done
  BK=/root/metalgo-identity-$NODEID-$ts.tar.gz
  (umask 077; tar czf "$BK" -C "$DATA" staking/staker.crt staking/staker.key staking/signer.key)
  tmpx=$(mktemp -d); tar xzf "$BK" -C "$tmpx"
  for f in staker.crt staker.key signer.key; do
    [ "$(sha256sum < "$DATA/staking/$f")" = "$(sha256sum < "$tmpx/staking/$f")" ] || { rm -rf "$tmpx"; die "backup verification failed for $f: $BK is not a faithful copy"; }
  done
  rm -rf "$tmpx"
  [ "$(tar tzf "$BK" | grep -c .)" = 3 ] || die "backup $BK does not contain exactly the three identity files"
  BKSHA=$(sha256sum "$BK" | cut -d' ' -f1)
  BK_USER=""; UH=""
  if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ]; then
    UH=$(getent passwd "$SUDO_USER" | cut -d: -f6)
    if [ -n "$UH" ] && [ -d "$UH" ]; then BK_USER="$UH/$(basename "$BK")"; install -m 600 -o "$SUDO_USER" "$BK" "$BK_USER"; fi
  fi
}

write_records() {
  PUBIP=$(info_rpc info.getNodeIP | jget result.ip)
  PRODUCER=$(sed -n '/^\[beacon\]/,/^\[/s/^producer *= *"\([a-z1-5.]*\)".*/\1/p' /etc/pulse-cutover/beacon.toml 2>/dev/null | head -1 || true)
  [ -n "$PRODUCER" ] || PRODUCER=youraccount
  cat > "$ETC/identity.txt" <<T
NodeID:              $NODEID
BLS public key:      $BLSPUB
Proof of possession: $BLSPOP
Metal network:       $MF_NETWORK (network-id $MF_NETID) · metalgo $MF_VERSION ($METHOD)
Recorded:            $(date -u +%Y-%m-%dT%H:%M:%SZ) (use --check for the live values)
Key backup (PRIVATE KEYS): $BK (sha256 $BKSHA)${BK_USER:+
Copy for $SUDO_USER (PRIVATE KEYS): $BK_USER}
T
  chmod 644 "$ETC/identity.txt"
  python3 - > "$ETC/identity.json" <<PY
import json
print(json.dumps({"schema": "metal-identity-v1", "xpr_network": "$XPR", "producer": "$PRODUCER",
  "metal_network": "$MF_NETWORK", "network_id": $MF_NETID, "metalgo_version": "$MF_VERSION",
  "runtime_version": "$RUN_VERSION", "install_method": "$METHOD", "node_id": "$NODEID",
  "bls_public_key": "$BLSPUB", "bls_proof_of_possession": "$BLSPOP", "staking_address": "${PUBIP:-}",
  "staking_port_reachable": {"true": True, "false": False}.get("${REACH:-}"),
  "key_backup": "$BK", "key_backup_sha256": "$BKSHA", "state": "metal-node-prepared"}, indent=1))
PY
  chmod 644 "$ETC/identity.json"
}

print_summary() {
  echo
  printf "  ${G}✓${N} ${B}Metal node prepared${N} ($METHOD, $RUN_VERSION, $MF_NETWORK). Not yet a registered or funded validator.\n"
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
  echo "       staker.key + staker.crt = your NodeID · signer.key = your BLS key  (verified copies of all three)"
  echo "       These files contain PRIVATE KEYS:"
  echo "         $BK   (root only)"
  [ -n "$BK_USER" ] && echo "         $BK_USER   (readable only by $SUDO_USER, so you can scp it)"
  echo "       From your own computer:"
  if [ -n "$BK_USER" ]; then echo "         scp $SUDO_USER@${PUBIP%:*}:$BK_USER ."; else echo "         scp root@${PUBIP%:*}:$BK ."; fi
  echo "       archive sha256: $BKSHA"
  [ -n "$BK_USER" ] && echo "       then store it in your password manager / encrypted storage and delete the copy: rm $BK_USER"
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
  echo "       • a Metal P-Chain address you control ($( [ "$MF_NETWORK" = mainnet ] && echo 'P-metal1…' || echo 'P-tahoe1…')), from a key you already back up, not on this server"
  echo "       • METAL on the P-Chain to prepay the validator's continuous fee"
  echo "       This script never registers, stakes or spends anything."
  echo
  echo "  ── Good to know ────────────────────────────────────────────────────────────────────────────"
  if [ -n "$MF_CHAIN" ]; then echo "  Subnet tracking configured for $MF_SUBNET (chain $MF_CHAIN). The PulseVM plugin and chain config are a separate step."
  else echo "  No PulseVM chain is published for XPR $XPR yet: this node syncs the Metal $MF_NETWORK primary network for now."; fi
  [ -n "$MF_NOTE" ] && echo "  $MF_NOTE"
  echo "  Upgrade: re-run this same command (keys are kept; failed upgrades roll back) · Health: curl -s 127.0.0.1:9650/ext/health | head -c 300"
  echo "  Previous binary kept as $BIN.prev.keep · previous config as $ETC/config.json.prev"
}

main() {
  parse_args "$@"
  [ "$(id -u)" = 0 ] || die "run as root (sudo)"
  command -v python3 >/dev/null || die "python3 is required (it ships with every supported Ubuntu)"
  if [ "$UNINSTALL" = 1 ]; then uninstall; exit 0; fi
  if [ "$CHECK" = 1 ]; then do_check; exit 0; fi

  # ---- which network: from the XPR chain this box runs; --metal must agree ----------------------------
  local cid=""; cid=$(curl -fsS -m5 -X POST http://127.0.0.1:8888/v1/chain/get_info -d '{}' 2>/dev/null | jget chain_id || true)
  case "$cid" in
    "$MAINNET_CHAIN") XPR=mainnet; [ -z "$METAL" ] || [ "$METAL" = mainnet ] || die "this box runs XPR mainnet but --metal $METAL was given"; METAL=mainnet;;
    "$TESTNET_CHAIN") XPR=testnet; [ -z "$METAL" ] || [ "$METAL" = tahoe ] || die "this box runs XPR testnet but --metal $METAL was given"; METAL=tahoe;;
    "") [ -n "$METAL" ] || die "could not read this box's XPR chain id: pass --metal tahoe (testnet) or --metal mainnet"
        XPR=$([ "$METAL" = mainnet ] && echo mainnet || echo testnet); warn "no local nodeos answered: trusting --metal $METAL";;
    *) die "this box's nodeos is on an unknown chain (${cid:0:12}…): only XPR mainnet and testnet are supported";;
  esac
  check_flags

  # ---- manifest: fetched, structurally validated, bound to this chain. Fail closed. -------------------
  TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
  [ -n "$MANIFEST_URL" ] || MANIFEST_URL="$CONTROL/api/manifest/$XPR"
  if curl -fsS -m10 -o "$TMP/manifest.json" "$MANIFEST_URL" 2>/dev/null; then say "manifest: $MANIFEST_URL"
  elif [ "$ALLOW_UNPINNED" = 1 ] && [ "$XPR" = testnet ]; then warn "manifest unreachable: using the checksum pins built into this script (--allow-unpinned)"; builtin_manifest > "$TMP/manifest.json"
  else die "could not fetch the network manifest from $MANIFEST_URL: refusing to install without it (testnet only: --allow-unpinned uses this script's built-in pins)"; fi
  local mv; mv=$(manifest_validate "$TMP/manifest.json" "$XPR" "$cid" "$METAL") || die "manifest rejected (see above): nothing was changed"
  eval "$mv"
  say "XPR $XPR → Metal $MF_NETWORK (network-id $MF_NETID) · metalgo $MF_VERSION${MF_PROTOCOL:+ · plugin protocol $MF_PROTOCOL}"

  # ---- preflight (read-only) ---------------------------------------------------------------------------
  local ram free glibc probe
  ARCH=$(uname -m); case "$ARCH" in x86_64) A=amd64;; aarch64|arm64) A=arm64;; *) die "unsupported arch $ARCH";; esac
  ram=$(awk '/MemTotal/{printf "%d", $2/1024/1024}' /proc/meminfo)
  probe=$DATA; [ -d "$probe" ] || probe=$(dirname "$DATA")
  free=$(df -Pk "$probe" | awk 'NR==2{printf "%d", $4/1024/1024}')
  glibc=$(ldd --version 2>/dev/null | head -1 | grep -o '[0-9]\+\.[0-9]\+$' || echo 0)
  say "box: ${ram} GB RAM · ${free} GB free for $DATA · glibc $glibc · $ARCH"
  [ "$ram" -ge 8 ] || warn "less than 8 GB RAM: metalgo next to nodeos will be tight"
  [ "$free" -ge 100 ] || warn "less than 100 GB free: the primary network (P/X/C chains) can outgrow this"

  # ---- ownership: only upgrade what this script installed, unless --adopt ---------------------------
  PREV_NODEID=""
  if existing_unit; then
    if [ ! -f "$MARKER" ]; then
      [ "$ADOPT" = 1 ] || die "a metalgo service already exists that this script did not install. Re-run with --adopt to take it over (supported only for the /var/lib/metalgo layout), or manage it with the tool that installed it."
      local dd; dd=$(existing_data_dir)
      [ "$dd" = "$DATA" ] || die "--adopt: the existing metalgo uses data dir '$dd', not $DATA. Adopting a custom layout is not supported (it would risk the node identity)."
      say "adopting the existing metalgo service (data dir $DATA)"
    else say "metalgo installed by this script found: upgrading in place (identity kept, rollback on failure)"; fi
    if systemctl is-active --quiet metalgo; then read_live_identity; PREV_NODEID=$NODEID; fi
  else
    for p in 9650 9651; do ss -ltn "sport = :$p" 2>/dev/null | grep -q LISTEN && die "port $p is already in use by something that is not a metalgo service this script knows about"; done
  fi

  if [ -z "$METHOD" ]; then
    if awk "BEGIN{exit !($glibc >= 2.34)}"; then METHOD=binary
    elif [ "$A" = amd64 ] && [ -n "$MF_COMPAT_URL" ] && awk "BEGIN{exit !($glibc >= 2.31)}"; then METHOD=compat
    elif [ -n "$MF_IMAGE_DIGEST" ] && command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then METHOD=docker
    else
      die "this OS has glibc $glibc; metalgo needs glibc 2.34+, i.e. Ubuntu 22.04 or newer.
    Recommended: upgrade this server to Ubuntu 22.04/24.04 (20.04 is out of standard support), then re-run.
    Stopgap: re-run with --build (compiles the pinned metalgo commit here at lowest CPU priority, ~10 min; best effort)"
    fi
  fi
  say "install method: $METHOD"
  if [ "$DRY" = 1 ]; then say "dry run: nothing downloaded, installed or changed"; exit 0; fi

  # ---- stage + verify everything before touching the running node --------------------------------------
  STAGE="$TMP/stage"; mkdir -p "$STAGE"; IMAGE_REF=""
  stage_artifact
  stage_config

  # ---- apply ---------------------------------------------------------------------------------------------
  id metalgo >/dev/null 2>&1 || useradd --system --home "$DATA" --shell /usr/sbin/nologin metalgo
  mkdir -p "$ETC"
  if [ ! -d "$DATA" ]; then mkdir -p "$DATA"/{db,logs,plugins,chains}; chown -R metalgo:metalgo "$DATA"
  else
    mkdir -p "$DATA"/{db,logs,plugins,chains}
    # Only this script's own data dir, and only when its owner is wrong (adoption of a root-run install).
    [ "$(stat -c %U "$DATA")" = metalgo ] || { say "handing $DATA to the metalgo user"; chown -R metalgo:metalgo "$DATA"; }
  fi
  swap_in
  verify_runtime
  printf 'installed-by=pulse-cutover metal-install.sh\nversion=%s\nmethod=%s\nnetwork=%s\n' "$MF_VERSION" "$METHOD" "$MF_NETWORK" > "$MARKER"
  backup_identity

  # ---- port, records, summary -------------------------------------------------------------------------
  sleep 3
  [ "$OPEN_PORT" = 1 ] && open_port_local
  port_diag
  write_records
  print_summary
}

# Tests source this file with METAL_INSTALL_SOURCED=1 to exercise the functions without running anything.
[ "${METAL_INSTALL_SOURCED:-0}" = 1 ] || main "$@"
