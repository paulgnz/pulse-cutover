#!/usr/bin/env bash
# tools/test/run.sh — unit tests for the installers' pure functions (no root, no network, nothing installed).
#   bash tools/test/run.sh
set -uo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  FAIL %s\n' "$1"; }
# expect_die "name" cmd...  → passes when cmd exits non-zero (die) in a subshell
expect_die() { local n=$1; shift; if ( "$@" ) >/dev/null 2>&1; then bad "$n (expected refusal)"; else ok "$n"; fi; }
expect_ok()  { local n=$1; shift; if ( "$@" ) >/dev/null 2>&1; then ok "$n"; else bad "$n (expected success)"; fi; }
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
TESTNET=71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd
rep() { printf "%${2}s" | tr ' ' "$1"; }

echo "metal-install.sh"
METAL_INSTALL_SOURCED=1 source "$HERE/metal-install.sh"
set +e
expect_die "flags: --check --open-port --dry-run conflict" parse_args --check --open-port --dry-run
expect_die "flags: --open-port --dry-run conflict"          parse_args --open-port --dry-run
expect_die "flags: --build with --method binary"            parse_args --method binary --build
expect_die "flags: --uninstall with --dry-run"              parse_args --uninstall --dry-run
expect_die "flags: --metal must be tahoe|mainnet"           parse_args --metal fuji
expect_die "flags: --allow-unpinned refused on mainnet"     parse_args --metal mainnet --allow-unpinned
expect_die "flags: unknown option"                          parse_args --yolo
expect_ok  "flags: --check alone"                           parse_args --check
expect_ok  "flags: --dry-run alone"                         parse_args --dry-run

good() { cat <<J
{"xpr_network":"testnet","xpr_chain_id":"$TESTNET","network":"tahoe","network_id":5,"metalgo_version":"v1.14.2-tahoe",
 "rpcchainvm_protocol":45,"sha256_linux_amd64":"$(rep a 64)","docker_image":"metalblockchain/metalgo:v1.14.2-tahoe",
 "compat_glibc231_url":"https://example.org/x.tgz","compat_glibc231_sha256":"$(rep b 64)","subnet_id":null,"blockchain_id":null}
J
}
good > "$T/m.json"
expect_ok  "manifest: valid testnet manifest accepted"   manifest_validate "$T/m.json" testnet "$TESTNET" tahoe
out=$(manifest_validate "$T/m.json" testnet "$TESTNET" ""); eval "$out"
if [ "$MF_NETID" = 5 ] && [ "$MF_VERSION" = v1.14.2-tahoe ] && [ "$MF_PROTOCOL" = 45 ] && [ "$MF_IMAGE" = metalblockchain/metalgo ]; then ok "manifest: fields exported"; else bad "manifest: fields exported"; fi
expect_die "manifest: wrong chain id rejected"            manifest_validate "$T/m.json" testnet "$(rep c 64)" tahoe
expect_die "manifest: wrong XPR network rejected"         manifest_validate "$T/m.json" mainnet "" ""
expect_die "manifest: --metal mismatch rejected"          manifest_validate "$T/m.json" testnet "$TESTNET" mainnet
good | sed 's/"network_id":5/"network_id":88888/' > "$T/b1.json"; expect_die "manifest: network_id mismatch rejected" manifest_validate "$T/b1.json" testnet "$TESTNET" ""
good | sed 's/"network":"tahoe"/"network":"rehearsal"/' > "$T/b2.json"; expect_die "manifest: non-installable network rejected" manifest_validate "$T/b2.json" testnet "$TESTNET" ""
good | sed 's/"sha256_linux_amd64":"a*"/"sha256_linux_amd64":"nothex"/' > "$T/b3.json"; expect_die "manifest: bad sha256 rejected" manifest_validate "$T/b3.json" testnet "$TESTNET" ""
good | sed 's|https://example.org|http://example.org|' > "$T/b4.json"; expect_die "manifest: http compat url rejected" manifest_validate "$T/b4.json" testnet "$TESTNET" ""
good | sed 's/"metalgo_version":"v1.14.2-tahoe"/"metalgo_version":"latest; rm -rf x"/' > "$T/b5.json"; expect_die "manifest: non-tag version rejected" manifest_validate "$T/b5.json" testnet "$TESTNET" ""
echo '<html>nope</html>' > "$T/b6.json"; expect_die "manifest: HTML rejected" manifest_validate "$T/b6.json" testnet "$TESTNET" ""
good | sed 's/"docker_image"/"docker_image_digest":"latest","docker_image"/' > "$T/b7.json"; expect_die "manifest: mutable docker digest rejected" manifest_validate "$T/b7.json" testnet "$TESTNET" ""
builtin_manifest > "$T/bi.json"; expect_ok "manifest: built-in testnet pins validate" manifest_validate "$T/bi.json" testnet "$TESTNET" tahoe
expect_ok  "version: metalgo/1.14.2 matches v1.14.2-tahoe"  version_matches "metalgo/1.14.2 [database=v1.4.5, rpcchainvm=45]" v1.14.2-tahoe
expect_die "version: metalgo/1.13.5 differs from v1.14.2-tahoe" version_matches "metalgo/1.13.5 [x]" v1.14.2-tahoe
NODEID=NodeID-B7Af7jpSQN1Wq7iz7sKhyCmvKLVeMkucC; BLSPUB=0x$(rep a 96); BLSPOP=0x$(rep b 192)
expect_ok  "identity: complete identity accepted" identity_complete
BLSPOP=""; expect_die "identity: missing PoP rejected" identity_complete
if echo '{"result":{"nodeID":"NodeID-x","nodePOP":{"publicKey":"0xab"}}}' | jget result.nodePOP.publicKey | grep -qx 0xab; then ok "jget: nested path"; else bad "jget: nested path"; fi
if echo 'not json' | jget a.b | grep -q .; then bad "jget: garbage yields empty"; else ok "jget: garbage yields empty"; fi

# --- identity: NodeID derivation from the certificate (restore test) ---------------------------------
FIX="$HERE/test/fixtures/staker-NodeID-2orXC7XT9GsKqjJ8tn4PUdt6qz18a2VRz.crt"
if [ "$(node_id_from_cert "$FIX")" = NodeID-2orXC7XT9GsKqjJ8tn4PUdt6qz18a2VRz ]; then ok "restore: certificate derives its NodeID"; else bad "restore: certificate derives its NodeID"; fi
echo "not a cert" > "$T/bad.crt"; expect_die "restore: garbage certificate refused" node_id_from_cert "$T/bad.crt"

# --- adoption: identity must stay in DATA/staking at the default names ------------------------------
echo '{"data-dir":"/var/lib/metalgo"}' > "$T/c1.json"
expect_ok  "adopt: default key paths accepted"               identity_paths_ok /var/lib/metalgo "$T/c1.json" "/usr/bin/metalgo --config-file=$T/c1.json"
echo '{"staking-tls-cert-file":"/var/lib/metalgo/staking/staker.crt"}' > "$T/c2.json"
expect_ok  "adopt: explicit default path accepted"           identity_paths_ok /var/lib/metalgo "$T/c2.json" ""
echo '{"staking-signer-key-file":"/root/keys/signer.key"}' > "$T/c3.json"
expect_die "adopt: signer key outside data dir refused"      identity_paths_ok /var/lib/metalgo "$T/c3.json" ""
expect_die "adopt: cert path flag outside data dir refused"  identity_paths_ok /var/lib/metalgo "" "/usr/bin/metalgo --staking-tls-cert-file=/etc/x.crt"
echo '{"staking-ephemeral-signer-enabled":true}' > "$T/c4.json"
expect_die "adopt: ephemeral signer refused"                 identity_paths_ok /var/lib/metalgo "$T/c4.json" ""
echo '{"staking-tls-key-file-content":"abc"}' > "$T/c5.json"
expect_die "adopt: inline key content refused"               identity_paths_ok /var/lib/metalgo "$T/c5.json" ""

# --- round 3 (#8): adoption must catch ephemeral identity in every form metalgo accepts ------------------
# pflag bools: a bare flag means true (and does NOT consume the next word); =v parses like strconv.ParseBool.
EX='/usr/bin/metalgo --config-file=/etc/metalgo/config.json'
expect_die "adopt: bare --staking-ephemeral-signer-enabled before another flag refused" identity_paths_ok /var/lib/metalgo "" "$EX --staking-ephemeral-signer-enabled --staking-port=9651"
expect_die "adopt: bare --staking-ephemeral-cert-enabled at the end refused"          identity_paths_ok /var/lib/metalgo "" "$EX --staking-ephemeral-cert-enabled"
expect_die "adopt: bare bool followed by a word is still true (pflag)"                 identity_paths_ok /var/lib/metalgo "" "$EX --staking-ephemeral-signer-enabled false"
for v in t T TRUE True 1; do expect_die "adopt: --staking-ephemeral-signer-enabled=$v refused" identity_paths_ok /var/lib/metalgo "" "$EX --staking-ephemeral-signer-enabled=$v"; done
expect_ok  "adopt: --staking-ephemeral-signer-enabled=false accepted"                  identity_paths_ok /var/lib/metalgo "" "$EX --staking-ephemeral-signer-enabled=false"
expect_die "adopt: systemctl-show ExecStart format parsed"                             identity_paths_ok /var/lib/metalgo "" "{ path=/usr/bin/metalgo ; argv[]=/usr/bin/metalgo --staking-ephemeral-cert-enabled --http-host=127.0.0.1 ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }"
echo '{"staking-ephemeral-signer-enabled":"t"}' > "$T/c6.json"
expect_die "adopt: config JSON string \"t\" refused"                                    identity_paths_ok /var/lib/metalgo "$T/c6.json" ""
echo '{"staking-ephemeral-signer-enabled":false}' > "$T/c7.json"
expect_ok  "adopt: config JSON false accepted"                                         identity_paths_ok /var/lib/metalgo "$T/c7.json" ""
expect_die "adopt: env AVAGO_STAKING_EPHEMERAL_SIGNER_ENABLED=true refused"            identity_paths_ok /var/lib/metalgo "" "$EX" "AVAGO_STAKING_EPHEMERAL_SIGNER_ENABLED=true"
expect_die "adopt: env AVAGO_STAKING_SIGNER_KEY_FILE elsewhere refused"               identity_paths_ok /var/lib/metalgo "" "$EX" "HOME=/root AVAGO_STAKING_SIGNER_KEY_FILE=/etc/signer.key"
expect_ok  "adopt: unrelated env accepted"                                             identity_paths_ok /var/lib/metalgo "" "$EX" "AVAGO_HTTP_HOST=127.0.0.1"

# --- glibc detection must be a single clean version under pipefail (it used to become "2.39\n0") ---------
(
  set -euo pipefail
  getconf() { return 1; }
  ldd() { echo "ldd (Ubuntu GLIBC 2.39-0ubuntu8.4) 2.39"; for i in $(seq 1 3000); do echo "Copyright line $i"; done; }
  v=$(glibc_version); [ "$v" = 2.39 ]
) && ok "glibc: ldd fallback yields exactly 2.39 under pipefail" || bad "glibc: ldd fallback under pipefail"
( set -euo pipefail; getconf() { echo "glibc 2.31"; }; [ "$(glibc_version)" = 2.31 ] ) && ok "glibc: getconf path" || bad "glibc: getconf path"
( set -euo pipefail; getconf() { return 1; }; ldd() { return 1; }; [ "$(glibc_version)" = 0 ] ) && ok "glibc: unknown → 0" || bad "glibc: unknown → 0"

# --- --build needs a pinned commit ---------------------------------------------------------------------
MF_COMMIT=""; expect_die "build: refused without metalgo_commit" require_build_commit
MF_COMMIT=$(rep a 40); expect_ok "build: pinned commit accepted" require_build_commit

# --- upgrade transaction: failure right after the stop restores everything --------------------------
(
  R="$T/mi"; mkdir -p "$R/etc" "$R/bin" "$R/stage"
  BIN="$R/bin/metalgo"; ETC="$R/etc"; UNIT="$R/etc/metalgo.service"; STAGE="$R/stage"; TMP="$R/tmp"; mkdir -p "$TMP"
  echo old-bin > "$BIN"; echo old-cfg > "$ETC/config.json"; echo old-unit > "$UNIT"
  echo new-bin > "$STAGE/metalgo"; echo new-cfg > "$STAGE/config.json"; echo new-unit > "$STAGE/metalgo.service"
  systemctl() { echo "systemctl $*" >> "$R/sys.log"; [ "$1" = is-active ] && return 3; return 0; }
  existing_unit() { return 0; }
  read_live_identity() { NODEID=NodeID-OLD; BLSPUB=0xold; BLSPOP=""; }
  sleep() { :; }
  METHOD=binary; PREV_NODEID=NodeID-OLD; PREV_BLS=0xold
  trap on_exit EXIT
  FAULT_AFTER_STOP=false swap_in
) > "$T/mi.out" 2>&1; rc=$?
R="$T/mi"
if [ $rc -ne 0 ] && [ "$(cat "$R/bin/metalgo")" = old-bin ] && [ "$(cat "$R/etc/config.json")" = old-cfg ] && [ "$(cat "$R/etc/metalgo.service")" = old-unit ] \
   && grep -q "systemctl start metalgo" "$R/sys.log" && grep -q "same NodeID and BLS key" "$T/mi.out"; then ok "upgrade: failure after stop rolls back binary, config, unit and restarts the old node"
else bad "upgrade: failure after stop rolls back (rc=$rc; $(tr '\n' ' ' < "$T/mi.out" | head -c 300))"; fi
(
  R="$T/mi2"; mkdir -p "$R/etc" "$R/bin" "$R/stage"
  BIN="$R/bin/metalgo"; ETC="$R/etc"; UNIT="$R/etc/metalgo.service"; STAGE="$R/stage"; TMP="$R/tmp"; mkdir -p "$TMP"
  echo old-bin > "$BIN"; echo old-cfg > "$ETC/config.json"; echo old-unit > "$UNIT"
  echo new-bin > "$STAGE/metalgo"; echo new-cfg > "$STAGE/config.json"; echo new-unit > "$STAGE/metalgo.service"
  systemctl() { echo "systemctl $*" >> "$R/sys.log"; [ "$1" = is-active ] && return 3; return 0; }
  existing_unit() { return 0; }
  read_live_identity() { NODEID=NodeID-SOMETHINGELSE; BLSPUB=0xnew; }
  sleep() { :; }
  METHOD=binary; PREV_NODEID=NodeID-OLD; PREV_BLS=0xold
  trap on_exit EXIT
  FAULT_AFTER_STOP=false swap_in
) > "$T/mi2.out" 2>&1
if grep -q "ROLLBACK INCOMPLETE" "$T/mi2.out"; then ok "upgrade: a rollback that comes back with the wrong NodeID is reported, not hidden"; else bad "upgrade: wrong-NodeID rollback reported ($(tr '\n' ' ' < "$T/mi2.out" | head -c 200))"; fi

# --- round 3 (#11/#6): the transaction under production errexit ---------------------------------------
# swap_env: a fake installation in $1 with systemctl logged; callers override one command to fail.
swap_env() {
  R="$1"; mkdir -p "$R/etc" "$R/bin" "$R/stage" "$R/tmp"
  BIN="$R/bin/metalgo"; ETC="$R/etc"; UNIT="$R/etc/metalgo.service"; STAGE="$R/stage"; TMP="$R/tmp"
  echo old-bin > "$BIN"; echo old-cfg > "$ETC/config.json"; echo old-unit > "$UNIT"
  echo new-bin > "$STAGE/metalgo"; echo new-cfg > "$STAGE/config.json"; echo new-unit > "$STAGE/metalgo.service"
  systemctl() { echo "systemctl $*" >> "$R/sys.log"; [ "$1" = is-active ] && return 3; return 0; }
  existing_unit() { return 0; }
  read_live_identity() { NODEID=NodeID-OLD; BLSPUB=0xold; BLSPOP=""; }
  sleep() { :; }
  METHOD=binary; PREV_NODEID=NodeID-OLD; PREV_BLS=0xold; APPLYING=0; ROLLED=0; HAD_PREV=0
}
# (a) the first command after the stop fails (install), with set -euo pipefail exactly like production
(
  set -euo pipefail
  swap_env "$T/r3a"
  install() { echo "install $*" >> "$R/sys.log"; return 1; }
  trap on_exit EXIT
  swap_in
  echo "REACHED-AFTER-SWAP"
) > "$T/r3a.out" 2>&1; rc=$?
R="$T/r3a"
if [ $rc -ne 0 ] && ! grep -q REACHED-AFTER-SWAP "$T/r3a.out" && [ "$(cat "$R/bin/metalgo")" = old-bin ] && [ "$(cat "$R/etc/config.json")" = old-cfg ] \
   && [ "$(cat "$R/etc/metalgo.service")" = old-unit ] && [ "$(sed -n 1p "$R/sys.log")" = "systemctl stop metalgo" ] && [ "$(grep -v '^systemctl is-active' "$R/sys.log" | sed -n 2p | cut -d' ' -f1)" = install ] \
   && grep -q "systemctl start metalgo" "$R/sys.log" && grep -q "same NodeID and BLS key" "$T/r3a.out" && ! grep -q "(exit 0)" "$T/r3a.out" && [ ! -e "$R/bin/metalgo.new" ]
then ok "txn (set -e): install failing right after the stop rolls back and restarts the old node"
else bad "txn (set -e): install failing right after stop (rc=$rc; log: $(tr '\n' '|' < "$R/sys.log" 2>/dev/null); out: $(tr '\n' ' ' < "$T/r3a.out" | head -c 300))"; fi
# (b) a rollback step itself failing (mv of the saved binary) must not abort the rest of the rollback and must be reported
(
  set -euo pipefail
  swap_env "$T/r3b"
  install() { return 1; }
  mv() { case "$*" in *metalgo.prev*) return 1;; *) command mv "$@";; esac; }
  trap on_exit EXIT
  swap_in
) > "$T/r3b.out" 2>&1; rc=$?
if [ $rc -ne 0 ] && grep -q "systemctl start metalgo" "$T/r3b/sys.log" && grep -q "ROLLBACK INCOMPLETE" "$T/r3b.out"; then ok "txn (set -e): a failing rollback step is reported as ROLLBACK INCOMPLETE and the restart still runs"
else bad "txn (set -e): failing rollback step (rc=$rc; out: $(tr '\n' ' ' < "$T/r3b.out" | head -c 300))"; fi
# (c) stop fails → abort BEFORE swapping, nothing replaced, nothing rolled back
(
  set -euo pipefail
  swap_env "$T/r3c"
  systemctl() { echo "systemctl $*" >> "$R/sys.log"; [ "$1" = stop ] && return 1; return 0; }
  trap on_exit EXIT
  swap_in
  echo "REACHED-AFTER-SWAP"
) > "$T/r3c.out" 2>&1; rc=$?
if [ $rc -ne 0 ] && ! grep -q REACHED-AFTER-SWAP "$T/r3c.out" && [ "$(cat "$T/r3c/bin/metalgo")" = old-bin ] && ! grep -q "systemctl start" "$T/r3c/sys.log" \
   && grep -qi "could not stop" "$T/r3c.out"; then ok "txn (set -e): a failed stop aborts before anything is replaced"
else bad "txn (set -e): failed stop (rc=$rc; log: $(tr '\n' '|' < "$T/r3c/sys.log"); out: $(tr '\n' ' ' < "$T/r3c.out" | head -c 300))"; fi
# (d) stop "succeeds" but the unit is still active → same
(
  set -euo pipefail
  swap_env "$T/r3d"
  systemctl() { echo "systemctl $*" >> "$R/sys.log"; case "$1" in is-active) return 0;; esac; return 0; }
  trap on_exit EXIT
  swap_in
  echo "REACHED-AFTER-SWAP"
) > "$T/r3d.out" 2>&1; rc=$?
if [ $rc -ne 0 ] && ! grep -q REACHED-AFTER-SWAP "$T/r3d.out" && [ "$(cat "$T/r3d/bin/metalgo")" = old-bin ]; then ok "txn (set -e): a unit still active after stop aborts before anything is replaced"
else bad "txn (set -e): still-active after stop (rc=$rc; out: $(tr '\n' ' ' < "$T/r3d.out" | head -c 300))"; fi
# (e) the script ending (exit 0) while armed is an interruption, reported without a misleading "exit 0"
(
  set -euo pipefail
  swap_env "$T/r3e"
  trap on_exit EXIT
  swap_in
  exit 0
) > "$T/r3e.out" 2>&1; rc=$?
if [ $rc -ne 0 ] && ! grep -q "(exit 0)" "$T/r3e.out" && grep -q "before the upgrade was verified" "$T/r3e.out"; then ok "txn: ending while armed is reported as an unverified upgrade, not 'unexpected failure (exit 0)'"
else bad "txn: ending while armed (rc=$rc; out: $(tr '\n' ' ' < "$T/r3e.out" | head -c 300))"; fi
# (f) rollback that comes back with the wrong identity exits non-zero
(
  set -euo pipefail
  swap_env "$T/r3f"
  install() { return 1; }
  read_live_identity() { NODEID=NodeID-OTHER; BLSPUB=0xnew; }
  trap on_exit EXIT
  swap_in
) > "$T/r3f.out" 2>&1; rc=$?
if [ $rc -ne 0 ] && grep -q "ROLLBACK INCOMPLETE" "$T/r3f.out"; then ok "txn (set -e): wrong identity after rollback → ROLLBACK INCOMPLETE and non-zero exit"
else bad "txn (set -e): wrong identity after rollback (rc=$rc)"; fi

echo "beacon-install.sh"
BEACON_INSTALL_SOURCED=1 source "$HERE/beacon-install.sh"; set +e
expect_die "beacon: http URL refused"            check_url http://control.example.org
expect_ok  "beacon: https URL accepted"          check_url https://control.example.org
expect_ok  "beacon: http://127.0.0.1 accepted"   check_url http://127.0.0.1:8787
expect_die "beacon: http://localhost.example.org refused (not localhost)"   check_url http://localhost.example.org
expect_die "beacon: http://localhost:80@example.org refused (userinfo)"     check_url http://localhost:80@example.org
expect_die "beacon: https with userinfo refused"                            check_url https://user:pw@control.example.org
expect_ok  "beacon: http://localhost:8787 accepted"                         check_url http://localhost:8787
expect_ok  "beacon: http://[::1]:8787 accepted"                             check_url 'http://[::1]:8787'
expect_die "beacon: ftp refused"                                            check_url ftp://control.example.org

# --- round 3 (#7): the beacon unit is sandboxed the same way whether it runs as its own user or as root ----
for who in pulse-beacon root; do
  U=$(beacon_unit "$who" /usr/local/bin/pulse-cutover /etc/pulse-cutover /var/lib/pulse-beacon)
  for want in "NoNewPrivileges=yes" "ProtectSystem=strict" "ReadWritePaths=/var/lib/pulse-beacon" "RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX" \
              "ProtectKernelTunables=yes" "ProtectKernelModules=yes" "ProtectControlGroups=yes" "RestrictNamespaces=yes" "LockPersonality=yes" "SystemCallFilter=@system-service"; do
    printf '%s\n' "$U" | grep -qx "$want" || bad "beacon unit ($who): missing $want"
  done
  if [ "$who" = root ]; then printf '%s\n' "$U" | grep -qx "CapabilityBoundingSet=CAP_DAC_READ_SEARCH" && printf '%s\n' "$U" | grep -qx "AmbientCapabilities=" && ok "beacon unit (root fallback): only CAP_DAC_READ_SEARCH, hardened like the user unit" || bad "beacon unit (root fallback) capabilities"
  else printf '%s\n' "$U" | grep -qx "User=pulse-beacon" && ok "beacon unit (pulse-beacon): unprivileged and hardened" || bad "beacon unit (pulse-beacon) user"; fi
done

# --- update transaction: restore puts back binary, config, instance id, unit and the ENROLLED token ------
(
  R="$T/bi"; mkdir -p "$R/etc" "$R/bin"
  ETC="$R/etc"; BIN="$R/bin/pulse-cutover"; UNIT="$R/etc/pulse-beacon.service"
  echo old-bin > "$BIN.prev"; echo new-bin > "$BIN"
  echo old-cfg > "$ETC/beacon.toml.prev"; echo new-cfg > "$ETC/beacon.toml"
  echo old-unit > "$UNIT.prev"; echo new-unit > "$UNIT"
  echo 0123456789abcdef0123456789abcdef > "$ETC/beacon.instance.prev"; echo ffffffffffffffffffffffffffffffff > "$ETC/beacon.instance"
  echo enrolled-token > "$ETC/beacon.token.retired-x"; echo brand-new-token > "$ETC/beacon.token"
  RETIRED="$ETC/beacon.token.retired-x"; OLD_FOUND=1; APPLYING=1
  systemctl() { echo "systemctl $*" >> "$R/sys.log"; case "$1" in is-active) echo active;; esac; return 0; }
  sleep() { :; }
  trap on_exit EXIT
  false
) > "$T/bi.out" 2>&1; rc=$?
R="$T/bi"
if [ $rc -ne 0 ] && [ "$(cat "$R/bin/pulse-cutover")" = old-bin ] && [ "$(cat "$R/etc/beacon.toml")" = old-cfg ] && [ "$(cat "$R/etc/pulse-beacon.service")" = old-unit ] \
   && [ "$(cat "$R/etc/beacon.token")" = enrolled-token ] && [ "$(cat "$R/etc/beacon.instance")" = 0123456789abcdef0123456789abcdef ] && grep -q "systemctl restart pulse-beacon" "$R/sys.log"; then
  ok "beacon update: failure restores binary, config, unit, instance id and the enrolled token"
else bad "beacon update: failure restores everything (rc=$rc; $(tr '\n' ' ' < "$T/bi.out" | head -c 300))"; fi
mkdir -p "$T/ea/x"; if [ "$(existing_ancestor "$T/ea/x/y/z")" = "$T/ea/x" ] && [ "$(existing_ancestor "$T/ea/x")" = "$T/ea/x" ]; then ok "beacon: free-space probe uses the nearest existing dir"; else bad "beacon: existing_ancestor"; fi
expect_die "beacon: producer name validated"     check_producer "Bad.Name!"
expect_ok  "beacon: producer name ok"            check_producer protonnz
printf '[ceremony]\nmode = "api"\n[source]\nrpc_url = "http://127.0.0.1:8889"\nproducer_api_url = "http://127.0.0.1:8890"\n[snapshot]\ndir = "/data/snap"\n[beacon]\nurl = "https://x.example/api/report"\nproducer = "protonnz"\nnetwork = "testnet"\nnode = "hyperion-testnet"\nrole = "history"\ninterval_secs = 15\n' > "$T/b.toml"
if ( eval "$(toml_read "$T/b.toml")"; [ "$OLD_beacon_node" = hyperion-testnet ] && [ "$OLD_beacon_interval_secs" = 15 ] && [ "$OLD_source_rpc_url" = http://127.0.0.1:8889 ] && [ "$OLD_source_producer_api_url" = http://127.0.0.1:8890 ] && [ "$OLD_snapshot_dir" = /data/snap ] && [ "$OLD_ceremony_mode" = api ] && [ "$OLD_beacon_url" = https://x.example/api/report ] ); then ok "beacon: existing config values read back"; else bad "beacon: existing config values read back"; fi
if [ "$(mode_for_role producer)" = producer ] && [ "$(mode_for_role history)" = api ] && [ "$(mode_for_role api)" = api ]; then ok "beacon: ceremony mode follows role"; else bad "beacon: ceremony mode follows role"; fi

echo "install.sh (/v1 gateway modes)"
ROOT=$(cd "$HERE/.." && pwd)
# install.sh is not sourceable (it runs as root, top to bottom): extract its pure gateway helpers.
eval "$(sed -n '/^gw_mode_check(){/,/^}/p;/^gw_unit(){/,/^}/p' "$ROOT/install.sh")"
if ( bash -n "$ROOT/install.sh" ); then ok "install.sh: syntax"; else bad "install.sh: syntax"; fi
for m in legacy native edge; do
  if [ "$(gw_mode_check "$m" 2>/dev/null)" = "$m" ]; then ok "gateway mode: $m accepted"; else bad "gateway mode: $m accepted"; fi
done
if [ "$(gw_mode_check "" 2>/dev/null)" = legacy ]; then ok "gateway mode: absent defaults to legacy (unchanged)"; else bad "gateway mode: default"; fi
expect_die "gateway mode: unknown mode refused"            gw_mode_check translating
expect_die "gateway mode: injection-looking mode refused"  gw_mode_check 'edge; rm -rf /'
U=$(gw_unit edge TESTBID /usr/bin/node "PulseVM /v1 edge")
for want in "Environment=NATIVE_BASE=http://127.0.0.1:9650/ext/bc/TESTBID" "Environment=RPC_URL=http://127.0.0.1:9650/ext/bc/TESTBID/rpc" \
            "Environment=FEDERATOR_URL=http://127.0.0.1:7010" "Environment=STATIC_DIR=/etc/pulse-cutover/static" "Environment=PORT=8899" \
            "ExecStart=/usr/bin/node /opt/pulse-gateway/server.js"; do
  printf '%s\n' "$U" | grep -qxF "$want" || bad "edge unit: missing $want"
done
printf '%s\n' "$U" | grep -qxF "Environment=PORT=8899" && ok "edge unit: native base, rpc, federator, static dir, port 8899"
# legacy/native units are byte-identical to the pre-edge installer's unit
for m in legacy native; do
  WANT=$(printf '%s\n' "[Unit]" "Description=D" "After=network.target" "[Service]" "Environment=UPSTREAM=http://127.0.0.1:9650/ext/bc/B/rpc" \
    "Environment=NATIVE_BASE=http://127.0.0.1:9650/ext/bc/B" "Environment=PORT=8899" "ExecStart=/n /opt/pulse-gateway/server.js" "Restart=always" "RestartSec=3" "[Install]" "WantedBy=multi-user.target")
  if [ "$(gw_unit "$m" B /n D)" = "$WANT" ]; then ok "$m unit: unchanged"; else bad "$m unit: unchanged"; fi
done
# the native pass-through heredoc is still extractable (CI runs it)
awk "/cat > \/opt\/pulse-gateway\/server.js <<'JS'/{f=1;next} /^JS$/{f=0} f" "$ROOT/install.sh" > "$T/native.js"
if command -v node >/dev/null && node --check "$T/native.js" 2>/dev/null && [ -s "$T/native.js" ]; then ok "native pass-through heredoc extracts and parses"; else bad "native pass-through heredoc"; fi
if command -v node >/dev/null; then
  for f in gateway/server.js federator/server.js tools/capture-static.mjs; do
    if node --check "$ROOT/$f" 2>/dev/null; then ok "node --check $f"; else bad "node --check $f"; fi
  done
fi

echo; echo "$PASS passed, $FAIL failed"; [ "$FAIL" = 0 ]
