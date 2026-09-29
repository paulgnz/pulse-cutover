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

# --- --build needs a pinned commit ---------------------------------------------------------------------
MF_COMMIT=""; expect_die "build: refused without metalgo_commit" require_build_commit
MF_COMMIT=$(rep a 40); expect_ok "build: pinned commit accepted" require_build_commit

# --- upgrade transaction: failure right after the stop restores everything --------------------------
(
  R="$T/mi"; mkdir -p "$R/etc" "$R/bin" "$R/stage"
  BIN="$R/bin/metalgo"; ETC="$R/etc"; UNIT="$R/etc/metalgo.service"; STAGE="$R/stage"; TMP="$R/tmp"; mkdir -p "$TMP"
  echo old-bin > "$BIN"; echo old-cfg > "$ETC/config.json"; echo old-unit > "$UNIT"
  echo new-bin > "$STAGE/metalgo"; echo new-cfg > "$STAGE/config.json"; echo new-unit > "$STAGE/metalgo.service"
  systemctl() { echo "systemctl $*" >> "$R/sys.log"; return 0; }
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
  systemctl() { echo "systemctl $*" >> "$R/sys.log"; return 0; }
  existing_unit() { return 0; }
  read_live_identity() { NODEID=NodeID-SOMETHINGELSE; BLSPUB=0xnew; }
  sleep() { :; }
  METHOD=binary; PREV_NODEID=NodeID-OLD; PREV_BLS=0xold
  trap on_exit EXIT
  FAULT_AFTER_STOP=false swap_in
) > "$T/mi2.out" 2>&1
if grep -q "ROLLBACK INCOMPLETE" "$T/mi2.out"; then ok "upgrade: a rollback that comes back with the wrong NodeID is reported, not hidden"; else bad "upgrade: wrong-NodeID rollback reported ($(tr '\n' ' ' < "$T/mi2.out" | head -c 200))"; fi

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

echo; echo "$PASS passed, $FAIL failed"; [ "$FAIL" = 0 ]
