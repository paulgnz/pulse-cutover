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

echo "beacon-install.sh"
BEACON_INSTALL_SOURCED=1 source "$HERE/beacon-install.sh"; set +e
expect_die "beacon: http URL refused"            check_url http://control.example.org
expect_ok  "beacon: https URL accepted"          check_url https://control.example.org
expect_ok  "beacon: http://127.0.0.1 accepted"   check_url http://127.0.0.1:8787
expect_die "beacon: producer name validated"     check_producer "Bad.Name!"
expect_ok  "beacon: producer name ok"            check_producer protonnz
printf '[ceremony]\nmode = "api"\n[source]\nrpc_url = "http://127.0.0.1:8889"\nproducer_api_url = "http://127.0.0.1:8890"\n[snapshot]\ndir = "/data/snap"\n[beacon]\nurl = "https://x.example/api/report"\nproducer = "protonnz"\nnetwork = "testnet"\nnode = "hyperion-testnet"\nrole = "history"\ninterval_secs = 15\n' > "$T/b.toml"
if ( eval "$(toml_read "$T/b.toml")"; [ "$OLD_beacon_node" = hyperion-testnet ] && [ "$OLD_beacon_interval_secs" = 15 ] && [ "$OLD_source_rpc_url" = http://127.0.0.1:8889 ] && [ "$OLD_source_producer_api_url" = http://127.0.0.1:8890 ] && [ "$OLD_snapshot_dir" = /data/snap ] && [ "$OLD_ceremony_mode" = api ] && [ "$OLD_beacon_url" = https://x.example/api/report ] ); then ok "beacon: existing config values read back"; else bad "beacon: existing config values read back"; fi
if [ "$(mode_for_role producer)" = producer ] && [ "$(mode_for_role history)" = api ] && [ "$(mode_for_role api)" = api ]; then ok "beacon: ceremony mode follows role"; else bad "beacon: ceremony mode follows role"; fi

echo; echo "$PASS passed, $FAIL failed"; [ "$FAIL" = 0 ]
