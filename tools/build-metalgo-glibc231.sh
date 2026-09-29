#!/usr/bin/env bash
# build-metalgo-glibc231.sh — reproduce the Ubuntu 20.04-compatible metalgo builds published on the
# metalgo-glibc2.31-* releases. Metal's official binaries are built on a newer OS and need glibc 2.34+;
# the same source commit compiled inside ubuntu:20.04 runs natively on 20.04 (glibc 2.31).
#
#   ./build-metalgo-glibc231.sh v1.14.2-tahoe      # needs Docker; output in ./out
#
# Every input is pinned and recorded in out/<name>.provenance.json:
#   - platform linux/amd64, base image ubuntu:20.04 by digest (override: UBUNTU_DIGEST=sha256:…)
#   - the tag is resolved to a commit here, and the build checks out exactly that commit
#     (optionally require it: EXPECT_COMMIT=<40 hex>)
#   - the Go toolchain version comes from go.mod at that commit; its tarball sha256 comes from go.dev and is verified
# apt packages inside the image are the one moving input (Ubuntu 20.04's archive is frozen in practice); their
# versions are recorded in the provenance file.
#
# Note: -ldl is required on glibc < 2.34 (the bundled Firewood FFI calls dlsym, which moved into libc in 2.34).
set -euo pipefail
V=${1:?usage: $0 <metalgo tag>}
# ubuntu:20.04 multi-arch index digest, looked up from registry-1.docker.io on 2026-09-29.
UBUNTU_DIGEST=${UBUNTU_DIGEST:-sha256:8feb4d8ca5354def3d8fce243717141ce31e2c428701f6682bd2fafe15388214}
IMAGE="ubuntu@$UBUNTU_DIGEST"

deref=$(git ls-remote https://github.com/MetalBlockchain/metalgo "refs/tags/$V^{}" | awk '{print $1}')
COMMIT=${deref:-$(git ls-remote https://github.com/MetalBlockchain/metalgo "refs/tags/$V" | awk '{print $1}')}
[ -n "$COMMIT" ] || { echo "tag $V not found upstream" >&2; exit 1; }
if [ -n "${EXPECT_COMMIT:-}" ] && [ "$COMMIT" != "$EXPECT_COMMIT" ]; then echo "tag $V → $COMMIT, expected $EXPECT_COMMIT" >&2; exit 1; fi
GOV=$(curl -fsSL "https://raw.githubusercontent.com/MetalBlockchain/metalgo/$COMMIT/go.mod" | sed -n 's/^go //p')
GOSHA=$(curl -fsSL "https://go.dev/dl/?mode=json&include=all" | python3 -c 'import json,sys
f=sys.argv[1]
for r in json.load(sys.stdin):
  for x in r.get("files",[]):
    if x.get("filename")==f: print(x["sha256"]); sys.exit()' "go$GOV.linux-amd64.tar.gz")
[ -n "$GOV" ] && [ -n "$GOSHA" ] || { echo "could not pin the Go toolchain (go.mod version '$GOV')" >&2; exit 1; }
echo "tag $V → commit $COMMIT · go$GOV (sha256 $GOSHA) · $IMAGE"

NAME=metalgo-linux-amd64-$V-glibc2.31
mkdir -p out
docker run --rm --platform linux/amd64 -v "$PWD/out:/out" -e V="$V" -e COMMIT="$COMMIT" -e GOV="$GOV" -e GOSHA="$GOSHA" -e NAME="$NAME" "$IMAGE" bash -ec '
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq && apt-get install -y -qq git build-essential curl ca-certificates >/dev/null
  dpkg-query -W -f="\${Package}=\${Version}\n" git build-essential gcc libc6-dev > /out/$NAME.apt.txt
  curl -fsSL -o /tmp/go.tgz https://go.dev/dl/go$GOV.linux-amd64.tar.gz
  echo "$GOSHA  /tmp/go.tgz" | sha256sum -c --quiet
  tar xzf /tmp/go.tgz -C /usr/local
  export PATH=/usr/local/go/bin:$PATH GOFLAGS=-buildvcs=false CGO_LDFLAGS=-ldl
  git clone -q https://github.com/MetalBlockchain/metalgo /src && cd /src && git -c advice.detachedHead=false checkout -q "$COMMIT"
  [ "$(git rev-parse HEAD)" = "$COMMIT" ]
  ./scripts/build.sh
  d=/out/$NAME; mkdir -p $d; cp build/metalgo $d/metalgo
  objdump -T $d/metalgo | grep -o "GLIBC_[0-9.]*" | sort -Vu | tail -1 > /out/$NAME.glibc.txt
  $d/metalgo --version > /out/$NAME.version.txt'
(cd out && COPYFILE_DISABLE=1 tar czf "$NAME.tar.gz" "$NAME")
OUTSHA=$(sha256sum "out/$NAME.tar.gz" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "out/$NAME.tar.gz" | cut -d' ' -f1)
BINSHA=$(sha256sum "out/$NAME/metalgo" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "out/$NAME/metalgo" | cut -d' ' -f1)
python3 - "$V" "$COMMIT" "$GOV" "$GOSHA" "$IMAGE" "$OUTSHA" "$BINSHA" "out/$NAME" > "out/$NAME.provenance.json" <<'PY'
import json, sys
v, commit, gov, gosha, image, outsha, binsha, base = sys.argv[1:9]
rd = lambda s: open(base + s).read().strip()
print(json.dumps({"tag": v, "commit": commit, "go": {"version": gov, "linux_amd64_sha256": gosha},
  "image": image, "platform": "linux/amd64", "cgo_ldflags": "-ldl", "apt": rd(".apt.txt").splitlines(),
  "max_glibc_symbol": rd(".glibc.txt"), "metalgo_version": rd(".version.txt"),
  "binary_sha256": binsha, "archive": base.split("/")[-1] + ".tar.gz", "archive_sha256": outsha}, indent=2))
PY
cat "out/$NAME.provenance.json"
