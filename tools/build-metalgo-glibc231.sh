#!/usr/bin/env bash
# build-metalgo-glibc231.sh — reproduce the Ubuntu 20.04-compatible metalgo builds published on the
# metalgo-glibc2.31-* releases. Metal's official binaries are built on a newer OS and need glibc 2.34+;
# the same source tag compiled inside ubuntu:20.04 runs natively on 20.04 (glibc 2.31).
#
#   ./build-metalgo-glibc231.sh v1.14.2-tahoe      # needs Docker; output in ./out
#
# Note: -ldl is required on glibc < 2.34 (the bundled Firewood FFI calls dlsym, which moved into libc in 2.34).
set -euo pipefail
V=${1:?usage: $0 <metalgo tag>}
mkdir -p out
docker run --rm -v "$PWD/out:/out" -e V="$V" ubuntu:20.04 bash -ec '
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq && apt-get install -y -qq git build-essential curl ca-certificates >/dev/null
  GOV=$(curl -fsSL https://raw.githubusercontent.com/MetalBlockchain/metalgo/$V/go.mod | sed -n "s/^go //p")
  curl -fsSL https://go.dev/dl/go$GOV.linux-amd64.tar.gz | tar xz -C /usr/local
  export PATH=/usr/local/go/bin:$PATH GOFLAGS=-buildvcs=false CGO_LDFLAGS=-ldl
  git clone -q --depth 1 --branch $V https://github.com/MetalBlockchain/metalgo /src
  cd /src && ./scripts/build.sh
  d=/out/metalgo-linux-amd64-$V-glibc2.31; mkdir -p $d; cp build/metalgo $d/metalgo
  objdump -T $d/metalgo | grep -o "GLIBC_[0-9.]*" | sort -Vu | tail -1
  $d/metalgo --version'
