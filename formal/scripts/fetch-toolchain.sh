#!/usr/bin/env bash
# Populate formal/.toolchain with the pinned Aeneas/Charon toolchain.
# Uses prebuilt release artifacts: no OCaml build, no Lean-library source build.
set -euo pipefail

AENEAS_TAG="nightly-2026.07.06-45061fa"   # must match formal/README.md pins
CHARON_TAG="nightly-2026.07.02"           # the Charon this Aeneas pins (charon-pin)
RUST_NIGHTLY="nightly-2026-06-01"         # the Rust nightly Charon needs
ARCH="$(uname -m)"

DIR="$(cd "$(dirname "$0")/.." && pwd)/.toolchain"
mkdir -p "$DIR" && cd "$DIR"

echo "Fetching Aeneas $AENEAS_TAG ..."
curl -sL -o aeneas.tar.gz \
  "https://github.com/AeneasVerif/aeneas/releases/download/$AENEAS_TAG/aeneas-linux-$ARCH.tar.gz"
tar xzf aeneas.tar.gz

echo "Fetching prebuilt Aeneas Lean library ..."
curl -sL -o lean-lib.tar.gz \
  "https://github.com/AeneasVerif/aeneas/releases/download/$AENEAS_TAG/lean-build-aeneas-$ARCH-unknown-linux-gnu.tar.gz"
tar xzf lean-lib.tar.gz

echo "Fetching Charon $CHARON_TAG (the version pinned by this Aeneas) ..."
mkdir -p charon-bin
curl -sL -o charon.tar.gz \
  "https://github.com/AeneasVerif/charon/releases/download/$CHARON_TAG/charon-linux-$ARCH.tar.gz"
tar xzf charon.tar.gz -C charon-bin

echo "Installing Charon's pinned Rust toolchain ..."
rustup toolchain install "$RUST_NIGHTLY" \
  --component rustc-dev --component llvm-tools-preview --component rust-src

rm -f aeneas.tar.gz lean-lib.tar.gz charon.tar.gz
echo "Done. Toolchain in $DIR"
echo "Lean itself is managed by elan via formal/proofs/lean-toolchain."
