#!/usr/bin/env bash
#
# Rebuild the four bridge binaries from the sibling
# `acki-nacki-to-eth-bridge-halo2-prover` checkout and refresh the copies
# bundled under `acki-nacki/bridge_test_bin/` for the self-contained bridge
# E2E test (`tests/exchange/bridge_e2e_self_contained.py`).
#
# Expected layout:
#   <parent>/acki-nacki/                              ← this repo (cwd-agnostic)
#   <parent>/acki-nacki-to-eth-bridge-halo2-prover/   ← prover repo
#
# Prover repo must be on branch `main`. `bridge-prover-daemon` is built with
# `--features self-verify` so it inline-verifies each Circuit 1A + Circuit 2
# bundle and records the verdict to `state/prover_state.json::recent_bundles[]`.
# The other three event-side binaries do not need feature flags.
#
# Usage:
#   scripts/rebuild-bridge-bins.sh
#   PROVER_REPO=/abs/path scripts/rebuild-bridge-bins.sh   # override location
set -euo pipefail

ACKI_NACKI_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROVER_REPO="${PROVER_REPO:-$(cd "$ACKI_NACKI_ROOT/../acki-nacki-to-eth-bridge-halo2-prover" && pwd)}"

if [[ ! -d "$PROVER_REPO" ]]; then
    echo "error: prover repo not found at $PROVER_REPO" >&2
    echo "set PROVER_REPO=/abs/path to override" >&2
    exit 1
fi

current_branch="$(git -C "$PROVER_REPO" rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" != "main" ]]; then
    echo "warning: prover repo is on branch '$current_branch', expected 'main'" >&2
    echo "         continuing anyway." >&2
fi

echo "==> building bridge-prover-daemon (release, --features self-verify)"
( cd "$PROVER_REPO" && cargo build --release --features self-verify -p bridge-prover-daemon )

echo "==> building three event-side binaries (release)"
# `bridge-event-private-witness-export` and `bridge-event-witness-builder`
# are `[[bin]]` entries inside the `bridge-event-witness` crate (not separate
# workspace members), so we scope --bin lookups via -p. `bridge-event-halo2-prover`
# IS its own crate so it builds directly.
( cd "$PROVER_REPO" && cargo build --release \
    -p bridge-event-witness \
    --bin bridge-event-private-witness-export \
    --bin bridge-event-witness-builder )
( cd "$PROVER_REPO" && cargo build --release -p bridge-event-halo2-prover )

DEST="$ACKI_NACKI_ROOT/bridge_test_bin"
mkdir -p "$DEST"

for bin in \
    bridge-prover-daemon \
    bridge-event-private-witness-export \
    bridge-event-witness-builder \
    bridge-event-halo2-prover
do
    cp "$PROVER_REPO/target/release/$bin" "$DEST/$bin"
    echo "    refreshed $DEST/$bin"
done

echo "==> done. Run: python3 tests/exchange/bridge_e2e_self_contained.py"
