#!/usr/bin/env bash

set -euo pipefail

BM_ABI=../contracts/0.79.3_compiled/bksystem/AckiNackiBlockManagerNodeWallet.abi.json
ABI=../contracts/0.79.3_compiled/bksystem/BlockManagerContractRoot.abi.json
ROOT=0:6666666666666666666666666666666666666666666666666666666666666666

log() {
  echo "[$(date -Is)]" "$@"
}

while getopts 'p:' opts; do
  case "${opts}" in
    [p])
      SLEEP_TIME="$OPTARG"
      ;;
    *)
      echo "Error: Unknown parameter"
      exit 2 ;;
  esac
done

shift $((OPTIND - 1))

BM_KEY="$1"

if [[ ! -f "$BM_KEY" ]]; then
  log "Error: Key file '$BM_KEY' not found."
  exit 1
fi

log "Reading block manager public key from '$BM_KEY'"
BM_PUB_KEY_JSON=$(jq -e -r .public "$BM_KEY" || {
  log "Error: Failed to read block manager public key" >&2
  exit 1
})

BM_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$BM_PUB_KEY_JSON/g")

log "Fetching block manager wallet address"
BM_WALLET=$(tvm-cli -j runx --abi "$ABI" --addr "$ROOT" \
  -m getAckiNackiBlockManagerNodeWalletAddress "$BM_PUB_KEY" | \
  jq -e -r '.wallet' || {
    log "Error: Failed to get BM wallet address" >&2
    exit 1
  })

log "Block Manager Wallet: $BM_WALLET"

log "Fetching wallet details"
BM_WALLET_DETAILS=$(tvm-cli -j runx --abi "$BM_ABI" --addr "$BM_WALLET" \
  -m getDetails || {
    log "Error: Failed to get wallet details for $BM_WALLET" >&2
    exit 1
  })

log "Wallet details: $BM_WALLET_DETAILS"

log "Starting block manager"
tvm-cli -j callx --abi "$BM_ABI" --addr "$BM_WALLET" --keys "$BM_KEY" \
  -m startBM || {
    log "Error: Failed to start BM for $BM_WALLET" >&2
    exit 1
  }

log "Waiting for BM to start..."
sleep 10

process_bm() {
  log "Processing rewards for $BM_WALLET"
  tvm-cli -j callx --abi "$BM_ABI" --addr "$BM_WALLET" --keys "$BM_KEY" \
    -m getReward || {
      log "Error: Failed to get reward for $BM_WALLET" >&2
      exit 1
    }
}

log "Starting reward collection loop (sleep time: ${SLEEP_TIME}s)"
while true; do
  process_bm
  log "Sleeping for ${SLEEP_TIME} seconds..."
  set +e
  sleep "$SLEEP_TIME" &
  wait $!
  set -e
done
