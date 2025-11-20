#!/usr/bin/env bash

set -uo pipefail

BM_ABI=../contracts/0.79.3_compiled/bksystem/AckiNackiBlockManagerNodeWallet.abi.json
ABI=../contracts/0.79.3_compiled/bksystem/BlockManagerContractRoot.abi.json
ROOT=0:6666666666666666666666666666666666666666666666666666666666666666

while getopts 'p:' opts; do
  case "${opts}" in
    [p])
      SLEEP_TIME="$OPTARG"
      ;;
    *)
      echo Unknown parameter
      exit 2 ;;
  esac
done

shift $(( OPTIND - 1 ))

BM_KEY=$1

if [[ ! -f $BM_KEY ]]; then
  echo "$BM_KEY not found."
  exit 1
fi

BM_PUB_KEY_JSON=$(jq -e -r .public $BM_KEY || { echo "Error with reading block manager public key" >&2 ; exit 1 ;})
BM_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$BM_PUB_KEY_JSON/g")

BM_WALLET=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getAckiNackiBlockManagerNodeWalletAddress "$BM_PUB_KEY" | jq -e -r '.wallet')

BM_WALLET_DETAILS=$(tvm-cli -j runx --abi $BM_ABI --addr $BM_WALLET -m getDetails || { echo "Error with getting details $BM_WALLET" >&2 ; exit 1 ;})

echo $BM_WALLET_DETAILS

tvm-cli -j callx --abi $BM_ABI --addr $BM_WALLET --keys $BM_KEY -m startBM
sleep 10

# _start_bm
# _walletLastTouch
# _walletTouch
# _license_num
# _epochEnd

process_bm () {
  tvm-cli -j callx --abi $BM_ABI --addr $BM_WALLET --keys $BM_KEY -m getReward
}

# BM_START=$(tvm-cli -j decode account data --abi $BM_ABI --addr $BM_WALLET | jq -e -r '._start_bm' || { log "Error with getting start BM for $BM_WALLET" >&2 ; exit 1 ;})
# BM_LAST_TOUCH=$(tvm-cli -j decode account data --abi $BM_ABI --addr $BM_WALLET | jq -e -r '._start_bm' || { log "Error with getting start BM for $BM_WALLET" >&2 ; exit 1 ;})
# BM_WALLET_TOUCH=$(tvm-cli -j decode account data --abi $BM_ABI --addr $BM_WALLET | jq -e -r '._start_bm' || { log "Error with getting start BM for $BM_WALLET" >&2 ; exit 1 ;})
# BM_LICENSE_NUM=$(tvm-cli -j decode account data --abi $BM_ABI --addr $BM_WALLET | jq -e -r '._start_bm' || { log "Error with getting start BM for $BM_WALLET" >&2 ; exit 1 ;})
# BM_EPOCH_END=$(tvm-cli -j decode account data --abi $BM_ABI --addr $BM_WALLET | jq -e -r '._start_bm' || { log "Error with getting start BM for $BM_WALLET" >&2 ; exit 1 ;})

while true; do
  process_bm
  set +e
  sleep $SLEEP_TIME &
  wait $!
  set -e
done
