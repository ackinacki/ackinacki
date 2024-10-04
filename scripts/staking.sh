#!/usr/bin/env sh

set -eu

MASTER_KEY_FILE=$1
BLS_KEYS_FILE=$2
WALLET_ABI=../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json
ABI=../contracts/bksystem/BlockKeeperContractRoot.abi.json
ELECTION=0:7777777777777777777777777777777777777777777777777777777777777777

if [ ! -e $MASTER_KEY_FILE ]; then
  echo $MASTER_KEY_FILE not found.
  exit 1
fi

if [ ! -e $BLS_KEYS_FILE ]; then
  echo $BLS_KEYS_FILE not found.
  exit 1
fi

MASTER_PUB_KEY_JSON=$(jq .public $MASTER_KEY_FILE | tr -d '\"')
MASTER_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$MASTER_PUB_KEY_JSON/g")
BLS_PUB_KEY=$(jq .public $BLS_KEYS_FILE | tr -d '\"')
WALLET_ADDR=$(tvm-cli -j runx --abi $ABI --addr $ELECTION -m getAckiNackiBlockKeeperNodeWalletAddress "$MASTER_PUB_KEY" | jq '.value0' | tr -d '\"')

INIT_WALLET_STATE=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails)
INIT_WALLET_BALANCE=$(echo $INIT_WALLET_STATE | jq '.balance' | tr -d '\"' | xargs printf "%d")
INIT_ACTIVE_STAKES=$(echo $INIT_WALLET_STATE | jq '.activeStakes | length')
WALLET_STAKE=$(tvm-cli -j runx --abi $ABI --addr 0:7777777777777777777777777777777777777777777777777777777777777777 -m getDetails | jq '.minStake' | tr -d '\"' | xargs printf "%d * 1.1\n" | bc | cut -d'.' -f1)

echo "Current balance: $INIT_WALLET_BALANCE"
echo "Sending stake: $WALLET_STAKE"

compare=$(echo "$INIT_WALLET_BALANCE <= $WALLET_STAKE" | bc)

if [ "$compare" -eq 1 ]; then
  echo "Not enough token's on the wallet"
  exit 1
fi

PLACE_PARAMS="{\"bls_pubkey\": \"$BLS_PUB_KEY\", \"stake\": $WALLET_STAKE}"

tvm-cli -j callx --addr $WALLET_ADDR --abi $WALLET_ABI --keys $MASTER_KEY_FILE --method sendBlockKeeperRequestWithStake "$PLACE_PARAMS" && echo Block Keeper request has been sent.

echo "Waiting active stakes..."
sleep 5

tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails # | jq '.activeStakes'

