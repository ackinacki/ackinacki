#!/usr/bin/env sh

set -eu

MASTER_KEY_FILE=$1
BLS_KEYS_FILE=$2
WALLET_ABI=../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json
ABI=../contracts/bksystem/BlockKeeperContractRoot.abi.json
PRE_EPOCH_ABI=../contracts/bksystem/BlockKeeperPreEpochContract.abi.json
COOLER_ABI=../contracts/bksystem/BlockKeeperCoolerContract.abi.json
ROOT=0:7777777777777777777777777777777777777777777777777777777777777777
EPOCH_ABI=../contracts/bksystem/BlockKeeperEpochContract.abi.json

IS_EPOCH_ACTIVE=false
IS_EPOCH_CONTINUE=false
SEQNO=0

if [ ! -e $MASTER_KEY_FILE ]; then
  echo $MASTER_KEY_FILE not found.
  exit 1
fi

if [ ! -e $BLS_KEYS_FILE ]; then
  echo $BLS_KEYS_FILE not found.
  exit 1
fi

MASTER_PUB_KEY_JSON=$(jq -r .public $MASTER_KEY_FILE)
MASTER_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$MASTER_PUB_KEY_JSON/g")
BLS_PUB_KEY=$(jq -r .public $BLS_KEYS_FILE)
WALLET_ADDR=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getAckiNackiBlockKeeperNodeWalletAddress "$MASTER_PUB_KEY" | jq -r '.value0')
INIT_WALLET_STATE=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails)
INIT_ACTIVE_STAKES=$(echo $INIT_WALLET_STATE | jq '.activeStakes | length')
INIT_WALLET_BALANCE=$(echo $INIT_WALLET_STATE | jq -r '.balance' | xargs printf "%d")
echo "Count of stake: $INIT_ACTIVE_STAKES"

place_stake () {
  WALLET_STAKE=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails | jq -r '.minStake' | xargs printf "%d * 1.1\n" | bc | cut -d'.' -f1)
  
  echo "Current wallet $WALLET_ADDR balance: $INIT_WALLET_BALANCE"
  echo "Sending stake: $WALLET_STAKE"

  compare=$(echo "$INIT_WALLET_BALANCE <= $WALLET_STAKE" | bc)
  if [ "$compare" -eq 1 ]; then
    echo "Not enough token's on the wallet"
    exit 1
  fi
  PLACE_PARAMS="{\"bls_pubkey\": \"$BLS_PUB_KEY\", \"stake\": $WALLET_STAKE}"
  tvm-cli -j callx --addr $WALLET_ADDR --abi $WALLET_ABI --keys $MASTER_KEY_FILE --method sendBlockKeeperRequestWithStake "$PLACE_PARAMS" && echo Staking request has been sent.
  echo "Waiting active stakes..."
  sleep 5

  ACTIVE_STAKES=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '.activeStakes | length')
  compare=$(echo "$ACTIVE_STAKES < $INIT_ACTIVE_STAKES" | bc)
  if [ "$compare" -eq 1 ]; then
    echo "Stake request failed..."
    echo "Exiting"
    exit 1
  fi
  echo "Stake request successfuly accepted..."
}

place_continue_stake () {
  CONT_WALLET_STAKE=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails | jq -r '.minStake' | xargs printf "%d * 1.1\n" | bc | cut -d'.' -f1)
  compare=$(echo "$INIT_WALLET_BALANCE <= $CONT_WALLET_STAKE" | bc)
  if [ "$compare" -eq 1 ]; then
    echo "Not enough token's to continue staking"
    exit 1
  fi

  CONT_STAKING="{\"bls_pubkey\": \"$BLS_PUB_KEY\", \"stake\": $CONT_WALLET_STAKE, \"seqNoStartOld\": \"$1\"}"
  tvm-cli -j callx --addr $WALLET_ADDR --abi $WALLET_ABI --keys $MASTER_KEY_FILE --method sendBlockKeeperRequestWithStakeContinue "$CONT_STAKING" && echo Continue staking request has been sent.
}

process_epoch () {
  EPOCH_PARAMS=$(cat $MASTER_KEY_FILE | jq -r '.public')
  for k in $(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '.activeStakes | keys | .[]'); do
    ACTIVE_STAKES_SEQ=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -r ".activeStakes[$k].seqNoStart")
    case $(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -r ".activeStakes[$k].status") in
      0)
        echo "Pre Epoch - $k"
        CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no')
        PRE_EPOCH_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getBlockKeeperPreEpochAddress "{\"pubkey\": \"0x$EPOCH_PARAMS\", \"seqNoStart\": \"$ACTIVE_STAKES_SEQ\"}" | jq -r '.value0')
        echo "PreEpoch contract address \"$PRE_EPOCH_ADDRESS\" and sequence start is \"$ACTIVE_STAKES_SEQ\" and current sequence is \"$CUR_BLOCK_SEQ\""
        IS_EPOCH_ACTIVE=false
        IS_EPOCH_CONTINUE=true
        compare=$(echo "$ACTIVE_STAKES_SEQ < $CUR_BLOCK_SEQ" | bc)
        if [ "$compare" -eq 1 ]; then
          echo "Touching preEpoch contract"
          tvm-cli -j callx --abi $PRE_EPOCH_ABI --addr $PRE_EPOCH_ADDRESS -m touch
        fi
        ;;
      1)
        echo "Epoch in progress - $k"
        EPOCH_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getBlockKeeperEpochAddress "{\"pubkey\": \"0x$EPOCH_PARAMS\", \"seqNoStart\": \"$ACTIVE_STAKES_SEQ\"}" | jq -r '.value0')
        EPOCH_DETAILS=$(tvm-cli -j runx --abi $EPOCH_ABI --addr $EPOCH_ADDRESS -m getDetails)
        echo "There is active stake with epoch address \"$EPOCH_ADDRESS\""
        IS_EPOCH_ACTIVE=true
        IS_EPOCH_CONTINUE=$(echo $EPOCH_DETAILS | jq -r '.isContinue')
        SEQNO=$ACTIVE_STAKES_SEQ
        ;;
      2)
        echo "Cooler Epoch - $k"
        COOLER_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getBlockKeeperCoolerAddress "{\"pubkey\": \"0x$EPOCH_PARAMS\", \"seqNoStart\": \"$ACTIVE_STAKES_SEQ\"}" | jq -r '.value0')
        COOLER_SEQNO_FINISH=$(tvm-cli -j runx --abi $COOLER_ABI --addr $COOLER_ADDRESS -m getDetails | jq -r '.seqNoStart')
        echo "Cooler Epoch found with address \"$COOLER_ADDRESS\" and finish seqno \"$COOLER_SEQNO_FINISH\""
        CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no')
        compare=$(echo "$COOLER_SEQNO_FINISH < $CUR_BLOCK_SEQ" | bc)
        if [ "$compare" -eq 1 ]; then
          echo "Touching cooler contract"
          tvm-cli -j callx --abi $COOLER_ABI --addr $COOLER_ADDRESS -m touch
        fi
        ;;
    esac
  done
}

if [ $INIT_ACTIVE_STAKES -le 0 ]; then
  place_stake
fi

process_epoch

if [ "$IS_EPOCH_CONTINUE" = false ]; then
  place_continue_stake $SEQNO
fi
