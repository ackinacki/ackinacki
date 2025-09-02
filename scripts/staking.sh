#!/usr/bin/env bash

set -eEuo pipefail

WALLET_ABI=../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json
ABI=../contracts/bksystem/BlockKeeperContractRoot.abi.json
PRE_EPOCH_ABI=../contracts/bksystem/BlockKeeperPreEpochContract.abi.json
COOLER_ABI=../contracts/bksystem/BlockKeeperCoolerContract.abi.json
ROOT=0:7777777777777777777777777777777777777777777777777777777777777777
EPOCH_ABI=../contracts/bksystem/BlockKeeperEpochContract.abi.json

IS_EPOCH_ACTIVE=false
IS_EPOCH_CONTINUE=false
SEQNO=0

WILL_EPOCH_CONTINUE=true

DAEMON=false

while getopts 'd:l:' opts; do
  case "${opts}" in
    [d])
      DAEMON=true 
      SLEEP_TIME="$OPTARG"
      ;;
    l)
      LOGFILE="$OPTARG"
      ;;
    *)
      echo Unknown paremeter
      exit 2 ;;
  esac
done

shift $(( OPTIND - 1 ))

if [[ -f $LOGFILE ]]; then
  TIME_NOW=$(date +%s)
  cp $LOGFILE $LOGFILE-$TIME_NOW
fi

exec 1>"$LOGFILE" 2>&1
# Function for proper logging
log() {
  echo "[$(date -Is)]" "$@"
}

trigger_stopping_staking () {
  log "Stop signal has been recieved. Trying to shutdown gracefully"
  WILL_EPOCH_CONTINUE=false
  local WALLET_DETAILS=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails)
  local OLD_SEQ=$(echo $WALLET_DETAILS | jq -r '.activeStakes[] | select(.status == "1") | .seqNoStart')
  EPOCH_PARAMS=$(cat $NODE_OWNER_KEY | jq -e -r '.public' || { log "Error with reading node owner public key" >&2 ; exit 1 ;})
  EPOCH_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getBlockKeeperEpochAddress "{\"pubkey\": \"0x$EPOCH_PARAMS\", \"seqNoStart\": \"$OLD_SEQ\"}" | jq -r -e '.epochAddress' || { log "Can't get epoch address. Probably epoch has been touched. Exiting..." >&2 ; exit 0 ;})
  EPOCH_DETAILS=$(tvm-cli -j runx --abi $EPOCH_ABI --addr $EPOCH_ADDRESS -m getDetails || { log "Can't get epoch $EPOCH_ADDRESS details. Probably epoch $EPOCH_ADDRESS has been touched. Exiting..." >&2 ; exit 0 ;})
  EPOCH_FINISH=$(echo $EPOCH_DETAILS | jq -r '.seqNoFinish')
  log "Current epoch address $EPOCH_ADDRESS and epoch seqNo finish $EPOCH_FINISH"

  CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no')
  while [ "$(echo "$EPOCH_FINISH < $CUR_BLOCK_SEQ" | bc)" -ne 1 ]; do
    log "Current block seq_no is less than epoch finishing block seq_no. Waiting..."
    sleep 2
    CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no')
  done

  log "Current epoch $EPOCH_ADDRESS is ready to be touched"
  tvm-cli -j callx --abi $EPOCH_ABI --addr $EPOCH_ADDRESS -m touch

  log "Exiting..."
  exit 0
}

trigger_stopping_continue_staking () {
  log "SIGHUP signal has been recieved. Disabling continue staking"
  WILL_EPOCH_CONTINUE=false
  local WALLET_DETAILS=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails)
  local OLD_SEQ=$(echo $WALLET_DETAILS | jq -r '.activeStakes[] | select(.status == "1") | .seqNoStart')

  EPOCH_PARAMS=$(cat $NODE_OWNER_KEY | jq -e -r '.public' || { log "Error with reading node owner public key" >&2 ; exit 1 ;})
  EPOCH_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getBlockKeeperEpochAddress "{\"pubkey\": \"0x$EPOCH_PARAMS\", \"seqNoStart\": \"$OLD_SEQ\"}" | jq -r -e '.epochAddress' || { log "Can't get epoch address. Exiting..." >&2 ; exit 1 ;})
  EPOCH_DETAILS=$(tvm-cli -j runx --abi $EPOCH_ABI --addr $EPOCH_ADDRESS -m getDetails || { log "Can't get epoch $EPOCH_ADDRESS details. Exiting..." >&2 ; exit 1 ;})
  IS_EPOCH_CONTINUE=$(echo $EPOCH_DETAILS | jq -r '.isContinue')
  if [ "$IS_EPOCH_CONTINUE" = false ]; then
    log "Current epoch $EPOCH_ADDRESS is not being continued. Skipping epoch continue canceling..."
    return 0
  fi

  local BALANCE_BEFORE=$(echo $WALLET_DETAILS | jq -r '.balance' | xargs printf "%d")
  log "Current wallet balance - $BALANCE_BEFORE"
  local CANCEL_PARAMS="{\"seqNoStartOld\": \"$OLD_SEQ\"}"
  tvm-cli -j callx --addr $WALLET_ADDR --abi $WALLET_ABI --keys $NODE_OWNER_KEY --method sendBlockKeeperRequestWithCancelStakeContinue "$CANCEL_PARAMS" || { log "Can't cancel continue staking. Probably there is no active epoch for wallet $WALLET_ADDR. Exiting..." >&2 ; exit 1 ;}
  
  local BALANCE_AFTER=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -r '.balance' | xargs printf "%d")
  local BALANCE_ATTEMPTS=0
  while [ $BALANCE_AFTER -le $BALANCE_BEFORE ] && [ $BALANCE_ATTEMPTS -lt 10 ]; do
    log "Updated balance - $BALANCE_AFTER"
    local BALANCE_AFTER=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -r '.balance' | xargs printf "%d")
    local BALANCE_ATTEMPTS=$(( BALANCE_ATTEMPTS + 1 ))
    sleep 2
  done
  
  if [ "$BALANCE_BEFORE" -ge "$BALANCE_AFTER" ]; then
    log "It seems like continue stake hasn't been moved back or continue stake was 0"
  fi
  log "Balance after canceling continue staking - $BALANCE_AFTER"
  return 0
}

trap 'trigger_stopping_continue_staking' SIGHUP
trap trigger_stopping_staking SIGINT SIGTERM

NODE_OWNER_KEY=$1
BLS_KEYS_FILE=$2
NODE_IP=$3

if [ ! -e $NODE_OWNER_KEY ]; then
  log "$NODE_OWNER_KEY not found."
  exit 1
fi

if [ ! -e $BLS_KEYS_FILE ]; then
  log "$BLS_KEYS_FILE not found."
  exit 1
fi

NODE_OWNER_PUB_KEY_JSON=$(jq -e -r .public $NODE_OWNER_KEY || { log "Error with reading node owner public key" >&2 ; exit 1 ;})
NODE_OWNER_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$NODE_OWNER_PUB_KEY_JSON/g")
BLS_PUB_KEY=$(jq -e -r .[0].public $BLS_KEYS_FILE || { log "Error with reading BLS public key" >&2 ; exit 1 ;})
READINESS_COUNT=0
while ! tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails > /dev/null 2>&1 && [[ $READINESS_COUNT -lt 10 ]]; do
  log "Network endpoint is not reachable. Waiting..."
  sleep 2
  READINESS_COUNT=$(( READINESS_COUNT + 1 ))
done
WALLET_ADDR=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getAckiNackiBlockKeeperNodeWalletAddress "$NODE_OWNER_PUB_KEY" | jq -e -r '.wallet' || { log "Error with getting wallet address" >&2 ; exit 1 ;})
INIT_WALLET_STATE=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails || { log "Error with getting details $WALLET_ADDR" >&2 ; exit 1 ;})
INIT_ACTIVE_STAKES=$(echo $INIT_WALLET_STATE | jq '.activeStakes | length')
log "Wallet address: $WALLET_ADDR"
log "Count of stake: $INIT_ACTIVE_STAKES"

update_bls_keys () {
  if ! type -t node-helper > /dev/null 2>&1
  then
    log "node-helper: command not found"
    exit 1
  fi

  local KEY_LENGTH=$(jq '. | length' $1)
  UPD_BLS_PUBLIC_KEY=$(node-helper bls --path $1 | jq -r '.pubkey')
  local UPD_KEY_LENGTH=$(jq '. | length' $1)
  if [ $KEY_LENGTH -ge $UPD_KEY_LENGTH ] || [ -z $UPD_BLS_PUBLIC_KEY ]; then
    log "BLS key update failed. Exiting..."
    exit 1
  fi
  NODE_PID=$(pgrep -f "^node.* -c acki-nacki.conf.yaml$" || { log "Error with getting Node PID" >&2 ; exit 1 ;})
  kill -1 $NODE_PID
}

place_stake () {
  local SIGN_INDEX_START=1
  local SIGN_INDEX_END=60000
  local SIGNER_INDEX=1

  local WALLET_DETAILS_JSON=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails)
  # We don't need to get min stake for now
  # WALLET_STAKE=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails | jq -r '.minStake' | xargs printf "%d * 1.1 / 1000000000\n" | bc -l) # | cut -d'.' -f1)
  local WALLET_BALANCE=$(echo "$WALLET_DETAILS_JSON" | jq -r '.balance' | xargs printf "%d")
  local TOTAL_AVAILABLE=0

  local LICENSES_JSON=$(echo "$WALLET_DETAILS_JSON" | jq -c '.licenses')
  local LICENSES_KEYS=$(echo "$LICENSES_JSON" | jq -r 'keys[]')
  for key in $LICENSES_KEYS; do
    local LICENSE=$(echo "$LICENSES_JSON" | jq -r ".[\"$key\"]")
    local isLockToStake=$(echo "$LICENSE" | jq -r '.isLockToStake')
    local isLockToStakeByWallet=$(echo "$LICENSE" | jq -r '.isLockToStakeByWallet')
    if [ "$isLockToStake" = "true" ]; then
      log "License $key is locked for staking, skipping"
      continue
    fi
    if [ "$isLockToStakeByWallet" = "true" ]; then
      log "License $key is locked for staking by Wallet, skipping"
      continue
    fi
    local balance=$(echo "$LICENSE" | jq -r '.balance' | xargs printf "%d")
    local lockStake=$(echo "$LICENSE" | jq -r '.lockStake' | xargs printf "%d")
    local lockContinue=$(echo "$LICENSE" | jq -r '.lockContinue' | xargs printf "%d")
    local lockCooler=$(echo "$LICENSE" | jq -r '.lockCooler' | xargs printf "%d")
    
    local available=$((balance - lockStake - lockContinue - lockCooler))
    if [ $available -lt 0 ]; then
      available=0
    fi
    TOTAL_AVAILABLE=$((TOTAL_AVAILABLE + available))
  done

  WALLET_STAKE=$((TOTAL_AVAILABLE / 2))
  # WALLET_STAKE=$((${WALLET_STAKE%.*} + 1))

  log "Current wallet $WALLET_ADDR balance: $WALLET_BALANCE"
  log "Sending stake: $WALLET_STAKE"

  compare=$(echo "$WALLET_BALANCE < $WALLET_STAKE" | bc)
  if [ "$compare" -eq 1 ]; then
    log "Not enough token's on the wallet"
    exit 1
  fi

  # Get signer index
  for i in `seq $SIGN_INDEX_START $SIGN_INDEX_END`; do
    local SIGNER_INDEX_TEMP=$(($RANDOM%(60000-1+1)+1))
    log "Trying signer index: $SIGNER_INDEX_TEMP"
    SIGNER_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getSignerIndexAddress "{\"index\": $SIGNER_INDEX_TEMP}" | jq -r -e '.signerIndex' || { log "Error with getting signer address" >&2 ; exit 1 ;})
    ADDRESS_DETAILS=$(tvm-cli -j account $SIGNER_ADDRESS || /bin/true)
    #ADDRESS_DETAILS_LEN=$(echo $ADDRESS_DETAILS | jq '. | length')
    ADDRESS_DETAILS_TYPE=$(echo $ADDRESS_DETAILS | jq -r '.acc_type')

    # compare=$(echo "$ADDRESS_DETAILS_LEN == 0" | bc)
    if echo $ADDRESS_DETAILS | jq -r -e '.Error' | grep -q -e '^failed to get account' || [ "$ADDRESS_DETAILS_TYPE" != "Active" ]; then
      log "Found proper signer index: $SIGNER_INDEX_TEMP"
      SIGNER_INDEX=$SIGNER_INDEX_TEMP
      break
    fi
  done

  PREV_ACTIVE_STAKES=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '.activeStakes | length' || { log "Error with getting details $WALLET_ADDR" >&2 ; exit 1 ;})
  PLACE_PARAMS="{\"bls_pubkey\": \"$BLS_PUB_KEY\", \"stake\": $WALLET_STAKE, \"signerIndex\": $SIGNER_INDEX, \"ProxyList\": {}, \"myIp\": \"$NODE_IP\"}"
  tvm-cli -j callx --addr $WALLET_ADDR --abi $WALLET_ABI --keys $NODE_OWNER_KEY --method sendBlockKeeperRequestWithStake "$PLACE_PARAMS"
  log "Waiting active stakes..."
  sleep 5

  ACTIVE_STAKES=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '.activeStakes | length')
  log "Active stakes - $ACTIVE_STAKES"

  compare=$(echo "$ACTIVE_STAKES <= $PREV_ACTIVE_STAKES" | bc)
  if [ "$compare" -eq 1 ]; then
    log "Stake request failed..."
    log "Exiting"
    exit 1
  fi
  log "Stake request successfuly accepted..."
}

place_continue_stake () {
  local SIGN_INDEX_START=1
  local SIGN_INDEX_END=60000
  local SIGNER_INDEX=1

  local WALLET_DETAILS_JSON=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails)
  # CONT_WALLET_STAKE=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails | jq -r '.minStake' | xargs printf "%d * 1.1\n" | bc | cut -d'.' -f1)
  local WALLET_BALANCE=$(echo "$WALLET_DETAILS_JSON" | jq -r '.balance' | xargs printf "%d")
  local TOTAL_AVAILABLE=0

  local LICENSES_JSON=$(echo "$WALLET_DETAILS_JSON" | jq -c '.licenses')
  local LICENSES_KEYS=$(echo "$LICENSES_JSON" | jq -r 'keys[]')
  for key in $LICENSES_KEYS; do
    local LICENSE=$(echo "$LICENSES_JSON" | jq -r ".[\"$key\"]")
    local isLockToStake=$(echo "$LICENSE" | jq -r '.isLockToStake')
    local isLockToStakeByWallet=$(echo "$LICENSE" | jq -r '.isLockToStakeByWallet')
    if [ "$isLockToStake" = "true" ]; then
      log "License $key is locked for staking, skipping"
      continue
    fi
    if [ "$isLockToStakeByWallet" = "true" ]; then
      log "License $key is locked for staking by Wallet, skipping"
      continue
    fi
    local balance=$(echo "$LICENSE" | jq -r '.balance' | xargs printf "%d")
    local lockStake=$(echo "$LICENSE" | jq -r '.lockStake' | xargs printf "%d")
    local lockContinue=$(echo "$LICENSE" | jq -r '.lockContinue' | xargs printf "%d")
    local lockCooler=$(echo "$LICENSE" | jq -r '.lockCooler' | xargs printf "%d")
    
    local available=$((balance - lockStake - lockContinue - lockCooler))
    if [ $available -lt 0 ]; then
      available=0
    fi
    TOTAL_AVAILABLE=$((TOTAL_AVAILABLE + available))
  done
  ### Not needed to send only minStake. Send full wallet balance
  ###
  # Tokens could be in Cooler almost
  # compare=$(echo "$WALLET_BALANCE < $CONT_WALLET_STAKE" | bc)
  # if [ "$compare" -eq 1 ]; then
  #   log "Not enough token's to continue staking. Skipping placing..."
  #   return 0
  # fi

  # Get signer index
  for i in `seq $SIGN_INDEX_START $SIGN_INDEX_END`; do
    local SIGNER_INDEX_TEMP=$(($RANDOM%(60000-1+1)+1))
    log "Trying signer index: $SIGNER_INDEX_TEMP"
    SIGNER_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getSignerIndexAddress "{\"index\": $SIGNER_INDEX_TEMP}" | jq -r -e '.signerIndex' || { log "Error with getting signer address to continue" >&2 ; exit 1 ;})
    ADDRESS_DETAILS=$(tvm-cli -j account $SIGNER_ADDRESS || /bin/true)
    # ADDRESS_DETAILS_LEN=$(echo $ADDRESS_DETAILS | jq '. | length')
    ADDRESS_DETAILS_TYPE=$(echo $ADDRESS_DETAILS | jq -r '.acc_type')

    # compare=$(echo "$ADDRESS_DETAILS_LEN == 0" | bc)
    if echo $ADDRESS_DETAILS | jq -r -e '.Error' | grep -q -e '^failed to get account' || [ "$ADDRESS_DETAILS_TYPE" != "Active" ]; then
      log "Found proper signer index for continue: $SIGNER_INDEX_TEMP"
      SIGNER_INDEX=$SIGNER_INDEX_TEMP
      break
    fi
  done

  update_bls_keys $BLS_KEYS_FILE
  log "Sending continue stake - $TOTAL_AVAILABLE"
  CONT_STAKING="{\"bls_pubkey\": \"$UPD_BLS_PUBLIC_KEY\", \"stake\": $TOTAL_AVAILABLE, \"seqNoStartOld\": \"$1\", \"signerIndex\": $SIGNER_INDEX, \"ProxyList\": {}}"
  tvm-cli -j callx --addr $WALLET_ADDR --abi $WALLET_ABI --keys $NODE_OWNER_KEY --method sendBlockKeeperRequestWithStakeContinue "$CONT_STAKING" || { log "Error with sending continue stake request" >&2 ; exit 1 ;}
}

calculate_reward () {
  COOLER_REWARD=$(tvm-cli -j account $1 | jq -r -e '.ecc_balance."1"' || { echo 0 ; }) # || { log "Error with getting cooler balance" >&2 ; exit 1 ;}) # | xargs printf "%s / 1000000000\n" | bc -l)
  STAKE=$(echo $2 | xargs printf "%d\n")
  REWARD=$(printf "$COOLER_REWARD - $STAKE\n" | bc -l)
}

process_cooler_epoch () {
  COOLER_SEQNO_FINISH=$(tvm-cli -j runx --abi $COOLER_ABI --addr $1 -m getDetails | jq -r -e '.seqNoFinish' || { log "Error with getting cooler seq_no finish" >&2 ; exit 1 ;})
  log "Cooler Epoch found with address \"$1\" and finish seqno \"$COOLER_SEQNO_FINISH\""
  CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no')
  if [ "$(echo "$COOLER_SEQNO_FINISH < $CUR_BLOCK_SEQ" | bc)" -ne 1 ]; then
    log "Current block seq_no is less than cooler finish block seq_no. Skipping cooler processing..."
    return 0
  fi
  log "Calculating rewards..."
  calculate_reward $1 $2
  log "Reward is $REWARD"
  # log "There is no need to touch Cooler within staking..."
  # log "Touching cooler contract"
  # tvm-cli -j callx --abi $COOLER_ABI --addr $1 -m touch
}

process_epoch () {
  EPOCH_PARAMS=$(cat $NODE_OWNER_KEY | jq -e -r '.public' || { log "Error with reading node owner public key" >&2 ; exit 1 ;})
  readarray -t ACTIVE_STAKES_ARRAY < <(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '.activeStakes | keys | .[]')
  log "Active Stakes - ${ACTIVE_STAKES_ARRAY[@]}"
  log "Stakes count - ${#ACTIVE_STAKES_ARRAY[@]}"
  if [ ${#ACTIVE_STAKES_ARRAY[@]} -le 0 ] && [ "$WILL_EPOCH_CONTINUE" = true ]; then
    log "No active stakes have been found for wallet - $WALLET_ADDR. Placing stake..." >&2
    place_stake

    readarray -t ACTIVE_STAKES_ARRAY < <(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '.activeStakes | keys | .[]')
    log "Active Stakes - ${ACTIVE_STAKES_ARRAY[@]}"
    log "Stakes count - ${#ACTIVE_STAKES_ARRAY[@]}"
    # return 0
  fi

  # Processing another cases where is no epoch or pre-epoch
  if tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -e '.activeStakes | (. != {} and all(.[] | .status | tonumber; . != 0 and . != 1))' > /dev/null 2>&1; then
    log "No active epoch and pre-epoch have been found for wallet - $WALLET_ADDR. Placing stake..." >&2
    place_stake

    readarray -t ACTIVE_STAKES_ARRAY < <(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '.activeStakes | keys | .[]')
    log "Active Stakes - ${ACTIVE_STAKES_ARRAY[@]}"
    log "Stakes count - ${#ACTIVE_STAKES_ARRAY[@]}"
  fi

  for k in ${ACTIVE_STAKES_ARRAY[@]}; do
    ACTIVE_STAKES_SEQ=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -r ".activeStakes[$k].seqNoStart")
    ACTIVE_STAKES_STAKE=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -r ".activeStakes[$k].stake")
    case $(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -r ".activeStakes[$k].status") in
      0)
        log "Pre Epoch - $k"
        CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no')
        PRE_EPOCH_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getBlockKeeperPreEpochAddress "{\"pubkey\": \"0x$EPOCH_PARAMS\", \"seqNoStart\": \"$ACTIVE_STAKES_SEQ\"}" | jq -r -e '.preEpochAddress' || { log "Error with getting pre-epoch address" >&2 ; exit 1 ;})
        log "PreEpoch contract address \"$PRE_EPOCH_ADDRESS\" and sequence start is \"$ACTIVE_STAKES_SEQ\" and current sequence is \"$CUR_BLOCK_SEQ\""
        IS_EPOCH_ACTIVE=false
        while [ "$(echo "$ACTIVE_STAKES_SEQ < $CUR_BLOCK_SEQ" | bc)" -ne 1 ]; do
          log "Current block seq_no is less than starting block seq_no. Waiting..."
          sleep 1
          CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no')
        done
        log "Touching preEpoch contract"
        tvm-cli -j callx --abi $PRE_EPOCH_ABI --addr $PRE_EPOCH_ADDRESS -m touch
        ;;
      1)
        log "Epoch in progress - $k"
        EPOCH_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getBlockKeeperEpochAddress "{\"pubkey\": \"0x$EPOCH_PARAMS\", \"seqNoStart\": \"$ACTIVE_STAKES_SEQ\"}" | jq -r -e '.epochAddress' || { log "Error with getting epoch address" >&2 ; exit 1 ;})
        EPOCH_DETAILS=$(tvm-cli -j runx --abi $EPOCH_ABI --addr $EPOCH_ADDRESS -m getDetails || { log "Error with getting epoch $EPOCH_ADDRESS details" >&2 ; exit 1 ;})
        log "There is active stake with epoch address \"$EPOCH_ADDRESS\""
        IS_EPOCH_ACTIVE=true
        IS_EPOCH_CONTINUE=$(echo $EPOCH_DETAILS | jq -r '.isContinue')
        SEQNO=$ACTIVE_STAKES_SEQ
        EPOCH_SEQNO_START=$(echo $EPOCH_DETAILS | jq -r '.seqNoStart')
        EPOCH_SEQNO_FINISH=$(echo $EPOCH_DETAILS | jq -r '.seqNoFinish')
        CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq -r '.[0].seq_no')
        if [ "$IS_EPOCH_CONTINUE" = false ] && [ "$WILL_EPOCH_CONTINUE" = true ] && [ "$(echo "$EPOCH_SEQNO_FINISH - $EPOCH_SEQNO_FINISH * 0.7 < $CUR_BLOCK_SEQ" | bc)" -eq 1 ]; then
          # if tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -e '.activeStakes | .[] | select(.status == "2")' > /dev/null 2>&1; then
          #   log "There is active Cooler in stakes"
          #   continue
          # fi
          log "Current epoch is not being continued. Sending continue stake..."
          place_continue_stake $SEQNO
        fi
        IS_EPOCH_CONTINUE=$(echo $EPOCH_DETAILS | jq -r '.isContinue')
        log "Epoch with address \"$EPOCH_ADDRESS\" is being continued: $IS_EPOCH_CONTINUE"
        CUR_BLOCK_SEQ=$(tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no')
        if [ "$(echo $EPOCH_DETAILS | jq -r '.seqNoFinish')" -le $CUR_BLOCK_SEQ ]; then
          log "Current epoch $EPOCH_ADDRESS is ready to be touched"
          tvm-cli -j callx --abi $EPOCH_ABI --addr $EPOCH_ADDRESS -m touch
        fi
        ;;
      2)
        log "Cooler Epoch - $k"
        COOLER_ADDRESS=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getBlockKeeperCoolerAddress "{\"pubkey\": \"0x$EPOCH_PARAMS\", \"seqNoStart\": \"$ACTIVE_STAKES_SEQ\"}" | jq -r -e '.value0' || { log "Error with getting cooler address" >&2 ; exit 1 ;})
        log "Cooler address - $COOLER_ADDRESS"
        process_cooler_epoch $COOLER_ADDRESS $ACTIVE_STAKES_STAKE
        ;;
    esac
  done
}

# Use when there is no active stakes
if [ $INIT_ACTIVE_STAKES -le 0 ]; then
  place_stake
fi

if [[ "$DAEMON" == true ]]; then
  while true; do
    process_epoch
    set +e
    sleep $SLEEP_TIME &
    wait $!
    set -e
  done
else
  process_epoch
fi
