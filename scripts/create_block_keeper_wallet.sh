#!/usr/bin/env sh

set -eu

SPONSOR_WALLET_ADDRESS=""
SPONSOR_WALLET_KEY_FILE=""
MASTER_KEY_FILE_OUTPUT_PATH=""
SERVICE_KEY_FILE_OUTPUT_PATH=""

print_usage () {
  cat << EOF
Usage: create_block_keeper_wallet [options]

Create keys and wallet for running Acki Nacki Block Keeper.
Command line tool tvm-cli and wallet address with NACKL's are required.
To craete wallet see: https://dev.ackinacki.com/how-to-deploy-a-sponsor-wallet#create-a-wallet

REQUIRED OPTIONS:

    -w|--wallet        specify wallet address from where send tokens
    -wk|--wallet-keys  give wallet keys file
    -m|--master-keys   provide path to file with master keys
    -s|--service-keys  specify path to service keys file

EOF
}

get_options () {
  while [ $# -gt 0 ]; do
    key="$1"
    case $key in
        -w|--wallet)
            shift
            SPONSOR_WALLET_ADDRESS="$1"
            shift
            ;;
        -wk|--wallet-keys)
            shift
            SPONSOR_WALLET_KEY_FILE="$1"
            shift
            ;; 
        -m|--master-keys)
            shift
            MASTER_KEY_FILE_OUTPUT_PATH="$1"
            shift
            ;;
        -s|--service-keys)
            shift
            SERVICE_KEY_FILE_OUTPUT_PATH="$1"
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown options: $1"
            exit 1
            ;;
    esac
  done
}

get_options $*

if [ $# -gt 8 ] || [ $# -lt 1 ]; then
  print_usage
  exit 1
fi

if ! type tvm-cli > /dev/null 2>&1
then
  echo "tvm-cli: command not found"
  echo "To build and install tvm-cli see: https://dev.ackinacki.com/how-to-deploy-a-sponsor-wallet#create-a-wallet" ; echo
  print_usage
  exit 1
fi

ABI=../contracts/bksystem/BlockKeeperContractRoot.abi.json
WALLET_ABI=../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json
ELECTION=0:7777777777777777777777777777777777777777777777777777777777777777
SPONSOR_WALLET_ABI="../contracts/giver/GiverV3.abi.json"

WALLET_INIT_CC=$((10 * 2))
ECC_KEY="1"

gen_key () {
  if [ ! -e $MASTER_KEY_FILE_OUTPUT_PATH ]; then
    echo $MASTER_KEY_FILE_OUTPUT_PATH not found. Generating master keys...
    tvm-cli -j genphrase --dump $MASTER_KEY_FILE_OUTPUT_PATH > $MASTER_KEY_FILE_OUTPUT_PATH.phrase
  fi

  if [ ! -e $SERVICE_KEY_FILE_OUTPUT_PATH ]; then
    echo $SERVICE_KEY_FILE_OUTPUT_PATH not found. Generating service keys...
    tvm-cli -j genphrase --dump $SERVICE_KEY_FILE_OUTPUT_PATH > $SERVICE_KEY_FILE_OUTPUT_PATH.phrase
  fi
}

read_key () {
  local MASTER_PUB_KEY_JSON=$(jq .public $MASTER_KEY_FILE_OUTPUT_PATH | tr -d '\"')
  MASTER_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$MASTER_PUB_KEY_JSON/g")

  SERVICE_PUB_KEY_JSON=$(jq .public $SERVICE_KEY_FILE_OUTPUT_PATH | tr -d '\"')
  SERVICE_PUB_KEY=$(echo '{"key": "0x{public}"}' | sed -e "s/{public}/$SERVICE_PUB_KEY_JSON/g")
}

TVM_ACCOUNT_STATUS=$(tvm-cli -j account 0:7777777777777777777777777777777777777777777777777777777777777777 | jq '.acc_type' | tr -d '\"')

if [ "$TVM_ACCOUNT_STATUS" != "Active" ]
then
  echo "Account status - $TVM_ACCOUNT_STATUS. It's not 'Active'."
  exit 1
fi

gen_key
read_key

tvm-cli -j callx --addr $ELECTION --abi $ABI --method deployAckiNackiBlockKeeperNodeWallet "$MASTER_PUB_KEY"

WALLET_ADDR=$(tvm-cli -j runx --abi $ABI --addr $ELECTION -m getAckiNackiBlockKeeperNodeWalletAddress "$MASTER_PUB_KEY" | jq '.value0' | tr -d '\"')

echo Wallet $WALLET_ADDR is deployed.

tvm-cli -j callx --addr $WALLET_ADDR --abi $WALLET_ABI --keys $MASTER_KEY_FILE_OUTPUT_PATH --method setServiceKey "$SERVICE_PUB_KEY" && echo Wallet key has been added.

SERVICE_WALLET_DETAILS=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '.service_key' | tr -d '\"')

if [ "0x$SERVICE_PUB_KEY_JSON" = "$SERVICE_WALLET_DETAILS" ]
then
  echo "Service wallet key match local key."
else
  echo "Key doesn't match. Exiting..."
  exit 1
fi

ELECTION_MIN_STAKE=$(tvm-cli -j runx --abi $ABI --addr 0:7777777777777777777777777777777777777777777777777777777777777777 -m getDetails | jq '.minStake' | tr -d '\"')
echo "Current minimum stake is $ELECTION_MIN_STAKE"

ELECTION_STAKE=$(tvm-cli -j runx --abi $ABI --addr 0:7777777777777777777777777777777777777777777777777777777777777777 -m getDetails | jq '.minStake' | tr -d '\"' | xargs printf "%d * 1.1\n" | bc | cut -d'.' -f1)

echo "Sending $ELECTION_STAKE tokens..."
GIVER_PARAMS="{\"dest\": \"$WALLET_ADDR\", \"value\": $ELECTION_STAKE, \"ecc\": {\"$ECC_KEY\": $WALLET_INIT_CC}}"

tvm-cli -j callx --addr $SPONSOR_WALLET_ADDRESS --abi $SPONSOR_WALLET_ABI --keys $SPONSOR_WALLET_KEY_FILE --method sendCurrency "$GIVER_PARAMS"

echo "Checking wallet balance..."
WALLET_DETAILS=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '{balance,walletId}')
echo $WALLET_DETAILS | jq '.balance' | tr -d '\"' | xargs printf "Current wallet balance: %d\n"
echo $WALLET_DETAILS | jq '.walletId' | tr -d '\"' | xargs printf "Node ID: %d\n"

printf "Initial steps have been done. Save your node id\n"
