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
ROOT=0:7777777777777777777777777777777777777777777777777777777777777777
SPONSOR_WALLET_ABI="../contracts/multisig/multisig.abi.json"

WALLET_INIT=1000000000
ECC_KEY="1"

gen_key () {
  if [ ! -e $MASTER_KEY_FILE_OUTPUT_PATH ]; then
    echo File $MASTER_KEY_FILE_OUTPUT_PATH not found. Generating master keys...
    tvm-cli -j genphrase --dump $MASTER_KEY_FILE_OUTPUT_PATH > $MASTER_KEY_FILE_OUTPUT_PATH.phrase
  fi

  if [ ! -e $SERVICE_KEY_FILE_OUTPUT_PATH ]; then
    echo File $SERVICE_KEY_FILE_OUTPUT_PATH not found. Generating service keys...
    tvm-cli -j genphrase --dump $SERVICE_KEY_FILE_OUTPUT_PATH > $SERVICE_KEY_FILE_OUTPUT_PATH.phrase
  fi
}

read_key () {
  local MASTER_PUB_KEY_JSON=$(jq -r .public $MASTER_KEY_FILE_OUTPUT_PATH)
  MASTER_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$MASTER_PUB_KEY_JSON/g")

  SERVICE_PUB_KEY_JSON=$(jq -r .public $SERVICE_KEY_FILE_OUTPUT_PATH)
  SERVICE_PUB_KEY=$(echo '{"key": "0x{public}"}' | sed -e "s/{public}/$SERVICE_PUB_KEY_JSON/g")
}

TVM_ACCOUNT_STATUS=$(tvm-cli -j account $SPONSOR_WALLET_ADDRESS | jq -r '.acc_type')

if [ "$TVM_ACCOUNT_STATUS" != "Active" ]
then
  echo "Account status - $TVM_ACCOUNT_STATUS. It's not 'Active'."
  exit 1
fi

gen_key
read_key

tvm-cli -j callx --addr $ROOT --abi $ABI --method deployAckiNackiBlockKeeperNodeWallet "$MASTER_PUB_KEY"

WALLET_ADDR=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getAckiNackiBlockKeeperNodeWalletAddress "$MASTER_PUB_KEY" | jq -r '.value0')

echo Wallet $WALLET_ADDR is deployed.

tvm-cli -j callx --addr $WALLET_ADDR --abi $WALLET_ABI --keys $MASTER_KEY_FILE_OUTPUT_PATH --method setServiceKey "$SERVICE_PUB_KEY" && echo Wallet service key has been added.

SERVICE_WALLET_DETAILS=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq -r '.service_key')

if [ "0x$SERVICE_PUB_KEY_JSON" = "$SERVICE_WALLET_DETAILS" ]
then
  echo "Service wallet key match local key."
else
  echo "Key doesn't match. Exiting..."
  exit 1
fi

ROOT_MIN_STAKE=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails | jq -r '.minStake' | xargs printf "%d / 1000000000\n" | bc -l)
ROOT_MIN_STAKE=$((${ROOT_MIN_STAKE%.*} + 1))

echo "Current minimum stake is $ROOT_MIN_STAKE NACKLs"

STAKE=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails | jq -r '.minStake' | xargs printf "%d * 2 / 1000000000\n" | bc -l)
STAKE=$((${STAKE%.*} + 1))

echo "Sending $STAKE NACKLs"
SPONSOR_PARAMS="{\"dest\": \"$WALLET_ADDR\", \"value\": $WALLET_INIT, \"cc\": {\"$ECC_KEY\": $STAKE}, \"payload\": \"\", \"flags\": 0, \"bounce\": false}"

tvm-cli -j call $SPONSOR_WALLET_ADDRESS sendTransaction "$SPONSOR_PARAMS" --abi $SPONSOR_WALLET_ABI --sign $SPONSOR_WALLET_KEY_FILE

echo "Checking wallet balance..."
WALLET_DETAILS=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails | jq '{balance,walletId}')
echo $WALLET_DETAILS | jq -r '.balance' | xargs printf "Current wallet balance: %d\n"
echo $WALLET_DETAILS | jq -r '.walletId' | xargs printf "Node ID: %d\n"

printf "Initial steps have been done. Save your node id\n"
