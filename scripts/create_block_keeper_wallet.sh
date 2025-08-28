#!/usr/bin/env sh

set -eu

# SPONSOR_WALLET_ADDRESS=""
# SPONSOR_WALLET_KEY_FILE=""
NODE_OWNER_KEY_FILE_OUTPUT_PATH=""
LICENSE_NUMBERS=""

print_usage () {
  cat << EOF
Usage: create_block_keeper_wallet [options]

Create keys and wallet for running Acki Nacki Block Keeper.
Command line tool tvm-cli and wallet address with NACKL's are required.
To craete wallet see: https://dev.ackinacki.com/how-to-deploy-a-sponsor-wallet#create-a-wallet

REQUIRED OPTIONS:

    -w|--wallet             specify wallet address from where send tokens
    -wk|--wallet-keys       give wallet keys file
    -nk|--node-owner-keys   provide path to file with node owner keys
    -l|--license-numbers    provide license numbers for wallet. Use ',' as a delimiter for numbers without any whitespaces. For example "-l 1,2,3"

EOF
}

get_options () {
  while [ $# -gt 0 ]; do
    key="$1"
    case $key in
        -nk|--node-owner-keys)
            shift
            NODE_OWNER_KEY_FILE_OUTPUT_PATH="$1"
            shift
            ;;
        -l|--license-numbers)
            shift
            LICENSE_NUMBERS="$1"
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

if [ $# -gt 4 ] || [ $# -lt 1 ]; then
  print_usage
  exit 1
fi

if ! type tvm-cli > /dev/null 2>&1
then
  echo "tvm-cli: command not found"
  echo "To build and install tvm-cli see: https://dev.ackinacki.com/how-to-deploy-a-multisig-wallet#create-a-wallet-1" ; echo
  print_usage
  exit 1
fi

MAX_LICENSES_PER_WALLET=20

ABI=../contracts/bksystem/BlockKeeperContractRoot.abi.json
WALLET_ABI=../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json
ROOT=0:7777777777777777777777777777777777777777777777777777777777777777
LICENSE_ROOT_ABI=../contracts/bksystem/LicenseRoot.abi.json
LICENSE_ROOT_ADDR=0:4444444444444444444444444444444444444444444444444444444444444444
LICENSE_ABI=../contracts/bksystem/License.abi.json

WALLET_INIT=1000000000
ECC_KEY="1"

gen_key () {
  if [ ! -e $NODE_OWNER_KEY_FILE_OUTPUT_PATH ]; then
    echo File $NODE_OWNER_KEY_FILE_OUTPUT_PATH not found. Generating node owner keys...
    tvm-cli -j genphrase --dump $NODE_OWNER_KEY_FILE_OUTPUT_PATH > $NODE_OWNER_KEY_FILE_OUTPUT_PATH.phrase
  fi
}

read_key () {
  local NODE_OWNER_PUB_KEY_JSON=$(jq -r .public $NODE_OWNER_KEY_FILE_OUTPUT_PATH)
  local WHITELISTPARAMS=$(echo "$LICENSE_NUMBERS" | jq -R 'split(",") | map({(.): true}) | add')
  
  [ "$(echo $WHITELISTPARAMS | jq -r '. | length')" -gt $MAX_LICENSES_PER_WALLET ] && { echo "License numbers couldn't be greater than $MAX_LICENSES_PER_WALLET" ; exit 1 ; }

  NODE_OWNER_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$NODE_OWNER_PUB_KEY_JSON/g")
  NODE_OWNER_PUB_KEY_LICENSE=$(echo "{\"pubkey\": \"0x{public}\", \"whiteListLicense\": $WHITELISTPARAMS}" | sed -e "s/{public}/$NODE_OWNER_PUB_KEY_JSON/g")
}

# TVM_ACCOUNT_STATUS=$(tvm-cli -j account $SPONSOR_WALLET_ADDRESS | jq -r '.acc_type')

# if [ "$TVM_ACCOUNT_STATUS" != "Active" ]
# then
#   echo "Account status - $TVM_ACCOUNT_STATUS. It's not 'Active'."
#   exit 1
# fi

gen_key
read_key

echo Deploying wallet...

tvm-cli -j callx --addr $ROOT --abi $ABI --method deployAckiNackiBlockKeeperNodeWallet "$NODE_OWNER_PUB_KEY_LICENSE"

WALLET_ADDR=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getAckiNackiBlockKeeperNodeWalletAddress "$NODE_OWNER_PUB_KEY" | jq -r '.wallet')

echo Wallet $WALLET_ADDR is deployed.

IFS="," ; for license in $LICENSE_NUMBERS; do
  LICENSE_ADDR=$(tvm-cli -j runx --abi $LICENSE_ROOT_ABI --addr $LICENSE_ROOT_ADDR -m getLicenseAddress "{\"num\": $license}" | jq -r '.license_address')
  echo License number $license and license address is $LICENSE_ADDR
done

# Not needed for now
# ROOT_MIN_STAKE=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails | jq -r '.minStake' | xargs printf "%d / 1000000000\n" | bc -l)
# ROOT_MIN_STAKE=$((${ROOT_MIN_STAKE%.*} + 1))

# echo "Current minimum stake is $ROOT_MIN_STAKE NACKLs"

# STAKE=$(tvm-cli -j runx --abi $ABI --addr $ROOT -m getDetails | jq -r '.minStake' | xargs printf "%d * 2 * 1.1 / 1000000000\n" | bc -l)
# STAKE=$((${STAKE%.*} + 1))

# echo "Sending $STAKE NACKLs"
# SPONSOR_PARAMS="{\"dest\": \"$WALLET_ADDR\", \"value\": $WALLET_INIT, \"cc\": {\"$ECC_KEY\": $STAKE}, \"payload\": \"\", \"flags\": 0, \"bounce\": false}"

# tvm-cli -j call $SPONSOR_WALLET_ADDRESS sendTransaction "$SPONSOR_PARAMS" --abi $SPONSOR_WALLET_ABI --sign $SPONSOR_WALLET_KEY_FILE

echo "Checking wallet balance..."
WALLET_DETAILS=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails)
echo $WALLET_DETAILS
echo $WALLET_DETAILS | jq -r '.balance' | xargs printf "Current wallet balance: %d\n"

echo $WALLET_ADDR | cut -d ':' -f2 | xargs printf "Node ID: %s\n"

printf "Initial steps have been done. Save your node id\n"
