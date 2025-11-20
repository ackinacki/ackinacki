#!/usr/bin/env bash

set -euo pipefail

if ! type tvm-cli > /dev/null 2>&1
then
  echo "tvm-cli: command not found"
  echo "To build and install tvm-cli see: https://dev.ackinacki.com/how-to-deploy-a-multisig-wallet#create-a-wallet-1" ; echo
  exit 1
fi

BM_OWNER_KEY_FILE_OUTPUT_PATH=$1
BM_SIGNING_KEY_FILE_OUTPUT_PATH=$2
LICENSE_NUMBERS=$3
TVM_ENDPOINT=$4

MAX_LICENSES_PER_WALLET=20

ABI="../../contracts/0.79.3_compiled/bksystem/BlockManagerContractRoot.abi.json"
WALLET_ABI="../../contracts/0.79.3_compiled/bksystem/AckiNackiBlockManagerNodeWallet.abi.json"
ROOT=0:7777777777777777777777777777777777777777777777777777777777777777
LICENSE_ROOT_ABI=../../contracts/0.79.3_compiled/bksystem/LicenseRoot.abi.json
LICENSE_ROOT_ADDR=0:4444444444444444444444444444444444444444444444444444444444444444
LICENSE_ABI=../../contracts/0.79.3_compiled/bksystem/License.abi.json
BMSYSTEM_ROOT="0:6666666666666666666666666666666666666666666666666666666666666666"

if [[ -z "$TVM_ENDPOINT" ]]; then
  echo "TVM endpoint has not been set"
  exit 1
fi

tvm-cli config --url $TVM_ENDPOINT

gen_key () {
  if [ ! -e $1 ]; then
    echo File $1 not found. Generating block manager keys...
    tvm-cli -j genphrase --dump $1 > $1.phrase
  fi
}

read_key () {
  local BM_OWNER_PUB_KEY_JSON=$(jq -r .public $BM_OWNER_KEY_FILE_OUTPUT_PATH)
  local BM_SIGNING_PUB_KEY_JSON=$(jq -r .public $BM_SIGNING_KEY_FILE_OUTPUT_PATH)
  local WHITELISTPARAMS=$(echo "$LICENSE_NUMBERS" | jq -R 'split(",") | map({(.): true}) | add')
  
  [ "$(echo $WHITELISTPARAMS | jq -r '. | length')" -gt $MAX_LICENSES_PER_WALLET ] && { echo "License numbers couldn't be greater than $MAX_LICENSES_PER_WALLET" ; exit 1 ; }

  BM_OWNER_PUB_KEY=$(echo '{"pubkey": "0x{public}"}' | sed -e "s/{public}/$BM_OWNER_PUB_KEY_JSON/g")  
  BM_OWNER_PUB_KEY_PARAMS=$(echo "{\"pubkey\": \"0x{public}\", \"signerPubkey\": \"0x{signer_public}\", \"whiteListLicense\": $WHITELISTPARAMS}" | sed -e "s/{public}/$BM_OWNER_PUB_KEY_JSON/g" | sed -e "s/{signer_public}/$BM_SIGNING_PUB_KEY_JSON/g")
}

gen_key $BM_OWNER_KEY_FILE_OUTPUT_PATH
gen_key $BM_SIGNING_KEY_FILE_OUTPUT_PATH
read_key

echo Deploying Block Manager wallet...

tvm-cli -j callx --addr $BMSYSTEM_ROOT --abi $ABI --method deployAckiNackiBlockManagerNodeWallet "$BM_OWNER_PUB_KEY_PARAMS"

sleep 3

WALLET_ADDR=$(tvm-cli -j runx --abi $ABI --addr $BMSYSTEM_ROOT -m getAckiNackiBlockManagerNodeWalletAddress "$BM_OWNER_PUB_KEY" | jq -r '.wallet')

echo "Block Manager wallet $WALLET_ADDR is deployed."

IFS="," ; for license in $LICENSE_NUMBERS; do
  LICENSE_ADDR=$(tvm-cli -j runx --abi $LICENSE_ROOT_ABI --addr $LICENSE_ROOT_ADDR -m getLicenseBMAddress "{\"num\": $license}" | jq -r '.license_address')
  echo License number $license and license address is $LICENSE_ADDR
done

echo "Checking Block Manager wallet..."
WALLET_DETAILS=$(tvm-cli -j runx --abi $WALLET_ABI --addr $WALLET_ADDR -m getDetails)
echo "$WALLET_DETAILS"
