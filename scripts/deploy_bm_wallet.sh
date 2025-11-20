#!/bin/bash

NET=localhost

BM_ROOT=0:6666666666666666666666666666666666666666666666666666666666666666
BM_ROOT_ABI=contracts/0.79.3_compiled/bksystem/BlockManagerContractRoot.abi.json
BM_ROOT_KEYS=config/BlockManagerContractRoot.keys.json

BM_WALLET_ABI=contracts/0.79.3_compiled/bksystem/AckiNackiBlockManagerNodeWallet.abi.json
BM_WALLET_KEYS=config/block_manager.keys.json

BM_OWNER_WALLET_PUBKEY=0x$(cat $BM_WALLET_KEYS | jq -r .public)

SIGNING_KEYS=config/block_manager_signing.keys.json
SIGNING_PUBKEY=0x$(cat $SIGNING_KEYS | jq -r .public)

tvm-cli -j -u $NET call $BM_ROOT \
    deployAckiNackiBlockManagerNodeWallet '{"pubkey":"'$BM_OWNER_WALLET_PUBKEY'", "signingPubkey":"'$SIGNING_PUBKEY'", "whiteListLicense":{}}' \
    --abi $BM_ROOT_ABI --sign $BM_ROOT_KEYS

WALLET_ADDR=$(tvm-cli -j -u $NET run $BM_ROOT \
    getAckiNackiBlockManagerNodeWalletAddress '{"pubkey":"'$BM_OWNER_WALLET_PUBKEY'"}' \
    --abi $BM_ROOT_ABI | jq -r .wallet)

echo wallet: $WALLET_ADDR

sleep 1

WALLET_STATE=$(tvm-cli -j -u $NET account $WALLET_ADDR | jq -r .acc_type)

echo "    state: $WALLET_STATE"

tvm-cli -j -u $NET call $WALLET_ADDR \
    setSigningPubkey '{"pubkey":"'$SIGNING_PUBKEY'"}' \
    --abi $BM_WALLET_ABI --sign $BM_WALLET_KEYS
