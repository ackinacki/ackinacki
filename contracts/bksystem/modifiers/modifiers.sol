// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;

import "./replayprotection.sol";
import "./structs/structs.sol";

abstract contract Modifiers is ReplayProtection {   
    string constant versionModifiers = "1.0.0";
    
    //TvmCell constants
    uint8 constant m_BlockKeeperEpochCode = 1;
    uint8 constant m_AckiNackiBlockKeeperNodeWalletCode = 2;
    uint8 constant m_BlockKeeperEpochCoolerCode = 3;
    uint8 constant m_BlockKeeperPreEpochCode = 4;
    uint8 constant m_BlockKeeperEpochProxyListCode = 5;
    uint8 constant m_BLSKeyCode = 6;
    uint8 constant m_SignerIndexCode = 7;
    uint8 constant m_LicenseCode = 8;
    uint8 constant m_LicenseBMCode = 9;
    uint8 constant m_AckiNackiBlockManagerNodeWalletCode = 10;
    
    //Deploy constants
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_WALLET = 20 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_MANAGER_WALLET = 21 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET = 30 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET = 10 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET = 2 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_SLASH = 4 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST = 10 vmshell;
    uint64 constant FEE_DEPLOY_BLS_KEY = 3 vmshell;
    uint64 constant FEE_DEPLOY_LICENSE = 6 vmshell;
    uint64 constant FEE_DEPLOY_LICENSE_BM = 5 vmshell;
    uint64 constant FEE_DEPLOY_SIGNER_INDEX = 7 vmshell;
    uint64 constant FEE_DEPLOY_NAME_INDEX = 8 vmshell;
    uint64 constant ROOT_BALANCE = 1000000 vmshell;

    uint128 constant MIN_REP_COEF = 1000000000;
    
    uint8 constant MAX_LICENSE_NUMBER = 10;

    uint8 constant PRE_EPOCH_DEPLOYED = 0;
    uint8 constant EPOCH_DEPLOYED = 1;
    uint8 constant COOLER_DEPLOYED = 2;

    uint32 constant CURRENCIES_ID = 1;
    uint32 constant CURRENCIES_ID_SHELL = 2;

    uint8 constant FULL_STAKE_SLASH = 0;
    uint8 constant FULL_STAKE_PERCENT = 100;

    uint8 constant LICENSE_REST = 0;
    uint8 constant LICENSE_PRE_EPOCH = 1;
    uint8 constant LICENSE_EPOCH = 2;
    uint8 constant LICENSE_CONTINUE = 3;

    uint128 constant MAX_LOCK_NUMBER = 1000000000;

    uint32 constant UNLOCK_LICENSES = 63072000;

    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }

    modifier onlyOwnerWallet(optional(address) owner_wallet, uint256 rootpubkey) {
        if (msg.pubkey() != rootpubkey) {
            require(owner_wallet.hasValue(), ERR_NOT_OWNER);
            require(msg.sender == owner_wallet.get(), ERR_NOT_OWNER);
        }
        _;
    }    

    modifier onlyOwnerWalletOpt(optional(address) owner_wallet, optional(uint256) rootpubkey) {
        if (rootpubkey.hasValue()) {
            require(msg.pubkey() == rootpubkey.get(), ERR_NOT_OWNER);
        } else {
            require(owner_wallet.hasValue(), ERR_NOT_OWNER);
            require(msg.sender == owner_wallet.get(), ERR_NOT_OWNER);
        }
        _;
    }  
    
    modifier onlyOwnerAddress(address addr) {
        require(msg.sender == addr, ERR_NOT_OWNER);
        _;
    }
    
    modifier minValue(uint128 val) {
        require(msg.value >= val, ERR_LOW_VALUE);
        _;
    }
    
    modifier senderIs(address sender) {
        require(msg.sender == sender, ERR_INVALID_SENDER);
        _;
    }
    
    modifier minBalance(uint128 val) {
        require(address(this).balance > val + 1 vmshell, ERR_LOW_BALANCE);
        _;
    }
}
