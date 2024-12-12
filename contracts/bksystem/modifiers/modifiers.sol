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
    uint8 constant m_BlockKeeperSlashCode = 6;
    
    //Deploy constants
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_WALLET = 20 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET = 30 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET = 10 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET = 2 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_SLASH = 4 vmshell;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST = 10 vmshell;

    uint8 constant PRE_EPOCH_DEPLOYED = 0;
    uint8 constant EPOCH_DEPLOYED = 1;
    uint8 constant COOLER_DEPLOYED = 2;

    uint32 constant CURRENCIES_ID = 1;
    uint32 constant CURRENCIES_ID_SHELL = 2;

    uint128 constant MAX_LOCK_NUMBER = 1000000000;
            
    modifier onlyOwnerPubkeyArray(uint256[] rootpubkeys) {
        uint256 zero;
        bool res = false;
        for (uint256 val : rootpubkeys) {
            if (val == zero) {
                continue;
            }
            if (msg.pubkey() == val) {
                res = true;
                break;
            }
        }
        require(res == true, ERR_NOT_OWNER);
        _;
    }

    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }
    
    
    modifier onlyOwnerAddress(address addr) {
        require(msg.sender == addr, ERR_NOT_OWNER);
        _;
    }

    modifier onlyOwnerCombine(optional(address) addr, optional(uint256) service_pubkey, uint256 rootpubkey) {
        if (msg.pubkey() != rootpubkey) {
            if (service_pubkey.hasValue()) {
                if (msg.pubkey() != service_pubkey.get()) {
                    require(addr.hasValue(), ERR_NOT_OWNER);
                    require(msg.sender == addr.get(), ERR_NOT_OWNER);  
                }
            }
        }
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
