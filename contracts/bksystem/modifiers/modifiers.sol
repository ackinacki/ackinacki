// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;

import "./replayprotection.sol";
import "./structs/structs.sol";

function getreward(
    uint128 active_bk,
    uint128 bked, 
    uint128 t,
    uint128 totalbkstake,
    uint128 bkstake,
    uint128 bkrt
    ) assembly pure returns(uint128) {
    ".blob xc729"
}

function exchange(uint64 stake) assembly pure {
    ".blob xC727"
}

function mint(uint64 stake, uint32 key) assembly pure {
    ".blob xC726"
}

function mintshell(uint64 value) assembly pure {
    ".blob xC728"
}

function getminstake(
    uint128 bkpd,
    uint128 t,
    uint128 bk_num,
    uint128 need_bk_num
    ) assembly pure returns(uint128) {
    ".blob xc730"
}

abstract contract Modifiers is ReplayProtection {   
    string constant versionModifiers = "1.0.0";
    
    //TvmCell constants
    uint8 constant m_BlockKeeperEpochCode = 1;
    uint8 constant m_AckiNackiBlockKeeperNodeWalletCode = 2;
    uint8 constant m_BlockKeeperEpochCoolerCode = 3;
    uint8 constant m_BlockKeeperPreEpochCode = 4;
    uint8 constant m_BlockKeeperSlashCode = 5;
    
    //Deploy constants
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_WALLET = 20 ton;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET = 30 ton;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET = 10 ton;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET = 2 ton;
    uint64 constant FEE_DEPLOY_BLOCK_KEEPER_SLASH = 4 ton;

    uint8 constant PRE_EPOCH_DEPLOYED = 0;
    uint8 constant EPOCH_DEPLOYED = 1;
    uint8 constant COOLER_DEPLOYED = 2;

    uint32 constant CURRENCIES_ID = 1;
    uint32 constant CURRENCIES_ID_SHELL = 2;
            
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
    
    modifier minValue(uint128 val) {
        require(msg.value >= val, ERR_LOW_VALUE);
        _;
    }
    
    modifier senderIs(address sender) {
        require(msg.sender == sender, ERR_INVALID_SENDER);
        _;
    }
    
    modifier minBalance(uint128 val) {
        require(address(this).balance > val + 1 ton, ERR_LOW_BALANCE);
        _;
    }
}
