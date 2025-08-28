// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;

import "./structs/structs.sol";
import "./errors.sol";

abstract contract Modifiers is Errors {
    string constant versionModifiers = "1.0.0";

    //TvmCell constants
    uint8 constant m_PopitGame = 1;
    uint8 constant m_PopCoinWallet = 2;
    uint8 constant m_Game = 3;
    uint8 constant m_PopCoinRoot = 4;
    uint8 constant m_MvMultifactor = 5;
    uint8 constant m_Indexer = 6;
    uint8 constant m_Boost = 7;

    //Deploy constants
    uint64 constant FEE_DEPLOY_POPIT_GAME_WALLET = 51 vmshell;
    uint64 constant FEE_DEPLOY_POP_COIN_WALLET = 12 vmshell;
    uint64 constant FEE_DEPLOY_GAME = 13 vmshell;
    uint64 constant FEE_DEPLOY_POP_COIN_ROOT = 14 vmshell;
    uint64 constant FEE_DEPLOY_MULTIFACTOR = 15 vmshell;
    uint64 constant FEE_DEPLOY_INDEXER = 16 vmshell;
    uint64 constant FEE_DEPLOY_BOOST = 17 vmshell;

    uint64 constant BUSY_BLOCKS = 10;

    uint64 constant ROOT_BALANCE = 200 vmshell;
    uint64 constant CONTRACT_BALANCE = 100 vmshell;

    uint32 constant CURRENCIES_ID = 1;
    uint32 constant CURRENCIES_ID_SHELL = 2;

    uint32 constant RewardPeriod = 86400;

    uint32 constant vectorSize = 200;
    uint32 constant TAP_DENOMINATOR = 100;

    uint8   constant MAX_QUEUED_REQUESTS =  20;
    uint64  constant EXPIRATION_TIME = 3601; // lifetime is 1 hour
    uint64  constant MIN_EPK_LIFE_TIME = 300; //5 min             //60; // lifetime is 1 min
    uint64  constant MAX_EPK_LIFE_TIME = 2592000; // 30 days      //3601; //
    uint64  constant MIN_JWK_LIFE_TIME = 300;  // 5 min          //3601;
    uint8   constant MAX_CARDS = 5;
    uint8   constant MAX_NUM_OF_FACTORS = 10;
    uint8   constant NUMBER_OF_FACTORS_TO_CLEAR = 5;
    uint8   constant NUMBER_OF_JWK_TO_CLEAR = 5;
    uint8   constant MAX_NUM_OF_JWK = 12;
    uint128 constant MAX_MIRROR_INDEX = 1000;


    uint256 constant BASE_PART = 0x2;
    uint256 constant SHIFT = 2 ** 252;

    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }


    modifier onlyOwner {
        require(msg.pubkey() == tvm.pubkey(), ERR_NOT_OWNER);
        _;
    }

    modifier accept() {
        tvm.accept();
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

    function checkName(string name) internal pure returns(bool) {
        bytes bStr = bytes(name);
        if (bStr.length == 0) { return false; }
        if (bStr.length > 39) { return false; }
        for (uint i = 0; i < bStr.length; i++) {
            if ((uint8(bStr[i]) >= 97) && (uint8(bStr[i]) <= 122)) { continue; }
            if ((uint8(bStr[i]) >= 48) && (uint8(bStr[i]) <= 57)) { continue; }
            if (i != 0) {
            	if ((uint8(bStr[i]) == 95) && (uint8(bStr[i - 1]) != 95)) { continue; }
            	if ((uint8(bStr[i]) == 45) && (uint8(bStr[i - 1]) != 45)) {  continue; }
            }
            return false;
        }
        return true;
    }
}
