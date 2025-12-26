/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

import "./structs/structs.sol";
import "./errors.sol";

abstract contract Modifiers is Errors {
    string constant versionModifiers = "1.0.0";

    //TvmCell constants
    uint8 constant m_PopitGame = 1;
    uint8 constant m_PopCoinWallet = 2;
    uint8 constant m_PopCoinRoot = 4;
    uint8 constant m_MvMultifactor = 5;
    uint8 constant m_Indexer = 6;
    uint8 constant m_Boost = 7;
    uint8 constant m_Miner = 8;
    uint8 constant m_Mirror = 9;

    //Deploy constants
    uint64 constant FEE_DEPLOY_POPIT_GAME_WALLET = 51 vmshell;
    uint64 constant FEE_DEPLOY_POP_COIN_WALLET = 12 vmshell;
    uint64 constant FEE_DEPLOY_POP_COIN_ROOT = 14 vmshell;
    uint64 constant FEE_DEPLOY_MULTIFACTOR = 15 vmshell;
    uint64 constant FEE_DEPLOY_INDEXER = 16 vmshell;
    uint64 constant FEE_DEPLOY_BOOST = 17 vmshell;
    uint64 constant FEE_DEPLOY_MINER = 18 vmshell;

    uint64 constant BUSY_BLOCKS = 10;

    uint64 constant ROOT_BALANCE = 200 vmshell;
    uint64 constant CONTRACT_BALANCE = 100 vmshell;
    uint64 constant MINER_BALANCE = 50 vmshell;

    uint32 constant CURRENCIES_ID = 1;
    uint32 constant CURRENCIES_ID_SHELL = 2;

    uint32 constant RewardPeriod = 86400;

    uint32 constant vectorSize = 200;
    uint32 constant TAP_DENOMINATOR = 100;

    string constant WASM_MODULE = "docs:tlschecker/tls-check-interface@0.1.0";
    string constant WASM_FUNCTION = "tlscheck";
    bytes constant WASM_BINARY = "";

    string constant WASM_MINER_MODULE = "docs:bee-engine/verifier-interface@0.1.0";
    string constant WASM_MINER_FUNCTION = "verify";

    uint64 constant BIG_TAP = 12000;
    uint64 constant SMALL_TAP = 70;
    uint64 constant MINING_DUR_TAP = 330;
    

    uint8   constant MAX_QUEUED_REQUESTS =  20;
    uint64  constant EXPIRATION_TIME = 3601; // lifetime is 1 hour
    uint64  constant MIN_EPK_LIFE_TIME = 300; //5 min             //60; // lifetime is 1 min
    uint64  constant MAX_EPK_LIFE_TIME = 15552000; // 180 days 
    uint64  constant MIN_JWK_LIFE_TIME = 300;  // 5 min          //3601;
    uint64  constant MAX_JWK_LIFE_TIME = 21600;  // 6 hours 
    uint8   constant MAX_CARDS = 5;
    uint8   constant MAX_NUM_OF_FACTORS = 10;
    uint8   constant NUMBER_OF_FACTORS_TO_CLEAR = 5;
    uint8   constant MAX_NUM_OF_JWK = 12;
    uint8   constant MAX_LEN = 20;
    uint8   constant MAX_LEN_TAPS = 10;
    uint8   constant MAX_LEN_EVENT = 5;
    uint8   constant MAX_LEN_WASM = 6;
    uint128 constant MAX_MIRROR_INDEX = 1000;
    uint128 constant MAX_PUBKEY_SIZE = 100;

    uint constant bitCntAddress = 256;


    uint256 constant BASE_PART = 0x2;
    uint256 constant SHIFT = 2 ** 252;

    uint128 constant PopitGameDeployedEmit = 1;
    uint128 constant MultifactorDeployedEmit = 2;
    uint128 constant PopCoinRootDeployedEmit = 3;
    uint128 constant MinerDeployedEmit = 4;
    uint128 constant MinerIntervalEmit = 5;
    uint128 constant MinerNewSeedEmit = 6;
    uint128 constant MinerNewComplexityEmit = 7;
    uint128 constant BoostNewVersionEmit = 8;
    uint128 constant BoostNewHasSimpleWalletEmit = 9;

    uint64 constant MinerRewardPeriod = 1000;
    uint64 constant MinerRewardDelay = 262000;
    uint64 constant MinerTapDelay = 262000;

    uint64 constant IntervalRadius = 20;

    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }

    modifier onlyOwnerPubkeyOpt(optional(uint256) rootpubkey) {
        require(rootpubkey.hasValue(), ERR_NOT_READY);
        require(msg.pubkey() == rootpubkey.get(), ERR_NOT_OWNER);
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
}
