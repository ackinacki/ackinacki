// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/VerifiersLib.sol";
import "./PopitGame.sol";
import "./PopCoinRoot.sol";
import "./Mvmultifactor.sol";
import "./Indexer.sol";

contract MobileVerifiersContractRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;
    uint32 _networkStart;
    uint32 _epochStart;
    uint32 _epochEnd;
    uint128 _prevEpochDuration = 0;
    uint128 _numberOfActiveGameAtPrevEpoch = 0;
    uint128 _numberOfActiveGameAtEpoch = 0;
    uint128 _reward_sum = 0;
    uint128 _reward_adjustment = 0;
    uint128 _reward_adjustment_prev_epoch = 0;
    uint32 _reward_last_time = 0;
    uint32 _min_reward_period = 520000;
    uint32 _reward_period = 520000;
    uint128 _sum_coef = 1;
    uint32 _calc_reward_num = 0;
    uint32 _last_tap = 0;

    uint64[] MBNLstPrev;
    uint64[] MBNLstCur;
    uint64[] GLstPrev;
    uint64[] GLstCur;
    uint64[] BCLst;

    event PopCoinRootDestroyed(string name);

    constructor (
        uint128 reward_adjustment
    ) {
        _networkStart = block.timestamp;
        _last_tap = _networkStart;
        _epochStart = _networkStart;
        _epochEnd = _epochStart + RewardPeriod;
        MBNLstPrev = new uint64[](vectorSize);
        MBNLstCur = new uint64[](vectorSize);
        GLstPrev = new uint64[](vectorSize);
        GLstCur = new uint64[](vectorSize);
        BCLst = new uint64[](vectorSize);
        _reward_adjustment = reward_adjustment;
        _reward_adjustment_prev_epoch = reward_adjustment;
    }

    function popCoinRootDestroyed(string name) public senderIs(VerifiersLib.calculatePopCoinRootAddress(_code[m_PopCoinRoot], address(this), name)) accept {
        ensureBalance();
        address addrExtern = address.makeAddrExtern(0, 0);
        emit PopCoinRootDestroyed{dest: addrExtern}(name);
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwner accept {
        ensureBalance();
        _code[id] = code;
    }

    function addGameNumber(address owner, address popcoinroot, address popcoinwallet) public senderIs(VerifiersLib.calculateGameAddress(_code[m_Game], address(this), owner, popcoinroot, popcoinwallet)) accept {
        ensureBalance();
        _numberOfActiveGameAtEpoch += 1;
    }

    function log2(uint256 x) private pure returns (uint64 n) {
        require(x >= 1, ERR_WRONG_DATA);
        if (x >= 2**128) { x >>= 128; n += 128; }
        if (x >= 2**64)  { x >>= 64;  n += 64;  }
        if (x >= 2**32)  { x >>= 32;  n += 32;  }
        if (x >= 2**16)  { x >>= 16;  n += 16;  }
        if (x >= 2**8)   { x >>= 8;   n += 8;   }
        if (x >= 2**4)   { x >>= 4;   n += 4;   }
        if (x >= 2**2)   { x >>= 2;   n += 2;   }
        if (x >= 2**1)   {             n += 1;   }
        return n;
    }

    function sendRewards(address owner, address popCoinWallet, address popcoinroot, uint64 mbiCur, uint64 mbiPrev, uint64 gCur, uint64 gPrev) public senderIs(VerifiersLib.calculateGameAddress(_code[m_Game], address(this), owner, popcoinroot, popCoinWallet)) accept {
        ensureBalance();
        MBNLstCur[mbiCur] += 1;
        GLstCur[mbiCur] += log2(gCur + 1);
        uint128 reward = gosh.calcmvreward(_prevEpochDuration, _reward_adjustment_prev_epoch, _reward_sum, gPrev, mbiPrev);
        _reward_sum += reward;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(reward);
        gosh.mintecc(uint64(reward), CURRENCIES_ID);
        VerifiersLib.calculatePopitGameAddress(_code[m_PopitGame], address(this), owner).transfer({value: 0.1 vmshell, flag: 1, currencies: data_cur});
    }

    function tapReward() private returns(uint128) {
        uint32 diff = block.timestamp - _last_tap;
        _last_tap = block.timestamp;
        uint128 reward = _reward_adjustment * diff;
        _reward_sum += reward;
        gosh.mintecc(uint64(reward), CURRENCIES_ID);
        return reward;
    }

    function sendTapRewards(string name) public senderIs(VerifiersLib.calculatePopCoinRootAddress(_code[m_PopCoinRoot], address(this), name)) accept {
        ensureBalance();
        uint128 reward = tapReward();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(reward);
        msg.sender.transfer({value: 0.1 vmshell, flag: 1, currencies: data_cur});
    }

    function sendTapRewardsPopit(string name, uint256 key, optional(string) media) public senderIs(VerifiersLib.calculatePopCoinRootAddress(_code[m_PopCoinRoot], address(this), name)) accept {
        ensureBalance();
        uint128 reward = tapReward();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(reward);
        PopCoinRoot(msg.sender).getTapReward{value: 0.1 vmshell, flag: 1, currencies: data_cur}(key, media);
    }

    function calcRewardAdjustment() private returns (uint128) {
        _reward_period = (_reward_period * _calc_reward_num + block.timestamp - _reward_last_time) / (_calc_reward_num + 1);
        _reward_last_time = block.timestamp;
        _calc_reward_num += 1;
        _reward_adjustment = gosh.calcbmmvrewardadj(_reward_sum, _reward_period, _reward_adjustment, uint128(block.timestamp - _networkStart), false);
    }

    function ensureBalance() private {
        if ((_reward_last_time + _min_reward_period < block.timestamp) || (_calc_reward_num == 0)) {
            calcRewardAdjustment();
        }
        if (block.timestamp >= _epochEnd) {
            _prevEpochDuration = block.timestamp - _epochStart;
            _epochStart = block.timestamp;
            _epochEnd = block.timestamp + RewardPeriod;
            _numberOfActiveGameAtPrevEpoch = _numberOfActiveGameAtEpoch;
            _numberOfActiveGameAtEpoch = 0;
            _reward_adjustment_prev_epoch = _reward_adjustment;
            /*
            MBNLstPrev = MBNLstCur;
            GLstPrev = GLstCur;
            GLstCur = new uint64[](vectorSize);
            MBNLstCur = new uint64[](vectorSize);
            TvmCell BCLstCell;
            (_sum_coef, BCLstCell) = gosh.calcboostcoef(abi.encode(BCLst), abi.encode(GLstPrev));
            BCLst = abi.decode(BCLstCell, (uint64[]));
            */
        }
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshellq(ROOT_BALANCE);
    }

    function updateGameCode(address owner) public senderIs(VerifiersLib.calculatePopitGameAddress(_code[m_PopitGame], address(this), owner)) accept {
        ensureBalance();
        PopitGame(msg.sender).setGameCode{value: 0.1 vmshell, flag: 1}(_code[m_Game]);
    }

    function isDeployMultifactor(
        string name,
        bool ready,
        string zkid,
        bytes proof,
        uint256 epk,
        bytes epk_sig,
        uint64 epk_expire_at,
        bytes jwk_modulus, 
        bytes kid,
        uint64 jwk_modulus_expire_at,
        uint8 index_mod_4, 
        string iss_base_64, 
        string header_base_64,
        uint256 pub_recovery_key,
        bytes pub_recovery_key_sig,
        mapping(uint256 => bytes) root_provider_certificates,
        uint256 owner_pubkey) public senderIs(VerifiersLib.calculateIndexerAddress(_code[m_Indexer], name)) accept {
        ensureBalance();
        if (!ready) { return; }
        TvmCell data = VerifiersLib.composeMultifactorStateInit(_code[m_MvMultifactor], owner_pubkey, address(this));
        new Multifactor {stateInit: data, value: varuint16(FEE_DEPLOY_MULTIFACTOR), wid: 0, flag: 1}(name, zkid, proof, epk, epk_sig, epk_expire_at, jwk_modulus, kid, jwk_modulus_expire_at, index_mod_4, iss_base_64, header_base_64, pub_recovery_key, pub_recovery_key_sig,root_provider_certificates, 0);
    }

    //Fallback/Receive
    receive() external {
    }


    //Getters
    function getPopitGameAddress(address owner) external view returns(address popitGameAddress) {
        return VerifiersLib.calculatePopitGameAddress(_code[m_PopitGame], address(this), owner);
    }

    function getMvMultifactorAddress(uint256 pubkey) external view returns(address mvMultifactorAddress) {
        return VerifiersLib.calculateMultifactorAddress(_code[m_MvMultifactor], pubkey, address(this));
    }

    function getPopCoinRootAddress(string name) external view returns(address popCoinRootAddress) {
        return VerifiersLib.calculatePopCoinRootAddress(_code[m_PopCoinRoot], address(this), name);
    }

    function getIndexerAddress(string name) external view returns(address indexerAddress) {
        return VerifiersLib.calculateIndexerAddress(_code[m_Indexer], name);
    }

    function getIndexerCode() external view returns(TvmCell data) {
        return VerifiersLib.buildIndexerCode(_code[m_Indexer]);
    }

    function getCodes() external view returns(mapping(uint8 => TvmCell) code) {
        return _code;
    }

    function getEpoch() external view returns(uint32 epochStart, uint32 epochEnd) {
        return (_epochStart, _epochEnd);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "MobileVerifiersContractRoot");
    }
}
