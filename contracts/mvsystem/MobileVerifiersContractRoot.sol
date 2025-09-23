/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
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
        _reward_last_time = _networkStart;
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
        }
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshellq(ROOT_BALANCE);
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

    function getReward(
        uint128 mbi,
        uint64[] MBNLst,
        uint64[] TAPLst,
        uint64 value,
        uint128 basicRewards
    ) external pure returns(uint128) {
        TvmCell data1 = abi.encode(TAPLst);
        TvmCell data2 = abi.encode(MBNLst);
        return gosh.calcmvreward(mbi, data2, data1, value + 1, basicRewards);
    }
}
