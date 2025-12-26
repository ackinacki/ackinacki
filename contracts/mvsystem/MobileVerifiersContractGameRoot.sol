/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/VerifiersLib.sol";
import "./Miner.sol";

contract MobileVerifiersContractGameRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;

    event Rewarded(address popitGame, uint128 reward);
    event RewardedPopitGame(uint128 reward);

    uint64 _epochStart;

    mapping(uint64 => TapData) _tapData;

    uint128 _reward_sum = 0;
    uint128 _reward_adjustment = 0;
    uint32 _reward_last_time = 0;
    uint32 _min_reward_period = 520000;
    uint32 _reward_period = 520000;
    uint32 _calc_reward_num = 0;
    uint32 _networkStart;

    uint32 _easyComplexity;
    uint32 _hardComplexity;

    constructor (
        uint128 reward_adjustment
    ) {
        (, uint64 r) = math.divmod(block.seqno, MinerRewardPeriod); 
        _networkStart = block.timestamp;
        _reward_last_time = _networkStart;
        _epochStart = block.seqno - r; 
        _easyComplexity = 10;
        _hardComplexity = 11;
        _reward_adjustment = reward_adjustment;
    }

    function setNewCode(uint8 id, TvmCell code) public senderIs(address(this)) accept {
        ensureBalance();
        _code[id] = code;
    }

    function setConfig(uint128 reward_sum, uint128 reward_adjustment, uint32 reward_period, uint32 min_reward_period, uint32 reward_last_time, uint32 calc_reward_num) public senderIs(address(this)) accept {
        _reward_period = reward_period;
        _min_reward_period = min_reward_period;
        _reward_last_time = reward_last_time;
        _calc_reward_num = calc_reward_num;
        _reward_sum = reward_sum;
        _reward_adjustment = reward_adjustment;
    }

    function setComplexity(uint32 easyComplexity, uint32 hardComplexity) public onlyOwner accept {
        ensureBalance();
        require(easyComplexity != 0, ERR_WRONG_DATA);
        require(hardComplexity != 0, ERR_WRONG_DATA);
        _easyComplexity = easyComplexity;
        _hardComplexity = hardComplexity;
    }

    function destroyNode() public senderIs(address(this)) accept {
        selfdestruct(address(this));
    }

    function getTap(uint64 tap, uint64 tapEpochStart, address owner, uint64 mbiCur, uint32 easyComplexity, uint32 hardComplexity) public senderIs(VerifiersLib.calculateMinerGameAddress(_code[m_Miner], owner)) accept {
        ensureBalance();
        if ((easyComplexity != _easyComplexity) || (hardComplexity != _hardComplexity)) {
            Miner(msg.sender).setComplexity{value: 0.1 vmshell, flag: 1}(_easyComplexity, _hardComplexity);
        }
        (, uint64 r) = math.divmod(block.seqno, MinerRewardPeriod); 
        uint64 epochStart = block.seqno - r;
        if (_epochStart < epochStart){
            _epochStart = epochStart;
            _tapData[_epochStart] = TapData(new uint64[](vectorSize), _reward_adjustment * MINING_DUR_TAP); 
            optional(uint64, TapData) oldData = _tapData.min();
            if (oldData.hasValue()) {
                (uint64 data,) = oldData.get();
                if (data + MinerRewardDelay < _epochStart) {
                    delete _tapData[data];
                }
            }
        }
        if (_tapData.exists(tapEpochStart) == false) { 
            _tapData[tapEpochStart] = TapData(new uint64[](vectorSize), _reward_adjustment * MINING_DUR_TAP); 
        }
        _tapData[tapEpochStart].tapData[mbiCur] += tap;
    }

    function tapReward(TvmCell tapData, uint64 epochStart, address popitGame, address owner, uint32 easyComplexity, uint32 hardComplexity) public senderIs(VerifiersLib.calculateMinerGameAddress(_code[m_Miner], owner)) accept {
        ensureBalance(); 
        if ((easyComplexity != _easyComplexity) || (hardComplexity != _hardComplexity)) {
            Miner(msg.sender).setComplexity{value: 0.1 vmshell, flag: 1}(_easyComplexity, _hardComplexity);
        }
        if (_tapData.exists(epochStart) == false) { return; }
        (uint128[] Taps, uint64[] MbiCurTaps) = abi.decode(tapData, (uint128[], uint64[]));
        uint128 reward = 0;
        TvmCell data1 = abi.encode(_tapData[epochStart].tapData);
        for (uint256 i = 0; i < Taps.length; i++) {
            reward += gosh.calcminerreward(MbiCurTaps[i], data1, Taps[i], _tapData[epochStart].basicReward); 
        }
        _reward_sum += reward;
        gosh.mintecc(uint64(reward), CURRENCIES_ID);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(reward);
        popitGame.transfer({value: 0.1 vmshell, flag: 1, currencies: data_cur});
        address addrExtern = address.makeAddrExtern(0, bitCntAddress);
        emit Rewarded{dest: addrExtern}(popitGame, reward);
        addrExtern = address.makeAddrExtern(popitGame.value, bitCntAddress);
        emit RewardedPopitGame{dest: addrExtern}(reward);
    }

    function ensureBalance() private {
        if ((_reward_last_time + _min_reward_period < block.timestamp) || (_calc_reward_num == 0)) {
            calcRewardAdjustment();
        }
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshellq(ROOT_BALANCE);
    }

    function calcRewardAdjustment() private returns (uint128) {
        _reward_period = (_reward_period * _calc_reward_num + block.timestamp - _reward_last_time) / (_calc_reward_num + 1);
        _reward_last_time = block.timestamp;
        _calc_reward_num += 1;
        _reward_adjustment = gosh.calcbmmvrewardadj(_reward_sum, _reward_period, _reward_adjustment, uint128(block.timestamp - _networkStart), false);
    }

    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwner accept {
        ensureBalance();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private {
        cell;
        tvm.accept();
        tvm.resetStorage();
    }

    //Getters
    function getCellForBoost(address wallet,
        address popitGame,
        address root,
        uint64 mbiCur,
        uint256 rootPubkey,
        address miner) external pure returns(TvmCell) {
        return abi.encode(wallet, popitGame, root, mbiCur, rootPubkey, miner);
    }

    function calculateTaps(
        uint128 tapSum,
        uint128 commitTaps,
        uint128 mining_dur,
        uint128 modified_tap_rem_q40,
        uint128 miningDurSum,
        uint128 modifiedTapSum,
        uint128 tapSum5m
    ) external pure returns(
        uint128 rtapCoef,
        uint128 rmodified_tap_rem_q40,
        uint128 rminingDurSum,
        uint128 rmodifiedTapSum,
        uint128 rtapSum5m,
        uint128 rtapSum
    ) {
        return gosh.calcminertapcoef(tapSum, commitTaps, mining_dur, modified_tap_rem_q40, miningDurSum, modifiedTapSum, tapSum5m);

    }

    function getVersion() external pure returns(string, string) {
        return (version, "MobileVerifiersContractGameRoot");
    }
}
