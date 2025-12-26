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
import "./MobileVerifiersContractGameRoot.sol";
import "./modifiers/structs/structs.sol";

contract Miner is Modifiers {
    string constant version = "1.0.0";
    
    address _mobileVerifiersContractGameRoot; 

    address static _owner;
    address _popitGame;
    address _boost;
    address _root;

    optional(uint64) _mbiCur;
    mapping(uint256=>uint256) _owner_pubkey;
    uint32 _owner_pubkey_size = 0;

    uint64 _epochStart;
    uint64 _epochStartOld;
    uint128[] _oldTaps;
    uint128 _oldTapsSize = 0;
    uint64[] _oldMbiCurTaps;
    uint128[] _taps;
    uint64[] _mbiCurTaps;
    uint128 _tapsSize = 0;

    uint128 _tapSum = 0;
    uint128 _tapSum5m = 0;
    uint128 _modifiedTapSum = 0;
    uint128 _miningDurSum = 0;
    uint128 _modified_tap_rem_q40 = 0;
    uint64 _epochBigStart = 0;

    uint256 _seed;
    uint256 _seedNext;

    optional(bytes) _commitData;
    uint128 _commitTaps;
    optional(Interval, Interval) _commitInterval;

    uint32 _easyComplexity = 10;
    uint32 _hardComplexity = 11;
    uint32 _easyComplexityOld = 10;
    uint32 _hardComplexityOld = 11;

    bytes constant _wasm_hash = hex"2d577ca2e693700282d6d778dce8cfcedbada644497e411ec6aed889f5a3d5f4";
    
    event TapSucceed(bytes data, bytes wasmResult);
    event TapFailed(bytes data, bytes wasmResult);
    event GetInterval(Interval easy, Interval hard, uint64 workerId);
    event NewSeed(uint256 seed, uint256 seednext);
    event NewComplexity(uint32 easyComplexity, uint32 hardComplexity);

    constructor (
        uint128 index,
        address popitGame,
        address mobileVerifiersContractGameRoot,
        address boost,
        address root
    ) {
        require(index >= 0, ERR_WRONG_DATA);
        require(index < MAX_MIRROR_INDEX, ERR_WRONG_DATA);
        uint256 addrValue = BASE_PART * SHIFT + index + 1;
        address expectedAddress = address.makeAddrStd(0, addrValue);
        require(msg.sender == expectedAddress, ERR_INVALID_SENDER);
        (, uint64 r) = math.divmod(block.seqno, MinerRewardPeriod); 
        _epochStart = block.seqno - r;
        _popitGame = popitGame;
        _mobileVerifiersContractGameRoot = mobileVerifiersContractGameRoot;
        _boost = boost;
        _root = root;
        rnd.shuffle();
        _seed = rnd.next(type(uint32).max);
        _seedNext = rnd.next(type(uint32).max);
        address addrExtern = address.makeAddrExtern(MinerNewSeedEmit, bitCntAddress);
        emit NewSeed{dest: addrExtern}(_seed, _seedNext);
        addrExtern = address.makeAddrExtern(MinerNewComplexityEmit, bitCntAddress);
        emit NewComplexity{dest: addrExtern}(_easyComplexity, _hardComplexity);
    }

    function destroyNode() public senderIs(address(this)) accept {
        ensureBalance();
        selfdestruct(address(this));
    }

    function setComplexity(uint32 easyComplexity, uint32 hardComplexity) public senderIs(_mobileVerifiersContractGameRoot) accept {
        ensureBalance();
        _easyComplexity = easyComplexity;
        _hardComplexity = hardComplexity;
        address addrExtern = address.makeAddrExtern(MinerNewComplexityEmit, bitCntAddress);
        emit NewComplexity{dest: addrExtern}(_easyComplexity, _hardComplexity);
    }

    function setMbiCur(uint64 mbiCur) public senderIs(_boost) accept {
        ensureBalance();
        _mbiCur = mbiCur;
    }

    function setCommitData(uint256 id, uint64 easyNumber, uint64 tapNumber, uint64 workerId, bytes data) public onlyOwnerPubkey(_owner_pubkey[id]) accept {
        ensureBalance();
        require(_commitData.hasValue() == false, ERR_WRONG_DATA);
        require(_tapsSize <= MAX_LEN_TAPS, ERR_FULL_TAPS); 
        if (easyNumber == 0) { return; }
        if (tapNumber == 0) { return; }
        _commitData = data;
        rnd.shuffle();
        Interval checkTap;
        if (tapNumber > IntervalRadius * 2) {
            uint64 point = IntervalRadius + rnd.next(tapNumber - IntervalRadius * 2);
            checkTap = Interval(point - IntervalRadius, point + IntervalRadius);
        } else {
            checkTap = Interval(0, tapNumber - 1);
        }
        Interval checkEasy;
        if (easyNumber > IntervalRadius * 2) {
            uint64 point = IntervalRadius + rnd.next(easyNumber - IntervalRadius * 2);
            checkEasy = Interval(point - IntervalRadius, point + IntervalRadius);
        } else {
            checkEasy = Interval(0, easyNumber - 1);
        }
        _commitInterval = (checkEasy, checkTap);
        _commitTaps = tapNumber;
        address addrExtern = address.makeAddrExtern(MinerIntervalEmit, bitCntAddress);
        emit GetInterval{dest: addrExtern}(checkEasy, checkTap, workerId);
    }

    function cancelCommitData(uint256 id) public onlyOwnerPubkey(_owner_pubkey[id]) accept {
        ensureBalance();
        require(_commitData.hasValue(), ERR_WRONG_DATA);
        delete _commitData;
        delete _commitInterval;
        rnd.shuffle();
        _seed = _seedNext;
        _seedNext = rnd.next(type(uint32).max);
        address addrExtern = address.makeAddrExtern(MinerNewSeedEmit, bitCntAddress);
        emit NewSeed{dest: addrExtern}(_seed, _seedNext);
        
    }

    function setOwnerPubkey(uint256 id, uint256 pubkey) public senderIs(_owner) accept {
        ensureBalance();
        if (!_owner_pubkey.exists(id)) {
            require(_owner_pubkey_size < MAX_PUBKEY_SIZE, ERR_NOT_READY);
            _owner_pubkey_size = _owner_pubkey_size + 1;
        } 
        _owner_pubkey[id] = pubkey;
    }

    function deleteOwnerPubkey(uint256 id) public senderIs(_owner) accept {
        ensureBalance();
        if (_owner_pubkey.exists(id)) {
            if (_owner_pubkey_size == 0) { return; }
            delete _owner_pubkey[id];
            _owner_pubkey_size = _owner_pubkey_size - 1;
        } 
    }

    function uint32ToBytes(uint32 value) private pure returns (bytes) {
        return bytes(bytes4(value));
    }

    function uint64ToBytes(uint64 value) private pure returns (bytes) {
        return bytes(bytes8(value));
    }

    function uint256ToBytes(uint256 value) private pure returns (bytes) {
        return bytes(bytes32(value));
    }

    function getReward(uint256 id) public onlyOwnerPubkey(_owner_pubkey[id]) accept {
        ensureBalance();
        require(_mbiCur.hasValue(), ERR_NOT_READY);
        (, uint64 remainder) = math.divmod(block.seqno, MinerRewardPeriod);
        uint64 currentEpochStart = block.seqno - remainder;        
        _processOldEpochRewards(currentEpochStart);        
        if (_epochStart != currentEpochStart) {
            _updateEpoch(currentEpochStart);            
            if (_epochStartOld + MinerRewardPeriod < currentEpochStart && _oldTapsSize != 0) {
                _sendTapReward();
                _clearOldTaps();
            }
        }        
        _updateBigEpoch();
    }

    function _processOldEpochRewards(uint64 currentEpochStart) private {
        if (_epochStartOld + MinerRewardPeriod < currentEpochStart && _oldTapsSize != 0) {
            _sendTapReward();
            _clearOldTaps();
        }
    }

    function _updateEpoch(uint64 newEpochStart) private {
        _oldMbiCurTaps = _mbiCurTaps;
        _oldTaps = _taps;
        _oldTapsSize = _tapsSize;
        
        delete _taps;
        delete _tapsSize;
        delete _mbiCurTaps;        
        _epochStartOld = _epochStart;
        _epochStart = newEpochStart;
        
        _tapSum5m = 0;
        _modifiedTapSum = 0;
        _miningDurSum = 0;
    }

    function _updateBigEpoch() private {
        (, uint64 bigRemainder) = math.divmod(block.seqno, MinerTapDelay);
        uint64 currentBigEpochStart = block.seqno - bigRemainder;
        if (_epochBigStart != currentBigEpochStart) {
            _tapSum = 0;
            _tapSum5m = 0;
            _modifiedTapSum = 0;
            _miningDurSum = 0;
            _modified_tap_rem_q40 = 0;
            _epochBigStart = currentBigEpochStart;
        }
    }

    function _sendTapReward() private view {
        TvmCell tapData = abi.encode(_oldTaps, _oldMbiCurTaps);
        MobileVerifiersContractGameRoot(_mobileVerifiersContractGameRoot).tapReward{
            value: 0.1 vmshell, 
            flag: 1
        }(
            tapData,
            _epochStartOld,
            _popitGame,
            _owner,
            _easyComplexity,
            _hardComplexity
        );
    }

    function _clearOldTaps() private {
        delete _oldTapsSize;
        delete _oldTaps;
        delete _oldMbiCurTaps;
    }

    function acceptTap(uint256 id, bytes verifyData, address[] eventAddrSuccess, bytes[] eventCellSuccess, address[] eventAddrFailed, bytes[] eventCellFailed) public onlyOwnerPubkey(_owner_pubkey[id]) accept {
        require(eventAddrSuccess.length <= MAX_LEN_EVENT, ERR_WRONG_DATA);
        require(eventCellSuccess.length <= MAX_LEN_EVENT, ERR_WRONG_DATA);
        require(eventAddrFailed.length <= MAX_LEN_EVENT, ERR_WRONG_DATA);
        require(eventCellFailed.length <= MAX_LEN_EVENT, ERR_WRONG_DATA);
        require(eventCellFailed.length == eventAddrFailed.length, ERR_WRONG_DATA);
        require(eventAddrSuccess.length == eventCellSuccess.length, ERR_WRONG_DATA);
        require(_mbiCur.hasValue(), ERR_NOT_READY);
        require(_commitInterval.hasValue(), ERR_NOT_READY);
        require(_commitData.hasValue(), ERR_NOT_READY);
        ensureBalance();
        (, uint64 remainder) = math.divmod(block.seqno, MinerRewardPeriod);
        uint64 currentEpochStart = block.seqno - remainder;        
        _processOldEpochRewards(currentEpochStart);        
        if (_epochStart != currentEpochStart) {
            _updateEpoch(currentEpochStart);            
            if (_epochStartOld + MinerRewardPeriod < currentEpochStart && _oldTapsSize != 0) {
                _sendTapReward();
                _clearOldTaps();
            }
        }        
        _updateBigEpoch();
        (Interval easyInterval, Interval hardInterval) = _commitInterval.get();
        bytes dataForWasm;
        dataForWasm.append(uint256ToBytes(_seed));
        dataForWasm.append(uint32ToBytes(_easyComplexityOld));
        dataForWasm.append(uint64ToBytes(easyInterval.first));
        dataForWasm.append(uint64ToBytes(easyInterval.second));
        dataForWasm.append(uint32ToBytes(_hardComplexityOld));
        dataForWasm.append(uint64ToBytes(hardInterval.first));
        dataForWasm.append(uint64ToBytes(hardInterval.second));
        dataForWasm.append(_commitData.get());
        dataForWasm.append(verifyData);
        TvmCell finalData = abi.encode(dataForWasm);
        TvmCell result = gosh.runwasm(abi.encode(_wasm_hash), finalData, abi.encode(WASM_MINER_FUNCTION), abi.encode(WASM_MINER_MODULE), abi.encode(WASM_BINARY));
        bytes data = abi.decode(result, (bytes));
        require(data.length >= MAX_LEN_WASM, ERR_WRONG_DATA);
        uint8 isSuccess = uint8(data[0]);
        uint8 isCorrectTap = uint8(data[1]);
        uint64 mining_dur = uint32(uint8(data[2])) |
                   (uint32(uint8(data[3])) << 8) |
                   (uint32(uint8(data[4])) << 16) |
                   (uint32(uint8(data[5])) << 24);     
        if ((isCorrectTap == 1) && (isSuccess == 1)) {
            delete _commitData;
            delete _commitInterval;
            for (uint256 i = 0; i < eventAddrSuccess.length; i++) {
                address addrExtern = address.makeAddrExtern(eventAddrSuccess[i].value, bitCntAddress);
                emit TapSucceed{dest: addrExtern}(eventCellSuccess[i], data);
            }
        } else {
            for (uint256 i = 0; i < eventAddrFailed.length; i++) {
                address addrExtern = address.makeAddrExtern(eventAddrFailed[i].value, bitCntAddress);
                emit TapFailed{dest: addrExtern}(eventCellFailed[i], data);
            }
            return;
        }
        uint128 tapCoef;
        rnd.shuffle();
        _seed = _seedNext;
        _seedNext = rnd.next();
        _easyComplexityOld = _easyComplexity;
        _hardComplexityOld = _hardComplexity;
        address addrExternNew = address.makeAddrExtern(MinerNewSeedEmit, bitCntAddress);
        emit NewSeed{dest: addrExternNew}(_seed, _seedNext);
        if (_tapSum >= BIG_TAP) { return; }
        if (_tapSum5m >= SMALL_TAP) { return; }
        if (_miningDurSum >= MINING_DUR_TAP) { return; }
        if (mining_dur == 0) { return; }
        (tapCoef, _modified_tap_rem_q40, _miningDurSum, _modifiedTapSum, _tapSum5m, _tapSum) = gosh.calcminertapcoef(_tapSum, _commitTaps, mining_dur, _modified_tap_rem_q40, _miningDurSum, _modifiedTapSum, _tapSum5m);
        if (tapCoef == 0) { return; }
        _taps.push(tapCoef);
        _tapsSize += 1;
        _mbiCurTaps.push(_mbiCur.get());
        MobileVerifiersContractGameRoot(_mobileVerifiersContractGameRoot).getTap{value: 0.1 vmshell, flag: 1}(
            uint64(tapCoef),
            _epochStart,
            _owner,
            _mbiCur.get(),
            _easyComplexity,
            _hardComplexity
        );
    }

    function ensureBalance() private pure {
        if (address(this).balance > MINER_BALANCE) { return; }
        gosh.mintshellq(MINER_BALANCE);
    }


    //Getters
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

    function getDetails() external view returns(
        address mobileVerifiersContractGameRoot,
        address owner,
        address popitGame,
        address boost,
        optional(uint64) mbiCur,
        mapping(uint256=>uint256) owner_pubkey,
        uint64 epochStart,
        uint64 epochStartOld,
        uint128[] oldTaps,
        uint128 oldTapsSize,
        uint64[] oldMbiCurTaps,
        uint128[] taps,
        uint64[] mbiCurTaps,
        uint128 tapsSize,
        uint128 tapSum,
        uint128 modifiedTapSum,
        uint128 miningDurSum,
        uint64 epochBigStart,
        uint256 seed,
        uint256 seedNext,
        optional(bytes) commitData,
        optional(Interval, Interval) commitInterval,
        uint32 easyComplexity,
        uint32 hardComplexity
    ) {
        return (
            _mobileVerifiersContractGameRoot,
            _owner,
            _popitGame,
            _boost,
            _mbiCur,
            _owner_pubkey,
            _epochStart,
            _epochStartOld,
            _oldTaps,
            _oldTapsSize,
            _oldMbiCurTaps,
            _taps,
            _mbiCurTaps,
            _tapsSize,
            _tapSum,
            _modifiedTapSum,
            _miningDurSum,
            _epochBigStart,
            _seed,
            _seedNext,
            _commitData,
            _commitInterval,
            _easyComplexity,
            _hardComplexity
        );
    }

    function decodeResult(TvmCell result) external pure returns(uint8 isSuccess, uint8 isCorrectTap, uint32 miningDur) {
        (isSuccess, isCorrectTap, miningDur) = abi.decode(result, (uint8, uint8, uint32));
    }

    function getVersion() external pure returns(string, string) {
        return (version, "Miner");
    }
}
