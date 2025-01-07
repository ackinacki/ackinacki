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
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";
import "./BlockKeeperPreEpochContract.sol";
import "./BlockKeeperEpochContract.sol";
import "./BLSKeyIndex.sol";
import "./SignerIndex.sol";

contract AckiNackiBlockKeeperNodeWallet is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 _owner_pubkey;
    optional(uint256) _service_key;
    address _root; 
    mapping (uint256 => Stake) _activeStakes;
    uint128 _walletId;
    uint8 _stakesCnt = 0;

    address _bk_root = address.makeAddrStd(0, 0);

    uint256 _locked = 0;
    uint128 _indexLock = 0;
    mapping(uint128 => LockStake) _lockmap;
    mapping(bytes => bool) _bls_keys;

    constructor (
        TvmCell BlockKeeperPreEpochCode,
        TvmCell AckiNackiBlockKeeperNodeWalletCode,
        TvmCell BlockKeeperEpochCode,
        TvmCell BlockKeeperEpochCoolerCode,
        TvmCell BlockKeeperEpochProxyListCode,
        TvmCell AckiNackiBlockKeeperNodeWalletConfigCode,
        TvmCell BLSKeyCode,
        TvmCell SignerIndexCode,
        uint128 walletId
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root, uint256 pubkey) = abi.decode(data, (string, address, uint256));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _owner_pubkey = pubkey;
        _root = root;
        require(msg.sender == _root, ERR_SENDER_NO_ALLOWED);
        _code[m_AckiNackiBlockKeeperNodeWalletCode] = AckiNackiBlockKeeperNodeWalletCode;
        _code[m_BlockKeeperPreEpochCode] = BlockKeeperPreEpochCode;
        _code[m_BlockKeeperEpochCode] = BlockKeeperEpochCode;
        _code[m_BlockKeeperEpochCoolerCode] = BlockKeeperEpochCoolerCode;
        _code[m_BlockKeeperEpochProxyListCode] = BlockKeeperEpochProxyListCode;
        _code[m_AckiNackiBlockKeeperNodeWalletConfigCode] = AckiNackiBlockKeeperNodeWalletConfigCode;
        _code[m_BLSKeyCode] = BLSKeyCode;
        _code[m_SignerIndexCode] = SignerIndexCode;
        _walletId = walletId;
        BlockKeeperContractRoot(_root).deployAckiNackiBlockKeeperNodeWalletConfig{value: 0.1 vmshell}(_owner_pubkey, _walletId);
        BlockKeeperContractRoot(_root).deployAckiNackiBlockKeeperNodeWalletConfig{value: 0.1 vmshell}(_owner_pubkey, _walletId);
    }

    function serLockIndex(uint128 index) public onlyOwnerPubkey(_owner_pubkey) accept {
        _indexLock = index;
    }

    function lockNACKL(uint256 value, uint32 time) public onlyOwnerPubkey(_owner_pubkey) accept {
        require(_lockmap.exists(_indexLock) == false, ERR_LOCK_EXIST);
        require(_locked + value <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        _locked += value;
        _lockmap[_indexLock] = LockStake(value, block.timestamp + time);
        _indexLock += 1;
        if (_indexLock == MAX_LOCK_NUMBER) {
            _indexLock = 0;
        }
    }

    function unlockNACKL(uint128 index) public onlyOwnerPubkey(_owner_pubkey) accept {
        require(_lockmap.exists(index) == true, ERR_LOCK_NOT_EXIST);
        require(_lockmap[index].timeStampFinish > block.timestamp, ERR_LOCK_NOT_READY);
        _locked -= _lockmap[index].value;
        delete _lockmap[index];
    }

    function setServiceKey(optional(uint256) key) public onlyOwnerPubkey(_owner_pubkey) accept {
        getMoney();
        _service_key = key;
    }

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_WALLET * 3) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_KEEPER_WALLET * 3);
    }

    function setLockStake(uint64 seqNoStart, uint256 stake, bytes bls_key, uint16 signerIndex) public senderIs(BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        _activeStakes[stakeHash] = Stake(stake, seqNoStart, 0, 0, bls_key, PRE_EPOCH_DEPLOYED, signerIndex);
        _bls_keys[bls_key] = true;
    } 

    function slash(uint8 slash_type, bytes bls_key) internalMsg public view senderIs(address(this)) {
        this.iterateStakes{value: 0.1 vmshell, flag: 1}(slash_type, bls_key, _activeStakes.min());
    }

    function iterateStakes(uint8 slash_type, bytes bls_key, optional(uint256, Stake) data) public view senderIs(address(this)) {
        if (data.hasValue() == false) {
            return;
        }  
        (uint256 key, Stake value) = data.get();
        if (value.bls_key == bls_key) {
            if (value.status == 1) {
                BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, value.seqNoStart)).slash{value: 0.1 vmshell, flag: 1}(slash_type);
                return;
            } else {
                BlockKeeperCooler(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, _activeStakes[key].seqNoStart)).slash{value: 0.1 vmshell, flag: 1}(slash_type);
                return;
            }   
        }        
        this.iterateStakes{value: 0.1 vmshell, flag: 1}(slash_type, bls_key, _activeStakes.next(key));
    }

    function updateLockStake(uint64 seqNoStart, uint32 timeStampFinish, uint256 stake, bytes bls_key, uint16 signerIndex) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        _activeStakes[stakeHash] = Stake(stake, seqNoStart, timeStampFinish, 0, bls_key, EPOCH_DEPLOYED, signerIndex);
    }

    function stakeNotAccepted() public senderIs(_root) accept {
        _stakesCnt -= 1;
    }

    function sendBlockKeeperRequestWithStake(bytes bls_pubkey, varuint32 stake, uint16 signerIndex, mapping(uint8 => string) ProxyList) public  onlyOwnerPubkeyArray([_owner_pubkey, _service_key.getOrDefault()]) accept saveMsg {
        getMoney();
        require(bls_pubkey.length == 48, ERR_WRONG_BLS_PUBKEY);
        require(stake <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_stakesCnt == 0, ERR_STAKE_EXIST);
        _stakesCnt += 1;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = stake;
        BlockKeeperContractRoot(_root).receiveBlockKeeperRequestWithStakeFromWallet{value: 0.1 vmshell, currencies: data_cur, flag: 1} (_owner_pubkey, bls_pubkey, signerIndex, ProxyList);
    } 

    function sendBlockKeeperRequestWithCancelStakeContinue(uint64 seqNoStartOld) public onlyOwnerPubkeyArray([_owner_pubkey, _service_key.getOrDefault()]) accept saveMsg {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStartOld);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)).cancelContinueStake{value: 0.1 vmshell, flag: 1}();    
    } 

    function cancelContinueStake(uint64 seqNoStartOld, bytes bls_key, uint16 signerIndex) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept {
        _stakesCnt -=1;
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key, _root)).destroy{value: 0.1 ton, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 ton, flag: 1}();
    }

    function sendBlockKeeperRequestWithStakeContinue(bytes bls_pubkey, varuint32 stake, uint64 seqNoStartOld, uint16 signerIndex) public onlyOwnerPubkeyArray([_owner_pubkey, _service_key.getOrDefault()]) accept saveMsg {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStartOld);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        require(bls_pubkey.length == 48, ERR_WRONG_BLS_PUBKEY);
        require(stake <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_stakesCnt == 1, ERR_STAKE_EXIST);
        _stakesCnt += 1;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = stake;
        BlockKeeperContractRoot(_root).receiveBlockKeeperRequestWithStakeFromWalletContinue{value: 0.1 vmshell, currencies: data_cur, flag: 1} (_owner_pubkey, bls_pubkey, seqNoStartOld, signerIndex);
    } 

    function deployPreEpochContract(uint32 epochDuration, uint64 epochCliff, uint64 waitStep, bytes bls_pubkey, uint16 signerIndex, mapping(uint8 => string) ProxyList) public view senderIs(_root) accept  {
        getMoney();
        uint64 seqNoStart = block.seqno + epochCliff;
        TvmCell data = BlockKeeperLib.composeBlockKeeperPreEpochStateInit(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, seqNoStart);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = msg.currencies[CURRENCIES_ID];
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == false, ERR_STAKE_DIDNT_EXIST);
        new BlockKeeperPreEpoch {
            stateInit: data, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST + 1 vmshell),
            currencies: msg.currencies,
            wid: 0, 
            flag: 1
        } (waitStep, epochDuration, bls_pubkey, _code, _walletId, signerIndex, ProxyList);
    }

    function deployBlockKeeperContractContinue(uint32 epochDuration, uint64 waitStep, uint64 seqNoStartold, bytes bls_pubkey, uint16 signerIndex) public view senderIs(_root) accept  {
        getMoney();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = msg.currencies[CURRENCIES_ID];
        TvmBuilder b;
        b.store(seqNoStartold);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartold)).continueStake{value: 0.1 vmshell, flag: 1, currencies: data_cur}(epochDuration, waitStep, bls_pubkey, signerIndex);    
    }

    function deployBlockKeeperContractContinueAfterDestroy(uint32 epochDuration, uint64 waitStep, bytes bls_pubkey, uint64 seqNoStartOld, uint32 reputationTime, uint16 signerIndex) public view senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept  {
        getMoney();
        uint64 seqNoStart = block.seqno;
        TvmCell data = BlockKeeperLib.composeBlockKeeperEpochStateInit(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart);
        new BlockKeeperEpoch {
            stateInit: data, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET),
            currencies: msg.currencies,
            wid: 0, 
            flag: 1
        } (waitStep, epochDuration, bls_pubkey, _code, true, _walletId, reputationTime, signerIndex);
    }

    function updateLockStakeCooler(uint64 seqNoStart, uint32 time) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        _stakesCnt -= 1;
        _activeStakes[hash].status = COOLER_DEPLOYED;
        _activeStakes[hash].timeStampFinishCooler = time;
    } 

    function unlockStakeCooler(uint64 seqNoStart) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        delete _bls_keys[_activeStakes[hash].bls_key];
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[hash].bls_key, _root)).destroy{value: 0.1 ton, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[hash].signerIndex, _root)).destroy{value: 0.1 ton, flag: 1}();
        delete _activeStakes[hash];
    } 

    function slashCooler(uint64 seqNoStart, uint8 slash_type, uint256 slash_stake) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        if (slash_type == FULL_STAKE_SLASH) {
            delete b;
            delete _bls_keys[_activeStakes[stakeHash].bls_key];
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[stakeHash].bls_key, _root)).destroy{value: 0.1 ton, flag: 1}();
            SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[stakeHash].signerIndex, _root)).destroy{value: 0.1 ton, flag: 1}();
            delete _activeStakes[stakeHash];
            return;
        }
        _activeStakes[stakeHash].stake -= slash_stake;
        return;
    }

    function slashStake(uint64 seqNoStart, uint8 slash_type, uint256 slash_stake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        if (slash_type == FULL_STAKE_SLASH) {
            if (_activeStakes[hash].status == EPOCH_DEPLOYED) {
                _stakesCnt -= 1;
            }
            delete _bls_keys[_activeStakes[hash].bls_key];
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[hash].bls_key, _root)).destroy{value: 0.1 ton, flag: 1}();
            SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[hash].signerIndex, _root)).destroy{value: 0.1 ton, flag: 1}();
            delete _activeStakes[hash];
            return;
        }
        _activeStakes[hash].stake -= slash_stake;
        return;
    } 

    function withdrawToken(address to, varuint32 value) public view onlyOwnerPubkey (_owner_pubkey) accept {
        getMoney();
        require(value + _locked <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        to.transfer({value: 0.1 vmshell, currencies: data, flag: 1});
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        optional(uint256) service_key,
        address root,
        uint256 balance,
        mapping (uint256 => Stake) activeStakes,
        uint256 walletId,
        uint256 locked,
        uint128 indexLock,
        uint8 stakesCnt,
        mapping(uint128 => LockStake) lockmap
    ) {
        return  (_owner_pubkey, _service_key, _root, address(this).currencies[CURRENCIES_ID], _activeStakes, _walletId, _locked, _indexLock, _stakesCnt, _lockmap);
    }

    function getProxyListAddr() external view returns(address) {
        return BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root);
    }

    function getProxyListCodeHash() external view returns(uint256) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperEpochProxyListCode(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _root));
    }

    function getProxyListCode() external view returns(TvmCell) {
        return BlockKeeperLib.buildBlockKeeperEpochProxyListCode(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _root);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "AckiNackiBlockKeeperNodeWallet");
    }
}
