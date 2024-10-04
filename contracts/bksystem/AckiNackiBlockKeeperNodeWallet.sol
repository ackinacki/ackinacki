// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";
import "./BlockKeeperPreEpochContract.sol";
import "./BlockKeeperEpochContract.sol";

contract AckiNackiBlockKeeperNodeWallet is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    optional(uint256) _service_key;
    address _root; 
    mapping (uint256 => Stake) _activeStakes;
    uint256 _walletId;
    uint8 _stakesCnt = 0;

    constructor (
        TvmCell BlockKeeperPreEpochCode,
        TvmCell AckiNackiBlockKeeperNodeWalletCode,
        TvmCell BlockKeeperEpochCode,
        TvmCell BlockKeeperEpochCoolerCode,
        TvmCell BlockKeeperSlashCode,
        uint256 walletId
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _root = root;
        require(msg.sender == _root, ERR_SENDER_NO_ALLOWED);
        _code[m_AckiNackiBlockKeeperNodeWalletCode] = AckiNackiBlockKeeperNodeWalletCode;
        _code[m_BlockKeeperPreEpochCode] = BlockKeeperPreEpochCode;
        _code[m_BlockKeeperEpochCode] = BlockKeeperEpochCode;
        _code[m_BlockKeeperEpochCoolerCode] = BlockKeeperEpochCoolerCode;
        _code[m_BlockKeeperSlashCode] = BlockKeeperSlashCode;
        _walletId = walletId;
    }

    function setServiceKey(optional(uint256) key) public onlyOwnerPubkey(_owner_pubkey) accept {
        getMoney();
        _service_key = key;
    }

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_WALLET * 3) { return; }
        mintshell(FEE_DEPLOY_BLOCK_KEEPER_WALLET * 3);
    }

    function slash(uint256 pubkey, uint64 seqNoStart) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, pubkey, seqNoStart)) accept {
        getMoney();
        if  (_owner_pubkey != pubkey) { return; }
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        delete _activeStakes[stakeHash];
    } 

    function slashCooler(uint64 seqNoStart) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        delete _activeStakes[stakeHash];
    } 

    function setLockStake(uint64 seqNoStart, uint256 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        _activeStakes[stakeHash] = Stake(stake, seqNoStart, 0, PRE_EPOCH_DEPLOYED);
    } 

    function updateLockStake(uint64 seqNoStart, uint32 timeStampFinish, uint256 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        _activeStakes[stakeHash] = Stake(stake, seqNoStart, timeStampFinish, EPOCH_DEPLOYED);
    }

    function sendBLSPrivateKey(bytes key, uint32 unixtimeStart) public onlyOwnerPubkeyArray([_owner_pubkey, _service_key.getOrDefault()]) accept saveMsg {
        getMoney();
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, unixtimeStart)).getBLSPrivateKey{value: 0.3 ton, flag: 1}(key);
    }

    function stakeNotAccepted() public senderIs(_root) accept {
        _stakesCnt -= 1;
    }

    function sendBlockKeeperRequestWithStake(bytes bls_pubkey, varuint32 stake) public  onlyOwnerPubkeyArray([_owner_pubkey, _service_key.getOrDefault()]) accept saveMsg {
        getMoney();
        require(bls_pubkey.length == 48, ERR_WRONG_BLS_PUBKEY);
        require(stake <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_stakesCnt == 0, ERR_STAKE_EXIST);
        _stakesCnt += 1;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = stake;
        BlockKeeperContractRoot(_root).receiveBlockKeeperRequestWithStakeFromWallet{value: 0.1 ton, currencies: data_cur, flag: 1} (_owner_pubkey, bls_pubkey);
    } 

    function sendBlockKeeperRequestWithCancelStakeContinue(uint64 seqNoStartOld) public onlyOwnerPubkeyArray([_owner_pubkey, _service_key.getOrDefault()]) accept saveMsg {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStartOld);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)).cancelContinueStake{value: 0.1 ton, flag: 1}();    
    } 

    function cancelContinueStake(uint64 seqNoStartOld) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept {
        _stakesCnt -=1;
    }

    function sendBlockKeeperRequestWithStakeContinue(bytes bls_pubkey, varuint32 stake, uint64 seqNoStartOld) public onlyOwnerPubkeyArray([_owner_pubkey, _service_key.getOrDefault()]) accept saveMsg {
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
        BlockKeeperContractRoot(_root).receiveBlockKeeperRequestWithStakeFromWalletContinue{value: 0.1 ton, currencies: data_cur, flag: 1} (_owner_pubkey, bls_pubkey, seqNoStartOld);
    } 

    function deployPreEpochContract(uint32 epochDuration, uint64 epochCliff, uint64 waitStep, bytes bls_pubkey) public view senderIs(_root) accept  {
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
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET),
            currencies: msg.currencies,
            wid: 0, 
            flag: 1
        } (waitStep, epochDuration, bls_pubkey, _code, _walletId);
    }

    function deployBlockKeeperContractContinue(uint32 epochDuration, uint64 waitStep, uint64 seqNoStartold, bytes bls_pubkey) public view senderIs(_root) accept  {
        getMoney();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = msg.currencies[CURRENCIES_ID];
        TvmBuilder b;
        b.store(seqNoStartold);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartold)).continueStake{value: 0.1 ton, flag: 1, currencies: data_cur}(epochDuration, waitStep, bls_pubkey);    
    }

    function deployBlockKeeperContractContinueAfterDestroy(uint32 epochDuration, uint64 waitStep, bytes bls_pubkey, uint64 seqNoStartOld, uint32 reputationTime) public view senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept  {
        getMoney();
        uint64 seqNoStart = block.seqno;
        TvmCell data = BlockKeeperLib.composeBlockKeeperEpochStateInit(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = msg.currencies[CURRENCIES_ID];
        new BlockKeeperEpoch {
            stateInit: data, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET),
            currencies: msg.currencies,
            wid: 0, 
            flag: 1
        } (waitStep, epochDuration, bls_pubkey, _code, true, _walletId, reputationTime);
    }

    function updateLockStakeCooler(uint64 seqNoStart) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        _stakesCnt -= 1;
        _activeStakes[hash].status = COOLER_DEPLOYED;
    } 

    function unlockStakeCooler(uint64 seqNoStart) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        delete _activeStakes[hash];
    } 

    function slashStake(uint64 seqNoStart) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        getMoney();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        if (_activeStakes[hash].status == EPOCH_DEPLOYED) {
            _stakesCnt -= 1;
        }
        delete _activeStakes[hash];
    } 

    function withdrawToken(address to, varuint32 value) public view onlyOwnerPubkey (_owner_pubkey) accept {
        getMoney();
        require(value <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        to.transfer({value: 0.1 ton, currencies: data, flag: 1});
    }

    function sendRequestToSlashBlockKeeper(uint256 slashpubkey, uint64 slashSeqNoStart, uint64 seqNoStart) public view onlyOwnerPubkey (_owner_pubkey) accept {
        getMoney();
        BlockKeeperContractRoot(_root).sendRequestToSlashBlockKeeper{value: 1 ton, flag: 1}(slashpubkey, slashSeqNoStart, seqNoStart, _owner_pubkey);
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
        uint256 walletId
    ) {
        return  (_owner_pubkey, _service_key, _root, address(this).currencies[CURRENCIES_ID], _activeStakes, _walletId);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "AckiNackiBlockKeeperNodeWallet");
    }
}
