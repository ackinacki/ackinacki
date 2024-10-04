// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";
import "./AckiNackiBlockKeeperNodeWallet.sol";
import "./BlockKeeperCoolerContract.sol";
import "./BlockKeeperSlashContract.sol";


contract BlockKeeperEpoch is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 
    uint64 static _seqNoStart;
    uint32 _unixtimeFinish;
    bytes _bls_pubkey;
    mapping(uint256 => bool) _slashMember;
    uint128 _slashed = 0;
    bool _isContinue = false;
    uint64 _waitStep;
    uint256 _stake;
    uint256 _totalStake;

    uint32 _epochDurationContinue;
    uint64 _waitStepContinue;
    bytes _bls_pubkeyContinue;
    uint256 _stakeContinue;
    uint256 _walletId;
    uint32 _epochDuration;
    uint128 _numberOfActiveBlockKeepers;
    uint32 _unixtimeStart;

    uint32 _reputationTime; 

    constructor (
        uint64 waitStep,
        uint32 epochDuration,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        bool isContinue,
        uint256 walletId,
        uint32 reputationTime
    ) {
        _code = code;
        _walletId = walletId;
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, uint256 hashwalletsalt, uint256 hashpreepochsalt, address root) = abi.decode(data, (string, uint256, uint256, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _root = root;
        _epochDuration = epochDuration;
        TvmBuilder b;
        b.store(_code[m_AckiNackiBlockKeeperNodeWalletCode]);
        uint256 hashwallet = tvm.hash(b.toCell());
        delete b;
        require(hashwallet == hashwalletsalt, ERR_SENDER_NO_ALLOWED);
        b.store(_code[m_BlockKeeperPreEpochCode]);
        uint256 hashpreepoch = tvm.hash(b.toCell());
        _reputationTime = reputationTime + epochDuration;
        delete b;
        require(hashpreepoch == hashpreepochsalt, ERR_SENDER_NO_ALLOWED);
        if (isContinue) {
            require(msg.sender == BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey), ERR_SENDER_NO_ALLOWED);
        }
        else {
            require(msg.sender == BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, _seqNoStart), ERR_SENDER_NO_ALLOWED);
        }
        _waitStep = waitStep;
        _unixtimeFinish = block.timestamp + epochDuration;
        _unixtimeStart = block.timestamp;
        _bls_pubkey = bls_pubkey;
        _stake = msg.currencies[CURRENCIES_ID];
        BlockKeeperContractRoot(_root).increaseActiveBlockKeeperNumber{value: 0.1 ton, flag: 1}(_owner_pubkey, _seqNoStart, _stake);
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).updateLockStake{value: 0.1 ton, flag: 1}(_seqNoStart, _unixtimeFinish, msg.currencies[CURRENCIES_ID]);
    }

    function setStake(uint256 totalStake, uint128 numberOfActiveBlockKeepers) public senderIs(_root) accept {
        _totalStake = totalStake;
        _numberOfActiveBlockKeepers = numberOfActiveBlockKeepers;
    } 

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET) { return; }
        mintshell(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET);
    }

    function getBLSPrivateKey(bytes key) public view minValue(0.2 ton) senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] , _root, _owner_pubkey)) accept {
        getMoney();
        key;
        //SEND TO SLASHING SYSTEM
    }

    function sendRequestToSlashBlockKeeper(uint256 slashpubkey, uint64 slashseqNoStart, uint128 numberOfActiveBlockKeepers) public view senderIs(_root) accept {        
        getMoney();
        BlockKeeperSlashContract(BlockKeeperLib.calculateBlockKeeperSlashAddress(_code[m_BlockKeeperSlashCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, slashpubkey, slashseqNoStart)).getRequestToSlashBlockKeeper{value: 0.5 ton, flag: 1}(_owner_pubkey, _seqNoStart, numberOfActiveBlockKeepers);
    }

    function continueStake(uint32 epochDuration, uint64 waitStep, bytes bls_pubkey) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)) accept {
        getMoney();
        require(_isContinue == false, ERR_EPOCH_ALREADY_CONTINUE);
        _epochDurationContinue = epochDuration;
        _waitStepContinue = waitStep;
        _bls_pubkeyContinue = bls_pubkey;
        _isContinue = true;
        _stakeContinue = msg.currencies[CURRENCIES_ID];
    }

    function cancelContinueStake() public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)) accept {
        getMoney();
        require(_isContinue == true, ERR_EPOCH_ALREADY_CONTINUE);
        _isContinue = false;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(_stakeContinue);
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).cancelContinueStake{value: 0.1 ton, currencies: data_cur, flag: 1}(_seqNoStart);
        _stakeContinue = 0;
    }

    function slash() public view senderIs(BlockKeeperLib.calculateBlockKeeperSlashAddress(_code[m_BlockKeeperSlashCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, _seqNoStart)) accept {
        getMoney();
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).slashStake{value: 0.1 ton, flag: 1}(_seqNoStart);
        this.destroy{value: 0.1 ton, flag: 1}(true);
    }

    function touch() public saveMsg {       
        if (_unixtimeFinish < block.timestamp) { tvm.accept(); }
        else { return; } 
        getMoney();
        if (address(this).balance < 0.2 ton) { return; }
        BlockKeeperContractRoot(_root).canDeleteEpoch{value: 0.1 ton, flag: 1}(_owner_pubkey, _seqNoStart, _stake, _epochDuration, _reputationTime, _totalStake, _numberOfActiveBlockKeepers, _unixtimeStart);
    }

    function canDelete(uint256 reward) public senderIs(_root) saveMsg {  
        reward;
        this.destroy{value: 0.1 ton, flag: 1}(false);
    }

    function destroy(bool isSlash) public senderIs(address(this)) accept {            
        BlockKeeperContractRoot(_root).decreaseActiveBlockKeeperNumber{value: 0.3 ton, flag: 1}(_owner_pubkey, _seqNoStart, _stake);   
        if (isSlash) {
            mapping(uint32 => varuint32) data_cur;
            data_cur[CURRENCIES_ID] = varuint32(_stakeContinue);
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).cancelContinueStake{value: 0.1 ton, currencies: data_cur, flag: 1}(_seqNoStart);
            selfdestruct(_root);
        } else {
            if (_isContinue){
                mapping(uint32 => varuint32) data_curr;
                data_curr[CURRENCIES_ID] = varuint32(_stakeContinue);
                AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).deployBlockKeeperContractContinueAfterDestroy{value: 0.1 ton, flag: 1, currencies: data_curr}(_epochDurationContinue, _waitStepContinue, _bls_pubkeyContinue, _seqNoStart, _reputationTime);
            }
            TvmCell data = BlockKeeperLib.composeBlockKeeperCoolerEpochStateInit(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, _seqNoStart);
            mapping(uint32 => varuint32) data_cur = address(this).currencies;
            data_cur[CURRENCIES_ID] -= varuint32(_stakeContinue);
            new BlockKeeperCooler {
                stateInit: data, 
                value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET),
                currencies: data_cur,
                wid: 0, 
                flag: 1
            } (_waitStep, BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey), _root, _bls_pubkey, _slashMember, _slashed, _code, _walletId); 
            selfdestruct(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey));
        }
    } 
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        address root, 
        uint64 seqNoStart,
        uint32 unixtimeFinish,
        address owner,
        uint256 continueStakes,
        bool isContinue,
        uint256 walletId) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _unixtimeFinish, BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey), _stakeContinue, _isContinue, _walletId);
    }

    function getEpochCoolerCodeHash() external view returns(uint256 epochCoolerCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperCoolerEpochCode(_code[m_BlockKeeperEpochCoolerCode], address(this)));
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperEpoch");
    }
}
