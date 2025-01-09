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
import "./AckiNackiBlockKeeperNodeWallet.sol";
import "./BlockKeeperCoolerContract.sol";
import "./BlockKeeperEpochProxyList.sol";


contract BlockKeeperEpoch is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 
    uint64 static _seqNoStart;
    uint32 _unixtimeFinish;
    bytes _bls_pubkey;
    bool _isContinue = false;
    uint64 _waitStep;
    uint256 _stake;
    uint256 _totalStake;
    address _owner_address;

    uint32 _epochDurationContinue;
    uint64 _waitStepContinue;
    bytes _bls_pubkeyContinue;
    uint256 _stakeContinue;
    uint256 _walletId;
    uint32 _epochDuration;
    uint128 _numberOfActiveBlockKeepers;
    uint32 _unixtimeStart;

    uint32 _reputationTime; 
    uint16 _signerIndex;
    uint16 _signerIndexContinue;
    mapping(uint8 => string) _proxyListContinue;

    constructor (
        uint64 waitStep,
        uint32 epochDuration,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        bool isContinue,
        uint256 walletId,
        uint32 reputationTime,
        uint16 signerIndex
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
        _owner_address = BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey);
        if (isContinue) {
            require(msg.sender == _owner_address, ERR_SENDER_NO_ALLOWED);
        }
        else {
            require(msg.sender == BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, _seqNoStart), ERR_SENDER_NO_ALLOWED);
            BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).toClose{value: 0.1 ton, flag: 1}(_seqNoStart); 
        }
        _waitStep = waitStep;
        _unixtimeFinish = block.timestamp + epochDuration;
        _unixtimeStart = block.timestamp;
        _bls_pubkey = bls_pubkey;
        _stake = msg.currencies[CURRENCIES_ID];
        _signerIndex = signerIndex;
        BlockKeeperContractRoot(_root).increaseActiveBlockKeeperNumber{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _seqNoStart, _stake);
        AckiNackiBlockKeeperNodeWallet(_owner_address).updateLockStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, _unixtimeFinish, msg.currencies[CURRENCIES_ID], _bls_pubkey, _signerIndex);
    }

    function setStake(uint256 totalStake, uint128 numberOfActiveBlockKeepers) public senderIs(_root) accept {
        _totalStake = totalStake;
        _numberOfActiveBlockKeepers = numberOfActiveBlockKeepers;
    } 

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET);
    }

    function continueStake(uint32 epochDuration, uint64 waitStep, bytes bls_pubkey, uint16 signerIndex, mapping(uint8 => string) ProxyList) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)) accept {
        getMoney();
        require(_isContinue == false, ERR_EPOCH_ALREADY_CONTINUE);
        _epochDurationContinue = epochDuration;
        _waitStepContinue = waitStep;
        _bls_pubkeyContinue = bls_pubkey;
        _isContinue = true;
        _signerIndexContinue = signerIndex;
        _stakeContinue = msg.currencies[CURRENCIES_ID];
        _proxyListContinue = ProxyList;
    }

    function cancelContinueStake() public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)) accept {
        getMoney();
        require(_isContinue == true, ERR_EPOCH_ALREADY_CONTINUE);
        _isContinue = false;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(_stakeContinue);
        AckiNackiBlockKeeperNodeWallet(msg.sender).cancelContinueStake{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_seqNoStart, _bls_pubkeyContinue, _signerIndexContinue);
        _stakeContinue = 0;
    }

    function slash(uint8 slash_type) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)) accept {
        getMoney();
        optional(uint8) is_slash = slash_type;
        this.destroy{value: 0.1 vmshell, flag: 1}(is_slash);
    }

    function touch() public saveMsg {       
        if (_unixtimeFinish < block.timestamp) { tvm.accept(); }
        else { return; } 
        getMoney();
        BlockKeeperContractRoot(_root).canDeleteEpoch{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _seqNoStart, _stake, _epochDuration, _reputationTime, _totalStake, _numberOfActiveBlockKeepers, _unixtimeStart);
    }

    function canDelete(uint256 reward) public senderIs(_root) saveMsg {  
        reward;
        this.destroy{value: 0.1 vmshell, flag: 1}(null);
    }

    function destroy(optional(uint8) isSlash) public senderIs(address(this)) accept {   
        address wallet = BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey);         
        if (isSlash.hasValue()) {
            uint8 slash_type = isSlash.get();
            if (slash_type == FULL_STAKE_SLASH) {
                mapping(uint32 => varuint32) data_cur;
                data_cur[CURRENCIES_ID] = varuint32(_stakeContinue);
                AckiNackiBlockKeeperNodeWallet(wallet).slashStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, FULL_STAKE_SLASH, 0);
                BlockKeeperContractRoot(_root).decreaseActiveBlockKeeperNumber{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _seqNoStart, _stake);
                AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).cancelContinueStake{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_seqNoStart, _bls_pubkeyContinue, _signerIndexContinue);
                selfdestruct(_root);
                return;
            } 
            if (slash_type == PART_STAKE_0) {
                uint256 slash_stake = _stake * PART_STAKE_PERCENT_0 / 100;
                AckiNackiBlockKeeperNodeWallet(wallet).slashStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, slash_type, slash_stake);
                mapping(uint32 => varuint32) data_cur;
                data_cur[CURRENCIES_ID] = varuint32(slash_stake);
                BlockKeeperContractRoot(_root).decreaseStakes{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_owner_pubkey, _seqNoStart, slash_stake);
                _stake = _stake - slash_stake;
                _totalStake = _totalStake - slash_stake;
                return;
            } 
        } else {
            if (_isContinue){
                mapping(uint32 => varuint32) data_curr;
                data_curr[CURRENCIES_ID] = varuint32(_stakeContinue);
                AckiNackiBlockKeeperNodeWallet(wallet).deployBlockKeeperContractContinueAfterDestroy{value: 0.1 vmshell, flag: 1, currencies: data_curr}(_epochDurationContinue, _waitStepContinue, _bls_pubkeyContinue, _seqNoStart, _reputationTime, _signerIndexContinue);
                BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).setNewProxyList{value: 0.1 ton, flag: 1}(_seqNoStart, _proxyListContinue); 
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
            } (_waitStep, wallet, _root, _bls_pubkey, _code, _walletId, _signerIndex); 
            if (!_isContinue) {
                BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).destroy{value: 0.1 ton, flag: 1}(_seqNoStart); 
            }
            selfdestruct(wallet);
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
        uint256 walletId,
        uint16 signerIndex,
        uint16 signerIndexContinue,
        mapping(uint8 => string) proxyListContinue) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _unixtimeFinish, BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey), _stakeContinue, _isContinue, _walletId, _signerIndex, _signerIndexContinue, _proxyListContinue);
    }

    function getProxyListContinue() external view returns(mapping(uint8 => string) proxyListContinue) 
    {
        return (_proxyListContinue);
    }

    function getEpochCoolerCodeHash() external view returns(uint256 epochCoolerCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperCoolerEpochCode(_code[m_BlockKeeperEpochCoolerCode], address(this)));
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperEpoch");
    }
}
