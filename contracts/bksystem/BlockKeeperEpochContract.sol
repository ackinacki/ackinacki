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
    uint64 _seqNoFinish;
    bytes _bls_pubkey;
    bool _isContinue = false;
    uint64 _waitStep;
    uint128 _stake;
    address _owner_address;

    bytes _bls_pubkeyContinue;
    uint128 _stakeContinue;
    uint128 _numberOfActiveBlockKeepers;
    uint32 _unixtimeStart;


    uint128 _sumReputationCoef; 
    uint128 _sumReputationCoefContinue;
    uint16 _signerIndex;
    uint16 _signerIndexContinue;
    mapping(uint8 => string) _proxyListContinue;
    string _myIp;

    LicenseStake[] _licenses;
    LicenseStake[] _licensesContinue;

    optional(uint128) _virtualStake;
    optional(uint128) _virtualStakeContinue;

    uint128 _reward_sum;

    uint64 _busy_seqno = 0;

    bool _is_full_slashing = false;
    bool _is_touching = false;
    optional(uint8) _slash_type;

    uint256 _gparam;

    constructor (
        uint64 waitStep,
        uint64 epochDuration,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        uint128 sumReputationCoef,
        uint16 signerIndex,
        LicenseStake[] licenses,
        optional(uint128) virtualStake,
        uint128 reward_sum,
        string myIp,
        bool isContinue,
        optional(uint32) timeStart
    ) {
        _code = code;
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, uint256 hashwalletsalt, uint256 hashpreepochsalt, address root) = abi.decode(data, (string, uint256, uint256, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _root = root;
        TvmBuilder b;
        b.store(_code[m_AckiNackiBlockKeeperNodeWalletCode]);
        uint256 hashwallet = tvm.hash(b.toCell());
        delete b;
        require(hashwallet == hashwalletsalt, ERR_SENDER_NO_ALLOWED);
        b.store(_code[m_BlockKeeperPreEpochCode]);
        uint256 hashpreepoch = tvm.hash(b.toCell());
        _sumReputationCoef = sumReputationCoef;
        delete b;
        require(hashpreepoch == hashpreepochsalt, ERR_SENDER_NO_ALLOWED);
        _owner_address = BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey);
        if (isContinue) {
            require(msg.sender == _owner_address, ERR_SENDER_NO_ALLOWED);
        }
        else {
            require(msg.sender == BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, _seqNoStart), ERR_SENDER_NO_ALLOWED);
            BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).toClose{value: 0.1 vmshell, flag: 1}(_seqNoStart); 
        }
        _waitStep = waitStep;
        _seqNoFinish = block.seqno + epochDuration;
        if (timeStart.hasValue()) {
            _unixtimeStart = timeStart.get();
        } else {
            _unixtimeStart = block.timestamp;
        }
        _bls_pubkey = bls_pubkey;
        _stake = uint128(msg.currencies[CURRENCIES_ID]);
        _signerIndex = signerIndex;
        _licenses = licenses;
        _virtualStake = virtualStake;
        _reward_sum = reward_sum;
        _myIp = myIp;
        BlockKeeperContractRoot(_root).increaseActiveBlockKeeperNumber{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _seqNoStart, _stake, _sumReputationCoef, _virtualStake);
        AckiNackiBlockKeeperNodeWallet(_owner_address).updateLockStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, _seqNoFinish, msg.currencies[CURRENCIES_ID], _bls_pubkey, _signerIndex, _licenses);
    }

    function setStake(uint128 numberOfActiveBlockKeepers, uint256 gparam) public senderIs(_root) accept {
        ensureBalance();
        _numberOfActiveBlockKeepers = numberOfActiveBlockKeepers; 
        _gparam = gparam;
    } 

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET) { return; }
        gosh.mintshellq(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET);
    }

    function continueStake(bytes bls_pubkey, uint16 signerIndex, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 sumReputationCoef) public senderIs(_owner_address) accept {
        ensureBalance();
        if (_isContinue == true) {
            AckiNackiBlockKeeperNodeWallet(msg.sender).continueStakeNotAccept{value: 0.1 vmshell, flag: 1}(_seqNoStart, bls_pubkey, signerIndex, licenses);
            return;
        }
        _bls_pubkeyContinue = bls_pubkey;
        _isContinue = true;
        _signerIndexContinue = signerIndex;
        _licensesContinue = licenses;
        _stakeContinue = uint128(msg.currencies[CURRENCIES_ID]);
        _virtualStakeContinue = virtualStake;
        _proxyListContinue = ProxyList;
        _sumReputationCoefContinue = sumReputationCoef;
        AckiNackiBlockKeeperNodeWallet(msg.sender).continueStakeAccept{value: 0.1 vmshell, flag: 1}(_seqNoStart);
    }

    function cancelContinueStake() public senderIs(_owner_address) accept {
        ensureBalance();
        require(_isContinue == true, ERR_EPOCH_NOT_CONTINUE);
        cancelContinueStakeIn();
    }

    function cancelContinueStakeIn() private {
        _isContinue = false;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(_stakeContinue);
        AckiNackiBlockKeeperNodeWallet(msg.sender).cancelContinueStake{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_seqNoStart, _bls_pubkeyContinue, _signerIndexContinue, _licensesContinue);
        _stakeContinue = 0;
    }

    function slash(uint8 slash_type) public senderIs(_owner_address) accept {
        ensureBalance();
        if (_is_touching) {
            _slash_type = slash_type;
            return;
        }
        if (slash_type == FULL_STAKE_SLASH) {
            _is_full_slashing = true;
            this.destroy_full_slash{value: 0.1 vmshell, flag: 1}();
        } else {
            part_slash(slash_type);
        }
    }

    function touch() public accept {   
        ensureBalance();    
        if (_seqNoFinish > block.seqno) { return; }
        if (block.seqno <= _busy_seqno + EPOCH_CLIFF) { return; }
        require(_licenses.length > 0, ERR_NOT_SUPPORT);
        if (_is_full_slashing) { return; }
        _is_touching = true;
        _busy_seqno = block.seqno;
        BlockKeeperContractRoot(_root).decreaseActiveBlockKeeper{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _sumReputationCoef, _seqNoStart, _stake, _virtualStake, _reward_sum, _gparam, false);
    }

    function cantDelete() public senderIs(_root) accept {  
        ensureBalance();
        _is_touching = false;
        if (_slash_type.hasValue()) {
            uint8 slash_type = _slash_type.get();
            delete _slash_type;
            if (slash_type == FULL_STAKE_SLASH) {
                _is_full_slashing = true;
                this.destroy_full_slash{value: 0.1 vmshell, flag: 1}();
            } else {
                part_slash(slash_type);
            }
        }
    }

    function canDelete(uint256 reward, uint64 epochDuration, uint64 waitStep, uint128 reward_sum) public view senderIs(_root) accept {  
        ensureBalance();
        reward;
        this.destroy{value: 0.1 vmshell, flag: 1}(epochDuration, waitStep, reward_sum);
    }

    function destroy(uint64 epochDurationContinue, uint64 waitStepContinue, uint128 reward_sum_continue) public view senderIs(address(this)) accept {   
        ensureBalance();
        TvmCell data = BlockKeeperLib.composeBlockKeeperCoolerEpochStateInit(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, _seqNoStart);
        if (_isContinue){
            BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).setNewProxyList{value: 0.1 vmshell, flag: 1}(_seqNoStart, _proxyListContinue); 
        } else {
            BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}(_seqNoStart); 
        }
        new BlockKeeperCooler {
            stateInit: data, 
            value: 0.1 vmshell,
            wid: 0, 
            flag: 161,
            bounce: false
        } (_waitStep, _owner_address, _root, _signerIndex, _licenses, _stake, _isContinue, _stakeContinue, epochDurationContinue, waitStepContinue, _bls_pubkeyContinue, _signerIndexContinue, _licensesContinue, _virtualStakeContinue, reward_sum_continue, _myIp, _unixtimeStart, _sumReputationCoefContinue, _slash_type); 
    }

    function destroy_full_slash() public view senderIs(address(this)) accept {      
        ensureBalance();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(_stakeContinue);
        AckiNackiBlockKeeperNodeWallet(_owner_address).slashStake{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_seqNoStart, FULL_STAKE_SLASH, 0, _licenses, _isContinue, _bls_pubkeyContinue, _signerIndexContinue, _licensesContinue);
        BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}(_seqNoStart); 
        BlockKeeperContractRoot(_root).decreaseActiveBlockKeeper{value: 0.1 vmshell, flag: 161, bounce: false}(_owner_pubkey, _sumReputationCoef, _seqNoStart, _stake, _virtualStake, _reward_sum, _gparam, true);
    }

    function part_slash(uint8 slash_type) private {            
        uint128 slash_stake = 0;
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = varuint32(_stakeContinue);
        for (uint i = 0; i < _licenses.length; i++) {
            slash_stake = slashPartHelper(i, slash_type, slash_stake);   
        }
        AckiNackiBlockKeeperNodeWallet(_owner_address).slashStake{value: 0.1 vmshell, currencies: data, flag: 1}(_seqNoStart, slash_type, slash_stake, _licenses, _isContinue, _bls_pubkeyContinue, _signerIndexContinue, _licensesContinue);
        data[CURRENCIES_ID] = varuint32(slash_stake);
        _stake = _stake - slash_stake;
        if (_virtualStake.hasValue()) {
            uint128 value = _virtualStake.get();
            slash_stake = value * slash_type / FULL_STAKE_PERCENT;
            value -= slash_stake;
            _virtualStake = value;
        }
        BlockKeeperContractRoot(_root).decreaseStakes{value: 0.1 vmshell, currencies: data, flag: 1}(_owner_pubkey, _seqNoStart, slash_stake, _sumReputationCoef);
        if (_isContinue) {
            _isContinue = false;
            _stakeContinue = 0;
        }
    } 

    function slashPartHelper(uint i, uint8 slash_type, uint128 slash_stake) private returns(uint128) {
        uint128 slash_value = _licenses[i].stake * slash_type / FULL_STAKE_PERCENT;
        _licenses[i].stake -= slash_value;
        slash_stake += slash_value;
        return slash_stake;
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        address root, 
        uint64 seqNoStart,
        uint64 seqNoFinish,
        address owner,
        uint256 continueStakes,
        bool isContinue,
        uint16 signerIndex,
        uint16 signerIndexContinue,
        mapping(uint8 => string) proxyListContinue,
        string myIp) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _seqNoFinish, _owner_address, _stakeContinue, _isContinue, _signerIndex, _signerIndexContinue, _proxyListContinue, _myIp);
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
