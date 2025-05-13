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
    uint128 _epochDuration;
    uint128 _numberOfActiveBlockKeepers;
    uint32 _unixtimeStart;

    uint128 _sumReputationCoef; 
    uint128 _sumReputationCoefContinue;
    uint16 _signerIndex;
    uint16 _signerIndexContinue;
    mapping(uint8 => string) _proxyListContinue;

    LicenseStake[] _licenses;
    LicenseStake[] _licensesContinue;

    optional(uint128) _virtualStake;
    optional(uint128) _virtualStakeContinue;
    constructor (
        uint64 waitStep,
        uint32 epochDuration,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        bool isContinue,
        uint128 sumReputationCoef,
        uint16 signerIndex,
        LicenseStake[] licenses,
        optional(uint128) virtualStake
    ) {
        _code = code;
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
        _unixtimeFinish = block.timestamp + epochDuration;
        _unixtimeStart = block.timestamp;
        _bls_pubkey = bls_pubkey;
        _stake = msg.currencies[CURRENCIES_ID];
        _signerIndex = signerIndex;
        _licenses = licenses;
        _virtualStake = virtualStake;
        BlockKeeperContractRoot(_root).increaseActiveBlockKeeperNumber{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _seqNoStart, _stake, uint128(_sumReputationCoef / licenses.length), _virtualStake);
        AckiNackiBlockKeeperNodeWallet(_owner_address).updateLockStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, _unixtimeFinish, msg.currencies[CURRENCIES_ID], _bls_pubkey, _signerIndex, _licenses, isContinue);
    }

    function setStake(uint256 totalStake, uint128 numberOfActiveBlockKeepers) public senderIs(_root) accept {
        _totalStake = totalStake;
        _numberOfActiveBlockKeepers = numberOfActiveBlockKeepers;
    } 

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET);
    }

    function continueStake(uint32 epochDuration, uint64 waitStep, bytes bls_pubkey, uint16 signerIndex, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)) accept {
        ensureBalance();
        if (_isContinue == true) {
            AckiNackiBlockKeeperNodeWallet(msg.sender).continueStakeNotAccept{value: 0.1 vmshell, flag: 1}(_seqNoStart, bls_pubkey, signerIndex);
            return;
        }
        _epochDurationContinue = epochDuration;
        _waitStepContinue = waitStep;
        _bls_pubkeyContinue = bls_pubkey;
        _isContinue = true;
        _signerIndexContinue = signerIndex;
        _sumReputationCoefContinue = rep_coef;
        _licensesContinue = licenses;
        _stakeContinue = msg.currencies[CURRENCIES_ID];
        _virtualStakeContinue = virtualStake;
        _proxyListContinue = ProxyList;
    }

    function cancelContinueStake() public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)) accept {
        ensureBalance();
        require(_isContinue == true, ERR_EPOCH_ALREADY_CONTINUE);
        _isContinue = false;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(_stakeContinue);
        AckiNackiBlockKeeperNodeWallet(msg.sender).cancelContinueStake{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_seqNoStart, _bls_pubkeyContinue, _signerIndexContinue, _licensesContinue);
        _stakeContinue = 0;
    }

    function slash(uint8 slash_type) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)) accept {
        ensureBalance();
        this.destroy_slash{value: 0.1 vmshell, flag: 1}(slash_type);
    }

    function touch() public saveMsg {       
        if (_unixtimeFinish < block.timestamp) { tvm.accept(); }
        else { return; } 
        ensureBalance();
        BlockKeeperContractRoot(_root).canDeleteEpoch{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _seqNoStart, _stake, _sumReputationCoef / uint128(_licenses.length) + _epochDuration + block.timestamp - _unixtimeFinish, _totalStake, _numberOfActiveBlockKeepers, _unixtimeStart, _virtualStake);
    }

    function canDelete(uint256 reward) public senderIs(_root) saveMsg {  
        reward;
        this.destroy{value: 0.1 vmshell, flag: 1}();
    }

    function destroy() public senderIs(address(this)) accept {   
        address wallet = BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey);         
        TvmCell data = BlockKeeperLib.composeBlockKeeperCoolerEpochStateInit(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, _seqNoStart);
        mapping(uint32 => varuint32) data_cur = address(this).currencies;
        data_cur[CURRENCIES_ID] -= varuint32(_stakeContinue);
        if (_isContinue){
            mapping(uint32 => varuint32) data_curr;
            data_curr[CURRENCIES_ID] = varuint32(_stakeContinue);
            AckiNackiBlockKeeperNodeWallet(wallet).deployBlockKeeperContractContinueAfterDestroy{value: 0.1 vmshell, flag: 1, currencies: data_curr}(_epochDurationContinue, _waitStepContinue, _bls_pubkeyContinue, _seqNoStart, _sumReputationCoefContinue, _signerIndexContinue, _licensesContinue, _licenses, _virtualStakeContinue, block.timestamp - _unixtimeStart);
            BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).setNewProxyList{value: 0.1 vmshell, flag: 1}(_seqNoStart, _proxyListContinue); 
        }
        if (!_isContinue) {
            BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}(_seqNoStart); 
        }
        new BlockKeeperCooler {
            stateInit: data, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET),
            currencies: data_cur,
            wid: 0, 
            flag: 1
        } (_waitStep, wallet, _root, _bls_pubkey, _code, _signerIndex, _licenses, _epochDuration, _isContinue); 
        selfdestruct(wallet);
    }

    function destroy_slash(uint8 slash_type) public senderIs(address(this)) accept {   
        address wallet = BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey);         
        mapping(uint32 => varuint32) data_cur;
        if (slash_type == FULL_STAKE_SLASH) {
            data_cur[CURRENCIES_ID] = varuint32(_stakeContinue);
            AckiNackiBlockKeeperNodeWallet(wallet).slashStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, FULL_STAKE_SLASH, 0, _licenses);
            BlockKeeperContractRoot(_root).decreaseActiveBlockKeeperNumber{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _seqNoStart, _stake, _sumReputationCoef / uint128(_licenses.length), true, _virtualStake);
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).cancelContinueStake{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_seqNoStart, _bls_pubkeyContinue, _signerIndexContinue, _licensesContinue);
            selfdestruct(_root);
            return;
        } 
        uint256 slash_stake = _stake * slash_type / 100;
        AckiNackiBlockKeeperNodeWallet(wallet).slashStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, slash_type, slash_stake, _licenses);
        data_cur[CURRENCIES_ID] = varuint32(slash_stake);
        BlockKeeperContractRoot(_root).decreaseStakes{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_owner_pubkey, _seqNoStart, slash_stake);
        _stake = _stake - slash_stake;
    } 

    function changeReputationHelper(uint8 i, bool is_inc, uint128 value, uint256 num) private {
        if (_licensesContinue[i].num == num) {
            if (is_inc == true) {
                _sumReputationCoefContinue += value;
            } else {
                _sumReputationCoefContinue -= value;
            }
        }
    }

    function changeReputation(bool is_inc, uint128 value, uint256 num) public senderIs(_owner_address) {
        _sumReputationCoef -= value;
        if (_isContinue) {
            require(_licensesContinue.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
            uint8 i = 0;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }  
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }  
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }  
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }  
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }  
            i += 1;
            if (i + 1 >= _licensesContinue.length) {
                changeReputationHelper(i, is_inc, value, num);
            }            
        }
    }

    function changeReputationContinue(uint128 value) public senderIs(_owner_address) {
        _sumReputationCoefContinue -= value;
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
        uint16 signerIndex,
        uint16 signerIndexContinue,
        mapping(uint8 => string) proxyListContinue) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _unixtimeFinish, BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey), _stakeContinue, _isContinue, _signerIndex, _signerIndexContinue, _proxyListContinue);
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
