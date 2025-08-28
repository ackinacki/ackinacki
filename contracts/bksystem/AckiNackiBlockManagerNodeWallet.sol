// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";
import "./BlockManagerContractRoot.sol";
import "./LicenseBM.sol";

contract AckiNackiBlockManagerNodeWallet is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root;

    optional(uint256) _license_num;
    mapping(uint256=>bool) _whiteListLicense;
    address _licenseBMRoot;
    optional(uint32) _start_bm;
    uint128 _wallet_reward;
    uint128 _slashSum;
    uint32 _start_time;
    uint32 _stop_seqno;
    uint64 _waitStep;
    uint32 _rewarded;
    uint32 _epochStart;
    uint32 _epochEnd;
    uint64 _walletLastTouch;
    bool _isSlashing = false;
    uint32 _tryReward;
    uint8 _walletTouch;
    uint256 _signing_pubkey;

    constructor (
        TvmCell LicenseBMCode,
        mapping(uint256=>bool) whiteListLicense,
        address licenseBMRoot,
        uint32 start_time,
        uint64 waitStep,
        uint256 signing_pubkey,
        uint8 walletTouch
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);

        _root = root;
        require(msg.sender == _root, ERR_SENDER_NO_ALLOWED);

        _code[m_LicenseBMCode] = LicenseBMCode;
        _whiteListLicense = whiteListLicense;
        _licenseBMRoot = licenseBMRoot;
        _start_time = start_time;
        _waitStep = waitStep;
        _signing_pubkey = signing_pubkey;
        _walletTouch = walletTouch;
    }

    function removeLicense(uint256 license_number) public senderIs(BlockKeeperLib.calculateLicenseBMAddress(_code[m_LicenseBMCode], license_number, _licenseBMRoot)) accept {
        ensureBalance();
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST); 
        require(_license_num.get() == license_number, ERR_WRONG_LICENSE); 
        require(_start_bm.hasValue() == false, ERR_ALREADY_STARTED);
        require(_stop_seqno + _waitStep < block.seqno, ERR_LICENSE_BUSY);
        _license_num = null;
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = address(this).currencies[CURRENCIES_ID];
        LicenseBMContract(msg.sender).deleteWallet{value: 0.1 vmshell, currencies: data, flag: 1}(_wallet_reward, _slashSum);
        _wallet_reward = 0;
        _slashSum = 0;
    }

    function setSigningPubkey(uint256 pubkey) public onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        _signing_pubkey = pubkey;
    }

    function setLicenseWhiteList(mapping(uint256 => bool) whiteListLicense) public onlyOwnerPubkey(_owner_pubkey) accept {
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        require(whiteListLicense.keys().length <= MAX_LICENSE_NUMBER_WHITELIST_BM, ERR_TOO_MANY_LICENSES);
        ensureBalance();
        _whiteListLicense = whiteListLicense;
    }

    function addLicense(uint256 license_number, uint128 reward, uint128 slashSum) public senderIs(BlockKeeperLib.calculateLicenseBMAddress(_code[m_LicenseBMCode], license_number, _licenseBMRoot)) accept {
        ensureBalance();
        if ((_license_num.hasValue()) || (_whiteListLicense[license_number] != true)) {
            LicenseBMContract(msg.sender).notAcceptLicense{value: 0.1 vmshell, flag: 1}(_owner_pubkey);
            return;
        }

        _license_num = license_number;
        _wallet_reward = reward;
        _slashSum = slashSum;
        LicenseBMContract(msg.sender).acceptLicense{value: 0.1 vmshell, flag: 1}(_owner_pubkey);
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_MANAGER_WALLET * 3) { return; }
        gosh.mintshellq(FEE_DEPLOY_BLOCK_MANAGER_WALLET * 3);
    }
    
    function slash() public senderIs(address(this)) {
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST);
        require(_isSlashing == false, ERR_ALREADY_SLASH);
        _walletLastTouch = block.seqno;
        ensureBalance();
        _stop_seqno = block.seqno;
        _isSlashing = true;
        if (_tryReward + MANAGER_REWARD_WAIT > block.seqno) {
            return;
        }
        if (block.timestamp < _epochEnd) {
            finalSlash();
            return;
        }
        BlockManagerContractRoot(_root).getReward{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _signing_pubkey, address(this), _rewarded, _start_bm.get(), false);
        _start_bm = null;
    }

    function noRewards() public senderIs(_root) accept {
        if (_isSlashing) {
            finalSlash();
        }
        _tryReward = 0;
    }

    function finalSlash() private {
        mapping(uint32 => varuint32) data_cur;
        uint128 diff = 0;
        if (_wallet_reward > _slashSum) {
            diff = _wallet_reward - _slashSum;
        }
        uint128 slash_value = math.min(gosh.calcminstakebm(diff, block.timestamp - _start_time), uint128(address(this).currencies[CURRENCIES_ID]));
        data_cur[CURRENCIES_ID] = varuint32(slash_value);
        _slashSum += slash_value;
        _isSlashing = false;
        BlockManagerContractRoot(_root).slashed{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_owner_pubkey);
    }

    function withdrawToken(address to, varuint32 value) public view senderIs(BlockKeeperLib.calculateLicenseBMAddress(_code[m_LicenseBMCode], _license_num.get(), _licenseBMRoot)) accept {
        ensureBalance();  
        require(_start_bm.hasValue() == false, ERR_ALREADY_STARTED);
        require(_stop_seqno + _waitStep < block.seqno, ERR_LICENSE_BUSY);
        require(value <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        to.transfer({value: 0.1 vmshell, currencies: data, flag: 1});
    }

    function startBM() public onlyOwnerPubkey(_owner_pubkey) accept {
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST);
        require(_start_bm.hasValue() == false, ERR_ALREADY_STARTED);
        require(_stop_seqno + _waitStep < block.seqno, ERR_LICENSE_BUSY);
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        uint128 diff = 0;
        if (_wallet_reward > _slashSum) {
            diff = _wallet_reward - _slashSum;
        }
        require(gosh.calcminstakebm(diff, block.timestamp - _start_time) <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        ensureBalance();
        _start_bm = block.timestamp;
        _rewarded = block.timestamp;
        BlockManagerContractRoot(_root).increaseBM{value: 0.1 vmshell, flag: 1}(_owner_pubkey);
    }

    function getReward() public onlyOwnerPubkey(_owner_pubkey) accept {
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST);
        require(_start_bm.hasValue(), ERR_ALREADY_CONFIRMED);
        if (_epochEnd != 0) {        
            require(block.timestamp > _epochEnd, ERR_ALREADY_REWARDED);
        }
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        ensureBalance();
        _tryReward = block.seqno;
        BlockManagerContractRoot(_root).getReward{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _signing_pubkey, address(this), _rewarded, _start_bm.get(), false);
    }

    function takeReward(uint8 walletTouch, uint64 waitStep, uint32 epochStart, uint32 epochEnd) public senderIs(_root) accept {
        ensureBalance();
        _walletTouch = walletTouch;
        _waitStep = waitStep;
        _rewarded = block.timestamp;
        _epochStart = epochStart;
        _epochEnd = epochEnd;
        _tryReward = 0;
        _wallet_reward += uint128(msg.currencies[CURRENCIES_ID]);
        if (_isSlashing) {
            finalSlash();
        }
    }

    function stopBM() public onlyOwnerPubkey(_owner_pubkey) accept {
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST);
        require(_start_bm.hasValue(), ERR_ALREADY_CONFIRMED);
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        _tryReward = block.seqno;
        ensureBalance();
        BlockManagerContractRoot(_root).getReward{value: 0.1 vmshell, flag: 1}(_owner_pubkey, _signing_pubkey, address(this), _rewarded, _start_bm.get(), true);
        _start_bm = null;
        _stop_seqno = block.seqno;
    }

    //Fallback/Receive
    receive() external {
        tvm.accept();
        ensureBalance();
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        address root,
        uint256 balance,
        optional(uint256) license_num,
        uint128 minstake,
        uint256 signerPubkey
    ) {
        uint128 diff = 0;
        if (_wallet_reward > _slashSum) {
            diff = _wallet_reward - _slashSum;
        }
        return  (_owner_pubkey, _root, address(this).currencies[CURRENCIES_ID], _license_num, gosh.calcminstakebm(diff, block.timestamp - _start_time), _signing_pubkey);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "AckiNackiBlockManagerNodeWallet");
    }
}
