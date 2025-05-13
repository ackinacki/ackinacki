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
import "./BlockManagerContractRoot.sol";
import "./LicenseBM.sol";

contract AckiNackiBlockManagerNodeWallet is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 

    address _bm_root = address.makeAddrStd(0, 0);
    optional(uint256) _license_num;
    mapping(uint256=>bool) _whiteListLicense;
    address _licenseBMRoot;
    optional(uint32) _start_bm;
    uint32 _stop_bm; 
    optional(uint256) _work_key;
    uint256 _hash_wallet;

    constructor (
        TvmCell LicenseBMCode,
        mapping(uint256=>bool) whiteListLicense,
        address licenseBMRoot
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _root = root;
        require(msg.sender == _root, ERR_SENDER_NO_ALLOWED);
        _code[m_LicenseBMCode] = LicenseBMCode;
        _whiteListLicense = whiteListLicense;
        _licenseBMRoot = licenseBMRoot;
    }

    function removeLicense(uint256 license_number) public senderIs(BlockKeeperLib.calculateLicenseBMAddress(_code[m_LicenseBMCode], license_number, _licenseBMRoot)) accept {
        ensureBalance();
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST); 
        require(_license_num.get() == license_number, ERR_WRONG_LICENSE); 
        _license_num = null;
        LicenseBMContract(msg.sender).deleteLicense{value: 0.1 vmshell, flag: 1}();
    }

    function setLicenseWhiteList(mapping(uint256 => bool) whiteListLicense) public onlyOwnerPubkey(_owner_pubkey) accept saveMsg {
        ensureBalance();
        _whiteListLicense = whiteListLicense;
    }

    function addLicense(uint256 license_number) public senderIs(BlockKeeperLib.calculateLicenseBMAddress(_code[m_LicenseBMCode], license_number, _licenseBMRoot)) accept {
        ensureBalance();
        if (_license_num.hasValue()) {
            LicenseBMContract(msg.sender).notAcceptLicense{value: 0.1 vmshell, flag: 1}(_owner_pubkey);
            return;
        }
        _license_num = license_number;
        LicenseBMContract(msg.sender).acceptLicense{value: 0.1 vmshell, flag: 1}(_owner_pubkey);
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_MANAGER_WALLET * 3) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_MANAGER_WALLET * 3);
    }

    function withdrawToken(uint256 license_number, address to, varuint32 value) public view senderIs(BlockKeeperLib.calculateLicenseBMAddress(_code[m_LicenseBMCode], license_number, _licenseBMRoot)) accept {
        ensureBalance();
        require(value <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST);
        require(_license_num.get() == license_number, ERR_WRONG_LICENSE);
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        to.transfer({value: 0.1 vmshell, currencies: data, flag: 1});
    }

    function startBM(uint256 key) public onlyOwnerPubkey(_owner_pubkey) accept saveMsg {
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST);
        require(_start_bm.hasValue() == false, ERR_ALREADY_CONFIRMED);
        _start_bm = block.timestamp;
        _work_key = key;
        BlockManagerContractRoot(_root).increaseBM{value: 0.1 ton, flag: 1}(_owner_pubkey);
    }

    function stopBM() public onlyOwnerPubkey(_owner_pubkey) accept saveMsg {
        require(_license_num.hasValue(), ERR_LICENSE_NOT_EXIST);
        require(_start_bm.hasValue(), ERR_ALREADY_CONFIRMED);
        _stop_bm = block.timestamp;
        _work_key = null;
        BlockManagerContractRoot(_root).getReward{value: 0.1 ton, flag: 1}(_owner_pubkey, _start_bm.get(), _stop_bm);
        _start_bm = null;
    }

    function tryUpdateCode() public onlyOwnerPubkey(_owner_pubkey) accept saveMsg {
        BlockManagerContractRoot(_bm_root).tryUpdateCode{value: 0.1 ton, flag: 1}(_owner_pubkey, _hash_wallet);
        ///NEED TO FINISH IT 
    }

    function updateCode(TvmCell newcode, TvmCell cell, uint256 new_hash) public senderIs(_bm_root) {
        _hash_wallet = new_hash;
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private pure {
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        address root,
        uint256 balance,
        optional(uint256) license_num,
        optional(uint256) work_key
    ) {
        return  (_owner_pubkey, _root, address(this).currencies[CURRENCIES_ID], _license_num, _work_key);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "AckiNackiBlockManagerNodeWallet");
    }
}
