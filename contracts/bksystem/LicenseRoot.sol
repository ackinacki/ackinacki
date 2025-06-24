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
import "./License.sol";
import "./LicenseBM.sol";

contract LicenseRoot is Modifiers {
    string constant version = "1.0.0";

    optional(address) _owner_wallet;
    mapping(uint8 => TvmCell) _code;
    uint32 _timeUnlock;
    uint256 _license_number;
    uint256 _license_number_bm;
    address _rootElection;
    address _rootBM;
    address _lastAddress;
    address _lastAddressBM;

    constructor (
        uint32 timeUnlock,
        uint256 license_number,
        uint256 license_number_bm,
        address rootElection,
        address rootBM
    ) {
        ensureBalance();
        _timeUnlock = timeUnlock;
        _license_number = license_number;
        _license_number_bm = license_number_bm;
        _rootElection = rootElection;
        _rootBM = rootBM;
    }

    function ensureBalance() private pure {
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshell(ROOT_BALANCE);
    }

    function deployLicense(uint256 pubkey) public accept {
        ensureBalance();
        require(block.timestamp > _timeUnlock, ERR_NOT_READY);
        TvmCell data = BlockKeeperLib.composeLicenseStateInit(_code[m_LicenseCode], _license_number, address(this));
        _license_number += 1;
        _lastAddress = new LicenseContract {stateInit: data, value: varuint16(FEE_DEPLOY_LICENSE), wid: 0, flag: 1}(pubkey, _code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection);
    }
 
    function deployLicenseOwner(uint256 pubkey) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        ensureBalance();
        TvmCell data = BlockKeeperLib.composeLicenseStateInit(_code[m_LicenseCode], _license_number, address(this));
        _license_number += 1;
        _lastAddress = new LicenseContract {stateInit: data, value: varuint16(FEE_DEPLOY_LICENSE), wid: 0, flag: 1}(pubkey, _code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection);
    }

    function deployLicenseBM(uint256 pubkey) public accept {
        ensureBalance();
        require(block.timestamp > _timeUnlock, ERR_NOT_READY);
        TvmCell data = BlockKeeperLib.composeLicenseBMStateInit(_code[m_LicenseBMCode], _license_number_bm, address(this));
        _license_number_bm += 1;
        _lastAddressBM = new LicenseBMContract {stateInit: data, value: varuint16(FEE_DEPLOY_LICENSE_BM), wid: 0, flag: 1}(pubkey, _code[m_AckiNackiBlockManagerNodeWalletCode], _rootBM);
    }
 
    function deployLicenseBMOwner(uint256 pubkey) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        ensureBalance();
        TvmCell data = BlockKeeperLib.composeLicenseBMStateInit(_code[m_LicenseBMCode], _license_number_bm, address(this));
        _license_number_bm += 1;
        _lastAddressBM = new LicenseBMContract {stateInit: data, value: varuint16(FEE_DEPLOY_LICENSE_BM), wid: 0, flag: 1}(pubkey, _code[m_AckiNackiBlockManagerNodeWalletCode], _rootBM);
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept saveMsg { 
        _code[id] = code;
    }

    function setOwner(address wallet) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) {
        _owner_wallet = wallet;
    }

    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept saveMsg {
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
    function getLastAddress() external view returns(address lastAddress, address lastAddressBM, uint256 num) {
        return (_lastAddress, _lastAddressBM, _license_number - 1);
    }

    function getLastLicenseNum() external view returns(uint256 num, uint256 numbm) {
        return (_license_number, _license_number_bm);
    }

    function getLicenseAddress(uint256 num) external view returns(address license_address) {
        return BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], num, address(this));
    }

    function getLicenseBMAddress(uint256 num) external view returns(address license_address) {
        return BlockKeeperLib.calculateLicenseBMAddress(_code[m_LicenseBMCode], num, address(this));
    }

    function getVersion() external pure returns(string, string) {
        return (version, "LicenseRoot");
    }   
}
