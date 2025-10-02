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
import "./License.sol";
import "./LicenseBM.sol";

contract LicenseRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;
    uint32 _timeUnlock;
    uint256 _license_number;
    uint256 _license_number_bm;
    address _rootElection;
    address _rootBM;

    uint128 _licenseLeft = 10000;
    uint128 _licenseBMLeft = 10000;

    constructor (
        uint32 timeUnlock,
        uint256 license_number,
        uint256 license_number_bm,
        address rootElection,
        address rootBM
    ) {
        _timeUnlock = timeUnlock;
        _license_number = license_number;
        _license_number_bm = license_number_bm;
        _rootElection = rootElection;
        _rootBM = rootBM;
    }

    function ensureBalance() private pure {
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshellq(ROOT_BALANCE);
    }

    function deployLicense(uint256 pubkey) public accept  {
        ensureBalance();
        require(block.timestamp > _timeUnlock, ERR_NOT_READY);
        TvmCell data = BlockKeeperLib.composeLicenseStateInit(_code[m_LicenseCode], _license_number, address(this));
        _license_number += 1;
        new LicenseContract {stateInit: data, value: varuint16(FEE_DEPLOY_LICENSE), wid: 0, flag: 1}(pubkey, _code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, false);
    }
 
    function deployLicenseOwner(uint256 pubkey, bool isPrivileged) public onlyOwner accept  {
        ensureBalance();
        require(_licenseLeft >= 1, ERR_TOO_LOW_LICENSES);
        _licenseLeft -= 1;
        TvmCell data = BlockKeeperLib.composeLicenseStateInit(_code[m_LicenseCode], _license_number, address(this));
        _license_number += 1;
        new LicenseContract {stateInit: data, value: varuint16(FEE_DEPLOY_LICENSE), wid: 0, flag: 1}(pubkey, _code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, isPrivileged);
    }

    function deployLicenseBM(uint256 pubkey) public accept {
        ensureBalance();
        require(block.timestamp > _timeUnlock, ERR_NOT_READY);
        TvmCell data = BlockKeeperLib.composeLicenseBMStateInit(_code[m_LicenseBMCode], _license_number_bm, address(this));
        _license_number_bm += 1;
        new LicenseBMContract {stateInit: data, value: varuint16(FEE_DEPLOY_LICENSE_BM), wid: 0, flag: 1}(pubkey, _code[m_AckiNackiBlockManagerNodeWalletCode], _rootBM);
    }
 
    function deployLicenseBMOwner(uint256 pubkey) public onlyOwner accept  {
        ensureBalance();
        require(_licenseBMLeft >= 1, ERR_TOO_LOW_LICENSES);
        _licenseBMLeft -= 1;
        TvmCell data = BlockKeeperLib.composeLicenseBMStateInit(_code[m_LicenseBMCode], _license_number_bm, address(this));
        _license_number_bm += 1;
        new LicenseBMContract {stateInit: data, value: varuint16(FEE_DEPLOY_LICENSE_BM), wid: 0, flag: 1}(pubkey, _code[m_AckiNackiBlockManagerNodeWalletCode], _rootBM);
    }

    function setNewCode(uint8 id, TvmCell code) public senderIs(address(this)) accept  { 
        ensureBalance();
        _code[id] = code;
    }
    
    //Fallback/Receive
    receive() external {
    }


    //Getters
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
