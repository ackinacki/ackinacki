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
import "./LicenseRoot.sol";
import "./AckiNackiBlockManagerNodeWallet.sol";

contract LicenseBMContract is Modifiers {
    string constant version = "1.0.0";

    uint256 static _license_number;
    address static _root;
    optional(uint256) _owner_pubkey;
    optional(address) _owner_address;
    address _rootBM;
    optional(address) _bmwallet;
    uint128 _rewarded = 0;
    uint128 _slashSum;
    uint64 _licenseLastTouch;

    mapping(uint8 => TvmCell) _code;

    constructor (
        uint256 pubkey,
        TvmCell walletCode,
        address rootBM
    ) senderIs(_root) accept {
        _owner_pubkey = pubkey;
        _code[m_AckiNackiBlockManagerNodeWalletCode] = walletCode;
        _rootBM = rootBM;
        _licenseLastTouch = 0;
    }

    function setOwnerAddress(address owner) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        ensureBalance();
        _owner_address = owner;
        _owner_pubkey = null;
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_LICENSE) { return; }
        gosh.mintshellq(FEE_DEPLOY_LICENSE);
    }

    function setOwnerPubkey(uint256 pubkey) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        ensureBalance();
        _owner_pubkey = pubkey;
        _owner_address = null;
    }

    function removeBMWallet() public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        require(_bmwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        ensureBalance();
        AckiNackiBlockManagerNodeWallet(_bmwallet.get()).removeLicense{value: 0.1 vmshell, flag: 1}(_license_number);
    }

    function deleteWallet(uint128 reward, uint128 slashSum) public accept {
        require(_bmwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(msg.sender == _bmwallet.get(), ERR_INVALID_SENDER);
        _licenseLastTouch = block.seqno;
        ensureBalance();
        _rewarded = reward;
        _bmwallet = null;
        _slashSum = slashSum;
    }

    function addBMWallet(uint256 pubkey) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        require(_bmwallet.hasValue() == false, ERR_WALLET_EXIST);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        ensureBalance();
        AckiNackiBlockManagerNodeWallet(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], _rootBM, pubkey)).addLicense{value: 0.1 vmshell, flag: 1}(_license_number, _rewarded, _slashSum);
    }

    function withdrawToken(address to, uint128 value) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        ensureBalance();
        require(!_bmwallet.hasValue(), ERR_WALLET_EXIST);
        require(uint128(address(this).currencies[CURRENCIES_ID]) >= value, ERR_TOO_LOW_BALANCE);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        to.transfer({value: 0.1 vmshell, currencies: data, flag: 1});    
    }

    function notAcceptLicense(uint256 pubkey) public view senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], _rootBM, pubkey)) accept {
        ensureBalance();
    }

    function acceptLicense(uint256 pubkey) public senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], _rootBM, pubkey)) accept {
        ensureBalance();
        _bmwallet = msg.sender;
    }

    function toWithdrawToken(address to, uint128 value) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        ensureBalance();
        require(_bmwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        AckiNackiBlockManagerNodeWallet(_bmwallet.get()).withdrawToken{value: 0.1 vmshell, flag: 1}(to, value);
    }

    //Fallback/Receive
    receive() external {
    }


    //Getters
    function getDetails() external view returns (uint256 license_number, optional(address) bmwallet, optional(uint256) owner_pubkey, optional(address) owner_address) {
        return (_license_number, _bmwallet, _owner_pubkey, _owner_address);
    }
 
    function getBM() external view returns (optional(address) bmwallet) {
        return _bmwallet;
    }

    function getOwner() external view returns (optional(uint256) owner_pubkey, optional(address) owner_address) {
        return (_owner_pubkey, _owner_address);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "LicenseBlockManager");
    }   
}
