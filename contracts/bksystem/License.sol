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
import "./AckiNackiBlockKeeperNodeWallet.sol";

contract LicenseContract is Modifiers {
    string constant version = "1.0.0";

    uint256 static _license_number;
    address static _root;
    optional(uint256) _owner_pubkey;
    optional(address) _owner_address;
    address _rootElection;

    optional(address) _bkwallet;
    uint128 _reputationTime;
    bool _isPrivileged = true;

    mapping(uint8 => TvmCell) _code;
    uint64 _license_start;
    bool is_ready = false;
    uint64 _last_touch;
    uint64 _licenseLastTouch;

    constructor (
        uint256 pubkey,
        TvmCell walletCode,
        address rootElection,
        bool isPrivileged
    ) senderIs(_root) accept {
        _owner_pubkey = pubkey;
        _code[m_AckiNackiBlockKeeperNodeWalletCode] = walletCode;
        _rootElection = rootElection;
        _licenseLastTouch = 0;
        _isPrivileged = isPrivileged;
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

    function setLockToStake(bool lock) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        ensureBalance();
        AckiNackiBlockKeeperNodeWallet(_bkwallet.get()).setLockToStake{value: 0.1 vmshell, flag: 1}(_license_number, lock);
    }
    
    function removeBKWallet() public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        ensureBalance();
        AckiNackiBlockKeeperNodeWallet(_bkwallet.get()).removeLicense{value: 0.1 vmshell, flag: 1}(_license_number);
    }

    function deleteWallet(uint128 reputationTime, bool isPrivileged, uint64 last_touch) public {
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(msg.sender == _bkwallet.get(), ERR_INVALID_SENDER);
        _licenseLastTouch = block.seqno;
        tvm.accept();
        ensureBalance();
        _bkwallet = null;
        _reputationTime = reputationTime;
        _isPrivileged = isPrivileged;
        _last_touch = last_touch;
    }

    function destroyLicense() public  {
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(msg.sender == _bkwallet.get(), ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        selfdestruct(_rootElection);
    }

    function addBKWallet(uint256 pubkey) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        require(_bkwallet.hasValue() == false, ERR_WALLET_EXIST);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        ensureBalance();
        if (is_ready == false) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, pubkey)).addLicense{value: 0.1 vmshell, flag: 1}(_license_number, _reputationTime, block.seqno, _isPrivileged);
        } else {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, pubkey)).addLicense{value: 0.1 vmshell, flag: 1}(_license_number, _reputationTime, _last_touch, _isPrivileged);
        }
    }

    function notAcceptLicense(uint256 pubkey) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, pubkey)) accept {
        ensureBalance();
    }

    function acceptLicense(uint256 pubkey, uint64 last_touch) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, pubkey)) accept {
        ensureBalance();
        if (is_ready == false) {
            is_ready = true;
            _license_start = last_touch;
        }
        _last_touch = last_touch;
        _bkwallet = msg.sender;
        if (uint128(address(this).currencies[CURRENCIES_ID]) != 0) {
            mapping(uint32 => varuint32) data;
            data[CURRENCIES_ID] = address(this).currencies[CURRENCIES_ID];
            AckiNackiBlockKeeperNodeWallet(msg.sender).addBalance{value: 0.1 vmshell, flag: 1, currencies: data}(_license_number);
        }
    }

    function toWithdrawToken(uint128 value) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        ensureBalance();
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        AckiNackiBlockKeeperNodeWallet(_bkwallet.get()).withdrawToken{value: 0.1 vmshell, flag: 1}(_license_number, value);
    }

    function withdrawToken(address to, uint128 value) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        ensureBalance();
        require(!_bkwallet.hasValue(), ERR_WALLET_EXIST);
        require(uint128(address(this).currencies[CURRENCIES_ID]) >= value, ERR_TOO_LOW_BALANCE);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        _isPrivileged = false;
        to.transfer({value: 0.1 vmshell, currencies: data, flag: 1});    
    }

    function toAddBalance(uint128 value) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept {
        ensureBalance();
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        require(uint128(address(this).currencies[CURRENCIES_ID]) >= value, ERR_TOO_LOW_BALANCE);
        require(block.seqno > _licenseLastTouch + LICENSE_TOUCH, ERR_LICENSE_BUSY);
        _licenseLastTouch = block.seqno;
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        AckiNackiBlockKeeperNodeWallet(_bkwallet.get()).addBalance{value: 0.1 vmshell, currencies: data, flag: 1}(_license_number);
    }

    //Fallback/Receive
    receive() external {
    }


    //Getters
    function getDetails() external view returns (uint256 license_number, optional(address) bkwallet, optional(uint256) owner_pubkey, optional(address) owner_address, uint128 reputationTime, uint64 license_start) {
        return (_license_number, _bkwallet, _owner_pubkey, _owner_address, _reputationTime, _license_start);
    }
    
    function getBK() external view returns (optional(address) bkwallet) {
        return _bkwallet;
    }

    function getOwner() external view returns (optional(uint256) owner_pubkey, optional(address) owner_address) {
        return (_owner_pubkey, _owner_address);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "LicenseBlockKeeper");
    }   
}
