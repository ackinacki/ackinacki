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

    mapping(uint8 => TvmCell) _code;
    uint32 _license_start;
    bool is_ready = false;
    uint128 _lock_seqno;

    /**
     * @dev Initializes the License contract with the public key of the owner, wallet code, and the root election address.
     * @param pubkey The public key of the license owner.
     * @param walletCode The code of the wallet to be used in the contract.
     * @param rootElection The address of the root election entity.
     *
     * Requirements:
     * - Only callable by the `_root` address.
     */
    constructor (
        uint256 pubkey,
        TvmCell walletCode,
        address rootElection
    ) senderIs(_root) {
        ensureBalance();
        _owner_pubkey = pubkey;
        _code[m_AckiNackiBlockKeeperNodeWalletCode] = walletCode;
        _rootElection = rootElection;
    }

    /**
     * @dev Allows the owner to update their owner address.
     * @param owner The new address for the owner. 
     *
     * Requirements:
     * - Only callable by the current owner (verified by '_owner_address' or `_owner_pubkey`).
     */
    function setOwnerAddress(address owner) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept saveMsg {
        _owner_address = owner;
        _owner_pubkey = null;
    }

    /**
     * @dev Ensures the contract has enough balance for operations.
     * If the balance is insufficient, mints additional funds.
     */
    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_LICENSE) { return; }
        gosh.mintshell(FEE_DEPLOY_LICENSE);
    }

    /**
     * @dev Allows the owner to update their public key.
     * @param pubkey The new public key for the owner.
     *
     * Requirements:
     * - Only callable by the current owner (verified by '_owner_address' or `_owner_pubkey`).
     */
    function setOwnerPubkey(uint256 pubkey) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept saveMsg {
        _owner_pubkey = pubkey;
        _owner_address = null;
    }

    /**
     * @dev Sets whether the license wallet can be locked to stake.
     * @param lock Boolean value indicating whether the wallet should be locked.
     *
     * Requirements:
     * - The wallet must exist before setting the lock.
     */
    function setLockToStake(bool lock) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept saveMsg {
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        ensureBalance();
        AckiNackiBlockKeeperNodeWallet(_bkwallet.get()).setLockToStake{value: 0.1 vmshell, flag: 1}(_license_number, lock);
    }

    /**
     * @dev Removes the connecting with the BlockKeeper wallet for this license.
     * @param to The address to which the wallet will transfer free rewards.
     *
     * Requirements:
     * - The license must be connected to wallet before it can be removed.
     */
    function removeBKWallet(address to) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept saveMsg {
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        ensureBalance();
        AckiNackiBlockKeeperNodeWallet(_bkwallet.get()).removeLicense{value: 0.1 vmshell, flag: 1}(_license_number, to);
    }

    /**
     * @dev Delete associated wallet from license. 
     * Sets the actual reputation time.
     * @param reputationTime The time to set for the reputation after deletion.
     *
     * Requirements:
     * - Only callable by the BlockKeeper wallet.
     */
    function deleteLicense(uint128 reputationTime) public senderIs(_bkwallet.get()) accept {
        require(_bkwallet.hasValue(), ERR_WALLET_NOT_EXIST);
        ensureBalance();
        _bkwallet = null;
        _reputationTime = reputationTime;
    }

    /**
     * @dev Adds a BlockKeeper wallet connect for this license.
     * @param pubkey The public key of the BlockKeeper wallet to add.
     *
     * Requirements:
     * - A license should not already connect.
     * - Need to wait 10 blocks after the last operation.
     */
    function addBKWallet(uint256 pubkey) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept saveMsg {
        require(_bkwallet.hasValue() == false, ERR_WALLET_EXIST);
        require(_lock_seqno + 10 < block.seqno, ERR_LICENSE_BUSY);
        ensureBalance();
        _lock_seqno = block.seqno;
        if (is_ready == false) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, pubkey)).addLicense{value: 0.1 vmshell, flag: 1}(_license_number, _reputationTime, block.timestamp);
        } else {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, pubkey)).addLicense{value: 0.1 vmshell, flag: 1}(_license_number, _reputationTime, _license_start);
        }
    }

    /**
     * @dev Get message that the license as not accepted for the BlockKeeperWallet with given public key.
     * @param pubkey The public key of the BlockKeeper wallet.
     *
     * Requirements:
     * - Only callable by the associated BlockKeeper wallet.
     */
    function notAcceptLicense(uint256 pubkey) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, pubkey)) accept {
        ensureBalance();
    }

    /**
     * @dev Accepts a license for a BlockKeeperWallet with given public key and starts the license from a specified time if it first time.
     * @param pubkey The public key of the BlockKeeper wallet.
     * @param license_start The actual time of the license connect.
     *
     * Requirements:
     * - Only callable by the associated BlockKeeper wallet.
     */
    function acceptLicense(uint256 pubkey, uint32 license_start) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _rootElection, pubkey)) accept {
        ensureBalance();
        if (is_ready == false) {
            is_ready = true;
            _license_start = license_start;
        }
        _bkwallet = msg.sender;
    }

    /**
     * @dev Allows the owner to withdraw tokens from the BlockKeeper wallet.
     * @param to The address to which the tokens will be sent.
     * @param value The amount of tokens to withdraw.
     *
     * Requirements:
     * - Only callable by the current owner.
     */
    function toWithdrawToken(address to, uint128 value) public onlyOwnerWalletOpt(_owner_address, _owner_pubkey) accept saveMsg {
        AckiNackiBlockKeeperNodeWallet(_bkwallet.get()).withdrawToken{value: 0.1 vmshell, flag: 1}(_license_number, to, value);
    }

    //Fallback/Receive
    receive() external {
    }


    //Getters
    function getDetails() external view returns (uint256 license_number, optional(address) bkwallet, optional(uint256) owner_pubkey, optional(address) owner_address, uint128 reputationTime) {
        return (_license_number, _bkwallet, _owner_pubkey, _owner_address, _reputationTime);
    }
    
    /*
     * @dev Retrieves the BlockKeeper wallet address.
     * @return The address of the BlockKeeper wallet if available.
     */
    function getBK() external view returns (optional(address) bkwallet) {
        return _bkwallet;
    }

    function getOwner() external view returns (optional(uint256) owner_pubkey, optional(address) owner_address) {
        return (_owner_pubkey, _owner_address);
    }

    /**
     * @dev Retrieves the contract version and its type.
     * @return version The version of the contract.
     * @return type The type of the contract.
     */
    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperContractRoot");
    }   
}
