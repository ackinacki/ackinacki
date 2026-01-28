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
import "./TokenWallet.sol";

/// @title Transaction Contract
/// @notice Handles atomic token operations with time-bound execution
/// @dev Manages transfer, burn, and destroy transactions with automatic expiration
contract Transaction is Modifiers {
    string constant version = "1.0.0";

    address _wallet;
    uint256 static _dataHash;
    uint8 _transactionType;
    TvmCell _data;
    uint64 _seqnoDestroy;
    address _ownerAddress;

    /// @notice Transaction constructor
    /// @dev Initializes transaction with validation and expiration settings
    /// @param transactionType Type of transaction (TRANSFER_TYPE, BURN_TYPE, DESTROY_TYPE)
    /// @param data Encoded transaction parameters
    /// @param owner Address of the wallet owner initiating the transaction
    constructor(
        uint8 transactionType,
        TvmCell data,
        address owner
    ) accept
    {
        ensureBalance();
        TvmCell saltdata = abi.codeSalt(tvm.code()).get();
        (string lib, address wallet) = abi.decode(saltdata, (string, address));
        _wallet = wallet;
        require(msg.sender == _wallet, ERR_WRONG_SENDER);
        require(version == lib, ERR_WRONG_SENDER);
        require(tvm.hash(data) == _dataHash, ERR_WRONG_HASH); 
        _seqnoDestroy = block.seqno + TRANSACTION_LIFETIME; 
        _ownerAddress = owner;
        _transactionType = transactionType;
        _data = data;
    }

    /// @notice Manually destroys expired transaction contract
    /// @dev Can only be called after transaction lifetime has expired
    /// @dev Self-destructs and returns remaining balance to wallet
    function touch() public accept {
        require(block.seqno > _seqnoDestroy, ERR_NOT_READY_FOR_DESTROY);
        ensureBalance();
        selfdestruct(_wallet);        
    }

    /// @notice Ensures contract maintains minimum balance
    /// @dev Automatically mints shell if balance falls below threshold
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Fallback/receive function for processing transactions
    /// @dev Executes transaction when called by authorized owner
    /// @dev Forwards transaction to wallet for actual processing
    receive() external {
        ensureBalance();
        if (msg.sender == _ownerAddress) {
            TokenWallet(_wallet).acceptTransaction{value: 0.1 vmshell, flag: 161}(_transactionType, _data);
        }
    }

    /// @notice Returns transaction details and current state
    /// @return wallet Address of the originating wallet
    /// @return dataHash Hash of the transaction data
    /// @return transactionType Type of transaction
    /// @return data Encoded transaction parameters
    /// @return seqnoDestroy Block sequence number when transaction expires
    /// @return ownerAddress Address authorized to execute this transaction
    function getDetails() external view returns(
        address wallet,
        uint256 dataHash,
        uint8 transactionType,
        TvmCell data,
        uint64 seqnoDestroy,
        address ownerAddress    
    ) {
        return (_wallet, _dataHash, _transactionType, _data, _seqnoDestroy, _ownerAddress);
    } 

    /// @notice Parses transfer transaction data
    /// @return destinationOwner Recipient wallet owner address
    /// @return value Amount of tokens to transfer
    function getTransferData() external view returns(
        address destinationOwner,
        uint128 value
    ) {
        require(_transactionType == TRANSFER_TYPE, ERR_WRONG_TRANSACTION_TYPE);
        return abi.decode(_data, (address, uint128));
    }

    /// @notice Parses burn transaction data
    /// @return value Amount of tokens to burn
    function getBurnData() external view returns(
        uint128 value
    ) {
        require(_transactionType == BURN_TYPE, ERR_WRONG_TRANSACTION_TYPE);
        return abi.decode(_data, (uint128));
    }

    /// @notice Parses bridge withdrawal transaction data
    /// @return value Amount of tokens to withdraw
    /// @return to External chain address for withdrawal
    function getWithdrawData() external view returns(
        uint128 value,
        uint256 to
    ) {
        require(_transactionType == WITHDRAW_TYPE, ERR_WRONG_TRANSACTION_TYPE);
        return abi.decode(_data, (uint128, uint256));
    }

    /// @notice Parses set subscriber transaction data
    /// @dev Extracts subscriber address from transaction data
    /// @return subscriber Optional address of the subscriber contract to be set
    function getSetSubscriberData() external view returns(
        optional(address) subscriber
    ) {
        require(_transactionType == SET_SUBSCRIBER_TYPE, ERR_WRONG_TRANSACTION_TYPE);
        return abi.decode(_data, (optional(address)));
    }

    /// @notice Returns contract version information
    /// @return version String Contract version number
    /// @return name String Contract name identifier
    function getVersion() external pure returns(string, string) {
        return (version, "Transaction");
    }        
}