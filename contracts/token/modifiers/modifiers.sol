/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

import "./errors.sol";

/// @title Modifiers and Constants Contract
/// @notice Provides common modifiers, constants and configuration for token contracts
/// @dev Abstract contract containing reusable modifiers and system constants
abstract contract Modifiers is Errors {       
    /// @notice Minimum balance required for contract operation
    /// @dev Contracts will auto-mint shell if balance falls below this threshold
    uint64 constant MIN_BALANCE = 100 vmshell;
    
    /// @notice Hash of the compiled TokenWallet contract code
    /// @dev Used for validating wallet code integrity during deployment
    uint256 constant WALLET_HASH = 0x0acc1d213d29bc7bf36a84007801ffc20bc1926994a833e3a5c9cf149adc5157;
    
    /// @notice Hash of the compiled Transaction contract code
    /// @dev Used for validating transaction code integrity during deployment
    uint256 constant TRANSACTION_HASH = 0xd3f851daef97c15c38278ba5a0d16f941b35de6809ead8ffbcbac4eefa9e6e61;

    /// @notice Transaction type constant for token transfers
    /// @dev Used in transaction processing to identify transfer operations
    uint8 constant TRANSFER_TYPE = 1;
    
    /// @notice Transaction type constant for token burning
    /// @dev Used in transaction processing to identify burn operations
    uint8 constant BURN_TYPE = 2;
    
    /// @notice Transaction type constant for wallet destruction
    /// @dev Used in transaction processing to identify destroy operations
    uint8 constant DESTROY_TYPE = 3;

    /// @notice Transaction type constant for cross-chain withdrawal operations
    /// @dev Used in transaction processing to identify bridge withdrawal operations
    uint8 constant WITHDRAW_TYPE = 4;

    /// @notice Transaction type constant for subscriber management
    /// @dev Used in transaction processing to identify set subscriber operations
    uint8 constant SET_SUBSCRIBER_TYPE = 5;

    /// @notice Lifetime of transaction contracts in blocks
    /// @dev Transactions auto-expire after this many blocks for cleanup
    uint64 constant TRANSACTION_LIFETIME = 2000;

    uint16 constant DECIMALS_MAX = 18;

    /// @notice Modifier for owner authorization using public key
    /// @dev Verifies message sender's public key matches required owner key
    /// @param rootpubkey The expected public key of the owner
    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }

    /// @notice Modifier for accepting incoming messages
    /// @dev Automatically accepts incoming messages to process transactions
    modifier accept() {
        tvm.accept();
        _;
    }

    /// @notice Modifier for sender address validation
    /// @dev Ensures the message sender matches the expected address
    /// @param sender The expected sender address
    modifier senderIs(address sender) {
        require(msg.sender == sender, ERR_INVALID_SENDER);
        _;
    }
}