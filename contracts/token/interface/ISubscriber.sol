/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

/// @title Token Transfer Subscriber Interface
/// @notice Standard interface for contracts that want to receive transfer notifications from token wallets
/// @dev Contracts implementing this interface can be registered as subscribers to receive callbacks
interface ISubscriber {
    /// @notice Callback function invoked when a token wallet receives a transfer
    /// @dev Called automatically by token wallet after successful token transfer processing
    /// @dev Implementations should use tvm.accept() to handle incoming messages
    /// @param from Address of the sender wallet owner
    /// @param to Address of the recipient wallet contract (the one that received tokens)
    /// @param value Amount of tokens transferred
    /// @param balance New balance of the recipient wallet after transfer
    /// @notice This function is called with minimal gas (0.1 vmshell), so keep logic lightweight
    function onTransferReceived(
        address from,
        address to, 
        uint128 value,
        uint128 balance
    ) external;
}