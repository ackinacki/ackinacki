/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

/// @title Wallet State Receiver Interface
/// @notice Standard interface for contracts that can receive wallet state data
/// @dev Contracts implementing this interface can handle wallet state callbacks
interface IReceiver {
    /// @notice Callback function for receiving wallet state data
    /// @dev Called by token wallets when sendData() is invoked
    /// @param root Address of the root token contract
    /// @param owner Address of the wallet owner
    /// @param balance Current token balance of the wallet
    /// @param subscriber Optional address of the subscriber contract (zero if none)
    function getWalletState(
        address root,
        address owner,
        uint128 balance,
        optional(address) subscriber
    ) external;
}