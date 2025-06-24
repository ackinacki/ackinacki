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
import "./libraries/DappLib.sol";

/*
 * @title DappConfig
 * @dev The configuration contract for an individual DApp.
 *      Stores the credit configuration and manages updates.
 * @version 1.0.0
 */
contract DappConfig is Modifiers {
    string constant version = "1.0.0";

    CreditConfig _data;
    address _owner;
    address _voter;
    uint256 _dapp_id;

    /**
     * @dev Initializes the DappConfig contract with a unique DApp ID and its credit configuration.
     * @param data The initial credit configuration for the DApp, including balance and limits.
     *
     * Requirements:
     * - The sender of the deployment transaction must be the DappRoot address.
     */
    constructor (
        CreditConfig data
    ) {
        TvmCell salt = abi.codeSalt(tvm.code()).get();
        (uint256 dapp_id_salt) = abi.decode(salt, (uint256));
        _owner = address(0x9999999999999999999999999999999999999999999999999999999999999999);
        _dapp_id = dapp_id_salt;
        _voter = address.makeAddrStd(0, 0);
        require(msg.sender == _owner, ERR_INVALID_SENDER);
        _data = data;
    }

    /**
     * @dev Updates the configuration by deducting minted tokens from the available balance.
     * @param minted The amount of tokens to deduct.
     *
     * Requirements:
     * - Callable only by the contract itself. Message created by BlockProducer.
     */
    function setNewConfig(
        uint128 minted
    ) public internalMsg senderIs(address(this)) functionID(5) {
        _data.available_balance -= int128(minted);
    }

    /**
     * @dev Handles incoming transfers to the contract. Updates the available balance
     *      with the amount of `ECC_SHELL` currency received.
     */
    receive() external {
        tvm.accept();
        _data.available_balance += int128(msg.currencies[CURRENCIES_ID_SHELL]);
    }

    function getDetails() external view returns(uint256 dapp_id, CreditConfig data) {
        return (_dapp_id, _data);
    }          
}
