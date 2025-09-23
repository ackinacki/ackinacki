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
    uint256 _dapp_id;

    /**
     * @dev Initializes the DappConfig contract with a unique DApp ID and its credit configuration.
     * @param data The initial credit configuration for the DApp, including balance and limits.
     *
     * Requirements:
     * - The sender of the deployment transaction must be the DappRoot address.
     */
    constructor (
        uint256 dapp_id,
        CreditConfig data
    ) {
        dapp_id;
        TvmCell salt = abi.codeSalt(tvm.code()).get();
        (uint256 dapp_id_salt) = abi.decode(salt, (uint256));
        _owner = address(0x9999999999999999999999999999999999999999999999999999999999999999);
        _dapp_id = dapp_id_salt;
        require(msg.sender == _owner, ERR_INVALID_SENDER);
        _data = data;
    }

    /**
     * @dev Updates the configuration by deducting minted tokens from the available balance.
     * @param value The amount of tokens to deduct.
     *
     * Requirements:
     * - Callable only by the contract itself. Message created by BlockProducer.
     */
    function setNewConfig(
        int128 value
    ) public internalMsg senderIs(address(this)) functionID(5) {
        _data.available_balance -= value;
    }

    /**
     * @dev Handles incoming transfers to the contract. Updates the available balance
     *      with the amount of `ECC_SHELL` currency received.
     */
    receive() external {
        tvm.accept();
        _data.available_balance += int128(msg.currencies[CURRENCIES_ID_SHELL]);
        uint128 balance_vmshell = uint128(address(this).balance);
        uint128 value = uint128(address(this).currencies[CURRENCIES_ID_SHELL]);
        uint128 converted = 0;
        if (balance_vmshell < MIN_BALANCE) {
            uint128 for_convert = MIN_BALANCE - balance_vmshell;
            converted = math.min(value, for_convert);
            gosh.cnvrtshellq(uint64(converted));
        }
        value -= converted;
        if (value > BALANCE_ECC) {
            value -= BALANCE_ECC;
            if (value > type(uint64).max) {
                value = type(uint64).max;
            }
            gosh.burnecc(uint64(value), CURRENCIES_ID_SHELL);
        }
    }

    function getDetails() external view returns(uint256 dapp_id, CreditConfig data) {
        return (_dapp_id, _data);
    }          
}
