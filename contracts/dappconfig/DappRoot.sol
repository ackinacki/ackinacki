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
import "DappConfig.sol";

/**
 * @title DappRoot
 * @dev The main contract for managing DApp configurations.
 *      It provides functionality for deploying new configurations
 *      and managing their code.
 */
contract DappRoot is Modifiers {
    string constant version = "1.0.0";
    
    mapping(uint8 => TvmCell) _codeStorage;    

    /**
     * @dev Initializes the contract by setting the owner to a default address
     *      and minting an initial balance for the contract.
     */
    constructor (
    ) {
    }

    /**
     * @dev Sets a new code for a specific configuration ID.
     * @param id The unique identifier of the configuration.
     * @param code The TVM cell containing the code to be set.
     * 
     * Requirements:
     * - Only callable by the contract owner.
     */
    function setNewCode(uint8 id, TvmCell code) public onlyOwnerPubkey(tvm.pubkey()) accept { 
        ensureBalance();
        _codeStorage[id] = code;
    }

    /**
     * @dev Ensures the contract has sufficient balance.
     *      If the balance is less than the required minimum, mints additional funds.
     */
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    /**
     * @dev Deploys a new DApp configuration with default parameters.
     * @param dapp_id The unique identifier for the DApp.
     *
     * Requirements:
     * - `dapp_id` must not conflict with an existing configuration.
     */
    function deployNewConfigCustom(
        uint256 dapp_id
    ) public view accept {
        ensureBalance();
        CreditConfig info = CreditConfig(false, 100 vmshell);
        deployConfig(dapp_id, info);
    }

    /**
     * @dev Deploys a new DApp configuration with custom parameters.
     * @param dapp_id The unique identifier for the DApp.
     * @param is_unlimit Indicates whether the DApp has unlimited balance access.
     * @param available_balance The initial available balance for the DApp configuration.
     *
     * Requirements:
     * - Only callable by the contract owner.
     * - `available_balance` must not be negative.
     */
    function deployNewConfig(
        uint256 dapp_id,
        bool is_unlimit,
        int128 available_balance
    ) public view onlyOwnerPubkey(tvm.pubkey()) accept {
        ensureBalance();
        require(available_balance >= 0, ERR_WRONG_DATA);
        CreditConfig info = CreditConfig(is_unlimit, available_balance);
        deployConfig(dapp_id, info);
    }

    /**
     * @dev A private helper function to deploy a new DApp configuration.
     *      It initializes the configuration contract with the provided ID and credit settings.
     * @param dapp_id The unique identifier for the DApp.
     * @param info The credit configuration for the DApp, containing balance and unlimited access flag.
     *
     * Requirements:
     * - The `_code[m_ConfigCode]` mapping must contain valid bytecode for the configuration contract.
     */
    function deployConfig(
        uint256 dapp_id,
        CreditConfig info
    ) private view {
        require(_codeStorage.exists(m_ConfigCode), ERR_NO_DATA);
        TvmCell data = DappLib.composeDappConfigStateInit(_codeStorage[m_ConfigCode], dapp_id);
        new DappConfig {stateInit: data, value: varuint16(FEE_DEPLOY_CONFIG), wid: 0, flag: 1}(dapp_id, info);
    }

    /**
     * @dev Retrieves the address of the configuration for the given DApp ID.
     * @param dapp_id The unique identifier for the DApp.
     * @return config The address of the configuration contract.
     */
    function getConfigAddr(uint256 dapp_id) external view returns(address config) {
        return DappLib.calculateDappConfigAddress(_codeStorage[m_ConfigCode], dapp_id);
    }            
}
