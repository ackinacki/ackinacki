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
    bool _is_close_owner = false;  

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
    function setNewCode(uint8 id, TvmCell code) public senderIs(address(this)) accept { 
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

    function deployNewConfigCustom(
        optional(address) authorityAddress
    ) public view internalMsg accept {
        ensureBalance();
        require(msg.currencies[CURRENCIES_ID_SHELL] >= MIN_BALANCE_DEPLOY, ERR_LOW_BALANCE);
        gosh.burnecc(0, CURRENCIES_ID_SHELL);
        CreditConfig info = CreditConfig(false, int128(msg.currencies[CURRENCIES_ID_SHELL]));
        deployConfig(msg.sender.value, info, authorityAddress);
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
        int128 available_balance,
        optional(address) authorityAddress
    ) public view onlyOwnerPubkey(tvm.pubkey()) accept {
        require(_is_close_owner == false, ERR_NOT_READY);
        ensureBalance();
        require(available_balance >= 0, ERR_WRONG_DATA);
        CreditConfig info = CreditConfig(is_unlimit, available_balance);
        deployConfig(dapp_id, info, authorityAddress);
    }

    function deployNewConfigNode(
        uint256 dapp_id,
        bool is_unlimit,
        int128 available_balance,
        optional(address) authorityAddress
    ) public view senderIs(address(this)) accept {
        ensureBalance();
        CreditConfig info = CreditConfig(is_unlimit, available_balance);
        deployConfig(dapp_id, info, authorityAddress);
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
        CreditConfig info,
        optional(address) authorityAddress
    ) private view {
        require(_codeStorage.exists(m_ConfigCode), ERR_NO_DATA);
        TvmCell data = DappLib.composeDappConfigStateInit(_codeStorage[m_ConfigCode], dapp_id);
        new DappConfig {stateInit: data, value: varuint16(FEE_DEPLOY_CONFIG), wid: 0, flag: 1}(dapp_id, info, authorityAddress);
    }

    function closeRoot() public onlyOwnerPubkey(tvm.pubkey()) accept {
        _is_close_owner = true;
        ensureBalance();
    }

    /**
     * @dev Retrieves the address of the configuration for the given DApp ID.
     * @param dapp_id The unique identifier for the DApp.
     * @return config The address of the configuration contract.
     */
    function getConfigAddr(uint256 dapp_id) external view returns(address config) {
        return DappLib.calculateDappConfigAddress(_codeStorage[m_ConfigCode], dapp_id);
    }            

    function getDappConfigCode(uint256 dapp_id) external view returns(TvmCell code) {
        return DappLib.buildDappConfigCode(_codeStorage[m_ConfigCode], dapp_id);
    }            
}
