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

contract EccConfig is Modifiers {
    string constant version = "1.0.0";

    mapping(uint32 => EccData) _data;
    address _owner;

    constructor (
    ) {
        _owner = address(this);
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
     * @dev Sets a new token and mints the corresponding amount.
     * @param token The EccToken structure containing token details.
     * @param to Optional recipient address for token transfer.
    */
    function setNewToken(EccToken token, optional(address) to) public internalMsg senderIs(_owner) {
        ensureBalance();
        require(_data.exists(token.key) == false, ERR_WRONG_KEY);
        if (token.baseMinted != 0) {
            require(to.hasValue(), ERR_NO_DATA);
        }
        EccData data;
        data.data = token;
        data.time = block.timestamp;
        _data[token.key] = data;
        gosh.mintecc(token.baseMinted, token.key);
        if (to.hasValue()) {
            mapping(uint32 => varuint32) data_cur;
            data_cur[token.key] = varuint32(token.baseMinted);
            to.get().transfer({value: 0.1 vmshell, flag: 1, currencies: data_cur});
        }
    }

    function getDetails() external view returns(mapping(uint32 => EccData) data) {
        return _data;
    }          
}
