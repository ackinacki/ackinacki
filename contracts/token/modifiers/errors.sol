/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

abstract contract Errors {
    string constant versionErrors = "1.0.0";
    
    uint16 constant CONTRACT_NOT_DEPLOYABLE = 100;
    uint16 constant ERR_NOT_OWNER = 101;
    uint16 constant ERR_WRONG_HASH = 102;
    uint16 constant ERR_WRONG_SENDER = 103;
    uint16 constant ERR_WRONG_HASH_TRANSACTION = 104;
    uint16 constant ERR_MINT_DISABLED = 105;
    uint16 constant ERR_INVALID_SENDER = 106;
    uint16 constant ERR_LOW_BALANCE = 107;
    uint16 constant ERR_NOT_READY_FOR_DESTROY = 108;
    uint16 constant ERR_WRONG_TRANSACTION_TYPE = 109;
    uint16 constant ERR_TOO_BIG_DECIMALS = 110;
}
