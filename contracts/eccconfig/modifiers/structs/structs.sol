/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

//Structs
struct MessageInfo {
        uint256 messageHash;
        uint32 expireAt;
}

struct EccToken {
        uint32 key;
        string name;
        uint64 decimals;
        uint64 baseMinted;
        string description;
}

struct EccData {
        EccToken data;
        uint32 time;
}