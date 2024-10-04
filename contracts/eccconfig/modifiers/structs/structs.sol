// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;

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