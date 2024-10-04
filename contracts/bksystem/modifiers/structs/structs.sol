// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;

//Structs
struct MessageInfo {
        uint256 messageHash;
        uint32 expireAt;
}

struct Stake {
        uint256 stake;
        uint64 seqNoStart;
        uint32 timeStampFinish;
        uint8 status;
}