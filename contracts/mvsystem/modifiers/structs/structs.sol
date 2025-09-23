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

struct PopitMedia {
        string media;
        uint256 id;
        optional(uint32) protopopit;
}

struct Popit {
        uint128 rewards;
        uint64 value;
        uint128 leftTaps;
        uint128 leftRewards;
        uint64[] MBNLst;
        uint64[] TAPLst;
        uint64[] BCLst;
}

struct PopitCandidateWithMedia {
        uint64 value;
        string media;
        optional(uint32) protopopit;
        uint32 time;
        uint64[] MBNLst;
        uint64[] TAPLst;
        uint64[] BCLst;
}