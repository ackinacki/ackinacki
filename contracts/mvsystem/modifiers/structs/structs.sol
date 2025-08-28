// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
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
        uint128 value;
        uint128 leftValue;
}

struct PopitCandidateWithMedia {
        uint128 value;
        string media;
        optional(uint32) protopopit;
        uint32 time;
}