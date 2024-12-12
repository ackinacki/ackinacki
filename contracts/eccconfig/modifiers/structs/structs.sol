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