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

struct Stake {
        uint256 stake;
        uint64 seqNoStart;
        uint32 timeStampFinish;
        uint32 timeStampFinishCooler;
        uint8 status;
}

struct LockStake {
        uint256 value;
        uint32 timeStampFinish;
}