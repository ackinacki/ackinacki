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
        uint64 seqNoFinish;       
        bytes bls_key;
        uint8 status;
        uint16 signerIndex;
}
struct LicenseData {
        uint128 reputationTime;
        uint8 status;
        bool isPriority;
        optional(address) stakeController;
        uint32 last_touch;
        uint128 balance;
        uint128 lockStake;
        uint128 lockContinue;
        uint128 lockCooler;
        bool isLockToStake;
}

struct LicenseStake {
        uint256 num;
        uint128 stake;
}

struct LockStake {
        uint256 value;
        uint32 timeStampFinish;
}