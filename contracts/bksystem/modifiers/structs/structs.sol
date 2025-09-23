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
        bool isPrivileged;
        optional(address) stakeController;
        uint64 last_touch;
        uint128 balance;
        uint128 lockStake;
        uint128 lockContinue;
        uint128 lockCooler;
        bool isLockToStake;
        uint32 coolerCount;
        bool isLockToStakeByWallet;
        bool isLockBecauseOfSlashing;
}

struct LicenseStake {
        uint256 num;
        uint128 stake;
}

struct LockStake {
        uint256 value;
        uint32 timeStampFinish;
}