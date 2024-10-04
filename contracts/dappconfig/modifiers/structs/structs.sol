// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;

//Structs
struct MessageInfo {
        uint256 messageHash;
        uint32 expireAt;
}

struct CreditConfig {
	bool is_unlimit;
    int128 available_credit;
	uint128 credit_per_block;
	uint128 available_credit_max_value;
    uint128 start_block_seqno;
	uint128 end_block_seqno;
	uint128 last_updated_seqno;
	uint128 available_personal_limit;
}