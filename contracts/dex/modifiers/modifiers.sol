/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

import "./errors.sol";

/// @title Modifiers and Constants Contract
/// @notice Provides common modifiers, constants and configuration for PMP contracts
abstract contract Modifiers is Errors {
    /// @notice Bit length used for external event destination addresses.
    uint constant bitCntAddress = 256;
    
    // RootPN events
    /// @notice External event id for `RootPN.PrivateNoteDeployed`.
    uint128 constant ROOTPN_PRIVATE_NOTE_DEPLOYED = 101;
    /// @notice External event id for `RootPN.NullifierDeployed`.
    uint128 constant ROOTPN_NULLIFIER_DEPLOYED = 102;
    /// @notice Reserved RootPN external event id for oracle deployment notifications.
    uint128 constant ROOTPN_ORACLE_DEPLOYED = 103;
    /// @notice External event id for `RootPN.TokensWithdrawn`.
    uint128 constant ROOTPN_TOKENS_WITHDRAWN = 154;
    /// @notice External event id for `RootPN.ProtocolFeeCollected`.
    uint128 constant ROOTPN_PROTOCOL_FEE_COLLECTED = 155;
    /// @notice External event id for `RootPN.ProtocolFeeWithdrawn`.
    uint128 constant ROOTPN_PROTOCOL_FEE_WITHDRAWN = 156;

    // Oracle events
    /// @notice External event id for `Oracle.OracleEventListDeployed`.
    uint128 constant ORACLE_DEPLOYED = 104;
    /// @notice Reserved external event id for OracleEventList deployment.
    uint128 constant ORACLE_EVENT_LIST_DEPLOYED = 105;
    /// @notice External event id for `OracleEventList.EventConfirmed`.
    uint128 constant ORACLE_EVENT_CONFIRMED = 106;
    /// @notice External event id for `OracleEventList.DescriptionUpdated`.
    uint128 constant ORACLE_LIST_DESCRIPTION_UPDATED = 107;

    // PrivateNote events
    /// @notice External event id for `PrivateNote.PMPDeployed`.
    uint128 constant PRIVATENOTE_PMP_DEPLOYED = 111;
    /// @notice External event id for `PrivateNote.OwnerChanged`.
    uint128 constant PRIVATENOTE_OWNER_CHANGED = 112;
    /// @notice External event id for `PrivateNote.StakeConfirmed`.
    uint128 constant PRIVATENOTE_STAKE_CONFIRMED = 113;
    /// @notice External event id for `PrivateNote.InferenceOrderPlacedConfirmed` (owner-facing mirror of the OB placement).
    uint128 constant PRIVATENOTE_INFERENCE_PLACED = 1100;
    /// @notice External event id for `PrivateNote.InferenceFilledConfirmed` (owner-facing mirror carrying the deal TC).
    uint128 constant PRIVATENOTE_INFERENCE_FILLED = 1101;
    /// @notice External event id for `PrivateNote.ClaimAccepted`.
    uint128 constant PRIVATENOTE_CLAIM_ACCEPTED = 114;
    /// @notice External event id for `PrivateNote.StakeCancelled`.
    uint128 constant PRIVATENOTE_STAKE_CANCELLED = 115;
    /// @notice External event id for `PrivateNote.FullSetStakeConfirmed`.
    uint128 constant PRIVATENOTE_FULLSET_STAKE_CONFIRMED = 116;
    /// @notice External event id for `PrivateNote.FullSetStakeCancelled`.
    uint128 constant PRIVATENOTE_FULLSET_STAKE_CANCELLED = 117;
    // PMP events
    /// @notice External event id for `PMP.StakeAccepted`.
    uint128 constant PMP_STAKE_ACCEPTED = 118;
    /// @notice External event id for `PMP.ApprovedByOracle`.
    uint128 constant PMP_APPROVED_BY_ORACLE = 119;
    /// @notice External event id for `PMP.Resolved`.
    uint128 constant PMP_RESOLVED = 120;
    /// @notice External event id for `PMP.ClaimProcessed`.
    uint128 constant PMP_CLAIM_PROCESSED = 121;
    /// @notice Reserved external event id for network fee burn accounting.
    uint128 constant PMP_NETWORK_FEE_BURNED = 122;
    /// @notice Reserved external event id for legacy stake deadline updates.
    uint128 constant PMP_STAKE_DEADLINE_SET = 123;
    /// @notice External event id for `PMP.TimingsSet`.
    uint128 constant PMP_SET_TIMINGS = 124;
    /// @notice Reserved external event id for number-of-outcomes updates.
    uint128 constant PMP_NUM_OUTCOMES_SET = 125;
    /// @notice External event id for `PMP.EventCancelled`.
    uint128 constant PMP_EVENT_CANCELLED = 126;
    /// @notice Reserved external event id for per-oracle confirmation.
    uint128 constant PMP_ORACLE_CONFIRMED = 127;
    /// @notice Reserved external event id for full oracle confirmation.
    uint128 constant PMP_ALL_ORACLES_CONFIRMED = 128;
    /// @notice Reserved external event id for PMP initialization.
    uint128 constant PMP_INITIALIZED = 129;
    /// @notice External event id for `PMP.PMPRejected`.
    uint128 constant PMP_REJECTED_BY_ORACLE = 132;

    // OracleList events
    /// @notice External event id for `OracleEventList.EventAdded`.
    uint128 constant ORACLE_EVENT_ADDED = 133;
    /// @notice Reserved external event id for oracle event publishing.
    uint128 constant ORACLE_EVENT_PUBLISHED = 134;
    /// @notice External event id for `OracleEventList.RangeEventAdded` (PMP↔OB binding).
    uint128 constant ORACLE_RANGE_EVENT_ADDED = 162;

    // Vault events
    /// @notice External event id for `RootPN.VoucherGenerated`.
    uint128 constant VAULT_voucher_GENERATED = 135;
    // Root Oracle event
    /// @notice External event id for `RootOracle.OracleDeployed`.
    uint128 constant ROOTORACLE_ORACLE_DEPLOYED = 136;

    // Creator fee event
    /// @notice External event id for `PMP.CreatorFeeCollected`.
    uint128 constant PMP_CREATOR_FEE_COLLECTED = 137;

    // Split/Merge events
    /// @notice External event id for `PrivateNote.SplitConfirmed`.
    uint128 constant PRIVATENOTE_SPLIT_CONFIRMED = 138;
    /// @notice External event id for `PrivateNote.MergeConfirmed`.
    uint128 constant PRIVATENOTE_MERGE_CONFIRMED = 139;
    /// @notice External event id for `PMP.PoolsFrozen`.
    uint128 constant PMP_POOLS_FROZEN = 140;
    /// @notice External event id for `PMP.SplitProcessed`.
    uint128 constant PMP_SPLIT_PROCESSED = 141;
    /// @notice External event id for `PMP.MergeProcessed`.
    uint128 constant PMP_MERGE_PROCESSED = 142;

    // OrderBook events
    /// @notice External event id for `OrderBook.OrderPlaced`.
    uint128 constant OB_ORDER_PLACED = 143;
    /// @notice External event id for `OrderBook.OrderCancelled`.
    uint128 constant OB_ORDER_CANCELLED = 144;
    /// @notice External event id for `OrderBook.EpochSettled`.
    uint128 constant OB_EPOCH_SETTLED = 145;
    /// @notice External event id for `OrderBook.OrderFilled`.
    uint128 constant OB_ORDER_FILLED = 146;
    /// @notice External event ids for OrderBook events that previously shared dst=0.
    uint128 constant OB_PARTIAL_FILL = 157;
    uint128 constant OB_FULLY_FILLED = 158;
    uint128 constant OB_QUEUED = 159;
    uint128 constant OB_REJECTED = 160;
    uint128 constant OB_CALLBACK_BOUNCED = 161;
    /// @notice External event id for `PrivateNote.OrderPlaced`.
    uint128 constant PRIVATENOTE_ORDER_PLACED = 147;
    /// @notice External event id for `PrivateNote.OrderFilled`.
    uint128 constant PRIVATENOTE_ORDER_FILLED = 148;

    /// @notice External event id for `PrivateNote.OrderSubmitted`.
    uint128 constant PRIVATENOTE_ORDER_SUBMITTED = 151;
    /// @notice External event id for `PrivateNote.OrderCancelledConfirmed`.
    uint128 constant PRIVATENOTE_ORDER_CANCELLED = 152;
    /// @notice External event id for `PrivateNote.OrderPlaceRejected`.
    uint128 constant PRIVATENOTE_ORDER_REJECTED = 153;

    // Transfer events
    /// @notice External event id for `PrivateNote.TransferInitiated`.
    uint128 constant PRIVATENOTE_TRANSFER_INITIATED = 149;
    /// @notice External event id for `PrivateNote.TransferReceived`.
    uint128 constant PRIVATENOTE_TRANSFER_CONFIRMED = 150;

    /// @notice Minimum native balance required for contract operation
    uint64 constant MIN_BALANCE = 100 vmshell;

    /// @notice Minimum allowed deposit value for NACKL tokens (9 decimals)
    uint64 constant MIN_VALUE = 10_000_000; // 0.01 NACKL

    /// @notice Minimum allowed deposit value for Shell tokens (9 decimals)
    uint64 constant MIN_VALUE_SHELL = 10_000_000; // 0.01 Shell

    /// @notice Minimum allowed deposit value for USDC tokens (6 decimals)
    uint64 constant MIN_VALUE_USDC = 10_000; // 0.01 USDC

    /// @notice Minimum order NOTIONAL (total value in quote currency) per token type.
    ///         An order's value = amount * price / FULL_PERCENT must be >= this.
    ///         For market buy the full `amount` is locked as quote collateral so
    ///         `amount` itself is compared directly. Market sells skip this check
    ///         since the price is unknown until fill time.
    uint128 constant MIN_ORDER_NOTIONAL_NACKL = 10_000_000_000;  // 10 NACKL
    uint128 constant MIN_ORDER_NOTIONAL_SHELL = 100_000_000_000; // 100 Shell
    uint128 constant MIN_ORDER_NOTIONAL_USDC  = 1_000_000;       // 1 USDC

    /// @notice Lot size (amount quantisation step) per token type.
    ///         Order amount must be an integer multiple of lot size.
    ///         Uniform value of 0.01 token across all token types, scaled by decimals.
    uint128 constant LOT_SIZE_NACKL = 10_000_000; // 0.01 NACKL (9 decimals)
    uint128 constant LOT_SIZE_SHELL = 10_000_000; // 0.01 Shell (9 decimals)
    uint128 constant LOT_SIZE_USDC  = 10_000;     // 0.01 USDC  (6 decimals)

    /// @notice Tick size (price quantisation step) in basis points.
    ///         Limit order price must be an integer multiple of tick size.
    ///         Market orders (FLAG_MARKET) skip this check since price is ignored.
    ///         Common across all token types.
    uint256 constant TICK_SIZE = 10; // 10 bps = 0.1%

    /// @notice Maximum number of orders (or cancels) in a single batch.
    ///         Applies independently to the `orders` and `cancelIds` arrays of
    ///         PrivateNote.placeBatch / OrderBook.executeBatch — keeping the
    ///         limit in one place prevents PN from accepting more than OB can
    ///         dispatch (truncation on OB side would leak collateral / stake).
    uint32 constant MAX_BATCH_SIZE = 10;

    /// @notice Currency ID used for PMP pools (staking tokens)
    uint32 constant CURRENCIES_ID = 1;

    /// @notice Currency ID used for shell tokens 
    uint32 constant CURRENCIES_ID_SHELL = 2;

    /// @notice Currency ID used for shell tokens (network fees)
    uint32 constant CURRENCIES_ID_SHELL_FEE = 300;

    /// @notice Currency ID used for USDC tokens
    uint32 constant CURRENCIES_ID_USDC = 3;

    /// @notice Fixed network fee to burn on approval
    uint64 constant NETWORK_FEE_AMOUNT = 1_000_000_000; // 1 shell tokens

    /// @notice Address of RootPN contract
    address constant ROOT_PN_ADDRESS = address.makeAddrStd(0, 0x1010101010101010101010101010101010101010101010101010101010101010);

    /// @notice Address of RootOracle contract
    address constant ROOT_ORACLE_ADDRESS = address.makeAddrStd(0, 0x1515151515151515151515151515151515151515151515151515151515151515);

    /// @notice DApp identifier for the PMPRoot ("rootPN") system — RootPN, PrivateNote,
    ///         PMP, and OrderBook all share this dapp_id. Used as `dest_dapp_id` on
    ///         cross-contract message sends targeted at any of these contracts.
    uint256 constant ROOT_PN_DAPP_ID = 0x0000000000000000000000000000000000000000000000000000000000000004;

    /// @notice DApp identifier for the Oracle system — RootOracle, Oracle, and
    ///         OracleEventList all share this dapp_id. Used as `dest_dapp_id` on
    ///         cross-contract message sends targeted at any of these contracts.
    uint256 constant ORACLE_DAPP_ID = 0x0000000000000000000000000000000000000000000000000000000000000004;

    /// @notice Voting threshold for OracleUnion decisions
    uint32 constant THRESHOLD = 6600; // 66% = 6600

    /// @notice Full percentage constant
    uint128 constant FULL_PERCENT = 10000; // 100% = 10000

    /// @notice Grace period for oracle resolve (24 hours in seconds)
    uint64 constant GRACE_PERIOD = 86400;

    /// @notice Minimum lead time from now to resultStart on first setTimings call.
    uint64 constant MIN_RESULT_GAP = 120;

    /// @notice Fee percentage for staking operations
    uint128 constant FEE_PERCENT = 1; // 0.01% = 1

    /// @notice Trading fee denominator (0.001% precision)
    uint128 constant FEE_DENOMINATOR = 100000;

    /// @notice Taker fee rate. Maker pays no fee — instead receives a rebate
    ///         funded from the taker fee (see MAKER_REBATE_NUM / _DEN).
    /// @dev 45 / 100000 = 0.045%
    uint128 constant TAKER_FEE_RATE = 45;

    /// @notice Maker rebate as a fraction of the taker fee on each fill.
    /// @dev 3 / 4 = 75% → maker gets 0.03375% of notional, the contract
    ///      retains 0.01125% (= takerFee - makerRebate) as protocol revenue.
    uint128 constant MAKER_REBATE_NUM = 3;
    uint128 constant MAKER_REBATE_DEN = 4;

    /// @notice Bet type identifiers
    uint8 constant BET_TYPE_CLEAN = 0;    // Stake without debt
    uint8 constant BET_TYPE_DEBT = 1;     // Stake with debt
    uint8 constant BET_TYPE_COUPON = 2;   // Stake with free coupon
    uint8 constant BET_TYPE_OB_SELL = 3;  // OrderBook sell order (bounce restores stake.amount)
    uint8 constant BET_TYPE_MERGE = 4;   // mergeFullSet (bounce: no-op on _balance, just clear candidate)

    /// @notice Maximum coupon pool as percentage of total pool
    /// @dev 500 = 5% (of 10000 = 100%)
    uint128 constant COUPON_POOL_LIMIT_PERCENT = 500;

    /// @notice Maximum coupon payout multiplier
    /// @dev Maximum win = couponSize * COUPON_MAX_PAYOUT_MULTIPLIER
    uint128 constant COUPON_MAX_PAYOUT_MULTIPLIER = 20000;

    /// @notice Redistribution percentage from debt bets to clean bets
    /// @dev 500 = 5% (of 10000 = 100%)
    uint128 constant DEBT_REDISTRIBUTION_PERCENT = 500;

    /// @notice Base allowed nominals for vault (without decimals)
    uint128[] constant ALLOWED_NOMINALS = [
        uint128(100),
        uint128(1000),
        uint128(10000),
        uint128(100000),
        uint128(1000000)
    ];

    /// @notice Returns decimals for a given token type
    /// @param tokenType Currency identifier.
    /// @return decimals Token decimals multiplier (1e6 for USDC, 1e9 otherwise).
    function tokenDecimals(uint32 tokenType) internal pure returns (uint128) {
        if (tokenType == CURRENCIES_ID_USDC) return 1_000_000;
        return 1_000_000_000;
    }

    /// @notice Shell token coupon value
    uint128 constant SHELL_COUPON_VALUE = 100000000000; // 100 shell token

    /// @notice NACKL token coupon value
    uint128 constant NACKL_COUPON_VALUE = 100000000000; // 100 NACKL token

    /// @notice USDC token coupon value
    uint128 constant USDC_COUPON_VALUE = 100000000; // 100 USDC (6 decimals)
 
    /// @notice Returns the minimum allowed stake value for a given token type
    /// @param tokenType Currency identifier
    /// @return minValue Minimum allowed stake amount for the token type.
    function minStakeValue(uint32 tokenType) internal pure returns (uint128) {
        if (tokenType == CURRENCIES_ID_SHELL) return uint128(MIN_VALUE_SHELL);
        if (tokenType == CURRENCIES_ID_USDC)  return uint128(MIN_VALUE_USDC);
        return uint128(MIN_VALUE);
    }

    /// @notice Returns the minimum allowed order size for a given token type
    function minOrderNotional(uint32 tokenType) internal pure returns (uint128) {
        if (tokenType == CURRENCIES_ID_SHELL) return MIN_ORDER_NOTIONAL_SHELL;
        if (tokenType == CURRENCIES_ID_USDC)  return MIN_ORDER_NOTIONAL_USDC;
        return MIN_ORDER_NOTIONAL_NACKL;
    }

    /// @notice Returns the lot size (amount quantisation step) for a token type.
    function lotSize(uint32 tokenType) internal pure returns (uint128) {
        if (tokenType == CURRENCIES_ID_SHELL) return LOT_SIZE_SHELL;
        if (tokenType == CURRENCIES_ID_USDC)  return LOT_SIZE_USDC;
        return LOT_SIZE_NACKL;
    }

    /// @notice Modifier for owner authorization using public key
    /// @param rootpubkey Expected owner public key
    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_INVALID_SENDER);
        _;
    }

    /// @notice Modifier for accepting incoming messages
    modifier accept() {
        tvm.accept();
        _;
    }

    /// @notice Modifier for sender address validation
    /// @param sender Expected sender address
    modifier senderIs(address sender) {
        require(msg.sender == sender, ERR_INVALID_SENDER);
        _;
    }

    /// @notice Stake information per PMP
    struct StakeInfo {
        uint128[] amount;          // confirmed stakes per outcome
        uint128[] debtAmount;     // confirmed stakes with debt
        uint128[] couponsAmount;  // confirmed coupon stakes per outcome
        uint128 candidateAmount;  // pending stakes per outcome
        uint32 candidateOutcome;  // pending stake outcome
        uint8 candidateBetType;  // bet type for pending stake (0=clean, 1=debt, 2=coupon)
        /// @notice Token type used by the stake record.
        uint32 tokenType;
        /// @notice Hash of oracle set used by related PMP.
        uint256 oracleListHash;
    }

    /// @notice Event information structure
    struct EventInfo {
        /// @notice Human-readable oracle event name.
        string eventName;
        /// @notice Oracle service fee required for confirmation.
        uint128 oracleFee;
        /// @notice Service deadline as Unix timestamp.
        uint64 deadline;
        /// @notice Human-readable event description.
        string describe;
        /// @notice Mapping outcome id => human-readable outcome name.
        mapping(uint32 => string) outcomeNames;
        /// @notice Number of active PMP confirmations bound to this event.
        uint128 count;
        /// @notice Optional trusted internal address for oracle governance calls.
        optional(uint256) trustAddr;
    }
}
