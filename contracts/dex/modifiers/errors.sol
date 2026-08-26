/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

/// @title PMP Errors
/// @notice Common error codes for PMP contracts
abstract contract Errors {    
    /// @notice Message sender address mismatch
    uint16 constant ERR_INVALID_SENDER = 101;

    /// @notice Deposit value is too low
    uint16 constant ERR_LOW_VALUE = 102;

    /// @notice Event already resolved
    uint16 constant ERR_ALREADY_RESOLVED = 103;

    /// @notice Wallet already initialized / stake confirmed
    uint16 constant ERR_ALREADY_INITIALIZED = 107;

    /// @notice User already claimed winnings

    /// @notice Wallet or stake not yet initialized/confirmed
    uint16 constant ERR_NOT_INITIALIZED = 114;


    /// @notice Contract not approved by oracle
    uint16 constant ERR_NOT_APPROVED = 116;



    /// @notice Stake submission period has ended
    uint16 constant ERR_STAKE_PERIOD_ENDED = 120;

    /// @notice PrivateNote is currently busy with another operation
    uint16 constant ERR_NOTE_BUSY = 121;

    /// @notice Stake candidate amount not zero
    uint16 constant ERR_STAKE_NOT_APPROVED = 122;
    

    /// @notice Stake submission period has not started
    uint16 constant ERR_STAKE_NOT_STARTED = 124;

    /// @notice Result submission period has not started
    uint16 constant ERR_RESULT_NOT_STARTED = 125;

    /// @notice Result submission period has ended
    uint16 constant ERR_RESULT_ENDED = 126; 
    

    /// @notice Zero token amount provided
    uint16 constant ERR_ZERO_TOKEN_AMOUNT = 128;

    /// @notice Invalid parameters provided
    uint16 constant ERR_INVALID_PARAMS = 129;

    /// @notice Invalid outcome ID provided
    uint16 constant ERR_INVALID_OUTCOME_ID = 130;


    /// @notice Order already cancelled
    uint16 constant ERR_ALREADY_CANCELLED = 132;

    /// @notice Order not cancelled
    uint16 constant ERR_NOT_CANCELLED = 133;



    /// @notice Wrong hash for oracle members

    /// @notice Invalid zero-knowledge proof
    uint16 constant ERR_INVALID_ZKPROOF = 137;

    /// @notice Invalid token type
    uint16 constant ERR_INVALID_TOKEN_TYPE = 138;



    /// @notice voucher nominal not allow
    uint16 constant ERR_NOT_ALLOWED = 141;

    /// @notice Cancel for unexist stake
    uint16 constant ERR_STAKE_NOT_EXISTS = 142;

    /// @notice Cannot generate coupon: user has debt
    uint16 constant ERR_HAS_DEBT = 143;

    /// @notice Cannot generate coupon: non-zero balance exists
    uint16 constant ERR_NON_ZERO_BALANCE = 144;

    /// @notice Coupon pool limit exceeded
    uint16 constant ERR_COUPON_POOL_LIMIT_EXCEEDED = 145;

    /// @notice No coupon available for staking
    uint16 constant ERR_NO_COUPON_AVAILABLE = 146;

    /// @notice Invalid bet type specified
    uint16 constant ERR_INVALID_BET_TYPE = 147;

    /// @notice Coupon already exists for this token type
    uint16 constant ERR_COUPON_ALREADY_EXISTS = 148;

    /// @notice Cannot perform operation: coupon is active
    uint16 constant ERR_COUPON_ACTIVE = 149;

    /// @notice Debt is non-zero for this token type
    uint16 constant ERR_DEBT_NON_ZERO = 150;

    /// @notice Invalid state for this operation
    uint16 constant ERR_INVALID_STATE = 151;



    /// @notice Base pools already frozen
    uint16 constant ERR_ALREADY_FROZEN = 154;


    /// @notice Stake period has not ended yet
    uint16 constant ERR_NOT_STAKEEND = 156;


    /// @notice Order book: order not found
    uint16 constant ERR_ORDER_NOT_FOUND = 158;


    /// @notice Order book: amount below minimum order size
    uint16 constant ERR_ORDER_TOO_SMALL = 160;

    /// @notice Order book: batch size exceeds MAX_BATCH_SIZE
    uint16 constant ERR_BATCH_TOO_LARGE = 161;

    /// @notice Order book: empty batch
    uint16 constant ERR_EMPTY_BATCH = 162;

    /// @notice Order book: amount is not a multiple of lot size
    uint16 constant ERR_AMOUNT_NOT_LOT_MULTIPLE = 163;

    /// @notice Order book: price is not a multiple of tick size
    uint16 constant ERR_PRICE_NOT_TICK_MULTIPLE = 164;

    /// @notice OrderBook shutdown not yet complete — claim blocked
    uint16 constant ERR_ORDERBOOK_NOT_SHUTDOWN = 165;


    /// @notice Open OrderBook orders prevent the requested action (e.g. coupon
    ///         issuance must wait for all orders to settle / cancel).
    uint16 constant ERR_OPEN_ORDERS_EXIST = 167;

    uint16 constant ERR_NOTIONAL_OVERFLOW = 168;

    /// @notice deployPMP: this note already holds a stake record for the event
    ///         (a prior deployPMP already succeeded). Blocks a re-deploy that
    ///         would overwrite the committed stake and re-debit the balance.
    uint16 constant ERR_STAKE_EXISTS = 169;

    /// @notice A root (RootPN / RootOracle) may only be brought up via the
    ///         stub + `updateCode` bootstrap; deploying it with its own
    ///         constructor is not a supported path and is rejected outright.
    uint16 constant ERR_NOT_ALLOWED_CONSTRUCTOR = 170;

    // ===== Replay protection =====

    /// @notice External message hash already processed within its expireAt window.
    uint16 constant ERR_MESSAGE_IS_EXIST = 400;

    /// @notice External message expireAt is too far in the future (> 5 minutes).
    uint16 constant ERR_MESSAGE_WITH_HUGE_EXPIREAT = 401;

    /// @notice External message expireAt is in the past — already expired.
    uint16 constant ERR_MESSAGE_EXPIRED = 402;

    // ===== Halo2 historical-proof verification =====

    /// @notice The supplied final-layer historical hash root is not present
    ///         in the node's GlobalHistoricalData for the requested layer.
    ///         Either the layer number is wrong or the hash has aged out of
    ///         the historical window.
    uint16 constant ERR_INVALID_HISTORY_PROOF = 403;

    /// @notice PMP._ensureFrozen sent a normalization refund to the deployer
    ///         but the deployer PN has not yet acknowledged via
    ///         `confirmRefundReceived`. Split/merge are gated until then to
    ///         prevent stale-stake races.
    uint16 constant ERR_NORM_REFUND_PENDING = 404;

    /// @notice `postSellOffer` ttl is 0 or exceeds `MAX_SELL_TTL` (1 hour). A SELL
    ///         offer commits no collateral at offer time, so its lifetime is
    ///         mandatory and capped (spec §2.1.1). The note rejects an out-of-range
    ///         ttl up front, before it can reach the book.
    uint16 constant ERR_SELL_DEADLINE_TOO_LONG = 405;

    /// @notice `placeSellOffer` caller is not the canonical TokenContract for
    ///         `(sellerPubkey, nonce)` derived from the pinned code + the seller's
    ///         key, so only a canonical TC can post an offer.

    /// @notice A deposit arrived with a currency mix `generateVoucher` cannot take gas from.
    /// @dev    Either more than two currencies, or two of which neither is SHELL, or two whose
    ///         SHELL leg is not exactly `GAS_DEPOSIT`, or a single non-SHELL currency on a
    ///         non-gas voucher — that last one has nothing to deduct the gas from, and quietly
    ///         letting it through would mint a note nobody paid the gas for.
    uint16 constant ERR_BAD_GAS_MIX = 407;

    /// @notice A single-SHELL deposit smaller than `GAS_DEPOSIT`.
    /// @dev    Checked BEFORE the subtraction, on purpose. Relying on the subtraction to revert
    ///         means relying on what this compiler does with an underflow, which is a property of
    ///         the toolchain rather than of the contract — and a silently wrapping subtraction
    ///         would turn a 1-SHELL deposit into an astronomically large nominal.
    uint16 constant ERR_BELOW_GAS_DEPOSIT = 408;

    /// @notice The second leg of a two-currency deposit is SHELL_FEE (300).
    /// @dev    Rejected because `deployPrivateNote` does not accept type 300: the voucher would be
    ///         unspendable — money taken, note impossible to place. Better to refuse the deposit
    ///         than to issue a claim nothing can honour.
    uint16 constant ERR_FEE_TYPE_NOT_DEPOSITABLE = 409;
}