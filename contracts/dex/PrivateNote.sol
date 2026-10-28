pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./modifiers/replayprotection.sol";
import "./PMP.sol";
import "./RootPN.sol";
import "./OrderBook.sol";
import "./libraries/DexLib.sol";
import "../airegistry/InferenceOrderBook.sol";
import "../airegistry/RootModel.sol";
import "../airegistry/TokenContract.sol";

/// @notice Buyer-side calls into an inference streaming deal (TokenContract).
///         Both are gated on `msg.sender == _buyer` in TokenContract, so this
///         note must be the bound buyer note (spec §4.1/§4.2).
interface IInferenceDeal {
    function stop() external;
    function dispute() external;
    function cleanupUnopened() external;
    /// @notice Gas (attached ECC[2], flag 17) plus `amount` as a private-balance figure.
    function fundDeal(uint128 amount, optional(bytes) endpointCipher) external;
    /// @dev Added beside `fundDeal`, not as a second interface for the same contract: two
    ///      names for one deal would let the two drift and give `abi.functionId` two answers.
    function fundBuyerBond(uint128 amount) external;
    /// @notice Sender check and nothing else — the answer is whether it bounces.
    function touchDeal() external;
}

/// @notice Wallet that can deploy and interact with PMP contracts
contract PrivateNote is Modifiers, ReplayProtection {

    /// @notice Contract semantic version.
    string constant version = "4.0.36";

    /// @notice Max lifetime (seconds) of a resting SELL offer (spec §2.1.1). A SELL commits NO
    ///         collateral at offer time (the seller bond is funded only after a match), so an ask
    ///         left on the book is a free option; a mandatory, bounded expiry caps that exposure.
    ///         Enforced HERE because the note is the seller's trusted entry and the ONLY path to
    ///         the book — the TC posts the offer for this note, and there is no direct seller-key
    ///         post path. The note hands the book an already-anchored ABSOLUTE deadline, so the
    ///         book neither re-validates the bound (its clock has moved on) nor re-anchors it.
    uint64 constant MAX_SELL_TTL = 3600;   // 1 hour

    /// @notice Mirrors of the book's order-flag bits and physical bounds, needed to validate an
    ///         order HERE before it is submitted. They are contract-local constants in
    ///         `InferenceOrderBook` / `AiRegistryModifiers` and not reachable across contracts, so
    ///         they are restated — keep in sync with the book if a bit is ever renumbered.
    uint8   constant FLAG_MARKET         = 0x04;
    uint8   constant FLAG_AON            = 0x20;
    uint8   constant FLAG_SUBSCRIPTION   = 0x40;
    uint8   constant SUB_WEEKS           = 4;         // a subscription is always one month
    uint128 constant SUB_TICKS_PER_WEEK  = 10_080;    // week / MIN_SECONDS_PER_TICK (60s per tick)

    /// @notice Canonical-deal derivation anchors (note-funded model). The TokenContract
    ///         and RootModel code hashes/depths are NOT pinned constants here — they are
    ///         baked into this note at deploy by RootPN (`_tokenContractCodeHash` /
    ///         `_rootModelCodeHash` below). This keeps the pin ONE-WAY: the TokenContract
    ///         pins the note's code hash (`postFromNote` identity check); the note pinning
    ///         the TC hash back would make the two mutually recursive and the build never
    ///         converge (same rule as the IOB code baked below). `fundDeal` /
    ///         `fundDeployShell` / `postSellOffer` derive the canonical RootModel /
    ///         TokenContract from these baked hashes + this note's own key (+nonce),
    ///         never a caller-supplied address.
    // Canonical AI SuperRoot account id (workchain 0) — anchor for the RootModel-address derivation.
    // FIXED at the vanity 0:0c0c… on LOCAL, SHELLNET and MAINNET (zerostate force-places the SuperRoot
    // here). shellnet == local, no per-version / code-derived rotation — see dexdo-specs/shellnet-update.md.
    uint256 constant SUPER_ROOT_ADDR           = 0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c;

    /// @notice Unique deposit identifier hash (static)
    uint256 public static _depositIdentifierHash;

    /// @notice Ephemeral public key for authorization
    uint256 _ephemeralPubkey;

    /// @notice PMP code for deployment
    TvmCell _pmpCode;

    /// @notice PrivateNote code for deployment
    TvmCell _privateNoteCode;

    /// @notice Oracle code hash + depth for address computation
    uint256 _oracleCodeHash;
    uint16  _oracleCodeDepth;

    /// @notice OracleEventList code hash + depth for address computation
    uint256 _oracleEventListCodeHash;
    uint16  _oracleEventListCodeDepth;

    /// @notice TokenContract code hash + depth for canonical deal-address derivation,
    ///         baked at deploy by RootPN (NOT a code constant — keeps the note<->TC pin
    ///         one-way; the TC pins the note's code hash, never the reverse).
    uint256 _tokenContractCodeHash;
    uint16  _tokenContractCodeDepth;

    /// @notice RootModel code hash + depth for canonical deal-address derivation,
    ///         baked at deploy by RootPN.
    uint256 _rootModelCodeHash;
    uint16  _rootModelCodeDepth;

    /// @notice OrderBook code for deployment
    TvmCell _orderBookCode;

    /// @notice InferenceOrderBook code (§8) baked at deploy by RootPN. The note
    ///         deploys/derives the canonical book from THIS code — never from a
    ///         caller-supplied code or a raw address — so it cannot be tricked
    ///         to the canonical book of the matching build. (Kept in data, not as a code
    ///         constant, so the note↔OB pin stays one-way: OB pins the note's
    ///         NOTE_CODE_HASH; pinning the OB hash in the note's CODE would make
    ///         the two hashes mutually recursive and the build never converge.)
    TvmCell _inferenceOrderBookCode;

    /// @notice TokenContract code (§3) baked at deploy by RootPN, for the SAME reason the book code
    ///         above is: the note deploys the deal now (4.0.36), and a deploy needs the code cell,
    ///         not the pin. `_tokenContractCodeHash`/`_tokenContractCodeDepth` stay beside it and
    ///         keep doing what they always did — deriving the address of a deal this note did not
    ///         deploy (the buyer's side of a match) — so both forms of the same fact are present:
    ///         the code for the one deal this note creates, the hash for every deal it merely talks to.
    TvmCell _tokenContractCode;

    /// @notice Mapping from stake hash to stake info
    mapping(uint256 => StakeInfo) public _stakes;

    /// @notice Address of PMP currently being interacted with
    optional(address) _busy;

    /// @notice Hash of the last stake operation
    uint256 _lastHash;

    /// @notice Amount locked in an outbound transfer (cleared on accept/bounce)
    uint128 _pendingTransferAmount;

    /// @notice Token type of the pending outbound transfer
    uint32 _pendingTransferTokenType;

    /// @notice Single-order placeOrder buy: collateral + fee reserve locked
    ///         (mirrored into _lockedInOrders). Restored on bounce / cleared
    ///         on onOrderPlaced. Kept SEPARATE from _pendingTransferAmount so
    ///         the bounce handler can correctly distinguish a bounced
    ///         offerTransfer (no _lockedInOrders touch) from a bounced
    ///         placeOrder (must release _lockedInOrders). Before this split,
    ///         every offerTransfer bounce also mutated _lockedInOrders,
    ///         corrupting open-orders accounting and opening a double-spend
    ///         path for users with resting orders.
    uint128 public _pendingPlaceBuyLock;
    uint32  public _pendingPlaceBuyTokenType;

    /// @notice clientOrderId reserved by the in-flight single-order
    ///         placeOrder. On bounce we use this to release the sentinel
    ///         from `_clientOrderIds` (otherwise the cid leaks). Cleared in
    ///         onOrderPlaced / onOrderRejected on the happy path.
    uint128 _pendingPlaceClientOrderId;

    /// @notice clientOrderIds reserved by the in-flight placeBatch.
    ///         Same role as `_pendingPlaceClientOrderId` but for batches.
    uint128[] _pendingBatchClientOrderIds;

    /// @notice Per-OB-per-order fee reserves
    ///         (obAddress => orderId => remaining fee reserve).
    ///         Populated in onOrderPlaced from feeReserve provided by OrderBook.
    ///         Keyed by OB address because a PN can trade on several OBs
    ///         (one per PMP event) and each OB's `_nextOrderId` is
    ///         independent — two OBs may hand out the same orderId, which
    ///         would collide in a flat mapping.
    mapping(address => mapping(uint128 => uint128)) _orderFeeReserves;

    /// @notice Per-OB-per-order total lock
    ///         (obAddress => orderId => remaining collateral+fee lock).
    ///         Populated in onOrderPlaced from the authoritative `lock`
    ///         param that OrderBook computes as `cost + maxFee`. Each
    ///         `onOrderFilled` decrements it by the consumed amount; on
    ///         `isFinal` fill (or cancel) any residual — accumulated from
    ///         `floor(amount*price/10000)` per-fill truncation — is
    ///         refunded to `_balance` so `_lockedInOrders` drains exactly.
    ///         Buy orders only (sells lock outcome tokens, not collateral).
    ///         Keyed by OB address to avoid cross-OB orderId collisions.
    mapping(address => mapping(uint128 => uint128)) _orderLocks;

    /// @notice Collateral locked in open OrderBook orders, per token type.
    ///         Incremented on order placement, decremented on fill/cancel.
    mapping(uint32 => uint128) _lockedInOrders;

    /// @notice Number of open orders this PN has resting in any OrderBook.
    ///         Incremented on `onOrderPlaced` (per accepted order), decremented
    ///         on `onOrderCancelled` and on `onOrderFilled` with `isFinal=true`.
    ///         Rejected placements never increment (no `onOrderPlaced` fires for
    ///         them), so this counter mirrors the OB-side resting set.
    ///         Used to gate operations that must not proceed while orders are
    ///         in flight (e.g. `generateCoupon`).
    uint32 _openOrderCount;

    /// @notice Inference orders of this note's that still rest in a book, by order id.
    /// @notice Deals of this note's that are still alive, by order id.
    /// @dev    TWO COUNTERS, NOT ONE, and the reason is that a match does two things at once: it
    ///         removes an order from the book AND starts a deal. With a single quantity the result
    ///         would depend on which of those two messages arrived first. Kept apart, the order of
    ///         arrival cannot matter — one falls, the other rises, and neither reads the other.
    ///         A partial fill grows the deals and leaves the order resting, which follows from the
    ///         same separation rather than from a special case.
    ///
    ///         Withdrawal requires BOTH to be empty. Ids rather than addresses because a note has
    ///         no business storing a deal's address — the removal side authenticates by DERIVING
    ///         the caller, exactly as `creditFromDeal` does, so nothing has to be kept to compare
    ///         against.
    ///
    ///         WHAT THIS IS NOT. Both mirrors arrive `bounce: false` and deliberately do not block
    ///         execution: making them blocking would let one note stall a fill for both sides. So a
    ///         mirror that never arrives leaves no record, and the guard then says nothing — it
    ///         does not break, it goes quiet. This protects against an owner withdrawing too early
    ///         by accident; it is not a guarantee, and reading it as one would be a mistake.
    /// @dev Key = `tvm.hash(abi.encode(book, orderId))`, the same composite-key idiom this
    ///      contract already uses for stakes. THE BOOK MUST BE INSIDE THE KEY: each book runs its
    ///      own `_nextOrderId`, so ids from different models collide — order 7 in one book and
    ///      order 7 in another are different orders. A flat map keyed on the id alone would merge
    ///      them silently, and clearing one would clear the other. Composing the key closes that by
    ///      construction rather than by whoever reads this next being careful.
    ///
    ///      Kept apart from the PMP order structures on purpose: the two paths are not mixed.
    mapping(uint256 => bool) _restingInf;
    /// @dev Keyed by the DEAL'S ADDRESS, which needs no second level: a deal address is derived
    ///      from the seller's key and nonce and is unique on its own. It also authenticates the
    ///      removal for free — a deal announcing its own destruction is `msg.sender`, so nothing
    ///      has to be passed and nothing has to be compared.
    /// @notice Requests this note has sent to a book and not yet had answered (task Q).
    /// @dev    KEYED BY A NUMBER THIS NOTE CHOSE ITSELF, before sending, and holding the escrow it
    ///         committed. The book assigns an order id only if the request becomes an order, which
    ///         is precisely the case that was already covered — the uncovered ones are a request
    ///         that is refused without ever resting and a request whose message bounces. Neither
    ///         has a book-side id, so nothing but a number of our own can name them.
    ///
    ///         It holds the AMOUNT, not just a flag, because every answer has to undo the
    ///         subtraction this note made before sending. Carrying the figure back in each answer
    ///         would work for three of the four and not for the bounce, where only the leading bits
    ///         of the body survive.
    ///
    ///         AND IT GATES WITHDRAWAL, like the other two maps. Between the debit and the answer
    ///         the money is neither here nor there: subtracted from `_balance`, not yet recorded
    ///         anywhere else. A withdrawal in that window would take a balance that is missing the
    ///         escrow and can never be topped up again, because the answer credits a note that has
    ///         already latched `_hasWithdrawn`.
    mapping(uint64 => uint128) _pendingInf;
    /// @notice Source of the numbers above. Monotonic, never reused within this note.
    /// @dev    Uniqueness only has to hold PER NOTE: the book stores what it was given and echoes
    ///         it back to the same note, so two notes choosing the same number never meet.
    uint64 _nextClientOrderId;
    mapping(address => bool) _liveDeals;
    /// @notice When the last touch went out. One stamp for the note, not one per deal.
    uint32 _lastDealTouch;
    /// @notice How rarely a touch may be sent.
    /// @dev    CHOSEN FROM WHAT THE TOUCH IS FOR, not picked. It is a diagnostic for a contract
    ///         believed dead, never part of trading, so it needs no throughput at all — one probe
    ///         a minute clears a stale record within a minute of the owner noticing it, which is
    ///         faster than any human notices. What the limit is really guarding is the other
    ///         direction: without it, anyone holding the owner key could burn the note's gas by
    ///         probing in a loop, and a note pays for its own sends.
    ///
    ///         60s rather than a new number because the deal's own pacing already uses it —
    ///         `MIN_SECONDS_PER_TICK` and `MIN_CLAIM_INTERVAL` are both a minute — and inventing a
    ///         second cadence for the same system means two things to keep in step later.
    uint32 constant DEAL_TOUCH_INTERVAL = 60;
    // NO PARALLEL COUNTERS. A count kept beside a map is a second source of truth about the same
    // fact, and the two drift silently: the map says one thing, the counter another, and the
    // withdraw gate reads the counter. `.empty()` asks the map itself, which is how the note
    // already gates on `_stakes` in `initTransfer`. One truth, one check.

    /// @notice Per-PMP-event open-order counter.
    ///         Key = tvm.hash(abi.encode(eventId, oracleListHash, tokenType))
    ///         — same key shape as `_stakes`. Gates `claim` so that a holder
    ///         of a resting SELL at PMP shutdown cannot claim before all
    ///         `onOrderCancelled` callbacks for that event have folded the
    ///         outcome tokens back into `_stakes[hash].amount[...]`. Without
    ///         this gate, OB→PMP `onOrderBookShutdownComplete` (which flips
    ///         `_orderBookDone`) can land before OB→PN `onOrderCancelled`,
    ///         the user claims, `_stakes[hash]` is deleted, and the late
    ///         cancel-callback silently drops the outcome tokens (see
    ///         `onOrderCancelled` sell-branch comment).
    mapping(uint256 => uint32) _openOrdersByEvent;

    /// @notice Active `clientOrderId → orderId` index (per-PN, not per-OB).
    ///         Sentinel `type(uint128).max` means "place sent, OB-assigned
    ///         orderId not yet received". On `onOrderPlaced` it becomes the
    ///         real orderId. Cleared in onOrderCancelled / onOrderFilled
    ///         (isFinal) / onOrderRejected. cid==0 means "not set" and is
    ///         never put in this map. Owns the uniqueness invariant:
    ///         placeOrder rejects a cid that already exists here.
    mapping(uint128 => uint128) _clientOrderIds;

    /// @notice Monotonic counter of user-initiated ops sent to any OB.
    ///         Each placeOrder / placeBatch / cancelOrder /
    ///         cancelOrderByClient increments it. Echoed by OB in every
    ///         callback so PN can tell "this is ack of MY current op"
    ///         from "this is a late callback from a previous/incidental
    ///         event" (shutdown drain, maker-side fill, etc.).
    uint64 _opNonce;

    /// @notice Nonce of the op currently holding `_busy`. 0 = no op in
    ///         flight. Set on each user op together with `_busy`. Callbacks
    ///         clear `_busy`/pending state only when their echoed nonce
    ///         matches this value — stale callbacks become no-ops on the
    ///         `_busy` slot.
    uint64 _busyOpNonce;

    /// @notice True while a batch order-book operation is in flight between the
    ///         outbound executePlaceBatch/executeCancelBatch/executeCancelAll call
    ///         and the terminating onBatchComplete callback. Prevents per-order
    ///         callbacks from clearing _busy prematurely.
    bool _pendingBatchActive;

    /// @notice Total balance locked on _balance for an in-flight place-batch
    ///         (cost + max taker fee, summed across all buy orders). Restored
    ///         by onBounce if executePlaceBatch bounces.
    uint128 _pendingBatchBuyLock;

    /// @notice Token type of the pending batch buy lock.
    uint32  _pendingBatchTokenType;

    /// @notice Stake hash associated with the pending batch (for sell-side restore).
    uint256 _pendingBatchStakeHash;

    /// @notice In-flight `deleteStake` → `PMP.forfeitStake` notification. Set
    ///         when `deleteStake` dispatches; cleared on
    ///         `onForfeitAccepted` (PMP accepted the forfeit) or on
    ///         `onBounce` (PMP unreachable — e.g. already self-destructed).
    ///         Used by `onBounce` to disambiguate from the stake/order
    ///         bounce branch (no candidate-amount restoration needed).
    bool _pendingForfeit;

    /// @notice Single record per sell order in the pending batch (used for
    ///         bounce-protection restore). Combined into one array (vs two
    ///         parallel arrays) to halve cell writes per push.
    struct PendingBatchSell {
        uint32  outcomeId;
        uint128 amount;
    }
    PendingBatchSell[] _pendingBatchSells;

    /// @notice Current token balance
    mapping(uint32 => uint128) _balance;

    /// @notice User debt per token type (created after coupon win)
    /// @dev Debt is created when user wins with a coupon
    uint128 _debt;

    /// @notice Token type for the debt
    uint32 _debtTokenType;
    
    /// @notice Indicates that tokens were withdrawn before
    bool _hasWithdrawn;

    /// @notice Indicates that a P2P transfer was sent from this wallet
    bool _hasTransferred;

    /// @notice Available coupons
    uint128 _couponsValue;

    /// @notice Coupon type (used for tracking which token type the coupon is for)
    uint32 _couponsTokenType;

    // Events

    /// @notice Emitted when owner pubkey is changed
    /// @param oldPubkey Previous ephemeral public key
    /// @param newPubkey New ephemeral public key
    event OwnerChanged(uint256 oldPubkey, uint256 newPubkey);

    /// @notice Emitted when a single-outcome stake is confirmed by PMP
    /// @param stakeController PMP address (stake controller)
    /// @param outcome Outcome index used in stake
    /// @param amount Confirmed stake amount
    /// @param betType 0 - clean bet, 1 - debt bet, 2 - coupon bet
    event StakeConfirmed(address stakeController, uint32 outcome, uint128 amount, uint8 betType);

    /// @notice Emitted when a single-outcome stake is cancelled and funds returned
    /// @param stakeController PMP address
    /// @param value Returned token value to balance
    event StakeCancelled(address stakeController, uint128 value);

    /// @notice Emitted when a full-set stake is confirmed by PMP
    /// @param stakeController PMP address
    /// @param amount Confirmed stake amounts per outcome
    event FullSetStakeConfirmed(address stakeController, uint128[] amount);

    /// @notice Emitted when a full-set stake is cancelled and funds returned
    /// @param stakeController PMP address
    /// @param value Total returned token value to balance
    event FullSetStakeCancelled(address stakeController, uint128 value);

    /// @notice Emitted when claim is accepted by PMP
    /// @param stakeController PMP address
    /// @param outcome Optional outcome (set when market resolved)
    /// @param payout Total payout (including debt payout)
    event ClaimAccepted(address stakeController, optional(uint32) outcome, uint128 payout);

    /// @notice Emitted when a PMP is about to be deployed by this wallet
    /// @param eventId Event identifier
    /// @param tokenType Token type used by PMP
    /// @param pmpAddress Deterministic PMP address
    /// @param oracleEventLists OracleEventList addresses used by PMP
    /// @param oracleFee Fee per oracle (same order as oracleEventLists)
    event PMPDeployed(uint256 eventId, uint32 tokenType, address pmpAddress, address[] oracleEventLists, uint128[] oracleFee);

    /// @notice Emitted immediately when placeOrder is called (before OB confirmation)
    event OrderSubmitted(uint128 clientOrderId, uint32 outcomeId, bool isBuy, uint256 price, uint128 amount, uint8 flags, uint256 eventId, uint32 tokenType);

    /// @notice Emitted when an order is confirmed placed on OrderBook
    /// @param orderBook OrderBook address
    /// @param orderId Assigned order ID
    event OrderPlacedConfirmed(
        address orderBook,
        uint128 orderId,
        uint128 clientOrderId,
        uint32  outcomeId,
        bool    isBuy,
        uint8   flags,
        uint256 price,
        uint128 amount
    );

    /// @notice Emitted when an order fill callback is received from OrderBook
    event OrderFilledConfirmed(address orderBook, uint128 orderId, uint32 outcomeId, uint128 filledAmount, uint256 clearingPrice, bool isBuy, uint128 feeAmount, bool isRebate, bool isFinal);

    /// @notice Emitted when an order cancel callback is received from OrderBook
    event OrderCancelledConfirmed(address orderBook, uint128 orderId, uint32 outcomeId, bool isBuy, uint128 returnAmount);

    /// @notice Emitted when a place callback comes back as a rejection (OB
    ///         refused the placement — e.g. epoch mismatch, FOK pre-check
    ///         failed, post-only crossed). PN has already unlocked the funds
    ///         (`onOrderRejected` body); this event is for off-chain monitors
    ///         to attribute the rejection back to the originating client_order_id.
    event OrderPlaceRejected(
        address orderBook,
        uint256 eventId,
        uint128 clientOrderId,
        uint32  outcomeId,
        bool    isBuy,
        uint8   flags,
        uint256 price,
        uint128 amount,
        uint64  opNonce
    );

    /// @notice Emitted when an outbound transfer is initiated
    event TransferInitiated(address dest, uint32 tokenType, uint128 amount);

    /// @notice Emitted when an inbound transfer is received and credited
    event TransferReceived(address from, uint32 tokenType, uint128 amount);

    /// @notice Emitted when a TokenContract credits this note (generation 4.0.33).
    /// @dev    Separate from `TransferReceived` on purpose: that one is note-to-note and its
    ///         counterparty is a note, this one's is a deal. Off-chain reconciliation pairs each
    ///         credit against the deal's own subtraction, and one event carrying two different
    ///         kinds of counterparty would make that pairing guesswork.
    event DealCredited(address deal, uint128 amount);

    /// @notice Emitted when an InferenceOrderBook credits this note (generation 4.0.33).
    /// @dev    Distinct from `DealCredited` for the same reason that one is distinct from
    ///         `TransferReceived`: the counterparty is a different KIND of contract, verified by a
    ///         different derivation, and an event that blurred them would make an audit of who paid
    ///         whom depend on guessing.
    event BookCredited(address book, uint128 amount);

    /// @notice PrivateNote constructor
    /// @param value Initial token balance
    /// @param ephemeralPubkey Ephemeral public key for authorization
    /// @param tokenType Type of token
    /// @param pmpCode PMP contract code used for deterministic PMP derivation.
    /// @param orderBookCode OrderBook contract code used for deterministic OB derivation.
    /// @param inferenceOrderBookCode InferenceOrderBook code (§8) for deterministic book derivation.
    /// @param oracleCodeHash Oracle contract code hash.
    /// @param oracleCodeDepth Oracle contract code depth.
    /// @param oracleEventListCodeHash OracleEventList contract code hash.
    /// @param oracleEventListCodeDepth OracleEventList contract code depth.
    constructor(uint128 value, uint256 ephemeralPubkey, uint32 tokenType, TvmCell pmpCode, TvmCell orderBookCode,
                TvmCell inferenceOrderBookCode, TvmCell tokenContractCode,
                uint256 oracleCodeHash, uint16 oracleCodeDepth, uint256 oracleEventListCodeHash, uint16 oracleEventListCodeDepth,
                uint256 tokenContractCodeHash, uint16 tokenContractCodeDepth,
                uint256 rootModelCodeHash, uint16 rootModelCodeDepth) {
        tvm.accept();
        require(msg.sender == ROOT_PN_ADDRESS, ERR_INVALID_SENDER);
        _pmpCode = pmpCode;
        _privateNoteCode = tvm.code();
        _oracleCodeHash = oracleCodeHash;
        _oracleCodeDepth = oracleCodeDepth;
        _oracleEventListCodeHash = oracleEventListCodeHash;
        _oracleEventListCodeDepth = oracleEventListCodeDepth;
        _tokenContractCodeHash = tokenContractCodeHash;
        _tokenContractCodeDepth = tokenContractCodeDepth;
        _rootModelCodeHash = rootModelCodeHash;
        _rootModelCodeDepth = rootModelCodeDepth;
        _orderBookCode = orderBookCode;
        _inferenceOrderBookCode = inferenceOrderBookCode;
        _tokenContractCode = tokenContractCode;
        _balance[tokenType] = value;
        _ephemeralPubkey = ephemeralPubkey;
        RootPN(ROOT_PN_ADDRESS).privateNoteDeployed{value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID}(_depositIdentifierHash, tokenType, value);
    }

    /// @notice Changes the owner public key
    /// @param newPubkey New public key
    /// @dev Authenticated by the MODIFIER, not in the body, so the check runs BEFORE `accept`.
    ///      Modifiers execute in declaration order: with `accept` first, the note paid the compute
    ///      for every external message that reached this entry point and only then discovered the
    ///      sender was a stranger. The self-funding floor does not cover that case — `ensureBalance`
    ///      sat after the check, so on exactly the path being abused it was never reached, and a
    ///      revert would have undone it anyway while the gas stayed spent. Every other owner-gated
    ///      method here already reads `onlyOwnerPubkey(_ephemeralPubkey) accept`; this one did not.
    function changeOwner(uint256 newPubkey) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        // Refuse to demote ownership to pubkey=0 — every onlyOwnerPubkey-gated
        // PN method would then accept msg.pubkey()==0, i.e. any unsigned tx.
        require(newPubkey != 0, ERR_INVALID_PARAMS);
        ensureBalance();

        uint256 oldPubkey = _ephemeralPubkey;
        _ephemeralPubkey = newPubkey;
        
        address addrExtern = address.makeAddrExtern(PRIVATENOTE_OWNER_CHANGED, bitCntAddress);
        emit OwnerChanged{dest: addrExtern}(oldPubkey, newPubkey);
    }

    /// @notice Ensures minimal native balance for operations
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) return;
        gosh.mintshellq(MIN_BALANCE);
    }

    // ── Inference-market confirmation mirrors (owner-facing) ─────────────────
    // The canonical InferenceOrderBook pushes a confirmation into the owner's note so the
    // owner can read JUST this note's ext-out stream and get the deal address — no full index.
    // Auth: the caller must be the canonical book for `modelHash`, derived from the IOB code this
    // note already stores (`_inferenceOrderBookCode`, baked by RootPN) — no pin, only the canonical book.

    /// @notice Owner mirror of a resting order placement. `tokenContract` = seller's TC for a SELL
    ///         offer, 0 for a BUY (no TC until a match binds one).
    event InferenceOrderPlacedConfirmed(address orderBook, address tokenContract, uint128 orderId, bool isBuy, uint256 price, uint128 ticks);
    /// @notice Owner mirror of a match. `tokenContract` is the deal contract: the buyer reads it to
    ///         track the stream; the seller gets it symmetrically (their own TC).
    /// @notice Emitted when the book reports one of this note's orders left it.
    /// @dev    Cancel, expiry and fill are indistinguishable here on purpose — the book's one
    ///         removal point sends the same message for all three, and the note only needs to
    ///         know the order is no longer resting.
    // Mirrors InferenceOrderBook's removal causes; only REMOVED_REJECTED is acted on here, the
    // rest are carried to the owner untouched.
    uint8 constant REMOVED_FILLED    = 1;
    uint8 constant REMOVED_CANCELLED = 2;
    uint8 constant REMOVED_EXPIRED   = 3;
    uint8 constant REMOVED_DUST      = 4;
    uint8 constant REMOVED_REJECTED  = 5;

    event InferenceOrderRemoved(address book, uint128 orderId, uint8 cause, uint128 refunded);
    /// @notice A request refused by the book without ever becoming an order (task Q, outcome 3).
    /// @dev    Carries the CLIENT number, because that is the only name this outcome has: no order
    ///         id was ever assigned. `refunded` is what actually landed on the balance, not what the
    ///         book claimed, so the event describes this note's own books rather than a message.
    event InferenceOrderRejectedMirror(address book, uint64 clientOrderId, uint8 reason, uint128 refunded);

    /// @notice Emitted when a stake record is dropped locally, with no PMP to answer.
    /// @dev    The PMP self-destructed before the forfeit reached it, so there is no
    ///         authoritative counterpart — this is the only record that will exist.
    event StakeDroppedLocally(uint256 stakeHash, uint32 tokenType,
                              uint128[] amount, uint128[] debtAmount, uint128[] couponsAmount);

    /// @notice Emitted when a forfeit becomes final on the owner's side.
    /// @dev    Carries the stake key rather than the amounts: PMP's `StakeForfeited` is the
    ///         authoritative record and has them, and duplicating figures across two contracts
    ///         is how the two come to disagree.
    event StakeForfeitConfirmed(address pmp, uint256 stakeHash);

    /// @notice Emitted when a deal tells this note it is gone.
    event InferenceDealClosed(address deal);

    event InferenceFilledConfirmed(address orderBook, address tokenContract, uint128 orderId, uint128 ticks, uint256 clearingPrice, bool isBuy);

    /// @dev Authenticated BEFORE `tvm.accept()`. These mirrors are the note's only inbound entry
    ///      points that anyone can address, and accepting first would make the note pay the compute
    ///      for every message sent to them, whoever sent it — the note is the owner's wallet and
    ///      identity, and without native balance it cannot send anything at all. Same ordering as
    ///      `TokenContract.fundFromOrderBook`, for the same reason.
    function onInferencePlaced(uint256 modelHash, address tokenContract, uint128 orderId, uint64 clientOrderId, bool isBuy, uint256 price, uint128 ticks) public {
        require(msg.sender == DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash), ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        // The order now rests in the book. Recorded here because this mirror is the only moment
        // the note learns an order id at all — `placeInferenceBuy` returns nothing, the book
        // assigns the id, and the refund path (`creditFromBook`) carries only an amount and a model.
        uint256 k = tvm.hash(abi.encode(msg.sender, orderId));
        _restingInf[k] = true;
        // OUTCOME 1 OF 4 — the request became an order. The pending record hands over to the
        // resting record: the money is accounted for in the book from here on, and the guard that
        // refuses a withdrawal moves from one map to the other without a gap between them.
        delete _pendingInf[clientOrderId];
        emit InferenceOrderPlacedConfirmed{dest: address.makeAddrExtern(PRIVATENOTE_INFERENCE_PLACED, bitCntAddress)}(
            msg.sender, tokenContract, orderId, isBuy, price, ticks);
    }

    /// @notice What this note still has outstanding, and therefore why a withdrawal is refused.
    /// @dev    SEPARATE FROM `getDetails` on purpose: its nine-field answer is read by tooling that
    ///         has no interest in this, and widening it would break those readers for everyone.
    ///
    ///         THE TWO LISTS ARE NOT EQUALLY USEFUL, and it is worth saying which is which.
    ///
    ///         `deals` are ADDRESSES — exactly what `touchDeal` takes. That closes the hard case
    ///         completely: a stuck owner reads an address here and unblocks with it, without having
    ///         to dig the match event out of his own history. Before this there was a mechanism to
    ///         clear a stale deal and no way to learn what to point it at.
    ///
    ///         `orders` are `hash(book, orderId)` and are OPAQUE. They answer "how many are
    ///         outstanding" and not "which" — cancelling needs `(modelHash, orderId)`, and neither
    ///         survives the hash. That is the lighter case: an order can always be cancelled from
    ///         the owner's own event history, and the count is enough to tell whether the gate is
    ///         held by orders or by deals. Anyone tempted to make these readable should widen the
    ///         key rather than the getter — the opacity is in what was stored, not in what is
    ///         returned here.
    function getOutstanding() external view returns (address[] deals, uint256[] orders) {
        for ((address d, bool live) : _liveDeals) {
            if (live) { deals.push(d); }
        }
        for ((uint256 k, bool resting) : _restingInf) {
            if (resting) { orders.push(k); }
        }
    }

    /// @notice Ask a deal whether it is still there, and drop the record if it is not.
    /// @dev    THE SIGNAL IS THE BOUNCE. A live deal that belongs to this note answers by simply
    ///         not bouncing, and closes itself later on its own. A deal that no longer exists
    ///         bounces, and so does a live one this note was never party to — both mean the record
    ///         here is stale, and both clear it. See `TokenContract.touchDeal` for why the third
    ///         case has to bounce as well.
    ///
    ///         `require(_liveDeals.exists(deal))` is what keeps this a signal rather than a button.
    ///         An address that is not already tracked cannot be passed in, so this cannot be aimed
    ///         at anything — the owner may only ask about deals the note already believes are hers.
    ///
    ///         NO MONEY IS RESTORED when a record is dropped, exactly as with a cancelled order.
    ///         This says "stop waiting for this deal", not "give the escrow back".
    ///
    ///         Takes no `_busy` latch: it is one message with no follow-up to serialise, and taking
    ///         the latch would let a single stuck touch block transfers and withdrawals.
    function touchDeal(address deal) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(_liveDeals.exists(deal), ERR_INVALID_PARAMS);
        require(uint32(block.timestamp) >= _lastDealTouch + DEAL_TOUCH_INTERVAL, ERR_NOTE_BUSY);
        _lastDealTouch = uint32(block.timestamp);
        IInferenceDeal(deal).touchDeal{value: 0.1 vmshell, flag: 1, bounce: true}();
    }

    /// @notice A deal this note is party to is destroying itself.
    /// @dev    NO PARAMETERS, deliberately. `_liveDeals` is keyed by the deal's address, and a deal
    ///         announcing its own death IS `msg.sender` — so the sender authenticates itself and
    ///         identifies itself in the same fact, and there is nothing to pass or verify.
    ///
    ///         SILENT WHEN THERE IS NO RECORD, and this is required rather than tidy: the message
    ///         comes from a contract in the act of self-destructing, which has no second attempt
    ///         and nowhere to receive a bounce. A `require` here would turn one undelivered message
    ///         into a note that can never be withdrawn — trading "withdrew too early" for "never
    ///         withdraws", which is the worse of the two.
    function onDealClosed() public {
        // ACCEPTS BEFORE CHECKING, and unlike the three mirrors above that is fine here. Their
        // rule — authorise first, or the note pays compute for anyone's message — rests on the note
        // having gas it can run out of. It does not: `ensureBalance()` mints, in this very call,
        // and the note's dapp has a config to mint from. Nothing is spent that could be exhausted.
        //
        // The membership test is therefore about STATE and only state, which is all this function
        // has to protect. It also keeps the silence the destroying deal depends on — that contract
        // has no second attempt and nowhere to receive a bounce.
        tvm.accept();
        ensureBalance();
        if (_liveDeals.exists(msg.sender)) {
            delete _liveDeals[msg.sender];
            emit InferenceDealClosed{dest: address.makeAddrExtern(PRIVATENOTE_INFERENCE_DEAL_CLOSED, bitCntAddress)}(msg.sender);
        }
    }

    /// @notice The book says one of this note's orders no longer rests there.
    /// @dev    Sent from the book's SINGLE removal point, so cancel, expiry and fill all arrive
    ///         here — the note does not have to know which happened, only that the order is gone.
    ///         That matters because the money side says nothing: a refund arrives via
    ///         `creditFromBook`, which carries an amount and a model and no identifier, so without
    ///         this message a resting record could never be cleared and the note would become
    ///         permanently unwithdrawable.
    ///
    ///         Clearing by ID is what makes redelivery harmless: setting a flag false twice is the
    ///         same as once, where a counter would go wrong in both directions — a duplicate
    ///         message would double-decrement, and a lost one would leave it high forever.
    ///
    ///         Authenticated BEFORE `tvm.accept()`, like the other two mirrors: this is an entry
    ///         point anyone can address, and accepting first would let a stranger spend the note's
    ///         gas.
    function onInferenceOrderRemoved(uint256 modelHash, uint128 orderId, uint8 cause, uint128 refunded) public {
        require(msg.sender == DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash), ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        // Silent when there is no record: a removal that arrives twice, or for an order this
        // note never recorded, must not revert — the book sends this from inside a match walk,
        // and a revert would bounce into a book that has already moved on.
        uint256 k = tvm.hash(abi.encode(msg.sender, orderId));
        // A REFUSED CANCEL REMOVES NOTHING, and the record must survive it. The book sends this
        // same entry point when it declines a cancel — order not found, or not this owner — and
        // dropping the resting record there would lose an order that is still live in the book.
        if (cause != REMOVED_REJECTED) { delete _restingInf[k]; }
        // The cause and the amount travel together: an owner who sees only "gone" cannot tell a
        // cancel he asked for from an expiry he did not, and cannot reconcile what came back.
        emit InferenceOrderRemoved{dest: address.makeAddrExtern(PRIVATENOTE_INFERENCE_REMOVED, bitCntAddress)}(
            msg.sender, orderId, cause, refunded);
    }

    /// @dev Authenticated before `tvm.accept()` — see `onInferencePlaced`.
    function onInferenceFilled(uint256 modelHash, address tokenContract, uint128 orderId, uint64 clientOrderId, uint128 ticks, uint256 clearingPrice, uint128 spent, bool isBuy) public {
        require(msg.sender == DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash), ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        // A deal began. The ORDER is not cleared here: the book clears it from its own single
        // removal point, and a partial fill leaves it resting on purpose. Touching both here would
        // reintroduce exactly the ordering dependence the two counters exist to avoid.
        _liveDeals[tokenContract] = true;
        // OUTCOME 2 OF 4 — REDUCED BY WHAT WAS SPENT, not deleted.
        //
        // A fill is not necessarily the end of the request. A buy for ten ticks that meets three
        // ticks of liquidity is settled for three and the other seven are still in flight; deleting
        // the record here would call the whole thing resolved on the strength of its first partial
        // match, and the seven ticks of escrow would then be unaccounted for on both sides.
        //
        // Subtracting makes the number mean what it says: not "I do not know how this ended" but
        // "this much is still unresolved". Floored at zero rather than allowed to wrap — a figure
        // that went negative would read as an enormous outstanding amount and lock the note out of
        // withdrawal forever. At zero the key goes, because zero outstanding is no record at all.
        uint128 outstanding = _pendingInf[clientOrderId];
        if (outstanding != 0) {
            uint128 left = spent >= outstanding ? 0 : outstanding - spent;
            if (left == 0) { delete _pendingInf[clientOrderId]; }
            else { _pendingInf[clientOrderId] = left; }
        }
        emit InferenceFilledConfirmed{dest: address.makeAddrExtern(PRIVATENOTE_INFERENCE_FILLED, bitCntAddress)}(
            msg.sender, tokenContract, orderId, ticks, clearingPrice, isBuy);

        // THE BUYER'S BOND, POSTED FROM HERE AND NOWHERE ELSE. This mirror is the first moment the
        // amount can even be computed: it is `2 * clearingPrice`, and the clearing price does not
        // exist until this fill happened. It is also the first moment this note knows the deal's
        // address. So the bond is sent automatically, on the fill, with no client involvement —
        // there is no step for an operator to forget, and no window in which the deal waits for a
        // human.
        //
        // BUY SIDE ONLY. The book sends this same mirror to BOTH notes; the seller's copy carries
        // `isBuy == false` and his bond went in with `fundDeal` long before. Without this test the
        // seller would post a second bond into his own deal on every fill.
        //
        // THE BUYER PAYS FOR HIS OWN ENTRY. The BOND still travels as `amount`, a figure on the
        // call exactly as in `fundDeal` — the ECC attached here is not the bond and the deal does
        // not count it as one (`fundBuyerBond` credits `_balance` from the argument alone). It is
        // exactly what this entry burns, `GAS_FUND_DEAL`, arriving with the call that burns it.
        //
        // `flag: 1`, not 17: flag 16 converts the ECC to native on arrival, which refills the one
        // pocket a deal refills itself (`ensureBalance`) and leaves the pocket that is actually
        // spent untouched. The native `value` is this message's own gas, as before.
        //
        // Sent only when this note really holds it — a note cannot mint ECC[2] either, and a short
        // balance would fail in the action phase rather than at a `require`.
        //
        // `bounce: true`, like `fundDeal`: if the deal refuses the bond, the figure must come home
        // rather than be recorded as spent against a deal that never took it.
        if (isBuy) {
            uint128 bond = uint128(2 * clearingPrice);
            uint128 held = uint128(address(this).currencies[CURRENCIES_ID_SHELL]);
            if (_balance[CURRENCIES_ID_SHELL] >= bond) {
                _balance[CURRENCIES_ID_SHELL] -= bond;

                mapping(uint32 => varuint32) ecc;
                if (held >= DEAL_GAS_FUND) {
                    ecc[CURRENCIES_ID_SHELL] = varuint32(DEAL_GAS_FUND);
                }
                IInferenceDeal(tokenContract).fundBuyerBond{
                    value: 0.05 vmshell, flag: 1, bounce: true, currencies: ecc
                }(bond);
            }
            // NOT ENOUGH FOR THE BOND: nothing is sent, and that is the whole of it. Winding the
            // deal down from here was tried and taken out: this note cannot tell the deal anything
            // it does not already know, and the deal cannot tell a note that CANNOT bond from an
            // owner who simply changed his mind — both arrive from `_buyer` with `_buyerBondFunded`
            // still false. Letting the message through therefore handed every buyer a way to undo
            // a matched deal on demand. `MATCH_OPEN_TIMEOUT` recovers the seller's money instead,
            // and `fundBuyerBondNow` below lets the owner post the bond late, once the note has it.
        }
    }

    /// @notice OUTCOME 3 OF 4 — the book refused the request outright and gave the money back.
    /// @dev    THE FAMILY THAT HAD NO ANSWER. A POST_ONLY that would cross, an FOK that cannot fill
    ///         whole, a deadline already past: the book returns normally, so there is no bounce, and
    ///         no order id was ever assigned, so the refund arrived as an ordinary credit that named
    ///         nothing. This note received money and could not say which request it closed.
    ///
    ///         The credit happens HERE rather than through `creditFromBook`, so the money and its
    ///         explanation cannot arrive separately. `refunded` is trusted the same way every other
    ///         book mirror is trusted — by re-deriving the book from `modelHash` and refusing anyone
    ///         else — and it is bounded by what this note itself committed under that number.
    function onInferenceRejected(uint256 modelHash, uint64 clientOrderId, uint8 reason, uint128 refunded) public {
        require(msg.sender == DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash), ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        // Clamped by the pending record, not taken on faith. The book should never return more than
        // was committed under this number, and if it ever did, the excess would be minted here out
        // of a message — the one thing a record-based balance must never let happen.
        uint128 committed = _pendingInf[clientOrderId];
        uint128 credit = refunded < committed ? refunded : committed;
        if (credit > 0) { _balance[CURRENCIES_ID_SHELL] += credit; }
        delete _pendingInf[clientOrderId];
        emit InferenceOrderRejectedMirror{dest: address.makeAddrExtern(PRIVATENOTE_INFERENCE_REJECTED, bitCntAddress)}(
            msg.sender, clientOrderId, reason, credit);
    }


    // ── Inference market: deploy an InferenceOrderBook FROM this note (§2/§8) ──

    /// @notice Deploy an InferenceOrderBook from this note. The OB code is the
    ///         one baked into this note at deploy (`_inferenceOrderBookCode`) —
    ///         NOT caller-supplied — so the address a note carries is always the canonical
    ///         build. The address derives from that code + the model
    ///         (spec §8), so a given model maps to exactly one book.
    ///         Permissionless: any note holder may (re)deploy the canonical book
    ///         at its deterministic address.
    function deployInferenceOrderBook(uint256 modelHash, string modelName)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        TvmCell stateInit = DexLib.buildInferenceOrderBookStateInit(_inferenceOrderBookCode, modelHash);
        // The book's ctor verifies the deployer is a genuine note (NOTE_CODE_HASH + depositHash)
        // AND that `sha256(modelName) == modelHash` — so the on-chain model name is the genuine
        // `producer--model--version` preimage of the address-defining hash, not a free-text label.
        new InferenceOrderBook{stateInit: stateInit, value: 5 vmshell, flag: 1, bounce: false}(_depositIdentifierHash, modelName);
    }

    /// @notice Deterministic address of the InferenceOrderBook for the given
    ///         the model, using the OB code baked into this note.
    function getInferenceOrderBookAddress(uint256 modelHash)
        external view returns (address)
    {
        return DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
    }

    // ── Inference market: note as active participant (spec §2-4) ────────────
    // Inference settles in SHELL (ECC[2], §1) — held physically by the note
    // (like the PMP network fee), NOT in the RootPN-custodied `_balance`. These
    // methods forward that physical SHELL, so the note itself is the market
    // participant (no external multisig). SHELL id: CURRENCIES_ID_SHELL == 2.

    /// @notice Seller places its resting sell offer in ONE call. Derives the canonical
    ///         per-deal TC for `(this note's ephemeral pubkey, nonce)` from the baked TC
    ///         code hash, then hands it the canonical InferenceOrderBook code hash/depth
    ///         (`_inferenceOrderBookCode`, baked by RootPN) + this note's deposit id +
    ///         order `flags`. The TC verifies the caller is a canonical PrivateNote (its
    ///         pinned note-code hash) and posts its own ask (`msg.sender == TC` at the
    ///         book), so the derived book/TC is canonical and no RootPN round-trip is needed.
    ///         Order params (price/ticks) live in the TC (constructor). Re-run after a
    ///         cancel to re-list; the TC self-guards double-posts.
    /// @param flags Order flags (POST_ONLY / IOC / FOK / MARKET) forwarded to the book.
    /// @param nonce Deal nonce identifying the seller's per-deal TC.
    /// @param ttl Mandatory offer lifetime in seconds, `1 <= ttl <= MAX_SELL_TTL` (spec §2.1.1).
    ///        `ttl == 0` (GTC) is rejected: a SELL commits no collateral, so it MUST auto-expire.
    ///        Validated here and converted to an ABSOLUTE deadline anchored NOW, so any time spent
    ///        reaching the book counts against the offer's life and never extends it past the
    ///        seller's intent. Out of range → revert `ERR_SELL_DEADLINE_TOO_LONG`.
    function postSellOffer(uint8 flags, uint64 nonce, uint64 ttl) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(ttl != 0 && ttl <= MAX_SELL_TTL, ERR_SELL_DEADLINE_TOO_LONG);
        // Same shape rule the buy side enforces at submission: a SUBSCRIPTION is one deal against
        // ONE counterparty for the WHOLE volume, so the flag implies AON. Checked here as well as
        // in the book because the note is the seller's trusted entry and the only path to it —
        // a malformed offer should not spend a queue slot or a bounce round-trip to be refused,
        // and the flags travel from here to `postFromNote` and on to the book untouched.
        require((flags & FLAG_SUBSCRIPTION) == 0 || (flags & FLAG_AON) != 0, ERR_INVALID_PARAMS);
        uint64 deadline = uint64(block.timestamp) + ttl;   // absolute, anchored at the seller's call
        TokenContract(_tokenContractAddr(_ephemeralPubkey, nonce)).postFromNote{value: 1 vmshell, flag: 1, bounce: false}(
            tvm.hash(_inferenceOrderBookCode), _inferenceOrderBookCode.depth(), _depositIdentifierHash, flags, deadline);
    }

    /// @notice Send a BUY order from this note with SHELL escrow (spec §2.3).
    ///         This note is bound as the buyer; on a match the OB forwards the
    ///         cost to the offer's token_contract and binds this note there, so
    ///         the same note can later `streamStop`/`streamDispute` the deal.
    function placeInferenceBuy(
        uint256 modelHash,
        uint128 maxPricePerTick,
        uint128 ticks,
        uint128 escrow,
        uint8   flags,
        uint64  deadline
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        // Limit buy on inference: budget = escrow (held in the OB), ceiling = maxPricePerTick,
        // TIF = deadline (0 = GTC), flags = IOC/FOK/MARKET/POST_ONLY/AON/TEE/SUBSCRIPTION.
        // Owner/buyer = this note (OB uses msg.sender).
        //
        // A SUBSCRIPTION is placed through THIS call, not a separate one: set FLAG_SUBSCRIPTION
        // (which requires FLAG_AON — one seller, the whole volume). The term is always one month,
        // so the order carries no duration; `ticks` only has to divide evenly into its weeks.
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        // Validate the subscription shape HERE, at submission, not only in the book: the note is the
        // buyer's trusted entry and the only path to the book, so a malformed order should never
        // consume a placement slot or a bounce round-trip. The book re-checks the same invariants —
        // it holds the money and must not depend on a caller for them — but a genuine order never
        // reaches that check in a bad shape.
        bool isSub = (flags & FLAG_SUBSCRIPTION) != 0;
        require(!isSub || (flags & FLAG_AON) != 0, ERR_INVALID_PARAMS);
        require(!isSub || ticks % uint128(SUB_WEEKS) == 0, ERR_INVALID_PARAMS);
        // Physical ceiling: nothing produces more than one tick per minute, so a subscription may
        // not buy more volume than its own month could ever deliver.
        require(!isSub || ticks <= uint128(SUB_WEEKS) * SUB_TICKS_PER_WEEK, ERR_INVALID_PARAMS);
        // A subscription posts the buyer's own `2P` beside its escrow — the deal contract holds it
        // apart from the money the weeks are drawn from, so a dispute costs the same in the last
        // week of the term as in the first, and it comes back whole when none is raised. Sized at
        // the limit price, since the deal clears at or below it. A market order cannot carry one:
        // its price exists only once it clears, which is after the escrow is already committed.
        require(!isSub || (flags & FLAG_MARKET) == 0, ERR_INVALID_PARAMS);
        // Fee-free lower bound. The book applies the exact one — it holds the money and cannot
        // depend on a caller for it — and the platform fee lives on that side of the split, so the
        // book's requirement is strictly the stronger of the two. This only catches, at submission,
        // an escrow that could not possibly cover volume plus bond.
        require(!isSub || uint256(escrow) >= uint256(ticks) * uint256(maxPricePerTick)
            + 2 * uint256(maxPricePerTick), ERR_INVALID_PARAMS);
        // The escrow leaves this note's RECORD (generation 4.0.33), where it used to leave its
        // account as physical ECC[2] — note that the old path never touched `_balance` at all,
        // because the currency it sent was a different pot from the custodied figure. Now there is
        // one pot: the buy is paid from `_balance[CURRENCIES_ID_SHELL]` and nothing physical moves.
        require(_balance[CURRENCIES_ID_SHELL] >= escrow, ERR_LOW_VALUE);
        _balance[CURRENCIES_ID_SHELL] -= escrow;

        // A BUY ORDER COSTS WHAT A SELL OFFER COSTS. Posting a sell goes through the deal and burns
        // GAS_POST_FROM_NOTE there; a buy rested in the same book for free, and every charge the
        // buyer's side caused afterwards came out of a reserve the seller had funded. Same amount,
        // taken here, where the order is placed.
        //
        // From this note's OWN ECC[2], not from the recorded balance — that balance is money owed
        // to the owner, gas is not. A note cannot mint ECC[2], so an order costs a note holding
        // none, and that is the intended answer rather than an accident.
        require(uint128(address(this).currencies[CURRENCIES_ID_SHELL]) >= BUY_ORDER_GAS, ERR_LOW_VALUE);
        gosh.burnecc(uint64(BUY_ORDER_GAS), CURRENCIES_ID_SHELL);

        // §3.1.1: forward this note's pubkey so the OB can record it in the deal
        // contract (the gateway authenticates the buyer against it). The escrow figure
        // goes to the derived canonical book, never a caller-supplied address.
        address orderBook = DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
        // bounce:true — if the book rejects the placement (expired / bad flags / insufficient
        // deposit) the subtraction above has to be undone, and the bounce is what says so. Under
        // currency this was automatic, because the ECC physically came back; a figure does not do
        // that by itself, and this is the one behaviour the conversion silently drops if nobody
        // puts it back. `onBounce` does — and it reads the CLIENT NUMBER, not the amount, which is
        // why `clientOrderId` leads `placeBuyOrder`'s parameter list.
        //
        // DO NOT MOVE `escrow` BACK TO THE FRONT. A bounce carries only the leading bits of the
        // body, so whatever restores the record has to be first. The number is 64 bits and the
        // escrow was written under it before the send, so the number alone restores it. Put the
        // uint128 first and `onBounce` would load its HIGH half as the number — zero for any real
        // amount — credit `_pendingInf[0]`, which is empty, and leave the real record standing
        // forever. Money lost and the note wedged, with nothing to see.
        //
        // Deliberately NOT via `_pendingPlaceBuyLock`: that slot belongs to the PMP order path and
        // is only examined under the `_busy` latch, which an inference buy does not take. Reusing
        // it would put the restore behind a gate this call never opens — the money would be gone
        // with no error anywhere — and taking the latch instead would serialise inference buys
        // against PMP activity for no reason. The body carries the figure, so neither is needed.
        // THE NUMBER IS CHOSEN HERE, BEFORE THE SEND, and the record is written in the same breath
        // as the subtraction (task Q). Both halves matter: a number handed out by the book would
        // not exist for the two outcomes that never create an order, and a record written after the
        // send would leave a gap in which the money is subtracted and nothing remembers why.
        uint64 clientOrderId = ++_nextClientOrderId;
        _pendingInf[clientOrderId] = escrow;
        InferenceOrderBook(orderBook).placeBuyOrder{value: 2 vmshell, flag: 1, bounce: true}(
            clientOrderId, escrow, maxPricePerTick, ticks, flags, deadline, _ephemeralPubkey, _depositIdentifierHash);
    }

    // The legacy §8 subscription entry point (resting weekly-cycle bid) was removed with the
    // single-seller redesign: a subscription is now an ORDINARY buy order carrying FLAG_AON plus
    // the SUBSCRIPTION deal-flag, placed through `placeInferenceBuy` — no separate call, no
    // separate registry, no separate deal contract.

    /// @notice Cancel one resting inference order owned by this note (refunds any
    ///         held BUY escrow back to this note).
    function cancelInferenceOrder(uint256 modelHash, uint128 orderId)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        address orderBook = DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
        // `bounce: false` since task P. It was true so that an undeployed book would answer, and
        // the answer was handled by the cancel-bounce branch in `onBounce` — which is gone, because
        // a bounce cannot say WHY and this note was drawing the wrong conclusion from it.
        //
        // Nothing is lost by not hearing back. A record in `_restingInf` is written by the book's
        // own placement mirror, so a book that was never deployed cannot have caused one; there is
        // no state here for a failed cancel to correct. Every outcome that DOES need answering —
        // gone, not yours, queue busy — the book now answers explicitly.
        InferenceOrderBook(orderBook).cancelOrder{value: 1 vmshell, flag: 1, bounce: false}(orderId);
    }

    /// @notice Cancel all resting inference orders owned by this note.
    /// @dev    `bounce: true` here, unlike its single-order neighbour above. Task P removed the
    ///         cancel-bounce BRANCH from `onBounce`, not the bounce flag on this send, and the two
    ///         are easy to read as one. Nothing arrives: a bounce from the book carries the book's
    ///         address as `msg.sender`, and the gate below the inference branches turns away
    ///         anything whose sender is not `_busy` — which only ever holds a PMP address, a
    ///         PMP-orderbook address (derived from a different code cell) or a transfer
    ///         destination. So the flag costs nothing and decides nothing; it is left as it is
    ///         rather than changed, because changing it would move every hash for no behaviour.
    function cancelAllInferenceOrders(uint256 modelHash)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        address orderBook = DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
        // No figure moves here: a bounce returns the value and there is nothing to correct.
        InferenceOrderBook(orderBook).cancelAllOrders{value: 1 vmshell, flag: 1, bounce: true}();
    }

    /// @notice Post the buyer's bond to a matched deal this note could not bond at match time.
    /// @dev    The bond normally goes out by itself, from `onInferenceFilled`, the moment the book
    ///         reports a fill. It does not when the note's recorded balance is short of `2P` right
    ///         then — a deposit still arriving, a figure spent on another order — and the match is
    ///         left waiting: `open` refuses without `_buyerBondFunded`, and after
    ///         `MATCH_OPEN_TIMEOUT` anyone may unwind the deal and hand the seller his money back.
    ///
    ///         This is the way back into that window, and it is the ONLY thing this note may do
    ///         about a bond it missed. Winding the deal down from here was tried and taken out:
    ///         the deal cannot tell a note that CANNOT bond from an owner who changed his mind —
    ///         both arrive from `_buyer` with `_buyerBondFunded` still false — so letting that
    ///         message through handed every buyer a way to undo a matched deal on demand.
    ///
    ///         Conditions are enforced where they belong: the deal refuses a second bond
    ///         (`_buyerBondFunded`), an opened stream (`_opened`), and a sender that is not its
    ///         buyer. Nothing here can talk another note's deal into anything.
    ///
    ///         `bounce: true`, like the automatic path: if the deal refuses, the figure must come
    ///         home rather than be recorded as spent against a bond that was never taken.
    /// @param tokenContract The matched deal, as reported to this note by the book.
    /// @param bond          `2 * clearingPrice` of that match.
    function fundBuyerBondNow(address tokenContract, uint128 bond)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(bond > 0, ERR_INVALID_PARAMS);
        require(_liveDeals.exists(tokenContract), ERR_INVALID_PARAMS);
        require(_balance[CURRENCIES_ID_SHELL] >= bond, ERR_LOW_VALUE);
        _balance[CURRENCIES_ID_SHELL] -= bond;
        mapping(uint32 => varuint32) ecc;
        if (uint128(address(this).currencies[CURRENCIES_ID_SHELL]) >= DEAL_GAS_FUND) {
            ecc[CURRENCIES_ID_SHELL] = varuint32(DEAL_GAS_FUND);
        }
        IInferenceDeal(tokenContract).fundBuyerBond{
            value: 0.05 vmshell, flag: 1, bounce: true, currencies: ecc
        }(bond);
    }


    /// @notice Buyer note stops the stream — amicable exit (spec §4.1). Max loss
    ///         is the two in-flight ticks; refund returns to this note.
    function streamStop(address tokenContract) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        // bounce:true — `tokenContract` is supplied by the caller, so a wrong address would
        // otherwise make this a silent no-op.
        // No figure moves here: a bounce returns the value and there is nothing to correct.
        // The charge rides WITH the call: `stop` burns GAS_TERMINAL, so the ECC that pays
        // for it arrives in the same message under `flag: 1` and is burned in the same
        // transaction. Nothing has to be parked in the deal beforehand, and the buyer stops
        // spending a reserve that only his counterparty can refill.
        mapping(uint32 => varuint32) ecc;
        if (uint128(address(this).currencies[CURRENCIES_ID_SHELL]) >= DEAL_GAS_TERMINAL) {
            ecc[CURRENCIES_ID_SHELL] = varuint32(DEAL_GAS_TERMINAL);
        }
        IInferenceDeal(tokenContract).stop{value: 1 vmshell, flag: 1, bounce: true, currencies: ecc}();
    }

    /// @notice Buyer note disputes the current ≤2 ticks (spec §4.2). Neither note is locked by
    ///         this — no note is locked by a deal at all (§4.3). Every stake a dispute can reach
    ///         lives in the TokenContract: the buyer's escrow and bond, the seller's mirror bond.
    ///         A note is an identity, so one note runs as many deals at once as its owner cares to
    ///         open, and a dispute on one of them leaves the rest untouched.
    function streamDispute(address tokenContract) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        // bounce:true — same caller-supplied address, and here a silent no-op costs the buyer his
        // contest window: he would believe the dispute was filed while it never arrived.
        // No figure moves here: a bounce returns the value and there is nothing to correct.
        // The charge rides WITH the call: `dispute` burns GAS_TERMINAL, so the ECC that pays
        // for it arrives in the same message under `flag: 1` and is burned in the same
        // transaction. Nothing has to be parked in the deal beforehand, and the buyer stops
        // spending a reserve that only his counterparty can refill.
        mapping(uint32 => varuint32) ecc;
        if (uint128(address(this).currencies[CURRENCIES_ID_SHELL]) >= DEAL_GAS_TERMINAL) {
            ecc[CURRENCIES_ID_SHELL] = varuint32(DEAL_GAS_TERMINAL);
        }
        IInferenceDeal(tokenContract).dispute{value: 1 vmshell, flag: 1, bounce: true, currencies: ecc}();
    }

    // `streamReclaim` was removed together with the deal's `reclaimOnTimeout`: there is no longer
    // a prepaid/frozen tick to unwind. A seller who goes silent simply never gets his last claim
    // promoted, so the buyer's ordinary `streamStop` settles what is trusted and refunds the rest.

    /// @notice Buyer note recovers a funded-but-never-opened deal: after
    ///         `MATCH_OPEN_TIMEOUT` from funding with no `open()`, the seller is a
    ///         no-show — `cleanupUnopened` refunds the full deposit and returns the
    ///         seller's seller bond (nothing delivered → no fee/penalty, §2.1).
    ///         The TC gates the
    ///         timer; the deposit/bond (ECC) refund to their fixed notes and the residual native
    ///         gas is swept to the canonical SuperRoot — no caller-chosen payout.
    function streamCleanup(address tokenContract) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        // No figure moves here: a bounce returns the value and there is nothing to correct.
        // The charge rides WITH the call: `cleanupUnopened` burns GAS_TERMINAL, so the ECC that pays
        // for it arrives in the same message under `flag: 1` and is burned in the same
        // transaction. Nothing has to be parked in the deal beforehand, and the buyer stops
        // spending a reserve that only his counterparty can refill.
        mapping(uint32 => varuint32) ecc;
        if (uint128(address(this).currencies[CURRENCIES_ID_SHELL]) >= DEAL_GAS_TERMINAL) {
            ecc[CURRENCIES_ID_SHELL] = varuint32(DEAL_GAS_TERMINAL);
        }
        IInferenceDeal(tokenContract).cleanupUnopened{value: 1 vmshell, flag: 1, bounce: true, currencies: ecc}();
    }

    /// @notice Deterministic per-deal TokenContract address from the RootPN-baked code hash/depth + the
    ///         seller's statics (sellerPubkey, real RootModel, nonce). Single source of truth in DexLib
    ///         (shared with the IOB's placeSellOffer check) — the ONLY address the note-funded helpers ever pay.
    function _tokenContractAddr(uint256 sellerPubkey, uint64 nonce) private view returns (address) {
        (, address tokenContract) = DexLib.computeCanonicalTokenContractAddress(
            _rootModelCodeHash, _rootModelCodeDepth,
            _tokenContractCodeHash, _tokenContractCodeDepth,
            address.makeAddrStd(0, SUPER_ROOT_ADDR), sellerPubkey, nonce);
        return tokenContract;
    }


    /// @notice THE funding entry for a deal (generation 4.0.33): gas and money in one call.
    /// @dev    The two things a deal needs arrive by two different mechanisms in the SAME message,
    ///         which is why this is one function and not two:
    ///
    ///         GAS — `gasShell` rides in `currencies` as physical ECC[2] and flag 17 converts it
    ///         into the target's native balance. A deal cannot mint its own: a mint draws on the
    ///         dapp config of the dapp the contract lives in, and a TokenContract is deployed by an
    ///         external message into its OWN dapp, which has none. So its gas comes from here or
    ///         it comes from nowhere. Flag 17 is 16 | 1 — 16 converts the sent token, 1 pays the
    ///         message fees from this note rather than out of the amount being sent.
    ///
    ///         MONEY — `amount` is a FIGURE, not currency. It is subtracted from this note's
    ///         private `_balance[CURRENCIES_ID_SHELL]` and passed as a call argument; the deal adds
    ///         the same figure to its own record. That is why this CALLS the deal instead of
    ///         transferring to it: a bare transfer arrives with no sender the receiver can check,
    ///         and a figure that anyone could send is a figure anyone could invent. A call has a
    ///         `msg.sender`, and the deal re-derives it — the credit sticks only because the caller
    ///         is provably this note.
    ///
    ///         Conservation is the pair: this note subtracts, the deal adds, and `bounce: true`
    ///         covers the case where it never lands. The target is DERIVED from this note's own key
    ///         plus `nonce`, never supplied by the caller, so neither gas nor money can be aimed at
    ///         an address the seller did not earn — a mistyped nonce addresses an account that does
    ///         not exist and the message bounces rather than settling somewhere unrecoverable.
    /// @param endpointCipher OPTIONAL, carried straight through to the deal. The note is the deal's
    ///        only permitted sender for this call, so this is the one road the endpoint can travel
    ///        without the seller keeping an operational wallet. Nothing here reads it: an opaque
    ///        blob addressed to the buyer, forwarded unchanged.
    function fundDeal(uint64 nonce, uint128 gasShell, uint128 amount, optional(bytes) endpointCipher)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(gasShell > 0 || amount > 0, ERR_INVALID_PARAMS);
        require(_balance[CURRENCIES_ID_SHELL] >= amount, ERR_LOW_VALUE);

        _balance[CURRENCIES_ID_SHELL] -= amount;

        mapping(uint32 => varuint32) ecc;
        if (gasShell > 0) { ecc[CURRENCIES_ID_SHELL] = varuint32(gasShell); }

        IInferenceDeal(_tokenContractAddr(_ephemeralPubkey, nonce))
        // `flag: 1`, not 17. Flag 16 converted `gasShell` to native on arrival, so a seller
        // aiming to fill the deal's ECC[2] reserve filled the one pocket the deal refills
        // itself through `ensureBalance`, while the reserve `_chargeGas` actually burns from
        // did not grow at all. The conversion flags are gone from this generation entirely:
        // note and deal live in the same dapp, nothing crosses a boundary, and ECC[2] has to
        // arrive as ECC[2].
            .fundDeal{value: 1 vmshell, flag: 1, bounce: true, currencies: ecc}(amount, endpointCipher);
    }

    /// @notice A deal pays this note back (generation 4.0.33) — the receiving half of the pair.
    /// @dev    Money comes home the same way it went out: as a figure on a call, never as currency.
    ///         The deal subtracted `amount` from its own `_balance` before sending, and this adds
    ///         the same figure here, so between the two records nothing was created or lost.
    ///
    ///         THE SENDER CHECK IS THE WHOLE THING. `amount` is a number anyone could put on a
    ///         message; what makes it credit is that `msg.sender` re-derives to the canonical
    ///         TokenContract for `(sellerPubkey, nonce)` — an address that exists only if it was
    ///         built from the code hash this note has baked in, at a static pair this note recomputes
    ///         rather than believes. A contract that is not a real deal produces a different address
    ///         and is turned away, so an unknown contract cannot hand this note a figure and have it
    ///         stick. Note the derivation runs on the CALLER'S claimed identity: passing someone
    ///         else's `sellerPubkey`/`nonce` only derives an address that is not the caller.
    ///
    ///         This reverts rather than returning quietly when the caller fails the check, because
    ///         a genuine deal has already taken the figure off its record and its `onBounce` is
    ///         waiting to put it back. A silent return here would destroy the money in the one case
    ///         where the caller was honest and something else was wrong.
    ///         The unnamed `uint8` between `amount` and `sellerPubkey` is the DEAL's earmark tag.
    ///         It rides in the bounced window so a credit that never lands can tell the payer which
    ///         counter to restore; nothing on this side reads it, because money arriving is money
    ///         arriving whatever it was set aside for.
    ///
    ///         NO `_hasWithdrawn` GUARD, deliberately. A note does not get to refuse money. The
    ///         nine inbound callbacks that predate this one — `onOrderCancelled`, `onMergeAccepted`,
    ///         `acceptFee` and the rest — all credit unconditionally, and adding a refusal here
    ///         would have made this the one door in the system that can turn a payment away. If the
    ///         owner withdrew the note and can no longer reach what arrives afterwards, that is the
    ///         consequence of withdrawing, not a reason to destroy the payer's money: a refusal
    ///         does not return the figure, it bounces it back to a contract that already subtracted.
    ///         THE TWO REFUSALS HERE ARE NOT THE SAME KIND, and only one of them is a refusal.
    ///         A zero amount RETURNS: the sender is already proven, and a proven sender that sent
    ///         nothing sent nothing. Reverting would manufacture a bounce with no work to do —
    ///         there is no figure on the other side to give back — so it would be a failure report
    ///         about an event that is not a failure. A bad SENDER reverts, because there the bounce
    ///         is the only thing that saves the money.
    /// @dev THE EARMARK PARAMETER IS GONE. An unnamed `uint8` used to sit second in this list —
    ///      not for anything read here, but because the payer's `onBounce` recovered it from the
    ///      bounced body, and a parameter absent from the signature is absent from the body. The
    ///      deal now pays `bounce: false` to notes that are party to it and keeps no bounce handler,
    ///      so nothing reads that field on either side and carrying it would be cargo.
    ///
    ///      What did NOT change is the only thing this entry point actually does: re-derive the
    ///      caller's canonical address and refuse anyone else. A credit is a number arriving by
    ///      message, so the sender check is the whole of its authenticity.
    function creditFromDeal(uint128 amount, uint256 sellerPubkey, uint64 nonce) public accept {
        ensureBalance();
        if (amount == 0) { return; }
        require(msg.sender == _tokenContractAddr(sellerPubkey, nonce), ERR_INVALID_SENDER);

        _balance[CURRENCIES_ID_SHELL] += amount;

        emit DealCredited{dest: address.makeAddrExtern(PRIVATENOTE_DEAL_CREDITED, bitCntAddress)}(
            msg.sender, amount);
    }

    /// @notice An order book returns escrow to this note (generation 4.0.33) — refund, leftover,
    ///         cancellation, expiry.
    /// @dev    The other side of `placeInferenceBuy`'s subtraction. THE BOOK HOLDS NOTHING: it is a
    ///         matcher and a registry of orders, and what it "returns" is a figure it was recording
    ///         against an order, not currency it was keeping. So a cancellation is two actions in
    ///         two contracts — the book drops the order, this note puts the figure back — where it
    ///         used to be one physical send that could not half-happen.
    ///
    ///         Authentication is by derivation from `modelHash`, one book per model, using the book
    ///         code this note has baked in. Same reasoning as `creditFromDeal` and a DIFFERENT
    ///         derivation, which is why it is a separate entry point: a receiver that let the caller
    ///         pick which check to run would be running no check at all.
    ///
    ///         Reverting rather than returning quietly on a FAILED SENDER CHECK matters here: a real
    ///         book has already let go of the order, and a bounce is the only signal it gets.
    ///
    ///         But the sender check is the ONLY thing that can turn a credit away. Like
    ///         `creditFromDeal`, and like the nine callbacks that predate both, this carries no
    ///         `_hasWithdrawn` guard: a note accepts what it is sent. Anything else would put a
    ///         refusal in the path of a book that no longer holds the escrow it is handing over.
    ///         A zero amount returns rather than reverting, for the same reason as in
    ///         `creditFromDeal`: nothing arrived, so there is nothing to credit and nothing on the
    ///         book's side for a bounce to restore.
    function creditFromBook(uint128 amount, uint256 modelHash) public accept {
        ensureBalance();
        if (amount == 0) { return; }
        require(
            msg.sender == DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash),
            ERR_INVALID_SENDER
        );

        _balance[CURRENCIES_ID_SHELL] += amount;

        emit BookCredited{dest: address.makeAddrExtern(PRIVATENOTE_BOOK_CREDITED, bitCntAddress)}(
            msg.sender, amount);
    }

    /// @notice (note-funded model, step 1a) Owner pre-funds the seller's UNINIT cross-dapp deploy
    ///         targets — the canonical RootModel (per-seller) and/or per-deal TokenContract — with
    ///         SHELL (ECC[2]) straight from this note, so no external operational wallet (one-seed)
    ///         is needed; the seller-signed deploy message then lands on the funded address.
    /// @dev    THIS IS THE DEAL'S ONLY TOP-UP, and it must land as ECC[2], not as native. Since
    ///         4.0.36 the deal is deployed by this note into the DEX dapp, so it mints its own
    ///         native floor in `ensureBalance` and needs no vmshell sent ahead of it. What it
    ///         cannot mint is ECC[2], and ECC[2] is what every entry burns (`_chargeGas`). The
    ///         deploy seeds that reserve once (`deployDeal`, `flag: 1`); without this call there
    ///         would be no second way to add to it, and a deal that outlived its reserve would
    ///         stall on the terminal paths — `stop`, `dispute`, `cleanupUnopened` — that a buyer
    ///         needs. Hence `flag: 1`: it funds the balance in the currency sent. `flag: 16` would
    ///         convert the ECC to native on arrival and top up the one pocket that refills itself.
    ///
    ///         Targets are DERIVED from THIS note's own key (`_ephemeralPubkey`) + `nonce`, never a
    ///         caller-supplied address — SHELL can only ever reach the seller's canonical
    ///         TokenContract (same derivation as the IOB). `bounce:false` because the deal carries
    ///         no `onBounce` and a refusal has nothing to return to. amount 0 = skip.
    /// @dev    ONE LEG NOW, NOT TWO. The `rootModelShell` leg is gone with the reason it existed:
    ///         a RootModel used to be deployed by its owner as an external message, so somebody had
    ///         to put native gas at that address first, and the note was the somebody. The super
    ///         root deploys it now, with an internal `new`, and an internal deploy carries its own
    ///         value — there is nothing left to pre-fund.
    ///
    ///         That change also revived dead code. Deployed externally, a RootModel landed in a dapp
    ///         of its own with no configuration, and `gosh.mintshellq` draws on a dapp's
    ///         configuration — so `RootModel.ensureBalance()` could never do anything, exactly as
    ///         written out for `TokenContract`. Deployed by the super root it lands in the super
    ///         root's dapp, and the same untouched line starts working.
    ///
    ///         `_rootModelAddr` went with it — deriving that address was the only thing this note
    ///         ever needed it for.
    function fundDeployShell(uint64 nonce, uint128 tcShell)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        if (tcShell > 0) {
            mapping(uint32 => varuint32) tcEcc;
            tcEcc[CURRENCIES_ID_SHELL] = varuint32(tcShell);
            _tokenContractAddr(_ephemeralPubkey, nonce).transfer({value: 1 vmshell, bounce: false, flag: 1, currencies: tcEcc});
        }
    }

    /// @notice Deploy this seller's per-deal TokenContract from the note (generation 4.0.36).
    /// @dev    THE DEAL IS BORN INSIDE THE SYSTEM NOW. It used to be deployed off-chain: the seller
    ///         signed an external message carrying the whole contract code, which landed the deal in
    ///         a dapp of its OWN — with no configuration, so it could not mint, so every vmshell it
    ///         would ever spend had to be sent ahead of it (`fundDeployShell` above) and sized by a
    ///         published formula. Undershoot and the deal stopped forever with the escrow inside.
    ///
    ///         Deployed here it lands in THIS NOTE'S dapp, which is configured like everything else,
    ///         so `TokenContract.ensureBalance` works and the deal keeps itself answering. What the
    ///         seller sends now is not a life-support budget but a RESERVE of ECC[2] that each entry
    ///         burns its measured charge from (#950 table) — running it down costs the seller a
    ///         refused call, not a contract that can never be closed.
    ///
    ///         Terms are arguments, not statics: the address derives from
    ///         (sellerPubkey, rootModel, nonce) exactly as before, so every party that computes this
    ///         deal's address from the pinned hashes — the buyer's note, the book, RootPN — still
    ///         finds it. The constructor authenticates THIS note as the canonical one for
    ///         `_depositIdentifierHash`, which is what stops anyone else from occupying the address
    ///         with terms of their own.
    /// @param nonce Deal nonce; with this note's key it fixes the deal address.
    /// @param modelName Canonical `producer--model--version` string; the ctor re-derives its hash.
    /// @param modelHash sha256(modelName), verified on-chain against the name.
    /// @param pricePerTick SHELL per tick (P).
    /// @param maxTicks Upper bound on ticks this deal serves.
    /// @param gasReserve ECC[2] sent with the deploy. What lands on it is NOT decided by whether a
    ///        call is internal or external — it is decided by whether the caller attached anything.
    ///
    ///        ATTACHED, so never on the reserve — the four calls a buyer's note makes:
    ///          fundBuyerBond 0.010 · stop / dispute / cleanupUnopened 0.045 each
    ///
    ///        ON THE RESERVE, because the caller cannot attach:
    ///          the seller's own entries, all `onlyOwnerPubkey(_sellerPubkey)` + `accept`, i.e.
    ///          EXTERNAL messages, which carry no value by construction —
    ///            open 0.015 · acceptProbe 0.015 · claimTokens 0.015 per tick
    ///            close / sellerStop / releaseDispute / destroy 0.045 · withdrawShell 0.005
    ///
    ///        ON THE RESERVE TOO, though internal — nobody attaches on these paths:
    ///          postFromNote 0.025 · fundDeal 0.010 · fundFromOrderBook 0.020 (the order book
    ///          attaches no ECC) · onSellClosed 0.005 · finalize / settleWeek 0.015
    ///          · resolveDisputeTimeout 0.045
    ///
    ///        plus GAS_DEPLOY 0.100, which the constructor takes out of this very reserve.
    ///
    ///        A plain run — deploy, offer, match, open, probe, T claims, close, withdraw — comes to
    ///        about 0.300 + 0.015·T. An earlier figure here said 0.240 and undercounted by 0.070:
    ///        it credited `fundDeal` with topping the reserve up (it sent under flag 17, so the ECC
    ///        converted to native and never reached it) and left out the paths nobody attaches on. Budget a second terminal for the paths that survive their own
    ///        charge and are wound down by a later one: `dispute` -> `releaseDispute` and
    ///        `sellerStop` -> `close`. The older figures (0.215 + 0.013·T, and 0.210 + 0.015·T in
    ///        #950) sized a mechanism that no longer exists, when every entry drew on the reserve.
    ///
    ///        A short reserve is recoverable: `fundDeployShell` tops it up with no limit on repeats.
    function deployDeal(
        uint64 nonce, string modelName, uint256 modelHash,
        uint128 pricePerTick, uint128 maxTicks, uint128 gasReserve
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        (address rootModel, ) = DexLib.computeCanonicalTokenContractAddress(
            _rootModelCodeHash, _rootModelCodeDepth,
            _tokenContractCodeHash, _tokenContractCodeDepth,
            address.makeAddrStd(0, SUPER_ROOT_ADDR), _ephemeralPubkey, nonce);
        TvmCell stateInit = DexLib.buildTokenContractStateInit(
            _tokenContractCode, _ephemeralPubkey, rootModel, nonce);
        mapping(uint32 => varuint32) reserve;
        reserve[CURRENCIES_ID_SHELL] = varuint32(gasReserve);
        // `bounce: false` for the same reason every other deploy here says it: there is no account
        // at the target yet, so a bounce has nothing to come back from, and the deal's own refusal
        // paths are what report a bad deploy.
        new TokenContract{
            stateInit: stateInit, value: 1 vmshell, flag: 1, bounce: false, currencies: reserve
        }(modelName, modelHash, pricePerTick, maxTicks, address(this), _depositIdentifierHash);
    }

    // The seller bond no longer has a funding call of its own. It was `postSellerBond`, and it did
    // what `fundDeal` above does — send the deal SHELL from this note against a derived address —
    // only by attaching currency instead of passing a figure. With money held as a record there is
    // one way in, so there is one function: the bond is just an `amount`, and what makes it a bond
    // is what the DEAL does with it on arrival, not which door it came through.

    /// @notice Deploys a new PMP contract for a prediction market event.
    /// @dev This function deterministically computes oracle addresses and
    ///      OracleEventList contracts, prepares the required currency balances
    ///      for oracle and network fees, and deploys a new PMP instance.
    ///
    ///      Deployment flow:
    ///      1. Validate input array lengths.
    ///      2. Compute oracle addresses from `names`.
    ///      3. Compute OracleEventList addresses using oracle code and indexes.
    ///      4. Aggregate oracle fees and include network fee.
    ///      5. Build PMP StateInit and compute deterministic PMP address.
    ///      6. Emit `PMPDeployed` event.
    ///      7. Deploy PMP contract with required currencies.
    ///
    /// @param eventId Unique identifier of the PMP event.
    /// @param oracleFee Array of additional fees (in shell tokens) for each oracle.
    ///                  Must match the length of `names` and `index`.
    /// @param tokenType Token type used by the PMP contract.
    /// @param names Array of oracle names used to compute oracle addresses.
    /// @param index Array of oracle indexes used to compute OracleEventList addresses.
    /// @param initialStakes Initial clean stakes for each outcome submitted with deployment.
    ///
    /// @custom:requirements
    /// - Caller must be authorized by the owner ephemeral public key.
    /// - `names.length == oracleFee.length == index.length`.
    ///
    /// @custom:effects
    /// - Emits `PMPDeployed` with the computed PMP address and oracle configuration.
    /// - Deploys a new PMP contract at a deterministic address.
    ///
    /// @custom:reverts
    /// - If input array lengths mismatch.
    /// - If caller is not the owner.
    function deployPMP(uint256 eventId, uint128[] oracleFee, uint32 tokenType, string[] names, uint128[] index, uint128[] initialStakes) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        uint256 length = names.length;
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        // Empty `names` means no OracleEventList is asked to confirm the PMP.
        // PMP.constructor would set `_numberOfOracleEvents = 0`, no confirmEvent
        // dispatches, and no approveEvent / onInitialStakesAccepted / onInitialStakesFailed
        // ever fires — so require at least one name, keeping PN out of a permanent
        // `_busy` state with its initial stakes held in candidateAmount.
        // Cap outcome/list counts at ingress (< 20, matching the canonical OracleEventList's
        // outcomeCount cap). Every per-outcome loop in the _busy-clearing callbacks
        // (onInitialStakesAccepted / onSplitAccepted / onMergeAccepted / onBounce) iterates this
        // count; an uncapped oversized market would gas-exhaust the only path that clears _busy and
        // brick the note. Bounding it here also keeps PMP.approveEvent's mismatch-refund loop small.
        require(length > 0 && length < 20, ERR_INVALID_PARAMS);
        require(length == oracleFee.length, ERR_INVALID_PARAMS);
        require(length == index.length, ERR_INVALID_PARAMS);
        require(initialStakes.length > 0 && initialStakes.length < 20, ERR_INVALID_PARAMS);
        require(_debt == 0, ERR_DEBT_NON_ZERO);

        // Validate initial stakes and compute total
        uint128 initialLot = lotSize(tokenType);
        uint128 initialTotal = 0;
        for (uint32 i = 0; i < initialStakes.length; i++) {
            require(initialStakes[i] >= minStakeValue(tokenType), ERR_LOW_VALUE);
            require(initialStakes[i] % initialLot == 0, ERR_AMOUNT_NOT_LOT_MULTIPLE);
            initialTotal += initialStakes[i];
        }
        require(_balance[tokenType] >= initialTotal, ERR_LOW_VALUE);
        require(!_busy.hasValue(), ERR_NOTE_BUSY);

        ensureBalance();

        mapping(uint256 => bool) forOracleHash;

        // Include shell tokens for network fee
        uint128 sumFee = 0;
        address[] oracleEventLists;
        for (uint32 i = 0; i < length; i++) {
            sumFee += oracleFee[i];
            oracleEventLists.push(DexLib.computeOracleEventListAddressFromHash(
                _oracleEventListCodeHash, _oracleEventListCodeDepth,
                DexLib.computeOracleAddressFromHash(_oracleCodeHash, _oracleCodeDepth, names[i]),
                index[i]
            ));
            // Names must be unique: `oracleListHash` is a hash of the name-set,
            // so duplicates collapse there and desync `_numberOfOracleEvents`
            // from the distinct-oracle count the contract actually tracks.
            uint256 nameHash = tvm.hash(names[i]);
            require(!forOracleHash.exists(nameHash), ERR_INVALID_PARAMS);
            forOracleHash[nameHash] = true;
        }
        mapping(uint32 => varuint32) dataCur;
        dataCur[CURRENCIES_ID_SHELL] = sumFee + NETWORK_FEE_AMOUNT;

        uint256 oracleListHash = tvm.hash(abi.encode(forOracleHash));
        TvmCell stateInit = DexLib.buildPMPStateInit(_privateNoteCode, _pmpCode, eventId, oracleListHash, tokenType);
        address pmpAddress = DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, oracleListHash, tokenType);

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        // Re-deploy guard. `onInitialStakesAccepted` clears `_busy` after the
        // FIRST oracle approval (so refunds can still authenticate by PMP
        // address), which opens a window where the `!_busy` check above no
        // longer blocks a second deployPMP for the SAME event. Without this, the
        // second call would OVERWRITE the already-committed stake record (zeroing
        // `stake.amount`) and re-debit `_balance`; the duplicate PMP-create then
        // bounces and only the second debit is refunded — silently destroying the
        // first, already-confirmed stake (funds gone from the note, still held by
        // the PMP). A genuinely failed first deploy `delete`s the record, so a
        // legitimate retry still passes.
        require(!_stakes.exists(hash), ERR_STAKE_EXISTS);

        // Deduct initial stakes from balance and set pending stake record
        _balance[tokenType] -= initialTotal;
        // candidateAmount = initialTotal signals pending initial stake
        _stakes[hash] = StakeInfo({
            amount: new uint128[](initialStakes.length),
            debtAmount: new uint128[](initialStakes.length),
            couponsAmount: new uint128[](initialStakes.length),
            candidateAmount: initialTotal,
            candidateOutcome: 0,
            candidateBetType: BET_TYPE_CLEAN,
            oracleListHash: oracleListHash,
            tokenType: tokenType
        });
        _busy = pmpAddress;
        _lastHash = hash;

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_PMP_DEPLOYED, bitCntAddress);
        emit PMPDeployed{dest: addrExtern}(eventId, tokenType, pmpAddress, oracleEventLists, oracleFee);

        new PMP{
            stateInit: stateInit,
            value: 50 vmshell,
            currencies: dataCur,
            flag: 1,
            bounce: true
        }(_depositIdentifierHash, tokenType, oracleEventLists, oracleFee, initialStakes, _orderBookCode);
    }

    /// @notice Called by PMP after initial stakes (passed with deployPMP) are accepted
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param amounts Per-outcome initial stake amounts confirmed by PMP
    function onInitialStakesAccepted(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint128[] amounts)
        public senderIs(_busy.get()) accept
    {
        ensureBalance();
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        StakeInfo stake = _stakes[hash];
        for (uint32 i = 0; i < amounts.length; i++) {
            stake.amount[i] = amounts[i];
        }
        stake.candidateAmount = 0;
        _stakes[hash] = stake;
        delete _busy;
    }

    /// @notice Called by PMP when initial stakes are invalid (outcome count mismatch → PMP cancelled)
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param refundTotal Total amount to refund to balance
    function onInitialStakesFailed(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint128 refundTotal)
        public senderIs(DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, oracleListHash, tokenType)) accept
    {
        // Auth via deterministic PMP address (not `_busy`): after the first
        // oracle approval `_busy` is cleared by `onInitialStakesAccepted`,
        // so a later refund originating from `rejectEvent` / `onBounce`
        // can still authenticate. Only clear `_busy` if it actually points
        // to the sender — the user may have moved on to another PMP.
        ensureBalance();
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        // If the user pre-emptively deleteStake'd the record, the refund is
        // forfeited. Otherwise deleteStake → generateCoupon → rejectEvent
        // would let them pocket both a coupon (issued under empty `_stakes`)
        // and the returned principal. Burn balance here = pay for misuse.
        if (_stakes.exists(hash)) {
            _balance[tokenType] += refundTotal;
            delete _stakes[hash];
        }
        if (_busy.hasValue() && _busy.get() == msg.sender) {
            delete _busy;
        }
    }

    /// @notice PMP normalization-refund callback (creator-only).
    /// @dev Sent by PMP at freeze time when each clean pool is reduced to a
    ///      multiple of `min(_initialStakes)`. Decrements the creator's
    ///      stake.amount[k] by the refunded outcome-token amounts (so the
    ///      creator can no longer claim/sell/merge tokens already returned
    ///      as collateral) and credits `refundTotal` back to `_balance`.
    /// @param eventId PMP event ID (used to recompute PMP address for sender check)
    /// @param oracleListHash Oracle list hash for PMP address derivation
    /// @param tokenType Token type managed by this PMP
    /// @param refundAmounts Per-outcome refunded clean-token amounts
    /// @param refundTotal Total collateral credited back to _balance
    function onPmpCleanRefund(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128[] refundAmounts,
        uint128 refundTotal
    ) public senderIs(DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, oracleListHash, tokenType)) accept {
        ensureBalance();
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        // A pre-emptive deleteStake removes the record: skip the credit and the
        // stake decrement (the normalization refund is forfeited, matching
        // onInitialStakesFailed above). The acknowledgement below is sent
        // unconditionally so the PMP always clears `_normRefundPending` and
        // re-enables split/merge, even when the record is gone.
        if (_stakes.exists(hash)) {
            StakeInfo stake = _stakes[hash];
            uint128 applied = 0;
            for (uint32 k = 0; k < uint32(refundAmounts.length) && k < uint32(stake.amount.length); k++) {
                uint128 r = refundAmounts[k];
                if (r == 0) continue;
                // `r` is `cleanPool[k] % min(_initialStakes)`, so it is strictly
                // below every component of the accepted initial stake and the
                // holding covers it. Clamped rather than required: the
                // acknowledgement at the tail has to leave in every case, and a
                // revert here would take it with it — leaving `_normRefundPending`
                // set on the PMP with nothing left able to clear it.
                if (r > stake.amount[k]) { r = stake.amount[k]; }
                stake.amount[k] -= r;
                applied += r;
            }
            _stakes[hash] = stake;
            // ONE normalization point for the refund. Two figures reach this callback and either
            // can be the smaller: `refundTotal`, which the PMP may itself have clamped to its
            // remaining unclaimed balance without rewriting the per-outcome array, and `applied`,
            // the tokens this note actually gave up. Crediting `refundTotal` outright would pay
            // collateral for tokens still held; crediting `applied` outright would pay more than
            // the PMP removed from its own ledger. The lower of the two is the only figure both
            // sides agree on, so the stake decrements and the balance credit can never diverge.
            _balance[tokenType] += applied < refundTotal ? applied : refundTotal;
        }
        // Tell PMP we processed the refund so it can clear `_normRefundPending`
        // and re-enable split/merge. Sent as a follow-up internal so this
        // callback's storage mutations commit before PMP's check.
        PMP(msg.sender).confirmRefundReceived{
            value: 0.05 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_depositIdentifierHash);
    }

    /// @notice Abandons a stake. The user forfeits any claim against the
    ///         PMP. Before deleting the local stake record, PN notifies
    ///         PMP via `forfeitStake(...)` so PMP can decrement its
    ///         `_totalWinPool` by this PN's win-outcome contribution.
    ///         Without that notification PMP would be unable to reach
    ///         the `_totalWinPool == 0` condition that triggers
    ///         `selfdestruct(_deployer)` (every winning stake must be
    ///         either claimed or forfeited).
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of Oracles
    /// @param tokenType Token type
    function deleteStake(uint256 eventId, uint256 oracleListHash, uint32 tokenType) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        require(_stakes.exists(hash), ERR_STAKE_NOT_EXISTS);
        // Mirror the cancel/claim path: any resting orders against this
        // PMP must be cleared first, otherwise a late `onOrderCancelled`
        // callback would find no `_stakes[hash]` and silently drop the
        // released outcome tokens.
        require(_openOrdersByEvent[hash] == 0, ERR_OPEN_ORDERS_EXIST);

        StakeInfo stake = _stakes[hash];
        address pmpAddress = DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, stake.oracleListHash, tokenType);
        _busy = pmpAddress;
        _lastHash = hash;
        _pendingForfeit = true;

        PMP(pmpAddress).forfeitStake{
            value: 0.1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(stake.amount, stake.debtAmount, stake.couponsAmount, _depositIdentifierHash);
    }

    /// @notice PMP→PN callback acknowledging `forfeitStake`. Deletes the
    ///         local stake record and clears the busy lock.
    function onForfeitAccepted() public senderIs(_busy.get()) accept {
        ensureBalance();
        // THE OWNER'S SIDE OF WALKING AWAY FROM MONEY, and the moment it becomes final: the stake
        // record is about to be deleted here and there is nothing to undo it. PMP emits the
        // authoritative record with the amounts; this mirrors it in the owner-facing stream, the
        // same pairing a cancellation already has (book emits `OrderCancelled`, note mirrors
        // `OrderCancelledConfirmed`). A forfeit is the more consequential of the two and was the
        // only one nobody announced.
        emit StakeForfeitConfirmed{dest: address.makeAddrExtern(PRIVATENOTE_STAKE_FORFEITED, bitCntAddress)}(
            msg.sender, _lastHash);
        delete _stakes[_lastHash];
        _pendingForfeit = false;
        delete _busy;
        _busyOpNonce = 0;
    }

    /// @notice Cancels a stake on a PMP contract
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of Oracles
    /// @param tokenType Token type
    function cancelStake(uint256 eventId, uint256 oracleListHash, uint32 tokenType) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        require(_stakes.exists(hash), ERR_STAKE_NOT_EXISTS);
        // Race fix (mirrors claim): PMP gates cancelStake on `_orderBookDone`
        // (OB finished SENDING cancel callbacks), but a late OB→PN
        // `onOrderCancelled` from a resting SELL can still be in flight when
        // PMP.onStakeCancelled lands and deletes _stakes[hash] (see line 560).
        // Without this counter, the late cancel-callback would silently drop
        // the returned outcome tokens.
        require(_openOrdersByEvent[hash] == 0, ERR_OPEN_ORDERS_EXIST);
        address pmpAddress = DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, _stakes[hash].oracleListHash, tokenType);
        _busy = pmpAddress;
        _lastHash = hash;
        PMP(pmpAddress).cancelStake{
            value: 0.1 vmshell, 
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_stakes[hash].amount, _stakes[hash].debtAmount, _stakes[hash].couponsAmount,_depositIdentifierHash);
    }

    /// @notice Called by PMP after stake is cancelled
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of Oracles
    /// @param tokenType Token type
    /// @param value Amount refunded
    /// @param couponValue Coupon amount refunded
    function onStakeCancelled(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint128 value, uint128 couponValue) 
        public senderIs(_busy.get()) accept
    {
        ensureBalance();
        
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        delete _stakes[hash];
        _balance[tokenType] += value;
        if (couponValue > 0) {
            if (_couponsValue == 0) { _couponsTokenType = tokenType; }
            _couponsValue += couponValue;
        }
        
        address addrExtern = address.makeAddrExtern(PRIVATENOTE_STAKE_CANCELLED, bitCntAddress);
        emit StakeCancelled{dest: addrExtern}(_busy.get(), value);
        delete _busy;
    }



    // ===== Split/Merge Functions =====

    /// @notice Splits collateral into proportional outcome tokens via PMP.
    /// @dev Deducts collateral from balance immediately. PMP computes
    ///      outcome token amounts and calls back onSplitAccepted.
    ///      Requires PMP pools to be frozen (after stakeEnd).
    ///
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param collateral Amount of collateral to split
    function splitFullSet(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128 collateral
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(collateral > 0, ERR_LOW_VALUE);
        // No lotSize check here: the minimal mintable basket (derived from
        // per-outcome unit sizes `u_k`) can be a large prime that exceeds
        // lotSize. Gating split by lotSize would make such baskets undivisible
        // and block the downstream OrderBook flow. lotSize stays enforced on
        // stake placement and order placement paths.
        require(_balance[tokenType] >= collateral, ERR_LOW_VALUE);
        require(_debt == 0, ERR_DEBT_NON_ZERO);

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);

        _balance[tokenType] -= collateral;

        address pmpAddress = DexLib.computePMPAddress(
            _privateNoteCode,
            _pmpCode,
            eventId,
            oracleListHash,
            tokenType
        );

        _busy = pmpAddress;
        _lastHash = hash;

        StakeInfo stake = _stakes[hash];
        stake.candidateAmount = collateral;
        stake.candidateBetType = BET_TYPE_CLEAN;
        stake.oracleListHash = oracleListHash;
        stake.tokenType = tokenType;
        _stakes[hash] = stake;

        PMP(pmpAddress).splitFullSet{
            value: 0.1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(collateral, _depositIdentifierHash);
    }

    /// @notice Called by PMP after split is accepted.
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param amounts Outcome token amounts received from split
    function onSplitAccepted(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128[] amounts,
        uint128 collateralUsed
    ) public senderIs(_busy.get()) accept {
        ensureBalance();

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);

        StakeInfo stake = _stakes[hash];
        // Initialize arrays if this is the first split (no prior staking)
        if (stake.amount.length == 0) {
            stake.amount = new uint128[](amounts.length);
            stake.debtAmount = new uint128[](amounts.length);
            stake.couponsAmount = new uint128[](amounts.length);
        }
        for (uint32 i = 0; i < amounts.length; i++) {
            stake.amount[i] += amounts[i];
        }
        // Refund unused collateral (F - F_use) back to free balance.
        if (stake.candidateAmount > collateralUsed) {
            _balance[tokenType] += stake.candidateAmount - collateralUsed;
        }
        stake.candidateAmount = 0;
        _stakes[hash] = stake;

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_SPLIT_CONFIRMED, bitCntAddress);
        emit FullSetStakeConfirmed{dest: addrExtern}(_busy.get(), amounts);

        delete _busy;
    }

    /// @notice Merges proportional outcome tokens back into collateral via PMP.
    /// @dev Sends outcome token amounts to PMP for merge. PMP verifies
    ///      proportionality, checks solvency, and calls back onMergeAccepted.
    ///
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param amount Array of outcome token amounts to merge
    function mergeFullSet(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128[] amount
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(amount.length > 0, ERR_INVALID_PARAMS);
        require(_debt == 0, ERR_DEBT_NON_ZERO);


        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        require(_stakes.exists(hash), ERR_STAKE_NOT_EXISTS);

        StakeInfo stake = _stakes[hash];
        require(amount.length == stake.amount.length, ERR_INVALID_PARAMS);

        // Verify PN has enough outcome tokens to merge.
        // No lotSize check on per-outcome amounts: the basket unit sizes `u_k`
        // can be large primes that aren't lot-multiples. Gating merge by
        // lotSize would block users from returning partial baskets minted via
        // split. lotSize stays enforced on stake / order placement paths.
        uint128 total = 0;
        for (uint32 i = 0; i < amount.length; i++) {
            require(stake.amount[i] >= amount[i], ERR_LOW_VALUE);
            total += amount[i];
        }

        stake.candidateAmount = total;
        stake.candidateBetType = BET_TYPE_MERGE;
        _stakes[hash] = stake;

        address pmpAddress = DexLib.computePMPAddress(
            _privateNoteCode,
            _pmpCode,
            eventId,
            oracleListHash,
            tokenType
        );

        _busy = pmpAddress;
        _lastHash = hash;

        PMP(pmpAddress).mergeFullSet{
            value: 0.1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(amount, _depositIdentifierHash);
    }

    /// @notice Called by PMP after merge is accepted.
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param collateral Collateral amount returned from merge
    /// @param amounts Per-outcome token amounts that were merged
    function onMergeAccepted(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128 collateral,
        uint128[] amounts
    ) public senderIs(_busy.get()) accept {
        ensureBalance();

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);

        StakeInfo stake = _stakes[hash];
        bool isEmpty = true;
        for (uint32 i = 0; i < amounts.length; i++) {
            stake.amount[i] -= amounts[i];
            if ((stake.amount[i] > 0) || (stake.debtAmount[i] > 0) || (stake.couponsAmount[i] > 0)) {
                isEmpty = false;
            }
        }
        stake.candidateAmount = 0;

        if (isEmpty) {
            // Race fix: if any orders for this event are still resting on OB,
            // keep a zero-stake record so a late onOrderCancelled callback for
            // a resting SELL can add the returned outcome tokens back to
            // stake.amount[outcomeId]. User can re-merge or place new orders.
            // MM-friendly: partial merges never delete; only a true full-exit
            // with no open orders trims the storage entry.
            if (_openOrdersByEvent[hash] > 0) {
                _stakes[hash] = stake;
            } else {
                delete _stakes[hash];
            }
        } else {
            _stakes[hash] = stake;
        }

        _balance[tokenType] += collateral;

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_MERGE_CONFIRMED, bitCntAddress);
        emit FullSetStakeCancelled{dest: addrExtern}(_busy.get(), collateral);

        delete _busy;
    }


    /// @notice Places a stake on a specific outcome in PMP.
    ///
    /// @dev Deducts funds from wallet balance or coupons immediately.
    ///      The stake is first stored as candidate and finalized only
    ///      after `onStakeAccepted` callback from PMP.
    ///
    /// @param eventId PMP event identifier.
    /// @param oracleListHash Hash of oracle configuration.
    /// @param tokenType Token type.
    /// @param outcome Outcome index.
    /// @param amount Stake amount.
    /// @param useCoupon Whether to use coupon for this stake (if true, amount will be taken from available coupons instead of balance)
    ///
    /// Requirements:
    /// - Wallet must not be busy.
    /// - Amount must be greater than zero.
    /// - Sufficient balance or coupons must exist.
    function setStake(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint32 outcome, uint128 amount, bool useCoupon)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(amount >= minStakeValue(tokenType), ERR_LOW_VALUE);
        require(amount % lotSize(tokenType) == 0, ERR_AMOUNT_NOT_LOT_MULTIPLE);
        if (useCoupon) {
            require(_couponsValue >= amount, ERR_LOW_VALUE);
            require(tokenType == _couponsTokenType, ERR_INVALID_TOKEN_TYPE);
        } else {
            require(_balance[tokenType] >= amount, ERR_LOW_VALUE);
        }
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        address pmpAddress = DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, oracleListHash, tokenType);
        uint8 betType = _debt > 0 && _debtTokenType == tokenType ? 1 : 0;
        betType = useCoupon ? 2 : betType;

        if (_stakes.exists(hash)) {
            StakeInfo stake = _stakes[hash];
            require(stake.candidateAmount == 0, ERR_STAKE_NOT_APPROVED);
            stake.candidateAmount = amount;
            stake.candidateOutcome = outcome;
            stake.candidateBetType = betType;
            _stakes[hash] = stake;
        } else {
            _stakes[hash] = StakeInfo({
                amount: new uint128[](0),
                debtAmount: new uint128[](0),
                couponsAmount: new uint128[](0),
                candidateAmount: amount,
                candidateOutcome: outcome,
                candidateBetType: betType,
                oracleListHash: oracleListHash,
                tokenType: tokenType
            });
        }
        
        _busy = pmpAddress;
        _lastHash = hash;
        if (useCoupon) {
            _couponsValue -= amount;
        } else {
            _balance[tokenType] -= amount;
        }
        PMP(pmpAddress).acceptStake{
            value: 0.1 vmshell, 
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(outcome, amount, _depositIdentifierHash, betType);
    }

    /// @notice Called by PMP after stake is accepted
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of Oracles
    /// @param tokenType Token type
    /// @param outcomeCount Number of outcomes configured in PMP for this event.
    /// @param betType 0 - clean bet, 1 - debt bet, 2 - coupon bet
    function onStakeAccepted(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint128 outcomeCount, uint8 betType) 
        public senderIs(_busy.get()) 
    {
        tvm.accept();
        ensureBalance();
        
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        StakeInfo stake = _stakes[hash];
        uint128 amount = stake.candidateAmount;

        // Initialize all arrays on first stake (regardless of type)
        // This ensures claim() always has properly sized arrays to pass to PMP
        if (stake.amount.length == 0) {
            stake.amount = new uint128[](outcomeCount);
            stake.debtAmount = new uint128[](outcomeCount);
            stake.couponsAmount = new uint128[](outcomeCount);
        }

        if (betType == BET_TYPE_COUPON) {
            stake.couponsAmount[stake.candidateOutcome] += amount;
        } else if (betType == BET_TYPE_DEBT) {
            stake.debtAmount[stake.candidateOutcome] += amount;
        } else {
            stake.amount[stake.candidateOutcome] += amount;
        }
        stake.candidateAmount = 0;
        _stakes[hash] = stake;
        
        address addrExtern = address.makeAddrExtern(PRIVATENOTE_STAKE_CONFIRMED, bitCntAddress);
        emit StakeConfirmed{dest: addrExtern}(_busy.get(), stake.candidateOutcome, amount, betType);
        delete _busy;
    }

    /// @notice Claims winnings from PMP
    /// @dev Sends claim request to PMP. Wallet enters busy state
    ///      until `onClaimAccepted` callback is received.
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    function claim(uint256 eventId, uint256 oracleListHash, uint32 tokenType) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        require(_stakes.exists(hash), ERR_STAKE_NOT_EXISTS);
        StakeInfo stake = _stakes[hash];

        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        require(stake.candidateAmount == 0, ERR_STAKE_NOT_APPROVED);
        // Race fix: OB→PMP `onOrderBookShutdownComplete` (sets _orderBookDone)
        // may land before OB→PN `onOrderCancelled` callbacks from the same
        // shutdown round. Without this gate the user could claim early and
        // `delete _stakes[hash]`; the late cancel-callback for any resting
        // SELL would then silently drop the returned outcome tokens (see
        // `onOrderCancelled` sell-branch). Per-event counter so claim on
        // PMP_A is not blocked by unrelated open orders on PMP_B.
        require(_openOrdersByEvent[hash] == 0, ERR_OPEN_ORDERS_EXIST);

        ensureBalance();
        
        address pmpaddress = DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, stake.oracleListHash, stake.tokenType);
        _busy = pmpaddress;
        _lastHash = hash;
        
        PMP(pmpaddress).claim{
            value: 0.1 vmshell, 
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(stake.amount, stake.debtAmount, stake.couponsAmount, _depositIdentifierHash);
    }

    /// @notice Called by PMP after claim is processed
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of Oracles
    /// @param tokenType Token type for payout and debt accounting.
    /// @param outcome Optional outcome (if resolved)
    /// @param payoutClean Payout amount for clean bets
    /// @param payoutDebt Debt payout amount
    /// @param payoutCoupon Coupon payout amount
    /// @param debtPaid Amount of debt repaid from this payout (formula 17)
    function onClaimAccepted(uint256 eventId, uint256 oracleListHash, uint32 tokenType, optional(uint32) outcome, uint128 payoutClean, uint128 payoutDebt, uint128 payoutCoupon, uint128 debtPaid)
        public senderIs(_busy.get()) 
    {        
        tvm.accept();
        ensureBalance();
        
        if (!outcome.hasValue()) {
            delete _busy;
            return;
        } 
        
        _balance[tokenType] += payoutClean + payoutDebt + payoutCoupon;

        // Formula 10: Increase debt from coupon profit
        if (payoutCoupon > 0) {
            _debt += payoutCoupon;
        }

        // Formula 18: Decrease debt by debtPaid
        if (_debt > debtPaid) {
            _debt -= debtPaid;
        } else {
            _debt = 0;
        }
        address addrExtern = address.makeAddrExtern(PRIVATENOTE_CLAIM_ACCEPTED, bitCntAddress);
        emit ClaimAccepted{dest: addrExtern}(_busy.get(), outcome, payoutClean + payoutDebt + payoutCoupon);
        
        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);
        delete _stakes[hash];
        delete _busy;
    }

    /// @notice Accepts creator fee transfer from PMP and credits local token balance.
    /// @param fee Creator fee amount transferred from PMP.
    /// @param tokenType Token type in which the fee is credited.
    /// @param eventId Event identifier of the source PMP.
    /// @param oracleListHash Oracle set hash of the source PMP.
    function acceptFee(uint128 fee, uint32 tokenType, uint256 eventId, uint256 oracleListHash) public senderIs(DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, oracleListHash, tokenType)) accept {
        ensureBalance();
        // A withdrawn note is finalized — ignore tokens arriving from a PMP
        // close (residual / creator fee) so they can't re-credit a drained note.
        if (_hasWithdrawn) {
            return;
        }
        _balance[tokenType] += fee;
    }

    /// @notice Returns fixed coupon nominal for a token type.
    /// @param tokenType Token type used for coupon issuance.
    /// @return couponValue Coupon nominal value for the given token type (0 if unsupported).
    function getCouponValue(uint32 tokenType) private pure returns (uint128) {
        if (tokenType == CURRENCIES_ID_SHELL) {
            return SHELL_COUPON_VALUE;
        } else if (tokenType == CURRENCIES_ID) {
            return NACKL_COUPON_VALUE;
        } else if (tokenType == CURRENCIES_ID_USDC) {
            return USDC_COUPON_VALUE;
        } else {
            return 0;
        }
    }

    /// @notice Generates a free coupon for the specified token type
    /// @param tokenType Token type for which to generate coupon
    /// @dev Can only generate coupon when:
    ///      - the token type has a coupon nominal (`getCouponValue > 0`)
    ///      - all token balances < minStakeValue (i.e. too small to stake)
    ///      - debt == 0
    ///      - No withdrawals from this wallet have been performed.
    ///      - No active stakes exist.
    ///      - No coupon currently exists.
    function generateCoupon(uint32 tokenType) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        // Only the three configured token types carry a nominal. Without this
        // gate an unsupported type would set `_couponsTokenType` while leaving
        // `_couponsValue` at zero, so the call does nothing and the coupon
        // slot still reads as free.
        uint128 nominal = getCouponValue(tokenType);
        require(nominal > 0, ERR_INVALID_TOKEN_TYPE);
        require(_debt == 0, ERR_HAS_DEBT);
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(!_hasTransferred, ERR_INVALID_STATE);
        require(_stakes.empty(), ERR_NOTE_BUSY);
        for ((uint32 tt, uint128 bal) : _balance) {
            require(bal < minStakeValue(tt), ERR_NON_ZERO_BALANCE);
        }
        // Reject if collateral is parked in the OrderBook layer — otherwise a
        // user with a resting buy could pass the `bal < minStakeValue` gate
        // (lock pulled balance below the threshold), mint a coupon, then
        // cancel the order to recover the original balance ⇒ free coupon.
        for ((uint32 tt, uint128 locked) : _lockedInOrders) {
            tt;
            require(locked == 0, ERR_NON_ZERO_BALANCE);
        }
        require(_pendingPlaceBuyLock == 0, ERR_NON_ZERO_BALANCE);
        require(_pendingBatchBuyLock == 0, ERR_NON_ZERO_BALANCE);
        // Belt-and-braces: even if the locked-balance gates above pass,
        // sell-side resting orders don't show up in `_lockedInOrders` (they
        // lock outcome tokens, not collateral) and a stake with all-zero
        // amounts could in principle remain after orders drained it. The
        // explicit counter is the canonical "no orders open" check.
        require(_openOrderCount == 0, ERR_OPEN_ORDERS_EXIST);
        // INFERENCE HAS ITS OWN TWO, and they are asked separately because a match does two things
        // at once: it takes an order out of the book and starts a deal. One quantity would make the
        // answer depend on which of those two messages arrived first; two make the order of arrival
        // irrelevant. A partial fill leaves the order resting AND starts a deal, which falls out of
        // the separation rather than being a case anyone has to handle.
        //
        // The maps answer about their own emptiness — no counter is kept beside them, because a
        // count next to a map is a second source of truth that drifts silently and is exactly what
        // a gate like this would then be reading.
        //
        // `ERR_OPEN_ORDERS_EXIST` rather than new codes: the condition it already names on the line
        // above is the same one — this note still has business outstanding in a book — and a second
        // number for the same meaning is how an exit code stops telling you anything.
        require(_restingInf.empty(), ERR_OPEN_ORDERS_EXIST);
        // AND NOTHING IN FLIGHT (task Q). Between the debit and the book's answer the escrow is in
        // neither place: subtracted here, not yet recorded there. Withdrawing in that window empties
        // a balance that is already short by the escrow, and the answer — whichever of the four it
        // is — then credits a note that has latched `_hasWithdrawn` and can never pay it out.
        require(_pendingInf.empty(), ERR_OPEN_ORDERS_EXIST);
        require(_liveDeals.empty(), ERR_OPEN_ORDERS_EXIST);
        require(_couponsValue == 0, ERR_COUPON_ALREADY_EXISTS);
        _couponsValue = nominal;
        _couponsTokenType = tokenType;

        uint128 baseDebt = _couponsValue * 5 / 100;
        _debt = baseDebt;
        _debtTokenType = tokenType;
    }

    /// @notice Receives funds to the wallet
    receive() external {
        tvm.accept();
        ensureBalance();
    }

    /// @notice Handles bounced messages from PMP contracts and remote PrivateNotes.
    /// @dev Distinguishes between a transfer bounce (_pendingTransferAmount > 0)
    ///      and a PMP operation bounce (candidateAmount in _stakes[_lastHash]).
    /// @param body Bounced message body (kept for ABI compatibility; not decoded).
    onBounce(TvmSlice body) external {
        tvm.accept();
        ensureBalance();
        // --- Inference-buy bounce (generation 4.0.33): the book refused the placement.
        //
        // Handled BEFORE the `_busy` gate below, and this ordering is the whole point. Everything
        // past that gate is a PMP-side operation that takes the busy latch; an inference buy takes
        // no latch, so it would be turned away at the gate and the escrow — already subtracted from
        // `_balance` — would simply cease to exist. Under currency this case needed no code at all,
        // because the ECC came back on its own. That is the shape of what the conversion to figures
        // takes away: not a check that fails, a behaviour that silently stops happening.
        //
        // OUTCOME 4 OF 4 — the message never reached the book. Several buys can be in flight to the
        // same book at once, so the answer has to say WHICH one came back; the client number does,
        // and it leads `placeBuyOrder`'s parameter list for exactly this reason.
        //
        // Budget: function id (32) + `clientOrderId` (64) = 96 bits, well inside the 256 a bounce
        // carries. The escrow itself no longer has to travel — it was written under this number
        // before the send, so the number alone restores it. That is also what keeps the four
        // outcomes symmetrical: every one of them names the request the same way.
        //
        // A bounce is marked as such by the node, so this cannot be forged by a contract that
        // simply calls in.
        uint32 bouncedFn = body.load(uint32);
        if (bouncedFn == abi.functionId(InferenceOrderBook.placeBuyOrder)) {
            uint64 bouncedClientId = body.load(uint64);
            uint128 escrow = _pendingInf[bouncedClientId];
            delete _pendingInf[bouncedClientId];
            _balance[CURRENCIES_ID_SHELL] += escrow;
            return;
        }
        // --- Deal-funding bounce (generation 4.0.33): same reasoning, other counterparty.
        //
        // `fundDeal` subtracts the figure here and asks the deal to add it there. A deal that is
        // not deployed yet, was destroyed, or refuses the bond (wrong size, already bonded) leaves
        // the subtraction standing on its own. The GAS half of that call needs no undoing — ECC
        // sent under flag 17 comes back the way currency always did — so it is only ever the figure
        // that has to be put back by hand.

        // NO CANCEL-BOUNCE BRANCH — it used to sit here and is gone (task P). The book no longer
        // bounces a cancel: a full queue answers with `CANCEL_REJ_QUEUE_FULL` and leaves the order
        // resting, and the two rejections that DO mean the order is gone send the removal mirror
        // themselves. A bounce carries no reason, so this branch had to guess one — and it guessed
        // "gone", which was right for a missing order and exactly wrong for a busy queue: it
        // dropped the record of a LIVE order, and that record is what refuses a withdrawal.
        //
        // The decision moved to where the data is. Nothing here has to infer it any more.

        // --- Touch bounce (task E): the deal is gone, or it was never this note's.
        //
        // THIS IS THE ONLY OUTCOME THE TOUCH HAS. A live deal that belongs here answers by not
        // bouncing at all, so arriving in this branch already means the record is stale — whether
        // because the contract no longer exists or because it refused a stranger. Both are the same
        // fact from this side: stop waiting for it.
        //
        // Cleared by `msg.sender`, not from the body: the bounce comes back FROM the deal, so its
        // address is the key and nothing needs decoding. No money is restored, exactly as with a
        // cancelled order — this says the deal is over, not that anything is owed.
        if (bouncedFn == abi.functionId(IInferenceDeal.touchDeal)) {
            if (_liveDeals.exists(msg.sender)) {
                delete _liveDeals[msg.sender];
                emit InferenceDealClosed{dest: address.makeAddrExtern(PRIVATENOTE_INFERENCE_DEAL_CLOSED, bitCntAddress)}(msg.sender);
            }
            return;
        }
        if (bouncedFn == abi.functionId(IInferenceDeal.fundDeal)) {
            uint128 amount = body.load(uint128);
            _balance[CURRENCIES_ID_SHELL] += amount;
            return;
        }
        // Same shape, same reason: the buyer's bond leaves as a FIGURE (`_balance` is decremented
        // before the send), and a bounce returns the attached value but not the record. Without
        // this the note would stay short by `2 * clearing` on every refused bond — and the send is
        // automatic on each buy fill, so a refusal is an ordinary path, not an edge.
        if (bouncedFn == abi.functionId(IInferenceDeal.fundBuyerBond)) {
            uint128 amount = body.load(uint128);
            _balance[CURRENCIES_ID_SHELL] += amount;
            return;
        }
        if (!_busy.hasValue() || msg.sender != _busy.get()) {
            return;
        }

        // --- Forfeit bounce: PMP was unreachable (already self-destructed
        //     by an earlier claim) when `deleteStake` dispatched. The
        //     stake is moot — clean up locally.
        if (_pendingForfeit) {
            // ANNOUNCED WITH THE AMOUNTS, TAKEN BEFORE THE DELETE. This is the one place a stake
            // record vanishes with nothing happening on the PMP side — the PMP is gone, so there
            // is no authoritative record to pair with and this event is the only trace there will
            // ever be. What makes it worth having is the amounts: "a stake was dropped" is barely
            // better than silence, while "this much was dropped" is a fact somebody can check.
            //
            // Mitigating and not exculpating: a PMP self-destructs after settlement, so the claim
            // really is empty by now. But from outside, "there was a stake" and "there was nothing"
            // look identical — which is to say, they look like nothing at all.
            // THE THREE CONFIRMED ARRAYS, not `candidateAmount`. That field is the PENDING stake —
            // one outcome, not yet accepted — and on a settled record it is normally zero. Naming
            // it here would have made this event report nothing in the one place where nothing else
            // will ever be reported, which is precisely the failure the event was added to fix.
            // These are the same three `deleteStake` sends to the PMP and the same three PMP emits
            // in `StakeForfeited`, so the two records describe one quantity in one vocabulary.
            StakeInfo dropped = _stakes[_lastHash];
            emit StakeDroppedLocally{dest: address.makeAddrExtern(PRIVATENOTE_STAKE_DROPPED, bitCntAddress)}(
                _lastHash, dropped.tokenType, dropped.amount, dropped.debtAmount, dropped.couponsAmount);
            delete _stakes[_lastHash];
            _pendingForfeit = false;
            delete _busy;
            _busyOpNonce = 0;
            return;
        }

        // --- Batch bounce: executePlaceBatch/executeCancelBatch/executeCancelAll ---
        if (_pendingBatchActive) {
            // Restore pre-batch balance and stake amounts.
            if (_pendingBatchBuyLock > 0) {
                _balance[_pendingBatchTokenType] += _pendingBatchBuyLock;
                if (_lockedInOrders[_pendingBatchTokenType] >= _pendingBatchBuyLock) {
                    _lockedInOrders[_pendingBatchTokenType] -= _pendingBatchBuyLock;
                } else {
                    _lockedInOrders[_pendingBatchTokenType] = 0;
                }
            }
            if (_pendingBatchSells.length > 0 && _stakes.exists(_pendingBatchStakeHash)) {
                StakeInfo s = _stakes[_pendingBatchStakeHash];
                for (uint32 i = 0; i < uint32(_pendingBatchSells.length); i++) {
                    PendingBatchSell ps = _pendingBatchSells[i];
                    s.amount[ps.outcomeId] += ps.amount;
                }
                _stakes[_pendingBatchStakeHash] = s;
            }
            // Release cid sentinels reserved by the bounced batch so they can be reused.
            for (uint32 i = 0; i < uint32(_pendingBatchClientOrderIds.length); i++) {
                delete _clientOrderIds[_pendingBatchClientOrderIds[i]];
            }
            _pendingBatchActive = false;
            _pendingBatchBuyLock = 0;
            _pendingBatchTokenType = 0;
            _pendingBatchStakeHash = 0;
            delete _pendingBatchSells;
            delete _pendingBatchClientOrderIds;
            delete _busy;
            _busyOpNonce = 0;
            return;
        }

        // --- Single-order placeOrder buy bounce ---
        // placeOrder buy locks BOTH _balance and _lockedInOrders, so the
        // bounce must release BOTH. (Previously this path overloaded
        // _pendingTransferAmount and indiscriminately mutated
        // _lockedInOrders on every bounce, which corrupted state on a
        // bounced offerTransfer — fixed by splitting into a dedicated
        // _pendingPlaceBuyLock slot.)
        if (_pendingPlaceBuyLock > 0) {
            _balance[_pendingPlaceBuyTokenType] += _pendingPlaceBuyLock;
            if (_lockedInOrders[_pendingPlaceBuyTokenType] >= _pendingPlaceBuyLock) {
                _lockedInOrders[_pendingPlaceBuyTokenType] -= _pendingPlaceBuyLock;
            } else {
                _lockedInOrders[_pendingPlaceBuyTokenType] = 0;
            }
            // Release cid sentinel reserved by the bounced single-order place.
            if (_pendingPlaceClientOrderId != 0) {
                delete _clientOrderIds[_pendingPlaceClientOrderId];
                _pendingPlaceClientOrderId = 0;
            }
            _pendingPlaceBuyLock = 0;
            _pendingPlaceBuyTokenType = 0;
            delete _busy;
            _busyOpNonce = 0;
            return;
        }

        // --- initTransfer (offerTransfer) bounce ---
        // Restore _balance only. initTransfer never mutates _lockedInOrders
        // (transfers move user-owned tokens between PNs, not order
        // collateral), so onBounce must not touch it either.
        //
        // AND IT MUST NOT RESTORE THE COINS EITHER, now that `initTransfer` sends ECC alongside the
        // record. A bounced message returns its `currencies` to the sender by itself — that is what
        // currency does and the reason the figure needed hand-written restoration in the first
        // place. Crediting them here as well would hand the note its coins twice, which is the one
        // failure worth looking for on this path. The line below touches `_balance` and nothing
        // else, deliberately.
        if (_pendingTransferAmount > 0) {
            _balance[_pendingTransferTokenType] += _pendingTransferAmount;
            _pendingTransferAmount = 0;
            delete _busy;
            return;
        }

        // --- PMP / OB-sell bounce: acceptStake / acceptFullSetStake / cancelStake / placeOrder(sell) ---
        delete _busy;
        _busyOpNonce = 0;
        StakeInfo stake = _stakes[_lastHash];

        // Return funds to proper balance based on bet type
        if (stake.candidateBetType == BET_TYPE_COUPON) {
            if (stake.candidateAmount > 0) {
                if (_couponsValue == 0) { _couponsTokenType = stake.tokenType; }
                _couponsValue += stake.candidateAmount;
            }
        } else if (stake.candidateBetType == BET_TYPE_OB_SELL) {
            // Sell order bounce: return outcome tokens to stake
            stake.amount[stake.candidateOutcome] += stake.candidateAmount;
            // Release cid sentinel reserved by the bounced single-order sell.
            if (_pendingPlaceClientOrderId != 0) {
                delete _clientOrderIds[_pendingPlaceClientOrderId];
                _pendingPlaceClientOrderId = 0;
            }
        } else if (stake.candidateBetType == BET_TYPE_MERGE) {
            // Merge bounce: nothing was deducted from _balance, outcome tokens
            // are still in stake.amount — just clear candidate, no restoration needed.
        } else {
            _balance[stake.tokenType] += stake.candidateAmount;
        }

        stake.candidateAmount = 0;
        _stakes[_lastHash] = stake;

        // Delete stake record if no confirmed amounts remain
        // (covers both: regular stake with no history, and full-set
        //  stake that bounced on the very first attempt)
        bool allEmpty = true;
        for (uint32 i = 0; i < stake.amount.length; i++) {
            if (stake.amount[i] > 0) { allEmpty = false; break; }
        }
        for (uint32 i = 0; i < stake.debtAmount.length; i++) {
            if (stake.debtAmount[i] > 0) { allEmpty = false; break; }
        }
        for (uint32 i = 0; i < stake.couponsAmount.length; i++) {
            if (stake.couponsAmount[i] > 0) { allEmpty = false; break; }
        }
        if (allEmpty) {
            delete _stakes[_lastHash];
        }
    }

    // ── PrivateNote-to-PrivateNote transfer ──────────────────────────────────────

    /// @notice Initiates a token transfer to another PrivateNote.
    /// @dev Destination address is derived deterministically from destDepositHash.
    ///      Deducts amount from balance immediately and sets _busy = dest.
    ///      The receiving PrivateNote credits the tokens automatically.
    ///      If offerTransfer bounces, onBounce restores the balance.
    /// @param destDepositHash _depositIdentifierHash of the destination PrivateNote
    /// @param tokenType Token type to transfer
    /// @param amount Amount to transfer (must be >= minStakeValue)
    /// @param eccAmount Physical ECC of `tokenType` to send along with the record. Named rather
    ///        than "the whole pocket" because the owner decides how much — a named figure can say
    ///        both "all of it" and "part of it", and "all" can say only one of those. Zero is
    ///        allowed and means the record moves alone, which is what this call did before.
    function initTransfer(uint256 destDepositHash, uint32 tokenType, uint128 amount, uint128 eccAmount)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        require(_stakes.empty(), ERR_NOTE_BUSY);
        require(amount >= minStakeValue(tokenType), ERR_LOW_VALUE);
        require(_balance[tokenType] >= amount, ERR_LOW_VALUE);
        require(_debt == 0, ERR_DEBT_NON_ZERO);
        require(_couponsValue == 0, ERR_COUPON_ACTIVE);
        require(destDepositHash != _depositIdentifierHash, ERR_INVALID_PARAMS);
        // Same in-flight order-book gate as withdrawTokens: transferring value out
        // while an OB order is live lets that order fill afterwards and credit a
        // stake, leaving the note unable to withdraw the remainder normally (the
        // withdraw path is blocked by the new stake). Both value-extraction paths
        // must wait for all OB activity to drain.
        for ((uint32 tt, uint128 locked) : _lockedInOrders) {
            tt;
            require(locked == 0, ERR_NON_ZERO_BALANCE);
        }
        require(_pendingPlaceBuyLock == 0, ERR_NON_ZERO_BALANCE);
        require(_pendingBatchBuyLock == 0, ERR_NON_ZERO_BALANCE);
        require(_openOrderCount == 0, ERR_OPEN_ORDERS_EXIST);
        // INFERENCE HAS ITS OWN TWO, and they are asked separately because a match does two things
        // at once: it takes an order out of the book and starts a deal. One quantity would make the
        // answer depend on which of those two messages arrived first; two make the order of arrival
        // irrelevant. A partial fill leaves the order resting AND starts a deal, which falls out of
        // the separation rather than being a case anyone has to handle.
        //
        // The maps answer about their own emptiness — no counter is kept beside them, because a
        // count next to a map is a second source of truth that drifts silently and is exactly what
        // a gate like this would then be reading.
        //
        // `ERR_OPEN_ORDERS_EXIST` rather than new codes: the condition it already names on the line
        // above is the same one — this note still has business outstanding in a book — and a second
        // number for the same meaning is how an exit code stops telling you anything.
        require(_restingInf.empty(), ERR_OPEN_ORDERS_EXIST);
        // AND NOTHING IN FLIGHT (task Q). Between the debit and the book's answer the escrow is in
        // neither place: subtracted here, not yet recorded there. Withdrawing in that window empties
        // a balance that is already short by the escrow, and the answer — whichever of the four it
        // is — then credits a note that has latched `_hasWithdrawn` and can never pay it out.
        require(_pendingInf.empty(), ERR_OPEN_ORDERS_EXIST);
        require(_liveDeals.empty(), ERR_OPEN_ORDERS_EXIST);

        address dest = DexLib.computePrivateNoteAddress(_privateNoteCode, destDepositHash);

        _balance[tokenType] -= amount;
        _pendingTransferAmount = amount;
        _pendingTransferTokenType = tokenType;
        _hasTransferred = true;
        _busy = dest;

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_TRANSFER_INITIATED, bitCntAddress);
        emit TransferInitiated{dest: addrExtern}(dest, tokenType, amount);

        // THE COINS TRAVEL WITH THE RECORD. Until now this message carried no `currencies` at all:
        // the figure moved and the physical ECC[2] stayed behind, so a note could hand its whole
        // balance away and keep the currency that balance was backed by.
        //
        // `eccAmount` is NAMED rather than "whatever is in the pocket", and the reason is that the
        // owner decides how much — a named amount expresses both "all of it" and "some of it",
        // while "all" can express only one. It is NOT a safety reserve: emptying the pocket
        // entirely is an ordinary thing to do. A note mints its own gas (`ensureBalance`, called at
        // the head of `onTransferAccepted` itself), and it lives in a dapp that has a config to
        // mint from — unlike a deal, whose identical-looking three lines do nothing. Reasoning from
        // the deal's constraint to the note's is a mistake this comment exists to prevent.
        require(uint128(address(this).currencies[tokenType]) >= eccAmount, ERR_LOW_VALUE);
        mapping(uint32 => varuint32) cc;
        if (eccAmount > 0) { cc[tokenType] = varuint32(eccAmount); }

        // `bounce: true` was already here and now matters more: on a refusal — `offerTransfer` is
        // the one receiver allowed to say no, when `_hasWithdrawn` — the COINS come back on their
        // own, because that is what currency does. `onBounce` must therefore restore the RECORD and
        // nothing else; restoring the coins too would credit them twice.
        PrivateNote(dest).offerTransfer{value: 0.1 vmshell, flag: 1, bounce: true, currencies: cc, dest_dapp_id: ROOT_PN_DAPP_ID}(
            tokenType, amount, _depositIdentifierHash
        );
    }

    /// @notice Called by a remote PrivateNote to deliver a transfer.
    /// @dev Verifies the sender is a valid PrivateNote via deterministic address derivation.
    ///      Credits tokens immediately and notifies the sender.
    /// @param tokenType Token type being transferred
    /// @param amount Amount being transferred
    /// @param senderDepositHash _depositIdentifierHash of the sending PrivateNote
    function offerTransfer(uint32 tokenType, uint128 amount, uint256 senderDepositHash) public accept {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(
            msg.sender == DexLib.computePrivateNoteAddress(_privateNoteCode, senderDepositHash),
            ERR_INVALID_SENDER
        );
        _balance[tokenType] += amount;

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_TRANSFER_CONFIRMED, bitCntAddress);
        emit TransferReceived{dest: addrExtern}(msg.sender, tokenType, amount);

        PrivateNote(msg.sender).onTransferAccepted{value: 0.1 vmshell, flag: 1, bounce: false, dest_dapp_id: ROOT_PN_DAPP_ID}();
    }

    /// @notice Called by the receiving PrivateNote after crediting the transfer.
    /// @dev Clears busy state once the receiver has credited the transfer. This is the
    ///      only path that clears `_busy` on success; the failure path is the offerTransfer
    ///      bounce (onBounce). While `_busy` holds, no second transfer can start, so the
    ///      pending amount always matches the single in-flight transfer's own bounce.
    ///      Sent with bounce: false; ensureBalance keeps the note funded so it lands.
    function onTransferAccepted() public senderIs(_busy.get()) accept {
        ensureBalance();
        _pendingTransferAmount = 0;
        delete _busy;
    }

    // ── Coupon management ─────────────────────────────────────────────────────────

    /// @notice Discards the current coupon without using it.
    /// @dev Allowed only when coupon exists and no active stakes are pending.
    ///      The debt created at coupon issuance remains and must be repaid normally.
    function discardCoupon() public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(_couponsValue > 0, ERR_NO_COUPON_AVAILABLE);
        // Only an UNTOUCHED coupon may be discarded: the outstanding value has to still equal the
        // nominal its type is issued at. Staking spends `_couponsValue` down, so anything below
        // the nominal means the coupon has been put to work and the debt it created is owed.
        require(_couponsValue == getCouponValue(_couponsTokenType), ERR_INVALID_STATE);
        // Discarding an unused coupon takes its debt with it. The debt exists because the coupon
        // does; leaving it behind would strand the note, since `_debt == 0` gates almost every
        // path including the only exit. Subtracted rather than zeroed so debt from any other
        // source is untouched — an unused coupon can only account for its own 5%.
        uint128 baseDebt = _couponsValue * 5 / 100;
        _debt = _debt > baseDebt ? _debt - baseDebt : 0;
        if (_debt == 0) { _debtTokenType = 0; }
        _couponsValue = 0;
        _couponsTokenType = 0;
    }

    /// @notice Withdraws tokens to a specified wallet.
    /// @dev Inner action flag is hard-coded to 1 inside RootPN — accepting a
    ///      caller-supplied flag opens TVM flag 128 (CARRY_ALL_BALANCE) and 32
    ///      (DELETE_IF_EMPTY) paths that would otherwise reach RootPN's own balance.
    /// @param destWalletAddr Destination wallet address
    /// @param dapp_id DApp id forwarded to RootPN.withdrawTokens (surfaced in the TokensWithdrawn event).
    function withdrawTokens(address destWalletAddr, uint256 dapp_id) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        require(_stakes.empty(), ERR_NOTE_BUSY);
        require(_debt == 0, ERR_DEBT_NON_ZERO);
        // Mirror generateCoupon's OB gate: in-flight order-book activity must
        // be fully drained before withdrawing. Otherwise a buy order placed
        // pre-withdraw could fill afterwards and credit outcome tokens to a
        // stake the user can no longer claim (`_hasWithdrawn` blocks claim).
        for ((uint32 tt, uint128 locked) : _lockedInOrders) {
            tt;
            require(locked == 0, ERR_NON_ZERO_BALANCE);
        }
        require(_pendingPlaceBuyLock == 0, ERR_NON_ZERO_BALANCE);
        require(_pendingBatchBuyLock == 0, ERR_NON_ZERO_BALANCE);
        require(_openOrderCount == 0, ERR_OPEN_ORDERS_EXIST);
        // INFERENCE HAS ITS OWN TWO, and they are asked separately because a match does two things
        // at once: it takes an order out of the book and starts a deal. One quantity would make the
        // answer depend on which of those two messages arrived first; two make the order of arrival
        // irrelevant. A partial fill leaves the order resting AND starts a deal, which falls out of
        // the separation rather than being a case anyone has to handle.
        //
        // The maps answer about their own emptiness — no counter is kept beside them, because a
        // count next to a map is a second source of truth that drifts silently and is exactly what
        // a gate like this would then be reading.
        //
        // `ERR_OPEN_ORDERS_EXIST` rather than new codes: the condition it already names on the line
        // above is the same one — this note still has business outstanding in a book — and a second
        // number for the same meaning is how an exit code stops telling you anything.
        require(_restingInf.empty(), ERR_OPEN_ORDERS_EXIST);
        // AND NOTHING IN FLIGHT (task Q). Between the debit and the book's answer the escrow is in
        // neither place: subtracted here, not yet recorded there. Withdrawing in that window empties
        // a balance that is already short by the escrow, and the answer — whichever of the four it
        // is — then credits a note that has latched `_hasWithdrawn` and can never pay it out.
        require(_pendingInf.empty(), ERR_OPEN_ORDERS_EXIST);
        require(_liveDeals.empty(), ERR_OPEN_ORDERS_EXIST);
        // Drain the note's PHYSICAL ECC pool too, and note that this is no longer the same money
        // as the inference escrow: since 4.0.33 an inference buy is paid out of `_balance`, so
        // releasing `_balance` already releases the trading side. What is left on the ACCOUNT is
        // the coin pocket — what `fundDeal` converts to a deal's gas under flag 17 — and leaving it
        // behind would strand real value on a note that can never be withdrawn again. Attach that
        // physical SHELL to this message; RootPN forwards it straight through to `destWalletAddr`
        // (and returns it on the revert path). After this the note holds zero ECC[2] and is dead.
        mapping(uint32 => varuint32) physCc;
        uint128 physShell = uint128(address(this).currencies[CURRENCIES_ID_SHELL]);
        if (physShell > 0) { physCc[CURRENCIES_ID_SHELL] = varuint32(physShell); }
        // Withdraw the ENTIRE balance — pass the full per-token-type map so
        // RootPN moves every currency the note holds in one transfer.
        RootPN(ROOT_PN_ADDRESS).withdrawTokens{value: 0.1 vmshell, bounce: false, flag: 1, currencies: physCc, dest_dapp_id: ROOT_PN_DAPP_ID}(_balance, destWalletAddr, _depositIdentifierHash, dapp_id);
        delete _balance;
        _hasWithdrawn = true;
	}

    /// @notice Sweep physical ECC[2] that reached the note AFTER it was withdrawn.
    ///
    /// @dev    `withdrawTokens` runs once and empties the note, but inference SHELL can still
    ///         arrive afterwards: an inference order cancelled out of the book refunds to the owner
    ///         note, a deal contract refunds the buyer's deposit or returns the seller's bond to the
    ///         note that posted it, and none of those paths ask whether the note has been withdrawn
    ///         — nor should they, since the money is owed either way. Without this the SHELL would
    ///         sit on a note that can never withdraw again.
    ///
    ///         Deliberately narrow. It moves ONLY physical ECC[2] and never touches `_balance`,
    ///         which `withdrawTokens` already released and which stays empty. The withdrawn note
    ///         cannot take on new obligations either — `placeInferenceBuy`, `postSellOffer` and
    ///         `deployInferenceOrderBook` all refuse once `_hasWithdrawn` — so this only ever
    ///         collects the tail of commitments made before the withdrawal.
    function sweepShell(address destWalletAddr, uint256 dapp_id)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        require(_hasWithdrawn, ERR_INVALID_STATE);
        uint128 physShell = uint128(address(this).currencies[CURRENCIES_ID_SHELL]);
        require(physShell > 0, ERR_NON_ZERO_BALANCE);
        mapping(uint32 => varuint32) physCc;
        physCc[CURRENCIES_ID_SHELL] = varuint32(physShell);
        destWalletAddr.transfer({value: 0.1 vmshell, bounce: false, flag: 1,
                                 currencies: physCc, dest_dapp_id: dapp_id});
    }

    /// @notice Reverts a withdraw operation (called by Vault)
    /// @param amounts Per-token-type amounts to restore to the note balance
    function revertWithdraw(mapping(uint32 => uint128) amounts) public senderIs(ROOT_PN_ADDRESS) accept {
        ensureBalance();
        for ((uint32 tt, uint128 amt) : amounts) {
            _balance[tt] += amt;
        }
        // Clear the withdrawn latch: RootPN.withdrawTokens only calls this
        // path when the withdraw did NOT happen (insufficient RootPN
        // liquidity). Without the reset the PN stays permanently pinned
        // in the `_hasWithdrawn=true` state, which blocks setStake,
        // split/merge, claim, generateCoupon, initTransfer, batch OB ops,
        // etc. — functionally the wallet is frozen despite having a
        // restored balance.
        _hasWithdrawn = false;
    }



    // ===== Order Book Functions =====

    /// @notice Places a limit order on the order book for a specific outcome.
    /// @dev For sell orders: locks outcome tokens (reduces stake.amount[outcomeId]).
    ///      For buy orders: locks collateral from balance.
    ///      Sets _busy until onOrderPlaced callback.
    ///
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param outcomeId Outcome to trade
    /// @param isBuy True for buy, false for sell
    /// @param price Limit price in basis points (10000 bps = 1 collateral; no upper bound; ignored for market orders)
    /// @param amount Amount of outcome tokens to trade
    /// @param flags Order flags: IOC=0x01, FOK=0x02, MARKET=0x04
    /// @param minAmount Minimum fill amount (0 = no minimum)
    /// @param epochId Epoch identifier used by dark order book matching.
    function placeOrder(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint32 outcomeId,
        bool isBuy,
        uint256 price,
        uint128 amount,
        uint8 flags,
        uint128 minAmount,
        uint64 epochId,
        uint128 clientOrderId
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        require(amount > 0, ERR_LOW_VALUE);
        require(_debt == 0, ERR_DEBT_NON_ZERO);

        // clientOrderId uniqueness (cid==0 = "not set", bypasses both gate and map).
        // Reserve the slot now with a sentinel; replaced with real orderId in
        // onOrderPlaced or cleared in onOrderRejected / bounce.
        if (clientOrderId != 0) {
            require(!_clientOrderIds.exists(clientOrderId), ERR_INVALID_PARAMS);
            _clientOrderIds[clientOrderId] = type(uint128).max;
            _pendingPlaceClientOrderId = clientOrderId;
        }

        // Minimum order notional (value in quote currency) + tick size on price.
        uint128 minNotional = minOrderNotional(tokenType);
        if (flags & 0x04 != 0) {
            // Market buy: amount is quote/collateral, not base. Base-lot
            // quantisation and minAmount do not apply.
            require(minAmount == 0, ERR_INVALID_PARAMS);
            if (isBuy) {
                require(amount >= minNotional, ERR_ORDER_TOO_SMALL);
            }
        } else {
            // Limit order: amount is in base (outcome-tokens) → lot quantisation.
            require(amount % lotSize(tokenType) == 0, ERR_AMOUNT_NOT_LOT_MULTIPLE);
            require(price % TICK_SIZE == 0, ERR_PRICE_NOT_TICK_MULTIPLE);
            require(price == 0 || uint256(amount) <= type(uint256).max / uint256(price), ERR_NOTIONAL_OVERFLOW);
            uint256 notionalFull = (uint256(amount) * uint256(price)) / uint256(FULL_PERCENT);
            require(notionalFull <= uint256(type(uint128).max), ERR_NOTIONAL_OVERFLOW);
            require(notionalFull >= uint256(minNotional), ERR_ORDER_TOO_SMALL);
        }

        {
            address addrExtern = address.makeAddrExtern(PRIVATENOTE_ORDER_SUBMITTED, bitCntAddress);
            emit OrderSubmitted{dest: addrExtern}(clientOrderId, outcomeId, isBuy, price, amount, flags, eventId, tokenType);
        }

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);

        if (isBuy) {
            // Buy: lock collateral + max fee reserve from balance.
            // The feeReserve is recomputed by OrderBook and sent back in onOrderPlaced.
            uint128 cost;
            if (flags & 0x04 != 0) {
                cost = amount; // market buy: lock full balance as collateral
            } else {
                cost = uint128((uint256(amount) * uint256(price)) / uint256(FULL_PERCENT));
            }
            uint128 maxFee = uint128(
                (uint256(cost) * uint256(TAKER_FEE_RATE)) / uint256(FEE_DENOMINATOR)
            );
            require(_balance[tokenType] >= cost + maxFee, ERR_LOW_VALUE);
            _balance[tokenType] -= (cost + maxFee);
            _lockedInOrders[tokenType] += (cost + maxFee);
            // Track locked collateral + fee reserve so onBounce can restore
            // BOTH _balance and _lockedInOrders. Separate slot from
            // _pendingTransferAmount (which belongs to initTransfer's
            // bounce path that does NOT touch _lockedInOrders).
            _pendingPlaceBuyLock = cost + maxFee;
            _pendingPlaceBuyTokenType = tokenType;
        } else {
            // Sell: lock outcome tokens
            require(_stakes.exists(hash), ERR_STAKE_NOT_EXISTS);
            StakeInfo stake = _stakes[hash];
            require(outcomeId < uint32(stake.amount.length), ERR_INVALID_OUTCOME_ID);
            require(stake.amount[outcomeId] >= amount, ERR_LOW_VALUE);
            stake.amount[outcomeId] -= amount;
            // Track locked tokens so onBounce can restore them if execute() bounces
            stake.candidateAmount = amount;
            stake.candidateOutcome = outcomeId;
            stake.candidateBetType = BET_TYPE_OB_SELL;
            _stakes[hash] = stake;
        }

        address obAddress = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );

        _busy = obAddress;
        _lastHash = hash;
        _opNonce++;
        _busyOpNonce = _opNonce;

        OrderBook.PlaceParams[] orderArr;
        orderArr.push(OrderBook.PlaceParams(outcomeId, isBuy, flags, price, amount, minAmount, epochId, clientOrderId));
        uint128[] noCancels;
        OrderBook(obAddress).executeBatch{
            value: 1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_depositIdentifierHash, orderArr, noCancels, _opNonce);
    }

    /// @notice Called by OrderBook after order is placed.
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param orderId Assigned order ID
    /// @param feeReserve Max fee reserve for this order (OB-computed, authoritative).
    ///                   Non-zero for buy orders; zero for sells.
    /// @param lock Full buy-side lock = cost + feeReserve (authoritative OB
    ///             value). Stored in `_orderLocks[orderId]` so floor-accumulated
    ///             residuals can be refunded on final fill / cancel. Zero for
    ///             sells.
    function onOrderPlaced(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128 orderId,
        uint128 feeReserve,
        uint128 lock,
        uint128 clientOrderId,
        uint32  outcomeId,
        bool    isBuy,
        uint8   flags,
        uint256 price,
        uint128 amount,
        uint64  opNonce
    ) public accept {
        // Sender check: allow either active single-op OB (_busy) or any OB from this event
        // (because in batch mode _busy is cleared in onBatchComplete, not on first callback).
        address expectedOb = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );
        require(msg.sender == expectedOb, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();

        // Promote sentinel to real orderId for the caller's cid lookup.
        // Clear the bounce-cleanup slot — terminal callback arrived, no
        // bounce can land for this place anymore (single-place path only;
        // batch path resets _pendingBatchClientOrderIds in onBatchComplete).
        if (clientOrderId != 0) {
            _clientOrderIds[clientOrderId] = orderId;
            if (!_pendingBatchActive && _pendingPlaceClientOrderId == clientOrderId) {
                _pendingPlaceClientOrderId = 0;
            }
        }

        // Store fee reserve + total lock per (OB, orderId). The two records
        // are independent and gated on their own non-zero condition:
        //   _orderFeeReserves — only when there's actually a reserve to draw
        //     fees from (current TAKER_FEE_RATE makes every valid buy hit
        //     this; sells always skip).
        //   _orderLocks — whenever the order has any locked collateral.
        //     A buy with cost > 0 must always get a lock record so that
        //     subsequent fill/cancel can decrement `_lockedInOrders[tt]`.
        //     Gating this on `feeReserve > 0` would leak locked collateral
        //     if fees ever round / drop to zero (e.g. zero-fee mode or a
        //     lowered MIN_ORDER_NOTIONAL); gate on `lock > 0` instead.
        // msg.sender == expectedOb is verified above, so we use it directly
        // as the outer key.
        if (feeReserve > 0) {
            _orderFeeReserves[msg.sender][orderId] = feeReserve;
        }
        if (lock > 0) {
            _orderLocks[msg.sender][orderId] = lock;
        }

        // Order successfully accepted by OB and resting on the book.
        _openOrderCount += 1;
        uint256 _eventHash = tvm.hash(abi.encode(eventId, oracleListHash, tokenType));
        _openOrdersByEvent[_eventHash] += 1;

        // For single-place path only: clear pending place-buy lock, sell candidate, _busy.
        // Batch-place path leaves these alone; onBatchComplete performs final cleanup.
        // Nonce gate: only clear state if this callback acks MY current op. A
        // stale callback (opNonce != _busyOpNonce) from a previous op must NOT
        // wipe slots that now belong to a subsequent op.
        if (!_pendingBatchActive && opNonce == _busyOpNonce) {
            _pendingPlaceBuyLock = 0;
            _pendingPlaceBuyTokenType = 0;
            TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
            uint256 hash = tvm.hash(data);
            if (_stakes.exists(hash)) {
                StakeInfo stake = _stakes[hash];
                if (stake.candidateAmount > 0 && stake.candidateBetType == BET_TYPE_OB_SELL) {
                    stake.candidateAmount = 0;
                    _stakes[hash] = stake;
                }
            }
            if (_busy.hasValue() && _busy.get() == msg.sender) {
                delete _busy;
                _busyOpNonce = 0;
            }
        }

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_ORDER_PLACED, bitCntAddress);
        emit OrderPlacedConfirmed{dest: addrExtern}(msg.sender, orderId, clientOrderId, outcomeId, isBuy, flags, price, amount);
    }

    /// @notice Called by OrderBook when a place submission was rejected (queue full or
    ///         invalid params). Restores any balance / outcome-token lock that was
    ///         taken at placement time.
    /// @dev Signature mirrors PlaceParams so PN can deterministically reconstruct
    ///      the original lock amount without consulting state.
    function onOrderRejected(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint32 outcomeId,
        bool isBuy,
        uint8 flags,
        uint256 price,
        uint128 amount,
        uint32 numOutcomes,
        uint128 clientOrderId,
        uint64  opNonce
    ) public accept {
        ensureBalance();
        address expectedOb = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );
        require(msg.sender == expectedOb, ERR_INVALID_SENDER);
        tvm.accept();

        // Release the cid sentinel so the caller can reuse the cid.
        if (clientOrderId != 0) {
            delete _clientOrderIds[clientOrderId];
            if (!_pendingBatchActive && _pendingPlaceClientOrderId == clientOrderId) {
                _pendingPlaceClientOrderId = 0;
            }
        }

        if (isBuy) {
            // Reconstruct original lock (cost + maxFee)
            uint128 cost;
            if (flags & 0x04 != 0) {
                cost = amount;
            } else {
                cost = uint128((uint256(amount) * uint256(price)) / uint256(FULL_PERCENT));
            }
            uint128 maxFee = uint128(
                (uint256(cost) * uint256(TAKER_FEE_RATE)) / uint256(FEE_DENOMINATOR)
            );
            uint128 lock = cost + maxFee;
            _balance[tokenType] += lock;
            if (_lockedInOrders[tokenType] >= lock) {
                _lockedInOrders[tokenType] -= lock;
            } else {
                _lockedInOrders[tokenType] = 0;
            }
            // Clear the single-place sentinel: the lock has just been refunded,
            // any later message bounce that hits `onBounce` would otherwise
            // re-credit the same amount via the stale `_pendingPlaceBuyLock`
            // slot (double-refund). Batch-place path keeps `_pendingBatchActive`
            // true and lets `onBatchComplete` perform final cleanup.
            // Nonce gate: only clear slots owned by MY current op.
            if (!_pendingBatchActive && opNonce == _busyOpNonce) {
                _pendingPlaceBuyLock = 0;
                _pendingPlaceBuyTokenType = 0;
            }
        } else {
            // Sell: restore outcome-token lock in stake. Auto-create / grow
            // the record on demand so we never silently drop tokens — the
            // user already had them debited via `placeOrder` and is owed
            // them back on rejection.
            TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
            uint256 hash = tvm.hash(data);
            StakeInfo stake;
            if (_stakes.exists(hash)) {
                stake = _stakes[hash];
            } else {
                stake.oracleListHash = oracleListHash;
                stake.tokenType = tokenType;
            }
            while (uint32(stake.amount.length) < numOutcomes) {
                stake.amount.push(0);
                stake.debtAmount.push(0);
                stake.couponsAmount.push(0);
            }
            stake.amount[outcomeId] += amount;
            if (stake.candidateAmount > 0 && stake.candidateBetType == BET_TYPE_OB_SELL) {
                stake.candidateAmount = 0;
            }
            _stakes[hash] = stake;
        }

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_ORDER_REJECTED, bitCntAddress);
        emit OrderPlaceRejected{dest: addrExtern}(
            msg.sender, eventId, clientOrderId, outcomeId, isBuy, flags, price, amount, opNonce
        );
    }

    /// @notice Cancels an existing order on the order book.
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param orderId Order ID to cancel
    function cancelOrder(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128 orderId
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_busy.hasValue(), ERR_NOTE_BUSY);


        address obAddress = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        _lastHash = tvm.hash(data);
        _busy = obAddress;
        _opNonce++;
        _busyOpNonce = _opNonce;

        OrderBook.PlaceParams[] noOrders;
        uint128[] cancelArr;
        cancelArr.push(orderId);
        OrderBook(obAddress).executeBatch{
            value: 1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_depositIdentifierHash, noOrders, cancelArr, _opNonce);
    }

    function cancelOrderByClient(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128 clientOrderId
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        require(clientOrderId != 0, ERR_INVALID_PARAMS);
        require(_clientOrderIds.exists(clientOrderId), ERR_ORDER_NOT_FOUND);
        uint128 orderId = _clientOrderIds[clientOrderId];
        // Sentinel `type(uint128).max` means the place is in flight — cannot
        // cancel until OB has confirmed via onOrderPlaced.
        require(orderId != type(uint128).max, ERR_NOTE_BUSY);

        address obAddress = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        _lastHash = tvm.hash(data);
        _busy = obAddress;
        _opNonce++;
        _busyOpNonce = _opNonce;

        OrderBook.PlaceParams[] noOrders;
        uint128[] cancelArr;
        cancelArr.push(orderId);
        OrderBook(obAddress).executeBatch{
            value: 1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_depositIdentifierHash, noOrders, cancelArr, _opNonce);
    }

    /// @notice Called by OrderBook after order is cancelled. Returns locked tokens.
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param orderId Cancelled order ID
    /// @param outcomeId Outcome of the cancelled order
    /// @param isBuy Whether it was a buy order
    /// @param amount Amount that was locked
    function onOrderCancelled(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint128 orderId,
        uint32 outcomeId,
        bool isBuy,
        uint128 amount,
        uint128 clientOrderId,
        uint64  opNonce
    ) public accept {
        // Verify sender is the correct OrderBook (same pattern as onOrderFilled)
        address expectedOb = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );
        require(msg.sender == expectedOb, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        orderId; // suppress unused warning

        // Free the cid slot — order is gone (user-cancel, FOK reject, shutdown).
        if (clientOrderId != 0) {
            delete _clientOrderIds[clientOrderId];
        }

        // Order left the book — counter must match OB-side resting set. Guarded
        // against underflow in case the cancellation arrives for an order PN
        // never saw confirmed (defensive — should not happen in normal flow).
        if (_openOrderCount > 0) {
            _openOrderCount -= 1;
        }
        uint256 _eventHash = tvm.hash(abi.encode(eventId, oracleListHash, tokenType));
        if (_openOrdersByEvent[_eventHash] > 0) {
            _openOrdersByEvent[_eventHash] -= 1;
        }

        if (isBuy) {
            // Return the authoritative remaining lock for this order.
            // `_orderLocks[ob][orderId]` is kept in sync by
            // onOrderPlaced/Filled and already accounts for per-fill floor
            // residuals, so we refund it verbatim (rather than recomputing
            // `amount + feeReserve` from OB's floor-truncated `amount`,
            // which would leak sub-unit dust back into `_lockedInOrders`).
            // Keyed by (ob, orderId) — different OBs (one per PMP event)
            // have independent `_nextOrderId` sequences and would collide
            // under a flat mapping.
            uint128 returned = _orderLocks[msg.sender][orderId];
            // Defensive fallback: if for some reason `_orderLocks` is
            // missing (pre-upgrade orders), fall back to the old formula.
            if (returned == 0) {
                uint128 feeReserveFallback = _orderFeeReserves[msg.sender][orderId];
                returned = amount + feeReserveFallback;
            }
            _balance[tokenType] += returned;
            if (_lockedInOrders[tokenType] >= returned) {
                _lockedInOrders[tokenType] -= returned;
            } else {
                _lockedInOrders[tokenType] = 0;
            }
            if (_orderFeeReserves[msg.sender].exists(orderId)) {
                delete _orderFeeReserves[msg.sender][orderId];
            }
            if (_orderLocks[msg.sender].exists(orderId)) {
                delete _orderLocks[msg.sender][orderId];
            }
        } else {
            // Return outcome tokens to the stake. If the stake was already
            // deleted (e.g., user has claimed), the outcome tokens are
            // PMP-internal and are silently dropped — they have no
            // standalone value here. Unlike collateral above, we cannot
            // credit them to _balance (different unit, different semantics).
            TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
            uint256 hash = tvm.hash(data);
            if (_stakes.exists(hash)) {
                StakeInfo stake = _stakes[hash];
                // Length-checked for the same reason the branch is guarded by `exists` at all,
                // one level finer. `setStake` creates a record with EMPTY arrays and they are
                // sized only when the stake is accepted, so a record recreated after a
                // `deleteStake` can be shorter than an index carried by a cancellation for an
                // order placed against the previous one. This handler is the only one of the
                // three without `numOutcomes` in its signature, so it cannot grow the array the
                // way `onOrderRejected` and `onOrderFilled` do — and it must not throw: the
                // counters above have already been decremented in this same transaction, and
                // reverting would leave them pointing at an order the book has removed.
                if (outcomeId < uint32(stake.amount.length)) {
                    stake.amount[outcomeId] += amount;
                    _stakes[hash] = stake;
                }
            }
        }

        // Clear _busy only if it still points to this OrderBook (explicit cancelOrder flow).
        // For IOC/FOK auto-cancels, _busy was already cleared by onOrderPlaced.
        // For batch operations, _busy is cleared in onBatchComplete.
        // Nonce gate (Race 2): late onOrderCancelled from a prior op must NOT
        // clear _busy of a subsequent op that has already started — even when
        // both target the same OB (same event), `_busy.get() == msg.sender`
        // alone is not enough to identify "my current op".
        if (!_pendingBatchActive && opNonce == _busyOpNonce
            && _busy.hasValue() && _busy.get() == msg.sender) {
            delete _busy;
            _busyOpNonce = 0;
        }

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_ORDER_CANCELLED, bitCntAddress);
        emit OrderCancelledConfirmed{dest: addrExtern}(msg.sender, orderId, outcomeId, isBuy, amount);
    }

    /// @notice Called by OrderBook when an order is filled during epoch settlement.
    /// @param eventId PMP event ID
    /// @param oracleListHash Hash of oracle configuration
    /// @param tokenType Token type
    /// @param outcomeId Outcome that was traded
    /// @param filledAmount Amount of outcome tokens filled
    /// @param clearingPrice Clearing price in basis points
    /// @param isBuy Whether this was a buy fill
    /// @param refundAmount Collateral refund for buy orders (overpaid above clearing price)
    /// @param feeAmount Per-fill fee component computed by OrderBook. Semantics
    ///        depend on `isRebate`:
    ///          isRebate=false → taker fee (debit from reserve / proceeds);
    ///          isRebate=true  → maker rebate (credit to balance).
    /// @param isRebate True for the maker side of a fill, false for the taker
    ///        side. Maker pays no fee — the rebate is funded out of the
    ///        taker's fee on the same fill (see OrderBook._processFillTo).
    function onOrderFilled(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint32 outcomeId,
        uint128 filledAmount,
        uint256 clearingPrice,
        bool isBuy,
        uint128 refundAmount,
        uint128 feeAmount,
        bool isRebate,
        uint128 orderId,
        bool isFinal,
        uint32 numOutcomes,
        uint128 clientOrderId
    ) public accept {
        ensureBalance();
        // Verify sender is the OrderBook for this event
        address expectedOb = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );
        require(msg.sender == expectedOb, ERR_INVALID_SENDER);

        // Free cid slot when the order has fully consumed.
        if (isFinal && clientOrderId != 0) {
            delete _clientOrderIds[clientOrderId];
        }

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);

        if (isBuy) {
            // Bought outcome tokens: add to stakes. The stake record may not
            // exist yet (PN-only buyer who never staked / split) or its
            // `amount[]` array may be shorter than `outcomeId` (lazy
            // initialisation pattern from setStake). Either case must NOT
            // burn the bought tokens — collateral was already paid, the user
            // is entitled to the outcome tokens.
            StakeInfo stake;
            if (_stakes.exists(hash)) {
                stake = _stakes[hash];
            } else {
                stake.oracleListHash = oracleListHash;
                stake.tokenType = tokenType;
            }
            while (uint32(stake.amount.length) < numOutcomes) {
                stake.amount.push(0);
                stake.debtAmount.push(0);
                stake.couponsAmount.push(0);
            }
            stake.amount[outcomeId] += filledAmount;
            _stakes[hash] = stake;
            // Fee accounting. The per-order fee reserve (populated by OB in
            // onOrderPlaced at TAKER_FEE_RATE) covers the worst case where the
            // whole order is taken as a taker. Per fill:
            //
            //   isRebate=false (taker fill): debit feeAmount from reserve;
            //     unused reserve is refunded on isFinal.
            //   isRebate=true  (maker fill): nothing is debited from reserve;
            //     feeAmount is the rebate, credited to the maker's balance.
            //     Reserve drains on isFinal as unused.
            //
            // All per-order state is keyed by (obAddress, orderId) to avoid
            // collisions with other PMP events' OBs (each OB has its own
            // `_nextOrderId` starting at 1). msg.sender is the verified OB.
            uint128 reserve = _orderFeeReserves[msg.sender][orderId];
            uint128 newReserve;
            if (isRebate) {
                newReserve = reserve;            // maker side: no fee debited
            } else {
                newReserve = reserve >= feeAmount ? reserve - feeAmount : 0;
            }
            uint128 feeRefund = 0;
            if (isFinal) {
                // Order done: refund the unused reserve and clean up.
                feeRefund = newReserve;
                newReserve = 0;
            }
            if (newReserve == 0) {
                if (_orderFeeReserves[msg.sender].exists(orderId)) {
                    delete _orderFeeReserves[msg.sender][orderId];
                }
            } else {
                _orderFeeReserves[msg.sender][orderId] = newReserve;
            }
            // Credit the price-diff refund (for limit buys filled below cap
            // price), any final-fill fee-reserve refund, and — on maker fills
            // — the rebate.
            _balance[tokenType] += refundAmount + feeRefund + (isRebate ? feeAmount : uint128(0));
            // Unlock from the order's lock: price-diff refund + final-fill
            // reserve refund + actualCost + (taker fills only) the per-fill
            // taker fee. Maker rebate is credited from outside the lock and is
            // NOT part of `consumed`.
            uint128 actualCost = uint128(
                (uint256(filledAmount) * uint256(clearingPrice)) / uint256(FULL_PERCENT)
            );
            uint128 consumed = refundAmount + feeRefund + actualCost + (isRebate ? uint128(0) : feeAmount);

            // Per-order lock tracker: `_orderLocks[ob][orderId]` is the
            // authoritative remaining lock for this order (set at
            // placement from OB's `cost + feeReserve`). Decrement by the
            // consumed amount; on `isFinal`, drain any floor-accumulated
            // residual back to `_balance` so `_lockedInOrders` empties
            // exactly per closed order instead of leaving cosmetic dust.
            uint128 orderLock = _orderLocks[msg.sender][orderId];
            uint128 applyDec = orderLock >= consumed ? consumed : orderLock;

            // Invariant: applyDec <= orderLock <= _lockedInOrders[tt]
            // (orderLock is set by OB at placement, _lockedInOrders is the
            // sum of all live orderLocks for this tokenType). So plain
            // subtraction is safe — no defensive clamps needed.
            if (isFinal) {
                uint128 residual = orderLock - applyDec;
                _balance[tokenType] += residual;
                _lockedInOrders[tokenType] -= orderLock; // == applyDec + residual
                delete _orderLocks[msg.sender][orderId];
            } else {
                _orderLocks[msg.sender][orderId] = orderLock - applyDec;
                _lockedInOrders[tokenType] -= applyDec;
            }
        } else {
            // Sold outcome tokens: receive collateral. Maker side gets a rebate
            // on top of proceeds; taker side has the fee deducted.
            uint128 proceeds = uint128(
                (uint256(filledAmount) * uint256(clearingPrice)) / uint256(FULL_PERCENT)
            );
            if (isRebate) {
                _balance[tokenType] += proceeds + feeAmount;
            } else if (proceeds > feeAmount) {
                _balance[tokenType] += (proceeds - feeAmount);
            }
        }

        // Order fully consumed — leaves the book. Partial fills (isFinal=false)
        // keep the residue resting, so the counter stays unchanged for those.
        if (isFinal) {
            if (_openOrderCount > 0) {
                _openOrderCount -= 1;
            }
            uint256 _eventHash = tvm.hash(abi.encode(eventId, oracleListHash, tokenType));
            if (_openOrdersByEvent[_eventHash] > 0) {
                _openOrdersByEvent[_eventHash] -= 1;
            }
        }

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_ORDER_FILLED, bitCntAddress);
        emit OrderFilledConfirmed{dest: addrExtern}(msg.sender, orderId, outcomeId, filledAmount, clearingPrice, isBuy, feeAmount, isRebate, isFinal);
    }

    // ===== Batch order-book operations =====
    // MAX_BATCH_SIZE is inherited from Modifiers (shared with OrderBook).

    /// @notice Atomic batch: cancels `cancelIds` and places `orders` in a single
    ///         OrderBook.executeBatch dispatch. Either side may be empty (but not
    ///         both). All-or-nothing in WASM; on bounce the pre-batch state is
    ///         restored by onBounce.
    function placeBatch(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        OrderBook.PlaceParams[] orders,
        uint128[] cancelIds
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_busy.hasValue(), ERR_NOTE_BUSY);
        require(orders.length + cancelIds.length > 0, ERR_EMPTY_BATCH);
        require(orders.length <= MAX_BATCH_SIZE, ERR_BATCH_TOO_LARGE);
        require(cancelIds.length <= MAX_BATCH_SIZE, ERR_BATCH_TOO_LARGE);
        if (orders.length > 0) {
            require(!_hasWithdrawn, ERR_INVALID_STATE);
            require(_debt == 0, ERR_DEBT_NON_ZERO);
        }

        TvmCell data = abi.encode(eventId, oracleListHash, tokenType);
        uint256 hash = tvm.hash(data);

        // Clear any leftover batch bounce-protection arrays (defensive).
        delete _pendingBatchSells;

        uint128 totalBuyLock = 0;
        bool hasSells = false;
        StakeInfo stake;
        if (_stakes.exists(hash)) {
            stake = _stakes[hash];
        }

        uint128 minNotional = minOrderNotional(tokenType);
        uint128 lot = lotSize(tokenType);
        for (uint32 i = 0; i < uint32(orders.length); i++) {
            OrderBook.PlaceParams p = orders[i];
            require(p.amount > 0, ERR_LOW_VALUE);

            // clientOrderId uniqueness — both vs existing and intra-batch.
            // Sentinel set here; intra-batch dupes hit it on the second seen.
            if (p.clientOrderId != 0) {
                require(!_clientOrderIds.exists(p.clientOrderId), ERR_INVALID_PARAMS);
                _clientOrderIds[p.clientOrderId] = type(uint128).max;
                _pendingBatchClientOrderIds.push(p.clientOrderId);
            }
            if (p.flags & 0x04 != 0) {
                // Market buy: amount = quote/collateral, not base.
                require(p.minAmount == 0, ERR_INVALID_PARAMS);
                if (p.isBuy) {
                    require(p.amount >= minNotional, ERR_ORDER_TOO_SMALL);
                }
            } else {
                require(p.amount % lot == 0, ERR_AMOUNT_NOT_LOT_MULTIPLE);
                require(p.price % TICK_SIZE == 0, ERR_PRICE_NOT_TICK_MULTIPLE);
                require(p.price == 0 || uint256(p.amount) <= type(uint256).max / uint256(p.price), ERR_NOTIONAL_OVERFLOW);
                uint256 notionalFull = (uint256(p.amount) * uint256(p.price)) / uint256(FULL_PERCENT);
                require(notionalFull <= uint256(type(uint128).max), ERR_NOTIONAL_OVERFLOW);
                require(notionalFull >= uint256(minNotional), ERR_ORDER_TOO_SMALL);
            }

            if (p.isBuy) {
                uint128 cost;
                if (p.flags & 0x04 != 0) {
                    cost = p.amount; // market buy: lock full amount as collateral
                } else {
                    cost = uint128((uint256(p.amount) * p.price) / uint256(FULL_PERCENT));
                }
                uint128 maxFee = uint128(
                    (uint256(cost) * uint256(TAKER_FEE_RATE)) / uint256(FEE_DENOMINATOR)
                );
                totalBuyLock += cost + maxFee;
            } else {
                require(_stakes.exists(hash), ERR_STAKE_NOT_EXISTS);
                require(p.outcomeId < uint32(stake.amount.length), ERR_INVALID_OUTCOME_ID);
                require(stake.amount[p.outcomeId] >= p.amount, ERR_LOW_VALUE);
                stake.amount[p.outcomeId] -= p.amount;
                _pendingBatchSells.push(PendingBatchSell({
                    outcomeId: p.outcomeId, amount: p.amount
                }));
                hasSells = true;
            }
        }

        require(_balance[tokenType] >= totalBuyLock, ERR_LOW_VALUE);
        _balance[tokenType] -= totalBuyLock;
        _lockedInOrders[tokenType] += totalBuyLock;

        if (hasSells) {
            _stakes[hash] = stake;
        }

        address obAddress = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );

        _pendingBatchActive = true;
        _pendingBatchBuyLock = totalBuyLock;
        _pendingBatchTokenType = tokenType;
        _pendingBatchStakeHash = hash;
        _busy = obAddress;
        _lastHash = hash;
        _opNonce++;
        _busyOpNonce = _opNonce;

        OrderBook(obAddress).executeBatch{
            value: 1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_depositIdentifierHash, orders, cancelIds, _opNonce);
    }

    /// @notice Enqueues a CANCEL_ALL request on the given OrderBook. The OB will
    ///         cancel up to MAX_MATCHES_PER_CALL orders per processing pass and
    ///         self-invoke until all owner orders are cleared.
    function cancelAllOrders(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_busy.hasValue(), ERR_NOTE_BUSY);

        address obAddress = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );

        // Latch busy + batch-mode so per-order onOrderCancelled callbacks during
        // the cancellation pass don't accidentally clear _busy. onBatchComplete
        // releases the latch.
        _pendingBatchActive = true;
        _pendingBatchTokenType = tokenType;
        _busy = obAddress;
        _opNonce++;
        _busyOpNonce = _opNonce;

        OrderBook(obAddress).cancelAllOrders{
            value: 1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_depositIdentifierHash, _opNonce);
    }

    /// @notice Sentinel callback sent by OrderBook after all effects of a batch
    ///         operation have been dispatched. Clears _busy and any pending
    ///         bounce-protection state. Only the OrderBook for this pair may call.
    function onBatchComplete(
        uint256 eventId,
        uint256 oracleListHash,
        uint32 tokenType,
        uint64  opNonce
    ) public accept {
        address expectedOb = DexLib.computeOrderBookAddressFromPmpCode(
            _privateNoteCode,
            _pmpCode,
            _orderBookCode,
            eventId,
            oracleListHash,
            tokenType
        );
        require(msg.sender == expectedOb, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();

        // Nonce gate (Race 1): a late onBatchComplete from a previous op
        // (e.g. a single placeOrder that internally uses executeBatch and so
        // also fires this sentinel) must NOT wipe `_pendingBatch*` state that
        // now belongs to a subsequent placeBatch the user has already started.
        // Only the current op's ack is authoritative.
        if (opNonce != _busyOpNonce) {
            return;
        }

        // Single-mode pendingBatch slots are all zero — these writes are no-ops.
        // Batch-mode owns these slots and clears them here.
        _pendingBatchActive = false;
        _pendingBatchBuyLock = 0;
        _pendingBatchTokenType = 0;
        _pendingBatchStakeHash = 0;
        delete _pendingBatchSells;
        delete _pendingBatchClientOrderIds;

        // _busy release: nonce match alone makes this safe. The previous
        // `wasBatch` gate was a stand-in for "is this my current op?" —
        // superseded by the nonce gate above. Without this, a rejected
        // single placeOrder/cancelOrder (which leaves `_pendingBatchActive`
        // false and has no per-order callback clearing `_busy`) would leave
        // PN busy forever. In the success path `_busy` is already cleared
        // by `onOrderPlaced` / `onOrderCancelled`, so this is a no-op.
        if (_busy.hasValue() && _busy.get() == msg.sender) {
            delete _busy;
            _busyOpNonce = 0;
        }
    }

    /// @notice Returns the salted PMP contract code
    /// @return pmpCode The salted PMP contract code as TvmCell
    /// @return pmpCodeHash Hash of PMP contract code
    function getPMPCode() external view returns(TvmCell pmpCode, uint256 pmpCodeHash) {
        TvmCell salt = abi.encode(_privateNoteCode);
        TvmCell code = abi.setCodeSalt(_pmpCode, salt);
        return (code, tvm.hash(code));
    }

    /// @notice Returns all global variables
    /// @return depositIdentifierHash Deposit identifier hash
    /// @return ephemeralPubkey Ephemeral public key
    /// @return balance Current free token balance
    /// @return lockedInOrders Collateral locked in open buy orders
    /// @return pmpCodeHash Hash of PMP code
    /// @return privateNoteCodeHash Hash of PrivateNote code
    /// @return busyAddress Current busy PMP address (if any)
    function getDetails() external view returns (
        uint256 depositIdentifierHash,
        uint256 ephemeralPubkey,
        mapping(uint32 => uint128) balance,
        mapping(uint32 => uint128) lockedInOrders,
        uint256 pmpCodeHash,
        uint256 privateNoteCodeHash,
        optional(address) busyAddress,
        uint128 couponsValue,
        bool hasWithdrawn
    ) {
        return (
            _depositIdentifierHash,
            _ephemeralPubkey,
            _balance,
            _lockedInOrders,
            tvm.hash(_pmpCode),
            tvm.hash(_privateNoteCode),
            _busy,
            _couponsValue,
            _hasWithdrawn
        );
    }

    /// @notice Returns contract name
    /// @return value0 Contract semantic version.
    /// @return value1 Contract identifier.
    function getVersion() external pure returns (string, string) {
        return (version, "PrivateNote");
    }
}
