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
    function reclaimOnTimeout() external;
    function cleanupUnopened() external;
    function fundProbeCommission() external;
}

/// @notice Wallet that can deploy and interact with PMP contracts
contract PrivateNote is Modifiers, ReplayProtection {

    /// @notice Contract semantic version.
    string constant version = "4.0.27";

    /// @notice Canonical-deal derivation anchors (note-funded model). The TokenContract
    ///         and RootModel code hashes/depths are NOT pinned constants here — they are
    ///         baked into this note at deploy by RootPN (`_tokenContractCodeHash` /
    ///         `_rootModelCodeHash` below). This keeps the pin ONE-WAY: the TokenContract
    ///         pins the note's code hash (`postFromNote` identity check); the note pinning
    ///         the TC hash back would make the two mutually recursive and the build never
    ///         converge (same rule as the IOB code baked below). `fundDeployShell` /
    ///         `postProbeCommission` / `postSellOffer` derive the canonical RootModel /
    ///         TokenContract from these baked hashes + this note's own key (+nonce),
    ///         never a caller-supplied address.
    // Canonical AI SuperRoot account id (workchain 0) — anchor for the RootModel-address derivation.
    // FIXED at the vanity 0:0c0c… on LOCAL, SHELLNET and MAINNET (zerostate force-places the SuperRoot
    // here). shellnet == local, no per-version / code-derived rotation — see dexdo-specs/shellnet-update.md.
    uint256 constant SUPER_ROOT_ADDR           = 0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c;

    /// @notice Owner escape hatch: stale stream/dispute locks can be force
    ///         cleared after this many seconds since the last lock change, so the
    ///         owner can always recover the note's withdraw path.
    uint64 constant STREAM_LOCK_MAX = 7 days;

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
    ///         into a forged book / wrong build. (Kept in data, not as a code
    ///         constant, so the note↔OB pin stays one-way: OB pins the note's
    ///         NOTE_CODE_HASH; pinning the OB hash in the note's CODE would make
    ///         the two hashes mutually recursive and the build never converge.)
    TvmCell _inferenceOrderBookCode;

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

    // ── Inference-market stream locks (spec §4.3) ───────────────────────────
    // A `token_contract` (streaming deal) that this note is party to can lock
    // the note while a stream is live, and dispute-lock it while a dispute is
    // open. While any lock is held the note cannot withdraw / split / merge —
    // this caps demand-side hopping and stops a party fleeing mid-dispute.
    // Auth is locker-only: only the deal that set a lock can clear it, so a lock
    // can at most freeze withdraw (never touch funds); an owner escape hatch
    // clears stale locks after `STREAM_LOCK_MAX`.
    mapping(address => bool) _streamLocks;   // deal => stream lock held
    mapping(address => bool) _disputeLocks;  // deal => dispute lock held
    uint32 _streamLockCount;
    uint32 _disputeLockCount;
    uint64 _lastStreamLockChange;

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
                TvmCell inferenceOrderBookCode,
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
        _balance[tokenType] = value;
        _ephemeralPubkey = ephemeralPubkey;
        RootPN(ROOT_PN_ADDRESS).privateNoteDeployed{value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID}(_depositIdentifierHash, tokenType, value);
    }

    /// @notice Changes the owner public key
    /// @param newPubkey New public key
    function changeOwner(uint256 newPubkey) public accept saveMsg {
        require(msg.pubkey() == _ephemeralPubkey, ERR_INVALID_SENDER);
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

    // ── Inference-market stream locks (spec §4.3) ───────────────────────────

    /// @notice Reverts if any stream/dispute lock is held — gate for value-
    ///         extracting / state-restructuring ops (withdraw, split, merge).
    function _requireNotStreamLocked() private view {
        require(_streamLockCount == 0 && _disputeLockCount == 0, ERR_STREAM_LOCKED);
    }

    /// @notice A streaming deal locks this note while its stream is live.
    /// @dev Auth: `msg.sender` must be the canonical TokenContract for
    ///      (sellerPubkey, nonce) — recomputed here from the pinned TC code hash
    ///      via `_tokenContractAddr`. A genuine TC only ever locks its own
    ///      counterparty note, so this transitively proves the caller is THIS
    ///      note's deal, and only that deal can lock this note.
    ///      `_lastStreamLockChange` refreshes only on a genuinely new lock, so the
    ///      `forceClearStreamLocks` timer cannot be pushed forward by re-locking an
    ///      already-held slot.
    function streamLock(uint256 sellerPubkey, uint64 nonce) public accept {
        address deal = _tokenContractAddr(sellerPubkey, nonce);
        require(msg.sender == deal, ERR_INVALID_SENDER);
        ensureBalance();
        if (!_streamLocks[deal]) { _streamLocks[deal] = true; _streamLockCount += 1; _lastStreamLockChange = uint64(block.timestamp); }
    }

    /// @notice The deal releases its stream lock (amicable close / timeout).
    function streamUnlock(uint256 sellerPubkey, uint64 nonce) public accept {
        address deal = _tokenContractAddr(sellerPubkey, nonce);
        require(msg.sender == deal, ERR_INVALID_SENDER);
        ensureBalance();
        if (_streamLocks[deal]) { delete _streamLocks[deal]; _streamLockCount -= 1; _lastStreamLockChange = uint64(block.timestamp); }
    }

    /// @notice The deal escalates to a dispute — both notes get dispute-locked.
    function streamDisputeLock(uint256 sellerPubkey, uint64 nonce) public accept {
        address deal = _tokenContractAddr(sellerPubkey, nonce);
        require(msg.sender == deal, ERR_INVALID_SENDER);
        ensureBalance();
        if (!_disputeLocks[deal]) { _disputeLocks[deal] = true; _disputeLockCount += 1; _lastStreamLockChange = uint64(block.timestamp); }
    }

    /// @notice The deal resolves the dispute — dispute lock released.
    function streamDisputeUnlock(uint256 sellerPubkey, uint64 nonce) public accept {
        address deal = _tokenContractAddr(sellerPubkey, nonce);
        require(msg.sender == deal, ERR_INVALID_SENDER);
        ensureBalance();
        if (_disputeLocks[deal]) { delete _disputeLocks[deal]; _disputeLockCount -= 1; _lastStreamLockChange = uint64(block.timestamp); }
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
    event InferenceFilledConfirmed(address orderBook, address tokenContract, uint128 orderId, uint128 ticks, uint256 clearingPrice, bool isBuy);

    function onInferencePlaced(uint256 modelHash, address tokenContract, uint128 orderId, bool isBuy, uint256 price, uint128 ticks) public accept {
        require(msg.sender == DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash), ERR_INVALID_SENDER);
        ensureBalance();
        emit InferenceOrderPlacedConfirmed{dest: address.makeAddrExtern(PRIVATENOTE_INFERENCE_PLACED, bitCntAddress)}(
            msg.sender, tokenContract, orderId, isBuy, price, ticks);
    }

    function onInferenceFilled(uint256 modelHash, address tokenContract, uint128 orderId, uint128 ticks, uint256 clearingPrice, bool isBuy) public accept {
        require(msg.sender == DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash), ERR_INVALID_SENDER);
        ensureBalance();
        emit InferenceFilledConfirmed{dest: address.makeAddrExtern(PRIVATENOTE_INFERENCE_FILLED, bitCntAddress)}(
            msg.sender, tokenContract, orderId, ticks, clearingPrice, isBuy);
    }

    /// @notice Owner escape hatch — clear ALL stale locks after STREAM_LOCK_MAX
    ///         (deal escrow lives in the `token_contract`, untouched by this).
    function forceClearStreamLocks() public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(_streamLockCount != 0 || _disputeLockCount != 0, ERR_INVALID_STATE);
        require(uint64(block.timestamp) > _lastStreamLockChange + STREAM_LOCK_MAX, ERR_INVALID_STATE);
        delete _streamLocks;
        delete _disputeLocks;
        _streamLockCount = 0;
        _disputeLockCount = 0;
    }

    /// @notice Lock state (for off-chain monitoring / tests).
    function getStreamLocks() external view returns (uint32 streamCount, uint32 disputeCount, uint64 lastChange) {
        return (_streamLockCount, _disputeLockCount, _lastStreamLockChange);
    }

    // ── Inference market: deploy an InferenceOrderBook FROM this note (§2/§8) ──

    /// @notice Deploy an InferenceOrderBook from this note. The OB code is the
    ///         one baked into this note at deploy (`_inferenceOrderBookCode`) —
    ///         NOT caller-supplied — so a note can never plant a forged/wrong
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

    /// @notice Post a SELL offer to the canonical book for the model from
    ///         this note, attaching the platform fee in SHELL (spec §2.1). The
    ///         fee becomes irrevocable on match (no-show penalty); this note is
    ///         recorded as the sellerNote (fee refund target on cancel,
    ///         dispute-lock target). The OB address is derived from the baked
    ///         `_inferenceOrderBookCode` + model — never a raw address —
    ///         so SHELL/fee flows cannot be routed to a forged book.
    /// @notice Seller places its resting sell offer in ONE call. Derives the canonical
    ///         per-deal TC for `(this note's ephemeral pubkey, nonce)` from the baked TC
    ///         code hash, then hands it the canonical InferenceOrderBook code hash/depth
    ///         (`_inferenceOrderBookCode`, baked by RootPN) + this note's deposit id +
    ///         order `flags`. The TC verifies the caller is a canonical PrivateNote (its
    ///         pinned note-code hash) and posts its own ask (`msg.sender == TC` at the
    ///         book), so a forged book/TC is impossible and no RootPN round-trip is needed.
    ///         Order params (price/ticks) live in the TC (constructor). Re-run after a
    ///         cancel to re-list; the TC self-guards double-posts.
    /// @param flags Order flags (POST_ONLY / IOC / FOK / MARKET) forwarded to the book.
    /// @param nonce Deal nonce identifying the seller's per-deal TC.
    function postSellOffer(uint8 flags, uint64 nonce) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        TokenContract(_tokenContractAddr(_ephemeralPubkey, nonce)).postFromNote{value: 1 vmshell, flag: 1, bounce: false}(
            tvm.hash(_inferenceOrderBookCode), _inferenceOrderBookCode.depth(), _depositIdentifierHash, flags);
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
        // §8 limit buy on inference: budget = escrow (held in the OB), ceiling =
        // maxPricePerTick, TIF = deadline (0 = GTC), flags = IOC/FOK/MARKET/POST_ONLY.
        // Owner/buyer = this note (OB uses msg.sender).
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        mapping(uint32 => varuint32) ecc;
        ecc[CURRENCIES_ID_SHELL] = varuint32(escrow);
        // §3.1.1: forward this note's pubkey so the OB can record it in the deal
        // contract (the gateway authenticates the buyer against it). SHELL escrow
        // goes to the derived canonical book, never a caller-supplied address.
        address orderBook = DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
        // bounce:true — if the book rejects the placement (expired / bad flags / insufficient
        // deposit), the SHELL escrow bounces straight back to this note rather than staying on
        // the book. onBounce safely no-ops here (msg.sender != _busy) and the ECC is
        // re-credited on accept.
        InferenceOrderBook(orderBook).placeBuyOrder{value: 2 vmshell, flag: 1, bounce: true, currencies: ecc}(
            maxPricePerTick, ticks, flags, deadline, _ephemeralPubkey);
    }

    /// @notice Place a §8 subscription (semantic order) from this note: budget
    ///         (escrow) throttled into weekly cycles, unspent forfeited by-fact to
    ///         sellers. `autoRenew` is a client hint (renewal = re-place, §8.2).
    function placeInferenceSubscription(
        uint256 modelHash,
        uint128 maxPricePerTick,
        uint128 ticks,
        uint128 escrow,
        bool    autoRenew
    ) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        mapping(uint32 => varuint32) ecc;
        ecc[CURRENCIES_ID_SHELL] = varuint32(escrow);
        // §3.1.1: forward this note's pubkey (gateway auth, recorded in the deal).
        address orderBook = DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
        // bounce:true — escrow returns to this note if the book rejects the placement.
        InferenceOrderBook(orderBook).placeSubscription{value: 2 vmshell, flag: 1, bounce: true, currencies: ecc}(
            maxPricePerTick, ticks, autoRenew, _ephemeralPubkey);
    }

    /// @notice Claim this note's pro-rata share of a subscription cycle's forfeited
    ///         budget (spec §8.2). Sent FROM this note so the order book credits the
    ///         caller (`msg.sender`) as a seller that served `orderId`'s `cycle`; the
    ///         share is paid back to this note.
    function claimInferenceForfeit(uint256 modelHash, uint128 orderId, uint8 cycle)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        address orderBook = DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
        InferenceOrderBook(orderBook).claimForfeit{value: 2 vmshell, flag: 1, bounce: false}(orderId, cycle);
    }

    /// @notice Cancel one resting inference order owned by this note (refunds any
    ///         held BUY escrow back to this note).
    function cancelInferenceOrder(uint256 modelHash, uint128 orderId)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        address orderBook = DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
        InferenceOrderBook(orderBook).cancelOrder{value: 1 vmshell, flag: 1, bounce: false}(orderId);
    }

    /// @notice Cancel all resting inference orders owned by this note.
    function cancelAllInferenceOrders(uint256 modelHash)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        address orderBook = DexLib.computeInferenceOrderBookAddress(_inferenceOrderBookCode, modelHash);
        InferenceOrderBook(orderBook).cancelAllOrders{value: 1 vmshell, flag: 1, bounce: false}();
    }

    /// @notice Buyer note stops the stream — amicable exit (spec §4.1). Max loss
    ///         is the two in-flight ticks; refund returns to this note.
    function streamStop(address tokenContract) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        IInferenceDeal(tokenContract).stop{value: 1 vmshell, flag: 1, bounce: false}();
    }

    /// @notice Buyer note disputes the current ≤2 ticks (spec §4.2). Locks both
    ///         this note and the seller note until resolved/timed out.
    function streamDispute(address tokenContract) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        IInferenceDeal(tokenContract).dispute{value: 1 vmshell, flag: 1, bounce: false}();
    }

    /// @notice Buyer note reclaims the frozen tick after the seller goes silent
    ///         for STREAM_TIMEOUT (spec §3.4 / §3.1.2). On the probe tick the
    ///         buyer pays nothing; the seller's probe commission is returned.
    function streamReclaim(address tokenContract) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        IInferenceDeal(tokenContract).reclaimOnTimeout{value: 1 vmshell, flag: 1, bounce: false}();
    }

    /// @notice Buyer note recovers a funded-but-never-opened deal: after
    ///         `MATCH_OPEN_TIMEOUT` from funding with no `open()`, the seller is a
    ///         no-show — `cleanupUnopened` refunds the full deposit and returns the
    ///         seller's probe commission (nothing delivered → no fee/penalty, §2.1).
    ///         Distinct from `streamReclaim` (opened-then-abandoned). The TC gates the
    ///         timer; the deposit/commission (ECC) refund to their fixed notes and the residual native
    ///         gas is swept to the canonical SuperRoot — no caller-chosen payout.
    function streamCleanup(address tokenContract) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        IInferenceDeal(tokenContract).cleanupUnopened{value: 1 vmshell, flag: 1, bounce: false}();
    }

    /// @notice Deterministic RootModel address for `ownerPubkey` from the RootPN-baked code hash/depth
    ///         + the canonical SuperRoot. Delegates to DexLib so the note and RootPN derive identically.
    function _rootModelAddr(uint256 ownerPubkey) private returns (address) {
        return DexLib.computeRootModelAddressFromHash(
            _rootModelCodeHash, _rootModelCodeDepth, address.makeAddrStd(0, SUPER_ROOT_ADDR), ownerPubkey);
    }

    /// @notice Deterministic per-deal TokenContract address from the RootPN-baked code hash/depth + the
    ///         seller's statics (sellerPubkey, real RootModel, nonce). Single source of truth in DexLib
    ///         (shared with the IOB's placeSellOffer check) — the ONLY address the note-funded helpers ever pay.
    function _tokenContractAddr(uint256 sellerPubkey, uint64 nonce) private returns (address) {
        (, address tokenContract) = DexLib.computeCanonicalTokenContractAddress(
            _rootModelCodeHash, _rootModelCodeDepth,
            _tokenContractCodeHash, _tokenContractCodeDepth,
            address.makeAddrStd(0, SUPER_ROOT_ADDR), sellerPubkey, nonce);
        return tokenContract;
    }


    /// @notice (note-funded model, step 1a) Owner pre-funds the seller's UNINIT cross-dapp deploy
    ///         targets — the canonical RootModel (per-seller) and/or per-deal TokenContract — with
    ///         SHELL (ECC[2]) straight from this note, so no external operational wallet (one-seed)
    ///         is needed; the seller-signed deploy message then lands on the funded address.
    /// @dev    Targets are DERIVED from THIS note's own key (`_ephemeralPubkey`) + `nonce`, never a
    ///         caller-supplied address — SHELL can only ever reach the seller's canonical RootModel /
    ///         TokenContract (same derivation as the IOB). ONE `flag:16` send per target (mirrors the giver
    ///         `fund_deploy_address`): flag 16 carries the ECC[2] to an UNINIT cross-dapp address so the
    ///         seller-signed deploy then lands + activates (flag 1 funds the balance but the cross-dapp
    ///         deploy would not activate). `bounce:false` is REQUIRED (uninit target). amount 0 = skip.
    function fundDeployShell(uint64 nonce, uint128 rootModelShell, uint128 tcShell)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        if (rootModelShell > 0) {
            mapping(uint32 => varuint32) rmEcc;
            rmEcc[CURRENCIES_ID_SHELL] = varuint32(rootModelShell);
            _rootModelAddr(_ephemeralPubkey).transfer({value: 1 vmshell, bounce: false, flag: 16, currencies: rmEcc});
        }
        if (tcShell > 0) {
            mapping(uint32 => varuint32) tcEcc;
            tcEcc[CURRENCIES_ID_SHELL] = varuint32(tcShell);
            _tokenContractAddr(_ephemeralPubkey, nonce).transfer({value: 1 vmshell, bounce: false, flag: 16, currencies: tcEcc});
        }
    }

    /// @notice (note-funded model, step 1b) Owner funds the seller's probe commission into the
    ///         per-deal TokenContract straight from this note — the seller mirror of the buyer's
    ///         `placeInferenceBuy` escrow (no external operational wallet).
    /// @dev    The target is the DERIVED canonical TC for `(this note's seller key, nonce)` — never a
    ///         caller-supplied address (same derivation as the IOB). `tc.fundProbeCommission` consumes the
    ///         attached ECC[2] (`require >= probeCommission`, excess refunded to this note).
    function postProbeCommission(uint64 nonce, uint128 amount)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        mapping(uint32 => varuint32) ecc;
        ecc[CURRENCIES_ID_SHELL] = varuint32(amount);
        IInferenceDeal(_tokenContractAddr(_ephemeralPubkey, nonce)).fundProbeCommission{value: 2 vmshell, flag: 1, bounce: false, currencies: ecc}();
    }

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
        require(length > 0, ERR_INVALID_PARAMS);
        require(length == oracleFee.length, ERR_INVALID_PARAMS);
        require(length == index.length, ERR_INVALID_PARAMS);
        require(initialStakes.length > 0, ERR_INVALID_PARAMS);
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
            for (uint32 k = 0; k < uint32(refundAmounts.length) && k < uint32(stake.amount.length); k++) {
                uint128 r = refundAmounts[k];
                if (r == 0) continue;
                require(stake.amount[k] >= r, ERR_LOW_VALUE);
                stake.amount[k] -= r;
            }
            _stakes[hash] = stake;
            _balance[tokenType] += refundTotal;
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
        _requireNotStreamLocked();
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
        _requireNotStreamLocked();
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
    ///      - all token balances < minStakeValue (i.e. too small to stake)
    ///      - debt == 0
    ///      - No withdrawals from this wallet have been performed.
    ///      - No active stakes exist.
    ///      - No coupon currently exists.
    function generateCoupon(uint32 tokenType) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
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
        require(_couponsValue == 0, ERR_COUPON_ALREADY_EXISTS);
        _couponsValue = getCouponValue(tokenType);
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
        body;
        if (!_busy.hasValue() || msg.sender != _busy.get()) {
            return;
        }

        // --- Forfeit bounce: PMP was unreachable (already self-destructed
        //     by an earlier claim) when `deleteStake` dispatched. The
        //     stake is moot — clean up locally.
        if (_pendingForfeit) {
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
    function initTransfer(uint256 destDepositHash, uint32 tokenType, uint128 amount)
        public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg
    {
        ensureBalance();
        // A stream/dispute lock means this note collateralises a live deal;
        // transferring its balance out would drain value from under the lock.
        _requireNotStreamLocked();
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

        address dest = DexLib.computePrivateNoteAddress(_privateNoteCode, destDepositHash);

        _balance[tokenType] -= amount;
        _pendingTransferAmount = amount;
        _pendingTransferTokenType = tokenType;
        _hasTransferred = true;
        _busy = dest;

        address addrExtern = address.makeAddrExtern(PRIVATENOTE_TRANSFER_INITIATED, bitCntAddress);
        emit TransferInitiated{dest: addrExtern}(dest, tokenType, amount);

        PrivateNote(dest).offerTransfer{value: 0.1 vmshell, flag: 1, bounce: true, dest_dapp_id: ROOT_PN_DAPP_ID}(
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
    /// @dev Clears busy state. Sent with bounce: false — if it fails to arrive,
    ///      use clearTransferBusy() as a recovery hatch.
    function onTransferAccepted() public senderIs(_busy.get()) accept {
        ensureBalance();
        _pendingTransferAmount = 0;
        delete _busy;
    }

    /// @notice Recovery hatch: owner can force-clear a stuck transfer state.
    /// @dev Only callable when a pending transfer exists (_pendingTransferAmount > 0).
    ///      Does NOT restore balance — tokens are already at the destination.
    ///      Use only after verifying off-chain that the receiver credited the tokens.
    function clearTransferBusy() public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(_pendingTransferAmount > 0, ERR_INVALID_STATE);
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
        _couponsValue = 0;
        _couponsTokenType = 0;
    }

    /// @notice Withdraws tokens to a specified wallet.
    /// @dev Inner action flag is hard-coded to 1 inside RootPN — accepting a
    ///      caller-supplied flag opens TVM flag 128 (CARRY_ALL_BALANCE) and 32
    ///      (DELETE_IF_EMPTY) abuse paths that drain or destroy RootPN.
    /// @param destWalletAddr Destination wallet address
    /// @param dapp_id DApp id forwarded to RootPN.withdrawTokens (surfaced in the TokensWithdrawn event).
    function withdrawTokens(address destWalletAddr, uint256 dapp_id) public onlyOwnerPubkey(_ephemeralPubkey) accept saveMsg {
        ensureBalance();
        require(!_hasWithdrawn, ERR_INVALID_STATE);
        _requireNotStreamLocked();
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
        // Drain the note's PHYSICAL ECC pool too. Inference SHELL (ECC[2]) is held
        // on the note ACCOUNT, NOT in the RootPN-custodied `_balance` — so releasing
        // `_balance` alone leaves live ECC[2] on a "withdrawn" note, which could still
        // fund an inference buy. Attach that physical SHELL to this message; RootPN
        // forwards it straight through to `destWalletAddr` (and returns it on the
        // revert path). After this the note holds zero ECC[2] and is dead.
        mapping(uint32 => varuint32) physCc;
        uint128 physShell = uint128(address(this).currencies[CURRENCIES_ID_SHELL]);
        if (physShell > 0) { physCc[CURRENCIES_ID_SHELL] = varuint32(physShell); }
        // Withdraw the ENTIRE balance — pass the full per-token-type map so
        // RootPN moves every currency the note holds in one transfer.
        RootPN(ROOT_PN_ADDRESS).withdrawTokens{value: 0.1 vmshell, bounce: false, flag: 1, currencies: physCc, dest_dapp_id: ROOT_PN_DAPP_ID}(_balance, destWalletAddr, _depositIdentifierHash, dapp_id);
        delete _balance;
        _hasWithdrawn = true;
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

        address obAddress = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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
        address expectedOb = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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
        address expectedOb = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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


        address obAddress = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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

        address obAddress = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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
        address expectedOb = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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
                stake.amount[outcomeId] += amount;
                _stakes[hash] = stake;
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
        address expectedOb = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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

        address obAddress = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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

        address obAddress = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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
        address expectedOb = DexLib.computeOrderBookAddress(
            _privateNoteCode,
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

    /// @notice Helper to return empty optional for event emission
    function _resolvedOutcomeNone() private pure returns (optional(uint32)) {
        optional(uint32) none;
        return none;
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
