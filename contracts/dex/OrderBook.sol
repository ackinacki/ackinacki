pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./PrivateNote.sol";
import "./PMP.sol";
import "./RootPN.sol";
import "./libraries/DexLib.sol";

/// @title OrderBook — native Solidity dark order book with price-indexed levels.
/// @notice Orders are stored by id in `_orders`. Per-market orders are organised
///         into a price-sorted dictionary of price levels (`_levels`). Each level
///         keeps a FIFO of orders at that price (time priority within level).
///         Matching walks levels in best-first order and stops as soon as it has
///         filled the taker order or hit MAX_MATCHES_PER_CALL.
contract OrderBook is Modifiers {

    /// @notice Contract semantic version.
    string constant version = "4.0.27";

    /// @notice Event identifier associated with this order book.
    uint256 static _eventId;
    /// @notice Oracle list hash associated with this order book.
    uint256 static _oracleListHash;
    /// @notice Token type associated with this order book.
    uint32 static _tokenType;

    /// @notice PrivateNote code HASH/DEPTH (not the full code) for deterministic
    ///         wallet-address resolution via DexLib.computePrivateNoteAddressFromHash.
    uint256 _privateNoteCodeHash;
    uint16  _privateNoteCodeDepth;

    /// @notice PMP contract that deployed this OrderBook.
    address _pmpAddress;

    /// @notice Result-window start propagated from PMP. Once
    ///         `block.timestamp >= _resultStart`, no new place/cancel actions
    ///         are accepted — the book is closed at the result deadline rather
    ///         than waiting for resolve, so trading cannot continue after the
    ///         outcome is observable off-chain.
    uint64 _resultStart;

    /// @notice Number of outcomes for the underlying PMP event. Propagated to
    ///         PrivateNote in stake-mutating callbacks (`onOrderFilled` buy,
    ///         `onOrderRejected` sell) so PN can size stake arrays to the
    ///         full width PMP::claim() expects, regardless of which outcomes
    ///         the user actually trades.
    uint32 _numOutcomes;

    // ===== Order storage =====

    /// @notice Resting order record. Doubly linked into the per-price-level FIFO
    ///         (for time priority) and the per-owner FIFO (for cancel-all/getOrdersByOwner).
    struct Order {
        uint256 depositHash;
        uint256 price;          // basis points — also used as the level key
        uint128 amount;         // remaining
        uint128 minAmount;      // taker pre-check; not used for resting
        uint128 initialAmount;  // original size at placement — for PartialFill math
        uint128 filledAccum;    // cumulative filled size (survives continuations)
        uint128 clientOrderId;  // optional user-supplied id (0 = not set)
        uint64  epochId;
        uint64  opNonce;        // nonce of the place op that created this order
        uint32  outcomeId;
        uint8   flags;
        bool    isBuy;
        // Per-price-level intrusive FIFO links.
        uint128 nextAtPrice;    // 0 = end
        uint128 prevAtPrice;    // 0 = head
        // Per-owner intrusive FIFO links.
        uint128 nextInOwner;
        uint128 prevInOwner;
    }

    mapping(uint128 => Order) _orders;

    /// @notice Per-price-level metadata. Keyed by (outcomeId, isBuy, epochId, price).
    ///         epochId is part of the key so orders from different epochs live
    ///         in disjoint FIFOs and never co-mingle. The mapping over `price`
    ///         is sorted (Patricia trie), enabling best-price iteration via
    ///         min()/max()/next()/prev() within a single epoch.
    struct PriceLevel {
        uint128 firstOrderId;   // FIFO head
        uint128 lastOrderId;    // FIFO tail
        uint128 totalAmount;    // sum of order amounts at this level (single-epoch)
    }

    mapping(uint32 => mapping(bool => mapping(uint64 => mapping(uint256 => PriceLevel)))) _levels;

    /// @notice Per-owner FIFO (insertion order across all markets).
    mapping(uint256 => uint128) _ownerHead;
    mapping(uint256 => uint128) _ownerTail;

    /// @notice Monotonic order id (next id to assign; id 0 reserved for "none").
    uint128 _nextOrderId;

    /// @notice Total number of resting orders across all markets.
    uint32 _orderCount;

    /// @notice True once shutdown() has been initiated. While set, no new
    ///         place/cancel operations are accepted; the contract is draining.
    bool _shuttingDown;

    /// @notice Deferred-shutdown latch. `shutdown()` sets this (instead of
    ///         `_shuttingDown`) when the queue is still draining an in-flight
    ///         order match — the drain must not preempt an order that has
    ///         already started executing. Once `_queueSize` reaches zero the
    ///         next `_processHeadCore` pass promotes this into a real
    ///         shutdown via a self-call.
    bool _shutdownPending;


    /// @notice Resume cursor for shutdown's batch scan across self-calls.
    ///         Persisted across tx boundaries so each self-call picks up
    ///         where the previous one left off instead of re-scanning
    ///         already-emptied slots 1..cursor. Without this, draining
    ///         N live orders in batches of MAX_SHUTDOWN_BATCH costs
    ///         O(N^2/B) storage reads; with the cursor it is O(N).
    ///         `_nextOrderId` is frozen once `_shuttingDown=true` (no new
    ///         orders can be placed), so the cursor always terminates.
    uint128 _shutdownCursor;

    /// @notice Accumulated rebates paid out to makers (in collateral units).
    ///         Each fill pays takerFee × MAKER_REBATE_NUM/MAKER_REBATE_DEN
    ///         (= 0.03375% of notional) to the matched maker.
    uint128 _totalMakerRebatesPaid;

    /// @notice Accumulated protocol fees retained by the contract
    ///         (= takerFee - makerRebate per fill, i.e. 0.01125% of notional).
    uint128 _totalProtocolFees;

    /// @notice Monotonic match counter. Both legs (maker + taker) of one match
    ///         emit OrderFilled with the same `matchId`, so off-chain consumers
    ///         can pair the two legs unambiguously.
    uint64 _matchSeq;

    // ===== Matching constants =====

    /// @notice Maximum fill count per single processHead invocation.
    ///         Bounded by TVM's 255 outgoing-action limit: each match emits 4
    ///         actions (2 extern events + 2 internal callbacks). 30 × 4 = 120
    ///         leaves ample room for the worst-case executeBatch interleave
    ///         (5 PLACE × validation/Queued/onOrderRejected actions + the
    ///         processHead invocation that follows in the same tx + the
    ///         onBatchComplete sentinel).
    uint8 constant MAX_MATCHES_PER_CALL = 30;

    /// @notice Maximum cancellations per ACTION_CANCEL_ALL pass.
    ///         Each cancel emits 2 actions; 30 × 2 = 60 with similar headroom.
    uint8 constant MAX_CANCEL_ALL_PER_CALL = 30;

    /// @notice Maximum cancellations per shutdown pass (any owner).
    uint8 constant MAX_SHUTDOWN_BATCH = 10;

    // MAX_BATCH_SIZE is inherited from Modifiers (shared with PrivateNote).

    // ===== Order flags =====
    uint8 constant FLAG_IOC       = 0x01;
    uint8 constant FLAG_FOK       = 0x02;
    uint8 constant FLAG_MARKET    = 0x04;
    uint8 constant FLAG_POST_ONLY = 0x08;
    uint8 constant TAKER_FLAGS_MASK = 0x07;

    /// @notice Parameters for placing a single order.
    /// @dev `clientOrderId` is optional (0 = not set). When non-zero it is
    ///      validated as unique among the caller's currently-active orders.
    ///      A duplicate cid reverts the whole batch (no silent override).
    struct PlaceParams {
        uint32  outcomeId;
        bool    isBuy;
        uint8   flags;
        uint256 price;
        uint128 amount;
        uint128 minAmount;
        uint64  epochId;
        uint128 clientOrderId;
    }

    // ===== Queue (circular, slot 0..99) =====

    uint8 constant QENTRY_PLACE      = 1;
    uint8 constant QENTRY_CANCEL     = 2;
    uint8 constant QENTRY_CANCEL_ALL = 3;

    uint8 constant QUEUE_CAPACITY = 100;
    uint8 constant QUEUE_PLACE_LIMIT = 90;

    /// @notice Maximum levels the FOK / minAmount pre-check may walk in
    ///         a single tx before yielding to a continuation. Bounds gas
    ///         on extremely deep books (otherwise a sufficiently large
    ///         book would let pre-check exhaust gas mid-tx, wedging the
    ///         queue at the head entry).
    uint32 constant MAX_PRECHECK_ITERATIONS = 3000;

    struct QueueEntry {
        uint8   entryType;
        uint32  queueId;
        uint256 depositHash;
        // Place fields:
        uint32  outcomeId;
        bool    isBuy;
        uint8   flags;
        uint256 price;
        uint128 amount;
        uint128 minAmount;
        uint64  epochId;
        uint128 clientOrderId;
        // Continuation field for matching after MAX_MATCHES cap.
        uint128 targetOrderId;
        // Cumulative taker-side fill across continuations. PartialFill /
        // FullyFilled is emitted ONCE at final completion (no more cont) using
        // this aggregate.
        uint128 filledAccum;
        // Continuation fields for the pre-check phase. precheckDone=false
        // means the pre-check still has more levels to walk; precheckAccum
        // and precheckLastPrice are the resume cursor.
        bool    precheckDone;
        uint128 precheckAccum;
        uint256 precheckLastPrice;
        // Marks the last entry of an `executeBatch` / `cancelAllOrders` call.
        // When this entry leaves the head, OB fires `onBatchComplete` to PN.
        bool    isBatchEnd;
        // Echo of the caller's `_opNonce` for this op. Threaded through every
        // PN callback (onOrderPlaced/Rejected/Cancelled/BatchComplete) so PN
        // can tell "this is ack of MY current op" from "this is a stale late
        // callback of a previous op" — the latter must NOT clear `_busy` or
        // wipe `_pendingBatch*` state.
        uint64  opNonce;
    }

    mapping(uint8 => QueueEntry) _queue;

    uint8  _queueHead;
    uint8  _queueTail;
    uint8  _queueSize;
    uint32 _nextQueueId;

    // ===== Events =====

    event OrderPlaced(uint128 orderId, uint32 outcomeId, bool isBuy, uint8 flags, uint256 price, uint128 amount, uint128 clientOrderId, uint256 depositHash, uint64 opNonce);
    event OrderCancelled(uint128 orderId, uint128 clientOrderId);
    event OrderFilled(uint128 orderId, uint128 filledAmount, uint256 clearingPrice, uint128 feeAmount, bool isTaker, uint64 matchId, uint256 depositHash);
    /// @notice Aggregated MM-friendly fill events. Emitted ONCE per order
    ///         after matching for that order completes (across continuations):
    ///         - `PartialFill` if the order remains in the book with leftover
    ///         - `FullyFilled` if the order is fully consumed
    ///         Per-fill `OrderFilled` continues to fire for raw analytics.
    event PartialFill(uint128 orderId, uint128 clientOrderId, uint128 filledAmount, uint128 remainingAmount);
    event FullyFilled(uint128 orderId, uint128 clientOrderId, uint128 filledAmount);
    event Queued(uint8 slot, uint32 queueId, uint8 entryType);
    event Rejected(uint8 entryType, uint256 depositHash);
    /// @notice Emitted when an outgoing callback (onOrderFilled / onOrderCancelled / onOrderPlaced
    ///         / onBatchComplete / onOrderRejected) bounces back. Off-chain monitors should pick
    ///         this up and reconcile the affected PN. State on OB is NOT auto-rolled back —
    ///         an order removed during matching whose Filled callback later bounces stays
    ///         removed; the bounced credit needs operator-driven recovery.
    event CallbackBounced(address dest, uint64 lt);

    // ===== Constructor =====

    constructor(
        uint256 pmpSaltedCodeHash,
        uint16 pmpSaltedCodeDepth,
        uint64 resultStart,
        uint32 numOutcomes
    ) {
        tvm.accept();
        ensureBalance();

        TvmCell salt = abi.codeSalt(tvm.code()).get();
        (TvmCell PrivateNoteCode) = abi.decode(salt, (TvmCell));
        _privateNoteCodeHash  = tvm.hash(PrivateNoteCode);
        _privateNoteCodeDepth = uint16(PrivateNoteCode.depth());

        _pmpAddress = msg.sender;
        require(msg.sender == DexLib.computePMPAddressFromHash(
            pmpSaltedCodeHash, pmpSaltedCodeDepth,
            _eventId, _oracleListHash, _tokenType
        ), ERR_INVALID_SENDER);

        _resultStart = resultStart;
        _numOutcomes = numOutcomes;
        _nextOrderId = 1;
        _orderCount = 0;

        _queueHead = 0;
        _queueTail = 0;
        _queueSize = 0;
        _nextQueueId = 1;
    }

    /// @notice PMP propagates a new resultStart when oracles update timings
    ///         before the prior result window has elapsed.
    function setResultStart(uint64 resultStart) public {
        require(msg.sender == _pmpAddress, ERR_INVALID_SENDER);
        require(block.timestamp < _resultStart, ERR_RESULT_NOT_STARTED);
        require(resultStart > block.timestamp, ERR_INVALID_PARAMS);
        tvm.accept();
        ensureBalance();
        _resultStart = resultStart;
    }

    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) return;
        gosh.mintshellq(MIN_BALANCE);
    }

    function _notifyRejectedPlace(
        uint256 depositHash,
        PlaceParams op,
        uint64  opNonce
    ) private view {
        address pn = DexLib.computePrivateNoteAddressFromHash(_privateNoteCodeHash, _privateNoteCodeDepth, depositHash);
        PrivateNote(pn).onOrderRejected{
            value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType,
          op.outcomeId, op.isBuy, op.flags, op.price, op.amount, _numOutcomes, op.clientOrderId, opNonce);
    }

    // ===== Unified entry point: enqueue + processHead =====

    function executeBatch(
        uint256 depositIdentifierHash,
        PlaceParams[] orders,
        uint128[] cancelIds,
        uint64  opNonce
    ) public {
        require(!_shuttingDown, ERR_ALREADY_CANCELLED);
        // Book is closed at the result deadline.
        require(block.timestamp < _resultStart, ERR_RESULT_NOT_STARTED);
        address wallet = DexLib.computePrivateNoteAddressFromHash(_privateNoteCodeHash, _privateNoteCodeDepth,
            depositIdentifierHash
        );
        require(msg.sender == wallet, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();

        uint32 nPlace  = uint32(orders.length)    > MAX_BATCH_SIZE ? MAX_BATCH_SIZE : uint32(orders.length);
        uint32 nCancel = uint32(cancelIds.length) > MAX_BATCH_SIZE ? MAX_BATCH_SIZE : uint32(cancelIds.length);

        uint128 minNotional = minOrderNotional(_tokenType);
        uint128 lot = lotSize(_tokenType);
        bool anyQueued = false;
        uint8 lastSlot = 0;

        // Cancels first: frees price-level slots, owner orders, and queue
        // capacity before new placements race for the same room. Avoids
        // self-match against the about-to-be-cancelled orders.
        for (uint32 j = 0; j < nCancel; j++) {
            if (_enqueueCancel(depositIdentifierHash, cancelIds[j], opNonce)) {
                lastSlot = _queueTail == 0 ? uint8(QUEUE_CAPACITY - 1) : _queueTail - 1;
                anyQueued = true;
            }
        }

        for (uint32 i = 0; i < nPlace; i++) {
            PlaceParams op = orders[i];
            bool valid = true;

            // Validate at queue-insertion time. Every revert path inside
            // _doPlace must be pre-caught here so a bad entry can never
            // enter the queue — a later _processHeadCore pass that reverts
            // on a stuck head would permanently deadlock the book.
            bool opIsPostOnly = (op.flags & FLAG_POST_ONLY) != 0;
            bool opIsMarket   = (op.flags & FLAG_MARKET)    != 0;
            bool opIsIoc      = (op.flags & FLAG_IOC)       != 0;
            bool opIsFok      = (op.flags & FLAG_FOK)       != 0;

            if (op.amount == 0) valid = false;
            // A POST_ONLY order only ever rests, so a taker-side minAmount is meaningless on it —
            // reject it here rather than let it pass validation and be dropped silently in _doPlace.
            else if (opIsPostOnly && (opIsMarket || opIsIoc || opIsFok || op.minAmount != 0)) valid = false;
            else if (opIsIoc && opIsFok) valid = false;
            else if (opIsMarket && (opIsIoc || opIsFok)) valid = false;
            else if (opIsMarket) {
                // Market buy: amount is quote/collateral, not base. Base-lot
                // quantisation and minAmount (a base-unit threshold) do not
                // apply here — forbid them outright to avoid cross-unit
                // comparisons in the FOK/minAmount precheck loop.
                if (op.minAmount != 0) valid = false;
                else if (op.isBuy && op.amount < minNotional) valid = false;
            } else {
                if (op.amount % lot != 0) valid = false;
                else if (op.minAmount > op.amount) valid = false;
                else if (op.price == 0) valid = false;
                else if (op.price % TICK_SIZE != 0) valid = false;
                else if (op.price != 0 && uint256(op.amount) > type(uint256).max / uint256(op.price)) {
                    valid = false;
                }
                else {
                    uint256 notionalFull = (uint256(op.amount) * uint256(op.price)) / uint256(FULL_PERCENT);
                    if (notionalFull > uint256(type(uint128).max)) {
                        valid = false;
                    } else if (notionalFull < uint256(minNotional)) {
                        valid = false;
                    }
                }
            }
            // clientOrderId uniqueness is enforced on the PN side now —
            // OB just carries the cid for event emission.

            if (valid) {
                bool ok = _enqueuePlace(
                    depositIdentifierHash, op.outcomeId, op.isBuy,
                    op.flags, op.price, op.amount, op.minAmount, op.epochId, op.clientOrderId,
                    opNonce
                );
                if (ok) {
                    lastSlot = _queueTail == 0 ? uint8(QUEUE_CAPACITY - 1) : _queueTail - 1;
                    anyQueued = true;
                } else {
                    _notifyRejectedPlace(depositIdentifierHash, op, opNonce);
                }
            } else {
                address addrExtern = address.makeAddrExtern(OB_REJECTED, bitCntAddress);
                emit Rejected{dest: addrExtern}(QENTRY_PLACE, depositIdentifierHash);
                _notifyRejectedPlace(depositIdentifierHash, op, opNonce);
            }
        }

        if (anyQueued) {
            _queue[lastSlot].isBatchEnd = true;
        }
        _processHeadCore();
        // All-rejected case — no queue entry will fire the ack.
        if (!anyQueued) {
            _notifyBatchAccepted(depositIdentifierHash, opNonce);
        }
    }

    function cancelAllOrders(uint256 depositIdentifierHash, uint64 opNonce) public {
        require(!_shuttingDown, ERR_ALREADY_CANCELLED);
        require(block.timestamp < _resultStart, ERR_RESULT_NOT_STARTED);
        address wallet = DexLib.computePrivateNoteAddressFromHash(_privateNoteCodeHash, _privateNoteCodeDepth,
            depositIdentifierHash
        );
        require(msg.sender == wallet, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();

        bool ok = _enqueueCancelAll(depositIdentifierHash, opNonce);
        if (ok) {
            uint8 slot = _queueTail == 0 ? uint8(QUEUE_CAPACITY - 1) : _queueTail - 1;
            _queue[slot].isBatchEnd = true;
        }
        _processHeadCore();
        if (!ok) {
            _notifyBatchAccepted(depositIdentifierHash, opNonce);
        }
    }

    function _notifyBatchAccepted(uint256 depositIdentifierHash, uint64 opNonce) private view {
        address pn = DexLib.computePrivateNoteAddressFromHash(_privateNoteCodeHash, _privateNoteCodeDepth, depositIdentifierHash
        );
        PrivateNote(pn).onBatchComplete{
            value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType, opNonce);
    }

    // ===== Queue helpers =====

    function _canQueuePlace() private view returns (bool) {
        return _queueSize < QUEUE_PLACE_LIMIT;
    }

    function _canQueueCancel() private view returns (bool) {
        return _queueSize < QUEUE_CAPACITY;
    }

    function _allocSlot() private returns (uint8 slot, uint32 queueId) {
        slot = _queueTail;
        queueId = _nextQueueId;
        _nextQueueId++;
        if (_nextQueueId == 0) {
            _nextQueueId = 1;
        }
        _queueTail = uint8((uint32(_queueTail) + 1) % uint32(QUEUE_CAPACITY));
        _queueSize++;
    }

    function _advanceHead() private {
        delete _queue[_queueHead];
        _queueHead = uint8((uint32(_queueHead) + 1) % uint32(QUEUE_CAPACITY));
        _queueSize--;
    }

    function _enqueuePlace(
        uint256 depositHash,
        uint32  outcomeId,
        bool    isBuy,
        uint8   flags,
        uint256 price,
        uint128 amount,
        uint128 minAmount,
        uint64  epochId,
        uint128 clientOrderId,
        uint64  opNonce
    ) private returns (bool ok) {
        if (!_canQueuePlace()) {
            address addrExtern = address.makeAddrExtern(OB_REJECTED, bitCntAddress);
            emit Rejected{dest: addrExtern}(QENTRY_PLACE, depositHash);
            return false;
        }
        (uint8 slot, uint32 queueId) = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_PLACE,
            queueId: queueId,
            depositHash: depositHash,
            outcomeId: outcomeId,
            isBuy: isBuy,
            flags: flags,
            price: price,
            amount: amount,
            minAmount: minAmount,
            epochId: epochId,
            clientOrderId: clientOrderId,
            targetOrderId: 0,
            filledAccum: 0,
            precheckDone: false,
            precheckAccum: 0,
            precheckLastPrice: 0,
            isBatchEnd: false,
            opNonce: opNonce
        });
        // (placeholder anchor — _enqueueCancel/_enqueueCancelAll use clientOrderId: 0)
        address addrExtern2 = address.makeAddrExtern(OB_QUEUED, bitCntAddress);
        emit Queued{dest: addrExtern2}(slot, queueId, QENTRY_PLACE);
        return true;
    }

    function _enqueueCancel(
        uint256 depositHash,
        uint128 targetOrderId,
        uint64  opNonce
    ) private returns (bool ok) {
        if (!_canQueueCancel()) {
            address addrExtern = address.makeAddrExtern(OB_REJECTED, bitCntAddress);
            emit Rejected{dest: addrExtern}(QENTRY_CANCEL, depositHash);
            return false;
        }
        (uint8 slot, uint32 queueId) = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_CANCEL,
            queueId: queueId,
            depositHash: depositHash,
            outcomeId: 0,
            isBuy: false,
            flags: 0,
            price: 0,
            amount: 0,
            minAmount: 0,
            epochId: 0,
            clientOrderId: 0,
            targetOrderId: targetOrderId,
            filledAccum: 0,
            precheckDone: false,
            precheckAccum: 0,
            precheckLastPrice: 0,
            isBatchEnd: false,
            opNonce: opNonce
        });
        address addrExtern2 = address.makeAddrExtern(OB_QUEUED, bitCntAddress);
        emit Queued{dest: addrExtern2}(slot, queueId, QENTRY_CANCEL);
        return true;
    }

    function _enqueueCancelAll(uint256 depositHash, uint64 opNonce) private returns (bool ok) {
        if (!_canQueueCancel()) {
            address addrExtern = address.makeAddrExtern(OB_REJECTED, bitCntAddress);
            emit Rejected{dest: addrExtern}(QENTRY_CANCEL_ALL, depositHash);
            return false;
        }
        (uint8 slot, uint32 queueId) = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_CANCEL_ALL,
            queueId: queueId,
            depositHash: depositHash,
            outcomeId: 0,
            isBuy: false,
            flags: 0,
            price: 0,
            amount: 0,
            minAmount: 0,
            epochId: 0,
            clientOrderId: 0,
            targetOrderId: 0,
            filledAccum: 0,
            precheckDone: false,
            precheckAccum: 0,
            precheckLastPrice: 0,
            isBatchEnd: false,
            opNonce: opNonce
        });
        address addrExtern2 = address.makeAddrExtern(OB_QUEUED, bitCntAddress);
        emit Queued{dest: addrExtern2}(slot, queueId, QENTRY_CANCEL_ALL);
        return true;
    }

    function _selfCallProcessHead() private pure {
        OrderBook(address(this)).processHead{
            value: 1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }();
    }

    /// @notice Public entry: accepts the call (anyone can poke the queue) and
    ///         delegates to the internal core. Same-tx invocations from
    ///         executeBatch/cancelAllOrders should call _processHeadCore
    ///         directly to avoid a redundant tvm.accept().
    function processHead() public {
        tvm.accept();
        ensureBalance();
        _processHeadCore();
    }

    function _processHeadCore() private {
        // Once the drain itself is latched we do not process queue entries —
        // their side effects (book insertions / cancels) would race with the
        // drain cancelling the same orders. The queue was expected to be empty
        // by the time `_shuttingDown` becomes true (see `shutdown()` below,
        // which defers via `_shutdownPending` until `_queueSize == 0`).
        if (_shuttingDown) return;
        if (_queueSize == 0) {
            // Queue drained — if a shutdown has been queued behind in-flight
            // order processing, promote it now via a self-call to run in its
            // own tx (gas headroom, clean stack).
            if (_shutdownPending) {
                OrderBook(address(this)).shutdown{
                    value: 1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
                }();
            }
            return;
        }

        QueueEntry head = _queue[_queueHead];

        // ── Shutdown-pending fast-path ─────────────────────────────────────
        // If `shutdown()` was deferred behind in-flight queue entries, only
        // entries that have ALREADY started executing (head with
        // `precheckDone == true`) get to finish their match-chain.
        // Untouched entries are refused immediately — letting the drain
        // proceed without firing fills that the user no longer expects.
        // CANCEL/CANCEL_ALL just advance: the upcoming book drain will
        // clear those orders anyway.
        if (_shutdownPending && !head.precheckDone) {
            uint256 doneHash = head.depositHash;
            uint64  doneNonce = head.opNonce;
            if (head.entryType == QENTRY_PLACE) {
                if (head.targetOrderId == 0) {
                    // _doPlace never ran → onOrderPlaced never fired → reject path.
                    PlaceParams op = PlaceParams(
                        head.outcomeId, head.isBuy, head.flags, head.price,
                        head.amount, head.minAmount, head.epochId,
                        head.clientOrderId
                    );
                    _notifyRejectedPlace(doneHash, op, doneNonce);
                } else {
                    // onOrderPlaced fired (precheck started, didn't finish) →
                    // must cancel symmetrically so _openOrderCount decrements.
                    address callerPn = DexLib.computePrivateNoteAddressFromHash(_privateNoteCodeHash, _privateNoteCodeDepth, doneHash
                    );
                    uint128 returnAmt = _collateralFor(
                        head.isBuy, head.price, head.amount
                    );
                    _cancelNoBookEntryTo(
                        callerPn, doneHash, head.targetOrderId,
                        head.outcomeId, head.isBuy, returnAmt, head.clientOrderId,
                        doneNonce
                    );
                }
            }
            bool wasBatchEnd = head.isBatchEnd;
            _advanceHead();
            if (wasBatchEnd) _notifyBatchAccepted(doneHash, doneNonce);
            if (_queueSize > 0) {
                _selfCallProcessHead();
            } else {
                OrderBook(address(this)).shutdown{
                    value: 1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
                }();
            }
            return;
        }

        bool keepHead = false;

        if (head.entryType == QENTRY_CANCEL) {
            _doCancel(head.depositHash, head.targetOrderId, head.opNonce);
        } else if (head.entryType == QENTRY_CANCEL_ALL) {
            uint32 cancelled = _doCancelAll(head.depositHash, head.opNonce);
            keepHead = (cancelled >= MAX_CANCEL_ALL_PER_CALL);
        } else {
            PlaceParams p = PlaceParams(
                head.outcomeId, head.isBuy, head.flags, head.price,
                head.amount, head.minAmount, head.epochId, head.clientOrderId
            );
            (bool cont, uint128 contOrderId, uint128 contRemaining,
             bool ctPrecheckDone, uint128 ctPrecheckAccum, uint256 ctPrecheckLastPrice,
             uint128 ctFilledAccum) =
                _doPlace(
                    head.depositHash, p, head.targetOrderId,
                    head.precheckDone, head.precheckAccum, head.precheckLastPrice,
                    head.filledAccum, head.opNonce
                );
            if (cont) {
                _queue[_queueHead].amount = contRemaining;
                _queue[_queueHead].targetOrderId = contOrderId;
                _queue[_queueHead].precheckDone = ctPrecheckDone;
                _queue[_queueHead].precheckAccum = ctPrecheckAccum;
                _queue[_queueHead].precheckLastPrice = ctPrecheckLastPrice;
                _queue[_queueHead].filledAccum = ctFilledAccum;
                keepHead = true;
            }
        }

        if (!keepHead) {
            uint256 advancedHash = head.depositHash;
            uint64  advancedNonce = head.opNonce;
            bool wasBatchEnd = head.isBatchEnd;
            _advanceHead();
            if (wasBatchEnd) _notifyBatchAccepted(advancedHash, advancedNonce);
        }
        if (_queueSize > 0) {
            _selfCallProcessHead();
        } else if (_shutdownPending) {
            // Queue drained, deferred shutdown promotes now (claim() gate).
            OrderBook(address(this)).shutdown{
                value: 1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }();
        }
    }

    // ===== Native matching engine =====

    /// @notice Place a single order, optionally as a continuation.
    ///         This function must NEVER revert — it runs from _processHeadCore
    ///         after a queue entry has been committed. A revert here would
    ///         abort every future _processHeadCore tx and permanently wedge
    ///         the queue (all subsequent executeBatch/cancel operations would
    ///         also revert because they end with _processHeadCore).
    ///         Input validity is enforced at queue-insertion time in
    ///         executeBatch (the boundary).
    ///
    ///         The function has two continuation modes, both signaled by
    ///         returning cont=true:
    ///         1. Pre-check continuation (precheckDone=false): the FOK /
    ///            minAmount level walk hit MAX_PRECHECK_ITERATIONS without
    ///            reaching its target. accum + lastPrice are the resume
    ///            cursor for the next pass.
    ///         2. Match continuation (precheckDone=true): pre-check is done,
    ///            the match loop hit MAX_MATCHES_PER_CALL with leftover.
    ///            contRemaining + contOrderId are the resume cursor.
    function _doPlace(
        uint256 callerHash,
        PlaceParams p,
        uint128 existingOrderId,
        bool    inPrecheckDone,
        uint128 inPrecheckAccum,
        uint256 inPrecheckLastPrice,
        uint128 inFilledAccum,
        uint64  opNonce
    ) private returns (
        bool cont,
        uint128 contOrderId,
        uint128 contRemaining,
        bool contPrecheckDone,
        uint128 contPrecheckAccum,
        uint256 contPrecheckLastPrice,
        uint128 contFilledAccum
    ) {
        bool isContinuation = existingOrderId != 0;
        // Taker-side fill aggregator (carried across continuations via the
        // queue entry). Used to emit a single PartialFill / FullyFilled event
        // when matching for this order is fully done.
        uint128 takerFilledAccum = inFilledAccum;

        uint8 fl = p.flags;
        bool isMarket = (fl & FLAG_MARKET) != 0;
        bool isIoc    = (fl & FLAG_IOC) != 0;
        bool isFok    = (fl & FLAG_FOK) != 0;
        bool isPostOnly = (fl & FLAG_POST_ONLY) != 0;

        uint256 storedPrice;
        if (isMarket) {
            storedPrice = p.isBuy ? type(uint256).max : 0;
        } else {
            storedPrice = p.price;
        }

        uint128 orderId = isContinuation ? existingOrderId : _nextOrderId++;

        // Compute caller PN address once and reuse across all callbacks.
        address callerPn = DexLib.computePrivateNoteAddressFromHash(_privateNoteCodeHash, _privateNoteCodeDepth, callerHash);

        if (!isContinuation) _emitOrderPlacedTo(callerPn, callerHash, orderId, p, opNonce);

        // ── POST_ONLY check: only on first call. (Cheap — single _bestOpposite.)
        if (!isContinuation && isPostOnly) {
            optional(uint256, PriceLevel) bestLevel = _bestOpposite(p.outcomeId, p.isBuy, p.epochId);
            if (bestLevel.hasValue()) {
                (uint256 bp, ) = bestLevel.get();
                if (_pricesCross(p.isBuy, storedPrice, false, bp)) {
                    uint128 returnAmt = _collateralFor(p.isBuy, storedPrice, p.amount);
                    _cancelNoBookEntryTo(callerPn, callerHash, orderId, p.outcomeId, p.isBuy, returnAmt, p.clientOrderId, opNonce);
                    return (false, 0, 0, false, 0, 0, 0);
                }
            }
        }

        // ── FOK / minAmount pre-check (resumable, capped at MAX_PRECHECK_ITERATIONS).
        //    On the first call inPrecheckDone is false and the cursor is (0, 0).
        //    On a pre-check continuation we resume from inPrecheckAccum / inPrecheckLastPrice.
        //    Once pre-check passes (or there's no minFill), inPrecheckDone is true on
        //    subsequent (match) continuations and this block is skipped entirely.
        uint128 minFill = isFok ? p.amount : p.minAmount;
        if (minFill > 0 && !inPrecheckDone) {
            uint128 accum = inPrecheckAccum;
            uint256 lastPrice = inPrecheckLastPrice;
            optional(uint256, PriceLevel) pcIter;
            // Resume from where we left off (inPrecheckAccum > 0 means we've walked at least one level).
            if (inPrecheckAccum == 0) {
                pcIter = _bestOpposite(p.outcomeId, p.isBuy, p.epochId);
            } else {
                pcIter = _nextOpposite(p.outcomeId, p.isBuy, p.epochId, lastPrice);
            }
            uint32 walked = 0;
            bool reached = false;
            while (pcIter.hasValue()) {
                (uint256 pcLevelPrice, PriceLevel pcLvl) = pcIter.get();
                if (!_pricesCross(p.isBuy, storedPrice, isMarket, pcLevelPrice)) {
                    // No more crossing levels in our epoch.
                    break;
                }
                accum += pcLvl.totalAmount;
                lastPrice = pcLevelPrice;
                walked++;
                if (accum >= minFill) {
                    reached = true;
                    break;
                }
                if (walked >= MAX_PRECHECK_ITERATIONS) {
                    // Out of budget for this tx — yield and resume next pass.
                    return (true, orderId, p.amount, false, accum, lastPrice, takerFilledAccum);
                }
                pcIter = _nextOpposite(p.outcomeId, p.isBuy, p.epochId, pcLevelPrice);
            }
            if (!reached) {
                // Walked the full crossing region but didn't reach minFill → reject.
                uint128 returnAmt = _collateralFor(p.isBuy, storedPrice, p.amount);
                _cancelNoBookEntryTo(callerPn, callerHash, orderId, p.outcomeId, p.isBuy, returnAmt, p.clientOrderId, opNonce);
                return (false, 0, 0, false, 0, 0, 0);
            }
            // Pre-check passed: fall through into the match loop.
        }

        // ── Match: walk levels best-first within our epoch, FIFO inside each ──
        uint128 remaining = p.amount;
        uint8 matchesDone = 0;
        bool hitCapWithMore = false;
        // Set when MARKET BUY remaining quote can't afford one base unit at
        // the current clearingPrice. Subsequent levels are strictly worse
        // (outer walks best-first), so we stop the outer walk too.
        bool marketBuyDust = false;

        optional(uint256, PriceLevel) iter = _bestOpposite(p.outcomeId, p.isBuy, p.epochId);
        while (iter.hasValue() && remaining > 0) {
            (uint256 levelPrice, PriceLevel lvl) = iter.get();
            if (!_pricesCross(p.isBuy, storedPrice, isMarket, levelPrice)) break;

            uint128 cur = lvl.firstOrderId;
            while (cur != 0 && remaining > 0) {
                if (matchesDone >= MAX_MATCHES_PER_CALL) {
                    hitCapWithMore = true;
                    break;
                }
                Order cp = _orders[cur];
                uint128 nextOrd = cp.nextAtPrice;

                uint256 clearingPrice = levelPrice;

                // Compute trade (base units).
                //
                // LIMIT BUY / any SELL: remaining is already in base; trade = min.
                //
                // MARKET BUY: remaining is in QUOTE (lock = amount, not scaled
                //   by price). Cap trade by what the remaining quote can
                //   afford at this clearingPrice. Without this cap, an ask at
                //   clearingPrice > FULL_PERCENT produces actualCost >
                //   remaining and the seller's credited proceeds
                //   (= filled * clearingPrice / FULL_PERCENT) exceed the
                //   buyer's lock, inflating the sum of PN `_balance` across
                //   the two sides and letting the seller later drain pool
                //   share beyond the quote that physically entered the
                //   trade (breaks the conservation invariant vs
                //   RootPN.currencies / _deployedValues).
                uint128 trade;
                if (p.isBuy && isMarket) {
                    uint128 affordBase = clearingPrice > 0
                        ? uint128((uint256(remaining) * uint256(FULL_PERCENT)) / clearingPrice)
                        : cp.amount;
                    trade = cp.amount < affordBase ? cp.amount : affordBase;
                    if (trade == 0) {
                        // Remaining quote can't cover one base unit at this
                        // clearingPrice. Subsequent levels are worse (outer
                        // loop walks best-first) — flag to stop the outer
                        // walk too. (Previously this `break` only exited the
                        // inner loop and the outer kept scanning worse
                        // levels for nothing.)
                        marketBuyDust = true;
                        break;
                    }
                } else {
                    trade = remaining < cp.amount ? remaining : cp.amount;
                }

                // Quote actually spent this fill (market-buy only).
                uint128 spentQuote = 0;
                if (p.isBuy && isMarket) {
                    spentQuote = uint128((uint256(trade) * clearingPrice) / uint256(FULL_PERCENT));
                }

                uint128 newBuyerRefund = 0;
                if (p.isBuy) {
                    if (isMarket) {
                        // Market-buy: trade already capped by affordBase so
                        // spentQuote ≤ remaining. Buyer pays exactly
                        // spentQuote per fill → no per-fill refund. Unused
                        // quote at end of match is returned in bulk via the
                        // IOC/FOK/MARKET cancel-no-book branch below.
                        newBuyerRefund = 0;
                    } else {
                        uint256 diff = storedPrice > clearingPrice ? storedPrice - clearingPrice : 0;
                        newBuyerRefund = uint128((uint256(trade) * diff) / FULL_PERCENT);
                    }
                }
                uint128 cpBuyerRefund = 0;
                if (!p.isBuy) {
                    uint256 diff = cp.price > clearingPrice ? cp.price - clearingPrice : 0;
                    cpBuyerRefund = uint128((uint256(trade) * diff) / FULL_PERCENT);
                }

                // isFinal: true when this fill completes the order on that side.
                //   Maker:  resting order fully consumed  → cp.amount == trade.
                //   Taker:  for MARKET BUY, when spentQuote == remaining
                //           (all locked quote consumed by this fill).
                //           For LIMIT BUY / SELL, when remaining (base) ==
                //           trade (base) as before.
                bool makerFinal = (cp.amount == trade);
                bool takerFinal = (p.isBuy && isMarket) ? (spentQuote == remaining) : (remaining == trade);
                // One match_id per match — both legs (maker + taker) carry it.
                _matchSeq += 1;
                uint64 matchId = _matchSeq;
                // Maker callback: maker's address differs per fill, must compute.
                _processFill(cp.depositHash, cur, p.outcomeId, trade, clearingPrice, cp.isBuy, cpBuyerRefund, false, makerFinal, cp.clientOrderId, matchId);
                // Taker callback: reuse cached caller address.
                _processFillTo(callerPn, orderId, p.outcomeId, trade, clearingPrice, p.isBuy, newBuyerRefund, true, takerFinal, p.clientOrderId, matchId, callerHash);

                // Maker-side aggregated MM event — emitted right at the fill
                // (maker is touched at most once per overall taker placement).
                if (makerFinal) {
                    _emitFullyFilled(cur, cp.clientOrderId, _orders[cur].filledAccum + trade);
                } else {
                    _emitPartialFill(cur, cp.clientOrderId, trade, cp.amount - trade);
                }

                // Track maker fill total on the resting order itself (kept on
                // the Order so future taker matches against the same maker
                // accumulate cleanly).
                _orders[cur].filledAccum += trade;
                takerFilledAccum += trade;

                if (cp.amount == trade) {
                    _removeFromBook(cur);
                } else {
                    _orders[cur].amount = cp.amount - trade;
                    _levels[p.outcomeId][cp.isBuy][cp.epochId][cp.price].totalAmount -= trade;
                }

                // Advance taker cursor in the taker's native unit:
                //   MARKET BUY → quote spent this fill.
                //   LIMIT BUY / SELL → base traded this fill.
                if (p.isBuy && isMarket) {
                    remaining = remaining > spentQuote ? remaining - spentQuote : 0;
                } else {
                    remaining -= trade;
                }
                matchesDone++;
                cur = nextOrd;
            }
            if (hitCapWithMore) break;
            if (marketBuyDust) break;

            // Move to next level in our epoch (worse price). Re-fetch since storage may have changed.
            iter = _nextOpposite(p.outcomeId, p.isBuy, p.epochId, levelPrice);
        }

        if (hitCapWithMore) {
            // Match continuation: pre-check is done; carry over remaining + orderId + accum.
            return (true, orderId, remaining, true, 0, 0, takerFilledAccum);
        }

        // No post-match minAmount check: the FOK / minAmount pre-check above
        // already verified totalAvailable >= minFill against the crossing price
        // levels in our epoch. Queue serialization (we hold the head across
        // continuations, no other place/cancel runs in between) guarantees that
        // the available liquidity does not shrink before we consume it. Levels
        // are now keyed by epochId, so foreign-epoch orders cannot inflate
        // totalAmount nor force extra skip walks.

        if (remaining > 0) {
            if (isIoc || isFok || isMarket) {
                uint128 returnAmt = (isMarket && p.isBuy) ? remaining : _collateralFor(p.isBuy, storedPrice, remaining);
                _cancelNoBookEntryTo(callerPn, callerHash, orderId, p.outcomeId, p.isBuy, returnAmt, p.clientOrderId, opNonce);
            } else {
                _insertIntoBook(orderId, callerHash, p, storedPrice, remaining, takerFilledAccum, opNonce);
            }
        }

        // Taker-side aggregated MM event — emitted ONCE after all matching
        // (across continuations) is done. PartialFill if leftover remains
        // (book or cancelled); FullyFilled if the order was fully consumed.
        if (takerFilledAccum > 0) {
            if (remaining == 0) {
                _emitFullyFilled(orderId, p.clientOrderId, takerFilledAccum);
            } else {
                _emitPartialFill(orderId, p.clientOrderId, takerFilledAccum, remaining);
            }
        }
        // cid lifecycle is owned by PN — nothing to clean up here.
        return (false, 0, 0, false, 0, 0, 0);
    }

    function _doCancel(uint256 callerHash, uint128 orderId, uint64 opNonce) private {
        Order o = _orders[orderId];
        if (o.amount == 0 && o.depositHash == 0) return;
        if (o.depositHash != callerHash) return;
        uint128 returnAmt = _collateralFor(o.isBuy, o.price, o.amount);
        uint32 outcomeId = o.outcomeId;
        bool isBuy = o.isBuy;
        uint128 cid = o.clientOrderId;
        _removeFromBook(orderId);
        _emitOrderCancelled(orderId, cid);
        _notifyOrderCancelled(callerHash, orderId, outcomeId, isBuy, returnAmt, cid, opNonce);
    }

    function _doCancelAll(uint256 callerHash, uint64 opNonce) private returns (uint32 cancelled) {
        cancelled = 0;
        uint128 cur = _ownerHead[callerHash];
        while (cur != 0 && cancelled < MAX_CANCEL_ALL_PER_CALL) {
            Order o = _orders[cur];
            uint128 next = o.nextInOwner;
            uint128 returnAmt = _collateralFor(o.isBuy, o.price, o.amount);
            uint32 outcomeId = o.outcomeId;
            bool isBuy = o.isBuy;
            uint128 cid = o.clientOrderId;
            _removeFromBook(cur);
            _emitOrderCancelled(cur, cid);
            _notifyOrderCancelled(callerHash, cur, outcomeId, isBuy, returnAmt, cid, opNonce);
            cancelled++;
            cur = next;
        }
    }

    // ===== Level navigation =====

    /// @notice Best price level on the opposite side within the taker's epoch
    ///         (asks for buyers, bids for sellers). Levels of other epochs are
    ///         in disjoint storage subtrees and are never visited.
    function _bestOpposite(uint32 outcomeId, bool newIsBuy, uint64 epochId) private view returns (optional(uint256, PriceLevel)) {
        if (newIsBuy) {
            return _levels[outcomeId][false][epochId].min();
        }
        return _levels[outcomeId][true][epochId].max();
    }

    /// @notice Next price level on the opposite side after `currentPrice` within
    ///         the taker's epoch (worse direction).
    function _nextOpposite(uint32 outcomeId, bool newIsBuy, uint64 epochId, uint256 currentPrice) private view returns (optional(uint256, PriceLevel)) {
        if (newIsBuy) {
            return _levels[outcomeId][false][epochId].next(currentPrice);
        }
        return _levels[outcomeId][true][epochId].prev(currentPrice);
    }

    function _pricesCross(
        bool newIsBuy, uint256 newPrice, bool newIsMarket, uint256 cpPrice
    ) private pure returns (bool) {
        if (newIsMarket) return true;
        if (newIsBuy) return newPrice >= cpPrice;
        return cpPrice >= newPrice;
    }

    function _collateralFor(bool isBuy, uint256 price, uint128 amount) private pure returns (uint128) {
        if (isBuy) {
            return uint128((uint256(amount) * price) / uint256(FULL_PERCENT));
        }
        return amount;
    }

    // ===== Book mutation =====

    function _insertIntoBook(
        uint128 orderId,
        uint256 callerHash,
        PlaceParams p,
        uint256 storedPrice,
        uint128 amount,
        uint128 priorFilledAccum,
        uint64  opNonce
    ) private {
        uint128 oTail = _ownerTail[callerHash];
        PriceLevel level = _levels[p.outcomeId][p.isBuy][p.epochId][storedPrice];
        uint128 priceTail = level.lastOrderId;

        _orders[orderId] = Order({
            depositHash: callerHash,
            price: storedPrice,
            amount: amount,
            minAmount: 0,
            initialAmount: p.amount,
            filledAccum: priorFilledAccum,
            clientOrderId: p.clientOrderId,
            epochId: p.epochId,
            opNonce: opNonce,
            outcomeId: p.outcomeId,
            flags: p.flags,
            isBuy: p.isBuy,
            nextAtPrice: 0,
            prevAtPrice: priceTail,
            nextInOwner: 0,
            prevInOwner: oTail
        });

        // cid lifecycle is owned by PN; OB just stores it on the Order for
        // event emission.
        callerHash;

        if (priceTail == 0) {
            // First order at this (epoch, price) level
            _levels[p.outcomeId][p.isBuy][p.epochId][storedPrice] = PriceLevel({
                firstOrderId: orderId,
                lastOrderId: orderId,
                totalAmount: amount
            });
        } else {
            _orders[priceTail].nextAtPrice = orderId;
            level.lastOrderId = orderId;
            level.totalAmount += amount;
            _levels[p.outcomeId][p.isBuy][p.epochId][storedPrice] = level;
        }

        if (oTail == 0) {
            _ownerHead[callerHash] = orderId;
        } else {
            _orders[oTail].nextInOwner = orderId;
        }
        _ownerTail[callerHash] = orderId;

        _orderCount++;
    }

    function _removeFromBook(uint128 orderId) private {
        Order o = _orders[orderId];

        // Idempotency guard: an already-removed / never-existed slot is all-zero.
        // Without this, a double _removeFromBook would run the unconditional
        // _orderCount-- below twice for one order → uint32 underflow → _orderCount
        // stuck at ~4.29e9 → shutdown's drain could never terminate (queue flood).
        if (o.depositHash == 0 && o.amount == 0) { return; }

        // cid lifecycle is owned by PN — OB does not track cid → orderId
        // anymore, the corresponding cleanup happens on PN side via
        // onOrderCancelled / onOrderFilled (isFinal) callbacks.

        // Price-level FIFO (within order's epoch)
        uint128 prevP = o.prevAtPrice;
        uint128 nextP = o.nextAtPrice;
        PriceLevel level = _levels[o.outcomeId][o.isBuy][o.epochId][o.price];

        if (prevP == 0) {
            level.firstOrderId = nextP;
        } else {
            _orders[prevP].nextAtPrice = nextP;
        }
        if (nextP == 0) {
            level.lastOrderId = prevP;
        } else {
            _orders[nextP].prevAtPrice = prevP;
        }
        if (level.totalAmount >= o.amount) {
            level.totalAmount -= o.amount;
        } else {
            level.totalAmount = 0;
        }

        if (level.firstOrderId == 0) {
            delete _levels[o.outcomeId][o.isBuy][o.epochId][o.price];
        } else {
            _levels[o.outcomeId][o.isBuy][o.epochId][o.price] = level;
        }

        // Owner FIFO
        uint128 oPrev = o.prevInOwner;
        uint128 oNext = o.nextInOwner;
        if (oPrev == 0) {
            _ownerHead[o.depositHash] = oNext;
        } else {
            _orders[oPrev].nextInOwner = oNext;
        }
        if (oNext == 0) {
            _ownerTail[o.depositHash] = oPrev;
        } else {
            _orders[oNext].prevInOwner = oPrev;
        }

        delete _orders[orderId];
        _orderCount--;
    }

    // ===== Effect emit & callbacks =====

    /// @notice Variant that takes the resolved PN address directly. Used by
    ///         _doPlace to avoid recomputing the caller's PN address per fill.
    function _emitOrderPlacedTo(address pn, uint256 depositHash, uint128 orderId, PlaceParams p, uint64 opNonce) private view {
        uint128 feeReserve = 0;
        uint128 lock = 0;
        if (p.isBuy) {
            uint128 cost = (p.flags & FLAG_MARKET != 0)
                ? p.amount
                : uint128((uint256(p.amount) * p.price) / uint256(FULL_PERCENT));
            feeReserve = uint128((uint256(cost) * uint256(TAKER_FEE_RATE)) / uint256(FEE_DENOMINATOR));
            // Full buy-side lock = cost + feeReserve. Authoritative value so
            // PN can track per-order lock and drain floor-accumulated
            // residuals back to `_balance` on isFinal fill or cancel.
            lock = cost + feeReserve;
        }
        address addrExtern = address.makeAddrExtern(OB_ORDER_PLACED, bitCntAddress);
        emit OrderPlaced{dest: addrExtern}(orderId, p.outcomeId, p.isBuy, p.flags, p.price, p.amount, p.clientOrderId, depositHash, opNonce);
        PrivateNote(pn).onOrderPlaced{
            value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType, orderId, feeReserve, lock, p.clientOrderId, p.outcomeId, p.isBuy, p.flags, p.price, p.amount, opNonce);
    }

    function _emitOrderCancelled(uint128 orderId, uint128 clientOrderId) private pure {
        address addrExtern = address.makeAddrExtern(OB_ORDER_CANCELLED, bitCntAddress);
        emit OrderCancelled{dest: addrExtern}(orderId, clientOrderId);
    }

    function _notifyOrderCancelled(
        uint256 callerHash, uint128 orderId, uint32 outcomeId, bool isBuy, uint128 returnAmt, uint128 clientOrderId, uint64 opNonce
    ) private view {
        address pn = DexLib.computePrivateNoteAddressFromHash(_privateNoteCodeHash, _privateNoteCodeDepth, callerHash);
        _notifyOrderCancelledTo(pn, orderId, outcomeId, isBuy, returnAmt, clientOrderId, opNonce);
    }

    function _notifyOrderCancelledTo(
        address pn, uint128 orderId, uint32 outcomeId, bool isBuy, uint128 returnAmt, uint128 clientOrderId, uint64 opNonce
    ) private view {
        PrivateNote(pn).onOrderCancelled{
            value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType, orderId, outcomeId, isBuy, returnAmt, clientOrderId, opNonce);
    }

    function _cancelNoBookEntryTo(
        address pn, uint256 callerHash, uint128 orderId, uint32 outcomeId, bool isBuy,
        uint128 returnAmt, uint128 clientOrderId, uint64 opNonce
    ) private view {
        callerHash;
        _emitOrderCancelled(orderId, clientOrderId);
        _notifyOrderCancelledTo(pn, orderId, outcomeId, isBuy, returnAmt, clientOrderId, opNonce);
    }

    /// @notice Aggregated fill events for MM-friendly subscribers. Emitted
    ///         once per order completion (PartialFill on any leftover,
    ///         FullyFilled on full consumption). Per-fill `OrderFilled`
    ///         continues to fire alongside for raw analytics.
    function _emitPartialFill(uint128 orderId, uint128 clientOrderId, uint128 filledAmount, uint128 remainingAmount) private pure {
        address addrExtern = address.makeAddrExtern(OB_PARTIAL_FILL, bitCntAddress);
        emit PartialFill{dest: addrExtern}(orderId, clientOrderId, filledAmount, remainingAmount);
    }

    function _emitFullyFilled(uint128 orderId, uint128 clientOrderId, uint128 filledAmount) private pure {
        address addrExtern = address.makeAddrExtern(OB_FULLY_FILLED, bitCntAddress);
        emit FullyFilled{dest: addrExtern}(orderId, clientOrderId, filledAmount);
    }

    function _processFill(
        uint256 pnHash,
        uint128 orderId,
        uint32  outcomeId,
        uint128 filledAmount,
        uint256 clearingPrice,
        bool    isBuy,
        uint128 buyerRefund,
        bool    isTaker,
        bool    isFinal,
        uint128 clientOrderId,
        uint64  matchId
    ) private {
        address pn = DexLib.computePrivateNoteAddressFromHash(_privateNoteCodeHash, _privateNoteCodeDepth, pnHash);
        _processFillTo(pn, orderId, outcomeId, filledAmount, clearingPrice, isBuy, buyerRefund, isTaker, isFinal, clientOrderId, matchId, pnHash);
    }

    function _processFillTo(
        address pn,
        uint128 orderId,
        uint32  outcomeId,
        uint128 filledAmount,
        uint256 clearingPrice,
        bool    isBuy,
        uint128 buyerRefund,
        bool    isTaker,
        bool    isFinal,
        uint128 clientOrderId,
        uint64  matchId,
        uint256 depositHash
    ) private {
        uint128 notional = uint128(
            (uint256(filledAmount) * clearingPrice) / uint256(FULL_PERCENT)
        );
        // Taker pays the full fee on each fill. Of that fee, MAKER_REBATE_NUM /
        // MAKER_REBATE_DEN (75%) is rebated to the matched maker; the rest
        // (25%) is retained as protocol revenue. Maker pays nothing.
        uint128 takerFee = uint128(
            (uint256(notional) * uint256(TAKER_FEE_RATE)) / uint256(FEE_DENOMINATOR)
        );
        uint128 makerRebate = uint128(
            (uint256(takerFee) * uint256(MAKER_REBATE_NUM)) / uint256(MAKER_REBATE_DEN)
        );

        // `feeAmount` semantics depend on `isTaker`:
        //   isTaker=true  → fee debited from taker's reserve / proceeds.
        //   isTaker=false → rebate credited to the maker's balance.
        uint128 feeAmount;
        if (isTaker) {
            feeAmount = takerFee;
            // Only the protocol share stays in the contract; the rebate share
            // leaves with the maker on the paired _processFill call.
            _totalProtocolFees += takerFee - makerRebate;
        } else {
            feeAmount = makerRebate;
            _totalMakerRebatesPaid += makerRebate;
        }

        address addrExtern = address.makeAddrExtern(OB_ORDER_FILLED, bitCntAddress);
        emit OrderFilled{dest: addrExtern}(orderId, filledAmount, clearingPrice, feeAmount, isTaker, matchId, depositHash);

        PrivateNote(pn).onOrderFilled{
            value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType, outcomeId, filledAmount, clearingPrice, isBuy, buyerRefund, feeAmount, !isTaker, orderId, isFinal, _numOutcomes, clientOrderId);
    }

    // ===== Getters =====

    function getDetails() external view returns (
        uint256 eventId,
        uint256 oracleListHash,
        uint32  tokenType,
        uint128 nextOrderId,
        uint128 orderCount,
        uint128 totalMakerRebatesPaid,
        uint128 totalProtocolFees
    ) {
        return (
            _eventId,
            _oracleListHash,
            _tokenType,
            _nextOrderId,
            uint128(_orderCount),
            _totalMakerRebatesPaid,
            _totalProtocolFees
        );
    }

    function getQueueSize() external view returns (uint8 size) {
        return _queueSize;
    }

    /// @notice Shutdown-state getter for tests/monitoring.
    /// @return shuttingDown True once the drain has started.
    /// @return shutdownPending True once shutdown was queued behind the
    ///         active-order queue but has not yet been promoted.
    function getShutdownState() external view returns (bool shuttingDown, bool shutdownPending) {
        return (_shuttingDown, _shutdownPending);
    }

    function getOrder(uint128 orderId) external view returns (
        uint256 depositIdentifierHash,
        uint32  outcomeId,
        bool    isBuy,
        uint8   flags,
        uint256 price,
        uint128 amount,
        uint128 minAmount,
        uint64  epochId
    ) {
        Order o = _orders[orderId];
        if (o.amount == 0 && o.depositHash == 0) revert(ERR_ORDER_NOT_FOUND);
        return (
            o.depositHash, o.outcomeId, o.isBuy, o.flags,
            o.price, o.amount, o.minAmount, o.epochId
        );
    }

    function getOrdersByOwner(uint256 depositHash) external view returns (
        uint128[] orderIds,
        uint32[]  outcomeIds,
        bool[]    isBuys,
        uint256[] prices,
        uint128[] amounts,
        uint64[]  epochIds,
        uint128[] clientOrderIds
    ) {
        uint128 cur = _ownerHead[depositHash];
        while (cur != 0) {
            Order o = _orders[cur];
            orderIds.push(cur);
            outcomeIds.push(o.outcomeId);
            isBuys.push(o.isBuy);
            prices.push(o.price);
            amounts.push(o.amount);
            epochIds.push(o.epochId);
            clientOrderIds.push(o.clientOrderId);
            cur = o.nextInOwner;
        }
    }

    // ===== Shutdown =====

    function shutdown() public {
        require(msg.sender == _pmpAddress || msg.sender == address(this), ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();

        // If an order is mid-execution (queue still has entries being matched
        // across self-calls) defer the drain — the in-flight order must be
        // allowed to finish before we start cancelling. The next
        // `_processHeadCore` pass that empties the queue promotes this latch
        // into a real shutdown self-call. Self-calls during drain bypass this
        // guard via `_shuttingDown` which is already set.
        if (_queueSize > 0 && !_shuttingDown) {
            _shutdownPending = true;
            return;
        }

        // Latch shutdown on the first call so no new place/cancel may slip in
        // while the contract is draining its order map across self-call passes.
        _shuttingDown = true;
        _shutdownPending = false;

        uint128[] toCancel;
        uint128 i = _shutdownCursor == 0 ? 1 : _shutdownCursor;
        uint128 start = i;
        while (i < _nextOrderId && (i - start) < MAX_SHUTDOWN_BATCH) {
            if (_orders[i].amount > 0) {
                toCancel.push(i);
            }
            i++;
        }
        _shutdownCursor = i;

        for (uint k = 0; k < toCancel.length; k++) {
            uint128 oid = toCancel[k];
            Order o = _orders[oid];
            uint128 returnAmt = _collateralFor(o.isBuy, o.price, o.amount);
            uint256 pnHash = o.depositHash;
            uint32 outcomeId = o.outcomeId;
            bool isBuy = o.isBuy;
            uint128 cid = o.clientOrderId;
            _removeFromBook(oid);
            _emitOrderCancelled(oid, cid);
            // opNonce=0: recipient maker isn't currently busy on THIS event from
            // their POV — guard their PN `_busy` from accidental clearing.
            _notifyOrderCancelled(pnHash, oid, outcomeId, isBuy, returnAmt, cid, 0);
        }

        // Terminate the drain on the strictly-advancing scan cursor, NOT on the
        // derived _orderCount: a counter corruption (e.g. an underflow) must
        // never wedge this into a perpetual 1-vmshell self-call (queue flood).
        // _shutdownCursor only ever increases and is bounded by the frozen
        // _nextOrderId, so this is guaranteed to terminate.
        if (_shutdownCursor < _nextOrderId) {
            OrderBook(address(this)).shutdown{
                value: 1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }();
        } else {
            // Report accumulated protocol fees to RootPN before teardown. The
            // backing real ECC is already custodied by RootPN; this only marks
            // the amount as owner-withdrawable. Sent before the destroy below.
            if (_totalProtocolFees > 0) {
                RootPN(ROOT_PN_ADDRESS).collectProtocolFee{
                    value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
                }(_eventId, _oracleListHash, _tokenType, _totalProtocolFees);
                _totalProtocolFees = 0;
            }
            // flag 161 = 128 (carry all remaining balance) + 32 (destroy source
            // once balance hits zero) + 1 (pay msg forward fees separately).
            // Notifies the PMP that the drain is complete and tears down this
            // OrderBook in a single action; any residual balance lands in PMP.
            PMP(_pmpAddress).onOrderBookShutdownComplete{
                value: 0, flag: 161, dest_dapp_id: ROOT_PN_DAPP_ID
            }();
        }
    }

    /// @notice Observability hook for bounced outgoing callbacks. We deliberately
    ///         don't try to auto-recover state here — order/queue mutations done
    ///         by the dispatch are committed and a bounce arrives in a later block.
    ///         External monitors must reconcile the affected PN. Without this hook
    ///         the bounce would silently be a no-op; here it surfaces as an event.
    onBounce(TvmSlice body) external pure {
        body;
        address addrExtern = address.makeAddrExtern(OB_CALLBACK_BOUNCED, bitCntAddress);
        emit CallbackBounced{dest: addrExtern}(msg.sender, tx.logicaltime);
    }

    function getVersion() external pure returns (string, string) {
        return (version, "OrderBook");
    }
}
