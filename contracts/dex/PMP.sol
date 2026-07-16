pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./PrivateNote.sol";
import "./OracleEventList.sol";
import "./OrderBook.sol";
import "./libraries/DexLib.sol";

/// @title PMP - Pari Mutuel Pool
/// @notice PMP contract with creator fee, requires oracle approval
contract PMP is Modifiers {

    /// @notice Contract semantic version.
    string constant version = "4.0.27";

    /// @notice PMP name (static, unique identifier)
    string _name;

    /// @notice Describe of event
    string _describe;

    /// @notice Token type (static)
    uint32 static _tokenType;

    /// @notice Event identifier
    uint256 static _eventId;

    /// @notice Oracle list hash (static)
    uint256 static _oracleListHash;

    /// @notice Contract deployer (PrivateNote address)
    address _deployer;

    /// @notice PrivateNote code for address computation
    TvmCell _privateNoteCode;

    /// @notice Total pool of all stakes
    uint128 _totalPool;

    /// @notice Pools separated by bet type
    /// @dev Structure: outcome => betType => poolAmount
    mapping(uint32 => mapping(uint8 => uint128)) _typedOutcomePools;

    /// @notice Abandoned (forfeited) mass per outcome/bet type. Incremented by
    ///         `forfeitStake` in EVERY lifecycle state; the live pools and the
    ///         close counters are never decremented by a forfeit. Instead each
    ///         close condition compares the remaining (unclaimed / un-refunded)
    ///         mass against this total — forfeited mass can never be claimed
    ///         (the PN deletes its record), so the remainder asymptotes to it.
    /// @dev Structure: outcome => betType => forfeitedAmount
    mapping(uint32 => mapping(uint8 => uint128)) _forfeited;

    /// @notice Stake counts separated by bet type
    /// @dev Structure: outcome => betType => stakeCount
    mapping(uint32 => mapping(uint8 => uint128)) _typedOutcomeCounts;

    /// @notice Total coupon pool across all outcomes
    /// @dev Used to enforce COUPON_POOL_LIMIT_PERCENT
    uint128 _totalCouponPool;

    /// @notice Total pool for clean bets
    uint128 _totalCleanPool;

    /// @notice Total pool for debt bets
    uint128 _totalDebtPool;

    /// @notice Coefficient for calculating coupon winnings, set at resolution
    uint128 _couponWinCoef;

    /// @notice Coefficient for calculating debt winnings, set at resolution
    uint128 _debtWinCoef;

    /// @notice Total winnings from the winning outcome, used for payout calculations
    uint128 _totalWinPool;

    /// @notice Profit allocated to clean bets after resolve
    uint128 _profitToClean;

    /// @notice Remaining claimable budget for clean-bet winners (principal + profit).
    uint128 _totalRewardsClean;

    /// @notice Remaining claimable budget for debt-bet winners (principal + profit).
    uint128 _totalRewardsDebt;

    /// @notice Remaining claimable budget for coupon-bet winners (profit only).
    uint128 _totalRewardsCoupon;

    /// @notice Creator fee collected at resolve, sent to _deployer
    uint128 _creatorFee;

    /// @notice Finalized outcome
    optional(uint32) _resolvedOutcome;

    /// @notice Approval flag from oracle
    bool _approved;

    /// @notice Number of outcome intervals (set by oracle)
    uint32 _numOutcomes;

    /// @notice Outcome names
    mapping(uint32 => string) _outcomeNames;

    /// ===== Time windows (set by oracle) =====

    /// @notice Stake acceptance start time (set once, immutable after start)
    uint64 _stakeStart;

    /// @notice Result acceptance start time (resolve deadline = _resultStart + GRACE_PERIOD)
    uint64 _resultStart;

    /// @notice Cancellation flag
    bool _isCancelled;

    /// @notice Mapping of confirmed oracle events
    mapping(uint256 => bool) _oracleEventsConfirmed;

    /// @notice Mapping of oracle event list public keys
    mapping(uint256 => bool) _oracleEventsPubkeys;
    
    /// @notice Mapping of oracle event list addresses
    mapping(uint256 => uint256) _oracleEventsAddress;

    /// @notice Number of oracle events
    uint128 _numberOfOracleEvents;

    /// @notice Number of approved oracle events
    uint128 _approvedOracleEvents;

    // ===== Consensus state for setTimings =====

    /// @notice Per-oracle hash of submitted timing params (oracle pubkey => hash)
    mapping(uint256 => uint256) _timingsOracleHash;

    /// @notice Count of oracles per hash of timing params (hash => count)
    mapping(uint256 => uint128) _timingsHashCount;

    // ===== Consensus state for resolve =====

    /// @notice Per-oracle hash of submitted resolve params (oracle pubkey => hash)
    mapping(uint256 => uint256) _resolveOracleHash;

    /// @notice Count of oracles per hash of resolve params (hash => count)
    mapping(uint256 => uint128) _resolveHashCount;

    // ===== Consensus state for cancelEvent =====

    /// @notice Tracks which oracles have voted to cancel (oracle pubkey => voted)
    mapping(uint256 => bool) _cancelOracleVoted;

    /// @notice Total number of cancel votes
    uint128 _cancelVoteCount;

    /// @notice Initial per-outcome stakes submitted at deploy time (before numOutcomes known)
    uint128[] _initialStakes;

    // ===== Split/Merge state =====

    /// @notice Whether base pools have been frozen (snapshot taken at stakeEnd)
    bool _frozen;

    /// @notice True between `_ensureFrozen` emitting `onPmpCleanRefund` to the
    ///         deployer and the deployer PN acknowledging via
    ///         `confirmRefundReceived`. Gates `splitFullSet` and `mergeFullSet`
    ///         so that no PN (especially the deployer) can use stale stake
    ///         data before the refund has been applied. Cleared when the
    ///         refund callback has been processed by the deployer PN.
    bool _normRefundPending;

    /// @notice Snapshot of _totalPool at freeze time (clean + debt only)
    uint128 _baseTotalPool;

    /// @notice Normalized clean pool per outcome at stakeEnd (M*_k in spec —
    ///         after refunding the mod-G remainder back to the PMP creator).
    mapping(uint32 => uint128) _frozenCleanPools;

    /// @notice Live winning clean pool snapshot at resolve, used as the
    ///         denominator for proportional clean payouts.
    uint128 _resolvedWinClean;

    /// @notice Set at resolve when no outcome attracted any stake
    ///         (`totalWinMass == 0`) but debt bettors exist. While true,
    ///         `claim()` takes the alternate refund path: debt principals are
    ///         returned to their owners, while clean / coupon stakes stay
    ///         forfeit and the residual flows to the creator only after every
    ///         debt position has been claimed.
    bool _debtRefundMode;

    /// @notice Outstanding debt principal owed to bettors under
    ///         `_debtRefundMode`. Initialised to `_totalDebtPool` at resolve;
    ///         each refund claim decrements it. The PMP self-destructs once
    ///         this reaches zero (or no debt-claimer is left).
    uint128 _debtRefundRemaining;

    /// @notice Market-level collateral quantum Q used by split/merge.
    uint128 _splitMergeQ;

    /// @notice Per-outcome basket units u_k for clean tokens.
    mapping(uint32 => uint128) _frozenCleanUnitsPerQ;

    /// @notice OrderBook contract code for deployment
    TvmCell _orderBookCode;

    /// @notice True once the OrderBook has finished its shutdown drain and
    ///         reported back via `onOrderBookShutdownComplete`. Required by
    ///         `claim()` — blocks payouts until all open orders have been
    ///         cancelled and their collateral refunded to the owning PNs.
    bool _orderBookDone;

    /// @notice Cached deterministic OrderBook address — set once in the
    ///         constructor. Avoids re-hashing the OB stateInit on every
    ///         PMP↔OB interaction.
    address _orderBookAddress;

    /// @notice Latches after the very first `OrderBook.shutdown` message has
    ///         been sent so that subsequent calls to `ensureBalance` (which
    ///         fires on every entry point) don't spam redundant shutdown
    ///         messages. Cleared implicitly by `_orderBookDone` taking over.
    bool _shutdownTriggered;

    /// @notice Defense-in-depth invariant: total collateral currently held by
    ///         the PMP that is owed back to participants.
    ///
    ///         Increments on every collateral inflow:
    ///           - initial stakes (deployer)             ─ approveEvent
    ///           - clean / debt stakes                   ─ acceptStake
    ///           - splitFullSet                          ─ F_use
    ///         Decrements on every collateral outflow:
    ///           - cancelStake refunds (clean + debt)    ─ stakeAmount + debtAmount
    ///           - splitFullSet normalization refund     ─ refundTotal
    ///           - mergeFullSet                          ─ collateral
    ///           - resolve creator fee                   ─ _creatorFee
    ///           - claim winner payout                   ─ payoutClean+Debt+Coupon
    ///           - claim residual to creator             ─ residual
    ///
    ///         Coupon stakes don't move real collateral and are excluded.
    ///
    ///         Every outflow is **clamped** to whatever balance is left rather
    ///         than reverting — the tx always succeeds, but a buggy reward
    ///         calculation can never withdraw more than was actually deposited.
    ///         The user gets a smaller (possibly zero) payout instead of the
    ///         pool over-paying. This keeps the system live even after
    ///         math drift.
    uint128 _totalUnclaimedBalance;

    /// Events

    /// @notice Emitted when a stake is accepted and accounted into the pool.
    /// @dev `note` is a PrivateNote wallet address that sent the stake.
    /// @param note PrivateNote address (wallet) that placed the stake.
    /// @param outcomeId Outcome identifier the stake is placed on.
    /// @param amount Stake amount added to the pool.
    /// @param betType 0 - clean bet, 1 - debt bet, 2 - coupon bet
    event StakeAccepted(address indexed note, uint32 outcomeId, uint128 amount, uint8 betType);

    /// @notice Emitted when the event is fully approved by oracle(s).
    /// @dev This event is emitted only when all required oracle confirmations are collected.
    /// @param oracleEventList Address of the oracle event list that triggered the last required confirmation.
    /// @param oraclePubkey Public key of the oracle that confirmed/approved the event.
    event ApprovedByOracle(address oracleEventList, uint256 oraclePubkey);

    /// @notice Emitted when the event outcome is resolved.
    /// @param outcomeId Final outcome identifier.
    event Resolved(uint32 outcomeId);

    /// @notice Emitted when a claim is processed (either payout or rejection).
    /// @dev In this implementation the event is emitted only after resolution logic is executed.
    /// @param note PrivateNote address (wallet) that claimed.
    /// @param payout Calculated payout amount (0 if no payout).
    /// @param win True if the claim is winning and payout > 0.
    event ClaimProcessed(address indexed note, uint128 payout, bool win);

    /// @notice Emitted when network fee was burned.
    /// @dev The contract currently does not emit this event in the shown code; reserved for future accounting.
    /// @param amount Burned fee amount in native units.
    event NetworkFeeBurned(uint64 amount);

    /// @notice Emitted when stake/result time windows are set and the event becomes approved for staking.
    /// @param stakeStart Stake acceptance start timestamp.
    /// @param stakeEnd Stake acceptance end timestamp.
    /// @param resultStart Result acceptance start timestamp.
    /// @param resultEnd Result acceptance end timestamp.
    event TimingsSet(uint64 stakeStart, uint64 stakeEnd, uint64 resultStart, uint64 resultEnd);

    /// @notice Emitted when number of outcomes is set.
    /// @dev The contract currently derives `_numOutcomes` from `outcomeNames` and does not emit this event.
    /// @param numOutcomes Number of available outcomes.
    event NumOutcomesSet(uint32 numOutcomes);

    /// @notice Emitted when the event is cancelled by oracle governance.
    event EventCancelled();

    /// @notice Emitted when an oracle event list rejects the PMP before
    ///         oracle approval is complete (pre-approval termination path).
    event PMPRejected();

    /// @notice Emitted when creator fee is collected at resolution.
    /// @param fee Fee amount credited to deployer.
    event CreatorFeeCollected(uint128 fee);

    /// @notice Emitted when base pools are frozen at stakeEnd
    event PoolsFrozen(uint128 baseTotalPool);

    /// @notice Emitted when a split is processed
    /// @param note PrivateNote address
    /// @param collateral Collateral amount used for split
    event SplitProcessed(address indexed note, uint128 collateral);

    /// @notice Emitted when a merge is processed
    /// @param note PrivateNote address
    /// @param collateral Collateral amount returned from merge
    event MergeProcessed(address indexed note, uint128 collateral);


    /// @notice PMP constructor
    /// @param depositIdentifierHash Deposit identifier hash from PrivateNote
    /// @param tokenType Token type of collateral used by this PMP.
    /// @param oracleEventLists OracleEventList contracts that must confirm this event.
    /// @param oracleFees Per-oracle shell fees transferred during confirmation.
    /// @param initialStakes Per-outcome initial clean stakes from deployer (validated in approveEvent)
    /// @param orderBookCode OrderBook contract code used for deterministic OrderBook address.
    constructor(uint256 depositIdentifierHash, uint32 tokenType, address[] oracleEventLists, uint128[] oracleFees, uint128[] initialStakes, TvmCell orderBookCode) {
        tvm.accept();
        _tokenType = tokenType;
        TvmCell salt = abi.codeSalt(tvm.code()).get();
        (TvmCell PrivateNoteCode) = abi.decode(salt, (TvmCell));
        _privateNoteCode = PrivateNoteCode;
        _orderBookCode = orderBookCode;
        _orderBookAddress = DexLib.computeOrderBookAddress(
            _privateNoteCode, _orderBookCode,
            _eventId, _oracleListHash, _tokenType
        );
        _approved = false;
        _deployer = msg.sender;
        _numOutcomes = 0; // Initialize with 0 outcomes
        _initialStakes = initialStakes;

        address expectedNote = DexLib.computePrivateNoteAddress(_privateNoteCode, depositIdentifierHash);
        require(msg.sender == expectedNote, ERR_INVALID_SENDER);
        _numberOfOracleEvents = uint128(oracleEventLists.length);
        for (uint32 i = 0; i < oracleEventLists.length; i++) {
            mapping(uint32 => varuint32) data;
            data[CURRENCIES_ID_SHELL] = oracleFees[i];
            _oracleEventsConfirmed[oracleEventLists[i].value] = false;
            OracleEventList(oracleEventLists[i]).confirmEvent{
                value: 0.1 vmshell,
                flag: 1,
                currencies: data,
                dest_dapp_id: ORACLE_DAPP_ID
            }(_eventId, _oracleListHash, _tokenType);
        }
    }

    /// @notice Rejects the event and self-destructs the contract
    function rejectEvent() public {
        require(_oracleEventsConfirmed.exists(msg.sender.value), ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        address addrExtern = address.makeAddrExtern(PMP_REJECTED_BY_ORACLE, bitCntAddress);
        emit PMPRejected{dest: addrExtern}();
        
        for ((uint256 key, bool value) : _oracleEventsConfirmed) {
            if (value == true) {
                OracleEventList(address.makeAddrStd(0, key)).cancelEvent{
                    value: 0.1 vmshell,
                    flag: 1,
                    dest_dapp_id: ORACLE_DAPP_ID
                }(_eventId, _oracleListHash, _tokenType);
            }
        }

        // Refund the deployer's initial stakes regardless of approval state:
        //   - Pre-approval (`_approvedOracleEvents == 0`): stakes still in PN's
        //     `candidateAmount` slot, blocking `_busy`.
        //   - Post-partial-approval: stakes were committed into `stake.amount[k]`
        //     by `onInitialStakesAccepted` and `_balance` was reduced; without a
        //     refund here they'd stay locked in PN forever once the PMP self-
        //     destructs and the only mutation path disappears.
        if (_initialStakes.length > 0) {
            uint128 refundTotal = 0;
            for (uint32 i = 0; i < _initialStakes.length; i++) {
                refundTotal += _initialStakes[i];
            }
            PrivateNote(_deployer).onInitialStakesFailed{value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID}(
                _eventId, _oracleListHash, _tokenType, refundTotal
            );
        }

        // Return the oracle fee to the depositor (the note that funded the event),
        // not the protocol: the event was rejected, no oracle service was rendered.
        // `msg.currencies` is exactly the fee OEL forwarded back (SHELL only); the
        // PMP's remaining balance (initial-stake ECC) still goes to RootPN below.
        if (uint128(msg.currencies[CURRENCIES_ID_SHELL]) > 0) {
            _deployer.transfer({value: 0.1 vmshell, flag: 1, bounce: false, currencies: msg.currencies, dest_dapp_id: ROOT_PN_DAPP_ID});
        }

        selfdestruct(ROOT_PN_ADDRESS);
    }

    /// @notice Confirms and approves the PMP event by an oracle
    /// @dev
    /// - Callable only by registered OracleEventList contract addresses.
    /// - Each oracle can approve the event only once (duplicate approvals are ignored).
    /// - On every call, updates the pool name (`_name`).
    /// - On the first approval only, initializes event metadata:
    ///   - event description (`_describe`)
    ///   - outcome names mapping (`_outcomeNames`)
    ///   - number of outcomes (`_numOutcomes`)
    /// - Optionally binds an internal trusted address to the oracle public key
    ///   for internal governance message authorization.
    /// - When all required oracle approvals are collected, emits `ApprovedByOracle`.
    ///
    /// @param oraclePubkey Oracle public key used as a unique oracle identifier
    /// @param outcomeNames Mapping of outcome identifiers to human-readable names;
    ///        used only on the first approval call
    /// @param describe Human-readable description of the event;
    ///        used only on the first approval call
    /// @param name Pool name;
    ///        updated on every approval call (last call wins)
    /// @param trustAddr Optional trusted internal address to bind with `oraclePubkey`
    function approveEvent(uint256 oraclePubkey, mapping(uint32 => string) outcomeNames, string describe, string name, optional(uint256) trustAddr) public {
        require(_oracleEventsConfirmed.exists(msg.sender.value), ERR_INVALID_SENDER);
        // Reject pubkey=0 so an OEL deployed with a zero oracle pubkey cannot
        // register it in _oracleEventsPubkeys — otherwise any unsigned ext msg
        // (msg.pubkey()==0) would pass the governance gates in _getOraclePubkey
        // (submitResolve / submitSetTimings / submitCancelEvent).
        require(oraclePubkey != 0, ERR_INVALID_PARAMS);
        if (_oracleEventsConfirmed[msg.sender.value] == true) {
            return;
        }
        if (_approvedOracleEvents >= _numberOfOracleEvents) {
            return;
        }
        tvm.accept();
        ensureBalance();
        _name = name;
        if (_approvedOracleEvents == 0){
            _describe = describe;
            _outcomeNames = outcomeNames;
            _numOutcomes = uint32(outcomeNames.keys().length);

            // Validate and process deployer's initial stakes (must cover all outcomes)
            if (_initialStakes.length != _numOutcomes) {
                // Length mismatch → refund deployer and cancel PMP
                uint128 refundTotal = 0;
                for (uint32 i = 0; i < _initialStakes.length; i++) {
                    refundTotal += _initialStakes[i];
                }
                PrivateNote(_deployer).onInitialStakesFailed{value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID}(
                    _eventId, _oracleListHash, _tokenType, refundTotal
                );
                // This OracleEventList (msg.sender) counted the event in confirmEvent; ask it to
                // cancel so its per-event count is decremented before this PMP self-destructs.
                // The oracle fee stays with the oracle here -- only the count is corrected.
                OracleEventList(msg.sender).cancelEvent{
                    value: 0.1 vmshell, flag: 1, dest_dapp_id: ORACLE_DAPP_ID
                }(_eventId, _oracleListHash, _tokenType);
                selfdestruct(ROOT_PN_ADDRESS);
                return;
            }
            // Add initial stakes to the clean pool; mark deployer as covering all outcomes
            uint128 initialTotal = 0;
            for (uint32 i = 0; i < _numOutcomes; i++) {
                require(_initialStakes[i] > 0, ERR_ZERO_TOKEN_AMOUNT);
                _typedOutcomePools[i][BET_TYPE_CLEAN] += _initialStakes[i];
                initialTotal += _initialStakes[i];
            }
            _totalPool += initialTotal;
            _totalCleanPool += initialTotal;
            _totalUnclaimedBalance += initialTotal;
            PrivateNote(_deployer).onInitialStakesAccepted{value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID}(
                _eventId, _oracleListHash, _tokenType, _initialStakes
            );
        }
        _oracleEventsConfirmed[msg.sender.value] = true;
        _oracleEventsPubkeys[oraclePubkey] = true;
        if (trustAddr.hasValue()) {
            require(!_oracleEventsAddress.exists(trustAddr.get()), ERR_ALREADY_INITIALIZED);
            _oracleEventsAddress[trustAddr.get()] = oraclePubkey;
        }

        _approvedOracleEvents += 1;
        bool allConfirmed = _approvedOracleEvents == _numberOfOracleEvents;

        if (allConfirmed) {
            address addrExtern = address.makeAddrExtern(PMP_APPROVED_BY_ORACLE, bitCntAddress);
            emit ApprovedByOracle{dest: addrExtern}(msg.sender, oraclePubkey);
        }
    }

    /// @notice Ensures minimal native balance for operations. Also piggybacks
    ///         the OrderBook shutdown trigger: once `_resultStart` has elapsed
    ///         any interaction with the PMP will kick off the drain (idempotent,
    ///         latched by `_shutdownTriggered`). This avoids a dedicated
    ///         external trigger method — the book always gets shut down as soon
    ///         as someone touches the PMP past the result deadline.
    function ensureBalance() private {
        if (address(this).balance <= MIN_BALANCE) {
            gosh.mintshellq(MIN_BALANCE);
        }
        triggerOrderBookShutdown();
    }

    /// @notice Idempotent OrderBook shutdown trigger. Gated by `if` (not
    ///         `require`) so routine calls don't revert — the drain simply
    ///         won't start until `_resultStart` is reached, and won't be
    ///         re-sent once latched or once the OB has reported back.
    function triggerOrderBookShutdown() private {
        if (_shutdownTriggered) return;
        if (_orderBookDone) return;
        // Before setTimings, `_resultStart == 0` — any block.timestamp is >=0,
        // so we must explicitly guard against the uninitialised case. Also,
        // no OrderBook exists until auto-freeze (`_frozen`), so shutting-down
        // early would send the message to an empty address.
        if (_resultStart == 0) return;
        if (!_frozen) return;
        // Trigger when EITHER the result window has opened, OR the event has
        // been cancelled by oracle/grace-period — otherwise a cancelled event
        // would leave the book live and let users keep trading on what is
        // effectively a refund-only market.
        if (block.timestamp < _resultStart && !_isCancelled) return;

        _shutdownTriggered = true;

        OrderBook(_orderBookAddress).shutdown{
            value: 10 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }();
    }

    /// @notice Computes stakeEnd as 10% of (stakeStart..resultStart).
    function _computeStakeEnd() private view returns (uint64) {
        return _stakeStart + (_resultStart - _stakeStart) / 10;
    }

    /// @param resultStart Result acceptance start time (resultEnd = resultStart + GRACE_PERIOD)
    function setTimings(
        uint64 resultStart
    ) private {
        require(!_resolvedOutcome.hasValue(), ERR_ALREADY_RESOLVED);
        require(_approvedOracleEvents == _numberOfOracleEvents, ERR_NOT_INITIALIZED);
        require(resultStart > block.timestamp, ERR_INVALID_PARAMS);
        // Once the previous resultStart has elapsed, the result window is open
        // and cancellation/resolve become live — moving the deadline retroactively
        // would let oracles invalidate in-flight resolves.
        require(_resultStart == 0 || block.timestamp < _resultStart, ERR_INVALID_PARAMS);

        // stakeStart = now on first call (PMP approval)
        if (_stakeStart == 0) {
            require(resultStart >= uint64(block.timestamp) + MIN_RESULT_GAP, ERR_INVALID_PARAMS);
            _stakeStart = uint64(block.timestamp);
        }

        _resultStart = resultStart;
        _approved = true;

        // If the new stakeEnd <= now, auto-freeze
        if (block.timestamp >= _computeStakeEnd()) {
            _ensureFrozen();
        }

        // OB already alive — propagate the new resultStart so its time-gate stays consistent.
        if (_frozen) {
            OrderBook(_orderBookAddress).setResultStart{
                value: 0.1 vmshell,
                flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }(_resultStart);
        }

        tvm.accept();
        ensureBalance();

        address addrExtern = address.makeAddrExtern(PMP_SET_TIMINGS, bitCntAddress);
        emit TimingsSet{dest: addrExtern}(_stakeStart, _computeStakeEnd(), _resultStart, _resultStart + GRACE_PERIOD);
    }

    /// @notice Cancels the event
    function cancelEvent() private {
        require(_approvedOracleEvents == _numberOfOracleEvents, ERR_NOT_APPROVED);
        require(!_isCancelled, ERR_ALREADY_CANCELLED);
        // Once an outcome is resolved, claims are open and a cancel must be impossible
        // — otherwise users could double-dip via cancelStake and claim.
        require(!_resolvedOutcome.hasValue(), ERR_ALREADY_RESOLVED);
        tvm.accept();
        _isCancelled = true;
        // ensureBalance fires triggerOrderBookShutdown, which now also fires
        // on `_isCancelled` — so flipping the flag first is required for the
        // OB drain to start in the same tx as the cancel.
        ensureBalance();

        address addrExtern = address.makeAddrExtern(PMP_EVENT_CANCELLED, bitCntAddress);
        emit EventCancelled{dest: addrExtern}();
    }

    /// @notice Accepts stake from PrivateNote and confirms it
    /// @param outcomeId Stake outcome identifier (must be < _numOutcomes)
    /// @param stakeAmount Stake amount
    /// @param depositIdentifierHash Deposit identifier hash
    /// @param betType 0 - clean bet, 1 - debt bet, 2 - coupon bet
    function acceptStake(
        uint32 outcomeId,
        uint128 stakeAmount,
        uint256 depositIdentifierHash,
        uint8 betType
    ) public {
        require(_approved, ERR_NOT_APPROVED);
        require(!_isCancelled, ERR_ALREADY_CANCELLED);
        require(!_resolvedOutcome.hasValue(), ERR_ALREADY_RESOLVED);
        require(_numOutcomes > 0, ERR_NOT_INITIALIZED);
        require(outcomeId < _numOutcomes, ERR_INVALID_OUTCOME_ID);
        require(!_frozen, ERR_ALREADY_FROZEN);
        require(block.timestamp >= _stakeStart, ERR_STAKE_NOT_STARTED);
        require(block.timestamp < _computeStakeEnd(), ERR_STAKE_PERIOD_ENDED);
        require(betType <= BET_TYPE_COUPON, ERR_INVALID_BET_TYPE);
        
        address wallet = DexLib.computePrivateNoteAddress(_privateNoteCode, depositIdentifierHash);
        require(msg.sender == wallet, ERR_INVALID_SENDER);

        tvm.accept();
        ensureBalance();

        if (betType == BET_TYPE_COUPON) {
            uint128 currentOutcomeCouponPool = _typedOutcomePools[outcomeId][BET_TYPE_COUPON];
            uint128 newOutcomeCouponPool = currentOutcomeCouponPool + stakeAmount;            
            uint128 currentOutcomeTotal = _typedOutcomePools[outcomeId][BET_TYPE_COUPON]
                                        + _typedOutcomePools[outcomeId][BET_TYPE_DEBT]
                                        + _typedOutcomePools[outcomeId][BET_TYPE_CLEAN];            
            uint128 newOutcomeTotal = currentOutcomeTotal + stakeAmount;            
            uint128 maxOutcomeCouponPool = uint128(
                (uint256(newOutcomeTotal) * uint256(COUPON_POOL_LIMIT_PERCENT)) / uint256(FULL_PERCENT)
            );
            require(newOutcomeCouponPool <= maxOutcomeCouponPool, ERR_COUPON_POOL_LIMIT_EXCEEDED);            
            _totalCouponPool += stakeAmount;
        } else if (betType == BET_TYPE_CLEAN) {
            _totalCleanPool += stakeAmount;
            _totalPool += stakeAmount;
            _totalUnclaimedBalance += stakeAmount;
        } else if (betType == BET_TYPE_DEBT) {
            _totalDebtPool += stakeAmount;
            _totalPool += stakeAmount;
            _totalUnclaimedBalance += stakeAmount;
        }
    
        _typedOutcomePools[outcomeId][betType] += stakeAmount;

        PrivateNote(wallet).onStakeAccepted{
            value: 0.1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType, _numOutcomes, betType);
        
        address addrExtern = address.makeAddrExtern(PMP_STAKE_ACCEPTED, bitCntAddress);
        emit StakeAccepted{dest: addrExtern}(wallet, outcomeId, stakeAmount, betType);
    }

    /// @notice Cancels user stakes after event cancellation and processes refund
    /// @dev
    /// - Callable only by the corresponding PrivateNote wallet.
    /// - The event must be cancelled.
    /// - If the result window has ended and the event is not resolved,
    ///   the function triggers automatic cancellation.
    /// - Decreases internal pool balances for clean, debt and coupon bets.
    /// - Clean and debt amounts reduce `_totalPool`.
    /// - Coupon amounts reduce `_totalCouponPool` only.
    /// - Sends aggregated refund data back to PrivateNote via `onStakeCancelled`.
    ///
    /// @param stakeAmount Array of clean stake amounts per outcome.
    ///        Length should correspond to `_numOutcomes`.
    /// @param debtAmount Array of debt stake amounts per outcome.
    ///        Length should correspond to `_numOutcomes`.
    /// @param couponsAmount Array of coupon stake amounts per outcome.
    ///        Length should correspond to `_numOutcomes`.
    /// @param depositIdentifierHash Deposit identifier hash used to
    ///        deterministically compute the caller's PrivateNote address.
    function cancelStake(
        uint128[] stakeAmount,
        uint128[] debtAmount,
        uint128[] couponsAmount,
        uint256 depositIdentifierHash
    ) public {
        if ((block.timestamp > (_resultStart + GRACE_PERIOD))
            && (!_resolvedOutcome.hasValue())
            && (!_isCancelled))
        {
            cancelEvent();
        }
        require(_isCancelled, ERR_NOT_CANCELLED);
        // If an OrderBook was deployed (_frozen == true), wait until it has
        // finished draining all resting orders. Otherwise PN.onStakeCancelled
        // would delete the stake record while a sell-side resting order is
        // still on the book, and a later onOrderCancelled callback would have
        // no stake to credit the released outcome tokens to (silent loss).
        require(!_frozen || _orderBookDone, ERR_ORDERBOOK_NOT_SHUTDOWN);

        address wallet = DexLib.computePrivateNoteAddress(_privateNoteCode, depositIdentifierHash);
        require(msg.sender == wallet, ERR_INVALID_SENDER);

        tvm.accept();
        ensureBalance();
        uint128 totalStake = 0;
        uint128 totalCouponRefund = 0;

        uint32 nMax = uint32(stakeAmount.length);
        if (uint32(debtAmount.length) > nMax) nMax = uint32(debtAmount.length);
        if (uint32(couponsAmount.length) > nMax) nMax = uint32(couponsAmount.length);

        // Three arrays are typically same length (per-outcome). Iterate up to
        // their max length once and decrement whichever pools have a value.
        for (uint32 outcomeId = 0; outcomeId < nMax; outcomeId++) {
            if (outcomeId < uint32(stakeAmount.length)) {
                uint128 a = stakeAmount[outcomeId];
                _typedOutcomePools[outcomeId][BET_TYPE_CLEAN] -= a;
                _totalPool -= a;
                _totalCleanPool -= a;
                totalStake += a;
            }
            if (outcomeId < uint32(debtAmount.length)) {
                uint128 a = debtAmount[outcomeId];
                _typedOutcomePools[outcomeId][BET_TYPE_DEBT] -= a;
                _totalPool -= a;
                _totalDebtPool -= a;
                totalStake += a;
            }
            if (outcomeId < uint32(couponsAmount.length)) {
                uint128 a = couponsAmount[outcomeId];
                _typedOutcomePools[outcomeId][BET_TYPE_COUPON] -= a;
                _totalCouponPool -= a;
                totalCouponRefund += a;
            }
        }

        // Defense-in-depth: real (clean+debt) refund is clamped to whatever
        // unclaimed balance the PMP actually has. Coupon refunds are virtual
        // and excluded. If math drift left us under-reserved, the user gets
        // back less rather than the pool over-paying.
        if (totalStake > _totalUnclaimedBalance) {
            totalStake = _totalUnclaimedBalance;
        }
        _totalUnclaimedBalance -= totalStake;

        PrivateNote(wallet).onStakeCancelled{
            value: 0.1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType, totalStake, totalCouponRefund);

        // Self-destruct once every non-forfeited stake has been refunded — the
        // live pools have decayed to exactly the forfeited map, and the only
        // real balance left (the forfeited principal) is swept to the deployer.
        // (cancelStake is already gated on `!_frozen || _orderBookDone`.)
        if (_poolsEqualForfeited()) {
            _finalizeResidualClose();
        }
    }



    // ===== Split/Merge Functions =====

    /// @dev Ensures pools are frozen. Called automatically by split/merge/resolve
    ///      on first access after stakeEnd.
    ///
    ///      New (clean-only split/merge) model:
    ///        1. Refund mod-G remainder of each clean pool to the PMP creator
    ///           (where G = min(_initialStakes)) so the residual pools are
    ///           multiples of G — guarantees an integer split/merge basket.
    ///        2. Build basket: g = gcd(M*_k), u_k = M*_k / g, Q = Σ u_k.
    ///        3. Deploy OrderBook (with resultStart for time-gate).
    ///
    ///      No payout coefficients (creatorFee, couponCoef, debtCoef,
    ///      profitToClean) are frozen here — they are computed at resolve
    ///      from live pools.
    /// @notice Public entry (incl. external messages) to freeze the pools and
    ///         deploy the OrderBook once the stake window has ended. Lets
    ///         traders bootstrap the OB without having to first call
    ///         split/merge themselves.
    /// @dev Checks run before tvm.accept() so spam on already-frozen /
    ///      cancelled / pre-stakeEnd state doesn't burn contract gas.
    function freezeNow() public {
        require(!_frozen, ERR_ALREADY_FROZEN);
        require(_approved, ERR_NOT_APPROVED);
        require(!_isCancelled, ERR_ALREADY_CANCELLED);
        require(block.timestamp >= _computeStakeEnd(), ERR_NOT_STAKEEND);
        tvm.accept();
        ensureBalance();
        _ensureFrozen();
    }

    function _ensureFrozen() private {
        if (_frozen) return;
        require(_approved, ERR_NOT_APPROVED);
        require(!_isCancelled, ERR_ALREADY_CANCELLED);
        require(block.timestamp >= _computeStakeEnd(), ERR_NOT_STAKEEND);

        _frozen = true;
        _baseTotalPool = _totalPool;

        // ===== Step 1: normalize clean pools to multiples of G ========
        // G := min(_initialStakes). Each outcome's clean pool gets its mod-G
        // remainder refunded back to the creator's PrivateNote (collateral
        // returned + stake.amount[k] decremented). Without this step the
        // GCD basket would have to absorb arbitrary "dust" tails.
        uint128 minInitialStake = type(uint128).max;
        for (uint32 i = 0; i < _initialStakes.length; i++) {
            if (_initialStakes[i] < minInitialStake) minInitialStake = _initialStakes[i];
        }
        require(
            minInitialStake > 0 && minInitialStake != type(uint128).max,
            ERR_INVALID_PARAMS
        );

        uint128[] refundAmounts = new uint128[](_numOutcomes);
        uint128 refundTotal = 0;
        for (uint32 k = 0; k < _numOutcomes; k++) {
            uint128 cleanPool = _typedOutcomePools[k][BET_TYPE_CLEAN];
            uint128 remainder = cleanPool % minInitialStake;
            refundAmounts[k] = remainder;
            if (remainder > 0) {
                _typedOutcomePools[k][BET_TYPE_CLEAN] = cleanPool - remainder;
                refundTotal += remainder;
            }
            _frozenCleanPools[k] = _typedOutcomePools[k][BET_TYPE_CLEAN];
        }
        if (refundTotal > 0) {
            _totalCleanPool -= refundTotal;
            _totalPool      -= refundTotal;
            // Clamp the normalization refund to whatever unclaimed balance is
            // left. In normal operation this is always equal — the require
            // is here purely as a math-drift safety net.
            if (refundTotal > _totalUnclaimedBalance) {
                refundTotal = _totalUnclaimedBalance;
            }
            _totalUnclaimedBalance -= refundTotal;
            // Race fix: block split/merge until deployer PN acks the refund
            // (via `confirmRefundReceived`). Without this, deployer could use
            // stale stake.amount before refund is applied (e.g. sell or split
            // tokens that are about to be normalized out of stake).
            _normRefundPending = true;
            // Update creator's PN: decrement stake.amount[k] and credit balance.
            PrivateNote(_deployer).onPmpCleanRefund{
                value: 0.1 vmshell,
                flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }(_eventId, _oracleListHash, _tokenType, refundAmounts, refundTotal);
        }

        // ===== Step 2: integer split/merge basket from M*_k =====
        //   g    = gcd_{k: M*_k>0}(M*_k)
        //   u_k  = M*_k / g
        //   Q    = sum_k u_k
        // Each split/merge consumes/produces t baskets of (Q collateral,
        // t*u_k clean[k]) — no remainders since M*_k is a multiple of G.
        {
            uint128 gcdValue = 0;
            for (uint32 k = 0; k < _numOutcomes; k++) {
                uint128 cleanPool = _frozenCleanPools[k];
                if (cleanPool == 0) continue;
                if (gcdValue == 0) {
                    gcdValue = cleanPool;
                } else {
                    uint128 a = gcdValue;
                    uint128 b = cleanPool;
                    while (b != 0) {
                        uint128 r = a % b;
                        a = b;
                        b = r;
                    }
                    gcdValue = a;
                }
            }
            uint128 basketSize = 0;
            if (gcdValue > 0) {
                for (uint32 k = 0; k < _numOutcomes; k++) {
                    uint128 cleanPool = _frozenCleanPools[k];
                    if (cleanPool == 0) continue;
                    uint128 units = cleanPool / gcdValue;
                    _frozenCleanUnitsPerQ[k] = units;
                    basketSize += units;
                }
            }
            _splitMergeQ = basketSize;
        }

        // Deploy OrderBook (address pre-computed in constructor and cached
        // in `_orderBookAddress`).
        TvmCell stateInit = DexLib.buildOrderBookStateInit(
            _privateNoteCode,
            _orderBookCode,
            _eventId,
            _oracleListHash,
            _tokenType
        );

        new OrderBook{
            stateInit: stateInit,
            value: 10 vmshell,
            flag: 1
        }(tvm.hash(tvm.code()), tvm.code().depth(), _resultStart, _numOutcomes);

        address addrExtern = address.makeAddrExtern(PMP_POOLS_FROZEN, bitCntAddress);
        emit PoolsFrozen{dest: addrExtern}(_baseTotalPool);
    }

    /// @notice Splits collateral into proportional outcome tokens across all outcomes.
    /// @dev Per spec: δ_k = floor(F × M_k / T) where M_k = frozen clean pool,
    ///      T = baseTotalPool. Pool update: _totalPool += F (full collateral).
    ///      Tokens minted: _cleanPools[k] += δ_k.
    ///      Remainder F - Σδ_k stays in pool as surplus (benefits existing stakers).
    ///
    /// @param collateral Amount of collateral to split (F).
    /// @param depositIdentifierHash Caller's PrivateNote deposit ID.
    function splitFullSet(
        uint128 collateral,
        uint256 depositIdentifierHash
    ) public {
        _ensureFrozen();
        require(!_resolvedOutcome.hasValue(), ERR_ALREADY_RESOLVED);
        require(!_isCancelled, ERR_ALREADY_CANCELLED);
        // Once the result window opens, the pool composition must be frozen for
        // claim accounting — no further split/merge is permitted.
        require(block.timestamp < _resultStart, ERR_RESULT_NOT_STARTED);
        require(collateral > 0, ERR_LOW_VALUE);
        // Race fix: deployer must process the normalization refund before any
        // further pool mutation, else stake.amount may be out of sync with
        // pools (deployer could use stale amounts).
        require(!_normRefundPending, ERR_NORM_REFUND_PENDING);
        require(_baseTotalPool > 0, ERR_INVALID_PARAMS);

        address wallet = DexLib.computePrivateNoteAddress(
            _privateNoteCode,
            depositIdentifierHash
        );
        require(msg.sender == wallet, ERR_INVALID_SENDER);

        tvm.accept();
        ensureBalance();

        // Quantized split:
        //   t      = floor(F / Q)
        //   F_use  = t * Q
        //   F_back = F - F_use               (refunded to user)
        //   amounts[k] = t * u_k
        uint128 Q = _splitMergeQ;
        require(Q > 0, ERR_INVALID_PARAMS);
        uint128 t = collateral / Q;
        require(t > 0, ERR_LOW_VALUE);
        uint128 F_use = t * Q;

        uint128[] amounts = new uint128[](_numOutcomes);
        uint128 mintedTotal = 0;
        for (uint32 k = 0; k < _numOutcomes; k++) {
            uint128 u_k = _frozenCleanUnitsPerQ[k];
            if (u_k == 0) {
                amounts[k] = 0;
                continue;
            }
            amounts[k] = t * u_k;
            _typedOutcomePools[k][BET_TYPE_CLEAN] += amounts[k];
            mintedTotal += amounts[k];
        }
        _totalPool += F_use;
        _totalCleanPool += mintedTotal;
        _totalUnclaimedBalance += F_use;

        PrivateNote(wallet).onSplitAccepted{
            value: 0.1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType, amounts, F_use);

        address addrExtern = address.makeAddrExtern(PMP_SPLIT_PROCESSED, bitCntAddress);
        emit SplitProcessed{dest: addrExtern}(wallet, F_use);
    }

    /// @notice Merges proportional outcome tokens back into collateral.
    /// @dev Burns t baskets of clean tokens (`t * u_k` per outcome),
    ///      returns `t * Q` collateral. Pool update: _totalPool -= F.
    ///      No fixed-obligation check — payout obligations are computed
    ///      live at resolve, so merge solvency is governed by basic pool
    ///      arithmetic only.
    ///
    /// @param amount Array of outcome token amounts to merge (upper bounds).
    /// @param depositIdentifierHash Caller's PrivateNote deposit ID.
    function mergeFullSet(
        uint128[] amount,
        uint256 depositIdentifierHash
    ) public {
        _ensureFrozen();
        require(!_resolvedOutcome.hasValue(), ERR_ALREADY_RESOLVED);
        require(!_isCancelled, ERR_ALREADY_CANCELLED);
        // Once the result window opens, the pool composition must be frozen for
        // claim accounting — no further split/merge is permitted.
        require(block.timestamp < _resultStart, ERR_RESULT_NOT_STARTED);
        require(amount.length == _numOutcomes, ERR_INVALID_OUTCOME_ID);
        require(_baseTotalPool > 0, ERR_INVALID_PARAMS);
        // Race fix: see splitFullSet — block until normalization refund is
        // applied by deployer PN.
        require(!_normRefundPending, ERR_NORM_REFUND_PENDING);

        address wallet = DexLib.computePrivateNoteAddress(
            _privateNoteCode,
            depositIdentifierHash
        );
        require(msg.sender == wallet, ERR_INVALID_SENDER);

        tvm.accept();
        ensureBalance();

        // Quantized merge:
        //   For each outcome with u_k > 0:  t_k = floor(amount[k] / u_k)
        //   t           = min_k t_k
        //   collateral  = t * Q
        //   actual[k]   = t * u_k    (consumed tokens; remainder stays with user)
        uint128 Q = _splitMergeQ;
        require(Q > 0, ERR_INVALID_PARAMS);

        uint128 t = type(uint128).max;
        for (uint32 k = 0; k < _numOutcomes; k++) {
            uint128 u_k = _frozenCleanUnitsPerQ[k];
            if (u_k == 0) {
                require(amount[k] == 0, ERR_INVALID_PARAMS);
                continue;
            }
            uint128 t_k = amount[k] / u_k;
            if (t_k < t) t = t_k;
        }
        require(t > 0 && t != type(uint128).max, ERR_LOW_VALUE);

        uint128 collateral = t * Q;

        // Solvency: payout obligations are computed live at resolve, so basic
        // pool arithmetic is sufficient here — no fixed-obligation reserve.
        require(_totalPool      >= collateral, ERR_INVALID_PARAMS);
        require(_totalCleanPool >= collateral, ERR_INVALID_PARAMS);

        uint128[] actual;
        uint128 burnedTotal = 0;
        for (uint32 k = 0; k < _numOutcomes; k++) {
            uint128 u_k = _frozenCleanUnitsPerQ[k];
            uint128 a = u_k == 0 ? uint128(0) : (t * u_k);
            require(_typedOutcomePools[k][BET_TYPE_CLEAN] >= a, ERR_INVALID_PARAMS);
            actual.push(a);
            burnedTotal += a;
        }

        // Apply merge
        for (uint32 k = 0; k < _numOutcomes; k++) {
            _typedOutcomePools[k][BET_TYPE_CLEAN] -= actual[k];
        }
        // Per spec: _totalPool -= F (full collateral)
        _totalPool -= collateral;
        _totalCleanPool -= burnedTotal;
        // Clamp the merge collateral refund — pool math is supposed to keep
        // them equal, but if drift happens, hand back what we can.
        if (collateral > _totalUnclaimedBalance) {
            collateral = _totalUnclaimedBalance;
        }
        _totalUnclaimedBalance -= collateral;

        PrivateNote(wallet).onMergeAccepted{
            value: 0.1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(_eventId, _oracleListHash, _tokenType, collateral, actual);

        address addrExtern = address.makeAddrExtern(PMP_MERGE_PROCESSED, bitCntAddress);
        emit MergeProcessed{dest: addrExtern}(wallet, collateral);
    }

    /// @notice Resolves the event outcome.
    /// @dev All payout coefficients (creatorFee, couponWinCoef, debtWinCoef,
    ///      profitToClean) are computed here from LIVE pools — i.e. after
    ///      every post-freeze split/merge — not from frozen snapshots.
    ///      Clean payouts use the proportional formula (see `claim`).
    /// @param outcomeId Resolution outcome identifier (must be < _numOutcomes)
    function resolve(uint32 outcomeId) private {
        require(_approved, ERR_NOT_APPROVED);
        _ensureFrozen();
        require(!_isCancelled, ERR_ALREADY_CANCELLED);
        require(!_resolvedOutcome.hasValue(), ERR_ALREADY_RESOLVED);
        require(outcomeId < _numOutcomes, ERR_INVALID_OUTCOME_ID);
        require(block.timestamp >= _resultStart, ERR_RESULT_NOT_STARTED);
        require(block.timestamp <= (_resultStart + GRACE_PERIOD), ERR_RESULT_ENDED);

        tvm.accept();
        ensureBalance();
        _resolvedOutcome = outcomeId;

        // Live pools at resolve time (post all split/merge).
        uint128 winClean  = _typedOutcomePools[outcomeId][BET_TYPE_CLEAN];
        uint128 winDebt   = _typedOutcomePools[outcomeId][BET_TYPE_DEBT];
        uint128 winCoupon = _typedOutcomePools[outcomeId][BET_TYPE_COUPON];
        uint128 totalWinMass = winClean + winDebt + winCoupon;

        _resolvedWinClean = winClean;
        _totalWinPool     = totalWinMass;

        if (totalWinMass == 0) {
            // No winning stakes — zero out coefficients and reward buckets so
            // the standard claim path produces nothing.
            _creatorFee         = 0;
            _couponWinCoef      = 0;
            _debtWinCoef        = 0;
            _profitToClean      = 0;
            _totalRewardsClean  = 0;
            _totalRewardsDebt   = 0;
            _totalRewardsCoupon = 0;
            _totalWinPool       = 0;

            // If there are debt-funded losing stakes, refund them — burning a
            // bettor's principal *and* leaving them with on-PN debt would be
            // unfair when no winning side ever existed. Clean / coupon stakes
            // are still forfeit (creator gets the residual after refunds).
            if (_totalDebtPool > 0) {
                _debtRefundMode = true;
                _debtRefundRemaining = _totalDebtPool;
            }
        } else {
            // Live profit budget: total pool minus principal owed to clean+debt winners.
            uint128 profitBudget = _totalPool > (winClean + winDebt)
                ? _totalPool - winClean - winDebt
                : 0;

            // Creator fee (pct of live total pool, capped by profit budget).
            uint128 fee = uint128(
                (uint256(_totalPool) * uint256(FEE_PERCENT)) / uint256(FULL_PERCENT)
            );
            if (fee > profitBudget) fee = profitBudget;
            _creatorFee = fee;
            profitBudget -= fee;

            // Coupon coefficient (capped at COUPON_MAX_PAYOUT_MULTIPLIER).
            // Exception: when ONLY coupon stakes won (no clean/debt winners),
            // the cap is lifted so the surplus flows to the coupon winners (who
            // absorb the whole profitBudget) rather than remaining in the contract
            // to be swept to the creator.
            uint128 profitPerUnit = uint128(
                (uint256(profitBudget) * FULL_PERCENT) / totalWinMass
            );
            uint128 couponCoef = profitPerUnit;
            bool onlyCouponWins = (winClean == 0 && winDebt == 0 && winCoupon > 0);
            if (!onlyCouponWins && couponCoef > COUPON_MAX_PAYOUT_MULTIPLIER) {
                couponCoef = COUPON_MAX_PAYOUT_MULTIPLIER;
            }
            _couponWinCoef = couponCoef;

            uint128 couponPaid = uint128(
                (uint256(winCoupon) * couponCoef) / FULL_PERCENT
            );
            if (couponPaid > profitBudget) couponPaid = profitBudget;
            profitBudget -= couponPaid;

            // Debt coefficient.
            uint128 realWinMass = winClean + winDebt;
            uint128 debtCoef    = 0;
            uint128 debtProfit  = 0;
            if (realWinMass > 0) {
                uint128 baseRealPPU = uint128(
                    (uint256(profitBudget) * FULL_PERCENT) / realWinMass
                );
                if (winClean > 0) {
                    // Standard split: debt winners give up `R%` of their PPU
                    // so clean winners can absorb it via `_profitToClean`.
                    debtCoef = uint128(
                        (uint256(baseRealPPU) *
                         uint256(FULL_PERCENT - DEBT_REDISTRIBUTION_PERCENT)) / uint256(FULL_PERCENT)
                    );
                } else {
                    // No clean winners → no one to receive the redistribution
                    // share, so debt winners keep the full PPU and `debtPaid`
                    // (formula 17) stays zero in claim().
                    debtCoef = baseRealPPU;
                }
                debtProfit = uint128(
                    (uint256(winDebt) * uint256(debtCoef)) / FULL_PERCENT
                );
                if (debtProfit > profitBudget) debtProfit = profitBudget;
                profitBudget -= debtProfit;
            }
            _debtWinCoef    = debtCoef;
            _profitToClean  = profitBudget;

            _totalRewardsClean  = winClean + _profitToClean;
            _totalRewardsDebt   = winDebt + debtProfit;
            _totalRewardsCoupon = couponPaid;
        }

        // Send creator fee to deployer's PrivateNote — clamped.
        if (_creatorFee > _totalUnclaimedBalance) {
            _creatorFee = _totalUnclaimedBalance;
        }
        if (_creatorFee > 0) {
            _totalUnclaimedBalance -= _creatorFee;
            PrivateNote(_deployer).acceptFee{
                value: 0.1 vmshell,
                flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }(_creatorFee, _tokenType, _eventId, _oracleListHash);

            address addrExternFee = address.makeAddrExtern(PMP_CREATOR_FEE_COLLECTED, bitCntAddress);
            emit CreatorFeeCollected{dest: addrExternFee}(_creatorFee);
        }

        address addrExtern = address.makeAddrExtern(PMP_RESOLVED, bitCntAddress);
        emit Resolved{dest: addrExtern}(outcomeId);
    }



    /// @notice Claims winnings for the caller's PrivateNote wallet.
    /// @dev
    /// - Callable only by the corresponding PrivateNote wallet.
    /// - The event must be approved.
    /// - `stakeAmount.length` must equal `_numOutcomes`.
    /// - If the event is not yet resolved, the function processes
    ///   a zero-payout claim and returns immediately.
    /// - Determines whether the caller has a winning position
    ///   based on the resolved outcome.
    /// - Calculates payout for:
    ///     * clean bets — proportional share of `_totalPool`
    ///       relative to the winning clean pool,
    ///     * debt bets — original debt amount plus proportional
    ///       profit based on `_debtWinCoef`,
    ///     * coupon bets — profit based on `_couponWinCoef`.
    /// - Updates `_totalWinPool` to track remaining distributable
    ///   winning stake amounts.
    /// - Notifies the caller’s PrivateNote via `onClaimAccepted`.
    /// - Emits `ClaimProcessed` only for resolved events.
    /// - If `_totalWinPool` becomes zero after processing,
    ///   the contract self-destructs to `_deployer`.
    ///
    /// @param stakeAmount Array of clean stake amounts per outcome.
    /// @param debtAmount Array of debt stake amounts per outcome.
    /// @param couponsAmount Array of coupon stake amounts per outcome.
    /// @param depositIdentifierHash Deposit identifier hash used to
    ///        deterministically compute the caller's PrivateNote address.
    function claim(
    uint128[] stakeAmount,
    uint128[] debtAmount,
    uint128[] couponsAmount,
    uint256 depositIdentifierHash
    ) public {
        require(_approved, ERR_NOT_APPROVED);
        require(_orderBookDone, ERR_ORDERBOOK_NOT_SHUTDOWN);
        require(stakeAmount.length == _numOutcomes, ERR_INVALID_OUTCOME_ID);
        address wallet = DexLib.computePrivateNoteAddress(_privateNoteCode, depositIdentifierHash);
        require(msg.sender == wallet, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        if (!_resolvedOutcome.hasValue()) {
            PrivateNote(wallet).onClaimAccepted{
                value: 0.1 vmshell,
                flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }(_eventId, _oracleListHash, _tokenType, _resolvedOutcome, 0, 0, 0, 0);
            return;
        }

        // Alternate path: no winning side at resolve, but debt-funded losers
        // exist. Refund their principal across all outcomes; clean / coupon
        // stakes are forfeit and the residual is swept to the deployer once
        // the last debt position has been claimed.
        if (_debtRefundMode) {
            uint128 debtSum = 0;
            for (uint32 i = 0; i < debtAmount.length; i++) {
                debtSum += debtAmount[i];
            }
            if (debtSum > _debtRefundRemaining) {
                debtSum = _debtRefundRemaining;
            }
            if (debtSum > _totalUnclaimedBalance) {
                debtSum = _totalUnclaimedBalance;
            }
            _debtRefundRemaining -= debtSum;
            _totalUnclaimedBalance -= debtSum;

            PrivateNote(wallet).onClaimAccepted{
                value: 0.1 vmshell,
                flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }(_eventId, _oracleListHash, _tokenType, _resolvedOutcome,
              0, debtSum, 0, 0);

            address addrExternRefund = address.makeAddrExtern(PMP_CLAIM_PROCESSED, bitCntAddress);
            emit ClaimProcessed{dest: addrExternRefund}(wallet, debtSum, debtSum > 0);

            // Close once the only un-refunded debt left is the forfeited debt
            // (forfeiters deleted their PN record and can never claim it).
            if (_debtRefundRemaining <= _forfeitedDebtTotal()) {
                _finalizeResidualClose();
            }
            return;
        }

        uint32 W = _resolvedOutcome.get();
        uint128 payoutClean  = 0;
        uint128 payoutDebt   = 0;
        uint128 payoutCoupon = 0;
        uint128 debtPaid     = 0;
        bool win             = false;
        // Proportional clean claim — share of profitBudget by stake share in
        // the live winning clean pool, plus the principal returned 1:1:
        //   payoutClean = stakeAmount[W]
        //               + floor(stakeAmount[W] * _profitToClean / _resolvedWinClean)
        if (_resolvedWinClean > 0 && stakeAmount[W] > 0) {
            uint128 cleanProfit = uint128(
                (uint256(stakeAmount[W]) * uint256(_profitToClean)) / uint256(_resolvedWinClean)
            );
            payoutClean = stakeAmount[W] + cleanProfit;
            win = true;
        }

        if (debtAmount.length > W && debtAmount[W] > 0) {
            uint128 profit = uint128((uint256(debtAmount[W]) * uint256(_debtWinCoef)) / FULL_PERCENT);
            payoutDebt = debtAmount[W] + profit;

            // Formula 17: debtPaid_i = ⌊(profit · R) / (P - R)⌋
            // Skipped when there are no clean winners — without a redistribution
            // recipient the debt-share carve-out doesn't apply (resolve() also
            // gave debt winners the full PPU in that case), so no implicit
            // debt repayment happens here.
            if (_resolvedWinClean > 0) {
                debtPaid = uint128(
                    (uint256(profit) * uint256(DEBT_REDISTRIBUTION_PERCENT)) /
                    uint256(FULL_PERCENT - DEBT_REDISTRIBUTION_PERCENT)
                );
            }
            win = true;
        }

        if (couponsAmount.length > W && couponsAmount[W] > 0) {
            payoutCoupon = uint128((uint256(couponsAmount[W]) * uint256(_couponWinCoef)) / FULL_PERCENT);
            win = true;
        }

        // Defensive cap per reward type: under correct arithmetic never triggers.
        if (payoutClean > _totalRewardsClean) {
            payoutClean = _totalRewardsClean;
        }
        _totalRewardsClean -= payoutClean;

        if (payoutDebt > _totalRewardsDebt) {
            payoutDebt = _totalRewardsDebt;
            uint128 debtPrincipal = debtAmount.length > W ? debtAmount[W] : 0;
            uint128 newDebtProfit = payoutDebt > debtPrincipal ? payoutDebt - debtPrincipal : 0;
            if (_resolvedWinClean > 0) {
                debtPaid = uint128(
                    (uint256(newDebtProfit) * uint256(DEBT_REDISTRIBUTION_PERCENT)) /
                    uint256(FULL_PERCENT - DEBT_REDISTRIBUTION_PERCENT)
                );
            } else {
                debtPaid = 0;
            }
        }
        _totalRewardsDebt -= payoutDebt;

        if (payoutCoupon > _totalRewardsCoupon) {
            payoutCoupon = _totalRewardsCoupon;
        }
        _totalRewardsCoupon -= payoutCoupon;

        uint128 totalPayout = payoutClean + payoutDebt + payoutCoupon;

        // Defense-in-depth: clamp the payout to the tracked unclaimed
        // balance. If math drift would have caused an over-pay, the user
        // gets less (down to 0) instead of the pool leaking funds. We trim
        // components in order of "speculation" — coupon (free bet), then
        // debt (redistributed profit), then clean (principal-backed).
        if (totalPayout > _totalUnclaimedBalance) {
            uint128 deficit = totalPayout - _totalUnclaimedBalance;
            uint128 cut;
            if (deficit > 0 && payoutCoupon > 0) {
                cut = deficit > payoutCoupon ? payoutCoupon : deficit;
                payoutCoupon -= cut; deficit -= cut;
            }
            if (deficit > 0 && payoutDebt > 0) {
                cut = deficit > payoutDebt ? payoutDebt : deficit;
                payoutDebt -= cut; deficit -= cut;
            }
            if (deficit > 0 && payoutClean > 0) {
                cut = deficit > payoutClean ? payoutClean : deficit;
                payoutClean -= cut; deficit -= cut;
            }
            totalPayout = payoutClean + payoutDebt + payoutCoupon;
        }
        _totalUnclaimedBalance -= totalPayout;

        PrivateNote(wallet).onClaimAccepted{
            value: 0.1 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }(
            _eventId,
            _oracleListHash,
            _tokenType,
            _resolvedOutcome,
            payoutClean,
            payoutDebt,
            payoutCoupon,
            debtPaid
        );

        address addrExtern = address.makeAddrExtern(PMP_CLAIM_PROCESSED, bitCntAddress);
        emit ClaimProcessed{dest: addrExtern}(wallet, totalPayout, win);
        if (_totalWinPool > 0) {
            uint128 debtW = (debtAmount.length > W) ? debtAmount[W] : 0;
            uint128 couponsW = (couponsAmount.length > W) ? couponsAmount[W] : 0;
            uint128 claimedMass = stakeAmount[W] + debtW + couponsW;
            if (_totalWinPool >= claimedMass) {
                _totalWinPool -= claimedMass;
            } else {
                _totalWinPool = 0;
            }
        }
        // Close once the only unclaimed winning mass left is the forfeited mass
        // (forfeiters deleted their PN record and can never claim it). `<=` (not
        // `==`) so the `_totalWinPool = 0` clamp above can't make us skip it.
        // OrderBook is already down (gate for `claim` entry).
        if (_totalWinPool <= _forfeitedWinMass()) {
            _finalizeWinClose();
        }
    }

    /// @notice Forfeited winning-outcome mass (all bet types) for the resolved
    ///         outcome. The win close fires when `_totalWinPool` has decayed to
    ///         this (only forfeited winners left unclaimed).
    function _forfeitedWinMass() private view returns (uint128) {
        uint32 W = _resolvedOutcome.get();
        return _forfeited[W][BET_TYPE_CLEAN]
             + _forfeited[W][BET_TYPE_DEBT]
             + _forfeited[W][BET_TYPE_COUPON];
    }

    /// @notice Total forfeited debt across all outcomes (debt-refund close
    ///         counter — there is no single winning outcome in that mode).
    function _forfeitedDebtTotal() private view returns (uint128) {
        uint128 s = 0;
        for (uint32 i = 0; i < _numOutcomes; i++) {
            s += _forfeited[i][BET_TYPE_DEBT];
        }
        return s;
    }

    /// @notice True once every non-forfeited stake has been refunded on a
    ///         cancelled event — i.e. the live pools have decayed to exactly
    ///         the forfeited map (cancelStake decrements `_typedOutcomePools`).
    function _poolsEqualForfeited() private view returns (bool) {
        for (uint32 i = 0; i < _numOutcomes; i++) {
            if (_typedOutcomePools[i][BET_TYPE_CLEAN]  > _forfeited[i][BET_TYPE_CLEAN])  return false;
            if (_typedOutcomePools[i][BET_TYPE_DEBT]   > _forfeited[i][BET_TYPE_DEBT])   return false;
            if (_typedOutcomePools[i][BET_TYPE_COUPON] > _forfeited[i][BET_TYPE_COUPON]) return false;
        }
        return true;
    }

    /// @notice Sweeps the win-path residual (leftover rewards, incl. the
    ///         forfeited winners' shares) to the deployer and self-destructs.
    function _finalizeWinClose() private {
        uint128 residual = _totalRewardsClean + _totalRewardsDebt + _totalRewardsCoupon;
        _totalRewardsClean  = 0;
        _totalRewardsDebt   = 0;
        _totalRewardsCoupon = 0;
        // Clamp to whatever's left in the unclaimed balance (math-drift net).
        if (residual > _totalUnclaimedBalance) {
            residual = _totalUnclaimedBalance;
        }
        if (residual > 0) {
            _totalUnclaimedBalance -= residual;
            PrivateNote(_deployer).acceptFee{
                value: 0.1 vmshell,
                flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }(residual, _tokenType, _eventId, _oracleListHash);
        }
        selfdestruct(_deployer);
    }

    /// @notice Sweeps the remaining real balance (the forfeited principal) to
    ///         the deployer and self-destructs. Used by the debt-refund and
    ///         cancelled close paths (both leave `_totalUnclaimedBalance` ==
    ///         forfeited at close).
    function _finalizeResidualClose() private {
        if (_totalUnclaimedBalance > 0) {
            uint128 residual = _totalUnclaimedBalance;
            _totalUnclaimedBalance = 0;
            PrivateNote(_deployer).acceptFee{
                value: 0.1 vmshell,
                flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
            }(residual, _tokenType, _eventId, _oracleListHash);
        }
        selfdestruct(_deployer);
    }

    /// @notice Closes the PMP if the only unclaimed/un-refunded mass left is the
    ///         forfeited mass, in whichever lifecycle phase applies. Called
    ///         after a forfeit and from `onOrderBookShutdownComplete` (deferred
    ///         close when the counter hit the threshold mid OrderBook drain).
    function _tryClose() private {
        if (_resolvedOutcome.hasValue()) {
            // Don't self-destruct while the OrderBook is still draining.
            if (!_orderBookDone) return;
            if (_debtRefundMode) {
                if (_debtRefundRemaining <= _forfeitedDebtTotal()) {
                    _finalizeResidualClose();
                }
            } else if (_totalWinPool <= _forfeitedWinMass()) {
                _finalizeWinClose();
            }
        } else if (_isCancelled) {
            if (_frozen && !_orderBookDone) return;
            if (_poolsEqualForfeited()) {
                _finalizeResidualClose();
            }
        }
        // Pre-resolve, not cancelled (active market): nothing to close.
    }

    /// @notice Forfeit path: a PrivateNote calls this from `deleteStake` to drop
    ///         its stake without claiming a payout. In EVERY lifecycle state the
    ///         abandoned mass is recorded in `_forfeited` (pools / close counters
    ///         are never mutated); the close conditions in `claim` / `cancelStake`
    ///         / `_tryClose` then fire once the remaining mass equals the
    ///         forfeited total. Forfeited principal/rewards are swept to the
    ///         deployer at close.
    ///
    /// @param stakeAmount   Clean stakes per outcome.
    /// @param debtAmount    Debt stakes per outcome.
    /// @param couponsAmount Coupon stakes per outcome.
    /// @param depositIdentifierHash Hash that derives the caller's PN address.
    function forfeitStake(
        uint128[] stakeAmount,
        uint128[] debtAmount,
        uint128[] couponsAmount,
        uint256 depositIdentifierHash
    ) public {
        address wallet = DexLib.computePrivateNoteAddress(_privateNoteCode, depositIdentifierHash);
        require(msg.sender == wallet, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();

        uint32 nMax = uint32(stakeAmount.length);
        if (uint32(debtAmount.length) > nMax) nMax = uint32(debtAmount.length);
        if (uint32(couponsAmount.length) > nMax) nMax = uint32(couponsAmount.length);
        for (uint32 i = 0; i < nMax; i++) {
            if (i < uint32(stakeAmount.length))   _forfeited[i][BET_TYPE_CLEAN]  += stakeAmount[i];
            if (i < uint32(debtAmount.length))    _forfeited[i][BET_TYPE_DEBT]   += debtAmount[i];
            if (i < uint32(couponsAmount.length)) _forfeited[i][BET_TYPE_COUPON] += couponsAmount[i];
        }

        // Ack first so PN clears `_busy` / deletes its local stake before any
        // selfdestruct flushes this tx's outbound messages.
        PrivateNote(wallet).onForfeitAccepted{
            value: 0.05 vmshell,
            flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
        }();

        // This forfeit may be the action that makes forfeited == remaining.
        _tryClose();
    }

    /// @notice Returns the oracle pubkey from the current message sender.
    ///         Accepts both internal (address-based) and external (pubkey-based) messages.
    /// @return pubkey Resolved oracle public key bound to the caller.
    function _getOraclePubkey() private view returns (uint256 pubkey) {
        if (msg.isInternal) {
            require(_oracleEventsAddress.exists(msg.sender.value), ERR_INVALID_SENDER);
            pubkey = _oracleEventsAddress[msg.sender.value];
        } else {
            pubkey = msg.pubkey();
            require(_oracleEventsPubkeys.exists(pubkey), ERR_INVALID_SENDER);
        }
    }

    /// @notice Computes the quorum threshold: ceil(total * 66 / 100)
    /// @return quorumCount Minimum oracle votes required to execute governance actions.
    function _quorum() private view returns (uint128) {
        return uint128((_numberOfOracleEvents * uint128(THRESHOLD) + 9999) / 10000);
    }

    /// @notice Oracle submits timing parameters; executes when 66% quorum is reached.
    ///         An oracle can update its submission before quorum is reached.
    /// @param resultStart Result acceptance start time (resultEnd = resultStart + GRACE_PERIOD)
    function submitSetTimings(
        uint64 resultStart
    ) public {
        uint256 pubkey = _getOraclePubkey();
        tvm.accept();
        ensureBalance();

        uint256 newHash = tvm.hash(abi.encode(resultStart));

        if (_timingsOracleHash.exists(pubkey)) {
            uint256 oldHash = _timingsOracleHash[pubkey];
            if (oldHash == newHash) return;
            _timingsHashCount[oldHash] -= 1;
            if (_timingsHashCount[oldHash] == 0) {
                delete _timingsHashCount[oldHash];
            }
        }

        _timingsOracleHash[pubkey] = newHash;
        _timingsHashCount[newHash] += 1;

        uint128 count = _timingsHashCount[newHash];
        if (count >= _quorum()) {
            delete _timingsOracleHash;
            delete _timingsHashCount;
            setTimings(resultStart);
        }
    }

    /// @notice Oracle submits resolve outcome; executes when 66% quorum is reached.
    ///         An oracle can update its submission before quorum is reached.
    /// @param outcomeId Outcome identifier to resolve
    function submitResolve(uint32 outcomeId) public {
        uint256 pubkey = _getOraclePubkey();
        tvm.accept();
        ensureBalance();

        uint256 newHash = tvm.hash(abi.encode(outcomeId));

        if (_resolveOracleHash.exists(pubkey)) {
            uint256 oldHash = _resolveOracleHash[pubkey];
            if (oldHash == newHash) return;
            _resolveHashCount[oldHash] -= 1;
            if (_resolveHashCount[oldHash] == 0) {
                delete _resolveHashCount[oldHash];
            }
        }

        _resolveOracleHash[pubkey] = newHash;
        _resolveHashCount[newHash] += 1;

        uint128 count = _resolveHashCount[newHash];
        if (count >= _quorum()) {
            delete _resolveOracleHash;
            delete _resolveHashCount;
            resolve(outcomeId);
        }
    }

    /// @notice Oracle votes to cancel the event; executes when 66% quorum is reached.
    ///         Each oracle can only vote once.
    function submitCancelEvent() public {
        uint256 pubkey = _getOraclePubkey();
        tvm.accept();
        ensureBalance();

        if (_cancelOracleVoted.exists(pubkey)) return;

        _cancelOracleVoted[pubkey] = true;
        _cancelVoteCount += 1;

        if (_cancelVoteCount >= _quorum()) {
            delete _cancelOracleVoted;
            _cancelVoteCount = 0;
            cancelEvent();
        }
    }

    /// @notice Handles bounce from downstream oracle calls and rolls back oracle confirmations.
    /// @dev
    /// - If bounce sender is one of configured oracle event lists, the contract asks every
    ///   already-confirmed oracle list to cancel this event and then self-destructs.
    /// - Prevents half-initialized market state after failed oracle interactions.
    /// @param body Bounced message body (unused, accepted to satisfy ABI).
    onBounce(TvmSlice body) external {
        tvm.accept();
        ensureBalance();
        body;
        if (_oracleEventsConfirmed.exists(msg.sender.value)) {
            // Only cancel oracles that have already confirmed (not the one that bounced)
            for ((uint256 key, bool confirmed) : _oracleEventsConfirmed) {
                if (confirmed) {
                    OracleEventList(address.makeAddrStd(0, key)).cancelEvent{
                        value: 0.1 vmshell,
                        flag: 1, dest_dapp_id: ORACLE_DAPP_ID
                    }(_eventId, _oracleListHash, _tokenType);
                }
            }

            // Refund the deployer's initial stakes — symmetric with rejectEvent.
            // Must fire regardless of `_approvedOracleEvents`: post-partial-approval
            // the stakes are already committed in PN as `stake.amount[k]`, and
            // without this callback they'd stay locked once the PMP is gone.
            if (_initialStakes.length > 0) {
                uint128 refundTotal = 0;
                for (uint32 i = 0; i < _initialStakes.length; i++) {
                    refundTotal += _initialStakes[i];
                }
                PrivateNote(_deployer).onInitialStakesFailed{
                    value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID
                }(_eventId, _oracleListHash, _tokenType, refundTotal);
            }

            selfdestruct(ROOT_PN_ADDRESS);
        }
    }

    /// @notice Returns full current state of the PMP contract.
    /// @dev
    /// - Provides a complete snapshot of event configuration,
    ///   lifecycle state, oracle confirmations and pool balances.
    /// - Intended for frontend applications, indexers and analytics tools.
    /// - Does not modify contract state.
    ///
    /// @return name Human-readable pool name.
    /// @return tokenType Static token type used by the pool.
    /// @return eventId Identifier of the associated event.
    /// @return oracleListHash Hash of the oracle list used during deployment.
    /// @return deployer Address of the PrivateNote wallet that deployed the contract.
    /// @return privateNoteCodeHash Hash of the PrivateNote contract code used for address derivation.
    /// @return totalPool Total amount currently stored in the pool.
    /// @return approved Whether the event has been approved for staking.
    /// @return numOutcomes Total number of available outcomes.
    /// @return resolvedOutcome Final resolved outcome identifier (if set).
    /// @return stakeStart Stake acceptance start timestamp.
    /// @return stakeEnd Stake acceptance end timestamp.
    /// @return resultStart Result acceptance start timestamp.
    /// @return resultEnd Result acceptance end timestamp.
    /// @return isCancelled Whether the event has been cancelled.
    /// @return numberOfOracleEvents Total number of required oracle confirmations.
    /// @return approvedOracleEvents Number of oracle confirmations received.
    /// @return typedOutcomePools Mapping of outcome → bet type → pool amount.
    /// @return outcomeNames Mapping of outcome identifiers to human-readable names.
    /// @return creatorFee Creator fee collected at resolve.
    function getDetails() external view returns (
        string name,
        uint32 tokenType,
        uint256 eventId,
        uint256 oracleListHash,
        address deployer,
        uint256 privateNoteCodeHash,
        uint128 totalPool,
        bool approved,
        uint32 numOutcomes,
        optional(uint32) resolvedOutcome,
        uint64 stakeStart,
        uint64 stakeEnd,
        uint64 resultStart,
        uint64 resultEnd,
        bool isCancelled,
        uint128 numberOfOracleEvents,
        uint128 approvedOracleEvents,
        mapping(uint32 => mapping(uint8 => uint128)) typedOutcomePools,
        mapping(uint32 => string) outcomeNames,
        uint128 creatorFee,
        bool frozen,
        uint128 baseTotalPool,
        uint128 profitToClean,
        uint128 totalRewardsClean,
        uint128 totalRewardsDebt,
        uint128 totalRewardsCoupon
    ) {
        return (
            _name,
            _tokenType,
            _eventId,
            _oracleListHash,
            _deployer,
            tvm.hash(_privateNoteCode),
            _totalPool,
            _approved,
            _numOutcomes,
            _resolvedOutcome,
            _stakeStart,
            _computeStakeEnd(),
            _resultStart,
            (_resultStart + GRACE_PERIOD),
            _isCancelled,
            _numberOfOracleEvents,
            _approvedOracleEvents,
            _typedOutcomePools,
            _outcomeNames,
            _creatorFee,
            _frozen,
            _baseTotalPool,
            _profitToClean,
            _totalRewardsClean,
            _totalRewardsDebt,
            _totalRewardsCoupon
        );
    }

    /// @notice Shutdown-state getter for tests/monitoring.
    /// @return orderBookDone True once OrderBook has reported completion.
    /// @return shutdownTriggered True once PMP has fired the first shutdown.
    function getShutdownState() external view returns (bool orderBookDone, bool shutdownTriggered) {
        return (_orderBookDone, _shutdownTriggered);
    }

    /// @notice Returns the safety counter tracking total collateral the PMP
    ///         currently owes to participants. Useful for tests and external
    ///         monitors that want to detect math drift before a payout fails.
    function getUnclaimedBalance() external view returns (uint128) {
        return _totalUnclaimedBalance;
    }

    /// @notice Returns the OrderBook address for this PMP
    /// @return orderBookAddress Deterministic OrderBook address for this market.
    function getOrderBookAddress() external view returns (address orderBookAddress) {
        return _orderBookAddress;
    }

    /// @notice Callback from the OrderBook emitted as part of its final
    ///         self-destruct message (flag 161 — carries balance, deletes
    ///         source). Flips the gate that `claim()` checks.
    /// @notice Deployer PN acknowledges receipt and application of the
    ///         normalization refund. Clears `_normRefundPending`, re-enabling
    ///         `splitFullSet` / `mergeFullSet`. Auto-called at the tail of
    ///         `PrivateNote.onPmpCleanRefund`.
    function confirmRefundReceived(uint256 depositIdentifierHash) public {
        require(msg.sender == DexLib.computePrivateNoteAddress(_privateNoteCode, depositIdentifierHash),
                ERR_INVALID_SENDER);
        require(msg.sender == _deployer, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        _normRefundPending = false;
    }

    function onOrderBookShutdownComplete() public {
        require(msg.sender == _orderBookAddress, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();
        _orderBookDone = true;

        // A forfeit may have driven the closure counter to the forfeited
        // threshold while the book was still draining (close was deferred).
        _tryClose();
    }

    /// @notice Returns contract name
    /// @return value0 Contract semantic version.
    /// @return value1 Contract identifier.
    function getVersion() external pure returns (string, string) {
        return (version, "PMP");
    }
}
