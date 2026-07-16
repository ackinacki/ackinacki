pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./PMP.sol";
import "./libraries/DexLib.sol";

/// @notice Pull the weekly median reference price from an InferenceOrderBook
///         (spec §6.2). The OB calls onWeeklyMedian back here.
interface IInferenceOB {
    function requestWeeklyMedian(uint256 eventId, uint256 oracleListHash, uint32 tokenType) external;
}

/// @title Oracle Event List Contract
contract OracleEventList is Modifiers {

    /// @notice Contract semantic version.
    string constant version = "4.0.27";

    // ── Range events (numeric outcomes): the price is resolved on-chain from an
    // InferenceOrderBook's weekly median, mapped to a numeric range = outcome.
    // The Oracle converts ranges → string labels and feeds PMP ONLY strings, so
    // PMP is unchanged. This OEL is the single trust-addr oracle of the event
    // (decision: one oracle per range event), so it drives setTimings + resolve
    // through PMP's existing submitSetTimings/submitResolve quorum-of-1.
    struct RangeData {
        uint256[] bounds;   // strictly increasing upper bounds; n bounds → n+1 outcomes
        address ob;         // InferenceOrderBook providing reference_price (§6.2)
        bool exists;
    }
    mapping(uint256 => RangeData) _rangeData;

    uint16 constant ERR_NOT_RANGE_EVENT = 350;

    /// @notice Oracle contract address bound to this list (state-init static field).
    address static _oracle;
    /// @notice OracleEventList index for deterministic deployment (state-init static field).
    uint128 static _index;

    /// @notice Oracle owner pubkey used for access control and approvals.
    uint256 _oraclePubkey;

    /// @notice Hash of salted PMP code used to validate caller PMP address.
    uint256 _pmpSaltedCodeHash;
    /// @notice Depth of salted PMP code used to validate caller PMP address.
    uint16  _pmpSaltedCodeDepth;

    /// @notice Human-readable description of this OracleEventList (set at deploy).
    string _description;

    /// @notice Registry of events managed by this OracleEventList.
    mapping(uint256 => EventInfo) public _events;

    /// @notice Emitted when a new event is added to the registry.
    /// @param eventId Deterministic event identifier hash.
    /// @param eventName Human-readable event name.
    /// @param oracleFee Oracle fee required to confirm the event.
    /// @param deadline Service deadline timestamp.
    event EventAdded(uint256 eventId, string eventName, uint128 oracleFee, uint64 deadline);
    
    /// @notice Emitted when an event is confirmed for a PMP.
    /// @param eventId Event identifier hash.
    /// @param pmpAddress PMP address that received confirmation.
    event EventConfirmed(uint256 eventId, address pmpAddress);

    /// @notice Emitted when a RANGE event is added — carries the bound
    ///         InferenceOrderBook so an indexer can identify the linked
    ///         prediction-market (which book's price this PMP resolves on)
    ///         straight from the log, without a per-event getter call.
    /// @param eventId Event identifier hash.
    /// @param ob InferenceOrderBook whose weekly median resolves the range.
    /// @param bounds Numeric range bounds (ascending) bracketing the outcomes.
    event RangeEventAdded(uint256 eventId, address ob, uint256[] bounds);

    /// @notice Emitted when the list description is updated via setDescription.
    /// @param description New description string.
    event DescriptionUpdated(string description);

    /// @notice Initializes OracleEventList parameters.
    /// @param pubkey Oracle owner pubkey.
    /// @param pmpSaltedCodeHash Hash of salted PMP code.
    /// @param pmpSaltedCodeDepth Depth of salted PMP code.
    /// @param description Human-readable description of this list.
    constructor(
        uint256 pubkey,
        uint256 pmpSaltedCodeHash,
        uint16 pmpSaltedCodeDepth,
        string description
    ) {
        tvm.accept();
        // `_oracle` is a static field (set via stateInit to the legitimate
        // Oracle address). Require the sender to match that static field rather
        // than assigning `_oracle = msg.sender` in the constructor, so `_oracle`
        // is fixed by the deterministic stateInit and cannot be reassigned by the
        // deploying message.
        require(msg.sender == _oracle, ERR_INVALID_SENDER);
        // pubkey=0 would hand OEL admin access (addEvent/deleteEvent) to any
        // keyless ext tx — and since PMP.approveEvent propagates this into
        // _oracleEventsPubkeys, it would also break governance on downstream
        // PMPs. Reject at deploy.
        require(pubkey != 0, ERR_INVALID_PARAMS);
        _oraclePubkey = pubkey;
        _pmpSaltedCodeHash = pmpSaltedCodeHash;
        _pmpSaltedCodeDepth = pmpSaltedCodeDepth;
        _description = description;
    }

    /// @notice Ensures minimal native balance for operations.
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) return;
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Updates the human-readable description of this list. Only the
    ///         Oracle owner pubkey may rotate it.
    /// @param description New description string.
    function setDescription(string description) public onlyOwnerPubkey(_oraclePubkey) accept {
        ensureBalance();
        _description = description;
        address addrExtern = address.makeAddrExtern(ORACLE_LIST_DESCRIPTION_UPDATED, bitCntAddress);
        emit DescriptionUpdated{dest: addrExtern}(description);
    }
    
    /// @notice Adds a new event that Oracle is willing to service
    /// @param eventName Human-readable event name
    /// @param oracleFee Oracle fee
    /// @param deadline Timestamp when Oracle is ready to service
    /// @param describe Human-readable event description passed to PMP on approval.
    /// @param outcomeNames Mapping of outcome id to outcome label.
    /// @param trustAddr Trusted addr for oracle event
    function addEvent(
        string eventName,
        uint128 oracleFee,
        uint64 deadline,
        string describe,
        mapping(uint32 => string) outcomeNames,
        optional(uint256) trustAddr
    ) public onlyOwnerPubkey(_oraclePubkey) accept {
        require(deadline > block.timestamp, ERR_INVALID_PARAMS);
        ensureBalance();
        uint32 outcomeCount = uint32(outcomeNames.keys().length);
        require(outcomeCount >= 2, ERR_INVALID_PARAMS);
        require(outcomeCount < 20, ERR_INVALID_PARAMS);
        // Outcome ids must be a dense 0..n-1 range. PMP derives _numOutcomes from the
        // key count and indexes _typedOutcomePools by id, validating outcomeId <
        // _numOutcomes (PMP.stake/resolve). Requiring a dense 0-based map keeps the
        // stakeable ids and the resolvable ids identical.
        for (uint32 i = 0; i < outcomeCount; i++) {
            require(outcomeNames.exists(i), ERR_INVALID_PARAMS);
        }
        uint256 eventId = tvm.hash(abi.encode(eventName, deadline, describe, outcomeNames));
        require(!_events.exists(eventId), ERR_ALREADY_INITIALIZED);
        _events[eventId] = EventInfo({
            eventName: eventName,
            oracleFee: oracleFee,
            deadline: deadline,
            outcomeNames: outcomeNames,
            describe: describe,
            count: 0,
            trustAddr: trustAddr
        });

        address addrExtern = address.makeAddrExtern(ORACLE_EVENT_ADDED, bitCntAddress);
        emit EventAdded{dest: addrExtern}(eventId, eventName, oracleFee, deadline);
    }

    /// @notice Adds a numeric RANGE event (spec: Range Event). `bounds` are the
    ///         strictly increasing upper bounds; n bounds → n+1 outcomes
    ///         (`[0,b0)`,…,`[b_{n-1},inf)`). The creator passes the matching string
    ///         labels in `outcomeNames` (built off-chain — "Oracle converts ranges
    ///         to strings additionally"); the contract stores the numeric `bounds`
    ///         for on-chain mapping and feeds PMP ONLY the strings (PMP unchanged).
    ///         Resolves on-chain from `ob`'s weekly median (§6.2). This OEL is the
    ///         single trust-addr oracle of the event.
    /// @param outcomeNames Dense 0..n labels, exactly `bounds.length + 1` entries.
    /// @param ob InferenceOrderBook providing the reference price.
    function addRangeEvent(
        string eventName,
        uint128 oracleFee,
        uint64 deadline,
        string describe,
        uint256[] bounds,
        mapping(uint32 => string) outcomeNames,
        address ob
    ) public onlyOwnerPubkey(_oraclePubkey) accept {
        // deadline doubles as the PMP result-start; first setTimings needs the gap.
        require(deadline >= uint64(block.timestamp) + MIN_RESULT_GAP, ERR_INVALID_PARAMS);
        ensureBalance();
        uint32 n = uint32(bounds.length);
        require(n >= 1, ERR_INVALID_PARAMS);                         // ≥1 bound → ≥2 outcomes
        for (uint32 i = 1; i < n; i++) {
            require(bounds[i] > bounds[i - 1], ERR_INVALID_PARAMS);  // strictly increasing
        }
        // Labels must be dense 0..n (n+1 outcomes), matching the ranges.
        uint32 outcomeCount = uint32(outcomeNames.keys().length);
        require(outcomeCount == n + 1, ERR_INVALID_PARAMS);
        require(outcomeCount < 20, ERR_INVALID_PARAMS);
        for (uint32 i = 0; i < outcomeCount; i++) {
            require(outcomeNames.exists(i), ERR_INVALID_PARAMS);
        }

        uint256 eventId = tvm.hash(abi.encode(eventName, deadline, describe, outcomeNames));
        require(!_events.exists(eventId), ERR_ALREADY_INITIALIZED);
        optional(uint256) trustAddr;
        trustAddr.set(address(this).value);   // OEL is the single oracle (trust-addr)
        _events[eventId] = EventInfo({
            eventName: eventName, oracleFee: oracleFee, deadline: deadline,
            outcomeNames: outcomeNames, describe: describe, count: 0, trustAddr: trustAddr
        });
        _rangeData[eventId] = RangeData({bounds: bounds, ob: ob, exists: true});

        emit EventAdded{dest: address.makeAddrExtern(ORACLE_EVENT_ADDED, bitCntAddress)}(eventId, eventName, oracleFee, deadline);
        emit RangeEventAdded{dest: address.makeAddrExtern(ORACLE_RANGE_EVENT_ADDED, bitCntAddress)}(eventId, ob, bounds);
    }

    /// @notice Confirms an event for a PMP after fee and deadline checks.
    /// @param eventId Event identifier hash.
    /// @param oracleListHash Hash of PMP oracle list.
    /// @param tokenType PMP token type.
    function confirmEvent(uint256 eventId, uint256 oracleListHash, uint32 tokenType)
        public senderIs(DexLib.computePMPAddressFromHash(_pmpSaltedCodeHash, _pmpSaltedCodeDepth, eventId, oracleListHash, tokenType)) accept
    {
        ensureBalance();
        // Do NOT forward the oracle fee yet: on a reject path the oracle never
        // services the event, so the fee (msg.currencies) must go back to the
        // PMP (which handles staker refunds), not be gifted to the oracle. Only
        // the approve branch below pays the oracle.
        if (!_events.exists(eventId)) {
            PMP(msg.sender).rejectEvent{value: 0.1 vmshell, flag: 1, currencies: msg.currencies, dest_dapp_id: ROOT_PN_DAPP_ID}();
            return;
        }
        EventInfo eventInfo = _events[eventId];
        if ((eventInfo.deadline < block.timestamp) || (msg.currencies[CURRENCIES_ID_SHELL] < eventInfo.oracleFee)) {
            PMP(msg.sender).rejectEvent{value: 0.1 vmshell, flag: 1, currencies: msg.currencies, dest_dapp_id: ROOT_PN_DAPP_ID}();
        } else {
            _oracle.transfer({value: 0.1 vmshell, flag: 1, currencies: msg.currencies, dest_dapp_id: ORACLE_DAPP_ID});
            eventInfo.count += 1;
            _events[eventId] = eventInfo;
            PMP(msg.sender).approveEvent{value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID}(_oraclePubkey, eventInfo.outcomeNames, eventInfo.describe, eventInfo.eventName, eventInfo.trustAddr);
            // Range event: this OEL is the single oracle, so it also sets the PMP
            // timing (result-start = deadline). Sent after approveEvent so the
            // trust-addr oracle is already registered (quorum-of-1 → applies).
            if (_rangeData[eventId].exists) {
                PMP(msg.sender).submitSetTimings{value: 0.1 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID}(eventInfo.deadline);
            }
            address addrExtern = address.makeAddrExtern(ORACLE_EVENT_CONFIRMED, bitCntAddress);
            emit EventConfirmed{dest: addrExtern}(eventId, msg.sender);
        }
    }

    /// @notice Map a reference price to a range outcome id via stored bounds:
    ///         price < bounds[0] → 0; bounds[i-1] ≤ price < bounds[i] → i;
    ///         price ≥ bounds[n-1] → n.
    function _priceToOutcome(uint256 eventId, uint256 price) private view returns (uint32) {
        uint256[] bounds = _rangeData[eventId].bounds;
        uint32 n = uint32(bounds.length);
        for (uint32 i = 0; i < n; i++) {
            if (price < bounds[i]) { return i; }
        }
        return n;
    }

    /// @notice Resolve a RANGE event — callable by the oracle owner after the
    ///         deadline, once at least one PMP has confirmed the event. Pulls the
    ///         OB weekly median async; the mapping + PMP resolve happen in
    ///         onWeeklyMedian.
    function resolveRange(uint256 eventId, uint256 oracleListHash, uint32 tokenType)
        external onlyOwnerPubkey(_oraclePubkey) accept
    {
        require(_rangeData[eventId].exists, ERR_NOT_RANGE_EVENT);
        require(_events.exists(eventId), ERR_NOT_RANGE_EVENT);
        require(_events[eventId].deadline <= block.timestamp, ERR_INVALID_PARAMS);
        require(_events[eventId].count > 0, ERR_INVALID_PARAMS);
        ensureBalance();
        IInferenceOB(_rangeData[eventId].ob).requestWeeklyMedian{value: 1 vmshell, flag: 1, bounce: false}(
            eventId, oracleListHash, tokenType);
    }

    /// @notice OB callback with the weekly median (spec §6.2). Maps the price to a
    ///         numeric range outcome and resolves the PMP via the existing
    ///         submitResolve — this OEL is the single trust-addr oracle, so a
    ///         quorum-of-1 resolves immediately. PMP is unchanged.
    function onWeeklyMedian(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint256 price)
        public senderIs(_rangeData[eventId].ob) accept
    {
        ensureBalance();
        uint32 outcomeId = _priceToOutcome(eventId, price);
        address pmp = DexLib.computePMPAddressFromHash(_pmpSaltedCodeHash, _pmpSaltedCodeDepth, eventId, oracleListHash, tokenType);
        PMP(pmp).submitResolve{value: 0.2 vmshell, flag: 1, dest_dapp_id: ROOT_PN_DAPP_ID}(outcomeId);
    }

    /// @notice Range-event data (bounds + bound OB) for off-chain inspection.
    function getRangeData(uint256 eventId) external view returns (uint256[] bounds, address ob, bool exists) {
        RangeData rd = _rangeData[eventId];
        return (rd.bounds, rd.ob, rd.exists);
    }

    /// @notice Decrements active confirmation counter for an event when PMP is canceled.
    /// @param eventId Event identifier hash.
    /// @param oracleListHash Hash of PMP oracle list.
    /// @param tokenType PMP token type.
    function cancelEvent(uint256 eventId, uint256 oracleListHash, uint32 tokenType)
        public senderIs(DexLib.computePMPAddressFromHash(_pmpSaltedCodeHash, _pmpSaltedCodeDepth, eventId, oracleListHash, tokenType)) accept
    {
        ensureBalance();
        EventInfo eventInfo = _events[eventId];
        eventInfo.count -= 1;
        _events[eventId] = eventInfo;
    }

    /// @notice Deletes an event when there are no active confirmations or deadline is expired.
    /// @param eventId Event identifier hash.
    function deleteEvent(uint256 eventId) public onlyOwnerPubkey(_oraclePubkey) accept {
        ensureBalance();
        EventInfo eventInfo = _events[eventId];
        if ((eventInfo.count == 0) && (eventInfo.deadline < block.timestamp)) {
            delete _events[eventId];
        }
    } 
    
    /// @notice Returns contract version
    /// @return value0 Contract semantic version.
    /// @return value1 Contract identifier.
    function getVersion() external pure returns (string, string) {
        return (version, "OracleEventList");
    }
}
