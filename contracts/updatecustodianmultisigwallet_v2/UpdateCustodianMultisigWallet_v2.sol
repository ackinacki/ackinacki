pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

/// @title Multisignature wallet with setcode and exchange ecc.
contract UpdateCustodianMultisigWallet_V2 {

    /*
     *  Storage
     */
    struct Custodian {
        optional(uint256) owner_pubkey;
        optional(address) owner_address;
        uint8 index;
    }

    struct Transaction {
        // Transaction Id.
        uint64 id;
        // Transaction confirmations from custodians.
        uint32 confirmationsMask;
        // Number of required confirmations.
        uint8 signsRequired;
        // Number of confirmations already received.
        uint8 signsReceived;
        // Custodian queued transaction.
        Custodian creator;
        // Destination address of gram transfer.
        address dest;
        // Amount of nanograms to transfer.
        uint128 value;
        // Amount of ECC token to transfer.
        mapping(uint32 => varuint32) cc;
        // Flags for sending internal message (see SENDRAWMSG in TVM spec).
        uint16 sendFlags;
        // Payload used as body of outbound internal message.
        TvmCell payload;
        // Bounce flag for header of outbound internal message.
        bool bounce;
        // Recipient dapp_id. Threaded through the external interface and stored
        // (needed on confirm, where the event is emitted later), but NOT used
        // when the outbound message is actually sent: network-level dest_dapp_id
        // addressing is not wired yet, dapp_id is used only for reading account
        // state on the client.
        uint256 dapp_id;
    }

    struct UpdateData {
        // Data update Id.
        uint64 id;
        // Data update confirmations from custodians.
        uint32 confirmationsMask;
        // Number of required confirmations.
        uint8 signsRequired;
        // Number of confirmations already received.
        uint8 signsReceived;
        // Custodian queued transaction.
        Custodian creator;
        // New data of contract
        uint256[] owners_pubkey;
        address[] owners_address;

        uint8 reqConfirms;
        uint8 reqConfirmsData;
    }

    struct CodeUpdate {
        // Code update Id.
        uint64 id;
        // Code update confirmations from custodians.
        uint32 confirmationsMask;
        // Number of required confirmations.
        uint8 signsRequired;
        // Number of confirmations already received.
        uint8 signsReceived;
        // Custodian that queued the code update.
        Custodian creator;
        // New code of the contract.
        TvmCell newcode;
        // Migration data cell passed to onCodeUpgrade.
        TvmCell cell;
    }

    // Queued code update without the cells: a whole queue of full CodeUpdate
    // entries would carry a copy of the pending code each, so the listing form
    // identifies the code by hash instead.
    struct CodeUpdateInfo {
        uint64 id;
        uint32 confirmationsMask;
        uint8 signsRequired;
        uint8 signsReceived;
        uint8 creatorIndex;
        uint256 codeHash;
        uint256 cellHash;
    }

    struct BalanceConfig {
        // Below this vmshell balance the wallet tops itself up (0 disables it).
        uint128 minBalance;
        // vmshell balance the wallet converts SHELL up to when topping up.
        uint128 targetBalance;
    }

    struct ConfigUpdate {
        // Config update Id.
        uint64 id;
        // Config update confirmations from custodians.
        uint32 confirmationsMask;
        // Number of required confirmations.
        uint8 signsRequired;
        // Number of confirmations already received.
        uint8 signsReceived;
        // Custodian that queued this config update.
        Custodian creator;
        // New balance configuration to apply once confirmed.
        BalanceConfig config;
    }

    /*
     *  Constants
     */
    uint8   constant MAX_QUEUED_REQUESTS = 5;
    uint64  constant EXPIRATION_TIME = 3601; // lifetime is 1 hour
    uint8   constant MAX_CUSTODIAN_COUNT = 32;
    // Keeps every cleanup pass able to remove at least one expired request.
    uint    constant MIN_CLEANUP_OPERATIONS = 1;
    // Cleanup budget a freshly deployed wallet starts with.
    uint    constant DEFAULT_CLEANUP_OPERATIONS = 40;
    uint32 constant ZERO_TIME = 1000000000;
    uint16 constant SENDMSG_ALL_BALANCE = 128;
    // ECC currency id of SHELL, converted to vmshell by ensureBalance().
    uint32 constant CURRENCIES_ID_SHELL = 2;

    // Destination ids for external outbound messages produced by events.
    uint constant bitCntAddress = 256;
    uint128 constant TransactionSentEmit      = 1100;
    uint128 constant TransactionSubmittedEmit = 1101;
    uint128 constant TransactionConfirmedEmit = 1102;
    uint128 constant FundsReceivedEmit        = 1103;
    uint128 constant WalletSetupEmit          = 1104;
    uint128 constant ExecutionFailureEmit     = 1105;
    uint128 constant CustodiansUpdatedEmit    = 1108;
    uint128 constant CodeUpdateSubmittedEmit  = 1109;
    uint128 constant CodeUpdateConfirmedEmit  = 1110;
    uint128 constant CodeUpdateAppliedEmit    = 1111;
    uint128 constant UnknownCallEmit          = 1112;
    uint128 constant RequestsDroppedEmit      = 1113;
    uint128 constant ConfigUpdateSubmittedEmit = 1114;
    uint128 constant ConfigUpdateConfirmedEmit = 1115;
    uint128 constant ConfigUpdateAppliedEmit   = 1116;

    // Send flags.
    // Forward fees for message will be paid from contract balance.
    uint8 constant FLAG_PAY_FWD_FEE_FROM_BALANCE = 1;
    // Tells node to send all remaining balance.
    uint8 constant FLAG_SEND_ALL_REMAINING = 128;

    /*
     * Variables
     */
    optional(uint256) m_ownerKey;
    optional(address) m_ownerAddress;

    // Binary mask with custodian requests (max 32 custodians).
    uint256 m_requestsMask;
    // Binary mask with custodian requests (max 32 custodians).
    uint256 m_requestsMaskData;
    // Binary mask with custodian code-update requests (max 32 custodians).
    uint256 m_requestsMaskCode;
    // Binary mask with custodian config-update requests (max 32 custodians).
    uint256 m_requestsMaskConfig;
    // Dictionary of queued transactions waiting confirmations.
    mapping(uint64 => Transaction) m_transactions;
    // Dictionary of queued custodian-set updates waiting confirmations.
    mapping(uint64 => UpdateData) m_data;
    // Dictionary of queued code updates waiting confirmations.
    mapping(uint64 => CodeUpdate) m_code;
    // Dictionary of queued balance-config updates waiting confirmations.
    mapping(uint64 => ConfigUpdate) m_config;
    // Gas self-management config. minBalance == 0 disables auto top-up; survives
    // custodian changes (operator setting, changed only via submitConfigUpdate).
    BalanceConfig m_balanceConfig;
    // Set of custodians, initiated in constructor, but values can be changed later in code.
    mapping(uint256 => Custodian) m_custodians; // pub_key -> custodian_index
    // Read-only custodian count, initiated in constructor.
    uint8 m_custodianCount;
    // Default number of confirmations needed to execute transaction.
    uint8 m_defaultRequiredConfirmations;
    // Default number of confirmations needed to update data.
    uint8 m_defaultRequiredConfirmationsData;

    // Survives custodian changes: it is an operator setting, not part of the
    // custodian set, so _initialize must not reset it.
    uint _max_cleanup_operations = DEFAULT_CLEANUP_OPERATIONS;

    /*
    Exception codes:
    100 - message sender is not a custodian;
    101 - zero owner
    102 - transaction does not exist;
    103 - operation is already confirmed by this custodian;
    108 - wallet should have only one custodian;
    113 - Too many requests for one custodian;
    117 - invalid number of custodians;
    123 - need at least 1 reqConfirms
    124 - cleanup budget is below MIN_CLEANUP_OPERATIONS
    125 - zero transfer destination
    126 - balance config targetBalance is below minBalance
    */

    /*
     *  Events
     */
    /// @dev Emitted when an outbound message is actually sent. Every send carries
    /// its own transactionId, whether or not it was queued for confirmations.
    /// For a send-all transfer (flag 128) value is the balance being swept, taken
    /// before action-phase fees, because the value argument is ignored in that case.
    event TransactionSent(uint64 transactionId, address dest, uint128 value, mapping(uint32 => varuint32) cc, uint16 sendFlags, bool bounce, uint256 dapp_id);
    /// @dev Emitted when a transaction is queued for confirmations.
    event TransactionSubmitted(uint64 transactionId, uint8 creatorIndex, uint8 signsRequired, address dest, uint128 value, uint256 dapp_id);
    /// @dev Emitted when a queued transaction receives a confirmation and stays queued.
    event TransactionConfirmed(uint64 transactionId, uint8 custodianIndex, uint8 signsReceived, uint8 signsRequired);
    /// @dev Emitted whenever the custodian set or the confirmation thresholds change.
    event CustodiansUpdated(uint8 custodianCount, uint8 reqConfirms, uint8 reqConfirmsData);
    /// @dev Emitted when a custodian change drops the pending requests that were
    /// issued against the previous custodian set. Not emitted when nothing was
    /// pending, and the data update being applied is not counted as dropped.
    event RequestsDropped(uint32 transfers, uint32 dataUpdates, uint32 codeUpdates, uint32 configUpdates);
    /// @dev Emitted when a code update is queued, identifying the pending code by hash.
    event CodeUpdateSubmitted(uint64 codeUpdateId, uint8 creatorIndex, uint8 signsRequired, uint256 codeHash, uint256 cellHash);
    /// @dev Emitted when a queued code update receives a confirmation and stays queued.
    event CodeUpdateConfirmed(uint64 codeUpdateId, uint8 custodianIndex, uint8 signsReceived, uint8 signsRequired);
    /// @dev Emitted before the code is replaced. codeUpdateId is 0 when not queued.
    event CodeUpdateApplied(uint64 codeUpdateId, uint256 codeHash, uint256 cellHash);
    /// @dev Emitted when a balance-config update is queued.
    event ConfigUpdateSubmitted(uint64 configUpdateId, uint8 creatorIndex, uint8 signsRequired, uint128 minBalance, uint128 targetBalance);
    /// @dev Emitted when a queued config update receives a confirmation and stays queued.
    event ConfigUpdateConfirmed(uint64 configUpdateId, uint8 custodianIndex, uint8 signsReceived, uint8 signsRequired);
    /// @dev Emitted when the balance config is applied. configUpdateId is 0 when not queued.
    event ConfigUpdateApplied(uint64 configUpdateId, uint128 minBalance, uint128 targetBalance);
    /// @dev Emitted on a plain incoming transfer handled by `receive`.
    event FundsReceived(address sender, uint128 value, mapping(uint32 => varuint32) cc);
    /// @dev Emitted when a message arrives with a body the wallet does not handle,
    /// so funds cannot reach the wallet unreported.
    event UnknownCall(address sender, uint128 value, mapping(uint32 => varuint32) cc);
    /// @dev Emitted once from the constructor when the wallet is first set up.
    event WalletSetup(uint8 custodianCount, uint8 reqConfirms, uint8 reqConfirmsData);
    /// @dev Emitted from onBounce when an outbound transfer sent with bounce:true
    /// is returned. The queued request is already gone by then, so the leading
    /// function id of the returned body is reported to identify what bounced
    /// (0 when the body carries no function id).
    event ExecutionFailure(address dest, uint128 value, uint32 bouncedFunctionId);

    /*
     * Constructor
     */

    /// @dev Internal function called from constructor to initialize custodians.
    function _initialize(uint256[] owners_pubkey, address[] owners_address, uint8 reqConfirms, uint8 reqConfirmsData) inline private {
        // Queued requests carry the custodian indices and confirmation threshold
        // they were issued against, so they are dropped with the custodian set.
        // Report what is being dropped before the counters are cleared.
        uint32 droppedTransfers = _countRequests(m_requestsMask);
        uint32 droppedData = _countRequests(m_requestsMaskData);
        uint32 droppedCode = _countRequests(m_requestsMaskCode);
        uint32 droppedConfig = _countRequests(m_requestsMaskConfig);
        if (droppedTransfers + droppedData + droppedCode + droppedConfig > 0) {
            emit RequestsDropped{dest: address.makeAddrExtern(RequestsDroppedEmit, bitCntAddress)}(droppedTransfers, droppedData, droppedCode, droppedConfig);
        }
        delete m_requestsMask;
        delete m_requestsMaskData;
        delete m_requestsMaskCode;
        delete m_requestsMaskConfig;
        delete m_transactions;
        delete m_data;
        delete m_code;
        delete m_config;
        delete m_custodians;
        delete m_ownerKey;
        delete m_ownerAddress;
        address empty_address = address(0);
        if (owners_pubkey.length > 0){
            m_ownerKey = owners_pubkey[0];
        }
        if (owners_address.length > 0){
            m_ownerAddress = owners_address[0];
        }

        _validateOwners(owners_pubkey, owners_address);

        uint8 ownerCount = 0;
        uint256 len = owners_pubkey.length;
        for (uint256 i = 0; i < len; i++) {
            uint256 key = owners_pubkey[i];
            TvmBuilder b;
            b.store(key);
            b.store(empty_address);
            uint256 hash_key = tvm.hash(b.toCell());
            if (!m_custodians.exists(hash_key)) {
                m_custodians[hash_key] = Custodian(key, null, ownerCount++);
            }
        }

        len = owners_address.length;
        for (uint256 i = 0; i < len; i++) {
            address key_address = owners_address[i];
            TvmBuilder b;
            b.store(uint256(0));
            b.store(key_address);
            uint256 hash_key = tvm.hash(b.toCell());
            if (!m_custodians.exists(hash_key)) {
                m_custodians[hash_key] = Custodian(null, key_address, ownerCount++);
            }
        }

        require(ownerCount > 0, 101);
        m_defaultRequiredConfirmations = ownerCount <= reqConfirms ? ownerCount : reqConfirms;
        m_defaultRequiredConfirmationsData = ownerCount <= reqConfirmsData ? ownerCount : reqConfirmsData;
        m_defaultRequiredConfirmationsData = m_defaultRequiredConfirmationsData <= m_defaultRequiredConfirmations ? m_defaultRequiredConfirmations : m_defaultRequiredConfirmationsData;
        m_custodianCount = ownerCount;
    }

    /// @dev Reports the custodian set actually changing. Not emitted from the
    /// constructor, where the initial set is reported by WalletSetup instead.
    function _emitCustodiansUpdated() inline private view {
        emit CustodiansUpdated{dest: address.makeAddrExtern(CustodiansUpdatedEmit, bitCntAddress)}(m_custodianCount, m_defaultRequiredConfirmations, m_defaultRequiredConfirmationsData);
    }

    /// @dev Checks a custodian set before its update is queued, so a request
    /// cannot collect confirmations and then fail on the one that applies it.
    function _validateOwners(uint256[] owners_pubkey, address[] owners_address) inline private pure {
        uint256 keysCount = owners_pubkey.length + owners_address.length;
        require(keysCount > 0 && keysCount <= MAX_CUSTODIAN_COUNT, 117);
        for (uint256 i = 0; i < owners_pubkey.length; i++) {
            require(owners_pubkey[i] != 0, 101);
        }
        for (uint256 i = 0; i < owners_address.length; i++) {
            require(owners_address[i] != address(0), 101);
        }
    }

    /// @dev Value reported in TransactionSent. A send-all transfer ignores the
    /// value argument, so the balance about to be swept is reported instead.
    function _reportedValue(uint128 value, uint16 flags) inline private view returns (uint128) {
        return (flags & SENDMSG_ALL_BALANCE != 0) ? uint128(address(this).balance) : value;
    }

    /// @dev Contract constructor.
    /// @param owners_pubkey Array of custodian keys.
    /// @param owners_address Array of custodian addresses.
    /// @param reqConfirms Default number of confirmations required for executing transaction.
    /// @param minBalance Initial gas auto-top-up threshold (0 disables it).
    /// @param targetBalance vmshell balance to convert SHELL up to when topping up.
    constructor(uint256[] owners_pubkey, address[] owners_address, uint8 reqConfirms, uint8 reqConfirmsData, uint64 value, uint128 minBalance, uint128 targetBalance) {
        gosh.cnvrtshellq(value);
        require(msg.pubkey() == tvm.pubkey(), 100);
        require(reqConfirms > 0, 123);
        require(reqConfirmsData > 0, 123);
        require(targetBalance >= minBalance, 126);
        tvm.accept();
        m_balanceConfig = BalanceConfig(minBalance, targetBalance);
        _initialize(owners_pubkey, owners_address, reqConfirms, reqConfirmsData);
        emit WalletSetup{dest: address.makeAddrExtern(WalletSetupEmit, bitCntAddress)}(m_custodianCount, m_defaultRequiredConfirmations, m_defaultRequiredConfirmationsData);
        ensureBalance();
    }

    function setMaxCleanupOperations(uint value) public {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        cstd;
        require(value >= MIN_CLEANUP_OPERATIONS, 124);
        tvm.accept();
        ensureBalance();
        _max_cleanup_operations = value;
    }

    /*
     * Inline helper macros
     */

    /// @dev Returns queued transaction count by custodian with defined index.
    function _getMaskValue(uint256 mask, uint8 index) inline private pure returns (uint8) {
        return uint8((mask >> (8 * uint256(index))) & 0xFF);
    }

    /// @dev Increment queued transaction count by custodian with defined index.
    function _incMaskValue(uint256 mask, uint8 index) inline private pure returns (uint256) {
        return mask + (1 << (8 * uint256(index)));
    }

    /// @dev Decrement queued transaction count by custodian with defined index.
    function _decMaskValue(uint256 mask, uint8 index) inline private pure returns (uint256) {
        return mask - (1 << (8 * uint256(index)));
    }

    /// @dev Checks bit with defined index in the mask.
    function _checkBit(uint32 mask, uint8 index) inline private pure returns (bool) {
        return (mask & (uint32(1) << index)) != 0;
    }

    /// @dev Checks if object is confirmed by custodian.
    function _isConfirmed(uint32 mask, uint8 custodianIndex) inline private pure returns (bool) {
        return _checkBit(mask, custodianIndex);
    }

    /// @dev Sets custodian confirmation bit in the mask.
    function _setConfirmed(uint32 mask, uint8 custodianIndex) inline private pure returns (uint32) {
        mask |= (uint32(1) << custodianIndex);
        return mask;
    }

    /// @dev Total queued requests recorded in a per-custodian counter mask. Reads
    /// the counters instead of walking the queue, so the cost does not grow with it.
    function _countRequests(uint256 mask) inline private pure returns (uint32 total) {
        for (uint8 i = 0; i < MAX_CUSTODIAN_COUNT; i++) {
            total += uint32(_getMaskValue(mask, i));
        }
    }

    /// @dev Keeps the wallet's vmshell (gas) balance topped up. When it drops
    ///      below minBalance, converts enough SHELL to reach targetBalance.
    ///      No-op while minBalance is 0 (auto top-up disabled).
    function ensureBalance() private view {
        if (m_balanceConfig.minBalance == 0) { return; }
        if (uint128(address(this).balance) >= m_balanceConfig.minBalance) { return; }
        uint128 deficit = m_balanceConfig.targetBalance - uint128(address(this).balance);
        uint128 available = uint128(address(this).currencies[CURRENCIES_ID_SHELL]);
        uint128 toConvert = math.min(deficit, available);
        if (toConvert == 0) { return; }
        if (toConvert > type(uint64).max) { toConvert = type(uint64).max; }
        gosh.cnvrtshellq(uint64(toConvert));
    }

    /// @dev Checks that custodian with supplied public key exists in custodian set.
    function _findCustodian(uint256 senderKey, address senderAddress) inline private view returns (Custodian) {
        TvmBuilder b;
        b.store(senderKey);
        b.store(senderAddress);
        uint256 key = tvm.hash(b.toCell());
        optional(Custodian) custodian = m_custodians.fetch(key);
        require(custodian.hasValue(), 100);
        return custodian.get();
    }

    /// @dev Generates new id for object.
    function _generateId() inline private pure returns (uint64) {
        return (uint64(block.timestamp - ZERO_TIME) << 32) | (tx.logicaltime & 0xFFFFFFFF);
    }

    /// @dev Returns timestamp after which transactions are treated as expired.
    function _getExpirationBound() inline private pure returns (uint64) {
        return (uint64(block.timestamp) - EXPIRATION_TIME - ZERO_TIME) << 32;
    }

    /*
     * Public functions
     */
    /// @dev Allows custodian if she is the only owner of multisig to transfer funds with minimal fees.
    /// @param dest Transfer target address.
    /// @param value Amount of funds to transfer.
    /// @param cc Amount of ECC Token to transfer.
    /// @param bounce Bounce flag. Set true if need to transfer funds to existing account;
    /// set false to create new account.
    /// @param flags `sendmsg` flags.
    /// @param payload Tree of cells used as body of outbound internal message.
    /// @param dapp_id Recipient dapp id — reported in the event, not used for the transfer.
    function sendTransaction(
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        uint8 flags,
        TvmCell payload,
        uint256 dapp_id) public view returns(address)
    {
        require(m_custodianCount == 1, 108);
        if (m_ownerAddress.hasValue()) {
            require(msg.sender == m_ownerAddress.get(), 100);
        }
        if (m_ownerKey.hasValue()) {
            require(msg.pubkey() == m_ownerKey.get(), 100);
        }
        require(dest != address(0), 125);
        tvm.accept();
        ensureBalance();
        uint128 reported = _reportedValue(value, flags);
        dest.transfer(varuint16(value), bounce, flags, payload, cc);
        emit TransactionSent{dest: address.makeAddrExtern(TransactionSentEmit, bitCntAddress)}(_generateId(), dest, reported, cc, flags, bounce, dapp_id);
        return dest;
    }

    /// @dev Allows custodian to submit and confirm new transaction.
    /// @param dest Transfer target address.
    /// @param value Nanograms value to transfer.
    /// @param bounce Bounce flag. Set true if need to transfer grams to existing account; set false to create new account.
    /// @param flag Set flag.
    /// @param payload Tree of cells used as body of outbound internal message.
    /// @param dapp_id Recipient dapp id — stored and reported in the event, not used for the transfer.
    /// @return transId Transaction ID.
    function submitTransaction(
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        uint8 flag,
        TvmCell payload,
        uint256 dapp_id)
    public returns (uint64 transId)
    {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        require(dest != address(0), 125);
        _removeExpiredTransactions();
        require(_getMaskValue(m_requestsMask, cstd.index) < MAX_QUEUED_REQUESTS, 113);
        tvm.accept();
        ensureBalance();

        uint8 requiredSigns = m_defaultRequiredConfirmations;
        if (flag & SENDMSG_ALL_BALANCE != 0) {
            value = 0;
        }
        if (requiredSigns == 1) {
            uint128 reported = _reportedValue(value, flag);
            dest.transfer(varuint16(value), bounce, flag, payload, cc);
            emit TransactionSent{dest: address.makeAddrExtern(TransactionSentEmit, bitCntAddress)}(_generateId(), dest, reported, cc, flag, bounce, dapp_id);
            return 0;
        } else {
            m_requestsMask = _incMaskValue(m_requestsMask, cstd.index);
            uint64 trId = _generateId();
            Transaction txn = Transaction(trId, 0/*mask*/, requiredSigns, 0/*signsReceived*/,
                cstd, dest, value, cc, flag, payload, bounce, dapp_id);

            emit TransactionSubmitted{dest: address.makeAddrExtern(TransactionSubmittedEmit, bitCntAddress)}(trId, cstd.index, requiredSigns, dest, value, dapp_id);
            _confirmTransaction(trId, txn, cstd.index);
            return trId;
        }
    }

    /// @dev Allows custodian to confirm a transaction.
    /// @param transactionId Transaction ID.
    function confirmTransaction(uint64 transactionId) public {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredTransactions();
        optional(Transaction) otxn = m_transactions.fetch(transactionId);        
        require(otxn.hasValue(), 102);
        Transaction txn = otxn.get();
        require(!_isConfirmed(txn.confirmationsMask, cstd.index), 103);
        tvm.accept();
        ensureBalance();
        uint64 marker = _getExpirationBound();
        bool needCleanup = transactionId <= marker;
        if (needCleanup) {
            m_requestsMask = _decMaskValue(m_requestsMask, txn.creator.index);
            delete m_transactions[transactionId];
        } else {
            _confirmTransaction(transactionId, txn, cstd.index);
        }
    }

    /// @dev Allows custodian to submit and confirm new data of contract.
    /// @param owners_pubkey Array of custodian keys.
    /// @param owners_address Array of custodian address.
    /// @param reqConfirms Default number of confirmations required for executing transaction.
    function submitDataUpdate(
        uint256[] owners_pubkey,
        address[] owners_address, 
        uint8 reqConfirms,
        uint8 reqConfirmsData)
    public returns (uint64 transId)
    {
        require(reqConfirms > 0, 123);
        require(reqConfirmsData > 0, 123);
        _validateOwners(owners_pubkey, owners_address);
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredDataUpdate();
        require(_getMaskValue(m_requestsMaskData, cstd.index) < MAX_QUEUED_REQUESTS, 113);
        tvm.accept();
        ensureBalance();
        uint8 requiredSigns = m_defaultRequiredConfirmationsData;

        if (requiredSigns == 1) {
            _initialize(owners_pubkey, owners_address, reqConfirms, reqConfirmsData);
            _emitCustodiansUpdated();
            return 0;
        } else {
            m_requestsMaskData = _incMaskValue(m_requestsMaskData, cstd.index);
            uint64 dataUpdateId = _generateId();
            
            UpdateData du = UpdateData(dataUpdateId, 0/*mask*/, requiredSigns, 0/*signsReceived*/,
                cstd, owners_pubkey, owners_address, reqConfirms, reqConfirmsData);

            _confirmDataUpdate(dataUpdateId, du, cstd.index);
            return dataUpdateId;
        }
    }

    /// @dev Allows custodian to confirm a transaction.
    /// @param dataUpdateId Transaction ID.
    function confirmDataUpdate(uint64 dataUpdateId) public {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredDataUpdate();
        optional(UpdateData) odu = m_data.fetch(dataUpdateId);        
        require(odu.hasValue(), 102);
        UpdateData du = odu.get();
        require(!_isConfirmed(du.confirmationsMask, cstd.index), 103);
        tvm.accept();
        ensureBalance();
        uint64 marker = _getExpirationBound();
        bool needCleanup = dataUpdateId <= marker;
        if (needCleanup) {
            m_requestsMaskData = _decMaskValue(m_requestsMaskData, du.creator.index);
            delete m_data[dataUpdateId];
        } else {
            _confirmDataUpdate(dataUpdateId, du, cstd.index);
        }
    }

    /// @dev Allows a custodian to submit a contract code upgrade for confirmation.
    ///      The upgrade is applied only once reqConfirmsData custodians confirm it
    ///      (or immediately when a single confirmation is required).
    /// @param newcode New code of the contract.
    /// @param cell Migration data cell passed to onCodeUpgrade.
    /// @return codeUpdateId Id of the queued code update (0 when applied immediately).
    function submitUpdateCode(TvmCell newcode, TvmCell cell) public returns (uint64 codeUpdateId) {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredCodeUpdate();
        require(_getMaskValue(m_requestsMaskCode, cstd.index) < MAX_QUEUED_REQUESTS, 113);
        tvm.accept();
        ensureBalance();
        uint8 requiredSigns = m_defaultRequiredConfirmationsData;

        if (requiredSigns == 1) {
            _applyUpdateCode(0, newcode, cell);
            return 0;
        } else {
            m_requestsMaskCode = _incMaskValue(m_requestsMaskCode, cstd.index);
            codeUpdateId = _generateId();

            CodeUpdate cu = CodeUpdate(codeUpdateId, 0/*mask*/, requiredSigns, 0/*signsReceived*/,
                cstd, newcode, cell);

            emit CodeUpdateSubmitted{dest: address.makeAddrExtern(CodeUpdateSubmittedEmit, bitCntAddress)}(codeUpdateId, cstd.index, requiredSigns, tvm.hash(newcode), tvm.hash(cell));
            _confirmUpdateCode(codeUpdateId, cu, cstd.index);
            return codeUpdateId;
        }
    }

    /// @dev Allows a custodian to confirm a queued code update.
    /// @param codeUpdateId Code update id.
    function confirmUpdateCode(uint64 codeUpdateId) public {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredCodeUpdate();
        optional(CodeUpdate) ocu = m_code.fetch(codeUpdateId);
        require(ocu.hasValue(), 102);
        CodeUpdate cu = ocu.get();
        require(!_isConfirmed(cu.confirmationsMask, cstd.index), 103);
        tvm.accept();
        ensureBalance();
        uint64 marker = _getExpirationBound();
        bool needCleanup = codeUpdateId <= marker;
        if (needCleanup) {
            m_requestsMaskCode = _decMaskValue(m_requestsMaskCode, cu.creator.index);
            delete m_code[codeUpdateId];
        } else {
            _confirmUpdateCode(codeUpdateId, cu, cstd.index);
        }
    }

    /// @dev Allows a custodian to submit a balance-config change for confirmation.
    ///      Applied once reqConfirmsData custodians confirm it (or immediately when
    ///      a single confirmation is required). minBalance == 0 disables auto top-up.
    /// @param minBalance New minimum vmshell balance triggering a top-up.
    /// @param targetBalance New vmshell balance to convert SHELL up to.
    function submitConfigUpdate(uint128 minBalance, uint128 targetBalance) public returns (uint64 configUpdateId) {
        require(targetBalance >= minBalance, 126);
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredConfigUpdate();
        require(_getMaskValue(m_requestsMaskConfig, cstd.index) < MAX_QUEUED_REQUESTS, 113);
        tvm.accept();
        ensureBalance();
        uint8 requiredSigns = m_defaultRequiredConfirmationsData;

        if (requiredSigns == 1) {
            _applyConfigUpdate(0, BalanceConfig(minBalance, targetBalance));
            return 0;
        } else {
            m_requestsMaskConfig = _incMaskValue(m_requestsMaskConfig, cstd.index);
            configUpdateId = _generateId();

            ConfigUpdate cu = ConfigUpdate(configUpdateId, 0/*mask*/, requiredSigns, 0/*signsReceived*/,
                cstd, BalanceConfig(minBalance, targetBalance));

            emit ConfigUpdateSubmitted{dest: address.makeAddrExtern(ConfigUpdateSubmittedEmit, bitCntAddress)}(configUpdateId, cstd.index, requiredSigns, minBalance, targetBalance);
            _confirmConfigUpdate(configUpdateId, cu, cstd.index);
            return configUpdateId;
        }
    }

    /// @dev Allows a custodian to confirm a queued config update.
    /// @param configUpdateId Config update id.
    function confirmConfigUpdate(uint64 configUpdateId) public {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredConfigUpdate();
        optional(ConfigUpdate) ocu = m_config.fetch(configUpdateId);
        require(ocu.hasValue(), 102);
        ConfigUpdate cu = ocu.get();
        require(!_isConfirmed(cu.confirmationsMask, cstd.index), 103);
        tvm.accept();
        ensureBalance();
        uint64 marker = _getExpirationBound();
        bool needCleanup = configUpdateId <= marker;
        if (needCleanup) {
            m_requestsMaskConfig = _decMaskValue(m_requestsMaskConfig, cu.creator.index);
            delete m_config[configUpdateId];
        } else {
            _confirmConfigUpdate(configUpdateId, cu, cstd.index);
        }
    }

    /*
     * Internal functions
     */

    /// @dev Confirms transaction by custodian with defined index.
    /// @param transactionId Transaction id to confirm.
    /// @param txn Transaction object to confirm.
    /// @param custodianIndex Index of custodian.
    function _confirmTransaction(uint64 transactionId, Transaction txn, uint8 custodianIndex) inline private {
        if ((txn.signsReceived + 1) >= txn.signsRequired) {
            uint128 reported = _reportedValue(txn.value, txn.sendFlags);
            txn.dest.transfer(varuint16(txn.value), txn.bounce, txn.sendFlags, txn.payload, txn.cc);
            m_requestsMask = _decMaskValue(m_requestsMask, txn.creator.index);
            delete m_transactions[transactionId];
            emit TransactionSent{dest: address.makeAddrExtern(TransactionSentEmit, bitCntAddress)}(transactionId, txn.dest, reported, txn.cc, txn.sendFlags, txn.bounce, txn.dapp_id);
        } else {
            txn.confirmationsMask = _setConfirmed(txn.confirmationsMask, custodianIndex);
            txn.signsReceived++;
            m_transactions[transactionId] = txn;
            emit TransactionConfirmed{dest: address.makeAddrExtern(TransactionConfirmedEmit, bitCntAddress)}(transactionId, custodianIndex, txn.signsReceived, txn.signsRequired);
        }
    }

    /// @dev Confirms transaction by custodian with defined index.
    /// @param dataUpdateId Data update id to confirm.
    /// @param du Data update object to confirm.
    /// @param custodianIndex Index of custodian.
    function _confirmDataUpdate(uint64 dataUpdateId, UpdateData du, uint8 custodianIndex) inline private {
        if ((du.signsReceived + 1) >= du.signsRequired) {
            // Retire the request being applied first: it is executed, not dropped,
            // and must not be reported as such by _initialize.
            m_requestsMaskData = _decMaskValue(m_requestsMaskData, du.creator.index);
            delete m_data[dataUpdateId];
            _initialize(du.owners_pubkey, du.owners_address, du.reqConfirms, du.reqConfirmsData);
            _emitCustodiansUpdated();
        } else {
            du.confirmationsMask = _setConfirmed(du.confirmationsMask, custodianIndex);
            du.signsReceived++;
            m_data[dataUpdateId] = du;
        }
    }

    /// @dev Confirms a code update by custodian with defined index.
    ///      Applies the upgrade once the required number of confirmations is reached.
    /// @param codeUpdateId Code update id to confirm.
    /// @param cu Code update object to confirm.
    /// @param custodianIndex Index of custodian.
    function _confirmUpdateCode(uint64 codeUpdateId, CodeUpdate cu, uint8 custodianIndex) inline private {
        if ((cu.signsReceived + 1) >= cu.signsRequired) {
            m_requestsMaskCode = _decMaskValue(m_requestsMaskCode, cu.creator.index);
            delete m_code[codeUpdateId];
            _applyUpdateCode(codeUpdateId, cu.newcode, cu.cell);
        } else {
            cu.confirmationsMask = _setConfirmed(cu.confirmationsMask, custodianIndex);
            cu.signsReceived++;
            m_code[codeUpdateId] = cu;
            emit CodeUpdateConfirmed{dest: address.makeAddrExtern(CodeUpdateConfirmedEmit, bitCntAddress)}(codeUpdateId, custodianIndex, cu.signsReceived, cu.signsRequired);
        }
    }

    /// @dev Performs the actual code upgrade. The event is emitted before the
    ///      code is replaced, because onCodeUpgrade does not return to this frame.
    /// @param codeUpdateId Code update id, 0 when the upgrade was not queued.
    /// @param newcode New code of the contract.
    /// @param cell Migration data cell passed to onCodeUpgrade.
    function _applyUpdateCode(uint64 codeUpdateId, TvmCell newcode, TvmCell cell) inline private {
        emit CodeUpdateApplied{dest: address.makeAddrExtern(CodeUpdateAppliedEmit, bitCntAddress)}(codeUpdateId, tvm.hash(newcode), tvm.hash(cell));
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    /// @dev Confirms a config update by custodian with defined index.
    ///      Applies the balance config once the required confirmations are reached.
    /// @param configUpdateId Config update id to confirm.
    /// @param cu Config update object to confirm.
    /// @param custodianIndex Index of custodian.
    function _confirmConfigUpdate(uint64 configUpdateId, ConfigUpdate cu, uint8 custodianIndex) inline private {
        if ((cu.signsReceived + 1) >= cu.signsRequired) {
            m_requestsMaskConfig = _decMaskValue(m_requestsMaskConfig, cu.creator.index);
            delete m_config[configUpdateId];
            _applyConfigUpdate(configUpdateId, cu.config);
        } else {
            cu.confirmationsMask = _setConfirmed(cu.confirmationsMask, custodianIndex);
            cu.signsReceived++;
            m_config[configUpdateId] = cu;
            emit ConfigUpdateConfirmed{dest: address.makeAddrExtern(ConfigUpdateConfirmedEmit, bitCntAddress)}(configUpdateId, custodianIndex, cu.signsReceived, cu.signsRequired);
        }
    }

    /// @dev Applies the balance config. configUpdateId is 0 when not queued.
    function _applyConfigUpdate(uint64 configUpdateId, BalanceConfig config) inline private {
        m_balanceConfig = config;
        emit ConfigUpdateApplied{dest: address.makeAddrExtern(ConfigUpdateAppliedEmit, bitCntAddress)}(configUpdateId, config.minBalance, config.targetBalance);
    }

    /// @dev Removes expired transactions from storage.
    function _removeExpiredTransactions() inline private {
        uint64 marker = _getExpirationBound();
        optional(uint64, Transaction) otxn= m_transactions.min();
        if (!otxn.hasValue()) { return; }
        (uint64 trId, Transaction txn) = otxn.get();
        bool needCleanup = trId <= marker;
        if (!needCleanup) { return; }

        tvm.accept();
        uint i = 0;
        while (needCleanup && i < _max_cleanup_operations) {
            // transaction is expired, remove it
            i++;
            m_requestsMask = _decMaskValue(m_requestsMask, txn.creator.index);
            delete m_transactions[trId];
            otxn = m_transactions.next(trId);
            if (!otxn.hasValue()) {
                needCleanup = false;
            } else {
                (trId, txn) = otxn.get();
                needCleanup = trId <= marker;
            }
        }        
        tvm.commit();
    }

    /// @dev Removes expired code update from storage.
    function _removeExpiredDataUpdate() inline private {
        uint64 marker = _getExpirationBound();
        optional(uint64, UpdateData) odu= m_data.min();
        if (!odu.hasValue()) { return; }
        (uint64 dataUpdateId, UpdateData du) = odu.get();
        bool needCleanup = dataUpdateId <= marker;
        if (!needCleanup) { return; }

        tvm.accept();
        uint i = 0;
        while (needCleanup && i < _max_cleanup_operations) {
            // transaction is expired, remove it
            i++;
            m_requestsMaskData = _decMaskValue(m_requestsMaskData, du.creator.index);
            delete m_data[dataUpdateId];
            odu = m_data.next(dataUpdateId);
            if (!odu.hasValue()) {
                needCleanup = false;
            } else {
                (dataUpdateId, du) = odu.get();
                needCleanup = dataUpdateId <= marker;
            }
        }
        tvm.commit();
    }

    /// @dev Removes expired code updates from storage.
    function _removeExpiredCodeUpdate() inline private {
        uint64 marker = _getExpirationBound();
        optional(uint64, CodeUpdate) ocu = m_code.min();
        if (!ocu.hasValue()) { return; }
        (uint64 codeUpdateId, CodeUpdate cu) = ocu.get();
        bool needCleanup = codeUpdateId <= marker;
        if (!needCleanup) { return; }

        tvm.accept();
        uint i = 0;
        while (needCleanup && i < _max_cleanup_operations) {
            // code update is expired, remove it
            i++;
            m_requestsMaskCode = _decMaskValue(m_requestsMaskCode, cu.creator.index);
            delete m_code[codeUpdateId];
            ocu = m_code.next(codeUpdateId);
            if (!ocu.hasValue()) {
                needCleanup = false;
            } else {
                (codeUpdateId, cu) = ocu.get();
                needCleanup = codeUpdateId <= marker;
            }
        }
        tvm.commit();
    }

    /// @dev Removes expired config updates from storage.
    function _removeExpiredConfigUpdate() inline private {
        uint64 marker = _getExpirationBound();
        optional(uint64, ConfigUpdate) ocu = m_config.min();
        if (!ocu.hasValue()) { return; }
        (uint64 configUpdateId, ConfigUpdate cu) = ocu.get();
        bool needCleanup = configUpdateId <= marker;
        if (!needCleanup) { return; }

        tvm.accept();
        uint i = 0;
        while (needCleanup && i < _max_cleanup_operations) {
            // config update is expired, remove it
            i++;
            m_requestsMaskConfig = _decMaskValue(m_requestsMaskConfig, cu.creator.index);
            delete m_config[configUpdateId];
            ocu = m_config.next(configUpdateId);
            if (!ocu.hasValue()) {
                needCleanup = false;
            } else {
                (configUpdateId, cu) = ocu.get();
                needCleanup = configUpdateId <= marker;
            }
        }
        tvm.commit();
    }

    /*
     * Get methods
     */
    function isConfirmed(uint32 mask, uint8 index) public pure returns (bool confirmed) {
        confirmed = _isConfirmed(mask, index);
    }

    /// @dev Get-method that returns wallet configuration parameters.
    /// @return maxQueuedTransactions The maximum number of unconfirmed transactions that a custodian can submit.
    /// @return maxCustodianCount The maximum allowed number of wallet custodians.
    /// @return expirationTime Transaction lifetime in seconds.
    /// @return requiredTxnConfirms The minimum number of confirmations required to execute transaction.
    /// @return requiredDataConfirms The minimum number of confirmations required to execute data update.
    function getParameters() public view
        returns (uint8 maxQueuedTransactions,
                uint8 maxCustodianCount,
                uint64 expirationTime,
                uint8 requiredTxnConfirms,
                uint8 requiredDataConfirms
                ) {

        maxQueuedTransactions = MAX_QUEUED_REQUESTS;
        maxCustodianCount = MAX_CUSTODIAN_COUNT;
        expirationTime = EXPIRATION_TIME;
        requiredTxnConfirms = m_defaultRequiredConfirmations;
        requiredDataConfirms = m_defaultRequiredConfirmationsData;
    }

    /// @dev Get-method that returns the current expired-request cleanup budget,
    /// the value set through setMaxCleanupOperations.
    /// @return maxCleanupOperations Requests removed per cleanup pass.
    function getMaxCleanupOperations() public view returns (uint maxCleanupOperations) {
        maxCleanupOperations = _max_cleanup_operations;
    }

    /// @dev Get-method that returns transaction info by id. Returns the stored
    /// request even if it has already expired, unlike the listing get-methods.
    /// @return trans Transaction structure.
    /// Throws exception if transaction does not exist.
    function getTransaction(uint64 transactionId) public view
        returns (Transaction trans) {
        optional(Transaction) txn = m_transactions.fetch(transactionId);
        require(txn.hasValue(), 102);
        trans = txn.get();
    }

    /// @dev Get-method that returns data update info by id. Returns the stored
    /// request even if it has already expired, unlike the listing get-methods.
    /// @return data UpdateData structure.
    /// Throws exception if the data update does not exist.
    function getUpdateData(uint64 updateDataId) public view
        returns (UpdateData data) {
        optional(UpdateData) odu = m_data.fetch(updateDataId);
        require(odu.hasValue(), 102);
        data = odu.get();
    }

    /// @dev Get-method that returns array of pending transactions.
    /// Returns not expired transactions only.
    /// @return transactions Array of queued transactions.
    function getTransactions() public view returns (Transaction[] transactions) {
        uint64 bound = _getExpirationBound();
        optional(uint64, Transaction) otxn = m_transactions.min();
        while (otxn.hasValue()) {
            // returns only not expired transactions
            (uint64 id, Transaction txn) = otxn.get();
            if (id > bound) {
                transactions.push(txn);
            }
            otxn = m_transactions.next(id);
        }
    }

    /// @dev Get-method that returns array of pending transactions.
    /// Returns not expired transactions only.
    /// @return data Array of queued transactions.
    function getUpdateDatas() public view returns (UpdateData[] data) {
        uint64 bound = _getExpirationBound();
        optional(uint64, UpdateData) odu = m_data.min();
        while (odu.hasValue()) {
            // returns only not expired transactions
            (uint64 id, UpdateData du) = odu.get();
            if (id > bound) {
                data.push(du);
            }
            odu = m_data.next(id);
        }
    }

    /// @dev Get-method that returns queued data update ids.
    /// Returns not expired data updates only, like getUpdateDatas().
    /// @return ids Array of data update ids.
    function getUpdateDataIds() public view returns (uint64[] ids) {
        uint64 bound = _getExpirationBound();
        uint64 duId = 0;
        optional(uint64, UpdateData) odu = m_data.min();
        while (odu.hasValue()) {
            (duId, ) = odu.get();
            if (duId > bound) {
                ids.push(duId);
            }
            odu = m_data.next(duId);
        }
    }

    /// @dev Get-method that returns submitted transaction ids.
    /// Returns not expired transactions only, like getTransactions().
    /// @return ids Array of transaction ids.
    function getTransactionIds() public view returns (uint64[] ids) {
        uint64 bound = _getExpirationBound();
        uint64 trId = 0;
        optional(uint64, Transaction) otxn = m_transactions.min();
        while (otxn.hasValue()) {
            (trId, ) = otxn.get();
            if (trId > bound) {
                ids.push(trId);
            }
            otxn = m_transactions.next(trId);
        }
    }

    /// @dev Get-method that returns a queued code update by id, including the
    /// pending code itself so a custodian can inspect it before confirming.
    /// Returns the stored request even if it has already expired.
    /// @return codeUpdate CodeUpdate structure.
    /// Throws exception if the code update does not exist.
    function getUpdateCode(uint64 codeUpdateId) public view
        returns (CodeUpdate codeUpdate) {
        optional(CodeUpdate) ocu = m_code.fetch(codeUpdateId);
        require(ocu.hasValue(), 102);
        codeUpdate = ocu.get();
    }

    /// @dev Get-method that returns the queued code updates, identifying the
    /// pending code by hash rather than carrying the cells. Use getUpdateCode to
    /// fetch one entry with its code cell.
    /// Returns not expired code updates only.
    /// @return codeUpdates Array of queued code update summaries.
    function getUpdateCodes() public view returns (CodeUpdateInfo[] codeUpdates) {
        uint64 bound = _getExpirationBound();
        optional(uint64, CodeUpdate) ocu = m_code.min();
        while (ocu.hasValue()) {
            // returns only not expired code updates
            (uint64 id, CodeUpdate cu) = ocu.get();
            if (id > bound) {
                codeUpdates.push(CodeUpdateInfo(
                    cu.id, cu.confirmationsMask, cu.signsRequired, cu.signsReceived,
                    cu.creator.index, tvm.hash(cu.newcode), tvm.hash(cu.cell)));
            }
            ocu = m_code.next(id);
        }
    }

    /// @dev Get-method that returns queued code-update ids.
    /// Returns not expired code updates only.
    /// @return ids Array of code-update ids.
    function getUpdateCodeIds() public view returns (uint64[] ids) {
        uint64 bound = _getExpirationBound();
        uint64 cuId = 0;
        optional(uint64, CodeUpdate) ocu = m_code.min();
        while (ocu.hasValue()) {
            (cuId, ) = ocu.get();
            if (cuId > bound) {
                ids.push(cuId);
            }
            ocu = m_code.next(cuId);
        }
    }

    /// @dev Get-method that returns the current balance (gas) auto-top-up config.
    /// @return config Current BalanceConfig (minBalance == 0 means disabled).
    function getBalanceConfig() public view returns (BalanceConfig config) {
        config = m_balanceConfig;
    }

    /// @dev Get-method that returns a single queued config update.
    /// @param configUpdateId Config update id.
    /// @return configUpdate The queued config update.
    function getConfigUpdate(uint64 configUpdateId) public view
        returns (ConfigUpdate configUpdate) {
        optional(ConfigUpdate) ocu = m_config.fetch(configUpdateId);
        require(ocu.hasValue(), 102);
        configUpdate = ocu.get();
    }

    /// @dev Get-method that returns queued config updates (not expired only).
    /// @return configUpdates Array of queued config updates.
    function getConfigUpdates() public view returns (ConfigUpdate[] configUpdates) {
        uint64 bound = _getExpirationBound();
        optional(uint64, ConfigUpdate) ocu = m_config.min();
        while (ocu.hasValue()) {
            (uint64 id, ConfigUpdate cu) = ocu.get();
            if (id > bound) {
                configUpdates.push(cu);
            }
            ocu = m_config.next(id);
        }
    }

    /// @dev Get-method that returns queued config-update ids (not expired only).
    /// @return ids Array of config-update ids.
    function getConfigUpdateIds() public view returns (uint64[] ids) {
        uint64 bound = _getExpirationBound();
        uint64 cuId = 0;
        optional(uint64, ConfigUpdate) ocu = m_config.min();
        while (ocu.hasValue()) {
            (cuId, ) = ocu.get();
            if (cuId > bound) {
                ids.push(cuId);
            }
            ocu = m_config.next(cuId);
        }
    }

    /// @dev Get-method that returns info about wallet custodians.
    /// @return custodians Array of custodians.
    function getCustodians() public view returns (Custodian[] custodians) {
        optional(uint256, Custodian) oind = m_custodians.min();
        while (oind.hasValue()) {
            (uint256 key, Custodian cstd) = oind.get();
            custodians.push(cstd);
            oind = m_custodians.next(key);
        }
    }  

    // Code upgrades are performed through submitUpdateCode / confirmUpdateCode,
    // which require reqConfirmsData custodian confirmations (see above).

	// This function will never be called. But it must be defined.
	function onCodeUpgrade(TvmCell stateVars) private pure {
	}

    /*
     * Fallback function to receive simple transfers
     */
    
    fallback () external {
        emit UnknownCall{dest: address.makeAddrExtern(UnknownCallEmit, bitCntAddress)}(msg.sender, uint128(msg.value), msg.currencies);
    }

    receive () external {
        emit FundsReceived{dest: address.makeAddrExtern(FundsReceivedEmit, bitCntAddress)}(msg.sender, uint128(msg.value), msg.currencies);
    }

    /// @dev Handles bounced outbound transfers. Only messages sent with
    /// bounce:true can come back here; dest/value come from the bounce message.
    onBounce(TvmSlice body) external {
        uint32 bouncedFunctionId = 0;
        if (body.bits() >= 32) {
            bouncedFunctionId = body.load(uint32);
        }
        emit ExecutionFailure{dest: address.makeAddrExtern(ExecutionFailureEmit, bitCntAddress)}(msg.sender, uint128(msg.value), bouncedFunctionId);
    }

    function getVersion() external pure returns(string, string) {
        return ("2.4.0", "UpdateCustodianMultisigWallet_v2");
    }
}