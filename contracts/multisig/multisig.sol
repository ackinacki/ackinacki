pragma gosh-solidity >=0.76.1;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

interface IAccept {
    function acceptTransfer(bytes payload) external;
}

/// @title Multisignature wallet with setcode and exchange ecc.
contract MultisigWallet is IAccept {

    /*
     *  Storage
     */

    struct Transaction {
        // Transaction Id.
        uint64 id;
        // Transaction confirmations from custodians.
        uint32 confirmationsMask;
        // Number of required confirmations.
        uint8 signsRequired;
        // Number of confirmations already received.
        uint8 signsReceived;
        // Public key of custodian queued transaction.
        uint256 creator;
        // Index of custodian.
        uint8 index;
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
    }

    /// Request for code update
    struct UpdateRequest {
        // request id
        uint64 id;
        // index of custodian submitted request
        uint8 index;
        // number of confirmations from custodians
        uint8 signs;
        // confirmation binary mask
        uint32 confirmationsMask;
        // public key of custodian submitted request
        uint256 creator;
        // hash from code's tree of cells
        uint256 codeHash;
        // array with new wallet custodians
        uint256[] custodians;
        // Default number of confirmations required to execute transaction
        uint8 reqConfirms;
    }

    /*
     *  Constants
     */
    uint8   constant MAX_QUEUED_REQUESTS = 5;
    uint64  constant EXPIRATION_TIME = 3601; // lifetime is 1 hour
    uint8   constant MAX_CUSTODIAN_COUNT = 32;
    uint128 constant MIN_VALUE = 1e6;
    uint    constant MAX_CLEANUP_TXNS = 40;

    // Send flags.
    // Forward fees for message will be paid from contract balance.
    uint8 constant FLAG_PAY_FWD_FEE_FROM_BALANCE = 1;
    // Tells node to send all remaining balance.
    uint8 constant FLAG_SEND_ALL_REMAINING = 128;

    /*
     * Variables
     */
    // Public key of custodian who deployed a contract.
    uint256 m_ownerKey;
    // Binary mask with custodian requests (max 32 custodians).
    uint256 m_requestsMask;
    // Dictionary of queued transactions waiting confirmations.
    mapping(uint64 => Transaction) m_transactions;
    // Set of custodians, initiated in constructor, but values can be changed later in code.
    mapping(uint256 => uint8) m_custodians; // pub_key -> custodian_index
    // Read-only custodian count, initiated in constructor.
    uint8 m_custodianCount;
    // Set of update requests.
    mapping (uint64 => UpdateRequest) m_updateRequests;
    // Binary mask for storing update request counts from custodians.
    // Every custodian can submit only one request.
    uint32 m_updateRequestsMask;
    // Number of custodian confirmations for updating code
    uint8 m_requiredVotes;
    // Default number of confirmations needed to execute transaction.
    uint8 m_defaultRequiredConfirmations;

    /*
    Exception codes:
    100 - message sender is not a custodian;
    101 - zero owner
    102 - transaction does not exist;
    103 - operation is already confirmed by this custodian;
    107 - input value is too low;
    108 - wallet should have only one custodian;
    110 - Too many custodians;
    113 - Too many requests for one custodian;
    115 - update request does not exist;
    116 - update request already confirmed by this custodian;
    117 - invalid number of custodians;
    119 - stored code hash and calculated code hash are not equal;
    120 - update request is not confirmed;
    121 - payload size is too big;
    122 - object is expired;
    123 - need at least 1 reqConfirms
    */

    /*
     *  Events
     */
    event TransferAccepted(bytes payload);

    /*
     * Runtime functions
     */
    function tvm_ctos(TvmCell cell) private pure returns (uint /* slice */) {}
    function tvm_tree_cell_size(uint slice) private pure returns (uint, uint) {}

    /*
     * Constructor
     */

    /// @dev Internal function called from constructor to initialize custodians.
    function _initialize(uint256[] owners, uint8 reqConfirms) inline private {
        uint8 ownerCount = 0;
        m_ownerKey = owners[0];
        require(m_ownerKey != 0, 101);

        uint256 len = owners.length;
        for (uint256 i = 0; (i < len && ownerCount < MAX_CUSTODIAN_COUNT); i++) {
            uint256 key = owners[i];
            require(key != 0, 101);
            if (!m_custodians.exists(key)) {
                m_custodians[key] = ownerCount++;
            }
        }
        m_defaultRequiredConfirmations = ownerCount <= reqConfirms ? ownerCount : reqConfirms;
        m_requiredVotes = (ownerCount <= 2) ? ownerCount : ((ownerCount * 2 + 1) / 3);
        m_custodianCount = ownerCount;
    }

    /// @dev Contract constructor.
    /// @param owners Array of custodian keys.
    /// @param reqConfirms Default number of confirmations required for executing transaction.
    constructor(uint256[] owners, uint8 reqConfirms, uint64 value) {
        gosh.cnvrtshellq(value);
        require(msg.pubkey() == tvm.pubkey(), 100);
        require(owners.length > 0 && owners.length <= MAX_CUSTODIAN_COUNT, 117);
        require(reqConfirms > 0, 123);
        tvm.accept();
        _initialize(owners, reqConfirms);
    }

    function exchangeToken(uint64 value) public view {
        uint8 _index = _findCustodian(msg.pubkey());
        _index;
        tvm.accept();
        gosh.cnvrtshellq(value);
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

    function _isSubmitted(uint32 mask, uint8 custodianIndex) inline private pure returns (bool) {
        return _checkBit(mask, custodianIndex);
    }

    /// @dev Sets custodian confirmation bit in the mask.
    function _setConfirmed(uint32 mask, uint8 custodianIndex) inline private pure returns (uint32) {
        mask |= (uint32(1) << custodianIndex);
        return mask;
    }

    function _setSubmitted(uint32 mask, uint8 custodianIndex) inline private pure returns (uint32) {
        return _setConfirmed(mask, custodianIndex);
    }

    /// @dev Checks that custodian with supplied public key exists in custodian set.
    function _findCustodian(uint256 senderKey) inline private view returns (uint8) {
        optional(uint8) index = m_custodians.fetch(senderKey);
        require(index.hasValue(), 100);
        return index.get();
    }

    /// @dev Generates new id for object.
    function _generateId() inline private pure returns (uint64) {
        return (uint64(block.timestamp) << 32) | (tx.logicaltime & 0xFFFFFFFF);
    }

    /// @dev Returns timestamp after which transactions are treated as expired.
    function _getExpirationBound() inline private pure returns (uint64) {
        return (uint64(block.timestamp) - EXPIRATION_TIME) << 32;
    }

    /// @dev Returns transfer flags according to input value and `allBalance` flag.
    function _getSendFlags(bool allBalance) inline private pure returns (uint8) {        
        uint8 flags = FLAG_PAY_FWD_FEE_FROM_BALANCE;
        if (allBalance) {
            flags = FLAG_SEND_ALL_REMAINING;
        }
        return flags;
    }

    /*
     * Public functions
     */

    /// @dev A payable method for accepting incoming funds. Generates
    /// an event with incoming payload.
    /// @param payload Payload from message body.
    function acceptTransfer(bytes payload) external override {
        emit TransferAccepted(payload);
    }

    /// @dev Allows custodian if she is the only owner of multisig to transfer funds with minimal fees.
    /// @param dest Transfer target address.
    /// @param value Amount of funds to transfer.
    /// @param cc Amount of ECC Token to transfer.
    /// @param bounce Bounce flag. Set true if need to transfer funds to existing account;
    /// set false to create new account.
    /// @param flags `sendmsg` flags.
    /// @param payload Tree of cells used as body of outbound internal message.
    function sendTransaction(
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        uint8 flags,
        TvmCell payload) public view returns(address)
    {
        require(m_custodianCount == 1, 108);
        require(msg.pubkey() == m_ownerKey, 100);
        tvm.accept();
        dest.transfer(varuint16(value), bounce, flags, payload, cc);
        return dest;
    }

    /// @dev Allows custodian to submit and confirm new transaction.
    /// @param dest Transfer target address.
    /// @param value Nanograms value to transfer.
    /// @param bounce Bounce flag. Set true if need to transfer grams to existing account; set false to create new account.
    /// @param allBalance Set true if need to transfer all remaining balance.
    /// @param payload Tree of cells used as body of outbound internal message.
    /// @return transId Transaction ID.
    function submitTransaction(
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        bool allBalance,
        TvmCell payload)
    public returns (uint64 transId)
    {
        uint256 senderKey = msg.pubkey();
        uint8 index = _findCustodian(senderKey);
        require(value >= MIN_VALUE, 107);
        (uint bits, uint cells) = tvm_tree_cell_size(tvm_ctos(payload));
        require(bits < 8192 && cells < 8, 121);
        _removeExpiredTransactions();
        require(_getMaskValue(m_requestsMask, index) < MAX_QUEUED_REQUESTS, 113);
        tvm.accept();

        uint8 flags = _getSendFlags(allBalance);        
        uint8 requiredSigns = m_defaultRequiredConfirmations;

        if (requiredSigns == 1) {
            dest.transfer(varuint16(value), bounce, flags, payload, cc);
            return 0;
        } else {
            m_requestsMask = _incMaskValue(m_requestsMask, index);
            uint64 trId = _generateId();
            Transaction txn = Transaction(trId, 0/*mask*/, requiredSigns, 0/*signsReceived*/,
                senderKey, index, dest, value, cc, flags, payload, bounce);

            _confirmTransaction(trId, txn, index);
            return trId;
        }
    }

    /// @dev Allows custodian to confirm a transaction.
    /// @param transactionId Transaction ID.
    function confirmTransaction(uint64 transactionId) public {
        uint8 index = _findCustodian(msg.pubkey());
        _removeExpiredTransactions();
        optional(Transaction) otxn = m_transactions.fetch(transactionId);        
        require(otxn.hasValue(), 102);
        Transaction txn = otxn.get();
        require(!_isConfirmed(txn.confirmationsMask, index), 103);
        tvm.accept();
        uint64 marker = _getExpirationBound();
        bool needCleanup = transactionId <= marker;
        if (needCleanup) {
            delete m_transactions[transactionId];   
        } else {
            _confirmTransaction(transactionId, txn, index);
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
            txn.dest.transfer(varuint16(txn.value), txn.bounce, txn.sendFlags, txn.payload, txn.cc);
            m_requestsMask = _decMaskValue(m_requestsMask, txn.index);
            delete m_transactions[transactionId];
        } else {
            txn.confirmationsMask = _setConfirmed(txn.confirmationsMask, custodianIndex);
            txn.signsReceived++;
            m_transactions[transactionId] = txn;
        }
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
        while (needCleanup && i < MAX_CLEANUP_TXNS) {
            // transaction is expired, remove it
            i++;
            m_requestsMask = _decMaskValue(m_requestsMask, txn.index);
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

    /*
     * Get methods
     */
    
    /// @dev Helper get-method for checking if custodian confirmation bit is set.
    /// @return confirmed True if confirmation bit is set.
    function isConfirmed(uint32 mask, uint8 index) public pure returns (bool confirmed) {
        confirmed = _isConfirmed(mask, index);
    }

    /// @dev Get-method that returns wallet configuration parameters.
    /// @return maxQueuedTransactions The maximum number of unconfirmed transactions that a custodian can submit.
    /// @return maxCustodianCount The maximum allowed number of wallet custodians.
    /// @return expirationTime Transaction lifetime in seconds.
    /// @return minValue The minimum value allowed to transfer in one transaction.
    /// @return requiredTxnConfirms The minimum number of confirmations required to execute transaction.
    /// @return requiredUpdConfirms The minimum number of confirmations required to update wallet code.
    function getParameters() public view
        returns (uint8 maxQueuedTransactions,
                uint8 maxCustodianCount,
                uint64 expirationTime,
                uint128 minValue,
                uint8 requiredTxnConfirms,
                uint8 requiredUpdConfirms) {

        maxQueuedTransactions = MAX_QUEUED_REQUESTS;
        maxCustodianCount = MAX_CUSTODIAN_COUNT;
        expirationTime = EXPIRATION_TIME;
        minValue = MIN_VALUE;
        requiredTxnConfirms = m_defaultRequiredConfirmations;
        requiredUpdConfirms = m_requiredVotes;
    }

    /// @dev Get-method that returns transaction info by id.
    /// @return trans Transaction structure.
    /// Throws exception if transaction does not exist.
    function getTransaction(uint64 transactionId) public view
        returns (Transaction trans) {
        optional(Transaction) txn = m_transactions.fetch(transactionId);
        require(txn.hasValue(), 102);
        trans = txn.get();
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

    /// @dev Get-method that returns submitted transaction ids.
    /// @return ids Array of transaction ids.
    function getTransactionIds() public view returns (uint64[] ids) {
        uint64 trId = 0;
        optional(uint64, Transaction) otxn = m_transactions.min();
        while (otxn.hasValue()) {
            (trId, ) = otxn.get();
            ids.push(trId);
            otxn = m_transactions.next(trId);
        }
    }

    /// @dev Helper structure to return information about custodian.
    /// Used in getCustodians().
    struct CustodianInfo {
        uint8 index;
        uint256 pubkey;
    }

    /// @dev Get-method that returns info about wallet custodians.
    /// @return custodians Array of custodians.
    function getCustodians() public view returns (CustodianInfo[] custodians) {
        optional(uint256, uint8) oind = m_custodians.min();
        while (oind.hasValue()) {
            (uint256 key, uint8 index) = oind.get();
            custodians.push(CustodianInfo(index, key));
            oind = m_custodians.next(key);
        }
    }    

    /*
        SETCODE public functions
     */
    
    /// @dev Allows to submit update request. New custodians can be supplied.
    /// @param codeHash Representation hash of code's tree of cells.
    /// @param owners Array with new custodians.
    /// @param reqConfirms Default number of confirmations required for executing transaction.
    /// @return updateId Id of submitted update request.
    function submitUpdate(uint256 codeHash, uint256[] owners, uint8 reqConfirms) public 
        returns (uint64 updateId) 
    {
        uint256 sender = msg.pubkey();
        uint8 index = _findCustodian(sender);
        require(owners.length > 0 && owners.length <= MAX_CUSTODIAN_COUNT, 117);
        _removeExpiredUpdateRequests();
        require(!_isSubmitted(m_updateRequestsMask, index), 113);
        require(reqConfirms > 0, 123);
        tvm.accept();

        m_updateRequestsMask = _setSubmitted(m_updateRequestsMask, index);
        updateId = _generateId();
        m_updateRequests[updateId] = UpdateRequest(updateId, index, 0/*signs*/, 0/*mask*/, 
            sender, codeHash, owners, reqConfirms);
        _confirmUpdate(updateId, index);
    }

    /// @dev Allow to confirm submitted update request. Call executeUpdate to do `setcode`
    /// after necessary confirmation count.
    /// @param updateId Id of submitted update request.
    function confirmUpdate(uint64 updateId) public {
        uint8 index = _findCustodian(msg.pubkey());
        _removeExpiredUpdateRequests();
        optional(UpdateRequest) oreq = m_updateRequests.fetch(updateId);
        require(oreq.hasValue(), 115);
        UpdateRequest request = oreq.get();
        require(!_isConfirmed(request.confirmationsMask, index), 116);
        tvm.accept();

        _confirmUpdate(updateId, index);
    }

    /// @dev Allows to execute confirmed update request.
    /// @param updateId Id of update request.
    /// @param code Root cell of tree of cells with contract code.
    function executeUpdate(uint64 updateId, TvmCell code) public {
        require(m_custodians.exists(msg.pubkey()), 100);
        _removeExpiredUpdateRequests();
        optional(UpdateRequest) oreq = m_updateRequests.fetch(updateId);
        require(oreq.hasValue(), 115);
        UpdateRequest request = oreq.get();
        require(tvm.hash(code) == request.codeHash, 119);
        require(request.signs >= m_requiredVotes, 120);
        tvm.accept();

        _deleteUpdateRequest(updateId, request.index);

        tvm.setcode(code);
        tvm.setCurrentCode(code);
        onCodeUpgrade(request.custodians, request.reqConfirms);
    }

    /// @dev Get-method to query all pending update requests.
    function getUpdateRequests() public view returns (UpdateRequest[] updates) {
        uint64 bound = _getExpirationBound();
        optional(uint64, UpdateRequest) oreq = m_updateRequests.min();
        while (oreq.hasValue()) {
            (uint64 updateId, UpdateRequest req) = oreq.get();
            if (updateId > bound) {
                updates.push(req);
            }
            oreq = m_updateRequests.next(updateId);
        }
    }

    /// @dev Worker function after code update.
    function onCodeUpgrade(uint256[] newOwners, uint8 reqConfirms) private {
        tvm.resetStorage();
        _initialize(newOwners, reqConfirms);
    }
    
    /*
     * Internal functions
     */

    /// @dev Internal function for update confirmation.
    function _confirmUpdate(uint64 updateId, uint8 custodianIndex) inline private {
        UpdateRequest request = m_updateRequests[updateId];
        request.signs++;
        request.confirmationsMask = _setConfirmed(request.confirmationsMask, custodianIndex);
        m_updateRequests[updateId] = request;
    }

    /// @dev Removes expired update requests.
    function _removeExpiredUpdateRequests() inline private {
        uint64 marker = _getExpirationBound();
        optional(uint64, UpdateRequest) oreq = m_updateRequests.min();
        if (!oreq.hasValue()) { return; }
        (uint64 updateId, UpdateRequest req) = oreq.get();
        bool needCleanup = updateId <= marker;
        if (!needCleanup) { return; }

        tvm.accept();
        while (needCleanup) {
            // transaction is expired, remove it
            _deleteUpdateRequest(updateId, req.index);
            oreq = m_updateRequests.next(updateId);
            needCleanup = oreq.hasValue();
            if (needCleanup) {
                (updateId, req) = oreq.get();
                needCleanup = updateId <= marker;
            }
        }
        tvm.commit();
    }

    /// @dev Helper function to correctly delete request.
    function _deleteUpdateRequest(uint64 updateId, uint8 index) inline private {
        m_updateRequestsMask &= ~(uint32(1) << index);
        delete m_updateRequests[updateId];
    }

    /*
     * Fallback function to receive simple transfers
     */
    
    fallback () external {}

    receive () external {}

    function getVersion() external pure returns(string, string) {
        return ("1.0.0", "Multisig");
    }
}