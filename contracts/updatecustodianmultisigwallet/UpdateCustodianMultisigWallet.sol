pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

/// @title Multisignature wallet with setcode and exchange ecc.
contract UpdateCustodianMultisigWallet {

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

    /*
     *  Constants
     */
    uint8   constant MAX_QUEUED_REQUESTS = 5;
    uint64  constant EXPIRATION_TIME = 3601; // lifetime is 1 hour
    uint8   constant MAX_CUSTODIAN_COUNT = 32;
    uint32 constant ZERO_TIME = 1000000000;
    uint16 constant SENDMSG_ALL_BALANCE = 128;

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
    // Dictionary of queued transactions waiting confirmations.
    mapping(uint64 => Transaction) m_transactions;
    // Dictionary of queued update code waiting confirmations.
    mapping(uint64 => UpdateData) m_data;
    // Set of custodians, initiated in constructor, but values can be changed later in code.
    mapping(uint256 => Custodian) m_custodians; // pub_key -> custodian_index
    // Read-only custodian count, initiated in constructor.
    uint8 m_custodianCount;
    // Default number of confirmations needed to execute transaction.
    uint8 m_defaultRequiredConfirmations;
    // Default number of confirmations needed to update data.
    uint8 m_defaultRequiredConfirmationsData;

    uint _max_cleanup_operations = 40;

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
     * Constructor
     */

    /// @dev Internal function called from constructor to initialize custodians.
    function _initialize(uint256[] owners_pubkey, address[] owners_address, uint8 reqConfirms, uint8 reqConfirmsData) inline private {
        delete m_requestsMask;
        delete m_requestsMaskData;
        delete m_transactions;
        delete m_data;
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
        _max_cleanup_operations = 40;

        uint8 ownerCount = 0;
        uint256 keysCount = owners_pubkey.length + owners_address.length;
        require(keysCount > 0 && keysCount <= MAX_CUSTODIAN_COUNT, 117);

        uint256 len = owners_pubkey.length;
        for (uint256 i = 0; i < len; i++) {
            uint256 key = owners_pubkey[i];
            require(key != 0, 101);
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
            require(key_address != empty_address, 101);
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

    /// @dev Contract constructor.
    /// @param owners_pubkey Array of custodian keys.
    /// @param owners_address Array of custodian addresses.
    /// @param reqConfirms Default number of confirmations required for executing transaction.
    constructor(uint256[] owners_pubkey, address[] owners_address, uint8 reqConfirms, uint8 reqConfirmsData, uint64 value) {
        gosh.cnvrtshellq(value);
        require(msg.pubkey() == tvm.pubkey(), 100);
        require(reqConfirms > 0, 123);
        require(reqConfirmsData > 0, 123);
        tvm.accept();
        _initialize(owners_pubkey, owners_address, reqConfirms, reqConfirmsData);
    }

    function setMaxCleanupOperations(uint value) public {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        cstd;
        tvm.accept();
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
    function sendTransaction(
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        uint8 flags,
        TvmCell payload) public view returns(address)
    {
        require(m_custodianCount == 1, 108);
        if (m_ownerAddress.hasValue()) {
            require(msg.sender == m_ownerAddress.get(), 100);    
        }
        if (m_ownerKey.hasValue()) {
            require(msg.pubkey() == m_ownerKey.get(), 100);    
        }
        tvm.accept();
        dest.transfer(varuint16(value), bounce, flags, payload, cc);
        return dest;
    }

    /// @dev Allows custodian to submit and confirm new transaction.
    /// @param dest Transfer target address.
    /// @param value Nanograms value to transfer.
    /// @param bounce Bounce flag. Set true if need to transfer grams to existing account; set false to create new account.
    /// @param flag Set flag.
    /// @param payload Tree of cells used as body of outbound internal message.
    /// @return transId Transaction ID.
    function submitTransaction(
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        uint8 flag,
        TvmCell payload)
    public returns (uint64 transId)
    {
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredTransactions();
        require(_getMaskValue(m_requestsMask, cstd.index) < MAX_QUEUED_REQUESTS, 113);
        tvm.accept();
     
        uint8 requiredSigns = m_defaultRequiredConfirmations;
        if (flag & SENDMSG_ALL_BALANCE != 0) {
            value = 0;
        }
        if (requiredSigns == 1) {
            dest.transfer(varuint16(value), bounce, flag, payload, cc);
            return 0;
        } else {
            m_requestsMask = _incMaskValue(m_requestsMask, cstd.index);
            uint64 trId = _generateId();
            Transaction txn = Transaction(trId, 0/*mask*/, requiredSigns, 0/*signsReceived*/,
                cstd, dest, value, cc, flag, payload, bounce);

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
        uint64 marker = _getExpirationBound();
        bool needCleanup = transactionId <= marker;
        if (needCleanup) {
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
        Custodian cstd = _findCustodian(msg.pubkey(), msg.sender);
        _removeExpiredDataUpdate();
        require(_getMaskValue(m_requestsMaskData, cstd.index) < MAX_QUEUED_REQUESTS, 113);
        tvm.accept();     
        uint8 requiredSigns = m_defaultRequiredConfirmationsData;

        if (requiredSigns == 1) {
            _initialize(owners_pubkey, owners_address, reqConfirms, reqConfirmsData);
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
        uint64 marker = _getExpirationBound();
        bool needCleanup = dataUpdateId <= marker;
        if (needCleanup) {
            delete m_data[dataUpdateId];   
        } else {
            _confirmDataUpdate(dataUpdateId, du, cstd.index);
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
            m_requestsMask = _decMaskValue(m_requestsMask, txn.creator.index);
            delete m_transactions[transactionId];
        } else {
            txn.confirmationsMask = _setConfirmed(txn.confirmationsMask, custodianIndex);
            txn.signsReceived++;
            m_transactions[transactionId] = txn;
        }
    }

    /// @dev Confirms transaction by custodian with defined index.
    /// @param dataUpdateId Data update id to confirm.
    /// @param du Data update object to confirm.
    /// @param custodianIndex Index of custodian.
    function _confirmDataUpdate(uint64 dataUpdateId, UpdateData du, uint8 custodianIndex) inline private {
        if ((du.signsReceived + 1) >= du.signsRequired) {
            _initialize(du.owners_pubkey, du.owners_address, du.reqConfirms, du.reqConfirmsData); 
        } else {
            du.confirmationsMask = _setConfirmed(du.confirmationsMask, custodianIndex);
            du.signsReceived++;
            m_data[dataUpdateId] = du;
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

    /// @dev Get-method that returns transaction info by id.
    /// @return trans Transaction structure.
    /// Throws exception if transaction does not exist.
    function getTransaction(uint64 transactionId) public view
        returns (Transaction trans) {
        optional(Transaction) txn = m_transactions.fetch(transactionId);
        require(txn.hasValue(), 102);
        trans = txn.get();
    }

    /// @dev Get-method that returns transaction info by id.
    /// @return data UpdateData structure.
    /// Throws exception if transaction does not exist.
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

    /// @dev Get-method that returns submitted transaction ids.
    /// @return ids Array of transaction ids.
    function getUpdateCodeIds() public view returns (uint64[] ids) {
        uint64 duId = 0;
        optional(uint64, UpdateData) odu = m_data.min();
        while (odu.hasValue()) {
            (duId, ) = odu.get();
            ids.push(duId);
            odu = m_data.next(duId);
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

	// This function will never be called. But it must be defined.
	function onCodeUpgrade(TvmCell stateVars) private pure {
	}

    /*
     * Fallback function to receive simple transfers
     */
    
    fallback () external {}

    receive () external {}

    function getVersion() external pure returns(string, string) {
        return ("1.0.0", "UpdateCustodianMultisigWallet");
    }
}