pragma gosh-solidity >= 0.76.1;
pragma AbiHeader expire;

abstract contract Upgradable {
    /*
     * Set code
     */

    function upgrade(TvmCell newcode) public virtual {
        require(msg.pubkey() == tvm.pubkey(), 101);
        tvm.accept();
        tvm.commit();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade();
    }

    function onCodeUpgrade() internal virtual;
}

contract GiverV3 is Upgradable {

    uint8 constant MAX_CLEANUP_MSGS = 30;

    // Per-call upper bounds on faucet payouts. A send that requests more than
    // these caps is clamped down to them rather than rejected.
    varuint16 constant MAX_SEND_VALUE = 10000000 vmshell;
    varuint32 constant MAX_SEND_ECC = 10000000000000000;
    // Replay-protection storage. Outer key = expireAt so iteration is sorted by
    // expiry — gc() can stop at the first non-expired bucket.
    mapping(uint32 => mapping(uint256 => bool)) m_messages;

    struct MessageInfo {
        uint256 messageHash;
        uint32 expireAt;
    }
    MessageInfo m_lastMsg;

    modifier acceptOnlyOwner {
        require(msg.pubkey() == tvm.pubkey(), 101);
        tvm.accept();
        _;
    }

    modifier accept() {
        tvm.accept();
        _;
    }

    modifier saveMsg() {
        _saveMsg();
        tvm.commit();
        _;
    }

    function _saveMsg() inline internal {
        gc();
        m_messages[m_lastMsg.expireAt][m_lastMsg.messageHash] = true;
    }

    // Replaces default replay protection. Called by the framework after the
    // signature is verified, before the message body is dispatched.
    function afterSignatureCheck(TvmSlice body, TvmCell message) private inline returns (TvmSlice) {
        body.load(uint64); // 64-bit timestamp header (unused for dedup)
        uint32 expireAt = body.load(uint32);
        require(expireAt > block.timestamp, 102);              // expired
        require(expireAt < block.timestamp + 5 minutes, 103);  // expireAt too far
        uint256 messageHash = tvm.hash(message);
        optional(mapping(uint256 => bool)) m = m_messages.fetch(expireAt);
        require(!m.hasValue() || !m.get()[messageHash], 104);  // duplicate
        m_lastMsg = MessageInfo({messageHash: messageHash, expireAt: expireAt});
        return body;
    }

    function gc() private {
        uint counter = 0;
        for ((uint32 expireAt, mapping(uint256 => bool) m) : m_messages) {
            m;
            if (counter >= MAX_CLEANUP_MSGS) { break; }
            counter++;
            if (expireAt <= block.timestamp) {
                delete m_messages[expireAt];
            } else {
                break;
            }
        }
    }

    event SentCurrency(address dst, varuint16 value, mapping(uint32 => varuint32));
    event SentCurrencyWithFlag(address dst, varuint16 value, mapping(uint32 => varuint32), uint8 flag);

    constructor () {}

    /*
     * Publics
     */

    /// @notice Allows to accept simple transfers.
    receive() external {}

    /// @notice Transfers grams to other contracts.
    function sendTransaction(address dest, varuint16 value, bool bounce) public accept saveMsg {
        dest.transfer(value, bounce, 3);
    }

    function sendCurrency(address dest, varuint16 value, mapping(uint32 => varuint32) ecc) public accept saveMsg {
        if (value > MAX_SEND_VALUE) {
            value = MAX_SEND_VALUE;
        }
        for (uint32 id = 1; id <= 3; id++) {
            if (ecc.exists(id) && ecc[id] > MAX_SEND_ECC) {
                ecc[id] = MAX_SEND_ECC;
            }
        }
        _mintEccIfNeeded(ecc);
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: 1, currencies: ecc});
        emit SentCurrency(dest, value, ecc);
    }

    function sendWithBody(address dest, varuint16 value, bool bounce, uint8 flag, TvmCell body) public acceptOnlyOwner saveMsg {
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: bounce, flag: flag, body: body});
    }

    function _mintEccIfNeeded(mapping(uint32 => varuint32) ecc) private pure {
        for (uint32 id = 1; id <= 3; id++) {
            if (ecc.exists(id)) {
                if (address(this).currencies[id] < ecc[id]) {
                    gosh.mintecc(uint64(ecc[id]) - uint64(address(this).currencies[id]), id);
                }
            }
        }
    }

    function sendCurrencyWithFlag(address dest, varuint16 value, mapping(uint32 => varuint32) ecc, uint8 flag) public accept saveMsg {
        if (value > MAX_SEND_VALUE) {
            value = MAX_SEND_VALUE;
        }
        for (uint32 id = 1; id <= 3; id++) {
            if (ecc.exists(id) && ecc[id] > MAX_SEND_ECC) {
                ecc[id] = MAX_SEND_ECC;
            }
        }
        _mintEccIfNeeded(ecc);
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: flag, currencies: ecc});
        emit SentCurrencyWithFlag(dest, value, ecc, flag);
    }

    function sendCurrencyWithBody(address dest, varuint16 value, mapping(uint32 => varuint32) ecc, uint8 flag, TvmCell body) public accept saveMsg {
        if (value > MAX_SEND_VALUE) {
            value = MAX_SEND_VALUE;
        }
        for (uint32 id = 1; id <= 3; id++) {
            if (ecc.exists(id) && ecc[id] > MAX_SEND_ECC) {
                ecc[id] = MAX_SEND_ECC;
            }
        }
        _mintEccIfNeeded(ecc);
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: flag, currencies: ecc, body: body});
    }

    function sendFreeToken(address dest) public accept saveMsg {
        varuint32 drop = 50 vmshell;
        uint64 value = 10 vmshell;
        mapping(uint32 => varuint32) data_cur;
        data_cur[2] = drop;
        gosh.mintecc(uint64(drop), 2);
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: 1, currencies: data_cur});
    }

    /*
     * Get methods
     */
    struct Message {
        uint256 hash;
        uint32 expireAt;
    }
    function getMessages() public view returns (Message[] messages) {
        for ((uint32 expireAt, mapping(uint256 => bool) bucket) : m_messages) {
            for ((uint256 msgHash, bool _exists) : bucket) {
                _exists;
                messages.push(Message(msgHash, expireAt));
            }
        }
    }

    function getData(string name, uint128 decimals, TvmCell walletCode, TvmCell transactionCode, uint256 pubkey, bool mintDisabled, address initialSupplyToOwner, uint128 initialSupply) public view returns (TvmCell) {
        return abi.encode(name, decimals, walletCode, transactionCode, pubkey, mintDisabled, initialSupplyToOwner, initialSupply);
    }

    function getAccumulatorData(TvmCell sellOrderCode, uint256 pubkey) public view returns (TvmCell) {
        return abi.encode(sellOrderCode, pubkey);
    }

    function getExchangeData(uint256 pubkey, address usdcWallet, uint128 totalMinted, uint64 mintNonce, uint64 mintAccumulatorNonce) public view returns (TvmCell) {
        return abi.encode(pubkey, usdcWallet, totalMinted, mintNonce, mintAccumulatorNonce);
    }

    function getUSDCBridgeData(
        uint256 pubkey, address usdcWallet,
        uint128 totalMinted, uint64 mintNonce, uint64 mintAccumulatorNonce,
        uint32[] mintedKeys, uint128[] mintedValues,
        uint32[] burnedKeys, uint128[] burnedValues,
        TvmCell depositVoucherCode
    ) public pure returns (TvmCell) {
        mapping(uint32 => uint128) totalMintedBridgeByToken;
        mapping(uint32 => uint128) totalBurnedBridgeByToken;
        for (uint i = 0; i < mintedKeys.length; i++) {
            totalMintedBridgeByToken[mintedKeys[i]] = mintedValues[i];
        }
        for (uint i = 0; i < burnedKeys.length; i++) {
            totalBurnedBridgeByToken[burnedKeys[i]] = burnedValues[i];
        }
        // 9th field mirrors USDCBridge.updateCode's `userCell` passthrough so
        // onCodeUpgrade decodes one shape on both paths. Empty here: the
        // zerostate carries the voucher code in `depositVoucherCode`.
        TvmCell userCell;
        return abi.encode(pubkey, usdcWallet, totalMinted, mintNonce, mintAccumulatorNonce,
                          totalMintedBridgeByToken, totalBurnedBridgeByToken, depositVoucherCode, userCell);
    }

    function getDataForAuthService(TvmCell profileCode, uint256 pubkey) public view returns (TvmCell) {
        return abi.encode(profileCode, pubkey);
    }

    function getDataForPMP(TvmCell PMPCode, TvmCell PMPWalletCode, TvmCell NullifierCode, TvmCell OracleCode, TvmCell OracleEventListCode, TvmCell OrderBookCode, uint256 pubkey) public view returns (TvmCell) {
        return abi.encode(PMPCode, PMPWalletCode, NullifierCode, OracleCode, OracleEventListCode, OrderBookCode, pubkey);
    }

    function getDataForOracle(TvmCell PMPCode, TvmCell PMPWalletCode, TvmCell OracleCode, TvmCell OracleEventListCode, uint256 pubkey) public view returns (TvmCell) {
        return abi.encode(PMPCode, PMPWalletCode, OracleCode, OracleEventListCode, pubkey);
    }

    function getDataForVault(TvmCell PMPWalletCode, uint256 pubkey, address root) public view returns (TvmCell) {
        return abi.encode(PMPWalletCode, pubkey, root);
    }

    function getDataForBoost(address wallet, address popitGame, address root, uint64 mbiCur, uint256 rootPubkey, address miner) public view returns (TvmCell) {
        return abi.encode(wallet, popitGame, root, mbiCur, rootPubkey, miner);
    }

    function onCodeUpgrade() internal override {}
}
