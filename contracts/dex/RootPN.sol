pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./PrivateNote.sol";
import "./Nullifier.sol";
import "./Oracle.sol";
import "./libraries/DexLib.sol";
import "../airegistry/TokenContract.sol";

/// @notice Root contract responsible for deploying PrivateNote contracts
contract RootPN is Modifiers {

    /// @notice Contract semantic version.
    string constant version = "4.0.27";

    // Canonical SuperRoot account id + RootModel/TokenContract code hashes. Baked
    // into every PrivateNote at deploy (`deployPrivateNote`) so the note derives the
    // canonical RootModel / deal TC locally and posts its offer in a single call.
    // RootPN is not pinned by anyone, so pinning these here is cycle-free
    // (cascade-updated together with the note's baked copies).
    uint256 constant SUPER_ROOT_ADDR           = 0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c;
    uint256 constant TOKEN_CONTRACT_CODE_HASH  = 0xa2c32147ed9bedec588e81ad2f55300e0640635428254b710964f38331c84f45;
    uint16  constant TOKEN_CONTRACT_CODE_DEPTH = 10;
    uint256 constant ROOT_MODEL_CODE_HASH      = 0x0a6fe90e89faa99bdd4286965ec75e5085d7c0f365b8c2e3e1467cf584d359bc;
    uint16  constant ROOT_MODEL_CODE_DEPTH     = 8;

    /// @notice Stored code of PrivateNote contract
    TvmCell _privateNoteCode;

    /// @notice Stored code of PMP contract
    TvmCell _pmpCode;

    /// @notice Stored code of Nullifier contract
    TvmCell _nullifierCode;

    /// @notice Stored code of Oracle contract
    TvmCell _oracleCode;

    /// @notice Stored code of OracleEventList contract
    TvmCell _oracleEventListCode;

    /// @notice Stored code of OrderBook contract
    TvmCell _orderBookCode;

    /// @notice Stored code of InferenceOrderBook contract (§8 inference market).
    ///         Baked into every PrivateNote at deploy so the note derives the
    ///         canonical book address itself instead of trusting a caller-
    ///         supplied OB code / raw address.
    TvmCell _inferenceOrderBookCode;

    /// @notice Root owner public key
    uint256 _ownerPubkey;

    /// @notice Mapping of deployed PrivateNote values
    mapping(uint32 => uint128) _deployedValues;

    /// @notice Accumulated OrderBook protocol fees per token type. Reported by
    ///         each OrderBook at shutdown (`collectProtocolFee`) and withdrawable
    ///         only by the root owner (`withdrawProtocolFees`). The backing real
    ///         ECC already sits in this contract's reserves / `_deployedValues`
    ///         (it was the taker-fee share never credited to any note).
    mapping(uint32 => uint128) _protocolFees;

    /// @notice Encode a uint64 into the bn254 Fr representation that halo2
    ///         emits for `voucherNominal` (32 LE bytes, padded with zeros,
    ///         then read as a big-endian uint256).
    function _u64ToFr(uint64 v) private pure returns (uint256) {
        uint64 swapped =
            ((v >> 56) & 0xff)
          | (((v >> 48) & 0xff) << 8)
          | (((v >> 40) & 0xff) << 16)
          | (((v >> 32) & 0xff) << 24)
          | (((v >> 24) & 0xff) << 32)
          | (((v >> 16) & 0xff) << 40)
          | (((v >>  8) & 0xff) << 48)
          | (( v        & 0xff) << 56);
        return uint256(swapped) << 192;
    }

    /// @notice Encode a uint32 into the bn254 Fr representation halo2 emits
    ///         for `tokenType`.
    function _u32ToFr(uint32 v) private pure returns (uint256) {
        uint32 swapped =
            ((v >> 24) & 0xff)
          | (((v >> 16) & 0xff) << 8)
          | (((v >>  8) & 0xff) << 16)
          | (( v        & 0xff) << 24);
        return uint256(swapped) << 224;
    }

    /// @notice BN254 Fr modulus (a.k.a. group order of the bn254 G1 group's
    ///         scalar field). All halo2 instances are Fr elements; raw
    ///         uint256 inputs that exceed this must be reduced.
    uint256 constant FR_MODULUS = 0x30644e72e131a029b85045b68181585d2833e84879b9709143e1f593f0000001;

    /// @notice Encode an arbitrary uint256 (e.g. ephemeralPubkey) into the
    ///         bn254 Fr representation halo2 emits — that is,
    ///         `(v mod p).to_bytes_le()` packed into a uint256 whose
    ///         big-endian 32-byte view IS the LE-bytes (so when later
    ///         shipped to gosh.zkhalo2verify via `bytes(bytes32(...))` the
    ///         on-wire bytes are exactly the LE Fr canonical form).
    ///
    ///         Without the mod-reduce step, a pubkey whose top 2 bits are
    ///         set (above Fr) gets reduced inside the prover but not
    ///         on-chain → instance mismatch and verification fails.
    function _u256ToFr(uint256 v) private pure returns (uint256) {
        uint256 reduced = v % FR_MODULUS;
        uint256 r = 0;
        // 32-byte byte-swap (BE → LE)
        for (uint8 i = 0; i < 32; i++) {
            r = (r << 8) | (reduced & 0xff);
            reduced >>= 8;
        }
        return r;
    }
    
    // Events

    /// @notice Emitted when a new voucher is generated.
    /// @param skUCommit Commitment of the secret key
    /// @param voucherNominal Nominal value of the voucher
    /// @param tokenType Type of token for the voucher
	event VoucherGenerated(uint256 skUCommit, uint voucherNominal, uint32 tokenType);

    /// @notice Emitted when a PrivateNote contract is successfully deployed and registered.
    /// @param depositIdentifierHash Deposit identifier hash
    /// @param noteAddress — Deployed PrivateNote address
    /// @param initialBalance — Initial token balance
    event PrivateNoteDeployed(uint256 depositIdentifierHash, address noteAddress, uint128 initialBalance);

    /// @notice Emitted when a Nullifier contract is deployed.
    /// @param nullifierAddress — Address associated with the deployment
    /// @param value — Value linked to the nullifier
    event NullifierDeployed(address nullifierAddress, uint64 value);

    /// @notice Emitted when tokens are withdrawn from a PrivateNote to a wallet.
    /// @param amounts — Per-token-type amounts withdrawn
    /// @param noteAddress — PrivateNote the tokens were withdrawn from
    /// @param to — Destination wallet address
    /// @param dapp_id — DApp id passed through from the withdraw call
    event TokensWithdrawn(mapping(uint32 => uint128) amounts, address noteAddress, address to, uint256 dapp_id);

    /// @notice Emitted when an OrderBook reports its protocol fees at shutdown.
    /// @param tokenType — Token type of the collected fee
    /// @param amount — Amount collected
    event ProtocolFeeCollected(uint32 tokenType, uint128 amount);

    /// @notice Emitted when the owner withdraws accumulated protocol fees.
    /// @param to — Destination address
    /// @param dapp_id — Destination dapp id
    /// @param tokenType — Token type withdrawn
    /// @param amount — Amount withdrawn
    event ProtocolFeeWithdrawn(address to, uint256 dapp_id, uint32 tokenType, uint128 amount);

    /// @notice Root constructor
    constructor() {
        tvm.accept();
    }

    /// @notice Ensures minimal native balance for root operations
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) return;
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Verifies a zero-knowledge proof together with a node-side
    ///         historical-hash check, then deploys a Nullifier contract
    ///         linked to a deterministic PrivateNote address.
    ///
    /// @dev Public inputs for zk verification are 4 × 32 bytes, in order:
    ///      0. `depositIdentifierHash` — Poseidon commitment.
    ///      1. `finalLayerHistoricalHashRoot` — dense chain root that
    ///         the node verifies against `GlobalHistoricalData[layerNumber]`.
    ///      2. `voucherNominalFr` — voucher nominal as Fr.
    ///      3. `tokenTypeFr` — token type as Fr.
    ///
    ///      The flow is:
    ///      1. `gosh.check_layer_hash` confirms the node still has this
    ///         historical root in the requested layer's window. If the
    ///         hash has aged out of the historical window, the caller
    ///         must rebuild the proof against an older layer (L+1) and
    ///         retry — this is the L1 → L2 fallback used by the live
    ///         test (generate_vouchers_with_live_event_proving.py).
    ///      2. `gosh.zkhalo2verify` validates the zk proof against the
    ///         4-field public-input vector built above.
    ///      3. On success, deploys the Nullifier and emits
    ///         `NullifierDeployed`.
    ///
    /// @param proof Zero-knowledge proof of ownership of the source SHELL_FEE
    ///        voucher (whose dih is `nullifierHash`).
    /// @param nullifierHash Source SHELL_FEE voucher's deposit identifier hash
    ///        (Poseidon commitment), used as the proof's instance 0 and as the
    ///        Nullifier contract's varInit. Replays of the same source voucher
    ///        produce the same Nullifier address, so the second `new` is a
    ///        no-op — natural one-shot semantics, no state tracking needed.
    /// @param depositIdentifierHash Destination PrivateNote's dih — derives
    ///        the recipient PN address. Independent of the source voucher.
    /// @param finalLayerHistoricalHashRoot Dense chain root (instance 1).
    /// @param voucherNominalFr Source voucher nominal as Fr (instance 2);
    ///        must encode the user-supplied `value`.
    /// @param tokenTypeFr Source voucher token type as Fr (instance 3); must
    ///        encode CURRENCIES_ID_SHELL_FEE (300) — only fee vouchers may
    ///        top up a PN's fee balance via this path.
    /// @param value Amount of SHELL ECC forwarded to the Nullifier and on to
    ///        the PrivateNote. Must equal the source voucher's nominal.
    /// @param layerNumber Historical layer being proved against (L1, L2, …).
    function sendEccShellToPrivateNote(
        bytes proof,
        uint256 nullifierHash,
        uint256 depositIdentifierHash,
        uint256 finalLayerHistoricalHashRoot,
        uint256 voucherNominalFr,
        uint256 tokenTypeFr,
        uint64 value,
        uint8 layerNumber,
        uint256 recipientEphemeralPubkey
    ) public view {
        tvm.accept();
        ensureBalance();
        // `depositIdentifierHash` (recipient) is NOT in the zk proof's public inputs,
        // so the proof alone doesn't bind the destination. The caller must sign the
        // ext message with the destination PN's ephemeral key, which binds the proof
        // to a recipient the signer controls. (This restricts third-party SHELL
        // top-ups; use case is typically self-funding, and the owner holds this key.)
        require(msg.pubkey() == recipientEphemeralPubkey, ERR_INVALID_SENDER);
        require(recipientEphemeralPubkey != 0, ERR_INVALID_PARAMS);

        require(
            gosh.check_layer_hash(finalLayerHistoricalHashRoot, layerNumber),
            ERR_INVALID_HISTORY_PROOF
        );

        // Bind `value` to the proof's nominal, so the minted balance always equals
        // the voucher's committed nominal.
        require(_u64ToFr(value) == voucherNominalFr, ERR_INVALID_ZKPROOF);
        // Only SHELL_FEE vouchers may be spent here, so this path spends exactly the
        // SHELL_FEE token type and not a regular (type 2) SHELL voucher.
        require(_u32ToFr(CURRENCIES_ID_SHELL_FEE) == tokenTypeFr, ERR_INVALID_ZKPROOF);

        // Instance 0 = nullifierHash (the source voucher's dih). The proof
        // attests ownership of the SHELL_FEE voucher whose dih is
        // nullifierHash. `depositIdentifierHash` (recipient) is NOT in
        // the proof; the recipient is bound two independent ways:
        //   a) instance 4 = recipientEphemeralPubkey: the voucher was
        //      generated with a commit to this exact pubkey (see
        //      generateVoucher), so changing recipientEphemeralPubkey
        //      breaks zk verification.
        //   b) msg.pubkey() == recipientEphemeralPubkey gate above:
        //      belt-and-suspenders so a mismatched key also fails at the
        //      signature check.
        bytes pubInputs;
        pubInputs.append(bytes(bytes32(nullifierHash)));
        pubInputs.append(bytes(bytes32(finalLayerHistoricalHashRoot)));
        pubInputs.append(bytes(bytes32(voucherNominalFr)));
        pubInputs.append(bytes(bytes32(tokenTypeFr)));
        pubInputs.append(bytes(bytes32(_u256ToFr(recipientEphemeralPubkey))));

        require(gosh.zkhalo2verify(pubInputs, proof), ERR_INVALID_ZKPROOF);

        mapping(uint32 => varuint32) dataCur;
        dataCur[CURRENCIES_ID_SHELL] = value;

        TvmCell stateInit = abi.encodeStateInit({
            contr: Nullifier,
            varInit: {
                _nullifierHash: nullifierHash
            },
            code: _nullifierCode
        });

        TvmCell stateInitNote = DexLib.buildPrivateNoteInitData(_privateNoteCode, depositIdentifierHash);
        address noteAddress = address.makeAddrStd(0, tvm.hash(stateInitNote));

        address nullifier = address.makeAddrStd(0, tvm.hash(stateInit));

        new Nullifier{
            stateInit: stateInit,
            value: 10 vmshell,
            flag: 1,
            currencies: dataCur
        }(noteAddress);

        address addrExtern = address.makeAddrExtern(ROOTPN_NULLIFIER_DEPLOYED, bitCntAddress);
        emit NullifierDeployed{dest: addrExtern}(nullifier, value);
    }

    /// @notice Deploys a new PrivateNote contract.
    ///         Same proof-verification flow as `sendEccShellToPrivateNote`:
    ///         the node-side `check_layer_hash` must succeed for the supplied
    ///         `finalLayerHistoricalHashRoot` at `layerNumber`, and the
    ///         halo2 proof must validate against the 4-field public-input
    ///         vector below.
    ///
    /// @param zkproof Zero-knowledge proof used to validate the deposit public inputs.
    /// @param depositIdentifierHash Poseidon commitment (instance 0), also
    ///        derives the PrivateNote address.
    /// @param finalLayerHistoricalHashRoot Dense chain root (instance 1),
    ///        checked against the node's `GlobalHistoricalData[layerNumber]`.
    /// @param voucherNominalFr Voucher nominal as Fr (instance 2).
    /// @param tokenTypeFr Token type as Fr (instance 3).
    /// @param ephemeralPubkey Ephemeral public key for authorizing the deployed PrivateNote.
    /// @param value Initial token balance.
    /// @param tokenType Token type used by the deployed PrivateNote.
    /// @param layerNumber Historical layer being proved against (L1, L2, …).
    function deployPrivateNote(
        bytes zkproof,
        uint256 depositIdentifierHash,
        uint256 finalLayerHistoricalHashRoot,
        uint256 voucherNominalFr,
        uint256 tokenTypeFr,
        uint256 ephemeralPubkey,
        uint64 value,
        uint32 tokenType,
        uint8 layerNumber
    ) public view accept {
        ensureBalance();

        require(
            gosh.check_layer_hash(finalLayerHistoricalHashRoot, layerNumber),
            ERR_INVALID_HISTORY_PROOF
        );

        // Bind user's `value` and `tokenType` to the proof's pubInputs so the
        // minted deposit always equals the voucher's committed nominal and token
        // type (a 100-shell voucher proof cannot mint a 1M-NACKL deposit).
        require(_u64ToFr(value) == voucherNominalFr, ERR_INVALID_ZKPROOF);
        require(_u32ToFr(tokenType) == tokenTypeFr, ERR_INVALID_ZKPROOF);
        // Require a non-zero ephemeral key: eph=0 would make msg.pubkey()==0 pass
        // onlyOwnerPubkey on every PN method, so a zero key is rejected up front.
        require(ephemeralPubkey != 0, ERR_INVALID_PARAMS);
        // SHELL_FEE (300) is the gas-only token used by sendEccShellToPrivateNote.
        // RootPN custodies only type-2 ECC, not type-300, so a PN's main ledger
        // must not hold SHELL_FEE; deployment for the fee-only token is rejected.
        require(tokenType != CURRENCIES_ID_SHELL_FEE, ERR_INVALID_PARAMS);

        // Bind ephemeralPubkey to the proof. The halo2 circuit emits
        // ephemeralPubkey as instance 4; any mismatch between the caller-supplied
        // value and the one baked into the proof aborts zkhalo2verify, so the
        // pubkey cannot be substituted without re-running the prover against the
        // committed secret.
        //
        // CIRCUIT/PROVER CONTRACT:
        //   pubInputs[0] = depositIdentifierHash
        //   pubInputs[1] = finalLayerHistoricalHashRoot
        //   pubInputs[2] = voucherNominalFr
        //   pubInputs[3] = tokenTypeFr
        //   pubInputs[4] = ephemeralPubkey (raw uint256 big-endian 32 bytes)
        // The halo2 prover MUST expose the same 5-field instance vector.
        bytes pubInputs;
        pubInputs.append(bytes(bytes32(depositIdentifierHash)));
        pubInputs.append(bytes(bytes32(finalLayerHistoricalHashRoot)));
        pubInputs.append(bytes(bytes32(voucherNominalFr)));
        pubInputs.append(bytes(bytes32(tokenTypeFr)));
        pubInputs.append(bytes(bytes32(_u256ToFr(ephemeralPubkey))));

        require(gosh.zkhalo2verify(pubInputs, zkproof), ERR_INVALID_ZKPROOF);
        TvmCell stateInit = DexLib.buildPrivateNoteInitData(_privateNoteCode, depositIdentifierHash);

        new PrivateNote{
            stateInit: stateInit,
            value: 50 vmshell,
            flag: 1
        }(value, ephemeralPubkey, tokenType, _pmpCode, _orderBookCode, _inferenceOrderBookCode,
          tvm.hash(_oracleCode), _oracleCode.depth(), tvm.hash(_oracleEventListCode), _oracleEventListCode.depth(),
          TOKEN_CONTRACT_CODE_HASH, TOKEN_CONTRACT_CODE_DEPTH, ROOT_MODEL_CODE_HASH, ROOT_MODEL_CODE_DEPTH);
    }

    /// @notice Records deployment of a PrivateNote contract
    /// @param depositIdentifierHash Unique identifier for the deposit
    /// @param tokenType Type of token deployed 
    /// @param deployedValue Value of the deployed token
    function privateNoteDeployed(uint256 depositIdentifierHash, uint32 tokenType, uint128 deployedValue) public senderIs(address.makeAddrStd(0, tvm.hash(DexLib.buildPrivateNoteInitData(_privateNoteCode, depositIdentifierHash)))) accept {
        _deployedValues[tokenType] += deployedValue;
        address addrExtern = address.makeAddrExtern(ROOTPN_PRIVATE_NOTE_DEPLOYED, bitCntAddress);
        emit PrivateNoteDeployed{dest: addrExtern}(depositIdentifierHash, msg.sender, deployedValue);
    }

    /// @notice Updates the contract code for RootPN
    /// @param newcode New contract code
    /// @param cell Encoded persistent state used by `onCodeUpgrade`.
    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    /// @notice Handles root code upgrade
    /// @param cell Code upgrade data cell
    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        ensureBalance();
        tvm.resetStorage();
        // 6 codes + pubkey only. The InferenceOrderBook code is set separately via
        // setInferenceOrderBookCode — keeps this upgrade cell small enough to POST
        // to the shellnet BM gateway (a 7-code cell overflows the JSON-body limit).
        (_pmpCode, _privateNoteCode, _nullifierCode, _oracleCode, _oracleEventListCode, _orderBookCode, _ownerPubkey) = abi.decode(cell, (TvmCell, TvmCell, TvmCell, TvmCell, TvmCell, TvmCell, uint256));
    }

    /// @notice Owner-only setter for the InferenceOrderBook code (§8 inference
    ///         market). Kept out of `onCodeUpgrade` so the upgrade message stays
    ///         small; this sets/updates just the one code in a tiny message.
    /// @param inferenceOrderBookCode New InferenceOrderBook code baked into notes.
    function setInferenceOrderBookCode(TvmCell inferenceOrderBookCode) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _inferenceOrderBookCode = inferenceOrderBookCode;
    }

    // The InferenceOrderBook code hash is no longer requested from RootPN at runtime.
    // RootPN bakes the book code (`_inferenceOrderBookCode`) AND the TokenContract /
    // RootModel code hashes into every note at deploy (see `deployPrivateNote`); the
    // seller's canonical PrivateNote derives the deal TC locally and hands it the book
    // hash directly via `TokenContract.postFromNote` — a single seller call, no round-trip.

    /// @notice Owner-only setter for the PrivateNote code. Kept out of `onCodeUpgrade` so the upgrade
    ///         message stays small — the PrivateNote code is the largest in the bundle and a full
    ///         6-code upgrade cell overflows the shellnet BM JSON-body limit. `updateCode` carries an
    ///         EMPTY PrivateNote slot; this sets the real code in a separate small message.
    /// @param privateNoteCode New PrivateNote code baked into deployed notes.
    function setPrivateNoteCode(TvmCell privateNoteCode) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _privateNoteCode = privateNoteCode;
    }

    /// @notice Returns the salted PrivateNote contract code
    /// @return privateNoteCode The salted PrivateNote contract code as TvmCell
    /// @return privateNoteHash Hash of PrivateNote contract code
    function getPrivateNoteCode() external view returns(TvmCell privateNoteCode, uint256 privateNoteHash) {
        TvmCell saltPN = abi.encode(_privateNoteCode);
        TvmCell codePN = abi.setCodeSalt(_privateNoteCode, saltPN);
        return (codePN, tvm.hash(codePN));
    }

    /// @notice Returns the deterministic address of a PrivateNote by deposit identifier hash
    /// @param depositIdentifierHash Unique identifier hash for the deposit
    /// @return privateNoteAddress Deterministic PrivateNote contract address
    function getPrivateNoteAddress(uint256 depositIdentifierHash) external view returns(address privateNoteAddress) {
        return DexLib.computePrivateNoteAddress(_privateNoteCode, depositIdentifierHash);
    }

    /// @notice Returns the deterministic address of a PMP for the given event and oracle set
    /// @param eventId Event identifier used by the PMP
    /// @param names List of oracle names used to build the oracle list hash
    /// @param tokenType Token type used by the PMP
    /// @return pmpAddress Deterministic PMP contract address
    function getPMPAddress(uint256 eventId, string[] names, uint32 tokenType) external view returns(address pmpAddress) {
        mapping(uint256 => bool) forOracleHash;
        uint256 length = names.length;

        for (uint32 i = 0; i < length; i++) {
            forOracleHash[tvm.hash(names[i])] = true;
        }
        uint256 oracleListHash = tvm.hash(abi.encode(forOracleHash));
        return DexLib.computePMPAddress(_privateNoteCode, _pmpCode, eventId, oracleListHash, tokenType);
    }

    /// VAULT FUNCTIONS

    /// @notice Check is it Allowed
    /// @param nominal Voucher nominal in token base units.
    /// @param tokenType Token type to validate.
    /// @return isAllowed True if nominal matches one of `ALLOWED_NOMINALS` for the token decimals.
    function isAllowedNominal(uint128 nominal, uint32 tokenType) private view returns (bool) {
        uint128 decimals = tokenDecimals(tokenType);
        for (uint i = 0; i < ALLOWED_NOMINALS.length; i++) {
            if (ALLOWED_NOMINALS[i] * decimals == nominal) {
                return true;
            }
        }
        return false;
    }

    /// @notice Checks if the nominal is allowed for vault operations
    /// @param skUCommit Commitment of user secret key used in off-chain flows.
    /// @param isFee Whether incoming shell tokens must be treated as fee token type.
    function generateVoucher(uint256 skUCommit, bool isFee) public view internalMsg {
		require(msg.currencies.keys().length == 1, 300);
		uint32 tokenType = msg.currencies.keys()[0];
		require(msg.currencies[tokenType] > 0, 303);
		tvm.accept();
        ensureBalance();

		uint voucherNominal = msg.currencies[tokenType];
        require(isAllowedNominal(uint128(voucherNominal), tokenType), ERR_NOT_ALLOWED);

        if ((tokenType == CURRENCIES_ID_SHELL) && (isFee)) {
            tokenType = CURRENCIES_ID_SHELL_FEE;
        }

        address addrExtern = address.makeAddrExtern(VAULT_voucher_GENERATED, bitCntAddress);
		emit VoucherGenerated{dest: addrExtern}(skUCommit, voucherNominal, tokenType);
	}

    /// @notice Withdraws tokens to a specified wallet.
    /// @dev The inner `transfer` flag is hard-coded to 1. A caller-supplied flag
    ///      could pass TVM flags 128 (CARRY_ALL_BALANCE) or 32 (DELETE_IF_EMPTY),
    ///      which would move or clear RootPN's whole balance. RootPN custodies
    ///      every PN's ECC, so the flag is fixed to 1 to keep that custody intact.
    /// @param amounts Per-token-type amounts to withdraw (the note's full balance)
    /// @param walletAddr Destination wallet address
    /// @param initialDataHash Initial data hash for verification
    /// @param dapp_id DApp id — drives no logic, only surfaced in the event
    function withdrawTokens(
        mapping(uint32 => uint128) amounts,
        address walletAddr,
        uint256 initialDataHash,
        uint256 dapp_id
    ) public senderIs(DexLib.computePrivateNoteAddress(_privateNoteCode, initialDataHash)) accept {
        // `dapp_id` drives no logic here — only surfaced in the TokensWithdrawn
        // event below; kept for forward compatibility / off-chain context.
        ensureBalance();
        // Verify every requested token type up front — both real currency
        // reserves and the bookkeeping pool must cover it. Any gap → revert the
        // whole withdraw on the PN side (atomic: nothing is transferred). A
        // plain require would leave the PN's `_balance` permanently low.
        for ((uint32 tt, uint128 amt) : amounts) {
            if (amt > 0 && (address(this).currencies[tt] < amt || _deployedValues[tt] < amt)) {
                // Bounce the note's attached PHYSICAL currency (its inference SHELL pool,
                // drained on withdraw) back to it along with the revert, so it returns to
                // the note when the custody withdraw is refused.
                PrivateNote(msg.sender).revertWithdraw{value: 0.1 vmshell, flag: 1, currencies: msg.currencies, dest_dapp_id: ROOT_PN_DAPP_ID}(
                    amounts
                );
                return;
            }
        }

        // Prepare the combined currency transfer and debit the bookkeeping pools.
        mapping(uint32 => varuint32) cc;
        for ((uint32 tt, uint128 amt) : amounts) {
            if (amt > 0) {
                cc[tt] = varuint32(amt);
                _deployedValues[tt] -= amt;
            }
        }
        // Pass through any PHYSICAL currency the note attached to this message (its
        // inference SHELL pool, drained on withdraw) straight to the destination —
        // it is NOT custodied bookkeeping, so no `_deployedValues` debit.
        for ((uint32 tt, varuint32 amt) : msg.currencies) {
            cc[tt] += amt;
        }

        // Transfer every currency at once — flag is intentionally hard-coded to 1.
        walletAddr.transfer({value: 0.1 vmshell, bounce: false, flag: 1, currencies: cc});

        // External event: per-token-type amounts, from which PrivateNote
        // (msg.sender) and to which destination wallet.
        address addrExtern = address.makeAddrExtern(ROOTPN_TOKENS_WITHDRAWN, bitCntAddress);
        emit TokensWithdrawn{dest: addrExtern}(amounts, msg.sender, walletAddr, dapp_id);
    }

    /// @notice Records an OrderBook's accumulated protocol fees at its shutdown.
    /// @dev The backing real ECC is already custodied by this RootPN (it was the
    ///      taker-fee share never credited to any note); this call only marks the
    ///      amount as owner-withdrawable. The caller is authenticated as the
    ///      legitimate OrderBook for (eventId, oracleListHash, tokenType).
    /// @param eventId Event id of the calling OrderBook.
    /// @param oracleListHash Oracle list hash of the calling OrderBook.
    /// @param tokenType Token type of the collected protocol fee.
    /// @param amount Accumulated protocol fee amount.
    function collectProtocolFee(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint128 amount)
        public
        senderIs(DexLib.computeOrderBookAddress(_privateNoteCode, _orderBookCode, eventId, oracleListHash, tokenType))
        accept
    {
        ensureBalance();
        _protocolFees[tokenType] += amount;
        address addrExtern = address.makeAddrExtern(ROOTPN_PROTOCOL_FEE_COLLECTED, bitCntAddress);
        emit ProtocolFeeCollected{dest: addrExtern}(tokenType, amount);
    }

    /// @notice Withdraws accumulated OrderBook protocol fees to any address in any
    ///         dapp. Root-owner only.
    /// @param to Destination account address.
    /// @param dapp_id Destination dapp id to route the transfer to.
    /// @param tokenType Token type to withdraw.
    /// @param amount Amount to withdraw (must be <= accumulated protocol fees).
    function withdrawProtocolFees(address to, uint256 dapp_id, uint32 tokenType, uint128 amount)
        public onlyOwnerPubkey(_ownerPubkey) accept
    {
        ensureBalance();
        require(amount > 0 && amount <= _protocolFees[tokenType], ERR_INVALID_PARAMS);
        require(
            address(this).currencies[tokenType] >= amount && _deployedValues[tokenType] >= amount,
            ERR_INVALID_PARAMS
        );
        _protocolFees[tokenType] -= amount;
        _deployedValues[tokenType] -= amount;

        mapping(uint32 => varuint32) cc;
        cc[tokenType] = varuint32(amount);
        to.transfer({value: 0.1 vmshell, bounce: false, flag: 1, currencies: cc, dest_dapp_id: dapp_id});

        address addrExtern = address.makeAddrExtern(ROOTPN_PROTOCOL_FEE_WITHDRAWN, bitCntAddress);
        emit ProtocolFeeWithdrawn{dest: addrExtern}(to, dapp_id, tokenType, amount);
    }

    /// @notice Returns accumulated (un-withdrawn) protocol fees for a token type.
    function getProtocolFee(uint32 tokenType) external view returns (uint128) {
        return _protocolFees[tokenType];
    }

    /// @notice Returns all global variables
    /// @return pmpCodeHash Hash of PMP code
    /// @return privateNoteCodeHash Hash of PrivateNote code
    /// @return ownerPubkey Root owner public key
    /// @return balance Current contract balance
    function getDetails() external view returns (
        uint256 pmpCodeHash,
        uint256 privateNoteCodeHash,
        uint256 ownerPubkey,
        uint128 balance
    ) {
        return (
            tvm.hash(_pmpCode),
            tvm.hash(_privateNoteCode),
            _ownerPubkey,
            address(this).balance
        );
    }

    /// @notice Returns root version
    /// @return value0 Contract semantic version.
    /// @return value1 Contract identifier.
    function getVersion() external pure returns (string, string) {
        return (version, "RootPN");
    }
}
