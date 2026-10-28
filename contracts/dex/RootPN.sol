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
    string constant version = "4.0.36";

    // RootModel/TokenContract code hashes. Baked into every PrivateNote at deploy
    // (`deployPrivateNote`) so the note derives the canonical RootModel / deal TC
    // locally and posts its offer in a single call. The SuperRoot account id the
    // note pairs these with is its own constant — RootPN never derives an address
    // itself. RootPN is not pinned by anyone, so pinning these here is cycle-free
    // (cascade-updated together with the note's baked copies).
    uint256 constant TOKEN_CONTRACT_CODE_HASH  = 0xee4105b4800d852dde1a86cec4e270ecfa2ae0e199f05a46823aed792933e711;
    uint16  constant TOKEN_CONTRACT_CODE_DEPTH = 18;
    uint256 constant ROOT_MODEL_CODE_HASH      = 0xe92a14cb9c5ac757e16be2f453d5c3a25e7bec90044a1389b97414d1b785cac8;
    uint16  constant ROOT_MODEL_CODE_DEPTH     = 8;

    /// @notice Stored code of PrivateNote contract
    TvmCell _privateNoteCode;

    /// @notice Code hash + depth of the PREVIOUS PrivateNote generation, so notes deployed before
    ///         this build can still be served (4.0.36).
    /// @dev    A note's address derives from its code, so changing the code changes where every
    ///         FUTURE note lives — and leaves the ones already deployed at addresses this root can
    ///         no longer derive. `withdrawTokens` is gated on that derivation, which means an
    ///         upgraded root would stop recognising live notes and their custodied ECC would sit
    ///         here with no caller able to claim it. That is not a migration cost, it is a loss.
    ///
    ///         The root keeps the current CODE (it deploys with it) and the previous HASH (it only
    ///         needs to address them). Zero means "no previous generation", which is the honest
    ///         state of a fresh chain and refuses nothing that ever existed.
    ///
    ///         ONE generation back, deliberately. Two would need a list and a policy for how long
    ///         a generation stays honoured; one covers the case that actually occurs — an upgrade
    ///         with notes still holding balances — and says plainly when it stops.
    uint256 _prevPrivateNoteCodeHash;
    uint16  _prevPrivateNoteCodeDepth;

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

    /// @notice Stored code of TokenContract (§3 per-deal contract), baked into every PrivateNote at
    ///         deploy so the note can DEPLOY the deal rather than only address it (4.0.36).
    /// @dev    The pin `TOKEN_CONTRACT_CODE_HASH` below still travels beside it and still does its
    ///         own job — a note derives the address of deals it did not create from the hash. This
    ///         holds the code itself, which only a deploy needs. Set through `setTokenContractCode`
    ///         rather than the constructor, for the reason `setInferenceOrderBookCode` gives: the
    ///         upgrade message stays small enough for the shellnet BM body limit.
    TvmCell _tokenContractCode;

    /// @notice Canonical AI SuperRoot account id, the anchor a deal's address derives from.
    /// @dev    Needed only by `_canonicalDeal`: a TokenContract's statics include its RootModel,
    ///         and that RootModel derives under this SuperRoot. Same literal the note and the book
    ///         carry, for the same derivation — one anchor, three contracts, no rotation.
    uint256 constant SUPER_ROOT_ADDR = 0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c;

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

    /// @notice SHELL that deals have written off, reported by them and never redeemable.
    /// @dev    Accumulated, not destroyed — see `reportDealWriteOff`.
    mapping(uint32 => uint128) _writtenOff;

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

    /// @notice A deal reported SHELL it wrote off its own record.
    event DealWriteOffReported(address deal, uint128 amount);

    /// @notice Emitted when the owner withdraws accumulated protocol fees.
    /// @param to — Destination address
    /// @param dapp_id — Destination dapp id
    /// @param tokenType — Token type withdrawn
    /// @param amount — Amount withdrawn
    event ProtocolFeeWithdrawn(address to, uint256 dapp_id, uint32 tokenType, uint128 amount);

    /// @notice Root constructor — intentionally unreachable.
    /// @dev This root is only ever brought up via the stub + `updateCode`
    ///      bootstrap, which installs the code and `_ownerPubkey` through
    ///      `onCodeUpgrade` and does not run this constructor. A direct deploy is
    ///      not a supported path, so it is rejected outright.
    constructor() {
        require(false, ERR_NOT_ALLOWED_CONSTRUCTOR);
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

        // `bounce: true` WRITTEN OUT, because this message carries currency. A deploy defaults to
        // bounce:false, and under that default a message that fails leaves its `currencies` on the
        // destination instead of returning them — the destination being an address derived from the
        // voucher, so a repeat aims at an account that already exists and the coins simply stay
        // there. Stated rather than inherited: the fate of money must not depend on a default
        // nobody in this project could recite from memory.
        new Nullifier{
            stateInit: stateInit,
            value: 10 vmshell,
            flag: 1,
            bounce: true,
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
        // Bind the message signer to the ephemeral key (mirrors
        // sendEccShellToPrivateNote). The proof only commits ephemeralPubkey as a
        // single field element via _u256ToFr, which is not injective over 256 bits
        // (~5 congruent siblings share one Fr), so the zk check alone does NOT pin
        // the full key: a third party could replay the proof with a congruent
        // sibling and deploy the note at the same (dih-derived) address under a key
        // nobody holds, permanently locking the deposit. Requiring the signer to
        // hold the ephemeral secret closes this — at the cost of third-party deploy.
        require(msg.pubkey() == ephemeralPubkey, ERR_INVALID_SENDER);
        // SHELL_FEE (300) is the gas-only token used by sendEccShellToPrivateNote.
        // RootPN custodies only type-2 ECC, not type-300, so a PN's main ledger
        // must not hold SHELL_FEE; deployment for the fee-only token is rejected.
        require(tokenType != CURRENCIES_ID_SHELL_FEE, ERR_INVALID_PARAMS);

        // Bind ephemeralPubkey to the proof as instance 4. NOTE: _u256ToFr reduces
        // mod FR_MODULUS, so this pins only the Fr projection of the key, not its
        // full 256 bits — the msg.pubkey() gate above is what binds the exact key.
        //
        // CIRCUIT/PROVER CONTRACT:
        //   pubInputs[0] = depositIdentifierHash
        //   pubInputs[1] = finalLayerHistoricalHashRoot
        //   pubInputs[2] = voucherNominalFr
        //   pubInputs[3] = tokenTypeFr
        //   pubInputs[4] = ephemeralPubkey (Fr, _u256ToFr of the raw uint256)
        // The halo2 prover MUST expose the same 5-field instance vector.
        bytes pubInputs;
        pubInputs.append(bytes(bytes32(depositIdentifierHash)));
        pubInputs.append(bytes(bytes32(finalLayerHistoricalHashRoot)));
        pubInputs.append(bytes(bytes32(voucherNominalFr)));
        pubInputs.append(bytes(bytes32(tokenTypeFr)));
        pubInputs.append(bytes(bytes32(_u256ToFr(ephemeralPubkey))));

        require(gosh.zkhalo2verify(pubInputs, zkproof), ERR_INVALID_ZKPROOF);
        TvmCell stateInit = DexLib.buildPrivateNoteInitData(_privateNoteCode, depositIdentifierHash);

        // THE GAS COLLECTED AT DEPOSIT IS HANDED OVER HERE, and it must ride as ECC[2] rather than
        // as native value: a note is deployed CROSS-DAPP, and native does not cross that boundary —
        // ECC does, converting on arrival. The `value: 50 vmshell` below pays for THIS message; it
        // is not what the note ends up living on.
        //
        // The two sides balance IN AGGREGATE, never per note. One non-gas voucher yields exactly
        // one deploy, so as many `GAS_DEPOSIT`s as were collected, that many notes were funded.
        // Matching a particular deposit to a particular deploy is precisely what this scheme's
        // privacy rests on being impossible, so no attempt is made to reconcile them pairwise.
        mapping(uint32 => varuint32) gasCc;
        gasCc[CURRENCIES_ID_SHELL] = varuint32(GAS_DEPOSIT);
        // `bounce: true`, AND THIS IS THE LINE THAT CLOSES A LEAK. Nothing here refuses a voucher
        // that was already used: the proof, the key and the token type all still check out on a
        // second call, and the address derives from the same `stateInit`, so the deploy aims at the
        // live note. Under the bounce:false default the constructor call failed, the transaction
        // aborted, and `GAS_DEPOSIT` STAYED on that note — drawn from the root's pool, against a
        // collection that happened once. The holder of one valid voucher could repeat it without
        // limit.
        //
        // With the bounce written out the coins come home on every failure, so a repeat costs the
        // caller his gas and moves no money. The voucher can still be presented again — that is
        // accepted deliberately, not overlooked: a replay is now empty, and emptiness needs no
        // guard.
        new PrivateNote{
            stateInit: stateInit,
            value: 50 vmshell,
            flag: 1,
            bounce: true,
            currencies: gasCc
        }(value, ephemeralPubkey, tokenType, _pmpCode, _orderBookCode, _inferenceOrderBookCode, _tokenContractCode,
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
        // `onlyOwnerPubkey(k)` is `require(msg.pubkey() == k)`, so a zero key admits every
        // unsigned message and this contract's own `updateCode` stops being owner-gated. The key
        // arrives in the migration cell, which is where it has to be rejected — the constructor
        // does not run on an account upgraded from a stub.
        require(_ownerPubkey != 0, ERR_INVALID_PARAMS);
    }

    /// @notice Owner-only setter for the InferenceOrderBook code (§8 inference
    ///         market). Kept out of `onCodeUpgrade` so the upgrade message stays
    ///         small; this sets/updates just the one code in a tiny message.
    /// @param inferenceOrderBookCode New InferenceOrderBook code baked into notes.
    function setInferenceOrderBookCode(TvmCell inferenceOrderBookCode) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _inferenceOrderBookCode = inferenceOrderBookCode;
    }

    /// @notice Owner-only setter for the TokenContract code baked into notes (4.0.36).
    /// @dev    Same reason as the book code above: one code in one small message, so an upgrade
    ///         never has to carry the whole bundle. A note deployed before this is set holds an
    ///         empty cell and its `deployDeal` cannot produce the canonical deal — set it in the
    ///         same provisioning step as the book code, before any note is issued.
    /// @param tokenContractCode New TokenContract code baked into notes.
    function setTokenContractCode(TvmCell tokenContractCode) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _tokenContractCode = tokenContractCode;
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
    /// @param isFee THIS FLAG NOW DECIDES TWO THINGS, and the owner calls it `isGas`.
    ///
    ///        1. whether incoming SHELL is remapped to the fee token type (2 -> 300), as before;
    ///        2. whether `GAS_DEPOSIT` is deducted — a gas voucher pays no gas, since charging gas
    ///           for buying gas would be circular.
    ///
    ///        The field is NOT renamed to `isGas` despite the owner's word for it, because this is
    ///        ABI the client encodes (`encode_generatevoucher_body`) and renaming would break that
    ///        for a word. The disagreement is written here so the next reader does not have to
    ///        solve it: same field, two names, one of which lives outside this repository.
    function generateVoucher(uint256 skUCommit, bool isFee) public view internalMsg {
        // EVERY NON-GAS DEPOSIT PAYS `GAS_DEPOSIT` IN SHELL, and this is where it is taken. The
        // note that this voucher will deploy is created cross-dapp, where a plain native value
        // does not reach — only ECC[2] crosses and converts. So without this collection a note
        // would come into existence unable to do anything at all.
        //
        // `isFee` is the flag the owner calls `isGas`, and after this change that is the better
        // name: it now decides whether gas is deducted, not only whether the voucher's type is
        // remapped to SHELL_FEE. The field keeps its old name because it is ABI the client encodes
        // (`encode_generatevoucher_body`), and renaming it would break that for a word.
        //
        // Four shapes, and nothing else is accepted:
        //
        //   one currency,  isFee=true   -> take nothing; the whole amount is the nominal. This is
        //                                  the gas-voucher path itself (type 2 becomes 300 below),
        //                                  and charging gas for buying gas would be circular.
        //   one currency,  isFee=false  -> take GAS_DEPOSIT; nominal is the remainder. The currency
        //                                  MUST be SHELL, since there is nothing else to take from.
        //   two currencies              -> the SHELL leg is the gas, exactly GAS_DEPOSIT, and is
        //                                  consumed whole; the other currency is the nominal,
        //                                  whatever `isFee` says.
        //   anything else               -> refused.
        uint32[] keys = msg.currencies.keys();
        require(keys.length == 1 || keys.length == 2, ERR_BAD_GAS_MIX);

        uint32 tokenType;
        uint voucherNominal;

        if (keys.length == 2) {
            // Which leg is the gas is decided by TYPE, not by position: a currency map has no
            // order the caller controls, so reading `keys[0]` as "the gas one" would depend on
            // something neither side promises.
            uint32 a = keys[0];
            uint32 b = keys[1];
            uint32 shellLeg = a == CURRENCIES_ID_SHELL ? a : b;
            tokenType       = a == CURRENCIES_ID_SHELL ? b : a;
            require(shellLeg == CURRENCIES_ID_SHELL, ERR_BAD_GAS_MIX);
            require(uint128(msg.currencies[shellLeg]) == GAS_DEPOSIT, ERR_BAD_GAS_MIX);
            // A SHELL_FEE nominal cannot be deposited: `deployPrivateNote` refuses type 300, so the
            // voucher would be money taken against a note that can never be placed.
            require(tokenType != CURRENCIES_ID_SHELL_FEE, ERR_FEE_TYPE_NOT_DEPOSITABLE);
            voucherNominal = msg.currencies[tokenType];
        } else {
            tokenType = keys[0];
            voucherNominal = msg.currencies[tokenType];
            // WHY A GAS VOUCHER PAYS NO GAS — read this before "fixing" the missing deduction.
            //
            // A fee voucher CANNOT DEPLOY A NOTE AT ALL. `deployPrivateNote` refuses type 300
            // outright (`require(tokenType != CURRENCIES_ID_SHELL_FEE)`), and
            // `sendEccShellToPrivateNote` demands it — so a fee voucher's only destination is a
            // note that already exists, and it arrives there whole. Deducting 250 here would mean
            // taking ECC from a deposit in order to hand the same ECC back to the same note.
            //
            // That is what makes part B's arithmetic EXACT rather than approximate:
            //
            //   250 is taken on exactly the path that ends in a deploy   (non-fee voucher)
            //   250 is given out on exactly the deploy
            //   the fee path takes part in neither side
            //
            // There is no balance to reconcile between the two paths, because the second path is
            // not in this arithmetic at all. Not "it evens out on average" — it does not enter.
            // A SINGLE-CURRENCY DEPOSIT IS SHELL OR IT IS NOTHING — both branches, one line.
            //
            // The non-fee branch has always needed it: there is nothing else to take the gas from.
            // The fee branch needs it for the opposite reason, and the gap between the two was a
            // MINT. `isFee` skips the deduction, while the remap to SHELL_FEE below is guarded by
            // `tokenType == CURRENCIES_ID_SHELL` — so a NACKL deposit carrying the flag produced an
            // ordinary NACKL voucher with no gas taken, that voucher deployed a note, and the root
            // handed that note `GAS_DEPOSIT` out of the common pool. Nothing bounded the loop: the
            // same NACKL comes back and mints another 250 SHELL every time.
            //
            // Hoisted out of the branch rather than written twice, because two copies of one
            // requirement drift apart — and the drift is invisible until it is the whole defect.
            require(tokenType == CURRENCIES_ID_SHELL, ERR_BAD_GAS_MIX);
            if (!isFee) {
                // BEFORE the subtraction, deliberately. Leaving it to the subtraction to revert
                // would make correctness a property of what this compiler does with an underflow
                // rather than of this contract; a wrapping subtraction turns a one-SHELL deposit
                // into an astronomically large nominal. An amount EQUAL to GAS_DEPOSIT is allowed
                // through here and dies one check later, on `require(voucherNominal > 0)` —
                // `ERR_ZERO_TOKEN_AMOUNT` (128), not the nominal list (141), which it never
                // reaches. Both refuse it, and the intended outcome is asserted rather than
                // assumed; but a depositor reading 128 needs to find that number written here.
                require(uint128(voucherNominal) >= GAS_DEPOSIT, ERR_BELOW_GAS_DEPOSIT);
                voucherNominal -= GAS_DEPOSIT;
            }
        }

        // Named, and named from the DEX table. The literal here used to be 303 — a code that
        // exists in airegistry and not in dex, so a depositor looking it up found either nothing
        // or somebody else's meaning. A raw number in a require is a message to whoever is reading
        // the failure, and this one was addressed to the wrong contract set.
        require(voucherNominal > 0, ERR_ZERO_TOKEN_AMOUNT);
		tvm.accept();
        ensureBalance();

        require(isAllowedNominal(uint128(voucherNominal), tokenType), ERR_NOT_ALLOWED);

        if ((tokenType == CURRENCIES_ID_SHELL) && (isFee)) {
            tokenType = CURRENCIES_ID_SHELL_FEE;
        }

        address addrExtern = address.makeAddrExtern(VAULT_voucher_GENERATED, bitCntAddress);
		emit VoucherGenerated{dest: addrExtern}(skUCommit, voucherNominal, tokenType);
	}

    /// @notice Accept a note of EITHER generation as the sender for `depositIdentifierHash`.
    /// @dev    Runs before `accept`, like every other sender gate here, so a stranger's message is
    ///         refused at the door and paid for by whoever sent it. The current generation is
    ///         derived from the code this root holds; the previous one from its hash, and only if
    ///         one has been recorded — an unset `_prevPrivateNoteCodeHash` widens nothing.
    modifier senderIsNoteOfAnyGeneration(uint256 depositIdentifierHash) {
        require(
            msg.sender == DexLib.computePrivateNoteAddress(_privateNoteCode, depositIdentifierHash)
            || (_prevPrivateNoteCodeHash != 0
                && msg.sender == DexLib.computePrivateNoteAddressFromHash(
                       _prevPrivateNoteCodeHash, _prevPrivateNoteCodeDepth, depositIdentifierHash)),
            ERR_INVALID_SENDER);
        _;
    }

    /// @notice Owner-only record of the previous PrivateNote generation's code hash and depth.
    /// @dev    Set as part of an upgrade, from the artifact the chain was running BEFORE it — the
    ///         hash of the deployed note code, not of whatever is in the tree now. Passing zero
    ///         clears it, which closes the door on the old generation deliberately.
    /// @param codeHash Code hash of the previous PrivateNote code.
    /// @param codeDepth Cell depth of that code.
    function setPrevPrivateNoteCode(uint256 codeHash, uint16 codeDepth) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _prevPrivateNoteCodeHash = codeHash;
        _prevPrivateNoteCodeDepth = codeDepth;
    }

    /// @notice The previous PrivateNote generation this root still serves (0 = none).
    function getPrevPrivateNoteCode() external view returns (uint256 codeHash, uint16 codeDepth) {
        return (_prevPrivateNoteCodeHash, _prevPrivateNoteCodeDepth);
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
    ) public senderIsNoteOfAnyGeneration(initialDataHash) accept {
        // `dapp_id` drives no logic here — only surfaced in the TokensWithdrawn
        // event below; kept for forward compatibility / off-chain context.
        ensureBalance();
        // Verify every requested token type up front — both real currency
        // reserves and the bookkeeping pool must cover it. Any gap → revert the
        // whole withdraw on the PN side (atomic: nothing is transferred). A
        // plain require would leave the PN's `_balance` permanently low.
        for ((uint32 tt, uint128 amt) : amounts) {
            // Measure the custodial reserve WITHOUT whatever the note attached to this very
            // message. That pool is not custody — it is the note's own physical currency, passed
            // straight through to the destination as a separate term below — so counting it as
            // reserve would clear a withdraw against currency that is merely in transit.
            uint128 attached = msg.currencies.exists(tt) ? uint128(msg.currencies[tt]) : 0;
            uint128 reserve  = uint128(address(this).currencies[tt]);
            reserve = reserve > attached ? reserve - attached : 0;
            if (amt > 0 && (reserve < amt || _deployedValues[tt] < amt)) {
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

    /// @notice A deal reports SHELL it wrote off, so the custodian's ledger stops counting it.
    /// @dev    THE DEAL BURNS NOTHING — it holds no currency, which is the whole of this
    ///         generation. It subtracts a figure from its own record and tells this contract, where
    ///         the backing actually sits. No uint64 chunking either: word size is a problem for
    ///         whoever holds the coins, and that is here.
    ///
    ///         Same shape as `collectProtocolFee` above: derive the caller's canonical address from
    ///         the codes this root already bakes into every note, and admit nobody else. What that
    ///         proves is stronger than "a deal is calling" — it proves the caller runs CANONICAL
    ///         BYTECODE. The amount cannot be inflated, because canonical code only writes off what
    ///         the deal legitimately received, and it can only receive through funding that is
    ///         itself guarded.
    ///
    ///         `_deployedValues` MUST come down by the same figure, and this is the line that would
    ///         be easy to leave out. That ledger says how many claims are still unredeemed; a
    ///         written-off figure will never be redeemed, so leaving it counted makes the ledger
    ///         overstate what is owed. Today that only leaves the pool OVER-collateralised, which is
    ///         safe — but accept the report without the subtraction, then burn what accumulates,
    ///         and the pool becomes UNDER-collateralised and honest withdrawals start hitting a
    ///         shortfall. That failure is silent, which is why the subtraction is not optional.
    ///
    ///         Floored rather than allowed to underflow: an accounting disagreement must not become
    ///         a wrapped subtraction, which would turn a small mismatch into an enormous claim.
    ///
    ///         NOTHING IS BURNED HERE. The root accumulates, as it accumulates fees, and whether to
    ///         destroy the currency is the owner's call — he holds it. A `gosh.burnecc` on
    ///         `CURRENCIES_ID_SHELL` would go exactly here if that decision is ever taken.
    ///
    ///         Losing this message degrades SAFELY: no report, no accounting, and the pool stays
    ///         over-collateralised — which is precisely today's state. That is why it is a plain
    ///         one-way call and not something that retries.
    /// @dev    ACCEPT BEFORE THE GUARD, and the order of these two lines is the whole point.
    ///         Modifiers run in the order written, so a guard above `accept` is charged to the
    ///         INCOMING message — and this guard is not a comparison, it is `_canonicalDeal`, a
    ///         full address derivation. That is why this entry's floor was measured at 10 000 000
    ///         while the deal sends exactly 10 000 000: no margin at all, and one byte of extra
    ///         work ahead of `accept` would have turned the call into `-14` rather than into a
    ///         refusal with a code. Nothing would have reported it: the deal sends this one-way,
    ///         `bounce: false`, and a lost write-off leaves the root's books permanently off.
    ///
    ///         The cost of the swap is that the root now runs the derivation on its own gas for
    ///         any message that arrives, including one it will reject. That is affordable here and
    ///         not elsewhere: this root mints its own gas (`ensureBalance` -> `gosh.mintshellq`,
    ///         :180) because it lives in a configured dapp. A deal cannot — its dapp is its own and
    ///         has no configuration, which is why `TokenContract` carries no such call.
    function reportDealWriteOff(uint256 sellerPubkey, uint64 nonce, uint128 amount)
        public
        accept
        senderIs(_canonicalDeal(sellerPubkey, nonce))
    {
        ensureBalance();
        _writtenOff[CURRENCIES_ID_SHELL] += amount;
        uint128 pool = _deployedValues[CURRENCIES_ID_SHELL];
        _deployedValues[CURRENCIES_ID_SHELL] = pool > amount ? pool - amount : 0;
        emit DealWriteOffReported{dest: address.makeAddrExtern(ROOTPN_DEAL_WRITE_OFF, bitCntAddress)}(
            msg.sender, amount);
    }

    /// @notice Canonical deal address for `(sellerPubkey, nonce)` from this root's baked codes.
    /// @dev    `pure`, not `view`: every input is a constant of this contract or an argument, so
    ///         the derivation reads no state and the compiler says so. Left as `view` it was the
    ///         only warning in the whole set, and a build with one warning is a build nobody reads
    ///         warnings in.
    function _canonicalDeal(uint256 sellerPubkey, uint64 nonce) private pure returns (address) {
        (, address tc) = DexLib.computeCanonicalTokenContractAddress(
            ROOT_MODEL_CODE_HASH, ROOT_MODEL_CODE_DEPTH,
            TOKEN_CONTRACT_CODE_HASH, TOKEN_CONTRACT_CODE_DEPTH,
            address.makeAddrStd(0, SUPER_ROOT_ADDR), sellerPubkey, nonce);
        return tc;
    }

    /// @notice An OrderBook reports the protocol fee it collected before shutting down.
    /// @param eventId PMP event id the book was bound to.
    /// @param oracleListHash Oracle set hash the book was bound to.
    /// @param tokenType Token type of the collected fee.
    /// @param amount Amount collected.
    /// @dev    ACCEPT BEFORE THE GUARD, for the same reason as `reportDealWriteOff` above and with
    ///         a heavier guard: this one derives the book's address from THREE code cells — the
    ///         note's, the PMP's and the book's own — plus the event id, the oracle-set hash and
    ///         the token type. Above `accept` all of that was billed to the caller.
    ///
    ///         Unlike the write-off, this entry was never close to its floor: `OrderBook` sends
    ///         `0.1 vmshell` here (`OrderBook.sol:1502`), ten times what a deal attaches anywhere.
    ///         So the swap buys headroom that was not in danger — worth doing because the failure
    ///         it guards against is the same silent one. The book calls this once, on its way to
    ///         shutting down; a call that ran out of gas would take the protocol fee with it and
    ///         say nothing.
    function collectProtocolFee(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint128 amount)
        public
        accept
        senderIs(DexLib.computeOrderBookAddressFromPmpCode(_privateNoteCode, _pmpCode, _orderBookCode, eventId, oracleListHash, tokenType))
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
