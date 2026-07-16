/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 *
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 *
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

import "../PrivateNote.sol";
import "../PMP.sol";
import "../Oracle.sol";
import "../OracleEventList.sol";
import "../OrderBook.sol";
import "../../airegistry/InferenceOrderBook.sol";
import "../../airegistry/TokenContract.sol";
import "../../airegistry/RootModel.sol";

/// @title DexLib
/// @notice Utility library for deterministic address and StateInit/code construction.
library DexLib {

    /// @notice Computes deterministic PrivateNote address for a deposit hash.
    /// @param PrivateNoteCode PrivateNote contract code.
    /// @param depositIdentifierHash Deposit identifier hash.
    /// @return PrivateNote deterministic address.
    function computePrivateNoteAddress(TvmCell PrivateNoteCode, uint256 depositIdentifierHash) public returns(address) {
        TvmCell s1 = buildPrivateNoteInitData(PrivateNoteCode, depositIdentifierHash);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    /// @notice Builds StateInit for PrivateNote contract
    /// @param PrivateNoteCode Code of PrivateNote contract
    /// @param depositIdentifierHash Unique identifier for the deposit
    /// @return StateInit cell for PrivateNote
    function buildPrivateNoteInitData(TvmCell PrivateNoteCode, uint256 depositIdentifierHash) public returns (TvmCell) {
        return abi.encodeStateInit({
            contr: PrivateNote,
            varInit: { _depositIdentifierHash: depositIdentifierHash },
            code: PrivateNoteCode
        });
    }

    /// @notice Computes deterministic PMP address for event and oracle set.
    /// @param PrivateNoteCode Code of PrivateNote contract
    /// @param pmpCode Code of PMP contract
    /// @param eventId Event identifier
    /// @param oracleListHash Hash of Oracle list
    /// @param tokenType Token type
    /// @return PMP contract address
    function computePMPAddress(TvmCell PrivateNoteCode, TvmCell pmpCode, uint256 eventId, uint256 oracleListHash, uint32 tokenType) public returns (address) {
        TvmCell stateInit = buildPMPStateInit(PrivateNoteCode, pmpCode, eventId, oracleListHash, tokenType);
        return address.makeAddrStd(0, tvm.hash(stateInit));
    }

    /// @notice Builds PMP StateInit with salted code and static ids.
    /// @param PrivateNoteCode PrivateNote contract code used for salt.
    /// @param pmpCode PMP base code.
    /// @param eventId Event identifier.
    /// @param oracleListHash Hash of oracle set.
    /// @param tokenType Token type.
    /// @return PMP StateInit cell.
    function buildPMPStateInit(TvmCell PrivateNoteCode, TvmCell pmpCode, uint256 eventId, uint256 oracleListHash, uint32 tokenType) public returns (TvmCell) {
        TvmCell code = buildPMPCode(PrivateNoteCode, pmpCode);
        return abi.encodeStateInit({
            contr: PMP,
            varInit: { _eventId: eventId, _oracleListHash: oracleListHash, _tokenType: tokenType },
            code: code
        });
    }

    /// @notice Produces salted PMP code where salt stores PrivateNote code.
    /// @param PrivateNoteCode PrivateNote contract code used as salt payload.
    /// @param pmpCode PMP base code.
    /// @return Salted PMP code cell.
    function buildPMPCode(TvmCell PrivateNoteCode, TvmCell pmpCode) public returns (TvmCell) {
        TvmCell salt = abi.encode(PrivateNoteCode);
        return abi.setCodeSalt(pmpCode, salt);
    }

    /// @notice Computes deterministic Oracle address by name.
    /// @param oracleCode Oracle contract code.
    /// @param name Oracle unique name.
    /// @return Oracle deterministic address.
    function computeOracleAddress(TvmCell oracleCode, string name) public returns (address) {
        TvmCell stateInit = buildOracleStateInit(oracleCode, name);
        return address.makeAddrStd(0, tvm.hash(stateInit));
    }

    /// @notice Builds Oracle StateInit for a given name.
    /// @param oracleCode Oracle contract code.
    /// @param name Oracle unique name.
    /// @return Oracle StateInit cell.
    function buildOracleStateInit(TvmCell oracleCode, string name) public returns (TvmCell) {
        return abi.encodeStateInit({
            contr: Oracle,
            varInit: { _name: name },
            code: oracleCode
        });
    }

    /// @notice Computes deterministic OracleEventList address.
    /// @param oracleEventListCode OracleEventList contract code.
    /// @param oracle Oracle contract address.
    /// @param index OracleEventList index.
    /// @return OracleEventList deterministic address.
    function computeOracleEventListAddress(TvmCell oracleEventListCode, address oracle, uint128 index) public returns (address) {
        TvmCell stateInit = buildOracleEventListStateInit(oracleEventListCode, oracle, index);
        return address.makeAddrStd(0, tvm.hash(stateInit));
    }

    /// @notice Builds OracleEventList StateInit for `(oracle, index)`.
    /// @param oracleEventListCode OracleEventList contract code.
    /// @param oracle Oracle contract address.
    /// @param index OracleEventList index.
    /// @return OracleEventList StateInit cell.
    function buildOracleEventListStateInit(TvmCell oracleEventListCode, address oracle, uint128 index) public returns (TvmCell) {
        return abi.encodeStateInit({
            contr: OracleEventList,
            varInit: { _oracle: oracle, _index: index },
            code: oracleEventListCode
        });
    }

    /// @notice Computes deterministic OrderBook address for a market.
    /// @param PrivateNoteCode PrivateNote contract code used for salt.
    /// @param orderBookCode OrderBook base code.
    /// @param eventId Event identifier.
    /// @param oracleListHash Oracle list hash.
    /// @param tokenType Token type.
    /// @return OrderBook deterministic address.
    function computeOrderBookAddress(TvmCell PrivateNoteCode, TvmCell orderBookCode, uint256 eventId, uint256 oracleListHash, uint32 tokenType) public returns (address) {
        TvmCell stateInit = buildOrderBookStateInit(PrivateNoteCode, orderBookCode, eventId, oracleListHash, tokenType);
        return address.makeAddrStd(0, tvm.hash(stateInit));
    }

    /// @notice Builds OrderBook StateInit with salted code and static ids.
    /// @param PrivateNoteCode PrivateNote contract code used for salt.
    /// @param orderBookCode OrderBook base code.
    /// @param eventId Event identifier.
    /// @param oracleListHash Oracle list hash.
    /// @param tokenType Token type.
    /// @return OrderBook StateInit cell.
    function buildOrderBookStateInit(TvmCell PrivateNoteCode, TvmCell orderBookCode, uint256 eventId, uint256 oracleListHash, uint32 tokenType) public returns (TvmCell) {
        TvmCell code = buildOrderBookCode(PrivateNoteCode, orderBookCode);
        return abi.encodeStateInit({
            contr: OrderBook,
            varInit: { _eventId: eventId, _oracleListHash: oracleListHash, _tokenType: tokenType },
            code: code
        });
    }

    /// @notice Produces salted OrderBook code where salt stores PrivateNote code.
    /// @param PrivateNoteCode PrivateNote contract code used as salt payload.
    /// @param orderBookCode OrderBook base code.
    /// @return Salted OrderBook code cell.
    function buildOrderBookCode(TvmCell PrivateNoteCode, TvmCell orderBookCode) public returns (TvmCell) {
        TvmCell salt = abi.encode(PrivateNoteCode);
        return abi.setCodeSalt(orderBookCode, salt);
    }

    // ═══ InferenceOrderBook (deployed FROM a PrivateNote) ═══
    // No code salt: the book binds to the note family via NOTE_CODE_HASH pinned
    // in the InferenceOrderBook code itself, and the deploy is gated in its ctor
    // (deployer must be a genuine note). The address is just (book code + §8
    // statics): same (model, tick) ⇒ same address ⇒ one book.

    /// @notice InferenceOrderBook StateInit: book code + the §8 static set
    ///         (model). One book per model.
    function buildInferenceOrderBookStateInit(TvmCell inferenceOrderBookCode, uint256 modelHash) public returns (TvmCell) {
        return abi.encodeStateInit({
            contr: InferenceOrderBook,
            varInit: { _modelHash: modelHash },
            code: inferenceOrderBookCode
        });
    }

    function computeInferenceOrderBookAddress(TvmCell inferenceOrderBookCode, uint256 modelHash) public returns (address) {
        TvmCell stateInit = buildInferenceOrderBookStateInit(inferenceOrderBookCode, modelHash);
        return address.makeAddrStd(0, tvm.hash(stateInit));
    }

    // ═══ Hash-based address computation (stores uint256+uint16 instead of TvmCell) ═══

    /// @notice Extracts data cell from StateInit.
    /// @param stateInit StateInit cell.
    /// @return Extracted data cell.
    function _extractDataCell(TvmCell stateInit) private returns (TvmCell) {
        TvmSlice s = stateInit.toSlice();
        s.skip(5);
        s.loadRef();
        return s.loadRef();
    }

    /// @notice Deterministic per-seller RootModel address from its pinned code
    ///         hash/depth + the canonical SuperRoot. `_ownerPubkey == sellerPubkey`.
    /// @param codeHash RootModel canonical code hash.
    /// @param codeDepth RootModel canonical code depth.
    /// @param superRoot Canonical SuperRoot address (`_superRootAddress` static).
    /// @param ownerPubkey Seller pubkey the RootModel is derived for.
    /// @return RootModel deterministic address.
    function computeRootModelAddressFromHash(
        uint256 codeHash, uint16 codeDepth, address superRoot, uint256 ownerPubkey
    ) public returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            code: dummyCode, contr: RootModel, pubkey: ownerPubkey,
            varInit: { _ownerPubkey: ownerPubkey, _superRootAddress: superRoot }
        });
        TvmCell dataCell = _extractDataCell(si);
        return address.makeAddrStd(0, abi.stateInitHash(codeHash, tvm.hash(dataCell), codeDepth, dataCell.depth()));
    }

    /// @notice Canonical inference deal addresses: derives the seller's per-seller
    ///         RootModel from `rmCodeHash`+SuperRoot, then the per-deal
    ///         TokenContract bound to it. Single source of truth so the buyer/
    ///         seller PrivateNote (stream-lock auth) and RootPN (fundFromOrderBook
    ///         hash delivery) derive identical addresses from the same pins.
    /// @return rootModel Per-seller RootModel address.
    /// @return tokenContract Per-deal TokenContract address.
    function computeCanonicalTokenContractAddress(
        uint256 rmCodeHash, uint16 rmCodeDepth,
        uint256 tcCodeHash, uint16 tcCodeDepth,
        address superRoot, uint256 sellerPubkey, uint64 nonce
    ) public returns (address rootModel, address tokenContract) {
        rootModel = computeRootModelAddressFromHash(rmCodeHash, rmCodeDepth, superRoot, sellerPubkey);
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            code: dummyCode, contr: TokenContract, pubkey: sellerPubkey,
            varInit: { _sellerPubkey: sellerPubkey, _rootModelAddress: rootModel, _nonce: nonce }
        });
        TvmCell dataCell = _extractDataCell(si);
        tokenContract = address.makeAddrStd(0, abi.stateInitHash(tcCodeHash, tvm.hash(dataCell), tcCodeDepth, dataCell.depth()));
    }

    /// @notice Deterministic InferenceOrderBook address from its (unsalted) code
    ///         hash/depth + the model static — matches
    ///         computeInferenceOrderBookAddress(code, modelHash) but takes the
    ///         hash so a TokenContract can verify a `fundFromOrderBook` caller
    ///         without storing the full book code.
    /// @param codeHash InferenceOrderBook code hash (`tvm.hash(code)`).
    /// @param codeDepth InferenceOrderBook code depth.
    /// @param modelHash Model identity (one book per model).
    /// @return InferenceOrderBook deterministic address.
    function computeInferenceOrderBookAddressFromHash(
        uint256 codeHash, uint16 codeDepth, uint256 modelHash
    ) public returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            contr: InferenceOrderBook, code: dummyCode,
            varInit: { _modelHash: modelHash }
        });
        TvmCell dataCell = _extractDataCell(si);
        return address.makeAddrStd(0, abi.stateInitHash(codeHash, tvm.hash(dataCell), codeDepth, dataCell.depth()));
    }

    /// @notice Deterministic PrivateNote address from its (unsalted) code hash/depth
    ///         + the deposit-identifier static — mirrors buildPrivateNoteInitData but
    ///         takes the hash so a TokenContract can prove a note-driven caller is a
    ///         canonical PrivateNote (pinned code) without storing the full note code
    ///         (which would create a note<->TC pin cycle).
    /// @param codeHash PrivateNote code hash (`tvm.hash(code)`).
    /// @param codeDepth PrivateNote code depth.
    /// @param depositIdentifierHash Note deposit identifier (the note's only static).
    /// @return PrivateNote deterministic address.
    function computeCanonicalNoteAddressFromHash(
        uint256 codeHash, uint16 codeDepth, uint256 depositIdentifierHash
    ) public returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            contr: PrivateNote, code: dummyCode,
            varInit: { _depositIdentifierHash: depositIdentifierHash }
        });
        TvmCell dataCell = _extractDataCell(si);
        return address.makeAddrStd(0, abi.stateInitHash(codeHash, tvm.hash(dataCell), codeDepth, dataCell.depth()));
    }

    /// @notice Computes deterministic PMP address from salted code hash/depth.
    /// @param saltedCodeHash Salted PMP code hash.
    /// @param saltedCodeDepth Salted PMP code depth.
    /// @param eventId Event identifier.
    /// @param oracleListHash Oracle list hash.
    /// @param tokenType Token type.
    /// @return PMP deterministic address.
    function computePMPAddressFromHash(
        uint256 saltedCodeHash, uint16 saltedCodeDepth,
        uint256 eventId, uint256 oracleListHash, uint32 tokenType
    ) public returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            contr: PMP, code: dummyCode,
            varInit: { _eventId: eventId, _oracleListHash: oracleListHash, _tokenType: tokenType }
        });
        TvmCell dataCell = _extractDataCell(si);
        return address.makeAddrStd(0, abi.stateInitHash(saltedCodeHash, tvm.hash(dataCell), saltedCodeDepth, dataCell.depth()));
    }

    /// @notice Computes deterministic PrivateNote address from its code hash/depth
    ///         (so callers can store the hash instead of the full note code).
    /// @param codeHash PrivateNote code hash.
    /// @param codeDepth PrivateNote code depth.
    /// @param depositIdentifierHash Deposit identifier hash.
    /// @return PrivateNote deterministic address.
    function computePrivateNoteAddressFromHash(
        uint256 codeHash, uint16 codeDepth, uint256 depositIdentifierHash
    ) public returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            contr: PrivateNote, code: dummyCode,
            varInit: { _depositIdentifierHash: depositIdentifierHash }
        });
        TvmCell dataCell = _extractDataCell(si);
        return address.makeAddrStd(0, abi.stateInitHash(codeHash, tvm.hash(dataCell), codeDepth, dataCell.depth()));
    }

    /// @notice Computes deterministic Oracle address from code hash/depth.
    /// @param codeHash Oracle code hash.
    /// @param codeDepth Oracle code depth.
    /// @param name Oracle unique name.
    /// @return Oracle deterministic address.
    function computeOracleAddressFromHash(uint256 codeHash, uint16 codeDepth, string name) public returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            contr: Oracle, code: dummyCode,
            varInit: { _name: name }
        });
        TvmCell dataCell = _extractDataCell(si);
        return address.makeAddrStd(0, abi.stateInitHash(codeHash, tvm.hash(dataCell), codeDepth, dataCell.depth()));
    }

    /// @notice Computes deterministic OracleEventList address from code hash/depth.
    /// @param codeHash OracleEventList code hash.
    /// @param codeDepth OracleEventList code depth.
    /// @param oracle Oracle address.
    /// @param index OracleEventList index.
    /// @return OracleEventList deterministic address.
    function computeOracleEventListAddressFromHash(uint256 codeHash, uint16 codeDepth, address oracle, uint128 index) public returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            contr: OracleEventList, code: dummyCode,
            varInit: { _oracle: oracle, _index: index }
        });
        TvmCell dataCell = _extractDataCell(si);
        return address.makeAddrStd(0, abi.stateInitHash(codeHash, tvm.hash(dataCell), codeDepth, dataCell.depth()));
    }

    /// @notice Computes deterministic OrderBook address from salted code hash/depth.
    /// @param saltedCodeHash Salted OrderBook code hash.
    /// @param saltedCodeDepth Salted OrderBook code depth.
    /// @param eventId Event identifier.
    /// @param oracleListHash Oracle list hash.
    /// @param tokenType Token type.
    /// @return OrderBook deterministic address.
    function computeOrderBookAddressFromHash(
        uint256 saltedCodeHash, uint16 saltedCodeDepth,
        uint256 eventId, uint256 oracleListHash, uint32 tokenType
    ) public returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            contr: OrderBook, code: dummyCode,
            varInit: { _eventId: eventId, _oracleListHash: oracleListHash, _tokenType: tokenType }
        });
        TvmCell dataCell = _extractDataCell(si);
        return address.makeAddrStd(0, abi.stateInitHash(saltedCodeHash, tvm.hash(dataCell), saltedCodeDepth, dataCell.depth()));
    }
}
