/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

import "../AckiNackiBlockKeeperNodeWallet.sol";
import "../AckiNackiBlockManagerNodeWallet.sol";
import "../BlockKeeperEpochContract.sol";
import "../BlockKeeperPreEpochContract.sol";
import "../BlockKeeperEpochProxyList.sol";
import "../BLSKeyIndex.sol";
import "../SignerIndex.sol";
import "../License.sol";

library BlockKeeperLib {
    string constant versionLib = "1.0.0";

    function calculateBlockKeeperWalletAddress(TvmCell code, address root, uint256 pubkey) public returns(address) {
        TvmCell s1 = composeBlockKeeperWalletStateInit(code, root, pubkey);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBlockKeeperWalletStateInit(TvmCell code, address root, uint256 pubkey) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBlockKeeperWalletCode(code, root),
            contr: AckiNackiBlockKeeperNodeWallet,
            varInit: {_owner_pubkey: pubkey}
        });
    }

    function buildBlockKeeperWalletCode(
        TvmCell originalCode,
        address root
    ) public returns (TvmCell) {
        TvmCell finalcell;
        finalcell = abi.encode(versionLib, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateBlockManagerWalletAddress(TvmCell code, address root, uint256 pubkey) public returns(address) {
        TvmCell s1 = composeBlockManagerWalletStateInit(code, root, pubkey);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBlockManagerWalletStateInit(TvmCell code, address root, uint256 pubkey) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBlockManagerWalletCode(code, root),
            contr: AckiNackiBlockManagerNodeWallet,
            varInit: {_owner_pubkey: pubkey}
        });
    }

    function buildBlockManagerWalletCode(
        TvmCell originalCode,
        address root
    ) public returns (TvmCell) {
        TvmCell finalcell;
        finalcell = abi.encode(versionLib, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateBlockKeeperPreEpochAddress(TvmCell code, TvmCell walletCode, address root, uint256 pubkey, uint64 seqNoStart) public returns(address) {
        TvmCell s1 = composeBlockKeeperPreEpochStateInit(code, walletCode, root, pubkey, seqNoStart);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBlockKeeperPreEpochStateInit(TvmCell code, TvmCell walletCode, address root, uint256 pubkey, uint64 seqNoStart) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBlockKeeperPreEpochCode(code, walletCode, root),
            contr: BlockKeeperPreEpoch,
            varInit: { _owner_pubkey : pubkey, _seqNoStart : seqNoStart }
        });
    }

    function buildBlockKeeperPreEpochCode(
        TvmCell originalCode,
        TvmCell walletCode,
        address root
    ) public returns (TvmCell) {
        TvmBuilder b;
        b.store(walletCode);
        uint256 hash1 = tvm.hash(b.toCell());
        delete b;
        TvmCell finalcell;
        finalcell = abi.encode(versionLib, hash1, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateBlockKeeperEpochAddress(TvmCell code, TvmCell walletCode, TvmCell preEpochCode, address root, uint256 pubkey, uint64 seqNoStart) public returns(address) {
        TvmCell s1 = composeBlockKeeperEpochStateInit(code, walletCode, preEpochCode, root, pubkey, seqNoStart);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBlockKeeperEpochStateInit(TvmCell code, TvmCell walletCode, TvmCell preEpochCode, address root, uint256 pubkey, uint64 seqNoStart) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBlockKeeperEpochCode(code, walletCode, preEpochCode, root),
            contr: BlockKeeperEpoch,
            varInit: { _owner_pubkey : pubkey, _seqNoStart : seqNoStart }
        });
    }

    function buildBlockKeeperEpochCode(
        TvmCell originalCode,
        TvmCell walletCode,
        TvmCell preEpochCode,
        address root
    ) public returns (TvmCell) {
        TvmBuilder b;
        b.store(walletCode);
        uint256 hash1 = tvm.hash(b.toCell());
        delete b;
        b.store(preEpochCode);
        uint256 hash2 = tvm.hash(b.toCell());
        delete b;
        TvmCell finalcell;
        finalcell = abi.encode(versionLib, hash1, hash2, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateBlockKeeperCoolerEpochAddress(TvmCell code, TvmCell walletCode, TvmCell preEpochCode, TvmCell codeEpoch, address root, uint256 pubkey, uint64 seqNoStart) public returns(address) {
        TvmCell s1 = composeBlockKeeperCoolerEpochStateInit(code, walletCode, preEpochCode, codeEpoch, root, pubkey, seqNoStart);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBlockKeeperCoolerEpochStateInit(TvmCell code, TvmCell walletCode, TvmCell preEpochCode, TvmCell codeEpoch, address root, uint256 pubkey, uint64 seqNoStart) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBlockKeeperCoolerEpochCode(code, calculateBlockKeeperEpochAddress(codeEpoch, walletCode, preEpochCode, root, pubkey, seqNoStart)),
            contr: BlockKeeperCooler,
            varInit: {_owner_pubkey: pubkey, _seqNoStart: seqNoStart}
        });
    }

    function buildBlockKeeperCoolerEpochCode(
        TvmCell originalCode,
        address root
    ) public returns (TvmCell) {
        TvmCell finalcell;
        finalcell = abi.encode(versionLib, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateBlockKeeperEpochProxyListAddress(TvmCell code, TvmCell walletCode, TvmCell epochCode, TvmCell preepochCode, uint256 pubkey, address root) public returns(address) {
        TvmCell s1 = composeBlockKeeperEpochProxyListStateInit(code, walletCode, epochCode, preepochCode, pubkey, root);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBlockKeeperEpochProxyListStateInit(TvmCell code, TvmCell walletCode, TvmCell epochCode, TvmCell preepochCode, uint256 pubkey, address root) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBlockKeeperEpochProxyListCode(code, walletCode, epochCode, preepochCode, root),
            contr: BlockKeeperEpochProxyList,
            varInit: { _owner_pubkey: pubkey }
        });
    }

    function buildBlockKeeperEpochProxyListCode(
        TvmCell originalCode,
        TvmCell walletCode,
        TvmCell epochCode,
        TvmCell preepochCode,
        address root
    ) public returns (TvmCell) {
        TvmBuilder b;
        b.store(walletCode);
        uint256 hash1 = tvm.hash(b.toCell());
        delete b;
        b.store(epochCode);
        uint256 hash2 = tvm.hash(b.toCell());
        delete b;
        b.store(preepochCode);
        uint256 hash3 = tvm.hash(b.toCell());
        delete b;
        TvmCell finalcell;
        finalcell = abi.encode(versionLib, hash1, hash2, hash3, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateBLSKeyAddress(TvmCell code, bytes bls_key, address root) public returns(address) {
        TvmCell s1 = composeBLSKeyStateInit(code, bls_key, root);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBLSKeyStateInit(TvmCell code, bytes bls_key, address root) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBLSKeyCode(code),
            contr: BLSKeyIndex,
            varInit: {_bls: bls_key, _root: root}
        });
    }

    function buildBLSKeyCode(
        TvmCell originalCode
    ) public returns (TvmCell) {
        return originalCode;
    }

    function calculateLicenseAddress(TvmCell code, uint256 license_number, address root) public returns(address) {
        TvmCell s1 = composeLicenseStateInit(code, license_number, root);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeLicenseStateInit(TvmCell code, uint256 license_number, address root) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildLicenseCode(code),
            contr: LicenseContract,
            varInit: {_license_number: license_number, _root: root}
        });
    }

    function buildLicenseCode(
        TvmCell originalCode
    ) public returns (TvmCell) {
        return originalCode;
    }

    function calculateLicenseBMAddress(TvmCell code, uint256 license_number, address root) public returns(address) {
        TvmCell s1 = composeLicenseBMStateInit(code, license_number, root);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeLicenseBMStateInit(TvmCell code, uint256 license_number, address root) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildLicenseBMCode(code),
            contr: LicenseBMContract,
            varInit: {_license_number: license_number, _root: root}
        });
    }

    function buildLicenseBMCode(
        TvmCell originalCode
    ) public returns (TvmCell) {
        return originalCode;
    }

    function calculateSignerIndexAddress(TvmCell code, uint16 index, address root) public returns(address) {
        TvmCell s1 = composeSignerIndexStateInit(code, index, root);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeSignerIndexStateInit(TvmCell code, uint16 index, address root) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildSignerIndexCode(code),
            contr: SignerIndex,
            varInit: {_signerIndex: index, _root: root}
        });
    }

    function buildSignerIndexCode(
        TvmCell originalCode
    ) public returns (TvmCell) {
        return originalCode;
    }
}
