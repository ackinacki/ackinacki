// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;

import "../AckiNackiBlockKeeperNodeWallet.sol";
import "../BlockKeeperEpochContract.sol";
import "../BlockKeeperPreEpochContract.sol";
import "../BlockKeeperEpochProxyList.sol";

library BlockKeeperLib {
    string constant versionLib = "1.0.0";

    function calculateBlockKeeperWalletAddress(TvmCell code, address root, uint256 pubkey) public returns(address) {
        TvmCell s1 = composeBlockKeeperWalletStateInit(code, root, pubkey);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBlockKeeperWalletStateInit(TvmCell code, address root, uint256 pubkey) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBlockKeeperWalletCode(code, root, pubkey),
            contr: AckiNackiBlockKeeperNodeWallet,
            varInit: {}
        });
    }

    function buildBlockKeeperWalletCode(
        TvmCell originalCode,
        address root, 
        uint256 pubkey
    ) public returns (TvmCell) {
        TvmCell finalcell;
        finalcell = abi.encode(versionLib, root, pubkey);
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
}
