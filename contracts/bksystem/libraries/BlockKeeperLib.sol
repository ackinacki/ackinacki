// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;

import "../AckiNackiBlockKeeperNodeWallet.sol";
import "../BlockKeeperEpochContract.sol";
import "../BlockKeeperPreEpochContract.sol";

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
            varInit: { _owner_pubkey : pubkey }
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

    function calculateBlockKeeperSlashAddress(TvmCell code, TvmCell walletCode, TvmCell preEpochCode, address root, uint256 pubkey, uint64 seqNoStart) public returns(address) {
        TvmCell s1 = composeBlockKeeperSlashStateInit(code, walletCode, preEpochCode, root, pubkey, seqNoStart);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBlockKeeperSlashStateInit(TvmCell code, TvmCell walletCode, TvmCell preEpochCode, address root, uint256 pubkey, uint64 seqNoStart) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBlockKeeperSlashCode(code, walletCode, preEpochCode, root),
            contr: BlockKeeperSlashContract,
            varInit: { _owner_pubkey : pubkey, _seqNoStart : seqNoStart }
        });
    }

    function buildBlockKeeperSlashCode(
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
}
