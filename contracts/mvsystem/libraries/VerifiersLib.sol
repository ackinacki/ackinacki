// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;

import "../PopitGame.sol";
import "../PopCoinRoot.sol";
import "../Game.sol";
import "../Mvmultifactor.sol";
import "../Indexer.sol";
import "../Boost.sol";

library VerifiersLib {
    string constant versionLib = "1.0.0";

    function calculatePopitGameAddress(TvmCell code, address root, address owner) public returns(address) {
        TvmCell s1 = composePopitGameStateInit(code, root, owner);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composePopitGameStateInit(TvmCell code, address root, address owner) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildPopitGameCode(code, root),
            contr: PopitGame,
            varInit: {_owner: owner}
        });
    }

    function buildPopitGameCode(
        TvmCell originalCode,
        address root
    ) public returns (TvmCell) {
        TvmCell finalcell;
        finalcell = abi.encode(versionLib, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculatePopCoinWalletAddress(TvmCell code, uint256 PopitGamehash, address root, string name, address owner) public returns(address) {
        TvmCell s1 = composePopCoinWalletStateInit(code, PopitGamehash, root, name, owner);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composePopCoinWalletStateInit(TvmCell code, uint256 PopitGamehash, address root, string name, address owner) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildPopCoinWalletCode(code, root, PopitGamehash),
            contr: PopCoinWallet,
            varInit: {_name: name, _owner: owner}
        });
    }

    function buildPopCoinWalletCode(
        TvmCell originalCode,
        address root,
        uint256 PopitGamehash
    ) public returns (TvmCell) {
        TvmCell finalcell; 
        finalcell = abi.encode(versionLib, root, PopitGamehash);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateGameAddress(TvmCell code, address root, address owner, address popcoinroot, address popcoinwallet) public returns(address) {
        TvmCell s1 = composeGameStateInit(code, root, owner, popcoinroot, popcoinwallet);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeGameStateInit(TvmCell code, address root, address owner, address popcoinroot, address popcoinwallet) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildGameCode(code, root),
            contr: Game,
            varInit: {_owner: owner, _popcoinroot_address: popcoinroot, _popCoinWallet: popcoinwallet}
        });
    }

    function buildGameCode(
        TvmCell originalCode,
        address root
    ) public returns (TvmCell) {
        TvmCell finalcell; 
        finalcell = abi.encode(versionLib, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculatePopCoinRootAddress(TvmCell code, address root, string name) public returns(address) {
        TvmCell s1 = composePopCoinRootStateInit(code, root, name);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composePopCoinRootStateInit(TvmCell code, address root, string name) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildPopCoinRootCode(code, root),
            contr: PopCoinRoot,
            varInit: {_name: name}
        });
    }

    function buildPopCoinRootCode(
        TvmCell originalCode,
        address root
    ) public returns (TvmCell) {
        TvmCell finalcell; 
        finalcell = abi.encode(versionLib, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateMultifactorAddress(TvmCell code, uint256 pubkey, address root) public returns(address) {
        TvmCell s1 = composeMultifactorStateInit(code, pubkey, root);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeMultifactorStateInit(TvmCell code, uint256 pubkey, address root) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildMultifactorCode(code, root),
            contr: Multifactor,
            varInit: {_owner_pubkey: pubkey}
        });
    }

    function buildMultifactorCode(
        TvmCell originalCode,
        address root
    ) public returns (TvmCell) {
        TvmCell finalcell; 
        finalcell = abi.encode(root);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateIndexerAddress(TvmCell code, string name) public returns(address) {
        TvmCell s1 = composeIndexerStateInit(code, name);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeIndexerStateInit(TvmCell code, string name) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildIndexerCode(code),
            contr: NameIndex,
            varInit: {_name: name}
        });
    }

    function buildIndexerCode(
        TvmCell originalCode
    ) public returns (TvmCell) {
        TvmCell finalcell; 
        finalcell = abi.encode(versionLib);
        return abi.setCodeSalt(originalCode, finalcell);
    }

    function calculateBoostAddress(TvmCell code, address popitgame, address root) public returns(address) {
        TvmCell s1 = composeBoostStateInit(code, popitgame, root);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeBoostStateInit(TvmCell code, address popitgame, address root) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildBoostCode(code, root),
            contr: Boost,
            varInit: {_popitGame: popitgame}
        });
    }

    function buildBoostCode(
        TvmCell originalCode,
        address root
    ) public returns (TvmCell) {
        TvmCell finalcell; 
        finalcell = abi.encode(versionLib, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }
}
