/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

import "../PopitGame.sol";
import "../PopCoinRoot.sol";
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

    function log2(uint256 x) public returns (uint64 n) {
        if (x >= 2**128) { x >>= 128; n += 128; }
        if (x >= 2**64)  { x >>= 64;  n += 64;  }
        if (x >= 2**32)  { x >>= 32;  n += 32;  }
        if (x >= 2**16)  { x >>= 16;  n += 16;  }
        if (x >= 2**8)   { x >>= 8;   n += 8;   }
        if (x >= 2**4)   { x >>= 4;   n += 4;   }
        if (x >= 2**2)   { x >>= 2;   n += 2;   }
        if (x >= 2**1)   {             n += 1;   }
        return n;
    }

    function checkName(string name) public returns(bool) {
        bytes bStr = bytes(name);
        if (bStr.length == 0) { return false; }
        if (bStr.length > 39) { return false; }
        for (uint i = 0; i < bStr.length; i++) {
            if ((uint8(bStr[i]) >= 97) && (uint8(bStr[i]) <= 122)) { continue; }
            if ((uint8(bStr[i]) >= 48) && (uint8(bStr[i]) <= 57)) { continue; }
            if (i != 0) {
            	if ((uint8(bStr[i]) == 95) && (uint8(bStr[i - 1]) != 95)) { continue; }
            	if ((uint8(bStr[i]) == 45) && (uint8(bStr[i - 1]) != 45)) {  continue; }
            }
            return false;
        }
        return true;
    }
}
