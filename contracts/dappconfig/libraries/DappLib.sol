// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;

import "../DappConfig.sol";

library DappLib {
    string constant versionLib = "1.0.0";

    function calculateDappConfigAddress(TvmCell code, uint256 dapp_id) public returns(address) {
        TvmCell s1 = composeDappConfigStateInit(code, dapp_id);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeDappConfigStateInit(TvmCell code, uint256 dapp_id) public returns(TvmCell) {
        return abi.encodeStateInit({
            code: buildDappConfigCode(code, dapp_id),
            contr: DappConfig,
            varInit: {}
        });
    }

    function buildDappConfigCode(
        TvmCell originalCode,
        uint256 dapp_id
    ) public returns (TvmCell) {
        TvmCell finalcell;
        finalcell = abi.encode(dapp_id);
        return abi.setCodeSalt(originalCode, finalcell);
    }
}
