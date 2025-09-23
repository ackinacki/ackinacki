/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
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
