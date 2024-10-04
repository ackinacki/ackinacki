// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;

import "../DappConfig.sol";

function exchange(uint64 stake) assembly pure {
    ".blob xC727"
}

function mint(uint64 stake, uint32 key) assembly pure {
    ".blob xC726"
}

function mintshell(uint64 value) assembly pure {
    ".blob xC728"
}

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
