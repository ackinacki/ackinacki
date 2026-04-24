pragma gosh-solidity >=0.76.1;

import "../ShellSellOrderLot.sol";

library AccumulatorLib {
    string constant versionLib = "1.0.2";

    function calculateSellOrderAddress(TvmCell code, address root, uint16 denom, uint64 orderId) public returns (address) {
        TvmCell s1 = composeSellOrderStateInit(code, root, denom, orderId);
        return address.makeAddrStd(0, tvm.hash(s1));
    }

    function composeSellOrderStateInit(TvmCell code, address root, uint16 denom, uint64 orderId) public returns (TvmCell) {
        return abi.encodeStateInit({
            code: buildSellOrderCode(code, root),
            contr: ShellSellOrderLot,
            varInit: {
                _denom: denom,
                _orderId: orderId
            }
        });
    }

    function buildSellOrderCode(TvmCell originalCode, address root) public returns (TvmCell) {
        TvmCell finalcell = abi.encode(versionLib, root);
        return abi.setCodeSalt(originalCode, finalcell);
    }
}
