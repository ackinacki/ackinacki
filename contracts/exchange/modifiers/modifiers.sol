pragma gosh-solidity >=0.76.1;

import "./errors.sol";

abstract contract USDCBridgeModifiers is USDCBridgeErrors {
    uint64 constant MIN_BALANCE = 100 vmshell;

    uint32 constant USDC_ECC_ID = 3;
    uint128 constant USDC_DECIMALS_FACTOR = 1_000_000;

    address constant ACCUMULATOR_ADDRESS = address.makeAddrStd(0, 0x3535353535353535353535353535353535353535353535353535353535353535);
    address constant USDC_BRIDGE_ADDRESS = address.makeAddrStd(0, 0x1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a);

    // External address constants for directed events
    uint constant bitCntAddress = 256;
    uint128 constant UsdcMigratedEmit        = 615;
    uint128 constant UsdcMintedEmit          = 616;
    uint128 constant WithdrawalInitiatedEmit = 618;
    uint128 constant DepositFinalizedEmit    = 619;

    modifier accept() {
        tvm.accept();
        _;
    }

    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }
}
