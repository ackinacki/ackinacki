pragma gosh-solidity >=0.76.1;

import "./errors.sol";

abstract contract AccumulatorModifiers is AccumulatorErrors {
    uint64 constant MIN_BALANCE = 100 vmshell;

    uint128 constant SHELL_DECIMALS_FACTOR = 1_000_000_000;
    uint128 constant SHELL_PER_USDC = 100 * SHELL_DECIMALS_FACTOR;  // 100 Shell in nanoShell
    uint128 constant USDC_DECIMALS_FACTOR = 1_000_000;

    uint16 constant DENOM_1 = 1;
    uint16 constant DENOM_10 = 10;
    uint16 constant DENOM_100 = 100;
    uint16 constant DENOM_1000 = 1000;


    uint32 constant NACKL_ECC_ID = 1;
    uint32 constant SHELL_ECC_ID = 2;
    uint32 constant USDC_ECC_ID  = 3;
    // NACKL emission: M(t) = T_KM * (1 - exp(-u_M * t)), capped at T
    uint128 constant NACKL_T = 10_400_000_000_000_000_000;              // T = 1.04 * 10^10 nanoNACKL
    uint128 constant NACKL_T_KM = 10_400_104_000_000_000_000;          // T * (1 + K_M), K_M = 0.00001, nanoNACKL
    uint256 constant NACKL_U_M_FP18 = 5_756_467_732;                    // u_M * 10^18
    uint256 constant FP18 = 1_000_000_000_000_000_000;                 // 10^18
    uint256 constant INV_E_FP18 = 367_879_441_171_442_322;             // exp(-1) * 10^18

    varuint16 constant SELL_ORDER_DEPLOY_VALUE = 10 vmshell;

    // External address constants for directed events
    uint constant bitCntAddress = 256;
    uint128 constant SellOrderCreatedEmit   = 610;
    uint128 constant ShellPurchasedEmit     = 611;
    uint128 constant UsdcClaimedEmit        = 612;
    uint128 constant NacklRedeemedEmit      = 613;
    uint128 constant MatchedOrdersEmit      = 617;

    modifier accept() {
        tvm.accept();
        _;
    }

    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }

    modifier senderIs(address sender) {
        require(msg.sender == sender, ERR_INVALID_SENDER);
        _;
    }

    function _isValidDenom(uint16 D) internal pure returns (bool) {
        return (D == DENOM_1 || D == DENOM_10 || D == DENOM_100 || D == DENOM_1000);
    }
}
