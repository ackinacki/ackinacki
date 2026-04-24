pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/AccumulatorLib.sol";
import "./ShellSellOrderLot.sol";

/// @title Shell Accumulator Root USDC
/// @notice Central accumulator for Shell/USDC exchange with FIFO order matching.
///         Sellers deposit ECC[2] Shell and receive ECC[3] USDC when their order is matched.
///         Buyers deposit ECC[3] USDC and receive ECC[2] Shell immediately.
///         Denominations: 1, 10, 100, 1000 USDC. Rate: 100 Shell = 1 USDC.
contract ShellAccumulatorRootUSDC is AccumulatorModifiers {
    string constant version = "1.0.2";

    event SellOrderCreated(address seller, uint16 denom, uint64 orderId, uint128 shellAmount);
    event ShellPurchased(address buyer, uint128 usdcAmount, uint128 shellFromSellers, uint128 shellMinted);
    event MatchedOrders(
        uint64 lastSold1, uint64 lastSold10,
        uint64 lastSold100, uint64 lastSold1000
    );
    event UsdcClaimed(uint64 orderId, uint16 denom, address seller, uint128 payout);
    event NacklRedeemed(address recipient, uint128 burnAmount, uint128 payout);

    uint256 _ownerPubkey;

    TvmCell _sellOrderCode;

    // FIFO queue state: Denom 1
    uint64 _nextId1;
    uint64 _available1;
    uint64 _soldPrefix1;
    uint64 _owedCount1;

    // FIFO queue state: Denom 10
    uint64 _nextId10;
    uint64 _available10;
    uint64 _soldPrefix10;
    uint64 _owedCount10;

    // FIFO queue state: Denom 100
    uint64 _nextId100;
    uint64 _available100;
    uint64 _soldPrefix100;
    uint64 _owedCount100;

    // FIFO queue state: Denom 1000
    uint64 _nextId1000;
    uint64 _available1000;
    uint64 _soldPrefix1000;
    uint64 _owedCount1000;

    // ECC Shell tokens held from seller deposits (ECC[SHELL_ECC_ID])
    uint128 _sellerShellPool;
    // USDC ECC balance (accounting)
    uint128 _usdcBalance;
    // Total NACKL ECC burned via redeemNACKL
    uint128 _nacklBurned;
    // NACKL emission start timestamp (set in constructor)
    uint32 _unixstart;

    /// @notice Contract constructor. Initializes the accumulator with sell order code,
    ///         owner public key, and NACKL emission start timestamp.
    /// @param sellOrderCode — compiled TvmCell code of the ShellSellOrderLot contract
    /// @param pubkey — owner public key for admin operations (setPubkey, updateCode)
    /// @param unixstart — NACKL emission start timestamp (network start)
    constructor(
        TvmCell sellOrderCode,
        uint256 pubkey,
        uint32 unixstart
    ) accept {
        _sellOrderCode = sellOrderCode;
        _ownerPubkey = pubkey;
        _unixstart = unixstart;

        _nextId1 = 1; _available1 = 0; _soldPrefix1 = 0; _owedCount1 = 0;
        _nextId10 = 1; _available10 = 0; _soldPrefix10 = 0; _owedCount10 = 0;
        _nextId100 = 1; _available100 = 0; _soldPrefix100 = 0; _owedCount100 = 0;
        _nextId1000 = 1; _available1000 = 0; _soldPrefix1000 = 0; _owedCount1000 = 0;

        _sellerShellPool = 0;
        _usdcBalance = 0;
    }

    /// @notice Ensures contract balance stays above MIN_BALANCE by minting vmshell if needed.
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    // ========================================================
    // Incoming ECC deposits: USDC (ECC[3]) or Shell (ECC[2])
    // ========================================================

    /// @notice Fallback receiver for incoming ECC currency transfers.
    ///         Routes ECC[3] USDC deposits to _processUsdcDeposit (buy flow),
    ///         ECC[2] Shell deposits to _processShellDeposit (sell flow),
    ///         and ECC[1] NACKL deposits to _redeemNACKL (redeem flow).
    ///         Messages without recognized ECC currencies are silently ignored.
    receive() external {
        ensureBalance();
        mapping(uint32 => varuint32) currencies = msg.currencies;
        require(currencies.keys().length <= 1, ERR_MULTIPLE_CURRENCIES);
        if (currencies.exists(USDC_ECC_ID)) {
            tvm.accept();
            uint128 usdcAmount = uint128(currencies[USDC_ECC_ID]);
            if (usdcAmount == 0) { return; }
            _processUsdcDeposit(msg.sender, usdcAmount);
        } else if (currencies.exists(SHELL_ECC_ID)) {
            tvm.accept();
            uint128 shellAmount = uint128(currencies[SHELL_ECC_ID]);
            if (shellAmount == 0) { return; }
            _processShellDeposit(msg.sender, shellAmount);
        } else if (currencies.exists(NACKL_ECC_ID)) {
            tvm.accept();
            uint128 nacklAmount = uint128(currencies[NACKL_ECC_ID]);
            if (nacklAmount == 0) { return; }
            _redeemNACKL(msg.sender, nacklAmount);
        }
    }

    // ========================================================
    // Buy for: called by Exchange (or anyone with real USDC ECC attached)
    // ========================================================

    /// @notice Accepts ECC[3] USDC attached to the message and processes a Shell purchase
    ///         on behalf of the specified buyer. The buyer receives ECC[2] Shell.
    /// @param buyer — address to receive the purchased Shell
    function buyShellFor(address buyer) public {
        ensureBalance();
        mapping(uint32 => varuint32) currencies = msg.currencies;
        require(currencies.exists(USDC_ECC_ID), ERR_ZERO_AMOUNT);
        uint128 usdcAmount = uint128(currencies[USDC_ECC_ID]);
        require(usdcAmount > 0, ERR_ZERO_AMOUNT);
        tvm.accept();
        _processUsdcDeposit(buyer, usdcAmount);
    }

    // ========================================================
    // Sell: ECC Shell deposit -> create SellOrderLot
    // ========================================================

    /// @notice Processes an incoming ECC[2] Shell deposit from a seller.
    ///         Validates the amount corresponds to a valid denomination (1, 10, 100, or 1000 USDC),
    ///         assigns a FIFO order ID, deploys a ShellSellOrderLot contract, and updates the queue.
    /// @param seller — address of the seller who sent ECC[2] Shell
    /// @param shellAmount — amount of ECC[2] Shell deposited (must be D * SHELL_PER_USDC)
    function _processShellDeposit(address seller, uint128 shellAmount) private {
        require(shellAmount % SHELL_PER_USDC == 0, ERR_WRONG_SHELL_AMOUNT);
        uint128 D128 = shellAmount / SHELL_PER_USDC;
        require(D128 <= uint128(DENOM_1000), ERR_INVALID_DENOM);
        uint16 D = uint16(D128);
        require(_isValidDenom(D), ERR_INVALID_DENOM);

        uint64 orderId;
        if (D == DENOM_1) {
            orderId = _nextId1; _nextId1 += 1; _available1 += 1;
        } else if (D == DENOM_10) {
            orderId = _nextId10; _nextId10 += 1; _available10 += 1;
        } else if (D == DENOM_100) {
            orderId = _nextId100; _nextId100 += 1; _available100 += 1;
        } else {
            orderId = _nextId1000; _nextId1000 += 1; _available1000 += 1;
        }

        _sellerShellPool += shellAmount;

        TvmCell stateInit = AccumulatorLib.composeSellOrderStateInit(_sellOrderCode, address(this), D, orderId);
        new ShellSellOrderLot{
            stateInit: stateInit,
            value: SELL_ORDER_DEPLOY_VALUE,
            flag: 1
        }(seller);

        address sellerExtern = address.makeAddrExtern(uint256(seller.value), bitCntAddress);
        emit SellOrderCreated{dest: sellerExtern}(seller, D, orderId, shellAmount);

        address addrExtern = address.makeAddrExtern(SellOrderCreatedEmit, bitCntAddress);
        emit SellOrderCreated{dest: addrExtern}(seller, D, orderId, shellAmount);
    }

    // ========================================================
    // Buy: USDC ECC deposit -> FIFO matching -> send ECC Shell directly
    // ========================================================

    /// @notice Processes an incoming ECC[3] USDC deposit from a buyer.
    ///         Matches against available sell orders in FIFO order (largest denomination first:
    ///         1000 → 100 → 10 → 1). If no sellers are available for the remaining amount,
    ///         mints fresh ECC[2] Shell. Sends the total Shell to the buyer in a single transfer.
    /// @param buyer — address of the buyer who sent ECC[3] USDC
    /// @param usdcAmount — amount of ECC[3] USDC deposited (in micro-USDC, must be whole USDC units)
    function _processUsdcDeposit(address buyer, uint128 usdcAmount) private {
        require(usdcAmount > 0, ERR_ZERO_AMOUNT);
        require(usdcAmount % USDC_DECIMALS_FACTOR == 0, ERR_NOT_WHOLE_USDC);

        uint128 N = usdcAmount / USDC_DECIMALS_FACTOR;
        uint128 remaining = N;

        _usdcBalance += usdcAmount;

        uint128 totalShellFromSellers = 0;

        // 1000
        if (remaining > 0 && _available1000 > 0) {
            uint64 take = uint64(math.min(uint128(_available1000), remaining / 1000));
            if (take > 0) {
                totalShellFromSellers += uint128(take) * 1000 * SHELL_PER_USDC;
                _available1000 -= take;
                _soldPrefix1000 += take;
                _owedCount1000 += take;
                remaining -= uint128(take) * 1000;
            }
        }

        // 100
        if (remaining > 0 && _available100 > 0) {
            uint64 take = uint64(math.min(uint128(_available100), remaining / 100));
            if (take > 0) {
                totalShellFromSellers += uint128(take) * 100 * SHELL_PER_USDC;
                _available100 -= take;
                _soldPrefix100 += take;
                _owedCount100 += take;
                remaining -= uint128(take) * 100;
            }
        }

        // 10
        if (remaining > 0 && _available10 > 0) {
            uint64 take = uint64(math.min(uint128(_available10), remaining / 10));
            if (take > 0) {
                totalShellFromSellers += uint128(take) * 10 * SHELL_PER_USDC;
                _available10 -= take;
                _soldPrefix10 += take;
                _owedCount10 += take;
                remaining -= uint128(take) * 10;
            }
        }

        // 1
        if (remaining > 0 && _available1 > 0) {
            uint64 take = uint64(math.min(uint128(_available1), remaining));
            if (take > 0) {
                totalShellFromSellers += uint128(take) * SHELL_PER_USDC;
                _available1 -= take;
                _soldPrefix1 += take;
                _owedCount1 += take;
                remaining -= uint128(take);
            }
        }

        uint128 mintedShell = 0;
        if (remaining > 0) {
            mintedShell = remaining * SHELL_PER_USDC;
            require(mintedShell <= uint128(type(uint64).max), ERR_OVERFLOW);
            gosh.mintecc(uint64(mintedShell), SHELL_ECC_ID);
        }

        uint128 totalShellToBuyer = totalShellFromSellers + mintedShell;
        if (totalShellToBuyer > 0) {
            if (totalShellFromSellers > 0) {
                _sellerShellPool -= totalShellFromSellers;
            }
            mapping(uint32 => varuint32) ecc;
            ecc[SHELL_ECC_ID] = varuint32(totalShellToBuyer);
            buyer.transfer({value: 1 vmshell, bounce: false, flag: 1, currencies: ecc});
        }

        address addrExtern = address.makeAddrExtern(ShellPurchasedEmit, bitCntAddress);
        emit ShellPurchased{dest: addrExtern}(buyer, usdcAmount, totalShellFromSellers, mintedShell);

        address matchedExtern = address.makeAddrExtern(MatchedOrdersEmit, bitCntAddress);
        emit MatchedOrders{dest: matchedExtern}(
            _soldPrefix1, _soldPrefix10,
            _soldPrefix100, _soldPrefix1000
        );
    }

    // ========================================================
    // ClaimUSDC: called by SellOrderLot
    // ========================================================

    /// @notice Called by a ShellSellOrderLot contract to claim USDC payout for a sold order.
    ///         Verifies the caller is the expected SellOrderLot address (derived from code, denom, orderId),
    ///         checks the order has been sold (orderId <= soldPrefix), and sends ECC[3] USDC to the seller.
    ///         Then notifies the SellOrderLot via onReceiveUSDC (which triggers selfdestruct).
    /// @param D — denomination of the sell order (1, 10, 100, or 1000)
    /// @param orderId — FIFO order ID within the denomination queue
    /// @param seller — address to receive the ECC[3] USDC payout
    function claimUSDC(uint16 D, uint64 orderId, address seller) public {
        ensureBalance();
        require(_isValidDenom(D), ERR_INVALID_DENOM);

        address expectedAddr = AccumulatorLib.calculateSellOrderAddress(_sellOrderCode, address(this), D, orderId);
        require(msg.sender == expectedAddr, ERR_WRONG_ADDRESS);

        tvm.accept();

        uint64 soldPrefix;
        uint64 owedCount;
        if (D == DENOM_1) {
            soldPrefix = _soldPrefix1; owedCount = _owedCount1;
        } else if (D == DENOM_10) {
            soldPrefix = _soldPrefix10; owedCount = _owedCount10;
        } else if (D == DENOM_100) {
            soldPrefix = _soldPrefix100; owedCount = _owedCount100;
        } else {
            soldPrefix = _soldPrefix1000; owedCount = _owedCount1000;
        }

        require(orderId <= soldPrefix, ERR_ORDER_NOT_SOLD);
        require(owedCount > 0, ERR_NO_OWED);
        require(_usdcBalance >= _owedUsdcTotal(), ERR_WRONG_USDC_AMOUNT);

        uint128 payout = uint128(D) * USDC_DECIMALS_FACTOR;

        if (D == DENOM_1) { _owedCount1 -= 1; }
        else if (D == DENOM_10) { _owedCount10 -= 1; }
        else if (D == DENOM_100) { _owedCount100 -= 1; }
        else { _owedCount1000 -= 1; }

        _usdcBalance -= payout;

        // Direct ECC[3] USDC payout to seller
        mapping(uint32 => varuint32) ecc;
        ecc[USDC_ECC_ID] = varuint32(payout);
        seller.transfer({value: 1 vmshell, bounce: false, flag: 1, currencies: ecc});

        ShellSellOrderLot(msg.sender).onReceiveUSDC{value: 0.5 vmshell, flag: 1}(payout);

        address addrExtern = address.makeAddrExtern(UsdcClaimedEmit, bitCntAddress);
        emit UsdcClaimed{dest: addrExtern}(orderId, D, seller, payout);
    }

    // ========================================================
    // RedeemNACKL
    // ========================================================

    /// @notice Burns ECC[1] NACKL tokens sent with this message and pays out a proportional
    ///         share of the redeemable USDC pool as ECC[3]. The payout formula:
    ///         payout = redeemable * burnAmount / currentSupply,
    ///         where redeemable = _usdcBalance - owedTotal, and currentSupply = M(t) - _nacklBurned.
    ///         M(t) follows the emission curve: M(t) = T_KM * (1 - exp(-u_M * t)), capped at T.
    /// @param recipient — address to receive the ECC[3] USDC payout (msg.sender from receive())
    /// @param burnAmount — amount of ECC[1] NACKL to burn (extracted in receive())
    function _redeemNACKL(address recipient, uint128 burnAmount) private {
        require(burnAmount > 0, ERR_ZERO_AMOUNT);
        require(burnAmount <= uint128(type(uint64).max), ERR_OVERFLOW);

        gosh.burnecc(uint64(burnAmount), NACKL_ECC_ID);

        uint128 supply = _nacklSupply();
        require(supply > _nacklBurned, ERR_INSUFFICIENT_REDEEMABLE);
        uint128 currentSupply = supply - _nacklBurned;
        require(currentSupply >= burnAmount, ERR_ZERO_AMOUNT);

        uint128 owed = _owedUsdcTotal();
        require(_usdcBalance >= owed, ERR_WRONG_USDC_AMOUNT);
        uint128 redeemable = _usdcBalance - owed;
        require(redeemable > 0, ERR_INSUFFICIENT_REDEEMABLE);

        uint128 payout = math.muldiv(redeemable, burnAmount, currentSupply);
        _nacklBurned += burnAmount;
        require(_nacklBurned <= supply, ERR_INSUFFICIENT_REDEEMABLE);
        require(payout > 0, ERR_ZERO_AMOUNT);

        _usdcBalance -= payout;

        // Direct ECC[3] USDC payout to recipient
        mapping(uint32 => varuint32) ecc;
        ecc[USDC_ECC_ID] = varuint32(payout);
        recipient.transfer({value: 1 vmshell, bounce: false, flag: 1, currencies: ecc});

        address addrExtern = address.makeAddrExtern(NacklRedeemedEmit, bitCntAddress);
        emit NacklRedeemed{dest: addrExtern}(recipient, burnAmount, payout);
    }

    // ========================================================
    // Admin
    // ========================================================

    /// @notice Replaces the owner public key. Only callable by the current owner.
    /// @param pubkey — new owner public key (uint256)
    function setPubkey(uint256 pubkey) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _ownerPubkey = pubkey;
    }

    // ========================================================
    // Internal
    // ========================================================

    /// @notice Computes exp(-x) in FP18 fixed-point arithmetic.
    ///         Uses 12-term Taylor series for the fractional part
    ///         and repeated multiplication by exp(-1) for the integer part.
    /// @param x — input value in FP18 (scaled by 10^18)
    /// @return exp(-x) in FP18; returns 0 for x > 20*FP18
    function _expNegFP18(uint256 x) private pure returns (uint256) {
        if (x == 0) return FP18;
        if (x > 20 * FP18) return 0;

        uint256 n = x / FP18;
        uint256 r = x % FP18;

        // Taylor: exp(-r) = sum_{k=0..11} (-r)^k / k!
        uint256 result = FP18;
        uint256 term = r;
        result -= term;                        // k=1
        term = term * r / FP18 / 2;
        result += term;                        // k=2
        term = term * r / FP18 / 3;
        result -= term;                        // k=3
        term = term * r / FP18 / 4;
        result += term;                        // k=4
        term = term * r / FP18 / 5;
        result -= term;                        // k=5
        term = term * r / FP18 / 6;
        result += term;                        // k=6
        term = term * r / FP18 / 7;
        result -= term;                        // k=7
        term = term * r / FP18 / 8;
        result += term;                        // k=8
        term = term * r / FP18 / 9;
        result -= term;                        // k=9
        term = term * r / FP18 / 10;
        result += term;                        // k=10
        term = term * r / FP18 / 11;
        result -= term;                        // k=11

        // Multiply by exp(-1) for each integer unit
        for (uint256 i = 0; i < n; i++) {
            result = result * INV_E_FP18 / FP18;
        }

        return result;
    }

    /// @notice Computes the current NACKL emission supply using the formula:
    ///         M(t) = T_KM * (1 - exp(-u_M * t)), capped at NACKL_T.
    ///         t = block.timestamp - _unixstart (seconds since emission start).
    /// @return Current NACKL supply in micro-NACKL
    function _nacklSupply() private view returns (uint128) {
        if (_unixstart == 0 || block.timestamp <= _unixstart) return 0;

        uint256 t = uint256(block.timestamp - _unixstart);
        uint256 x = NACKL_U_M_FP18 * t;
        uint256 oneMinusExp = FP18 - _expNegFP18(x);
        uint128 m = uint128(uint256(NACKL_T_KM) * oneMinusExp / FP18);

        return m > NACKL_T ? NACKL_T : m;
    }

    /// @notice Computes the total USDC owed to sellers across all denominations.
    /// @return Total owed amount in micro-USDC
    function _owedUsdcTotal() private view returns (uint128) {
        return (uint128(_owedCount1) * 1 +
                uint128(_owedCount10) * 10 +
                uint128(_owedCount100) * 100 +
                uint128(_owedCount1000) * 1000) * USDC_DECIMALS_FACTOR;
    }

    // ========================================================
    // Getters
    // ========================================================

    /// @notice Returns the FIFO queue state for a given denomination.
    /// @param D — denomination (1, 10, 100, or 1000)
    /// @return nextId — next order ID to be assigned
    /// @return available — number of unsold orders in the queue
    /// @return soldPrefix — number of orders sold (FIFO prefix)
    /// @return owedCount — number of sold orders still awaiting USDC claim
    function getQueueState(uint16 D) external view returns (
        uint64 nextId,
        uint64 available,
        uint64 soldPrefix,
        uint64 owedCount
    ) {
        require(_isValidDenom(D), ERR_INVALID_DENOM);
        if (D == DENOM_1) {
            return (_nextId1, _available1, _soldPrefix1, _owedCount1);
        } else if (D == DENOM_10) {
            return (_nextId10, _available10, _soldPrefix10, _owedCount10);
        } else if (D == DENOM_100) {
            return (_nextId100, _available100, _soldPrefix100, _owedCount100);
        } else {
            return (_nextId1000, _available1000, _soldPrefix1000, _owedCount1000);
        }
    }

    /// @notice Returns high-level accumulator state.
    /// @return ownerPubkey — current owner public key
    /// @return sellerShellPool — total ECC[2] Shell held from seller deposits
    /// @return usdcBalance — total ECC[3] USDC balance (accounting)
    /// @return owedTotal — total USDC owed to sellers awaiting claim
    function getDetails() external view returns (
        uint256 ownerPubkey,
        uint128 sellerShellPool,
        uint128 usdcBalance,
        uint128 owedTotal
    ) {
        return (_ownerPubkey, _sellerShellPool, _usdcBalance, _owedUsdcTotal());
    }

    /// @notice Computes the deterministic address of a ShellSellOrderLot contract.
    /// @param D — denomination (1, 10, 100, or 1000)
    /// @param orderId — FIFO order ID within the denomination queue
    /// @return sellOrderAddr — computed contract address
    function getSellOrderAddress(uint16 D, uint64 orderId) external view returns (address sellOrderAddr) {
        sellOrderAddr = AccumulatorLib.calculateSellOrderAddress(_sellOrderCode, address(this), D, orderId);
    }

    /// @notice Returns total USDC owed to sellers across all denominations (micro-USDC).
    function owedUsdcTotal() external view returns (uint128) {
        return _owedUsdcTotal();
    }

    /// @notice Returns total ECC[2] Shell held in the seller pool.
    function getSellerShellPool() external view returns (uint128) {
        return _sellerShellPool;
    }

    /// @notice Returns the USDC balance tracked by the accumulator (micro-USDC).
    function getUsdcBalance() external view returns (uint128) {
        return _usdcBalance;
    }

    /// @notice Returns NACKL emission info.
    /// @return supply — current NACKL supply from emission curve M(t)
    /// @return burned — total NACKL burned via redeemNACKL
    /// @return unixstart — emission start timestamp (seconds)
    function getNacklInfo() external view returns (uint128 supply, uint128 burned, uint32 unixstart) {
        return (_nacklSupply(), _nacklBurned, _unixstart);
    }

    /// @notice Returns contract version and name.
    function getVersion() external pure returns (string, string) {
        return (version, "ShellAccumulatorRootUSDC");
    }

    // ========================================================
    // On-chain code upgrade (owner only).
    // After setCurrentCode, onCodeUpgrade runs from the NEW code.
    // ========================================================

    /// @notice Upgrades the contract code on-chain. Only callable by the owner.
    ///         After tvm.setCurrentCode, onCodeUpgrade runs in the context of the new code.
    /// @param newcode — new contract code TvmCell
    /// @param cell — ABI-encoded initialization data: (TvmCell sellOrderCode, uint256 pubkey)
    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        tvm.commit();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    /// @notice Initializes state after code upgrade.
    ///         Called by UpdateZeroContract (zerostate) and updateCode().
    /// @param cell — ignored (kept for interface compatibility)
    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        cell;
    }
}
