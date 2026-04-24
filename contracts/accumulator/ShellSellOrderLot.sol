pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./ShellAccumulatorRootUSDC.sol";

/// @title Shell Sell Order Lot
/// @notice Represents a single sell order in the accumulator's FIFO queue.
///         Created by ShellAccumulatorRootUSDC when a seller deposits ECC[2] Shell.
///         The seller calls claim() to request USDC payout after their order is matched.
///         Self-destructs after receiving the USDC confirmation from the root.
contract ShellSellOrderLot is AccumulatorModifiers {
    string constant version = "1.0.2";

    event ClaimInitiated(uint64 orderId, uint16 denom, address owner);
    event OrderDestroyed(uint64 orderId, uint16 denom, uint128 amount);

    address _root;
    address _owner;
    uint16 static _denom;
    uint64 static _orderId;
    bool _claimed;

    /// @notice Contract constructor. Only callable by the root accumulator contract.
    ///         Validates code salt version and sender address.
    /// @param owner — address of the seller who deposited ECC[2] Shell
    constructor(address owner) accept {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(version == lib, ERR_INVALID_SENDER);
        require(msg.sender == root, ERR_INVALID_SENDER);
        _root = root;
        _owner = owner;
        _claimed = false;
    }

    /// @notice Ensures contract balance stays above MIN_BALANCE by minting vmshell if needed.
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Initiates USDC claim for this sell order. Only callable by the order owner.
    ///         Sets _claimed = true and calls claimUSDC on the root accumulator.
    ///         If the root rejects (order not yet sold), the bounced message resets _claimed.
    function claim() public {
        ensureBalance();
        require(!_claimed, ERR_ALREADY_CLAIMED);
        _claimed = true;
        tvm.accept();
        ShellAccumulatorRootUSDC(_root).claimUSDC{value: 1 vmshell, flag: 1}(_denom, _orderId, _owner);
        emit ClaimInitiated(_orderId, _denom, _owner);
    }

    /// @notice Bounce handler. Resets _claimed to false when claimUSDC bounces
    ///         (e.g. order not yet sold). Only accepts bounces from _root to prevent
    ///         a malicious actor from resetting _claimed via a crafted bounce.
    onBounce(TvmSlice /*body*/) external {
        if (msg.sender == _root) {
            tvm.accept();
            _claimed = false;
        }
    }

    /// @notice Callback from the root accumulator confirming USDC payout was sent to the seller.
    ///         Verifies the amount matches the expected payout (D * USDC_DECIMALS_FACTOR)
    ///         and self-destructs, returning remaining balance to the root.
    /// @param amount — USDC amount paid out (in micro-USDC), must equal _denom * USDC_DECIMALS_FACTOR
    function onReceiveUSDC(uint128 amount) public senderIs(_root) accept {
        require(amount == uint128(_denom) * USDC_DECIMALS_FACTOR, ERR_WRONG_USDC_AMOUNT);
        emit OrderDestroyed(_orderId, _denom, amount);
        selfdestruct(_root);
    }

    /// @notice Returns the sell order details.
    /// @return root — address of the parent accumulator contract
    /// @return owner — address of the seller
    /// @return denom — denomination (1, 10, 100, or 1000 USDC)
    /// @return orderId — FIFO order ID within the denomination queue
    /// @return claimed — whether claim() has been called (true if pending or completed)
    function getDetails() external view returns (
        address root,
        address owner,
        uint16 denom,
        uint64 orderId,
        bool claimed
    ) {
        return (_root, _owner, _denom, _orderId, _claimed);
    }

    /// @notice Returns contract version and name.
    function getVersion() external pure returns (string, string) {
        return (version, "ShellSellOrderLot");
    }
}
