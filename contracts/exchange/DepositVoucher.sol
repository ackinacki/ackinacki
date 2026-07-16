pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;

import "./modifiers/modifiers.sol";
import "./USDCBridge.sol";

/// @title DepositVoucher
/// @notice Deterministic one-shot marker contract for inbound bridge deposits
///         from any source chain. Its address is derived from `_depositHash`,
///         so re-deploying with the same hash collides with the existing
///         account and the second constructor call is a no-op — natural
///         replay protection.
contract DepositVoucher is USDCBridgeModifiers {
    string constant version = "1.0.0";

    /// @notice Hash of the proof-bound deposit identity (deposit_id,
    ///         contract_addr, dapp_id). Forms the deterministic address of this
    ///         voucher. amount/recipient are deliberately NOT hashed: they are
    ///         fixed by the proof, so keying the replay slot on the identity
    ///         alone makes one L1 deposit = one voucher = one mint, and a replay
    ///         can never re-route the payout (same identity ⇒ same voucher).
    uint256 static _depositHash;

    /// @notice Deploy callback: forwards confirmation to USDCBridge so it mints
    ///         USDC for the proof-bound AN recipient. Only callable by USDCBridge.
    /// @dev    The identity params (depositId, contractAddr, dappId) are bound to
    ///         `_depositHash`; amount + recipient (anAccount) ride
    ///         through to `confirmDeposit` for the payout.
    constructor(
        uint256 depositId,
        uint256 contractAddr,
        uint256 dappId,
        uint128 amount,
        uint256 anAccount
    ) {
        tvm.accept();
        require(msg.sender == USDC_BRIDGE_ADDRESS, ERR_INVALID_SENDER);
        require(
            tvm.hash(abi.encode(depositId, contractAddr, dappId)) == _depositHash,
            ERR_HASH_MISMATCH
        );
        USDCBridge(USDC_BRIDGE_ADDRESS).confirmDeposit{value: 0.5 vmshell, flag: 1}(
            depositId, contractAddr, dappId, amount, anAccount
        );
    }

    function getVersion() external pure returns (string, string) {
        return (version, "DepositVoucher");
    }
}
