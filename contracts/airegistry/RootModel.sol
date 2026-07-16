pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./interfaces.sol";
import "./TokenContract.sol";

/// @title RootModel
/// @notice Per-AI-model root. Stores the TokenContract code and registers
///         TokenContracts that anyone deploys with this RootModel as parent.
contract RootModel is AiRegistryModifiers {
    string constant version = "4.0.27";

    // Native value attached to THIS contract's cross-dapp message (registerRoot).
    // Tunable; recipients self-fund via `accept`, so it only covers a non-accepting hop.
    // (TC/RM-local — NOT the shared REGISTER_FORWARD_VALUE the IOB also uses.)
    varuint16 constant DAPP_MSG_VALUE = 0.01 vmshell;

    /// @notice Canonical code hash of `TokenContract`. The constructor
    ///         rejects any caller-supplied code whose `tvm.hash` does not
    ///         match this constant — this locks the registry to one
    ///         specific TokenContract bytecode. To bump versions, rebuild
    ///         TokenContract, recompute the hash below, recompile
    ///         RootModel, and redeploy.
    uint256 constant TOKEN_CONTRACT_CODE_HASH  = 0xa2c32147ed9bedec588e81ad2f55300e0640635428254b710964f38331c84f45;
    uint16  constant TOKEN_CONTRACT_CODE_DEPTH = 10;

    event ContractDeployed(address self);
    event TokenContractRegistered(address tokenContractAddress);

    // Static (part of stateInit, contribute to address derivation).
    // `_superRootAddress` binds this RootModel to a specific SuperRoot
    // instance — the SuperRoot at that address derives our expected
    // address from the same value, so a RootModel deployed pointing at a
    // bogus SuperRoot will not pass registration.
    uint256 static _ownerPubkey;
    address static _superRootAddress;

    // Note: we do NOT store the TokenContract code — only its pinned hash/depth
    // (constants above). The ctor still verifies the supplied code matches, then
    // discards it (hash, not code).
    constructor(TvmCell tokenContractCode) {
        tvm.accept();
        require(tvm.hash(tokenContractCode) == TOKEN_CONTRACT_CODE_HASH, ERR_BAD_CODE_HASH);
        require(tokenContractCode.depth() == TOKEN_CONTRACT_CODE_DEPTH, ERR_BAD_CODE_HASH);
        ensureBalance();

        address selfExtern = address.makeAddrExtern(ContractDeployedEmit, bitCntAddress);
        emit ContractDeployed{dest: selfExtern}(address(this));

        ISuperRootRegistry(_superRootAddress).registerRoot{value: DAPP_MSG_VALUE, flag: 1}(_ownerPubkey);
    }

    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    // ========================================================
    // Verifies sender == derived(tokenContractCode, varInit)
    // ========================================================

    function _calculateTokenContractAddress(uint256 sellerPubkey, uint64 nonce) private view returns (address) {
        // Hash-based: derive the TC address from its pinned (code hash, depth) +
        // the data cell, without storing the full code.
        TvmCell dummyCode;
        TvmCell s = abi.encodeStateInit({
            code: dummyCode,
            contr: TokenContract,
            pubkey: sellerPubkey,
            varInit: {
                _sellerPubkey: sellerPubkey,
                _rootModelAddress: address(this),
                _nonce: nonce
            }
        });
        TvmSlice sl = s.toSlice();
        sl.skip(5);
        sl.loadRef();                       // code (dummy)
        TvmCell dataCell = sl.loadRef();    // data
        return address.makeAddrStd(0, abi.stateInitHash(
            TOKEN_CONTRACT_CODE_HASH, tvm.hash(dataCell), TOKEN_CONTRACT_CODE_DEPTH, dataCell.depth()));
    }

    function registerTokenContract(uint256 sellerPubkey, uint64 nonce) public {
        ensureBalance();
        address expected = _calculateTokenContractAddress(sellerPubkey, nonce);
        require(msg.sender == expected, ERR_INVALID_SENDER);
        tvm.accept();

        address regExtern = address.makeAddrExtern(TokenContractRegisteredEmit, bitCntAddress);
        emit TokenContractRegistered{dest: regExtern}(expected);
    }

    // ========================================================
    // Getters
    // ========================================================

    function getTokenContractAddress(uint256 sellerPubkey, uint64 nonce) external view returns (address) {
        return _calculateTokenContractAddress(sellerPubkey, nonce);
    }

    function getOwnerPubkey() external view returns (uint256) { return _ownerPubkey; }

    function getVersion() external pure returns (string, string) {
        return (version, "RootModel");
    }
}
