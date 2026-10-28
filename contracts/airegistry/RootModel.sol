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
    string constant version = "4.0.36";

    /// @notice Canonical code hash of `TokenContract`. The constructor
    ///         rejects any caller-supplied code whose `tvm.hash` does not
    ///         match this constant — this locks the registry to one
    ///         specific TokenContract bytecode. To bump versions, rebuild
    ///         TokenContract, recompute the hash below, recompile
    ///         RootModel, and redeploy.
    uint256 constant TOKEN_CONTRACT_CODE_HASH  = 0xee4105b4800d852dde1a86cec4e270ecfa2ae0e199f05a46823aed792933e711;
    uint16  constant TOKEN_CONTRACT_CODE_DEPTH = 18;

    event ContractDeployed(address self);
    event TokenContractRegistered(address tokenContractAddress);

    // Static (part of stateInit, contribute to address derivation).
    // `_superRootAddress` binds this RootModel to a specific SuperRoot
    // instance — the SuperRoot at that address derives our expected
    // address from the same value, so a RootModel deployed pointing at a
    // bogus SuperRoot will not pass registration.
    uint256 static _ownerPubkey;
    address static _superRootAddress;

    // NO CODE CELL COMES IN ANY MORE, and the two checks that used to guard it are gone with it.
    // The constructor took a `TvmCell tokenContractCode`, verified it hashed to the pin below, and
    // then discarded it — nothing else in this contract ever touched it. Deal addresses are derived
    // from `TOKEN_CONTRACT_CODE_HASH` and `TOKEN_CONTRACT_CODE_DEPTH` directly, with a dummy code
    // cell (`_calculateTokenContractAddress`), so the pin was always the authority and the argument
    // was a copy of it. The check said "the code you handed me hashes to what I already know": it
    // proved the DEPLOYER held a correct copy, which was worth something while a person deployed
    // this contract by hand, and is worth nothing now that the super root deploys it from the code
    // it keeps itself. The two constants stay — they are what the derivation runs on.
    constructor() {
        // ONLY THE SUPER ROOT MAY CREATE A ROOT MODEL, and until this line that was an intention
        // rather than a rule. The address of a root derives from `_ownerPubkey`,
        // `_superRootAddress` and the code — all public — so anyone could compute where a given
        // key's root will live and put one there first. The contract landing there would be
        // byte-identical; what differs is the DAPP.
        //
        //   internal `new` from the super root   the child lands in the super root's dapp, which is
        //                                        configured, so `ensureBalance` -> `gosh.mintshellq`
        //                                        works — the whole reason the deploy moved here
        //   external deploy                      the child lands in a dapp of its own with no
        //                                        configuration, where that same line does nothing
        //
        // And the damage would be permanent: deploying onto an occupied address was MEASURED to
        // leave the existing code untouched and merely donate the attached value, so the super root
        // could not take the address back. The door closes here rather than being argued about
        // afterwards.
        //
        // `msg.sender`, not `msg.pubkey()`. `TokenContract` guards its own constructor by pubkey
        // because a seller deploys it by external message; this one is created by an internal
        // `new`, which carries no key at all, so a pubkey check would lock out the only legitimate
        // caller. For an external message `msg.sender` is `addr_none`, and this is a COMPARISON —
        // not a `.value` read, which is what threw on `addr_none` in #941.
        require(msg.sender == _superRootAddress, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();

        address selfExtern = address.makeAddrExtern(ContractDeployedEmit, bitCntAddress);
        emit ContractDeployed{dest: selfExtern}(address(this));

        // THE CALL BACK TO THE SUPER ROOT IS GONE, and so is the question it answered. It existed
        // because this contract used to be deployed by its owner as an external message: the super
        // root had no part in it and could not know the root existed, so the newborn announced
        // itself and the super root re-derived its address to check the announcement was not a
        // stranger's. Now the super root performs the deploy. It cannot be told about something it
        // did — there is no claim left to verify, and `registerRoot` was removed with it.
    }

    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    // ========================================================
    // Verifies sender == derived(TOKEN_CONTRACT_CODE_HASH/DEPTH, varInit).
    // Said as the pin, because that is what the derivation reads. The header used to say
    // `derived(tokenContractCode, ...)` — loose while the constructor still took such a cell,
    // and simply false now that it does not: no `tokenContractCode` exists anywhere here.
    // ========================================================

    function _calculateTokenContractAddress(uint256 sellerPubkey, uint64 nonce) private pure returns (address) {
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

    /// @dev    ACCEPT FIRST. Everything above `tvm.accept()` is billed to the INCOMING message, and
    ///         above it here stood `ensureBalance()` plus a full derivation of the caller's
    ///         canonical address — the two most expensive things on the path. A deal attaches
    ///         its own `DAPP_MSG_VALUE` (0.01) to this call and nothing derived that figure; measured on
    ///         the sibling paths, that much did not always reach the guard, and a call that runs
    ///         out of gas becomes `-14` rather than a refusal with a code. This one is sent
    ///         `flag: 1` with no reply expected, so the loss would be silent: the deal would
    ///         believe it registered and the root would never have heard of it.
    ///
    ///         The swap makes this root pay compute for messages it rejects. It can: unlike a deal,
    ///         a RootModel deployed BY THE SUPER ROOT lives in the super root's configured dapp, so
    ///         `ensureBalance` -> `gosh.mintshellq` actually works here. Under the old external
    ///         deploy it did not — same code, dead, for exactly the reason written out in
    ///         `TokenContract.sol`.
    function registerTokenContract(uint256 sellerPubkey, uint64 nonce) public pure {
        tvm.accept();
        ensureBalance();
        address expected = _calculateTokenContractAddress(sellerPubkey, nonce);
        require(msg.sender == expected, ERR_INVALID_SENDER);

        address regExtern = address.makeAddrExtern(TokenContractRegisteredEmit, bitCntAddress);
        emit TokenContractRegistered{dest: regExtern}(expected);
    }

    // ========================================================
    // Getters
    // ========================================================

    function getTokenContractAddress(uint256 sellerPubkey, uint64 nonce) external pure returns (address) {
        return _calculateTokenContractAddress(sellerPubkey, nonce);
    }

    function getOwnerPubkey() external view returns (uint256) { return _ownerPubkey; }

    function getVersion() external pure returns (string, string) {
        return (version, "RootModel");
    }
}
