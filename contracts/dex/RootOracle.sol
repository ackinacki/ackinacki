pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./Oracle.sol";
import "./libraries/DexLib.sol";

/// @notice Root contract responsible for deploying Oracle contracts
contract RootOracle is Modifiers {

    /// @notice Contract semantic version.
    string constant version = "4.0.27";

    /// @notice Stored code of PrivateNote contract
    TvmCell _privateNoteCode;

    /// @notice Stored code of PMP contract
    TvmCell _pmpCode;

    /// @notice Stored code of Oracle contract
    TvmCell _oracleCode;

    /// @notice Stored code of OracleEventList contract
    TvmCell _oracleEventListCode;

    /// @notice Root owner public key
    uint256 _ownerPubkey;

    /// Events

    /// @notice Emitted when a new Oracle contract is deployed by this root.
    /// @param oracle Deployed Oracle contract address.
    /// @param pubkey Oracle owner public key provided at deployment.
    /// @param name Oracle unique name used for deterministic address derivation.
    event OracleDeployed(address oracle, uint256 pubkey, string name);

    /// @notice Root constructor
    constructor() {
        tvm.accept();
    }

    /// @notice Ensures minimal native balance for root operations
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) return;
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Deploys a new Oracle contract
    /// @param oraclePubkey Public key of the oracle
    /// @param oracleName Name of the oracle
    function deployOracle(uint256 oraclePubkey, string oracleName) public view accept {
        ensureBalance();
        // Mirror the Oracle constructor guard: reject pubkey=0 at the root so we
        // don't waste gas on a deploy the Oracle constructor would reject.
        require(oraclePubkey != 0, ERR_INVALID_PARAMS);
        TvmCell stateInit = DexLib.buildOracleStateInit(_oracleCode, oracleName);
        address oracle = new Oracle{
            stateInit: stateInit,
            value: 60 vmshell,
            flag: 1
        }(oraclePubkey, _oracleEventListCode, _privateNoteCode, _pmpCode);

        address addrExtern = address.makeAddrExtern(ROOTORACLE_ORACLE_DEPLOYED, bitCntAddress);
        emit OracleDeployed{dest: addrExtern}(oracle, oraclePubkey, oracleName);
    }

    /// @notice Updates the contract code for RootOracle
    /// @param newcode New contract code
    /// @param cell Encoded persistent state used by `onCodeUpgrade`
    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    /// @notice Handles root code upgrade
    /// @param cell Code upgrade data cell
    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        ensureBalance();
        tvm.resetStorage();
        (_pmpCode, _privateNoteCode, _oracleCode, _oracleEventListCode, _ownerPubkey) = abi.decode(cell, (TvmCell, TvmCell, TvmCell, TvmCell, uint256));
    }

    /// @notice Returns the deterministic address of an Oracle by name
    /// @param name Unique name of the Oracle
    /// @return oracleAddress Oracle contract address corresponding to the given name
    function getOracleAddress(string name) external view returns(address oracleAddress) {
        return DexLib.computeOracleAddress(_oracleCode, name);
    }

    /// @notice Returns root version
    /// @return value0 Contract semantic version.
    /// @return value1 Contract identifier.
    function getVersion() external pure returns (string, string) {
        return (version, "RootOracle");
    }
}
