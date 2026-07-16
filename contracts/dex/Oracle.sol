pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./OracleEventList.sol";
import "./libraries/DexLib.sol";

/// @title Oracle Contract
contract Oracle is Modifiers {

    /// @notice Contract semantic version.
    string constant version = "4.0.27";

    /// @notice Oracle owner pubkey used for access control.
    uint256 _oraclePubkey;
    /// @notice OracleEventList code used for deterministic deployments.
    TvmCell _oracleEventListCode;

    /// @notice Hash of salted PMP code for sender verification in OracleEventList.
    uint256 _pmpSaltedCodeHash;
    /// @notice Depth of salted PMP code for sender verification in OracleEventList.
    uint16  _pmpSaltedCodeDepth;

    /// @notice Oracle name (state-init static field).
    string static _name;

    /// @notice Emitted when a new OracleEventList is deployed.
    /// @param eventListAddress Deployed OracleEventList address.
    /// @param index Deployed OracleEventList index.
    /// @param description Human-readable description of the list.
    event OracleEventListDeployed(address eventListAddress, uint128 index, string description);

    /// @notice Reserved event for external publication flow.
    /// @param eventId Event identifier.
    /// @param eventName Human-readable event name.
    event EventPublished(uint256 eventId, string eventName);

    /// @notice Initializes Oracle and deploys default OracleEventList with index 0.
    /// @param oraclePubkey Oracle owner public key.
    /// @param oracleEventListCode OracleEventList contract code.
    /// @param PrivateNoteCode PrivateNote contract code used to salt PMP.
    /// @param pmpCode Base PMP contract code.
    constructor(
        uint256 oraclePubkey,
        TvmCell oracleEventListCode,
        TvmCell PrivateNoteCode,
        TvmCell pmpCode
    ) {
        tvm.accept();
        require(msg.sender == ROOT_ORACLE_ADDRESS, ERR_INVALID_SENDER);
        // pubkey=0 would make every onlyOwnerPubkey-gated method callable
        // by any keyless ext tx (msg.pubkey()==0). Reject at deploy.
        require(oraclePubkey != 0, ERR_INVALID_PARAMS);

        _oraclePubkey = oraclePubkey;
        _oracleEventListCode = oracleEventListCode;

        TvmCell saltedPmpCode = DexLib.buildPMPCode(PrivateNoteCode, pmpCode);
        _pmpSaltedCodeHash = tvm.hash(saltedPmpCode);
        _pmpSaltedCodeDepth = saltedPmpCode.depth();

        address oracleEventList = new OracleEventList{
            stateInit: DexLib.buildOracleEventListStateInit(
                _oracleEventListCode, address(this), 0
            ),
            value: 10 vmshell, flag: 1
        }(_oraclePubkey, _pmpSaltedCodeHash, _pmpSaltedCodeDepth, "");

        address addrExtern = address.makeAddrExtern(ORACLE_DEPLOYED, bitCntAddress);
        emit OracleEventListDeployed{dest: addrExtern}(oracleEventList, 0, "");
    }

    /// @notice Deploys an OracleEventList with a custom index.
    /// @param index OracleEventList index.
    /// @param description Human-readable description of the list.
    function deployEventList(uint128 index, string description) public view onlyOwnerPubkey(_oraclePubkey) accept {
        ensureBalance();

        address oracleEventList = new OracleEventList{
            stateInit: DexLib.buildOracleEventListStateInit(
                _oracleEventListCode, address(this), index
            ),
            value: 10 vmshell, flag: 1
        }(_oraclePubkey, _pmpSaltedCodeHash, _pmpSaltedCodeDepth, description);

        address addrExtern = address.makeAddrExtern(ORACLE_DEPLOYED, bitCntAddress);
        emit OracleEventListDeployed{dest: addrExtern}(oracleEventList, index, description);
    }

    /// @notice Ensures minimal native balance for operations.
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) return;
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Withdraws shell fees from the Oracle contract.
    /// @param to Recipient address.
    /// @param amount Amount of shell fees to withdraw.
    function withdrawFees(address to, uint128 amount) public view onlyOwnerPubkey(_oraclePubkey) accept {
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID_SHELL] = amount;
        to.transfer({ value: 0.1 vmshell, flag: 1, currencies: data });
    }

    /// @notice Accepts native transfers and replenishes balance if needed.
    receive() external pure {
        tvm.accept();
        ensureBalance();
    }

    /// @notice Returns OracleEventList address for specified index
    /// @param index Index of the OracleEventList
    /// @return eventListAddress OracleEventList contract address
    function getEventListAddress(uint128 index) external view returns (address) {
        return DexLib.computeOracleEventListAddress(
            _oracleEventListCode, address(this), index
        );
    }
    
    /// @notice Returns contract version identifier
    /// @return value0 Contract semantic version.
    /// @return value1 Contract identifier.
    function getVersion() external pure returns (string, string) {
        return (version, "Oracle");
    }
}
