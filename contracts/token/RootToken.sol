/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./TokenWallet.sol";

/// @title Token Root Contract
/// @notice Main contract for token management in the GOSH ecosystem
/// @dev Handles token minting, burning, wallet deployment and upgrade functionality
/// @dev Serves as the central authority for token operations and wallet management
contract RootToken is Modifiers {
    /// @notice Contract version identifier
    string constant version = "1.0.0";
    
    /// @notice Address that deployed this contract (immutable)
    address static _deployer;
    
    /// @notice Token name
    string static _name;
    
    /// @notice Number of decimal places for token divisibility
    uint128 static _decimals; 

    /// @notice Compiled code for TokenWallet contracts
    TvmCell _walletCode;
    
    /// @notice Compiled code for transaction contracts
    TvmCell _transactionCode;

    /// @notice Total amount of tokens minted
    uint128 _minted = 0;
    
    /// @notice Total amount of tokens burned
    uint128 _burned = 0;

    /// @notice Flag indicating if minting is permanently disabled
    bool _mintDisabled = false;

    /// @notice Public key of the contract owner for authorization
    uint256 _ownerPubkey;

    /// @notice Contract constructor
    /// @dev Initializes the token root with initial supply and configuration
    /// @param initialSupplyToOwner Address to receive initial token supply
    /// @param initialSupply Amount of tokens to mint initially
    /// @param mintDisabled Whether minting is disabled from start
    /// @param walletCode Compiled code for token wallet contracts
    /// @param transactionCode Compiled code for transaction contracts
    /// @param pubkey Public key of the contract owner
    constructor(
        address initialSupplyToOwner,
        uint128 initialSupply,
        bool mintDisabled,
        TvmCell walletCode,
        TvmCell transactionCode,
        uint256 pubkey
    ) accept
    {
        require(msg.sender == _deployer, ERR_NOT_OWNER);
        require(_decimals <= DECIMALS_MAX, ERR_TOO_BIG_DECIMALS);
        initialize(
            initialSupplyToOwner,
            initialSupply,
            mintDisabled,
            walletCode,
            transactionCode,
            pubkey
        );
    }

    /// @notice Internal initialization function
    /// @dev Sets up contract state after deployment or upgrade
    /// @param initialSupplyToOwner Address to receive initial token supply
    /// @param initialSupply Amount of tokens to mint initially
    /// @param mintDisabled Whether minting is disabled
    /// @param walletCode Compiled code for token wallet contracts
    /// @param transactionCode Compiled code for transaction contracts
    /// @param pubkey Public key of the contract owner
    function initialize(
        address initialSupplyToOwner,
        uint128 initialSupply,
        bool mintDisabled,
        TvmCell walletCode,
        TvmCell transactionCode,
        uint256 pubkey) private {
        ensureBalance();
        require(tvm.hash(walletCode) == WALLET_HASH, ERR_WRONG_HASH);
        _walletCode = walletCode;
        require(tvm.hash(transactionCode) == TRANSACTION_HASH, ERR_WRONG_HASH_TRANSACTION);
        _transactionCode = transactionCode;
        _ownerPubkey = pubkey;
        _minted = 0;
        _burned = 0;
        mintIn(initialSupply, initialSupplyToOwner);
        _mintDisabled = mintDisabled;
    }

    /// @notice Permanently disables token minting
    /// @dev Can only be called by owner, irreversible operation
    /// @custom:security Only callable by owner via public key authorization
    function setDisableMint() public onlyOwnerPubkey(_ownerPubkey) accept {
        _mintDisabled = true;
    }

    /// @notice Clears burned tokens data
    /// @dev Resets internal counters for burned tokens, effectively removing them from circulation tracking
    /// @custom:security Only callable by owner
    function clearData() public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _minted -= _burned;
        _burned = 0;
    }

    /// @notice Updates owner public key
    /// @dev Allows transfer of ownership to new key pair
    /// @param pubkey New public key for contract ownership
    /// @custom:security Only callable by current owner
    function setPubkey(uint256 pubkey) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _ownerPubkey = pubkey;
    }

    /// @notice Records token burning operation
    /// @dev Can only be called by valid token wallet. Updates internal burn tracking.
    /// @param value Amount of tokens burned
    /// @param walletOwner Address of wallet owner performing burn
    /// @custom:security Only callable by verified token wallet contracts
    function burn(uint128 value, address walletOwner) public {
        ensureBalance();
        address wallet = address.makeAddrStd(0, tvm.hash(buildWalletInitData(walletOwner)));
        require(msg.sender == wallet, ERR_WRONG_SENDER);
        _burned += value;
    }

    /// @notice Processes cross-chain withdrawal operation
    /// @dev Handles token burning for bridge withdrawals to external chains
    /// @param value Amount of tokens to withdraw/burn
    /// @param to External blockchain address or chain identifier for withdrawal
    /// @param owner Address of the wallet owner initiating the withdrawal
    /// @custom:security Only callable by verified token wallet contracts
    function processWithdraw(
        uint128 value,
        uint256 to,
        address owner
    ) public {
        ensureBalance();
        address wallet = address.makeAddrStd(0, tvm.hash(buildWalletInitData(owner)));
        require(msg.sender == wallet, ERR_WRONG_SENDER);
        _burned += value;
        // The 'to' parameter represents the external chain address or identifier
        // This would typically be used in bridge integration logic
        to;
    }

    /// @notice Mints new tokens to specified wallet
    /// @dev Requires minting to be enabled and caller to be owner
    /// @param value Amount of tokens to mint
    /// @param walletOwner Address of recipient wallet owner
    /// @custom:security Only callable by owner when minting is enabled
    function mint(uint128 value, address walletOwner) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        mintIn(value, walletOwner);
    }

    /// @notice Internal token minting implementation
    /// @dev Handles actual minting logic and state updates
    /// @param value Amount of tokens to mint
    /// @param walletOwner Address of recipient wallet owner
    function mintIn(uint128 value, address walletOwner) private {
        if (value == 0) { return; }
        require(_mintDisabled == false, ERR_MINT_DISABLED);
        address wallet = address.makeAddrStd(0, tvm.hash(buildWalletInitData(walletOwner)));
        deployWallet(walletOwner);
        _minted += value;
        TokenWallet(wallet).mint{value: 0.1 vmshell, flag: 1}(value);
    }

    /// @notice Ensures contract maintains minimum balance
    /// @dev Automatically mints shell if balance falls below threshold
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Builds initialization data for wallet contracts
    /// @dev Creates state init cell for wallet deployment
    /// @param walletOwner Owner address for the wallet
    /// @return TvmCell Initialization data for wallet
    function buildWalletInitData(address walletOwner) private view returns (TvmCell) {
        TvmCell finalcell = abi.encode(version, address(this));
        TvmCell wallet = abi.setCodeSalt(_walletCode, finalcell);
        return abi.encodeStateInit({
            contr: TokenWallet,
            varInit: {
                _owner: walletOwner
            },
            code: wallet
        });
    }

    /// @notice Deploys new token wallet for specified owner
    /// @dev Creates new wallet contract if it doesn't exist
    /// @param owner Address that will own the new wallet
    function deployWallet(address owner) public view accept {
        ensureBalance();
        new TokenWallet {
            stateInit: buildWalletInitData(owner),
            value: 50 vmshell,
            flag: 1
        }(true, _walletCode, _transactionCode, null);
    }

    /// @notice Updates contract code (upgrade functionality)
    /// @dev Allows contract code migration while preserving state
    /// @param newcode New contract code to deploy
    /// @param cell Data cell containing initialization parameters
    /// @custom:security Only callable by the contract itself (during upgrade process)
    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerPubkey(_ownerPubkey) {
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    /// @notice Handles contract upgrade process
    /// @dev Called after code update to reinitialize state
    /// @param cell Data cell containing initialization parameters
    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        ensureBalance();
        tvm.resetStorage();
        (string name, uint128 decimals, TvmCell walletCode, TvmCell transactionCode, uint256 pubkey, bool mintDisabled, address initialSupplyToOwner, uint128 initialSupply) = abi.decode(cell, (string, uint128, TvmCell, TvmCell, uint256, bool, address, uint128));
        require(tvm.hash(walletCode) == WALLET_HASH, ERR_WRONG_HASH);
        _name = name;
        _decimals = decimals;
        require(_decimals <= DECIMALS_MAX, ERR_TOO_BIG_DECIMALS);
        initialize(
            initialSupplyToOwner,
            initialSupply,
            mintDisabled,
            walletCode,
            transactionCode, 
            pubkey
        );
    }

    /// @notice Returns contract details and state
    /// @return name Token name
    /// @return decimals Number of decimal places
    /// @return deployer Address that deployed the contract
    /// @return minted Total tokens minted
    /// @return burned Total tokens burned
    /// @return mintDisabled Whether minting is disabled
    /// @return ownerPubkey Current owner public key
    function getDetails() external view returns(
        string name,
        uint128 decimals,
        address deployer,
        uint128 minted,
        uint128 burned,
        bool mintDisabled,
        uint256 ownerPubkey
    ) {
        return (_name, _decimals, _deployer, _minted, _burned, _mintDisabled, _ownerPubkey);
    }  

    /// @notice Calculates wallet address for a given owner
    /// @dev Computes the deterministic address of a token wallet without deploying it
    /// @param walletOwner Owner address to calculate wallet for
    /// @return walletAddress Calculated address of the token wallet
    function getWalletAddress(address walletOwner) external view returns(address walletAddress) {
        TvmCell initData = buildWalletInitData(walletOwner);
        walletAddress = address.makeAddrStd(0, tvm.hash(initData));
    }

    /// @notice Returns contract version information
    /// @return version String Contract version number
    /// @return name String Contract name identifier
    function getVersion() external pure returns(string, string) {
        return (version, "RootToken");
    }        
}