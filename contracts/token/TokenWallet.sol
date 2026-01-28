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

import "./interface/ISubscriber.sol";
import "./interface/IReceiver.sol";

import "./modifiers/modifiers.sol";
import "./TokenWallet.sol";
import "./RootToken.sol";
import "./Transaction.sol";

/// @title Token Wallet Contract
/// @notice Individual token wallet for managing user token balances and transactions
/// @dev Handles token transfers, burns, and transaction deployment for a single user
contract TokenWallet is Modifiers {
    string constant version = "1.0.0";

    address _root;
    address static _owner;
    TvmCell _walletCode;
    TvmCell _transactionCode;
    optional(address) _subscriber;

    uint128 _balance;

    /// @notice Wallet constructor
    /// @dev Initializes wallet with root reference and code configuration
    /// @param isRoot Indicates if this is the root wallet deployment
    /// @param walletCode Compiled code for token wallet contracts
    /// @param transactionCode Compiled code for transaction contracts
    /// @param senderOwner Optional sender address for non-root wallet validation
    constructor(
        bool isRoot,
        TvmCell walletCode,
        TvmCell transactionCode,
        optional(address) senderOwner
    ) accept
    {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        _root = root;
        if (isRoot) {
            require(msg.sender == _root, ERR_WRONG_SENDER);
        }
        require(version == lib, ERR_WRONG_SENDER);
        _walletCode = walletCode;
        require(tvm.hash(transactionCode) == TRANSACTION_HASH, ERR_WRONG_HASH_TRANSACTION);
        _transactionCode = transactionCode;        
        if (!isRoot) {
            require(msg.sender == address.makeAddrStd(0, tvm.hash(buildWalletInitData(senderOwner.get()))), ERR_WRONG_SENDER);
        }
        require(address(this) == address.makeAddrStd(0, tvm.hash(buildWalletInitData(_owner))), ERR_WRONG_SENDER);
    }

    /// @notice Ensures contract maintains minimum balance
    /// @dev Automatically mints shell if balance falls below threshold
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Mints tokens to this wallet
    /// @dev Can only be called by root contract, increases wallet balance
    /// @param value Amount of tokens to mint
    function mint(uint128 value) public senderIs(_root) accept {
        ensureBalance();
        _balance += value;
    }

    /// @notice Deploys a new transaction contract
    /// @dev Creates transaction for transfer, burn, destroy, or bridge withdrawal operations
    /// @param transactionType Type of transaction (TRANSFER_TYPE, BURN_TYPE, DESTROY_TYPE, WITHDRAW_TYPE)
    /// @param value Optional amount of tokens for transfer/burn/withdrawal operations
    /// @param destinationOwner Optional recipient address for transfer operations or optional subscriber address
    /// @param toWithdraw Optional external chain address for bridge withdrawal operations
    function deployTransaction(
        uint8 transactionType,
        optional(uint128) value,
        optional(address) destinationOwner,
        optional(uint256) toWithdraw
    ) public view accept {
        ensureBalance();
        TvmCell data;
        if (transactionType == TRANSFER_TYPE) {
            require(_balance >= value.get(), ERR_LOW_BALANCE);
            data = abi.encode(destinationOwner.get(), value.get());
            deployTransactionIn(transactionType, data);
            return;
        }
        if (transactionType == BURN_TYPE) {
            require(_balance >= value.get(), ERR_LOW_BALANCE);
            data = abi.encode(value.get());
            deployTransactionIn(transactionType, data);
            return;
        }
        if (transactionType == DESTROY_TYPE) {
            deployTransactionIn(transactionType, data);
            return;
        }
        if (transactionType == WITHDRAW_TYPE) {
            data = abi.encode(value.get(), toWithdraw.get());
            require(_balance >= value.get(), ERR_LOW_BALANCE);
            deployTransactionIn(transactionType, data);
            return;
        }
        if (transactionType == SET_SUBSCRIBER_TYPE) {
            data = abi.encode(destinationOwner);
            deployTransactionIn(transactionType, data);
            return;
        }
        revert(ERR_WRONG_TRANSACTION_TYPE);
    }

    /// @notice Internal transaction deployment implementation
    /// @dev Handles actual transaction contract creation
    /// @param transactionType Type of transaction to deploy
    /// @param data Encoded transaction parameters
    function deployTransactionIn(uint8 transactionType, TvmCell data) private view {
        new Transaction {
            stateInit: buildTransactionInitData(data),
            value: 10 vmshell,
            flag: 1
        }(transactionType, data, _owner);
    }

    /// @notice Builds initialization data for transaction contracts
    /// @dev Creates state init cell for transaction deployment
    /// @param data Transaction data to be hashed for initialization
    /// @return TvmCell Initialization data for transaction contract
    function buildTransactionInitData(TvmCell data) private view returns (TvmCell) {
        TvmCell finalcell = abi.encode(version, address(this));
        TvmCell wallet = abi.setCodeSalt(_transactionCode, finalcell);
        return abi.encodeStateInit({
            contr: Transaction,
            varInit: {
                _dataHash: tvm.hash(data)
            },
            code: wallet
        });
    }

    /// @notice Accepts token transfer from another wallet
    /// @dev Called by sending wallet to complete transfer operation
    /// @param value Amount of tokens being transferred
    /// @param walletOwner Address of the sending wallet owner
    function acceptTransfer(uint128 value, address walletOwner) public {
        ensureBalance();
        address wallet = address.makeAddrStd(0, tvm.hash(buildWalletInitData(walletOwner)));
        require(msg.sender == wallet, ERR_WRONG_SENDER);
        tvm.accept();
        _balance += value;

        if (_subscriber.hasValue()) {
            ISubscriber(_subscriber.get()).onTransferReceived{
                value: 0.1 vmshell,
                flag: 1
            }(walletOwner, _owner, value, _balance);
        }
    }

    /// @notice Sends wallet state data to the caller via callback interface
    /// @dev Can be called from any address that implements IReceiver interface
    /// @dev This provides a standardized way for contracts to query wallet state
    function sendData() public view internalMsg accept {
        ensureBalance();
        IReceiver(msg.sender).getWalletState{value: 0.1 vmshell, flag: 1}(_root, _owner, _balance, _subscriber);
    }                                   

    /// @notice Processes completed transaction
    /// @dev Called by transaction contract to execute transfer, burn, or destroy
    /// @param transactionType Type of transaction being processed
    /// @param data Encoded transaction parameters
    function acceptTransaction(
        uint8 transactionType,
        TvmCell data
    ) public {
        ensureBalance();
        address transaction = address.makeAddrStd(0, tvm.hash(buildTransactionInitData(data)));        
        require(msg.sender == transaction, ERR_WRONG_SENDER);
        tvm.accept();
        if (transactionType == TRANSFER_TYPE) {
            (address destinationOwner, uint128 value) = abi.decode(data, (address, uint128));
            require(_balance >= value, ERR_LOW_BALANCE);
            _balance -= value;
            address wallet = deployWallet(destinationOwner);
            TokenWallet(wallet).acceptTransfer{value: 0.1 vmshell, flag: 1}(value, _owner);
            return;
        }
        if (transactionType == BURN_TYPE) {
            (uint128 value) = abi.decode(data, (uint128));
            require(_balance >= value, ERR_LOW_BALANCE);
            _balance -= value;
            RootToken(_root).burn{value: 0.1 vmshell, flag: 1}(value, _owner);
            return;
        }
        if (transactionType == DESTROY_TYPE) {
            RootToken(_root).burn{value: 0.1 vmshell, flag: 161}(_balance, _owner);
            return;
        }
        if (transactionType == WITHDRAW_TYPE) {
            (uint128 value, uint256 to) = abi.decode(data, (uint128, uint256));
            require(_balance >= value, ERR_LOW_BALANCE);
            _balance -= value;
            RootToken(_root).processWithdraw{value: 0.1 vmshell}(value, to, _owner);
            return;
        }  
        if (transactionType == SET_SUBSCRIBER_TYPE) {
            (optional(address) subscriber) = abi.decode(data, (optional(address)));
            _subscriber = subscriber;
            return;
        }
    }    

    /// @notice Builds initialization data for wallet contracts
    /// @dev Creates state init cell for wallet deployment
    /// @param walletOwner Owner address for the wallet
    /// @return TvmCell Initialization data for wallet contract
    function buildWalletInitData(address walletOwner) private view returns (TvmCell) {
        TvmCell finalcell = abi.encode(version, _root);
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
    /// @dev Internal function to create recipient wallet for transfers
    /// @param owner Address that will own the new wallet
    /// @return address Address of the deployed wallet contract
    function deployWallet(address owner) private view returns(address) {
        address tokenWallet = new TokenWallet {
            stateInit: buildWalletInitData(owner),
            value: 50 vmshell,
            flag: 1
        }(false, _walletCode, _transactionCode, _owner);
        return tokenWallet;
    }

    /// @notice Calculates wallet address for a given owner
    /// @dev Computes the deterministic address of a token wallet without deploying it
    /// @param walletOwner Owner address to calculate wallet for
    /// @return walletAddress Calculated address of the token wallet
    function getWalletAddress(address walletOwner) external view returns(address walletAddress) {
        TvmCell initData = buildWalletInitData(walletOwner);
        walletAddress = address.makeAddrStd(0, tvm.hash(initData));
    }

    /// @notice Calculates transaction address for given transaction parameters
    /// @dev Computes the deterministic address of a transaction contract without deploying it
    /// @dev Handles different transaction types and encodes data accordingly
    /// @param transactionType Type of transaction (TRANSFER_TYPE, BURN_TYPE, WITHDRAW_TYPE)
    /// @param value Amount of tokens for the transaction (required for TRANSFER, BURN, WITHDRAW)
    /// @param destinationOwner Recipient address for TRANSFER_TYPE transactions of optional subscriber address for SET_SUBSCRIBER_TYPE
    /// @param toWithdraw External chain address for WITHDRAW_TYPE transactions
    /// @return transactionAddress Calculated deterministic address of the transaction contract
    /// @notice For DESTROY_TYPE transactions, use empty optional parameters as they don't require additional data
    function getTransactionAddress(
        uint8 transactionType,
        optional(uint128) value,
        optional(address) destinationOwner,
        optional(uint256) toWithdraw
    ) external view returns(address transactionAddress) {
        ensureBalance();
        TvmCell data;
        if (transactionType == TRANSFER_TYPE) {
            data = abi.encode(destinationOwner.get(), value.get());
        }
        if (transactionType == BURN_TYPE) {
            data = abi.encode(value.get());
        }
        if (transactionType == WITHDRAW_TYPE) {
            data = abi.encode(value.get(), toWithdraw.get());
        }
        if (transactionType == SET_SUBSCRIBER_TYPE) {
            data = abi.encode(destinationOwner);
        }
        TvmCell initData = buildTransactionInitData(data);
        transactionAddress = address.makeAddrStd(0, tvm.hash(initData));
    }

    /// @notice Returns wallet details and current state
    /// @return root Root token contract address
    /// @return owner This wallet's owner address
    /// @return balance Current token balance of this wallet
    function getDetails() external view returns(
        address root,
        address owner,
        uint128 balance
    ) {
        return (_root, _owner, _balance);
    } 

    /// @notice Returns wallet subscriber
    function getSubscriber() external view returns(
        optional(address) subscriber
    ) {
        return _subscriber;
    } 

    /// @notice Returns contract version information
    /// @return version String Contract version number
    /// @return name String Contract name identifier
    function getVersion() external pure returns(string, string) {
        return (version, "TokenWallet");
    }        
}