// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";

contract BLSKeyIndex is Modifiers {
    string constant version = "1.0.0";
    bytes static _bls;
    address static _root;
    address _wallet;
    uint256 _wallet_pubkey;
    uint256 _stake;
    uint16 _signerIndex;
    bool _ready;

    /**
     * @dev Initializes the contract with wallet, pubkey, stake, and signer index.
     * @param wallet The wallet address linked to the BLS key.
     * @param pubkey The public key associated with the wallet.
     * @param stake The amount of stake allocated to this BLS key.
     * @param signerIndex The index of the signer in the validator set.
     * 
     * Requirements:
     * - Only callable by the `_root` address.
     */
    constructor (
        address wallet,
        uint256 pubkey,
        uint256 stake,
        uint16 signerIndex
    ) accept senderIs(_root) {
        ensureBalance();
        _ready = false;
        _wallet = wallet;
        _wallet_pubkey = pubkey;
        _stake = stake;
        _signerIndex = signerIndex;
    }

    /**
     * @dev Ensures the contract has enough balance for operations.
     *      If the balance is insufficient, mints additional funds.
     */
    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLS_KEY) { 
            return; 
        }
        gosh.mintshell(FEE_DEPLOY_BLS_KEY);
    }

    /**
     * @dev Marks the BLS key as used by the system.
     * @param rep_coef Reputation coefficient for the wallet.
     * @param licenses Array of licenses tied to this wallet.
     * @param virtualStake Optional virtual stake value.
     * @param ProxyList A mapping of proxies associated with the wallet.
     *
     * Requirements:
     * - Only callable by the `_root` address.
     */
    function isBLSKeyAccept(uint256 pubkey, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList) public senderIs(_root) accept {
        ensureBalance();
        BlockKeeperContractRoot(_root).isBLSAccepted{value: 0.1 vmshell, flag: 1}(pubkey, _bls, _stake, _ready, _signerIndex, rep_coef, licenses, virtualStake, ProxyList);
        _ready = true;
    }

    /**
     * @dev Marks the BLS key as used by the system with continue epoch process.
     * @param seqNoStartOld Sequence number to resume from.
     * @param rep_coef Reputation coefficient for the wallet.
     * @param licenses Array of licenses tied to this wallet.
     * @param virtualStake Optional virtual stake value.
     * @param ProxyList A mapping of proxies associated with the wallet.
     *
     * Requirements:
     * - Only callable by the `_root` address.
     */
    function isBLSKeyAcceptContinue(uint256 pubkey, uint64 seqNoStartOld, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList) public senderIs(_root) accept {
        ensureBalance();
        BlockKeeperContractRoot(_root).isBLSAcceptedContinue{value: 0.1 vmshell, flag: 1}(pubkey, _bls, _stake, _ready, seqNoStartOld, _signerIndex, rep_coef, licenses, virtualStake, ProxyList);
        _ready = true;
    }

    /**
     * @dev Destroys the contract by BlockKeeperContractRoot contract and transfers remaining funds to BlockKeeperContractRoot.
     *
     * Requirements:
     * - Only callable by the `_root` address.
     */
    function destroyRoot() public senderIs(_root) accept {
        ensureBalance();
        selfdestruct(_root);
    }

    /**
     * @dev Destroys the contract by AckiNackiBlockKeeperNodeWallet and transfers remaining funds to the BlockKeeperContractRoot address.
     *
     * Requirements:
     * - Only callable by the `_wallet` address.
     */
    function destroy() public senderIs(_wallet) accept {
        ensureBalance();
        selfdestruct(_root);
    }

    /**
     * @dev Retrieves the readiness status of the BLS key.
     * @return ready `true` if the BLS key is used; otherwise, `false`.
     */
    function getReadyStatus() external view returns(bool ready) {
        return _ready;
    }
    
    /**
     * @dev Retrieves the version of the contract and its type.
     * @return version The version of the contract.
     * @return type A string identifying the contract type.
     */
    function getVersion() external pure returns(string, string) {
        return (version, "BLSKey");
    }
}
