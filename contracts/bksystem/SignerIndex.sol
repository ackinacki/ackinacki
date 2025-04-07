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

contract SignerIndex is Modifiers {
    string constant version = "1.0.0";
    uint16 static _signerIndex;
    address static _root;
    address _wallet;
    uint256 _wallet_pubkey;
    uint256 _stake;
    bool _ready;
    bytes _bls;

    /**
     * @dev Initializes the contract with wallet, pubkey, stake, and BLS key.
     * @param wallet The wallet address linked to the signer.
     * @param pubkey The public key associated with the wallet.
     * @param stake The amount of stake allocated to this signer.
     * @param bls_key The BLS key associated with the wallet.
     * 
     * Requirements:
     * - Only callable by the `_root` address.
     */
    constructor (
        address wallet,
        uint256 pubkey,
        uint256 stake,
        bytes bls_key
    ) accept senderIs(_root) {
        ensureBalance();
        _ready = false;
        _wallet = wallet;
        _wallet_pubkey = pubkey;
        _stake = stake;
        _bls = bls_key;
    }

    /**
     * @dev Ensures the contract has enough balance for operations.
     * If the balance is insufficient, mints additional funds.
     */
    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_SIGNER_INDEX) { return; }
        gosh.mintshell(FEE_DEPLOY_SIGNER_INDEX);
    }

    /**
     * @dev Marks the signer index as used and notifies the `BlockKeeperContractRoot`.
     * @param rep_coef Reputation coefficient of the wallet.
     * @param licenses Array of licenses associated with the wallet.
     * @param virtualStake Optional virtual stake value.
     * @param ProxyList Mapping of proxy addresses related to the wallet.
     *
     * Requirements:
     * - Only callable by the `_root` address.
     */
    function isSignerIndexAccept(uint256 pubkey, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList) public senderIs(_root) accept {
        ensureBalance();
        BlockKeeperContractRoot(_root).isSignerIndexAccepted{value: 0.1 vmshell, flag: 1}(pubkey, _bls, _stake, _ready, _signerIndex, rep_coef, licenses, virtualStake, ProxyList);
        _ready = true;
    }

    /**
     * @dev Marks the signer index as used and notifies the `BlockKeeperContractRoot` with a continue process.
     * @param seqNoStartOld Sequence number of block from the previous epoch.
     * @param rep_coef Reputation coefficient of the wallet.
     * @param licenses Array of licenses associated with the wallet.
     * @param virtualStake Optional virtual stake value.
     * @param ProxyList Mapping of proxy addresses related to the wallet.
     *
     * Requirements:
     * - Only callable by the `_root` address.
     */
    function isSignerIndexAcceptContinue(uint256 pubkey, uint64 seqNoStartOld, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList) public senderIs(_root) accept {
        ensureBalance();
        BlockKeeperContractRoot(_root).isSignerIndexContinue{value: 0.1 vmshell, flag: 1}(pubkey, _bls, _stake, _ready, seqNoStartOld, _signerIndex, rep_coef, licenses, virtualStake, ProxyList);
        _ready = true;
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
        return (version, "SignerIndex");
    }
}
