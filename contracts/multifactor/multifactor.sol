// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";

contract Multifactor is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => Factor) _factors;
    uint256 _owner_pubkey;
    optional(uint256) _candidate_pubkey;
    uint256 _pub_recovery_phrase;

    string public _zkaddr;

    constructor (
        string zkaddr,
        uint256 pub_recovery_phrase,
        uint64 value
    ) {
        gosh.cnvrtshellq(value);
        _zkaddr = zkaddr;
        _owner_pubkey = msg.pubkey();
        _pub_recovery_phrase = pub_recovery_phrase;
    }   

    function addZKP(uint8 index, bytes modulus, uint8 index_mod_4, string iss_base_64, string header_base_64, uint64 expire_at, uint256 epk) public onlyOwnerPubkey(_owner_pubkey) {
        bytes ph = gosh.poseidon(index_mod_4, expire_at, epk, modulus, iss_base_64, header_base_64, _zkaddr);
        Factor data = Factor(ph, epk, expire_at);
        _factors[index] = data;
    }

    function changeSeedPhrase(
        uint8 index,
        bytes proof,
        uint256 pub_seed_phrase) public {
        require(_factors.exists(index), ERR_WRONG_CUSTODIAN);
        require(_factors[index].epk == msg.pubkey(), ERR_WRONG_CUSTODIAN);
        require(gosh.vergrth16(proof, _factors[index].ph, 0), ERR_WRONG_CUSTODIAN);
        tvm.accept();
        require(!_candidate_pubkey.hasValue(), ERR_CANDIDATE_EXIST);
        _candidate_pubkey = pub_seed_phrase;
    }

    function acceptCandidateSeedPhrase() public onlyOwnerPubkey(_pub_recovery_phrase) accept {
        require(_candidate_pubkey.hasValue(), ERR_CANDIDATE_EXIST);
        _owner_pubkey = _candidate_pubkey.get();
    }

    function deleteCandidateSeedPhrase(
        uint8 index,
        bytes proof) public {
        require(_factors.exists(index), ERR_WRONG_CUSTODIAN);
        require(_factors[index].epk == msg.pubkey(), ERR_WRONG_CUSTODIAN);
        require(gosh.vergrth16(proof, _factors[index].ph, 0), ERR_WRONG_CUSTODIAN);
        tvm.accept();
        _candidate_pubkey = null;
    }

    function deleteZKP(uint8 index) public onlyOwnerPubkey(_owner_pubkey) {
        delete _factors[index];
    }

    function updateRecoveryPhrase(uint256 pubkey) public onlyOwnerPubkey(_owner_pubkey) {
        _pub_recovery_phrase = pubkey;
    }

    function updateSeedPhrase(uint256 pubkey) public onlyOwnerPubkey(_owner_pubkey) {
        _owner_pubkey = pubkey;
    }

    function sendTransaction(
        uint8 index,
        bytes proof,
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        uint8 flags,
        TvmCell payload) public view
    {
        require(_factors.exists(index), ERR_WRONG_CUSTODIAN);
        require(_factors[index].epk == msg.pubkey(), ERR_WRONG_CUSTODIAN);
        require(gosh.vergrth16(proof, _factors[index].ph, 0), ERR_WRONG_CUSTODIAN);
        tvm.accept();
        dest.transfer(varuint16(value), bounce, flags, payload, cc);
    }

    function exchangeToken(uint64 value) public pure {
        tvm.accept();
        gosh.cnvrtshellq(value);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "Multifactor");
    }
}
