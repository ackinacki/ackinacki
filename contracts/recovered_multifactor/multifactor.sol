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

    mapping(uint256 => uint64) public _factors; // key - ephemeral pk, value - expiration timestamp
    uint256 public _owner_pubkey;
    optional(uint256) _candidate_pubkey;
    uint256 public _pub_recovery_key;
    string public _zkaddr;

    //TODO: extra arg jwk modulus for first signIn (addZKP call). We are confident that constructor is called by honest entity
    constructor (
        string zkaddr,
        uint256 pub_recovery_key,
        uint64 value
    ) {
        gosh.cnvrtshellq(value);
        require(tvm.pubkey() != 0, 101);
        require(msg.pubkey() == tvm.pubkey(), 102); 
        tvm.accept();
        _zkaddr = zkaddr;
        _owner_pubkey = msg.pubkey();
        _pub_recovery_key = pub_recovery_key;
    }

    function getTimeStamp() external pure returns (uint32) {
		return block.timestamp;
	}

    function updateRecoveryPhrase(uint256 pubkey) public onlyOwnerPubkey(_owner_pubkey) accept {
        _pub_recovery_key = pubkey;
    }

    function updateZkaddr(string zkaddr) public onlyOwnerPubkey(_owner_pubkey) accept {
        _zkaddr = zkaddr;
    }

    function updateSeedPhrase(uint256 pubkey) public onlyOwnerPubkey(_owner_pubkey) accept {
        _owner_pubkey = pubkey;
    }

    //TODO: for the first time it should be called with modulus preset in constructor?
    function addZKP(
        bytes proof,
        uint256 epk, 
        bytes modulus, // be careful with this jwk modulus
        uint8 index_mod_4, 
        string iss_base_64, 
        string header_base_64,
        uint64 expire_at) public 
    {
        require(block.timestamp <= expire_at, ERR_FACTOR_EXPIRED); // TODO: block.timestamp + min_time_period <= expire_at
        bytes ph = gosh.poseidon(index_mod_4, expire_at, epk, modulus, iss_base_64, header_base_64, _zkaddr);
        require(gosh.vergrth16(proof, ph, 0), ERR_WRONG_CUSTODIAN);
        //TODO: be careful that poseidon and vergrth16 can be done before tvm.accept, gas price issue is not resolved yet?
        tvm.accept();
        _factors[epk] = expire_at;
        
        //this.tryCleanOnlyMinZKP{value: 0.1 ton, flag: 1}();
        this.tryPartiallyCleanZKP{value: 0.1 ton, flag: 1}(1);
    }

    function sendTransaction(
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        uint8 flags,
        TvmCell payload) public view
    {
        require(_factors.exists(msg.pubkey()), ERR_WRONG_CUSTODIAN);
        require(block.timestamp <= _factors[msg.pubkey()], ERR_FACTOR_EXPIRED); // TODO: block.timestamp + min_time_period <= expire_at
        tvm.accept();
        dest.transfer(varuint16(value), bounce, flags, payload, cc);
    }
    
    function changeSeedPhrase (
        uint256 pub_seed_phrase) public {
        require(_factors.exists(msg.pubkey()), ERR_WRONG_CUSTODIAN);
        require(block.timestamp <= _factors[msg.pubkey()], ERR_FACTOR_EXPIRED); // TODO: block.timestamp + min_time_period <= expire_at
        require(!_candidate_pubkey.hasValue(), ERR_CANDIDATE_EXIST);
        tvm.accept();
        _candidate_pubkey = pub_seed_phrase;
    }

    function acceptCandidateSeedPhrase() public onlyOwnerPubkey(_pub_recovery_key)  {
        require(_candidate_pubkey.hasValue(), ERR_CANDIDATE_EXIST);
        tvm.accept();
        _owner_pubkey = _candidate_pubkey.get();
        _candidate_pubkey = null;
    }

    /*function deleteCandidateSeedPhrase() public {
        require(_factors.exists(msg.pubkey()), ERR_WRONG_CUSTODIAN);
        require(block.timestamp <= _factors[msg.pubkey()], ERR_FACTOR_EXPIRED); // TODO: block.timestamp + min_time_period <= expire_at
        tvm.accept();
        _candidate_pubkey = null;
    }*/

    /*function tryCleanOnlyMinZKP() {
        optional(uint256, uint64) min_pair = _factors.min();
        if (min_pair.hasValue()) {
            (uint256 epk, uint64 expire_at) = min_pair.get(); 
            if(block.timestamp > expire_at) {
               delete _factors[epk];
            }
        }
    }*/

    function tryPartiallyCleanZKP(uint8 num_iter) public {
        tvm.accept();
        optional(uint256, uint64) pair = _factors.min();
        repeat(num_iter) {
            if (pair.hasValue()) {
                (uint256 epk, uint64 expire_at) = pair.get(); 
                if(block.timestamp > expire_at) {
                    delete _factors[epk];
                    break;
                }
                pair = _factors.next(epk);
            }
            else {
                break;
            }
        }
    }

    function deleteZKP(uint256 epk) public onlyOwnerPubkey(_owner_pubkey) {
        require(_factors.exists(epk), ERR_WRONG_CUSTODIAN);
        tvm.accept();
        delete _factors[epk];
    }

    function exchangeToken(uint64 value) public pure {
        tvm.accept();
        gosh.cnvrtshellq(value);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "Multifactor");
    }

    
}
