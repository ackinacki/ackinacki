// SPDX-License-Identifier: GPL-3.0-or-later

pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/VerifiersLib.sol";
import "./MobileVerifiersContractRoot.sol";
import "./PopitGame.sol";

contract Boost is Modifiers {
    string constant version = "1.0.0";
    address _wallet;
    address static _popitGame;
    address _root;
    uint64 _mbiCur;
    uint256 _rootPubkey;

    constructor (
        address wallet,
        uint256 rootPubkey
    ) senderIs(_popitGame) accept {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(VerifiersLib.versionLib == lib, ERR_INVALID_SENDER);
        _root = root;
        require(msg.sender == _popitGame, ERR_NOT_OWNER); 
        _wallet = wallet;
        _rootPubkey = rootPubkey;
    }

    function ensureBalance() private pure {
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function  deleteMbiCur() public senderIs(_popitGame) accept {
        ensureBalance();
        _mbiCur = 0;
    }

    function  setMbiCur(uint64 mbiCur) public onlyOwnerPubkey(_rootPubkey) accept {
        ensureBalance();
        _mbiCur = mbiCur;
        PopitGame(_popitGame).setMbiCur{value: 0.1 vmshell, flag: 1}(mbiCur);
    }

    function getDetails() external view returns(uint64 mbiCur, address wallet) {
        return (_mbiCur, _wallet);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "Boost");
    } 
}