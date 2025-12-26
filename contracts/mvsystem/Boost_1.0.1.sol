// SPDX-License-Identifier: GPL-3.0-or-later

pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/VerifiersLib.sol";
import "./MobileVerifiersContractRoot.sol";
import "./PopitGame.sol";
import "./Miner.sol";

contract Boost101 is Modifiers {
    string constant version = "1.0.1";
    address _wallet;
    address static _popitGame;
    address _root;
    uint64 _mbiCur;
    uint256 _rootPubkey;
    address _miner;
    bool _hasSimpleWallet = false;

    event BoostNewVersionUpdated(string version);
    event BoostNewHasSimpleWallet(bool value);

    constructor (
        address wallet,
        uint256 rootPubkey
    ) senderIs(_popitGame) accept {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (, address root) = abi.decode(data, (string, address));
        _root = root;
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
        Miner(_miner).setMbiCur{value: 0.1 vmshell, flag: 1}(0);
    }

    function  setSimpleWallet(bool value) public senderIs(_wallet) accept {
        ensureBalance();
        if (_hasSimpleWallet != value) {
            address addrExtern = address.makeAddrExtern(BoostNewHasSimpleWalletEmit, bitCntAddress);
            emit BoostNewHasSimpleWallet{dest: addrExtern}(value);
        }
        _hasSimpleWallet = value;
    }

    function  setMbiCur(uint64 mbiCur) public onlyOwnerPubkey(_rootPubkey) accept {
        ensureBalance();
        _mbiCur = mbiCur;
        PopitGame(_popitGame).setMbiCur{value: 0.1 vmshell, flag: 1}(mbiCur);
        Miner(_miner).setMbiCur{value: 0.1 vmshell, flag: 1}(mbiCur);
    }

    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerPubkey(_rootPubkey) accept {
        ensureBalance();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        tvm.resetStorage();
        (_wallet, _popitGame, _root, _mbiCur, _rootPubkey, _miner) = abi.decode(cell, (address, address, address, uint64, uint256, address));
        address addrExtern = address.makeAddrExtern(BoostNewVersionEmit, bitCntAddress);
        emit BoostNewVersionUpdated{dest: addrExtern}(version);
        PopitGame(_popitGame).setMbiCur{value: 0.1 vmshell, flag: 1}(_mbiCur);
        Miner(_miner).setMbiCur{value: 0.1 vmshell, flag: 1}(_mbiCur);
    }

    function destroyNode() public senderIs(address(this)) accept {
        selfdestruct(address(this));
    }

    function getDetails() external view returns(uint64 mbiCur, address wallet, bool hasSimpleWallet) {
        return (_mbiCur, _wallet, _hasSimpleWallet);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "Boost");
    } 
}