pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

contract MVConfig {

    uint64[] _MBNLst;
    uint256 _rootPubkey;

    constructor() {
        tvm.accept();
    }

    function setConfig(uint64[] MBNLst) public {
        require(msg.pubkey() == _rootPubkey, 100);
        tvm.accept();
        if (address(this).balance < 100 vmshell) {
            gosh.mintshellq(100 vmshell);
        }
        _MBNLst = MBNLst;
    }

    function setPubkeyRoot(uint256 pubkey) public {
        require(msg.pubkey() == tvm.pubkey(), 100);
        tvm.accept();
        if (address(this).balance < 100 vmshell) {
            gosh.mintshellq(100 vmshell);
        }
        _rootPubkey = pubkey;
    }

    function setPubkey(uint256 pubkey) public {
        require(msg.pubkey() == _rootPubkey, 100);
        tvm.accept();
        if (address(this).balance < 100 vmshell) {
            gosh.mintshellq(100 vmshell);
        }
        _rootPubkey = pubkey;
    }

    function updateCode(TvmCell newcode, TvmCell cell) public  {
        require(msg.pubkey() == tvm.pubkey(), 100);
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        tvm.resetStorage();
    }

    function getVersion() external pure returns(string, string) {
        return ("1.0.0", "MVConfig");
    }
}