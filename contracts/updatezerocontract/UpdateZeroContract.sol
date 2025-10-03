pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

contract UpdateZeroContract {

    constructor() {
        tvm.accept();
    }

    function updateCode(TvmCell newcode, TvmCell cell) public view  {
        require(msg.pubkey() == tvm.pubkey(), 100);
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private pure {
    }

    function getVersion() external pure returns(string, string) {
        return ("1.0.0", "UpdateZeroContract");
    }
}