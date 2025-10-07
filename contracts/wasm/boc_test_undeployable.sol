pragma ton-solidity >=0.39.0;
pragma AbiHeader expire;
pragma AbiHeader pubkey;


contract Test{
    TvmCell public data;

    constructor(uint64 value) {
        // Call the VM command to convert SHELL tokens to VMSHELL tokens to pay the transaction fee.
        gosh.cnvrtshellq(value);
        // Ensure that the contract's public key is set.
        require(tvm.pubkey() != 0, 101);
        tvm.accept();
        Test(this).run{value: 0, flag: 128, bounce: false}();

    }

    function run() public{
        TvmCell d = data;
        TvmBuilder last;
        last.store(d);
        last.store(d);
        last.store(d);
        last.store(d);
        data = last.toCell();

        Test(address(this)).run{value: 0, flag: 128, bounce: false}();

        tvm.accept();
    }
}


