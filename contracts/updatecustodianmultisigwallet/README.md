**UpdateCustodianMultisigWallet** – A multisignature wallet with support for upgrade and SHELL exchange features.

- **Code Hash (sha256)**: `22dd0159fcdc7c0b1cc8b5bb2ebd18f44ed243a1d04f54b762d17be5d10957eb` (compiled with `sold` vsold 0.79.2 (commit.ac412858))

### Building `UpdateCustodianMultisigWallet` with `sold`

`sold` is an all-in-one compiler and linker for the TVM Solidity language, available as a single binary.

To manually build `sold`, follow [this guide](https://github.com/gosh-sh/TVM-Solidity-Compiler?tab=readme-ov-file#build-and-install), or download the binaries directly from [here](https://github.com/gosh-sh/TVM-Solidity-Compiler/releases).

To compile the `UpdateCustodianMultisigWallet` contract:

```bash
sold --tvm-version gosh  UpdateCustodianMultisigWallet.sol
```

