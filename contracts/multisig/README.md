**Multisig** – A multisignature wallet with support for upgrade and SHELL exchange features.

- **Code Hash**: `fd4a27165a071c289c0db32d30026d17bd4cfa8a504d6d43a8cf20c21b9283f8` (compiled with `sold` v0.77.0)

### Building Multisig with `sold`

`sold` is an all-in-one compiler and linker for the TVM Solidity language, available as a single binary.

To manually build `sold`, follow [this guide](https://github.com/gosh-sh/TVM-Solidity-Compiler?tab=readme-ov-file#build-and-install), or download the binaries directly from [here](https://github.com/gosh-sh/TVM-Solidity-Compiler/releases).

To compile the multisig contract:

```bash
sold multisig.sol
```

