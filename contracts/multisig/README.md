**Multisig** – A multisignature wallet with support for upgrade and SHELL exchange features.

- **Code Hash**: `f40a266df7336124e93775054eeb8cd6311a3e51433e071c35e50e7c5e50ec99` (compiled with `sold` v0.76.0)

### Building Multisig with `sold`

`sold` is an all-in-one compiler and linker for the TVM Solidity language, available as a single binary.

To manually build `sold`, follow [this guide](https://github.com/gosh-sh/TVM-Solidity-Compiler?tab=readme-ov-file#build-and-install), or download the binaries directly from [here](https://github.com/gosh-sh/TVM-Solidity-Compiler/releases).

To compile the multisig contract:

```bash
sold multisig.sol
```

