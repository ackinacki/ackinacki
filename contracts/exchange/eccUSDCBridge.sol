pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./DepositVoucher.sol";
import "../token/interface/ISubscriber.sol";

interface IShellAccumulator {
    function buyShellFor(address buyer) external;
}

/// @title eccUSDCBridge
/// @notice The name covers three distinct flows that share storage / owner key
///         but are otherwise independent:
///
///         1. TIP-3 USDC → ECC[3] stripe-mint gateway
///            `onTransferReceived` (ISubscriber callback) — fires when the
///            bridge's TIP-3 USDC TokenWallet receives a transfer. Mints
///            equivalent ECC[3] USDC and forwards to the original depositor.
///            One-way: TIP-3 → ECC. Counter: `_totalMinted`.
///
///         2. Owner-mint admin path
///            `mintAndSend` / `mintAndSendAccumulator` — owner-key (pubkey)
///            mints ECC[3] USDC and dispatches to recipient / Accumulator
///            buyShellFor. Nonce-protected (`_mintNonce`,
///            `_mintAccumulatorNonce`) against off-chain replay of signed
///            mint requests.
///
///         3. Cross-chain bridge — USDC-only (tokenId == USDC_ECC_ID)
///            `initiateWithdrawal` (burn ECC + emit `WithdrawalInitiated`) and
///            `finalizeDeposit` / `confirmDeposit` (verify proof, deploy
///            deterministic `DepositVoucher` for anti-replay, mint ECC).
///            The destination chain of a withdrawal is opaque; the SOURCE of a
///            deposit is proof-bound (chainId + emitting contract) and gated by
///            the owner-managed `_trustedL1Bridge` allowlist.
///            Counters: per-tokenId `_totalMintedBridgeByToken` /
///            `_totalBurnedBridgeByToken` (mapping kept for forward-compat).
///
///         Deployed at fixed address in zerostate.
contract eccUSDCBridge is eccUSDCBridgeModifiers, ISubscriber {
    string constant version = "1.3.1";

    event UsdcMigrated(address from, uint128 value);
    event UsdcMinted(address recipient, uint128 value);
    event WithdrawalInitiated(
        uint256 dstChainId,
        bytes recipient,
        uint128 amount,
        uint32 tokenId,
        address sender
    );
    event DepositFinalized(
        uint256 depositId,
        uint256 contractAddr,
        uint256 dappId,
        uint256 chainId,
        uint128 amount,
        uint256 anAccount
    );

    /// @notice Deposit fields read out of the proven public-inputs blob.
    struct DepositPI {
        uint256 depositId;     // fr[0] — anti-replay anchor (per source chain/contract/dapp)
        uint128 amount;        // fr[2]
        uint256 contractAddr;  // fr[3] — L1 bridge contract that emitted the event
        uint256 chainId;       // fr[4] — source L1 chain id (EIP-1559 tx proof-bound)
        uint256 dappId;        // pinned to 0 — see _parsePublicInputs
        uint256 anAccount;     // fr[7]<<128 | fr[8] — AN recipient (256-bit, proof-bound)
    }

    uint256 _ownerPubkey;

    // TokenWallet address for TIP-3 USDC bridge (one-way: TIP-3 -> ECC[3])
    address _usdcWallet;

    // Total ECC[3] USDC minted by the stripe (TIP-3) bridge
    uint128 _totalMinted;

    // Nonces for double-spend protection
    uint64 _mintNonce;
    uint64 _mintAccumulatorNonce;

    // Cross-chain bridge accounting per tokenId (any external chain; independent
    // of stripe `_totalMinted`). No invariant enforced between minted/burned —
    // intra-AN ECC distribution can outpace deposits, so on-AN withdrawal can
    // exceed historical deposits for the same token. The split is for
    // observability / per-token analytics, not for on-chain checks.
    mapping(uint32 => uint128) _totalMintedBridgeByToken;
    mapping(uint32 => uint128) _totalBurnedBridgeByToken;

    // Code of DepositVoucher contract — deployed per inbound deposit for replay protection
    TvmCell _depositVoucherCode;

    // Source-chain allowlist: L1 chainId -> SET of bridge contracts on that
    // chain whose deposit events this bridge accepts. A deposit passes if its
    // proof-bound (chainId, contractAddr) hits any present entry — so an L1
    // bridge rotation can keep the old and the new address trusted at once for
    // a migration window, then drop the old one. An absent entry (false) means
    // that address is not accepted. Owner-managed via `setTrustedL1Bridge`;
    // deliberately NOT carried through `onCodeUpgrade` — after a code upgrade
    // the bridge accepts no deposits until the owner re-seeds it (fail-closed).
    mapping(uint256 => mapping(uint256 => bool)) _trustedL1Bridge;

    // ZK verifying key (VkBlob) for the FINAL ETH-deposit circuit
    // (receipt-proof of an L1 deposit event, 12 public inputs — chainId added
    // at instance index 4). Keyed on the Hermez Perpetual Powers of Tau SRS
    // (s_g2 = 928fafb3…). Source:
    //   tvm-sdk@feature/deposit_circuit_chain_id_pi (commit 8615745c):
    //   tvm_vm/halo2_test_data/deposit_10proofs/deposit_vk_blob.bin
    // Byte-identical to bridge canonical
    //   bridge/deposit-prover/fixtures/deposit_10proofs/deposit_vk_blob.bin
    // (verified via keygen-only regen from current circuit_v2.rs on 2026-08-07).
    // Magic "VKBLOB\x00\x00" + version 2, shape "Rlc". 5006 bytes;
    // sha256 = 9dacd998af5fd03af8097cb80a571df098c925bba235af61d920cc808360fae3.
    // To rotate: regenerate via deposit-prover `export_vk_blob` and replace
    // the constant below (and the fixtures under tests/exchange/fixtures).
    bytes constant VK_BLOB =
        hex"564b424c4f4200000200010000000000ec0000007b22726c63223a7b2262617365223a7b226b223a31382c226e756d5f6164"
        hex"766963655f7065725f7068617365223a5b31372c31335d2c226e756d5f6669786564223a312c226e756d5f6c6f6f6b75705f"
        hex"6164766963655f7065725f7068617365223a5b312c312c305d2c226c6f6f6b75705f62697473223a382c226e756d5f696e73"
        hex"74616e63655f636f6c756d6e73223a317d2c226e756d5f726c635f636f6c756d6e73223a327d2c226b656363616b223a7b22"
        hex"636f6d705f6c6f616465725f706172616d73223a7b226d61785f686569676874223a302c2273686172645f63617073223a5b"
        hex"36345d7d7d7d8a1200000212000000002400000058c338696a199ecb26464c9bdedd6f46e6fa0c9974d91b9fcaa627c329df"
        hex"f21a4e7b0e1f40caf730f9cb23b5583598194673b2bc3c7f181c20266ff8b4d3cf263c5fe8e4306d821943491717d0628ffd"
        hex"ba76fcecc7821125a36b7248605e5f23761a6622b0bab9300986d70b8e5f49efe221acd3f85713f09de804432b80930b5fd2"
        hex"1dccbc7228aff95afe27989687390e02314dbb76851aca0f0eed3554fc18b3be9a3a4da11db71bf86500e86212ab3b3c524d"
        hex"02c197c14dd773879e342c058175537a1dc6dfc62cbc14ec73a57991d051b3e8e7008002f8469672c51d7e1ef9c6dd3592c2"
        hex"f442d338f77c0fcf2fbf435f14d811e778d32418d496eb6e4c18ef2a097b473418d4da72ee708a6b7aa4dc14912f8f8049fd"
        hex"838e12fdc22db81f3ae5d1abb6834b98f06b160f2402e5368e5a0e5eae6437eb1096346ad5e3ef297e9d67129484b9797c0c"
        hex"12c21a0f171fa8035f7831455362885767e579397306f0300e82850ab65baf1a2c6413e6b4b26a3825f17c67c1128e55dc01"
        hex"eaa1321775436210f7b4c48fb0d03b39b3ddbe7a2a902bec503ecb50008f63ebae412918b7b6ebafd72e8145607fc294acc3"
        hex"08adc81c0d985bdfadd2e0f4297ea2f05b111787ab3f4537bc311d35a40b6a156bdc03c7c17f6110352baeac0ca4d6837225"
        hex"0f5c7c76d89c1d5550a57d13f17e67b98a1c1a71ca9de880486a8b10647d332eb9ff59c66b6a56ed01c2dd35b6d93ba36b04"
        hex"bc2bf1a6fd5614d1b2f8c6476e0031f7fbdf4532a47e07ad3808551616565afbce736e174807a458e7c5f0ad5c21cacdf703"
        hex"04b8136bf95b60a13d7d33633adae6395c4c3acdaf8e05828414862c7518f494da8f47fd68f854413fc6917c8818a8c78128"
        hex"6a0a82d25bcdd58f9e0b0a4b31b8ee5ff6eda8b5c4fb3502b167b186dd22e00ee9284d8a7d661d7e1202a74fa70b29d3e084"
        hex"d0121a66df1b6a87c9162b77a20b79cc785803e3ef2e1c2edf78611a1017746a8bb4ed98b9dde95e471130760b46db39f142"
        hex"7c7f14118e2bea2730dee6c55a7a502883359d80b9f937d1d6dc61ee7126e18707567b6b841255f461f426bbf28ca86293f8"
        hex"3687830e54385d9b5743d8a4e392288174f6dd0df25695a24d2fd554368ef8c41f2b45fdd037ac01b8e8ccf7f0f6d3c3857c"
        hex"f7081f7c7e7ad8449e295b7a6656e4fcafa40ded1d39d92b818a99bce25cf1d08c212347730df4d54c6f85a6edbe67d06014"
        hex"654168d00f7a23a709f01abdc0a98d18c76ee5bce93da6baa42eda8b4992f83e3b9fc900c671a6dec1f8039f5515bc162673"
        hex"00f1b4711430de437f8b91a8cfe9f1f4e8e3b5fefd5135e50a88e872cd2681c677121c3981041154c711afaefcebd2e065cb"
        hex"f627e595274eb594b001d014b3a432f192f08b0bfa3713754830bd36e875b4c6173b3f7408077f973f786d20c04fbf93ecf2"
        hex"6d9d8f1a625f22b7e3beb48f2a6d6d6a539d87fdb70b8739561c1cff5483251c4eec6e7d9845f7fadf43bc91f25cdb18d446"
        hex"bd63979516f2f42b6aa54609a9f8b6da2cb6e1e4908d4d41d5552bbc508211883d21d287040ad90dd498ff381e8c325587cc"
        hex"4248247b779cc02b46998e9c9df732aeb650abf8cd15dc12e6b3cb77afae0b6b74f0506047cba035f511fbe3704d6f8be17b"
        hex"8b40020666ea07c515e4f2c9315187e24e6aa4b34ebe0cec2cf0e765fc8a0e2ad500061245f5f0350f1731f025a6593756ea"
        hex"e400fb453197621c26caf83154ab58ddbb140043e8e85a1ea61c2b003d5036b275b3592f0d9c23d077c6b5e202dda41e382b"
        hex"dbe685857049e51232e79d5c726369e76226a1856b35f7d8099215fac7854009ddd3a20d0390434a7d88a1b758ded170386c"
        hex"40210593dcff1050233e1e738d1094e491ba93e43211e392910b3ddc450be0674db64f37b16f739e390d1bbab0180a6cab91"
        hex"fd77bb16a3ffcfcef7001d8f295c4ebd73708524db7a46889d51b2001bcf5bff9887f0592419d4b9ac2cd0faef322a90baff"
        hex"1f2f76154c39b41de82aac47ba03e73a3ab987e68c8d7ba2c663ec3166d2fcaa09d68c2dfe7c6b0bd32bd7d83ad76d31e7c4"
        hex"e32ee2a76493510a25a2ca22c9dbd7027f9d1665b6d5542f91d4220bd73c6db8254f02cb1703975ba426dc84820a63f80166"
        hex"b1029a4a95281f020cb11e4ce3fdf322487aac31dee2b8af94c6766cb7bbc6e76e03f09b1229b3a6325e0b8a5f3f15d9a013"
        hex"bd50314f65245b182ed096036610c95e6c61111f41d96069893b17a23322789f7a9ff0a58c0ce759236cab7e5fdcd98d5d17"
        hex"6b112ee80f0de3f424ba597a31c7804b7b82a32de6acb74ae6ac7fb37eaf715df32f14d6fafce908de9bcdd831b91e4d053e"
        hex"315647e3c5036c2a25b40f6cc8e3710a35dcc890504a4de880b608cffdf1ee1cf04a8d35a79588bf9dece040ba47dd2a0f40"
        hex"3a38eaa9c2c5fdcbfd810f87e8fdaf83f54436f4d0f385f8c5698009e41bdd1067bb5ae8079664c26d235199b48317620175"
        hex"35d9894183cf2d4543c6a02b8bf34bbbafec7675c4957b8444904cee62d192aaf75c8d2f42fb4e85d61da428de867a1a717c"
        hex"c0eec7de9ba178c671be17c1dfda0446558491d88e9066d7da0eead2aec05e1754737734dd929ee1aeda274ba4a3a748d8dc"
        hex"e77fb037ec72e02a21f80c6636581bd495fce13b52b16b4236b7fdc4b53285951819c9dd6cb12313ef621078706473e424ad"
        hex"0ceb50f72e9f80d93a15dcd3176bf5fff9d4815d7d0cabac330c2d73977efc101d66a787c915ce9cb5092cc7ff96aef0a78a"
        hex"0f70370defaf6b15375eb840bd653b6700675f71195b21c8a157d66de8d0912ce1f5911de5311a5eb9dfe283bc8b1d089a7f"
        hex"779deed6a32cb87c2f9eb6060e6c886e5902a0113cdd1251d1273fba7674b6e6d14f67c0c7df0874d6b395426081e8a2f801"
        hex"369d18e286267683b67094e8b2ad12f99bf41b256b1d203912b4fc75e34b221a4a11b5fb3a6256e8de628ca35e9cb6e2ae30"
        hex"325fb9f83b5c0cf56bf006639f0d71ced889f722a66e88abf3960ed437975ec91fa3e9e2d03475abadde36106105bec07cc8"
        hex"6b05579141345a51a803764bd8befbe8dbcd23a5c30d958e60c7e21dfe7592ff0ae07dec2550b1f6991cc79d01e634257a97"
        hex"e89c016676c26b531c2239b6aba10646df33bfcb2ea3c007356cf038db7aaaa5d6fb8c95ebce4dd8f525f3b7d462ca520635"
        hex"1dddec032d4ef725be1207b76ee2dc0448ed4712dec14203b4e47ec9790775f84eba13bc71ce5b9af9d1ffda5364adcccdc9"
        hex"c4af4d0b3e227e92325c55998410d9034f9a3f0a66ac7e1a4208cec82feb85b3b8fe598514280e911fabcbd233fec1a6d618"
        hex"b0501cfe363e2508671cc47dbb976b5f2ab390207fa4cea27079c33243ebd17b49e351201f26ac7db57615d9432f82a2dabf"
        hex"451d225bd28db3865e3ec2d4a439e1dbccb0f2b7fe8135d31f21a852bc0a170f721c4cb98c4fc4cfb496994812172c927ff6"
        hex"17cbfd8cc0e25bbd8628e284e3f72e24f3605cebe0a84da1f00310335c9af460271099819ca4c85d8fa43d9ffab5e4272dc6"
        hex"72cdf182f9b25864706ac010155f9e123e2ff827940e14407d53e565cb2f80b42e86a3fe0f5a9d5f0ec1013984bd132249f6"
        hex"9978085cd7853a9907a54622e8096bee5bfc77ea01d0cb8e46ece8f3bf61881eda9be5536d98f9cb2d087420f7699f913103"
        hex"38616d7c8b68ec44065edf4257036460dbb0a39647022769502064784b15111865d6942dc23128dcec08d427e2e0ab869f3e"
        hex"202cb70bdf4f87164e0873e8300a02a342559bf43143ded6ab5aa3679a49044f198cec552b84fe1bdd17974cea18496802ab"
        hex"37ef3d27adb4be9df6411fef3617f73693774f9280232eaba2b5b9a4897f7432a0bcb0ed4a41630a94a3b048c66e8c7949d5"
        hex"f1ec372c2a8171ede20ce0dccff2b51716eb1a0d48fb8d122a0de6aac1820219d29085294c3025672a4646a5600249eb1da8"
        hex"cd4920ad5923e41a809b3674ba2bff2c242489d08ce7f7650ad5c9c403e6e5f5d7890fe70cb8f3ed22673dd5e944c2efa10d"
        hex"5beb4fd4eb7ff7e46d18e36f43a9570486676cd95dea26b8ea97b5cbb32bb52ecdcbd9db165c0da595b1e36adff57020f322"
        hex"b66d91d9d7286a97c77a977b6b23ed7b9d389ba6876449d146a45caa8d0d27dbee4b55f0c62b41b228c62c72f2189ea40210"
        hex"65b9524dce95e4c7aa8cdc952dac3fb805900424052b03c786f8d51a1245731dcfa56b5602a60159e49941941e5822f124ac"
        hex"c76a0129df0be513e12cceee3a3eef2544d1c336f180e020952ce72e4c66fc0e5afe6955051358ad3003cf2064844c2fd4bd"
        hex"fc8508b38593780bf690181a2694f92c4a022dc04aed12069c94345c8db15b7c4576a69f97f347f399871b5e324716d40b96"
        hex"ea2cb324840b214932f3ef72775e6defe0aa2a061e658cbcbb6e1165cbe0dc20d14b30be2f2217411278e151b142531db87b"
        hex"bca58ce902a124571d425a99a0bcc5d47880481ee7fad587b3e5fc05b1f352d0def52e3aa0aae5a6f0d2ac1ad30d3d311935"
        hex"731c9d029f379042c37a942a493052f89044eca06340b476d32f0bd7f3f355ad4817fd0538fb43e6a7919a2180f440a20d3d"
        hex"1c770727f51df94e8192a55518f9122ff912279537d80e0ad5e748177f15c302ba2af36e9a75b27b715ccf7ee45dda21f1be"
        hex"7cf5ad7c1a1f50aee4e98a9b85c96772f3c591b73e8b1ced40faf76bf2074c6a505cb8faf14a8fa04e81a9d2f436a187b28d"
        hex"6542c3ae9c78653a80712428a034ba767a06f76224ea0b67b60fab6e605512e2d99f7e85b62f70485029d0158e2ace1752e3"
        hex"c1494fdd925235f2164284e67b14d2b739e929eec7d7ec04dc1d68f73384d6aaf593fb455a7eae3c07284f41cfd61d98750e"
        hex"998cb5816efc0f099036623ca2d4a469aed5ac06d0f0f5a1be9651fd9f674eb4193d8e380b2710295b97afc6d82f30d52e2f"
        hex"e83003b2eab970afb68eaabfbd71e657eba06503e522865368a100fe84e67f794ba3324ec4e326609d3eb99ce6a91ab51a2b"
        hex"8e5277217b23a64ea97964d116c459de5ce215aeedd089bd25cd26f52b05abd73f4f8b03ccdb740bb5d75c1cd3b8824b5ae3"
        hex"bb6c8cb6340cb4c5f1c2081bfc248967e51aedb6dc4d9fe1555978cc58c9fd063e5f4ba2352407aae2f9a919ba60bbe7381d"
        hex"9078e99e7ed4e5a2c378b652146b5319389119e46ecc45d4b0d5ac63c3b9bd1e6cfced9053f8cd3dc1719b530a7b288eb5e9"
        hex"fc1402181b8fd33402eca7ecd40f9b1f41d423e9037e3ce5a0e4a1cb10f6e71fbe6e67e523088bdb5d182a75721c92dcfebf"
        hex"5735776d339d1d7277e86ace1b0e93aae1d0deced11bc151008ae101fc9449b8d00fcdaffc8c7690a42c4874710484e927d0"
        hex"7913c8818430a31ea82e8bf72f65a90bee2dc4f8217ff3b2a2a92cbb4e485d4b8fdc7041ef638eb10c2592ef6793209e0a00"
        hex"d635dc506f3427a153fd1e1c4d2f2542679a33886021e21097adeba70179dd758f225ff82c376667297fc466b12d073063a3"
        hex"f2151cfb21059b42270f4ac64f1c0f2bfa368b575a1fdd24ca99c33281460c8552302a15d82233e99a3b13a8e53b08358581"
        hex"adbd4e2303bc3aae485cf0873ab7e5c88bee8a2f9a50811c82c394553cffa8034c3a9672e35301b434745dda6c8bb8a00fa3"
        hex"7b0e769f19c723928f3c3c828e1e4f6460be300890cd4f20aa19932da3dc5c0e511d72ada43af2618a808a02c907b05aec6d"
        hex"944901dc08ca4d4098003f28c8616f1eeefc5d1e30f28beaf2f4f2dba93a4f11174ecf64b7ce554d177aa1cc1b8d7404754e"
        hex"ff61faaea0723eedd455ddc40d7b8f6c52be7a49b6b9aa2e48333bab64196d1ef3077248692615b007a0b05df067b5e3a7f3"
        hex"f4c038b09a098c33b4a5131158eb86b0af4e87f38fe0b8637edde73b6413e43396fcf735a2b70c47e67b861a8b13c86683a0"
        hex"45c2ac148ed62548e1793bd378c71b3929b2a1558520c41e4b021eee1fe82e09a9440ec59d1428f7a774033c28679174b4a4"
        hex"87046efe7b13c220e15587099f5ed94688bbde9b6751678fa406ef398427bf6f81467d61de3d6c1c4e473ff66fd250365bae"
        hex"77aefdb6564aa32e50a9fe0f14d1f866c14de14e4804da57857d02a2220a227b238b326cecca71f4af8990285781ff42946d"
        hex"7839c01e69d87e4e7a6ea746db3d4b378af123165f4b00bf4236017024643439ab84e817dacb28d544ff80854c938d9d2baf"
        hex"bf3dea2f6dc23db0153123ec8f142dffef1b4a516663c2cd0eb6bdd103c2afd885c7afafc824102823d99583a21632b21911"
        hex"644f6509820d6207f56d83bb879db7ae080cd4e3b31b2cd6b14548918978270b39542579e7abbd4c1ef890d1a1716e3f2456"
        hex"53a129621841150457948873c406cd2c40ab8b03b098cbc5fd758bd2418342ef2b37401373d5dcbca412005c2d06a04a7770"
        hex"d588c12bed62df2cae3171aab098adbca7588f99ba7543fc816e1317dbd4ae57cff424d134208c5b7a81f0f71a3976d82e06"
        hex"7b6d881ceb6aba9f4816a99d2c432bea9649eaf70862b05a226aae214bc9d2b7a7b7168261046194b9286213532123f45831"
        hex"df8f0d90880de3786c9042dae8425aec7f52fbcd7a16670da85e67a3554a97af98c18a1fec341f46090feeb5e4961955e0e9"
        hex"8254dbe6490b";

    /// @notice Contract constructor.
    /// @dev `_depositVoucherCode` is intentionally NOT a constructor arg:
    ///       in the only deploy path that matters (zerostate premine stub +
    ///       `updateCode` upgrade) the voucher code arrives via the
    ///       `onCodeUpgrade` payload. There is no standalone setter (B2 fix),
    ///       so the only way to populate / rotate `_depositVoucherCode` is a
    ///       full `updateCode` upgrade of eccUSDCBridge.
    /// @param pubkey — owner public key for admin operations
    /// @param usdcWallet — address of the Exchange's TIP-3 USDC TokenWallet (subscriber target)
    constructor(
        uint256 pubkey,
        address usdcWallet
    ) accept {
        _ownerPubkey = pubkey;
        _usdcWallet = usdcWallet;
    }

    /// @notice Ensures contract balance stays above MIN_BALANCE by minting vmshell if needed.
    function ensureBalance() private pure {
        if (address(this).balance >= MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    // ========================================================
    // TIP-3 USDC -> ECC[3] bridge (ISubscriber callback)
    // ========================================================

    /// @notice ISubscriber callback invoked by the bridge's TIP-3 USDC TokenWallet
    ///         when it receives a TIP-3 transfer. Mints equivalent ECC[3] USDC and sends
    ///         it to the original depositor. Only callable by _usdcWallet.
    /// @param from — address of the original depositor (wallet owner who sent TIP-3 USDC)
    /// @param value — amount of TIP-3 USDC received (in micro-USDC, 6 decimals)
    function onTransferReceived(
        address from,
        address /*to*/,
        uint128 value,
        uint128 /*balance*/
    ) external override {
        require(msg.sender == _usdcWallet, ERR_INVALID_SENDER);
        tvm.accept();
        ensureBalance();

        // TIP-3 USDC deposited -> mint ECC[3] and send to depositor
        require(value <= uint128(type(uint64).max), ERR_OVERFLOW);
        gosh.mintecc(uint64(value), USDC_ECC_ID);
        _totalMinted += value;

        mapping(uint32 => varuint32) ecc;
        ecc[USDC_ECC_ID] = varuint32(value);
        from.transfer({value: 1 vmshell, bounce: false, flag: 1, currencies: ecc});

        address addrExtern = address.makeAddrExtern(UsdcMigratedEmit, bitCntAddress);
        emit UsdcMigrated{dest: addrExtern}(from, value);
    }

    // ========================================================
    // Mint ECC[3] USDC and send to recipient (owner only)
    // ========================================================

    /// @notice Mints ECC[3] USDC and sends it to the specified recipient address.
    ///         Only callable by the owner (by public key).
    /// @param recipient — address to receive the minted ECC[3] USDC
    /// @param value — amount of ECC[3] USDC to mint and send (in micro-USDC)
    function mintAndSend(address recipient, uint128 value, uint64 nonce) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        require(nonce == _mintNonce + 1, ERR_INVALID_NONCE);
        require(value > 0, ERR_ZERO_AMOUNT);
        require(value <= uint128(type(uint64).max), ERR_OVERFLOW);
        _mintNonce = nonce;

        gosh.mintecc(uint64(value), USDC_ECC_ID);
        _totalMinted += value;

        mapping(uint32 => varuint32) ecc;
        ecc[USDC_ECC_ID] = varuint32(value);
        recipient.transfer({value: 1 vmshell, bounce: false, flag: 1, currencies: ecc});

        address addrExtern = address.makeAddrExtern(UsdcMintedEmit, bitCntAddress);
        emit UsdcMinted{dest: addrExtern}(recipient, value);
    }

    // ========================================================
    // Mint USDC and send to Accumulator for a buyer
    // ========================================================

    /// @notice Mints ECC[3] USDC and sends it to the Accumulator's buyShellFor,
    ///         which will process the purchase and send ECC[2] Shell to the buyer.
    /// @param buyer — address to receive Shell from the Accumulator
    /// @param value — amount of ECC[3] USDC to mint (in micro-USDC)
    function mintAndSendAccumulator(address buyer, uint128 value, uint64 nonce) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        require(nonce == _mintAccumulatorNonce + 1, ERR_INVALID_NONCE);
        require(value > 0, ERR_ZERO_AMOUNT);
        require(value % USDC_DECIMALS_FACTOR == 0, ERR_NOT_WHOLE_USDC);
        require(value <= uint128(type(uint64).max), ERR_OVERFLOW);
        _mintAccumulatorNonce = nonce;

        gosh.mintecc(uint64(value), USDC_ECC_ID);
        _totalMinted += value;

        mapping(uint32 => varuint32) ecc;
        ecc[USDC_ECC_ID] = varuint32(value);
        IShellAccumulator(ACCUMULATOR_ADDRESS).buyShellFor{value: 1 vmshell, bounce: false, flag: 1, currencies: ecc}(buyer);

        address addrExtern = address.makeAddrExtern(UsdcMintedEmit, bitCntAddress);
        emit UsdcMinted{dest: addrExtern}(buyer, value);
    }

    // ========================================================
    // Cross-chain bridge — outbound (AN -> any chain): burn ECC, emit proof-source event
    // ========================================================

    /// @notice Burns the ECC currency attached to this message and emits an event
    ///         carrying the data needed to mint the equivalent on the destination
    ///         chain. Exactly one ECC currency must be attached; its id and amount
    ///         are taken from `msg.currencies`. The destination chain is opaque
    ///         to this contract — `dstChainId` is just passed through to the event.
    /// @param dstChainId — opaque destination chain identifier (passed through to event)
    /// @param recipient  — destination-chain recipient bytes (≤64 bytes)
    function initiateWithdrawal(uint256 dstChainId, bytes recipient) public {
        tvm.accept();
        ensureBalance();
        require(recipient.length > 0, ERR_RECIPIENT_EMPTY);
        require(recipient.length <= 64, ERR_RECIPIENT_TOO_LONG);

        mapping(uint32 => varuint32) currencies = msg.currencies;
        uint32[] keys = currencies.keys();
        require(keys.length >= 1, ERR_NO_ECC);
        require(keys.length == 1, ERR_MULTIPLE_ECC);

        uint32 tokenId = keys[0];
        require(tokenId == USDC_ECC_ID, ERR_UNSUPPORTED_TOKEN);
        uint128 amount = uint128(currencies[tokenId]);
        require(amount > 0, ERR_ZERO_AMOUNT);
        require(amount <= uint128(type(uint64).max), ERR_OVERFLOW);

        gosh.burnecc(uint64(amount), tokenId);
        _totalBurnedBridgeByToken[tokenId] += amount;

        address addrExtern = address.makeAddrExtern(WithdrawalInitiatedEmit, bitCntAddress);
        emit WithdrawalInitiated{dest: addrExtern}(dstChainId, recipient, amount, tokenId, msg.sender);
    }

    // ========================================================
    // Cross-chain bridge — inbound (any chain -> AN): verify proof, deploy DepositVoucher, mint ECC
    // ========================================================

    /// @notice Owner-managed source-chain allowlist entry: add or remove ONE L1
    ///         bridge contract from the trusted SET of `chainId`. `l1Bridge` is
    ///         the L1 address left-padded to uint256, exactly as the circuit
    ///         exposes it in the public inputs (fr[3]). `allowed=true` trusts it,
    ///         `false` revokes it; several addresses may be trusted on the same
    ///         chain at once (rotation window). Applies to `finalizeDeposit`
    ///         only — the outbound path and the TIP-3/owner-mint flows are
    ///         unaffected.
    function setTrustedL1Bridge(uint256 chainId, uint256 l1Bridge, bool allowed) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        if (allowed) {
            _trustedL1Bridge[chainId][l1Bridge] = true;
        } else {
            delete _trustedL1Bridge[chainId][l1Bridge];
        }
    }

    /// @notice Returns the trusted L1 bridge SET for `chainId` (address -> true).
    function getTrustedL1Bridges(uint256 chainId) external view returns (mapping(uint256 => bool)) {
        return _trustedL1Bridge[chainId];
    }

    /// @notice True if `l1Bridge` is in the trusted set of `chainId`.
    function isTrustedL1Bridge(uint256 chainId, uint256 l1Bridge) external view returns (bool) {
        return _trustedL1Bridge[chainId][l1Bridge];
    }

    /// @notice Finalizes an L1 deposit proven by the final ETH-deposit halo2
    ///         circuit (receipt-proof of the L1 deposit event). The relayer
    ///         passes the proof and its public-inputs blob verbatim; we verify
    ///         against `VK_BLOB` and read every deposit field straight out of the
    ///         PROVEN instances — amount, recipient and source identity are all
    ///         proof-bound, nothing is caller-set. A deterministic
    ///         `DepositVoucher` (keyed on the proof-bound deposit identity) gives
    ///         replay protection; it calls back `confirmDeposit` to mint + pay.
    /// @param proof         — SHPLONK proof bytes (no header), fed verbatim as the
    ///                         `proof_cell` operand of TVM opcode
    ///                         ZKHALO2VERIFYWITHVK (0xC7 0x4A).
    /// @param publicInputs  — the circuit instance column: 12 × 32-byte LE Fr
    ///                         (deposit_id, sender, amount, contract, chain_id,
    ///                         dapp_hi, dapp_lo, an_account_hi, an_account_lo,
    ///                         + 2 block-hash halves + promise commit). Verified
    ///                         verbatim; business fields read at fixed offsets —
    ///                         see `_parsePublicInputs`.
    function finalizeDeposit(bytes proof, bytes publicInputs) public view {
        // Cheap parse + sanity BEFORE accept (within the pre-accept gas budget).
        DepositPI f = _parsePublicInputs(publicInputs);
        require(f.amount > 0, ERR_ZERO_AMOUNT);
        // The proof binds (chainId, contractAddr) to the L1 event; the allowlist
        // pins which (chain, bridge contract) pairs this side trusts. The deposit
        // passes if its proven address is in the chain's trusted set — an absent
        // entry is false, so unknown chains/addresses reject. The != 0 guard
        // keeps a stray `_trustedL1Bridge[chainId][0]=true` from ever admitting a
        // zero contract.
        require(f.contractAddr != 0 && _trustedL1Bridge[f.chainId][f.contractAddr],
                ERR_UNSUPPORTED_SRC_CHAIN);

        // accept() must precede the halo2 verify: ZKHALO2VERIFYWITHVK is a
        // multi-second WASM extern that vastly exceeds the external-message
        // pre-accept gas limit. Permissionless submission — the proof itself is
        // the authorization; a garbage proof only wastes the bridge's own gas.
        tvm.accept();
        require(
            gosh.zkhalo2VerifyWithVK(VK_BLOB, publicInputs, proof),
            ERR_INVALID_ZKPROOF
        );
        ensureBalance();

        // Anti-replay anchor = proof-bound (deposit_id, source contract, source
        // chain); the dapp component is pinned to 0 (see _parsePublicInputs).
        // chainId is IN the key: two different L1s may legitimately emit the
        // same (deposit_id, contract) pair. amount/recipient are NOT in the
        // key — they are fixed by the proof, so a replay can never re-route or
        // re-mint: same key ⇒ same voucher ⇒ no-op.
        uint256 depositHash = tvm.hash(abi.encode(f.depositId, f.contractAddr, f.dappId, f.chainId));

        TvmCell stateInit = abi.encodeStateInit({
            contr: DepositVoucher,
            varInit: { _depositHash: depositHash },
            code: _depositVoucherCode
        });

        new DepositVoucher{
            stateInit: stateInit,
            value: 2 vmshell,
            flag: 1
        }(f.depositId, f.contractAddr, f.dappId, f.chainId, f.amount, f.anAccount);
    }

    /// @notice Internal callback from a freshly deployed `DepositVoucher`. Mints
    ///         USDC ECC and sends it to the proof-bound AN recipient. The caller
    ///         must be the deterministic voucher address derived from the
    ///         deposit identity — replay attempts hit the existing voucher
    ///         account whose constructor was already consumed.
    function confirmDeposit(
        uint256 depositId,
        uint256 contractAddr,
        uint256 dappId,
        uint256 chainId,
        uint128 amount,
        uint256 anAccount
    ) public {
        uint256 depositHash = tvm.hash(abi.encode(depositId, contractAddr, dappId, chainId));
        TvmCell stateInit = abi.encodeStateInit({
            contr: DepositVoucher,
            varInit: { _depositHash: depositHash },
            code: _depositVoucherCode
        });
        require(msg.sender == address.makeAddrStd(0, tvm.hash(stateInit)), ERR_INVALID_SENDER);

        tvm.accept();
        ensureBalance();

        gosh.mintecc(uint64(amount), USDC_ECC_ID);
        _totalMintedBridgeByToken[USDC_ECC_ID] += amount;

        mapping(uint32 => varuint32) ecc;
        ecc[USDC_ECC_ID] = varuint32(amount);
        address.makeAddrStd(0, anAccount).transfer({
            value: 1 vmshell,
            bounce: false,
            flag: 1,
            currencies: ecc
        });

        address addrExtern = address.makeAddrExtern(DepositFinalizedEmit, bitCntAddress);
        emit DepositFinalized{dest: addrExtern}(
            depositId, contractAddr, dappId, chainId, amount, anAccount
        );
    }

    // DepositVoucher code rotation is intentionally not exposed as a
    // standalone setter. The only way to change `_depositVoucherCode` is via
    // a full `updateCode` upgrade of eccUSDCBridge (the new code+layout pass
    // through `onCodeUpgrade`). This removes the "owner can swap voucher
    // logic in one tx and free-mint" backdoor flagged in PR2112 review (B2).

    // ========================================================
    // Admin
    // ========================================================

    /// @notice Replaces the owner public key. Only callable by the current owner.
    /// @param pubkey — new owner public key (uint256)
    function setPubkey(uint256 pubkey) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _ownerPubkey = pubkey;
    }

    /// @notice Sends a plain transfer to the given address from the bridge.
    ///         Used to trigger Transaction contracts deployed by the bridge's USDC wallet
    ///         (e.g. SET_SUBSCRIBER_TYPE). Only callable by the owner.
    /// @param txAddr — address of the Transaction contract to trigger
    function triggerTransaction(address txAddr) public view onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        txAddr.transfer({value: 1 vmshell, bounce: true, flag: 1});
    }

    // ========================================================
    // On-chain code upgrade (owner only)
    // ========================================================

    /// @notice Upgrades the contract code on-chain. Only callable by the owner.
    /// @param newcode — new contract code TvmCell
    /// @param userCell — reserved passthrough for future upgrade payloads.
    ///        Currently unused (the new code receives a cell built purely
    ///        from snapshot of current storage). Future upgrades can read
    ///        this slot once `onCodeUpgrade` is extended; today it lets the
    ///        ABI stay stable.
    function updateCode(TvmCell newcode, TvmCell userCell) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        TvmCell migrationCell = abi.encode(
            _ownerPubkey, _usdcWallet, _totalMinted, _mintNonce, _mintAccumulatorNonce,
            _totalMintedBridgeByToken, _totalBurnedBridgeByToken, _depositVoucherCode,
            userCell
        );
        tvm.commit();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(migrationCell);
    }

    /// @notice Initializes state after code upgrade. Resets all storage and re-initializes
    ///         from the provided cell. Called by UpdateZeroContract (zerostate) and updateCode().
    /// @param cell — ABI-encoded tuple:
    ///                 (uint256 pubkey,
    ///                  address usdcWallet,
    ///                  uint128 totalMinted,
    ///                  uint64  mintNonce,
    ///                  uint64  mintAccumulatorNonce,
    ///                  mapping(uint32 => uint128) totalMintedBridgeByToken,
    ///                  mapping(uint32 => uint128) totalBurnedBridgeByToken,
    ///                  TvmCell depositVoucherCode,
    ///                  TvmCell userCell)
    ///         `depositVoucherCode` is the voucher code carried by the
    ///         zerostate path. `userCell` is `updateCode`'s passthrough: on a
    ///         code-bumping on-chain upgrade it carries the INTENDED NEW voucher
    ///         code (so a code bump can swap the voucher logic atomically — the
    ///         only way to rotate `_depositVoucherCode` post-deploy, per B2); it
    ///         is empty on the zerostate path. When non-empty it takes
    ///         precedence over `depositVoucherCode`.
    ///
    ///         `_trustedL1Bridge` is deliberately absent from the tuple: the
    ///         encode side may be a PREVIOUS code generation that does not know
    ///         the field (or knows it with a different type), so the tuple shape
    ///         stays fixed across generations. After any upgrade the allowlist
    ///         starts empty (deposits fail closed) until the owner re-seeds it
    ///         via `setTrustedL1Bridge`.
    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        tvm.resetStorage();
        (uint256 pubkey,
         address usdcWallet,
         uint128 totalMinted,
         uint64  mintNonce,
         uint64  mintAccumulatorNonce,
         mapping(uint32 => uint128) totalMintedBridgeByToken,
         mapping(uint32 => uint128) totalBurnedBridgeByToken,
         TvmCell depositVoucherCode,
         TvmCell userCell)
            = abi.decode(cell, (uint256, address, uint128, uint64, uint64,
                                mapping(uint32 => uint128), mapping(uint32 => uint128),
                                TvmCell, TvmCell));
        _ownerPubkey = pubkey;
        _usdcWallet = usdcWallet;
        _totalMinted = totalMinted;
        _mintNonce = mintNonce;
        _mintAccumulatorNonce = mintAccumulatorNonce;
        _totalMintedBridgeByToken = totalMintedBridgeByToken;
        _totalBurnedBridgeByToken = totalBurnedBridgeByToken;
        _depositVoucherCode = userCell.toSlice().empty() ? depositVoucherCode : userCell;
    }

    // ========================================================
    // Getters
    // ========================================================

    /// @notice Returns the TIP-3 USDC TokenWallet address used for the bridge.
    function getUsdcWallet() external view returns (address) {
        return _usdcWallet;
    }

    /// @notice Returns the owner public key.
    function getOwnerPubkey() external view returns (uint256) {
        return _ownerPubkey;
    }

    /// @notice Returns total ECC[3] USDC minted by this contract.
    function getTotalMinted() external view returns (uint128) {
        return _totalMinted;
    }

    /// @notice Returns total ECC minted/burned via the cross-chain bridge path
    ///         for a specific tokenId.
    function getTotalBridged(uint32 tokenId) external view returns (uint128 minted, uint128 burned) {
        return (_totalMintedBridgeByToken[tokenId], _totalBurnedBridgeByToken[tokenId]);
    }

    /// @notice Returns the hash of the currently installed DepositVoucher code.
    function getDepositVoucherCodeHash() external view returns (uint256) {
        return tvm.hash(_depositVoucherCode);
    }

    /// @notice Returns current nonces for double-spend protection.
    function getNonces() external view returns (uint64 mintNonce, uint64 mintAccumulatorNonce) {
        return (_mintNonce, _mintAccumulatorNonce);
    }

    /// @notice Returns contract version and name.
    function getVersion() external pure returns (string, string) {
        return (version, "eccUSDCBridge");
    }

    // ========================================================
    // Halo2 public-inputs assembly (consumer side of opcode 0xC7 0x4A)
    // ========================================================

    /// @dev Read the deposit fields out of the PROVEN public-inputs blob (the
    ///      contract verified the proof over this exact blob, so every value
    ///      here is proof-bound). Layout = 12 × 32-byte LE Fr; offsets per the
    ///      final ETH-deposit circuit: 0=deposit_id, 1=sender, 2=amount,
    ///      3=contract, 4=chain_id, 5..6=dapp_id(hi..lo),
    ///      7..8=an_account(hi..lo), 9..10=block hash halves, 11=promise commit
    ///      (9..11 ignored on the AN side). Only the first 9 Fr are needed.
    function _parsePublicInputs(bytes publicInputs) private pure returns (DepositPI f) {
        TvmSlice s = TvmSlice(publicInputs);
        uint256[] fr;
        for (uint k = 0; k < 9; k++) {
            uint256 v = 0;
            for (uint i = 0; i < 32; i++) {
                if (s.bits() < 8) { s = s.loadRef().toSlice(); }
                v |= (uint256(uint8(s.loadUint(8))) << (8 * i));   // little-endian
            }
            fr.push(v);
        }
        require(fr[2] <= uint256(type(uint64).max), ERR_OVERFLOW);
        // The circuit splits the 256-bit AN account into two 16-byte halves
        // (fr[6]=high, fr[7]=low), exactly like dapp_id above — reassemble it.
        // The workchain concept is retired on AN, so the recipient always lives
        // in workchain 0 (see confirmDeposit's makeAddrStd).
        f.depositId    = fr[0];
        f.amount       = uint128(fr[2]);
        f.contractAddr = fr[3];
        f.chainId      = fr[4];
        // Deposits into AN always land in dapp 0, so the dapp halves carried by
        // the circuit (fr[5]=high, fr[6]=low) are not used. Pinning the field to
        // 0 keeps the deposit identity — and therefore the DepositVoucher
        // address — independent of what the L1 side reports.
        f.dappId       = 0;
        f.anAccount    = (fr[7] << 128) | fr[8];
    }
}
