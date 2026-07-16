pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./DepositVoucher.sol";
import "../token/interface/ISubscriber.sol";

interface IShellAccumulator {
    function buyShellFor(address buyer) external;
}

/// @title USDCBridge
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
///            destination/source chain are opaque to this contract.
///            Counters: per-tokenId `_totalMintedBridgeByToken` /
///            `_totalBurnedBridgeByToken` (mapping kept for forward-compat).
///
///         Deployed at fixed address in zerostate.
contract USDCBridge is USDCBridgeModifiers, ISubscriber {
    string constant version = "1.1.0";

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
        uint128 amount,
        uint256 anAccount
    );

    /// @notice Deposit fields read out of the proven public-inputs blob.
    struct DepositPI {
        uint256 depositId;     // fr[0] — anti-replay anchor (per source contract/dapp)
        uint128 amount;        // fr[2]
        uint256 contractAddr;  // fr[3] — L1 bridge contract that emitted the event
        uint256 dappId;        // fr[4]<<128 | fr[5] — AN dapp id
        uint256 anAccount;     // fr[6]<<128 | fr[7] — AN recipient (256-bit, proof-bound)
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

    // ZK verifying key (VkBlob) for the FINAL ETH-deposit circuit
    // (receipt-proof of an L1 deposit event, 11 public inputs). Source:
    // tvm-sdk@feature/hermez-kzg-resurrection (Hermez SRS):
    //   tvm_vm/halo2_test_data/deposit_10proofs/deposit_vk_blob.bin
    //   sha256 304c1c4ed1e4cf09a00fb1d83a0ae2ba42db2afead035ae089a4faa85346251a.
    // Magic "VKBLOB\x00\x00" + version 2. To rotate: regenerate the VkBlob and
    // replace the constant below (and the fixtures under tests/exchange/fixtures).
    // 3982 bytes; sha256=304c1c4ed1e4cf09a00fb1d83a0ae2ba42db2afead035ae089a4faa85346251a
    bytes constant VK_BLOB =
        hex"564b424c4f4200000200010000000000ec0000007b22726c63223a7b2262617365223a7b226b223a31382c226e756d5f6164"
        hex"766963655f7065725f7068617365223a5b31332c31305d2c226e756d5f6669786564223a312c226e756d5f6c6f6f6b75705f"
        hex"6164766963655f7065725f7068617365223a5b312c312c305d2c226c6f6f6b75705f62697473223a382c226e756d5f696e73"
        hex"74616e63655f636f6c756d6e73223a317d2c226e756d5f726c635f636f6c756d6e73223a317d2c226b656363616b223a7b22"
        hex"636f6d705f6c6f616465725f706172616d73223a7b226d61785f686569676874223a302c2273686172645f63617073223a5b"
        hex"36345d7d7d7d8a0e00000212000000001c00000058c338696a199ecb26464c9bdedd6f46e6fa0c9974d91b9fcaa627c329df"
        hex"f21a4e7b0e1f40caf730f9cb23b5583598194673b2bc3c7f181c20266ff8b4d3cf26d4e9468a241607d2b62e1e956e7d03cd"
        hex"226d2f7c5ba012bdd86450bece55ea211ee43f43142e3de5f4ba698a3d165ef9ae062fbd70bc24ce57b90407a8c6b9185fd2"
        hex"1dccbc7228aff95afe27989687390e02314dbb76851aca0f0eed3554fc18b3be9a3a4da11db71bf86500e86212ab3b3c524d"
        hex"02c197c14dd773879e342c05db935101e59d4c505fb8904c071dfa423f0bb5fe7be040f3b164bd414f161d01a414d27ff209"
        hex"38c7329fb35a79bdeb029c823e35c2c23dcd45fb800a8a550926ef2a097b473418d4da72ee708a6b7aa4dc14912f8f8049fd"
        hex"838e12fdc22db81f3ae5d1abb6834b98f06b160f2402e5368e5a0e5eae6437eb1096346ad5e3ef297e9d67129484b9797c0c"
        hex"12c21a0f171fa8035f7831455362885767e579397306f0300e82850ab65baf1a2c6413e6b4b26a3825f17c67c1128e55dc01"
        hex"eaa1321775436210f7b4c48fb0d03b39b3ddbe7a2a902bec503ecb50008f63ebae412918b7b6ebafd72e8145607fc294acc3"
        hex"08adc81c0d985bdfadd2e0f4297ea2f05b11ab3a8fe5cf2675af3aa1d4826b6fa487760cc2c98793003b995657474c66410d"
        hex"7d9cc1c8fe954ea0a53979b9d033d9a74c5bc6f690256dcf7d2e09c4fca92f0794a7b3fd11e6feb4af2fdf7e7989b181d973"
        hex"246ed001a562c04d2947752ff521c5e4cde5255a4dd5b9252b6e44985b9d54fdcf1573ea0d514099062e180a8e1f2be68a23"
        hex"5f3903e6e3f1b542250774d1d163f49078174f9930b929bda1e1e302cbc49bde7d7530609c113d58482ea4c1f9ba3c794034"
        hex"86a90624e24b850fdc276377cfea7e0e8d4b73b69a5990bca5d1d7465edf3cdea3d569368034e766e71bc2826c1509e49d7a"
        hex"62791e8c40864131c4982d0aad762bd8bf42c36c59c5de140018b2505e00acec7916bb6f4ea7219c7524447d1da88d728c17"
        hex"5359c14acd26ad75f5bb76bcd534e025054ddb8d73cde45d9cf3bac1994239189b947fd857161fe8b7386fee070da31478c9"
        hex"3a7d3297988bf3e8d502cfe5dc81e4384bf2d41ac8217ccff707a1c3dd1d74c1894c0ad72cad9b2fc4682a45817002a9c335"
        hex"471671cd3c578a83b30fa31f0c94cff9acd268ef7cf7ca84477e77af24678154dc018db74e3856198376a5b1217834687b77"
        hex"1bdceb40da0e7de3f5bacf6a6a55132f0f7ff05b8f1ef685e1d51a9d249ea1f5b7583fc6bd87b187ce9a0b11dee843149d31"
        hex"0fe371e5f3f537201847618368b082e8ee0b1dbf4c1295e0e4049e24eb268f888ebee704b9c0cc582b96c6f590493bbc35b8"
        hex"8d8fc96e0078e64b99e089007954d99e9b02385ab817bc99ed21a5b5c8754cb673fabb51cb0a482530870a0f2ef13b2ed96b"
        hex"357bd6c640aabd370630a17dd50949fc23a38ec523110cc9721ab008cd1c221ecd5b624f320ffc11e7292b518e7e21377bc4"
        hex"ebd6c7402b58390c9e64ab2db709bed839a2e5bb66cea2791b349e1af890f71fff833fb4b98f6302d54f1fbfa4a32d0c21e8"
        hex"058af9ed2ca27d0036e383970b58e9eaf6842c0b2c25ade4e82bab2224406d25aa99fa2dc1869dd015e8ec92322ec96637c2"
        hex"a9adef18af3e150b4473f0eae4b35fdddc76d4283b7c7f1e6cccb8f1c2907d153c356720e561f1c7457f8456f1d4b2ca5718"
        hex"4c43614e2365cf38902d1e9bde611079532bf92df34ef811f99f5e3285e077db3f1d259f0db0acb53cad0006a0d6f0eb5d07"
        hex"b1d5cd8e6e20bf99a236e7e84fbe483ebe0ab4550dac5e9620165dc3dfe3fc28ba307b90283de47c7837956793f9bc3e4621"
        hex"5f75b9328fa3ccf12e6d2ef9ec22077bf474d774ada698c996fcf0fdc15a311bdaf31c5c5330310860fb6d2fd421310fdbf0"
        hex"598851d5b98e5ebfb889daeca67b76238329a2cc3afbdc334ac966070ede7f31c2abef6b19b8ab278d805a178dd133134a2b"
        hex"6923b6d5579a13f2c01d9546171274e1acb493bbec246bde50da4a9215670b3eb91bf96e1040593edc03bd06e57a3d442727"
        hex"96de0db7a65e46c21c6d1819cbfbab54e60326dbdb92cc092392cf430a4b53563327cba30954b8af978d9cb3c449800a610c"
        hex"a5bd56c7e102f817198acb63df1f35ba21adfc2ba45559546b9a271e08a1eaa2dafdf05e3522405bdf595e3e721dc8f4a281"
        hex"7ce09aa2f0e9f1124432681ef4ec45bf4ab47c1ddb577e58ded412d6587308e9f7bcc57c4095710afca949acbd371ad602e3"
        hex"941cbeecbab7c48b182ce152e552079d44b7583cb43fd150986126d8b6a2c655af0a4eabd1fdff4bd5e04c4459dfba44fb84"
        hex"eaeec2b5dafb4c3b9be67902988be513e5f957d8c06ee01e5583702f5ddc6c7b17bd007fa93b3ac26346ac240e26bb268d6f"
        hex"dd9a9efc33fbc88c00be276f2517108aa27843038731d792899f09e40006f53cb9633ec196dcdd5f93dd77fe5f769828bfe1"
        hex"2d4f627d0f68241a85083d15d12c95c6941d8cd8aa793ef8f9cd57406032ae906d0c9e80dd6f5e9a1357002df4c632b410e2"
        hex"b3f02b798f25eb20c6cee35b2551305b30f2d7141b0143ed9a17d2c4172e0614fc286faf211356cc57612851adbdc91ffe95"
        hex"2d0b0a4777d24629002f6a79a017ddd9da3ed9448d57b0daa6902305db8d8e1328b17db30c355f2944b6c872444c7b2b9852"
        hex"8d24adabe4009787070c06880c09a03b1c3a74765426f246da5443b102673151f01f4565f34cc74fd05f40712896219e8098"
        hex"66c5270be7c04fefefb6a1021eaf2c9dea98a3c6f6e21aad2cef57ec03fd09fa99097e0215d75df2a6f29bec75c95213508d"
        hex"5979d331052c7fca5c52bc7e83bca1403e16f799da6198080ef13cbbc4cbc33d9a96e9bdecbea8dba9b57d7de9d096e9db06"
        hex"0ee893688351a5fde6ad362394cfdd1d0da8b083b345c6800ab5048b2ed109086f2907cd70425baa06153280b8d435f0671f"
        hex"894961992bf26b290a1a405c50132a7c321c51189280b7fbd26b03c2df191a2cb6c8af977911bb8b4316f93a8f241fff91fa"
        hex"58354439b0766fe96e2ed4f06baa57e387f3c63236046becdbbe812b6fb1ce5d1360609c689943e2f4e96fc37156a4e548a1"
        hex"2026f01303ad33df0f0461b91df76647e58b3b51dcce5e4ccbcda2b2832c516eaca32ccafcb8af52070ec4f0ecac3f2898c1"
        hex"22d1b2dd6587d48ea38e6a5939017a2d95a7fbf02ef3f61baaffed0b5bf208951bdb5d0860a1fb39edc3bc2894dc6289a9de"
        hex"fbdee9d5a3196ac76850fade10ec0d156a5aeca5122e6df1711782844353813347c907870e18b6afd6bc50ab9c372f225732"
        hex"63d4d4b5210ec5aedbcb181d3b55952023399b1d88e81625afb60b622d2f9ede9fc725758cd36d47e50183b08fbe15b1c7dc"
        hex"5f0f65f25d612730947756ef4f1856525e0cf2ab1062c025a32aaa2d2920d7102a22dd25c9cb18ba93fbf772d0c07e6ea387"
        hex"c658ca08b6b37bc5d19e3e6cfac0bd01e67a7db9ac5c4d30056c87e1c85f719ec59959865f95ae47109403478764621891a3"
        hex"603206df1adc3ca8a15bb2140a250b75d7c6be89611ed004bd6f5bfafb2894193d04b41719ebcbf86fb4fa2121202a83d9e3"
        hex"806e5be7734471c8f72dd52cab0d0f7db8a7219688415dbb6539d3214b5246548f3cf8e26f68c7a7e6b97f0b2e4755092cdd"
        hex"8afadbac97e147a5bde1210546409c1506e5384805ef5abf9a14d983ff13f148a17edf29d45c159565de243310ebc7f8d70c"
        hex"438261197374140f2e92ba9319541d6f07dc124d8b37c7070e7e8ec277aec930022c127bf9aa352ebf09dba7ce3562aa3116"
        hex"63de2a48e215cdd9465da704eeb51ffedae9945ecb2c0b66f1de5e65100bfd0e0e15f4bca7d868a183617d95a12f9954251e"
        hex"1628671d45cee35aa888cf2d179c150d3226b822e583d494163926d9326ae9302da8b31e94c55ef844da4ccbd7f0f583afd9"
        hex"1a9967fd07bc94967b993fe4414727c75120569510f40da265e4b57d6644886e21fb1a690882e3613de0d93087a53718ae28"
        hex"0d2fdb367c45862b604f8f9c267970b0cf7f68cce655f4a3d224b3c6b4fff61efafac215150cefc956d1eb8a1afd9ee10095"
        hex"5fa921bef042c9eddd9b6ea7820404b0c0df8853abf4a1e9bbfbf2b4d60f401f07581a0de263df91eaa6ec7b951a8463d032"
        hex"22fcf65da21c125a54a6b78e491d80a8872fdbc067415d312ff06204347e133694e3dfc91c870250b1c39a30179486bef72b"
        hex"d31923c36a0f016a1008be75af0eb7cbd18b9db70a6b7db81069a6f385ab5ef26ee6bcedcee090fc53245bb4234ef0c7a0c4"
        hex"97d2e82c6c5850abd81be012bf45990e5b448b3ef4bc30241071a49825e2b33c801b275337473dce0963007b22886f2de4a8"
        hex"e401094ee7174276e98cab953828c665184d25f0f451eefcb1841b805513db9f97932eede3001bfe22800999331647aa29e9"
        hex"2e8d770538cb857ab19403144e1c21fe57d1c5022ea2ba5ea800e6fde15ffb26bb854353425fd120cacc0018473955be452f"
        hex"22100a7d8688b539bfe854d9dec57a06d4d7790133655fc6400162c007eafe32bc15a43bf337c80f9b838c87629d9b590f7d"
        hex"77761c2a05a7320a7114238dcf6b6400097d4bbf2fc72a5395e3f4a031eeaa0e4277198473e78dfdf3b813347ba8410630be"
        hex"a47e233f4d74a1ee26bd9c37c580c8a63cba3006b47d80c9dbd810cf4c279333939a39a1d67ff00b7df1af12388c8c051012"
        hex"f99788a00542430961126817f74d8b5213b2ca5a8b9d5609d9b7a37d052335b9fab5e216314d43209917891fd71bffc9e3a6"
        hex"122a0b72441c8bd7d2cc2dfbd67d6ec4664771b7b1125b5e72019ac393ae3f6b01892992bf8586ce6d898f9fe0940fad9f4a"
        hex"2815a7b5df23d210f80bdde63626e89434c603d85351a7ce0061592579df8d1cd6903864171c2b1842b77b9353938d1794da"
        hex"918be5cce97e68cff2b0195d55a624926f3e90be3a0486876de33a415ebef46fd7b1e979515782d896c5bba3b57cc5907410"
        hex"e8673503c06eb07950af616d8055d1a45a21766d19dc7f5a18727396134a453357077a094e6d99832ed52a1aaa0674b4b3fd"
        hex"9d6baa797e6370c3923a4ad6969d61f11c1cb924f3020d883eccb527a0c230861f8be55e4468f2be524b031bf045c2285c1c"
        hex"0e97416da7d69a920bedc1ef521c0dd626d745223130a12f578ed9be89035b07";

    /// @notice Contract constructor.
    /// @dev `_depositVoucherCode` is intentionally NOT a constructor arg:
    ///       in the only deploy path that matters (zerostate premine stub +
    ///       `updateCode` upgrade) the voucher code arrives via the
    ///       `onCodeUpgrade` payload. There is no standalone setter (B2 fix),
    ///       so the only way to populate / rotate `_depositVoucherCode` is a
    ///       full `updateCode` upgrade of USDCBridge.
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
    /// @param publicInputs  — the circuit instance column: 11 × 32-byte LE Fr
    ///                         (deposit_id, sender, amount, contract, dapp_hi,
    ///                         dapp_lo, an_account_hi, an_account_lo, + 3 receipt/
    ///                         block hashes). Verified verbatim; business fields
    ///                         read at fixed offsets — see `_parsePublicInputs`.
    function finalizeDeposit(bytes proof, bytes publicInputs) public {
        // Cheap parse + sanity BEFORE accept (within the pre-accept gas budget).
        DepositPI f = _parsePublicInputs(publicInputs);
        require(f.amount > 0, ERR_ZERO_AMOUNT);

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

        // Anti-replay anchor = proof-bound (deposit_id, source contract, dapp).
        // amount/recipient are NOT in the key — they are fixed by the proof, so a
        // replay can never re-route or re-mint: same key ⇒ same voucher ⇒ no-op.
        uint256 depositHash = tvm.hash(abi.encode(f.depositId, f.contractAddr, f.dappId));

        TvmCell stateInit = abi.encodeStateInit({
            contr: DepositVoucher,
            varInit: { _depositHash: depositHash },
            code: _depositVoucherCode
        });

        new DepositVoucher{
            stateInit: stateInit,
            value: 2 vmshell,
            flag: 1
        }(f.depositId, f.contractAddr, f.dappId, f.amount, f.anAccount);
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
        uint128 amount,
        uint256 anAccount
    ) public {
        uint256 depositHash = tvm.hash(abi.encode(depositId, contractAddr, dappId));
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
            depositId, contractAddr, dappId, amount, anAccount
        );
    }

    // DepositVoucher code rotation is intentionally not exposed as a
    // standalone setter. The only way to change `_depositVoucherCode` is via
    // a full `updateCode` upgrade of USDCBridge (the new code+layout pass
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
        return (version, "USDCBridge");
    }

    // ========================================================
    // Halo2 public-inputs assembly (consumer side of opcode 0xC7 0x4A)
    // ========================================================

    /// @dev Read the deposit fields out of the PROVEN public-inputs blob (the
    ///      contract verified the proof over this exact blob, so every value
    ///      here is proof-bound). Layout = 11 × 32-byte LE Fr; offsets per the
    ///      final ETH-deposit circuit: 0=deposit_id, 1=sender, 2=amount,
    ///      3=contract, 4..5=dapp_id(hi..lo), 6..7=an_account(hi..lo),
    ///      8..10=receipt/block hashes (ignored on the AN side). Only the first
    ///      8 Fr are needed.
    function _parsePublicInputs(bytes publicInputs) private pure returns (DepositPI f) {
        TvmSlice s = publicInputs.toSlice();
        uint256[] fr;
        for (uint k = 0; k < 8; k++) {
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
        f.dappId       = (fr[4] << 128) | fr[5];
        f.anAccount    = (fr[6] << 128) | fr[7];
    }
}
