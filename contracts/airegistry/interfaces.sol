pragma gosh-solidity >=0.76.1;

interface ISuperRootRegistry {
    function registerRoot(uint256 ownerPubkey) external;
}

interface IRootModelRegistry {
    function registerTokenContract(uint256 sellerPubkey, uint64 nonce) external;
}

/// @notice Streaming-deal hooks a TokenContract calls on the buyer's / seller's
///         PrivateNote to lock funds while a stream is live and to freeze both
///         notes during a dispute (spec §4.3). Implemented in PrivateNote
///         (phase 3); calls are fire-and-forget (flag:1, bounce:false).
interface IStreamNote {
    function streamLock(uint256 sellerPubkey, uint64 nonce) external;
    function streamUnlock(uint256 sellerPubkey, uint64 nonce) external;
    function streamDisputeLock(uint256 sellerPubkey, uint64 nonce) external;
    function streamDisputeUnlock(uint256 sellerPubkey, uint64 nonce) external;
}
