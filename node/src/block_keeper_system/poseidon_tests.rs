// Characterization tests: pin the output of `BlockKeeperSet::poseidon_commitment()`.
// The block producer writes this hash into every block, so it must never change.

use node_types::AccountIdentifier;

use super::bk_set_poseidon_commitment;
use super::validate_bls_pubkey;
use super::BlockKeeperData;
use super::BlockKeeperSet;
use crate::bls::gosh_bls::PubKey;

const GOLDEN_0: &str = "dd1f79fc36490292d8c60f10e6151098a1c1d4828d962f1a592a717b9d0a5822";
const GOLDEN_1: &str = "b3421036c7de5635b4feea4f6d792e9c8fb12a91ffffeaa45ea61ef5c2b52b29"; // set [7]
const GOLDEN_3: &str = "88cf63919f44c14b2c5ea62c913c7295b53ce73c99ee7ce395964e829e484718"; // set [5, 0, 2] (inserted unsorted)
const GOLDEN_299: &str = "77cb1ca913f63b8c87f216dff6da4cbb706b9bb25f6d143c84b40df8c835ef1e"; // set 0..299
const GOLDEN_300: &str = "06ecf2cd5d822d050063a16d6b547e70c391c37990cf7eb5e1da74f39805082d"; // set 0..300

fn test_pubkey(i: u16) -> [u8; 48] {
    let mut ikm = [0x5Au8; 32];
    ikm[..2].copy_from_slice(&i.to_le_bytes());
    gosh_blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap().sk_to_pk().to_bytes()
}

fn bk_set_of(indices: impl IntoIterator<Item = u16>) -> BlockKeeperSet {
    let mut set = BlockKeeperSet::new();
    for i in indices {
        let mut owner = [0u8; 32];
        owner[..2].copy_from_slice(&i.to_le_bytes());
        set.insert(
            i,
            BlockKeeperData {
                pubkey: PubKey::from(test_pubkey(i)),
                signer_index: i,
                owner_address: AccountIdentifier::new(owner),
                ..Default::default()
            },
        );
    }
    set
}

fn commitment_hex(indices: impl IntoIterator<Item = u16>) -> String {
    hex::encode(bk_set_of(indices).poseidon_commitment().unwrap())
}

#[test]
#[ignore]
fn print_golden_commitments() {
    println!("GOLDEN_0 {}", commitment_hex([]));
    println!("GOLDEN_1 {}", commitment_hex([7]));
    println!("GOLDEN_3 {}", commitment_hex([5, 0, 2]));
    println!("GOLDEN_299 {}", commitment_hex(0..299));
    println!("GOLDEN_300 {}", commitment_hex(0..300));
    println!("GOLDEN_301 {}", commitment_hex(0..301));
}

#[test]
fn poseidon_commitment_matches_golden_values() {
    assert_eq!(commitment_hex([]), GOLDEN_0);
    assert_eq!(commitment_hex([7]), GOLDEN_1);
    assert_eq!(commitment_hex([5, 0, 2]), GOLDEN_3);
    assert_eq!(commitment_hex(0..299), GOLDEN_299);
    assert_eq!(commitment_hex(0..300), GOLDEN_300);
}

#[test]
fn poseidon_commitment_ignores_signers_past_300() {
    // Documents MAX_SIGNERS truncation (step 3 of `bk_set_poseidon_commitment`): the 301st
    // signer does not change the hash.
    assert_eq!(
        bk_set_of(0..301).poseidon_commitment().unwrap(),
        bk_set_of(0..300).poseidon_commitment().unwrap(),
    );
}

#[test]
fn free_function_matches_golden_values() {
    let pairs = |r: std::ops::Range<u16>| r.map(|i| (i, test_pubkey(i))).collect::<Vec<_>>();
    assert_eq!(hex::encode(bk_set_poseidon_commitment(&pairs(0..300)).unwrap()), GOLDEN_300);
    assert_eq!(
        hex::encode(
            bk_set_poseidon_commitment(&[
                (5, test_pubkey(5)),
                (0, test_pubkey(0)),
                (2, test_pubkey(2))
            ])
            .unwrap()
        ),
        GOLDEN_3
    );
}

#[test]
fn free_function_rejects_duplicate_index() {
    assert!(bk_set_poseidon_commitment(&[(1, test_pubkey(1)), (1, test_pubkey(2))]).is_err());
}

#[test]
fn validate_bls_pubkey_accepts_real_and_rejects_bad_keys() {
    assert!(validate_bls_pubkey(&test_pubkey(1)).is_ok());
    assert!(validate_bls_pubkey(&[0u8; 48]).is_err());
    let mut flipped = test_pubkey(1);
    flipped[47] ^= 0x01; // off-subgroup / off-curve x
    assert!(validate_bls_pubkey(&flipped).is_err());
}
