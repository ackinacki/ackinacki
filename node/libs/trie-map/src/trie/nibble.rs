pub trait TrieNibble {
    const BRANCH_SIZE: usize;
    fn get(key: &[u8], i: usize) -> u8;
    fn set(key: &mut [u8], i: usize, value: u8);
}

pub struct TrieNibble4;

impl TrieNibble for TrieNibble4 {
    const BRANCH_SIZE: usize = 16;

    #[inline]
    fn get(key: &[u8], i: usize) -> u8 {
        let b = key[i / 2];
        if (i & 1) == 0 {
            (b >> 4) & 0x0F
        } else {
            b & 0x0F
        }
    }

    #[inline]
    fn set(key: &mut [u8], i: usize, value: u8) {
        debug_assert!(value < 16);
        let idx = i / 2;
        let v = value & 0x0F;

        if (i & 1) == 0 {
            key[idx] = (key[idx] & 0x0F) | (v << 4);
        } else {
            key[idx] = (key[idx] & 0xF0) | v;
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NibblePath {
    pub bits: [u8; 32],
    pub len: u8,
}
