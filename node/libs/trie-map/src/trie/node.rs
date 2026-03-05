use serde::Deserialize;
use serde::Serialize;

pub type NodeId = u32;
pub const NODE_ID_NONE: NodeId = 0;

pub const TAG_EMPTY: u8 = 0;
pub const TAG_LEAF: u8 = 1;
pub const TAG_BRANCH: u8 = 2;
pub const TAG_EXT: u8 = 3;

/// Compact node: hash and small payload.
/// Leaf data, ext paths, and branch children live in separate pools in Arena.
#[repr(C)]
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Node {
    pub hash: [u8; 32],
    pub tag: u8,
    _pad: [u8; 3],
    a: u32,
    b: u32,
    pub c: u32,
}

// payload (a, b, c) encoding:
//
// TAG_EMPTY: (0, 0, 0)
// TAG_LEAF: (value_index, 0, 0)
// TAG_BRANCH: (bitmap(u16) | (len(u16) << 16), base offset in children_pool, 0)
// TAG_EXT: (ext_base offset in ext_path pool, child NodeId, ext_len_nibbles in c)
//
// c is reserved for future metadata (GC, flags, versioning), but for EXT we use it as len.

impl Node {
    #[inline]
    pub fn with_empty(hash: [u8; 32]) -> Self {
        Self { hash, tag: TAG_EMPTY, _pad: [0; 3], a: 0, b: 0, c: 0 }
    }

    #[inline]
    pub fn with_leaf(hash: [u8; 32], value_index: u32) -> Self {
        Self { hash, tag: TAG_LEAF, _pad: [0; 3], a: value_index, b: 0, c: 0 }
    }

    #[inline]
    pub fn with_branch(hash: [u8; 32], bitmap: u16, base: u32, len: u16) -> Self {
        Node {
            hash,
            tag: TAG_BRANCH,
            _pad: [0; 3],
            a: (bitmap as u32) | ((len as u32) << 16),
            b: base,
            c: 0,
        }
    }

    #[inline]
    pub fn with_ext(hash: [u8; 32], ext_base: u32, child: NodeId, ext_len: u8) -> Self {
        Node { hash, tag: TAG_EXT, _pad: [0; 3], a: ext_base, b: child, c: ext_len as u32 }
    }

    #[inline]
    pub fn leaf(&self) -> usize {
        self.a as usize
    }

    #[inline]
    pub fn set_leaf(&mut self, value_index: usize) {
        self.a = value_index as u32;
    }

    #[inline]
    pub fn branch(&self) -> (u16, u16, usize) {
        ((self.a & 0xFFFF) as u16, (self.a >> 16) as u16, self.b as usize)
    }

    #[inline]
    pub fn set_branch(&mut self, base: usize) {
        self.b = base as u32;
    }

    #[inline]
    pub fn ext(&self) -> (usize, u8, NodeId) {
        (self.a as usize, self.c as u8, self.b)
    }

    #[inline]
    pub fn set_ext(&mut self, base: usize, child: NodeId) {
        self.a = base as u32;
        self.b = child;
    }
}

#[inline]
pub fn pop_count16(x: u16) -> u32 {
    (x as u32).count_ones()
}
