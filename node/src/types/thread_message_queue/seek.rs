//! Ordered lower-bound and single-pass cyclic traversal of queue Merkle maps.
use multi_map::node::NodeAug;
use multi_map::MapKey;
use multi_map::MultiMapAug;
use multi_map::MultiMapAugmentation;
use multi_map::MultiMapValue;

struct Frame<'a, V: MultiMapValue, A: MultiMapAugmentation> {
    node: &'a NodeAug<V, A>,
    key: [u8; 32],
    depth: usize,
}

pub struct SeekIter<'a, V: MultiMapValue, A: MultiMapAugmentation = ()> {
    stack: Vec<Frame<'a, V, A>>,
    lower: Option<MapKey>,
}

fn nibble(key: &mut [u8; 32], depth: usize, value: u8) {
    if depth.is_multiple_of(2) {
        key[depth / 2] = (key[depth / 2] & 15) | (value << 4);
    } else {
        key[depth / 2] = (key[depth / 2] & 240) | value;
    }
}

fn prefix_max(mut key: [u8; 32], depth: usize) -> [u8; 32] {
    if !depth.is_multiple_of(2) {
        key[depth / 2] |= 15;
    }
    key[depth.div_ceil(2)..].fill(255);
    key
}

impl<'a, V: MultiMapValue, A: MultiMapAugmentation> SeekIter<'a, V, A> {
    pub fn new(map: &'a MultiMapAug<V, A>, lower: Option<MapKey>) -> Self {
        let mut result = Self { stack: Vec::with_capacity(4), lower };
        if !map.root.is_empty() {
            result.stack.push(Frame {
                node: &map.root,
                key: map.root_path.prefix.0,
                depth: map.root_path.len as usize / 4,
            });
        }
        result
    }

    fn push(&mut self, node: &'a NodeAug<V, A>, key: [u8; 32], depth: usize) {
        if !node.is_empty() && self.lower.is_none_or(|lower| prefix_max(key, depth) >= lower.0) {
            self.stack.push(Frame { node, key, depth });
        }
    }
}

impl<'a, V: MultiMapValue, A: MultiMapAugmentation> Iterator for SeekIter<'a, V, A> {
    type Item = (MapKey, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Frame { node, mut key, depth } = self.stack.pop()?;
            match node {
                NodeAug::Empty => {}
                NodeAug::Leaf(leaf) => {
                    if self.lower.is_some_and(|lower| key < lower.0) {
                        continue;
                    }
                    self.lower = None;
                    return Some((MapKey(key), &leaf.value));
                }
                NodeAug::Ext(extension) => {
                    let count = extension.nibble_count as usize;
                    for i in 0..count {
                        nibble(&mut key, depth + i, extension.nibbles[i]);
                    }
                    self.push(&extension.child, key, depth + count);
                }
                NodeAug::Branch(branch) => {
                    for value in (0..16u8).rev() {
                        let mut child_key = key;
                        nibble(&mut child_key, depth, value);
                        self.push(&branch.children[value as usize], child_key, depth + 1);
                    }
                }
            }
        }
    }
}

pub struct CyclicIter<'a, V: MultiMapValue, A: MultiMapAugmentation = ()> {
    map: &'a MultiMapAug<V, A>,
    iter: SeekIter<'a, V, A>,
    start: Option<MapKey>,
    wrapped: bool,
    done: bool,
}

impl<'a, V: MultiMapValue, A: MultiMapAugmentation> CyclicIter<'a, V, A> {
    pub fn new(map: &'a MultiMapAug<V, A>, start: Option<MapKey>) -> Self {
        Self { map, iter: SeekIter::new(map, start), start, wrapped: false, done: false }
    }
}

impl<'a, V: MultiMapValue, A: MultiMapAugmentation> Iterator for CyclicIter<'a, V, A> {
    type Item = (MapKey, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            match self.iter.next() {
                Some((key, value)) => {
                    if self.wrapped && self.start.is_some_and(|start| key >= start) {
                        self.done = true;
                        return None;
                    }
                    return Some((key, value));
                }
                None if !self.wrapped && self.start.is_some() => {
                    self.wrapped = true;
                    self.iter = SeekIter::new(self.map, None);
                }
                None => {
                    self.done = true;
                    return None;
                }
            }
        }
    }
}
