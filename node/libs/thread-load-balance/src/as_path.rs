pub trait AsPath {
    type Unit: Clone + Copy;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn to_vec(&self) -> Vec<Self::Unit>;
    fn index(&self, i: usize) -> Self::Unit;
}
