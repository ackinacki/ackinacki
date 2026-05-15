expose::submodule!(cut);

#[derive(Clone, Debug)]
pub enum Recommendation<TBitmask> {
    None,
    Cut(CutRecommendation<TBitmask>),
    RetireThread,
}

impl<T> Recommendation<T> {
    pub fn cut_at(path: T) -> Self {
        Recommendation::Cut (CutRecommendation { path })
    }
}
