use derive_getters::Getters;

#[derive(Clone, Debug, Getters)]
pub struct CutRecommendation<TPath> {
    pub path: TPath,
    //pub expected_score: f64,
    //pub confidence: f64,     // e.g. based on uncertainty + freshness
}
