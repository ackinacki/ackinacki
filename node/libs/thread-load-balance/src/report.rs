use derive_getters::Getters;
use typed_builder::TypedBuilder;
use crate::AsPath;
use std::collections::BTreeMap;
use crate::Score;


#[derive(Clone, Debug, Getters, TypedBuilder)]
pub struct MonitorReport<TPath>
where
    TPath: AsPath,
{
    overall_load: Score,
    path: TPath,
    score_at_path: Score,
    path_child_scores: BTreeMap<TPath::Unit, Score>,
}
