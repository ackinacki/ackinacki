expose::submodule!(recommentations, request, report, as_path, score, simple_planner);

use derive_getters::Getters;

#[derive(Clone, Debug, Getters)]
pub struct PlanOutput<TBitmask> {
    requests: Vec<MonitorRequest<TBitmask>>,
    recommendation: Recommendation<TBitmask>,
}

pub trait LoadPlanner<TBitmask>
where
    TBitmask: AsPath + 'static,
{
    type State: Default;

    fn next<'a>(
        s: Self::State,
        updates: impl IntoIterator<Item = &'a MonitorReport<TBitmask>>,
    ) -> (Self::State, PlanOutput<TBitmask>);
}
