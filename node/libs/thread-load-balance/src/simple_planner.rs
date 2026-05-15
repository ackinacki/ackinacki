use crate::AsPath;
use crate::LoadPlanner;
use crate::MonitorReport;
use crate::MonitorRequest;
use crate::PlanOutput;
use crate::Recommendation;
use std::convert::From;

#[derive(Default)]
pub enum SimplePlannerState<TBitmask> {
    #[default]
    Empty,
    Awaiting(TBitmask),
}

// Simple strategy. Choose first split unit with the most score
pub struct SimplePlanner<TBitmask> {
    _phantom: std::marker::PhantomData<TBitmask>,
}

impl<TBitmask> LoadPlanner<TBitmask> for SimplePlanner<TBitmask>
where
    TBitmask: AsPath + for<'a> From<&'a [<TBitmask as AsPath>::Unit]> + 'static,
{
    type State = SimplePlannerState<TBitmask>;

    fn next<'a>(
        _s: Self::State,
        updates: impl IntoIterator<Item = &'a MonitorReport<TBitmask>>,
    ) -> (Self::State, PlanOutput<TBitmask>) {
        let mut is_all_loads_none = None;
        for report in updates.into_iter() {
            let total_load = report.overall_load();
            if total_load.is_none() {
                if is_all_loads_none.is_none() {
                    is_all_loads_none = Some(true);
                }
                continue;
            }
            if *total_load != *report.score_at_path() {
                // We can split here.
                let path = report.path().to_vec();
                return (
                    SimplePlannerState::<TBitmask>::Empty,
                    PlanOutput::<TBitmask> {
                        requests: vec![],
                        recommendation: Recommendation::<TBitmask>::cut_at(TBitmask::from(&path)),
                    },
                );
            }
            for (child_key, child_score) in report.path_child_scores().iter() {
                if total_load == child_score {
                    continue;
                }
                // We can split on this child
                let mut new_path: Vec<TBitmask::Unit> = report.path().to_vec();
                new_path.push(*child_key);
                return (
                    SimplePlannerState::<TBitmask>::Empty,
                    PlanOutput::<TBitmask> {
                        requests: vec![],
                        recommendation: Recommendation::cut_at(TBitmask::from(&new_path[..])),
                    },
                );
            }
        }
        if is_all_loads_none == Some(true) {
            (
                SimplePlannerState::Empty,
                PlanOutput { requests: vec![], recommendation: Recommendation::RetireThread },
            )
        } else {
            (
                SimplePlannerState::Awaiting(TBitmask::from(&[])),
                PlanOutput {
                    requests: vec![MonitorRequest::builder().target(TBitmask::from(&[])).build()],
                    recommendation: Recommendation::None,
                },
            )
        }
    }
}
