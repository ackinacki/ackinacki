use derive_getters::Getters;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, Getters, TypedBuilder)]
pub struct MonitorRequest<TPath>{
    target: TPath,
}
