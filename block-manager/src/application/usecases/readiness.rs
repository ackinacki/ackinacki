use std::sync::atomic::Ordering;
use std::sync::Arc;

use telemetry_utils::now_ms;

use crate::domain::models::AppState;

pub struct ReadyOutput {
    pub is_ready: bool,
    pub reason: String,
}

pub fn exec(app_state: &Arc<AppState>) -> ReadyOutput {
    let now = now_ms();
    let last_block_gen_utime = app_state.last_block_gen_utime.load(Ordering::Relaxed);
    let diff = now.saturating_sub(last_block_gen_utime);
    let max_block_age_ms = app_state.readiness_max_block_age_secs.saturating_mul(1000);
    let is_ready = diff < max_block_age_ms;
    let reason = format!(
        "ready = {is_ready}, diff = {diff}, max_block_age = {max_block_age_ms}, now = {now}, last_block_gen_utime = {last_block_gen_utime}"
    );
    ReadyOutput { is_ready, reason }
}
