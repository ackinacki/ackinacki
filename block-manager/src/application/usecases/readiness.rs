use std::sync::atomic::Ordering;
use std::sync::Arc;

use telemetry_utils::now_ms;

use crate::domain::models::AppState;

const HEALTH_GEN_UTIME_DIFF_SEC: u64 = 5;

pub struct ReadyOutput {
    pub is_ready: bool,
    pub reason: String,
}

pub fn exec(app_state: &Arc<AppState>) -> ReadyOutput {
    let now = now_ms();
    let last_block_gen_utime = app_state.last_block_gen_utime.load(Ordering::Relaxed);
    let diff = now.saturating_sub(last_block_gen_utime);
    let is_ready = diff < HEALTH_GEN_UTIME_DIFF_SEC * 1000;
    let reason = format!(
        "ready = {is_ready}, diff = {diff}, now = {now}, last_block_gen_utime = {last_block_gen_utime}"
    );
    ReadyOutput { is_ready, reason }
}
