use super::Config;
use super::GlobalConfig;
use crate::node::services::block_processor::service::MAX_ATTESTATION_TARGET_BETA;

impl Config {
    pub fn ensure_min_cpu(mut self, min_number_of_cores: usize) -> Self {
        let cpu_cnt = num_cpus::get();
        tracing::trace!("Number of cpu cores: {cpu_cnt}");
        assert!(
            cpu_cnt >= min_number_of_cores,
            "Number of CPU cores is less than minimum: {cpu_cnt} < {min_number_of_cores}"
        );
        tracing::trace!("Set parallelization level to number of cpu cores: {cpu_cnt}");
        self.local.parallelization_level = cpu_cnt;
        self
    }
}

impl GlobalConfig {
    pub fn ensure_min_sync_gap(mut self) -> Self {
        if self.sync_gap < MAX_ATTESTATION_TARGET_BETA as u64 * 2 + 1 {
            tracing::trace!("Too low sync gap. Change it to MAX_ATTESTATION_TARGET_BETA * 2 + 1");
            self.sync_gap = MAX_ATTESTATION_TARGET_BETA as u64 * 2 + 1;
        }
        self
    }

    pub fn ensure_execution_timeouts(mut self) -> Self {
        let time_to_produce_block = self.time_to_produce_block_millis;
        let time_to_produce_transaction_millis =
            if let Some(timeout) = self.time_to_produce_transaction_millis {
                assert!(timeout <= time_to_produce_block);
                timeout
            } else {
                self.time_to_produce_transaction_millis = Some(time_to_produce_block);
                time_to_produce_block
            };
        let time_to_verify_block = self.time_to_verify_block_millis;
        assert!(time_to_verify_block >= time_to_produce_block);
        if let Some(timeout) = self.time_to_verify_transaction_millis {
            assert!(timeout <= time_to_verify_block);
        } else {
            self.time_to_verify_transaction_millis = Some(time_to_verify_block);
        }

        if let Some(timeout) = self.time_to_verify_transaction_aborted_with_execution_timeout_millis
        {
            assert!(timeout <= time_to_produce_transaction_millis);
        } else {
            self.time_to_verify_transaction_aborted_with_execution_timeout_millis =
                Some((time_to_produce_transaction_millis as f64 * 0.9).ceil() as u64);
        }
        self
    }
}
