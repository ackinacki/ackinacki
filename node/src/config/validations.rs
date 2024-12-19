use super::Config;

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
