use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use tvm_types::UInt256;

use crate::config::Config;

pub struct ProductionTimeoutCorrection {
    last_production_duration: i64,
    correction: i64,
}

impl Default for ProductionTimeoutCorrection {
    fn default() -> Self {
        Self { last_production_duration: 0, correction: -50 }
    }
}

impl ProductionTimeoutCorrection {
    pub fn report_last_production(&mut self, duration: Duration) {
        self.last_production_duration = duration.as_millis() as i64;
    }

    pub fn get_production_timeout(&mut self, desired: Duration) -> Duration {
        let last_production = self.last_production_duration;
        let desired = desired.as_millis() as i64;
        if last_production > desired {
            self.correction -= 10;
        } else if last_production > 0 && last_production < desired - 20 {
            self.correction += 5;
        }
        // Do not let correction above 90% of the desired duration.
        let max_correction = -((desired as i128) * 9 / 10) as i64;
        if max_correction > self.correction {
            self.correction = max_correction;
        }
        Duration::from_millis((desired + self.correction) as u64)
    }

    pub(crate) fn get_correction(&self) -> i64 {
        self.correction
    }
}

pub struct ExecutionTimeLimits {
    block_deadline: Option<Instant>,
    default_message_timeout: Option<Duration>,
    alternative_message_timeout: Option<Duration>,
    alternative_messages: Option<HashSet<UInt256>>,
}

impl ExecutionTimeLimits {
    pub const NO_LIMITS: Self = Self {
        block_deadline: None,
        default_message_timeout: None,
        alternative_message_timeout: None,
        alternative_messages: None,
    };

    pub fn new(
        block_deadline: Option<Instant>,
        default_message_timeout: Option<Duration>,
        alternative_message_timeout: Option<Duration>,
    ) -> Self {
        Self {
            block_deadline,
            default_message_timeout,
            alternative_message_timeout,
            alternative_messages: None,
        }
    }

    pub fn production(block_timeout: Duration, config: &Config) -> Self {
        Self::new(
            Some(Instant::now() + block_timeout),
            config.global.time_to_produce_transaction_millis.map(Duration::from_millis),
            None,
        )
    }

    pub fn verification(config: &Config) -> Self {
        Self::new(
            Some(Instant::now() + Duration::from_millis(config.global.time_to_verify_block_millis)),
            config.global.time_to_verify_transaction_millis.map(Duration::from_millis),
            config
                .global
                .time_to_verify_transaction_aborted_with_execution_timeout_millis
                .map(Duration::from_millis),
        )
    }

    pub fn add_alternative_message(&mut self, message_hash: UInt256) {
        if let Some(alternative) = &mut self.alternative_messages {
            alternative.insert(message_hash);
        } else {
            self.alternative_messages = Some(HashSet::from([message_hash]));
        }
    }

    pub fn block_deadline(&self) -> Option<Instant> {
        self.block_deadline
    }

    pub fn get_message_timeout(&self, message_hash: &UInt256) -> Option<Duration> {
        if self
            .alternative_messages
            .as_ref()
            .map(|alternative| alternative.contains(message_hash))
            .unwrap_or_default()
        {
            self.alternative_message_timeout
        } else {
            self.default_message_timeout
        }
    }
}
