use std::time::Duration;

use typed_builder::TypedBuilder;

use crate::types::BlockRound;

#[derive(Clone)]
struct RoundBucket {
    round_duration: Duration,
    max_number_of_loops: u32,
}

#[derive(TypedBuilder, Clone)]
#[builder(mutators(
    fn add_bucket(&mut self, round_duration: Duration, max_number_of_loops: u16) {
        assert!(max_number_of_loops > 0, "Max number of loops must be at least one.");
        self.buckets.push(RoundBucket {
            round_duration,
            max_number_of_loops: max_number_of_loops.into(),
        });
    }
))]
pub struct RoundTime {
    #[builder(via_mutators(init = vec![]))]
    buckets: Vec<RoundBucket>,
    default_bucket: Duration,
}

pub struct CalculateRoundResult {
    pub round: BlockRound,
    pub round_remaining_time: Duration,
}

fn div_with_reminder(duration: Duration, divider: Duration) -> (u32, Duration) {
    // Note: f64 is required, initial duration is too large to fit and f32 precision is not enough
    let quot = duration.div_duration_f64(divider);
    let quot = quot.floor();
    let quotient = quot as u64;
    let quotient: u32 = quotient.try_into().unwrap();
    let subtract = divider.checked_mul(quotient).unwrap();
    let reminder = duration.checked_sub(subtract).unwrap();
    (quotient, reminder)
}

impl RoundTime {
    pub fn linear(
        min_round_duration: Duration,
        step: Duration,
        max_round_duration: Duration,
    ) -> Self {
        assert!(max_round_duration >= min_round_duration);
        assert!(!step.is_zero());
        let mut builder = RoundTime::builder().default_bucket(max_round_duration);
        let mut cursor = min_round_duration;
        while max_round_duration > cursor {
            builder = builder.add_bucket(cursor, 1);
            cursor += step;
        }
        builder.build()
    }

    pub fn calculate_round(
        &self,
        duration_from_parent_block: Duration,
        block_bk_set_size: u16,
    ) -> CalculateRoundResult {
        let mut remaining = duration_from_parent_block;
        let mut round = 0;
        let block_bk_set_size: u32 = block_bk_set_size.into();
        for bucket in self.buckets.iter() {
            let bucket_loop_duration =
                bucket.round_duration.checked_mul(block_bk_set_size).unwrap();
            let bucket_max_duration =
                bucket_loop_duration.checked_mul(bucket.max_number_of_loops).unwrap();
            if remaining >= bucket_max_duration {
                round += block_bk_set_size * bucket.max_number_of_loops;
                remaining -= bucket_max_duration;
                continue;
            }
            let (loops_count, r) = div_with_reminder(remaining, bucket_loop_duration);
            round += loops_count * block_bk_set_size;
            remaining = r;
            let (x, r) = div_with_reminder(remaining, bucket.round_duration);
            round += x;
            remaining = r;
            return CalculateRoundResult { round: round.into(), round_remaining_time: remaining };
        }

        let (x, r) = div_with_reminder(remaining, self.default_bucket);
        round += x;
        tracing::trace!("calculate_round x={x:?}");
        CalculateRoundResult { round: round.into(), round_remaining_time: r }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_simple_default_duration_works() {
        let round_time = RoundTime::builder().default_bucket(Duration::from_millis(330)).build();
        let block_bk_set_size = 10;
        let round_at = |duration: Duration| round_time.calculate_round(duration, block_bk_set_size);
        assert_eq!(0, round_at(Duration::new(0, 0)).round);
        assert_eq!(0, round_at(Duration::from_millis(100)).round);
        assert_eq!(1, round_at(Duration::from_millis(330)).round);
        assert_eq!(2, round_at(Duration::from_millis(800)).round);
        // Remainging time check
        assert_eq!(
            Duration::from_millis(140),
            round_at(Duration::from_millis(800)).round_remaining_time
        );
    }

    #[test]
    fn ensure_buckets_added_from_top_to_low_priority() {
        let block_bk_set_size = 10;
        let round_time = RoundTime::builder()
            // First minute is split by a second
            // With bkset size of 10 it will lock 10 minutes
            // on this bucket
            .add_bucket(Duration::from_secs(1), 60)
            // Next is a minute long intervals.
            // Takes another 10 minute
            .add_bucket(Duration::from_secs(60), 1)
            // 5 minutes interval
            .default_bucket(Duration::from_secs(300))
            .build();
        let round_at =
            |duration: Duration| round_time.calculate_round(duration, block_bk_set_size).round;
        assert_eq!(0, round_at(Duration::new(0, 0)));
        assert_eq!(30, round_at(Duration::from_secs(30)));
        // This should step into 2nd bucket intervals
        assert_eq!(600, round_at(Duration::from_secs(600)));
        // Since it is 1 minute intervals the round should not change
        assert_eq!(600, round_at(Duration::from_secs(659)));
        // The last: default bucket
        assert_eq!(610, round_at(Duration::from_secs(1200)));
        assert_eq!(611, round_at(Duration::from_secs(1799)));
        assert_eq!(612, round_at(Duration::from_secs(1800)));
        assert_eq!(612, round_at(Duration::from_secs(2099)));
        assert_eq!(613, round_at(Duration::from_secs(2100)));
    }
}
