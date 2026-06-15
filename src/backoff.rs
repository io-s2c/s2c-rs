use crate::config::S2cRetryOptions;
use rand::RngExt;
use rand::rngs::ThreadRng;
use std::time::Duration;
use tokio::time::sleep;

pub struct BackoffCounter<'a> {
    name: String,
    current_attempt: u32,
    s2c_retry_options: &'a S2cRetryOptions,
}

impl<'a> BackoffCounter<'a> {
    pub fn new(name: impl Into<String>, s2c_retry_options: &'a S2cRetryOptions) -> Self {
        Self {
            name: name.into(),
            current_attempt: 0,
            s2c_retry_options,
        }
    }

    pub async fn await_attempt(&mut self) {
        if !self.can_attempt() {
            return;
        }
        self.current_attempt += 1;
        let next_duration_ms = self.calculate_duration(self.current_attempt).as_millis() as u64;
        tracing::debug!(
            retrier = self.name,
            current_attempt = self.current_attempt,
            max_attempts = self.s2c_retry_options.max_attempts,
            delay_sec = format!("{:.2}", next_duration_ms as f64 / 1000.0),
            "Retrying..."
        );
        sleep(Duration::from_millis(next_duration_ms)).await
    }

    pub fn current_attempt(&self) -> u32 {
        self.current_attempt
    }

    pub fn can_attempt(&self) -> bool {
        if let Some(max_attempts) = self.s2c_retry_options.max_attempts {
            return self.current_attempt + 1 <= max_attempts;
        }
        true
    }

    pub fn reset(&mut self) {
        self.current_attempt = 0;
    }

    fn calculate_duration(&self, attempt: u32) -> Duration {
        let jitter: f64 = ThreadRng::default().random_range(0.1..1.0);
        let max_ms =
            Duration::from_secs(self.s2c_retry_options.max_delay_seconds).as_millis() as u64;
        // -1 for the multiplier to be 1 for first retry
        let multiplier = 1u64.checked_shl(attempt - 1).unwrap_or(u64::MAX);
        let base_backoff_ms = self.s2c_retry_options.base_delay_ms;

        let calculated_ms = base_backoff_ms.saturating_mul(multiplier);

        let final_ms = (std::cmp::min(max_ms, calculated_ms) as f64 * jitter) as u64;
        Duration::from_millis(final_ms)
    }
}
