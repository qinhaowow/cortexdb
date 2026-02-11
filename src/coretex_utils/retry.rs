//! Exponential backoff retry mechanism for coretexdb.
//!
//! This module provides a robust retry mechanism with exponential backoff
//! and jitter support for handling transient failures in distributed systems.
//!
//! # Features
//!
//! - Multiple retry strategies: Fixed, Exponential, Exponential with Jitter
//! - Configurable backoff parameters
//! - Task-level and global retry configuration
//! - Automatic retry statistics tracking
//! - Thread-safe implementation
//!
//! # Example
//!
//! ```ignore
//! use cortex_utils::retry::{RetryStrategy, ExponentialBackoff, RetryResult};
//!
//! let strategy = ExponentialBackoff::new()
//!     .with_initial_delay(Duration::from_millis(100))
//!     .with_max_delay(Duration::from_secs(30))
//!     .with_max_retries(5)
//!     .with_exponential_base(2.0)
//!     .with_jitter(0.1);
//!
//! let result = strategy.execute(|| async {
//!     // Your async operation here
//!     Ok("success")
//! }).await;
//! ```

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::time::sleep;
use rand::Rng;

/// Error type for retry operations
#[derive(Debug, Error, Clone, PartialEq)]
pub enum RetryError {
    #[error("Max retries exceeded: {0} attempts")]
    MaxRetriesExceeded(u32),

    #[error("Operation cancelled during retry")]
    Cancelled,

    #[error("Retry operation timed out after {0:?}")]
    Timeout(Duration),

    #[error("Underlying error: {0}")]
    Underlying(String),
}

/// Result type for retry operations
pub type RetryResult<T> = std::result::Result<T, RetryError>;

/// Statistics for retry operations
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RetryStats {
    total_attempts: AtomicU64,
    successful_retries: AtomicU64,
    failed_retries: AtomicU64,
    total_delay_ms: AtomicU64,
    max_attempts_on_single_op: AtomicU64,
}

impl RetryStats {
    pub fn new() -> Self {
        Self {
            total_attempts: AtomicU64::new(0),
            successful_retries: AtomicU64::new(0),
            failed_retries: AtomicU64::new(0),
            total_delay_ms: AtomicU64::new(0),
            max_attempts_on_single_op: AtomicU64::new(0),
        }
    }

    pub fn record_attempt(&self) {
        self.total_attempts.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_success(&self, attempts: u32, total_delay_ms: u64) {
        self.successful_retries.fetch_add(1, Ordering::SeqCst);
        self.total_delay_ms.fetch_add(total_delay_ms, Ordering::SeqCst);
        loop {
            let current_max = self.max_attempts_on_single_op.load(Ordering::SeqCst);
            if attempts <= current_max {
                break;
            }
            if self
                .max_attempts_on_single_op
                .compare_exchange(current_max, attempts, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn record_failure(&self) {
        self.failed_retries.fetch_add(1, Ordering::SeqCst);
    }

    pub fn get_stats(&self) -> RetryStatsSnapshot {
        RetryStatsSnapshot {
            total_attempts: self.total_attempts.load(Ordering::SeqCst),
            successful_retries: self.successful_retries.load(Ordering::SeqCst),
            failed_retries: self.failed_retries.load(Ordering::SeqCst),
            total_delay_ms: self.total_delay_ms.load(Ordering::SeqCst),
            max_attempts_on_single_op: self.max_attempts_on_single_op.load(Ordering::SeqCst),
        }
    }
}

/// Snapshot of retry statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryStatsSnapshot {
    pub total_attempts: u64,
    pub successful_retries: u64,
    pub failed_retries: u64,
    pub total_delay_ms: u64,
    pub max_attempts_on_single_op: u64,
}

/// Retry strategy type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed,
    /// Exponential backoff without jitter
    Exponential,
    /// Exponential backoff with full jitter
    ExponentialJittered,
    /// Exponential backoff with equal jitter (decorrelated)
    EqualJitter,
    /// Exponential backoff with decorrelated jitter
    DecorrelatedJitter,
}

/// Configuration for exponential backoff
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExponentialBackoffConfig {
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Base of exponential growth
    pub exponential_base: f64,
    /// Jitter factor (0.0 to 1.0)
    pub jitter_factor: f64,
    /// Whether to use jitter
    pub use_jitter: bool,
    /// Maximum total time for retries
    pub max_total_time: Option<Duration>,
    /// Retry on specific error types
    pub retryable_errors: Vec<String>,
}

impl Default for ExponentialBackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_retries: 3,
            exponential_base: 2.0,
            jitter_factor: 0.1,
            use_jitter: false,
            max_total_time: Some(Duration::from_secs(60)),
            retryable_errors: vec![
                "transient".to_string(),
                "timeout".to_string(),
                "connection".to_string(),
                "network".to_string(),
            ],
        }
    }
}

/// Exponential backoff retry strategy
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    config: ExponentialBackoffConfig,
    stats: Arc<RetryStats>,
}

impl ExponentialBackoff {
    /// Create a new exponential backoff with default settings
    pub fn new() -> Self {
        Self {
            config: ExponentialBackoffConfig::default(),
            stats: Arc::new(RetryStats::new()),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: ExponentialBackoffConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RetryStats::new()),
        }
    }

    /// Set initial delay
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.config.initial_delay = delay;
        self
    }

    /// Set maximum delay
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.config.max_delay = delay;
        self
    }

    /// Set maximum retries
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Set exponential base
    pub fn with_exponential_base(mut self, base: f64) -> Self {
        self.config.exponential_base = base;
        self
    }

    /// Set jitter factor
    pub fn with_jitter(mut self, factor: f64) -> Self {
        self.config.jitter_factor = factor.clamp(0.0, 1.0);
        self.config.use_jitter = true;
        self
    }

    /// Enable or disable jitter
    pub fn jitter_enabled(mut self, enabled: bool) -> Self {
        self.config.use_jitter = enabled;
        self
    }

    /// Set maximum total time for retries
    pub fn with_max_total_time(mut self, time: Option<Duration>) -> Self {
        self.config.max_total_time = time;
        self
    }

    /// Get current configuration
    pub fn config(&self) -> &ExponentialBackoffConfig {
        &self.config
    }

    /// Get retry statistics
    pub fn stats(&self) -> &Arc<RetryStats> {
        &self.stats
    }

    /// Calculate delay for a given attempt number
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base_delay = self
            .config
            .initial_delay
            .as_millis()
            .checked_mul(
                self.config.exponential_base
                    .powf(attempt as f64)
                    .round() as u128,
            )
            .unwrap_or(self.config.max_delay.as_millis())
            .min(self.config.max_delay.as_millis());

        let delay_ms = if self.config.use_jitter {
            self.apply_jitter(base_delay as u64, attempt)
        } else {
            base_delay as u64
        };

        Duration::from_millis(delay_ms)
    }

    /// Apply jitter to delay
    fn apply_jitter(&self, delay_ms: u64, attempt: u32) -> u64 {
        let jitter_range = (delay_ms as f64 * self.config.jitter_factor) as u64;
        let mut rng = rand::thread_rng();

        match attempt % 3 {
            0 => {
                // Full jitter: random between 0 and delay
                rng.gen_range(0..=delay_ms)
            }
            1 => {
                // Equal jitter: delay/2 + random * delay/2
                let half = delay_ms / 2;
                half + rng.gen_range(0..=half)
            }
            _ => {
                // Decorrelated jitter
                let min_delay = self.config.initial_delay.as_millis() as u64;
                let random_range = delay_ms.saturating_sub(min_delay);
                min_delay + rng.gen_range(0..=random_range.max(1))
            }
        }
    }

    /// Execute an async operation with retry
    pub async fn execute<F, T, E>(
        &self,
        mut operation: F,
    ) -> RetryResult<T>
    where
        F: FnMut() -> E,
        E: std::future::Future<Output = std::result::Result<T, String>>,
    {
        let start_time = Instant::now();
        let mut attempts = 0;
        let mut total_delay_ms = 0u64;

        loop {
            attempts += 1;
            self.stats.record_attempt();

            match operation().await {
                Ok(result) => {
                    self.stats.record_success(attempts, total_delay_ms);
                    return Ok(result);
                }
                Err(error) => {
                    // Check if we should retry
                    if attempts >= self.config.max_retries {
                        self.stats.record_failure();
                        return Err(RetryError::MaxRetriesExceeded(attempts));
                    }

                    // Check max total time
                    if let Some(max_total) = self.config.max_total_time {
                        if start_time.elapsed() > max_total {
                            self.stats.record_failure();
                            return Err(RetryError::Timeout(max_total));
                        }
                    }

                    // Calculate and apply delay
                    let delay = self.calculate_delay(attempts);
                    total_delay_ms += delay.as_millis() as u64;

                    // Check if adding this delay would exceed max total time
                    if let Some(max_total) = self.config.max_total_time {
                        if start_time.elapsed() + delay > max_total {
                            self.stats.record_failure();
                            return Err(RetryError::Timeout(max_total));
                        }
                    }

                    sleep(delay).await;
                }
            }
        }
    }

    /// Execute a sync operation with retry
    pub fn execute_sync<F, T, E>(
        &self,
        mut operation: F,
    ) -> RetryResult<T>
    where
        F: FnMut() -> std::result::Result<T, String>,
    {
        let start_time = Instant::now();
        let mut attempts = 0;
        let mut total_delay_ms = 0u64;
        let mut rng = rand::thread_rng();

        loop {
            attempts += 1;
            self.stats.record_attempt();

            match operation() {
                Ok(result) => {
                    self.stats.record_success(attempts, total_delay_ms);
                    return Ok(result);
                }
                Err(error) => {
                    if attempts >= self.config.max_retries {
                        self.stats.record_failure();
                        return Err(RetryError::MaxRetriesExceeded(attempts));
                    }

                    if let Some(max_total) = self.config.max_total_time {
                        if start_time.elapsed() > max_total {
                            self.stats.record_failure();
                            return Err(RetryError::Timeout(max_total));
                        }
                    }

                    let delay = self.calculate_delay(attempts);
                    total_delay_ms += delay.as_millis() as u64;

                    std::thread::sleep(delay);
                }
            }
        }
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self::new()
    }
}

/// Retry policy that combines multiple strategies
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    strategies: Vec<(BackoffStrategy, ExponentialBackoffConfig)>,
    default_strategy: BackoffStrategy,
}

impl RetryPolicy {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
            default_strategy: BackoffStrategy::ExponentialJittered,
        }
    }

    pub fn with_default_strategy(mut self, strategy: BackoffStrategy) -> Self {
        self.default_strategy = strategy;
        self
    }

    pub fn add_strategy(
        mut self,
        strategy: BackoffStrategy,
        config: ExponentialBackoffConfig,
    ) -> Self {
        self.strategies.push((strategy, config));
        self
    }

    pub fn get_strategy(&self, error_type: &str) -> &ExponentialBackoffConfig {
        for (strategy, config) in &self.strategies {
            if config.retryable_errors.iter().any(|e| error_type.contains(e)) {
                return config;
            }
        }
        // Return default configuration
        &ExponentialBackoffConfig::default()
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating retry policies
pub struct RetryPolicyBuilder {
    policy: RetryPolicy,
}

impl RetryPolicyBuilder {
    pub fn new() -> Self {
        Self {
            policy: RetryPolicy::new(),
        }
    }

    pub fn with_default_strategy(mut self, strategy: BackoffStrategy) -> Self {
        self.policy = self.policy.with_default_strategy(strategy);
        self
    }

    pub fn add_error_type(mut self, error_type: &str) -> Self {
        let mut config = ExponentialBackoffConfig::default();
        config.retryable_errors = vec![error_type.to_string()];
        self.policy.add_strategy(BackoffStrategy::Exponential, config);
        self
    }

    pub fn build(self) -> RetryPolicy {
        self.policy
    }
}

/// Helper function to create a default exponential backoff
pub fn exponential_backoff() -> ExponentialBackoff {
    ExponentialBackoff::new()
}

/// Helper function to create a fast retry policy
pub fn fast_retry() -> ExponentialBackoff {
    ExponentialBackoff::new()
        .with_initial_delay(Duration::from_millis(50))
        .with_max_delay(Duration::from_secs(5))
        .with_max_retries(3)
        .with_exponential_base(2.0)
        .with_jitter(0.2)
}

/// Helper function to create a slow retry policy
pub fn slow_retry() -> ExponentialBackoff {
    ExponentialBackoff::new()
        .with_initial_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(60))
        .with_max_retries(5)
        .with_exponential_base(2.0)
        .with_jitter(0.1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Barrier;
    use std::time::Duration;

    #[tokio::test]
    async fn test_exponential_backoff_calculation() {
        let backoff = ExponentialBackoff::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(30))
            .with_max_retries(10)
            .with_exponential_base(2.0)
            .jitter_enabled(false);

        // Attempt 0: 100ms
        assert_eq!(backoff.calculate_delay(0), Duration::from_millis(100));

        // Attempt 1: 200ms
        assert_eq!(backoff.calculate_delay(1), Duration::from_millis(200));

        // Attempt 2: 400ms
        assert_eq!(backoff.calculate_delay(2), Duration::from_millis(400));

        // Attempt 3: 800ms
        assert_eq!(backoff.calculate_delay(3), Duration::from_millis(800));

        // Attempt 10: should be capped at max_delay (30s)
        assert_eq!(backoff.calculate_delay(10), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_exponential_backoff_with_jitter() {
        let backoff = ExponentialBackoff::new()
            .with_initial_delay(Duration::from_millis(1000))
            .with_max_delay(Duration::from_secs(60))
            .with_max_retries(5)
            .with_exponential_base(2.0)
            .with_jitter(0.3);

        // Delay should vary when jitter is enabled
        let delay1 = backoff.calculate_delay(1);
        let delay2 = backoff.calculate_delay(1);

        // With jitter, delays should be different
        assert_ne!(delay1, delay2);

        // But both should be within expected range
        // Base: 2000ms, jitter 30%: 600ms range
        assert!(delay1 >= Duration::from_millis(1400));
        assert!(delay1 <= Duration::from_millis(2000));

        assert!(delay2 >= Duration::from_millis(1400));
        assert!(delay2 <= Duration::from_millis(2000));
    }

    #[tokio::test]
    async fn test_successful_retry() {
        let attempt_count = Arc::new(AtomicUsize::new(0));
        let attempt_count_clone = attempt_count.clone();

        let backoff = ExponentialBackoff::new()
            .with_initial_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(100))
            .with_max_retries(5)
            .with_exponential_base(2.0);

        let result = backoff
            .execute(move || {
                let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst);
                async move {
                    if count < 2 {
                        Err("temporary error".to_string())
                    } else {
                        Ok("success")
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_max_retries_exceeded() {
        let backoff = ExponentialBackoff::new()
            .with_initial_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(100))
            .with_max_retries(3)
            .with_exponential_base(2.0);

        let result = backoff
            .execute(|| async { Err("persistent error".to_string()) })
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), RetryError::MaxRetriesExceeded(3));
    }

    #[tokio::test]
    async fn test_max_total_time() {
        let backoff = ExponentialBackoff::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_millis(100))
            .with_max_retries(10)
            .with_max_total_time(Some(Duration::from_millis(150)))
            .with_exponential_base(2.0);

        let result = backoff
            .execute(|| async { Err("error".to_string()) })
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            RetryError::Timeout(_) => {
                // Expected
            }
            _ => panic!("Expected timeout error"),
        }
    }

    #[tokio::test]
    async fn test_retry_stats() {
        let backoff = ExponentialBackoff::new()
            .with_initial_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(100))
            .with_max_retries(5)
            .with_exponential_base(2.0);

        // Execute successful retry
        let _ = backoff
            .execute(|| async {
                Err("error".to_string())
            })
            .await;

        let stats = backoff.stats.get_stats();
        assert_eq!(stats.total_attempts, 5);
        assert_eq!(stats.failed_retries, 1);
        assert_eq!(stats.successful_retries, 0);
        assert_eq!(stats.max_attempts_on_single_op, 5);
    }

    #[tokio::test]
    async fn test_sync_execution() {
        let attempt_count = Arc::new(AtomicUsize::new(0));
        let attempt_count_clone = attempt_count.clone();

        let backoff = ExponentialBackoff::new()
            .with_initial_delay(Duration::from_millis(1))
            .with_max_delay(Duration::from_millis(10))
            .with_max_retries(3)
            .with_exponential_base(2.0);

        let result = backoff.execute_sync(move || {
            let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                Err("error".to_string())
            } else {
                Ok("success")
            }
        });

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_policy() {
        let policy = RetryPolicy::new()
            .with_default_strategy(BackoffStrategy::ExponentialJittered)
            .add_error_type("network")
            .add_error_type("timeout");

        let network_config = policy.get_strategy("network_error");
        let default_config = policy.get_strategy("unknown_error");

        assert!(network_config.exponential_base > 0.0);
        assert!(default_config.max_retries > 0);
    }

    #[tokio::test]
    async fn test_helper_functions() {
        let fast = fast_retry();
        assert_eq!(fast.config().initial_delay, Duration::from_millis(50));
        assert_eq!(fast.config().max_retries, 3);

        let slow = slow_retry();
        assert_eq!(slow.config().initial_delay, Duration::from_millis(500));
        assert_eq!(slow.config().max_retries, 5);
    }

    #[tokio::test]
    async fn test_concurrent_retry() {
        let n_tasks = 10;
        let barrier = Arc::new(Barrier::new(n_tasks));
        let attempt_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        for _ in 0..n_tasks {
            let barrier = barrier.clone();
            let attempt_count = attempt_count.clone();

            let handle = tokio::spawn(async move {
                barrier.wait().await;

                let backoff = ExponentialBackoff::new()
                    .with_initial_delay(Duration::from_millis(50))
                    .with_max_delay(Duration::from_millis(200))
                    .with_max_retries(3)
                    .with_exponential_base(2.0)
                    .with_jitter(0.3);

                backoff
                    .execute(|| {
                        let attempt_count = attempt_count.clone();
                        async move {
                            // All tasks succeed on second attempt
                            if attempt_count.load(Ordering::SeqCst) < n_tasks {
                                Err("retry".to_string())
                            } else {
                                Ok("success")
                            }
                        }
                    })
                    .await
            });

            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }
}

