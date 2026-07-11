use std::time::Duration;

pub mod drain;
pub mod executor;
pub mod prune;
pub mod runtime;
pub mod service;
pub mod store;
pub mod submit;

/// Claim lease; fenced by `claim_token` so a lapsed holder's writes are rejected.
pub const JOB_LEASE_MS: u64 = 60_000;
/// Heartbeat interval (renew + progress flush); must stay well under the lease.
pub const JOB_HEARTBEAT_MS: u64 = 20_000;
/// Maximum jobs executing locally at once; excess stays queued.
pub const JOB_CONCURRENCY_CAP: usize = 8;
/// Attempts before a retryable failure becomes terminal `Failed`.
pub const JOB_MAX_ATTEMPTS: u32 = 5;
/// Uniform terminal-state retention before pruning.
pub const JOB_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// Minimum spacing between throttled progress flushes.
pub const JOB_PROGRESS_FLUSH_INTERVAL_MS: u64 = 500;
/// Bounded OCC retries when a job mutation transaction conflicts.
pub const JOB_MUTATE_MAX_ATTEMPTS: u32 = 8;

pub const JOB_DRAIN_BATCH_SIZE: usize = 128;
pub const JOB_DRAIN_RETRY_AFTER: Duration = Duration::from_secs(1);

pub const JOB_PRUNE_SCAN_PAGE_SIZE: usize = 512;
pub const JOB_PRUNE_POLL_AFTER: Duration = Duration::from_secs(60 * 60);
pub const JOB_PRUNE_RETRY_AFTER: Duration = Duration::from_secs(30);
