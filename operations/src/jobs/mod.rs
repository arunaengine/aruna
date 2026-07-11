use std::time::Duration;

pub mod drain;
pub mod executor;
pub mod runtime;
pub mod store;
pub mod submit;

/// Lease held by a claiming executor. A claim is fenced by its `claim_token`; a
/// zombie executor whose lease lapsed and whose job was re-claimed has every write
/// rejected.
pub const JOB_LEASE_MS: u64 = 60_000;
/// Executor heartbeat interval. Each renew piggybacks a progress flush; must stay
/// well under `JOB_LEASE_MS`.
pub const JOB_HEARTBEAT_MS: u64 = 20_000;
/// Maximum jobs executing locally at once; excess stays queued.
pub const JOB_CONCURRENCY_CAP: usize = 8;
/// Attempts (inclusive) before a retryable failure becomes terminal `Failed`.
pub const JOB_MAX_ATTEMPTS: u32 = 5;
/// Uniform terminal-state retention before a job record is pruned.
pub const JOB_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// Minimum spacing between throttled progress flushes.
pub const JOB_PROGRESS_FLUSH_INTERVAL_MS: u64 = 500;

pub const JOB_DRAIN_BATCH_SIZE: usize = 128;
pub const JOB_DRAIN_RETRY_AFTER: Duration = Duration::from_secs(1);

pub const JOB_PRUNE_SCAN_PAGE_SIZE: usize = 512;
pub const JOB_PRUNE_POLL_AFTER: Duration = Duration::from_secs(60 * 60);
pub const JOB_PRUNE_RETRY_AFTER: Duration = Duration::from_secs(30);
