use std::time::Duration;

pub mod drain;
pub mod executor;
pub mod prune;
pub mod reconcile;
pub mod runtime;
pub mod service;
pub mod store;
pub mod submit;

/// Claim lease; fenced by `claim_token` so a lapsed holder's writes are rejected.
pub const JOB_LEASE_MS: u64 = 60_000;
/// Heartbeat interval (renew + progress flush); must stay well under the lease.
pub const JOB_HEARTBEAT_MS: u64 = 20_000;
/// Maximum in-process jobs executing locally at once; excess stays queued.
pub const JOB_CONCURRENCY_CAP: usize = 8;
/// External-attempt supervision slots. Supervising a remote container is IO-trivial,
/// so it gets its own bounded budget and never starves in-process jobs (or vice versa).
pub const JOB_EXTERNAL_CONCURRENCY_CAP: usize = 64;
/// Attempts before a retryable failure becomes terminal `Failed`.
pub const JOB_MAX_ATTEMPTS: u32 = 5;
/// Uniform terminal-state retention before pruning.
pub const JOB_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// Minimum spacing between throttled progress flushes.
pub const JOB_PROGRESS_FLUSH_INTERVAL_MS: u64 = 500;
/// Bounded OCC retries when a job mutation transaction conflicts.
pub const JOB_MUTATE_MAX_ATTEMPTS: u32 = 8;
/// How long a shutdown lets in-flight jobs wind down before handing their leases back.
pub const JOB_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

pub const JOB_DRAIN_BATCH_SIZE: usize = 128;
pub const JOB_DRAIN_RETRY_AFTER: Duration = Duration::from_secs(1);
/// Re-arm floor for the lease head after routing an external attempt to reconcile:
/// its expired lease row stays in place by design, so a zero re-arm would busy-loop
/// the drain.
pub const JOB_RECONCILE_REARM: Duration = Duration::from_millis(JOB_HEARTBEAT_MS);

pub const JOB_PRUNE_SCAN_PAGE_SIZE: usize = 512;
pub const JOB_PRUNE_POLL_AFTER: Duration = Duration::from_secs(60 * 60);
pub const JOB_PRUNE_RETRY_AFTER: Duration = Duration::from_secs(30);
