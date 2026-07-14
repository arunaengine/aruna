use std::fmt;
use std::str::FromStr;

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::invert_timestamp_ms;
use crate::types::{Key, UserId};

/// Version prefix keeping the record wrappable in a version envelope later (#286).
pub const JOB_RECORD_KEY_PREFIX: &[u8] = b"jobs-v1/";

pub const JOB_DUE_INDEX_PREFIX: &[u8] = b"due/";
pub const JOB_LEASE_INDEX_PREFIX: &[u8] = b"lease/";
pub const JOB_PRUNE_INDEX_PREFIX: &[u8] = b"prune/";

/// Creation-ordered job identifier.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JobId(pub Ulid);

impl JobId {
    #[inline]
    pub fn new() -> Self {
        Self(Ulid::r#gen())
    }

    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Ulid::from_bytes(bytes))
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_bytes()
    }

    #[inline]
    pub fn timestamp_ms(&self) -> u64 {
        self.0.timestamp_ms()
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JobId({})", self.0)
    }
}

impl FromStr for JobId {
    type Err = ConversionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(Ulid::from_string(value)?))
    }
}

/// Whether a payload runs in-process (idempotent, safe to requeue) or drives an
/// external attempt (a container that MUST NOT run twice). The lease sweep and
/// restart recovery branch on this to route external attempts to reconciliation
/// instead of a blind requeue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobExecutionClass {
    InProcess,
    ExternalAttempt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Claimed,
    Preparing,
    Ready,
    Running,
    Cancelling,
    Indeterminate,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Failed | JobState::Cancelled
        )
    }

    /// Stable machine-readable name for API payloads. Never change an existing mapping.
    pub fn name(&self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Claimed => "claimed",
            JobState::Preparing => "preparing",
            JobState::Ready => "ready",
            JobState::Running => "running",
            JobState::Cancelling => "cancelling",
            JobState::Indeterminate => "indeterminate",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        }
    }
}

/// Closed job payload enum, keeping the typed-queue discipline of `TaskKey` and
/// `DocumentSyncOutboxEvent`. Additive-only until a version envelope lands (#286).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobPayload {
    /// Test-only executor. Idempotency key: the `cleanup_marker` file, which a
    /// re-driven or cancelled Probe removes so re-running from scratch is safe.
    Probe {
        steps: u32,
        step_sleep_ms: u64,
        fail_at: Option<u32>,
        panic_at: Option<u32>,
        cleanup_marker: Option<String>,
    },
}

impl JobPayload {
    /// Stable discriminant string. Payload internals are never echoed verbatim.
    pub fn kind(&self) -> &'static str {
        match self {
            JobPayload::Probe { .. } => "probe",
        }
    }

    /// Default progress unit for a freshly submitted job of this kind.
    pub fn progress_unit(&self) -> &'static str {
        match self {
            JobPayload::Probe { .. } => "steps",
        }
    }

    /// Execution class. Internal payloads are safe to requeue; external attempts
    /// are not. The future `Execution` payload (Stage 2) is `ExternalAttempt`.
    pub fn execution_class(&self) -> JobExecutionClass {
        match self {
            JobPayload::Probe { .. } => JobExecutionClass::InProcess,
        }
    }
}

/// Closed result enum parallel to `JobPayload`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobResultPayload {
    Probe { completed_steps: u32 },
}

impl JobResultPayload {
    pub fn kind(&self) -> &'static str {
        match self {
            JobResultPayload::Probe { .. } => "probe",
        }
    }

    /// Payload-specific public projection returned by the REST surface.
    pub fn to_public_json(&self) -> serde_json::Value {
        match self {
            JobResultPayload::Probe { completed_steps } => {
                serde_json::json!({ "completed_steps": completed_steps })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobErrorKind {
    Retryable,
    Permanent,
}

impl JobErrorKind {
    pub fn name(&self) -> &'static str {
        match self {
            JobErrorKind::Retryable => "retryable",
            JobErrorKind::Permanent => "permanent",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobError {
    pub message: String,
    pub kind: JobErrorKind,
}

impl JobError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: JobErrorKind::Retryable,
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: JobErrorKind::Permanent,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    pub current: u64,
    pub total: Option<u64>,
    pub unit: String,
}

impl JobProgress {
    pub fn new(unit: impl Into<String>) -> Self {
        Self {
            current: 0,
            total: None,
            unit: unit.into(),
        }
    }
}

/// Lease on the job; `claim_token` fences zombie executors on every write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobClaim {
    pub holder_node_id: NodeId,
    pub claim_token: Ulid,
    pub lease_expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    pub job_id: JobId,
    pub payload: JobPayload,
    pub state: JobState,
    pub created_by: UserId,
    pub owner_node_id: NodeId,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub due_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub attempts: u32,
    pub has_run: bool,
    pub last_error: Option<JobError>,
    pub progress: JobProgress,
    pub cancel_requested: bool,
    pub claim: Option<JobClaim>,
    pub dedup_key: Option<Vec<u8>>,
    pub result: Option<JobResultPayload>,
    pub execution_class: JobExecutionClass,
}

impl JobRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: JobId,
        payload: JobPayload,
        created_by: UserId,
        owner_node_id: NodeId,
        created_at_ms: u64,
        due_at_ms: u64,
        dedup_key: Option<Vec<u8>>,
    ) -> Self {
        let unit = payload.progress_unit();
        let execution_class = payload.execution_class();
        Self {
            job_id,
            payload,
            state: JobState::Queued,
            created_by,
            owner_node_id,
            created_at_ms,
            updated_at_ms: created_at_ms,
            due_at_ms,
            finished_at_ms: None,
            attempts: 0,
            has_run: false,
            last_error: None,
            progress: JobProgress::new(unit),
            cancel_requested: false,
            claim: None,
            dedup_key,
            result: None,
            execution_class,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("illegal job transition {from:?} -> {to:?}")]
pub struct JobTransitionError {
    pub from: JobState,
    pub to: JobState,
}

/// Pure state-machine guard, guarded by execution class so in-process jobs keep the
/// original graph exactly and only external attempts use the extended states.
/// Terminal states absorb nothing: any transition out of a terminal state is rejected.
pub fn validate_transition(
    class: JobExecutionClass,
    from: JobState,
    to: JobState,
) -> Result<(), JobTransitionError> {
    let legal = match class {
        JobExecutionClass::InProcess => in_process_transition(from, to),
        JobExecutionClass::ExternalAttempt => external_attempt_transition(from, to),
    };
    if legal {
        Ok(())
    } else {
        Err(JobTransitionError { from, to })
    }
}

fn in_process_transition(from: JobState, to: JobState) -> bool {
    use JobState::*;
    matches!(
        (from, to),
        (Queued, Claimed)
            | (Queued, Cancelled)
            | (Claimed, Running)
            | (Claimed, Queued)
            | (Claimed, Cancelled)
            | (Claimed, Failed)
            | (Running, Succeeded)
            | (Running, Failed)
            | (Running, Cancelled)
            | (Running, Queued)
    )
}

/// The fenced execution graph (spec 16.7): a requeue is legal only before an attempt
/// is submitted; `Indeterminate` exits only on evidence.
fn external_attempt_transition(from: JobState, to: JobState) -> bool {
    use JobState::*;
    matches!(
        (from, to),
        (Queued, Claimed)
            | (Queued, Cancelled)
            | (Claimed, Preparing)
            | (Claimed, Queued)
            | (Claimed, Cancelled)
            | (Claimed, Failed)
            | (Preparing, Ready)
            | (Preparing, Queued)
            | (Ready, Running)
            | (Ready, Queued)
            | (Running, Succeeded)
            | (Running, Failed)
            | (Running, Cancelling)
            | (Running, Indeterminate)
            | (Cancelling, Cancelled)
            | (Cancelling, Indeterminate)
            | (Indeterminate, Running)
            | (Indeterminate, Cancelling)
            | (Indeterminate, Succeeded)
            | (Indeterminate, Failed)
            | (Indeterminate, Cancelled)
    )
}

pub fn job_record_key(job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(JOB_RECORD_KEY_PREFIX.len() + 16);
    bytes.extend_from_slice(JOB_RECORD_KEY_PREFIX);
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

fn schedule_index_key(prefix: &[u8], timestamp_ms: u64, job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(prefix.len() + 8 + 16);
    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(&timestamp_ms.to_be_bytes());
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

pub fn job_due_index_key(due_at_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_DUE_INDEX_PREFIX, due_at_ms, job_id)
}

pub fn job_lease_index_key(lease_expires_at_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_LEASE_INDEX_PREFIX, lease_expires_at_ms, job_id)
}

pub fn job_prune_index_key(retention_expiry_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_PRUNE_INDEX_PREFIX, retention_expiry_ms, job_id)
}

/// Extract `(timestamp_ms, job_id)` from a `due/`, `lease/`, or `prune/` schedule
/// index key.
pub fn parse_job_schedule_index_key(key: &[u8]) -> Result<(u64, JobId), ConversionError> {
    for prefix in [
        JOB_DUE_INDEX_PREFIX,
        JOB_LEASE_INDEX_PREFIX,
        JOB_PRUNE_INDEX_PREFIX,
    ] {
        if let Some(rest) = key.strip_prefix(prefix) {
            if rest.len() != 24 {
                return Err(ConversionError::InvalidLength(format!(
                    "expected 24-byte schedule index suffix, got {}",
                    rest.len()
                )));
            }
            let timestamp_ms = u64::from_be_bytes(rest[..8].try_into()?);
            let job_id = JobId::from_bytes(rest[8..24].try_into()?);
            return Ok((timestamp_ms, job_id));
        }
    }
    Err(ConversionError::InvalidLength(
        "unknown job schedule index prefix".to_string(),
    ))
}

pub fn job_owner_index_key(created_by: UserId, created_at_ms: u64, job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(72);
    bytes.extend_from_slice(&created_by.to_storage_key());
    bytes.extend_from_slice(&invert_timestamp_ms(created_at_ms).to_be_bytes());
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

pub fn job_owner_index_prefix(created_by: UserId) -> Key {
    ByteView::from(created_by.to_storage_key())
}

pub fn job_owner_cursor(created_at_ms: u64, job_id: JobId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(24);
    bytes.extend_from_slice(&invert_timestamp_ms(created_at_ms).to_be_bytes());
    bytes.extend_from_slice(&job_id.to_bytes());
    bytes
}

pub fn parse_job_owner_index_key(key: &[u8]) -> Result<(UserId, u64, JobId), ConversionError> {
    if key.len() != 72 {
        return Err(ConversionError::InvalidLength(format!(
            "expected 72-byte job owner index key, got {} bytes",
            key.len()
        )));
    }
    let created_by = UserId::from_storage_key(&key[..48])?;
    let created_at_ms = invert_timestamp_ms(u64::from_be_bytes(key[48..56].try_into()?));
    let job_id = JobId::from_bytes(key[56..72].try_into()?);
    Ok((created_by, created_at_ms, job_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn user(realm: u8, byte: u8) -> UserId {
        UserId::new(Ulid::from_bytes([byte; 16]), RealmId([realm; 32]))
    }

    fn probe_record(job_id: JobId, created_at_ms: u64) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 3,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: Some("/tmp/probe-marker".to_string()),
            },
            user(1, 2),
            node_id(7),
            created_at_ms,
            created_at_ms,
            Some(b"dedup".to_vec()),
        )
    }

    #[test]
    fn legal_transitions() {
        let legal = [
            (JobState::Queued, JobState::Claimed),
            (JobState::Queued, JobState::Cancelled),
            (JobState::Claimed, JobState::Running),
            (JobState::Claimed, JobState::Queued),
            (JobState::Claimed, JobState::Cancelled),
            (JobState::Claimed, JobState::Failed),
            (JobState::Running, JobState::Succeeded),
            (JobState::Running, JobState::Failed),
            (JobState::Running, JobState::Cancelled),
            (JobState::Running, JobState::Queued),
        ];
        for (from, to) in legal {
            assert!(
                validate_transition(JobExecutionClass::InProcess, from, to).is_ok(),
                "{from:?} -> {to:?}"
            );
        }
    }

    #[test]
    fn illegal_transitions() {
        let illegal = [
            (JobState::Queued, JobState::Running),
            (JobState::Queued, JobState::Succeeded),
            (JobState::Queued, JobState::Failed),
            (JobState::Queued, JobState::Queued),
            (JobState::Claimed, JobState::Succeeded),
            (JobState::Claimed, JobState::Claimed),
            (JobState::Running, JobState::Claimed),
            (JobState::Running, JobState::Running),
        ];
        for (from, to) in illegal {
            assert_eq!(
                validate_transition(JobExecutionClass::InProcess, from, to),
                Err(JobTransitionError { from, to }),
                "{from:?} -> {to:?}"
            );
        }
    }

    // External-only states must be rejected for an in-process job.
    #[test]
    fn internal_rejects_external() {
        let external_only = [
            (JobState::Claimed, JobState::Preparing),
            (JobState::Preparing, JobState::Ready),
            (JobState::Ready, JobState::Running),
            (JobState::Running, JobState::Cancelling),
            (JobState::Running, JobState::Indeterminate),
            (JobState::Cancelling, JobState::Cancelled),
            (JobState::Indeterminate, JobState::Running),
        ];
        for (from, to) in external_only {
            assert_eq!(
                validate_transition(JobExecutionClass::InProcess, from, to),
                Err(JobTransitionError { from, to }),
                "in-process must reject {from:?} -> {to:?}"
            );
        }
    }

    // The fenced execution graph is accepted for external attempts.
    #[test]
    fn external_graph_legal() {
        let legal = [
            (JobState::Claimed, JobState::Preparing),
            (JobState::Preparing, JobState::Ready),
            (JobState::Preparing, JobState::Queued),
            (JobState::Ready, JobState::Running),
            (JobState::Ready, JobState::Queued),
            (JobState::Running, JobState::Cancelling),
            (JobState::Running, JobState::Indeterminate),
            (JobState::Cancelling, JobState::Cancelled),
            (JobState::Cancelling, JobState::Indeterminate),
            (JobState::Indeterminate, JobState::Running),
            (JobState::Indeterminate, JobState::Succeeded),
        ];
        for (from, to) in legal {
            assert!(
                validate_transition(JobExecutionClass::ExternalAttempt, from, to).is_ok(),
                "external must accept {from:?} -> {to:?}"
            );
        }
        // Ready cannot skip straight to Succeeded.
        assert!(
            validate_transition(
                JobExecutionClass::ExternalAttempt,
                JobState::Ready,
                JobState::Succeeded,
            )
            .is_err()
        );
    }

    #[test]
    fn terminal_absorbs() {
        for class in [
            JobExecutionClass::InProcess,
            JobExecutionClass::ExternalAttempt,
        ] {
            for from in [JobState::Succeeded, JobState::Failed, JobState::Cancelled] {
                for to in [
                    JobState::Queued,
                    JobState::Claimed,
                    JobState::Preparing,
                    JobState::Ready,
                    JobState::Running,
                    JobState::Cancelling,
                    JobState::Indeterminate,
                    JobState::Succeeded,
                    JobState::Failed,
                    JobState::Cancelled,
                ] {
                    assert_eq!(
                        validate_transition(class, from, to),
                        Err(JobTransitionError { from, to }),
                        "terminal {from:?} must reject -> {to:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn record_roundtrips() {
        let record = probe_record(JobId::from_bytes([5u8; 16]), 1_700_000_000_000);
        let bytes = record.to_bytes().unwrap();
        assert_eq!(JobRecord::from_bytes(&bytes).unwrap(), record);
    }

    #[test]
    fn record_key_versioned() {
        let key = job_record_key(JobId::from_bytes([9u8; 16]));
        assert!(key.starts_with(JOB_RECORD_KEY_PREFIX));
        assert_eq!(key.len(), JOB_RECORD_KEY_PREFIX.len() + 16);
    }

    #[test]
    fn due_index_ordered() {
        let id = JobId::from_bytes([1u8; 16]);
        assert!(job_due_index_key(1_000, id) < job_due_index_key(2_000, id));
        let (ts, parsed) = parse_job_schedule_index_key(&job_due_index_key(1_234, id)).unwrap();
        assert_eq!(ts, 1_234);
        assert_eq!(parsed, id);
    }

    #[test]
    fn prefixes_disjoint() {
        let id = JobId::from_bytes([1u8; 16]);
        let due = job_due_index_key(5, id);
        let lease = job_lease_index_key(5, id);
        let prune = job_prune_index_key(5, id);
        assert!(due < lease);
        assert!(lease < prune);
        assert!(!due.starts_with(JOB_LEASE_INDEX_PREFIX));
        assert!(!lease.starts_with(JOB_PRUNE_INDEX_PREFIX));
    }

    #[test]
    fn owner_index_newest_first() {
        let u = user(1, 2);
        let id = JobId::from_bytes([3u8; 16]);
        assert!(job_owner_index_key(u, 2_000, id) < job_owner_index_key(u, 1_000, id));
        assert!(job_owner_index_key(u, 1_000, id).starts_with(job_owner_index_prefix(u).as_ref()));
    }

    #[test]
    fn owner_key_roundtrips() {
        let u = user(5, 9);
        let ts = 1_700_000_000_000u64;
        let id = JobId::new();
        let key = job_owner_index_key(u, ts, id);
        assert_eq!(parse_job_owner_index_key(&key).unwrap(), (u, ts, id));
        assert_eq!(job_owner_cursor(ts, id).as_slice(), &key[48..72]);
    }

    #[test]
    fn owner_index_scoped() {
        let a = user(1, 2);
        let b = user(1, 3);
        let id = JobId::from_bytes([4u8; 16]);
        let ka = job_owner_index_key(a, 1_000, id);
        assert!(!ka.starts_with(job_owner_index_prefix(b).as_ref()));
    }
}
