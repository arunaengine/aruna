use super::*;

/// Version prefix keeping the record wrappable in a version envelope later (#286).
pub const JOB_RECORD_KEY_PREFIX: &[u8] = b"jobs-v1/";

pub const JOB_DUE_INDEX_PREFIX: &[u8] = b"due/";
pub const JOB_LEASE_INDEX_PREFIX: &[u8] = b"lease/";
pub const JOB_PRUNE_INDEX_PREFIX: &[u8] = b"prune/";
/// Invalid UTF-8 byte separating generated report rows from user paths.
pub const JOB_SYSTEM_ENTRY_PREFIX: u8 = u8::MAX;
pub const DEFAULT_JOB_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Creation-ordered job identifier stored at API and persistence boundaries.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct JobId(Ulid);

impl JobId {
    pub fn from_routable(job_id: RoutableJobId) -> Self {
        Self(job_id.as_ulid())
    }

    pub fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, FieldError> {
        RoutableJobId::from_bytes(bytes).map(Self::from_routable)
    }

    /// Constructs an id from trusted structured bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self::try_from_bytes(bytes).expect("job id bytes carry a structured placement handle")
    }

    pub fn from_parts(
        timestamp_ms: u64,
        handle: PlacementHandle,
        bucket: BucketId,
        nonce: u64,
    ) -> Result<Self, FieldError> {
        RoutableJobId::from_parts(timestamp_ms, handle, bucket, nonce).map(Self::from_routable)
    }

    pub fn as_routable(self) -> Result<RoutableJobId, FieldError> {
        RoutableJobId::from_bytes(self.to_bytes())
    }

    pub fn as_ulid(self) -> Ulid {
        self.0
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_bytes()
    }

    pub fn timestamp_ms(&self) -> u64 {
        self.0.timestamp_ms()
    }
}

impl<'de> Deserialize<'de> for JobId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let ulid = Ulid::deserialize(deserializer)?;
        Self::try_from_bytes(ulid.to_bytes()).map_err(serde::de::Error::custom)
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
        RoutableJobId::parse(value)
            .map(Self::from_routable)
            .map_err(|error| ConversionError::FromStrError(error.to_string()))
    }
}

/// Whether a payload runs in-process (idempotent, safe to requeue) or drives an external attempt (a
/// container that MUST NOT run twice). The lease sweep and restart recovery branch on this to route
/// external attempts to reconciliation instead of a blind requeue.
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
