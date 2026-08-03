//! Job-control RPC contract: the discrete request/response verbs an owner node
//! answers, plus the wire projections they carry. Effects and events reference
//! these so the routing policy stays sans-I/O.

use std::ops::Range;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::metadata::MetadataAuthToken;
use crate::structs::{
    JobError, JobId, JobPayload, JobProgress, JobRecord, JobResultPayload, JobState, WorkspaceMode,
};
use crate::types::UserId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    Probe,
    Execution,
    WriteRunCrate,
    TerminalCleanup,
    Staging,
    ImportRoCrate,
    ExportRoCrate,
}

impl JobKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::Execution => "execution",
            Self::WriteRunCrate => "write_run_crate",
            Self::TerminalCleanup => "terminal_cleanup",
            Self::Staging => "staging",
            Self::ImportRoCrate => "import_rocrate",
            Self::ExportRoCrate => "export_rocrate",
        }
    }

    pub fn is_internal(self) -> bool {
        matches!(self, Self::WriteRunCrate | Self::TerminalCleanup)
    }

    pub fn is_report(self) -> bool {
        matches!(self, Self::ImportRoCrate | Self::ExportRoCrate)
    }
}

impl From<&JobPayload> for JobKind {
    fn from(payload: &JobPayload) -> Self {
        match payload {
            JobPayload::Probe { .. } => Self::Probe,
            JobPayload::Execution(_) => Self::Execution,
            JobPayload::WriteRunCrate { .. } => Self::WriteRunCrate,
            JobPayload::TerminalCleanup { .. } => Self::TerminalCleanup,
            JobPayload::Staging(_) => Self::Staging,
            JobPayload::ImportRoCrate(_) => Self::ImportRoCrate,
            JobPayload::ExportRoCrate(_) => Self::ExportRoCrate,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobStatusView {
    pub job_id: JobId,
    pub created_by: UserId,
    pub kind: JobKind,
    pub state: JobState,
    pub attempts: u32,
    pub cancel_requested: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub progress: JobProgress,
    pub last_error: Option<JobError>,
    #[serde(with = "json_value")]
    pub result: Option<JsonValue>,
    pub workspace_bucket: Option<String>,
    pub workspace_mode: WorkspaceMode,
}

mod json_value {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_json::Value;

    pub fn serialize<S>(value: &Option<Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.as_ref().map(Value::to_string).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|value| serde_json::from_str(&value).map_err(serde::de::Error::custom))
            .transpose()
    }
}

impl From<&JobRecord> for JobStatusView {
    fn from(record: &JobRecord) -> Self {
        Self {
            job_id: record.job_id,
            created_by: record.created_by,
            kind: JobKind::from(&record.payload),
            state: record.state,
            attempts: record.attempts,
            cancel_requested: record.cancel_requested,
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
            finished_at_ms: record.finished_at_ms,
            progress: record.progress.clone(),
            last_error: record.last_error.clone(),
            result: record.result.as_ref().map(JobResultPayload::to_public_json),
            workspace_bucket: record.workspace_bucket.clone(),
            workspace_mode: record.workspace_mode,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobReportView {
    pub job_id: JobId,
    pub created_by: UserId,
    pub kind: JobKind,
    pub report_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRange {
    pub start: u64,
    pub end: u64,
}

impl From<Range<u64>> for WireRange {
    fn from(range: Range<u64>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireArtifact {
    pub job_id: JobId,
    pub created_by: UserId,
    pub blake3: [u8; 32],
    pub size: u64,
    pub filename: String,
}

/// Owner-directed requests only: every operation on a job is answered by its
/// immutable owner, derived from the JobId on both ends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobRequest {
    Status {
        auth_token: MetadataAuthToken,
        job_id: JobId,
    },
    Report {
        auth_token: MetadataAuthToken,
        job_id: JobId,
        expected_digest: Option<[u8; 32]>,
        last_key: Option<Vec<u8>>,
        limit: u16,
    },
    Artifact {
        auth_token: MetadataAuthToken,
        job_id: JobId,
        range: Option<WireRange>,
    },
    Cancel {
        auth_token: MetadataAuthToken,
        job_id: JobId,
    },
    /// Full owner record for API projections (TES, staging) the status view
    /// cannot reconstruct off-owner.
    Record {
        auth_token: MetadataAuthToken,
        job_id: JobId,
    },
}

impl JobRequest {
    pub fn auth_token(&self) -> MetadataAuthToken {
        match self {
            Self::Status { auth_token, .. }
            | Self::Report { auth_token, .. }
            | Self::Artifact { auth_token, .. }
            | Self::Cancel { auth_token, .. }
            | Self::Record { auth_token, .. } => auth_token.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JobResponse {
    Unauthorized,
    Forbidden,
    NotFound,
    Unavailable(String),
    Status {
        job: JobStatusView,
        run_crate: Option<String>,
    },
    ReportPending(JobState),
    ReportConflict,
    ReportReady {
        job: JobReportView,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        next_key: Option<Vec<u8>>,
    },
    ArtifactPending(JobState),
    ArtifactGone,
    ArtifactReady {
        owned: WireArtifact,
        stream_size: u64,
    },
    Cancelled {
        job: JobStatusView,
        terminal: bool,
    },
    Record(Box<JobRecord>),
}
