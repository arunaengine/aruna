//! Defines the job control request and response verbs and the wire records they carry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use ulid::Ulid;

use crate::UserId;
use crate::events::Event;
use crate::id::NodeId;
use crate::metadata::AuthToken;
use crate::operation::SubOperation;
use crate::stream::{BackendStream, StreamError};
use crate::structs::execution::job::{
    InputSelection, JobError, JobId, JobPayload, JobProgress, JobRecord, JobResultPayload,
    JobState, KeyWait, LogicalJobState, StagingJobCheckpoint, SubmissionId, WorkspaceMode,
};
use crate::structs::execution::table::GzipCursor;
use crate::structs::identity::auth::PathRestriction;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::encryption::BucketKeyRef;
use crate::structs::storage::storage_purge::StoragePurgeScope;
use crate::types::GroupId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    Probe,
    Execution,
    WriteRunCrate,
    TerminalCleanup,
    Staging,
    ImportRoCrate,
    ExportRoCrate,
    Harvest,
    MintPersistentId,
    StoragePurge,
    CopyObject,
    RegisterIdentifiers,
    BackendImport,
    WebhookDelivery,
    EmailDelivery,
    PluginWork,
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
            Self::Harvest => "harvest",
            Self::MintPersistentId => "mint_persistent_id",
            Self::StoragePurge => "storage_purge",
            Self::CopyObject => "copy_object",
            Self::RegisterIdentifiers => "register_identifiers",
            Self::BackendImport => "backend_import",
            Self::WebhookDelivery => "webhook_delivery",
            Self::EmailDelivery => "email_delivery",
            Self::PluginWork => "plugin_work",
        }
    }

    pub fn is_internal(self) -> bool {
        matches!(
            self,
            Self::WriteRunCrate | Self::TerminalCleanup | Self::RegisterIdentifiers
        )
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
            JobPayload::Harvest(_) => Self::Harvest,
            JobPayload::MintPersistentId(_) => Self::MintPersistentId,
            JobPayload::StoragePurge(_) | JobPayload::StoragePurgeFiles(_) => Self::StoragePurge,
            JobPayload::CopyObject(_) => Self::CopyObject,
            JobPayload::RegisterIdentifiers(_) => Self::RegisterIdentifiers,
            JobPayload::BackendImport(_) => Self::BackendImport,
            JobPayload::WebhookDelivery(_) => Self::WebhookDelivery,
            JobPayload::EmailDelivery(_) => Self::EmailDelivery,
            JobPayload::PluginWork(_) => Self::PluginWork,
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
    /// This node spent its attempts without a job-specific verdict; no further
    /// automatic attempt runs here and the outcome stays undecided.
    pub locally_exhausted: bool,
    /// Set for a notebook session: the catalog runtime it runs.
    pub session_runtime: Option<String>,
    /// Bucket keys an `awaiting_key` job still needs; filled by the owning node.
    pub awaiting_keys: Vec<KeyWait>,
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
            locally_exhausted: record.locally_exhausted,
            session_runtime: match &record.payload {
                JobPayload::Execution(spec) => spec.session_runtime(),
                _ => None,
            },
            awaiting_keys: Vec::new(),
        }
    }
}

/// What a running job works on: a bucket with an optional key or prefix, or a dataset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobTarget {
    Bucket {
        bucket: String,
        path: Option<String>,
        /// `path` is one exact key; otherwise the job touches every key that starts with it.
        exact: bool,
    },
    Dataset {
        dataset_id: Ulid,
    },
}

/// A job running on this node now; the group and creator decide who may see its details.
#[derive(Clone, Debug, PartialEq)]
pub struct RunningJobView {
    pub kind: JobKind,
    pub target: Option<JobTarget>,
    pub group_id: Option<GroupId>,
    pub created_by: UserId,
    /// Only set while the job counts something.
    pub progress: Option<JobProgress>,
    pub started_at_ms: u64,
}

impl RunningJobView {
    pub fn new(record: &JobRecord, started_at_ms: u64) -> Self {
        let bucket = |bucket: &str, path: &str, exact: bool| JobTarget::Bucket {
            bucket: bucket.to_string(),
            path: (!path.is_empty()).then(|| path.to_string()),
            exact,
        };
        let dataset = |dataset_id: Ulid| Some(JobTarget::Dataset { dataset_id });
        let (target, group_id) = match &record.payload {
            JobPayload::Probe { .. }
            | JobPayload::WriteRunCrate { .. }
            | JobPayload::TerminalCleanup { .. } => (None, None),
            JobPayload::Execution(spec) => (
                record
                    .workspace_bucket
                    .as_deref()
                    .map(|name| bucket(name, "", false)),
                Some(spec.group_id),
            ),
            JobPayload::Staging(spec) => {
                let path = match spec.prefixes.as_slice() {
                    [prefix] => prefix.target_prefix.as_str(),
                    _ => "",
                };
                (Some(bucket(&spec.bucket, path, false)), Some(spec.group_id))
            }
            JobPayload::ImportRoCrate(spec) => (
                Some(bucket(&spec.target.bucket, &spec.target.prefix, false)),
                Some(spec.metadata.group_id),
            ),
            JobPayload::ExportRoCrate(spec) => (dataset(spec.document_id), None),
            JobPayload::Harvest(spec) => (None, Some(spec.group_id)),
            JobPayload::MintPersistentId(spec) => (dataset(spec.document_id), None),
            JobPayload::StoragePurge(spec) | JobPayload::StoragePurgeFiles(spec) => {
                let (path, exact) = match &spec.scope {
                    StoragePurgeScope::File { key, .. } => (key.as_str(), true),
                    StoragePurgeScope::Prefix { prefix, .. } => (prefix.as_str(), false),
                    StoragePurgeScope::Bucket { .. } => ("", false),
                };
                (
                    Some(bucket(spec.scope.bucket(), path, exact)),
                    Some(spec.group_id),
                )
            }
            JobPayload::CopyObject(spec) => (
                Some(bucket(&spec.dest_bucket, &spec.dest_key, true)),
                Some(spec.group_id),
            ),
            JobPayload::RegisterIdentifiers(spec) => (dataset(spec.document_id), None),
            JobPayload::BackendImport(spec) => (None, Some(spec.group_id)),
            JobPayload::WebhookDelivery(_) => (None, None),
            JobPayload::EmailDelivery(_) => (None, None),
            JobPayload::PluginWork(spec) => (None, Some(spec.group_id)),
        };
        Self {
            kind: JobKind::from(&record.payload),
            target,
            group_id,
            created_by: record.created_by,
            progress: None,
            started_at_ms,
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
        auth_token: AuthToken,
        job_id: JobId,
    },
    Report {
        auth_token: AuthToken,
        job_id: JobId,
        expected_digest: Option<[u8; 32]>,
        last_key: Option<Vec<u8>>,
        limit: u16,
    },
    Artifact {
        auth_token: AuthToken,
        job_id: JobId,
        range: Option<WireRange>,
    },
    Cancel {
        auth_token: AuthToken,
        job_id: JobId,
    },
    /// Full owner record for API projections (TES, staging) the status view
    /// cannot reconstruct off-owner.
    Record {
        auth_token: AuthToken,
        job_id: JobId,
    },
    /// Registers `job_id` of the sending node as waiting for the keys of these exact sources.
    /// The answer lists only keys still locked after the registration, closing the unlock race.
    AwaitKeys {
        auth_token: AuthToken,
        job_id: JobId,
        targets: Vec<WaitTarget>,
    },
    /// Tells the waiting node that `key` of the sending node is unlocked. Grants no read.
    KeyWake {
        auth_token: AuthToken,
        job_id: JobId,
        key: BucketKeyRef,
    },
}

/// The exact source a remote key wait names; the key node authorizes and admits only that.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitTarget {
    /// One object version, also a reference whose content hash is not known.
    Object {
        bucket: String,
        key: String,
        version_id: Ulid,
    },
    /// The version a captured input names among the aliases of its content.
    Captured { blake3: [u8; 32], version_id: Ulid },
    /// Any readable version of a content; it waits only while none is readable.
    Content { blake3: [u8; 32] },
}

impl JobRequest {
    pub fn auth_token(&self) -> AuthToken {
        match self {
            Self::Status { auth_token, .. }
            | Self::Report { auth_token, .. }
            | Self::Artifact { auth_token, .. }
            | Self::Cancel { auth_token, .. }
            | Self::Record { auth_token, .. }
            | Self::AwaitKeys { auth_token, .. }
            | Self::KeyWake { auth_token, .. } => auth_token.clone(),
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
    Record {
        record: Box<JobRecord>,
        checkpoint: Option<StagingJobCheckpoint>,
    },
    /// Keys still locked on the answering node; empty means every content is readable.
    KeysLocked(Vec<KeyWait>),
    KeyWakeAcked,
}

/// The holder's answer to a forwarded submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionAck {
    pub job_id: JobId,
    pub created: bool,
    pub state: LogicalJobState,
}

/// Why a holder did not accept a forwarded submission. Only a definitive
/// refusal ends the ingress; anything else lets it try the next holder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionRefusal {
    Unauthorized,
    /// This node does not hold the family in its own unconflicted view.
    NotHolder,
    /// The recomputed identity or request digest differs from the forwarded one.
    IdentityMismatch,
    Conflict {
        existing_job_id: JobId,
    },
    /// The holder read the request and refused it; the reason reaches the caller.
    Invalid {
        reason: String,
    },
    Unavailable,
}

/// A failed call leaves the request unanswered, which another holder may still answer.
impl From<crate::calls::IoError> for SubmissionRefusal {
    fn from(_: crate::calls::IoError) -> Self {
        Self::Unavailable
    }
}

/// The inputs an ingress asks their source node to resolve and grant for one caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputResolveRequest {
    pub created_by: UserId,
    pub submission_id: SubmissionId,
    /// Unix seconds the grants end at, bounded by this node's job retention.
    pub expires_at: u64,
    pub inputs: Vec<InputSelection>,
}

/// What a running job asks its host, the runtime that holds its stop signals, progress and
/// range turns; never persisted.
#[derive(Debug, PartialEq)]
pub enum JobEffect {
    /// Answers `JobEvent::Stops` with whether the job should stop now.
    Stops,
    /// Records progress; answered by `JobEvent::Reported`.
    Report(JobReport),
    /// Waits this long, or until the job stops; answered by `JobEvent::Paused`.
    Pause(Duration),
    /// Runs `child` while this node's turns of `turns` are held, taken in key order; answered by
    /// `JobEvent::Finished` with the child's event.
    InTurns {
        turns: BTreeSet<(Ulid, Ulid)>,
        child: Box<dyn SubOperation>,
    },
    /// Runs `child` until it ends or the job stops; `JobEvent::Finished` or `JobEvent::Stopped`.
    UntilStop(Box<dyn SubOperation>),
    /// Reads a stream to its end; answered by `JobEvent::Collected`.
    Collect(ByteStream),
    /// Feeds `work` until its cursor holds the next chunk; answered by `JobEvent::GzipFilled`.
    FillGzip(GzipWork),
    /// Opens an object as the job's submitter; answered by `JobEvent::Object`.
    ReadObject(Box<ObjectRead>),
}

/// A stream of bytes read from a source or an object.
pub type ByteStream = BackendStream<Result<Bytes, StreamError>>;

/// Whether a job was cancelled by its user or its node is shutting down.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JobStops {
    pub cancelled: bool,
    pub shutdown: bool,
}

impl JobStops {
    pub fn any(self) -> bool {
        self.cancelled || self.shutdown
    }
}

/// A change to a job's visible progress.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JobReport {
    pub current: Option<u64>,
    pub total: Option<u64>,
    pub advance: u64,
    /// A durable checkpoint advanced, so a later transient failure costs no attempt.
    pub checkpoint: bool,
}

/// An open gzip table stream with the cursor that decodes it.
pub struct GzipWork {
    pub stream: ByteStream,
    pub cursor: GzipCursor,
}

impl fmt::Debug for GzipWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GzipWork").finish_non_exhaustive()
    }
}

/// Equal only for the same stream: a decoder has no comparable state.
impl PartialEq for GzipWork {
    fn eq(&self, other: &Self) -> bool {
        self.stream == other.stream
    }
}

/// Why filling a gzip chunk stopped: the transport failed or the bytes are no valid gzip.
#[derive(Clone, Debug, PartialEq)]
pub enum GzipBroken {
    Stream(String),
    Gzip(String),
}

/// One object read, as the job's submitter.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectRead {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    /// The first and last byte to read.
    pub range: Option<(u64, u64)>,
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub restrictions: Option<Vec<PathRestriction>>,
}

/// An opened object: its bytes, the version they belong to and the object's whole size.
#[derive(Debug, PartialEq)]
pub struct ObjectBody {
    pub stream: ByteStream,
    pub version_id: Option<Ulid>,
    pub size: u64,
}

/// Why an object could not be opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectReadFailure {
    /// The key, the version or the object itself is gone.
    Gone(String),
    /// The range starts at or after the object's end.
    InvalidRange,
    Failed(String),
}

/// The answer to a [`JobEffect`].
#[derive(Debug, PartialEq)]
pub enum JobEvent {
    Stops(JobStops),
    Reported,
    Paused { stopped: bool },
    Finished(Box<Event>),
    Stopped,
    Collected(Result<Vec<u8>, String>),
    GzipFilled {
        work: GzipWork,
        result: Result<(), GzipBroken>,
    },
    Object(Result<ObjectBody, ObjectReadFailure>),
}
