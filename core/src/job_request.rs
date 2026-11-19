//! Normalizes a submission request, derives the identities that follow from it, mints job ids
//! and names why a submission fails. Nothing local enters a digest, so every node agrees.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::calls::IoError;
use crate::compute::SessionMount;
use crate::compute::runtimes::{
    MOUNT_PATH_TAG, MOUNT_PREFIX_TAG, SESSION_EXPIRY_TAG, SESSION_IDLE_TAG, SESSION_RUNTIME_TAG,
    SESSION_TAG, SESSION_TAG_NOTEBOOK,
};
use crate::errors::{ConversionError, StorageError};
use crate::id::NodeId;
use crate::id_mint::IdMint;
use crate::structs::execution::job::{
    CapturedInput, EffectiveResources, ExecutionSpec, JobFamilyId, JobId, SubmissionId,
    WorkspaceMode,
};
use crate::structs::placement::policy::{MAX_SELECTOR_LABELS, PlacementPolicyRef};
use crate::structs::placement::record::{LabelMatch, shard_for_subject};
use crate::structured_id::{
    BucketId, ClockHealthError, JobId as RoutableJobId, PlacementHandle, StructuredId,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// Domain tag of the normalized request digest.
const REQUEST_DIGEST_DOMAIN: &[u8] = b"aruna-job-request-v1";

/// Reserved tag namespace of the scheduling directives an execution spec stores.
/// The spec is the complete request, so these travel inside it rather than
/// beside it, in the tag namespace the engine already owns.
const WORKSPACE_MODE_TAG: &str = "aruna-engine.org/workspace-mode";
const WORKSPACE_BUCKET_TAG: &str = "aruna-engine.org/workspace-bucket";
/// `aruna-engine.org/label/<key> = <value>` is one required target label.
const LABEL_TAG_PREFIX: &str = "aruna-engine.org/label/";

/// Cores assumed when a request declares none.
const DEFAULT_CPU_CORES: u32 = 1;
/// Memory assumed when a request declares none: 1 GiB.
const DEFAULT_RAM_BYTES: u64 = 1024 * 1024 * 1024;
/// Walltime assumed when a request declares none, matching the executor default.
const DEFAULT_WALLTIME_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Debug, PartialEq, Error)]
pub enum RequestError {
    #[error("tag {0} is reserved for engine scheduling directives")]
    ReservedTag(String),
    #[error("a request declares at most {MAX_SELECTOR_LABELS} required labels")]
    LabelCount,
    #[error("required label keys and values must be non-empty")]
    InvalidLabel,
    #[error("existing workspace mode requires a bucket")]
    MissingBucket,
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

/// How a submission is keyed. A keyed request merges with its own retries; an
/// unkeyed one is a fresh family and never merges.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionScope {
    Keyed(String),
    Unkeyed(Ulid),
}

/// The complete external request, normalized once at ingress and carried
/// unchanged across the one forwarding hop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionRequest {
    pub created_by: UserId,
    pub spec: ExecutionSpec,
    pub scope: SubmissionScope,
    pub retention_ms: u64,
    /// The first node that resolved node-local object names for this request.
    pub ingress_node_id: NodeId,
    pub captured_inputs: Vec<CapturedInput>,
    pub output_policies: Vec<PlacementPolicyRef>,
    /// Inputs too large to travel inline: the lists are empty and the sender keeps the pages.
    /// The identity is always computed from the full lists.
    pub input_pages: Option<crate::structs::execution::job::PageManifest>,
}

/// The replicated identity of one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestIdentity {
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
}

impl RequestIdentity {
    pub fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }
}

impl SubmissionRequest {
    /// Derives the opaque submission identity and the normalized request digest.
    /// Both sides of a forwarding hop run this and compare.
    pub fn identity(&self) -> Result<RequestIdentity, ConversionError> {
        let submission_id = submission_of(self.created_by, &self.scope);
        Ok(RequestIdentity {
            submission_id,
            request_digest: request_digest(
                self.created_by,
                &self.spec,
                self.ingress_node_id,
                &self.captured_inputs,
                &self.output_policies,
            )?,
        })
    }
}

/// The submission id `created_by` asks under `scope`, known before any input resolves.
pub fn submission_of(created_by: UserId, scope: &SubmissionScope) -> SubmissionId {
    match scope {
        SubmissionScope::Keyed(key) => SubmissionId::keyed(created_by, key.as_bytes()),
        SubmissionScope::Unkeyed(nonce) => SubmissionId::unkeyed(*nonce),
    }
}

fn request_digest(
    created_by: UserId,
    spec: &ExecutionSpec,
    ingress_node_id: NodeId,
    captured_inputs: &[CapturedInput],
    output_policies: &[PlacementPolicyRef],
) -> Result<[u8; 32], ConversionError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(REQUEST_DIGEST_DOMAIN);
    hasher.update(&created_by.to_storage_key());
    hasher.update(&postcard::to_allocvec(spec)?);
    hasher.update(ingress_node_id.as_bytes());
    // Only the exact pins identify the request; metadata, grants and public reads may change.
    let pins: Vec<_> = captured_inputs
        .iter()
        .map(|captured| {
            (
                &captured.destination_key,
                captured.source_node_id.as_bytes(),
                captured.version_id,
                &captured.content,
                captured.bytes,
                &captured.policies,
                &captured.link,
            )
        })
        .collect();
    hasher.update(&postcard::to_allocvec(&pins)?);
    hasher.update(&postcard::to_allocvec(output_policies)?);
    Ok(*hasher.finalize().as_bytes())
}

/// Stores the workspace choice in the spec. A caller that already set a
/// reserved tag is refused rather than silently overridden.
pub fn store_workspace(
    spec: &mut ExecutionSpec,
    mode: WorkspaceMode,
    bucket: Option<String>,
) -> Result<(), RequestError> {
    if spec.tags.contains_key(WORKSPACE_MODE_TAG) || spec.tags.contains_key(WORKSPACE_BUCKET_TAG) {
        return Err(RequestError::ReservedTag(WORKSPACE_MODE_TAG.to_string()));
    }
    spec.tags
        .insert(WORKSPACE_MODE_TAG.to_string(), mode.name().to_string());
    match (mode, bucket) {
        (WorkspaceMode::Existing, None) => return Err(RequestError::MissingBucket),
        (_, Some(bucket)) => {
            spec.tags.insert(WORKSPACE_BUCKET_TAG.to_string(), bucket);
        }
        (_, None) => {}
    }
    Ok(())
}

/// The workspace the stored spec asks for. An unknown or absent mode owns no
/// bucket, which is what a run without a named bucket always meant.
pub fn workspace_of(spec: &ExecutionSpec) -> (WorkspaceMode, Option<String>) {
    let mode = match spec.tags.get(WORKSPACE_MODE_TAG).map(String::as_str) {
        Some("existing") => WorkspaceMode::Existing,
        _ => WorkspaceMode::None,
    };
    (mode, spec.tags.get(WORKSPACE_BUCKET_TAG).cloned())
}

/// What a session job carries next to its ordinary execution spec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionSpec {
    pub runtime: String,
    /// The idle wait the submitter asked for. The executing node clamps it to
    /// the realm value.
    pub idle_after_ms: Option<u64>,
    /// When the submitter's bearer expires. The credential never outlives it.
    pub expires_at_ms: Option<u64>,
    /// Where the workspace bucket is mounted, when the submission recorded it.
    pub mount: Option<SessionMount>,
}

/// The interactive session the stored spec asks for. `None` is an ordinary run.
pub fn session_of(spec: &ExecutionSpec) -> Option<SessionSpec> {
    if spec.tags.get(SESSION_TAG).map(String::as_str) != Some(SESSION_TAG_NOTEBOOK) {
        return None;
    }
    let mount = spec.tags.get(MOUNT_PATH_TAG).map(|path| SessionMount {
        prefix: spec.tags.get(MOUNT_PREFIX_TAG).cloned().unwrap_or_default(),
        path: path.clone(),
    });
    Some(SessionSpec {
        runtime: spec
            .tags
            .get(SESSION_RUNTIME_TAG)
            .cloned()
            .unwrap_or_default(),
        idle_after_ms: spec
            .tags
            .get(SESSION_IDLE_TAG)
            .and_then(|value| value.parse().ok()),
        expires_at_ms: spec
            .tags
            .get(SESSION_EXPIRY_TAG)
            .and_then(|value| value.parse().ok()),
        mount,
    })
}

/// Required target labels the spec stores, in canonical order.
pub fn required_labels(spec: &ExecutionSpec) -> Result<Vec<LabelMatch>, RequestError> {
    let mut labels: Vec<LabelMatch> = Vec::new();
    for (key, value) in &spec.tags {
        let Some(key) = key.strip_prefix(LABEL_TAG_PREFIX) else {
            continue;
        };
        if key.trim().is_empty() || value.trim().is_empty() {
            return Err(RequestError::InvalidLabel);
        }
        labels.push(LabelMatch {
            key: key.trim().to_string(),
            value: value.trim().to_string(),
        });
    }
    if labels.len() > MAX_SELECTOR_LABELS {
        return Err(RequestError::LabelCount);
    }
    Ok(labels)
}

/// Normalizes the requested ceilings once. Every field is filled, so comparing
/// the request against a static executor envelope is total.
pub fn effective_resources(spec: &ExecutionSpec) -> EffectiveResources {
    EffectiveResources {
        cpu_cores: spec.resources.cpu_cores.unwrap_or(DEFAULT_CPU_CORES).max(1),
        ram_bytes: spec.resources.ram_bytes.unwrap_or(DEFAULT_RAM_BYTES),
        disk_bytes: spec.resources.disk_bytes.unwrap_or_default(),
        max_walltime_ms: spec
            .resources
            .max_walltime_ms
            .unwrap_or(DEFAULT_WALLTIME_MS),
        preemptible: spec.resources.preemptible,
    }
}

/// The largest time and the largest nonce of a structured id, both 48 bits wide.
const FIELD_MAX: u64 = (1 << 48) - 1;

/// A job id for `(handle, bucket)` at `now_ms`, with its time and nonce from the run's `ids`. The
/// ids of one run strictly increase; ids of separate runs differ by their random nonce.
pub fn mint_job_id(
    handle: PlacementHandle,
    bucket: BucketId,
    ids: &mut IdMint,
    now_ms: u64,
) -> Result<JobId, ClockHealthError> {
    let overflow = |timestamp_ms| ClockHealthError::TimestampOverflow { timestamp_ms };
    if now_ms > FIELD_MAX {
        return Err(overflow(now_ms));
    }
    let minted = ids.mint(now_ms);
    let nonce = minted.random() as u64 & FIELD_MAX;
    RoutableJobId::from_parts(minted.timestamp_ms(), handle, bucket, nonce)
        .map(JobId::from_routable)
        .map_err(|_| overflow(minted.timestamp_ms()))
}

/// The owner's job-control handle and the bucket of `subject` in its strategy.
pub fn job_placement(
    config: &crate::structs::identity::realm::RealmConfigDocument,
    owner_node_id: NodeId,
    subject: &[u8],
) -> Result<(PlacementHandle, BucketId), SubmitJobError> {
    let handle = config.job_control_handle(&owner_node_id).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable(
            "serving node has no job-control binding yet".to_string(),
        )
    })?;
    let tuple = config
        .binding_directory()
        .resolve(handle)
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let strategy = config.strategy(&tuple.strategy_id).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("job-control strategy unavailable".to_string())
    })?;
    let shard = shard_for_subject(subject, strategy.shard_count);
    let bucket = u16::try_from(shard)
        .ok()
        .and_then(|shard| BucketId::new(shard).ok())
        .ok_or_else(|| {
            SubmitJobError::PlacementUnavailable("job bucket is out of range".to_string())
        })?;
    Ok((handle, bucket))
}

/// Synchronous form used by producer transactions that already fenced and read
/// the realm config. The resulting job id and dedup shard can therefore be
/// written atomically with the producer's own records.
pub fn mint_configured_job(
    config: &crate::structs::identity::realm::RealmConfigDocument,
    owner_node_id: NodeId,
    subject: &[u8],
    ids: &mut IdMint,
    now_ms: u64,
) -> Result<JobId, SubmitJobError> {
    let (handle, bucket) = job_placement(config, owner_node_id, subject)?;
    Ok(mint_job_id(handle, bucket, ids, now_ms)?)
}

#[derive(Debug, Error, PartialEq)]
pub enum SubmitJobError {
    #[error(transparent)]
    GroupWrite(#[from] crate::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    ClockHealth(#[from] ClockHealthError),
    #[error("job placement unavailable: {0}")]
    PlacementUnavailable(String),
    /// The routed authority no longer has the document the job is scoped to.
    #[error("persistent id target document is absent")]
    DocumentMissing,
    /// The routed authority refused the caller's permission on the document.
    #[error("persistent id submission is not authorized")]
    AuthorityDenied,
    #[error("{0}: input read denied")]
    InputDenied(String),
    #[error("idempotency key already bound to job {existing_job_id} with a different plan")]
    JobPlanConflict { existing_job_id: JobId },
    #[error("invalid workspace: {0}")]
    InvalidWorkspace(String),
    #[error("a job may declare at most {limit} outputs")]
    TooManyOutputs { limit: usize },
    /// Standing compute quota refused the new logical admission.
    #[error("compute quota denied: {0}")]
    QuotaDenied(#[from] crate::compute::quota::QuotaDenied),
    #[error(transparent)]
    Composition(#[from] crate::structs::execution::job::CompositionError),
    #[error("active RO-Crate job limit reached ({limit})")]
    ActiveJobLimit { limit: u32 },
    #[error(transparent)]
    Upload(#[from] UploadClaimError),
    #[error("the import rule does not exist")]
    ImportMissing,
    #[error("the last run of this import has not finished")]
    ImportRunning,
    #[error("the harvest source does not exist")]
    SourceMissing,
    #[error("the last run of this harvest source has not finished")]
    SourceRunning,
    #[error("unexpected event while submitting job: {0}")]
    UnexpectedEvent(String),
    #[error("a key range of distributed bucket {0} is moving away from this node; retry later")]
    RangeMoving(ulid::Ulid),
}

impl From<IoError> for SubmitJobError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            other => Self::UnexpectedEvent(other.to_string()),
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum UploadClaimError {
    #[error("upload not found")]
    NotFound,
    #[error("upload is owned by another user")]
    WrongOwner,
    #[error("upload expired")]
    Expired,
    #[error("upload is already claimed")]
    AlreadyClaimed,
    #[error("upload record is invalid: {0}")]
    Invalid(String),
    #[error("upload storage failed: {0}")]
    Storage(String),
}

impl From<IoError> for UploadClaimError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitJobResult {
    pub job_id: JobId,
    /// `false` when a job with the same `dedup_key` already existed.
    pub created: bool,
}

#[cfg(test)]
mod tests {
    use crate::structs::execution::job::ComputeResources;

    use super::*;

    fn session_spec() -> ExecutionSpec {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert(SESSION_TAG.to_string(), SESSION_TAG_NOTEBOOK.to_string());
        tags.insert(
            SESSION_RUNTIME_TAG.to_string(),
            "python-notebook".to_string(),
        );
        ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags,
            image: "session:latest".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: Some("/work".to_string()),
            env: Default::default(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        }
    }

    #[test]
    fn session_reads_mount() {
        // The recorded mount tags name the bucket slice and its folder; a spec
        // stored without them mounts nothing rather than guessing a layout.
        let mut spec = session_spec();
        assert_eq!(session_of(&spec).unwrap().mount, None);

        spec.tags
            .insert(MOUNT_PATH_TAG.to_string(), "/work/raw".to_string());
        assert_eq!(
            session_of(&spec).unwrap().mount,
            Some(SessionMount {
                prefix: String::new(),
                path: "/work/raw".to_string(),
            })
        );

        spec.tags
            .insert(MOUNT_PREFIX_TAG.to_string(), "raw/2024/".to_string());
        assert_eq!(
            session_of(&spec).unwrap().mount.unwrap().prefix,
            "raw/2024/"
        );
    }
}
