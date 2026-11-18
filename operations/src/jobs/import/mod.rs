//! Runs the RO-Crate import job and re-exports the staged upload it reads the archive from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod acquire;
#[cfg(test)]
mod consortium;
mod finish;
mod job;
mod plan;
mod reports;
pub(crate) mod rewrite;
pub mod upload;
pub mod upload_store;
mod write;

pub use job::ImportJobOperation;

use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, SourceResolutionError, StagingSourceError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobEffect, JobEvent, JobStops};
use aruna_core::metadata::MetadataValidationViolation;
use aruna_core::repository::RepositoryPull;
use aruna_core::rocrate_archive::{ArchiveCompression, ArchiveInspection};
use aruna_core::structs::execution::job::{
    ImportReportDetail, ImportReportRow, ImportRoCrateResult, ImportRoCrateSource,
    ImportRoCrateSpec, JobError, JobId, JobResultPayload, ReasonCode, RoCrateCheckpointRefs,
    SYSTEM_ENTRY_PREFIX,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::secondary_id::SecondaryIdentifier;
use aruna_core::structs::storage::blob::{BackendLocation, BucketInfo, HiddenBlobKey};
use aruna_core::structs::storage::data::identity::ObjectLocation;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::storage::routing::NodeRouting;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use self::rewrite::CrateValidationError;
use super::executor::JobRunOutcome;
use super::metadata_class::MetadataFailure;
use super::repository::JobScope;
use aruna_core::repository::remote::NodeScope;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::forward::transport::MetadataWriteError;
use crate::placement::policy::GateContextError;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::GetObjectError;
use crate::s3::object::put::PutObjectError;
use crate::staging::read_source::ReadSourceError;

/// What every operation of one import run knows about it.
#[derive(Clone, Debug, PartialEq)]
pub struct ImportRun {
    pub spec: Arc<ImportRoCrateSpec>,
    pub job_id: JobId,
    pub claim_token: Ulid,
    pub owner_node: NodeId,
    /// This node on the network, when it has a net transport.
    pub local_node: Option<NodeId>,
    pub routing: NodeRouting,
    pub scope: NodeScope,
    pub job: JobScope,
    /// Makes the version ids this run mints distinct.
    pub seed: u64,
}

/// Longest wait for the source realm to confirm a grant.
const SOURCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ImportPhase {
    Acquire,
    Inspect,
    Validate,
    Write,
    Rewrite,
    Create,
    Cleanup,
    Done,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ImportInput {
    location: BackendLocation,
    size: u64,
    blake3: [u8; 32],
    upload_id: Option<Ulid>,
    eln: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ImportEntryPlan {
    archive_path: String,
    path: String,
    target_key: String,
    described_id: Option<String>,
    code: ReasonCode,
    /// Minted while planning so the rewritten identifiers can be validated
    /// before the first write, and so a rollback knows what to delete.
    version_id: Ulid,
    data_offset: u64,
    compressed_size: u64,
    uncompressed_size: u64,
    crc32: u32,
    compression: ArchiveCompression,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ImportPlan {
    metadata_json: String,
    entries: Vec<ImportEntryPlan>,
}

/// Progress an import job keeps under its id; public so the doctor can migrate stored rows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImportCheckpoint {
    refs: RoCrateCheckpointRefs,
    phase: ImportPhase,
    input: Option<ImportInput>,
    inspection: Option<ArchiveInspection>,
    metadata_json: Option<String>,
    next_entry: usize,
    entries_total: u64,
    imported: u64,
    unlisted: u64,
    failed: u64,
    rewritten_json: Option<String>,
    created: bool,
    rolled_back: u64,
    failure: Option<String>,
    cancelled: bool,
    /// Repository identifiers registered on the created document during cleanup.
    identifiers: Vec<SecondaryIdentifier>,
    /// Set when the import keeps or updates a pull link.
    pull: Option<super::repository::pull::PullProgress>,
}

impl Default for ImportCheckpoint {
    fn default() -> Self {
        Self {
            refs: RoCrateCheckpointRefs::default(),
            phase: ImportPhase::Acquire,
            input: None,
            inspection: None,
            metadata_json: None,
            next_entry: 0,
            entries_total: 0,
            imported: 0,
            unlisted: 0,
            failed: 0,
            rewritten_json: None,
            created: false,
            rolled_back: 0,
            failure: None,
            cancelled: false,
            identifiers: Vec::new(),
            pull: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum ImportFailure {
    Permanent(String),
    Retryable(String),
    Validation(Vec<MetadataValidationViolation>),
    Cancelled,
    Interrupted,
    /// Waits without spending an attempt, such as for fresh consent to an import from a realm.
    Deferred(String),
}

impl From<IoError> for ImportFailure {
    fn from(error: IoError) -> Self {
        Self::Retryable(error.to_string())
    }
}

fn transfer_failure(error: super::repository::TransferError) -> ImportFailure {
    use super::repository::TransferError;
    match error {
        TransferError::Permanent(message) => ImportFailure::Permanent(message),
        error @ TransferError::Refused(_) => ImportFailure::Permanent(error.to_string()),
        TransferError::Retryable(message) => ImportFailure::Retryable(message),
        TransferError::Cancelled => ImportFailure::Cancelled,
        TransferError::Interrupted => ImportFailure::Interrupted,
    }
}

/// Rejects a crate that would fail validation before anything is written.
/// Every rewritten identifier is known here, and the literal placeholder content
/// hash cannot change the graph the validator walks.
fn preflight_crate(
    spec: &ImportRoCrateSpec,
    node_id: NodeId,
    value: serde_json::Value,
    entries: &[ImportEntryPlan],
) -> Result<(), ImportFailure> {
    let mut targets = HashMap::new();
    for entry in entries {
        let Some(file_id) = entry.described_id.clone() else {
            continue;
        };
        entry_arn(spec, node_id, entry)?;
        let location = entry_location(spec, entry);
        targets.insert(
            file_id,
            RewriteTarget {
                id: location.to_url(),
                content_url: location.to_url(),
                local_path: entry.path.clone(),
            },
        );
    }
    rewrite_document(value, &targets)
        .map(|_| ())
        .map_err(validation_failure)
}

/// WRITE or READ on `path` for `auth_context`.
fn permission_call(auth_context: &AuthContext, path: String, permission: Permission) -> IoCall {
    IoCall::child(CheckPermissionsOperation::new(CheckPermissionsConfig {
        auth_context: auth_context.clone(),
        path,
        required_permission: permission,
    }))
}

/// The answer to [`permission_call`].
fn permission_result(result: IoResult) -> Result<(), ImportFailure> {
    let allowed = result.child::<bool, aruna_core::errors::AuthorizationError>()?;
    match allowed {
        Ok(true) => Ok(()),
        Ok(false) => Err(ImportFailure::Permanent("permission denied".to_string())),
        Err(error) => Err(ImportFailure::Retryable(error.to_string())),
    }
}

fn bucket_call(bucket: &str) -> IoCall {
    IoCall::child(GetBucketOperation::new(bucket.to_string()))
}

/// The answer to [`bucket_call`].
fn bucket_result(bucket: &str, result: IoResult) -> Result<BucketInfo, ImportFailure> {
    match result.child::<BucketInfo, GetBucketError>()? {
        Ok(info) => Ok(info),
        Err(GetBucketError::NotFound) => Err(ImportFailure::Permanent(format!(
            "bucket `{bucket}` does not exist"
        ))),
        Err(error) => Err(ImportFailure::Retryable(error.to_string())),
    }
}

pub(in crate::jobs) fn job_call(effect: JobEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Job(effect)))
}

/// The answer to `JobEffect::Stops`.
pub(in crate::jobs) fn stops_of(result: IoResult) -> Result<JobStops, IoError> {
    match result.event()? {
        Event::Job(JobEvent::Stops(stops)) => Ok(stops),
        other => Err(unexpected("the job's stop state", &other)),
    }
}

/// The answer to `JobEffect::Report`.
pub(in crate::jobs) fn reported(result: IoResult) -> Result<(), IoError> {
    match result.event()? {
        Event::Job(JobEvent::Reported) => Ok(()),
        other => Err(unexpected("the recorded progress", &other)),
    }
}

pub(in crate::jobs) fn unexpected(expected: &'static str, event: &Event) -> IoError {
    IoError::Unexpected {
        expected,
        got: event.label(),
    }
}

/// Deletes a hidden blob.
pub(in crate::jobs) fn hidden_delete(location: &BackendLocation) -> Result<IoCall, String> {
    let key = HiddenBlobKey::try_from(location).map_err(|error| error.to_string())?;
    let delete = Effect::Blob(BlobEffect::DeleteHidden { key });
    Ok(IoCall::Effect(Box::new(delete)))
}

/// The answer to [`hidden_delete`].
fn hidden_deleted(result: IoResult) -> Result<(), String> {
    match result.event().map_err(|error| error.to_string())? {
        Event::Blob(BlobEvent::HiddenDeleted) => Ok(()),
        Event::Blob(BlobEvent::Error(error)) => Err(error.to_string()),
        other => Err(format!(
            "unexpected hidden blob delete event: {}",
            other.label()
        )),
    }
}

/// A failure while the job stops is that stop.
fn stopped_failure(stops: JobStops) -> Option<ImportFailure> {
    match (stops.cancelled, stops.shutdown) {
        (true, _) => Some(ImportFailure::Cancelled),
        (false, true) => Some(ImportFailure::Interrupted),
        (false, false) => None,
    }
}

fn entry_location(spec: &ImportRoCrateSpec, entry: &ImportEntryPlan) -> ObjectLocation {
    ObjectLocation {
        bucket: spec.target.bucket.clone(),
        key: entry.target_key.clone(),
    }
}

fn entry_arn(
    spec: &ImportRoCrateSpec,
    node_id: NodeId,
    entry: &ImportEntryPlan,
) -> Result<VersionedObjectArn, ImportFailure> {
    VersionedObjectArn::new(
        spec.auth_context.realm_id,
        node_id,
        spec.target.bucket.clone(),
        entry.target_key.clone(),
        entry.version_id,
    )
    .map_err(|error| ImportFailure::Permanent(error.to_string()))
}

fn updates_link(spec: &ImportRoCrateSpec) -> bool {
    matches!(
        &spec.source,
        ImportRoCrateSource::Repository {
            pull: Some(RepositoryPull::Update { .. }),
            ..
        }
    )
}

/// A failed import must leave nothing behind. A cancelled one keeps what it
/// already committed, which its report rows state entry by entry.
fn rollback_required(checkpoint: &ImportCheckpoint) -> bool {
    !checkpoint.created && checkpoint.failure.is_some()
}

/// Failure text before the keys a rollback kept because a release pins them.
const PINNED_KEPT: &str = "a release pins these written objects, so they were kept";

/// Report message of an entry whose written object a rollback removed.
const REMOVED: &str = "written object was removed after the import failed";

fn ensure_valid_path(spec: &ImportRoCrateSpec) -> Result<(), ImportFailure> {
    let normalized = MetadataRegistryRecord::normalize_document_path(&spec.metadata.path);
    if normalized.is_empty() {
        return Err(ImportFailure::Permanent(
            "metadata path must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn system_report_key(entry_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(entry_key.len().saturating_add(1));
    key.push(SYSTEM_ENTRY_PREFIX);
    key.extend_from_slice(entry_key.as_bytes());
    key
}

fn entry_report(entry: &ImportEntryPlan) -> ImportReportRow {
    ImportReportRow {
        entry_key: entry.path.clone(),
        code: entry.code,
        message: None,
        detail: ImportReportDetail {
            archive_path: entry.archive_path.clone(),
            target_key: Some(entry.target_key.clone()),
            version_id: None,
            blake3: None,
            size: None,
            arn: None,
            w3id: None,
            validation: None,
        },
    }
}

fn target_key(prefix: &str, path: &str, limit: u64) -> Result<String, ImportFailure> {
    let prefix = prefix.trim_matches('/');
    if prefix.contains('\\')
        || (!prefix.is_empty()
            && prefix
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."))
        || prefix.chars().any(char::is_control)
    {
        return Err(ImportFailure::Permanent(
            "target prefix is unsafe".to_string(),
        ));
    }
    let key = if prefix.is_empty() {
        path.to_string()
    } else {
        format!("{prefix}/{path}")
    };
    if key.len() as u64 > limit {
        return Err(ImportFailure::Permanent(format!(
            "target key exceeds limit {limit}"
        )));
    }
    Ok(key)
}

fn precheck_size(size: u64, limit: u64) -> Result<(), ImportFailure> {
    if size > limit {
        return Err(ImportFailure::Permanent(format!(
            "import source exceeds limit {limit}"
        )));
    }
    Ok(())
}

fn source_permission_path(
    realm_id: aruna_core::structs::identity::realm::RealmId,
    group_id: Ulid,
    node_id: NodeId,
    connector_id: Ulid,
    path: &str,
) -> String {
    let root = format!("/{realm_id}/g/{group_id}/data/{node_id}/_sources/{connector_id}");
    if path.is_empty() {
        root
    } else {
        format!("{root}/{path}")
    }
}

fn source_uses_eln(path: &str, content_type: Option<&str>) -> bool {
    path.rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.to_ascii_lowercase().ends_with(".eln"))
        || content_type
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/vnd.eln+zip"))
}

fn validation_failure(error: CrateValidationError) -> ImportFailure {
    match error {
        CrateValidationError::Violations(violations) => ImportFailure::Validation(violations),
        CrateValidationError::Invalid(message) => ImportFailure::Permanent(message),
    }
}

fn validation_message(violations: &[MetadataValidationViolation]) -> String {
    violations
        .iter()
        .map(|violation| {
            let entity = violation
                .entity_id
                .as_deref()
                .map(|entity| format!(" for `{entity}`"))
                .unwrap_or_default();
            format!(
                "{} at {}{entity}: {}",
                violation.code, violation.pointer, violation.message
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Why a backend blob write failed, as far as the import may decide. The typed
/// error carries the cause; local error text is never interpreted, so wording
/// cannot flip permanence, and an unknown cause stays retryable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlobWriteFailure {
    /// An integrity fault the source bytes will not heal by retrying.
    Checksum,
    /// A declared body length that does not match what arrived.
    ContentLength,
    /// A client/body fault, distinct from a backend failure.
    ClientBody,
    /// A backend fault the import cannot pin to the written bytes.
    Unknown,
}

impl BlobWriteFailure {
    fn retryable(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

/// The import's retry decision for a frozen blob write fault, taken from the
/// typed cause. Integrity and client-body faults are permanent; transport,
/// setup, cleanup, and storage faults stay retryable.
fn backend_blob_failure(error: &BlobError) -> BlobWriteFailure {
    match error {
        BlobError::IntegrityCheckFailed(_) => BlobWriteFailure::Checksum,
        BlobError::SizeLimitExceeded { .. } => BlobWriteFailure::ContentLength,
        BlobError::StreamFailed(_) => BlobWriteFailure::ClientBody,
        BlobError::WriteError(_)
        | BlobError::WriteCleanup { .. }
        | BlobError::OperatorCreationFailed(_)
        | BlobError::OutboardCreationFailed(_)
        | BlobError::MakeBucketError(_)
        | BlobError::ConnectionFailed(_) => BlobWriteFailure::Unknown,
        _ => BlobWriteFailure::Unknown,
    }
}

fn classify_put(error: PutObjectError) -> ImportFailure {
    match error {
        PutObjectError::StorageError(_) => ImportFailure::Retryable(error.to_string()),
        PutObjectError::BlobWriteFailed(ref cause) => {
            if backend_blob_failure(cause).retryable() {
                ImportFailure::Retryable(error.to_string())
            } else {
                ImportFailure::Permanent(error.to_string())
            }
        }
        _ => ImportFailure::Permanent(error.to_string()),
    }
}

fn classify_get(error: GetObjectError) -> ImportFailure {
    let permanent = match &error {
        GetObjectError::ConversionError(_)
        | GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker
        | GetObjectError::InvalidRange
        // Retrying cannot recover an observation the source no longer serves,
        // and only an explicit rebind clears an exhausted binding.
        | GetObjectError::HistoricalReferenceUnavailable
        | GetObjectError::ReferenceChecksumMismatch
        | GetObjectError::ReferenceAdvanceExhausted => true,
        // A drifting source can stabilize; the job attempt cap bounds the retries.
        GetObjectError::ReferenceSourceChanged => false,
        GetObjectError::ResolveReferenceError(error) => permanent_resolve(error),
        GetObjectError::StagingSourceError(error) => permanent_staging(error),
        _ => false,
    };
    if permanent {
        ImportFailure::Permanent(error.to_string())
    } else {
        ImportFailure::Retryable(error.to_string())
    }
}

fn classify_read(error: ReadSourceError) -> ImportFailure {
    let permanent = match &error {
        ReadSourceError::Resolve(error) => permanent_resolve(error),
        ReadSourceError::Staging(error) => permanent_staging(error),
        _ => false,
    };
    if permanent {
        ImportFailure::Permanent(error.to_string())
    } else {
        ImportFailure::Retryable(error.to_string())
    }
}

fn classify_blob(error: BlobError) -> ImportFailure {
    match &error {
        BlobError::SizeLimitExceeded { limit } => {
            ImportFailure::Permanent(format!("import source exceeds limit {limit}"))
        }
        BlobError::IntegrityCheckFailed(_) | BlobError::StreamFailed(_) => {
            ImportFailure::Permanent(error.to_string())
        }
        _ => ImportFailure::Retryable(error.to_string()),
    }
}

fn permanent_resolve(error: &SourceResolutionError) -> bool {
    matches!(
        error,
        SourceResolutionError::ConversionError(_)
            | SourceResolutionError::NotFound
            | SourceResolutionError::UnsupportedConnectorKind(_)
            | SourceResolutionError::InvalidSourcePath
    )
}

fn permanent_staging(error: &StagingSourceError) -> bool {
    matches!(
        error,
        StagingSourceError::NotFound
            | StagingSourceError::AccessDenied
            | StagingSourceError::UnsupportedKind(_)
    )
}

/// A rejected document never becomes acceptable by retrying, so it is reported
/// as violations and rolled back instead of burning the remaining attempts.
fn classify_metadata(error: MetadataWriteError) -> ImportFailure {
    match crate::jobs::metadata_class::classify_metadata(error) {
        MetadataFailure::Validation(violations) => ImportFailure::Validation(violations),
        MetadataFailure::Permanent(message) => ImportFailure::Permanent(message),
        MetadataFailure::Retryable(message) => ImportFailure::Retryable(message),
    }
}

fn phase_name(phase: ImportPhase) -> &'static str {
    match phase {
        ImportPhase::Acquire => "acquire",
        ImportPhase::Inspect => "inspect",
        ImportPhase::Validate => "validate",
        ImportPhase::Write => "write",
        ImportPhase::Rewrite => "rewrite",
        ImportPhase::Create => "create",
        ImportPhase::Cleanup => "cleanup",
        ImportPhase::Done => "done",
    }
}

fn import_outcome(checkpoint: &ImportCheckpoint, spec: &ImportRoCrateSpec) -> JobRunOutcome {
    if checkpoint.cancelled {
        return JobRunOutcome::Cancelled;
    }
    if let Some(error) = &checkpoint.failure {
        return JobRunOutcome::Failed(JobError::permanent(format!(
            "{error}; {}",
            write_state(checkpoint)
        )));
    }
    JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(ImportRoCrateResult {
        document_id: checkpoint.created.then_some(spec.document_id),
        entries_total: checkpoint.entries_total,
        imported: checkpoint.imported,
        unlisted: checkpoint.unlisted,
        failed: checkpoint.failed,
        report_digest: [0; 32],
    }))
}

/// States what a failed import left behind, so the report never leaves the
/// caller guessing whether data landed.
fn write_state(checkpoint: &ImportCheckpoint) -> String {
    if checkpoint.created {
        return "the metadata document was created".to_string();
    }
    let kept = (checkpoint.failure.as_deref()).is_some_and(|failure| failure.contains(PINNED_KEPT));
    match checkpoint.rolled_back {
        0 if kept => "no other objects were written".to_string(),
        0 => "no objects were written".to_string(),
        1 => "1 written object was removed".to_string(),
        removed => format!("{removed} written objects were removed"),
    }
}

fn failure_message(error: ImportFailure) -> String {
    match error {
        ImportFailure::Permanent(message)
        | ImportFailure::Retryable(message)
        | ImportFailure::Deferred(message) => message,
        ImportFailure::Validation(violations) => validation_message(&violations),
        ImportFailure::Cancelled => "import cancelled".to_string(),
        ImportFailure::Interrupted => "import interrupted".to_string(),
    }
}

fn retryable_error(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

/// A node that cannot build a destination gate is retryable: the transition it
/// is in ends on its own, and the import resumes from its checkpoint.
fn classify_gate(error: GateContextError) -> ImportFailure {
    ImportFailure::Retryable(error.to_string())
}

#[cfg(test)]
pub(crate) mod tests {

    use super::*;
    use crate::jobs::repository::test_support::{
        artifact, job_id, job_scope, local_scope, owner_node, realm,
    };
    use aruna_core::UserId;
    use aruna_core::structs::execution::job::{
        ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateTarget, RoCrateLimits,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::storage::blob::HIDDEN_BLOB_PREFIX;
    use std::time::SystemTime;

    pub(super) fn spec() -> ImportRoCrateSpec {
        ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: UserId::local(Ulid::from_parts(8, 8), realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload {
                upload_id: Ulid::from_parts(4, 4),
            },
            target: ImportRoCrateTarget {
                bucket: "data".to_string(),
                prefix: String::new(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_parts(2, 2),
                path: "doc".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::from_parts(3, 3),
        }
    }

    pub(super) fn run() -> ImportRun {
        ImportRun {
            spec: Arc::new(spec()),
            job_id: job_id(1),
            claim_token: Ulid::from_parts(9, 9),
            owner_node: owner_node(),
            local_node: Some(owner_node()),
            routing: Default::default(),
            scope: local_scope(),
            job: job_scope(),
            seed: 5,
        }
    }

    pub(super) fn bucket() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_parts(2, 2),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: spec().auth_context.user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        }
    }

    /// A location inside the hidden blob namespace of the job.
    pub(super) fn hidden_location() -> BackendLocation {
        let mut location = artifact().location;
        location.backend_path = format!("{HIDDEN_BLOB_PREFIX}/{}/input", Ulid::from_parts(1, 1));
        location
    }

    pub(super) fn entry(path: &str) -> ImportEntryPlan {
        ImportEntryPlan {
            archive_path: path.to_string(),
            path: path.to_string(),
            target_key: path.to_string(),
            described_id: Some(path.to_string()),
            code: ReasonCode::Imported,
            version_id: Ulid::from_parts(6, path.len() as u128),
            data_offset: 0,
            compressed_size: 4,
            uncompressed_size: 4,
            crc32: 1,
            compression: ArchiveCompression::Stored,
        }
    }

    pub(super) fn plan(paths: &[&str]) -> ImportPlan {
        ImportPlan {
            metadata_json: "{}".to_string(),
            entries: paths.iter().map(|path| entry(path)).collect(),
        }
    }

    pub(super) fn stops(cancelled: bool, shutdown: bool) -> IoResult {
        let stops = JobStops {
            cancelled,
            shutdown,
        };
        IoResult::Event(Box::new(Event::Job(JobEvent::Stops(stops))))
    }

    /// Encoded checkpoint of a job that holds one claimed upload as hidden input.
    pub(crate) fn checkpoint_bytes(
        location: BackendLocation,
        size: u64,
        blake3: [u8; 32],
        upload_id: Ulid,
    ) -> Vec<u8> {
        postcard::to_allocvec(&ImportCheckpoint {
            refs: RoCrateCheckpointRefs {
                hidden_locations: vec![location.clone()],
            },
            input: Some(ImportInput {
                location,
                size,
                blake3,
                upload_id: Some(upload_id),
                eln: false,
            }),
            phase: ImportPhase::Inspect,
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn target_checks_limits() {
        assert_eq!(
            target_key("/crate/", "data/a.txt", 64).ok().as_deref(),
            Some("crate/data/a.txt")
        );
        assert!(target_key("../crate", "data/a.txt", 64).is_err());
        assert!(target_key("crate//data", "a.txt", 64).is_err());
        assert!(target_key("crate\\data", "a.txt", 64).is_err());
        assert!(target_key("crate", "data/a.txt", 4).is_err());
    }

    #[test]
    fn source_checks_size() {
        assert!(precheck_size(8, 8).is_ok());
        assert!(matches!(
            precheck_size(9, 8),
            Err(ImportFailure::Permanent(message))
                if message == "import source exceeds limit 8"
        ));
    }

    #[test]
    fn report_keys_disjoint() {
        for entry_key in [
            "signature/ro-crate-metadata.json.minisig",
            "warning/00000000",
            "failure/write",
        ] {
            let system_key = system_report_key(entry_key);
            assert_ne!(system_key, entry_key.as_bytes());
            assert_eq!(system_key.first(), Some(&SYSTEM_ENTRY_PREFIX));
            assert_eq!(&system_key[1..], entry_key.as_bytes());
        }
    }

    #[test]
    fn detects_eln_source() {
        assert!(source_uses_eln("exports/Experiment.ELN", None));
        assert!(source_uses_eln(
            "exports/experiment.zip",
            Some("Application/Vnd.Eln+Zip; version=1")
        ));
        assert!(!source_uses_eln(
            "exports/experiment.zip",
            Some("application/zip")
        ));
    }

    // A source that keeps changing can settle, but a dropped historical
    // observation and an exhausted binding never heal by retrying.
    // The typed blob cause decides permanence; local wording never does.
    #[test]
    fn blob_write_classification() {
        let cases = [
            (
                BlobError::IntegrityCheckFailed("sha256 mismatch".to_string()),
                BlobWriteFailure::Checksum,
                false,
            ),
            (
                BlobError::SizeLimitExceeded { limit: 5 },
                BlobWriteFailure::ContentLength,
                false,
            ),
            (
                BlobError::StreamFailed("checksum mismatch".to_string()),
                BlobWriteFailure::ClientBody,
                false,
            ),
            (
                BlobError::WriteError("checksum mismatch for sha256".to_string()),
                BlobWriteFailure::Unknown,
                true,
            ),
            (
                BlobError::ConnectionFailed("connection reset by peer".to_string()),
                BlobWriteFailure::Unknown,
                true,
            ),
        ];
        for (error, expected, retryable) in cases {
            assert_eq!(backend_blob_failure(&error), expected, "{error:?}");
            assert_eq!(expected.retryable(), retryable, "{error:?}");
        }

        // A generic write fault stays retryable whatever its text says, so a
        // punctuation or wording change cannot flip its permanence.
        for wording in [
            "No space left on device",
            "no space left on device!",
            "checksum mismatch",
        ] {
            assert!(
                matches!(
                    classify_put(PutObjectError::BlobWriteFailed(BlobError::WriteError(
                        wording.to_string()
                    ))),
                    ImportFailure::Retryable(_)
                ),
                "{wording}"
            );
            assert!(matches!(
                classify_put(PutObjectError::BlobWriteFailed(BlobError::StreamFailed(
                    wording.to_string()
                ))),
                ImportFailure::Permanent(_)
            ));
            assert!(matches!(
                classify_put(PutObjectError::BlobWriteFailed(
                    BlobError::SizeLimitExceeded { limit: 5 }
                )),
                ImportFailure::Permanent(_)
            ));
            assert!(matches!(
                classify_put(PutObjectError::BlobWriteFailed(BlobError::ChannelClosed)),
                ImportFailure::Retryable(_)
            ));
            assert!(matches!(
                classify_put(PutObjectError::BlobWriteFailed(
                    BlobError::IntegrityCheckFailed(wording.to_string())
                )),
                ImportFailure::Permanent(_)
            ));
        }
    }

    #[test]
    fn classifies_reference_errors() {
        assert!(matches!(
            classify_get(GetObjectError::ReferenceSourceChanged),
            ImportFailure::Retryable(_)
        ));
        assert!(matches!(
            classify_get(GetObjectError::HistoricalReferenceUnavailable),
            ImportFailure::Permanent(_)
        ));
        assert!(matches!(
            classify_get(GetObjectError::ReferenceAdvanceExhausted),
            ImportFailure::Permanent(_)
        ));
    }

    #[test]
    fn classifies_source_errors() {
        assert!(matches!(
            classify_get(GetObjectError::NoSuchKey),
            ImportFailure::Permanent(_)
        ));
        assert!(matches!(
            classify_get(GetObjectError::StorageError(
                aruna_core::errors::StorageError::Timeout
            )),
            ImportFailure::Retryable(_)
        ));
        assert!(matches!(
            classify_read(ReadSourceError::Resolve(SourceResolutionError::NotFound)),
            ImportFailure::Permanent(_)
        ));
        assert!(matches!(
            classify_blob(BlobError::StreamFailed("source read failed".to_string())),
            ImportFailure::Permanent(_)
        ));
        assert!(matches!(
            classify_blob(BlobError::WriteError("backend failed".to_string())),
            ImportFailure::Retryable(_)
        ));
    }

    #[test]
    fn profile_rejections_permanent() {
        let finding = aruna_core::metadata::ProfileValidationFinding {
            code: "unsupported_constraint".to_string(),
            severity: aruna_core::metadata::ProfileValidationSeverity::Violation,
            focus_node: None,
            path: None,
            rule: "http://www.w3.org/ns/shacl#SPARQLConstraintComponent".to_string(),
            message: "unsupported".to_string(),
            profile_revision: None,
            completeness: aruna_core::metadata::ProfileValidationCompleteness::Incomplete,
        };
        let failure = classify_metadata(MetadataWriteError::Create(
            crate::metadata::create_document::CreateDocumentError::MetadataError(
                aruna_core::metadata::MetadataError::ProfileValidation(vec![finding]),
            ),
        ));
        assert!(matches!(failure, ImportFailure::Permanent(_)));
    }
}
