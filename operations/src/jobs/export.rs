use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Component, Path};

use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, ROCRATE_JOB_STATE_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::metadata::MetadataValidationViolation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    ArtifactRef, ArunaArn, ArunaArnType, BackendLocation, BlobVersion, BucketInfo,
    ExportOmissionCounts, ExportReportDetail, ExportReportRow, ExportReportSource,
    ExportRoCrateResult, ExportRoCrateSpec, HashPathIndexKey, JobError, JobId, JobResultPayload,
    Permission, RealmId, ReasonCode, RoCrateCheckpointRefs, VersionKey, VersionedObjectArn,
    W3idDataIdentifier, blob_object_permission_path, ensure_confined_relative_path,
};
use aruna_core::types::{Key, NodeId, Value};
use aruna_core::util::unix_timestamp_millis;
use async_zip::{Compression, ZipEntryBuilder};
use bytes::Bytes;
use byteview::ByteView;
use futures_util::StreamExt;
use futures_util::io::AsyncWriteExt;
use oxrdf::{NamedOrBlankNode, Term};
use oxttl::NQuadsParser;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use ulid::Ulid;
use unicode_normalization::UnicodeNormalization;
use url::Url;

use super::executor::{JobContext, JobRunOutcome};
use super::rocrate_jsonld::JsonLdKeywords;
use super::store::{put_job_entry, put_rocrate_checkpoint};
use crate::blob::hidden::delete_hidden;
use crate::blob::resolve_blob_permission_paths::ResolveBlobPermissionPathsOperation;
use crate::blob_holders::GetBlobHoldersOperation;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::metadata::raw::load_raw_view;
use crate::metadata::repository::StorageReadError;
use crate::replication::bao_read::{BaoReadError, BaoReadOperation, BaoReadOutput};
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};

const METADATA_PATH: &str = "ro-crate-metadata.json";
const REPORT_PATH: &str = "aruna-export-report.json";
const REMOTE_ATTEMPTS: usize = 8;
const JSONLD_BASE_IRI: &str = "https://craqle.invalid/";
const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SCHEMA_MEDIA_IRI: &str = "http://schema.org/MediaObject";
const SCHEMA_MEDIA_HTTPS_IRI: &str = "https://schema.org/MediaObject";
const SCHEMA_CONTENT_IRI: &str = "http://schema.org/contentUrl";
const SCHEMA_CONTENT_HTTPS_IRI: &str = "https://schema.org/contentUrl";
const SCHEMA_ABOUT_IRI: &str = "http://schema.org/about";
const SCHEMA_ABOUT_HTTPS_IRI: &str = "https://schema.org/about";
const SCHEMA_HAS_PART_IRI: &str = "http://schema.org/hasPart";
const SCHEMA_HAS_PART_HTTPS_IRI: &str = "https://schema.org/hasPart";
const SCHEMA_SUBJECT_IRI: &str = "http://schema.org/subjectOf";
const SCHEMA_SUBJECT_HTTPS_IRI: &str = "https://schema.org/subjectOf";
const SCHEMA_ENCODING_IRI: &str = "http://schema.org/encodingFormat";
const SCHEMA_ENCODING_HTTPS_IRI: &str = "https://schema.org/encodingFormat";
const SCHEMA_NAME_IRI: &str = "http://schema.org/name";
const SCHEMA_NAME_HTTPS_IRI: &str = "https://schema.org/name";
const LOCAL_PATH_IRI: &str = "https://w3id.org/ro/terms#localPath";
const LOCAL_PATH_HTTP_IRI: &str = "http://w3id.org/ro/terms#localPath";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ExportPhase {
    Snapshot,
    Resolve,
    Plan,
    Assemble,
    Publish,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ExportCheckpoint {
    refs: RoCrateCheckpointRefs,
    phase: ExportPhase,
    winning_event_id: Option<Ulid>,
    context_digest: Option<[u8; 32]>,
    dataset_digest: Option<[u8; 32]>,
    raw_jsonld: Option<String>,
    entities: Vec<ExportEntity>,
    rewritten_jsonld: Option<Vec<u8>>,
    report_json: Option<Vec<u8>>,
    report: Vec<ExportReportRow>,
    artifact: Option<ArtifactRef>,
}

impl Default for ExportCheckpoint {
    fn default() -> Self {
        Self {
            refs: RoCrateCheckpointRefs::default(),
            phase: ExportPhase::Snapshot,
            winning_event_id: None,
            context_digest: None,
            dataset_digest: None,
            raw_jsonld: None,
            entities: Vec::new(),
            rewritten_jsonld: None,
            report_json: None,
            report: Vec::new(),
            artifact: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ExportEntity {
    entity_id: String,
    local_path: Option<String>,
    exact: Option<VersionedObjectArn>,
    hash: Option<[u8; 32]>,
    hash_realm: Option<RealmId>,
    candidates: Vec<ExportCandidate>,
    omission: Option<ReasonCode>,
    message: Option<String>,
    zip_path: Option<String>,
    report_source: Option<ExportReportSource>,
    resolved_version: Option<Ulid>,
    path_synthesized: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ExportCandidate {
    source: CandidateSource,
    report_source: ExportReportSource,
    resolved_version: Option<Ulid>,
    expected_blake3: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum CandidateSource {
    Local(BackendLocation),
    RemoteExact {
        node_id: NodeId,
        target: VersionedObjectArn,
    },
    RemoteHash {
        node_id: NodeId,
        hash: [u8; 32],
    },
}

#[derive(Debug)]
struct ProbedEntry {
    entity_index: usize,
    candidate_index: usize,
    size: u64,
    hash: [u8; 32],
    report_source: ExportReportSource,
    resolved_version: Option<Ulid>,
}

#[derive(Debug)]
struct PlannedEntry {
    entity_index: usize,
    candidate_index: usize,
    path: String,
    source: PlannedSource,
    expected_blake3: [u8; 32],
}

#[derive(Debug)]
enum PlannedSource {
    Candidate {
        driver: std::sync::Arc<DriverContext>,
        spec: std::sync::Arc<ExportRoCrateSpec>,
        candidate: ExportCandidate,
    },
    #[cfg(test)]
    Ready(BackendStream<Result<Bytes, StreamError>>),
}

#[derive(Debug)]
enum ExportFailure {
    Permanent(String),
    Retryable(String),
    Validation(Vec<MetadataValidationViolation>),
    Candidate {
        entity_index: usize,
        candidate_index: usize,
        status: OpenStatus,
        message: String,
    },
    Cancelled,
    Interrupted,
}

enum ResolveResult {
    Candidate(ExportCandidate),
    Denied,
    Missing { hash: Option<[u8; 32]> },
}

#[derive(Clone, Copy, Debug)]
enum OpenStatus {
    Denied,
    Missing,
    Offline,
    Corrupt,
}

enum CandidateOpen {
    Opened(BaoReadOutput),
    Status(OpenStatus),
}

struct EntityIdentity {
    exact: Option<VersionedObjectArn>,
    hash: Option<[u8; 32]>,
    hash_realm: Option<RealmId>,
}

pub async fn run_export_job(ctx: &JobContext, spec: &ExportRoCrateSpec) -> JobRunOutcome {
    let mut checkpoint = match read_export_checkpoint(ctx, ctx.job_id).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => ExportCheckpoint::default(),
        Err(error) => return retryable(error),
    };
    if !checkpoint.entities.is_empty() {
        let total = checkpoint.entities.len() as u64;
        ctx.progress.set_total(total);
        ctx.progress.set_current(match checkpoint.phase {
            ExportPhase::Snapshot | ExportPhase::Resolve => 0,
            ExportPhase::Plan | ExportPhase::Assemble | ExportPhase::Publish => total,
        });
    }
    let mut probed = None;
    let mut candidate_failures = BTreeMap::<usize, BTreeMap<usize, OpenStatus>>::new();

    loop {
        if ctx.cancel.is_cancelled() {
            discard_artifact(ctx, &mut checkpoint, true).await;
            return JobRunOutcome::Cancelled;
        }
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }

        let result = match checkpoint.phase {
            ExportPhase::Snapshot => snapshot_export(ctx, spec, &mut checkpoint).await,
            ExportPhase::Resolve => resolve_entries(ctx, spec, &mut checkpoint).await,
            ExportPhase::Plan => {
                match probe_sources(ctx, spec, &mut checkpoint, &candidate_failures).await {
                    Ok(entries) => {
                        let result = plan_export(spec, &mut checkpoint, &entries);
                        probed = Some(entries);
                        result
                    }
                    Err(error) => Err(error),
                }
            }
            ExportPhase::Assemble => {
                if probed.is_none() {
                    match probe_sources(ctx, spec, &mut checkpoint, &candidate_failures).await {
                        Ok(entries) => {
                            if let Err(error) = plan_export(spec, &mut checkpoint, &entries) {
                                return finish_export(ctx, &mut checkpoint, error).await;
                            }
                            probed = Some(entries);
                        }
                        Err(error) => return finish_export(ctx, &mut checkpoint, error).await,
                    }
                }
                let Some(entries) = probed.take() else {
                    return permanent("planned export sources are missing");
                };
                match assemble_export(ctx, spec, &checkpoint, entries).await {
                    Ok(artifact) => {
                        checkpoint.refs.hidden_locations = vec![artifact.location.clone()];
                        checkpoint.artifact = Some(artifact);
                        checkpoint.phase = ExportPhase::Publish;
                        Ok(())
                    }
                    Err(ExportFailure::Candidate {
                        entity_index,
                        candidate_index,
                        status,
                        ..
                    }) => {
                        candidate_failures
                            .entry(entity_index)
                            .or_default()
                            .insert(candidate_index, status);
                        continue;
                    }
                    Err(error) => Err(error),
                }
            }
            ExportPhase::Publish => {
                let outcome = publish_export(ctx, &checkpoint).await;
                if matches!(&outcome, JobRunOutcome::Cancelled) {
                    discard_artifact(ctx, &mut checkpoint, true).await;
                }
                return outcome;
            }
        };
        if let Err(error) = result {
            return finish_export(ctx, &mut checkpoint, error).await;
        }
        if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
            discard_artifact(ctx, &mut checkpoint, false).await;
            return retryable(error);
        }
    }
}

async fn snapshot_export(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
) -> Result<(), ExportFailure> {
    let record = load_metadata_record_by_document(&ctx.driver, spec.document_id)
        .await
        .map_err(|error| match error {
            StorageReadError::Storage(error) => ExportFailure::Retryable(error.to_string()),
            StorageReadError::Conversion(error) => ExportFailure::Retryable(error.to_string()),
        })?
        .ok_or_else(|| ExportFailure::Permanent("metadata document not found".to_string()))?;
    if record.permission_path.is_empty() {
        return Err(ExportFailure::Permanent(
            "metadata document has no permission path".to_string(),
        ));
    }
    if !check_read(ctx, spec, record.permission_path).await? {
        return Err(ExportFailure::Permanent(
            "metadata document READ permission denied".to_string(),
        ));
    }

    let raw = load_raw_view(&ctx.driver, spec.document_id)
        .await
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?
        .ok_or_else(|| {
            ExportFailure::Permanent("metadata document has no raw RO-Crate revision".to_string())
        })?;
    if raw.revision.jsonld.len() as u64 > spec.limits.metadata_bytes {
        return Err(ExportFailure::Permanent(format!(
            "RO-Crate metadata exceeds the {} byte limit",
            spec.limits.metadata_bytes
        )));
    }
    let canonical =
        craqle::validate_rocrate_jsonld(&raw.revision.jsonld).map_err(map_crate_error)?;
    let document: JsonValue = serde_json::from_str(&raw.revision.jsonld)
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let entities = recognize_entities(&document, &canonical.nquads, spec.auth_context.realm_id)?;
    if entities.len() as u64 > spec.limits.max_entries {
        return Err(ExportFailure::Permanent(format!(
            "RO-Crate has more than {} File entities",
            spec.limits.max_entries
        )));
    }

    checkpoint.winning_event_id = Some(raw.revision.winning_event_id);
    checkpoint.context_digest = Some(raw.revision.context_digest);
    checkpoint.dataset_digest = Some(canonical.digest);
    checkpoint.raw_jsonld = Some(raw.revision.jsonld);
    checkpoint.entities = entities;
    checkpoint.phase = ExportPhase::Resolve;
    ctx.progress.set_total(checkpoint.entities.len() as u64);
    Ok(())
}

async fn resolve_entries(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
) -> Result<(), ExportFailure> {
    for index in 0..checkpoint.entities.len() {
        if ctx.cancel.is_cancelled() {
            return Err(ExportFailure::Cancelled);
        }
        if ctx.shutdown.is_cancelled() {
            return Err(ExportFailure::Interrupted);
        }
        let entity = &checkpoint.entities[index];
        if entity.omission.is_some() {
            ctx.progress.advance(1);
            continue;
        }
        let mut candidates = Vec::new();
        let mut denied = false;
        let mut hash = entity.hash.filter(|_| {
            entity
                .hash_realm
                .is_none_or(|realm_id| realm_id == spec.auth_context.realm_id)
        });
        let exact_version = entity
            .exact
            .as_ref()
            .filter(|exact| exact.realm_id == spec.auth_context.realm_id)
            .map(|exact| exact.version);
        let mut mismatched = false;

        if let Some(exact) = entity
            .exact
            .as_ref()
            .filter(|exact| exact.realm_id == spec.auth_context.realm_id)
        {
            if exact.node_id == ctx.owner_node_id {
                match resolve_exact(ctx, spec, exact).await? {
                    ResolveResult::Candidate(candidate) => {
                        if hash.is_some_and(|hash| Some(hash) != candidate.expected_blake3) {
                            mismatched = true;
                        } else {
                            candidates.push(candidate);
                        }
                    }
                    ResolveResult::Denied => denied = true,
                    ResolveResult::Missing {
                        hash: discovered_hash,
                    } => {
                        if let (Some(expected), Some(discovered)) = (hash, discovered_hash)
                            && expected != discovered
                        {
                            mismatched = true;
                        } else if hash.is_none() {
                            hash = discovered_hash;
                        }
                    }
                }
            } else {
                candidates.push(ExportCandidate {
                    source: CandidateSource::RemoteExact {
                        node_id: exact.node_id,
                        target: exact.clone(),
                    },
                    report_source: ExportReportSource::Remote,
                    resolved_version: Some(exact.version),
                    expected_blake3: hash,
                });
            }
        }

        if mismatched {
            checkpoint.entities[index].omission = Some(ReasonCode::Unsupported);
            checkpoint.entities[index].message =
                Some("versioned ARN and content hash disagree".to_string());
            ctx.progress.advance(1);
            continue;
        }

        if let Some(hash) = hash {
            let aliases = drive(ResolveBlobPermissionPathsOperation::new(hash), &ctx.driver)
                .await
                .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
            for alias in aliases
                .into_iter()
                .filter(|alias| alias.realm_id == spec.auth_context.realm_id)
            {
                match resolve_alias(ctx, spec, &alias).await? {
                    ResolveResult::Candidate(candidate) => {
                        if !candidates.contains(&candidate) {
                            candidates.push(candidate);
                        }
                    }
                    ResolveResult::Denied => denied = true,
                    ResolveResult::Missing { .. } => {}
                }
            }

            match drive(
                GetBlobHoldersOperation::new(hash, spec.auth_context.realm_id, ctx.owner_node_id),
                &ctx.driver,
            )
            .await
            {
                Ok(holders) => {
                    let remote_count = candidates
                        .iter()
                        .filter(|candidate| !matches!(candidate.source, CandidateSource::Local(_)))
                        .count();
                    let holder_limit = REMOTE_ATTEMPTS.saturating_sub(remote_count);
                    for node_id in holders.into_iter().take(holder_limit) {
                        let candidate = ExportCandidate {
                            source: CandidateSource::RemoteHash { node_id, hash },
                            report_source: ExportReportSource::Hash,
                            resolved_version: exact_version,
                            expected_blake3: Some(hash),
                        };
                        if !candidates.contains(&candidate) {
                            candidates.push(candidate);
                        }
                    }
                }
                Err(_) if candidates.is_empty() => {
                    checkpoint.entities[index].omission = Some(ReasonCode::Offline);
                    checkpoint.entities[index].message =
                        Some("blob holder discovery is unavailable".to_string());
                    ctx.progress.advance(1);
                    continue;
                }
                Err(_) => {}
            }
        }

        let target = &mut checkpoint.entities[index];
        target.candidates = candidates;
        if target.candidates.is_empty() {
            target.omission = Some(if denied {
                ReasonCode::Denied
            } else {
                ReasonCode::Missing
            });
            target.message = Some(
                match target.omission {
                    Some(ReasonCode::Denied) => "payload READ permission denied",
                    Some(ReasonCode::Offline) => "payload is currently unreachable",
                    _ => "no readable payload version was found",
                }
                .to_string(),
            );
        }
        ctx.progress.advance(1);
    }
    checkpoint.phase = ExportPhase::Plan;
    Ok(())
}

async fn resolve_exact(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    exact: &VersionedObjectArn,
) -> Result<ResolveResult, ExportFailure> {
    let Some(bucket) = storage_value(
        ctx,
        S3_BUCKET_KEYSPACE,
        exact.bucket.as_bytes().to_vec().into(),
    )
    .await?
    else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let bucket = BucketInfo::from_bytes(bucket.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    let permission_path = blob_object_permission_path(
        spec.auth_context.realm_id,
        bucket.group_id,
        exact.node_id,
        &exact.bucket,
        &exact.key,
    );
    if !check_read(ctx, spec, permission_path).await? {
        return Ok(ResolveResult::Denied);
    }
    let key = VersionKey::new(exact.bucket.clone(), exact.key.clone(), exact.version)
        .to_bytes()
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let Some(value) = storage_value(ctx, BLOB_VERSIONS_KEYSPACE, key.into()).await? else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let version = BlobVersion::from_bytes(value.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    let Some(hash) = version.blob_hash().copied() else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let Some(location) = storage_value(ctx, BLOB_LOCATIONS_KEYSPACE, hash.to_vec().into()).await?
    else {
        return Ok(ResolveResult::Missing { hash: Some(hash) });
    };
    let location = BackendLocation::from_bytes(location.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    if location.get_blake3() != Some(hash.as_slice()) {
        return Ok(ResolveResult::Missing { hash: Some(hash) });
    }
    Ok(ResolveResult::Candidate(ExportCandidate {
        source: CandidateSource::Local(location),
        report_source: ExportReportSource::Local,
        resolved_version: Some(exact.version),
        expected_blake3: Some(hash),
    }))
}

async fn resolve_alias(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    alias: &HashPathIndexKey,
) -> Result<ResolveResult, ExportFailure> {
    if !check_read(ctx, spec, alias.permission_path()).await? {
        return Ok(ResolveResult::Denied);
    }
    let key = VersionKey::new(alias.bucket.clone(), alias.key.clone(), alias.version_id)
        .to_bytes()
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let Some(value) = storage_value(ctx, BLOB_VERSIONS_KEYSPACE, key.into()).await? else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let version = BlobVersion::from_bytes(value.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    if version.blob_hash() != Some(&alias.blake3_hash) {
        return Ok(ResolveResult::Missing { hash: None });
    }
    let Some(location) = storage_value(
        ctx,
        BLOB_LOCATIONS_KEYSPACE,
        alias.blake3_hash.to_vec().into(),
    )
    .await?
    else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let location = BackendLocation::from_bytes(location.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    if location.get_blake3() != Some(alias.blake3_hash.as_slice()) {
        return Ok(ResolveResult::Missing { hash: None });
    }
    Ok(ResolveResult::Candidate(ExportCandidate {
        source: CandidateSource::Local(location),
        report_source: ExportReportSource::Hash,
        resolved_version: Some(alias.version_id),
        expected_blake3: Some(alias.blake3_hash),
    }))
}

async fn check_read(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    path: String,
) -> Result<bool, ExportFailure> {
    drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: spec.auth_context.clone(),
            path,
            required_permission: Permission::READ,
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| ExportFailure::Retryable(error.to_string()))
}

async fn storage_value(
    ctx: &JobContext,
    key_space: &str,
    key: Key,
) -> Result<Option<Value>, ExportFailure> {
    match ctx
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(ExportFailure::Retryable(error.to_string()))
        }
        event => Err(ExportFailure::Retryable(format!(
            "unexpected storage read event: {event:?}"
        ))),
    }
}

async fn probe_sources(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    candidate_failures: &BTreeMap<usize, BTreeMap<usize, OpenStatus>>,
) -> Result<Vec<ProbedEntry>, ExportFailure> {
    let mut probed = Vec::new();
    for index in 0..checkpoint.entities.len() {
        if ctx.cancel.is_cancelled() {
            return Err(ExportFailure::Cancelled);
        }
        if ctx.shutdown.is_cancelled() {
            return Err(ExportFailure::Interrupted);
        }
        if checkpoint.entities[index].omission.is_some() {
            continue;
        }
        let candidates = checkpoint.entities[index].candidates.clone();
        let mut denied = false;
        let mut missing = false;
        let mut offline = false;
        let mut corrupt = false;
        let mut selected = None;
        let failed = candidate_failures.get(&index);
        for status in failed.into_iter().flat_map(|failed| failed.values()) {
            match status {
                OpenStatus::Denied => denied = true,
                OpenStatus::Missing => missing = true,
                OpenStatus::Offline => offline = true,
                OpenStatus::Corrupt => corrupt = true,
            }
        }
        for (candidate_index, candidate) in candidates.into_iter().enumerate() {
            if failed.is_some_and(|failed| failed.contains_key(&candidate_index)) {
                continue;
            }
            if ctx.cancel.is_cancelled() {
                return Err(ExportFailure::Cancelled);
            }
            if ctx.shutdown.is_cancelled() {
                return Err(ExportFailure::Interrupted);
            }
            match open_candidate(&ctx.driver, spec, &candidate, true).await? {
                CandidateOpen::Opened(BaoReadOutput::Metadata { size, blake3 }) => {
                    selected = Some(ProbedEntry {
                        entity_index: index,
                        candidate_index,
                        size,
                        hash: blake3,
                        report_source: candidate.report_source,
                        resolved_version: candidate.resolved_version,
                    });
                    break;
                }
                CandidateOpen::Opened(BaoReadOutput::Stream { .. }) => {
                    return Err(ExportFailure::Retryable(
                        "source probe unexpectedly opened a stream".to_string(),
                    ));
                }
                CandidateOpen::Status(OpenStatus::Denied) => denied = true,
                CandidateOpen::Status(OpenStatus::Missing) => missing = true,
                CandidateOpen::Status(OpenStatus::Offline) => offline = true,
                CandidateOpen::Status(OpenStatus::Corrupt) => corrupt = true,
            }
        }
        if let Some(selected) = selected {
            probed.push(selected);
            continue;
        }
        if corrupt {
            return Err(ExportFailure::Retryable(
                "payload integrity check failed".to_string(),
            ));
        }

        let entity = &mut checkpoint.entities[index];
        entity.zip_path = None;
        entity.report_source = None;
        entity.resolved_version = None;
        entity.path_synthesized = false;
        entity.omission = Some(if denied {
            ReasonCode::Denied
        } else if offline {
            ReasonCode::Offline
        } else {
            ReasonCode::Missing
        });
        entity.message = Some(
            if denied {
                "all payload candidates denied READ"
            } else if offline {
                "all payload candidates are offline"
            } else if missing {
                "all payload candidates are missing"
            } else {
                "no payload candidate is available"
            }
            .to_string(),
        );
    }
    Ok(probed)
}

async fn open_candidate(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    candidate: &ExportCandidate,
    metadata_only: bool,
) -> Result<CandidateOpen, ExportFailure> {
    match &candidate.source {
        CandidateSource::Local(location) => {
            let Some(blake3) = candidate.expected_blake3 else {
                return Err(ExportFailure::Permanent(
                    "local export candidate has no BLAKE3 hash".to_string(),
                ));
            };
            let Some(blob_handle) = driver.blob_handle.as_ref() else {
                return Err(ExportFailure::Retryable(
                    "blob handle unavailable".to_string(),
                ));
            };
            match blob_handle
                .send_blob_effect(BlobEffect::Read {
                    location: location.clone(),
                })
                .await
            {
                Event::Blob(BlobEvent::ReadFinished { blob, stream_size }) => {
                    Ok(CandidateOpen::Opened(if metadata_only {
                        BaoReadOutput::Metadata {
                            size: stream_size,
                            blake3,
                        }
                    } else {
                        BaoReadOutput::Stream {
                            blob,
                            size: stream_size,
                            blake3,
                        }
                    }))
                }
                Event::Blob(BlobEvent::Error(BlobError::IntegrityCheckFailed(_))) => {
                    Ok(CandidateOpen::Status(OpenStatus::Corrupt))
                }
                Event::Blob(BlobEvent::Error(_)) => Ok(CandidateOpen::Status(OpenStatus::Offline)),
                event => Err(ExportFailure::Retryable(format!(
                    "unexpected local blob read event: {event:?}"
                ))),
            }
        }
        CandidateSource::RemoteExact { node_id, target } => {
            open_remote(
                driver,
                spec,
                *node_id,
                BaoReadTarget::ExactVersion(target.clone()),
                candidate.expected_blake3,
                metadata_only,
            )
            .await
        }
        CandidateSource::RemoteHash { node_id, hash } => {
            open_remote(
                driver,
                spec,
                *node_id,
                BaoReadTarget::Blake3(*hash),
                candidate.expected_blake3,
                metadata_only,
            )
            .await
        }
    }
}

async fn open_remote(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    node_id: NodeId,
    target: BaoReadTarget,
    expected_blake3: Option<[u8; 32]>,
    metadata_only: bool,
) -> Result<CandidateOpen, ExportFailure> {
    match drive(
        BaoReadOperation::new(
            node_id,
            BaoReadRequest {
                auth_context: spec.auth_context.clone(),
                realm_id: spec.auth_context.realm_id,
                target,
                expected_blake3,
                metadata_only,
            },
        ),
        driver,
    )
    .await
    {
        Ok(output) => Ok(CandidateOpen::Opened(output)),
        Err(BaoReadError::Refused(
            BaoReadRefusal::ReadDenied | BaoReadRefusal::RealmPeerDenied,
        )) => Ok(CandidateOpen::Status(OpenStatus::Denied)),
        Err(BaoReadError::Refused(BaoReadRefusal::NotFound | BaoReadRefusal::InvalidTarget)) => {
            Ok(CandidateOpen::Status(OpenStatus::Missing))
        }
        Err(BaoReadError::Refused(BaoReadRefusal::HashMismatch)) => {
            Ok(CandidateOpen::Status(OpenStatus::Corrupt))
        }
        Err(
            BaoReadError::Refused(BaoReadRefusal::BackendFailure)
            | BaoReadError::Blob(BlobError::ConnectionFailed(_))
            | BaoReadError::Blob(BlobError::ChannelClosed),
        ) => Ok(CandidateOpen::Status(OpenStatus::Offline)),
        Err(BaoReadError::Blob(BlobError::IntegrityCheckFailed(_))) => {
            Ok(CandidateOpen::Status(OpenStatus::Corrupt))
        }
        Err(
            BaoReadError::Blob(BlobError::ReadError(_))
            | BaoReadError::Blob(BlobError::OperatorCreationFailed(_))
            | BaoReadError::Blob(BlobError::HandleMissing)
            | BaoReadError::Blob(BlobError::SendError)
            | BaoReadError::Blob(BlobError::ReplicationFailed(_))
            | BaoReadError::Blob(BlobError::ReplicationRejected(_)),
        ) => Ok(CandidateOpen::Status(OpenStatus::Offline)),
        Err(error) => Err(ExportFailure::Retryable(error.to_string())),
    }
}

fn plan_export(
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    opened: &[ProbedEntry],
) -> Result<(), ExportFailure> {
    let mut paths = HashSet::new();
    for entry in opened {
        let entity = &mut checkpoint.entities[entry.entity_index];
        entity.report_source = Some(entry.report_source);
        entity.resolved_version = entry.resolved_version;
        let explicit = entity
            .local_path
            .as_deref()
            .and_then(safe_zip_path)
            .filter(|path| path != METADATA_PATH && path != REPORT_PATH);
        let path = match explicit {
            Some(path) => path,
            None => {
                entity.path_synthesized = true;
                synthesized_path(entry.hash, &entity.entity_id)
            }
        };
        if path.len() as u64 > spec.limits.key_bytes {
            return Err(ExportFailure::Permanent(format!(
                "ZIP path exceeds the {} byte limit",
                spec.limits.key_bytes
            )));
        }
        if !paths.insert(path.clone()) {
            return Err(ExportFailure::Permanent(format!(
                "multiple File entities resolve to ZIP path `{path}`"
            )));
        }
        entity.zip_path = Some(path);
    }

    let raw = checkpoint
        .raw_jsonld
        .as_deref()
        .ok_or_else(|| ExportFailure::Permanent("raw RO-Crate snapshot is missing".to_string()))?;
    let mut document: JsonValue =
        serde_json::from_str(raw).map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let replacements = checkpoint
        .entities
        .iter()
        .filter_map(|entity| {
            entity
                .zip_path
                .as_ref()
                .map(|path| (entity.entity_id.clone(), path.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let unrewritten = scan_unrewritten(&document, &replacements);
    rewrite_ids(&mut document, &replacements);
    checkpoint.report = build_rows(&checkpoint.entities, &unrewritten);
    let has_omissions = checkpoint.report.iter().any(|row| {
        matches!(
            row.code,
            ReasonCode::External
                | ReasonCode::Denied
                | ReasonCode::Missing
                | ReasonCode::Offline
                | ReasonCode::Unsupported
        )
    });
    checkpoint.report_json = if has_omissions {
        let report = build_report(checkpoint)?;
        add_report(&mut document)?;
        Some(
            serde_json::to_vec(&report)
                .map_err(|error| ExportFailure::Permanent(error.to_string()))?,
        )
    } else {
        None
    };
    let rewritten = serde_json::to_vec(&document)
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    if rewritten.len() as u64 > spec.limits.metadata_bytes {
        return Err(ExportFailure::Permanent(format!(
            "rewritten RO-Crate metadata exceeds the {} byte limit",
            spec.limits.metadata_bytes
        )));
    }
    craqle::validate_rocrate_jsonld(
        std::str::from_utf8(&rewritten)
            .map_err(|error| ExportFailure::Permanent(error.to_string()))?,
    )
    .map_err(map_crate_error)?;
    precheck_size(
        spec,
        &rewritten,
        checkpoint.report_json.as_deref(),
        opened,
        &checkpoint.entities,
    )?;
    checkpoint.rewritten_jsonld = Some(rewritten);
    checkpoint.phase = ExportPhase::Assemble;
    Ok(())
}

fn recognize_entities(
    document: &JsonValue,
    nquads: &str,
    realm_id: RealmId,
) -> Result<Vec<ExportEntity>, ExportFailure> {
    let keywords = JsonLdKeywords::new(document);
    let raw_ids = raw_entity_ids(document, &keywords)?;
    let mut files = BTreeSet::new();
    let mut content_urls = BTreeMap::<String, Vec<String>>::new();
    let mut local_paths = BTreeMap::<String, String>::new();
    for quad in NQuadsParser::new().for_slice(nquads) {
        let quad = quad.map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        let NamedOrBlankNode::NamedNode(subject) = quad.subject else {
            continue;
        };
        let subject = subject.as_str().to_string();
        match quad.predicate.as_str() {
            RDF_TYPE_IRI
                if matches!(
                    &quad.object,
                    Term::NamedNode(node)
                        if matches!(node.as_str(), SCHEMA_MEDIA_IRI | SCHEMA_MEDIA_HTTPS_IRI)
                ) =>
            {
                files.insert(subject);
            }
            SCHEMA_CONTENT_IRI | SCHEMA_CONTENT_HTTPS_IRI => {
                if let Some(value) = term_value(&quad.object) {
                    content_urls.entry(subject).or_default().push(value);
                }
            }
            LOCAL_PATH_IRI | LOCAL_PATH_HTTP_IRI => {
                if let Some(value) = term_value(&quad.object) {
                    local_paths.entry(subject).or_insert(value);
                }
            }
            _ => {}
        }
    }

    let mut entities = Vec::new();
    for (subject, entity_id) in raw_ids {
        if !files.remove(&subject) {
            continue;
        }
        let identity = entity_identity(
            &entity_id,
            content_urls.get(&subject).map_or(&[], Vec::as_slice),
        );
        let external = identity.exact.is_none() && identity.hash.is_none();
        let hash_realm = identity.hash_realm;
        let supported_exact = identity
            .exact
            .as_ref()
            .is_some_and(|exact| exact.realm_id == realm_id);
        let supported_hash =
            identity.hash.is_some() && hash_realm.is_none_or(|hash_realm| hash_realm == realm_id);
        let unsupported_realm = !external && !supported_exact && !supported_hash;
        entities.push(ExportEntity {
            entity_id,
            local_path: local_paths.remove(&subject),
            exact: identity.exact,
            hash: identity.hash,
            hash_realm,
            candidates: Vec::new(),
            omission: if external {
                Some(ReasonCode::External)
            } else if unsupported_realm {
                Some(ReasonCode::Unsupported)
            } else {
                None
            },
            message: if external {
                Some("external File entity was not fetched".to_string())
            } else if unsupported_realm {
                Some("Aruna identifier belongs to another realm".to_string())
            } else {
                None
            },
            zip_path: None,
            report_source: None,
            resolved_version: None,
            path_synthesized: false,
        });
    }
    if let Some(subject) = files.into_iter().next() {
        return Err(ExportFailure::Permanent(format!(
            "expanded File entity `{subject}` has no raw JSON-LD definition"
        )));
    }
    Ok(entities)
}

fn raw_entity_ids(
    document: &JsonValue,
    keywords: &JsonLdKeywords,
) -> Result<Vec<(String, String)>, ExportFailure> {
    fn collect(
        value: &JsonValue,
        keywords: &JsonLdKeywords,
        entities: &mut Vec<(String, String)>,
    ) -> Result<(), ExportFailure> {
        match value {
            JsonValue::Array(values) => {
                for value in values {
                    collect(value, keywords, entities)?;
                }
            }
            JsonValue::Object(object) => {
                if object.len() > 1
                    && let Some((_, id)) = keywords.object_id(object)
                {
                    let expanded = expanded_id(id)?;
                    if let Some((_, existing_id)) =
                        entities.iter().find(|(existing, _)| existing == &expanded)
                    {
                        if existing_id != id {
                            return Err(ExportFailure::Permanent(format!(
                                "JSON-LD entity `{expanded}` uses ambiguous identifiers"
                            )));
                        }
                    } else {
                        entities.push((expanded, id.to_string()));
                    }
                }
                for value in object.values() {
                    collect(value, keywords, entities)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    let mut entities = Vec::new();
    collect(document, keywords, &mut entities)?;
    Ok(entities)
}

fn expanded_id(id: &str) -> Result<String, ExportFailure> {
    if let Ok(url) = Url::parse(id) {
        return Ok(url.to_string());
    }
    Url::parse(JSONLD_BASE_IRI)
        .expect("static JSON-LD base is valid")
        .join(id)
        .map(String::from)
        .map_err(|error| ExportFailure::Permanent(error.to_string()))
}

fn term_value(term: &Term) -> Option<String> {
    match term {
        Term::NamedNode(value) => Some(value.as_str().to_string()),
        Term::Literal(value) => Some(value.value().to_string()),
        _ => None,
    }
}

fn entity_identity(entity_id: &str, content_urls: &[String]) -> EntityIdentity {
    let mut exact = None;
    let mut hash = None;
    let mut hash_realm = None;
    for value in std::iter::once(entity_id).chain(content_urls.iter().map(String::as_str)) {
        if let Ok(identifier) = W3idDataIdentifier::parse(value) {
            match identifier {
                W3idDataIdentifier::ContentHash(value) => hash = Some(value),
                W3idDataIdentifier::VersionedObject(value) => exact = Some(value),
            }
            continue;
        }
        if let Ok(value) = VersionedObjectArn::parse(value) {
            exact = Some(value);
            continue;
        }
        if let Ok(value) = ArunaArn::parse(value)
            && value.resource_type == ArunaArnType::ContentHash
            && let Some(value_hash) = parse_hash(&value.path)
        {
            hash = Some(value_hash);
            hash_realm = Some(value.realm_id);
        }
    }
    EntityIdentity {
        exact,
        hash,
        hash_realm,
    }
}

fn parse_hash(value: &str) -> Option<[u8; 32]> {
    let value = value.strip_prefix("blake3/").unwrap_or(value);
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return None;
    }
    let mut hash = [0; 32];
    hex::decode_to_slice(value, &mut hash).ok()?;
    Some(hash)
}

fn safe_zip_path(value: &str) -> Option<String> {
    let mut value = value;
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped;
    }
    let normalized = value.nfc().collect::<String>();
    let lower = normalized.to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.ends_with('/')
        || normalized.contains('\\')
        || lower.contains("%2f")
        || lower.contains("%5c")
        || normalized
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || ensure_confined_relative_path(Path::new(&normalized)).is_err()
        || Path::new(&normalized)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return None;
    }
    Some(normalized)
}

fn synthesized_path(hash: [u8; 32], entity_id: &str) -> String {
    let suffix = blake3::hash(entity_id.as_bytes()).to_hex();
    format!("data/{}-{}", hex::encode(hash), &suffix[..12])
}

fn scan_unrewritten(
    document: &JsonValue,
    replacements: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    fn scan(
        value: &JsonValue,
        key: Option<&str>,
        replacements: &BTreeMap<String, String>,
        keywords: &JsonLdKeywords,
        found: &mut BTreeSet<String>,
    ) {
        match value {
            JsonValue::String(value)
                if !key.is_some_and(|key| keywords.is_id(key))
                    && replacements.contains_key(value.as_str()) =>
            {
                found.insert(value.clone());
            }
            JsonValue::Array(values) => {
                for value in values {
                    scan(value, key, replacements, keywords, found);
                }
            }
            JsonValue::Object(values) => {
                for (key, value) in values {
                    scan(value, Some(key), replacements, keywords, found);
                }
            }
            _ => {}
        }
    }
    let keywords = JsonLdKeywords::new(document);
    let mut found = BTreeSet::new();
    scan(document, None, replacements, &keywords, &mut found);
    found
}

fn rewrite_ids(value: &mut JsonValue, replacements: &BTreeMap<String, String>) {
    let keywords = JsonLdKeywords::new(value);
    rewrite_id_values(value, replacements, &keywords);
}

fn rewrite_id_values(
    value: &mut JsonValue,
    replacements: &BTreeMap<String, String>,
    keywords: &JsonLdKeywords,
) {
    match value {
        JsonValue::Array(values) => {
            for value in values {
                rewrite_id_values(value, replacements, keywords);
            }
        }
        JsonValue::Object(values) => {
            let id_key = keywords.object_id(values).map(|(key, _)| key.to_string());
            if let Some(JsonValue::String(value)) =
                id_key.as_deref().and_then(|key| values.get_mut(key))
                && let Some(replacement) = replacements.get(value)
            {
                *value = replacement.clone();
            }
            for value in values.values_mut() {
                rewrite_id_values(value, replacements, keywords);
            }
        }
        _ => {}
    }
}

fn build_rows(entities: &[ExportEntity], unrewritten: &BTreeSet<String>) -> Vec<ExportReportRow> {
    let mut rows = Vec::new();
    for (index, entity) in entities.iter().enumerate() {
        let main_code = entity.omission.unwrap_or(ReasonCode::Included);
        rows.push(ExportReportRow {
            entry_key: format!("{index:016x}:main"),
            code: main_code,
            message: entity.message.clone(),
            detail: ExportReportDetail {
                entity_id: entity.entity_id.clone(),
                zip_path: entity.zip_path.clone(),
                source: entity.report_source,
                resolved_version: entity.resolved_version,
                validation: None,
            },
        });
        if entity.path_synthesized {
            rows.push(ExportReportRow {
                entry_key: format!("{index:016x}:path"),
                code: ReasonCode::PathSynthesized,
                message: Some("unsafe, absent, or reserved localPath was synthesized".to_string()),
                detail: ExportReportDetail {
                    entity_id: entity.entity_id.clone(),
                    zip_path: entity.zip_path.clone(),
                    source: entity.report_source,
                    resolved_version: entity.resolved_version,
                    validation: None,
                },
            });
        }
        if unrewritten.contains(&entity.entity_id) {
            rows.push(ExportReportRow {
                entry_key: format!("{index:016x}:reference"),
                code: ReasonCode::UnrewrittenReference,
                message: Some(
                    "a string-form reference outside an @id field was preserved".to_string(),
                ),
                detail: ExportReportDetail {
                    entity_id: entity.entity_id.clone(),
                    zip_path: entity.zip_path.clone(),
                    source: entity.report_source,
                    resolved_version: entity.resolved_version,
                    validation: None,
                },
            });
        }
    }
    rows
}

fn build_report(checkpoint: &ExportCheckpoint) -> Result<JsonValue, ExportFailure> {
    let event_id = checkpoint
        .winning_event_id
        .ok_or_else(|| ExportFailure::Permanent("snapshot event cursor is missing".to_string()))?;
    let context_digest = checkpoint.context_digest.ok_or_else(|| {
        ExportFailure::Permanent("snapshot context digest is missing".to_string())
    })?;
    let dataset_digest = checkpoint.dataset_digest.ok_or_else(|| {
        ExportFailure::Permanent("snapshot dataset digest is missing".to_string())
    })?;
    let omissions = checkpoint
        .report
        .iter()
        .filter(|row| {
            matches!(
                row.code,
                ReasonCode::External
                    | ReasonCode::Denied
                    | ReasonCode::Missing
                    | ReasonCode::Offline
                    | ReasonCode::Unsupported
            )
        })
        .map(|row| {
            json!({
                "entity_id": &row.detail.entity_id,
                "code": row.code,
                "message": &row.message,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "winning_event_id": event_id,
        "context_digest": hex::encode(context_digest),
        "dataset_digest": hex::encode(dataset_digest),
        "omissions": omissions,
    }))
}

fn add_report(document: &mut JsonValue) -> Result<(), ExportFailure> {
    let keywords = JsonLdKeywords::new(document);
    let graph = keywords
        .graph_mut(document)
        .ok_or_else(|| ExportFailure::Permanent("RO-Crate @graph is missing".to_string()))?;
    if graph.iter().any(|entity| {
        entity
            .as_object()
            .and_then(|entity| keywords.object_id(entity))
            .is_some_and(|(_, id)| id == REPORT_PATH || id == "#aruna-export-report")
    }) {
        return Err(ExportFailure::Permanent(
            "RO-Crate uses a reserved export report identifier".to_string(),
        ));
    }
    let root = graph
        .iter_mut()
        .find(|entity| {
            entity
                .as_object()
                .and_then(|entity| keywords.object_id(entity))
                .is_some_and(|(_, id)| id == "./")
        })
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| ExportFailure::Permanent("RO-Crate root Dataset is missing".to_string()))?;
    let subject_key = property_key(
        root,
        &keywords,
        &[
            "subjectOf",
            "schema:subjectOf",
            SCHEMA_SUBJECT_IRI,
            SCHEMA_SUBJECT_HTTPS_IRI,
        ],
        "subjectOf",
        SCHEMA_SUBJECT_HTTPS_IRI,
    );
    match root.get_mut(&subject_key) {
        Some(JsonValue::Array(values)) => values.push(json!({"@id": "#aruna-export-report"})),
        Some(value) => {
            let previous = std::mem::take(value);
            *value = json!([previous, {"@id": "#aruna-export-report"}]);
        }
        None => {
            root.insert(subject_key, json!({"@id": "#aruna-export-report"}));
        }
    }
    let part_key = property_key(
        root,
        &keywords,
        &[
            "hasPart",
            "schema:hasPart",
            SCHEMA_HAS_PART_IRI,
            SCHEMA_HAS_PART_HTTPS_IRI,
        ],
        "hasPart",
        SCHEMA_HAS_PART_HTTPS_IRI,
    );
    match root.get_mut(&part_key) {
        Some(JsonValue::Array(values)) => values.push(json!({"@id": REPORT_PATH})),
        Some(value) => {
            let previous = std::mem::take(value);
            *value = json!([previous, {"@id": REPORT_PATH}]);
        }
        None => {
            root.insert(part_key, json!({"@id": REPORT_PATH}));
        }
    }
    let encoding_key = safe_term(
        &keywords,
        "encodingFormat",
        &[
            SCHEMA_ENCODING_IRI,
            SCHEMA_ENCODING_HTTPS_IRI,
            "schema:encodingFormat",
        ],
        SCHEMA_ENCODING_HTTPS_IRI,
    );
    let about_key = safe_term(
        &keywords,
        "about",
        &[SCHEMA_ABOUT_IRI, SCHEMA_ABOUT_HTTPS_IRI, "schema:about"],
        SCHEMA_ABOUT_HTTPS_IRI,
    );
    let name_key = safe_term(
        &keywords,
        "name",
        &[SCHEMA_NAME_IRI, SCHEMA_NAME_HTTPS_IRI, "schema:name"],
        SCHEMA_NAME_HTTPS_IRI,
    );
    graph.push(JsonValue::Object(serde_json::Map::from_iter([
        ("@id".to_string(), json!(REPORT_PATH)),
        ("@type".to_string(), json!(SCHEMA_MEDIA_IRI)),
        (encoding_key, json!("application/json")),
        (about_key.clone(), json!({"@id": "#aruna-export-report"})),
    ])));
    graph.push(JsonValue::Object(serde_json::Map::from_iter([
        ("@id".to_string(), json!("#aruna-export-report")),
        ("@type".to_string(), json!("http://schema.org/CreativeWork")),
        (name_key, json!("Aruna RO-Crate export completeness report")),
        (about_key, json!({"@id": "./"})),
    ])));
    Ok(())
}

fn property_key(
    object: &serde_json::Map<String, JsonValue>,
    keywords: &JsonLdKeywords,
    values: &[&str],
    compact: &str,
    absolute: &str,
) -> String {
    object
        .keys()
        .find(|key| keywords.expands_to(key, values))
        .cloned()
        .unwrap_or_else(|| safe_term(keywords, compact, values, absolute))
}

fn safe_term(keywords: &JsonLdKeywords, compact: &str, values: &[&str], absolute: &str) -> String {
    if keywords.term_matches(compact, values) {
        compact.to_string()
    } else {
        absolute.to_string()
    }
}

fn precheck_size(
    spec: &ExportRoCrateSpec,
    metadata: &[u8],
    report: Option<&[u8]>,
    opened: &[ProbedEntry],
    entities: &[ExportEntity],
) -> Result<(), ExportFailure> {
    let mut size = (metadata.len() as u64)
        .checked_add(256 + 2 * METADATA_PATH.len() as u64)
        .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
    let mut entries = 1u64;
    if let Some(report) = report {
        size = size
            .checked_add(report.len() as u64)
            .and_then(|size| size.checked_add(256 + 2 * REPORT_PATH.len() as u64))
            .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
        entries += 1;
    }
    for entry in opened {
        let path = entities
            .get(entry.entity_index)
            .and_then(|entity| entity.zip_path.as_deref())
            .ok_or_else(|| ExportFailure::Permanent("planned ZIP path is missing".to_string()))?;
        size = size
            .checked_add(entry.size)
            .and_then(|size| size.checked_add(256 + 2 * path.len() as u64))
            .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
        entries += 1;
    }
    size = size
        .checked_add(256)
        .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
    if entries > spec.limits.max_entries.saturating_add(2) {
        return Err(ExportFailure::Permanent(format!(
            "export has more than {} payload entries",
            spec.limits.max_entries
        )));
    }
    if size > spec.limits.export_artifact_bytes {
        return Err(ExportFailure::Permanent(format!(
            "planned ZIP exceeds the {} byte artifact limit",
            spec.limits.export_artifact_bytes
        )));
    }
    Ok(())
}

async fn assemble_export(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &ExportCheckpoint,
    opened: Vec<ProbedEntry>,
) -> Result<ArtifactRef, ExportFailure> {
    let metadata = checkpoint
        .rewritten_jsonld
        .clone()
        .ok_or_else(|| ExportFailure::Permanent("rewritten metadata is missing".to_string()))?;
    let mut entries = Vec::with_capacity(opened.len());
    let source_spec = std::sync::Arc::new(spec.clone());
    for entry in opened {
        let entity = &checkpoint.entities[entry.entity_index];
        let path = entity
            .zip_path
            .clone()
            .ok_or_else(|| ExportFailure::Permanent("planned ZIP path is missing".to_string()))?;
        let mut candidate = entity
            .candidates
            .get(entry.candidate_index)
            .cloned()
            .ok_or_else(|| {
                ExportFailure::Permanent("planned export candidate is missing".to_string())
            })?;
        candidate.expected_blake3 = Some(entry.hash);
        entries.push(PlannedEntry {
            entity_index: entry.entity_index,
            candidate_index: entry.candidate_index,
            path,
            source: PlannedSource::Candidate {
                driver: ctx.driver.clone(),
                spec: source_spec.clone(),
                candidate,
            },
            expected_blake3: entry.hash,
        });
    }
    let report = checkpoint.report_json.clone();
    let Some(blob_handle) = ctx.driver.blob_handle.as_ref() else {
        return Err(ExportFailure::Retryable(
            "blob handle unavailable".to_string(),
        ));
    };
    let (writer, reader) = tokio::io::duplex(128 * 1024);
    let cancel = ctx.cancel.clone();
    let shutdown = ctx.shutdown.clone();
    let writer_task = tokio::spawn(async move {
        write_archive(writer, metadata, entries, report, cancel, shutdown).await
    });
    let event = blob_handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace: ctx.job_id.0,
            name: "rocrate.zip".to_string(),
            created_by: spec.auth_context.user_id,
            max_bytes: Some(spec.limits.export_artifact_bytes),
            blob: BackendStream::new(tokio_util::io::ReaderStream::new(reader)),
        })
        .await;
    let write_result = match writer_task.await {
        Ok(result) => result,
        Err(error) => Err(ExportFailure::Retryable(error.to_string())),
    };
    match (event, write_result) {
        (
            Event::Blob(BlobEvent::HiddenSpooled {
                location,
                blake3,
                size,
            }),
            Ok(()),
        ) => Ok(ArtifactRef {
            location,
            blake3,
            size,
            expires_at_ms: unix_timestamp_millis()
                .saturating_add(spec.limits.artifact_retention_ms),
        }),
        (Event::Blob(BlobEvent::HiddenSpooled { location, .. }), Err(error)) => {
            let _ = delete_hidden(&ctx.driver, &location).await;
            Err(error)
        }
        (Event::Blob(BlobEvent::Error(BlobError::SizeLimitExceeded { limit })), _) => {
            Err(ExportFailure::Permanent(format!(
                "assembled ZIP exceeds the {limit} byte artifact limit"
            )))
        }
        (Event::Blob(BlobEvent::Error(_)), Err(ExportFailure::Cancelled)) => {
            Err(ExportFailure::Cancelled)
        }
        (Event::Blob(BlobEvent::Error(_)), Err(ExportFailure::Interrupted)) => {
            Err(ExportFailure::Interrupted)
        }
        (Event::Blob(BlobEvent::Error(error)), _) => {
            Err(ExportFailure::Retryable(error.to_string()))
        }
        (event, _) => Err(ExportFailure::Retryable(format!(
            "unexpected hidden artifact event: {event:?}"
        ))),
    }
}

async fn write_archive(
    writer: tokio::io::DuplexStream,
    metadata: Vec<u8>,
    mut entries: Vec<PlannedEntry>,
    report: Option<Vec<u8>>,
    cancel: tokio_util::sync::CancellationToken,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), ExportFailure> {
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let mut archive = async_zip::base::write::ZipFileWriter::with_tokio(writer);
    archive
        .write_entry_whole(zip_entry(METADATA_PATH), &metadata)
        .await
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    for entry in entries {
        let PlannedEntry {
            entity_index,
            candidate_index,
            path,
            source,
            expected_blake3,
        } = entry;
        let opened = match source {
            PlannedSource::Candidate {
                driver,
                spec,
                candidate,
            } => open_candidate(&driver, &spec, &candidate, false).await?,
            #[cfg(test)]
            PlannedSource::Ready(blob) => CandidateOpen::Opened(BaoReadOutput::Stream {
                blob,
                size: 0,
                blake3: expected_blake3,
            }),
        };
        let mut blob = match opened {
            CandidateOpen::Opened(BaoReadOutput::Stream { blob, blake3, .. })
                if blake3 == expected_blake3 =>
            {
                blob
            }
            CandidateOpen::Opened(BaoReadOutput::Stream { .. })
            | CandidateOpen::Status(OpenStatus::Corrupt) => {
                return Err(ExportFailure::Candidate {
                    entity_index,
                    candidate_index,
                    status: OpenStatus::Corrupt,
                    message: format!("payload integrity check failed for `{path}`"),
                });
            }
            CandidateOpen::Opened(BaoReadOutput::Metadata { .. }) => {
                return Err(ExportFailure::Retryable(
                    "source open unexpectedly returned metadata".to_string(),
                ));
            }
            CandidateOpen::Status(status) => {
                return Err(ExportFailure::Candidate {
                    entity_index,
                    candidate_index,
                    status,
                    message: format!("payload source became unavailable for `{path}`"),
                });
            }
        };
        let mut writer = archive
            .write_entry_stream(zip_entry(&path))
            .await
            .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
        let mut hasher = blake3::Hasher::new();
        loop {
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ExportFailure::Cancelled),
                _ = shutdown.cancelled() => return Err(ExportFailure::Interrupted),
                next = blob.next() => next,
            };
            match next {
                Some(Ok(bytes)) => {
                    hasher.update(&bytes);
                    writer
                        .write_all(&bytes)
                        .await
                        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
                }
                Some(Err(error)) => {
                    return Err(ExportFailure::Candidate {
                        entity_index,
                        candidate_index,
                        status: stream_status(&error),
                        message: error.to_string(),
                    });
                }
                None => break,
            }
        }
        if hasher.finalize().as_bytes() != &expected_blake3 {
            return Err(ExportFailure::Candidate {
                entity_index,
                candidate_index,
                status: OpenStatus::Corrupt,
                message: format!("payload integrity check failed for `{path}`"),
            });
        }
        writer
            .close()
            .await
            .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    }
    if let Some(report) = report {
        archive
            .write_entry_whole(zip_entry(REPORT_PATH), &report)
            .await
            .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    }
    archive
        .close()
        .await
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    Ok(())
}

fn zip_entry(path: &str) -> ZipEntryBuilder {
    ZipEntryBuilder::new(path.to_string().into(), Compression::Stored)
}

fn stream_status(error: &StreamError) -> OpenStatus {
    if matches!(
        error.0.downcast_ref::<BlobError>(),
        Some(BlobError::IntegrityCheckFailed(_))
    ) {
        OpenStatus::Corrupt
    } else {
        OpenStatus::Offline
    }
}

async fn publish_export(ctx: &JobContext, checkpoint: &ExportCheckpoint) -> JobRunOutcome {
    let Some(artifact) = checkpoint.artifact.clone() else {
        return permanent("export artifact is missing");
    };
    for row in &checkpoint.report {
        if ctx.cancel.is_cancelled() {
            return JobRunOutcome::Cancelled;
        }
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }
        if let Err(error) = put_job_entry(
            &ctx.driver.storage_handle,
            ctx.job_id,
            ctx.claim_token,
            row.entry_key.as_bytes(),
            row,
        )
        .await
        {
            return retryable(error.to_string());
        }
    }
    let (included, omitted) = report_counts(&checkpoint.report);
    JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(ExportRoCrateResult {
        artifact: Some(artifact),
        included,
        omitted,
        report_digest: [0; 32],
    }))
}

fn report_counts(rows: &[ExportReportRow]) -> (u64, ExportOmissionCounts) {
    let mut included = 0u64;
    let mut omitted = ExportOmissionCounts::default();
    for row in rows {
        match row.code {
            ReasonCode::Included => included = included.saturating_add(1),
            ReasonCode::External => omitted.external = omitted.external.saturating_add(1),
            ReasonCode::Denied => omitted.denied = omitted.denied.saturating_add(1),
            ReasonCode::Missing => omitted.missing = omitted.missing.saturating_add(1),
            ReasonCode::Offline => omitted.offline = omitted.offline.saturating_add(1),
            ReasonCode::Unsupported => omitted.unsupported = omitted.unsupported.saturating_add(1),
            _ => {}
        }
    }
    (included, omitted)
}

async fn discard_artifact(ctx: &JobContext, checkpoint: &mut ExportCheckpoint, persist: bool) {
    if let Some(artifact) = checkpoint.artifact.as_ref() {
        let _ = delete_hidden(&ctx.driver, &artifact.location).await;
    }
    checkpoint.artifact = None;
    checkpoint.refs.hidden_locations.clear();
    if persist {
        let _ = persist_checkpoint(ctx, checkpoint).await;
    }
}

async fn read_export_checkpoint(
    ctx: &JobContext,
    job_id: JobId,
) -> Result<Option<ExportCheckpoint>, String> {
    match ctx
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: ROCRATE_JOB_STATE_KEYSPACE.to_string(),
            key: ByteView::from(job_id.to_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(value.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        event => Err(format!("unexpected export checkpoint event: {event:?}")),
    }
}

async fn persist_checkpoint(ctx: &JobContext, checkpoint: &ExportCheckpoint) -> Result<(), String> {
    let value = postcard::to_allocvec(checkpoint).map_err(|error| error.to_string())?;
    put_rocrate_checkpoint(
        &ctx.driver.storage_handle,
        ctx.job_id,
        ctx.claim_token,
        Value::from(value),
    )
    .await
    .map_err(|error| error.to_string())
}

fn map_crate_error(error: craqle::RoCrateError) -> ExportFailure {
    match error {
        craqle::RoCrateError::Update(craqle::UpdateError::ValidationFailed(violations)) => {
            ExportFailure::Validation(
                violations
                    .into_iter()
                    .map(|violation| MetadataValidationViolation {
                        code: violation.code.to_string(),
                        message: violation.message,
                        pointer: violation.pointer,
                        entity_id: violation.entity_id,
                    })
                    .collect(),
            )
        }
        error => ExportFailure::Permanent(error.to_string()),
    }
}

async fn finish_export(
    ctx: &JobContext,
    checkpoint: &mut ExportCheckpoint,
    error: ExportFailure,
) -> JobRunOutcome {
    discard_artifact(ctx, checkpoint, false).await;
    if let ExportFailure::Validation(violations) = &error
        && let Err(message) = write_validation_rows(ctx, violations).await
    {
        return retryable(message);
    }
    failure_outcome(error)
}

async fn write_validation_rows(
    ctx: &JobContext,
    violations: &[MetadataValidationViolation],
) -> Result<(), String> {
    for (index, violation) in violations.iter().enumerate() {
        let row = ExportReportRow {
            entry_key: format!("validation/{index:08}"),
            code: if violation.code == "unsupported_crate_version" {
                ReasonCode::UnsupportedCrateVersion
            } else {
                ReasonCode::Failed
            },
            message: Some(violation.message.clone()),
            detail: ExportReportDetail {
                entity_id: violation.entity_id.clone().unwrap_or_default(),
                zip_path: None,
                source: None,
                resolved_version: None,
                validation: Some(violation.clone()),
            },
        };
        put_job_entry(
            &ctx.driver.storage_handle,
            ctx.job_id,
            ctx.claim_token,
            row.entry_key.as_bytes(),
            &row,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn failure_outcome(error: ExportFailure) -> JobRunOutcome {
    match error {
        ExportFailure::Permanent(message) => permanent(message),
        ExportFailure::Retryable(message) => retryable(message),
        ExportFailure::Validation(violations) => permanent(validation_message(&violations)),
        ExportFailure::Candidate { message, .. } => retryable(message),
        ExportFailure::Cancelled => JobRunOutcome::Cancelled,
        ExportFailure::Interrupted => JobRunOutcome::Interrupted,
    }
}

fn validation_message(violations: &[MetadataValidationViolation]) -> String {
    violations
        .iter()
        .map(|violation| {
            format!(
                "{} at {}: {}",
                violation.code, violation.pointer, violation.message
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn retryable(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use tokio::io::AsyncReadExt;

    fn file_entity(id: &str, local_path: Option<&str>) -> JsonValue {
        let mut entity = json!({
            "@id": id,
            "@type": "File",
            "contentUrl": format!(
                "{}{}",
                aruna_core::structs::ARUNA_DATA_PREFIX,
                "11".repeat(32)
            ),
        });
        if let Some(local_path) = local_path {
            entity["localPath"] = json!(local_path);
        }
        entity
    }

    fn byte_stream(bytes: &'static [u8]) -> BackendStream<Result<Bytes, StreamError>> {
        BackendStream::new(futures_util::stream::iter([Ok::<Bytes, std::io::Error>(
            Bytes::from_static(bytes),
        )]))
    }

    fn recognized_entities(
        document: &JsonValue,
        realm_id: RealmId,
    ) -> Result<Vec<ExportEntity>, ExportFailure> {
        let jsonld = serde_json::to_string(document).unwrap();
        let canonical = craqle::canonicalize_jsonld(&jsonld).unwrap();
        recognize_entities(document, &canonical.nquads, realm_id)
    }

    async fn sample_archive() -> Vec<u8> {
        let (writer, mut reader) = tokio::io::duplex(4096);
        let task = tokio::spawn(write_archive(
            writer,
            b"metadata".to_vec(),
            vec![
                PlannedEntry {
                    entity_index: 0,
                    candidate_index: 0,
                    path: "data/b".to_string(),
                    source: PlannedSource::Ready(byte_stream(b"b")),
                    expected_blake3: *blake3::hash(b"b").as_bytes(),
                },
                PlannedEntry {
                    entity_index: 1,
                    candidate_index: 0,
                    path: "data/a".to_string(),
                    source: PlannedSource::Ready(byte_stream(b"a")),
                    expected_blake3: *blake3::hash(b"a").as_bytes(),
                },
            ],
            Some(b"report".to_vec()),
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new(),
        ));
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        task.await.unwrap().unwrap();
        bytes
    }

    #[test]
    fn recognizes_identifiers() {
        let realm_id = RealmId::from_bytes([2; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let version = Ulid::from_bytes([4; 16]);
        let arn = VersionedObjectArn::new(realm_id, node_id, "bucket", "a b", version).unwrap();
        let foreign = VersionedObjectArn::new(
            RealmId::from_bytes([9; 32]),
            node_id,
            "bucket",
            "foreign",
            version,
        )
        .unwrap();
        let document = json!({
            "@graph": [
                file_entity(&arn.to_w3id(), Some("a b")),
                file_entity(&foreign.to_w3id(), None),
                {"@id": foreign.to_string(), "@type": "File"},
                {"@id": "https://example.org/external", "@type": "File"},
            ]
        });

        let entities = recognized_entities(&document, realm_id).unwrap();

        assert_eq!(entities[0].exact.as_ref(), Some(&arn));
        assert_eq!(entities[0].hash, Some([0x11; 32]));
        assert_eq!(entities[1].omission, None);
        assert_eq!(entities[2].omission, Some(ReasonCode::Unsupported));
        assert_eq!(entities[3].omission, Some(ReasonCode::External));
    }

    #[test]
    fn recognizes_context_aliases() {
        let realm_id = RealmId::from_bytes([2; 32]);
        let document = json!({
            "@context": [
                "https://w3id.org/ro/crate/1.2/context",
                {
                    "graphItems": "@graph",
                    "idAlias": "@id",
                    "typeAlias": "@type",
                    "downloadAlias": "http://schema.org/contentUrl",
                    "pathAlias": LOCAL_PATH_IRI
                }
            ],
            "graphItems": [{
                "idAlias": "data/a.txt",
                "typeAlias": "File",
                "downloadAlias": format!(
                    "{}{}",
                    aruna_core::structs::ARUNA_DATA_PREFIX,
                    "11".repeat(32)
                ),
                "pathAlias": "data/a.txt"
            }]
        });

        let entities = recognized_entities(&document, realm_id).unwrap();

        assert_eq!(entities[0].hash, Some([0x11; 32]));
        assert_eq!(entities[0].local_path.as_deref(), Some("data/a.txt"));
    }

    #[test]
    fn reports_keyword_aliases() {
        let mut document = json!({
            "@context": [
                "https://w3id.org/ro/crate/1.2/context",
                {"graphItems": "@graph", "idAlias": "@id"}
            ],
            "graphItems": [
                {"idAlias": "./", "@type": "Dataset"},
                {
                    "idAlias": METADATA_PATH,
                    "@type": "CreativeWork",
                    "about": {"idAlias": "./"}
                }
            ]
        });

        add_report(&mut document).unwrap();

        assert_eq!(
            document["graphItems"][0]["subjectOf"]["@id"],
            JsonValue::String("#aruna-export-report".to_string())
        );
        assert_eq!(document["graphItems"][2]["@id"], REPORT_PATH);
    }

    #[test]
    fn reports_context_overrides() {
        let mut document = json!({
            "@context": [
                "https://w3id.org/ro/crate/1.2/context",
                {
                    "subjectOf": "https://example.test/subject",
                    "hasPart": "https://example.test/part",
                    "about": "https://example.test/about",
                    "encodingFormat": "https://example.test/format",
                    "name": "https://example.test/name"
                }
            ],
            "@graph": [
                {
                    "@id": "./",
                    "@type": "Dataset",
                    "hasPart": "preserved"
                },
                {
                    "@id": METADATA_PATH,
                    "@type": "CreativeWork",
                    "http://schema.org/about": {"@id": "./"}
                }
            ]
        });

        add_report(&mut document).unwrap();

        assert_eq!(document["@graph"][0]["hasPart"], "preserved");
        assert_eq!(
            document["@graph"][0][SCHEMA_SUBJECT_HTTPS_IRI]["@id"],
            "#aruna-export-report"
        );
        assert_eq!(
            document["@graph"][0][SCHEMA_HAS_PART_HTTPS_IRI]["@id"],
            REPORT_PATH
        );
        assert_eq!(
            document["@graph"][2][SCHEMA_ENCODING_HTTPS_IRI],
            "application/json"
        );
    }

    #[test]
    fn scan_ignores_aliases() {
        let document = json!({
            "@context": {"node": "@id"},
            "@graph": [
                {"node": "old"},
                {"name": "old"}
            ]
        });
        let found = scan_unrewritten(
            &document,
            &BTreeMap::from([("old".to_string(), "new".to_string())]),
        );

        assert_eq!(found, BTreeSet::from(["old".to_string()]));
    }

    #[test]
    fn rejects_report_collision() {
        let mut document = json!({
            "@graph": [
                {"@id": "./", "@type": "Dataset"},
                {"@id": REPORT_PATH, "@type": "File"}
            ]
        });

        assert!(matches!(
            add_report(&mut document),
            Err(ExportFailure::Permanent(message))
                if message.contains("reserved export report")
        ));
    }

    #[test]
    fn plans_ordered_paths() {
        assert_eq!(safe_zip_path("./a/b.txt").as_deref(), Some("a/b.txt"));
        assert_eq!(safe_zip_path("../escape"), None);
        assert_eq!(safe_zip_path("a%2fb"), None);
        assert_ne!(
            synthesized_path([7; 32], "one"),
            synthesized_path([7; 32], "two")
        );
    }

    #[test]
    fn rewrites_report_links() {
        let mut document = json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {
                    "@id": "./",
                    "@type": "Dataset",
                    "name": "test",
                    "description": "test crate",
                    "datePublished": "2026-07-23",
                    "hasPart": {"@id": "old"}
                },
                {
                    "@id": METADATA_PATH,
                    "@type": "CreativeWork",
                    "about": {"@id": "./"},
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}
                },
                {"@id": "old", "@type": "File"}
            ]
        });
        rewrite_ids(
            &mut document,
            &BTreeMap::from([("old".to_string(), "data/file".to_string())]),
        );
        add_report(&mut document).unwrap();

        assert_eq!(
            document["@graph"][0]["hasPart"]["@id"],
            JsonValue::String("data/file".to_string())
        );
        assert_eq!(
            document["@graph"][0]["subjectOf"]["@id"],
            JsonValue::String("#aruna-export-report".to_string())
        );
        craqle::validate_rocrate_jsonld(&document.to_string()).unwrap();
    }

    #[test]
    fn counts_omission_rows() {
        let realm_id = RealmId::from_bytes([2; 32]);
        let document = json!({
            "@graph": [
                {"@id": "https://example.org/external", "@type": "File"},
                file_entity("denied", None),
                file_entity("included", None),
            ]
        });
        let mut entities = recognized_entities(&document, realm_id).unwrap();
        entities[1].omission = Some(ReasonCode::Denied);
        let rows = build_rows(&entities, &BTreeSet::new());

        let (included, omitted) = report_counts(&rows);

        assert_eq!(included, 1);
        assert_eq!(omitted.external, 1);
        assert_eq!(omitted.denied, 1);
        assert_eq!(omitted.missing, 0);
    }

    #[test]
    fn checkpoint_prefix_decodes() {
        let bytes = postcard::to_allocvec(&ExportCheckpoint::default()).unwrap();
        let (refs, remaining) = postcard::take_from_bytes::<RoCrateCheckpointRefs>(&bytes).unwrap();

        assert_eq!(refs, RoCrateCheckpointRefs::default());
        assert!(!remaining.is_empty());
    }

    #[tokio::test]
    async fn orders_zip_entries() {
        let bytes = sample_archive().await;
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        let names = (0..archive.len())
            .map(|index| archive.by_index(index).unwrap().name().to_string())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                METADATA_PATH.to_string(),
                "data/a".to_string(),
                "data/b".to_string(),
                REPORT_PATH.to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn archives_are_deterministic() {
        assert_eq!(sample_archive().await, sample_archive().await);
    }

    #[tokio::test]
    async fn signals_corrupt_candidate() {
        let (writer, mut reader) = tokio::io::duplex(4096);
        let task = tokio::spawn(write_archive(
            writer,
            b"metadata".to_vec(),
            vec![PlannedEntry {
                entity_index: 4,
                candidate_index: 2,
                path: "data/corrupt".to_string(),
                source: PlannedSource::Ready(byte_stream(b"wrong")),
                expected_blake3: [0; 32],
            }],
            None,
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new(),
        ));
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();

        assert!(matches!(
            task.await.unwrap(),
            Err(ExportFailure::Candidate {
                entity_index: 4,
                candidate_index: 2,
                status: OpenStatus::Corrupt,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn cancels_streaming_archive() {
        let (writer, mut reader) = tokio::io::duplex(4096);
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let task = tokio::spawn(write_archive(
            writer,
            b"metadata".to_vec(),
            vec![PlannedEntry {
                entity_index: 0,
                candidate_index: 0,
                path: "data/pending".to_string(),
                source: PlannedSource::Ready(BackendStream::new(futures_util::stream::pending::<
                    Result<Bytes, std::io::Error>,
                >())),
                expected_blake3: [0; 32],
            }],
            None,
            cancel,
            tokio_util::sync::CancellationToken::new(),
        ));
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();

        assert!(matches!(task.await.unwrap(), Err(ExportFailure::Cancelled)));
    }

    #[tokio::test]
    async fn zip64_interoperates() {
        let mut writer = async_zip::base::write::ZipFileWriter::new(Vec::<u8>::new());
        for index in 0..70_000u32 {
            writer
                .write_entry_whole(zip_entry(&format!("data/{index:08}")), &[])
                .await
                .unwrap();
        }
        let bytes = writer.close().await.unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&bytes)).unwrap();
        assert_eq!(archive.len(), 70_000);
        let mut last = String::new();
        archive
            .by_index(69_999)
            .unwrap()
            .read_to_string(&mut last)
            .unwrap();

        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(&bytes).unwrap();
        file.as_file_mut().flush().unwrap();
        if let Ok(status) = std::process::Command::new("unzip")
            .arg("-t")
            .arg(file.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
        {
            assert!(status.success());
        }
    }
}
