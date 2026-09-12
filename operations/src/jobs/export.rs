use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Component, Path};

use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, BlobError};
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
    ManagedCopyKey, Permission, RealmId, ReasonCode, RoCrateCheckpointRefs, VersionKey,
    VersionedObjectArn, W3idDataIdentifier, ensure_confined_path, object_permission_path,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, NodeId, TxnId, Value};
use async_zip::{Compression, ZipDateTime, ZipDateTimeBuilder, ZipEntryBuilder};
#[cfg(test)]
use bytes::Bytes;
use byteview::ByteView;
use chrono::{DateTime, Datelike, Timelike, Utc};
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
use super::rocrate_jsonld::{
    JsonLdKeywords, RDF_TYPE_IRI, SCHEMA_MEDIA_HTTPS_IRI, SCHEMA_MEDIA_IRI, is_file_type,
};
use super::store::{put_job_entry, put_state, read_state};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::permission_rules::{
    PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
};
use crate::auth::request_policy::{
    PolicyEnforcementError, PolicyEvaluator, PolicyRequestExtras, policy_request_with,
};
use crate::blob::hidden::delete_hidden;
use crate::blob::holders::GetBlobHoldersOperation;
use crate::blob::managed_copy::{
    CopyRequest, serve_reads, split_serve_reads, validate_registration,
};
use crate::blob::permission_paths::{MAX_HASH_ALIASES, ResolveBlobPermissionPathsOperation};
use crate::driver::{DriverContext, drive};
use crate::metadata::MetadataAuthToken;
use crate::metadata::api::{
    ExportMetadataRoCrateRequest, ExportMetadataRoCrateResult, MetadataApiError,
    MetadataRoCrateExportView,
};
use crate::metadata::forward::export_rocrate_routed;
use crate::replication::bao_read::{BaoReadError, BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};

mod archive;
pub(crate) use archive::*;

const METADATA_PATH: &str = "ro-crate-metadata.json";
const REPORT_PATH: &str = "aruna-export-report.json";
const REMOTE_ATTEMPTS: usize = 8;
const MAX_LOCAL_CANDIDATES: usize = REMOTE_ATTEMPTS / 2;
const JSONLD_BASE_IRI: &str = "https://craqle.invalid/";
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

/// The bucket and key an entity was authored against, used to lay the archive
/// out like the source prefix instead of by content hash.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StorageKey {
    bucket: String,
    key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ExportEntity {
    entity_id: String,
    local_path: Option<String>,
    storage_key: Option<StorageKey>,
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
    Local {
        location: BackendLocation,
        group_id: GroupId,
        permission_path: String,
        node_id: NodeId,
        bucket: String,
        key: String,
    },
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
    modified_ms: u64,
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

pub(crate) struct EntityIdentity {
    pub(crate) exact: Option<VersionedObjectArn>,
    pub(crate) hash: Option<[u8; 32]>,
    pub(crate) hash_realm: Option<RealmId>,
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
    let mut policies = BTreeMap::<GroupId, std::sync::Arc<PolicyEvaluator>>::new();
    let mut permission_rules = BTreeMap::<GroupId, PermissionRules>::new();
    let mut alias_paths = BTreeSet::<(GroupId, String)>::new();
    let mut alias_keys = BTreeSet::<([u8; 32], String, Ulid)>::new();
    let mut alias_cache = BTreeMap::<[u8; 32], Vec<HashPathIndexKey>>::new();
    let mut resolved_aliases = BTreeMap::<[u8; 32], (Vec<ExportCandidate>, bool)>::new();
    let mut authorized_aliases = BTreeMap::<(GroupId, String), bool>::new();
    for entity in &checkpoint.entities {
        for candidate in &entity.candidates {
            if candidate.report_source != ExportReportSource::Hash {
                continue;
            }
            if let CandidateSource::Local {
                group_id,
                permission_path,
                ..
            } = &candidate.source
            {
                alias_paths.insert((*group_id, permission_path.clone()));
                if let (Some(hash), Some(version)) =
                    (candidate.expected_blake3, candidate.resolved_version)
                {
                    alias_keys.insert((hash, permission_path.clone(), version));
                }
            }
        }
    }

    loop {
        if ctx.cancel.is_cancelled() {
            discard_artifact(ctx, &mut checkpoint, true).await;
            return JobRunOutcome::Cancelled;
        }
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }
        if alias_paths.len() > MAX_HASH_ALIASES || alias_keys.len() > MAX_HASH_ALIASES {
            return finish_export(
                ctx,
                &mut checkpoint,
                ExportFailure::Retryable("export alias limit exceeded".to_string()),
            )
            .await;
        }
        if let Err(error) = load_candidate_policies(ctx, spec, &checkpoint, &mut policies).await {
            return finish_export(ctx, &mut checkpoint, error).await;
        }

        let result = match checkpoint.phase {
            ExportPhase::Snapshot => snapshot_export(ctx, spec, &mut checkpoint).await,
            ExportPhase::Resolve => {
                resolve_entries(
                    ctx,
                    spec,
                    &mut checkpoint,
                    &mut policies,
                    &mut permission_rules,
                    &mut alias_paths,
                    &mut alias_keys,
                    &mut alias_cache,
                    &mut resolved_aliases,
                    &mut authorized_aliases,
                )
                .await
            }
            ExportPhase::Plan => {
                match Box::pin(probe_sources_checked(
                    ctx,
                    spec,
                    &mut checkpoint,
                    &candidate_failures,
                    &mut policies,
                    &mut permission_rules,
                    &mut alias_paths,
                    &mut alias_keys,
                    &mut alias_cache,
                    &mut resolved_aliases,
                    &mut authorized_aliases,
                ))
                .await
                {
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
                    match Box::pin(probe_sources_checked(
                        ctx,
                        spec,
                        &mut checkpoint,
                        &candidate_failures,
                        &mut policies,
                        &mut permission_rules,
                        &mut alias_paths,
                        &mut alias_keys,
                        &mut alias_cache,
                        &mut resolved_aliases,
                        &mut authorized_aliases,
                    ))
                    .await
                    {
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
                match Box::pin(assemble_export(ctx, spec, &checkpoint, entries, &policies)).await {
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
    // Route the raw revision from a document holder; a job on a job-control bucket
    // rarely holds the document's bucket. The holder re-checks READ for this peer.
    let export = export_rocrate_routed(
        &ctx.driver,
        spec.auth_context.realm_id,
        ExportMetadataRoCrateRequest {
            document_id: spec.document_id,
            auth: Some(spec.auth_context.clone()),
            view: MetadataRoCrateExportView::Raw,
            limit: None,
            offset: None,
            after: None,
        },
        Some(MetadataAuthToken::internal(spec.auth_context.clone())),
        spec.limits.metadata_bytes,
    )
    .await
    .map_err(snapshot_read_failure)?;
    let ExportMetadataRoCrateResult::Raw { raw, .. } = export else {
        return Err(ExportFailure::Permanent(
            "raw export returned an unexpected view".to_string(),
        ));
    };
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

#[allow(clippy::too_many_arguments)]
async fn resolve_entries(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    policies: &mut BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>,
    permission_rules: &mut BTreeMap<GroupId, PermissionRules>,
    alias_paths: &mut BTreeSet<(GroupId, String)>,
    alias_keys: &mut BTreeSet<([u8; 32], String, Ulid)>,
    alias_cache: &mut BTreeMap<[u8; 32], Vec<HashPathIndexKey>>,
    resolved_aliases: &mut BTreeMap<[u8; 32], (Vec<ExportCandidate>, bool)>,
    authorized_aliases: &mut BTreeMap<(GroupId, String), bool>,
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
            let unavailable = extend_hash_candidates(
                ctx,
                spec,
                hash,
                exact_version,
                &mut candidates,
                &mut denied,
                policies,
                permission_rules,
                alias_paths,
                alias_keys,
                alias_cache,
                resolved_aliases,
                authorized_aliases,
            )
            .await?;
            if unavailable && candidates.is_empty() {
                checkpoint.entities[index].omission = Some(ReasonCode::Offline);
                checkpoint.entities[index].message =
                    Some("blob holder discovery is unavailable".to_string());
                ctx.progress.advance(1);
                continue;
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

#[allow(clippy::too_many_arguments)]
async fn extend_hash_candidates(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    hash: [u8; 32],
    resolved_version: Option<Ulid>,
    candidates: &mut Vec<ExportCandidate>,
    denied: &mut bool,
    policies: &mut BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>,
    permission_rules: &mut BTreeMap<GroupId, PermissionRules>,
    alias_paths: &mut BTreeSet<(GroupId, String)>,
    alias_keys: &mut BTreeSet<([u8; 32], String, Ulid)>,
    alias_cache: &mut BTreeMap<[u8; 32], Vec<HashPathIndexKey>>,
    resolved_aliases: &mut BTreeMap<[u8; 32], (Vec<ExportCandidate>, bool)>,
    authorized_aliases: &mut BTreeMap<(GroupId, String), bool>,
) -> Result<bool, ExportFailure> {
    if let Some((cached, cached_denied)) = resolved_aliases.get(&hash) {
        merge_candidates(candidates, cached, MAX_LOCAL_CANDIDATES);
        *denied |= *cached_denied;
    } else {
        if !alias_cache.contains_key(&hash) {
            let aliases = drive(ResolveBlobPermissionPathsOperation::new(hash), &ctx.driver)
                .await
                .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
            cache_aliases(alias_cache, hash, aliases)?;
        }
        let aliases = alias_cache
            .get(&hash)
            .ok_or_else(|| ExportFailure::Retryable("alias cache unavailable".to_string()))?;
        let distinct =
            collect_aliases(aliases, spec.auth_context.realm_id, alias_paths, alias_keys)?;
        let groups = distinct
            .keys()
            .map(|(group_id, _)| *group_id)
            .collect::<BTreeSet<_>>();
        load_rules(ctx, spec, permission_rules, groups).await?;
        // Only a group whose rules already allow an alias needs its object
        // policy; a foreign group is denied before its policy is consulted.
        let allowed_groups = distinct
            .keys()
            .filter(|(group_id, path)| {
                permission_rules
                    .get(group_id)
                    .is_some_and(|rules| rules.allows(path, &Permission::READ))
            })
            .map(|(group_id, _)| *group_id)
            .collect::<BTreeSet<_>>();
        load_policies(ctx, spec, policies, allowed_groups).await?;
        let mut resolved = Vec::new();
        let mut alias_denied = false;
        for key in distinct.keys() {
            if !authorized_aliases.contains_key(key) {
                let allowed = alias_allowed(spec, key.0, &key.1, permission_rules, policies)?;
                authorized_aliases.insert(key.clone(), allowed);
            }
        }
        for aliases in distinct.values() {
            for alias in aliases {
                match resolve_alias(ctx, spec, alias, authorized_aliases).await? {
                    ResolveResult::Candidate(candidate) => {
                        if resolved.len() < MAX_LOCAL_CANDIDATES {
                            resolved.push(candidate);
                        }
                    }
                    ResolveResult::Denied => alias_denied = true,
                    ResolveResult::Missing { .. } => {}
                }
            }
        }
        resolved_aliases.insert(hash, (resolved, alias_denied));
        let (cached, cached_denied) = resolved_aliases
            .get(&hash)
            .ok_or_else(|| ExportFailure::Retryable("alias cache unavailable".to_string()))?;
        merge_candidates(candidates, cached, MAX_LOCAL_CANDIDATES);
        *denied |= *cached_denied;
    }

    let holders = match drive(
        GetBlobHoldersOperation::new(hash, spec.auth_context.realm_id, ctx.owner_node_id),
        &ctx.driver,
    )
    .await
    {
        Ok(holders) => holders,
        Err(_) => return Ok(true),
    };
    let mut holder_candidates = Vec::new();
    let remote_count = candidates
        .iter()
        .filter(|candidate| matches!(candidate.source, CandidateSource::RemoteHash { .. }))
        .count();
    let holder_limit = REMOTE_ATTEMPTS.saturating_sub(remote_count);
    for node_id in holders.into_iter().take(holder_limit) {
        holder_candidates.push(ExportCandidate {
            source: CandidateSource::RemoteHash { node_id, hash },
            report_source: ExportReportSource::Hash,
            resolved_version,
            expected_blake3: Some(hash),
        });
    }
    merge_candidates(candidates, &holder_candidates, REMOTE_ATTEMPTS);
    Ok(false)
}

fn cache_aliases(
    alias_cache: &mut BTreeMap<[u8; 32], Vec<HashPathIndexKey>>,
    hash: [u8; 32],
    aliases: Vec<HashPathIndexKey>,
) -> Result<(), ExportFailure> {
    if aliases.len() > MAX_HASH_ALIASES {
        return Err(ExportFailure::Retryable(
            "hash alias limit exceeded".to_string(),
        ));
    }
    let cached = alias_cache
        .values()
        .try_fold(0usize, |total, aliases| {
            total.checked_add(aliases.len().max(1))
        })
        .ok_or_else(|| ExportFailure::Retryable("export alias limit exceeded".to_string()))?;
    let cost = aliases.len().max(1);
    if cached
        .checked_add(cost)
        .is_none_or(|total| total > MAX_HASH_ALIASES)
    {
        return Err(ExportFailure::Retryable(
            "export alias limit exceeded".to_string(),
        ));
    }
    alias_cache.insert(hash, aliases);
    Ok(())
}

fn collect_aliases(
    aliases: &[HashPathIndexKey],
    realm_id: RealmId,
    seen: &mut BTreeSet<(GroupId, String)>,
    alias_keys: &mut BTreeSet<([u8; 32], String, Ulid)>,
) -> Result<BTreeMap<(GroupId, String), Vec<HashPathIndexKey>>, ExportFailure> {
    if aliases.len() > MAX_HASH_ALIASES {
        return Err(ExportFailure::Retryable(
            "hash alias limit exceeded".to_string(),
        ));
    }
    let mut distinct = BTreeMap::new();
    for alias in aliases.iter().filter(|alias| alias.realm_id == realm_id) {
        let key = (alias.group_id, alias.permission_path());
        if !alias_keys.insert((alias.blake3_hash, key.1.clone(), alias.version_id)) {
            continue;
        }
        distinct
            .entry(key.clone())
            .or_insert_with(Vec::new)
            .push(alias.clone());
        seen.insert(key);
    }
    if seen.len() > MAX_HASH_ALIASES || alias_keys.len() > MAX_HASH_ALIASES {
        return Err(ExportFailure::Retryable(
            "export alias limit exceeded".to_string(),
        ));
    }
    Ok(distinct)
}

fn merge_candidates(
    target: &mut Vec<ExportCandidate>,
    additions: &[ExportCandidate],
    limit: usize,
) {
    if target.len() >= limit {
        return;
    }
    let mut local = BTreeSet::<(String, Ulid)>::new();
    let mut remote = BTreeSet::<(NodeId, [u8; 32])>::new();
    for candidate in target.iter() {
        match &candidate.source {
            CandidateSource::Local {
                permission_path, ..
            } => {
                if let Some(version) = candidate.resolved_version {
                    local.insert((permission_path.clone(), version));
                }
            }
            CandidateSource::RemoteHash { node_id, hash } => {
                remote.insert((*node_id, *hash));
            }
            CandidateSource::RemoteExact { .. } => {}
        }
    }
    for candidate in additions {
        if target.len() >= limit {
            break;
        }
        let insert = match &candidate.source {
            CandidateSource::Local {
                permission_path, ..
            } => candidate
                .resolved_version
                .is_none_or(|version| local.insert((permission_path.clone(), version))),
            CandidateSource::RemoteHash { node_id, hash } => remote.insert((*node_id, *hash)),
            CandidateSource::RemoteExact { .. } => true,
        };
        if insert {
            target.push(candidate.clone());
        }
    }
}

async fn resolve_exact(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    exact: &VersionedObjectArn,
) -> Result<ResolveResult, ExportFailure> {
    let txn_id = start_read_txn(&ctx.driver).await?;
    let result = resolve_exact_txn(ctx, spec, exact, txn_id).await;
    match result {
        Ok(result) => {
            commit_read_txn(&ctx.driver, txn_id).await?;
            Ok(result)
        }
        Err(error) => {
            abort_read_txn(&ctx.driver, txn_id).await;
            Err(error)
        }
    }
}

async fn resolve_exact_txn(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    exact: &VersionedObjectArn,
    txn_id: TxnId,
) -> Result<ResolveResult, ExportFailure> {
    let Some(bucket) = storage_value(
        &ctx.driver,
        S3_BUCKET_KEYSPACE,
        exact.bucket.as_bytes().to_vec().into(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let bucket = BucketInfo::from_bytes(bucket.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    let permission_path = object_permission_path(
        spec.auth_context.realm_id,
        bucket.group_id,
        exact.node_id,
        &exact.bucket,
        &exact.key,
    );
    let evaluator = load_policy_txn(&ctx.driver, spec, bucket.group_id, txn_id).await?;
    if !check_read_txn(&ctx.driver, spec, &permission_path, &evaluator, txn_id).await? {
        return Ok(ResolveResult::Denied);
    }
    let key = VersionKey::new(exact.bucket.clone(), exact.key.clone(), exact.version)
        .to_bytes()
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let Some(value) = storage_value(
        &ctx.driver,
        BLOB_VERSIONS_KEYSPACE,
        key.into(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let version = BlobVersion::from_bytes(value.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    let Some(hash) = version.blob_hash().copied() else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let Some(location_key) = version.location_key() else {
        return Ok(ResolveResult::Missing { hash: Some(hash) });
    };
    let Some(location) = storage_value(
        &ctx.driver,
        BLOB_LOCATIONS_KEYSPACE,
        location_key.to_bytes().into(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(ResolveResult::Missing { hash: Some(hash) });
    };
    let location = BackendLocation::from_bytes(location.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    if location.get_blake3() != Some(hash.as_slice()) {
        return Ok(ResolveResult::Missing { hash: Some(hash) });
    }
    Ok(ResolveResult::Candidate(ExportCandidate {
        source: CandidateSource::Local {
            location,
            group_id: bucket.group_id,
            permission_path,
            node_id: exact.node_id,
            bucket: exact.bucket.clone(),
            key: exact.key.clone(),
        },
        report_source: ExportReportSource::Local,
        resolved_version: Some(exact.version),
        expected_blake3: Some(hash),
    }))
}

async fn resolve_alias(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    alias: &HashPathIndexKey,
    allowed: &BTreeMap<(GroupId, String), bool>,
) -> Result<ResolveResult, ExportFailure> {
    let permission_path = alias.permission_path();
    let Some(is_allowed) = allowed.get(&(alias.group_id, permission_path.clone())) else {
        return Err(ExportFailure::Retryable(
            "alias authorization unavailable".to_string(),
        ));
    };
    if !is_allowed {
        return Ok(ResolveResult::Denied);
    }
    let txn_id = start_read_txn(&ctx.driver).await?;
    let result = resolve_alias_txn(ctx, spec, alias, txn_id, &permission_path).await;
    match result {
        Ok(result) => {
            commit_read_txn(&ctx.driver, txn_id).await?;
            Ok(result)
        }
        Err(error) => {
            abort_read_txn(&ctx.driver, txn_id).await;
            Err(error)
        }
    }
}

async fn resolve_alias_txn(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    alias: &HashPathIndexKey,
    txn_id: TxnId,
    permission_path: &str,
) -> Result<ResolveResult, ExportFailure> {
    let evaluator = load_policy_txn(&ctx.driver, spec, alias.group_id, txn_id).await?;
    if !check_read_txn(&ctx.driver, spec, permission_path, &evaluator, txn_id).await? {
        return Ok(ResolveResult::Denied);
    }
    let key = VersionKey::new(alias.bucket.clone(), alias.key.clone(), alias.version_id)
        .to_bytes()
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let Some(value) = storage_value(
        &ctx.driver,
        BLOB_VERSIONS_KEYSPACE,
        key.into(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let version = BlobVersion::from_bytes(value.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    if version.blob_hash() != Some(&alias.blake3_hash) {
        return Ok(ResolveResult::Missing { hash: None });
    }
    let Some(location_key) = version.location_key() else {
        return Ok(ResolveResult::Missing { hash: None });
    };
    let Some(location) = storage_value(
        &ctx.driver,
        BLOB_LOCATIONS_KEYSPACE,
        location_key.to_bytes().into(),
        Some(txn_id),
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
        source: CandidateSource::Local {
            location,
            group_id: alias.group_id,
            permission_path: permission_path.to_string(),
            node_id: alias.node_id,
            bucket: alias.bucket.clone(),
            key: alias.key.clone(),
        },
        report_source: ExportReportSource::Hash,
        resolved_version: Some(alias.version_id),
        expected_blake3: Some(alias.blake3_hash),
    }))
}

fn snapshot_read_failure(error: MetadataApiError) -> ExportFailure {
    match error {
        MetadataApiError::NotFound => {
            ExportFailure::Permanent("metadata document has no raw RO-Crate revision".to_string())
        }
        MetadataApiError::Unauthorized | MetadataApiError::Forbidden => {
            ExportFailure::Permanent("metadata document READ permission denied".to_string())
        }
        MetadataApiError::BadRequest => {
            ExportFailure::Permanent("invalid metadata export request".to_string())
        }
        MetadataApiError::ServiceUnavailable => {
            ExportFailure::Retryable("metadata document is unavailable".to_string())
        }
        MetadataApiError::PlacementUnavailable(error) => {
            ExportFailure::Retryable(error.to_string())
        }
        MetadataApiError::InvalidCursor(message) | MetadataApiError::Internal(message) => {
            ExportFailure::Retryable(message)
        }
    }
}

async fn check_read_txn(
    ctx: &DriverContext,
    spec: &ExportRoCrateSpec,
    path: &str,
    evaluator: &PolicyEvaluator,
    txn_id: TxnId,
) -> Result<bool, ExportFailure> {
    let allowed = match drive(
        CheckPermissionsOperation::new_with_txn(
            CheckPermissionsConfig {
                auth_context: spec.auth_context.clone(),
                path: path.to_string(),
                required_permission: Permission::READ,
            },
            txn_id,
        ),
        ctx,
    )
    .await
    {
        Ok(allowed) => allowed,
        // Missing authorization state denies; it must not retry the job.
        Err(AuthorizationError::AuthDocNotFound | AuthorizationError::GroupNotFound) => false,
        Err(error) => return Err(ExportFailure::Retryable(error.to_string())),
    };
    if !allowed {
        return Ok(false);
    }
    let request = policy_request_with(
        path,
        &Permission::READ,
        Some(&spec.auth_context),
        PolicyRequestExtras::operation("s3.GetObject"),
    );
    match evaluator.evaluate(&request) {
        Ok(()) => Ok(true),
        Err(PolicyEnforcementError::Denied { .. }) => Ok(false),
        Err(PolicyEnforcementError::Unavailable(error)) => {
            Err(ExportFailure::Retryable(error.to_string()))
        }
    }
}

async fn load_policy_txn(
    ctx: &DriverContext,
    spec: &ExportRoCrateSpec,
    group_id: GroupId,
    txn_id: TxnId,
) -> Result<PolicyEvaluator, ExportFailure> {
    PolicyEvaluator::load_with_txn(ctx, spec.auth_context.realm_id, group_id, txn_id)
        .await
        .map_err(|error| ExportFailure::Retryable(error.to_string()))
}

async fn start_read_txn(ctx: &DriverContext) -> Result<TxnId, ExportFailure> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: true })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(ExportFailure::Retryable(error.to_string()))
        }
        event => Err(ExportFailure::Retryable(format!(
            "unexpected storage transaction event: {event:?}"
        ))),
    }
}

async fn commit_read_txn(ctx: &DriverContext, txn_id: TxnId) -> Result<(), ExportFailure> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            abort_read_txn(ctx, txn_id).await;
            Err(ExportFailure::Retryable(error.to_string()))
        }
        event => {
            abort_read_txn(ctx, txn_id).await;
            Err(ExportFailure::Retryable(format!(
                "unexpected storage commit event: {event:?}"
            )))
        }
    }
}

async fn abort_read_txn(ctx: &DriverContext, txn_id: TxnId) {
    let _ = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn load_policies(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    policies: &mut BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>,
    group_ids: impl IntoIterator<Item = GroupId>,
) -> Result<(), ExportFailure> {
    let pending = group_ids
        .into_iter()
        .filter(|group_id| !policies.contains_key(group_id))
        .collect::<BTreeSet<_>>();
    if pending.is_empty() {
        return Ok(());
    }
    let realm_id = spec.auth_context.realm_id;
    let mut loaded = PolicyEvaluator::load_bulk(
        &ctx.driver,
        pending.iter().map(|group_id| (realm_id, *group_id)),
    )
    .await
    .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    for group_id in pending {
        let evaluator = loaded
            .remove(&(realm_id, group_id))
            .ok_or_else(|| ExportFailure::Retryable("object policy unavailable".to_string()))?;
        policies.insert(group_id, std::sync::Arc::new(evaluator));
    }
    Ok(())
}

async fn load_rules(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    rules: &mut BTreeMap<GroupId, PermissionRules>,
    group_ids: impl IntoIterator<Item = GroupId>,
) -> Result<(), ExportFailure> {
    let pending = group_ids
        .into_iter()
        .filter(|group_id| !rules.contains_key(group_id))
        .collect::<BTreeSet<_>>();
    for group_id in pending {
        let config = PermissionRulesConfig {
            auth_context: spec.auth_context.clone(),
            path: format!("/{}/g/{group_id}", spec.auth_context.realm_id),
        };
        let loaded = match drive(PermissionRulesOperation::new(config), &ctx.driver).await {
            Ok(loaded) => loaded,
            // A group this node cannot read grants nothing: that is a denial, not an
            // outage, so the export omits its aliases instead of retrying forever.
            Err(AuthorizationError::AuthDocNotFound | AuthorizationError::GroupNotFound) => {
                PermissionRules::default()
            }
            Err(error) => return Err(ExportFailure::Retryable(error.to_string())),
        };
        rules.insert(group_id, loaded);
    }
    Ok(())
}

fn alias_allowed(
    spec: &ExportRoCrateSpec,
    group_id: GroupId,
    path: &str,
    rules: &BTreeMap<GroupId, PermissionRules>,
    policies: &BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>,
) -> Result<bool, ExportFailure> {
    let rules = rules
        .get(&group_id)
        .ok_or_else(|| ExportFailure::Retryable("authorization rules unavailable".to_string()))?;
    if !rules.allows(path, &Permission::READ) {
        return Ok(false);
    }
    let evaluator = policies
        .get(&group_id)
        .ok_or_else(|| ExportFailure::Retryable("object policy unavailable".to_string()))?;
    let request = policy_request_with(
        path,
        &Permission::READ,
        Some(&spec.auth_context),
        PolicyRequestExtras::operation("s3.GetObject"),
    );
    match evaluator.evaluate(&request) {
        Ok(()) => Ok(true),
        Err(PolicyEnforcementError::Denied { .. }) => Ok(false),
        Err(PolicyEnforcementError::Unavailable(error)) => {
            Err(ExportFailure::Retryable(error.to_string()))
        }
    }
}

async fn load_candidate_policies(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &ExportCheckpoint,
    policies: &mut BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>,
) -> Result<(), ExportFailure> {
    // Collected eagerly: a closure iterator held across the await trips
    // rustc's "not general enough" limitation on the spawned job future.
    let groups: Vec<GroupId> = checkpoint
        .entities
        .iter()
        .flat_map(|entity| {
            entity.candidates.iter().filter_map(|candidate| {
                let CandidateSource::Local { group_id, .. } = &candidate.source else {
                    return None;
                };
                Some(*group_id)
            })
        })
        .collect();
    load_policies(ctx, spec, policies, groups).await
}

async fn storage_value(
    ctx: &DriverContext,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
) -> Result<Option<Value>, ExportFailure> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id,
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

fn learn_probe_hash(entity: &mut ExportEntity, candidate_index: usize, hash: [u8; 32]) -> bool {
    let Some(candidate) = entity.candidates.get_mut(candidate_index) else {
        return false;
    };
    let (node_id, realm_id, resolved_version) = match &candidate.source {
        CandidateSource::RemoteExact { node_id, target } if candidate.expected_blake3.is_none() => {
            (*node_id, target.realm_id, candidate.resolved_version)
        }
        _ => return false,
    };
    candidate.expected_blake3 = Some(hash);
    entity.hash = Some(hash);
    entity.hash_realm = Some(realm_id);
    let fallback = ExportCandidate {
        source: CandidateSource::RemoteHash { node_id, hash },
        report_source: ExportReportSource::Hash,
        resolved_version,
        expected_blake3: Some(hash),
    };
    merge_candidates(
        &mut entity.candidates,
        std::slice::from_ref(&fallback),
        REMOTE_ATTEMPTS,
    );
    true
}

#[allow(clippy::too_many_arguments)]
async fn probe_sources_checked(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    candidate_failures: &BTreeMap<usize, BTreeMap<usize, OpenStatus>>,
    policies: &mut BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>,
    permission_rules: &mut BTreeMap<GroupId, PermissionRules>,
    alias_paths: &mut BTreeSet<(GroupId, String)>,
    alias_keys: &mut BTreeSet<([u8; 32], String, Ulid)>,
    alias_cache: &mut BTreeMap<[u8; 32], Vec<HashPathIndexKey>>,
    resolved_aliases: &mut BTreeMap<[u8; 32], (Vec<ExportCandidate>, bool)>,
    authorized_aliases: &mut BTreeMap<(GroupId, String), bool>,
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
            match Box::pin(open_candidate_checked(
                &ctx.driver,
                spec,
                policies,
                &candidate,
                true,
            ))
            .await?
            {
                CandidateOpen::Opened(BaoReadOutput::Metadata { size, blake3, .. }) => {
                    if learn_probe_hash(&mut checkpoint.entities[index], candidate_index, blake3) {
                        extend_hash_candidates(
                            ctx,
                            spec,
                            blake3,
                            candidate.resolved_version,
                            &mut checkpoint.entities[index].candidates,
                            &mut denied,
                            policies,
                            permission_rules,
                            alias_paths,
                            alias_keys,
                            alias_cache,
                            resolved_aliases,
                            authorized_aliases,
                        )
                        .await?;
                    }
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

async fn open_candidate_checked(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    _policies: &BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>,
    candidate: &ExportCandidate,
    metadata_only: bool,
) -> Result<CandidateOpen, ExportFailure> {
    match &candidate.source {
        CandidateSource::Local {
            location,
            group_id,
            permission_path,
            node_id,
            bucket,
            key,
        } => {
            let Some(blake3) = candidate.expected_blake3 else {
                return Err(ExportFailure::Permanent(
                    "local export candidate has no BLAKE3 hash".to_string(),
                ));
            };
            let txn_id = start_read_txn(driver).await?;
            let result = open_local_txn(
                driver,
                spec,
                candidate,
                location,
                *group_id,
                permission_path,
                *node_id,
                bucket,
                key,
                blake3,
                metadata_only,
                txn_id,
            )
            .await;
            match result {
                Ok(result) => {
                    commit_read_txn(driver, txn_id).await?;
                    Ok(result)
                }
                Err(error) => {
                    abort_read_txn(driver, txn_id).await;
                    Err(error)
                }
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

#[allow(clippy::too_many_arguments)]
async fn open_local_txn(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    candidate: &ExportCandidate,
    location: &BackendLocation,
    group_id: GroupId,
    permission_path: &str,
    node_id: NodeId,
    bucket: &str,
    key: &str,
    blake3: [u8; 32],
    metadata_only: bool,
    txn_id: TxnId,
) -> Result<CandidateOpen, ExportFailure> {
    let Some(bucket_value) = storage_value(
        driver,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().to_vec().into(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(CandidateOpen::Status(OpenStatus::Missing));
    };
    let bucket_info = BucketInfo::from_bytes(bucket_value.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    if bucket_info.group_id != group_id
        || permission_path
            != object_permission_path(spec.auth_context.realm_id, group_id, node_id, bucket, key)
    {
        return Ok(CandidateOpen::Status(OpenStatus::Missing));
    }
    let Some(version_id) = candidate.resolved_version else {
        return Ok(CandidateOpen::Status(OpenStatus::Missing));
    };
    let version_key = VersionKey::new(bucket.to_string(), key.to_string(), version_id);
    let version_bytes = version_key
        .to_bytes()
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let Some(version_value) = storage_value(
        driver,
        BLOB_VERSIONS_KEYSPACE,
        version_bytes.into(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(CandidateOpen::Status(OpenStatus::Missing));
    };
    let version = BlobVersion::from_bytes(version_value.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    if version.blob_hash() != Some(&blake3) || version.location_key().is_none() {
        return Ok(CandidateOpen::Status(OpenStatus::Missing));
    }
    let location_key = version
        .location_key()
        .ok_or_else(|| ExportFailure::Retryable("blob location is missing".to_string()))?;
    let Some(location_value) = storage_value(
        driver,
        BLOB_LOCATIONS_KEYSPACE,
        location_key.to_bytes().into(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(CandidateOpen::Status(OpenStatus::Missing));
    };
    let current_location = BackendLocation::from_bytes(location_value.as_ref())
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    if current_location.get_blake3() != Some(blake3.as_slice())
        || !current_location.same_object(location)
    {
        return Ok(CandidateOpen::Status(OpenStatus::Missing));
    }
    let evaluator = load_policy_txn(driver, spec, group_id, txn_id).await?;
    if !check_read_txn(driver, spec, permission_path, &evaluator, txn_id).await? {
        return Ok(CandidateOpen::Status(OpenStatus::Denied));
    }
    // A local governed read is a serve like any other: authorization first,
    // then this node's own registration and subject.
    if !serveable_locally(driver, &version, version_key, location, txn_id).await? {
        return Ok(CandidateOpen::Status(OpenStatus::Denied));
    }
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
                    etag: None,
                    hashes: HashMap::new(),
                }
            } else {
                BaoReadOutput::Stream {
                    blob,
                    size: stream_size,
                    blake3,
                    etag: None,
                    hashes: HashMap::new(),
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

/// Whether this node may still serve its own copy of a governed version. An
/// ungoverned version has no refs and never consults the inventory.
async fn serveable_locally(
    driver: &DriverContext,
    version: &BlobVersion,
    version_key: VersionKey,
    location: &BackendLocation,
    txn_id: TxnId,
) -> Result<bool, ExportFailure> {
    if version.placement_policies.is_empty() {
        return Ok(true);
    }
    let key = ManagedCopyKey::new(version_key, location.backend.clone());
    let effect = match serve_reads(&key, Some(txn_id)) {
        Ok(Effect::Storage(effect)) => effect,
        _ => {
            return Err(ExportFailure::Retryable(
                "serve reads unavailable".to_string(),
            ));
        }
    };
    let Event::Storage(StorageEvent::BatchReadResult { values }) =
        driver.storage_handle.send_storage_effect(effect).await
    else {
        return Ok(false);
    };
    let Ok((copy, subject)) = split_serve_reads(values) else {
        return Ok(false);
    };
    Ok(validate_registration(
        copy.as_deref(),
        &CopyRequest {
            key: &key,
            node_id: Some(subject.subject.node_id),
            blake3: location.get_blake3().and_then(|hash| hash.try_into().ok()),
            refs: &version.placement_policies,
            subject_generation: Some(subject.subject.generation),
        },
    )
    .is_ok())
}

async fn open_remote(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    node_id: NodeId,
    target: BaoReadTarget,
    expected_blake3: Option<[u8; 32]>,
    metadata_only: bool,
) -> Result<CandidateOpen, ExportFailure> {
    // The challenge loop fills in this node's advertised subject and the refs
    // it learns, so a governed copy can be staged instead of dead-ending.
    match managed_read(
        driver,
        node_id,
        BaoReadRequest {
            auth_context: spec.auth_context.clone(),
            realm_id: spec.auth_context.realm_id,
            target,
            expected_blake3,
            metadata_only,
            destination: None,
            known_refs: Vec::new(),
        },
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
        // Non-disclosing: a refused destination looks exactly like a refused
        // authorization, and neither names a policy.
        Err(
            BaoReadError::PolicyDenied { .. }
            | BaoReadError::PolicyRequired { .. }
            | BaoReadError::Gate(_)
            | BaoReadError::NoDestination,
        ) => Ok(CandidateOpen::Status(OpenStatus::Denied)),
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

#[cfg(test)]
async fn probe_sources(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    candidate_failures: &BTreeMap<usize, BTreeMap<usize, OpenStatus>>,
) -> Result<Vec<ProbedEntry>, ExportFailure> {
    let mut policies = BTreeMap::new();
    let mut permission_rules = BTreeMap::new();
    let mut alias_paths = BTreeSet::new();
    let mut alias_keys = BTreeSet::new();
    let mut alias_cache = BTreeMap::new();
    let mut resolved_aliases = BTreeMap::new();
    let mut authorized_aliases = BTreeMap::new();
    load_candidate_policies(ctx, spec, checkpoint, &mut policies).await?;
    probe_sources_checked(
        ctx,
        spec,
        checkpoint,
        candidate_failures,
        &mut policies,
        &mut permission_rules,
        &mut alias_paths,
        &mut alias_keys,
        &mut alias_cache,
        &mut resolved_aliases,
        &mut authorized_aliases,
    )
    .await
}

#[cfg(test)]
async fn open_candidate(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    candidate: &ExportCandidate,
    metadata_only: bool,
) -> Result<CandidateOpen, ExportFailure> {
    let policies = BTreeMap::new();
    open_candidate_checked(driver, spec, &policies, candidate, metadata_only).await
}

#[cfg(test)]
async fn write_archive(
    writer: tokio::io::DuplexStream,
    metadata: Vec<u8>,
    entries: Vec<PlannedEntry>,
    report: Option<Vec<u8>>,
    cancel: tokio_util::sync::CancellationToken,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), ExportFailure> {
    write_archive_checked(
        writer,
        metadata,
        entries,
        report,
        std::sync::Arc::new(BTreeMap::new()),
        cancel,
        shutdown,
        FIXTURE_MOMENT_MS,
    )
    .await
}

#[cfg(test)]
mod tests;
