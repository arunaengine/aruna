//! Holds the pure rules of a run's workspace: how inputs are named, outputs selected and failures
//! classified.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use aruna_core::compute::{BackendError, MAX_OUTPUT_MATCHES, output_suffix};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{
    AttemptControl, InputSelection, InputSource, JobError, JobRecord, MAX_EXECUTION_OUTPUTS,
    OutputDestination, OutputObject, OutputSelection,
};
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BackendLocation, CONTENT_TYPE_KEY, HashIndex, ensure_confined_path, key_content_type,
};
use aruna_core::structs::storage::encryption::BucketKeyError;
use aruna_core::structs::storage::replication::ReplicationFailure;
use ulid::Ulid;

use crate::groups::routing::RoutingInputsError;
use crate::jobs::lifecycle::stage::stage_error;
use crate::placement::policy::GateContextError;
use crate::replication::bao_read::BaoReadError;
use crate::replication::protocol::BaoReadRefusal;
use crate::s3::bucket::get::GetBucketError;
use crate::s3::object::get::GetObjectError;
use crate::s3::object::put::{PutObjectError, PutObjectResult};

/// One declared input resolved to the object it names.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct SourceObject {
    pub(super) path: String,
    pub(super) bucket: String,
    pub(super) key: String,
    pub(super) version: Option<Ulid>,
}

/// The object one input names, with no storage read: a source this node does
/// not hold is read from its holder, which authorizes the read itself.
pub(super) fn source_object(input: &InputSelection) -> Result<SourceObject, JobError> {
    let InputSource::S3 {
        bucket,
        key,
        version_id,
    } = &input.source;
    ensure_confined_path(Path::new(key))
        .map_err(|error| JobError::permanent(format!("invalid input key: {error}")))?;
    let path = input
        .container_path
        .clone()
        .ok_or_else(|| JobError::permanent("input has no container path"))?;
    let version = version_id
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| JobError::permanent(format!("invalid input version_id for {bucket}/{key}")))?;
    Ok(SourceObject {
        path,
        bucket: bucket.clone(),
        key: key.clone(),
        version,
    })
}

/// Registered copies of the same bytes this node holds itself, deduplicated by
/// the object they name. Only copies of this realm on this node can be read
/// locally, so everything else is dropped.
pub(super) fn local_copies(
    candidates: Vec<HashIndex>,
    realm_id: RealmId,
    node_id: NodeId,
) -> Vec<HashIndex> {
    let mut unique = BTreeMap::new();
    for candidate in candidates {
        if candidate.realm_id != realm_id || candidate.node_id != node_id {
            continue;
        }
        let key = (
            candidate.bucket.clone(),
            candidate.key.clone(),
            candidate.version_id,
        );
        unique.entry(key).or_insert(candidate);
    }
    unique.into_values().collect()
}

pub(super) fn capture_cancelled() -> JobError {
    JobError::retryable("output capture cancelled")
}

pub(super) fn destination_of(
    selection: &OutputSelection,
) -> Result<(NodeId, String, String), JobError> {
    let OutputDestination::S3 { bucket, key } = &selection.destination;
    let node_id = selection
        .destination_node_id
        .ok_or_else(|| JobError::permanent("output destination endpoint is missing"))?;
    Ok((node_id, bucket.clone(), key.clone()))
}

/// One selection per destination object: a file several globs match is captured once,
/// and two files that would land on one object fail the job instead of losing one.
pub(super) fn distinct_selections(
    selections: Vec<OutputSelection>,
) -> Result<Vec<OutputSelection>, JobError> {
    let mut sources: HashMap<(Option<NodeId>, String, String), String> = HashMap::new();
    let mut distinct = Vec::with_capacity(selections.len());
    for selection in selections {
        let OutputDestination::S3 { bucket, key } = &selection.destination;
        let target = (selection.destination_node_id, bucket.clone(), key.clone());
        match sources.get(&target) {
            Some(source) if *source == selection.container_path => {}
            Some(source) => {
                return Err(JobError::permanent(format!(
                    "outputs `{source}` and `{}` both target {bucket}/{key}",
                    selection.container_path
                )));
            }
            None => {
                sources.insert(target, selection.container_path.clone());
                distinct.push(selection);
            }
        }
    }
    Ok(distinct)
}

/// One selection per matched file, keyed by the match with `path_prefix` stripped.
/// Zero matches captures nothing, which the spec allows for a selection pattern.
/// A staged input the pattern also matches is not an output and is skipped.
pub(super) fn expand_selection(
    output: &OutputSelection,
    matched: Vec<String>,
    inputs: &[InputSelection],
) -> Result<Vec<OutputSelection>, JobError> {
    let prefix = output.path_prefix.as_deref().ok_or_else(|| {
        JobError::permanent(format!(
            "output `{}` contains wildcards without a path_prefix",
            output.container_path
        ))
    })?;
    if matched.len() > MAX_OUTPUT_MATCHES {
        return Err(JobError::permanent(format!(
            "output `{}` matches more than {MAX_OUTPUT_MATCHES} files",
            output.container_path
        )));
    }
    let OutputDestination::S3 { bucket, key } = &output.destination;
    matched
        .into_iter()
        .filter(|path| {
            !inputs
                .iter()
                .any(|input| input.container_path.as_deref() == Some(path.as_str()))
        })
        .map(|path| {
            let suffix = output_suffix(&path, prefix).ok_or_else(|| {
                JobError::permanent(format!("output `{path}` is outside path_prefix `{prefix}`"))
            })?;
            Ok(OutputSelection {
                container_path: path,
                path_prefix: None,
                destination_node_id: output.destination_node_id,
                destination: OutputDestination::S3 {
                    bucket: bucket.clone(),
                    key: format!("{}/{suffix}", key.trim_end_matches('/')),
                },
                name: output.name.clone(),
                description: output.description.clone(),
            })
        })
        .collect()
}

pub(super) fn output_stage_bucket(record: &JobRecord) -> String {
    format!("out-{}", record.job_id.to_string().to_lowercase())
}

pub(super) fn output_replication_error(failure: ReplicationFailure, message: String) -> JobError {
    if failure.is_denied() {
        JobError::permanent(format!("output copy failed: {message}"))
    } else {
        JobError::retryable(format!("output copy failed: {message}"))
    }
}

pub(super) fn output_read_error(error: &BackendError) -> JobError {
    let message = format!("container output read failed: {error}");
    if error.retryable() {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

/// A captured output carries the type its destination key implies, so a chart
/// or a JSON result is served as itself instead of an opaque download.
pub(super) fn output_metadata(key: &str) -> HashMap<String, String> {
    HashMap::from([(
        CONTENT_TYPE_KEY.to_string(),
        key_content_type(key).to_string(),
    )])
}

/// Exact identity of one written output: the version this write created and the
/// physical execution that produced it, both read back from the stored version.
pub(super) fn output_object(
    output: &OutputSelection,
    node_id: NodeId,
    bucket: &str,
    key: &str,
    result: &PutObjectResult,
    execution_id: Ulid,
) -> OutputObject {
    OutputObject {
        node_id,
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: result.version_id,
        execution_id,
        container_path: output.container_path.clone(),
        size: result.location.blob_size,
        digest: result.location.get_blake3().map(hex::encode),
    }
}

/// One input's bytes and its size, from the local copy or from a remote holder.
#[derive(Debug, PartialEq)]
pub struct StagedSource {
    pub(super) blob: BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>>,
    pub(super) location: Option<BackendLocation>,
    pub(super) size: Option<u64>,
    /// The source of a reference that delivered the bytes.
    pub(super) served_source: Option<VersionSourceBinding>,
}

impl StagedSource {
    pub(super) fn from_local(get: crate::s3::object::get::GetObjectResult) -> Self {
        Self {
            blob: get.blob,
            location: get.location,
            size: get
                .source_metadata
                .as_ref()
                .map(|metadata| metadata.content_length),
            served_source: get.served_source,
        }
    }
}

/// A read outcome no retry can change: the version is not there, is not the one
/// named, is not the caller's to read, or is governed data a device may never
/// hold. Everything else stays retryable, because transport is.
pub(super) fn device_read_error(bucket: &str, key: &str, error: BaoReadError) -> JobError {
    match &error {
        BaoReadError::GovernedUnavailable
        | BaoReadError::Refused(
            BaoReadRefusal::NotFound
            | BaoReadRefusal::InvalidTarget
            | BaoReadRefusal::HashMismatch
            | BaoReadRefusal::ReadDenied,
        ) => JobError::permanent(format!("staging {bucket}/{key} failed: {error}")),
        _ => stage_error(bucket, key, Some(error)),
    }
}

pub(super) fn bucket_lookup_error(scope: &str, error: GetBucketError) -> JobError {
    let message = format!("{scope} bucket lookup failed: {error}");
    if matches!(&error, GetBucketError::StorageError(error) if storage_retryable(error)) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

/// Only transient source drift earns another attempt: a historical observation
/// the source dropped and an exhausted binding both need an explicit rebind.
fn get_input_retryable(error: &GetObjectError) -> bool {
    match error {
        GetObjectError::StorageError(error) => storage_retryable(error),
        GetObjectError::ReferenceSourceChanged => true,
        GetObjectError::HistoricalReferenceUnavailable
        | GetObjectError::ReferenceChecksumMismatch
        | GetObjectError::ReferenceAdvanceExhausted => false,
        _ => false,
    }
}

/// Message prefix of an input read refused because its bucket key is locked.
pub(crate) const LOCKED_INPUT: &str = "input bucket key is locked";

pub(super) fn source_input_error(error: GetObjectError) -> JobError {
    // A lock after the precheck parks the job before any attempt; it is never a verdict.
    if matches!(
        &error,
        GetObjectError::ConversionError(ConversionError::BucketKey(BucketKeyError::Locked(_)))
    ) {
        return JobError::locked(format!("{LOCKED_INPUT}: {error}"));
    }
    let message = format!("input read failed: {error}");
    if get_input_retryable(&error) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

pub(super) fn authorization_error(scope: &str, error: AuthorizationError) -> JobError {
    let message = format!("{scope} authorization failed: {error}");
    if matches!(&error, AuthorizationError::StorageError(error) if storage_retryable(error)) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

pub(super) fn put_object_error(scope: &str, error: PutObjectError) -> JobError {
    let message = format!("{scope} failed: {error}");
    if matches!(&error, PutObjectError::StorageError(error) if storage_retryable(error)) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

/// A node mid-transition admits nothing governed. That ends on its own, so the
/// attempt parks and retries instead of failing the job.
pub(super) fn gate_error(scope: &str, error: GateContextError) -> JobError {
    JobError::retryable(format!("{scope} destination unavailable: {error}"))
}

pub(super) fn gate_stopped(scope: &str) -> JobError {
    JobError::retryable(format!(
        "{scope} destination is not admitting governed data"
    ))
}

pub(super) fn routing_error(scope: &str, error: RoutingInputsError) -> JobError {
    let message = format!("{scope} routing lookup failed: {error}");
    if error.storage().is_some_and(storage_retryable) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

fn storage_retryable(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::TransactionConflict
            | StorageError::CleanupCapacity
            | StorageError::ReadError(_)
            | StorageError::WriteError(_)
            | StorageError::DeleteError
            | StorageError::PersistError(_)
            | StorageError::ChannelClosed
            | StorageError::QueueFull
            | StorageError::Timeout
    )
}

/// Version this execution reserved for `bucket`/`key` before writing, or `None`
/// when the object under that key was produced by another writer. This is the
/// only thing that attributes a workspace object to an execution.
pub(super) fn reserved_version(
    control: &AttemptControl,
    node_id: NodeId,
    bucket: &str,
    key: &str,
) -> Option<Ulid> {
    control
        .output_commits
        .iter()
        .find(|commit| commit.node_id == node_id && commit.bucket == bucket && commit.key == key)
        .map(|commit| commit.version_id)
}

/// Fold the exported manifest into the inventoried one under the same keyed limit
/// inventory enforces. A retried finalize inventories objects a previous export already
/// wrote; the export row wins, because it names a container path inventory cannot know.
pub(super) fn merge_outputs(
    inventoried: Vec<OutputObject>,
    captured: Vec<OutputObject>,
) -> Result<Vec<OutputObject>, JobError> {
    let exported: HashSet<(NodeId, &str, &str)> = captured
        .iter()
        .map(|output| (output.node_id, output.bucket.as_str(), output.key.as_str()))
        .collect();
    let retained: Vec<OutputObject> = inventoried
        .into_iter()
        .filter(|output| {
            !exported.contains(&(output.node_id, output.bucket.as_str(), output.key.as_str()))
        })
        .collect();

    let mut outputs = Vec::new();
    let mut keys = HashSet::new();
    for output in retained.into_iter().chain(captured) {
        insert_output(&mut outputs, &mut keys, output)?;
    }
    Ok(outputs)
}

pub(super) fn insert_output(
    outputs: &mut Vec<OutputObject>,
    keys: &mut HashSet<(NodeId, String, String)>,
    output: OutputObject,
) -> Result<(), JobError> {
    if !keys.insert((output.node_id, output.bucket.clone(), output.key.clone())) {
        return Ok(());
    }
    if outputs.len() >= MAX_EXECUTION_OUTPUTS {
        return Err(JobError::permanent(format!(
            "output manifest exceeds {MAX_EXECUTION_OUTPUTS} objects"
        )));
    }
    outputs.push(output);
    Ok(())
}

#[cfg(test)]
mod tests {
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_ACCESS_KEYSPACE,
    };
    use aruna_core::structs::execution::job::{
        InputMode, JobErrorKind, JobId, JobPayload, OutputCommitIntent,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    };
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    use std::collections::BTreeSet;
    use std::time::{Duration, SystemTime};

    use aruna_core::calls::Next;
    use aruna_core::operation::Operation;
    use aruna_core::structs::execution::job::{CapturedInput, ExecutionSpec};
    use aruna_core::structs::identity::auth::Permission;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, bucket_permission_path};
    use aruna_core::structs::storage::distribution::source_root::RootEntry;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::origin::SourcePin;

    use super::super::credential::{
        MintCredentialOperation, RunIdentity, WorkspaceAccessOperation, WorkspaceCredential,
    };
    use super::super::inputs::{keep_served, serving_pin};
    use super::super::inventory::CollectOutputsOperation;
    use super::super::outputs::{input_refs, uncaptured_input};
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::access::get::GetAccessOperation;
    use crate::s3::bucket::create::CreateBucketOperation;

    fn run_of(
        context: &DriverContext,
        spec: &ExecutionSpec,
        record: &JobRecord,
        node_id: NodeId,
    ) -> RunIdentity {
        let realm_id = context.serving_realm(record.created_by.realm_id);
        RunIdentity::new(spec, record, realm_id, node_id)
    }

    fn secret_inputs(
        context: &DriverContext,
    ) -> (
        Option<aruna_core::credential_encryption::CredentialEncryptionKey>,
        SystemTime,
    ) {
        let key = (context.net_handle.as_ref()).map(|net| net.credential_encryption_key());
        (key, SystemTime::now())
    }

    async fn mint_workspace(
        context: &DriverContext,
        spec: &ExecutionSpec,
        record: &JobRecord,
        node_id: NodeId,
        scope: (&str, Option<&str>),
    ) -> Result<WorkspaceCredential, JobError> {
        let run = run_of(context, spec, record, node_id);
        let secret = secret_inputs(context);
        let mint = MintCredentialOperation::workspace((spec, record), run, scope, secret);
        drive(mint, context).await
    }

    async fn mint_inputs(
        context: &DriverContext,
        spec: &ExecutionSpec,
        record: &JobRecord,
        node_id: NodeId,
        buckets: &BTreeSet<String>,
    ) -> Result<WorkspaceCredential, JobError> {
        let run = run_of(context, spec, record, node_id);
        let secret = secret_inputs(context);
        let mint = MintCredentialOperation::inputs((spec, record), run, buckets, secret);
        drive(mint, context).await
    }

    async fn group_write(
        context: &DriverContext,
        spec: &ExecutionSpec,
        record: &JobRecord,
        node_id: NodeId,
    ) -> Result<(), JobError> {
        let run = run_of(context, spec, record, node_id);
        drive(WorkspaceAccessOperation::group(run), context).await
    }

    fn spec(output_prefixes: Vec<String>) -> ExecutionSpec {
        ExecutionSpec {
            group_id: Ulid::from_bytes([2; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes,
            collision_policy: Default::default(),
        }
    }

    fn output(key: &str) -> OutputObject {
        OutputObject {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            bucket: "workspace".to_string(),
            key: key.to_string(),
            version_id: Ulid::generate(),
            execution_id: Ulid::from_bytes([9; 16]),
            container_path: key.to_string(),
            size: 0,
            digest: None,
        }
    }

    #[test]
    fn capture_sets_type() {
        // The captured put must name the type, or a chart serves as a download.
        let metadata = output_metadata("results/run-1/chart.png");
        assert_eq!(
            metadata.get(CONTENT_TYPE_KEY).map(String::as_str),
            Some("image/png")
        );
        assert_eq!(
            output_metadata("out/blob")
                .get(CONTENT_TYPE_KEY)
                .map(String::as_str),
            Some("application/octet-stream")
        );
    }

    // Both input mappings must retry only transient drift: a job that waits on a
    // rebind or a dropped observation would burn its whole attempt budget.
    #[test]
    fn device_read_fails() {
        // A governed or missing realm input must not burn every attempt.
        for error in [
            BaoReadError::GovernedUnavailable,
            BaoReadError::Refused(BaoReadRefusal::NotFound),
            BaoReadError::Refused(BaoReadRefusal::InvalidTarget),
            BaoReadError::Refused(BaoReadRefusal::ReadDenied),
        ] {
            assert_eq!(
                device_read_error("src", "reads.fastq", error).kind,
                JobErrorKind::Permanent
            );
        }
        assert_eq!(
            device_read_error(
                "src",
                "reads.fastq",
                BaoReadError::Refused(BaoReadRefusal::BackendFailure)
            )
            .kind,
            JobErrorKind::Retryable
        );
    }

    #[test]
    fn picks_local_copies() {
        // Only copies of this realm on this node may be read locally, and one
        // object named twice stays one candidate.
        let hash = [7u8; 32];
        let realm = RealmId::from_bytes([1u8; 32]);
        let other_realm = RealmId::from_bytes([2u8; 32]);
        let node = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
        let other_node = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let group = Ulid::from_bytes([5; 16]);
        let version = Ulid::from_bytes([6; 16]);
        let alias = |realm_id, node_id, bucket: &str, key: &str| {
            HashIndex::new(hash, version, realm_id, group, node_id, bucket, key)
        };
        let candidates = vec![
            alias(realm, node, "shared", "reads.fastq"),
            alias(realm, node, "shared", "reads.fastq"),
            alias(realm, node, "other", "copy.fastq"),
            alias(realm, other_node, "shared", "reads.fastq"),
            alias(other_realm, node, "shared", "reads.fastq"),
        ];

        let picked = local_copies(candidates, realm, node);
        assert_eq!(picked.len(), 2);
        assert!(picked.iter().all(|copy| copy.node_id == node));
        assert!(picked.iter().all(|copy| copy.realm_id == realm));
    }

    #[test]
    fn classifies_input_errors() {
        assert_eq!(
            source_input_error(GetObjectError::ReferenceSourceChanged).kind,
            JobErrorKind::Retryable
        );
        assert_eq!(
            source_input_error(GetObjectError::HistoricalReferenceUnavailable).kind,
            JobErrorKind::Permanent
        );
        assert_eq!(
            source_input_error(GetObjectError::ReferenceAdvanceExhausted).kind,
            JobErrorKind::Permanent
        );
        // A key locked after the precheck is a parkable condition, never a verdict.
        let locked = GetObjectError::ConversionError(ConversionError::BucketKey(
            BucketKeyError::Locked(Ulid::from_bytes([8; 16])),
        ));
        let error = source_input_error(locked);
        assert_eq!(error.kind, JobErrorKind::Retryable);
        assert!(error.locked);
    }

    // Transient fjall faults now carry their source; classification must key on
    // the variant, not on the payload.
    #[test]
    fn classifies_storage_retries() {
        for error in [
            StorageError::ReadError("io".to_string()),
            StorageError::WriteError("io".to_string()),
            StorageError::DeleteError,
            StorageError::TransactionConflict,
            // Capacity conditions prove no commit, so they stay infrastructure errors.
            StorageError::CleanupCapacity,
            StorageError::QueueFull,
        ] {
            assert!(storage_retryable(&error));
        }

        for error in [
            StorageError::KeyspaceError("missing".to_string()),
            StorageError::KeyNotFound,
        ] {
            assert!(!storage_retryable(&error));
        }
    }

    #[test]
    fn empty_outputs_remain() {
        let mut collect =
            CollectOutputsOperation::new(&spec(Vec::new()), "workspace", control(Vec::new()), None);
        assert_eq!(collect.first(), Ok(Next::Done(Vec::new())));
    }

    #[test]
    fn prefix_only_rejected() {
        let spec = spec(vec!["reports/".to_string()]);
        let mut collect =
            CollectOutputsOperation::new(&spec, "workspace", control(Vec::new()), None);
        let error = collect.first().unwrap_err();
        assert_eq!(error.kind, JobErrorKind::Permanent);
        assert_eq!(
            error.message,
            "output prefixes require declared file outputs"
        );
    }

    fn control(output_commits: Vec<OutputCommitIntent>) -> AttemptControl {
        AttemptControl {
            attempt_epoch: 1,
            execution_id: Ulid::from_bytes([9; 16]),
            controller_generation: 1,
            bound_token: None,
            tombstone_ref: None,
            output_commits,
            output_record: None,
        }
    }

    #[test]
    fn attributes_reserved_only() {
        // A duplicate execution's head under the same key, an unrelated later
        // write, and another bucket all stay unattributed.
        let version = Ulid::from_bytes([5; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let reserved = control(vec![OutputCommitIntent {
            node_id,
            bucket: "workspace".to_string(),
            key: "reports/a.txt".to_string(),
            version_id: version,
        }]);

        assert_eq!(
            reserved_version(&reserved, node_id, "workspace", "reports/a.txt"),
            Some(version)
        );
        assert_eq!(
            reserved_version(&reserved, node_id, "workspace", "reports/b.txt"),
            None
        );
        assert_eq!(
            reserved_version(&reserved, node_id, "other", "reports/a.txt"),
            None
        );
        assert_eq!(
            reserved_version(&control(Vec::new()), node_id, "workspace", "reports/a.txt"),
            None
        );
    }

    #[test]
    fn duplicates_keep_versions() {
        // Two executions writing one key reserve two versions, so neither can
        // be attributed to the other.
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let destinations = vec![(
            node_id,
            "workspace".to_string(),
            "reports/a.txt".to_string(),
        )];
        let mut first = control(Vec::new());
        let mut second = control(Vec::new());
        second.execution_id = Ulid::from_bytes([10; 16]);
        first.reserve_outputs(&destinations, |_| Ulid::generate());
        second.reserve_outputs(&destinations, |_| Ulid::generate());

        let left = reserved_version(&first, node_id, "workspace", "reports/a.txt").unwrap();
        let right = reserved_version(&second, node_id, "workspace", "reports/a.txt").unwrap();
        assert_ne!(left, right);
    }

    struct CredentialFixture {
        context: DriverContext,
        net: aruna_net::NetHandle,
        spec: ExecutionSpec,
        record: JobRecord,
        node_id: NodeId,
        bucket: String,
        _dir: tempfile::TempDir,
    }

    async fn credential_fixture() -> CredentialFixture {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let spec = spec(Vec::new());
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let realm_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let group_doc =
            GroupAuthorizationDocument::default_group_doc(user_id, realm_id, spec.group_id);
        // Policy loading fails closed without the realm config and group record.
        let realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let group = Group {
            display_name: "workspace".to_string(),
            group_id: spec.group_id,
            realm_id,
            roles: group_doc.roles.keys().copied().collect(),
            owner: user_id,
        };
        for (key_space, key, value) in [
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_doc.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                spec.group_id.to_bytes().to_vec(),
                group_doc.to_bytes(&actor).unwrap(),
            ),
            (
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_config.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                spec.group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            let _ = storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
        let net = aruna_net::NetHandle::new(
            aruna_net::NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                realm_id,
                discovery_method: aruna_net::DiscoveryMethod::None,
                relay_method: aruna_net::RelayMethod::None,
                ..aruna_net::NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let job_id = JobId::from_bytes([4; 16]);
        let record = JobRecord::new(
            job_id,
            JobPayload::Execution(spec.clone()),
            user_id,
            node_id,
            1,
            1,
            None,
        );
        let bucket = format!("run-{}", job_id.to_string().to_lowercase());
        CredentialFixture {
            context,
            net,
            spec,
            record,
            node_id,
            bucket,
            _dir: dir,
        }
    }

    #[tokio::test]
    async fn device_checks_grant() {
        // The cached group documents are the whole authority on a device too:
        // the owner binding does not stand in for a grant the owner lacks.
        let CredentialFixture {
            context,
            net,
            spec,
            record,
            node_id,
            _dir,
            ..
        } = credential_fixture().await;
        let realm_id = record.created_by.realm_id;
        let owner = record.created_by;
        let ungranted = UserId::local(Ulid::from_bytes([9; 16]), realm_id);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(
            node_id,
            aruna_core::structs::identity::realm::RealmNodeKind::User { owner },
        );
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;

        assert!(group_write(&context, &spec, &record, node_id).await.is_ok());
        let foreign = JobRecord::new(
            record.job_id,
            JobPayload::Execution(spec.clone()),
            ungranted,
            node_id,
            1,
            1,
            None,
        );
        assert!(
            group_write(&context, &spec, &foreign, node_id)
                .await
                .is_err()
        );
        net.shutdown().await;
    }

    #[tokio::test]
    async fn credential_reuses_secret() {
        let CredentialFixture {
            context,
            net,
            spec,
            record,
            node_id,
            bucket,
            _dir,
        } = credential_fixture().await;
        let realm_id = record.created_by.realm_id;

        let first = mint_workspace(&context, &spec, &record, node_id, (&bucket, None))
            .await
            .unwrap();
        let second = mint_workspace(&context, &spec, &record, node_id, (&bucket, None))
            .await
            .unwrap();

        assert_eq!(second.access_key, first.access_key);
        assert_eq!(second.secret, first.secret);

        let access = Box::pin(drive(
            GetAccessOperation::new(first.access_key.clone()),
            &context,
        ))
        .await
        .unwrap();
        let mut expired = access.clone();
        expired.expiry = SystemTime::UNIX_EPOCH;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: expired.access_key.as_bytes().into(),
                value: expired.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        let renewed = mint_workspace(&context, &spec, &record, node_id, (&bucket, None))
            .await
            .unwrap();
        assert_eq!(renewed.access_key, first.access_key);
        let renewed_access = Box::pin(drive(
            GetAccessOperation::new(first.access_key.clone()),
            &context,
        ))
        .await
        .unwrap();
        assert!(!renewed_access.is_expired(SystemTime::now()));
        let restrictions = renewed_access.path_restrictions.unwrap();
        let bucket_path = bucket_permission_path(realm_id, spec.group_id, node_id, &bucket);
        let permits = |path: &str| {
            restrictions.iter().any(|restriction| {
                globset::Glob::new(&restriction.pattern)
                    .unwrap()
                    .compile_matcher()
                    .is_match(path)
            })
        };
        assert!(permits(&bucket_path));
        assert!(permits(&format!("{bucket_path}/object")));
        assert!(!permits(&format!("{bucket_path}-sibling")));
        net.shutdown().await;
    }

    #[tokio::test]
    async fn rejects_foreign_key() {
        // A secret encrypted with another node's key must refuse to be reused rather
        // than hand the container an unusable or wrong secret.
        let CredentialFixture {
            context,
            net,
            spec,
            record,
            node_id,
            bucket,
            _dir,
        } = credential_fixture().await;

        let minted = mint_workspace(&context, &spec, &record, node_id, (&bucket, None))
            .await
            .unwrap();
        let mut access = Box::pin(drive(
            GetAccessOperation::new(minted.access_key.clone()),
            &context,
        ))
        .await
        .unwrap();
        access
            .encrypt_secret(
                &aruna_core::credential_encryption::CredentialEncryptionKey::random(),
                "foreign",
            )
            .unwrap();
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access.access_key.as_bytes().into(),
                value: access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let Err(error) = mint_workspace(&context, &spec, &record, node_id, (&bucket, None)).await
        else {
            panic!("a foreign key must not yield a credential")
        };

        assert_eq!(
            error.kind,
            aruna_core::structs::execution::job::JobErrorKind::Permanent
        );
        assert!(
            error
                .message
                .starts_with("workspace credential decryption failed")
        );
        net.shutdown().await;
    }

    #[tokio::test]
    async fn bounds_session_expiry() {
        // A session credential outlives neither its walltime nor its bearer.
        use aruna_core::compute::runtimes::{
            SESSION_EXPIRY_TAG, SESSION_RUNTIME_TAG, SESSION_TAG, SESSION_TAG_NOTEBOOK,
        };
        let CredentialFixture {
            _dir,
            context,
            net,
            record,
            node_id,
            bucket,
            mut spec,
        } = credential_fixture().await;
        let bearer_ms = aruna_core::time::unix_timestamp_millis() + 60_000;
        spec.resources.max_walltime_ms = Some(24 * 60 * 60 * 1000);
        spec.tags
            .insert(SESSION_TAG.to_string(), SESSION_TAG_NOTEBOOK.to_string());
        spec.tags.insert(
            SESSION_RUNTIME_TAG.to_string(),
            "python-notebook".to_string(),
        );
        spec.tags
            .insert(SESSION_EXPIRY_TAG.to_string(), bearer_ms.to_string());

        let credential = mint_workspace(&context, &spec, &record, node_id, (&bucket, None))
            .await
            .unwrap();

        assert_eq!(credential.expires_at_ms, bearer_ms);
        net.shutdown().await;
    }

    // A served source stored by its root's digest is named by its public URL once the root's
    // settings are back.
    #[test]
    fn compact_pins_hydrated() {
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::execution::staging::{PortableSourceDescriptor, StagingStrategy};
        let full = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::S3,
                public_config: [
                    ("endpoint", "https://s3.example.org"),
                    ("bucket", "refseq"),
                    ("skip_signature", "true"),
                ]
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .into(),
                source_path: "genomes/a.fna".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let (compact, root) = RootEntry::compact(&full).unwrap().unwrap();
        let pin = serving_pin(&compact, Some(&root));
        let url = "https://s3.example.org/refseq/genomes/a.fna";
        assert_eq!(pin.url.as_deref(), Some(url));
    }

    // The source that served a staged reference input is kept on the stored record, where
    // the run record reads it.
    #[tokio::test]
    async fn served_sources_kept() {
        use aruna_core::structs::execution::job::{CapturedInput, InputContent};
        let test_context = crate::tests::staging::setup_driver_context().await;
        let context = &test_context.driver_context;
        let realm_id = RealmId([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let mut record = JobRecord::new(
            JobId::from_bytes([5; 16]),
            JobPayload::Execution(spec(Vec::new())),
            user_id,
            node_id,
            1,
            1,
            None,
        );
        record.captured_inputs.push(CapturedInput {
            destination_key: "inputs/in.txt".to_string(),
            source_node_id: node_id,
            version_id: Ulid::from_bytes([6; 16]),
            content: InputContent::Reference {
                etag: None,
                range_bound: false,
            },
            bytes: 3,
            policies: Vec::new(),
            source_group_id: Ulid::from_bytes([7; 16]),
            public: false,
            link: None,
            source_pin: None,
            served_by: None,
            database: None,
            releases: Vec::new(),
            releases_complete: true,
            grant: None,
        });
        record.claim = Some(aruna_core::structs::execution::job::JobClaim {
            holder_node_id: node_id,
            claim_token: Ulid::from_bytes([8; 16]),
            lease_expires_ms: u64::MAX,
        });
        crate::jobs::store::test_calls::insert_job(context, &record)
            .await
            .unwrap();
        let pin = SourcePin {
            url: Some("https://mirror.example.org/in.txt".to_string()),
            path: "in.txt".to_string(),
            source_version: None,
        };
        let stored = || async {
            let read =
                crate::jobs::store::test_calls::read_job_record(context, record.job_id, None).await;
            read.unwrap().unwrap().captured_inputs[0].served_by.clone()
        };
        let token = Ulid::from_bytes([8; 16]);
        let stale_token = Ulid::from_bytes([9; 16]);
        let served = BTreeMap::from([("inputs/in.txt".to_string(), pin.clone())]);
        drive(keep_served(record.job_id, token, served.clone()), context)
            .await
            .unwrap();
        assert_eq!(stored().await, Some(pin.clone()));
        // A preparation under another claim writes nothing.
        let mut stale = record.clone();
        if let Some(claim) = stale.claim.as_mut() {
            claim.claim_token = Ulid::from_bytes([9; 16]);
        }
        assert!(
            drive(
                keep_served(stale.job_id, stale_token, BTreeMap::new()),
                context
            )
            .await
            .is_err()
        );
        assert_eq!(stored().await, Some(pin));
        // A later preparation that served nothing clears the earlier pin.
        drive(keep_served(record.job_id, token, BTreeMap::new()), context)
            .await
            .unwrap();
        assert_eq!(stored().await, None);
    }

    #[tokio::test]
    async fn rejects_oversized_mounts() {
        // Fifty patterns are the issuance cap, so twenty-five mounted buckets
        // still pass and the twenty-sixth must fail permanently.
        let (storage_handle, _receivers) = aruna_storage::StorageHandle::new();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let spec = spec(Vec::new());
        let record = JobRecord::new(
            JobId::from_bytes([4; 16]),
            JobPayload::Execution(spec.clone()),
            user_id,
            node_id,
            1,
            1,
            None,
        );
        let buckets = |count: usize| {
            (0..count)
                .map(|index| format!("bucket-{index}"))
                .collect::<BTreeSet<String>>()
        };

        // Without a net handle the accepted set fails later, past the limit check.
        let Err(within) = mint_inputs(&context, &spec, &record, node_id, &buckets(25)).await else {
            panic!("a credential cannot be encrypted without a net handle")
        };
        assert_eq!(within.message, "workspace credential needs a net handle");

        let Err(over) = mint_inputs(&context, &spec, &record, node_id, &buckets(26)).await else {
            panic!("an over-limit mount set must not yield a credential")
        };

        assert_eq!(
            over.kind,
            aruna_core::structs::execution::job::JobErrorKind::Permanent
        );
        assert!(
            over.message
                .starts_with("workspace credential restrictions invalid")
        );
    }

    #[test]
    fn inherits_effective_refs() {
        // A node that holds no copy of the input must still inherit its refs,
        // so they come from the captured inputs and a missing one fails closed.
        let version = Ulid::from_bytes([7; 16]);
        let mut spec = spec(Vec::new());
        spec.inputs.push(InputSelection {
            source: InputSource::S3 {
                bucket: "input".to_string(),
                key: "data.csv".to_string(),
                version_id: Some(version.to_string()),
            },
            source_node_id: None,
            dest_key: "in/data.csv".to_string(),
            mode: InputMode::Mount,
            container_path: Some("/in/data.csv".to_string()),
            name: None,
            description: None,
        });
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let realm_id = RealmId([1; 32]);
        let mut record = JobRecord::new(
            JobId::from_bytes([4; 16]),
            JobPayload::Execution(spec.clone()),
            UserId::local(Ulid::from_bytes([2; 16]), realm_id),
            node_id,
            1,
            1,
            None,
        );

        assert_eq!(uncaptured_input(&spec, &record), Some("in/data.csv"));

        let policy = PlacementPolicyRef {
            policy_id: Ulid::from_bytes([5; 16]),
            digest: [6; 32],
        };
        record.captured_inputs.push(CapturedInput {
            destination_key: "in/data.csv".to_string(),
            source_node_id: node_id,
            version_id: version,
            content: aruna_core::structs::execution::job::InputContent::Blake3([0; 32]),
            bytes: 4,
            policies: vec![policy],
            source_group_id: Default::default(),
            public: false,
            link: None,
            source_pin: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
            served_by: None,
        });

        assert_eq!(uncaptured_input(&spec, &record), None);
        assert_eq!(input_refs(&spec, &record).unwrap(), vec![policy]);
    }

    #[test]
    fn overlap_deduplicates() {
        let mut outputs = Vec::new();
        let mut keys = HashSet::new();
        insert_output(&mut outputs, &mut keys, output("result")).unwrap();
        insert_output(&mut outputs, &mut keys, output("result")).unwrap();
        assert_eq!(outputs.len(), 1);
    }

    #[test]
    fn merge_deduplicates() {
        // A retried finalize inventories the object its own export already wrote;
        // the manifest must name it once, with the export's container path.
        let mut inventoried = output("result");
        inventoried.container_path.clear();
        let captured = output("result");

        let outputs = merge_outputs(vec![inventoried], vec![captured.clone()]).unwrap();

        assert_eq!(outputs, vec![captured]);
    }

    #[test]
    fn merge_keeps_order() {
        let inventoried = vec![output("a"), output("b")];
        let captured = vec![output("c")];

        let outputs = merge_outputs(inventoried, captured).unwrap();

        let keys: Vec<_> = outputs.iter().map(|output| output.key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn merge_enforces_limit() {
        let inventoried: Vec<_> = (0..MAX_EXECUTION_OUTPUTS)
            .map(|index| output(&index.to_string()))
            .collect();

        // A duplicate is absorbed, so a full inventory still merges.
        let merged = merge_outputs(inventoried.clone(), vec![output("0")]).unwrap();
        assert_eq!(merged.len(), MAX_EXECUTION_OUTPUTS);

        let error = merge_outputs(inventoried, vec![output("overflow")]).unwrap_err();
        assert_eq!(
            error.kind,
            aruna_core::structs::execution::job::JobErrorKind::Permanent
        );
    }

    fn wildcard_output(pattern: &str) -> OutputSelection {
        OutputSelection {
            container_path: pattern.to_string(),
            path_prefix: Some("/out".to_string()),
            destination_node_id: Some(iroh::SecretKey::from_bytes(&[4u8; 32]).public()),
            destination: OutputDestination::S3 {
                bucket: "dest".to_string(),
                key: "results/".to_string(),
            },
            name: Some("reports".to_string()),
            description: None,
        }
    }

    fn expanded_keys(output: &OutputSelection, matched: Vec<String>) -> Vec<String> {
        expand_selection(output, matched, &[])
            .unwrap()
            .iter()
            .map(|output| {
                let OutputDestination::S3 { key, .. } = &output.destination;
                key.clone()
            })
            .collect()
    }

    #[test]
    fn expands_matched_keys() {
        let output = wildcard_output("/out/*.txt");
        let matched = vec!["/out/a.txt".to_string(), "/out/b.txt".to_string()];

        let expanded = expand_selection(&output, matched, &[]).unwrap();

        assert_eq!(expanded[1].container_path, "/out/b.txt");
        assert_eq!(expanded[1].name.as_deref(), Some("reports"));
        assert!(expanded[1].path_prefix.is_none());
        // The suffix below path_prefix keeps every nested component.
        assert_eq!(
            expanded_keys(&output, vec!["/out/a.txt".to_string()]),
            vec!["results/a.txt"]
        );
        assert_eq!(
            expanded_keys(
                &wildcard_output("/out/*/*/*.txt"),
                vec!["/out/sub/deep/b.txt".to_string()]
            ),
            vec!["results/sub/deep/b.txt"]
        );
    }

    #[test]
    fn empty_expansion_captures() {
        // Zero matches is a legitimate empty selection, not a job failure.
        assert!(
            expand_selection(&wildcard_output("/out/*.txt"), Vec::new(), &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn skips_staged_inputs() {
        // Nextflow globs the work directory that also holds the staged inputs.
        let input = InputSelection {
            source: InputSource::S3 {
                bucket: "input".to_string(),
                key: "reads.txt".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "out/reads.txt".to_string(),
            mode: InputMode::Mount,
            container_path: Some("/out/reads.txt".to_string()),
            name: None,
            description: None,
        };
        let matched = vec!["/out/reads.txt".to_string(), "/out/trimmed.txt".to_string()];

        let expanded = expand_selection(&wildcard_output("/out/*.txt"), matched, &[input]).unwrap();

        assert_eq!(expanded.len(), 1);
        assert_eq!(expanded[0].container_path, "/out/trimmed.txt");
    }

    #[test]
    fn counts_shared_matches() {
        // Two globs over the same 600 files fill a manifest of 600, not 1,200.
        let matched: Vec<String> = (0..600).map(|index| format!("/out/{index}.txt")).collect();
        let mut selections =
            expand_selection(&wildcard_output("/out/*.txt"), matched.clone(), &[]).unwrap();
        selections
            .extend(expand_selection(&wildcard_output("/out/*[0-9].txt"), matched, &[]).unwrap());

        let distinct = distinct_selections(selections).unwrap();

        assert_eq!(distinct.len(), 600);
    }

    #[test]
    fn refuses_colliding_files() {
        // Two files whose suffixes meet under one url must not overwrite each other.
        let glob = |directory: &str| OutputSelection {
            container_path: format!("{directory}/*.txt"),
            path_prefix: Some(directory.to_string()),
            ..wildcard_output("/out/*.txt")
        };
        let mut selections = expand_selection(
            &glob("/work/a"),
            vec!["/work/a/report.txt".to_string()],
            &[],
        )
        .unwrap();
        selections.extend(
            expand_selection(
                &glob("/work/b"),
                vec!["/work/b/report.txt".to_string()],
                &[],
            )
            .unwrap(),
        );

        let error = distinct_selections(selections).unwrap_err();

        assert_eq!(
            error.kind,
            aruna_core::structs::execution::job::JobErrorKind::Permanent
        );
        assert!(
            error.message.contains("results/report.txt"),
            "{}",
            error.message
        );
    }

    #[test]
    fn rejects_foreign_match() {
        let output = wildcard_output("/out/*.txt");
        let error = expand_selection(&output, vec!["/other/a.txt".to_string()], &[]).unwrap_err();
        assert_eq!(
            error.kind,
            aruna_core::structs::execution::job::JobErrorKind::Permanent
        );

        let mut output = output;
        output.path_prefix = None;
        let error = expand_selection(&output, vec!["/out/a.txt".to_string()], &[]).unwrap_err();
        assert_eq!(
            error.kind,
            aruna_core::structs::execution::job::JobErrorKind::Permanent
        );
    }

    #[test]
    fn output_limit_errors() {
        let mut outputs = Vec::new();
        let mut keys = HashSet::new();
        for index in 0..MAX_EXECUTION_OUTPUTS {
            insert_output(&mut outputs, &mut keys, output(&index.to_string())).unwrap();
        }
        insert_output(&mut outputs, &mut keys, output("0")).unwrap();
        let error = insert_output(&mut outputs, &mut keys, output("overflow")).unwrap_err();
        assert_eq!(
            error.kind,
            aruna_core::structs::execution::job::JobErrorKind::Permanent
        );
        assert_eq!(outputs.len(), MAX_EXECUTION_OUTPUTS);
    }
}
