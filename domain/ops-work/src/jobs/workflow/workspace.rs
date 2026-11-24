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
use aruna_core::structs::execution::job::family::MAX_EXECUTION_OUTPUTS;
use aruna_core::structs::execution::job::input::{
    InputSelection, InputSource, OutputDestination, OutputSelection,
};
use aruna_core::structs::execution::job::local::{
    AttemptControl, JobError, JobRecord, OutputObject,
};
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BackendLocation, CONTENT_TYPE_KEY, HashIndex, ensure_confined_path, key_content_type,
};
use aruna_core::structs::storage::encryption::BucketKeyError;
use aruna_core::structs::storage::replication::ReplicationFailure;
use ulid::Ulid;

use crate::jobs::lifecycle::stage::stage_error;
use aruna_core::bucket_read::GetBucketError;
use aruna_core::replication_protocol::BaoReadRefusal;
use aruna_ops_data::replication::bao_read::BaoReadError;
use aruna_ops_data::s3::object::get::GetObjectError;
use aruna_ops_data::s3::object::put::{PutObjectError, PutObjectResult};
use aruna_ops_identity::groups::routing::RoutingInputsError;
use aruna_ops_identity::placement::policy::gate::GateContextError;

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
    } = &input.source
    else {
        return Err(JobError::permanent("an inline input names no object"));
    };
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

/// The bucket and key of an output; a run area output names none before the executing node
/// binds it.
pub(super) fn s3_destination(selection: &OutputSelection) -> Result<(&str, &str), JobError> {
    match &selection.destination {
        OutputDestination::S3 { bucket, key } => Ok((bucket, key)),
        OutputDestination::RunArea { .. } => Err(JobError::permanent(
            "a run area output was not bound to the executing node",
        )),
    }
}

pub(super) fn destination_of(
    selection: &OutputSelection,
) -> Result<(NodeId, String, String), JobError> {
    let (bucket, key) = s3_destination(selection)?;
    let node_id = selection
        .destination_node_id
        .ok_or_else(|| JobError::permanent("output destination endpoint is missing"))?;
    Ok((node_id, bucket.to_owned(), key.to_owned()))
}

/// One selection per destination object: a file several globs match is captured once,
/// and two files that would land on one object fail the job instead of losing one.
pub(super) fn distinct_selections(
    selections: Vec<OutputSelection>,
) -> Result<Vec<OutputSelection>, JobError> {
    let mut sources: HashMap<(Option<NodeId>, String, String), String> = HashMap::new();
    let mut distinct = Vec::with_capacity(selections.len());
    for selection in selections {
        let (bucket, key) = s3_destination(&selection)?;
        let target = (
            selection.destination_node_id,
            bucket.to_owned(),
            key.to_owned(),
        );
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
    let (bucket, key) = s3_destination(output)?;
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
                    bucket: bucket.to_owned(),
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
    pub(super) fn from_local(get: aruna_ops_data::s3::object::get::GetObjectResult) -> Self {
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
#[path = "workspace_tests.rs"]
mod tests;
