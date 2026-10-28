//! Takes in one external submission: the receiver commits it when its view makes it the holder.
//! Otherwise it forwards the submission one hop and writes nothing; a device always forwards.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::JobRecordFrame;
use aruna_core::errors::StorageError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::{
    CapturedInput, ExecutionSpec, InputContent, InputSelection, JobAdmissionRecord, JobFamilyId,
    JobFamilyRecord, JobId, JobRecordEnvelope, JobRecordKind, JobRetryPolicy, LogicalJobSpec,
    LogicalJobState, OutputDestination, SubmissionClaim, SubmissionId, WorkspaceMode, spec_entries,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::structs::storage::origin::{SourcePin, public_source_url, publishable};
use aruna_core::structs::storage::read_grant::{GrantScope, ReadGrant};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::time::unix_timestamp_millis;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, warn};
use ulid::Ulid;

use super::admit::{
    AdmissionCandidate, AdmitSubmissionConfig, AdmitSubmissionOperation, AdmittedSubmission,
};
use super::ids::{
    RequestIdentity, SubmissionRequest, SubmissionScope, effective_resources, store_workspace,
};
use super::witness::arm_family;
use super::{LifecycleError, ids};
use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::forward::authorize::{is_sync_eligible, peer_acts_for};
use crate::jobs::records::keys::{family_prefix, kind_prefix};
use crate::jobs::records::pages::{load_chain, page_request};
use crate::jobs::records::rows::ProjectionCache;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::records::{FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation};
use crate::jobs::service::{mint_local_job, validate_execution};
use crate::jobs::source_read::{
    HEAD_OPERATION, READ_OPERATION, SourceReadError, authorize_read, issue_grant, public_read,
};
use crate::jobs::submit::SubmitJobError;
use crate::metadata::api::load_realm_config;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::{AuthToken, WritePeerError};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};

/// Launches one witness may spend on a request over its whole lifetime. It is
/// stored in the immutable spec, so a later config change cannot widen it.
pub const LAUNCHES_PER_WITNESS: u32 = 3;

/// What the caller learns about an accepted submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptedSubmission {
    /// The canonical alias of this request family at the accepting holder.
    pub job_id: JobId,
    /// False when a matching claim already existed.
    pub created: bool,
    pub submission_id: SubmissionId,
    /// The family's state at this accept: `Queued` for a fresh admission, and
    /// what this holder currently reduces for a replay of a request that may
    /// already be running or finished.
    pub state: LogicalJobState,
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

/// The inputs an ingress asks their source node to resolve and grant for one caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputResolveRequest {
    pub created_by: UserId,
    pub submission_id: SubmissionId,
    /// Unix seconds the grants end at, bounded by this node's job retention.
    pub expires_at: u64,
    pub inputs: Vec<InputSelection>,
}

/// Resolves inputs this node holds for an ingress. The caller's bearer is validated here, so
/// the grants vouch for an identity this node checked, never for one a peer asserted. Each
/// answer is the input after its links, pinned to its target here, with that capture.
pub async fn serve_input_resolve(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    request: InputResolveRequest,
) -> Result<Vec<(InputSelection, CapturedInput)>, SubmissionRefusal> {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(SubmissionRefusal::Unavailable);
    };
    if !matches!(auth_token, AuthToken::Bearer(_)) {
        return Err(SubmissionRefusal::Unauthorized);
    }
    let auth = metadata
        .authorize_write_peer(peer, Some(auth_token))
        .await
        .map_err(|error| match error {
            WritePeerError::Unauthorized => SubmissionRefusal::Unauthorized,
            WritePeerError::Unavailable(_) => SubmissionRefusal::Unavailable,
        })?;
    let (config, local) = local_view(context)
        .await
        .map_err(|_| SubmissionRefusal::Unavailable)?;
    // Only a realm node resolves for an ingress; a device forwards its whole submission.
    resolver_admits(&auth, request.created_by, &config, peer)?;
    if request
        .inputs
        .iter()
        .any(|input| input.source_node_id.is_some_and(|node| node != local))
    {
        return Err(SubmissionRefusal::Invalid {
            reason: "an input names another node".to_string(),
        });
    }
    let bound = unix_timestamp_millis().saturating_add(crate::jobs::JOB_RETENTION_MS) / 1000;
    let caller = (
        request.created_by,
        request.submission_id,
        request.expires_at.min(bound),
    );
    let mut answers = Vec::with_capacity(request.inputs.len());
    for input in request.inputs {
        let (mut input, link) = follow_link(context, input, caller.0, local)
            .await
            .map_err(refusal_of)?;
        if input.source_node_id.is_some_and(|node| node != local) {
            return Err(SubmissionRefusal::Invalid {
                reason: format!("{}: a link here names another node", input.dest_key),
            });
        }
        input.source_node_id = Some(local);
        let captured = capture_inputs(context, std::slice::from_ref(&input), caller, local)
            .await
            .map_err(refusal_of)?;
        let Some(mut captured) = captured.into_iter().next() else {
            return Err(SubmissionRefusal::Unavailable);
        };
        captured.link = link;
        answers.push((input, captured));
    }
    Ok(answers)
}

/// Whether `node` answered `asked` faithfully: the same destination, pinned to the version it
/// captured there, and a new source only behind a link that names the asked version there.
fn answers_input(
    asked: &InputSelection,
    (selection, captured): &(InputSelection, CapturedInput),
    node: NodeId,
) -> bool {
    use aruna_core::structs::execution::job::InputSource;
    let pin = |input: &InputSelection| {
        let InputSource::S3 { version_id, .. } = &input.source;
        version_id
            .as_deref()
            .and_then(|v| Ulid::from_string(v).ok())
    };
    let InputSource::S3 { bucket, key, .. } = &asked.source;
    let source_kept = match &captured.link {
        None => selection.source == asked.source,
        Some(link) => {
            link.node_id == node
                && link.bucket == *bucket
                && link.key == *key
                && Some(link.version) == pin(asked)
        }
    };
    source_kept
        && pin(selection) == Some(captured.version_id)
        && selection.dest_key == asked.dest_key
        && selection.mode == asked.mode
        && selection.container_path == asked.container_path
        && selection.source_node_id == Some(node)
        && captured.destination_key == asked.dest_key
        && captured.source_node_id == node
}

/// Whether a validated bearer may have inputs resolved and granted for it: its own user, this
/// realm, a realm node asking, and no path restriction. Grants and reads run unrestricted, so a
/// restricted bearer is refused, as submissions refuse restricted credentials.
fn resolver_admits(
    auth: &AuthContext,
    created_by: UserId,
    config: &RealmConfigDocument,
    peer: NodeId,
) -> Result<(), SubmissionRefusal> {
    match auth.user_id == created_by
        && auth.realm_id == config.realm_id
        && auth.path_restrictions.is_none()
        && is_sync_eligible(config, peer)
    {
        true => Ok(()),
        false => Err(SubmissionRefusal::Unauthorized),
    }
}

/// Normalizes, authorizes, and admits one external execution request.
#[allow(clippy::too_many_arguments)]
pub async fn submit_external_job(
    context: &DriverContext,
    mut spec: ExecutionSpec,
    created_by: UserId,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<String>,
    retention_ms: u64,
    auth_token: Option<AuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    validate_execution(&mut spec, workspace_mode, workspace_bucket.as_deref())?;
    store_workspace(&mut spec, workspace_mode, workspace_bucket.clone())
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
    ids::required_labels(&spec)
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
    let (config, local) = local_view(context).await?;
    let scope = match idempotency_key {
        Some(key) => SubmissionScope::Keyed(key),
        None => SubmissionScope::Unkeyed(Ulid::generate()),
    };
    if is_device(&config, local) {
        let request = SubmissionRequest {
            created_by,
            spec,
            scope,
            retention_ms,
            ingress_node_id: local,
            captured_inputs: Vec::new(),
            output_policies: Vec::new(),
            input_pages: None,
        };
        return forward_device(context, request, &config, local, auth_token).await;
    }
    pin_outputs(&mut spec, local);
    let (inputs, captured_inputs, output_policies) = resolve_inputs(
        context,
        &spec,
        caller_of(created_by, &scope, retention_ms),
        (local, auth_token.as_ref()),
    )
    .await?;
    // Directory inputs are stored as the files they expanded to.
    spec.inputs = inputs;
    let mut request = SubmissionRequest {
        created_by,
        spec,
        scope,
        retention_ms,
        ingress_node_id: local,
        captured_inputs,
        output_policies,
        input_pages: None,
    };
    let identity = request.identity().map_err(SubmitJobError::Conversion)?;
    page_inputs(context, &identity, &mut request)
        .await
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let view = family_view(&config, &identity)?;
    if view.holds(local) {
        return admit_here(context, &request, &identity, &config, local).await;
    }
    forward_once(context, &request, &identity, &view, local, auth_token).await
}

/// Whether `node_id` is a user device: it carries no shared realm
/// responsibility, holds nothing, and therefore admits nothing.
fn is_device(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    !is_sync_eligible(config, node_id)
}

/// Pins every declared output to the node that will admit the request and write
/// its results.
fn pin_outputs(spec: &mut ExecutionSpec, node_id: NodeId) {
    for output in &mut spec.file_outputs {
        output.destination_node_id = Some(node_id);
    }
    let (mode, bucket) = ids::workspace_of(spec);
    if mode == WorkspaceMode::Existing
        && let Some(bucket) = bucket
    {
        spec.resolve_outputs(&bucket, node_id);
    }
}

/// A device forwards without resolving anything: it holds none of the objects it
/// names, so only the reference shape is checked and the holder resolves the
/// rest; holder selection follows the submission id alone.
async fn forward_device(
    context: &DriverContext,
    request: SubmissionRequest,
    config: &RealmConfigDocument,
    local: NodeId,
    auth_token: Option<AuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    reference_shape(&request.spec)?;
    // A device may not assert an auth context for the realm, so the caller's own
    // bearer token is the only credential a holder accepts from it.
    if !matches!(auth_token, Some(AuthToken::Bearer(_))) {
        return Err(SubmitJobError::AuthorityDenied);
    }
    let identity = request.identity().map_err(SubmitJobError::Conversion)?;
    let view = family_view(config, &identity)?;
    forward_once(context, &request, &identity, &view, local, auth_token).await
}

/// Validates the shape of every input reference without requiring the object to
/// be present here. Local presence is the admitting holder's check.
fn reference_shape(spec: &ExecutionSpec) -> Result<(), SubmitJobError> {
    for input in &spec.inputs {
        let aruna_core::structs::execution::job::InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        if bucket.trim().is_empty() || key.trim().is_empty() {
            return Err(SubmitJobError::InvalidWorkspace(
                "input reference needs a bucket and a key".to_string(),
            ));
        }
        if let Some(version) = version_id {
            Ulid::from_string(version)
                .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
        }
    }
    Ok(())
}

/// How a refusal names the input it is about. Only the object reference is
/// named; a credential never reaches a reason string.
fn input_reference(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    match version_id {
        Some(version) => format!("s3://{bucket}/{key}?versionId={version}"),
        None => format!("s3://{bucket}/{key}"),
    }
}

/// A reference version without a content hash, accepted only when GetObject can serve it at
/// that exact version: a strong ETag on a source that enforces `If-Match` binds every range,
/// recorded digests bind a whole read. Anything else is not materialized.
fn reference_content(
    reference: &str,
    head: &crate::s3::object::head::HeadObjectResult,
) -> Result<(InputContent, Option<SourcePin>), SubmitJobError> {
    let refused = |reason: &str| SubmitJobError::InvalidWorkspace(format!("{reference}: {reason}"));
    let (Some(binding), Some(metadata), None) =
        (&head.source_binding, &head.source_metadata, &head.location)
    else {
        return Err(refused("input is not materialized"));
    };
    let range_bound = binding.descriptor.kind.enforces_if_match()
        && crate::s3::object::get::strong_validator(metadata.etag.as_deref());
    if !range_bound && metadata.enforced_checksums().is_empty() {
        return Err(refused(
            "input reference cannot be read at its exact version",
        ));
    }
    let descriptor = &binding.descriptor;
    let pin = SourcePin {
        url: publishable(descriptor)
            .then(|| public_source_url(descriptor))
            .flatten(),
        path: descriptor.source_path.clone(),
        source_version: descriptor.version_selector.clone(),
    };
    let content = InputContent::Reference {
        etag: metadata.etag.clone(),
        range_bound,
    };
    Ok((content, Some(pin)))
}

/// Who resolves inputs: the caller, the submission its grants name, and when they end.
fn caller_of(
    created_by: UserId,
    scope: &SubmissionScope,
    retention_ms: u64,
) -> (UserId, SubmissionId, u64) {
    let expires_at = (unix_timestamp_millis().saturating_add(retention_ms)) / 1000;
    (
        created_by,
        ids::submission_of(created_by, scope),
        expires_at,
    )
}

/// A denied read is the caller's own; anything undecided may clear on a retry.
fn source_error(reference: &str, error: SourceReadError) -> SubmitJobError {
    match error {
        SourceReadError::NotFound => {
            SubmitJobError::InvalidWorkspace(format!("{reference}: input bucket not found"))
        }
        SourceReadError::Denied => SubmitJobError::InputDenied(reference.to_string()),
        SourceReadError::Unavailable(error) => {
            SubmitJobError::PlacementUnavailable(format!("{reference}: {error}"))
        }
    }
}

/// Resolve every input at the node that holds it, once, at ingress. The values are stored in
/// the family spec and remain valid when admission or planning is forwarded.
async fn resolve_inputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    caller: (UserId, SubmissionId, u64),
    (local, auth_token): (NodeId, Option<&AuthToken>),
) -> Result<
    (
        Vec<InputSelection>,
        Vec<CapturedInput>,
        Vec<aruna_core::structs::placement::policy::PlacementPolicyRef>,
    ),
    SubmitJobError,
> {
    let mut remote: std::collections::BTreeMap<NodeId, Vec<InputSelection>> = Default::default();
    let mut here = Vec::new();
    for input in &spec.inputs {
        match input.source_node_id.filter(|node| *node != local) {
            Some(node) => remote.entry(node).or_default().push(input.clone()),
            None => here.push(input.clone()),
        }
    }
    let mut files = Vec::with_capacity(here.len());
    for input in here {
        match is_directory(&input) {
            true => files.extend(expand_directory(context, &input, caller.0, local).await?),
            false => files.push(input),
        }
    }
    // A link resolves to its target; the target may live on another node.
    let mut links = std::collections::BTreeMap::new();
    let mut here = Vec::with_capacity(files.len());
    for input in files {
        let (input, link) = follow_link(context, input, caller.0, local).await?;
        if let Some(link) = link {
            links.insert(input.dest_key.clone(), link);
        }
        match input.source_node_id.filter(|node| *node != local) {
            Some(node) => remote.entry(node).or_default().push(input),
            None => here.push(input),
        }
    }
    let mut captured = capture_inputs(context, &here, caller, local).await?;
    let mut remote_inputs = Vec::new();
    for (node, inputs) in remote {
        for (input, capture) in remote_captures(context, node, &inputs, caller, auth_token).await? {
            remote_inputs.push(input);
            captured.push(capture);
        }
    }
    let mut by_key: std::collections::BTreeMap<String, CapturedInput> = captured
        .into_iter()
        .map(|captured| (captured.destination_key.clone(), captured))
        .collect();
    let captured_inputs = here
        .iter()
        .chain(remote_inputs.iter())
        .filter_map(|input| by_key.remove(&input.dest_key))
        .map(|mut captured: CapturedInput| {
            // The first link the walk met wins, here or at the source node.
            captured.link = links.remove(&captured.destination_key).or(captured.link);
            captured
        })
        .collect();
    let output_policies = resolve_outputs(context, spec, local).await?;
    Ok((
        here.into_iter().chain(remote_inputs).collect(),
        captured_inputs,
        output_policies,
    ))
}

/// Follows links an input names on this node to the version they end at, which the caller
/// must be able to read like any input. Returns the input of the target and the first link.
/// A link to a link is followed; a cycle is refused; a target elsewhere ends the walk here.
async fn follow_link(
    context: &DriverContext,
    mut input: InputSelection,
    created_by: UserId,
    local: NodeId,
) -> Result<(InputSelection, Option<VersionedObjectArn>), SubmitJobError> {
    use aruna_core::structs::execution::job::InputSource;
    use aruna_core::structs::storage::link::LinkTarget;
    let realm_id = context.serving_realm(created_by.realm_id);
    let auth = AuthContext {
        user_id: created_by,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let mut first = None;
    let mut seen = std::collections::BTreeSet::new();
    while input.source_node_id.is_none_or(|node| node == local) {
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        let reference = input_reference(bucket, key, version_id.as_deref());
        let version = version_id
            .as_deref()
            .and_then(|version| Ulid::from_string(version).ok());
        let head = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version,
            }),
            context,
        )
        .await;
        let Ok(head) = head else {
            break;
        };
        let Some(target) =
            (head.source_binding.as_ref()).and_then(|binding| LinkTarget::of(binding, realm_id))
        else {
            break;
        };
        // The link itself must be readable by the caller before its target is.
        authorize_read(context, &auth, local, (bucket, key), READ_OPERATION)
            .await
            .map_err(|error| source_error(&reference, error))?;
        let link = VersionedObjectArn {
            realm_id,
            node_id: local,
            bucket: bucket.clone(),
            key: key.clone(),
            version: head
                .resolved_version_id
                .or(head.version_id)
                .unwrap_or(target.arn.version),
        };
        if !seen.insert(link.to_w3id()) {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "{reference}: the links form a cycle"
            )));
        }
        first.get_or_insert(link);
        input.source = InputSource::S3 {
            bucket: target.arn.bucket.clone(),
            key: target.arn.key.clone(),
            version_id: Some(target.arn.version.to_string()),
        };
        input.source_node_id = (target.arn.node_id != local).then_some(target.arn.node_id);
    }
    Ok((input, first))
}

/// A directory input names a key prefix, marked by its trailing `/`.
fn is_directory(input: &InputSelection) -> bool {
    let aruna_core::structs::execution::job::InputSource::S3 { key, .. } = &input.source;
    key.ends_with('/') && input.dest_key.ends_with('/')
}

/// One pinned input per object below a directory input's prefix, at the current version, each
/// below the directory's container path. An object the caller may not read refuses the whole
/// directory without naming it; a distributed bucket's prefix spans other members' ranges.
async fn expand_directory(
    context: &DriverContext,
    input: &InputSelection,
    created_by: UserId,
    local: NodeId,
) -> Result<Vec<InputSelection>, SubmitJobError> {
    use crate::s3::object::list::{ListBucketInput, ListBucketOperation};
    use aruna_core::structs::execution::job::InputSource;
    let InputSource::S3 {
        bucket,
        key: prefix,
        ..
    } = &input.source;
    let reference = format!("s3://{bucket}/{prefix}");
    let info = drive(GetBucketOperation::new(bucket.clone()), context)
        .await
        .map_err(|_| {
            SubmitJobError::InvalidWorkspace(format!("{reference}: input bucket not found"))
        })?;
    if info.distribution.is_some() {
        return Err(SubmitJobError::InvalidWorkspace(format!(
            "{reference}: directory inputs of a distributed bucket are not supported"
        )));
    }
    let directory = input
        .container_path
        .as_deref()
        .unwrap_or("/")
        .trim_end_matches('/');
    let auth = AuthContext {
        user_id: created_by,
        realm_id: context.serving_realm(created_by.realm_id),
        path_restrictions: None,
        session: None,
    };
    let mut files = Vec::new();
    let mut continuation_token = None;
    loop {
        let page = drive(
            ListBucketOperation::new(ListBucketInput {
                bucket: bucket.clone(),
                group_id: info.group_id,
                continuation_token,
                max_keys: Some(ListBucketOperation::DEFAULT_MAX_KEYS),
                prefix: Some(prefix.clone()),
                delimiter: None,
                start_after: None,
            }),
            context,
        )
        .await
        .map_err(|error| SubmitJobError::PlacementUnavailable(format!("{reference}: {error}")))?;
        for object in page.objects {
            let relative = &object.head.key[prefix.len()..];
            if relative.is_empty() || relative.ends_with('/') {
                continue;
            }
            let readable = (object.head.bucket.as_str(), object.head.key.as_str());
            authorize_read(context, &auth, local, readable, READ_OPERATION)
                .await
                .map_err(|error| source_error(&reference, error))?;
            files.push(InputSelection {
                source: InputSource::S3 {
                    bucket: bucket.clone(),
                    key: object.head.key.clone(),
                    version_id: Some(object.version_id.to_string()),
                },
                source_node_id: input.source_node_id,
                dest_key: format!("{}{relative}", input.dest_key),
                mode: input.mode,
                container_path: Some(format!("{directory}/{relative}")),
                name: None,
                description: None,
            });
        }
        match page.continuation_token {
            Some(token) => continuation_token = Some(token),
            None => break,
        }
    }
    if files.is_empty() {
        return Err(SubmitJobError::InvalidWorkspace(format!(
            "{reference}: the directory input holds no objects"
        )));
    }
    Ok(files)
}

/// The inputs of another node, resolved and granted by that node for the caller's own bearer.
/// Every input must name its exact version, so nothing depends on a remote head; the answer
/// is each input after the links that node followed, with its capture.
async fn remote_captures(
    context: &DriverContext,
    node: NodeId,
    inputs: &[InputSelection],
    (created_by, submission_id, expires_at): (UserId, SubmissionId, u64),
    auth_token: Option<&AuthToken>,
) -> Result<Vec<(InputSelection, CapturedInput)>, SubmitJobError> {
    for input in inputs {
        let aruna_core::structs::execution::job::InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        version_id
            .as_deref()
            .and_then(|version| Ulid::from_string(version).ok())
            .ok_or_else(|| {
                SubmitJobError::InvalidWorkspace(format!(
                    "s3://{bucket}/{key}: an input on another node must name its exact version"
                ))
            })?;
    }
    let (Some(metadata), Some(AuthToken::Bearer(token))) =
        (context.metadata_handle.as_ref(), auth_token)
    else {
        return Err(SubmitJobError::PlacementUnavailable(
            "an input on another node needs the caller's bearer token".to_string(),
        ));
    };
    let request = InputResolveRequest {
        created_by,
        submission_id,
        expires_at,
        inputs: inputs.to_vec(),
    };
    let answer = metadata
        .request_input_resolve(node, AuthToken::Bearer(token.clone()), request)
        .await
        .map_err(|error| {
            SubmitJobError::PlacementUnavailable(format!("input node {node}: {error}"))
        })?;
    let captured = match answer {
        Ok(captured) => captured,
        Err(SubmissionRefusal::Invalid { reason }) => {
            return Err(SubmitJobError::InvalidWorkspace(reason));
        }
        Err(SubmissionRefusal::Unauthorized) => {
            return Err(SubmitJobError::InputDenied(format!(
                "inputs on node {node}"
            )));
        }
        Err(refusal) => {
            return Err(SubmitJobError::PlacementUnavailable(format!(
                "input node {node} refused: {refusal:?}"
            )));
        }
    };
    // The node answers for exactly the asked inputs, each pinned to a version it holds.
    let exact = captured.len() == inputs.len()
        && (captured.iter().zip(inputs)).all(|(answer, asked)| answers_input(asked, answer, node));
    match exact {
        true => Ok(captured),
        false => Err(SubmitJobError::PlacementUnavailable(format!(
            "input node {node} answered other versions"
        ))),
    }
}

/// Captures inputs this node holds: each is read like an S3 GET by the caller, pinned to an
/// exact version and granted for later exact reads.
async fn capture_inputs(
    context: &DriverContext,
    inputs: &[InputSelection],
    (created_by, submission_id, expires_at): (UserId, SubmissionId, u64),
    local: NodeId,
) -> Result<Vec<CapturedInput>, SubmitJobError> {
    let mut captured_inputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let aruna_core::structs::execution::job::InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        // Every refusal names the input it is about: the object store's own
        // message says only that some key is missing.
        let reference = input_reference(bucket, key, version_id.as_deref());
        let requested = version_id
            .as_deref()
            .map(Ulid::from_string)
            .transpose()
            .map_err(|error| SubmitJobError::InvalidWorkspace(format!("{reference}: {error}")))?;
        let head = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: requested,
            }),
            context,
        )
        .await
        // A definitive miss is the submitter's error, never a retryable 503.
        .map_err(|error| match error {
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker => {
                SubmitJobError::InvalidWorkspace(format!("{reference}: input object not found"))
            }
            other => SubmitJobError::PlacementUnavailable(format!("{reference}: {other}")),
        })?;
        let version = head
            .resolved_version_id
            .or(head.version_id)
            .ok_or_else(|| {
                SubmitJobError::InvalidWorkspace(format!("{reference}: input has no version"))
            })?;
        let blake3 = head
            .location
            .as_ref()
            .and_then(|location| location.hashes.get(HASH_BLAKE3))
            .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok());
        let blake3 = match blake3 {
            Some(hash) => Some(hash),
            None => crate::jobs::lifecycle::plan::version_hash(context, bucket, key, version).await,
        };
        let (content, source_pin) = match blake3 {
            Some(hash) => (InputContent::Blake3(hash), None),
            None => reference_content(&reference, &head)?,
        };
        let bytes = head
            .location
            .as_ref()
            .map(|location| location.blob_size)
            .or_else(|| {
                head.source_metadata
                    .as_ref()
                    .map(|metadata| metadata.content_length)
            })
            .unwrap_or_default();
        let policies = aruna_core::structs::placement::policy::PlacementPolicyRef::canonical_set(
            &head.source_policies,
        )
        .map_err(|error| SubmitJobError::InvalidWorkspace(format!("{reference}: {error}")))?;
        // The caller must read the input like an S3 GET of it, in whatever group it lives.
        let realm_id = context.serving_realm(created_by.realm_id);
        let auth = AuthContext {
            user_id: created_by,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let object = (bucket.as_str(), key.as_str());
        let source_group_id = authorize_read(context, &auth, local, object, READ_OPERATION)
            .await
            .map_err(|error| source_error(&reference, error))?;
        let public = public_read(context, realm_id, (local, source_group_id), object)
            .await
            .map_err(|error| source_error(&reference, error))?;
        // This node validated the caller itself, so its grant vouches for exact reads later.
        let grant = issue_grant(
            context,
            ReadGrant {
                realm_id,
                issuer: local,
                submission_id,
                user_id: created_by,
                restrictions: None,
                scope: GrantScope::Version(VersionedObjectArn {
                    realm_id,
                    node_id: local,
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version,
                }),
                operations: vec![READ_OPERATION.to_string(), HEAD_OPERATION.to_string()],
                expires_at,
            },
        );
        captured_inputs.push(CapturedInput {
            destination_key: input.dest_key.clone(),
            source_node_id: local,
            version_id: version,
            content,
            bytes,
            policies,
            source_group_id,
            public,
            link: None,
            source_pin,
            database: None,
            releases: Vec::new(),
            // Release membership is not checked until the provenance lookup lands.
            releases_complete: false,
            grant,
        });
    }
    Ok(captured_inputs)
}

/// The placement refs of every bucket the run writes into; all live on the ingress node.
async fn resolve_outputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    local: NodeId,
) -> Result<Vec<aruna_core::structs::placement::policy::PlacementPolicyRef>, SubmitJobError> {
    let mut buckets = std::collections::BTreeSet::new();
    for output in &spec.file_outputs {
        if output.destination_node_id != Some(local) {
            return Err(SubmitJobError::InvalidWorkspace(
                "output destination endpoint is invalid".to_string(),
            ));
        }
        let OutputDestination::S3 { bucket, .. } = &output.destination;
        buckets.insert(bucket.clone());
    }
    let (mode, workspace) = ids::workspace_of(spec);
    if mode == WorkspaceMode::Existing
        && let Some(bucket) = workspace
    {
        buckets.insert(bucket);
    }
    let mut output_policies = Vec::new();
    for bucket in buckets {
        let info = match drive(GetBucketOperation::new(bucket.clone()), context).await {
            Ok(info) => info,
            Err(GetBucketError::NotFound) => {
                return Err(SubmitJobError::InvalidWorkspace(format!(
                    "s3://{bucket}: output bucket not found"
                )));
            }
            Err(
                error @ (GetBucketError::ConversionError(_)
                | GetBucketError::InvalidStateEvent { .. }),
            ) => {
                return Err(SubmitJobError::InvalidWorkspace(format!(
                    "s3://{bucket}: {error}"
                )));
            }
            Err(error) => {
                return Err(SubmitJobError::PlacementUnavailable(format!(
                    "s3://{bucket}: {error}"
                )));
            }
        };
        if info.group_id != spec.group_id {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "s3://{bucket}: output bucket is outside the execution group"
            )));
        }
        output_policies.extend(info.placement_policies);
    }
    output_policies =
        aruna_core::structs::placement::policy::PlacementPolicyRef::canonical_set(&output_policies)
            .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
    Ok(output_policies)
}

/// The realm config this node synchronized plus its own identity.
async fn local_view(
    context: &DriverContext,
) -> Result<(RealmConfigDocument, NodeId), SubmitJobError> {
    let net = context.net_handle.as_ref().ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("network handle unavailable".to_string())
    })?;
    let realm_id = *net.realm_id();
    let config = load_realm_config(context, realm_id).await.ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("realm config unavailable".to_string())
    })?;
    Ok((config, net.node_id()))
}

/// The unconflicted family view. A view this node cannot resolve is an
/// availability failure, never an accepted job.
fn family_view(
    config: &RealmConfigDocument,
    identity: &RequestIdentity,
) -> Result<FamilyView, SubmitJobError> {
    FamilyView::resolve(config, config.realm_id, identity.family()).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("job family placement is unavailable".to_string())
    })
}

/// Mints the alias, signs the immutable spec and its claim, and commits them.
/// The answered alias is canonical at this accept: a fresh admission holds the
/// only claim, and a replay settles on the already-reduced claim.
async fn admit_here(
    context: &DriverContext,
    request: &SubmissionRequest,
    identity: &RequestIdentity,
    config: &RealmConfigDocument,
    local: NodeId,
) -> Result<AcceptedSubmission, SubmitJobError> {
    let now_ms = unix_timestamp_millis();
    let placement = config
        .family_placement(identity.submission_id)
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let job_id = mint_local_job(
        context,
        config.realm_id,
        local,
        Some(&identity.submission_id.0),
    )
    .await?;
    let spec = LogicalJobSpec {
        submission_id: identity.submission_id,
        job_id,
        origin_node_id: local,
        ingress_node_id: request.ingress_node_id,
        realm_id: config.realm_id,
        group_id: request.spec.group_id,
        created_by: request.created_by,
        created_at_ms: now_ms,
        payload: request.spec.clone(),
        request_digest: identity.request_digest,
        spec_digest: [0u8; 32],
        resources: effective_resources(&request.spec),
        retention_ms: request.retention_ms,
        retry: JobRetryPolicy {
            launches_per_witness: LAUNCHES_PER_WITNESS,
        },
        admission: JobAdmissionRecord {
            submission_id: identity.submission_id,
            request_digest: identity.request_digest,
            job_id,
            group_id: request.spec.group_id,
            admitting_node_id: local,
            // The realm config publishes no membership generation yet; the
            // departure round fills it. Zero is unstamped, not an epoch claim.
            membership_generation: 0,
            resources: effective_resources(&request.spec),
            admitted_at_ms: now_ms,
        },
        captured_inputs: request.captured_inputs.clone(),
        input_pages: request.input_pages,
        output_policies: request.output_policies.clone(),
        placement,
    }
    .store_digest()
    .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let claim = SubmissionClaim {
        submission_id: identity.submission_id,
        job_id,
        request_digest: identity.request_digest,
        spec_digest: spec.spec_digest,
        committing_node_id: local,
        accepted_at_ms: now_ms,
    };
    let spec_frame = sign_frame(
        context,
        config.realm_id,
        JobFamilyRecord::Spec(Box::new(spec)),
    )?;
    let claim_frame = sign_frame(context, config.realm_id, JobFamilyRecord::Claim(claim))?;
    let candidate = AdmissionCandidate {
        job_id,
        spec: spec_frame,
        claim: claim_frame,
    };
    let admitted =
        admit_with_quota(context, config, local, request, identity, candidate, now_ms).await?;
    let state = match admitted.created {
        true => LogicalJobState::Queued,
        false => observed_state(context, admitted.family).await,
    };
    // Scheduling and replication are armed only after the claim is durable, so
    // a failed commit never leaves a round pointing at a family that is absent.
    arm_family(context, identity.family(), now_ms).await;
    super::outbox::kick(context).await;
    Ok(AcceptedSubmission {
        job_id: admitted.job_id,
        created: admitted.created,
        submission_id: identity.submission_id,
        state,
    })
}

/// The state this holder currently reduces for `family`. The cached projection
/// answers when current; otherwise the family is reduced once from its records.
/// A family this node cannot fully reduce is reported as indeterminate.
async fn observed_state(context: &DriverContext, family: JobFamilyId) -> LogicalJobState {
    let cached = match cached_projection(context, &family).await {
        Ok(cached) => cached,
        Err(error) => {
            warn!(error = %error, "Replayed submission could not read its projection cache");
            None
        }
    };
    let projection = match cached {
        Some(cache) if !cache.stale => cache.projection,
        _ => match drive(
            ProjectFamilyOperation::new(ProjectFamilyConfig {
                family: FamilyRef::Family(family),
                now_ms: unix_timestamp_millis(),
                rebuild: false,
            }),
            context,
        )
        .await
        {
            Ok(projected) if projected.truncated => {
                warn!("Replayed submission reduced only part of its family");
                None
            }
            Ok(projected) => projected.projection,
            Err(error) => {
                warn!(error = %error, "Replayed submission could not reduce its family");
                None
            }
        },
    };
    projection.map_or(LogicalJobState::Indeterminate, |projection| {
        projection.state
    })
}

async fn cached_projection(
    context: &DriverContext,
    family: &JobFamilyId,
) -> Result<Option<ProjectionCache>, String> {
    let (page, _) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        FAMILY_PROJECTION_KEYSPACE,
        Some(family_prefix(family)),
        None,
        1,
        None,
    )
    .await?;
    Ok(page
        .first()
        .and_then(|(_, value)| ProjectionCache::decode(value)))
}

/// Admission transactions one submission runs before it reports the group as
/// unavailable. Only a conflict with a concurrent admission is retried.
const ADMISSION_ATTEMPTS: usize = 3;

/// Decides the standing quota and commits the admission. A replay is settled
/// from already-held records and never reads the quota view; a lost transaction
/// against a concurrent submission of the same group is retried.
async fn admit_with_quota(
    context: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    request: &SubmissionRequest,
    identity: &RequestIdentity,
    candidate: AdmissionCandidate,
    now_ms: u64,
) -> Result<AdmittedSubmission, SubmitJobError> {
    for attempt in 0..ADMISSION_ATTEMPTS {
        let (quota_refusal, quota_revision) = match has_claim(context, &identity.family()).await? {
            true => (None, None),
            false => crate::jobs::quota::quota_refusal(
                context,
                config,
                local,
                request.spec.group_id,
                &effective_resources(&request.spec),
            )
            .await
            .map_err(SubmitJobError::PlacementUnavailable)?,
        };
        let admitted = drive(
            AdmitSubmissionOperation::new(AdmitSubmissionConfig {
                realm_id: config.realm_id,
                local_node_id: local,
                submission_id: identity.submission_id,
                request_digest: identity.request_digest,
                candidate: Box::new(candidate.clone()),
                now_ms,
                quota_refusal,
                quota_revision,
            }),
            context,
        )
        .await;
        match admitted {
            Ok(admitted) => return Ok(admitted),
            Err(LifecycleError::Storage(StorageError::TransactionConflict))
                if attempt + 1 < ADMISSION_ATTEMPTS =>
            {
                debug!(group_id = %request.spec.group_id, "Retrying an admission a concurrent one won");
            }
            Err(error) => return Err(admission_error(error)),
        }
    }
    Err(SubmitJobError::PlacementUnavailable(
        "admission exhausted conflict retries".to_string(),
    ))
}

/// Whether this node already holds a claim for `family`, which makes the
/// admission a replay that settles inside its own transaction.
async fn has_claim(context: &DriverContext, family: &JobFamilyId) -> Result<bool, SubmitJobError> {
    let (page, _) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        FAMILY_RECORD_KEYSPACE,
        Some(kind_prefix(family, JobRecordKind::Claim)),
        None,
        1,
        None,
    )
    .await
    .map_err(SubmitJobError::PlacementUnavailable)?;
    Ok(!page.is_empty())
}

fn sign_frame(
    context: &DriverContext,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    record: JobFamilyRecord,
) -> Result<JobRecordFrame, SubmitJobError> {
    let net = context.net_handle.as_ref().ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("network handle unavailable".to_string())
    })?;
    let envelope = JobRecordEnvelope::signed_with(realm_id, record, net.node_id(), |message| {
        net.sign(message)
    })
    .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    JobRecordFrame::new(envelope)
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))
}

fn admission_error(error: LifecycleError) -> SubmitJobError {
    match error {
        LifecycleError::IdempotencyConflict { existing_job_id } => {
            SubmitJobError::JobPlanConflict { existing_job_id }
        }
        LifecycleError::QuotaDenied(reason) => SubmitJobError::QuotaDenied(reason),
        LifecycleError::GroupWrite(error) => SubmitJobError::GroupWrite(error),
        error => SubmitJobError::PlacementUnavailable(error.to_string()),
    }
}

/// Forwards the complete request and its preassigned identity one hop. The
/// holder revalidates the caller and recomputes the identity itself, so this
/// node never becomes the authority for a job it does not hold.
async fn forward_once(
    context: &DriverContext,
    request: &SubmissionRequest,
    identity: &RequestIdentity,
    view: &FamilyView,
    local: NodeId,
    auth_token: Option<AuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    let (Some(metadata), Some(auth_token)) = (context.metadata_handle.as_ref(), auth_token) else {
        return Err(SubmitJobError::PlacementUnavailable(
            "forwarding a submission needs the caller's token".to_string(),
        ));
    };
    for holder in view.holders().iter().filter(|holder| **holder != local) {
        let message = MetadataTransportMessage::ForwardJobSubmission {
            auth_token: auth_token.clone(),
            submission_id: identity.submission_id,
            request: Box::new(request.clone()),
        };
        let reply = match metadata.request_forwarded_write(*holder, message).await {
            Ok(reply) => reply,
            Err(error) => {
                debug!(peer = %holder, error = %error, "Submission forwarding failed");
                continue;
            }
        };
        match reply {
            MetadataTransportMessage::ForwardedJobSubmission { result: Ok(ack) } => {
                return Ok(AcceptedSubmission {
                    job_id: ack.job_id,
                    created: ack.created,
                    submission_id: identity.submission_id,
                    state: ack.state,
                });
            }
            MetadataTransportMessage::ForwardedJobSubmission {
                result: Err(SubmissionRefusal::Conflict { existing_job_id }),
            } => return Err(SubmitJobError::JobPlanConflict { existing_job_id }),
            MetadataTransportMessage::ForwardedJobSubmission {
                result: Err(SubmissionRefusal::Unauthorized),
            } => return Err(SubmitJobError::AuthorityDenied),
            // A holder that read the request and refused it answers for every
            // holder; retrying the rest would only repeat the refusal.
            MetadataTransportMessage::ForwardedJobSubmission {
                result: Err(SubmissionRefusal::Invalid { reason }),
            } => return Err(SubmitJobError::InvalidWorkspace(reason)),
            MetadataTransportMessage::ForwardedJobSubmission {
                result: Err(reason),
            } => {
                debug!(peer = %holder, reason = ?reason, "Family holder refused the submission");
            }
            MetadataTransportMessage::ForwardedWriteUnavailable => continue,
            other => warn!(
                peer = %holder,
                reply = crate::metadata::transport_message_kind(&other),
                "Unexpected submission forwarding reply"
            ),
        }
    }
    Err(SubmitJobError::PlacementUnavailable(
        "no job family holder accepted the submission".to_string(),
    ))
}

/// Serves one forwarded submission. The caller's token, the identity, and this
/// node's own holder standing are all revalidated before anything is committed,
/// and the request is never forwarded a second hop.
pub async fn serve_submission(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    submission_id: SubmissionId,
    request: SubmissionRequest,
) -> MetadataTransportMessage {
    let result = admit_forwarded(context, peer, auth_token, submission_id, request).await;
    MetadataTransportMessage::ForwardedJobSubmission { result }
}

async fn admit_forwarded(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    submission_id: SubmissionId,
    mut request: SubmissionRequest,
) -> Result<SubmissionAck, SubmissionRefusal> {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(SubmissionRefusal::Unavailable);
    };
    let auth = metadata
        .authorize_write_peer(peer, Some(auth_token.clone()))
        .await
        .map_err(|error| match error {
            WritePeerError::Unauthorized => SubmissionRefusal::Unauthorized,
            WritePeerError::Unavailable(_) => SubmissionRefusal::Unavailable,
        })?;
    // The forwarded request keeps its own submitter: a relay may not re-attribute
    // a plan to another caller, and the identity is recomputed from that caller.
    if auth.user_id != request.created_by {
        return Err(SubmissionRefusal::Unauthorized);
    }
    let (config, local) = local_view(context)
        .await
        .map_err(|_| SubmissionRefusal::Unavailable)?;
    // The submitter may belong to another realm, but must be signed in to this one.
    if auth.realm_id != config.realm_id {
        return Err(SubmissionRefusal::Unauthorized);
    }
    // A user device is owner-bound: whatever token it presents, the submission it
    // forwards must be its owner's own. Its inputs resolve unrestricted, so the token must be.
    if !peer_acts_for(&config, peer, auth.user_id) || auth.path_restrictions.is_some() {
        return Err(SubmissionRefusal::Unauthorized);
    }
    authorize_group(context, &auth, &request, local)
        .await
        .map_err(|_| SubmissionRefusal::Unauthorized)?;
    // Paged inputs come from the forwarding node, checked page by page against the manifest.
    if let Some(manifest) = request.input_pages.take() {
        let pages = load_chain(context, &submission_id, &manifest, &[peer])
            .await
            .map_err(|_| SubmissionRefusal::Unavailable)?;
        let entries = spec_entries(pages).map_err(|_| SubmissionRefusal::IdentityMismatch)?;
        let (inputs, captured): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        request.spec.inputs = inputs;
        request.captured_inputs = captured;
    }
    // The holder owns what it writes and reads, so a device submission is
    // normalized here rather than trusted from a node that holds none of it.
    let ingress = match is_device(&config, peer) {
        true => {
            pin_device_request(context, &mut request, (local, &auth_token)).await?;
            local
        }
        false => peer,
    };
    if request.captured_inputs.len() != request.spec.inputs.len()
        || request.captured_inputs.iter().any(|captured| {
            !request
                .spec
                .inputs
                .iter()
                .any(|input| input.dest_key == captured.destination_key)
        })
        || request.ingress_node_id != ingress
        || !sources_match(&request.spec.inputs, &request.captured_inputs, ingress)
        || request
            .spec
            .file_outputs
            .iter()
            .any(|output| output.destination_node_id != Some(request.ingress_node_id))
        || (ids::workspace_of(&request.spec).0 == WorkspaceMode::Existing
            && !request.spec.workspace_outputs.is_empty())
    {
        return Err(SubmissionRefusal::IdentityMismatch);
    }
    let identity = request
        .identity()
        .map_err(|error| SubmissionRefusal::Invalid {
            reason: error.to_string(),
        })?;
    if identity.submission_id != submission_id {
        return Err(SubmissionRefusal::IdentityMismatch);
    }
    let view = FamilyView::resolve(&config, config.realm_id, identity.family())
        .ok_or(SubmissionRefusal::Unavailable)?;
    if !view.holds(local) {
        return Err(SubmissionRefusal::NotHolder);
    }
    page_inputs(context, &identity, &mut request)
        .await
        .map_err(|_| SubmissionRefusal::Unavailable)?;
    match admit_here(context, &request, &identity, &config, local).await {
        Ok(accepted) => Ok(SubmissionAck {
            job_id: accepted.job_id,
            created: accepted.created,
            state: accepted.state,
        }),
        Err(SubmitJobError::JobPlanConflict { existing_job_id }) => {
            Err(SubmissionRefusal::Conflict { existing_job_id })
        }
        Err(error) => {
            warn!(error = %error, "Forwarded submission could not be admitted");
            Err(SubmissionRefusal::Unavailable)
        }
    }
}

/// Moves inputs too large for the spec record into pages kept here; the identity is already
/// computed from the full lists, so it covers every page.
async fn page_inputs(
    context: &DriverContext,
    identity: &RequestIdentity,
    request: &mut SubmissionRequest,
) -> Result<(), crate::jobs::records::pages::PageReadError> {
    request.input_pages = page_request(
        context,
        &identity.submission_id,
        &mut request.spec.inputs,
        &mut request.captured_inputs,
    )
    .await?;
    Ok(())
}

/// Whether each capture comes from the node its input names, or from ingress when it names none.
pub(crate) fn sources_match(
    inputs: &[InputSelection],
    captured: &[CapturedInput],
    ingress: NodeId,
) -> bool {
    captured.iter().all(|captured| {
        inputs.iter().any(|input| {
            input.dest_key == captured.destination_key
                && input.source_node_id.unwrap_or(ingress) == captured.source_node_id
        })
    })
}

/// Normalizes a device submission at the holder: the outputs are pinned here and
/// the referenced inputs are resolved against this node's own objects, so the
/// recomputed identity covers names only this node assigned.
async fn pin_device_request(
    context: &Arc<DriverContext>,
    request: &mut SubmissionRequest,
    (local, auth_token): (NodeId, &AuthToken),
) -> Result<(), SubmissionRefusal> {
    pin_outputs(&mut request.spec, local);
    let (inputs, captured_inputs, output_policies) = resolve_inputs(
        context,
        &request.spec,
        caller_of(request.created_by, &request.scope, request.retention_ms),
        (local, Some(auth_token)),
    )
    .await
    .map_err(refusal_of)?;
    request.spec.inputs = inputs;
    request.captured_inputs = captured_inputs;
    request.output_policies = output_policies;
    request.ingress_node_id = local;
    Ok(())
}

/// A request this holder could not read is unavailable, so the device tries the
/// next holder; one it read and must refuse is definitively invalid.
fn refusal_of(error: SubmitJobError) -> SubmissionRefusal {
    match error {
        SubmitJobError::PlacementUnavailable(_) => SubmissionRefusal::Unavailable,
        other => SubmissionRefusal::Invalid {
            reason: other.to_string(),
        },
    }
}

async fn authorize_group(
    context: &DriverContext,
    auth: &AuthContext,
    request: &SubmissionRequest,
    local: NodeId,
) -> Result<(), AuthorizeError> {
    authorize(
        context,
        auth.realm_id,
        auth,
        &group_permission_path(auth.realm_id, request.spec.group_id, local),
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::execution::job::{
        InputMode, InputSelection, InputSource, OutputSelection, WorkspaceOutput,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::Key;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    use crate::tests::records::{node, payload};

    fn family(seed: u8) -> JobFamilyId {
        JobFamilyId {
            submission_id: SubmissionId([seed; 32]),
            request_digest: [seed; 32],
        }
    }

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn write_claim(context: &DriverContext, realm_id: RealmId, family: JobFamilyId) {
        let job_id = JobId::from_bytes([7u8; 16]);
        let claim = SubmissionClaim {
            submission_id: family.submission_id,
            job_id,
            request_digest: family.request_digest,
            spec_digest: [0u8; 32],
            committing_node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            accepted_at_ms: 1,
        };
        let envelope = JobRecordEnvelope::sign(
            realm_id,
            JobFamilyRecord::Claim(claim),
            &iroh::SecretKey::from_bytes(&[1u8; 32]),
        )
        .expect("record signs");
        let mut key = kind_prefix(&family, JobRecordKind::Claim).to_vec();
        key.extend_from_slice(&[0u8; 40]);
        write_row(
            context,
            FAMILY_RECORD_KEYSPACE,
            Key::from(key.as_slice()),
            postcard::to_allocvec(&envelope).unwrap(),
        )
        .await;
    }

    async fn write_projection(
        context: &DriverContext,
        family: JobFamilyId,
        state: LogicalJobState,
    ) {
        let cache = ProjectionCache {
            version: crate::jobs::records::rows::PROJECTION_CACHE_VERSION,
            revision: 1,
            stale: false,
            projection: Some(aruna_core::structs::execution::job::JobProjection {
                submission_id: family.submission_id,
                request_digest: family.request_digest,
                canonical_job_id: JobId::from_bytes([7u8; 16]),
                aliases: Vec::new(),
                state,
                canonical_execution_id: None,
                executions: Vec::new(),
                outputs: aruna_core::structs::execution::job::OutputSet::new(Vec::new())
                    .expect("empty outputs"),
                cancel_requested: false,
            }),
        };
        write_row(
            context,
            FAMILY_PROJECTION_KEYSPACE,
            family_prefix(&family),
            postcard::to_allocvec(&cache).unwrap(),
        )
        .await;
    }

    async fn write_row(context: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn replay_skips_quota() {
        // A family this node already claimed is a replay: the admission settles
        // from its own records, so no quota view is read for it.
        let dir = tempdir().unwrap();
        let context = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([3u8; 32]);

        assert!(!has_claim(&context, &family(1)).await.unwrap());

        write_claim(&context, realm_id, family(1)).await;

        assert!(has_claim(&context, &family(1)).await.unwrap());
        assert!(!has_claim(&context, &family(2)).await.unwrap());
    }

    #[tokio::test]
    async fn replay_reports_state() {
        // A replay answers with the family's observed state, so a request that
        // is already running or finished is never reported as freshly queued.
        let dir = tempdir().unwrap();
        let context = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([4u8; 32]);
        write_claim(&context, realm_id, family(1)).await;

        write_projection(&context, family(1), LogicalJobState::Running).await;
        assert_eq!(
            observed_state(&context, family(1)).await,
            LogicalJobState::Running
        );

        write_projection(&context, family(1), LogicalJobState::Succeeded).await;
        assert_eq!(
            observed_state(&context, family(1)).await,
            LogicalJobState::Succeeded
        );

        // A family this node cannot reduce is indeterminate, never queued.
        assert_eq!(
            observed_state(&context, family(2)).await,
            LogicalJobState::Indeterminate
        );
    }

    fn s3_input(bucket: &str, key: &str, version: Option<&str>) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: version.map(str::to_string),
            },
            source_node_id: None,
            dest_key: "in.txt".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        }
    }

    #[test]
    fn pins_holder_outputs() {
        // The admitting holder owns every destination: a device-supplied node id
        // is overwritten and the workspace outputs resolve to the holder.
        let mut spec = payload();
        spec.file_outputs.push(OutputSelection {
            container_path: "/out/result.txt".to_string(),
            path_prefix: None,
            destination_node_id: Some(node(2)),
            destination: OutputDestination::S3 {
                bucket: "results".to_string(),
                key: "result.txt".to_string(),
            },
            name: None,
            description: None,
        });
        spec.workspace_outputs.push(WorkspaceOutput {
            container_path: "/out/extra.txt".to_string(),
            dest_key: "extra.txt".to_string(),
        });
        store_workspace(
            &mut spec,
            WorkspaceMode::Existing,
            Some("results".to_string()),
        )
        .expect("workspace stored");

        pin_outputs(&mut spec, node(1));

        assert!(spec.workspace_outputs.is_empty());
        assert_eq!(spec.file_outputs.len(), 2);
        for output in &spec.file_outputs {
            assert_eq!(output.destination_node_id, Some(node(1)));
        }
    }

    #[test]
    fn keeps_reference_shape() {
        // A device holds none of the objects it names, so a well-formed absent
        // reference passes while a malformed one is still refused here.
        let mut spec = payload();
        spec.inputs.push(s3_input("bucket", "key", None));
        assert!(reference_shape(&spec).is_ok());

        let mut versioned = payload();
        versioned.inputs.push(s3_input(
            "bucket",
            "key",
            Some(&Ulid::generate().to_string()),
        ));
        assert!(reference_shape(&versioned).is_ok());

        let mut empty_key = payload();
        empty_key.inputs.push(s3_input("bucket", "  ", None));
        assert!(matches!(
            reference_shape(&empty_key),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));

        let mut bad_version = payload();
        bad_version
            .inputs
            .push(s3_input("bucket", "key", Some("not-a-ulid")));
        assert!(matches!(
            reference_shape(&bad_version),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
    }

    fn reference_head(
        kind: aruna_core::structs::execution::source_connector::SourceConnectorKind,
        etag: Option<&str>,
        digested: bool,
    ) -> crate::s3::object::head::HeadObjectResult {
        use aruna_core::structs::checksum::ChecksumAlgorithm;
        use aruna_core::structs::execution::source_access::{
            DigestOrigin, DigestScope, SourceDigest, SourceMetadata,
        };
        use aruna_core::structs::execution::staging::{
            PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
        };
        let mut digests = std::collections::BTreeMap::new();
        if digested {
            digests.insert(
                ChecksumAlgorithm::Sha256,
                SourceDigest {
                    digest: vec![1; 32],
                    scope: DigestScope::FullObject,
                    origin: DigestOrigin::Manifest,
                },
            );
        }
        crate::s3::object::head::HeadObjectResult {
            location: None,
            metadata: Default::default(),
            source_metadata: Some(SourceMetadata {
                content_length: 9,
                content_type: None,
                etag: etag.map(str::to_string),
                last_modified: None,
                source_version: None,
                digests,
            }),
            last_refresh: None,
            version_created_at: None,
            version_id: None,
            resolved_version_id: None,
            checksum_type:
                aruna_core::structs::storage::multipart::MultipartChecksumType::FullObject,
            composite_hashes: Default::default(),
            part_count: None,
            source_policies: Vec::new(),
            source_binding: Some(VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind,
                    public_config: Default::default(),
                    source_path: "db/bakta.db".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                    namespace: None,
                    root: None,
                },
                connector_id: None,
            }),
        }
    }

    #[test]
    fn binds_reference_reads() {
        // A strong ETag on an S3 source binds every range; recorded digests bind a whole read;
        // a source with neither cannot be read at its exact version.
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        let bound = reference_content(
            "s3://db/a",
            &reference_head(SourceConnectorKind::S3, Some("\"e1\""), false),
        );
        let (content, pin) = bound.expect("range bound reference");
        assert_eq!(
            content,
            InputContent::Reference {
                etag: Some("\"e1\"".to_string()),
                range_bound: true
            }
        );
        assert_eq!(pin.expect("pin").path, "db/bakta.db");
        let weak = reference_head(SourceConnectorKind::S3, Some("W/\"e1\""), true);
        let (content, _) = reference_content("s3://db/a", &weak).expect("digest bound");
        assert!(matches!(
            content,
            InputContent::Reference {
                range_bound: false,
                ..
            }
        ));
        let http = reference_head(SourceConnectorKind::Http, Some("\"e1\""), false);
        assert!(matches!(
            reference_content("s3://db/a", &http),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
        let mut stored = reference_head(SourceConnectorKind::S3, Some("\"e1\""), false);
        stored.source_binding = None;
        assert!(reference_content("s3://db/a", &stored).is_err());
    }

    fn captured_from(input: &InputSelection, source: NodeId) -> CapturedInput {
        CapturedInput {
            destination_key: input.dest_key.clone(),
            source_node_id: source,
            version_id: Ulid::from_parts(1, 1),
            content: InputContent::Blake3([1; 32]),
            bytes: 1,
            policies: Vec::new(),
            source_group_id: Ulid::from_parts(2, 2),
            public: false,
            link: None,
            source_pin: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
        }
    }

    #[test]
    fn answers_pins() {
        // A source node may change an input only behind a link that names the asked version,
        // and must pin what it answers to the version it captured.
        let node_id = node(2);
        let version = Ulid::from_parts(1, 1);
        let mut asked = s3_input("db", "bakta.db", Some(&version.to_string()));
        asked.source_node_id = Some(node_id);
        let plain = (asked.clone(), captured_from(&asked, node_id));
        assert!(answers_input(&asked, &plain, node_id));
        let target_version = Ulid::from_parts(2, 2);
        let mut target = s3_input("raw", "bakta-v6.db", Some(&target_version.to_string()));
        target.source_node_id = Some(node_id);
        let mut linked = captured_from(&asked, node_id);
        linked.version_id = target_version;
        assert!(!answers_input(
            &asked,
            &(target.clone(), linked.clone()),
            node_id
        ));
        linked.link = Some(VersionedObjectArn {
            realm_id: RealmId::from_bytes([1; 32]),
            node_id,
            bucket: "db".to_string(),
            key: "bakta.db".to_string(),
            version,
        });
        assert!(answers_input(
            &asked,
            &(target.clone(), linked.clone()),
            node_id
        ));
        let mut repinned = target.clone();
        repinned.source = s3_input(
            "raw",
            "bakta-v6.db",
            Some(&Ulid::from_parts(3, 3).to_string()),
        )
        .source;
        assert!(!answers_input(&asked, &(repinned, linked.clone()), node_id));
        let mut stale = linked;
        stale.link.as_mut().expect("link").version = Ulid::from_parts(9, 9);
        assert!(!answers_input(&asked, &(target, stale), node_id));
    }

    #[test]
    fn refuses_restricted_resolver() {
        // A bearer narrowed to some paths never gets unrestricted reads or grants resolved.
        let realm_id = RealmId::from_bytes([1; 32]);
        let peer = node(2);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 1);
        config
            .nodes
            .push(aruna_core::structs::identity::realm::RealmNode {
                node_id: peer.to_string(),
                kind: aruna_core::structs::identity::realm::RealmNodeKind::Server,
            });
        let user = UserId::local(Ulid::from_parts(3, 3), realm_id);
        let open = AuthContext {
            user_id: user,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        assert_eq!(resolver_admits(&open, user, &config, peer), Ok(()));
        let restricted = AuthContext {
            path_restrictions: Some(vec![aruna_core::structs::identity::auth::PathRestriction {
                pattern: "/x/**".to_string(),
                permission: Permission::READ,
            }]),
            ..open.clone()
        };
        assert_eq!(
            resolver_admits(&restricted, user, &config, peer),
            Err(SubmissionRefusal::Unauthorized)
        );
        let other = UserId::local(Ulid::from_parts(4, 4), realm_id);
        assert!(resolver_admits(&open, other, &config, peer).is_err());
        assert!(resolver_admits(&open, user, &config, node(9)).is_err());
    }

    #[test]
    fn checks_capture_sources() {
        // A capture must come from the node its input names, or from ingress without one.
        let (ingress, other) = (node(1), node(2));
        let unnamed = s3_input("a", "x", None);
        let mut named = s3_input("b", "y", Some(&Ulid::from_parts(1, 1).to_string()));
        named.dest_key = "named".to_string();
        named.source_node_id = Some(other);
        let inputs = vec![unnamed.clone(), named.clone()];
        let honest = vec![
            captured_from(&unnamed, ingress),
            captured_from(&named, other),
        ];
        assert!(sources_match(&inputs, &honest, ingress));
        let forged = vec![
            captured_from(&unnamed, ingress),
            captured_from(&named, ingress),
        ];
        assert!(!sources_match(&inputs, &forged, ingress));
        let moved = vec![captured_from(&unnamed, other), captured_from(&named, other)];
        assert!(!sources_match(&inputs, &moved, ingress));
    }

    #[tokio::test]
    async fn remote_needs_pin_and_bearer() {
        // An input on another node needs an exact version and the caller's own bearer.
        let dir = tempdir().unwrap();
        let context = test_ctx(dir.path().to_str().unwrap());
        let caller = (
            UserId::local(Ulid::from_parts(3, 3), RealmId::from_bytes([1; 32])),
            SubmissionId([4; 32]),
            10,
        );
        let unpinned = vec![s3_input("a", "x", None)];
        let refused = remote_captures(&context, node(2), &unpinned, caller, None).await;
        assert!(matches!(refused, Err(SubmitJobError::InvalidWorkspace(_))));
        let pinned = vec![s3_input(
            "a",
            "x",
            Some(&Ulid::from_parts(1, 1).to_string()),
        )];
        let tokenless = remote_captures(&context, node(2), &pinned, caller, None).await;
        assert!(matches!(
            tokenless,
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    #[test]
    fn maps_holder_refusal() {
        // A holder that could not read is retried at the next holder; one that
        // read the request and refused it ends the ingress definitively.
        assert_eq!(
            refusal_of(SubmitJobError::PlacementUnavailable("offline".to_string())),
            SubmissionRefusal::Unavailable
        );
        assert_eq!(
            refusal_of(SubmitJobError::InvalidWorkspace("absent".to_string())),
            SubmissionRefusal::Invalid {
                reason: "invalid workspace: absent".to_string()
            }
        );
    }
}
