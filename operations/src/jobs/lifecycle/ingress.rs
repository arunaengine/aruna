//! Takes in one external submission: the receiver commits it when its view makes it the holder.
//! Otherwise it forwards the submission one hop and writes nothing; a device always forwards.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::node_state::ReadInfoOperation;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::{
    CapturedInput, ExecutionSpec, InputContent, InputSelection, SubmissionId, WorkspaceMode,
    spec_entries,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::{
    BlobVersion, BlobVersionState, BucketInfo, VersionKey, bucket_permission_path,
    group_permission_path,
};
use aruna_core::structs::storage::bucket_metadata::{CapturedDatabase, ReleaseRef};
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use aruna_core::structs::storage::read_grant::{
    GrantScope, HEAD_OPERATION, READ_OPERATION, ReadGrant,
};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::time::unix_timestamp_millis;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;

use super::admit_here::{
    AcceptedSubmission, AdmitHereOperation, LocalAdmission, placement_failure,
};
use super::forward::ForwardSubmissionOperation;
use super::ids;
use super::ids::{RequestIdentity, SubmissionRequest, SubmissionScope, store_workspace};
use super::output_policies::OutputPoliciesOperation;
use super::submission_rules::{
    admitted_description, batch_len, caller_of, exact_version, family_view, fits,
    forwarded_request_fits, input_reference, is_device, is_directory, pin_outputs,
    reference_content, reference_shape, refusal_of, resolve_answer, resolver_admits, source_error,
};
use crate::auth::error::AuthorizeError;
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::jobs::records::pages::{load_chain, page_request};
use crate::jobs::records::verify::FamilyView;
use crate::jobs::service::mint_local_job;
use crate::jobs::source_read::{authorize_read, issue_grant, public_read};
use crate::jobs::submit::SubmitJobError;
use crate::jobs::submit_execution::validate_execution;
use crate::metadata::WritePeerError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::load_config::load_realm_config;
use crate::realm::peer_trust::peer_acts_for;
use crate::s3::bucket::dataset::{bucket_metadata, dataset_description};
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::bucket::release::read::release_members;
use crate::s3::bucket::release::{BucketScope, MembershipCaller};
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use aruna_core::jobs::{InputResolveRequest, SubmissionAck, SubmissionRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::structs::storage::release::MembershipQuery;
use crate::auth::request_authorization::AuthorizeOperation;

/// Resolves inputs this node holds for an ingress, under a bearer validated here, so grants
/// vouch only for an identity this node checked. Each answer is an input after its links with
/// its capture here; the answers that fit one message return, and the ingress asks again.
pub async fn serve_input_resolve(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    request: InputResolveRequest,
) -> Result<Vec<(InputSelection, CapturedInput)>, SubmissionRefusal> {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(SubmissionRefusal::Unavailable);
    };
    let AuthToken::Bearer(bearer) = &auth_token else {
        return Err(SubmissionRefusal::Unauthorized);
    };
    // Kept to ask range coordinators about release membership for the same caller.
    let bearer = bearer.as_str().to_string();
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
    let mut size = 0;
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
        let here = (local, Some(bearer.as_str()));
        let captured = capture_inputs(context, std::slice::from_ref(&input), caller, here)
            .await
            .map_err(refusal_of)?;
        let Some(mut captured) = captured.into_iter().next() else {
            return Err(SubmissionRefusal::Unavailable);
        };
        captured.link = link;
        // An input asked without a version is pinned to the one captured here.
        let aruna_core::structs::execution::job::InputSource::S3 { version_id, .. } =
            &mut input.source;
        version_id.get_or_insert_with(|| captured.version_id.to_string());
        let answer = (input, captured);
        if !fits(&mut size, &answer) {
            if answers.is_empty() {
                return Err(SubmissionRefusal::Invalid {
                    reason: format!("{}: the input exceeds one message", answer.0.dest_key),
                });
            }
            break;
        }
        answers.push(answer);
    }
    Ok(answers)
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
    let labels = ids::required_labels(&spec)
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
    if !labels.is_empty() {
        // Only a complete read of the advertisements proves that no executor carries a label.
        let (documents, unread) = advertisements(context, &config).await;
        if let Some(label) = super::plan::missing_label(&documents, &labels).filter(|_| !unread) {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "no executor in the realm carries the label `{}={}`",
                label.key, label.value
            )));
        }
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
    forward_once(context, (&request, &identity), &view, local, auth_token).await
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
    forward_once(context, (&request, &identity), &view, local, auth_token).await
}

/// Points a local input of a distributed bucket at the member that coordinates its key, so it
/// is captured and pinned there, as an S3 read of the key is served there.
async fn range_holder(
    context: &DriverContext,
    mut input: InputSelection,
    local: NodeId,
) -> Result<InputSelection, SubmitJobError> {
    if input.source_node_id.is_some_and(|node| node != local) {
        return Ok(input);
    }
    let aruna_core::structs::execution::job::InputSource::S3 {
        bucket,
        key,
        version_id,
    } = &input.source;
    // A missing or unreadable bucket is refused by the capture with its own message.
    let Ok(info) = drive(GetBucketOperation::new(bucket.clone()), context).await else {
        return Ok(input);
    };
    let Some(binding) = info.distribution else {
        return Ok(input);
    };
    let coordinator = crate::distribution::routing::read_coordinator(context, &binding, key)
        .await
        .map_err(|error| {
            let reference = input_reference(bucket, key, version_id.as_deref());
            SubmitJobError::PlacementUnavailable(format!("{reference}: {error}"))
        })?;
    if let Some((_, node)) = coordinator {
        input.source_node_id = Some(node);
    }
    Ok(input)
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
            Some(node) => {
                exact_version(input)?;
                remote.entry(node).or_default().push(input.clone());
            }
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
        let input = range_holder(context, input, local).await?;
        match input.source_node_id.filter(|node| *node != local) {
            Some(node) => remote.entry(node).or_default().push(input),
            None => here.push(input),
        }
    }
    let bearer = match auth_token {
        Some(AuthToken::Bearer(token)) => Some(token.as_str()),
        _ => None,
    };
    let mut captured = capture_inputs(context, &here, caller, (local, bearer)).await?;
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
    let output_policies = drive(OutputPoliciesOperation::new(spec, local), context)
        .await
        .map_err(placement_failure)?;
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
    let (Some(metadata), Some(AuthToken::Bearer(token))) =
        (context.metadata_handle.as_ref(), auth_token)
    else {
        return Err(SubmitJobError::PlacementUnavailable(
            "an input on another node needs the caller's bearer token".to_string(),
        ));
    };
    // Each request carries the inputs that fit one batch and the node answers a prefix of
    // them, so the rest go out again until every input is answered.
    let mut answered = Vec::with_capacity(inputs.len());
    while answered.len() < inputs.len() {
        let rest = &inputs[answered.len()..];
        let batch = &rest[..batch_len(rest)];
        if batch.is_empty() {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "{}: the input exceeds one message",
                rest[0].dest_key
            )));
        }
        let request = InputResolveRequest {
            created_by,
            submission_id,
            expires_at,
            inputs: batch.to_vec(),
        };
        let token = AuthToken::Bearer(token.clone());
        let answer = metadata.request_input_resolve(node, token, request).await;
        answered.extend(resolve_answer(answer, batch, node)?);
    }
    Ok(answered)
}

/// Captures inputs this node holds: each is read like an S3 GET by the caller, pinned to an
/// exact version and granted for later exact reads, with its bucket's metadata and the releases
/// that pin it. `bearer` lets range coordinators answer release membership.
async fn capture_inputs(
    context: &DriverContext,
    inputs: &[InputSelection],
    (created_by, submission_id, expires_at): (UserId, SubmissionId, u64),
    (local, bearer): (NodeId, Option<&str>),
) -> Result<Vec<CapturedInput>, SubmitJobError> {
    let mut captured_inputs = Vec::with_capacity(inputs.len());
    // Inputs of one bucket share its metadata and its dataset's description, read once.
    let mut databases = std::collections::HashMap::new();
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
            None => version_hash(context, bucket, key, version).await,
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
        let unavailable =
            |error: String| SubmitJobError::PlacementUnavailable(format!("{reference}: {error}"));
        let info = drive(GetBucketOperation::new(bucket.clone()), context)
            .await
            .map_err(|error| unavailable(error.to_string()))?;
        let database = match databases.get(bucket.as_str()) {
            Some(database) => Option::clone(database),
            None => {
                let database = readable_metadata(context, (&auth, local, bearer), bucket, &info)
                    .await
                    .map_err(unavailable)?;
                databases.insert(bucket.clone(), database.clone());
                database
            }
        };
        let scope = BucketScope {
            realm_id,
            node_id: local,
            bucket,
            info: &info,
        };
        let (releases, releases_complete) =
            input_releases(context, scope, (&auth, bearer), (key.as_str(), version)).await;
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
            database,
            releases,
            releases_complete,
            grant,
            served_by: None,
        });
    }
    Ok(captured_inputs)
}

/// The bucket's metadata and dataset description when the caller may read the bucket itself, as
/// the metadata route checks; READ on one key alone shows none of it and the run still starts.
/// An unreadable dataset leaves the description empty; an unavailable one fails the admission.
async fn readable_metadata(
    context: &DriverContext,
    (auth, local, bearer): (&AuthContext, NodeId, Option<&str>),
    bucket: &str,
    info: &BucketInfo,
) -> Result<Option<CapturedDatabase>, String> {
    let path = bucket_permission_path(auth.realm_id, info.group_id, local, bucket);
    let extras = PolicyRequestExtras::rest();
    let metadata = match drive(AuthorizeOperation::new(auth.realm_id, auth, &path, &Permission::READ, extras), context)
    .await
    {
        Ok(()) => bucket_metadata(context, bucket, info).await?,
        // Only a refusal omits the metadata; unreadable policy state stays a retryable failure.
        Err(
            AuthorizeError::PermissionDenied
            | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }),
        ) => None,
        Err(error) => return Err(error.to_string()),
    };
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let described = match metadata.dataset {
        Some(dataset) => {
            let token =
                bearer.and_then(|token| aruna_core::metadata::AuthToken::bearer(token).ok());
            let read = dataset_description(context, auth.realm_id, dataset, (Some(auth), token));
            admitted_description(read.await)?
        }
        None => Default::default(),
    };
    Ok(Some(CapturedDatabase::new(metadata, described)))
}

/// The releases that pin one input version, checked with the caller's own access; a check that
/// fails leaves them unproven rather than refusing the run.
async fn input_releases(
    context: &DriverContext,
    scope: BucketScope<'_>,
    (auth, bearer): (&AuthContext, Option<&str>),
    (key, version_id): (&str, Ulid),
) -> (Vec<ReleaseRef>, bool) {
    let caller = MembershipCaller { auth, bearer };
    let query = MembershipQuery {
        key: key.to_string(),
        version_id,
    };
    match release_members(context, scope, caller, &[query]).await {
        Ok(found) => match found.answers.into_iter().next() {
            Some(answer) => (answer.releases, answer.complete),
            None => (Vec::new(), false),
        },
        Err(_) => (Vec::new(), false),
    }
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

/// Mints the alias and admits the submission here; see [`AdmitHereOperation`].
async fn admit_here(
    context: &DriverContext,
    request: &SubmissionRequest,
    identity: &RequestIdentity,
    config: &RealmConfigDocument,
    local: NodeId,
) -> Result<AcceptedSubmission, SubmitJobError> {
    let subject = Some(identity.submission_id.0.as_slice());
    let job_id = mint_local_job(context, config.realm_id, local, subject).await?;
    let admission = LocalAdmission {
        request: request.clone(),
        identity: *identity,
        config: config.clone(),
        local,
        job_id,
        now_ms: unix_timestamp_millis(),
        fresh: context.fresh_heartbeats(),
    };
    drive(AdmitHereOperation::new(admission), context)
        .await
        .map_err(placement_failure)
}

/// Forwards the request to the other holders; see [`ForwardSubmissionOperation`].
async fn forward_once(
    context: &DriverContext,
    (request, identity): (&SubmissionRequest, &RequestIdentity),
    view: &FamilyView,
    local: NodeId,
    auth_token: Option<AuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    let holders = view.holders().iter().copied();
    let holders = holders.filter(|holder| *holder != local).collect();
    let forward = ForwardSubmissionOperation::new(request, identity, holders, auth_token);
    drive(forward, context).await.map_err(placement_failure)
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
    if !forwarded_request_fits(&request, ingress) {
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

async fn authorize_group(
    context: &DriverContext,
    auth: &AuthContext,
    request: &SubmissionRequest,
    local: NodeId,
) -> Result<(), AuthorizeError> {
    drive(AuthorizeOperation::new(auth.realm_id, auth, &group_permission_path(auth.realm_id, request.spec.group_id, local), &Permission::WRITE, PolicyRequestExtras::rest()), context)
    .await
}

/// Node-info documents of every sync-eligible realm member that has one, and
/// whether a read failed. An unread advertisement may hold the only legal
/// target, so the round is a continuation rather than a conclusive refusal.
pub(super) async fn advertisements(
    context: &DriverContext,
    config: &RealmConfigDocument,
) -> (BTreeMap<NodeId, NodeInfoDocument>, bool) {
    let mut documents = BTreeMap::new();
    let Ok(members) = config.sync_eligible_nodes() else {
        return (documents, true);
    };
    let mut unread = false;
    for node_id in members {
        let read = drive(ReadInfoOperation::new(vec![node_id]), context).await;
        match read.map(|mut documents| documents.remove(&node_id)) {
            Ok(Some(document)) => {
                documents.insert(node_id, document);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(node = %node_id, error = %error, "Node info advertisement stayed unread");
                unread = true;
            }
        }
    }
    (documents, unread)
}

/// The blake3 of a version this node does not hold locally, read from the
/// replicated version record.
async fn version_hash(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version: Ulid,
) -> Option<[u8; 32]> {
    let version_key = VersionKey::new(bucket, key, version).to_bytes().ok()?;
    let event = context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Read {
            key_space: aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.into(),
            txn_id: None,
        })
        .await;
    let aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::ReadResult {
        value: Some(bytes),
        ..
    }) = event
    else {
        return None;
    };
    match postcard::from_bytes::<BlobVersion>(&bytes).ok()?.state {
        BlobVersionState::Materialized { blob_hash, .. } => Some(blob_hash),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::lifecycle::submission_rules::{answers_input, sources_match};
    use crate::s3::bucket::dataset::DescriptionError;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::execution::job::OutputDestination;
    use aruna_core::structs::execution::job::{
        InputMode, InputSelection, InputSource, OutputSelection, WorkspaceOutput,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::dataset_description::DatasetDescription;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    use crate::tests::records::{node, payload};

    #[test]
    fn unavailable_dataset_retries() {
        // Only a dataset that gives no description for good leaves the capture empty.
        let described = DatasetDescription {
            title: Some("Bakta".to_string()),
            ..DatasetDescription::default()
        };
        assert_eq!(admitted_description(Ok(described.clone())), Ok(described));
        for definite in [
            DescriptionError::Unreadable,
            DescriptionError::Invalid("not a Dataset".to_string()),
        ] {
            assert_eq!(admitted_description(Err(definite)), Ok(Default::default()));
        }
        assert!(admitted_description(Err(DescriptionError::Unavailable)).is_err());
    }

    // A plain s3:// input of a distributed bucket is captured at the member that coordinates
    // its key, not refused because this member holds no copy.
    #[tokio::test]
    async fn routes_range_holder() {
        use crate::distribution::test_record::{binding, record, secret, store_record};
        use crate::distribution::test_store::{context, open};
        let (_dir, handle) = open();
        let (snapshot, bytes) = record(true);
        store_record(&handle, snapshot.set_id(), bytes).await;
        let bucket = aruna_core::structs::storage::blob::BucketInfo {
            group_id: Ulid::from_bytes([90; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(RealmId::from_bytes([9; 32])),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: Some(binding(&snapshot, 1, 5)),
        };
        let write = StorageEffect::Write {
            key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
            key: b"refseq".to_vec().into(),
            value: bucket.to_bytes().unwrap().into(),
            txn_id: None,
        };
        handle.send_storage_effect(write).await;
        let context = context(&handle);
        let input = |key: &str| InputSelection {
            source: InputSource::S3 {
                bucket: "refseq".to_string(),
                key: key.to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: key.to_string(),
            mode: InputMode::Snapshot,
            container_path: Some(format!("/in/{key}")),
            name: None,
            description: None,
        };
        let local = secret(1).public();

        let lower = range_holder(&context, input("a"), local).await.unwrap();
        let upper = range_holder(&context, input("x"), local).await.unwrap();

        assert_eq!(lower.source_node_id, None);
        assert_eq!(upper.source_node_id, Some(secret(2).public()));
    }

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
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
                mirrors: Vec::new(),
            }),
        }
    }

    #[test]
    fn binds_reference_reads() {
        // A strong ETag on an S3 source binds every range; any other reference is staged whole,
        // bound to its recorded observation.
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
        let (content, _) = reference_content("s3://db/a", &http).expect("observation bound");
        assert!(matches!(
            content,
            InputContent::Reference {
                range_bound: false,
                ..
            }
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
            served_by: None,
        }
    }

    #[test]
    fn pages_resolve_messages() {
        // Answers past one transport message split into pages that each encode within it, and
        // an answer that stops early is taken as a prefix, leaving the rest to ask again.
        let node_id = node(2);
        let long = "k".repeat(64 * 1024);
        let version = Ulid::from_parts(1, 1).to_string();
        let answers: Vec<(InputSelection, CapturedInput)> = (0..120u32)
            .map(|index| {
                let mut input = s3_input("db", &format!("{index}/{long}"), Some(&version));
                input.dest_key = format!("{index}/{long}");
                input.source_node_id = Some(node_id);
                let captured = captured_from(&input, node_id);
                (input, captured)
            })
            .collect();
        let inputs: Vec<InputSelection> = answers.iter().map(|(input, _)| input.clone()).collect();
        let encode = |answers: &[(InputSelection, CapturedInput)]| {
            let result = Ok(answers.to_vec());
            crate::metadata::protocol::encode_message(
                &MetadataTransportMessage::ResolvedJobInputs { result },
            )
        };
        assert!(encode(&answers).is_err());
        let mut done = 0;
        let mut pages = 0;
        while done < answers.len() {
            let page = &answers[done..][..batch_len(&answers[done..])];
            assert!(!page.is_empty() && encode(page).is_ok());
            let taken = resolve_answer(Ok(Ok(page.to_vec())), &inputs[done..], node_id).unwrap();
            assert_eq!(taken.len(), page.len());
            done += page.len();
            pages += 1;
        }
        assert!(pages > 1);
        assert!(resolve_answer(Ok(Ok(Vec::new())), &inputs, node_id).is_err());
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

        // An input routed to its range coordinator without a version is answered pinned there,
        // never unpinned and never as another key.
        let mut unpinned = s3_input("db", "bakta.db", None);
        unpinned.source_node_id = Some(node_id);
        assert!(answers_input(&unpinned, &plain, node_id));
        let bare = (unpinned.clone(), captured_from(&asked, node_id));
        assert!(!answers_input(&unpinned, &bare, node_id));
        let mut other = asked.clone();
        other.source = s3_input("db", "other.db", Some(&version.to_string())).source;
        let moved = (other.clone(), captured_from(&other, node_id));
        assert!(!answers_input(&unpinned, &moved, node_id));
    }

    // A node named by the caller still needs an exact version; a range coordinator does not.
    #[test]
    fn node_needs_version() {
        assert!(exact_version(&s3_input("db", "a", None)).is_err());
        let version = Ulid::from_parts(1, 1).to_string();
        assert!(exact_version(&s3_input("db", "a", Some(&version))).is_ok());
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
    async fn remote_needs_bearer() {
        // An input on another node needs the caller's own bearer; its version rule is checked
        // where the caller names the node (`node_needs_version`).
        let dir = tempdir().unwrap();
        let context = test_ctx(dir.path().to_str().unwrap());
        let caller = (
            UserId::local(Ulid::from_parts(3, 3), RealmId::from_bytes([1; 32])),
            SubmissionId([4; 32]),
            10,
        );
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
