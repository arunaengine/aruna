//! The rules of one external submission that read no state: batch sizes, input references,
//! what a resolving node may answer, and how refusals read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::events::PeerAuthRefusal;
use aruna_core::id::NodeId;
use aruna_core::jobs::SubmissionRefusal;
use aruna_core::structs::execution::job::{
    CapturedInput, ExecutionSpec, InputContent, InputSelection, SubmissionId, WorkspaceMode,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::dataset_description::{DatasetDescription, DescriptionError};
use aruna_core::structs::storage::origin::SourcePin;
use serde::Serialize;
use ulid::Ulid;

use super::ids::{self, RequestIdentity, SubmissionRequest, SubmissionScope};
use crate::jobs::read_access::SourceReadError;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::submit::SubmitJobError;
use crate::realm::peer_trust::is_sync_eligible;

/// What one resolve request or answer may carry: half a transport message, so the bearer and
/// the envelope always fit beside it.
const RESOLVE_BATCH_BYTES: usize = crate::metadata::protocol::MAX_MESSAGE_SIZE / 2;

/// Adds `item` to a batch of `size` encoded bytes when it still fits one resolve batch.
pub(super) fn fits(size: &mut usize, item: &impl Serialize) -> bool {
    let bytes = postcard::experimental::serialized_size(item).unwrap_or(usize::MAX);
    match size
        .checked_add(bytes)
        .filter(|total| *total <= RESOLVE_BATCH_BYTES)
    {
        Some(total) => {
            *size = total;
            true
        }
        None => false,
    }
}

/// How many leading `items` fit one resolve batch.
pub(super) fn batch_len<T: Serialize>(items: &[T]) -> usize {
    let mut size = 0;
    items
        .iter()
        .take_while(|item| fits(&mut size, *item))
        .count()
}

/// Whether `node` answered `asked` faithfully: the same destination, pinned to the version it
/// captured there, and a new source only behind a link that names the asked version there.
pub(super) fn answers_input(
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
    // A source asked without a version is answered at the version captured there.
    let same_source = |selection: &InputSelection| {
        let InputSource::S3 {
            bucket: answered,
            key: answered_key,
            ..
        } = &selection.source;
        answered == bucket
            && answered_key == key
            && pin(asked).is_none_or(|v| pin(selection) == Some(v))
    };
    let source_kept = match &captured.link {
        None => same_source(selection),
        Some(link) => {
            link.node_id == node
                && link.bucket == *bucket
                && link.key == *key
                && pin(asked).is_none_or(|version| link.version == version)
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
pub(super) fn resolver_admits(
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

/// Whether `node_id` is a user device: it carries no shared realm
/// responsibility, holds nothing, and therefore admits nothing.
pub(super) fn is_device(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    !is_sync_eligible(config, node_id)
}

/// Pins every declared output to the node that will admit the request and write
/// its results.
pub(super) fn pin_outputs(spec: &mut ExecutionSpec, node_id: NodeId) {
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

/// Validates the shape of every input reference without requiring the object to
/// be present here. Local presence is the admitting holder's check.
pub(super) fn reference_shape(spec: &ExecutionSpec) -> Result<(), SubmitJobError> {
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
pub(super) fn input_reference(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    match version_id {
        Some(version) => format!("s3://{bucket}/{key}?versionId={version}"),
        None => format!("s3://{bucket}/{key}"),
    }
}

/// A reference version without a content hash, served by GetObject while its source still
/// matches the recorded observation: a strong ETag on a source that enforces `If-Match` binds
/// every range, so only that input may be mounted. Anything else is not materialized.
pub(super) fn reference_content(
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
    let pin = SourcePin::of(&binding.descriptor);
    let content = InputContent::Reference {
        etag: metadata.etag.clone(),
        range_bound,
    };
    Ok((content, Some(pin)))
}

/// Who resolves inputs: the caller, the submission its grants name, and when they end.
pub(super) fn caller_of(
    created_by: UserId,
    scope: &SubmissionScope,
    retention_ms: u64,
    now_ms: u64,
) -> (UserId, SubmissionId, u64) {
    let expires_at = now_ms.saturating_add(retention_ms) / 1000;
    (
        created_by,
        ids::submission_of(created_by, scope),
        expires_at,
    )
}

/// A denied read is the caller's own; anything undecided may clear on a retry.
pub(super) fn source_error(reference: &str, error: SourceReadError) -> SubmitJobError {
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

/// An input the caller names on another node must name its exact version there too.
pub(super) fn exact_version(input: &InputSelection) -> Result<(), SubmitJobError> {
    let aruna_core::structs::execution::job::InputSource::S3 {
        bucket,
        key,
        version_id,
    } = &input.source;
    match version_id.as_deref().map(Ulid::from_string) {
        Some(Ok(_)) => Ok(()),
        _ => Err(SubmitJobError::InvalidWorkspace(format!(
            "s3://{bucket}/{key}: an input on another node must name its exact version"
        ))),
    }
}

/// A directory input names a key prefix, marked by its trailing `/`.
pub(super) fn is_directory(input: &InputSelection) -> bool {
    let aruna_core::structs::execution::job::InputSource::S3 { key, .. } = &input.source;
    key.ends_with('/') && input.dest_key.ends_with('/')
}

/// The answers of one resolve request: a non-empty prefix of `batch`, each faithful to it.
pub(super) fn resolve_answer(
    answer: Result<Result<Vec<(InputSelection, CapturedInput)>, SubmissionRefusal>, String>,
    batch: &[InputSelection],
    node: NodeId,
) -> Result<Vec<(InputSelection, CapturedInput)>, SubmitJobError> {
    let answer = answer.map_err(|error| {
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
    // The node answers a prefix of the asked inputs, each pinned to a version it holds.
    let exact = !captured.is_empty()
        && captured.len() <= batch.len()
        && (captured.iter().zip(batch)).all(|(answer, asked)| answers_input(asked, answer, node));
    match exact {
        true => Ok(captured),
        false => Err(SubmitJobError::PlacementUnavailable(format!(
            "input node {node} answered other versions"
        ))),
    }
}

/// The description a run captures: empty only when the dataset definitely gives none.
pub(super) fn admitted_description(
    read: Result<DatasetDescription, DescriptionError>,
) -> Result<DatasetDescription, String> {
    match read {
        Ok(described) => Ok(described),
        Err(DescriptionError::Unreadable | DescriptionError::Invalid(_)) => Ok(Default::default()),
        Err(error @ DescriptionError::Unavailable) => Err(error.to_string()),
    }
}

/// The unconflicted family view. A view this node cannot resolve is an
/// availability failure, never an accepted job.
pub(super) fn family_view(
    config: &RealmConfigDocument,
    identity: &RequestIdentity,
) -> Result<FamilyView, SubmitJobError> {
    FamilyView::resolve(config, config.realm_id, identity.family()).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("job family placement is unavailable".to_string())
    })
}

/// Whether each capture comes from the node its input names, or from ingress when it names none.
pub(super) fn sources_match(
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

/// A request this holder could not read is unavailable, so the device tries the
/// next holder; one it read and must refuse is definitively invalid.
pub(super) fn refusal_of(error: SubmitJobError) -> SubmissionRefusal {
    match error {
        SubmitJobError::PlacementUnavailable(_) => SubmissionRefusal::Unavailable,
        other => SubmissionRefusal::Invalid {
            reason: other.to_string(),
        },
    }
}

/// What a peer learns when its forwarded token gives no caller.
pub(super) fn peer_refusal(refusal: PeerAuthRefusal) -> SubmissionRefusal {
    match refusal {
        PeerAuthRefusal::Unauthorized => SubmissionRefusal::Unauthorized,
        PeerAuthRefusal::Unavailable(_) => SubmissionRefusal::Unavailable,
    }
}

/// Whether a forwarded request is what `ingress` could have sent: a capture for every input
/// from the node it names, and every output pinned to that ingress.
pub(super) fn forwarded_request_fits(request: &SubmissionRequest, ingress: NodeId) -> bool {
    let (spec, captured) = (&request.spec, &request.captured_inputs);
    captured.len() == spec.inputs.len()
        && captured.iter().all(|captured| {
            spec.inputs
                .iter()
                .any(|input| input.dest_key == captured.destination_key)
        })
        && request.ingress_node_id == ingress
        && sources_match(&spec.inputs, captured, ingress)
        && spec
            .file_outputs
            .iter()
            .all(|output| output.destination_node_id == Some(ingress))
        && (ids::workspace_of(spec).0 != WorkspaceMode::Existing
            || spec.workspace_outputs.is_empty())
}
