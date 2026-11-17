//! Answers inbound bao streams: peer admission, manifests, reads, summaries and index lookups.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use aruna_blob::blob::{BlobHandle, InboundError, refuse_stream};
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_resource_path,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    HashIndex, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::replication::ReplicationItemKind;
use aruna_net::streams::BiStream;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

use super::{IncomingVersionOperation, IncomingVersionResult};
use crate::auth::permission_rules::GroupPermissionRules;
use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::{
    PolicyEnforcementError, PolicyEvaluator, PolicyRequestExtras, policy_request_with,
};
use crate::blob::permission_paths::ResolvePathsOperation;
use crate::driver::{
    DriverContext, drive, gate_context, node_routing, now_ms, quota_marked_routing,
};
use crate::notifications::watch::emit::emit_watch_event;
use crate::realm::get_config::GetConfigOperation;
use crate::realm::mutate_placement::node_kind;
use crate::replication::bao_read::IncomingBaoOperation;
use crate::replication::locations::LocationSummaryOperation;
use crate::replication::protocol::{
    BaoReadRequest, LocationSummaryRequest, VersionReplicationManifest, VersionReplicationMessage,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::staging::offered_directory::{OfferedDirectoryError, guard_bucket_write};

const INBOUND_BAO_TIMEOUT: Duration = Duration::from_secs(5);

/// What the bao plane admits this peer as. Replication is trusted from
/// sync-eligible realm nodes only; an owner-bound device gets bao reads alone from
/// a sync-eligible node, never becoming a replication source. Fails closed.
async fn bao_peer_admitted(
    context: &DriverContext,
    realm_id: RealmId,
    local: NodeId,
    peer: NodeId,
) -> Option<BaoAdmission> {
    let config = match drive(GetConfigOperation::new(realm_id), context).await {
        Ok(config) => config,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read realm config for replication gate");
            return None;
        }
    };
    let eligible = match config.sync_eligible_nodes() {
        Ok(ids) => ids,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to resolve replication peers");
            return None;
        }
    };
    if eligible.contains(&peer) {
        return Some(BaoAdmission::Infra);
    }
    // Only a realm node answers a device, so a device never reaches another
    // device's plane.
    let device = node_kind(&config, peer)
        .and_then(|kind| kind.owner())
        .is_some();
    (device && eligible.contains(&local)).then_some(BaoAdmission::DeviceRead)
}

/// Bounded admission read: identity comes from the realm config, never from
/// the remote's claim, and a slow store read must not hold the stream open.
async fn bao_admission(
    context: &DriverContext,
    realm_id: RealmId,
    local: NodeId,
    peer: NodeId,
) -> Option<BaoAdmission> {
    timeout(
        INBOUND_BAO_TIMEOUT,
        bao_peer_admitted(context, realm_id, local, peer),
    )
    .await
    .ok()
    .flatten()
}

/// Executes the manifest/reference-advance family for an admitted infra
/// peer: routing, request policy, placement gate, and the watch emitted on
/// success. Every refusal here closes the registered bao stream.
async fn accept_version_replication(
    context: &DriverContext,
    blob_handle: &BlobHandle,
    realm_id: RealmId,
    local_node: NodeId,
    peer: NodeId,
    stream_id: Ulid,
    manifest: VersionReplicationManifest,
) {
    debug!(
        peer = %peer,
        stream_id = %stream_id,
        bucket = %manifest.bucket,
        key = %manifest.key,
        version_id = %manifest.version_id,
        kind = ?manifest.kind,
        "Received inbound version replication manifest"
    );
    let watch_manifest = manifest.clone();
    // Only a materialized item can place a blob, so only it reads the caps.
    let routing = if manifest.kind == ReplicationItemKind::Materialized {
        match quota_marked_routing(context).await {
            Ok(routing) => routing,
            Err(error) => {
                error!(
                    peer = %peer,
                    error = %error,
                    "Refusing inbound replication with unreadable routing inputs"
                );
                blob_handle.close_stream(stream_id).await;
                return;
            }
        }
    } else {
        node_routing(context)
    };
    let (manifest_path, writer_path) =
        match manifest_policy(context, realm_id, local_node, &manifest).await {
            Ok(path) => path,
            Err(error) => {
                error!(
                    peer = %peer,
                    stream_id = %stream_id,
                    error = %error,
                    "Refusing inbound replication with unavailable request policy"
                );
                blob_handle.close_stream(stream_id).await;
                return;
            }
        };
    let gate = match gate_context(context, realm_id, now_ms()).await {
        Ok(gate) => gate,
        Err(error) => {
            error!(
                peer = %peer,
                stream_id = %stream_id,
                error = %error,
                "Refusing inbound replication with unreadable placement subject"
            );
            blob_handle.close_stream(stream_id).await;
            return;
        }
    };
    let mut op = IncomingVersionOperation::new(stream_id, local_node, realm_id, manifest)
        .with_routing(routing)
        .with_rocrate_limits(rocrate_limits.clone())
        .with_publisher_node(peer)
        .with_manifest_policy(manifest_path)
        .with_writer_policy(writer_path);
    if let Some(gate) = gate {
        op = op.with_gate(gate);
    }
    match drive(op, context).await {
        Ok(result) => {
            emit_replication_watch(context, local_node, &watch_manifest, &result).await;
        }
        Err(err) => {
            error!(error = ?err, "Failed to process inbound version replication stream");
            blob_handle.close_stream(stream_id).await;
        }
    }
}

/// Answers the bao read family: policy resolution, then the read operation;
/// a policy or drive failure refuses the stream.
async fn answer_bao_read(
    context: &DriverContext,
    blob_handle: &BlobHandle,
    realm_id: RealmId,
    local_node: NodeId,
    peer: NodeId,
    stream_id: Ulid,
    request: BaoReadRequest,
) {
    let (policy_paths, policy_candidates, had_denial) =
        match bao_policy(context, realm_id, local_node, &request).await {
            Ok(result) => result,
            Err(error) => {
                error!(
                    peer = %peer,
                    stream_id = %stream_id,
                    error = %error,
                    "Refusing inbound bao read with unavailable request policy"
                );
                blob_handle.close_stream(stream_id).await;
                return;
            }
        };
    let op = IncomingBaoOperation::new(peer, local_node, realm_id, stream_id, request)
        .with_policy_paths(policy_paths)
        .with_policy_candidates(policy_candidates, had_denial)
        .with_now(now_ms())
        .with_snapshot();
    if let Err(error) = drive(op, context).await {
        error!(
            peer = %peer,
            stream_id = %stream_id,
            error = ?error,
            "Failed to process inbound bao read"
        );
        blob_handle.close_stream(stream_id).await;
    }
}

/// Answers the location summary family. The summary describes what this
/// node holds, so the request's own authenticated context decides whether
/// anything is disclosed; a drive failure refuses the stream.
async fn answer_location_summary(
    context: &DriverContext,
    blob_handle: &BlobHandle,
    realm_id: RealmId,
    local_node: NodeId,
    peer: NodeId,
    stream_id: Ulid,
    request: LocationSummaryRequest,
) {
    let identity_allowed =
        request.realm_id == realm_id && auth_matches(&request.auth_context, realm_id);
    let op = LocationSummaryOperation::new_incoming(peer, local_node, stream_id, request)
        .with_policy(identity_allowed);
    if let Err(error) = drive(op, context).await {
        error!(
            peer = %peer,
            stream_id = %stream_id,
            error = ?error,
            "Failed to answer inbound location summary"
        );
        blob_handle.close_stream(stream_id).await;
    }
}

/// Answers which key this member's version id index holds for one version.
async fn answer_version_key(
    context: &DriverContext,
    blob_handle: &BlobHandle,
    stream_id: Ulid,
    set_id: Ulid,
    version_id: Ulid,
) {
    let key = crate::distribution::resolve::indexed_key(context, set_id, version_id)
        .await
        .ok()
        .flatten();
    let Ok(payload) = VersionReplicationMessage::VersionKeyResponse(key).to_bytes() else {
        blob_handle.close_stream(stream_id).await;
        return;
    };
    let sent = blob_handle
        .send_blob_effect(BlobEffect::SendMessage { stream_id, payload })
        .await;
    if let Event::Blob(BlobEvent::Error(error)) = sent {
        debug!(error = ?error, "Failed to answer a version key lookup");
    }
    blob_handle.close_stream(stream_id).await;
}

/// Answers which versions this member's content index holds for one identity.
async fn answer_content(
    context: &DriverContext,
    blob_handle: &BlobHandle,
    stream_id: Ulid,
    set_id: Ulid,
    identity: [u8; 32],
) {
    use crate::distribution::resolve::{MAX_CONTENT_VERSIONS, content_versions};
    let Ok((versions, more)) =
        content_versions(context, set_id, &identity, MAX_CONTENT_VERSIONS).await
    else {
        blob_handle.close_stream(stream_id).await;
        return;
    };
    let answer = VersionReplicationMessage::ContentResponse { versions, more };
    let Ok(payload) = answer.to_bytes() else {
        blob_handle.close_stream(stream_id).await;
        return;
    };
    let sent = blob_handle
        .send_blob_effect(BlobEffect::SendMessage { stream_id, payload })
        .await;
    if let Event::Blob(BlobEvent::Error(error)) = sent {
        debug!(error = ?error, "Failed to answer a content lookup");
    }
    blob_handle.close_stream(stream_id).await;
}

/// The bao plane input boundary: admission, bounded frame reads, and
/// family-specific refusals all happen here or in the family handlers.
pub(crate) async fn handle_bao_stream(
    context: &DriverContext,
    rocrate_limits: &RoCrateLimits,
    stream: BiStream,
    node_id: NodeId,
) {
    let Some(blob_handle) = context.blob_handle.clone() else {
        error!("Cannot handle incoming bao stream without blob handle");
        return;
    };
    let Some(net_handle) = context.net_handle.clone() else {
        error!(peer = %node_id, "Cannot handle incoming bao stream without net handle");
        return;
    };
    let realm_id = *net_handle.realm_id();
    let local_node = net_handle.node_id();
    // #332: only an authenticated sync-eligible realm peer
    // may open the blob replication plane at all.
    if bao_admission(context, realm_id, local_node, node_id)
        .await
        .is_none()
    {
        warn!(peer = %node_id, "Rejecting bao stream from non-sync-eligible peer");
        refuse_stream(stream);
        return;
    }
    let stream_id = match blob_handle
        .accept_stream(node_id, stream, INBOUND_BAO_TIMEOUT)
        .await
    {
        Ok(stream_id) => stream_id,
        Err(InboundError::TimedOut) => {
            warn!(peer = %node_id, "Timed out registering inbound bao stream");
            return;
        }
        Err(err) => {
            error!(peer = %node_id, error = ?err, "Failed to register inbound bao stream");
            return;
        }
    };
    let Some(admission) = bao_admission(context, realm_id, local_node, node_id).await else {
        warn!(peer = %node_id, "Rejecting bao stream from non-sync-eligible peer");
        blob_handle.close_stream(stream_id).await;
        return;
    };
    let payload = match blob_handle
        .first_frame(stream_id, INBOUND_BAO_TIMEOUT)
        .await
    {
        Ok(payload) => payload,
        Err(InboundError::TimedOut) => {
            warn!(peer = %node_id, stream_id = %stream_id, "Timed out reading inbound bao control message");
            blob_handle.close_stream(stream_id).await;
            return;
        }
        Err(InboundError::Blob(err)) => {
            error!(error = ?err, "Failed to read initial inbound bao payload");
            blob_handle.close_stream(stream_id).await;
            return;
        }
        Err(InboundError::Unexpected(event)) => {
            error!(event, "Unexpected first event for inbound bao stream");
            blob_handle.close_stream(stream_id).await;
            return;
        }
    };
    match VersionReplicationMessage::from_bytes(&payload) {
        Ok(VersionReplicationMessage::VersionManifest(manifest))
        | Ok(VersionReplicationMessage::ReferenceAdvance { manifest, .. }) => {
            // Only infrastructure may publish a self-identifying manifest.
            if admission != BaoAdmission::Infra {
                warn!(
                    peer = %node_id,
                    stream_id = %stream_id,
                    "Refusing a replication manifest from a device"
                );
                blob_handle.close_stream(stream_id).await;
                return;
            }
            accept_version_replication(
                context,
                rocrate_limits,
                &blob_handle,
                realm_id,
                local_node,
                node_id,
                stream_id,
                manifest,
            )
            .await;
        }
        Ok(VersionReplicationMessage::BaoReadRequest(request)) => {
            answer_bao_read(
                context,
                &blob_handle,
                realm_id,
                local_node,
                node_id,
                stream_id,
                request,
            )
            .await;
        }
        Ok(VersionReplicationMessage::LocationSummaryRequest(request)) => {
            // The summary describes what this node
            // holds; a device is never told.
            if admission != BaoAdmission::Infra {
                warn!(
                    peer = %node_id,
                    stream_id = %stream_id,
                    "Refusing a location summary to a device"
                );
                blob_handle.close_stream(stream_id).await;
                return;
            }
            answer_location_summary(
                context,
                &blob_handle,
                realm_id,
                local_node,
                node_id,
                stream_id,
                request,
            )
            .await;
        }
        Ok(VersionReplicationMessage::VersionKeyRequest {
            realm_id: asked,
            set_id,
            version_id,
        }) => {
            // Keys of a set are told to realm infrastructure only.
            if admission != BaoAdmission::Infra || asked != realm_id {
                warn!(peer = %node_id, "Refusing a version key lookup");
                blob_handle.close_stream(stream_id).await;
                return;
            }
            answer_version_key(context, &blob_handle, stream_id, set_id, version_id).await;
        }
        Ok(VersionReplicationMessage::ContentRequest {
            realm_id: asked,
            set_id,
            identity,
        }) => {
            if admission != BaoAdmission::Infra || asked != realm_id {
                warn!(peer = %node_id, "Refusing a content lookup");
                blob_handle.close_stream(stream_id).await;
                return;
            }
            answer_content(context, &blob_handle, stream_id, set_id, identity).await;
        }
        _ => {
            error!(
                peer = %node_id,
                stream_id = %stream_id,
                "Unsupported inbound bao payload"
            );
            blob_handle.close_stream(stream_id).await;
        }
    }
}

/// What the bao plane admitted a peer as. A device reaches the read branch and
/// nothing else, so replication stays infrastructure-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BaoAdmission {
    Infra,
    DeviceRead,
}

async fn emit_replication_watch(
    context: &DriverContext,
    node_id: aruna_core::NodeId,
    manifest: &VersionReplicationManifest,
    result: &IncomingVersionResult,
) {
    let Some(group_id) = result.group_id else {
        return;
    };
    if !result.applied || manifest.kind != ReplicationItemKind::Materialized {
        return;
    }
    let size_bytes = manifest.blob.as_ref().map_or(0, |blob| blob.size);
    let occurred_at_ms = aruna_core::time::unix_timestamp_millis();
    emit_watch_event(
        context,
        WatchEvent {
            event_id: Ulid::generate(),
            realm_id: manifest.auth_context.realm_id,
            kind: WatchEventKind::DataUploaded,
            path: watch_resource_path(group_id, node_id, &manifest.bucket, &manifest.key),
            actor: manifest.auth_context.user_id,
            occurred_at_ms,
            detail: WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                size_bytes,
            },
        },
    )
    .await;
}

async fn allow_policy(
    context: &DriverContext,
    auth: &AuthContext,
    path: &str,
    permission: &Permission,
    operation: &str,
) -> Result<bool, String> {
    match authorize(
        context,
        auth.realm_id,
        auth,
        path,
        permission,
        PolicyRequestExtras::operation(operation),
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(AuthorizeError::PermissionDenied)
        | Err(AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

/// A user of another realm signed in here matches; an anonymous identity only in its own realm.
fn auth_matches(auth: &AuthContext, realm_id: RealmId) -> bool {
    auth.realm_id == realm_id && (auth.user_id.realm_id == realm_id || !auth.user_id.is_nil())
}

async fn manifest_policy(
    context: &DriverContext,
    local_realm: RealmId,
    local_node: NodeId,
    manifest: &VersionReplicationManifest,
) -> Result<(Option<String>, Option<String>), String> {
    // Denied like a missing policy, so the sender stops and no destination is created. An unbound
    // folder stays `Deleting` until its row is gone, so checking before the create suffices.
    match guard_bucket_write(context, &manifest.bucket).await {
        Ok(()) => {}
        Err(OfferedDirectoryError::ReadOnly(_)) => return Ok((None, None)),
        Err(error) => return Err(error.to_string()),
    }
    let group_id = match drive(GetBucketOperation::new(manifest.bucket.clone()), context).await {
        Ok(info) => info.group_id,
        Err(GetBucketError::NotFound) => manifest.group_id,
        Err(error) => return Err(error.to_string()),
    };
    // The member admits a publication against its record of the set, not a user's grants.
    if manifest.distribution.is_some() {
        return Ok((None, None));
    }
    let path = if manifest.key.is_empty() {
        bucket_permission_path(local_realm, group_id, local_node, &manifest.bucket)
    } else {
        object_permission_path(
            local_realm,
            group_id,
            local_node,
            &manifest.bucket,
            &manifest.key,
        )
    };
    if manifest.reference_advance.is_some() {
        // The sync-eligible publisher attests source READ for this advance.
        return Ok(if auth_matches(&manifest.auth_context, local_realm) {
            (Some(path), None)
        } else {
            (None, None)
        });
    }
    let operation = if manifest.kind == ReplicationItemKind::DeleteMarker {
        "s3.DeleteObject"
    } else {
        "s3.PutObject"
    };
    if !auth_matches(&manifest.auth_context, local_realm)
        || !allow_policy(
            context,
            &manifest.auth_context,
            &path,
            &Permission::WRITE,
            operation,
        )
        .await?
    {
        return Ok((None, None));
    }
    let manifest_path = Some(path.clone());
    let writer_path = if let Some(auth) = manifest.writer_auth_context.as_ref()
        && auth_matches(auth, local_realm)
    {
        if allow_policy(context, auth, &path, &Permission::WRITE, operation).await? {
            Some(path)
        } else {
            None
        }
    } else {
        None
    };
    Ok((manifest_path, writer_path))
}

async fn bao_policy(
    context: &DriverContext,
    local_realm: RealmId,
    local_node: NodeId,
    request: &crate::replication::protocol::BaoReadRequest,
) -> Result<(HashSet<String>, Vec<HashIndex>, bool), String> {
    let mut paths = HashSet::new();
    if request.realm_id != local_realm || !auth_matches(&request.auth_context, request.realm_id) {
        return Ok((paths, Vec::new(), false));
    }
    match &request.target {
        crate::replication::protocol::BaoReadTarget::ExactVersion(target) => {
            if target.realm_id != request.realm_id || target.node_id != local_node {
                return Ok((paths, Vec::new(), false));
            }
            let group_id =
                match drive(GetBucketOperation::new(target.bucket.clone()), context).await {
                    Ok(info) => info.group_id,
                    Err(GetBucketError::NotFound) => {
                        return Ok((paths, Vec::new(), false));
                    }
                    Err(error) => return Err(error.to_string()),
                };
            let path = object_permission_path(
                request.realm_id,
                group_id,
                local_node,
                &target.bucket,
                &target.key,
            );
            if allow_policy(
                context,
                &request.auth_context,
                &path,
                &Permission::READ,
                "s3.GetObject",
            )
            .await?
            {
                paths.insert(path);
            }
            Ok((paths, Vec::new(), false))
        }
        crate::replication::protocol::BaoReadTarget::Blake3(hash) => {
            let candidates = drive(ResolvePathsOperation::new(*hash), context)
                .await
                .map_err(|error| error.to_string())?;
            let mut unique = BTreeMap::new();
            for candidate in candidates {
                if candidate.realm_id != request.realm_id
                    || candidate.node_id != local_node
                    || candidate.blake3_hash != *hash
                {
                    continue;
                }
                let key = candidate.to_bytes().map_err(|error| error.to_string())?;
                unique.entry(key).or_insert(candidate);
            }
            let candidates = unique.into_values().collect::<Vec<_>>();
            let evaluators = PolicyEvaluator::load_bulk(
                context,
                candidates
                    .iter()
                    .map(|candidate| (candidate.realm_id, candidate.group_id)),
            )
            .await
            .map_err(|error| error.to_string())?;
            let permissions = GroupPermissionRules::collect(
                context,
                Some(&request.auth_context),
                candidates.iter().map(|candidate| candidate.group_id),
            )
            .await;
            let mut allowed = Vec::with_capacity(candidates.len());
            let mut had_denial = false;
            for candidate in candidates {
                let path = candidate.permission_path();
                if !permissions.allows(candidate.group_id, &path, &Permission::READ) {
                    had_denial = true;
                    continue;
                }
                let request = policy_request_with(
                    &path,
                    &Permission::READ,
                    Some(&request.auth_context),
                    PolicyRequestExtras::operation("s3.GetObject"),
                );
                let evaluator = evaluators
                    .get(&(candidate.realm_id, candidate.group_id))
                    .ok_or_else(|| "hash policy evaluator is unavailable".to_string())?;
                match evaluator.evaluate(&request) {
                    Ok(()) => {
                        paths.insert(path);
                        allowed.push(candidate);
                    }
                    Err(PolicyEnforcementError::Denied { .. }) => had_denial = true,
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok((paths, allowed, had_denial))
        }
        // The serve finds the version in its member bucket and checks the caller in its
        // snapshot transaction, as for every target.
        crate::replication::protocol::BaoReadTarget::Version { .. } => {
            Ok((paths, Vec::new(), false))
        }
    }
}

#[cfg(test)]
#[path = "stream_tests.rs"]
mod tests;
