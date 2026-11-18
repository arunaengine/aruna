//! Answers inbound bao streams: peer admission, manifests, reads, summaries and index lookups.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashSet};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_resource_path,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{
    BucketInfo, HashIndex, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::replication::ReplicationItemKind;
use aruna_core::structs::storage::routing::NodeRouting;
use thiserror::Error;
use tracing::{debug, error, warn};
use ulid::Ulid;

use super::{IncomingVersionError, IncomingVersionOperation, IncomingVersionResult};
use crate::auth::enforcement::{PolicyBulkOperation, PolicyEvaluators};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::permission_rules::GroupPermissionRules;
use crate::auth::request_authorization::{AuthorizeOperation, GroupRulesOperation};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::blob::permission_paths::{ResolvePathsError, ResolvePathsOperation};
use crate::distribution::resolve::{
    MAX_CONTENT_VERSIONS, ResolveError, content_versions, indexed_key,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::groups::routing::{QuotaOperation, RoutingInputsError};
use crate::notifications::watch::origin::EmitEventOperation;
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::realm::mutate_placement::node_kind;
use crate::replication::bao_read::{BaoReadError, IncomingBaoOperation, IncomingBaoResult};
use crate::replication::locations::{LocalSummary, LocationSummaryError, LocationSummaryOperation};
use crate::replication::protocol::{
    BaoReadRequest, BaoReadTarget, LocationSummaryRequest, VersionReplicationManifest,
    VersionReplicationMessage,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::staging::bucket_guard::{BucketGuardError, BucketGuardOperation};
use crate::staging::offered_directory::OfferedDirectoryError;

type StreamTurn = Result<Next<()>, IoError>;
type ManifestPaths = (Option<String>, Option<String>);
type ReadPolicy = (HashSet<String>, Vec<HashIndex>, bool);
type ContentPage = (Vec<(Ulid, String)>, bool);

fn close_call(stream_id: Ulid) -> IoCall {
    Effect::Blob(BlobEffect::CloseConnection { stream_id }).into()
}

fn closed(result: IoResult) -> StreamTurn {
    if let Event::Blob(BlobEvent::Error(error)) = result.event()? {
        error!(error = ?error, "Failed to close rejected inbound bao stream");
    }
    Ok(Next::Done(()))
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
    let emit = EmitEventOperation::new(WatchEvent {
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
    });
    // Failures only warn inside the operation.
    let _ = drive(emit, context).await;
}

async fn allow_policy(
    context: &DriverContext,
    auth: &AuthContext,
    path: &str,
    permission: &Permission,
    operation: &str,
) -> Result<bool, String> {
    match drive(
        AuthorizeOperation::new(
            auth.realm_id,
            auth,
            path,
            permission,
            PolicyRequestExtras::operation(operation),
        ),
        context,
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
    match drive(BucketGuardOperation::new(manifest.bucket.as_str()), context)
        .await
        .map_err(OfferedDirectoryError::from)
    {
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
            let evaluators = drive(
                PolicyBulkOperation::new(
                    candidates
                        .iter()
                        .map(|candidate| (candidate.realm_id, candidate.group_id)),
                ),
                context,
            )
            .await
            .map_err(|error| error.to_string())?;
            let permissions = drive(
                GroupRulesOperation::new(
                    Some(&request.auth_context),
                    candidates.iter().map(|candidate| candidate.group_id),
                ),
                context,
            )
            .await
            .unwrap_or_default();
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
