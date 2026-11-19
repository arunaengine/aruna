//! Serves inbound metadata stream requests from peers: query, search, sync and mirror delete.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::SyncRelationship;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::GroupId;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use tracing::Span;

use super::peer_auth::authorize_peer;
use super::transport::{
    close_stream, close_stream_at, drain_request_stream, drain_stream_at, read_budget,
    write_body_at, write_message_at, write_stream_body, write_transport_message,
};
use super::{
    MetadataHandle, RollbackRequest, create_sync_bucket, sync_identity_matches, valid_sync_request,
};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::forward::realm::ApplyRelayOperation;
use crate::forward::token::ApplyRevokeOperation;
use crate::forward::transport::reject;
use crate::metadata::message_kind::transport_message_kind;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::metadata::read_peer::ReadPeerOperation;
use crate::metadata::serve::{ServeError, ServeNode, ServeRequestOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::staging::bucket_guard::BucketGuardOperation;
use crate::staging::offered_directory::OfferedDirectoryError;
use crate::sync::sync_relationship::{
    DeleteRelationshipOperation, GetRelationshipOperation, RemoveOutgoingOperation,
    StoreRelationshipOperation, SyncRelationshipDirection, SyncRelationshipError,
};
use aruna_core::metadata::AuthToken;

impl MetadataHandle {
    /// The caller of a read `peer` forwarded, as [`ReadPeerOperation`] checks it.
    async fn read_caller(
        &self,
        context: &DriverContext,
        peer: NodeId,
        auth_token: Option<AuthToken>,
        trusted: bool,
    ) -> Result<Option<AuthContext>, MetadataReadError> {
        let net = self.inner.net_handle.as_ref();
        let realm_id = net
            .map(|net| *net.realm_id())
            .ok_or(MetadataReadError::Unavailable)?;
        let check = ReadPeerOperation::new((peer, realm_id), auth_token, trusted);
        drive(check, context).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn apply_sync_mirror(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        auth_token: Option<AuthToken>,
        relationship: SyncRelationship,
        source_group_id: Option<GroupId>,
        delete: bool,
        extras: PolicyRequestExtras,
        rollback: bool,
    ) -> MetadataTransportMessage {
        let Some(net_handle) = self.inner.net_handle.as_ref() else {
            return MetadataTransportMessage::Reject("mirror_internal".to_string());
        };
        let vouched = matches!(auth_token, Some(AuthToken::Internal(_)));
        let auth = match authorize_peer(
            &self.inner.auth_validation,
            &self.inner.storage_handle,
            peer,
            Some(*net_handle.realm_id()),
            auth_token,
            true,
        )
        .await
        {
            Ok(Some(auth)) => auth,
            Ok(None) => {
                return MetadataTransportMessage::Reject("access_denied".to_string());
            }
            Err(_) => return MetadataTransportMessage::Reject("access_denied".to_string()),
        };

        let Some(source_bucket) = relationship.source.bucket() else {
            return MetadataTransportMessage::Reject("invalid_relationship".to_string());
        };
        let Some(target_bucket) = relationship.target.bucket() else {
            return MetadataTransportMessage::Reject("invalid_relationship".to_string());
        };
        if !valid_sync_request(
            &relationship,
            source_bucket,
            target_bucket,
            *net_handle.realm_id(),
            auth.user_id,
            delete,
        ) {
            return MetadataTransportMessage::Reject("invalid_relationship".to_string());
        }

        let local_node = net_handle.node_id();
        let (local_bucket, direction) =
            if relationship.source.node_id == peer && relationship.target.node_id == local_node {
                (target_bucket, SyncRelationshipDirection::Incoming)
            } else if delete
                && relationship.target.node_id == peer
                && relationship.source.node_id == local_node
            {
                (source_bucket, SyncRelationshipDirection::Outgoing)
            } else {
                return MetadataTransportMessage::Reject("invalid_relationship".to_string());
            };

        if delete {
            let rollback = rollback.then_some(RollbackRequest {
                peer,
                local_node,
                vouched,
            });
            return delete_mirror(
                context,
                net_handle,
                &auth,
                &relationship,
                local_bucket,
                direction,
                extras,
                rollback,
            )
            .await;
        }

        // A read-only bucket refuses a mirror like a denied write, before any bucket is created.
        match drive(BucketGuardOperation::new(local_bucket), context.as_ref())
            .await
            .map_err(OfferedDirectoryError::from)
        {
            Ok(()) => {}
            Err(OfferedDirectoryError::ReadOnly(_)) => {
                return MetadataTransportMessage::Reject("access_denied".to_string());
            }
            Err(_) => return MetadataTransportMessage::Reject("mirror_internal".to_string()),
        }
        let (group_id, create_bucket) = match drive(
            GetBucketOperation::new(local_bucket.to_string()),
            context.as_ref(),
        )
        .await
        {
            Ok(bucket_info) => (bucket_info.group_id, false),
            Err(GetBucketError::NotFound) => {
                let Some(source_group_id) = source_group_id else {
                    return MetadataTransportMessage::Reject("invalid_relationship".to_string());
                };
                (source_group_id, true)
            }
            Err(_) => return MetadataTransportMessage::Reject("mirror_internal".to_string()),
        };
        let path = bucket_permission_path(
            *net_handle.realm_id(),
            group_id,
            net_handle.node_id(),
            local_bucket,
        );
        match drive(
            AuthorizeOperation::new(
                *net_handle.realm_id(),
                &auth,
                &path,
                &Permission::WRITE,
                extras,
            ),
            context.as_ref(),
        )
        .await
        {
            Ok(()) => {}
            Err(AuthorizeError::CheckFailed(_) | AuthorizeError::Storage(_)) => {
                return MetadataTransportMessage::Reject("mirror_internal".to_string());
            }
            Err(_) => return MetadataTransportMessage::Reject("access_denied".to_string()),
        }
        if create_bucket
            && create_sync_bucket(context.as_ref(), local_bucket, group_id, &relationship)
                .await
                .is_err()
        {
            return MetadataTransportMessage::Reject("mirror_internal".to_string());
        }

        match drive(
            StoreRelationshipOperation::new(relationship, direction),
            context.as_ref(),
        )
        .await
        {
            Ok(_) => MetadataTransportMessage::SyncMirrorCreated,
            Err(_) => MetadataTransportMessage::Reject("mirror_internal".to_string()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn delete_mirror(
    context: &Arc<DriverContext>,
    net_handle: &NetHandle,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    relationship: &SyncRelationship,
    local_bucket: &str,
    direction: SyncRelationshipDirection,
    extras: PolicyRequestExtras,
    rollback: Option<RollbackRequest>,
) -> MetadataTransportMessage {
    let stored = match drive(
        GetRelationshipOperation::new(relationship.id, direction),
        context.as_ref(),
    )
    .await
    {
        Ok(stored) => stored,
        Err(SyncRelationshipError::NotFound) => {
            return MetadataTransportMessage::SyncMirrorDeleted;
        }
        Err(_) => return MetadataTransportMessage::Reject("mirror_internal".to_string()),
    };
    if !sync_identity_matches(&stored, relationship) {
        return MetadataTransportMessage::Reject("invalid_relationship".to_string());
    }
    if let Some(request) = rollback {
        // A rollback never falls back to the WRITE-checked delete.
        if !request.allows(&stored, relationship) {
            return MetadataTransportMessage::Reject("invalid_relationship".to_string());
        }
    } else {
        // A present bucket must pass the origin's authorization boundary.
        match drive(
            GetBucketOperation::new(local_bucket.to_string()),
            context.as_ref(),
        )
        .await
        {
            Ok(bucket_info) => {
                let path = bucket_permission_path(
                    *net_handle.realm_id(),
                    bucket_info.group_id,
                    net_handle.node_id(),
                    local_bucket,
                );
                match drive(
                    AuthorizeOperation::new(
                        *net_handle.realm_id(),
                        auth,
                        &path,
                        &Permission::WRITE,
                        extras,
                    ),
                    context.as_ref(),
                )
                .await
                {
                    Ok(()) => {}
                    Err(AuthorizeError::CheckFailed(_) | AuthorizeError::Storage(_)) => {
                        return MetadataTransportMessage::Reject("mirror_internal".to_string());
                    }
                    Err(_) => return MetadataTransportMessage::Reject("access_denied".to_string()),
                }
            }
            Err(GetBucketError::NotFound) => {}
            Err(_) => {
                return MetadataTransportMessage::Reject("mirror_internal".to_string());
            }
        }
    }
    // Retain the peer's readable reference records when detaching an outgoing mirror.
    let removed = match direction {
        SyncRelationshipDirection::Outgoing => {
            drive(RemoveOutgoingOperation::new(stored), context.as_ref()).await
        }
        SyncRelationshipDirection::Incoming => {
            drive(
                DeleteRelationshipOperation::new(stored, direction),
                context.as_ref(),
            )
            .await
        }
    };
    match removed {
        Ok(()) => MetadataTransportMessage::SyncMirrorDeleted,
        Err(_) => MetadataTransportMessage::Reject("mirror_internal".to_string()),
    }
}

impl MetadataHandle {
    async fn mirror_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> MetadataTransportMessage {
        match message {
            MetadataTransportMessage::CreateSyncMirror {
                auth_token,
                source_group_id,
                relationship,
                extras,
            } => {
                Box::pin(async {
                    self.apply_sync_mirror(
                        context,
                        peer,
                        auth_token,
                        *relationship,
                        Some(source_group_id),
                        false,
                        extras,
                        false,
                    )
                    .await
                })
                .await
            }
            MetadataTransportMessage::DeleteSyncMirror {
                auth_token,
                relationship,
                extras,
                rollback,
            } => {
                Box::pin(async {
                    self.apply_sync_mirror(
                        context,
                        peer,
                        auth_token,
                        *relationship,
                        None,
                        true,
                        extras,
                        rollback,
                    )
                    .await
                })
                .await
            }
            _ => unreachable!("request family routed incorrectly"),
        }
    }

    /// This node's audit page for a peer, under its forwarded caller and the merge deadline.
    async fn serve_audit(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        request: aruna_core::audit::AuditPageRequest,
        deadline: tokio::time::Instant,
    ) -> Result<aruna_core::audit::AuditPageResponse, MetadataReadError> {
        use crate::metadata::audit_entry::{ServeAuditOperation, inbound_admission};
        let _admission = inbound_admission().ok_or(MetadataReadError::Unavailable)?;
        let node = context
            .node_and_realm()
            .ok_or(MetadataReadError::Unavailable)?;
        let auth_token = request.auth_token.clone();
        let auth =
            tokio::time::timeout_at(deadline, self.read_caller(context, peer, auth_token, false))
                .await
                .map_err(|_| MetadataReadError::Unavailable)??
                .ok_or(MetadataReadError::Unauthorized)?;
        let serve = ServeAuditOperation::new((node.1, node.0), auth, request);
        crate::driver::drive_until(serve, context.as_ref(), deadline).await
    }

    async fn forward_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        audit_deadline: tokio::time::Instant,
        message: MetadataTransportMessage,
    ) -> MetadataTransportMessage {
        match message {
            forward @ (MetadataTransportMessage::ForwardCreateDocument { .. }
            | MetadataTransportMessage::ForwardUpdateDocument { .. }
            | MetadataTransportMessage::ForwardDeleteDocument { .. }
            | MetadataTransportMessage::ForwardReadDocument { .. }
            | MetadataTransportMessage::ForwardValidationStatus { .. }) => {
                Box::pin(async {
                    super::forward_serve::apply_forwarded_write(context, peer, forward).await
                })
                .await
            }
            MetadataTransportMessage::ForwardAuditPage { request } => {
                let result =
                    Box::pin(self.serve_audit(context, peer, request, audit_deadline)).await;
                MetadataTransportMessage::ForwardedAuditPage { result }
            }
            forward @ MetadataTransportMessage::ForwardTokenRevocation { .. } => {
                Box::pin(async {
                    drive(
                        ApplyRevokeOperation::new(context.node_and_realm(), peer, forward),
                        context,
                    )
                    .await
                    .unwrap_or_else(|error| reject(format!("token revocation failed: {error}")))
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardPersistentId { .. } => {
                use crate::metadata::persistent_id::forward::ServePidOperation;
                let Some((node_id, realm_id)) = context.node_and_realm() else {
                    return MetadataTransportMessage::ForwardedWriteUnavailable;
                };
                match ServePidOperation::new(peer, (realm_id, node_id), forward) {
                    Some(serve) => Box::pin(drive(serve, context))
                        .await
                        .unwrap_or_else(|error| {
                            reject(format!("persistent id request failed: {error}"))
                        }),
                    None => reject("unexpected forwarded persistent id message"),
                }
            }
            MetadataTransportMessage::ForwardPlacementPolicy { policy_ref } => {
                let result = match context.net_handle.as_ref() {
                    Some(net) => {
                        let serve = crate::placement::policy::ServePolicyOperation::new(
                            *net.realm_id(),
                            peer,
                            policy_ref,
                        );
                        crate::driver::drive(serve, context.as_ref())
                            .await
                            .and_then(|answer| answer)
                    }
                    None => Err(MetadataReadError::Unavailable),
                };
                MetadataTransportMessage::ForwardedPlacementPolicy { result }
            }
            forward @ MetadataTransportMessage::ForwardVaultChange { .. } => {
                let apply = crate::users::vault_route::ApplyVaultOperation::new(
                    context.node_and_realm(),
                    peer,
                    forward,
                );
                Box::pin(crate::driver::drive(apply, context.as_ref()))
                    .await
                    .unwrap_or_else(|error| {
                        tracing::warn!(%error, "Forwarded vault change failed");
                        MetadataTransportMessage::ForwardedWriteUnavailable
                    })
            }
            MetadataTransportMessage::FetchVaultRecords { user_id, query } => {
                let serve = crate::users::vault_route::ServeVaultOperation::new(
                    context.node_and_realm(),
                    peer,
                    user_id,
                    query,
                );
                MetadataTransportMessage::FetchedVaultRecords {
                    result: Box::pin(crate::driver::drive(serve, context.as_ref())).await,
                }
            }
            forward @ MetadataTransportMessage::ForwardCreatePolicy { .. } => {
                let apply = crate::placement::policy::ApplyPolicyOperation::new(
                    context.node_and_realm(),
                    peer,
                    forward,
                );
                Box::pin(crate::driver::drive(apply, context.as_ref()))
                    .await
                    .unwrap_or_else(|error| reject(error.to_string()))
            }
            pull @ MetadataTransportMessage::ForwardSyncPull { .. } => {
                Box::pin(async { super::pull_serve::serve_sync_pull(context, peer, pull).await })
                    .await
            }
            poll @ MetadataTransportMessage::ForwardPullStatus { .. } => {
                Box::pin(async { super::pull_serve::serve_pull_status(context, peer, poll).await })
                    .await
            }
            listing @ MetadataTransportMessage::ForwardListVersions { .. } => {
                Box::pin(async {
                    super::pull_serve::serve_list_versions(context, peer, listing).await
                })
                .await
            }
            bucket @ (MetadataTransportMessage::ForwardCheckBucket { .. }
            | MetadataTransportMessage::ForwardCreateBucket { .. }
            | MetadataTransportMessage::ForwardNewBucket { .. }) => {
                Box::pin(async {
                    let local = context.node_and_realm();
                    let answer = crate::s3::bucket::forward::BucketForwardOperation::new(
                        local, peer, bucket,
                    );
                    crate::driver::drive(answer, context)
                        .await
                        .unwrap_or_else(|error| {
                            crate::forward::transport::reject(error.to_string())
                        })
                })
                .await
            }
            admit @ MetadataTransportMessage::ForwardAdmitMember { .. } => {
                Box::pin(async {
                    let serve =
                        crate::distribution::forward::serve_admission(context.here(), peer, admit);
                    crate::driver::drive(serve, context)
                        .await
                        .unwrap_or_else(|error| {
                            crate::forward::transport::reject(error.to_string())
                        })
                })
                .await
            }
            publish @ (MetadataTransportMessage::ForwardDistributionRequest { .. }
            | MetadataTransportMessage::SubmitDistributionStatement { .. }) => {
                Box::pin(async {
                    let serve = crate::distribution::forward::serve_publish(
                        context.node_and_realm(),
                        peer,
                        publish,
                    );
                    crate::driver::drive(serve, context)
                        .await
                        .unwrap_or_else(|error| {
                            crate::forward::transport::reject(error.to_string())
                        })
                })
                .await
            }
            fetch @ MetadataTransportMessage::FetchRealmDocuments { .. } => {
                Box::pin(async {
                    drive(
                        crate::device::forward::serve_realm_documents(
                            context.node_and_realm(),
                            peer,
                            fetch,
                        ),
                        context,
                    )
                    .await
                    .unwrap_or_else(|error| crate::forward::transport::reject(error.to_string()))
                })
                .await
            }
            fetch @ MetadataTransportMessage::FetchGraphState { .. } => {
                Box::pin(async {
                    drive(
                        crate::device::forward::serve_graph_state(
                            context.node_and_realm(),
                            peer,
                            fetch,
                        ),
                        context,
                    )
                    .await
                    .unwrap_or_else(|error| crate::forward::transport::reject(error.to_string()))
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardApplyBatch { .. } => {
                Box::pin(async {
                    drive(
                        crate::device::forward::apply_device_batch(
                            context.node_and_realm(),
                            peer,
                            forward,
                        ),
                        context,
                    )
                    .await
                    .unwrap_or_else(|error| crate::forward::transport::reject(error.to_string()))
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardAdminEvent { .. } => {
                Box::pin(async {
                    drive(
                        ApplyRelayOperation::new(context.node_and_realm(), peer, forward),
                        context,
                    )
                    .await
                    .unwrap_or_else(|error| {
                        tracing::warn!(%error, "Failed to persist a relayed admin event");
                        MetadataTransportMessage::ForwardedWriteUnavailable
                    })
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardGroupCreate { .. } => {
                let apply = crate::groups::forward::ApplyGroupOperation::new(
                    context.node_and_realm(),
                    peer,
                    forward,
                );
                Box::pin(crate::driver::drive(apply, context.as_ref()))
                    .await
                    .unwrap_or_else(|error| reject(format!("group create failed: {error}")))
            }
            request @ MetadataTransportMessage::GroupDeletion { .. } => {
                let apply = crate::groups::deletion::ApplyRequestOperation::new(
                    context.node_and_realm(),
                    peer,
                    request,
                    ulid::Ulid::generate(),
                );
                MetadataTransportMessage::GroupDeletionResult {
                    result: Box::pin(crate::driver::drive(apply, context.as_ref())).await,
                }
            }
            _ => unreachable!("request family routed incorrectly"),
        }
    }

    /// Reads one request frame from an inbound metadata stream under the shared frame budget
    /// and names it on the current span. Returns the deadline an audit request keeps.
    pub(crate) async fn read_request(
        &self,
        stream: &mut BiStream,
    ) -> Result<InboundRequest, MetadataError> {
        let audit_deadline = tokio::time::Instant::now()
            + Duration::from_secs(super::super::audit::AUDIT_DEADLINE_SECS);
        let (message, budget) = read_budget(&mut stream.1, &self.inner.inbound_frame_bytes).await?;
        Span::current().record("request", transport_message_kind(&message));
        let audit = matches!(&message, MetadataTransportMessage::ForwardAuditPage { .. });
        Ok(InboundRequest {
            message,
            budget,
            audit_deadline,
            audit,
        })
    }

    /// Answers one metadata request from `peer`; requests of other domains are rejected.
    pub(crate) async fn answer_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        metadata_bytes: u64,
        audit_deadline: tokio::time::Instant,
        message: MetadataTransportMessage,
    ) -> (MetadataTransportMessage, Option<Vec<u8>>) {
        let serving = ServeNode {
            local: context.node_and_realm(),
            graph: context.graph_handle.is_some(),
            indexed: context.storage_handle.range_indexes().is_some(),
            metadata_bytes,
        };
        let serve = ServeRequestOperation::new(peer, serving, message);
        let message = match drive(serve, context.as_ref()).await {
            Ok(reply) => return reply,
            Err(ServeError::Unserved(message)) => *message,
            Err(ServeError::Io(error)) => return (reject(error.to_string()), None),
        };
        match message {
            message @ (MetadataTransportMessage::CreateSyncMirror { .. }
            | MetadataTransportMessage::DeleteSyncMirror { .. }) => {
                (self.mirror_request(context, peer, message).await, None)
            }
            message @ (MetadataTransportMessage::ForwardCreateDocument { .. }
            | MetadataTransportMessage::ForwardUpdateDocument { .. }
            | MetadataTransportMessage::ForwardDeleteDocument { .. }
            | MetadataTransportMessage::ForwardReadDocument { .. }
            | MetadataTransportMessage::ForwardValidationStatus { .. }
            | MetadataTransportMessage::ForwardAuditPage { .. }
            | MetadataTransportMessage::ForwardTokenRevocation { .. }
            | MetadataTransportMessage::ForwardPersistentId { .. }
            | MetadataTransportMessage::ForwardPlacementPolicy { .. }
            | MetadataTransportMessage::ForwardVaultChange { .. }
            | MetadataTransportMessage::FetchVaultRecords { .. }
            | MetadataTransportMessage::ForwardCreatePolicy { .. }
            | MetadataTransportMessage::ForwardSyncPull { .. }
            | MetadataTransportMessage::ForwardPullStatus { .. }
            | MetadataTransportMessage::ForwardListVersions { .. }
            | MetadataTransportMessage::ForwardCreateBucket { .. }
            | MetadataTransportMessage::ForwardNewBucket { .. }
            | MetadataTransportMessage::ForwardCheckBucket { .. }
            | MetadataTransportMessage::ForwardAdmitMember { .. }
            | MetadataTransportMessage::ForwardDistributionRequest { .. }
            | MetadataTransportMessage::SubmitDistributionStatement { .. }
            | MetadataTransportMessage::FetchRealmDocuments { .. }
            | MetadataTransportMessage::FetchGraphState { .. }
            | MetadataTransportMessage::ForwardApplyBatch { .. }
            | MetadataTransportMessage::ForwardAdminEvent { .. }
            | MetadataTransportMessage::ForwardGroupCreate { .. }
            | MetadataTransportMessage::GroupDeletion { .. }) => (
                self.forward_request(context, peer, audit_deadline, message)
                    .await,
                None,
            ),
            _ => (
                MetadataTransportMessage::Reject("unexpected metadata control message".to_string()),
                None,
            ),
        }
    }
}

/// One request read from an inbound metadata stream.
pub(crate) struct InboundRequest {
    pub(crate) message: MetadataTransportMessage,
    /// Held until the answer is written, so frames in flight stay within the budget.
    pub(crate) budget: tokio::sync::OwnedSemaphorePermit,
    pub(crate) audit_deadline: tokio::time::Instant,
    pub(crate) audit: bool,
}

impl MetadataHandle {
    /// Drains the request, writes the answer and closes the stream. An audit request keeps
    /// the audit deadline for every step.
    pub(crate) async fn write_answer(
        mut stream: BiStream,
        audit_deadline: Option<tokio::time::Instant>,
        response: &MetadataTransportMessage,
        response_body: Option<Vec<u8>>,
    ) -> Result<(), MetadataError> {
        let span = Span::current();
        span.record("response", transport_message_kind(response));
        let drain_started = Instant::now();
        let drain_result = match audit_deadline {
            Some(deadline) => drain_stream_at(&mut stream.1, deadline).await,
            None => drain_request_stream(&mut stream).await,
        };
        if let Err(error) = drain_result {
            if let Some(deadline) = audit_deadline {
                close_stream_at(&mut stream, deadline);
            }
            return Err(error);
        }
        record_elapsed_ms(&span, "drain_ms", drain_started);

        let write_started = Instant::now();
        let response_written = match audit_deadline {
            Some(deadline) => write_message_at(&mut stream, response, deadline)
                .await
                .is_ok(),
            None => write_transport_message(&mut stream, response).await.is_ok(),
        };
        if response_written && let Some(body) = response_body {
            let _ = match audit_deadline {
                Some(deadline) => write_body_at(&mut stream, &body, deadline).await,
                None => write_stream_body(&mut stream, &body).await,
            };
        }
        record_elapsed_ms(&span, "write_ms", write_started);
        match audit_deadline {
            Some(deadline) => close_stream_at(&mut stream, deadline),
            None => close_stream(&mut stream).await,
        }
        Ok(())
    }
}
