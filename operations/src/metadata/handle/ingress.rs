use super::peer_auth::authorize_peer;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::{Permission, SyncRelationship, bucket_permission_path};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::GroupId;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use tracing::{Span, field};

use super::effects::metadata_read_error;
use super::peer_auth::{bucket_search_auth, config_digest_matches};
use super::query::query_local_graphs;
use super::search::{clamp_remote_limit, search_local_graphs};
use super::transport::{
    close_stream, close_stream_at, drain_request_stream, drain_stream_at, metadata_body_limit,
    read_budget, transport_message_kind, write_body_at, write_message_at, write_stream_body,
    write_transport_message,
};
use super::{MetadataHandle, create_sync_bucket, sync_identity_matches, valid_sync_request};
use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::{MetadataAuthToken, MetadataReadError, MetadataTransportMessage};
use crate::s3::get_bucket::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::search_buckets::{SearchBucketsInput, search_local_buckets};
use crate::s3::search_objects::{SearchObjectsInput, search_local_objects};
use crate::sync::sync_relationship::{
    DeleteSyncRelationshipOperation, GetSyncRelationshipOperation, StoreSyncRelationshipOperation,
    SyncRelationshipDirection, SyncRelationshipError, remove_outgoing_relationship,
};

impl MetadataHandle {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn apply_sync_mirror(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        auth_token: Option<MetadataAuthToken>,
        relationship: SyncRelationship,
        source_group_id: Option<GroupId>,
        delete: bool,
        extras: PolicyRequestExtras,
    ) -> MetadataTransportMessage {
        let Some(net_handle) = self.inner.net_handle.as_ref() else {
            return MetadataTransportMessage::Reject("mirror_internal".to_string());
        };
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
            return delete_mirror(
                context,
                net_handle,
                &auth,
                &relationship,
                local_bucket,
                direction,
                extras,
            )
            .await;
        }

        let (group_id, create_bucket) = match drive(
            GetBucketInfoOperation::new(local_bucket.to_string()),
            context.as_ref(),
        )
        .await
        {
            Ok(Some(Ok(bucket_info))) => (bucket_info.group_id, false),
            Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => {
                let Some(source_group_id) = source_group_id else {
                    return MetadataTransportMessage::Reject("invalid_relationship".to_string());
                };
                (source_group_id, true)
            }
            Ok(Some(Err(_))) => {
                return MetadataTransportMessage::Reject("mirror_internal".to_string());
            }
            Err(_) => return MetadataTransportMessage::Reject("mirror_internal".to_string()),
        };
        let path = bucket_permission_path(
            *net_handle.realm_id(),
            group_id,
            net_handle.node_id(),
            local_bucket,
        );
        match authorize(
            context.as_ref(),
            *net_handle.realm_id(),
            &auth,
            &path,
            &Permission::WRITE,
            extras,
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
            StoreSyncRelationshipOperation::new(relationship, direction),
            context.as_ref(),
        )
        .await
        {
            Ok(_) => MetadataTransportMessage::SyncMirrorCreated,
            Err(_) => MetadataTransportMessage::Reject("mirror_internal".to_string()),
        }
    }
}

async fn delete_mirror(
    context: &Arc<DriverContext>,
    net_handle: &NetHandle,
    auth: &aruna_core::structs::AuthContext,
    relationship: &SyncRelationship,
    local_bucket: &str,
    direction: SyncRelationshipDirection,
    extras: PolicyRequestExtras,
) -> MetadataTransportMessage {
    let stored = match drive(
        GetSyncRelationshipOperation::new(relationship.id, direction),
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
    // A present bucket must pass the origin's authorization boundary.
    match drive(
        GetBucketInfoOperation::new(local_bucket.to_string()),
        context.as_ref(),
    )
    .await
    {
        Ok(Some(Ok(bucket_info))) => {
            let path = bucket_permission_path(
                *net_handle.realm_id(),
                bucket_info.group_id,
                net_handle.node_id(),
                local_bucket,
            );
            match authorize(
                context.as_ref(),
                *net_handle.realm_id(),
                auth,
                &path,
                &Permission::WRITE,
                extras,
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
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => {}
        Ok(Some(Err(_))) | Err(_) => {
            return MetadataTransportMessage::Reject("mirror_internal".to_string());
        }
    }
    // Retain the peer's readable reference records when detaching an outgoing mirror.
    let removed = match direction {
        SyncRelationshipDirection::Outgoing => {
            remove_outgoing_relationship(context.as_ref(), stored).await
        }
        SyncRelationshipDirection::Incoming => {
            drive(
                DeleteSyncRelationshipOperation::new(stored, direction),
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
    async fn graph_request(
        &self,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> MetadataTransportMessage {
        let response = match message {
            MetadataTransportMessage::QueryGraphs {
                auth_token,
                graph_iris,
                sparql,
            } => {
                Box::pin(async {
                    match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth_context) => {
                            match query_local_graphs(
                                self.inner.clone(),
                                auth_context,
                                graph_iris,
                                sparql,
                            )
                            .await
                            {
                                Ok(results) => MetadataTransportMessage::QueryResults {
                                    result: Ok(results),
                                },
                                Err(error) => MetadataTransportMessage::QueryResults {
                                    result: Err(metadata_read_error(error)),
                                },
                            }
                        }
                        Err(error) => MetadataTransportMessage::QueryResults { result: Err(error) },
                    }
                })
                .await
            }
            MetadataTransportMessage::SearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                group_id,
            } => {
                Box::pin(async {
                    match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth_context) => match search_local_graphs(
                            self.inner.clone(),
                            auth_context,
                            graph_iris,
                            query,
                            clamp_remote_limit(limit),
                            group_id,
                            None,
                        )
                        .await
                        {
                            Ok(hits) => {
                                MetadataTransportMessage::SearchResults { result: Ok(hits) }
                            }
                            Err(error) => MetadataTransportMessage::SearchResults {
                                result: Err(metadata_read_error(error)),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::SearchResults { result: Err(error) }
                        }
                    }
                })
                .await
            }
            MetadataTransportMessage::FilteredSearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                predicate_iri,
                object_iri,
                group_id,
            } => {
                Box::pin(async {
                    match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth_context) => match search_local_graphs(
                            self.inner.clone(),
                            auth_context,
                            graph_iris,
                            query,
                            clamp_remote_limit(limit),
                            group_id,
                            Some((predicate_iri, object_iri)),
                        )
                        .await
                        {
                            Ok(hits) => {
                                MetadataTransportMessage::SearchResults { result: Ok(hits) }
                            }
                            Err(error) => MetadataTransportMessage::SearchResults {
                                result: Err(metadata_read_error(error)),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::SearchResults { result: Err(error) }
                        }
                    }
                })
                .await
            }
            _ => unreachable!("request family routed incorrectly"),
        };
        response
    }

    async fn preflight_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> MetadataTransportMessage {
        let response = match message {
            MetadataTransportMessage::ReferencePreflight {
                auth_token,
                request,
            } => {
                Box::pin(async {
                    let result = match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth) => match context.net_handle.as_ref() {
                            Some(net) => {
                                let endpoint = crate::node::node_info::read_info_document(
                                    &context.storage_handle,
                                    net.node_id(),
                                )
                                .await
                                .ok()
                                .flatten()
                                .and_then(|document| document.urls.s3);
                                super::super::api::references_preflight_local(
                                    context.as_ref(),
                                    *net.realm_id(),
                                    net.node_id(),
                                    auth,
                                    *request,
                                    endpoint,
                                )
                                .await
                                .map(Box::new)
                                .map_err(super::super::forward::read_error)
                            }
                            None => Err(MetadataReadError::Unavailable),
                        },
                        Err(error) => Err(error),
                    };
                    MetadataTransportMessage::ReferencePreflightResults { result }
                })
                .await
            }
            _ => unreachable!("request family routed incorrectly"),
        };
        response
    }

    async fn bucket_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> MetadataTransportMessage {
        let response = match message {
            MetadataTransportMessage::SearchBuckets {
                auth_token,
                query,
                limit,
            } => {
                Box::pin(async {
                    match bucket_search_auth(
                        &self.inner.auth_validation,
                        &self.inner.storage_handle,
                        peer,
                        self.inner.net_handle.as_ref().map(|net| *net.realm_id()),
                        auth_token,
                    )
                    .await
                    {
                        Ok(auth) => match self.inner.net_handle.as_ref() {
                            Some(net_handle) => match search_local_buckets(
                                context.as_ref(),
                                SearchBucketsInput {
                                    auth,
                                    realm_id: *net_handle.realm_id(),
                                    node_id: net_handle.node_id(),
                                    query,
                                    limit,
                                    start_after: None,
                                },
                            )
                            .await
                            {
                                Ok(hits) => MetadataTransportMessage::BucketSearchResults {
                                    result: Ok(hits),
                                },
                                Err(_) => MetadataTransportMessage::BucketSearchResults {
                                    result: Err(MetadataReadError::Unavailable),
                                },
                            },
                            None => MetadataTransportMessage::BucketSearchResults {
                                result: Err(MetadataReadError::Unavailable),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::BucketSearchResults { result: Err(error) }
                        }
                    }
                })
                .await
            }
            MetadataTransportMessage::SearchObjects {
                auth_token,
                query,
                key_match,
                bucket,
                limit,
                start_after,
                as_of,
            } => {
                Box::pin(async {
                    match bucket_search_auth(
                        &self.inner.auth_validation,
                        &self.inner.storage_handle,
                        peer,
                        self.inner.net_handle.as_ref().map(|net| *net.realm_id()),
                        auth_token,
                    )
                    .await
                    {
                        Ok(auth) => match self.inner.net_handle.as_ref() {
                            Some(net_handle) => match search_local_objects(
                                context.as_ref(),
                                SearchObjectsInput {
                                    auth,
                                    realm_id: *net_handle.realm_id(),
                                    node_id: net_handle.node_id(),
                                    query,
                                    key_match,
                                    bucket,
                                    limit,
                                    start_after,
                                    as_of,
                                },
                            )
                            .await
                            {
                                Ok(page) => MetadataTransportMessage::ObjectSearchResults {
                                    result: Ok(page),
                                },
                                Err(_) => MetadataTransportMessage::ObjectSearchResults {
                                    result: Err(MetadataReadError::Unavailable),
                                },
                            },
                            None => MetadataTransportMessage::ObjectSearchResults {
                                result: Err(MetadataReadError::Unavailable),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::ObjectSearchResults { result: Err(error) }
                        }
                    }
                })
                .await
            }
            _ => unreachable!("request family routed incorrectly"),
        };
        response
    }

    async fn mirror_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> MetadataTransportMessage {
        let response = match message {
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
                    )
                    .await
                })
                .await
            }
            MetadataTransportMessage::DeleteSyncMirror {
                auth_token,
                relationship,
                extras,
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
                    )
                    .await
                })
                .await
            }
            _ => unreachable!("request family routed incorrectly"),
        };
        response
    }

    async fn path_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> MetadataTransportMessage {
        let response = match message {
            query @ MetadataTransportMessage::QueryDocument { .. } => {
                Box::pin(async {
                    let result =
                        super::super::forward::apply_document_query(context, peer, query).await;
                    MetadataTransportMessage::DocumentQueryResults { result }
                })
                .await
            }
            MetadataTransportMessage::ForwardPathLookup {
                auth_token,
                group_id,
                document_path,
                config_digest,
            } => {
                Box::pin(async {
                    let result = match self.authorize_read_peer(peer, auth_token, true).await {
                        Ok(auth) => match context.net_handle.as_ref() {
                            Some(net) => {
                                let realm_id = *net.realm_id();
                                if !config_digest_matches(
                                    context.as_ref(),
                                    realm_id,
                                    &config_digest,
                                )
                                .await
                                {
                                    Err(MetadataReadError::Unavailable)
                                } else {
                                    let result = super::super::api::local_path_candidates(
                                        context.as_ref(),
                                        realm_id,
                                        group_id,
                                        &document_path,
                                        auth.as_ref(),
                                    )
                                    .await
                                    .map_err(super::super::forward::read_error);
                                    if !config_digest_matches(
                                        context.as_ref(),
                                        realm_id,
                                        &config_digest,
                                    )
                                    .await
                                    {
                                        Err(MetadataReadError::Unavailable)
                                    } else {
                                        result
                                    }
                                }
                            }
                            None => Err(MetadataReadError::Unavailable),
                        },
                        Err(error) => Err(error),
                    };
                    MetadataTransportMessage::ForwardedPathLookup { result }
                })
                .await
            }
            MetadataTransportMessage::ForwardPathResolution {
                auth_token,
                group_id,
                document_path,
                config_digest,
            } => {
                Box::pin(async {
                    let result = match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth) => match context.net_handle.as_ref() {
                            Some(net) => {
                                let realm_id = *net.realm_id();
                                if !config_digest_matches(
                                    context.as_ref(),
                                    realm_id,
                                    &config_digest,
                                )
                                .await
                                {
                                    Err(MetadataReadError::Unavailable)
                                } else {
                                    let result = super::super::api::resolve_local_path(
                                        context.as_ref(),
                                        realm_id,
                                        super::super::api::MetadataPathLookupRequest {
                                            group_id,
                                            document_path,
                                            auth,
                                        },
                                    )
                                    .await
                                    .map(|result| {
                                        Box::new(super::super::protocol::MetadataPathResolution {
                                            winner: result.winner,
                                            conflicts: result.conflicts,
                                        })
                                    })
                                    .map_err(super::super::forward::read_error);
                                    if !config_digest_matches(
                                        context.as_ref(),
                                        realm_id,
                                        &config_digest,
                                    )
                                    .await
                                    {
                                        Err(MetadataReadError::Unavailable)
                                    } else {
                                        result
                                    }
                                }
                            }
                            None => Err(MetadataReadError::Unavailable),
                        },
                        Err(error) => Err(error),
                    };
                    MetadataTransportMessage::ForwardedPathResolution { result }
                })
                .await
            }
            _ => unreachable!("request family routed incorrectly"),
        };
        response
    }

    async fn export_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        metadata_bytes: u64,
        message: MetadataTransportMessage,
    ) -> (MetadataTransportMessage, Option<Vec<u8>>) {
        let mut response_body = None;
        let response = match message {
            forward @ MetadataTransportMessage::ForwardExportDocument { .. } => {
                Box::pin(async {
                    match super::super::forward::apply_forwarded_export(
                        context,
                        peer,
                        forward,
                        metadata_bytes,
                    )
                    .await
                    {
                        Ok((export, metadata_bytes)) => match postcard::to_allocvec(&export) {
                            Ok(bytes) => {
                                let length = bytes.len() as u64;
                                if length > metadata_body_limit(metadata_bytes) {
                                    MetadataTransportMessage::ForwardedExport {
                                        result: Err(MetadataReadError::Unavailable),
                                    }
                                } else {
                                    response_body = Some(bytes);
                                    MetadataTransportMessage::ForwardedExport { result: Ok(length) }
                                }
                            }
                            Err(_) => MetadataTransportMessage::ForwardedExport {
                                result: Err(MetadataReadError::Unavailable),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::ForwardedExport { result: Err(error) }
                        }
                    }
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardExportProfile { .. } => {
                Box::pin(async {
                    match super::super::forward::apply_forwarded_profile(
                        context,
                        peer,
                        forward,
                        metadata_bytes,
                    )
                    .await
                    {
                        Ok((export, metadata_bytes)) => match postcard::to_allocvec(&export) {
                            Ok(bytes) => {
                                let length = bytes.len() as u64;
                                if length > metadata_body_limit(metadata_bytes) {
                                    MetadataTransportMessage::ForwardedExport {
                                        result: Err(MetadataReadError::Unavailable),
                                    }
                                } else {
                                    response_body = Some(bytes);
                                    MetadataTransportMessage::ForwardedExport { result: Ok(length) }
                                }
                            }
                            Err(_) => MetadataTransportMessage::ForwardedExport {
                                result: Err(MetadataReadError::Unavailable),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::ForwardedExport { result: Err(error) }
                        }
                    }
                })
                .await
            }
            _ => unreachable!("request family routed incorrectly"),
        };
        (response, response_body)
    }

    async fn forward_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        audit_deadline: tokio::time::Instant,
        message: MetadataTransportMessage,
    ) -> MetadataTransportMessage {
        let response = match message {
            forward @ (MetadataTransportMessage::ForwardCreateDocument { .. }
            | MetadataTransportMessage::ForwardUpdateDocument { .. }
            | MetadataTransportMessage::ForwardDeleteDocument { .. }
            | MetadataTransportMessage::ForwardReadDocument { .. }
            | MetadataTransportMessage::ForwardProfileValidationStatus { .. }) => {
                Box::pin(async {
                    super::super::forward::apply_forwarded_write(context, peer, forward).await
                })
                .await
            }
            MetadataTransportMessage::ForwardAuditPage { request } => {
                Box::pin(async {
                    super::super::audit::serve_local_audit(context, peer, request, audit_deadline)
                        .await
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardTokenRevocation { .. } => {
                Box::pin(async {
                    super::super::forward::apply_token_revoke(context, peer, forward).await
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardPersistentId { .. } => {
                Box::pin(async {
                    super::super::forward::apply_forwarded_pid(context, peer, forward).await
                })
                .await
            }
            MetadataTransportMessage::ForwardPlacementPolicy { policy_ref } => {
                Box::pin(async {
                    crate::placement::policy::serve_local_policy(context, peer, policy_ref).await
                })
                .await
            }
            record @ (MetadataTransportMessage::ForwardJobRecord { .. }
            | MetadataTransportMessage::ForwardJobRecordPage { .. }) => {
                Box::pin(async {
                    crate::jobs::records::serve_job_record(context, peer, record).await
                })
                .await
            }
            MetadataTransportMessage::ForwardLaunchOffer { launch } => {
                Box::pin(async {
                    crate::jobs::records::serve_launch_offer(context, peer, *launch).await
                })
                .await
            }
            MetadataTransportMessage::ForwardJobSubmission {
                auth_token,
                submission_id,
                request,
            } => {
                Box::pin(async {
                    crate::jobs::lifecycle::ingress::serve_submission(
                        context,
                        peer,
                        auth_token,
                        submission_id,
                        *request,
                    )
                    .await
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardCreatePlacementPolicy { .. } => {
                Box::pin(async {
                    crate::placement::policy::apply_forwarded_policy(context, peer, forward).await
                })
                .await
            }
            pull @ MetadataTransportMessage::ForwardSyncPull { .. } => {
                Box::pin(async {
                    super::super::device_pull::serve_sync_pull(context, peer, pull).await
                })
                .await
            }
            listing @ MetadataTransportMessage::ForwardListVersions { .. } => {
                Box::pin(async {
                    super::super::device_pull::serve_list_versions(context, peer, listing).await
                })
                .await
            }
            create @ MetadataTransportMessage::ForwardCreateBucket { .. } => {
                Box::pin(async {
                    super::super::forward::apply_bucket_create(context, peer, create).await
                })
                .await
            }
            fetch @ MetadataTransportMessage::FetchRealmDocuments { .. } => {
                Box::pin(async {
                    super::super::forward::serve_realm_documents(context, peer, fetch).await
                })
                .await
            }
            fetch @ MetadataTransportMessage::FetchGraphState { .. } => {
                Box::pin(async {
                    super::super::forward::serve_graph_state(context, peer, fetch).await
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardApplyBatch { .. } => {
                Box::pin(async {
                    super::super::forward::apply_device_batch(context, peer, forward).await
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardAdminEvent { .. } => {
                Box::pin(async {
                    super::super::forward::apply_admin_relay(context, peer, forward).await
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardGroupCreate { .. } => {
                Box::pin(async {
                    super::super::forward::apply_group_create(context, peer, forward).await
                })
                .await
            }
            _ => unreachable!("request family routed incorrectly"),
        };
        response
    }

    #[tracing::instrument(
        name = "metadata.remote.inbound",
        level = "debug",
        skip(self, stream),
        fields(
            peer = ?peer,
            request = field::Empty,
            response = field::Empty,
            read_ms = field::Empty,
            process_ms = field::Empty,
            drain_ms = field::Empty,
            write_ms = field::Empty,
            elapsed_ms = field::Empty,
        )
    )]
    pub async fn handle_inbound_stream(
        &self,
        context: &Arc<DriverContext>,
        mut stream: BiStream,
        peer: NodeId,
        metadata_bytes: u64,
    ) -> Result<(), MetadataError> {
        let total_started = Instant::now();
        let audit_deadline = tokio::time::Instant::now()
            + Duration::from_secs(super::super::audit::AUDIT_DEADLINE_SECS);
        let read_started = Instant::now();
        let (message, frame_budget) =
            read_budget(&mut stream.1, &self.inner.inbound_frame_bytes).await?;
        let is_audit = matches!(&message, MetadataTransportMessage::ForwardAuditPage { .. });
        let span = Span::current();
        record_elapsed_ms(&span, "read_ms", read_started);
        span.record("request", transport_message_kind(&message));

        let process_started = Instant::now();
        let (response, response_body) = match message {
            message @ (MetadataTransportMessage::QueryGraphs { .. }
            | MetadataTransportMessage::SearchGraphs { .. }
            | MetadataTransportMessage::FilteredSearchGraphs { .. }) => {
                (self.graph_request(peer, message).await, None)
            }
            message @ MetadataTransportMessage::ReferencePreflight { .. } => {
                (self.preflight_request(context, peer, message).await, None)
            }
            message @ (MetadataTransportMessage::SearchBuckets { .. }
            | MetadataTransportMessage::SearchObjects { .. }) => {
                (self.bucket_request(context, peer, message).await, None)
            }
            message @ (MetadataTransportMessage::CreateSyncMirror { .. }
            | MetadataTransportMessage::DeleteSyncMirror { .. }) => {
                (self.mirror_request(context, peer, message).await, None)
            }
            message @ (MetadataTransportMessage::QueryDocument { .. }
            | MetadataTransportMessage::ForwardPathLookup { .. }
            | MetadataTransportMessage::ForwardPathResolution { .. }) => {
                (self.path_request(context, peer, message).await, None)
            }
            message @ (MetadataTransportMessage::ForwardExportDocument { .. }
            | MetadataTransportMessage::ForwardExportProfile { .. }) => {
                self.export_request(context, peer, metadata_bytes, message)
                    .await
            }
            message @ (MetadataTransportMessage::ForwardCreateDocument { .. }
            | MetadataTransportMessage::ForwardUpdateDocument { .. }
            | MetadataTransportMessage::ForwardDeleteDocument { .. }
            | MetadataTransportMessage::ForwardReadDocument { .. }
            | MetadataTransportMessage::ForwardProfileValidationStatus { .. }
            | MetadataTransportMessage::ForwardAuditPage { .. }
            | MetadataTransportMessage::ForwardTokenRevocation { .. }
            | MetadataTransportMessage::ForwardPersistentId { .. }
            | MetadataTransportMessage::ForwardPlacementPolicy { .. }
            | MetadataTransportMessage::ForwardJobRecord { .. }
            | MetadataTransportMessage::ForwardJobRecordPage { .. }
            | MetadataTransportMessage::ForwardLaunchOffer { .. }
            | MetadataTransportMessage::ForwardJobSubmission { .. }
            | MetadataTransportMessage::ForwardCreatePlacementPolicy { .. }
            | MetadataTransportMessage::ForwardSyncPull { .. }
            | MetadataTransportMessage::ForwardListVersions { .. }
            | MetadataTransportMessage::ForwardCreateBucket { .. }
            | MetadataTransportMessage::FetchRealmDocuments { .. }
            | MetadataTransportMessage::FetchGraphState { .. }
            | MetadataTransportMessage::ForwardApplyBatch { .. }
            | MetadataTransportMessage::ForwardAdminEvent { .. }
            | MetadataTransportMessage::ForwardGroupCreate { .. }) => (
                self.forward_request(context, peer, audit_deadline, message)
                    .await,
                None,
            ),
            _ => (
                MetadataTransportMessage::Reject("unexpected metadata control message".to_string()),
                None,
            ),
        };
        record_elapsed_ms(&span, "process_ms", process_started);

        let drain_started = Instant::now();
        let drain_result = if is_audit {
            drain_stream_at(&mut stream.1, audit_deadline).await
        } else {
            drain_request_stream(&mut stream).await
        };
        if let Err(error) = drain_result {
            if is_audit {
                close_stream_at(&mut stream, audit_deadline);
            }
            return Err(error);
        }
        record_elapsed_ms(&span, "drain_ms", drain_started);

        let write_started = Instant::now();
        let response_written = if is_audit {
            write_message_at(&mut stream, &response, audit_deadline)
                .await
                .is_ok()
        } else {
            write_transport_message(&mut stream, &response)
                .await
                .is_ok()
        };
        if response_written && let Some(body) = response_body {
            if is_audit {
                let _ = write_body_at(&mut stream, &body, audit_deadline).await;
            } else {
                let _ = write_stream_body(&mut stream, &body).await;
            }
        }
        record_elapsed_ms(&span, "write_ms", write_started);
        if is_audit {
            close_stream_at(&mut stream, audit_deadline);
        } else {
            close_stream(&mut stream).await;
        }
        drop(frame_budget);
        record_elapsed_ms(&span, "elapsed_ms", total_started);
        span.record("response", transport_message_kind(&response));
        Ok(())
    }
}
