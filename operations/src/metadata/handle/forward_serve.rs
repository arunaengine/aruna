//! Serves the metadata writes, exports, Profile fetches and document queries that peers
//! forward here. The checks are operations; this file only drives them for the ingress.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::document_repository::GetConfigOperation;
use crate::driver::DriverContext;
use crate::driver::drive;
use crate::driver::drive_retry;
use crate::forward::authorize::ForwardAuthError;
use crate::forward::authorize::ForwardedCallerOperation;
use crate::forward::authorize::forward_auth_error;
use crate::forward::authorize::write_check;
use crate::forward::replay::CreateRowsOperation;
use crate::forward::replay::HeldRecordError;
use crate::forward::replay::HeldRecordOperation;
use crate::forward::replay::replay_answer;
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::forwarded_unavailable;
use crate::forward::transport::read_error;
use crate::forward::transport::reject;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::VisibleRecordOperation;
use crate::metadata::api::export_types::ExportMetadataRequest;
use crate::metadata::api::export_types::ExportMetadataResult;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::create_document::CreateDocumentConfig;
use crate::metadata::create_document::CreateDocumentError;
use crate::metadata::create_document::CreateDocumentOperation;
use crate::metadata::create_entry::CreateEntryOperation;
use crate::metadata::delete_document::DeleteDocumentError;
use crate::metadata::delete_document::DeleteDocumentOperation;
use crate::metadata::forward::profile_export::ProfileLocalOperation;
use crate::metadata::forward::read::admits_profile_peer;
use crate::metadata::forward::routed_export::ExportWithinOperation;
use crate::metadata::forward::routed_export::ensure_export_limit;
use crate::metadata::live_records::LiveRecordOperation;
use crate::metadata::persistent_id::forward::pid_authority_node;
use crate::metadata::profile::validation::RevalidateOperation;
use crate::metadata::profile::validation::ValidationStatusOperation;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::read_peer::ReadPeerOperation;
use crate::metadata::update_document::UpdateDocumentConfig;
use crate::metadata::update_document::UpdateDocumentError;
use crate::metadata::update_document::UpdateDocumentOperation;
use crate::metadata::update_entry::UpdateEntryOperation;
use crate::realm::load_config::config_unavailable;
use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::metadata::MetadataError;
use aruna_core::metadata::{MetadataEffect, MetadataEvent, MetadataQueryResults};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;

/// Applies a write forwarded by a non-holder under the caller's authority.
/// Re-runs the HTTP permission checks (a routing hop, not a trust bypass) and
/// gates on realm membership, not sync-eligibility (user nodes must forward).
pub(crate) async fn apply_forwarded_write(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    // Debug poll frames reserve stack for every branch at once, so each awaiting
    // branch is boxed to keep only the active one on the stack.
    let Some(net_handle) = context.net_handle.as_ref() else {
        return forwarded_unavailable(&message);
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = drive(GetConfigOperation::new(realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
    else {
        return forwarded_unavailable(&message);
    };
    let expected_digest = match &message {
        MetadataTransportMessage::ForwardCreateDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardUpdateDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardDeleteDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardReadDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardValidationStatus { config_digest, .. } => *config_digest,
        _ => return reject("unexpected forwarded metadata message"),
    };
    if config.digest().ok() != Some(expected_digest) {
        return forwarded_unavailable(&message);
    };

    if let MetadataTransportMessage::ForwardReadDocument {
        auth_token,
        document_id,
        ..
    } = &message
    {
        return Box::pin(async {
            if context.metadata_handle.is_none() {
                return reject("forwarded metadata read needs a metadata handle");
            }
            let check = ReadPeerOperation::new((peer, realm_id), auth_token.clone(), false);
            let result = match drive(check, context.as_ref()).await {
                Ok(auth)
                    if holds_metadata_id(&config, realm_id, net_handle.node_id(), *document_id) =>
                {
                    drive(
                        VisibleRecordOperation::new(
                            realm_id,
                            GetVisibleRequest {
                                document_id: *document_id,
                                auth,
                            },
                        ),
                        context.as_ref(),
                    )
                    .await
                    .map(Box::new)
                    .map_err(read_error)
                }
                Ok(_) => Err(MetadataReadError::Unavailable),
                Err(error) => Err(error),
            };
            MetadataTransportMessage::ForwardedRead { result }
        })
        .await;
    }

    if let MetadataTransportMessage::ForwardValidationStatus {
        auth_token,
        document_id,
        revalidate,
        ..
    } = &message
    {
        return Box::pin(async {
            if context.metadata_handle.is_none() {
                return MetadataTransportMessage::ForwardedValidationStatus {
                    result: Err(MetadataReadError::Unavailable),
                };
            }
            let check = ReadPeerOperation::new((peer, realm_id), auth_token.clone(), false);
            let result = match drive(check, context.as_ref()).await {
                Ok(auth)
                    if holds_metadata_id(&config, realm_id, net_handle.node_id(), *document_id) =>
                {
                    let live = drive(LiveRecordOperation::new(*document_id), context.as_ref());
                    let record = match live.await {
                        Ok(Some(record)) => record,
                        Ok(None) => {
                            return MetadataTransportMessage::ForwardedValidationStatus {
                                result: Err(read_error(MetadataApiError::NotFound)),
                            };
                        }
                        Err(error) => {
                            return MetadataTransportMessage::ForwardedValidationStatus {
                                result: Err(read_error(error.into())),
                            };
                        }
                    };
                    let readable = RecordReadableOperation::new(realm_id, auth, record.clone());
                    if let Err(error) = drive(readable, context.as_ref()).await {
                        return MetadataTransportMessage::ForwardedValidationStatus {
                            result: Err(read_error(error)),
                        };
                    }
                    if *revalidate {
                        drive(
                            RevalidateOperation::new(
                                context
                                    .as_ref()
                                    .net_handle
                                    .as_ref()
                                    .map(|net| net.node_id()),
                                record.clone(),
                            ),
                            context.as_ref(),
                        )
                        .await
                    } else {
                        drive(
                            ValidationStatusOperation::new(record.clone()),
                            context.as_ref(),
                        )
                        .await
                    }
                    .map(Box::new)
                    .map_err(|_| MetadataReadError::Unavailable)
                }
                Ok(_) => Err(MetadataReadError::Unavailable),
                Err(error) => Err(error),
            };
            MetadataTransportMessage::ForwardedValidationStatus { result }
        })
        .await;
    }

    let auth = match drive(
        ForwardedCallerOperation::new(peer, realm_id, &message),
        context,
    )
    .await
    {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };

    match message {
        MetadataTransportMessage::ForwardCreateDocument {
            group_id,
            document_id,
            document_path,
            public,
            payload,
            commit_message,
            ..
        } => {
            Box::pin(async {
                let normalized_document_path =
                    MetadataRegistryRecord::normalize_document_path(&document_path);
                if normalized_document_path.is_empty() {
                    return reject("forwarded metadata create has an empty document path");
                }
                let path = MetadataRegistryRecord::permission_path_for(
                    &realm_id,
                    group_id,
                    &normalized_document_path,
                    document_id,
                );
                if let Err(error) = drive(write_check(&auth, &path), context)
                    .await
                    .map_err(ForwardAuthError::from)
                {
                    return forward_auth_error(error);
                }
                let create_config = CreateDocumentConfig {
                    actor: Actor {
                        node_id: net_handle.node_id(),
                        user_id: auth.user_id,
                        realm_id,
                    },
                    group_id,
                    document_id,
                    document_path,
                    public,
                    payload,
                };
                match forwarded_create_replay(context, &create_config).await {
                    Ok(Some(response)) => return response,
                    Ok(None) => {}
                    Err(error) => return reject(error),
                }
                let operation = CreateDocumentOperation::new_forwarded(create_config.clone())
                    .with_message(commit_message);
                match drive(
                    CreateEntryOperation::new(
                        operation,
                        context.net_handle.as_ref().map(|net| net.node_id()),
                    ),
                    &context,
                )
                .await
                {
                    Ok(created) => MetadataTransportMessage::ForwardedRecord {
                        record: Box::new(created.record),
                    },
                    // Lost the race against a concurrent delivery of the same
                    // forward: the winner's record is the answer, not an error.
                    Err(CreateDocumentError::DocumentAlreadyExists) => {
                        match forwarded_create_replay(context, &create_config).await {
                            Ok(Some(response)) => response,
                            Ok(None) => reject(format!(
                                "forwarded metadata create for `{document_id}` raced a delete"
                            )),
                            Err(error) => reject(error),
                        }
                    }
                    Err(CreateDocumentError::MetadataError(MetadataError::ProfileValidation(
                        findings,
                    ))) => MetadataTransportMessage::ForwardedProfileValidation { findings },
                    Err(error) => reject(format!("forwarded metadata create failed: {error}")),
                }
            })
            .await
        }
        MetadataTransportMessage::ForwardUpdateDocument {
            document_id,
            public,
            mutation,
            expected_revision,
            commit_message,
            ..
        } => {
            Box::pin(async {
                let record = match drive(
                    HeldRecordOperation::new(config.clone(), net_handle.node_id(), document_id),
                    context,
                )
                .await
                {
                    Ok(record) => record,
                    Err(HeldRecordError::NotFound) => {
                        return MetadataTransportMessage::WriteNotFound;
                    }
                    Err(HeldRecordError::Unavailable(error)) => {
                        warn!(%document_id, %error, "Forwarded metadata update is unavailable");
                        return MetadataTransportMessage::ForwardedWriteUnavailable;
                    }
                };
                if let Err(error) =
                    drive(write_check(&auth, &record.permission_path.clone()), context)
                        .await
                        .map_err(ForwardAuthError::from)
                {
                    return forward_auth_error(error);
                }
                let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
                    actor: Actor {
                        node_id: net_handle.node_id(),
                        user_id: auth.user_id,
                        realm_id,
                    },
                    group_id: record.group_id,
                    document_id,
                    public: public.unwrap_or(record.public),
                    mutation,
                    expected_revision,
                })
                .with_message(commit_message);
                match drive(
                    UpdateEntryOperation::new(
                        operation,
                        context
                            .as_ref()
                            .net_handle
                            .as_ref()
                            .map(|net| net.node_id()),
                    ),
                    context.as_ref(),
                )
                .await
                {
                    Ok(record) => MetadataTransportMessage::ForwardedRecord {
                        record: Box::new(record),
                    },
                    Err(UpdateDocumentError::RawLimit) => {
                        MetadataTransportMessage::MetadataHistoryCapacity
                    }
                    Err(UpdateDocumentError::RevisionConflict { expected, current }) => {
                        MetadataTransportMessage::ForwardedRevisionConflict { expected, current }
                    }
                    Err(UpdateDocumentError::GraphPending { expected }) => {
                        MetadataTransportMessage::ForwardedGraphPending { expected }
                    }
                    Err(UpdateDocumentError::MetadataError(MetadataError::InvalidInput(
                        message,
                    ))) => MetadataTransportMessage::UpdateInvalidInput { message },
                    Err(UpdateDocumentError::MetadataError(MetadataError::ProfileValidation(
                        findings,
                    ))) => MetadataTransportMessage::ForwardedProfileValidation { findings },
                    Err(error) => reject(format!("forwarded metadata update failed: {error}")),
                }
            })
            .await
        }
        MetadataTransportMessage::ForwardDeleteDocument { document_id, .. } => {
            Box::pin(async {
                let record = match drive(
                    HeldRecordOperation::new(config.clone(), net_handle.node_id(), document_id),
                    context,
                )
                .await
                {
                    Ok(record) => record,
                    Err(HeldRecordError::NotFound) => {
                        return MetadataTransportMessage::WriteNotFound;
                    }
                    Err(HeldRecordError::Unavailable(error)) => {
                        warn!(%document_id, %error, "Forwarded metadata delete is unavailable");
                        return MetadataTransportMessage::ForwardedWriteUnavailable;
                    }
                };
                if pid_authority_node(&config, realm_id, document_id) != Some(net_handle.node_id())
                {
                    return MetadataTransportMessage::ForwardedWriteUnavailable;
                }
                if let Err(error) =
                    drive(write_check(&auth, &record.permission_path.clone()), context)
                        .await
                        .map_err(ForwardAuthError::from)
                {
                    return forward_auth_error(error);
                }
                let operation = DeleteDocumentOperation::new(
                    Actor {
                        node_id: net_handle.node_id(),
                        user_id: auth.user_id,
                        realm_id,
                    },
                    record.group_id,
                    document_id,
                );
                match delete_metadata_document(operation, context.as_ref(), document_id).await {
                    Ok(()) => MetadataTransportMessage::ForwardedDelete,
                    Err(error) => reject(format!("forwarded metadata delete failed: {error}")),
                }
            })
            .await
        }
        other => reject(format!(
            "unexpected forwarded metadata message: {}",
            crate::metadata::message_kind::transport_message_kind(&other)
        )),
    }
}

/// Deletes a document held here, retrying lost conflicts, and drops it from the registry cache.
async fn delete_metadata_document(
    operation: DeleteDocumentOperation,
    context: &DriverContext,
    document_id: Ulid,
) -> Result<(), DeleteDocumentError> {
    drive_retry(|| operation.fresh_copy(), context, &document_id.to_bytes()).await?;
    if let Some(metadata_handle) = context.metadata_handle.as_ref() {
        metadata_handle.remove_cached_record(document_id);
    }
    Ok(())
}

/// The stored answer when a forwarded create was already applied here.
async fn forwarded_create_replay(
    context: &Arc<DriverContext>,
    config: &CreateDocumentConfig,
) -> Result<Option<MetadataTransportMessage>, String> {
    let rows = drive(CreateRowsOperation::new(config.document_id), context)
        .await
        .map_err(|error| format!("metadata create replay read failed: {error}"))?;
    rows.map(|rows| replay_answer(config, rows)).transpose()
}

/// Answers a peer's Profile fetch. Every gate fails closed and none of them
/// reads a caller identity, because the channel never carries one.
pub(crate) async fn apply_forwarded_profile(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
    local_limit: u64,
) -> Result<(ExportMetadataResult, u64), MetadataReadError> {
    let MetadataTransportMessage::ForwardExportProfile {
        config_digest,
        profile_id,
        expected_revision,
    } = message
    else {
        return Err(MetadataReadError::Unavailable);
    };
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
        .ok_or(MetadataReadError::Unavailable)?;
    if config.digest().ok() != Some(config_digest) {
        return Err(MetadataReadError::Unavailable);
    }
    if !admits_profile_peer(&config, peer, realm_id) {
        return Err(MetadataReadError::Forbidden);
    }
    if !holds_metadata_id(&config, realm_id, net_handle.node_id(), profile_id) {
        return Err(MetadataReadError::Unavailable);
    }
    let local = ProfileLocalOperation::new(realm_id, profile_id, expected_revision);
    let export = drive(local, context.as_ref()).await?;
    ensure_export_limit(&export, local_limit).map_err(read_error)?;
    Ok((export, local_limit))
}

pub(crate) async fn apply_forwarded_export(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
    local_limit: u64,
) -> Result<(ExportMetadataResult, u64), MetadataReadError> {
    let MetadataTransportMessage::ForwardExportDocument {
        auth_token,
        config_digest,
        document_id,
        view,
        metadata_bytes,
        limit,
        offset,
        after,
    } = message
    else {
        return Err(MetadataReadError::Unavailable);
    };
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
        .ok_or(MetadataReadError::Unavailable)?;
    if config.digest().ok() != Some(config_digest) {
        return Err(MetadataReadError::Unavailable);
    }
    context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let check = ReadPeerOperation::new((peer, realm_id), auth_token, false);
    let auth = drive(check, context.as_ref()).await?;
    if !holds_metadata_id(&config, realm_id, net_handle.node_id(), document_id) {
        return Err(MetadataReadError::Unavailable);
    }
    let metadata_bytes = metadata_bytes.min(local_limit);
    let request = ExportMetadataRequest {
        document_id,
        auth,
        view,
        limit,
        offset,
        after,
    };
    let export = ExportWithinOperation::new(realm_id, request, metadata_bytes);
    let export = drive(export, context.as_ref()).await.map_err(read_error)?;
    Ok((export, metadata_bytes))
}

pub(crate) async fn apply_document_query(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataQueryResults, MetadataReadError> {
    let MetadataTransportMessage::QueryDocument {
        auth_token,
        config_digest,
        document_id,
        sparql,
    } = message
    else {
        return Err(MetadataReadError::Unavailable);
    };
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
        .ok_or(MetadataReadError::Unavailable)?;
    if config.digest().ok() != Some(config_digest)
        || !holds_metadata_id(&config, realm_id, net_handle.node_id(), document_id)
    {
        return Err(MetadataReadError::Unavailable);
    }
    context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let check = ReadPeerOperation::new((peer, realm_id), auth_token, false);
    let auth = drive(check, context.as_ref()).await?;
    let record = drive(
        VisibleRecordOperation::new(
            realm_id,
            GetVisibleRequest {
                document_id,
                auth: auth.clone(),
            },
        ),
        context.as_ref(),
    )
    .await
    .map_err(read_error)?;
    let query = MetadataEffect::QueryGraphs {
        auth_context: auth,
        graph_iris: Some(vec![record.graph_iri]),
        sparql,
    };
    match super::send_metadata_effect(context, query).await {
        Event::Metadata(MetadataEvent::QueryResult { results }) => Ok(results),
        _ => Err(MetadataReadError::Unavailable),
    }
}
