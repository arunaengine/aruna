//! Routes metadata create, update, delete and batch writes to the nodes that hold them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::AuthorizeError;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::device::edit::DeviceEditError;
use crate::device::edit::accepts_edits;
use crate::device::edit::apply_local_edit;
use crate::driver::{DriverContext, drive, drive_retry};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::VisibleRecordOperation;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::api::read::ensure_record_readable;
use crate::metadata::api::read::load_live_record;
use crate::metadata::create_document::CreateDocumentConfig;
use crate::metadata::create_document::CreateDocumentError;
use crate::metadata::create_document::CreateDocumentOperation;
use crate::metadata::create_document::CreateDocumentPayload;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::create_entry::CreateEntryOperation;
use crate::metadata::delete_document::{DeleteDocumentError, DeleteDocumentOperation};
use crate::metadata::profile::validation::{RevalidateOperation, ValidationStatusOperation};
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::update_document::UpdateDocumentConfig;
use crate::metadata::update_document::UpdateDocumentError;
use crate::metadata::update_document::UpdateDocumentMutation;
use crate::metadata::update_document::UpdateDocumentOperation;
use crate::metadata::update_entry::UpdateEntryOperation;
use crate::realm::mutate_placement::node_kind;
use aruna_core::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::metadata::MetadataError;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::read::device_replica;
use crate::document_repository::GetConfigOperation;
use crate::forward::authorize::ForwardAuthError;
use crate::forward::authorize::ForwardedCallerOperation;
use crate::forward::authorize::forward_auth_error;
use crate::forward::authorize::write_check;
use crate::forward::replay::HeldRecordError;
use crate::forward::replay::HeldRecordOperation;
use crate::forward::replay::routed_record_matches;
use crate::forward::replay::{CreateRowsOperation, replay_answer};
use crate::forward::routing::ForwardHoldersOperation;
use crate::forward::routing::holder_intersection;
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::MetadataWriteError;
use crate::forward::transport::forwarded_unavailable;
use crate::forward::transport::read_error;
use crate::forward::transport::reject;
use crate::forward::transport::unexpected_response;
use crate::metadata::persistent_id::forward::pid_authority_node;
use crate::realm::load_config::config_unavailable;

/// Errors of the shared authorized create flow. Every variant maps onto the
/// same transport status the former REST-local sequence produced.
#[derive(Debug, Error, PartialEq)]
pub enum CreateAuthorizedError {
    #[error("metadata document path must not be empty")]
    EmptyPath,
    #[error("metadata create is forbidden")]
    Forbidden,
    #[error(transparent)]
    Api(#[from] MetadataApiError),
    #[error(transparent)]
    Create(#[from] CreateDocumentError),
    #[error(transparent)]
    Authorize(#[from] AuthorizeError),
    #[error(transparent)]
    Write(#[from] MetadataWriteError),
}

/// A runner failure leaves the create undelivered.
impl From<aruna_core::calls::IoError> for CreateAuthorizedError {
    fn from(error: aruna_core::calls::IoError) -> Self {
        Self::Write(error.into())
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn route_metadata_update(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    public: Option<bool>,
    mutation: UpdateDocumentMutation,
    expected_revision: Option<Ulid>,
    auth_token: Option<AuthToken>,
    commit_message: Option<String>,
) -> Result<MetadataRegistryRecord, MetadataWriteError> {
    let config = drive(GetConfigOperation::new(actor.realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
    // On a device a selected or read document is edited locally and queued; a holder sees the
    // change set when the intake drain forwards it. Only a holder can check an expected revision
    // or change visibility, so those always go to a holder and fail while none answers.
    let by_owner =
        node_kind(&config, actor.node_id).and_then(|kind| kind.owner()) == Some(actor.user_id);
    if expected_revision.is_none()
        && public.is_none()
        && let Some(replica) =
            device_replica(context, &config, Some(actor.node_id), document_id).await
        && accepts_edits(&replica, by_owner)
    {
        let issued_at = crate::device::publish_queue::token_issued_at(auth_token.as_ref());
        return apply_local_edit(
            context,
            actor.user_id,
            actor.node_id,
            &replica,
            mutation,
            issued_at,
        )
        .await
        .map_err(device_edit_error);
    }
    let placement = resolve_metadata_id(
        &config,
        actor.realm_id,
        record.map(|record| record.group_id),
        document_id,
    )
    .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let config_digest = config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    if record.is_some_and(|record| {
        !routed_record_matches(&config, actor.realm_id, document_id, &placement, record)
    }) {
        return Err(MetadataWriteError::Undeliverable(
            "local metadata registry record does not match the routed document".to_string(),
        ));
    }
    let local_node_id = actor.node_id;
    let current_holders = resolve_shard_holders(&config, &placement);
    let holders = record.map_or_else(
        || current_holders.clone(),
        |record| holder_intersection(&current_holders, &record.holder_node_ids),
    );
    if record.is_some() && holders.is_empty() {
        return Err(MetadataWriteError::Undeliverable(
            "metadata document has no active frozen holder with history capacity".to_string(),
        ));
    }
    let local_holds = holders.contains(&local_node_id);
    let mut local_capacity = false;
    if local_holds && let Some(record) = record {
        match drive(
            UpdateEntryOperation::new(
                UpdateDocumentOperation::new(UpdateDocumentConfig {
                    actor: actor.clone(),
                    group_id: record.group_id,
                    document_id,
                    public: public.unwrap_or(record.public),
                    mutation: mutation.clone(),
                    expected_revision,
                })
                .with_message(commit_message.clone()),
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
            Ok(record) => return Ok(record),
            Err(UpdateDocumentError::RawLimit) => local_capacity = true,
            Err(error) => return Err(error.into()),
        }
    }
    let response = drive(
        ForwardHoldersOperation::new(
            context.node_and_realm().map(|(node_id, _)| node_id),
            &holders,
            &MetadataTransportMessage::ForwardUpdateDocument {
                auth_token,
                config_digest,
                document_id,
                public,
                mutation,
                expected_revision,
                commit_message,
            },
            local_holds.then_some(local_node_id),
            local_capacity,
        ),
        context,
    )
    .await
    .map_err(MetadataWriteError::from)?;
    match response {
        MetadataTransportMessage::ForwardedRecord {
            record: response_record,
        } if routed_record_matches(
            &config,
            actor.realm_id,
            document_id,
            &placement,
            &response_record,
        ) && record.is_none_or(|record| record.same_identity(&response_record))
            && public.is_none_or(|public| response_record.public == public) =>
        {
            Ok(*response_record)
        }
        MetadataTransportMessage::ForwardedRecord { .. } => Err(MetadataWriteError::Undeliverable(
            "holder returned a metadata update record for another document".to_string(),
        )),
        MetadataTransportMessage::UpdateInvalidInput { message } => {
            Err(UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message)).into())
        }
        MetadataTransportMessage::ForwardedProfileValidation { findings } => Err(
            UpdateDocumentError::MetadataError(MetadataError::ProfileValidation(findings)).into(),
        ),
        MetadataTransportMessage::ForwardedRevisionConflict { expected, current } => {
            Err(UpdateDocumentError::RevisionConflict { expected, current }.into())
        }
        MetadataTransportMessage::ForwardedGraphPending { expected } => {
            Err(UpdateDocumentError::GraphPending { expected }.into())
        }
        other => Err(unexpected_response(other)),
    }
}

/// An offline edit's verdict, as the metadata routes report it.
pub(super) fn device_edit_error(error: DeviceEditError) -> MetadataWriteError {
    match error {
        DeviceEditError::Invalid(message) => {
            UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message)).into()
        }
        DeviceEditError::NoReplica => MetadataWriteError::NotFound,
        other => MetadataWriteError::Undeliverable(other.to_string()),
    }
}

/// A holder's verdict on a device edit, as the drain classifies it.
pub(super) fn batch_refusal(refusal: SyncRefusal) -> MetadataWriteError {
    match refusal {
        SyncRefusal::Unauthorized => MetadataWriteError::Unauthorized,
        SyncRefusal::Forbidden => MetadataWriteError::Forbidden,
        SyncRefusal::NotFound => MetadataWriteError::NotFound,
        SyncRefusal::Invalid(message) => {
            UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message)).into()
        }
        SyncRefusal::Unavailable => {
            MetadataWriteError::Undeliverable("no holder could apply the device edit".to_string())
        }
        SyncRefusal::Coordinator(node) => {
            MetadataWriteError::Undeliverable(format!("node {node} coordinates this key"))
        }
    }
}

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
            let Some(metadata) = context.metadata_handle.as_ref() else {
                return reject("forwarded metadata read needs a metadata handle");
            };
            let result = match metadata
                .authorize_read_peer(peer, auth_token.clone(), false)
                .await
            {
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
            let Some(metadata) = context.metadata_handle.as_ref() else {
                return MetadataTransportMessage::ForwardedValidationStatus {
                    result: Err(MetadataReadError::Unavailable),
                };
            };
            let result = match metadata
                .authorize_read_peer(peer, auth_token.clone(), false)
                .await
            {
                Ok(auth)
                    if holds_metadata_id(&config, realm_id, net_handle.node_id(), *document_id) =>
                {
                    let record = match load_live_record(context.as_ref(), *document_id).await {
                        Ok(record) => record,
                        Err(error) => {
                            return MetadataTransportMessage::ForwardedValidationStatus {
                                result: Err(read_error(error)),
                            };
                        }
                    };
                    if let Err(error) = ensure_record_readable(
                        context.as_ref(),
                        realm_id,
                        auth.as_ref(),
                        &record,
                        None,
                    )
                    .await
                    {
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
