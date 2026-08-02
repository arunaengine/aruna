use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::METADATA_CREATE_ACCEPTANCE_KEYSPACE;
use aruna_core::metadata::{MetadataCreateEventRecord, MetadataError};
use aruna_core::storage_entries::metadata_create_acceptance_key;
use aruna_core::structs::{
    Actor, AuthContext, MetadataRegistryRecord, Permission, PlacementRef, RealmConfigDocument,
    RealmId, RealmNodeKind,
};
use aruna_core::{MetaResourceId, StructuredId};
use thiserror::Error;
use tracing::{error, warn};
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentResult, accepted_create_matches, create_metadata_document,
    mint_forward_document, resolve_metadata_id,
};
use crate::delete_metadata_document::{
    DeleteMetadataDocumentError, DeleteMetadataDocumentOperation, delete_metadata_document,
};
use crate::driver::{DriverContext, drive};
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::metadata::api::{
    GetVisibleMetadataDocumentRequest, MetadataApiError, get_visible_metadata_document,
};
use crate::metadata::handle::{MetadataRequestDelivery, MetadataWritePeerError};
use crate::metadata::protocol::{
    MetadataAuthToken, MetadataReadError, MetadataTransportMessage, MetadataWriteAuthError,
};
use crate::placement::{holds_placement, resolve_shard_holders};
use crate::process_placements::load_realm_config;
use crate::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentError, UpdateMetadataDocumentMutation,
    UpdateMetadataDocumentOperation, update_metadata_document,
};

/// Where a metadata write must be applied.
///
/// Topic membership is the bucket's holder set, so a non-holder can neither
/// publish the write nor join the topic to try: the mutation goes to a holder
/// instead. Membership is never widened to admit the origin — that would grow
/// every bucket toward every node and dissolve sharding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataWriteRoute {
    Local,
    /// Holders of the document's bucket in rank order (rank-0 first).
    Forward(Vec<NodeId>),
}

#[derive(Debug, Error)]
pub enum MetadataWriteError {
    #[error("metadata write requires authentication")]
    Unauthorized,
    #[error("metadata write is forbidden")]
    Forbidden,
    #[error("metadata document not found")]
    NotFound,
    #[error(transparent)]
    Create(#[from] CreateMetadataDocumentError),
    #[error(transparent)]
    Update(#[from] UpdateMetadataDocumentError),
    #[error(transparent)]
    Delete(#[from] DeleteMetadataDocumentError),
    /// The write reached a node that cannot publish it and no holder accepted
    /// the forward. Loud by construction: never accepted, never deferred into an
    /// outbox that can only drain to a topic this node may not join.
    #[error("metadata write is undeliverable: {0}")]
    Undeliverable(String),
}

/// Route for a write against `placement`, from the local node's point of view.
///
/// [`PlacementRef::NIL`] has no governing strategy (early bootstrap), so the
/// local node stays the authority even without readable config. A non-NIL
/// placement needs config to establish authority; when config is unavailable,
/// an empty forward route fails closed as undeliverable.
pub fn write_route(
    config: Option<&RealmConfigDocument>,
    placement: &PlacementRef,
    local_node_id: NodeId,
) -> MetadataWriteRoute {
    let Some(config) = config else {
        return if *placement == PlacementRef::NIL {
            MetadataWriteRoute::Local
        } else {
            MetadataWriteRoute::Forward(Vec::new())
        };
    };
    if holds_placement(config, placement, local_node_id) {
        return MetadataWriteRoute::Local;
    }
    MetadataWriteRoute::Forward(resolve_shard_holders(config, placement))
}

/// User-kind nodes hold no metadata or authorization buckets, so their HTTP
/// write handlers must defer permission checks to the selected holder.
pub async fn is_user_origin(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> Result<bool, MetadataApiError> {
    let config = drive(
        crate::get_realm_config::GetRealmConfigOperation::new(realm_id),
        context.as_ref(),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let node = config
        .nodes
        .into_iter()
        .find(|node| node.node_id == local_node_id.to_string())
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    Ok(node.kind == RealmNodeKind::User)
}

/// Whether the origin currently holds a structured metadata document's bucket.
pub async fn origin_holds_document(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> Result<bool, MetadataApiError> {
    let config = drive(
        crate::get_realm_config::GetRealmConfigOperation::new(realm_id),
        context.as_ref(),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    if !config.has_node(local_node_id) {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let placement = resolve_metadata_id(&config, realm_id, None, document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    Ok(holds_placement(&config, &placement, local_node_id))
}

pub async fn get_metadata_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: GetVisibleMetadataDocumentRequest,
    auth_token: Option<MetadataAuthToken>,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    if context.net_handle.is_none() {
        return get_visible_metadata_document(context.as_ref(), realm_id, request).await;
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let placement = resolve_metadata_id(&config, realm_id, None, request.document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let holders = resolve_shard_holders(&config, &placement);
    let holder_count = holders.len();
    let mut not_found = 0usize;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    if local_node.is_some_and(|node| holders.contains(&node)) {
        match get_visible_metadata_document(context.as_ref(), realm_id, request.clone()).await {
            Ok(record)
                if routed_record_matches(
                    &config,
                    realm_id,
                    request.document_id,
                    &placement,
                    &record,
                ) =>
            {
                return Ok(record);
            }
            Ok(_) => {}
            Err(MetadataApiError::NotFound) => not_found += 1,
            Err(MetadataApiError::ServiceUnavailable) => {}
            Err(error) => return Err(error),
        }
    }
    if holder_count > 0 && not_found == holder_count {
        return Err(MetadataApiError::NotFound);
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    for holder in holders
        .into_iter()
        .filter(|holder| Some(*holder) != local_node)
    {
        let response = metadata
            .request_forwarded_write(
                holder,
                MetadataTransportMessage::ForwardReadDocument {
                    auth_token: auth_token.clone(),
                    config_digest,
                    document_id: request.document_id,
                },
            )
            .await;
        match response {
            Ok(MetadataTransportMessage::ForwardedRead { result: Ok(record) }) => {
                if routed_record_matches(
                    &config,
                    realm_id,
                    request.document_id,
                    &placement,
                    &record,
                ) {
                    return Ok(*record);
                }
            }
            Ok(MetadataTransportMessage::ForwardedRead {
                result: Err(MetadataReadError::Unauthorized),
            }) => return Err(MetadataApiError::Unauthorized),
            Ok(MetadataTransportMessage::ForwardedRead {
                result: Err(MetadataReadError::Forbidden),
            }) => return Err(MetadataApiError::Forbidden),
            Ok(MetadataTransportMessage::ForwardedRead {
                result: Err(MetadataReadError::NotFound),
            }) => not_found += 1,
            Ok(MetadataTransportMessage::ForwardedRead {
                result: Err(MetadataReadError::Unavailable),
            })
            | Ok(MetadataTransportMessage::Reject(_))
            | Err(_) => {}
            Ok(_) => {}
        }
    }
    if holder_count > 0 && not_found == holder_count {
        Err(MetadataApiError::NotFound)
    } else {
        Err(MetadataApiError::ServiceUnavailable)
    }
}

/// Creates locally when the origin holds the bucket, otherwise at a holder.
/// Definitely unsent requests may try another holder; ambiguous delivery is
/// terminal so the create is not replayed.
pub async fn create_metadata_document_routed(
    operation: CreateMetadataDocumentOperation,
    context: Arc<DriverContext>,
    auth_token: Option<MetadataAuthToken>,
) -> Result<CreateMetadataDocumentResult, MetadataWriteError> {
    let config = operation.config().clone();
    match create_metadata_document(operation, context.clone()).await {
        Err(CreateMetadataDocumentError::OriginHoldsNoBucket) => {}
        Ok(created) => return Ok(created),
        Err(error) => return Err(error.into()),
    }

    // Mint the forwarded id at the origin with the blind-hash bucket of the D8
    // subject, so every candidate holder stamps the same bucket (D3/D4).
    let realm_config = load_realm_config(&context, config.actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
    let document_id = if config.document_id.is_nil() {
        mint_forward_document(
            &realm_config,
            &config.actor,
            config.group_id,
            &config.document_path,
        )?
    } else {
        MetaResourceId::from_bytes(config.document_id.to_bytes()).map_err(|error| {
            CreateMetadataDocumentError::PlacementBindingUnavailable(format!(
                "forwarded document id is not a structured id: {error}"
            ))
        })?
    };
    let (placement, holders) =
        create_forward_holders(&realm_config, &config, document_id.as_ulid()).ok_or_else(|| {
            MetadataWriteError::Undeliverable(
                "document id has no resolvable metadata placement".to_string(),
            )
        })?;
    let config_digest = realm_config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let response = forward_to_holders(
        &context,
        &holders,
        MetadataTransportMessage::ForwardCreateDocument {
            auth_token,
            config_digest,
            group_id: config.group_id,
            document_id: document_id.as_ulid(),
            document_path: config.document_path.clone(),
            public: config.public,
            payload: config.payload.clone(),
        },
        None,
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedRecord { record }
            if create_record_matches(&config, document_id.as_ulid(), &placement, &record) =>
        {
            Ok(CreateMetadataDocumentResult {
                event_id: record.last_event_id,
                record: *record,
            })
        }
        MetadataTransportMessage::ForwardedRecord { .. } => Err(MetadataWriteError::Undeliverable(
            "holder returned a metadata create record for another document".to_string(),
        )),
        other => Err(unexpected_response(other)),
    }
}

pub async fn update_metadata_document_routed(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    public: Option<bool>,
    mutation: UpdateMetadataDocumentMutation,
    auth_token: Option<MetadataAuthToken>,
) -> Result<MetadataRegistryRecord, MetadataWriteError> {
    let config = load_realm_config(context, actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
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
    let local_holds = holds_placement(&config, &placement, local_node_id);
    if local_holds && let Some(record) = record {
        return update_metadata_document(
            UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
                actor,
                group_id: record.group_id,
                document_id,
                public: public.unwrap_or(record.public),
                mutation,
            }),
            context.as_ref(),
        )
        .await
        .map_err(Into::into);
    }
    let holders = resolve_shard_holders(&config, &placement);

    let response = forward_to_holders(
        context,
        &holders,
        MetadataTransportMessage::ForwardUpdateDocument {
            auth_token,
            config_digest,
            document_id,
            public,
            mutation,
        },
        local_holds.then_some(local_node_id),
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedRecord {
            record: response_record,
        } if routed_record_matches(
            &config,
            actor.realm_id,
            document_id,
            &placement,
            &response_record,
        ) && record.is_none_or(|record| update_record_matches(record, &response_record))
            && public.is_none_or(|public| response_record.public == public) =>
        {
            Ok(*response_record)
        }
        MetadataTransportMessage::ForwardedRecord { .. } => Err(MetadataWriteError::Undeliverable(
            "holder returned a metadata update record for another document".to_string(),
        )),
        MetadataTransportMessage::ForwardedUpdateInvalidInput { message } => Err(
            UpdateMetadataDocumentError::MetadataError(MetadataError::InvalidInput(message)).into(),
        ),
        other => Err(unexpected_response(other)),
    }
}

pub async fn delete_metadata_document_routed(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    auth_token: Option<MetadataAuthToken>,
) -> Result<(), MetadataWriteError> {
    let config = load_realm_config(context, actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
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
    let local_holds = holds_placement(&config, &placement, local_node_id);
    if local_holds && let Some(record) = record {
        return delete_metadata_document(
            DeleteMetadataDocumentOperation::new(actor, record.group_id, document_id),
            context.as_ref(),
            document_id,
        )
        .await
        .map_err(Into::into);
    }
    let holders = resolve_shard_holders(&config, &placement);

    let response = forward_to_holders(
        context,
        &holders,
        MetadataTransportMessage::ForwardDeleteDocument {
            auth_token,
            config_digest,
            document_id,
        },
        local_holds.then_some(local_node_id),
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedDelete => Ok(()),
        other => Err(unexpected_response(other)),
    }
}

/// Applies a write forwarded by a non-holder, under the caller's authority.
///
/// The forwarded bearer token is re-validated and the same permission checks the
/// origin's HTTP handler runs are re-run here: forwarding is a routing hop, not
/// an internal trust bypass.
///
/// The peer gate is realm membership (`authorize_remote_peer` confirms the peer
/// is a configured node of the token's realm), deliberately *not*
/// sync-eligibility. User-kind nodes are never sync-eligible and therefore hold
/// no bucket at all, which makes them precisely the nodes that must forward every
/// write; gating the forward on sync-eligibility would reject exactly the case it
/// exists to serve. This grants nothing: a forward can do nothing the peer could
/// not do by calling this node's HTTP API directly, under the same token and the
/// same permission check. Sync-eligibility keeps guarding who may *hold* and sync
/// documents — that is a separate question from who may ask a holder to write.
pub(crate) async fn apply_forwarded_write(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return forwarded_unavailable(&message);
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return forwarded_unavailable(&message);
    };
    let expected_digest = match &message {
        MetadataTransportMessage::ForwardCreateDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardUpdateDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardDeleteDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardReadDocument { config_digest, .. } => *config_digest,
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
                get_visible_metadata_document(
                    context.as_ref(),
                    realm_id,
                    GetVisibleMetadataDocumentRequest {
                        document_id: *document_id,
                        auth,
                    },
                )
                .await
                .map(Box::new)
                .map_err(read_error)
            }
            Ok(_) => Err(MetadataReadError::Unavailable),
            Err(error) => Err(error),
        };
        return MetadataTransportMessage::ForwardedRead { result };
    }

    let auth = match authorize_forwarded_caller(context, peer, realm_id, &message).await {
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
            ..
        } => {
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
            if let Err(error) = authorize_write(context, auth.clone(), path).await {
                return forward_auth_error(error);
            }
            let create_config = CreateMetadataDocumentConfig {
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
            let operation = CreateMetadataDocumentOperation::new_forwarded(create_config.clone());
            match create_metadata_document(operation, context.clone()).await {
                Ok(created) => MetadataTransportMessage::ForwardedRecord {
                    record: Box::new(created.record),
                },
                // Lost the race against a concurrent delivery of the same
                // forward: the winner's record is the answer, not an error.
                Err(CreateMetadataDocumentError::DocumentAlreadyExists) => {
                    match forwarded_create_replay(context, &create_config).await {
                        Ok(Some(response)) => response,
                        Ok(None) => reject(format!(
                            "forwarded metadata create for `{document_id}` raced a delete"
                        )),
                        Err(error) => reject(error),
                    }
                }
                Err(error) => reject(format!("forwarded metadata create failed: {error}")),
            }
        }
        MetadataTransportMessage::ForwardUpdateDocument {
            document_id,
            public,
            mutation,
            ..
        } => {
            let record =
                match held_record(context, &config, net_handle.node_id(), document_id).await {
                    Ok(record) => record,
                    Err(HeldRecordError::NotFound) => {
                        return MetadataTransportMessage::ForwardedWriteNotFound;
                    }
                    Err(HeldRecordError::Unavailable(error)) => {
                        warn!(%document_id, %error, "Forwarded metadata update is unavailable");
                        return MetadataTransportMessage::ForwardedWriteUnavailable;
                    }
                };
            if let Err(error) =
                authorize_write(context, auth.clone(), record.permission_path.clone()).await
            {
                return forward_auth_error(error);
            }
            let operation = UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
                actor: Actor {
                    node_id: net_handle.node_id(),
                    user_id: auth.user_id,
                    realm_id,
                },
                group_id: record.group_id,
                document_id,
                public: public.unwrap_or(record.public),
                mutation,
            });
            match update_metadata_document(operation, context.as_ref()).await {
                Ok(record) => MetadataTransportMessage::ForwardedRecord {
                    record: Box::new(record),
                },
                Err(UpdateMetadataDocumentError::MetadataError(MetadataError::InvalidInput(
                    message,
                ))) => MetadataTransportMessage::ForwardedUpdateInvalidInput { message },
                Err(error) => reject(format!("forwarded metadata update failed: {error}")),
            }
        }
        MetadataTransportMessage::ForwardDeleteDocument { document_id, .. } => {
            let record =
                match held_record(context, &config, net_handle.node_id(), document_id).await {
                    Ok(record) => record,
                    Err(HeldRecordError::NotFound) => {
                        return MetadataTransportMessage::ForwardedWriteNotFound;
                    }
                    Err(HeldRecordError::Unavailable(error)) => {
                        warn!(%document_id, %error, "Forwarded metadata delete is unavailable");
                        return MetadataTransportMessage::ForwardedWriteUnavailable;
                    }
                };
            if let Err(error) =
                authorize_write(context, auth.clone(), record.permission_path.clone()).await
            {
                return forward_auth_error(error);
            }
            let operation = DeleteMetadataDocumentOperation::new(
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
        }
        other => reject(format!(
            "unexpected forwarded metadata message: {}",
            super::handle::transport_message_kind(&other)
        )),
    }
}

fn routed_record_matches(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
    placement: &PlacementRef,
    record: &MetadataRegistryRecord,
) -> bool {
    record.realm_id == realm_id
        && record.document_id == document_id
        && record.placement == *placement
        && record.graph_iri == MetadataRegistryRecord::graph_iri_for(document_id)
        && record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &realm_id,
                record.group_id,
                &record.document_path,
                document_id,
            )
        && resolve_metadata_id(config, realm_id, Some(record.group_id), record.document_id)
            .is_ok_and(|resolved| resolved == *placement)
}

fn create_record_matches(
    config: &CreateMetadataDocumentConfig,
    document_id: Ulid,
    placement: &PlacementRef,
    record: &MetadataRegistryRecord,
) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(&config.document_path);
    record.realm_id == config.actor.realm_id
        && record.group_id == config.group_id
        && record.document_id == document_id
        && record.document_path == normalized_path
        && record.graph_iri == MetadataRegistryRecord::graph_iri_for(document_id)
        && record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &config.actor.realm_id,
                config.group_id,
                &normalized_path,
                document_id,
            )
        && record.placement == *placement
        && record.public == config.public
}

fn update_record_matches(
    expected: &MetadataRegistryRecord,
    actual: &MetadataRegistryRecord,
) -> bool {
    expected.realm_id == actual.realm_id
        && expected.group_id == actual.group_id
        && expected.document_id == actual.document_id
        && expected.document_path == actual.document_path
        && expected.graph_iri == actual.graph_iri
        && expected.permission_path == actual.permission_path
        && expected.placement == actual.placement
        && expected.created_at_ms == actual.created_at_ms
        && expected.establishing_event_id == actual.establishing_event_id
}

fn holds_metadata_id(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> bool {
    resolve_metadata_id(config, realm_id, None, document_id)
        .is_ok_and(|placement| holds_placement(config, &placement, local_node_id))
}

fn read_error(error: MetadataApiError) -> MetadataReadError {
    match error {
        MetadataApiError::Unauthorized => MetadataReadError::Unauthorized,
        MetadataApiError::Forbidden => MetadataReadError::Forbidden,
        MetadataApiError::NotFound => MetadataReadError::NotFound,
        MetadataApiError::BadRequest
        | MetadataApiError::ServiceUnavailable
        | MetadataApiError::InvalidCursor(_)
        | MetadataApiError::Internal(_) => MetadataReadError::Unavailable,
    }
}

async fn forwarded_create_replay(
    context: &Arc<DriverContext>,
    config: &CreateMetadataDocumentConfig,
) -> Result<Option<MetadataTransportMessage>, String> {
    let Some(record) = existing_record(context, config.document_id).await? else {
        return Ok(None);
    };
    let accepted = accepted_create(context, config.document_id)
        .await?
        .ok_or_else(|| "existing metadata document has no create acceptance".to_string())?;
    if !accepted_create_matches(config, &accepted)
        || record.realm_id != config.actor.realm_id
        || record.group_id != config.group_id
        || record.document_path
            != MetadataRegistryRecord::normalize_document_path(&config.document_path)
    {
        return Err("forwarded metadata create collides with an existing document".to_string());
    }
    Ok(Some(MetadataTransportMessage::ForwardedRecord {
        record: Box::new(record),
    }))
}

async fn accepted_create(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<Option<MetadataCreateEventRecord>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
            key: metadata_create_acceptance_key(document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => postcard::from_bytes(&bytes)
            .map(Some)
            .map_err(|error| format!("metadata create acceptance decode failed: {error}")),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(format!("metadata create acceptance read failed: {error}"))
        }
        other => Err(format!(
            "unexpected metadata create acceptance read result: {other:?}"
        )),
    }
}

/// The document's registry record, whatever this node's holdership of it.
async fn existing_record(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, String> {
    load_metadata_record_by_document(context.as_ref(), document_id)
        .await
        .map_err(|error| format!("metadata registry read failed: {error:?}"))
}

enum HeldRecordError {
    NotFound,
    Unavailable(String),
}

/// Loads a document only when this node holds its current structured placement.
async fn held_record(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    local_node_id: NodeId,
    document_id: Ulid,
) -> Result<MetadataRegistryRecord, HeldRecordError> {
    let placement = resolve_metadata_id(config, config.realm_id, None, document_id)
        .map_err(|error| HeldRecordError::Unavailable(error.to_string()))?;
    if !holds_placement(config, &placement, local_node_id) {
        return Err(HeldRecordError::Unavailable(format!(
            "node does not hold bucket {}/{} of metadata document `{document_id}`",
            placement.strategy_id, placement.shard
        )));
    }
    let record = existing_record(context, document_id)
        .await
        .map_err(HeldRecordError::Unavailable)?
        .ok_or(HeldRecordError::NotFound)?;
    if !routed_record_matches(config, config.realm_id, document_id, &placement, &record) {
        return Err(HeldRecordError::Unavailable(
            "metadata registry record does not match its structured placement".to_string(),
        ));
    }
    Ok(record)
}

async fn authorize_forwarded_caller(
    context: &Arc<DriverContext>,
    peer: NodeId,
    realm_id: RealmId,
    message: &MetadataTransportMessage,
) -> Result<AuthContext, ForwardAuthError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Err(ForwardAuthError::Unavailable(
            "forwarded metadata write needs a metadata handle".to_string(),
        ));
    };
    let auth_token = match message {
        MetadataTransportMessage::ForwardCreateDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardUpdateDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardDeleteDocument { auth_token, .. } => auth_token.clone(),
        _ => None,
    };
    let auth = metadata_handle
        .authorize_write_peer(peer, auth_token)
        .await
        .map_err(|error| match error {
            MetadataWritePeerError::Unauthorized => ForwardAuthError::Unauthorized,
            MetadataWritePeerError::Unavailable(error) => {
                ForwardAuthError::Unavailable(error.to_string())
            }
        })?;
    if auth.realm_id != realm_id {
        return Err(ForwardAuthError::Forbidden);
    }
    Ok(auth)
}

async fn authorize_write(
    context: &Arc<DriverContext>,
    auth_context: AuthContext,
    path: String,
) -> Result<(), ForwardAuthError> {
    match drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context,
            path: path.clone(),
            required_permission: Permission::WRITE,
        }),
        context.as_ref(),
    )
    .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err(ForwardAuthError::Forbidden),
        Err(
            AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId
            | AuthorizationError::GroupNotFound
            | AuthorizationError::AuthDocNotFound,
        ) => Err(ForwardAuthError::Forbidden),
        Err(error) => Err(ForwardAuthError::Unavailable(error.to_string())),
    }
}

/// Holders of the document's blind-hashed bucket: the candidates for a create the
/// origin cannot place. Every candidate holds that one bucket, and a forwarded
/// create stamps exactly it (see `CreateMetadataDocumentOperation::new_forwarded`),
/// so which candidate answers cannot change where the document lands.
fn create_forward_holders(
    realm_config: &RealmConfigDocument,
    config: &CreateMetadataDocumentConfig,
    document_id: Ulid,
) -> Option<(PlacementRef, Vec<NodeId>)> {
    let placement = resolve_metadata_id(
        realm_config,
        config.actor.realm_id,
        Some(config.group_id),
        document_id,
    )
    .ok()?;
    let holders = resolve_shard_holders(realm_config, &placement);
    Some((placement, holders))
}

async fn forward_to_holders(
    context: &Arc<DriverContext>,
    holders: &[NodeId],
    message: MetadataTransportMessage,
    local_miss: Option<NodeId>,
) -> Result<MetadataTransportMessage, MetadataWriteError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Err(MetadataWriteError::Undeliverable(
            "no metadata handle to forward with".to_string(),
        ));
    };
    let local_node_id = local_miss.or_else(|| context.net_handle.as_ref().map(|net| net.node_id()));
    let tracks_not_found = matches!(
        &message,
        MetadataTransportMessage::ForwardUpdateDocument { .. }
            | MetadataTransportMessage::ForwardDeleteDocument { .. }
    );

    let mut failures: Vec<String> = Vec::new();
    let mut not_found = usize::from(local_miss.is_some());
    for holder in holders
        .iter()
        .filter(|holder| Some(**holder) != local_node_id)
    {
        match metadata_handle
            .request_forwarded_write(*holder, message.clone())
            .await
        {
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Unauthorized,
            }) => return Err(MetadataWriteError::Unauthorized),
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Forbidden,
            }) => return Err(MetadataWriteError::Forbidden),
            Ok(MetadataTransportMessage::ForwardedWriteNotFound) if tracks_not_found => {
                not_found += 1;
            }
            Ok(MetadataTransportMessage::ForwardedWriteNotFound) => {
                failures.push(format!(
                    "{holder}: holder returned not found for a forwarded create"
                ));
            }
            Ok(MetadataTransportMessage::ForwardedWriteUnavailable) => {
                failures.push(format!("{holder}: holder placement view is unavailable"));
            }
            Ok(MetadataTransportMessage::Reject(error)) => {
                warn!(holder = %holder, error = %error, "Holder rejected a forwarded metadata write");
                return Err(MetadataWriteError::Undeliverable(format!(
                    "holder `{holder}` rejected the forwarded metadata write; refusing to replay it: {error}"
                )));
            }
            Ok(response) => return Ok(response),
            Err(error) => {
                warn!(holder = %holder, error = %error, "Failed to forward a metadata write to holder");
                if retry_disposition(error.delivery()) == RetryDisposition::Stop {
                    return Err(MetadataWriteError::Undeliverable(format!(
                        "forward to holder `{holder}` may have applied the metadata write before failing; refusing to replay it: {error}"
                    )));
                }
                failures.push(format!("{holder}: {error}"));
            }
        }
    }

    if tracks_not_found && !holders.is_empty() && not_found == holders.len() {
        return Err(MetadataWriteError::NotFound);
    }

    let detail = if failures.is_empty() {
        "the document's bucket has no reachable holder".to_string()
    } else {
        failures.join("; ")
    };
    error!(
        holders = holders.len(),
        detail = %detail,
        "Metadata write reached a non-holder and no holder accepted the forward"
    );
    Err(MetadataWriteError::Undeliverable(detail))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetryDisposition {
    TryNext,
    Stop,
}

fn retry_disposition(delivery: MetadataRequestDelivery) -> RetryDisposition {
    match delivery {
        MetadataRequestDelivery::DefinitelyNotSent => RetryDisposition::TryNext,
        MetadataRequestDelivery::PossiblySent => RetryDisposition::Stop,
    }
}

fn unexpected_response(response: MetadataTransportMessage) -> MetadataWriteError {
    MetadataWriteError::Undeliverable(format!(
        "unexpected forwarded metadata response: {}",
        super::handle::transport_message_kind(&response)
    ))
}

fn reject(error: impl Into<String>) -> MetadataTransportMessage {
    MetadataTransportMessage::Reject(error.into())
}

fn forwarded_unavailable(message: &MetadataTransportMessage) -> MetadataTransportMessage {
    match message {
        MetadataTransportMessage::ForwardReadDocument { .. } => {
            MetadataTransportMessage::ForwardedRead {
                result: Err(MetadataReadError::Unavailable),
            }
        }
        MetadataTransportMessage::ForwardCreateDocument { .. }
        | MetadataTransportMessage::ForwardUpdateDocument { .. }
        | MetadataTransportMessage::ForwardDeleteDocument { .. } => {
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
        _ => reject("unexpected forwarded metadata message"),
    }
}

enum ForwardAuthError {
    Unauthorized,
    Forbidden,
    Unavailable(String),
}

fn forward_auth_error(error: ForwardAuthError) -> MetadataTransportMessage {
    match error {
        ForwardAuthError::Unauthorized => MetadataTransportMessage::ForwardedWriteDenied {
            error: MetadataWriteAuthError::Unauthorized,
        },
        ForwardAuthError::Forbidden => MetadataTransportMessage::ForwardedWriteDenied {
            error: MetadataWriteAuthError::Forbidden,
        },
        ForwardAuthError::Unavailable(error) => {
            warn!(%error, "Forwarded metadata authorization is unavailable");
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{METADATA_HANDLE, PlacementStrategy, RealmNodeKind};
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::{MetaResourceId, StructuredId};

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn config_and_placement() -> (RealmConfigDocument, PlacementRef) {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([4u8; 16]),
            name: "default".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        (
            config,
            PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard: 9,
            },
        )
    }

    #[test]
    fn holder_writes_stay_local() {
        let (config, placement) = config_and_placement();
        let holders = resolve_shard_holders(&config, &placement);

        assert_eq!(
            write_route(Some(&config), &placement, holders[0]),
            MetadataWriteRoute::Local
        );
    }

    #[test]
    fn non_holder_writes_forward() {
        // Rank order is the holder set's own: rank-0 is tried first, the rest on
        // failure. Replica 2 of 4 servers guarantees a non-holder exists.
        let (config, placement) = config_and_placement();
        let holders = resolve_shard_holders(&config, &placement);
        let outsider = (1..=4u8)
            .map(node)
            .find(|candidate| !holders.contains(candidate))
            .expect("a replica-capped bucket leaves a non-holder");

        assert_eq!(
            write_route(Some(&config), &placement, outsider),
            MetadataWriteRoute::Forward(holders)
        );
    }

    #[test]
    fn user_node_writes_forward() {
        // A User-kind node is never sync-eligible, so it holds no bucket at all:
        // locality is unattainable for it and every write must be forwarded. The
        // receiving half — a holder accepting that forward from a User peer, and
        // applying it under the caller's token — is
        // `metadata_forwarding::user_node_forwards_create`, which needs a real
        // node and a real token and so cannot live here.
        let (mut config, placement) = config_and_placement();
        config.ensure_node(node(9), RealmNodeKind::User);

        assert!(matches!(
            write_route(Some(&config), &placement, node(9)),
            MetadataWriteRoute::Forward(_)
        ));
    }

    #[test]
    fn missing_config_forwards() {
        let (_, placement) = config_and_placement();

        assert_eq!(
            write_route(None, &placement, node(1)),
            MetadataWriteRoute::Forward(Vec::new())
        );
    }

    #[test]
    fn unplaced_writes_stay_local() {
        // No strategy governs a NIL ref (early bootstrap): nowhere to forward to,
        // and no sharding to respect.
        let (config, _) = config_and_placement();

        assert_eq!(
            write_route(Some(&config), &PlacementRef::NIL, node(1)),
            MetadataWriteRoute::Local
        );
        assert_eq!(
            write_route(None, &PlacementRef::NIL, node(1)),
            MetadataWriteRoute::Local
        );
    }

    #[test]
    fn ambiguous_delivery_stops() {
        assert_eq!(
            retry_disposition(MetadataRequestDelivery::PossiblySent),
            RetryDisposition::Stop
        );
        assert_eq!(
            retry_disposition(MetadataRequestDelivery::DefinitelyNotSent),
            RetryDisposition::TryNext
        );
    }

    #[test]
    fn nonholder_read_rejected() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let document_id = MetaResourceId::from_parts(
            1,
            PlacementHandle::new(METADATA_HANDLE).unwrap(),
            BucketId::new(9).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let placement = resolve_metadata_id(&config, realm_id, None, document_id).unwrap();
        let holders = resolve_shard_holders(&config, &placement);
        let outsider = (1..=4u8)
            .map(node)
            .find(|candidate| !holders.contains(candidate))
            .unwrap();

        assert!(!holds_metadata_id(&config, realm_id, outsider, document_id));
    }

    #[test]
    fn response_records_checked() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        let document_id = MetaResourceId::from_parts(
            1,
            PlacementHandle::new(METADATA_HANDLE).unwrap(),
            BucketId::new(9).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let placement =
            resolve_metadata_id(&config, realm_id, Some(group_id), document_id).unwrap();
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "docs/one".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "docs/one",
                document_id,
            ),
            placement,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_bytes([4u8; 16]),
            last_event_id: Ulid::from_bytes([4u8; 16]),
        };

        assert!(routed_record_matches(
            &config,
            realm_id,
            document_id,
            &placement,
            &record,
        ));
        let mut substituted = record.clone();
        substituted.document_id = Ulid::from_bytes([5u8; 16]);
        assert!(!routed_record_matches(
            &config,
            realm_id,
            document_id,
            &placement,
            &substituted,
        ));

        let create = CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: node(1),
                user_id: aruna_core::UserId::local(Ulid::from_bytes([6u8; 16]), realm_id),
                realm_id,
            },
            group_id,
            document_id: Ulid::nil(),
            document_path: "/docs/one/".to_string(),
            public: true,
            payload: crate::create_metadata_document::CreateMetadataDocumentPayload::Scaffold {
                name: "one".to_string(),
                description: String::new(),
                date_published: "2026-01-01".to_string(),
                license: None,
            },
        };
        assert!(create_record_matches(
            &create,
            document_id,
            &placement,
            &record
        ));
        let mut moved = record.clone();
        moved.placement.shard += 1;
        assert!(!create_record_matches(
            &create,
            document_id,
            &placement,
            &moved
        ));

        let mut changed = record.clone();
        changed.document_path = "docs/two".to_string();
        assert!(!update_record_matches(&record, &changed));
    }
}
