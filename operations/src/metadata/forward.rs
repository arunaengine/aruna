use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{
    Actor, AuthContext, MetadataRegistryRecord, Permission, PlacementRef, RealmConfigDocument,
    RealmId,
};
use thiserror::Error;
use tracing::{error, warn};
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentResult, create_metadata_document,
};
use crate::delete_metadata_document::{
    DeleteMetadataDocumentError, DeleteMetadataDocumentOperation, delete_metadata_document,
};
use crate::driver::{DriverContext, drive};
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::metadata::protocol::{MetadataAuthToken, MetadataTransportMessage};
use crate::placement::{
    PlacementResolutionContext, holds_placement, plan_target_placement, resolve_shard_holders,
};
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

/// Route resolved against the live realm config. The presence of a local
/// registry record is deliberately not consulted: after a rebalance a node keeps
/// a stale copy of a document it no longer holds, and that copy is not authority.
pub async fn resolve_write_route(
    context: &Arc<DriverContext>,
    placement: &PlacementRef,
) -> MetadataWriteRoute {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataWriteRoute::Local;
    };
    let config = load_realm_config(context, *net_handle.realm_id()).await;
    write_route(config.as_ref(), placement, net_handle.node_id())
}

/// Creates locally when the origin holds a bucket of the governing strategy, and
/// forwards to a holder when it holds none (a User-kind node, or one filtered out
/// by affinity/`weight = 0`/`full`/`draining`). A forwarded create is offered to
/// the holders of the document's blind-hashed bucket, each of which stamps that
/// same bucket, so retrying the next holder after a lost response cannot fork the
/// document onto a second topic.
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

    let holders = create_forward_holders(&context, &config).await;
    let response = forward_to_holders(
        &context,
        &holders,
        MetadataTransportMessage::ForwardCreateDocument {
            auth_token,
            group_id: config.group_id,
            document_id: config.document_id,
            document_path: config.document_path.clone(),
            public: config.public,
            payload: config.payload.clone(),
        },
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedRecord { record } => Ok(CreateMetadataDocumentResult {
            event_id: record.last_event_id,
            record: *record,
        }),
        other => Err(unexpected_response(other)),
    }
}

pub async fn update_metadata_document_routed(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: &MetadataRegistryRecord,
    public: Option<bool>,
    mutation: UpdateMetadataDocumentMutation,
    auth_token: Option<MetadataAuthToken>,
) -> Result<MetadataRegistryRecord, MetadataWriteError> {
    let holders = match resolve_write_route(context, &record.placement).await {
        MetadataWriteRoute::Local => {
            return update_metadata_document(
                UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
                    actor,
                    group_id: record.group_id,
                    document_id: record.document_id,
                    public: public.unwrap_or(record.public),
                    mutation,
                }),
                context.as_ref(),
            )
            .await
            .map_err(Into::into);
        }
        MetadataWriteRoute::Forward(holders) => holders,
    };

    let response = forward_to_holders(
        context,
        &holders,
        MetadataTransportMessage::ForwardUpdateDocument {
            auth_token,
            document_id: record.document_id,
            public,
            mutation,
        },
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedRecord { record } => Ok(*record),
        other => Err(unexpected_response(other)),
    }
}

pub async fn delete_metadata_document_routed(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: &MetadataRegistryRecord,
    auth_token: Option<MetadataAuthToken>,
) -> Result<(), MetadataWriteError> {
    let holders = match resolve_write_route(context, &record.placement).await {
        MetadataWriteRoute::Local => {
            return delete_metadata_document(
                DeleteMetadataDocumentOperation::new(actor, record.group_id, record.document_id),
                context.as_ref(),
                record.document_id,
            )
            .await
            .map_err(Into::into);
        }
        MetadataWriteRoute::Forward(holders) => holders,
    };

    let response = forward_to_holders(
        context,
        &holders,
        MetadataTransportMessage::ForwardDeleteDocument {
            auth_token,
            document_id: record.document_id,
        },
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
        return reject("forwarded metadata write needs a net handle");
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return reject(format!("realm `{realm_id}` config unavailable"));
    };

    let auth = match authorize_forwarded_caller(context, peer, realm_id, &message).await {
        Ok(auth) => auth,
        Err(error) => return reject(error),
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
            if MetadataRegistryRecord::normalize_document_path(&document_path).is_empty() {
                return reject("forwarded metadata create has an empty document path");
            }
            let path = format!("/{realm_id}/g/{group_id}/meta/**");
            if let Err(error) = authorize_write(context, auth.clone(), path).await {
                return reject(error);
            }
            // Idempotent on the document's identity, never on its bucket: a
            // forward whose response was lost is retried, possibly against a
            // different holder, and a second create under the same document id
            // would fork one document into two. Replay the record instead.
            match existing_record(context, document_id).await {
                Ok(Some(record)) => {
                    return MetadataTransportMessage::ForwardedRecord {
                        record: Box::new(record),
                    };
                }
                Ok(None) => {}
                Err(error) => return reject(error),
            }
            let operation =
                CreateMetadataDocumentOperation::new_forwarded(CreateMetadataDocumentConfig {
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
                });
            match create_metadata_document(operation, context.clone()).await {
                Ok(created) => MetadataTransportMessage::ForwardedRecord {
                    record: Box::new(created.record),
                },
                // Lost the race against a concurrent delivery of the same
                // forward: the winner's record is the answer, not an error.
                Err(CreateMetadataDocumentError::DocumentAlreadyExists) => {
                    match existing_record(context, document_id).await {
                        Ok(Some(record)) => MetadataTransportMessage::ForwardedRecord {
                            record: Box::new(record),
                        },
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
                    Err(error) => return reject(error),
                };
            if let Err(error) =
                authorize_write(context, auth.clone(), record.permission_path.clone()).await
            {
                return reject(error);
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
                Err(error) => reject(format!("forwarded metadata update failed: {error}")),
            }
        }
        MetadataTransportMessage::ForwardDeleteDocument { document_id, .. } => {
            let record =
                match held_record(context, &config, net_handle.node_id(), document_id).await {
                    Ok(record) => record,
                    Err(error) => return reject(error),
                };
            if let Err(error) =
                authorize_write(context, auth.clone(), record.permission_path.clone()).await
            {
                return reject(error);
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

/// The document's registry record, whatever this node's holdership of it.
async fn existing_record(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, String> {
    load_metadata_record_by_document(context.as_ref(), document_id)
        .await
        .map_err(|error| format!("metadata registry read failed: {error:?}"))
}

/// The document as this node holds it. A forward is never chained: a node that
/// is not a holder rejects, so the origin tries the next holder in rank order.
async fn held_record(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    local_node_id: NodeId,
    document_id: Ulid,
) -> Result<MetadataRegistryRecord, String> {
    let record = existing_record(context, document_id)
        .await?
        .ok_or_else(|| format!("metadata document `{document_id}` not found"))?;
    match write_route(Some(config), &record.placement, local_node_id) {
        MetadataWriteRoute::Local => Ok(record),
        MetadataWriteRoute::Forward(_) => Err(format!(
            "node does not hold bucket {}/{} of metadata document `{document_id}`",
            record.placement.strategy_id, record.placement.shard
        )),
    }
}

async fn authorize_forwarded_caller(
    context: &Arc<DriverContext>,
    peer: NodeId,
    realm_id: RealmId,
    message: &MetadataTransportMessage,
) -> Result<AuthContext, String> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Err("forwarded metadata write needs a metadata handle".to_string());
    };
    let auth_token = match message {
        MetadataTransportMessage::ForwardCreateDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardUpdateDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardDeleteDocument { auth_token, .. } => auth_token.clone(),
        _ => None,
    };
    let auth = metadata_handle
        .authorize_remote_peer(peer, auth_token)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "forwarded metadata write needs an authenticated caller".to_string())?;
    if auth.realm_id != realm_id {
        return Err(format!(
            "forwarded metadata write carries a token for realm `{}`, not `{realm_id}`",
            auth.realm_id
        ));
    }
    Ok(auth)
}

async fn authorize_write(
    context: &Arc<DriverContext>,
    auth_context: AuthContext,
    path: String,
) -> Result<(), String> {
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
        Ok(false) => Err(format!(
            "forwarded metadata write is not permitted on `{path}`"
        )),
        Err(error) => Err(error.to_string()),
    }
}

/// Holders of the document's blind-hashed bucket: the candidates for a create the
/// origin cannot place. Every candidate holds that one bucket, and a forwarded
/// create stamps exactly it (see `CreateMetadataDocumentOperation::new_forwarded`),
/// so which candidate answers cannot change where the document lands.
async fn create_forward_holders(
    context: &Arc<DriverContext>,
    config: &CreateMetadataDocumentConfig,
) -> Vec<NodeId> {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return Vec::new();
    };
    let Some(realm_config) = load_realm_config(context, *net_handle.realm_id()).await else {
        return Vec::new();
    };
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: config.document_id,
    };
    let document_path = MetadataRegistryRecord::normalize_document_path(&config.document_path);
    plan_target_placement(
        &realm_config,
        &target,
        PlacementResolutionContext {
            group_id: Some(config.group_id),
            metadata_path: Some(document_path.as_str()),
        },
    )
    .map(|plan| plan.holders)
    .unwrap_or_default()
}

async fn forward_to_holders(
    context: &Arc<DriverContext>,
    holders: &[NodeId],
    message: MetadataTransportMessage,
) -> Result<MetadataTransportMessage, MetadataWriteError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Err(MetadataWriteError::Undeliverable(
            "no metadata handle to forward with".to_string(),
        ));
    };
    let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());

    let mut failures: Vec<String> = Vec::new();
    for holder in holders
        .iter()
        .filter(|holder| Some(**holder) != local_node_id)
    {
        match metadata_handle
            .request_forwarded_write(*holder, message.clone())
            .await
        {
            Ok(MetadataTransportMessage::Reject(error)) => {
                warn!(holder = %holder, error = %error, "Holder rejected a forwarded metadata write");
                failures.push(format!("{holder}: {error}"));
            }
            Ok(response) => return Ok(response),
            Err(error) => {
                warn!(holder = %holder, error = %error, "Failed to forward a metadata write to holder");
                failures.push(format!("{holder}: {error}"));
            }
        }
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

fn unexpected_response(response: MetadataTransportMessage) -> MetadataWriteError {
    MetadataWriteError::Undeliverable(format!(
        "unexpected forwarded metadata response: {}",
        super::handle::transport_message_kind(&response)
    ))
}

fn reject(error: impl Into<String>) -> MetadataTransportMessage {
    MetadataTransportMessage::Reject(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{PlacementStrategy, RealmNodeKind};

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
}
