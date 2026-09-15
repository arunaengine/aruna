use crate::driver::DriverContext;
use crate::driver::drive;
use crate::groups::get_group::GetGroupConfig;
use crate::groups::get_group::GetGroupOperation;
use crate::groups::list_groups::ListGroupOperation;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::ensure_record_readable;
use crate::metadata::api::load_live_record;
use crate::metadata::handle::WritePeerError;
use crate::metadata::protocol::AuthToken;
use crate::metadata::protocol::DeviceGroupDocuments;
use crate::metadata::protocol::GraphState;
use crate::metadata::protocol::MAX_DEVICE_GROUPS;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::RealmDocuments;
use crate::metadata::raw_revision::load_raw_view;
use crate::metadata::update_document::UpdateDocumentConfig;
use crate::metadata::update_document::UpdateDocumentError;
use crate::metadata::update_document::UpdateDocumentMutation;
use crate::metadata::update_document::UpdateDocumentOperation;
use crate::metadata::update_document::update_metadata_document;
use crate::node::node_info::read_info_documents;
use crate::placement::process_placements::load_realm_config;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::admin_documents::AdminDocumentClock;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::metadata::MetadataEffect;
use aruna_core::metadata::MetadataError;
use aruna_core::metadata::MetadataEvent;
use aruna_core::reducer::AdminDocumentState;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::SyncRefusal;
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;

use crate::forward::authorize::ForwardAuthError;
use crate::forward::authorize::authorize_forwarded_caller;
use crate::forward::authorize::authorize_write;
use crate::forward::authorize::is_sync_eligible;
use crate::forward::authorize::peer_acts_for;
use crate::forward::replay::HeldRecordError;
use crate::forward::replay::held_record;
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::reject;
use std::str::FromStr;

pub(super) const GROUP_SCAN_PAGE: usize = 10_000;

/// Serves the realm-wide documents to one of the realm's devices.
/// A device runs no document sync, so this is how it sees the realm config it is
/// judged by. Realm infrastructure only, bound to the device's owner.
pub(crate) async fn serve_realm_documents(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::FetchRealmDocuments { auth_token } = message else {
        return reject("unexpected metadata control message");
    };
    MetadataTransportMessage::FetchedRealmDocuments {
        result: read_realm_documents(context, peer, auth_token).await,
    }
}

pub(super) async fn read_realm_documents(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
) -> Result<RealmDocuments, SyncRefusal> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(SyncRefusal::Unavailable)?;
    // Only realm infrastructure answers: a device holds no realm state to serve.
    if !is_sync_eligible(&config, net_handle.node_id()) {
        return Err(SyncRefusal::Unavailable);
    }
    let auth = metadata
        .authorize_write_peer(peer, Some(auth_token))
        .await
        .map_err(|error| match error {
            WritePeerError::Unauthorized => SyncRefusal::Unauthorized,
            WritePeerError::Unavailable(_) => SyncRefusal::Unavailable,
        })?;
    // The documents are this realm's own; nothing about another realm is served.
    if auth.realm_id != realm_id || !peer_acts_for(&config, peer, auth.user_id) {
        return Err(SyncRefusal::Unauthorized);
    }
    let realm_config = read_document(context, DocumentTarget::RealmConfig { realm_id })
        .await?
        .ok_or(SyncRefusal::NotFound)?;
    let realm_authorization =
        read_document(context, DocumentTarget::RealmAuthorization { realm_id }).await?;
    // The token's own subject: for a device that is its owner by the check above.
    let owner = read_document(
        context,
        DocumentTarget::User {
            user_id: auth.user_id,
        },
    )
    .await?;
    let node_ids = config
        .sync_eligible_nodes()
        .map_err(|_| SyncRefusal::Unavailable)?;
    let node_infos = read_info_documents(context, &node_ids)
        .await
        .map_err(|error| {
            warn!(%error, "Failed to read the node info documents for a device");
            SyncRefusal::Unavailable
        })?
        .into_values()
        .collect();
    Ok(RealmDocuments {
        realm_config,
        realm_authorization,
        owner,
        groups: device_group_documents(context, auth.user_id).await,
        node_infos,
        management_urls: management_urls(context, &config).await,
        clock: applied_clock(context, realm_id).await,
    })
}

/// The api urls the realm's management nodes published, in node-id order so
/// repeated answers pin the same peer. A device relays its management-only
/// routes to these.
pub(super) async fn management_urls(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
) -> Vec<String> {
    let node_ids: Vec<NodeId> = config
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, RealmNodeKind::Management))
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .collect();
    let documents = match read_info_documents(context, &node_ids).await {
        Ok(documents) => documents,
        Err(error) => {
            warn!(%error, "Failed to read the management urls for a device");
            return Vec::new();
        }
    };
    let mut urls: Vec<String> = Vec::new();
    for url in documents
        .values()
        .filter_map(|document| document.urls.api.clone())
    {
        if !urls.contains(&url) {
            urls.push(url);
        }
    }
    urls
}

/// The caller's own groups, as the documents this node stores. A read that
/// fails yields an empty list: one unreadable group must not cost the device
/// the realm configuration it is judged by.
pub(super) async fn device_group_documents(
    context: &Arc<DriverContext>,
    user_id: UserId,
) -> Vec<DeviceGroupDocuments> {
    let mut documents = Vec::new();
    let mut offset = 0usize;
    loop {
        let groups = match drive(
            ListGroupOperation::with_pagination(GROUP_SCAN_PAGE, offset),
            context.as_ref(),
        )
        .await
        {
            Ok(groups) => groups,
            Err(error) => {
                warn!(error = %error, "Failed to list groups for a device");
                return Vec::new();
            }
        };
        let page_len = groups.len();
        for Group { group_id, .. } in groups {
            let read = drive(
                GetGroupOperation::new(GetGroupConfig { group_id }),
                context.as_ref(),
            )
            .await;
            let Ok((group, authorization)) = read else {
                warn!(%group_id, "Failed to read a group for a device");
                continue;
            };
            if !holds_any_role(&authorization, user_id) {
                continue;
            }
            documents.push(DeviceGroupDocuments {
                group,
                authorization,
            });
            if documents.len() >= MAX_DEVICE_GROUPS {
                return documents;
            }
        }
        if page_len < GROUP_SCAN_PAGE {
            return documents;
        }
        offset = offset.saturating_add(page_len);
    }
}

/// Whether the owner holds a role in this group. A device caches its owner's
/// groups only; a realm-scale group list is not theirs to hold.
pub(super) fn holds_any_role(authorization: &GroupAuthorizationDocument, user_id: UserId) -> bool {
    authorization
        .roles
        .values()
        .any(|role| role.assigned_users.contains(&user_id))
}

/// What this node has applied to the realm configuration, as the reducer keeps
/// it. A device compares it with its own copy's and never accepts less.
pub(super) async fn applied_clock(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
) -> AdminDocumentClock {
    let key = reducer_state_key(&AdminDocumentTarget::RealmConfig { realm_id });
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DOCUMENT_STATE_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    else {
        return AdminDocumentClock::default();
    };
    postcard::from_bytes::<AdminDocumentState>(&bytes)
        .map(|state| state.clock)
        .unwrap_or_default()
}

/// One stored document, or `None` when this node holds it not (yet).
pub(super) async fn read_document(
    context: &Arc<DriverContext>,
    target: DocumentTarget,
) -> Result<Option<Vec<u8>>, SyncRefusal> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|bytes| bytes.as_ref().to_vec()))
        }
        other => {
            warn!(event = ?other, "Failed to read a realm document for a device");
            Err(SyncRefusal::Unavailable)
        }
    }
}

/// Serves one document's graph state to a device that keeps a replica of it.
/// Only a holder answers, for the owner the realm config binds the device to; the
/// device joins the snapshot locally, so state travels, never authority.
pub(crate) async fn serve_graph_state(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::FetchGraphState {
        auth_token,
        document_id,
    } = message
    else {
        return reject("unexpected metadata control message");
    };
    MetadataTransportMessage::FetchedGraphState {
        result: read_graph_state(context, peer, auth_token, document_id)
            .await
            .map(Box::new),
    }
}

pub(super) async fn read_graph_state(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    document_id: Ulid,
) -> Result<GraphState, SyncRefusal> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(SyncRefusal::Unavailable)?;
    if !holds_metadata_id(&config, realm_id, net_handle.node_id(), document_id) {
        return Err(SyncRefusal::Unavailable);
    }
    let auth = metadata
        .authorize_write_peer(peer, Some(auth_token))
        .await
        .map_err(|error| match error {
            WritePeerError::Unauthorized => SyncRefusal::Unauthorized,
            WritePeerError::Unavailable(_) => SyncRefusal::Unavailable,
        })?;
    if auth.realm_id != realm_id || !peer_acts_for(&config, peer, auth.user_id) {
        return Err(SyncRefusal::Unauthorized);
    }
    let record = load_live_record(context.as_ref(), document_id)
        .await
        .map_err(sync_refusal)?;
    ensure_record_readable(context.as_ref(), realm_id, Some(&auth), &record, None)
        .await
        .map_err(sync_refusal)?;
    let graph_iri = record.graph_iri.clone();
    let snapshot = match metadata
        .send_metadata_effect(MetadataEffect::GraphSnapshot { graph_iri })
        .await
    {
        Event::Metadata(MetadataEvent::GraphSnapshotResult { snapshot, .. }) => *snapshot,
        other => {
            warn!(%document_id, event = ?other, "Could not snapshot a graph for a device");
            return Err(SyncRefusal::Unavailable);
        }
    };
    let raw = load_raw_view(context.as_ref(), document_id, None)
        .await
        .map_err(|_| SyncRefusal::Unavailable)?
        .ok_or(SyncRefusal::NotFound)?;
    Ok(GraphState {
        record,
        snapshot,
        displayed_jsonld: raw.revision.jsonld,
        dataset_digest: raw.revision.dataset_digest,
        findings: raw.revision.merged.map_or(0, |merged| merged.findings),
    })
}

pub(super) fn sync_refusal(error: MetadataApiError) -> SyncRefusal {
    match error {
        MetadataApiError::Unauthorized => SyncRefusal::Unauthorized,
        MetadataApiError::Forbidden => SyncRefusal::Forbidden,
        MetadataApiError::NotFound => SyncRefusal::NotFound,
        _ => SyncRefusal::Unavailable,
    }
}

/// Applies an edit a device already made on its replica.
/// The batch is appended unchanged as an ordinary update event, so every holder
/// materializes the same OR-Set change set the owner saw and both sides converge.
pub(crate) async fn apply_device_batch(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardedApplyBatch {
        result: run_device_batch(context, peer, message).await.map(Box::new),
    }
}

pub(super) async fn run_device_batch(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataRegistryRecord, SyncRefusal> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(SyncRefusal::Unavailable)?;
    if !is_sync_eligible(&config, net_handle.node_id()) {
        return Err(SyncRefusal::Unavailable);
    }
    let auth = authorize_forwarded_caller(context, peer, realm_id, &message)
        .await
        .map_err(|error| match error {
            ForwardAuthError::Unauthorized => SyncRefusal::Unauthorized,
            ForwardAuthError::Forbidden => SyncRefusal::Forbidden,
            ForwardAuthError::Unavailable(_) => SyncRefusal::Unavailable,
        })?;
    let MetadataTransportMessage::ForwardApplyBatch {
        config_digest,
        document_id,
        batch,
        authored,
        ..
    } = message
    else {
        return Err(SyncRefusal::Invalid(
            "unexpected metadata control message".to_string(),
        ));
    };
    if config.digest().ok() != Some(config_digest) {
        return Err(SyncRefusal::Unavailable);
    }
    let record = held_record(context, &config, net_handle.node_id(), document_id)
        .await
        .map_err(|error| match error {
            HeldRecordError::NotFound => SyncRefusal::NotFound,
            HeldRecordError::Unavailable(_) => SyncRefusal::Unavailable,
        })?;
    // The batch names the graph it was planned against; another graph's change
    // set must never be applied here.
    if batch.graph_iri != record.graph_iri {
        return Err(SyncRefusal::Invalid(
            "the batch was planned against another document".to_string(),
        ));
    }
    authorize_write(context, auth.clone(), record.permission_path.clone())
        .await
        .map_err(|error| match error {
            ForwardAuthError::Unauthorized => SyncRefusal::Unauthorized,
            ForwardAuthError::Forbidden => SyncRefusal::Forbidden,
            ForwardAuthError::Unavailable(_) => SyncRefusal::Unavailable,
        })?;
    let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
        actor: Actor {
            node_id: net_handle.node_id(),
            user_id: auth.user_id,
            realm_id,
        },
        group_id: record.group_id,
        document_id,
        public: record.public,
        mutation: UpdateDocumentMutation::ApplyBatch { batch, authored },
    });
    update_metadata_document(operation, context.as_ref())
        .await
        .map_err(|error| match error {
            UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message)) => {
                SyncRefusal::Invalid(message)
            }
            other => {
                warn!(%document_id, error = %other, "A device edit did not apply");
                SyncRefusal::Unavailable
            }
        })
}

#[cfg(test)]
mod tests {
    use super::GROUP_SCAN_PAGE;
    use super::device_group_documents;
    use crate::driver::DriverContext;
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::Event;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::Group;
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmId;
    use std::sync::Arc;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[tokio::test]
    async fn finds_later_membership() {
        // The only matching group sits just beyond the legacy default page.
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let member = UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
        let other = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
        let actor = Actor {
            node_id: node(1),
            user_id: member,
            realm_id,
        };
        let mut writes = Vec::with_capacity((GROUP_SCAN_PAGE + 1) * 2);
        for seed in 1..=GROUP_SCAN_PAGE + 1 {
            let group_id = Ulid::from(seed as u128);
            let group = Group {
                display_name: seed.to_string(),
                group_id,
                realm_id,
                roles: Default::default(),
                owner: other,
            };
            let authorization = GroupAuthorizationDocument::default_group_doc(
                if seed > GROUP_SCAN_PAGE {
                    member
                } else {
                    other
                },
                realm_id,
                group_id,
            );
            for (target, bytes) in [
                (
                    DocumentTarget::Group { group_id },
                    group.to_bytes(&actor).unwrap(),
                ),
                (
                    DocumentTarget::GroupAuthorization { group_id },
                    authorization.to_bytes(&actor).unwrap(),
                ),
            ] {
                writes.push((
                    target.storage_keyspace().to_string(),
                    target.storage_key(),
                    aruna_core::types::Value::from(bytes),
                ));
            }
        }
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes,
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));

        let documents = device_group_documents(&context, member).await;
        assert_eq!(documents.len(), 1);
        assert_eq!(
            documents[0].group.group_id,
            Ulid::from((GROUP_SCAN_PAGE + 1) as u128)
        );
    }
}
