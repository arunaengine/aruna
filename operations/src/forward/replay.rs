use crate::driver::DriverContext;
use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::CreateDocumentConfig;
use crate::metadata::create_document::accepted_create_matches;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::get_document::load_document_record;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::placement::holds_placement;
use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::METADATA_CREATE_ACCEPTANCE_KEYSPACE;
use aruna_core::keyspaces::METADATA_PENDING_PROJECTION_KEYSPACE;
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::storage_entries::create_acceptance_key;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::placement::placement_record::PlacementRef;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use std::sync::Arc;
use ulid::Ulid;

pub(crate) fn routed_record_matches(
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

pub(crate) fn create_record_matches(
    config: &CreateDocumentConfig,
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

pub(crate) fn update_record_matches(
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

pub(crate) async fn forwarded_create_replay(
    context: &Arc<DriverContext>,
    config: &CreateDocumentConfig,
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

pub(crate) async fn accepted_create(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<Option<MetadataEventRecord>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
            key: create_acceptance_key(document_id),
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

/// Whether this node has evidence that the document was created here and is now
/// gone, rather than a create whose projection has not landed: the create
/// acceptance survives the delete, while a queued projection means "not yet".
pub(crate) async fn document_deleted_here(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<bool, MetadataApiError> {
    if projection_queued(context, document_id).await? {
        return Ok(false);
    }
    accepted_create(context, document_id)
        .await
        .map(|accepted| accepted.is_some())
        .map_err(|_| MetadataApiError::ServiceUnavailable)
}

/// Whether a committed metadata event for this document is still waiting to be
/// projected into this node's registry.
pub(crate) async fn projection_queued(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<bool, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: Some(byteview::ByteView::from(document_id.to_bytes().to_vec())),
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// The document's registry record, whatever this node's holdership of it.
pub(crate) async fn existing_record(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, String> {
    load_document_record(context.as_ref(), document_id)
        .await
        .map_err(|error| format!("metadata registry read failed: {error:?}"))
}

pub(crate) enum HeldRecordError {
    NotFound,
    Unavailable(String),
}

/// Loads a document only when this node holds its current structured placement.
pub(crate) async fn held_record(
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
    let record = match existing_record(context, document_id)
        .await
        .map_err(HeldRecordError::Unavailable)?
    {
        Some(record) => record,
        // A pending committed projection makes an empty registry read inconclusive.
        None => {
            return Err(match projection_queued(context, document_id).await {
                Ok(true) => HeldRecordError::Unavailable(format!(
                    "metadata document `{document_id}` has a queued registry projection"
                )),
                Ok(false) => HeldRecordError::NotFound,
                Err(_) => HeldRecordError::Unavailable(
                    "pending metadata projection scan is unavailable".to_string(),
                ),
            });
        }
    };
    if !routed_record_matches(config, config.realm_id, document_id, &placement, &record) {
        return Err(HeldRecordError::Unavailable(
            "metadata registry record does not match its structured placement".to_string(),
        ));
    }
    if !record.holder_node_ids.contains(&local_node_id) {
        return Err(HeldRecordError::Unavailable(
            "node is not a frozen holder for this metadata document".to_string(),
        ));
    }
    Ok(record)
}
