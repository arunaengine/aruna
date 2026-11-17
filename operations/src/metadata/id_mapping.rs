//! Persistent id mapping rules: where a mapping lives, its revisions and the entries a
//! transition writes, plus its storage read and errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentOutboxEvent;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ID_MAPPING_KEYSPACE;
use aruna_core::storage_entries::{shard_manifest_entry, sync_revision_entry};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::{
    PersistentIdMapping, PersistentIdRevision, persistent_id_change, persistent_id_key,
    persistent_id_target, secondary_index_entries,
};
use aruna_core::types::TxnId;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::create_document::resolve_metadata_id;
use crate::sync::outbox_record::{new_outbox_record, outbox_write_entry};
use aruna_core::placement::resolve_shard_holders;

#[derive(Debug, Error, PartialEq)]
pub enum PersistentIdError {
    #[error(transparent)]
    Storage(StorageError),
    #[error(transparent)]
    Conversion(ConversionError),
    /// A mint whose document has no live registry row on this authority.
    #[error("persistent id target document is absent")]
    DocumentMissing,
    #[error("persistent id intent has not reached this authority yet")]
    IntentMissing,
    #[error("persistent id authority is unavailable: {0}")]
    Unavailable(String),
    /// The mapping's bucket cut over to a new holder set mid-transition.
    #[error("persistent id mapping bucket cut over; retry the transition")]
    PlacementFenced,
}

pub(crate) fn read_mapping_effect(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: ID_MAPPING_KEYSPACE.to_string(),
        key: ByteView::from(persistent_id_key(document_id)),
        txn_id,
    })
}

pub(crate) fn parse_mapping_read(
    event: Event,
) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                PersistentIdMapping::from_bytes(bytes.as_ref())
                    .map_err(PersistentIdError::Conversion)
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        _ => Err(PersistentIdError::Storage(StorageError::ReadError(
            "unexpected event".to_string(),
        ))),
    }
}

/// Where a mapping row lives and who replicates it: the document-lifecycle
/// placement, never the deletable registry row, so a withdrawal still routes and
/// replicates after the document is gone.
#[derive(Debug, Clone, PartialEq)]
pub struct MappingRoute {
    pub realm_id: RealmId,
    pub placement: PlacementRef,
    pub peers: Vec<aruna_core::NodeId>,
    pub actor: aruna_core::NodeId,
    /// Activation generation the mapping's bucket resolved at. `0` means the
    /// realm has no activation yet, so no transition can be in flight.
    pub generation: u64,
}

/// The route for a caller that already holds the realm config, such as the delete
/// transaction that writes the tombstone alongside the registry row it removes.
pub(crate) fn mapping_route_for(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
    actor: aruna_core::NodeId,
) -> Option<MappingRoute> {
    let placement = mapping_placement(config, realm_id, document_id).ok()?;
    Some(MappingRoute {
        realm_id,
        placement,
        peers: resolve_shard_holders(config, &placement),
        actor,
        generation: aruna_core::placement::fence::write_generation(config, &placement).unwrap_or(0),
    })
}

pub(crate) fn mapping_placement(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<PlacementRef, PersistentIdError> {
    resolve_metadata_id(config, realm_id, None, document_id)
        .map_err(|error| PersistentIdError::Unavailable(error.to_string()))
}

/// A context without a net handle replicates nothing, so its transitions record a
/// fixed placeholder actor rather than inventing an identity.
pub(crate) fn transition_actor(route: &Option<MappingRoute>) -> aruna_core::NodeId {
    route
        .as_ref()
        .map(|route| route.actor)
        .unwrap_or_else(|| iroh::SecretKey::from_bytes(&[0u8; 32]).public())
}

pub(crate) fn mapping_revision(
    route: &Option<MappingRoute>,
    occurred_at_ms: u64,
) -> PersistentIdRevision {
    PersistentIdRevision {
        event_id: Ulid::generate(),
        actor: transition_actor(route),
        occurred_at_ms,
    }
}

/// Deletion transition shared with the document delete. It is composed into the
/// transaction that removes the registry row; an absent mapping stays absent.
pub(crate) fn tombstone_transition(
    existing: Option<&PersistentIdMapping>,
    route: &Option<MappingRoute>,
    document_id: Ulid,
    deleted_at_ms: u64,
) -> Result<Option<(PersistentIdMapping, Vec<TransitionEntry>)>, PersistentIdError> {
    let Some(mapping) = existing else {
        return Ok(None);
    };
    debug_assert_eq!(mapping.target, document_id);
    let mut mapping = mapping.clone();
    if !mapping.mark_tombstoned(mapping_revision(route, deleted_at_ms)) {
        return Ok(None);
    }
    let writes = transition_entries(route, &mapping)?;
    Ok(Some((mapping, writes)))
}

pub type TransitionEntry = (String, ByteView, ByteView);

/// Row, identifier index, sync sidecar, shard-manifest entry and outbox publish, so an accepted
/// transition is either fully durable and replicated or not taken.
pub(crate) fn transition_entries(
    route: &Option<MappingRoute>,
    mapping: &PersistentIdMapping,
) -> Result<Vec<TransitionEntry>, PersistentIdError> {
    let target = persistent_id_target(mapping.target);
    let mut writes = vec![(
        ID_MAPPING_KEYSPACE.to_string(),
        ByteView::from(persistent_id_key(mapping.target)),
        ByteView::from(mapping.to_bytes().map_err(PersistentIdError::Conversion)?),
    )];
    writes.extend(secondary_index_entries(mapping));
    if let Some(route) = route {
        let change = persistent_id_change(mapping, route.placement);
        writes.push(sync_revision_entry(&target, &change).map_err(PersistentIdError::Conversion)?);
        if let Some(entry) =
            shard_manifest_entry(&target, &change).map_err(PersistentIdError::Conversion)?
        {
            writes.push(entry);
        }
        if !route.peers.is_empty() {
            let record = new_outbox_record(
                route.actor,
                target,
                route.peers.clone(),
                DocumentOutboxEvent::Upsert {
                    bytes: mapping.to_bytes().map_err(PersistentIdError::Conversion)?,
                    change,
                },
                route.placement,
                false,
            )
            .fenced_at(route.generation);
            let entry = outbox_write_entry(&record)
                .map_err(|error| PersistentIdError::Conversion(error.into()))?;
            writes.push(entry);
        }
    }
    Ok(writes)
}
