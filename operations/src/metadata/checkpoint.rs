//! Opens a new metadata history window once the current one fills up.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{EVENT_LOG_KEYSPACE, METADATA_CHECKPOINT_KEYSPACE};
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::metadata::{EVENT_LIMIT, RAW_BYTES_LIMIT};
use aruna_core::storage_entries::event_log_prefix;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::{NodeId, UserId};
use std::collections::BTreeMap;

use crate::driver::DriverContext;
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
    update_metadata_document,
};

/// Whether this holder should checkpoint once one origin has used `used` of its `share`:
/// the first origin at half, any other origin at three quarters, so a missing first origin
/// never freezes the document.
pub fn due(used: (u64, u64), share: (u64, u64), first: bool) -> bool {
    let (part, of) = if first { (1u64, 2u64) } else { (3, 4) };
    used.0.saturating_mul(of) >= share.0.saturating_mul(part)
        || used.1.saturating_mul(of) >= share.1.saturating_mul(part)
}

/// The most any one node has written in the current window, as events and bytes.
async fn window(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<(u64, u64), UpdateDocumentError> {
    let prefix = event_log_prefix(record.document_id);
    let last = context
        .storage_handle
        .send_storage_effect(StorageEffect::Last {
            key_space: METADATA_CHECKPOINT_KEYSPACE.to_string(),
            prefix: Some(prefix.clone()),
            txn_id: None,
        })
        .await;
    let start = match last {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            values.first().map(|(key, _)| IterStart::At(key.clone()))
        }
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(UpdateDocumentError::MissingTransaction),
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: EVENT_LOG_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start,
            limit: EVENT_LIMIT as usize,
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            let mut used: BTreeMap<NodeId, (u64, u64)> = BTreeMap::new();
            for (_, value) in &values {
                let event: MetadataEventRecord = postcard::from_bytes(value)
                    .map_err(aruna_core::errors::ConversionError::from)?;
                let entry = used.entry(event.node_id).or_default();
                entry.0 += 1;
                entry.1 += value.len() as u64;
            }
            Ok(used.into_values().fold((0, 0), |most, used| {
                (most.0.max(used.0), most.1.max(used.1))
            }))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(UpdateDocumentError::MissingTransaction),
    }
}

/// Checks the document's current window after an event materialized here and writes a
/// checkpoint when it is due. Nodes that are not origins of the document never write one.
pub async fn after_materialization(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<(), UpdateDocumentError> {
    let Some(node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return Ok(());
    };
    let Some(position) = record
        .holder_node_ids
        .iter()
        .position(|holder| *holder == node_id)
    else {
        return Ok(());
    };
    let origins = record.holder_node_ids.len() as u64;
    let share = (
        u64::from(EVENT_LIMIT - 1) / origins,
        RAW_BYTES_LIMIT / origins,
    );
    if !due(window(context, record).await?, share, position == 0) {
        return Ok(());
    }
    let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
        actor: Actor {
            node_id,
            user_id: UserId::nil(record.realm_id),
            realm_id: record.realm_id,
        },
        group_id: record.group_id,
        document_id: record.document_id,
        public: record.public,
        mutation: UpdateDocumentMutation::Checkpoint,
    });
    update_metadata_document(operation, context)
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_origin_leads() {
        let share = (340, 4000);
        assert!(due((170, 0), share, true));
        assert!(!due((169, 0), share, true));
        assert!(!due((170, 0), share, false));
        assert!(due((255, 0), share, false));
        assert!(due((0, 2000), share, true));
        assert!(!due((0, 2000), share, false));
    }
}
