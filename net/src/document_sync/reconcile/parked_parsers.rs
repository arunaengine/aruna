//! Parks database parser events whose entry is not known yet, so the realm config replay moves
//! past them. Each realm config batch takes them up again until they apply or are refused.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PARKED_PARSER_KEYSPACE;
use aruna_core::structs::SyncQuarantineIdentity;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use serde::{Deserialize, Serialize};

use crate::error::{NetError, Result};

/// Parked rows read per storage call; every row is read, page by page.
const PAGE: usize = 256;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ParkedParser {
    pub target: DocumentTarget,
    pub event: AdminDocumentEvent,
    pub placement: PlacementRef,
    pub identity: SyncQuarantineIdentity,
    pub signature: iroh::Signature,
}

fn topic_prefix(topic: &::irokle::TopicId) -> Result<Vec<u8>> {
    postcard::to_allocvec(topic).map_err(|error| NetError::Bootstrap(error.to_string()))
}

/// Stores a parser event until its entry is known.
pub(super) async fn park_parser(
    storage: &StorageHandle,
    parked: &ParkedParser,
) -> Result<ByteView> {
    let mut key = topic_prefix(&parked.identity.topic)?;
    key.extend_from_slice(&parked.event.event_id.to_bytes());
    let value =
        postcard::to_allocvec(parked).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let key: ByteView = key.into();
    crate::document_sync::storage::batch_write_to(
        storage,
        vec![(
            PARKED_PARSER_KEYSPACE.to_string(),
            key.clone(),
            value.into(),
        )],
    )
    .await?;
    Ok(key)
}

/// Every parser event parked for `topic`, with its key.
pub(super) async fn parked_parsers(
    storage: &StorageHandle,
    topic: &::irokle::TopicId,
) -> Result<Vec<(ByteView, ParkedParser)>> {
    let prefix: ByteView = topic_prefix(topic)?.into();
    let mut found = Vec::new();
    let mut start = None;
    loop {
        let page = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: PARKED_PARSER_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start.take(),
                limit: PAGE,
                txn_id: None,
            })
            .await;
        let (values, next) = match page {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(NetError::Dht(format!(
                    "unexpected storage event while listing parked parser events: {other:?}"
                )));
            }
        };
        for (key, value) in values {
            if let Ok(parked) = postcard::from_bytes::<ParkedParser>(&value) {
                found.push((key, parked));
            }
        }
        match next {
            Some(after) => start = Some(IterStart::After(after)),
            None => return Ok(found),
        }
    }
}

/// Drops a parked event once it applied or was refused.
pub(super) async fn unpark_parser(storage: &StorageHandle, key: ByteView) -> Result<()> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: PARKED_PARSER_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while unparking a parser event: {other:?}"
        ))),
    }
}
