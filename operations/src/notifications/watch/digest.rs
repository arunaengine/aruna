//! Bounds a node's watch interest digest and builds the effects that request its publish.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::Effect;
use aruna_core::keyspaces::WATCH_INTEREST_KEYSPACE;
use aruna_core::structs::execution::notification_watch::{
    INTEREST_BYTES_CAP, INTEREST_ENTRY_CAP, WatchEventKind, WatchEventMask, WatchInterestDigest,
    WatchInterestEntry, interest_dirty_key,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use ulid::Ulid;

/// Debounce window for the coalesced watch-interest publisher: `ShortenTimer`
/// fires this long after the first dirty write, collapsing a run of watch CRUD
/// into one publish with bounded latency.
pub const WATCH_PUBLISH_DEBOUNCE: Duration = Duration::from_secs(2);

/// Schedules (or shortens toward) the debounced watch-interest publish task.
pub fn schedule_publish_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PublishWatchInterest,
        after: WATCH_PUBLISH_DEBOUNCE,
    })
}

/// Dirty marker written in the same transaction as a subscription row change,
/// so the debounced publisher knows which realm's digest to rebuild. The value
/// is a generation id; a marker is only cleared if its generation still matches.
pub fn dirty_marker_write(realm_id: RealmId) -> (KeySpace, Key, Value) {
    let generation = ByteView::from(Ulid::generate().to_bytes().to_vec());
    (
        WATCH_INTEREST_KEYSPACE.to_string(),
        ByteView::from(interest_dirty_key(realm_id)),
        generation,
    )
}

pub(crate) fn digest_over(digest: &WatchInterestDigest) -> Result<bool, String> {
    if digest.entries.len() > INTEREST_ENTRY_CAP {
        return Ok(true);
    }
    Ok(digest.to_bytes().map_err(|error| error.to_string())?.len() > INTEREST_BYTES_CAP)
}

pub(crate) fn catchall_digest(node_id: NodeId) -> WatchInterestDigest {
    WatchInterestDigest {
        node_id,
        entries: vec![WatchInterestEntry {
            path_prefix: String::new(),
            event_mask: WatchEventMask::from_kinds([
                WatchEventKind::MetadataCreated,
                WatchEventKind::DataUploaded,
                WatchEventKind::SyncCompleted,
                WatchEventKind::SyncFailed,
            ]),
        }],
    }
}
