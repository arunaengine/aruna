//! Writes raw storage rows for the API integration tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_api::server::state::ServerState;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};

/// Writes one row through the storage of `state`; panics on any other answer.
pub(crate) async fn write_value(
    state: &ServerState,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) {
    let event = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}
