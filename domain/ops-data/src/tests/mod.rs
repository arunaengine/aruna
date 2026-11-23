//! Groups the shared test helpers of the data operations crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub(crate) use aruna_ops_identity::tests::{calls, locations, peers};

pub(crate) mod connectors;
pub(crate) mod device;
mod groups;
pub(crate) mod metadata;
pub(crate) mod metadata_api;
pub(crate) mod policy;

/// The plugin journal events of row `kind` on this node, at most `limit`.
pub(crate) async fn journal_events(
    context: &aruna_runner::driver::DriverContext,
    kind: u8,
    limit: usize,
) -> Vec<aruna_core::plugin_runs::SourceEvent> {
    let (rows, _) = aruna_storage::scan::iter_prefix_page(
        &context.storage_handle,
        aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE,
        Some(vec![kind].into()),
        None,
        limit,
        None,
    )
    .await
    .unwrap();
    let event = |(_, value): &(_, aruna_core::types::Value)| {
        aruna_core::plugin_runs::SourceEvent::from_bytes(value).ok()
    };
    rows.iter().filter_map(event).collect()
}

/// The plugin journal rows this node has not matched yet, at most a hundred.
pub(crate) async fn pending_events(
    context: &aruna_runner::driver::DriverContext,
) -> Vec<aruna_core::plugin_runs::SourceEvent> {
    journal_events(context, aruna_core::plugin_runs::PENDING_ROW, 100).await
}
