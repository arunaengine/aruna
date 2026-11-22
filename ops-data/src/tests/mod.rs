//! Groups the shared test helpers of the data operations crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub(crate) use aruna_ops_identity::tests::{calls, locations, peers};

pub(crate) mod connectors;
pub(crate) mod device;
pub(crate) mod metadata;
pub(crate) mod metadata_api;
pub(crate) mod policy;

/// The plugin journal rows this node has not matched yet, at most a hundred.
pub(crate) async fn pending_events(
    context: &aruna_drivers::driver::DriverContext,
) -> Vec<aruna_core::plugin_runs::SourceEvent> {
    let page = aruna_operations::plugins::journal::journal_page(
        aruna_core::plugin_runs::PENDING_ROW,
        None,
        100,
    );
    let (rows, _) = aruna_drivers::driver::drive(page, context).await.unwrap();
    rows.into_iter().map(|(_, event)| event).collect()
}
