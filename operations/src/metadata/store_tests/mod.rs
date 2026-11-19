//! Declares the graph store effect tests and the imports they share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_test_support::metadata::export_rocrate_jsonld;
use aruna_drivers::testing::send_metadata_effect;
use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::metadata::ApplyRoCrateRequest;
use aruna_core::metadata::MetadataError;
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::storage_entries::graph_lifecycle_key;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use aruna_drivers::driver::DriverContext;
use aruna_drivers::testing::send_metadata_effect;
use aruna_graph::GraphHandle;
use aruna_storage::FjallStorage;
use aruna_storage::StorageHandle;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

const ROCRATE_12: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/roundtrip-1.2.json"
));
const ROCRATE_13: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/roundtrip-1.3.json"
));

/// A context with this storage and graph store and nothing else.
fn graph_context(storage: StorageHandle, graph: GraphHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        graph_handle: Some(graph),
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    }
}

mod auth;
mod effect;
mod lifecycle;
mod listing;
mod versions;
