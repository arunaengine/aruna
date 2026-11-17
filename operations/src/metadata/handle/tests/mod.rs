//! Declares the metadata handle test modules and the imports they share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::lifecycle::records_for_group;
use super::search::{GraphVisibilityScope, LifecycleVisibility, record_for_graph};
use super::search::{filter_candidate_records, select_visible_records};
use super::*;
use aruna_core::auth::bearer_token_hash;
use aruna_core::events::Event;
use aruna_core::keys::generate_signing_key;
use aruna_core::metadata::ApplyRoCrateRequest;
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::storage_entries::graph_lifecycle_key;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::{RealmNodeKind, TokenRevocation};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{SyncMode, SyncState, SyncStatusSnapshot};
use aruna_storage::FjallStorage;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;
use tempfile::{TempDir, tempdir};
use tokio::io::AsyncWriteExt;

const ROCRATE_12: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/roundtrip-1.2.json"
));
const ROCRATE_13: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/roundtrip-1.3.json"
));

/// A context with this storage, metadata handle and graph store and nothing else.
fn graph_context(
    storage: StorageHandle,
    metadata: MetadataHandle,
    graph: GraphHandle,
) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: Some(metadata),
        graph_handle: Some(graph),
        task_handle: None,
        compute_handle: None,
    }
}

mod auth;
mod effect;
mod lifecycle;
mod query;
mod search;
mod sync;
mod visibility;
