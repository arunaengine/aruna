//! Fixtures shared by the Git operation tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoResult};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::fmt::Debug;
use ulid::Ulid;

pub(super) fn realm() -> RealmId {
    RealmId::from_bytes([7; 32])
}

pub(super) fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(super) fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::new(Ulid::from_bytes([4; 16]), realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

pub(super) fn dataset() -> MetadataRegistryRecord {
    let document_id = Ulid::from_bytes([1; 16]);
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_bytes([2; 16]),
        document_id,
        document_path: "datasets/one".into(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: "datasets/one".into(),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from(1),
        last_event_id: Ulid::from(1),
    }
}

pub(super) fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + Debug + PartialEq + 'static,
    E: Send + Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}
