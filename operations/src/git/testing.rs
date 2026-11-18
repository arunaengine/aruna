//! Fixtures shared by the Git operation tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::git::{CommitInfo, GitAnswer, GitChange, GitEvent, GitRecord, StoredObject};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::fmt::Debug;
use ulid::Ulid;

use super::GitNode;
use super::project::Projection;
use super::state::GitState;

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

/// This node as node one of the test realm.
pub(super) fn git_node() -> GitNode {
    GitNode {
        ids: Some((node(1), realm())),
        routing: Default::default(),
    }
}

/// A dataset of its own, for tests that touch the per-document caches.
pub(super) fn dataset_of(seed: u8) -> MetadataRegistryRecord {
    let document_id = Ulid::from_bytes([seed; 16]);
    MetadataRegistryRecord {
        document_id,
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        ..dataset()
    }
}

pub(super) fn answered(answer: GitAnswer) -> IoResult {
    IoResult::Event(Box::new(Event::Git(Box::new(answer))))
}

pub(super) fn ran(event: GitEvent) -> IoResult {
    answered(GitAnswer::Ran(event))
}

pub(super) fn asks_child<T, E>(turn: &Result<Next<T>, E>) -> bool {
    matches!(turn, Ok(Next::Call(IoCall::Child(_))))
}

pub(super) fn asks_store<T, E>(turn: &Result<Next<T>, E>) -> bool {
    let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
        return false;
    };
    matches!(**effect, Effect::Git(_))
}

pub(super) fn object(holder: NodeId) -> StoredObject {
    StoredObject {
        node_id: holder,
        group_id: Some(Ulid::from_bytes([2; 16])),
        bucket: "bucket".to_string(),
        key: "git-packs/one".to_string(),
        version_id: Ulid::from_bytes([3; 16]),
        size: 4,
        sha256: "ab".repeat(32),
        blake3: [1; 32],
        global: None,
    }
}

pub(super) fn record(seed: u128, change: GitChange) -> GitRecord {
    let document = dataset();
    GitRecord {
        event_id: Ulid::from(seed),
        realm_id: realm(),
        group_id: document.group_id,
        document_id: document.document_id,
        placement: PlacementRef::NIL,
        user_id: auth().user_id,
        node_id: node(1),
        occurred_at_ms: 1,
        change,
    }
}

/// A projection held by this node with these refs.
pub(super) fn projection(refs: &[(&str, &str)]) -> Projection {
    let mut state = GitState::default();
    for (name, target) in refs {
        state.refs.insert(name.to_string(), target.to_string());
    }
    Projection {
        state,
        records: Default::default(),
        holders: vec![node(1)],
        writes: 0,
    }
}

pub(super) fn commit(id: &str) -> CommitInfo {
    CommitInfo {
        commit: id.to_string(),
        parents: Vec::new(),
        author_name: "Aruna".to_string(),
        author_email: "git@aruna.local".to_string(),
        committer_email: "git@aruna.local".to_string(),
        authored_at_s: 0,
        message: "Edit".to_string(),
        signed: true,
    }
}
