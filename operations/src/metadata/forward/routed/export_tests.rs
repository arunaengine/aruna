//! Drives what follows a routed export's holder answers, without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoError};
use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::*;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn owner() -> UserId {
    UserId::local(Ulid::from_bytes([7; 16]), realm())
}

fn export() -> ExportMetadataResult {
    let document_id = Ulid::from_parts(3, 3);
    let record = MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: "datasets/exported".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: String::new(),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(4, 4),
        last_event_id: Ulid::from_parts(5, 5),
    };
    ExportMetadataResult::Full {
        record,
        jsonld: "{}".to_string(),
    }
}

/// An export on the owner's device, as `user_id`, whose one holder answered.
fn answered(user_id: UserId) -> RoutedExportOperation {
    let request = ExportMetadataRequest {
        document_id: Ulid::from_parts(3, 3),
        auth: Some(AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        }),
        view: RoCrateExportView::Full,
        limit: None,
        offset: None,
        after: None,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.nodes.push(RealmNode {
        node_id: node(1).to_string(),
        kind: RealmNodeKind::User { owner: owner() },
    });
    let mut routed = RoutedExportOperation::new(Some(node(1)), realm(), request, None, 1_000);
    routed.config = Some(config);
    routed.holder_count = 1;
    routed.answers = vec![(node(2), Ok(export()))];
    routed
}

#[test]
fn owner_export_kept() {
    let mut routed = answered(owner());
    assert!(matches!(routed.collect(), Ok(Next::Call(IoCall::Child(_)))));
    // Keeping is best effort, so its failure still answers with the export.
    let failed = Err::<(), _>(IoError::Unfinished);
    let answer = routed.next(IoResult::Child(ChildOutcome::new(failed)));
    assert_eq!(answer, Ok(Next::Done(export())));
}

#[test]
fn other_caller_answers() {
    let mut routed = answered(UserId::local(Ulid::from_bytes([8; 16]), realm()));
    assert_eq!(routed.collect(), Ok(Next::Done(export())));
}
