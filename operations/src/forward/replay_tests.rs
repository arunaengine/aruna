//! Tests the replay reads of forwarded document writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::record::METADATA_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::Value;
use aruna_core::{MetaResourceId, StructuredId};

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn fixture() -> (RealmConfigDocument, Ulid, NodeId, NodeId) {
    let realm_id = RealmId::from_bytes([8; 32]);
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for seed in 1..=4 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
    let id = MetaResourceId::from_parts(1, handle, BucketId::new(9).unwrap(), 1).unwrap();
    let document_id = id.as_ulid();
    let placement = resolve_metadata_id(&config, realm_id, None, document_id).unwrap();
    let holders = resolve_shard_holders(&config, &placement);
    let outsider = (1..=4).map(node).find(|node| !holders.contains(node));
    (config, document_id, holders[0], outsider.unwrap())
}

fn record(config: &RealmConfigDocument, id: Ulid, holder: NodeId) -> MetadataRegistryRecord {
    let realm_id = config.realm_id;
    let group_id = Ulid::from_bytes([3; 16]);
    MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id: id,
        document_path: "docs/one".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id, group_id, "docs/one", id,
        ),
        placement: resolve_metadata_id(config, realm_id, Some(group_id), id).unwrap(),
        holder_node_ids: vec![holder],
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_bytes([4; 16]),
        last_event_id: Ulid::from_bytes([4; 16]),
    }
}

fn stored(record: &MetadataRegistryRecord) -> IoResult {
    let bytes: Value = postcard::to_allocvec(record).unwrap().into();
    IoResult::Values(vec![Some(bytes)])
}

fn pending(rows: usize) -> IoResult {
    let row = (Key::from(vec![1u8]), Value::from(Vec::new()));
    IoResult::Page {
        rows: vec![row; rows],
        next: None,
    }
}

#[test]
fn holder_reads_record() {
    let (config, id, holder, _) = fixture();
    let expected = record(&config, id, holder);
    let mut operation = HeldRecordOperation::new(config, holder, id);
    let row = read_row(DOCUMENT_INDEX_KEYSPACE, metadata_document_key(id));
    assert_eq!(operation.first(), Ok(Next::Call(row)));
    assert_eq!(operation.next(stored(&expected)), Ok(Next::Done(expected)));
}

#[test]
fn outsider_refused_early() {
    let (config, id, _, outsider) = fixture();
    let mut operation = HeldRecordOperation::new(config, outsider, id);
    assert!(matches!(
        operation.first(),
        Err(HeldRecordError::Unavailable(_))
    ));
}

#[test]
fn frozen_holders_checked() {
    // A record frozen to other holders is not this node's to change.
    let (config, id, holder, outsider) = fixture();
    let foreign = record(&config, id, outsider);
    let mut operation = HeldRecordOperation::new(config, holder, id);
    operation.first().unwrap();
    assert!(matches!(
        operation.next(stored(&foreign)),
        Err(HeldRecordError::Unavailable(message)) if message.contains("frozen holder")
    ));
}

#[test]
fn missing_record_scans() {
    // An empty registry means not found only without a queued projection.
    for (rows, found) in [(0, true), (1, false)] {
        let (config, id, holder, _) = fixture();
        let mut operation = HeldRecordOperation::new(config, holder, id);
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Call(pending_scan(id)))
        );
        let ended = operation.next(pending(rows));
        assert_eq!(ended == Err(HeldRecordError::NotFound), found);
        assert!(ended.is_err());
    }
}

#[test]
fn deleted_needs_acceptance() {
    let id = Ulid::from_bytes([5; 16]);
    let mut queued = DeletedHereOperation::new(id);
    assert_eq!(queued.first(), Ok(Next::Call(pending_scan(id))));
    assert_eq!(queued.next(pending(1)), Ok(Next::Done(false)));

    let mut unknown = DeletedHereOperation::new(id);
    unknown.first().unwrap();
    let row = read_row(CREATE_ACCEPTANCE_KEYSPACE, create_acceptance_key(id));
    assert_eq!(unknown.next(pending(0)), Ok(Next::Call(row)));
    assert_eq!(
        unknown.next(IoResult::Values(vec![None])),
        Ok(Next::Done(false))
    );
}

#[test]
fn create_rows_read() {
    let (config, id, holder, _) = fixture();
    let expected = record(&config, id, holder);
    let mut operation = CreateRowsOperation::new(id);
    assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 2));
    let IoResult::Values(mut row) = stored(&expected) else {
        unreachable!("stored is a value row");
    };
    row.push(None);
    assert_eq!(
        operation.next(IoResult::Values(row)),
        Ok(Next::Done(Some((expected, None))))
    );
    let mut missing = CreateRowsOperation::new(id);
    assert_eq!(
        missing.next(IoResult::Values(vec![None, None])),
        Ok(Next::Done(None))
    );
}

#[test]
fn replay_needs_acceptance() {
    let (config, id, holder, _) = fixture();
    let create = CreateDocumentConfig {
        actor: aruna_core::structs::identity::auth::Actor {
            node_id: holder,
            user_id: aruna_core::UserId::nil(config.realm_id),
            realm_id: config.realm_id,
        },
        group_id: Ulid::from_bytes([3; 16]),
        document_id: id,
        document_path: "docs/one".to_string(),
        public: true,
        payload: crate::metadata::create_document::CreateDocumentPayload::Scaffold {
            name: "one".to_string(),
            description: String::new(),
            date_published: "2026-01-01".to_string(),
            license: None,
        },
    };
    let answer = replay_answer(&create, (record(&config, id, holder), None));
    assert_eq!(
        answer,
        Err("existing metadata document has no create acceptance".to_string())
    );
}
