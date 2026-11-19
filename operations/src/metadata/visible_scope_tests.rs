//! Drives the deleted-graph sweep and the read scope through their answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::metadata::api::access::tests::{access_for, realm};
use crate::tests::calls::{child, runs_child};
use aruna_core::events::Event;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::Value;
use ulid::Ulid;

pub(crate) fn record(seed: u64) -> MetadataRegistryRecord {
    let document_id = Ulid::from_parts(seed, 1);
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: format!("datasets/{seed}"),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: format!("/{}/g/{}/datasets/{seed}", realm(), Ulid::from_parts(2, 2)),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: document_id,
        last_event_id: document_id,
    }
}

pub(crate) fn answer(event: MetadataEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Metadata(event)))
}

fn tombstone(record: &MetadataRegistryRecord) -> (Key, Value) {
    let lifecycle = GraphLifecycleRecord::deleted(
        record.graph_iri.clone(),
        record.realm_id,
        record.group_id,
        record.document_id,
        1,
    );
    (
        Key::from(vec![1]),
        Value::from(postcard::to_allocvec(&lifecycle).unwrap()),
    )
}

#[test]
fn sweep_collects_deleted() {
    let mut sweep = DeletedGraphsOperation::new();
    assert!(matches!(sweep.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let page = IoResult::Page {
        rows: vec![tombstone(&record(1))],
        next: None,
    };
    let deleted = HashSet::from([record(1).graph_iri]);
    assert_eq!(sweep.next(page), Ok(Next::Done((deleted, 1))));
}

#[test]
fn scope_drops_deleted() {
    let records = [record(1), record(2), record(3)];
    let filter = [records[0].graph_iri.clone(), records[1].graph_iri.clone()];
    let mut scope =
        ReadScopeOperation::new((realm(), None), &records, (Some(filter.as_slice()), None));
    let generation = Effect::Metadata(MetadataEffect::CacheGeneration);
    assert_eq!(
        scope.first(),
        Ok(Next::Call(IoCall::Effect(Box::new(generation))))
    );
    let swept = scope.next(answer(MetadataEvent::CacheGeneration { generation: 3 }));
    assert!(runs_child(swept));
    let deleted = HashSet::from([records[0].graph_iri.clone()]);
    let refresh = MetadataEffect::RefreshLifecycle {
        lifecycle: vec![
            (records[0].graph_iri.clone(), true),
            (records[1].graph_iri.clone(), false),
        ],
        generation: 3,
    };
    let refreshed = scope.next(child(Ok::<_, MetadataError>((deleted, 1))));
    assert_eq!(
        refreshed,
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            refresh
        )))))
    );
    let stored = answer(MetadataEvent::RegistryStored { accepted: true });
    assert!(runs_child(scope.next(stored)));
    let access = access_for(&records[..2], None);
    let visible = scope.next(child(Ok::<_, MetadataApiError>(access)));
    let expected = VisibleRecords {
        records: vec![records[1].clone()],
        readable_groups: 0,
    };
    assert_eq!(visible, Ok(Next::Done(expected)));
}
