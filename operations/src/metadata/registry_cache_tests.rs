//! Drives the registry read and fill through the graph cache answers and storage pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{child, runs_child};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use std::collections::HashSet;
use ulid::Ulid;

fn record(seed: u64) -> MetadataRegistryRecord {
    let document_id = Ulid::from_parts(seed, 1);
    MetadataRegistryRecord {
        realm_id: RealmId::from_bytes([1; 32]),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: format!("datasets/{seed}"),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: String::new(),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: document_id,
        last_event_id: document_id,
    }
}

fn answer(event: MetadataEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Metadata(event)))
}

fn snapshot(records: Option<Vec<MetadataRegistryRecord>>, fresh: bool) -> IoResult {
    answer(MetadataEvent::RegistrySnapshot {
        records: records.map(Arc::new),
        fresh,
        generation: 7,
    })
}

fn row(value: Vec<u8>) -> (Key, Value) {
    (Key::from(vec![1]), Value::from(value))
}

#[test]
fn fresh_snapshot_answers() {
    let mut read = RegistryReadOperation::new();
    let call = graph_call(MetadataEffect::RegistrySnapshot);
    assert_eq!(read.first(), Ok(Next::Call(call)));
    let done = read.next(snapshot(Some(vec![record(1)]), true));
    assert_eq!(done, Ok(Next::Done(Arc::new(vec![record(1)]))));
}

#[test]
fn stale_snapshot_refills() {
    let mut read = RegistryReadOperation::new();
    read.first().unwrap();
    let refill = IoCall::background(RegistryFillOperation::new());
    let expected = Next::Then {
        output: Arc::new(vec![record(1)]),
        calls: vec![refill],
    };
    assert_eq!(
        read.next(snapshot(Some(vec![record(1)]), false)),
        Ok(expected)
    );
}

#[test]
fn missing_snapshot_fills() {
    let mut read = RegistryReadOperation::new();
    read.first().unwrap();
    assert!(runs_child(read.next(snapshot(None, false))));
    let filled = Arc::new(vec![record(2)]);
    let done = read.next(child(Ok::<_, MetadataError>(filled.clone())));
    assert_eq!(done, Ok(Next::Done(filled)));
}

#[test]
fn fill_stores_registry() {
    let mut fill = RegistryFillOperation::new();
    assert_eq!(fill.hold_scope(), Some(HoldScope::RegistryFill));
    fill.first().unwrap();
    let page = fill.next(snapshot(None, false));
    let Ok(Next::Call(IoCall::Page { space, .. })) = page else {
        panic!("expected the registry page, got {page:?}");
    };
    assert_eq!(space, METADATA_INDEX_KEYSPACE);
    let (live, gone) = (record(2), record(1));
    let rows = (live.clone(), gone.clone());
    let registry = IoResult::Page {
        rows: vec![
            row(postcard::to_allocvec(&rows.0).unwrap()),
            row(postcard::to_allocvec(&rows.1).unwrap()),
        ],
        next: None,
    };
    let sweep = IoCall::child(DeletedGraphsOperation::new());
    assert_eq!(fill.next(registry), Ok(Next::Call(sweep)));
    let lifecycle = child(Ok::<_, MetadataError>(HashSet::from([gone
        .graph_iri
        .clone()])));
    let store = MetadataEffect::StoreRegistry {
        records: vec![live.clone()],
        lifecycle: vec![
            (gone.graph_iri.clone(), true),
            (live.graph_iri.clone(), false),
        ],
        generation: 7,
    };
    assert_eq!(fill.next(lifecycle), Ok(Next::Call(graph_call(store))));
    let stored = answer(MetadataEvent::RegistryStored { accepted: true });
    assert_eq!(fill.next(stored), Ok(Next::Done(Arc::new(vec![live]))));
}

#[test]
fn warmup_fills_registry() {
    let mut warm = WarmCachesOperation::new();
    let indexes = graph_call(MetadataEffect::EnsureIndexes);
    assert_eq!(warm.first(), Ok(Next::Call(indexes)));
    let read = IoCall::child(RegistryReadOperation::new());
    assert_eq!(
        warm.next(answer(MetadataEvent::IndexesReady)),
        Ok(Next::Call(read))
    );
    let filled = child(Ok::<_, MetadataError>(Arc::new(vec![record(1)])));
    assert_eq!(warm.next(filled), Ok(Next::Done(())));
}

#[test]
fn oversized_fill_fails() {
    let mut fill = RegistryFillOperation::new();
    fill.first().unwrap();
    fill.next(snapshot(None, false)).unwrap();
    let value = postcard::to_allocvec(&record(1)).unwrap();
    let rows = vec![row(value); REGISTRY_CANDIDATE_LIMIT + 1];
    let page = IoResult::Page { rows, next: None };
    assert_eq!(fill.next(page), Err(limit_exceeded()));
}
