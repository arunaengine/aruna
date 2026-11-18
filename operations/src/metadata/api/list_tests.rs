//! Drives the listing operations through their cache, page, child and group answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::metadata::api::read_access::tests::{access_for, realm};
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::events::{GroupEvent, GroupResult};
use aruna_core::metadata::{MetadataError, MetadataEventPayload};
use aruna_core::storage_entries::pending_projection_key;
use aruna_core::types::Value;
use ulid::Ulid;

fn group() -> GroupId {
    Ulid::from_parts(2, 2)
}

fn record(group_id: GroupId, document: u64) -> MetadataRegistryRecord {
    let document_id = Ulid::from_parts(document, 3);
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id,
        document_id,
        document_path: format!("datasets/{document}"),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: String::new(),
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: document,
        establishing_event_id: Ulid::from_parts(4, 4),
        last_event_id: Ulid::from_parts(5, u128::from(document)),
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn answer(event: MetadataEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Metadata(event)))
}

fn encoded<T: serde::Serialize>(value: &T) -> Value {
    Value::from(postcard::to_allocvec(value).unwrap())
}

fn request(group_id: Option<GroupId>) -> ListVisibleRequest {
    ListVisibleRequest {
        group_id,
        path_prefix: None,
        include_summary: false,
        limit: None,
        offset: None,
        order: MetadataListOrder::Created,
        auth: None,
    }
}

#[test]
fn group_cache_answers() {
    let mut records = GroupRecordsOperation::new(group(), 2);
    let cached = MetadataEffect::CachedGroup { group_id: group() };
    let first = Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
        cached,
    )))));
    assert_eq!(records.first(), first);
    let hit = answer(MetadataEvent::CachedGroup {
        records: Some(vec![record(group(), 1)]),
    });
    assert!(matches!(
        records.next(hit),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let live = child(Ok::<_, LiveRecordsError>(vec![record(group(), 1)]));
    assert_eq!(records.next(live), Ok(Next::Done(vec![record(group(), 1)])));
}

#[test]
fn group_pages_capped() {
    let mut records = GroupRecordsOperation::new(group(), 1);
    records.first().unwrap();
    let missing = answer(MetadataEvent::Error {
        graph_iri: None,
        error: MetadataError::HandleMissing,
    });
    let Ok(Next::Call(IoCall::Page { prefix, .. })) = records.next(missing) else {
        panic!("expected the registry page");
    };
    assert_eq!(prefix, metadata_registry_prefix(group()));
    let row = |document| (Key::from(vec![document]), encoded(&record(group(), 1)));
    let page = IoResult::Page {
        rows: vec![row(1), row(2)],
        next: None,
    };
    assert_eq!(
        records.next(page),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

fn event(record: MetadataRegistryRecord) -> MetadataEventRecord {
    MetadataEventRecord {
        event_id: record.last_event_id,
        user_id: UserId::nil(realm()),
        node_id: iroh::SecretKey::from_bytes(&[7; 32]).public(),
        payload: MetadataEventPayload::RoCrate {
            jsonld: "{}".to_string(),
        },
        occurred_at_ms: 1,
        record,
    }
}

#[test]
fn pending_reads_events() {
    let pending = record(group(), 1);
    let other = record(Ulid::from_parts(9, 9), 2);
    let mut operation = PendingRecordsOperation::new(Some(group()), 4);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let marker = |record: &MetadataRegistryRecord| {
        let key = pending_projection_key(record.document_id, record.last_event_id);
        (key, Value::from(Vec::new()))
    };
    let page = IoResult::Page {
        rows: vec![marker(&pending), marker(&other)],
        next: None,
    };
    let Ok(Next::Call(IoCall::Read(reads))) = operation.next(page) else {
        panic!("expected the event reads");
    };
    assert_eq!(reads.len(), 2);
    let values = vec![
        Some(encoded(&event(pending.clone()))),
        Some(encoded(&event(other))),
    ];
    let turn = operation.next(IoResult::Values(values));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let live = child(Ok::<_, LiveRecordsError>(vec![pending.clone()]));
    let grouped = HashMap::from([(group(), vec![pending])]);
    assert_eq!(operation.next(live), Ok(Next::Done(grouped)));
}

#[test]
fn pending_scan_capped() {
    let mut operation = PendingRecordsOperation::new(None, 1);
    operation.first().unwrap();
    let row = |byte| (Key::from(vec![byte; 32]), Value::from(Vec::new()));
    let page = IoResult::Page {
        rows: vec![row(1), row(2)],
        next: None,
    };
    assert_eq!(
        operation.next(page),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn claims_join_pending() {
    let scan = CandidateScan::Claims { realm_id: realm() };
    let mut claims = CandidatesOperation::new(Some(group()), scan);
    assert!(matches!(claims.first(), Ok(Next::Call(IoCall::Child(_)))));
    let elsewhere = record(Ulid::from_parts(9, 9), 1);
    let pending = HashMap::from([(elsewhere.group_id, vec![elsewhere.clone()])]);
    let turn = claims.next(child(Ok::<_, MetadataApiError>(pending)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let listed = vec![record(group(), 3), record(group(), 2)];
    let done = claims.next(child(Ok::<_, MetadataApiError>(listed)));
    let expected = vec![record(group(), 2), record(group(), 3), elsewhere];
    assert_eq!(done, Ok(Next::Done(expected)));
}

#[test]
fn listing_skips_pending() {
    let scan = CandidateScan::Listing { pending: false };
    let mut listing = CandidatesOperation::new(None, scan);
    assert!(matches!(listing.first(), Ok(Next::Call(IoCall::Child(_)))));
    let groups = vec![Group {
        display_name: "listed".to_string(),
        group_id: group(),
        realm_id: realm(),
        roles: Default::default(),
        owner: UserId::nil(realm()),
    }];
    let turn = listing.next(child(Ok::<_, ListGroupError>(groups)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let listed = vec![record(group(), 3), record(group(), 2)];
    let done = listing.next(child(Ok::<_, MetadataApiError>(listed.clone())));
    assert_eq!(done, Ok(Next::Done(listed)));
}

#[test]
fn visible_page_windows() {
    let mut list = ListVisibleOperation::new(
        realm(),
        ListVisibleRequest {
            limit: Some(1),
            offset: Some(1),
            order: MetadataListOrder::Recent,
            ..request(Some(group()))
        },
    );
    assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));
    let records = vec![record(group(), 1), record(group(), 2), record(group(), 3)];
    let turn = list.next(child(Ok::<_, MetadataApiError>(records.clone())));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let access = access_for(&records, None);
    let Ok(Next::Done(page)) = list.next(child(Ok::<_, MetadataApiError>(access))) else {
        panic!("expected the page");
    };
    // Recent order is newest first, so the second visible record is document 2.
    let ids = page
        .documents
        .iter()
        .map(|document| document.record.document_id);
    assert_eq!(
        ids.collect::<Vec<_>>(),
        vec![record(group(), 2).document_id]
    );
    assert_eq!((page.limit, page.offset, page.total_returned), (1, 1, 1));
    assert_eq!(page.total_estimate, None);
}

#[test]
fn summaries_tolerate_pending() {
    let mut list = ListVisibleOperation::new(
        realm(),
        ListVisibleRequest {
            include_summary: true,
            ..request(Some(group()))
        },
    );
    list.first().unwrap();
    let records = vec![record(group(), 1), record(group(), 2)];
    list.next(child(Ok::<_, MetadataApiError>(records.clone())))
        .unwrap();
    let access = access_for(&records, None);
    let turn = list.next(child(Ok::<_, MetadataApiError>(access)));
    let Ok(Next::Call(IoCall::Group(group_call))) = turn else {
        panic!("expected the summary group");
    };
    assert_eq!(group_call.members.len(), 2);
    let member = |result: Result<String, MetadataApiError>| {
        GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(result))))
    };
    let results = vec![
        member(Ok("{}".to_string())),
        member(Err(MetadataApiError::ServiceUnavailable)),
    ];
    let finished = IoResult::Group(GroupEvent::Finished { results });
    let Ok(Next::Done(page)) = list.next(finished) else {
        panic!("expected the page");
    };
    let summaries = page
        .documents
        .iter()
        .map(|document| document.rocrate_summary_jsonld.clone());
    assert_eq!(
        summaries.collect::<Vec<_>>(),
        vec![Some("{}".to_string()), None]
    );
}

#[test]
fn summary_waits_materialized() {
    let mut summary = RecordSummaryOperation::new(record(group(), 1));
    assert!(matches!(summary.first(), Ok(Next::Call(IoCall::Child(_)))));
    let pending = child(Ok::<_, StorageReadError>(false));
    assert_eq!(
        summary.next(pending),
        Err(MetadataApiError::ServiceUnavailable)
    );

    let mut summary = RecordSummaryOperation::new(record(group(), 1));
    summary.first().unwrap();
    let turn = summary.next(child(Ok::<_, StorageReadError>(true)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let done = summary.next(child(Ok::<_, MetadataApiError>("{}".to_string())));
    assert_eq!(done, Ok(Next::Done("{}".to_string())));
}
