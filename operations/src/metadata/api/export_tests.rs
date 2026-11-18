//! Drives the export operations through their children and graph answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::metadata::MetadataError;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn record(cursor: u64) -> MetadataRegistryRecord {
    let document_id = Ulid::from_parts(3, 3);
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: "datasets/export".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: String::new(),
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(4, 4),
        last_event_id: Ulid::from_parts(cursor, 4),
    }
}

fn request(view: RoCrateExportView) -> ExportMetadataRequest {
    ExportMetadataRequest {
        document_id: Ulid::from_parts(3, 3),
        auth: None,
        view,
        limit: None,
        offset: None,
        after: None,
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

/// An export answered up to its graph export: found, readable and materialized.
fn at_export(view: RoCrateExportView) -> (ExportOperation, ExportTurn) {
    let mut export = ExportOperation::new(realm(), request(view));
    assert!(matches!(export.first(), Ok(Next::Call(IoCall::Child(_)))));
    export
        .next(child(Ok::<_, LiveRecordsError>(Some(record(5)))))
        .unwrap();
    export.next(child(Ok::<_, MetadataApiError>(()))).unwrap();
    let turn = export.next(child(Ok::<_, StorageReadError>(true)));
    (export, turn)
}

#[test]
fn summary_exports_at_cursor() {
    let (mut export, turn) = at_export(RoCrateExportView::Summary);
    let cursor = record(5).last_event_id;
    assert_eq!(
        turn,
        Ok(Next::Call(graph_effect(MetadataEffect::ExportSummaryAt {
            graph_iri: record(5).graph_iri,
            cursor,
        })))
    );
    let summary = answer(MetadataEvent::RoCrateSummaryResult {
        graph_iri: record(5).graph_iri,
        jsonld: "{}".to_string(),
    });
    assert_eq!(
        export.next(summary),
        Ok(Next::Done(ExportMetadataResult::Summary {
            jsonld: "{}".to_string(),
            record: record(5),
        }))
    );
}

#[test]
fn full_rechecks_cursor() {
    let (mut export, turn) = at_export(RoCrateExportView::Full);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
    let full = answer(MetadataEvent::RoCrateExportResult {
        graph_iri: record(5).graph_iri,
        jsonld: "{}".to_string(),
    });
    assert!(matches!(
        export.next(full),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    // A revision that landed during the export is refused.
    let moved = child(Ok::<_, LiveRecordsError>(Some(record(6))));
    assert_eq!(
        export.next(moved),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn page_refuses_two_cursors() {
    let mut export = ExportOperation::new(realm(), {
        let mut request = request(RoCrateExportView::Page);
        request.offset = Some(1);
        request.after = Some("x".to_string());
        request
    });
    export.first().unwrap();
    export
        .next(child(Ok::<_, LiveRecordsError>(Some(record(5)))))
        .unwrap();
    export.next(child(Ok::<_, MetadataApiError>(()))).unwrap();
    assert_eq!(
        export.next(child(Ok::<_, StorageReadError>(true))),
        Err(MetadataApiError::BadRequest)
    );
}

#[test]
fn unmaterialized_unavailable() {
    let mut export = ExportOperation::new(realm(), request(RoCrateExportView::Summary));
    export.first().unwrap();
    export
        .next(child(Ok::<_, LiveRecordsError>(Some(record(5)))))
        .unwrap();
    export.next(child(Ok::<_, MetadataApiError>(()))).unwrap();
    assert_eq!(
        export.next(child(Ok::<_, StorageReadError>(false))),
        Err(MetadataApiError::ServiceUnavailable)
    );
    let missing = answer(MetadataEvent::Error {
        graph_iri: None,
        error: MetadataError::GraphNotFound,
    });
    assert_eq!(exported(missing), Err(MetadataApiError::ServiceUnavailable));
}

#[test]
fn raw_reads_one_snapshot() {
    let mut raw = RawExportOperation::new(realm(), request(RoCrateExportView::Raw));
    assert_eq!(raw.transaction(), TransactionMode::ReadSnapshot);
    // The record read is the first call of the embedded live record read.
    assert!(matches!(raw.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(
        raw.next(IoResult::Values(vec![None])),
        Err(MetadataApiError::NotFound)
    );
}

#[test]
fn visible_needs_record() {
    let mut visible = VisibleRecordOperation::new(
        realm(),
        GetVisibleRequest {
            document_id: Ulid::from_parts(3, 3),
            auth: None,
        },
    );
    visible.first().unwrap();
    assert_eq!(
        visible.next(child(
            Ok::<Option<MetadataRegistryRecord>, LiveRecordsError>(None)
        )),
        Err(MetadataApiError::NotFound)
    );
}
