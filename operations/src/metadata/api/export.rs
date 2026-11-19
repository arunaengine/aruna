//! Exports one metadata document as RO-Crate, raw, or summary JSON-LD.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;

use super::export_types::{
    ExportMetadataRequest, ExportMetadataResult, GetVisibleRequest, PAGE_DEFAULT, PAGE_MAX,
    RoCrateExportView,
};
use super::readable::RecordReadableOperation;
use super::{MetadataApiError, RealmId, Ulid, map_event_error};
use crate::metadata::call_results::effect_event;
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};
use crate::metadata::raw_events::RawReadError;
use crate::metadata::record_reads::RecordMaterializedOperation;
use crate::metadata::revision_read::{MetadataRawView, RawDisplayOperation};
use aruna_core::errors::StorageReadError;

type ExportTurn = Result<Next<ExportMetadataResult>, MetadataApiError>;

/// The output of an embedded step once it finished, or the turn that passes its call on.
fn embedded<T, U>(turn: Next<T>) -> Result<T, Next<U>> {
    match turn {
        Next::Done(output) | Next::Then { output, .. } => Ok(output),
        Next::Call(call) => Err(Next::Call(call)),
        Next::Wait => Err(Next::Wait),
    }
}

fn graph_effect(effect: MetadataEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

/// The answer of a graph export, or why there is none.
fn exported(result: IoResult) -> Result<MetadataEvent, MetadataApiError> {
    match effect_event(result)? {
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(map_event_error(error)),
        Event::Metadata(event) => Ok(event),
        other => Err(MetadataApiError::Internal(format!(
            "unexpected metadata export event: {}",
            other.label()
        ))),
    }
}

fn materialized(result: IoResult) -> Result<(), MetadataApiError> {
    match result.child::<bool, StorageReadError>()? {
        Ok(true) => Ok(()),
        Ok(false) => Err(MetadataApiError::ServiceUnavailable),
        Err(error) => Err(MetadataApiError::Internal(error.to_string())),
    }
}

fn live(result: IoResult) -> Result<MetadataRegistryRecord, MetadataApiError> {
    result
        .child::<Option<MetadataRegistryRecord>, LiveRecordsError>()??
        .ok_or(MetadataApiError::NotFound)
}

pub(super) fn raw_identity_matches(
    record: &MetadataRegistryRecord,
    realm_id: RealmId,
    document_id: Ulid,
) -> bool {
    record.realm_id == realm_id && record.document_id == document_id
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum VisibleStage {
    Record,
    Readable,
    Materialized,
}

/// A live document the caller may read whose graph matches its record.
#[derive(Debug, PartialEq)]
pub struct VisibleRecordOperation {
    realm_id: RealmId,
    request: GetVisibleRequest,
    stage: VisibleStage,
    record: Option<MetadataRegistryRecord>,
}

impl VisibleRecordOperation {
    pub fn new(realm_id: RealmId, request: GetVisibleRequest) -> Self {
        Self {
            realm_id,
            request,
            stage: VisibleStage::Record,
            record: None,
        }
    }
}

impl Operation for VisibleRecordOperation {
    type Output = MetadataRegistryRecord;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<MetadataRegistryRecord>, MetadataApiError> {
        Ok(Next::Call(IoCall::child(LiveRecordOperation::new(
            self.request.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MetadataRegistryRecord>, MetadataApiError> {
        match self.stage {
            VisibleStage::Record => {
                let record = live(result)?;
                let readable = RecordReadableOperation::new(
                    self.realm_id,
                    self.request.auth.clone(),
                    record.clone(),
                );
                self.record = Some(record);
                self.stage = VisibleStage::Readable;
                Ok(Next::Call(IoCall::child(readable)))
            }
            VisibleStage::Readable => {
                result.child::<(), MetadataApiError>()??;
                let record = self.record.as_ref().ok_or(MetadataApiError::NotFound)?;
                self.stage = VisibleStage::Materialized;
                Ok(Next::Call(IoCall::child(RecordMaterializedOperation::new(
                    record,
                ))))
            }
            VisibleStage::Materialized => {
                materialized(result)?;
                let record = self.record.take().ok_or(MetadataApiError::NotFound)?;
                Ok(Next::Done(record))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RawStage {
    Record(LiveRecordOperation),
    Readable(RecordReadableOperation),
    Raw(RawDisplayOperation),
}

/// The raw view of a document, read from one storage snapshot: the record, the caller's read
/// permission and the raw revision all describe the same moment.
#[derive(Debug, PartialEq)]
pub struct RawExportOperation {
    realm_id: RealmId,
    request: ExportMetadataRequest,
    stage: RawStage,
    record: Option<MetadataRegistryRecord>,
    now_ms: u64,
}

impl RawExportOperation {
    pub fn new(realm_id: RealmId, request: ExportMetadataRequest) -> Self {
        Self {
            stage: RawStage::Record(LiveRecordOperation::new(request.document_id)),
            realm_id,
            request,
            record: None,
            now_ms: 0,
        }
    }

    fn record_turn(&mut self, turn: Next<Option<MetadataRegistryRecord>>) -> ExportTurn {
        let record = match embedded(turn) {
            Ok(record) => record.ok_or(MetadataApiError::NotFound)?,
            Err(pending) => return Ok(pending),
        };
        if !raw_identity_matches(&record, self.realm_id, self.request.document_id) {
            return Err(MetadataApiError::NotFound);
        }
        let mut readable =
            RecordReadableOperation::new(self.realm_id, self.request.auth.clone(), record.clone());
        self.record = Some(record);
        readable.stamp(self.now_ms);
        let turn = readable.first()?;
        self.stage = RawStage::Readable(readable);
        self.readable_turn(turn)
    }

    fn readable_turn(&mut self, turn: Next<()>) -> ExportTurn {
        if let Err(pending) = embedded::<(), ExportMetadataResult>(turn) {
            return Ok(pending);
        }
        let mut raw = RawDisplayOperation::new(self.request.document_id);
        let turn = raw.first().map_err(raw_error)?;
        self.stage = RawStage::Raw(raw);
        self.raw_turn(turn)
    }

    fn raw_turn(&mut self, turn: Next<Option<MetadataRawView>>) -> ExportTurn {
        let raw = match embedded(turn) {
            Ok(raw) => raw.ok_or(MetadataApiError::NotFound)?,
            Err(pending) => return Ok(pending),
        };
        let record = self.record.take().ok_or(MetadataApiError::NotFound)?;
        let dataset_digest = raw.revision.dataset_digest;
        Ok(Next::Done(ExportMetadataResult::Raw {
            record,
            raw,
            dataset_digest,
        }))
    }
}

fn raw_error(error: RawReadError) -> MetadataApiError {
    match error {
        RawReadError::LimitExceeded(_) => MetadataApiError::ServiceUnavailable,
        error => MetadataApiError::Internal(error.to_string()),
    }
}

impl Operation for RawExportOperation {
    type Output = ExportMetadataResult;
    type Error = MetadataApiError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    /// The embedded read check times the caller's permission on the runner's clock.
    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        if let RawStage::Readable(readable) = &mut self.stage {
            readable.stamp(now_ms);
        }
    }

    fn first(&mut self) -> ExportTurn {
        let RawStage::Record(record) = &mut self.stage else {
            return Err(MetadataApiError::Internal(
                "the raw export was started".into(),
            ));
        };
        let turn = record.first()?;
        self.record_turn(turn)
    }

    fn next(&mut self, result: IoResult) -> ExportTurn {
        match &mut self.stage {
            RawStage::Record(record) => {
                let turn = record.next(result)?;
                self.record_turn(turn)
            }
            RawStage::Readable(readable) => {
                let turn = readable.next(result)?;
                self.readable_turn(turn)
            }
            RawStage::Raw(raw) => {
                let turn = raw.next(result).map_err(raw_error)?;
                self.raw_turn(turn)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ExportStage {
    Raw,
    Record,
    Readable,
    Materialized,
    Export,
    /// A full export reads the record again, so a revision that landed meanwhile is not
    /// labeled with the older cursor.
    Recheck,
    Rematerialized,
}

/// Exports one document in the asked view after the caller's read check: the full or summary
/// RO-Crate, one page of it, or its raw revision.
#[derive(Debug, PartialEq)]
pub struct ExportOperation {
    realm_id: RealmId,
    request: ExportMetadataRequest,
    stage: ExportStage,
    record: Option<MetadataRegistryRecord>,
    jsonld: Option<String>,
}

impl ExportOperation {
    pub fn new(realm_id: RealmId, request: ExportMetadataRequest) -> Self {
        Self {
            realm_id,
            request,
            stage: ExportStage::Record,
            record: None,
            jsonld: None,
        }
    }

    fn record(&self) -> Result<&MetadataRegistryRecord, MetadataApiError> {
        self.record.as_ref().ok_or(MetadataApiError::NotFound)
    }

    fn export(&mut self) -> ExportTurn {
        let record = self.record()?;
        let graph_iri = record.graph_iri.clone();
        let effect = match self.request.view {
            RoCrateExportView::Full => MetadataEffect::ExportRoCrate { graph_iri },
            RoCrateExportView::Summary => MetadataEffect::ExportSummaryAt {
                graph_iri,
                cursor: record.last_event_id,
            },
            RoCrateExportView::Page => {
                if self.request.offset.is_some() && self.request.after.is_some() {
                    return Err(MetadataApiError::BadRequest);
                }
                MetadataEffect::ExportRoCratePage {
                    graph_iri,
                    limit: self
                        .request
                        .limit
                        .unwrap_or(PAGE_DEFAULT)
                        .clamp(1, PAGE_MAX),
                    offset: self.request.offset,
                    after: self.request.after.clone(),
                }
            }
            RoCrateExportView::Raw => {
                return Err(MetadataApiError::Internal(
                    "raw export snapshot dispatch mismatch".to_string(),
                ));
            }
        };
        self.stage = ExportStage::Export;
        Ok(Next::Call(graph_effect(effect)))
    }

    fn exported(&mut self, result: IoResult) -> ExportTurn {
        let event = exported(result)?;
        let record = self.record.take().ok_or(MetadataApiError::NotFound)?;
        match event {
            MetadataEvent::RoCrateExportResult { jsonld, .. } => {
                self.jsonld = Some(jsonld);
                self.record = Some(record);
                self.stage = ExportStage::Recheck;
                Ok(Next::Call(IoCall::child(LiveRecordOperation::new(
                    self.request.document_id,
                ))))
            }
            MetadataEvent::RoCrateSummaryResult { jsonld, .. } => {
                Ok(Next::Done(ExportMetadataResult::Summary { jsonld, record }))
            }
            MetadataEvent::RoCratePageResult { page, .. } => {
                Ok(Next::Done(ExportMetadataResult::Page { page, record }))
            }
            other => Err(MetadataApiError::Internal(format!(
                "unexpected metadata export event: {}",
                other.label()
            ))),
        }
    }
}

impl Operation for ExportOperation {
    type Output = ExportMetadataResult;
    type Error = MetadataApiError;

    fn first(&mut self) -> ExportTurn {
        if self.request.view == RoCrateExportView::Raw {
            self.stage = ExportStage::Raw;
            let raw = RawExportOperation::new(self.realm_id, self.request.clone());
            return Ok(Next::Call(IoCall::child(raw)));
        }
        Ok(Next::Call(IoCall::child(LiveRecordOperation::new(
            self.request.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> ExportTurn {
        match self.stage {
            ExportStage::Raw => Ok(Next::Done(
                result.child::<ExportMetadataResult, MetadataApiError>()??,
            )),
            ExportStage::Record => {
                let record = live(result)?;
                let readable = RecordReadableOperation::new(
                    self.realm_id,
                    self.request.auth.clone(),
                    record.clone(),
                );
                self.record = Some(record);
                self.stage = ExportStage::Readable;
                Ok(Next::Call(IoCall::child(readable)))
            }
            ExportStage::Readable => {
                result.child::<(), MetadataApiError>()??;
                self.stage = ExportStage::Materialized;
                Ok(Next::Call(IoCall::child(RecordMaterializedOperation::new(
                    self.record()?,
                ))))
            }
            ExportStage::Materialized => {
                materialized(result)?;
                self.export()
            }
            ExportStage::Export => self.exported(result),
            ExportStage::Recheck => {
                let current = live(result)?;
                if current.last_event_id != self.record()?.last_event_id {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
                self.stage = ExportStage::Rematerialized;
                Ok(Next::Call(IoCall::child(RecordMaterializedOperation::new(
                    &current,
                ))))
            }
            ExportStage::Rematerialized => {
                materialized(result)?;
                let record = self.record.take().ok_or(MetadataApiError::NotFound)?;
                let jsonld = self.jsonld.take().unwrap_or_default();
                Ok(Next::Done(ExportMetadataResult::Full { jsonld, record }))
            }
        }
    }
}

/// The summary of a graph at a document cursor. Run it only after the caller's read check
/// accepted that document, since the cached summary carries no authorization data.
#[derive(Debug, PartialEq)]
pub(crate) struct SummaryOperation {
    graph_iri: String,
    cursor: Ulid,
}

impl SummaryOperation {
    pub(crate) fn new(graph_iri: &str, cursor: Ulid) -> Self {
        Self {
            graph_iri: graph_iri.to_string(),
            cursor,
        }
    }
}

impl Operation for SummaryOperation {
    type Output = String;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<String>, MetadataApiError> {
        Ok(Next::Call(graph_effect(MetadataEffect::ExportSummaryAt {
            graph_iri: self.graph_iri.clone(),
            cursor: self.cursor,
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, MetadataApiError> {
        match exported(result)? {
            MetadataEvent::RoCrateSummaryResult { jsonld, .. } => Ok(Next::Done(jsonld)),
            other => Err(MetadataApiError::Internal(format!(
                "unexpected metadata summary event: {}",
                other.label()
            ))),
        }
    }
}

#[cfg(test)]
mod call_tests {
    //! Drives the export operations through their children and graph answers without I/O.

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
    fn summary_exports_cursor() {
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
    fn page_refuses_cursors() {
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
    fn raw_reads_snapshot() {
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
}
