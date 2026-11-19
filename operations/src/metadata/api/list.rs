//! Lists the metadata documents a caller may see, with paging limits and filters.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::keyspaces::METADATA_INDEX_KEYSPACE;
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, all_values, decode};
use aruna_core::storage_entries::metadata_registry_prefix;
use aruna_core::structs::identity::group::Group;
use aruna_core::types::Key;

use super::export::SummaryOperation;
use super::list_types::{
    ESTIMATE_MIN_LIMIT, ListVisibleRequest, ListVisibleResult, ListedMetadataDocument,
    MetadataListOrder, check_policy_limit, effective_list_limit, merge_pending_records,
};
use super::read_access::{ReadAccess, ReadAccessOperation, child_results, children, internal};
use super::{
    EVENT_LOG_KEYSPACE, Event, GroupId, HashMap, LIST_METADATA_SIZE, ListGroupOperation,
    MetadataApiError, MetadataEventRecord, MetadataRegistryRecord, PENDING_PROJECTION_KEYSPACE,
    REGISTRY_CANDIDATE_LIMIT, RealmId, event_log_key, pending_projection_target,
};
use crate::groups::list_groups::ListGroupError;
use crate::metadata::live_records::{LiveRecordsError, LiveRecordsOperation};
use crate::metadata::record_reads::RecordMaterializedOperation;
use crate::metadata::repository::StorageReadError;

type Records = Vec<MetadataRegistryRecord>;
type PendingRecords = HashMap<GroupId, Records>;

/// The live registry rows of one group, at most `limit`: the fresh copy of the registry cache
/// when it holds one, else every registry page of the group. Deleted documents are left out.
#[derive(Debug, PartialEq)]
pub(crate) struct GroupRecordsOperation {
    group_id: GroupId,
    limit: usize,
    pages: Option<PrefixPages>,
    records: Records,
    live: bool,
}

impl GroupRecordsOperation {
    pub(crate) fn new(group_id: GroupId, limit: usize) -> Self {
        Self {
            group_id,
            limit,
            pages: None,
            records: Vec::new(),
            live: false,
        }
    }

    fn live(&mut self) -> Next<Records> {
        self.live = true;
        let records = std::mem::take(&mut self.records);
        Next::Call(IoCall::child(LiveRecordsOperation::new(records, None)))
    }

    fn cached(&mut self, result: IoResult) -> Result<Next<Records>, MetadataApiError> {
        // Without a handle or a fresh copy the registry pages answer.
        if let IoResult::Event(event) = &result
            && let Event::Metadata(MetadataEvent::CachedGroup {
                records: Some(records),
            }) = event.as_ref()
            && records.len() <= self.limit
        {
            self.records = records.clone();
            return Ok(self.live());
        }
        let prefix = metadata_registry_prefix(self.group_id);
        let pages = PrefixPages::new(METADATA_INDEX_KEYSPACE, prefix, LIST_METADATA_SIZE);
        let call = pages.call();
        self.pages = Some(pages);
        Ok(Next::Call(call))
    }
}

impl Operation for GroupRecordsOperation {
    type Output = Records;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Records>, MetadataApiError> {
        let effect = MetadataEffect::CachedGroup {
            group_id: self.group_id,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            effect,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Records>, MetadataApiError> {
        if self.live {
            return Ok(Next::Done(result.child::<Records, LiveRecordsError>()??));
        }
        let Some(pages) = self.pages.as_mut() else {
            return self.cached(result);
        };
        let rows = pages.take(result)?;
        if self.records.len().saturating_add(rows.len()) > self.limit {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        for (_, value) in rows {
            self.records.push(decode(&value)?);
        }
        if !pages.done() {
            return Ok(Next::Call(pages.call()));
        }
        Ok(self.live())
    }
}

enum PendingStage {
    Markers,
    Events,
    Live,
}

/// The live documents whose create projection has not landed yet, by group, at most `limit`
/// of them: the pending markers, then their create events in one batch read.
#[derive(Debug, PartialEq)]
pub(crate) struct PendingRecordsOperation {
    group_filter: Option<GroupId>,
    limit: usize,
    markers: PrefixPages,
    scanned: usize,
    targets: Vec<(ulid::Ulid, ulid::Ulid)>,
    live: bool,
}

impl PendingRecordsOperation {
    pub(crate) fn new(group_filter: Option<GroupId>, limit: usize) -> Self {
        let space = PENDING_PROJECTION_KEYSPACE;
        Self {
            group_filter,
            limit: limit.min(REGISTRY_CANDIDATE_LIMIT),
            markers: PrefixPages::new(space, Key::from(Vec::new()), LIST_METADATA_SIZE),
            scanned: 0,
            targets: Vec::new(),
            live: false,
        }
    }

    fn stage(&self) -> PendingStage {
        match (self.live, self.markers.done()) {
            (true, _) => PendingStage::Live,
            (false, true) => PendingStage::Events,
            (false, false) => PendingStage::Markers,
        }
    }

    fn marked(&mut self, result: IoResult) -> Result<Next<PendingRecords>, MetadataApiError> {
        let rows = self.markers.take(result).map_err(internal)?;
        self.scanned = self.scanned.saturating_add(rows.len());
        if self.scanned > self.limit {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        let targets = rows
            .iter()
            .filter_map(|(key, _)| pending_projection_target(key));
        self.targets.extend(targets);
        if !self.markers.done() {
            return Ok(Next::Call(self.markers.call()));
        }
        if self.targets.is_empty() {
            return Ok(Next::Done(HashMap::new()));
        }
        let reads = (self.targets.iter())
            .map(|(document_id, event_id)| {
                let key = event_log_key(*document_id, *event_id);
                (EVENT_LOG_KEYSPACE.to_string(), key)
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn events(&mut self, result: IoResult) -> Result<Next<PendingRecords>, MetadataApiError> {
        let values = all_values(result).map_err(internal)?;
        if values.len() != self.targets.len() {
            return Err(internal(format!(
                "metadata pending event batch returned {} values for {} targets",
                values.len(),
                self.targets.len()
            )));
        }
        let mut pending = Vec::with_capacity(values.len());
        for ((document_id, event_id), value) in self.targets.iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            let event: MetadataEventRecord = postcard::from_bytes(&value).map_err(internal)?;
            if event.record.document_id != *document_id || event.event_id != *event_id {
                return Err(internal(format!(
                    "metadata create event log target {document_id}/{event_id} did not match payload {}/{}",
                    event.record.document_id, event.event_id
                )));
            }
            if (self.group_filter).is_none_or(|group_id| event.record.group_id == group_id) {
                pending.push(event.record);
            }
        }
        if pending.is_empty() {
            return Ok(Next::Done(HashMap::new()));
        }
        self.live = true;
        let live = LiveRecordsOperation::new(pending, None);
        Ok(Next::Call(IoCall::child(live)))
    }

    fn grouped(&self, result: IoResult) -> Result<Next<PendingRecords>, MetadataApiError> {
        let pending = result.child::<Records, LiveRecordsError>()??;
        if pending.len() > self.limit {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        let mut records: PendingRecords = HashMap::new();
        for record in pending {
            records.entry(record.group_id).or_default().push(record);
        }
        Ok(Next::Done(records))
    }
}

impl Operation for PendingRecordsOperation {
    type Output = PendingRecords;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<PendingRecords>, MetadataApiError> {
        Ok(Next::Call(self.markers.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PendingRecords>, MetadataApiError> {
        match self.stage() {
            PendingStage::Markers => self.marked(result),
            PendingStage::Events => self.events(result),
            PendingStage::Live => self.grouped(result),
        }
    }
}

/// The summary of one listed record, once its graph holds the record's accepted event.
#[derive(Debug, PartialEq)]
pub(crate) struct RecordSummaryOperation {
    record: MetadataRegistryRecord,
    checked: bool,
}

impl RecordSummaryOperation {
    pub(crate) fn new(record: MetadataRegistryRecord) -> Self {
        Self {
            record,
            checked: false,
        }
    }
}

impl Operation for RecordSummaryOperation {
    type Output = String;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<String>, MetadataApiError> {
        let check = RecordMaterializedOperation::new(&self.record);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, MetadataApiError> {
        if self.checked {
            return Ok(Next::Done(result.child::<String, MetadataApiError>()??));
        }
        // A pending graph cannot export content from before its accepted event.
        match result.child::<bool, StorageReadError>()? {
            Ok(true) => {}
            Ok(false) => return Err(MetadataApiError::ServiceUnavailable),
            Err(error) => return Err(internal(error)),
        }
        self.checked = true;
        let summary = SummaryOperation::new(&self.record.graph_iri, self.record.last_event_id);
        Ok(Next::Call(IoCall::child(summary)))
    }
}

#[derive(Debug, PartialEq)]
enum CandidateStage {
    Groups,
    Pending,
    Records,
}

/// Which registry rows a candidate scan keeps.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CandidateScan {
    /// Listed rows; with `pending`, creates whose projection has not landed join their group.
    Listing { pending: bool },
    /// Path claims of `realm_id`: every pending create joins, also of a group not listed, and
    /// each group's rows are in document order.
    Claims { realm_id: RealmId },
}

/// The registry rows of one group, or of every group, with the pending creates the scan asks
/// for; never more than [`REGISTRY_CANDIDATE_LIMIT`] of them.
#[derive(Debug, PartialEq)]
pub(crate) struct CandidatesOperation {
    group_id: Option<GroupId>,
    scan: CandidateScan,
    stage: CandidateStage,
    groups: VecDeque<GroupId>,
    pending: PendingRecords,
    records: Records,
}

impl CandidatesOperation {
    pub(crate) fn new(group_id: Option<GroupId>, scan: CandidateScan) -> Self {
        Self {
            group_id,
            scan,
            stage: CandidateStage::Groups,
            groups: VecDeque::new(),
            pending: HashMap::new(),
            records: Vec::new(),
        }
    }

    fn listed(&mut self, groups: Vec<GroupId>) -> Result<Next<Records>, MetadataApiError> {
        self.groups = check_policy_limit(groups)?.into();
        // The pending keyspace is scanned once per request, never once per group.
        let pending = match self.scan {
            CandidateScan::Listing { pending } => pending,
            CandidateScan::Claims { .. } => true,
        };
        if pending {
            self.stage = CandidateStage::Pending;
            let pending = PendingRecordsOperation::new(self.group_id, REGISTRY_CANDIDATE_LIMIT);
            return Ok(Next::Call(IoCall::child(pending)));
        }
        self.next_group()
    }

    fn next_group(&mut self) -> Result<Next<Records>, MetadataApiError> {
        let Some(group_id) = self.groups.front().copied() else {
            return self.finish();
        };
        self.stage = CandidateStage::Records;
        let remaining = REGISTRY_CANDIDATE_LIMIT.saturating_sub(self.records.len());
        let records = GroupRecordsOperation::new(group_id, remaining);
        Ok(Next::Call(IoCall::child(records)))
    }

    fn group_listed(&mut self, mut records: Records) -> Result<Next<Records>, MetadataApiError> {
        let group_id = (self.groups.pop_front()).ok_or_else(|| internal("no group listed"))?;
        let claims = matches!(self.scan, CandidateScan::Claims { .. });
        if let Some(pending) = self.pending.remove(&group_id) {
            let remaining = REGISTRY_CANDIDATE_LIMIT.saturating_sub(self.records.len());
            merge_pending_records(&mut records, pending);
            if records.len() > remaining {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            records.sort_by_key(|record| record.document_id);
        } else if claims {
            records.sort_by_key(|record| record.document_id);
        }
        self.records.extend(records);
        self.next_group()
    }

    fn finish(&mut self) -> Result<Next<Records>, MetadataApiError> {
        let mut records = std::mem::take(&mut self.records);
        if let CandidateScan::Claims { realm_id } = self.scan {
            for pending in std::mem::take(&mut self.pending).into_values() {
                if records.len().saturating_add(pending.len()) > REGISTRY_CANDIDATE_LIMIT {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
                records.extend(pending);
            }
            records.retain(|record| record.realm_id == realm_id);
        }
        Ok(Next::Done(records))
    }
}

impl Operation for CandidatesOperation {
    type Output = Records;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Records>, MetadataApiError> {
        match self.group_id {
            Some(group_id) => self.listed(vec![group_id]),
            None => {
                let groups = ListGroupOperation::with_pagination(REGISTRY_CANDIDATE_LIMIT + 1, 0);
                Ok(Next::Call(IoCall::child(groups)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Records>, MetadataApiError> {
        match self.stage {
            CandidateStage::Groups => {
                let groups = (result.child::<Vec<Group>, ListGroupError>()?).map_err(internal)?;
                self.listed(groups.into_iter().map(|group| group.group_id).collect())
            }
            CandidateStage::Pending => {
                self.pending = result.child::<PendingRecords, MetadataApiError>()??;
                self.next_group()
            }
            CandidateStage::Records => {
                let records = result.child::<Records, MetadataApiError>()??;
                self.group_listed(records)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ListStage {
    Candidates,
    Access,
    Summaries,
}

/// The page of documents the caller may see. It lists the candidate rows, orders them, checks
/// read rules and request policies, and pages the visible rows; summaries are read for the page
/// only.
#[derive(Debug, PartialEq)]
pub struct ListVisibleOperation {
    realm_id: RealmId,
    request: ListVisibleRequest,
    limit: usize,
    offset: usize,
    stage: ListStage,
    records: Records,
    selected: Records,
    total_estimate: Option<usize>,
}

impl ListVisibleOperation {
    pub fn new(realm_id: RealmId, request: ListVisibleRequest) -> Self {
        Self {
            realm_id,
            limit: effective_list_limit(request.limit, request.auth.is_none()),
            offset: request.offset.unwrap_or(0),
            request,
            stage: ListStage::Candidates,
            records: Vec::new(),
            selected: Vec::new(),
            total_estimate: None,
        }
    }

    fn recent(&self) -> bool {
        self.request.order == MetadataListOrder::Recent
    }

    /// The read rules and request policies of every listed group.
    fn access(&mut self) -> Result<Next<ListVisibleResult>, MetadataApiError> {
        // Ordering precedes both the estimate scan and the offset window so that pagination
        // and the early exit page the same sequence.
        if self.recent() {
            self.records.sort_by(|left, right| {
                (right.updated_at_ms.cmp(&left.updated_at_ms))
                    .then_with(|| right.document_id.cmp(&left.document_id))
            });
        }
        self.stage = ListStage::Access;
        let auth = self.request.auth.clone();
        let access = ReadAccessOperation::new(self.realm_id, auth, &self.records);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn select(&mut self, access: ReadAccess) -> Result<Next<ListVisibleResult>, MetadataApiError> {
        let path_prefix = self.request.path_prefix.as_deref();
        let shown = |record: &MetadataRegistryRecord| {
            record_matches_filters(record, path_prefix) && access.shows(record)
        };
        if self.limit >= ESTIMATE_MIN_LIMIT {
            self.total_estimate = Some(self.records.iter().filter(|record| shown(record)).count());
        }
        let needed = self.offset.saturating_add(self.limit);
        let mut selected = Vec::with_capacity(self.limit.min(self.records.len()));
        let mut visible_count = 0usize;
        for record in std::mem::take(&mut self.records) {
            if !shown(&record) {
                continue;
            }
            visible_count += 1;
            if visible_count > self.offset {
                selected.push(record);
                if visible_count >= needed {
                    break;
                }
            }
        }
        if !self.request.include_summary || selected.is_empty() {
            let documents = selected.into_iter().map(|record| ListedMetadataDocument {
                record,
                rocrate_summary_jsonld: None,
            });
            return Ok(Next::Done(self.result(documents.collect())));
        }
        let summaries = selected
            .iter()
            .cloned()
            .map(RecordSummaryOperation::new)
            .collect();
        self.selected = selected;
        self.stage = ListStage::Summaries;
        Ok(Next::Call(children(summaries)))
    }

    fn summarized(
        &mut self,
        result: IoResult,
    ) -> Result<Next<ListVisibleResult>, MetadataApiError> {
        let summaries = child_results::<String, MetadataApiError>(result)?;
        let mut documents = Vec::with_capacity(summaries.len());
        for (record, summary) in std::mem::take(&mut self.selected)
            .into_iter()
            .zip(summaries)
        {
            let rocrate_summary_jsonld = match summary {
                Some(Ok(summary)) => Some(summary),
                Some(Err(MetadataApiError::ServiceUnavailable)) => None,
                Some(Err(error)) => return Err(error),
                None => return Err(internal("metadata summary did not finish")),
            };
            documents.push(ListedMetadataDocument {
                record,
                rocrate_summary_jsonld,
            });
        }
        Ok(Next::Done(self.result(documents)))
    }

    fn result(&self, documents: Vec<ListedMetadataDocument>) -> ListVisibleResult {
        let total_returned = documents.len();
        ListVisibleResult {
            documents,
            limit: self.limit,
            offset: self.offset,
            total_returned,
            // Never report fewer than the page already discloses.
            total_estimate: (self.total_estimate).map(|estimate| estimate.max(total_returned)),
        }
    }
}

impl Operation for ListVisibleOperation {
    type Output = ListVisibleResult;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<ListVisibleResult>, MetadataApiError> {
        // Summary listings and recency listings must show documents whose projection has not
        // landed yet.
        let pending = self.request.include_summary || self.recent();
        let scan = CandidateScan::Listing { pending };
        let candidates = CandidatesOperation::new(self.request.group_id, scan);
        Ok(Next::Call(IoCall::child(candidates)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ListVisibleResult>, MetadataApiError> {
        match self.stage {
            ListStage::Candidates => {
                self.records = result.child::<Records, MetadataApiError>()??;
                self.access()
            }
            ListStage::Access => {
                let access = result.child::<ReadAccess, MetadataApiError>()??;
                self.select(access)
            }
            ListStage::Summaries => self.summarized(result),
        }
    }
}

pub(super) fn record_matches_filters(
    record: &MetadataRegistryRecord,
    path_prefix: Option<&str>,
) -> bool {
    path_prefix
        .map(|path_prefix| path_matches_prefix(&record.document_path, path_prefix))
        .unwrap_or(true)
}

fn path_matches_prefix(document_path: &str, path_prefix: &str) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(document_path);
    aruna_core::placement::resolver::path_prefix_match(&normalized_path, path_prefix).is_some()
}

#[cfg(test)]
mod call_tests {
    //! Drives the listing operations through their cache, page, child and group answers without I/O.

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
}
