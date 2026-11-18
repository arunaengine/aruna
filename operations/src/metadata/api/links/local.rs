//! Answers link lookups and set deletion previews from this node's postings, within one budget.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, IterStart, StorageEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, StorageEvent};
use aruna_core::keyspaces::{
    DATA_LINK_KEYSPACE, DEAD_LETTER_KEYSPACE, MATERIALIZATION_JOB_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::data::link::{
    DataLink, LinkPosting, LinkScope, posting_document, posting_key, set_versions_key,
    truncated_key, unverified_key,
};
use aruna_core::structs::storage::global_id::MAX_PREFIX_BYTES;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::types::{
    LINKS_MAX_LIMIT, LinkDocument, LinkHit, LinkNodePage, LinkNodeRequest, LinkQuery,
    SetPreviewPage,
};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::api::title::DocumentTitleOperation;
use crate::metadata::call_results::page_rows;
use crate::metadata::data_links::LinkBudget;
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};

const NODE_SEEKS: usize = 1100;
const NODE_ROWS: usize = 4096;
/// Materialization and link states are counted up to this many each.
const UNSETTLED_CAP: usize = 1024;
/// Documents described at the same time.
const DESCRIBE_LIMIT: usize = 8;

type Postings = Vec<(DataLink, LinkPosting)>;
type Rows = Vec<(Key, Value)>;

fn internal(error: impl ToString) -> MetadataApiError {
    MetadataApiError::Internal(error.to_string())
}

fn iter(prefix: Vec<u8>, start: Option<IterStart>, limit: usize, space: &str) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: space.to_string(),
        prefix: (!prefix.is_empty()).then(|| Key::from(prefix)),
        start,
        limit,
        txn_id: None,
    })
}

fn iter_call(prefix: Vec<u8>, start: Option<IterStart>, limit: usize) -> IoCall {
    IoCall::Effect(Box::new(iter(prefix, start, limit, DATA_LINK_KEYSPACE)))
}

fn child_member<O>(operation: O) -> Effect
where
    O: Operation + Send + 'static,
    O::Output: std::fmt::Debug + Send + PartialEq + 'static,
    O::Error: std::fmt::Debug + Send + PartialEq + 'static,
{
    Effect::SubOperation(boxed_suboperation(operation, |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

fn group(members: Vec<Effect>, limit: usize) -> IoCall {
    IoCall::Group(EffectGroup {
        members,
        limit,
        form: GroupForm::All { deadline: None },
    })
}

/// The rows of an iteration member of a group.
fn member_rows(member: GroupResult) -> Result<Rows, MetadataApiError> {
    match member {
        GroupResult::Done(event) => match *event {
            Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values),
            Event::Storage(StorageEvent::Error { error }) => Err(internal(error)),
            other => Err(internal(other.label())),
        },
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// What one matched document adds to a page.
#[derive(Debug, PartialEq)]
pub(crate) enum Described {
    /// Deleted, or only older postings of the document matched.
    Skipped,
    /// The caller may not read it.
    Hidden,
    Shown(LinkDocument),
}

#[derive(Debug, PartialEq)]
enum DescribeStage {
    Record,
    Readable(MetadataRegistryRecord),
    Title(MetadataRegistryRecord),
}

/// Describes one matched document: postings of an older document event are dropped, and an
/// unreadable document is only hidden.
#[derive(Debug, PartialEq)]
pub(crate) struct DescribeOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    document_id: Ulid,
    postings: Postings,
    stage: DescribeStage,
}

impl Operation for DescribeOperation {
    type Output = Described;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Described>, MetadataApiError> {
        Ok(Next::Call(IoCall::child(LiveRecordOperation::new(
            self.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Described>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, DescribeStage::Record) {
            DescribeStage::Record => {
                let record =
                    result.child::<Option<MetadataRegistryRecord>, LiveRecordsError>()??;
                let Some(record) = record else {
                    return Ok(Next::Done(Described::Skipped));
                };
                self.postings
                    .retain(|(_, posting)| posting.cursor == record.last_event_id);
                if self.postings.is_empty() {
                    return Ok(Next::Done(Described::Skipped));
                }
                let readable =
                    RecordReadableOperation::new(self.realm_id, self.auth.clone(), record.clone());
                self.stage = DescribeStage::Readable(record);
                Ok(Next::Call(IoCall::child(readable)))
            }
            DescribeStage::Readable(record) => {
                if result.child::<(), MetadataApiError>()?.is_err() {
                    return Ok(Next::Done(Described::Hidden));
                }
                let title = DocumentTitleOperation::new(&record);
                self.stage = DescribeStage::Title(record);
                Ok(Next::Call(IoCall::child(title)))
            }
            DescribeStage::Title(record) => {
                let title = result.child::<Option<String>, MetadataApiError>()??;
                let links = std::mem::take(&mut self.postings)
                    .into_iter()
                    .map(|(link, posting)| LinkHit {
                        id: link.to_id(),
                        part: posting.part,
                        subject_iris: posting.subjects,
                    })
                    .collect();
                Ok(Next::Done(Described::Shown(LinkDocument {
                    document_id: self.document_id,
                    group_id: record.group_id.to_string(),
                    document_path: record.document_path,
                    graph_iri: record.graph_iri,
                    title,
                    links,
                })))
            }
        }
    }
}

/// The checks of every matched document, at most [`DESCRIBE_LIMIT`] at a time.
fn describe_all(
    (realm_id, auth): (RealmId, &Option<AuthContext>),
    matches: &mut BTreeMap<Ulid, Postings>,
) -> (Vec<Ulid>, IoCall) {
    let documents = matches.keys().copied().collect();
    let members = std::mem::take(matches)
        .into_iter()
        .map(|(document_id, postings)| {
            child_member(DescribeOperation {
                realm_id,
                auth: auth.clone(),
                document_id,
                postings,
                stage: DescribeStage::Record,
            })
        })
        .collect();
    (documents, group(members, DESCRIBE_LIMIT))
}

/// Adds the answers of [`describe_all`] to `page`; `confirm` reports a hidden match.
fn described(
    documents: Vec<Ulid>,
    result: IoResult,
    confirm: Option<Ulid>,
    page: &mut LinkNodePage,
) -> Result<(), MetadataApiError> {
    let IoResult::Group(GroupEvent::Finished { results }) = result else {
        return Err(MetadataApiError::ServiceUnavailable);
    };
    for (document_id, member) in documents.into_iter().zip(results) {
        let GroupResult::Done(event) = member else {
            return Err(MetadataApiError::ServiceUnavailable);
        };
        let Event::Child(outcome) = *event else {
            return Err(MetadataApiError::ServiceUnavailable);
        };
        let described = outcome.take::<Described, MetadataApiError>();
        match described.ok_or(MetadataApiError::ServiceUnavailable)?? {
            Described::Skipped => {}
            Described::Hidden => {
                page.hidden = true;
                page.confirmed |= confirm == Some(document_id);
            }
            Described::Shown(document) => page.documents.push(document),
        }
    }
    Ok(())
}

/// The counts of queued and parked materializations and of truncated documents, and with
/// `unverified` the version links no member could verify yet.
fn status_call(unverified: bool) -> IoCall {
    let mut members = vec![
        iter(
            Vec::new(),
            None,
            UNSETTLED_CAP,
            MATERIALIZATION_JOB_KEYSPACE,
        ),
        iter(Vec::new(), None, UNSETTLED_CAP, DEAD_LETTER_KEYSPACE),
        iter(truncated_key(), None, UNSETTLED_CAP, DATA_LINK_KEYSPACE),
    ];
    if unverified {
        members.push(iter(
            unverified_key(),
            None,
            UNSETTLED_CAP,
            DATA_LINK_KEYSPACE,
        ));
    }
    let limit = members.len();
    group(members, limit)
}

/// Adds the counts of [`status_call`] to `page`; the unverified count comes back.
fn status(result: IoResult, page: &mut LinkNodePage) -> Result<usize, MetadataApiError> {
    let IoResult::Group(GroupEvent::Finished { results }) = result else {
        return Err(MetadataApiError::ServiceUnavailable);
    };
    let mut counts = results
        .into_iter()
        .map(|member| member_rows(member).map(|rows| rows.len()));
    let mut count = || counts.next().unwrap_or(Ok(0));
    page.pending_materializations = count()?;
    page.failed_materializations = count()?;
    page.truncated_documents = count()?;
    count()
}

/// A walk over the prefix lengths a scope's postings use, one seek per length found.
#[derive(Debug, PartialEq)]
struct PrefixWalk {
    scope: LinkScope,
    key: String,
    max: u16,
    next_length: u16,
    lengths: Vec<u16>,
}

impl PrefixWalk {
    fn new(scope: LinkScope, key: String) -> Self {
        let max = key.len().min(MAX_PREFIX_BYTES) as u16;
        Self {
            scope,
            key,
            max,
            next_length: 0,
            lengths: Vec::new(),
        }
    }

    /// The identifiers the walk found: one exact prefix of the key per length used.
    fn links(&self) -> impl Iterator<Item = DataLink> + '_ {
        (self.lengths.iter())
            .filter_map(|length| self.key.get(..usize::from(*length)))
            .map(|prefix| self.scope.link(prefix))
    }
}

#[derive(Debug, PartialEq)]
enum LinkStage {
    Prefixes,
    Postings,
    Describe(Vec<Ulid>),
    Status,
}

/// Answers a link lookup from this node's postings within one shared budget: the prefixes a
/// bucket uses, then each identifier's postings after the page start, then the readable
/// documents and the states that may leave postings missing or stale.
#[derive(Debug, PartialEq)]
pub struct LinksLocalOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    request: LinkNodeRequest,
    limit: usize,
    budget: LinkBudget,
    page: LinkNodePage,
    walks: VecDeque<PrefixWalk>,
    links: VecDeque<DataLink>,
    asked: Option<DataLink>,
    matches: BTreeMap<Ulid, Postings>,
    end: Option<Ulid>,
    stage: LinkStage,
}

impl LinksLocalOperation {
    pub fn new(realm_id: RealmId, auth: Option<AuthContext>, mut request: LinkNodeRequest) -> Self {
        let mut links = VecDeque::new();
        let mut walks = VecDeque::new();
        for query in std::mem::take(&mut request.queries) {
            match query {
                LinkQuery::Exact(link) => links.push_back(link),
                LinkQuery::KeyPrefixes { scope, key } => {
                    walks.push_back(PrefixWalk::new(scope, key))
                }
            }
        }
        Self {
            realm_id,
            auth,
            limit: request.limit.clamp(1, LINKS_MAX_LIMIT),
            request,
            budget: LinkBudget {
                seeks: NODE_SEEKS,
                rows: NODE_ROWS,
            },
            page: LinkNodePage::default(),
            walks,
            links,
            asked: None,
            matches: BTreeMap::new(),
            end: None,
            stage: LinkStage::Prefixes,
        }
    }

    fn seek_length(&mut self) -> Result<Next<LinkNodePage>, MetadataApiError> {
        let Some(walk) = self.walks.front() else {
            self.stage = LinkStage::Postings;
            return self.next_link();
        };
        if !self.budget.take_seek() {
            self.page.cut = true;
            return self.walked();
        }
        let start = IterStart::At(Key::from(walk.scope.length_start(walk.next_length)));
        Ok(Next::Call(iter_call(walk.scope.key(), Some(start), 1)))
    }

    fn walked(&mut self) -> Result<Next<LinkNodePage>, MetadataApiError> {
        if let Some(walk) = self.walks.pop_front() {
            self.links.extend(walk.links());
        }
        self.seek_length()
    }

    fn length_found(&mut self, rows: Rows) -> Result<Next<LinkNodePage>, MetadataApiError> {
        let Some(walk) = self.walks.front_mut() else {
            return self.seek_length();
        };
        let length = rows.first().and_then(|(key, _)| walk.scope.length_of(key));
        let Some(length) = length.filter(|length| *length <= walk.max) else {
            return self.walked();
        };
        walk.lengths.push(length);
        match length.checked_add(1) {
            Some(after) => {
                walk.next_length = after;
                self.seek_length()
            }
            None => self.walked(),
        }
    }

    fn next_link(&mut self) -> Result<Next<LinkNodePage>, MetadataApiError> {
        let Some(link) = self.links.pop_front() else {
            return self.describe();
        };
        if !self.budget.take_seek() {
            // An unread identifier may hold documents anywhere after the page start.
            self.page.cut = true;
            return self.next_link();
        }
        let link_key = link.key();
        let limit = self.limit.min(self.budget.rows).max(1);
        let start = (self.request.after)
            .map(|document_id| IterStart::After(Key::from(posting_key(&link_key, document_id))));
        self.asked = Some(link);
        Ok(Next::Call(iter_call(link_key, start, limit)))
    }

    fn postings_read(&mut self, result: IoResult) -> Result<Next<LinkNodePage>, MetadataApiError> {
        let link = self
            .asked
            .take()
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        let (rows, next) = page_rows(result).map_err(internal)?;
        self.budget.rows = self.budget.rows.saturating_sub(rows.len());
        let link_key = link.key();
        let mut last = None;
        for (key, value) in rows {
            // A longer identifier sharing this one as a byte prefix is no match.
            if key.len() != link_key.len() + 16 {
                continue;
            }
            let Some(document_id) = posting_document(&key) else {
                continue;
            };
            let posting = LinkPosting::from_bytes(&value).map_err(internal)?;
            last = Some(document_id);
            if self.request.parts_only && !posting.part {
                continue;
            }
            (self.matches.entry(document_id).or_default()).push((link.clone(), posting));
        }
        // Each identifier is read up to its last returned document; beyond the smallest such
        // end another identifier may still hold unread documents.
        if next.is_some() {
            match last {
                Some(last) => self.end = Some(self.end.map_or(last, |end| end.min(last))),
                None => self.page.cut = true,
            }
        }
        self.next_link()
    }

    fn describe(&mut self) -> Result<Next<LinkNodePage>, MetadataApiError> {
        self.page.cut |= self.budget.exhausted();
        if let Some((nth, _)) = self.matches.iter().nth(self.limit - 1)
            && self.matches.len() > self.limit
        {
            let nth = *nth;
            self.end = Some(self.end.map_or(nth, |end| end.min(nth)));
        }
        self.page.next = self.end;
        if let Some(end) = self.end {
            self.matches.retain(|document_id, _| *document_id <= end);
        }
        if self.matches.is_empty() {
            self.stage = LinkStage::Status;
            return Ok(Next::Call(status_call(false)));
        }
        let (documents, call) = describe_all((self.realm_id, &self.auth), &mut self.matches);
        self.stage = LinkStage::Describe(documents);
        Ok(Next::Call(call))
    }
}

impl Operation for LinksLocalOperation {
    type Output = LinkNodePage;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<LinkNodePage>, MetadataApiError> {
        self.seek_length()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LinkNodePage>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, LinkStage::Status) {
            LinkStage::Prefixes => {
                self.stage = LinkStage::Prefixes;
                let (rows, _) = page_rows(result).map_err(internal)?;
                self.length_found(rows)
            }
            LinkStage::Postings => {
                self.stage = LinkStage::Postings;
                self.postings_read(result)
            }
            LinkStage::Describe(documents) => {
                described(documents, result, self.request.confirm, &mut self.page)?;
                self.stage = LinkStage::Status;
                Ok(Next::Call(status_call(false)))
            }
            LinkStage::Status => {
                status(result, &mut self.page)?;
                Ok(Next::Done(std::mem::take(&mut self.page)))
            }
        }
    }
}

/// The version a set version posting names: the 16 bytes before its document id.
fn version_of(key: &[u8]) -> Option<Ulid> {
    let end = key.len().checked_sub(16)?;
    let bytes: [u8; 16] = key.get(end.checked_sub(16)?..end)?.try_into().ok()?;
    Some(Ulid::from_bytes(bytes))
}

#[derive(Debug, PartialEq)]
enum PreviewStage {
    Ranges,
    Describe(Vec<Ulid>),
    Status,
}

/// One page of the postings on this node that name a set, for its deletion preview: its prefix
/// links, then its verified versions, in key order after `after`, with the readable documents
/// and the states that leave the preview incomplete.
#[derive(Debug, PartialEq)]
pub struct SetPreviewOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    set_id: Ulid,
    scope: LinkScope,
    after: Option<Vec<u8>>,
    limit: usize,
    ranges: VecDeque<Vec<u8>>,
    rows: usize,
    last: Option<Vec<u8>>,
    matches: BTreeMap<Ulid, Postings>,
    preview: SetPreviewPage,
    stage: PreviewStage,
}

impl SetPreviewOperation {
    pub fn new(
        (realm_id, auth): (RealmId, Option<AuthContext>),
        set_id: Ulid,
        after: Option<Vec<u8>>,
        limit: usize,
    ) -> Self {
        let scope = LinkScope::Set { realm_id, set_id };
        Self {
            realm_id,
            auth,
            set_id,
            ranges: VecDeque::from([scope.key(), set_versions_key(set_id)]),
            scope,
            after,
            limit: limit.clamp(1, LINKS_MAX_LIMIT),
            rows: 0,
            last: None,
            matches: BTreeMap::new(),
            preview: SetPreviewPage::default(),
            stage: PreviewStage::Ranges,
        }
    }

    fn next_range(&mut self) -> Result<Next<SetPreviewPage>, MetadataApiError> {
        if self.rows >= self.limit {
            self.preview.next = self.last.clone();
            return self.describe();
        }
        let Some(range) = self.ranges.front().cloned() else {
            // A full page names its last key even at the end; the page after it is empty.
            self.preview.next = (self.rows >= self.limit)
                .then(|| self.last.clone())
                .flatten();
            return self.describe();
        };
        let start = match &self.after {
            Some(after) if after.starts_with(&range) => {
                Some(IterStart::After(Key::from(after.clone())))
            }
            Some(after) if after.as_slice() > range.as_slice() => {
                self.ranges.pop_front();
                return self.next_range();
            }
            _ => None,
        };
        Ok(Next::Call(iter_call(range, start, self.limit - self.rows)))
    }

    fn link_of(&self, key: &[u8]) -> DataLink {
        let scope_key = self.scope.key();
        let length = key
            .starts_with(&scope_key)
            .then(|| self.scope.length_of(key))
            .flatten();
        match length {
            Some(length) => {
                let start = scope_key.len() + 2;
                let prefix = (key.get(start..start + usize::from(length)))
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                    .unwrap_or_default();
                self.scope.link(&prefix)
            }
            None => DataLink::SetVersion {
                realm_id: self.realm_id,
                set_id: self.set_id,
                version_id: version_of(key).unwrap_or_default(),
            },
        }
    }

    fn range_read(&mut self, result: IoResult) -> Result<Next<SetPreviewPage>, MetadataApiError> {
        let (rows, next) = page_rows(result).map_err(internal)?;
        for (key, value) in rows {
            self.last = Some(key.to_vec());
            if let Some(document_id) = posting_document(&key) {
                let posting = LinkPosting::from_bytes(&value).map_err(internal)?;
                self.rows += 1;
                let link = self.link_of(&key);
                (self.matches.entry(document_id).or_default()).push((link, posting));
            }
        }
        if next.is_some() {
            self.preview.next = self.last.clone();
            return self.describe();
        }
        self.ranges.pop_front();
        self.next_range()
    }

    fn describe(&mut self) -> Result<Next<SetPreviewPage>, MetadataApiError> {
        self.ranges.clear();
        if self.matches.is_empty() {
            self.stage = PreviewStage::Status;
            return Ok(Next::Call(status_call(true)));
        }
        let (documents, call) = describe_all((self.realm_id, &self.auth), &mut self.matches);
        self.stage = PreviewStage::Describe(documents);
        Ok(Next::Call(call))
    }
}

impl Operation for SetPreviewOperation {
    type Output = SetPreviewPage;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<SetPreviewPage>, MetadataApiError> {
        self.next_range()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SetPreviewPage>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, PreviewStage::Status) {
            PreviewStage::Ranges => {
                self.stage = PreviewStage::Ranges;
                self.range_read(result)
            }
            PreviewStage::Describe(documents) => {
                described(documents, result, None, &mut self.preview.page)?;
                self.stage = PreviewStage::Status;
                Ok(Next::Call(status_call(true)))
            }
            PreviewStage::Status => {
                self.preview.unverified = status(result, &mut self.preview.page)?;
                Ok(Next::Done(std::mem::take(&mut self.preview)))
            }
        }
    }
}

#[cfg(test)]
#[path = "local_tests.rs"]
mod call_tests;
