//! Reads stored repository links and plans the push checks a dataset change queues.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    DOCUMENT_INDEX_KEYSPACE, LINK_QUEUE_KEYSPACE, REPOSITORY_LINK_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::repository::{LinkQueueEntry, LinkStatus, RepositoryLink, link_prefix};
use aruna_core::rows::{PrefixPages, all_values, value};
use aruna_core::storage_entries::metadata_document_key;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Key, Value};
use serde::de::DeserializeOwned;
use ulid::Ulid;

use super::links::{LinkError, id_key, queue_row};

const LINK_PAGE: usize = 256;

type Rows = Vec<(String, Key, Value)>;

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, LinkError> {
    Ok(postcard::from_bytes(bytes).map_err(ConversionError::from)?)
}

/// The queue row reads of `link_ids`, one per link.
fn queue_reads(link_ids: impl IntoIterator<Item = Ulid>) -> IoCall {
    let reads = link_ids
        .into_iter()
        .map(|link_id| (LINK_QUEUE_KEYSPACE.to_string(), id_key(link_id)))
        .collect();
    IoCall::Read(reads)
}

/// Reads one stored row and decodes it; a missing row is `None`.
#[derive(Debug, PartialEq)]
pub(crate) struct StoredRowOperation<T> {
    read: (String, Key),
    row: PhantomData<fn() -> T>,
}

impl<T> StoredRowOperation<T> {
    pub(crate) fn new(space: &str, key: impl Into<Key>) -> Self {
        Self {
            read: (space.to_string(), key.into()),
            row: PhantomData,
        }
    }
}

impl<T: DeserializeOwned + Send + Debug + PartialEq> Operation for StoredRowOperation<T> {
    type Output = Option<T>;
    type Error = LinkError;

    fn first(&mut self) -> Result<Next<Option<T>>, LinkError> {
        Ok(Next::Call(IoCall::Read(vec![self.read.clone()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<T>>, LinkError> {
        let row = value(result)?.map(|bytes| decode(&bytes)).transpose()?;
        Ok(Next::Done(row))
    }
}

/// Reads every link of one document, page by page.
#[derive(Debug, PartialEq)]
pub(crate) struct DocumentLinksOperation {
    pages: PrefixPages,
    links: Vec<RepositoryLink>,
}

impl DocumentLinksOperation {
    pub(crate) fn new(document_id: Ulid) -> Self {
        Self {
            pages: PrefixPages::new(
                REPOSITORY_LINK_KEYSPACE,
                link_prefix(document_id),
                LINK_PAGE,
            ),
            links: Vec::new(),
        }
    }
}

impl Operation for DocumentLinksOperation {
    type Output = Vec<RepositoryLink>;
    type Error = LinkError;

    fn first(&mut self) -> Result<Next<Vec<RepositoryLink>>, LinkError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<RepositoryLink>>, LinkError> {
        for (_, row) in self.pages.take(result)? {
            self.links.push(RepositoryLink::from_bytes(&row)?);
        }
        match self.pages.done() {
            true => Ok(Next::Done(std::mem::take(&mut self.links))),
            false => Ok(Next::Call(self.pages.call())),
        }
    }
}

/// The finished links of an embedded links read, or the turn that passes its call on.
fn links_turn<T>(turn: Next<Vec<RepositoryLink>>) -> Result<Vec<RepositoryLink>, Next<T>> {
    match turn {
        Next::Done(links) | Next::Then { output: links, .. } => Ok(links),
        Next::Call(call) => Err(Next::Call(call)),
        Next::Wait => Err(Next::Wait),
    }
}

/// The document's links with whether each still has a push check queued.
#[derive(Debug, PartialEq)]
pub(crate) struct ListLinksOperation {
    links: DocumentLinksOperation,
    found: Vec<RepositoryLink>,
}

impl ListLinksOperation {
    pub(crate) fn new(document_id: Ulid) -> Self {
        Self {
            links: DocumentLinksOperation::new(document_id),
            found: Vec::new(),
        }
    }

    fn links_read(&mut self, turn: Next<Vec<RepositoryLink>>) -> ListTurn {
        let links = match links_turn(turn) {
            Ok(links) => links,
            Err(pending) => return Ok(pending),
        };
        if links.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        let reads = queue_reads(links.iter().map(|link| link.link_id));
        self.found = links;
        Ok(Next::Call(reads))
    }
}

type ListTurn = Result<Next<Vec<(RepositoryLink, bool)>>, LinkError>;

impl Operation for ListLinksOperation {
    type Output = Vec<(RepositoryLink, bool)>;
    type Error = LinkError;

    fn first(&mut self) -> ListTurn {
        let turn = self.links.first()?;
        self.links_read(turn)
    }

    fn next(&mut self, result: IoResult) -> ListTurn {
        if self.found.is_empty() {
            let turn = self.links.next(result)?;
            return self.links_read(turn);
        }
        let queued = all_values(result)?;
        if queued.len() != self.found.len() {
            return Err(LinkError::Unexpected("a short queue read".to_string()));
        }
        let links = std::mem::take(&mut self.found);
        let listed = links.into_iter().zip(queued);
        Ok(Next::Done(
            listed.map(|(link, row)| (link, row.is_some())).collect(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CheckStage {
    /// Reading the links of the current document.
    Links,
    /// Reading the current document's registry row to see whether it was deleted.
    Registry,
    /// Reading the queued checks of the candidate links.
    Queue,
}

/// Push-check rows for the push links of changed documents that push or retry after unmet
/// requirements; each change moves the due time. A deleted document queues all its links,
/// so the check removes them.
#[derive(Debug, PartialEq)]
pub(crate) struct LinkChecksOperation {
    documents: VecDeque<Ulid>,
    now_ms: u64,
    stage: CheckStage,
    /// The document whose links are read, and its links reader while it pages.
    current: Option<Ulid>,
    reader: Option<DocumentLinksOperation>,
    links: Vec<RepositoryLink>,
    candidates: Vec<(Ulid, Ulid)>,
}

type ChecksTurn = Result<Next<Rows>, LinkError>;

impl LinkChecksOperation {
    pub(crate) fn new(documents: impl IntoIterator<Item = Ulid>, now_ms: u64) -> Self {
        Self {
            documents: documents.into_iter().collect(),
            now_ms,
            stage: CheckStage::Links,
            current: None,
            reader: None,
            links: Vec::new(),
            candidates: Vec::new(),
        }
    }

    /// Starts the links read of the next document, or reads the queued checks once all are read.
    fn next_document(&mut self) -> ChecksTurn {
        let Some(document_id) = self.documents.pop_front() else {
            return self.read_queue();
        };
        let mut reader = DocumentLinksOperation::new(document_id);
        let turn = reader.first()?;
        self.current = Some(document_id);
        self.reader = Some(reader);
        self.links_read(turn)
    }

    fn links_read(&mut self, turn: Next<Vec<RepositoryLink>>) -> ChecksTurn {
        let links = match links_turn(turn) {
            Ok(links) => links,
            Err(pending) => return Ok(pending),
        };
        self.reader = None;
        let Some(document_id) = self.current else {
            return Err(LinkError::Unexpected(
                "a links read without a document".to_string(),
            ));
        };
        if links.is_empty() {
            return self.next_document();
        }
        self.links = links;
        self.stage = CheckStage::Registry;
        let key = metadata_document_key(document_id);
        Ok(Next::Call(IoCall::Read(vec![(
            DOCUMENT_INDEX_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn registry_read(&mut self, result: IoResult) -> ChecksTurn {
        let record = value(result)?
            .map(|bytes| decode::<MetadataRegistryRecord>(&bytes))
            .transpose()?;
        let Some(document_id) = self.current else {
            return Err(LinkError::Unexpected(
                "a registry read without a document".to_string(),
            ));
        };
        let gone = record.is_none();
        for link in std::mem::take(&mut self.links) {
            let pushes = link.status == LinkStatus::Enabled || link.retries_on_change();
            if gone || (pushes && link.pull().is_none()) {
                self.candidates.push((link.link_id, document_id));
            }
        }
        self.stage = CheckStage::Links;
        self.next_document()
    }

    fn read_queue(&mut self) -> ChecksTurn {
        if self.candidates.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        self.stage = CheckStage::Queue;
        Ok(Next::Call(queue_reads(
            self.candidates.iter().map(|(link_id, _)| *link_id),
        )))
    }

    fn queue_read(&mut self, result: IoResult) -> ChecksTurn {
        let queued = all_values(result)?;
        if queued.len() != self.candidates.len() {
            return Err(LinkError::Unexpected("a short queue read".to_string()));
        }
        let mut rows = Vec::with_capacity(queued.len());
        for ((link_id, document_id), existing) in
            std::mem::take(&mut self.candidates).into_iter().zip(queued)
        {
            let existing = existing.and_then(|bytes| postcard::from_bytes(&bytes).ok());
            let entry = LinkQueueEntry::debounce(document_id, existing.as_ref(), self.now_ms);
            rows.push(queue_row(link_id, &entry)?);
        }
        Ok(Next::Done(rows))
    }
}

impl Operation for LinkChecksOperation {
    type Output = Rows;
    type Error = LinkError;

    fn first(&mut self) -> ChecksTurn {
        self.next_document()
    }

    fn next(&mut self, result: IoResult) -> ChecksTurn {
        match self.stage {
            CheckStage::Links => {
                let Some(reader) = self.reader.as_mut() else {
                    return Err(LinkError::Unexpected(
                        "a links page without a document".into(),
                    ));
                };
                let turn = reader.next(result)?;
                self.links_read(turn)
            }
            CheckStage::Registry => self.registry_read(result),
            CheckStage::Queue => self.queue_read(result),
        }
    }
}

/// Arms the link drain for the earliest queued check; the queue itself is the durable state.
#[derive(Debug, PartialEq)]
pub(crate) struct LinkTimerOperation {
    pages: PrefixPages,
    next_due: Option<u64>,
    now_ms: u64,
    waking: bool,
}

impl LinkTimerOperation {
    pub(crate) fn new() -> Self {
        Self {
            pages: PrefixPages::new(LINK_QUEUE_KEYSPACE, Key::from(Vec::new()), LINK_PAGE),
            next_due: None,
            now_ms: 0,
            waking: false,
        }
    }
}

impl Operation for LinkTimerOperation {
    type Output = ();
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        if self.next_due.is_none() && !self.waking {
            self.now_ms = now_ms;
        }
    }

    fn first(&mut self) -> Result<Next<()>, LinkError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, LinkError> {
        if self.waking {
            if let IoResult::Event(event) = result
                && let aruna_core::events::Event::Task(aruna_core::task::TaskEvent::Error {
                    message,
                    ..
                }) = *event
            {
                tracing::warn!(%message, "Failed to arm the repository link queue");
            }
            return Ok(Next::Done(()));
        }
        for (_, value) in self.pages.take(result)? {
            let due = postcard::from_bytes::<LinkQueueEntry>(&value)
                .map_or(self.now_ms, |entry| entry.due_at_ms);
            self.next_due = crate::tasks::queue_backoff::min_due_at(self.next_due, due);
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        let Some(due) = self.next_due else {
            return Ok(Next::Done(()));
        };
        self.waking = true;
        let after = crate::tasks::queue_backoff::due_after(self.now_ms, due);
        Ok(Next::Call(IoCall::Effect(Box::new(
            super::links::schedule_drain(after),
        ))))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum DeletedStage {
    Checks,
    Write,
    Timer,
}

/// Queues checks for the links of the deleted document behind `graph_iri`; each check then
/// removes its link and token.
#[derive(Debug, PartialEq)]
pub(crate) struct QueueDeletedOperation {
    document_id: Option<Ulid>,
    stage: DeletedStage,
    now_ms: u64,
}

impl QueueDeletedOperation {
    pub(crate) fn new(graph_iri: &str) -> Self {
        let document_id = graph_iri
            .rsplit('/')
            .next()
            .and_then(|id| Ulid::from_string(id).ok())
            .filter(|id| MetadataRegistryRecord::graph_iri_for(*id) == graph_iri);
        Self {
            document_id,
            stage: DeletedStage::Checks,
            now_ms: 0,
        }
    }
}

impl Operation for QueueDeletedOperation {
    type Output = ();
    type Error = LinkError;

    fn first(&mut self) -> Result<Next<()>, LinkError> {
        let Some(document_id) = self.document_id else {
            return Ok(Next::Done(()));
        };
        Ok(Next::Call(IoCall::child(LinkChecksOperation::new(
            [document_id],
            self.now_ms,
        ))))
    }

    fn stamp(&mut self, now_ms: u64) {
        if self.stage == DeletedStage::Checks {
            self.now_ms = now_ms;
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, LinkError> {
        match self.stage {
            DeletedStage::Checks => {
                let rows = result.child::<Rows, LinkError>()??;
                if rows.is_empty() {
                    return Ok(Next::Done(()));
                }
                self.stage = DeletedStage::Write;
                Ok(Next::Call(IoCall::Write(rows)))
            }
            DeletedStage::Write => {
                aruna_core::rows::done(result)?;
                self.stage = DeletedStage::Timer;
                Ok(Next::Call(IoCall::child(LinkTimerOperation::new())))
            }
            DeletedStage::Timer => {
                result.child::<(), LinkError>()??;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::calls::IoError;
    use aruna_core::repository::LinkFailure;
    use aruna_core::structs::identity::realm::RealmId;

    use crate::jobs::repository::links::tests::link;

    const NOW_MS: u64 = 1_700_000_000_000;

    fn link_row(link: &RepositoryLink) -> (Key, Value) {
        let key = aruna_core::repository::link_key(link.document_id, link.link_id);
        (key.into(), link.to_bytes().unwrap().into())
    }

    fn links_page(document_id: Ulid, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: REPOSITORY_LINK_KEYSPACE.to_string(),
            prefix: link_prefix(document_id).into(),
            after,
            limit: LINK_PAGE,
        }
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    fn registry_read(document_id: Ulid) -> IoCall {
        let key = metadata_document_key(document_id);
        IoCall::Read(vec![(DOCUMENT_INDEX_KEYSPACE.to_string(), key)])
    }

    fn record(document_id: Ulid) -> Option<Value> {
        let realm_id = RealmId::from_bytes([3; 32]);
        let record = MetadataRegistryRecord {
            realm_id,
            group_id: Ulid::from_bytes([4; 16]),
            document_id,
            document_path: "datasets/linked".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: aruna_core::structs::placement::record::PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(1, 1),
            last_event_id: Ulid::from_parts(1, 1),
        };
        Some(postcard::to_allocvec(&record).unwrap().into())
    }

    #[test]
    fn row_read_decodes() {
        let stored = link();
        let key = aruna_core::repository::link_key(stored.document_id, stored.link_id);
        let mut read =
            StoredRowOperation::<RepositoryLink>::new(REPOSITORY_LINK_KEYSPACE, key.clone());
        assert_eq!(
            read.first(),
            Ok(Next::Call(IoCall::Read(vec![(
                REPOSITORY_LINK_KEYSPACE.to_string(),
                key.into()
            )])))
        );
        let row = Some(stored.to_bytes().unwrap().into());
        assert_eq!(
            read.next(IoResult::Values(vec![row])),
            Ok(Next::Done(Some(stored)))
        );
        assert_eq!(
            read.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
        assert!(matches!(
            read.next(IoResult::Values(vec![Some(vec![0xff].into())])),
            Err(LinkError::Conversion(_))
        ));
        assert_eq!(
            read.next(IoResult::Done),
            Err(LinkError::from(IoError::Unexpected {
                expected: "IoResult::Values",
                got: IoResult::Done.label(),
            }))
        );
    }

    #[test]
    fn links_read_pages() {
        let first = link();
        let mut second = link();
        second.link_id = Ulid::from_bytes([9; 16]);
        let mut read = DocumentLinksOperation::new(first.document_id);
        assert_eq!(
            read.first(),
            Ok(Next::Call(links_page(first.document_id, None)))
        );
        let (cursor, _) = link_row(&first);
        assert_eq!(
            read.next(page(vec![link_row(&first)], Some(cursor.clone()))),
            Ok(Next::Call(links_page(first.document_id, Some(cursor))))
        );
        assert_eq!(
            read.next(page(vec![link_row(&second)], None)),
            Ok(Next::Done(vec![first, second]))
        );
    }

    #[test]
    fn list_marks_queued() {
        let first = link();
        let mut second = link();
        second.link_id = Ulid::from_bytes([9; 16]);
        let mut list = ListLinksOperation::new(first.document_id);
        list.first().unwrap();
        assert_eq!(
            list.next(page(vec![link_row(&first), link_row(&second)], None)),
            Ok(Next::Call(queue_reads([first.link_id, second.link_id])))
        );
        let queued = IoResult::Values(vec![Some(vec![1].into()), None]);
        assert_eq!(
            list.next(queued),
            Ok(Next::Done(vec![(first, true), (second, false)]))
        );

        let mut empty = ListLinksOperation::new(Ulid::from_bytes([8; 16]));
        empty.first().unwrap();
        assert_eq!(
            empty.next(page(Vec::new(), None)),
            Ok(Next::Done(Vec::new()))
        );
    }

    #[test]
    fn checks_push_links() {
        // A live document queues its pushing links and leaves a paused one alone.
        let pushing = link();
        let mut paused = link();
        paused.link_id = Ulid::from_bytes([9; 16]);
        paused.status = LinkStatus::Paused;
        let document_id = pushing.document_id;
        let mut checks = LinkChecksOperation::new([document_id], NOW_MS);
        assert_eq!(
            checks.first(),
            Ok(Next::Call(links_page(document_id, None)))
        );
        assert_eq!(
            checks.next(page(vec![link_row(&pushing), link_row(&paused)], None)),
            Ok(Next::Call(registry_read(document_id)))
        );
        assert_eq!(
            checks.next(IoResult::Values(vec![record(document_id)])),
            Ok(Next::Call(queue_reads([pushing.link_id])))
        );
        let earlier = LinkQueueEntry::debounce(document_id, None, NOW_MS - 1_000);
        let queued = Some(postcard::to_allocvec(&earlier).unwrap().into());
        let entry = LinkQueueEntry::debounce(document_id, Some(&earlier), NOW_MS);
        assert_eq!(
            checks.next(IoResult::Values(vec![queued])),
            Ok(Next::Done(vec![
                queue_row(pushing.link_id, &entry).unwrap()
            ]))
        );
    }

    #[test]
    fn deleted_queues_all() {
        // A deleted document queues every link, so each check removes it.
        let mut failed = link();
        failed.status = LinkStatus::Failed {
            reason: LinkFailure::TokenRejected,
        };
        let document_id = failed.document_id;
        let mut checks = LinkChecksOperation::new([document_id], NOW_MS);
        checks.first().unwrap();
        checks.next(page(vec![link_row(&failed)], None)).unwrap();
        assert_eq!(
            checks.next(IoResult::Values(vec![None])),
            Ok(Next::Call(queue_reads([failed.link_id])))
        );
        let entry = LinkQueueEntry::debounce(document_id, None, NOW_MS);
        assert_eq!(
            checks.next(IoResult::Values(vec![None])),
            Ok(Next::Done(vec![queue_row(failed.link_id, &entry).unwrap()]))
        );
    }

    #[test]
    fn unlinked_documents_skip() {
        // Documents without links read no registry row and queue nothing.
        let first = Ulid::from_bytes([10; 16]);
        let second = Ulid::from_bytes([11; 16]);
        let mut checks = LinkChecksOperation::new([first, second], NOW_MS);
        assert_eq!(checks.first(), Ok(Next::Call(links_page(first, None))));
        assert_eq!(
            checks.next(page(Vec::new(), None)),
            Ok(Next::Call(links_page(second, None)))
        );
        assert_eq!(
            checks.next(page(Vec::new(), None)),
            Ok(Next::Done(Vec::new()))
        );
        assert_eq!(
            LinkChecksOperation::new([], NOW_MS).first(),
            Ok(Next::Done(Vec::new()))
        );
    }

    #[test]
    fn short_reads_fail() {
        let pushing = link();
        let mut checks = LinkChecksOperation::new([pushing.document_id], NOW_MS);
        checks.first().unwrap();
        checks.next(page(vec![link_row(&pushing)], None)).unwrap();
        checks
            .next(IoResult::Values(vec![record(pushing.document_id)]))
            .unwrap();
        assert!(matches!(
            checks.next(IoResult::Values(Vec::new())),
            Err(LinkError::Unexpected(_))
        ));
    }
}
