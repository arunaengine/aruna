//! Reads and prunes this node's plugin event journal: kept rows for replay, ordered by journal
//! time (rows of one millisecond in no set order), and pending rows the matcher has not read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE;
use aruna_core::plugin_runs::{
    DEDUP_RETENTION_MS, JOURNAL_RETENTION_MS, JOURNAL_ROW, SOURCE_ROW, SourceEvent, journal_key,
};
use ulid::Ulid;

use super::rows::{Answer, Ask, Plan, RowsOperation, Scan, StoreError, Turn, done, unexpected};

/// Rows a journal page holds at most.
pub const JOURNAL_PAGE: usize = 500;

/// One journal row: its key and its event.
pub type JournalRow = (Vec<u8>, SourceEvent);

/// At most `limit` rows of `kind` after the key `after`, oldest first, and the key to continue
/// after when more may follow. A row that does not decode is skipped.
pub fn journal_page(kind: u8, after: Option<Vec<u8>>, limit: usize) -> RowsOperation<JournalPage> {
    RowsOperation::direct(JournalPage(Some(Ask::Page {
        space: EVENT_JOURNAL_KEYSPACE,
        prefix: vec![kind],
        after,
        limit,
    })))
}

#[derive(Debug, PartialEq)]
pub struct JournalPage(Option<Ask>);

impl Plan for JournalPage {
    type Output = (Vec<JournalRow>, Option<Vec<u8>>);
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        let page = self
            .0
            .take()
            .ok_or_else(|| StoreError("paged twice".into()))?;
        Ok(Turn::Ask(page))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        let Answer::Page(rows, next) = answer else {
            return Err(unexpected(answer));
        };
        let rows = (rows.into_iter())
            .filter_map(|(key, value)| Some((key, SourceEvent::from_bytes(&value).ok()?)))
            .collect();
        Ok(Turn::Finish((rows, next)))
    }
}

/// Deletes kept rows journaled longer than the retention ago, and source-id rows past the
/// dedup horizon, after which a source is not journaled again; returns how many rows.
pub(crate) fn prune_journal(now_ms: u64) -> RowsOperation<PruneJournal> {
    let kept_from = now_ms.saturating_sub(JOURNAL_RETENTION_MS);
    RowsOperation::direct(PruneJournal {
        now_ms,
        cutoff: journal_key(JOURNAL_ROW, kept_from, Ulid::nil()),
        phase: JournalPhase::Kept,
        pruned: 0,
    })
}

#[derive(Debug, PartialEq)]
pub struct PruneJournal {
    now_ms: u64,
    cutoff: Vec<u8>,
    phase: JournalPhase,
    pruned: usize,
}

#[derive(Debug, PartialEq)]
enum JournalPhase {
    /// Reads the oldest kept rows, deleted a page at a time.
    Kept,
    DeletingKept,
    Sources(Scan),
    DeletingSources,
}

impl PruneJournal {
    fn kept_page() -> Ask {
        Ask::Page {
            space: EVENT_JOURNAL_KEYSPACE,
            prefix: vec![JOURNAL_ROW],
            after: None,
            limit: JOURNAL_PAGE,
        }
    }
}

impl Plan for PruneJournal {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<usize>, StoreError> {
        Ok(Turn::Ask(Self::kept_page()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, JournalPhase::DeletingSources) {
            JournalPhase::Kept => {
                let Answer::Page(rows, _) = answer else {
                    return Err(unexpected(answer));
                };
                let old: Vec<_> = (rows.into_iter())
                    .map(|(key, _)| key)
                    .take_while(|key| *key < self.cutoff)
                    .map(|key| (EVENT_JOURNAL_KEYSPACE, key))
                    .collect();
                if old.is_empty() {
                    let scan = Scan::new(EVENT_JOURNAL_KEYSPACE, vec![SOURCE_ROW]);
                    let ask = scan.ask();
                    self.phase = JournalPhase::Sources(scan);
                    return Ok(Turn::Ask(ask));
                }
                self.pruned += old.len();
                self.phase = JournalPhase::DeletingKept;
                Ok(Turn::Ask(Ask::Delete(old)))
            }
            JournalPhase::DeletingKept => {
                done(answer)?;
                self.phase = JournalPhase::Kept;
                Ok(Turn::Ask(Self::kept_page()))
            }
            JournalPhase::Sources(mut scan) => {
                let Some(sources) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = JournalPhase::Sources(scan);
                    return Ok(Turn::Ask(ask));
                };
                let old: Vec<_> = (sources.into_iter())
                    .filter(|(_, value)| {
                        let anchor = value.as_slice().try_into().map(u64::from_be_bytes);
                        anchor.is_ok_and(|anchor| {
                            self.now_ms > anchor.saturating_add(DEDUP_RETENTION_MS)
                        })
                    })
                    .map(|(key, _)| (EVENT_JOURNAL_KEYSPACE, key))
                    .collect();
                self.pruned += old.len();
                Ok(Turn::Ask(Ask::Delete(old)))
            }
            JournalPhase::DeletingSources => {
                done(answer)?;
                Ok(Turn::Finish(self.pruned))
            }
        }
    }
}

/// The journal rows this node has not matched yet, at most a hundred.
#[cfg(test)]
pub(crate) async fn pending_events(ctx: &crate::driver::DriverContext) -> Vec<SourceEvent> {
    let page = journal_page(aruna_core::plugin_runs::PENDING_ROW, None, 100);
    let (rows, _) = crate::driver::drive(page, ctx).await.unwrap();
    rows.into_iter().map(|(_, event)| event).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::plugins::rows::{scan_rows, write_rows};
    use crate::plugins::runs::tests::opened;
    use aruna_core::plugin_runs::{SourceKind, journal_entries};

    fn event(journaled_ms: u64) -> SourceEvent {
        SourceEvent {
            kind: SourceKind::DatasetCreated,
            group_id: Ulid::from_parts(1, 1),
            path: "/r/g/x/meta/a@d".into(),
            resource: "d".into(),
            revision: "e".into(),
            source: format!("dataset_created/{journaled_ms}"),
            actor: None,
            anchor_ms: journaled_ms,
            journaled_ms,
        }
    }

    #[tokio::test]
    async fn retention_pruning() {
        let (_dir, ctx) = opened();
        for at in [10, 20, 10 + JOURNAL_RETENTION_MS] {
            let rows = journal_entries(&event(at)).unwrap();
            drive(write_rows(rows), &ctx).await.unwrap();
        }
        let now = 15 + JOURNAL_RETENTION_MS;
        assert_eq!(drive(prune_journal(now), &ctx).await.unwrap(), 1);
        let (kept, _) = drive(journal_page(JOURNAL_ROW, None, 10), &ctx)
            .await
            .unwrap();
        let times: Vec<u64> = kept.iter().map(|(_, row)| row.journaled_ms).collect();
        assert_eq!(times, [20, 10 + JOURNAL_RETENTION_MS]);
        // Pending rows are the matcher's; retention pruning leaves them.
        let (pending, _) = drive(journal_page(b'u', None, 10), &ctx).await.unwrap();
        assert_eq!(pending.len(), 3);
        // Source-id rows stay until the dedup horizon, so a late copy is not journaled again.
        let sources = async || {
            let scan = Scan::new(EVENT_JOURNAL_KEYSPACE, vec![SOURCE_ROW]);
            drive(scan_rows(scan), &ctx).await.unwrap()
        };
        assert_eq!(sources().await.len(), 3);
        let horizon = 25 + DEDUP_RETENTION_MS;
        assert_eq!(drive(prune_journal(horizon), &ctx).await.unwrap(), 3);
        assert_eq!(sources().await.len(), 1);
    }

    #[tokio::test]
    async fn local_create_once() {
        // The accepting node journals a creation in its batch; its projection adds nothing.
        use crate::metadata::create_document::{
            CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload,
            create_metadata_document,
        };
        let fixture = crate::plugins::install::tests::Fixture::new().await;
        let mut ctx = fixture.ctx.clone();
        let dir = tempfile::tempdir().unwrap();
        let storage = fixture.ctx.storage_handle.clone();
        let metadata = crate::metadata::MetadataHandle::new(
            dir.path().join("meta"),
            fixture.actor.node_id,
            storage,
            None,
            None,
            None,
        );
        ctx.metadata_handle = Some(metadata.unwrap());
        let ctx = std::sync::Arc::new(ctx);
        let create = CreateDocumentOperation::new(CreateDocumentConfig {
            actor: fixture.actor.clone(),
            group_id: fixture.group_id,
            document_id: Ulid::nil(),
            document_path: "/docs/one/".to_string(),
            public: false,
            payload: CreateDocumentPayload::Scaffold {
                name: "one".to_string(),
                description: String::new(),
                date_published: "2026-01-01".to_string(),
                license: None,
            },
        });
        let created = create_metadata_document(create, ctx.clone()).await.unwrap();
        for _ in 0..3 {
            crate::metadata::projector::drain_projection_queue(&ctx)
                .await
                .unwrap();
        }
        let page = journal_page(JOURNAL_ROW, None, 10);
        let (kept, _) = drive(page, &ctx).await.unwrap();
        let resources: Vec<String> = kept.into_iter().map(|(_, row)| row.resource).collect();
        assert_eq!(resources, [created.record.document_id.to_string()]);
    }

    #[test]
    fn prune_stops_young() {
        let mut plan = PruneJournal {
            now_ms: 100,
            cutoff: journal_key(JOURNAL_ROW, 50, Ulid::nil()),
            phase: JournalPhase::Kept,
            pruned: 0,
        };
        let old = journal_key(JOURNAL_ROW, 10, Ulid::from_parts(1, 1));
        let young = journal_key(JOURNAL_ROW, 60, Ulid::from_parts(1, 2));
        let page = vec![(old.clone(), Vec::new()), (young.clone(), Vec::new())];
        let deleted = Ask::Delete(vec![(EVENT_JOURNAL_KEYSPACE, old)]);
        assert_eq!(
            plan.next(Answer::Page(page, None)).unwrap(),
            Turn::Ask(deleted)
        );
        assert!(matches!(
            plan.next(Answer::Done).unwrap(),
            Turn::Ask(Ask::Page { .. })
        ));
        let page = vec![(young, Vec::new())];
        assert!(matches!(
            plan.next(Answer::Page(page, None)).unwrap(),
            Turn::Ask(Ask::Page { .. })
        ));
        // A source row past the dedup horizon goes; a recent one stays.
        let anchor = |at: u64| at.to_be_bytes().to_vec();
        let sources = vec![(b"s1".to_vec(), anchor(1)), (b"s2".to_vec(), anchor(99))];
        let next = plan.next(Answer::Page(sources, None)).unwrap();
        let expected = if 100 > 1 + DEDUP_RETENTION_MS { 1 } else { 0 };
        let Turn::Ask(Ask::Delete(keys)) = next else {
            panic!("old source rows are deleted");
        };
        assert_eq!(keys.len(), expected);
        assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish(1 + expected));
    }

    #[test]
    fn page_skips_unreadable() {
        let mut plan = JournalPage(None);
        assert!(plan.first().is_err());
        let rows = vec![(b"j1".to_vec(), Vec::new())];
        let next = Some(b"j1".to_vec());
        let Turn::Finish((events, after)) = plan.next(Answer::Page(rows, next.clone())).unwrap()
        else {
            panic!("a page answers its readable rows");
        };
        assert!(events.is_empty());
        assert_eq!(after, next);
        assert!(plan.next(Answer::Done).is_err());
    }
}
