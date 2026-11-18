//! Applies the metadata of accepted pushes to main, retrying until the graph takes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::calls::{
    configured_call, git_call, git_event, has_store, metadata_call, metadata_event,
};
use super::dataset::{self, PushTarget};
use super::snapshot::held_refresh;
use super::{GitError, GitNode, records};
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
};
use crate::metadata::update_entry::UpdateEntryOperation;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageReadError;
use aruna_core::git::{GitEffect, GitEvent, PENDING, PendingMerge, ZERO_OID};
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::collections::{BTreeSet, VecDeque};
use std::sync::{LazyLock, Mutex};
use tracing::warn;
use ulid::Ulid;

type Document = MetadataRegistryRecord;
type PendingRows = Vec<(Vec<u8>, PendingMerge)>;

/// How often a merge is planned again after the graph moved under it.
const MERGE_ATTEMPTS: usize = 8;

/// Orders pending merges as they are accepted; later ones build on earlier ones.
static SEQUENCE: LazyLock<Mutex<ulid::Generator>> = LazyLock::new(Default::default);

/// The local row of a pending merge, written with the push record that brings it. Keys
/// sort by acceptance, so merges apply in the order their pushes moved `main`.
pub fn entry(
    document_id: Ulid,
    merge: &PendingMerge,
) -> Result<(String, byteview::ByteView, byteview::ByteView), GitError> {
    let sequence = SEQUENCE
        .lock()
        .map_err(|_| GitError::Unavailable)?
        .generate()
        .map_err(|_| GitError::Unavailable)?;
    let mut key = document_id.to_bytes().to_vec();
    key.extend_from_slice(&sequence.to_bytes());
    let value = postcard::to_allocvec(merge).map_err(|_| GitError::Invalid)?;
    Ok((PENDING.into(), key.into(), value.into()))
}

/// What the pending call of applying a document's merges answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplyStep {
    Rows,
    Merge,
    Remove,
}

/// Applies every pending merge of the document. A merge that cannot apply yet stays for the
/// next call; one the metadata refuses is dropped and logged, since retrying cannot help.
pub(super) fn apply(node: &GitNode, document: &Document) -> ApplyPendingOperation {
    ApplyPendingOperation {
        node: node.clone(),
        document: document.clone(),
        step: ApplyStep::Rows,
        rows: VecDeque::new(),
        key: None,
        commit: String::new(),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct ApplyPendingOperation {
    node: GitNode,
    document: Document,
    step: ApplyStep,
    rows: VecDeque<(Vec<u8>, PendingMerge)>,
    key: Option<Vec<u8>>,
    commit: String,
}

impl ApplyPendingOperation {
    fn next_row(&mut self) -> Result<Next<()>, GitError> {
        let Some((key, merge)) = self.rows.pop_front() else {
            return Ok(Next::Done(()));
        };
        self.key = Some(key);
        self.commit.clone_from(&merge.new);
        self.step = ApplyStep::Merge;
        let run = apply_merge(&self.node, &self.document, merge);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn remove_row(&mut self) -> Result<Next<()>, GitError> {
        let key = self.key.take().ok_or(IoError::Unfinished)?;
        self.step = ApplyStep::Remove;
        Ok(Next::Call(IoCall::child(records::remove(PENDING, key))))
    }
}

impl Operation for ApplyPendingOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        let prefix = self.document.document_id.to_bytes().to_vec();
        let rows = records::prefixed::<PendingMerge>(PENDING, prefix);
        Ok(Next::Call(IoCall::child(rows)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        match self.step {
            ApplyStep::Rows => {
                self.rows = result.child::<PendingRows, GitError>()??.into();
                self.next_row()
            }
            ApplyStep::Merge => match result.child::<(), GitError>()? {
                Ok(()) => self.remove_row(),
                Err(GitError::Refused(reason)) => {
                    warn!(document_id = %self.document.document_id, commit = %self.commit, %reason,
                        "Pushed metadata could not be merged into the document");
                    self.remove_row()
                }
                // Later merges build on this one, so they wait too.
                Err(error) => Err(error),
            },
            ApplyStep::Remove => {
                result.child::<(), GitError>()??;
                self.next_row()
            }
        }
    }
}

/// What the pending call of applying one merge answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MergeStep {
    Target,
    Export,
    Merge,
    Store,
    Update,
}

/// Merges the values `merge` changed onto the current graph. The update is planned against
/// the exact graph version it was merged with, so a concurrent or late edit is never removed.
fn apply_merge(node: &GitNode, document: &Document, merge: PendingMerge) -> ApplyMergeOperation {
    ApplyMergeOperation {
        node: node.clone(),
        document: document.clone(),
        merge,
        step: MergeStep::Target,
        target: None,
        attempts: 0,
        graph: String::new(),
        version: String::new(),
        merged: None,
    }
}

#[derive(Debug, PartialEq)]
struct ApplyMergeOperation {
    node: GitNode,
    document: Document,
    merge: PendingMerge,
    step: MergeStep,
    target: Option<PushTarget>,
    attempts: usize,
    graph: String,
    version: String,
    merged: Option<String>,
}

type MergeTurn = Result<Next<()>, GitError>;

impl ApplyMergeOperation {
    fn export(&mut self) -> MergeTurn {
        if self.attempts >= MERGE_ATTEMPTS {
            return Err(GitError::Stale);
        }
        self.attempts += 1;
        self.step = MergeStep::Export;
        Ok(Next::Call(metadata_call(MetadataEffect::ExportVersioned {
            graph_iri: self.document.graph_iri.clone(),
        })))
    }

    /// Pushed files are stored and named by content even when no metadata changed.
    fn store_files(&mut self) -> MergeTurn {
        let Some(target) = self.target.take() else {
            return self.update(None);
        };
        let graph = self.merged.as_deref().unwrap_or(&self.graph);
        let store = dataset::store_files(&self.node, &self.document, &self.merge, target, graph);
        self.step = MergeStep::Store;
        Ok(Next::Call(IoCall::child(store)))
    }

    fn update(&mut self, stored: Option<String>) -> MergeTurn {
        let Some(jsonld) = stored.or_else(|| self.merged.take()) else {
            return Ok(Next::Done(()));
        };
        let node_id = self.node.node().ok_or(GitError::Unavailable)?;
        let document = &self.document;
        let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
            actor: Actor {
                node_id,
                user_id: self.merge.user_id,
                realm_id: document.realm_id,
            },
            group_id: document.group_id,
            document_id: document.document_id,
            public: document.public,
            mutation: UpdateDocumentMutation::ReplaceRoCrate { jsonld },
            expected_revision: None,
        })
        .with_expected_graph(std::mem::take(&mut self.version));
        self.step = MergeStep::Update;
        let update = UpdateEntryOperation::new(operation, Some(node_id));
        Ok(Next::Call(IoCall::child(update)))
    }
}

impl Operation for ApplyMergeOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> MergeTurn {
        self.node.node().ok_or(GitError::Unavailable)?;
        let target = dataset::target(&self.node, &self.document, &self.merge);
        Ok(Next::Call(IoCall::child(target)))
    }

    fn next(&mut self, result: IoResult) -> MergeTurn {
        match self.step {
            MergeStep::Target => {
                self.target = result.child::<Option<PushTarget>, GitError>()??;
                self.export()
            }
            MergeStep::Export => {
                let MetadataEvent::VersionedExport {
                    jsonld, version, ..
                } = metadata_event(result)?
                else {
                    return Err(GitError::Unavailable);
                };
                let merge = &self.merge;
                let effect = GitEffect::MergeMetadata {
                    document_id: self.document.document_id,
                    old: (merge.old != ZERO_OID).then(|| merge.old.clone()),
                    new: merge.new.clone(),
                    graph: jsonld.clone(),
                    location: (self.target.as_ref()).map(|target| target.location().clone()),
                };
                let actor = merge.user_id;
                self.graph = jsonld;
                self.version = version;
                self.step = MergeStep::Merge;
                Ok(Next::Call(git_call(effect, actor)))
            }
            MergeStep::Merge => match git_event(result)? {
                GitEvent::MetadataMerged(Ok(merged)) => {
                    self.merged = merged;
                    self.store_files()
                }
                GitEvent::MetadataMerged(Err(error)) => Err(GitError::Refused(error)),
                _ => Err(GitError::Unavailable),
            },
            MergeStep::Store => {
                let (target, stored) = result.child::<dataset::StoredFiles, GitError>()??;
                self.target = Some(target);
                self.update(stored)
            }
            MergeStep::Update => match result.child::<Document, UpdateDocumentError>()? {
                Ok(_) => Ok(Next::Done(())),
                Err(UpdateDocumentError::GraphChanged) => self.export(),
                // Only a verdict on the merged crate itself is final; anything else is retried.
                Err(UpdateDocumentError::MetadataError(
                    error @ (MetadataError::InvalidInput(_)
                    | MetadataError::Validation(_)
                    | MetadataError::ProfileValidation(_)),
                )) if !error.is_transient() => Err(GitError::Refused(error.to_string())),
                Err(error) => {
                    let document_id = self.document.document_id;
                    warn!(%document_id, %error, "Pushed metadata waits");
                    Err(GitError::Unavailable)
                }
            },
        }
    }
}

/// What the pending call of draining pending merges answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DrainStep {
    Store,
    Rows,
    Document,
    Refresh,
}

/// Refreshes every document with a pending merge, the same way Git traffic does: that rebuilds
/// a missing cache and applies the merges, so one left by a restart still lands.
pub fn drain_merges(node: &GitNode) -> DrainMergesOperation {
    DrainMergesOperation {
        node: node.clone(),
        step: DrainStep::Store,
        documents: VecDeque::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct DrainMergesOperation {
    node: GitNode,
    step: DrainStep,
    documents: VecDeque<Ulid>,
}

impl DrainMergesOperation {
    fn next_document(&mut self) -> Result<Next<()>, GitError> {
        let Some(id) = self.documents.pop_front() else {
            return Ok(Next::Done(()));
        };
        self.step = DrainStep::Document;
        Ok(Next::Call(IoCall::child(DocumentRecordOperation::new(id))))
    }
}

impl Operation for DrainMergesOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        Ok(Next::Call(configured_call(Ulid::nil())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        match self.step {
            DrainStep::Store => {
                if !has_store(result)? {
                    return Ok(Next::Done(()));
                }
                self.step = DrainStep::Rows;
                let rows = records::prefixed::<PendingMerge>(PENDING, Vec::new());
                Ok(Next::Call(IoCall::child(rows)))
            }
            DrainStep::Rows => {
                let rows = match result.child::<PendingRows, GitError>()? {
                    Ok(rows) => rows,
                    Err(error) => {
                        warn!(%error, "Pending metadata merges unreadable");
                        return Ok(Next::Done(()));
                    }
                };
                let documents: BTreeSet<Ulid> = (rows.iter())
                    .filter_map(|(key, _)| <[u8; 16]>::try_from(key.get(..16)?).ok())
                    .map(Ulid::from_bytes)
                    .collect();
                self.documents = documents.into_iter().collect();
                self.next_document()
            }
            DrainStep::Document => {
                let found = result.child::<Option<Document>, StorageReadError>()?;
                let Ok(Some(document)) = found else {
                    return self.next_document();
                };
                self.step = DrainStep::Refresh;
                let refresh = held_refresh(&self.node, &document);
                Ok(Next::Call(IoCall::child(refresh)))
            }
            DrainStep::Refresh => {
                result.child::<(), GitError>()??;
                self.next_document()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{
        self, answered, asks_child, asks_store, child, dataset, git_node, ran,
    };
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::git::GitAnswer;
    use aruna_core::structs::identity::realm::RealmId;

    #[test]
    fn rows_keep_order() {
        let merge = |new: char| PendingMerge {
            user_id: aruna_core::UserId::new(Ulid::from(1), RealmId([1; 32])),
            old: ZERO_OID.into(),
            new: new.to_string().repeat(40),
        };
        let document = Ulid::from(9);
        // Commit ids sort the other way; the rows must still follow acceptance.
        let keys: Vec<_> = ['f', 'a', 'c']
            .into_iter()
            .map(|new| entry(document, &merge(new)).expect("row").1.to_vec())
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    #[tokio::test]
    async fn drain_reads_all() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().to_str().expect("path");
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(path).expect("storage"),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let merge = PendingMerge {
            user_id: aruna_core::UserId::new(Ulid::from(1), RealmId([1; 32])),
            old: ZERO_OID.into(),
            new: "a".repeat(40),
        };
        // More rows than one storage page, as a backlog of blocked documents would leave.
        let rows: Vec<_> = (0..1100u128)
            .map(|id| entry(Ulid::from(id), &merge).expect("row"))
            .collect();
        let keys: Vec<_> = rows.iter().map(|row| row.1.to_vec()).collect();
        drive(records::commit(rows, &Default::default()), &context)
            .await
            .expect("commit");
        let read = drive(
            records::prefixed::<PendingMerge>(PENDING, Vec::new()),
            &context,
        )
        .await
        .expect("read");
        assert_eq!(read.into_iter().map(|row| row.0).collect::<Vec<_>>(), keys);
    }

    fn merge() -> PendingMerge {
        PendingMerge {
            user_id: testing::auth().user_id,
            old: ZERO_OID.into(),
            new: "a".repeat(40),
        }
    }

    fn rows() -> IoResult {
        child(Ok::<PendingRows, GitError>(vec![(vec![1; 32], merge())]))
    }

    fn applying() -> ApplyPendingOperation {
        let mut run = apply(&git_node(), &dataset());
        assert!(asks_child(&run.first()));
        assert!(asks_child(&run.next(rows())));
        run
    }

    // An applied merge loses its row, and the pass ends with the last one.
    #[test]
    fn applied_row_removed() {
        let mut run = applying();
        assert!(asks_child(&run.next(child(Ok::<(), GitError>(())))));
        assert_eq!(run.step, ApplyStep::Remove);
        assert_eq!(run.next(child(Ok::<(), GitError>(()))), Ok(Next::Done(())));
    }

    // Metadata the document refuses cannot succeed later, so its row goes too.
    #[test]
    fn refused_row_dropped() {
        let mut run = applying();
        let refused = GitError::Refused("invalid".to_string());
        assert!(asks_child(&run.next(child(Err::<(), _>(refused)))));
        assert_eq!(run.step, ApplyStep::Remove);
    }

    // Later merges build on this one, so a passing failure stops the pass.
    #[test]
    fn failure_keeps_order() {
        let failed = child(Err::<(), _>(GitError::Unavailable));
        assert_eq!(applying().next(failed), Err(GitError::Unavailable));
    }

    fn exported() -> IoResult {
        let event = MetadataEvent::VersionedExport {
            graph_iri: "graph".to_string(),
            jsonld: "{}".to_string(),
            version: "v1".to_string(),
        };
        IoResult::Event(Box::new(aruna_core::events::Event::Metadata(event)))
    }

    /// A merge stepped to the answer of its Git metadata merge.
    fn merging() -> ApplyMergeOperation {
        let mut run = apply_merge(&git_node(), &dataset(), merge());
        assert!(asks_child(&run.first()));
        let arc = child(Ok::<Option<PushTarget>, GitError>(None));
        assert!(run.next(arc).is_ok());
        assert!(asks_store(&run.next(exported())));
        run
    }

    #[test]
    fn merge_needs_node() {
        let mut run = apply_merge(&GitNode::default(), &dataset(), merge());
        assert_eq!(run.first(), Err(GitError::Unavailable));
    }

    // Nothing to change in the graph ends the merge.
    #[test]
    fn unchanged_graph_done() {
        let same = ran(GitEvent::MetadataMerged(Ok(None)));
        assert_eq!(merging().next(same), Ok(Next::Done(())));
    }

    #[test]
    fn git_refusal_final() {
        let refused = ran(GitEvent::MetadataMerged(Err("conflict".to_string())));
        let expected = GitError::Refused("conflict".to_string());
        assert_eq!(merging().next(refused), Err(expected));
    }

    // A graph that moved is merged again, and only a few times.
    #[test]
    fn moved_graph_retries() {
        let mut run = merging();
        let merged = ran(GitEvent::MetadataMerged(Ok(Some("{}".to_string()))));
        assert!(asks_child(&run.next(merged)));
        let moved = || child(Err::<Document, _>(UpdateDocumentError::GraphChanged));
        assert!(run.next(moved()).is_ok());
        assert_eq!(run.step, MergeStep::Export);
        run.attempts = MERGE_ATTEMPTS;
        run.step = MergeStep::Update;
        assert_eq!(run.next(moved()), Err(GitError::Stale));
    }

    // A node without a Git store has nothing to drain.
    #[test]
    fn drain_needs_store() {
        let mut run = drain_merges(&git_node());
        assert!(asks_store(&run.first()));
        let none = answered(GitAnswer::Failed("no store".to_string()));
        assert_eq!(run.next(none), Ok(Next::Done(())));
    }

    // Each document with waiting merges is refreshed once.
    #[test]
    fn drain_refreshes_documents() {
        let mut run = drain_merges(&git_node());
        run.first().unwrap();
        assert!(asks_child(
            &run.next(answered(GitAnswer::Configured(false)))
        ));
        let key = dataset().document_id.to_bytes().to_vec();
        let found = vec![(key.clone(), merge()), (key, merge())];
        let listed = child(Ok::<PendingRows, GitError>(found));
        assert!(asks_child(&run.next(listed)));
        assert!(run.documents.is_empty());
        let document = child(Ok::<_, StorageReadError>(Some(dataset())));
        assert!(asks_child(&run.next(document)));
        assert_eq!(run.step, DrainStep::Refresh);
        assert_eq!(run.next(child(Ok::<(), GitError>(()))), Ok(Next::Done(())));
    }
}
