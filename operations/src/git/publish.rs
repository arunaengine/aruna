//! Durably writes one Git record on a document holder and queues it for its co-holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::git::{
    GitChange, GitPackRecord, GitRecord, MAX_RECORDS, git_record_entry, record_change,
};
use aruna_core::keyspaces::GIT_PACK_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::storage_entries::{shard_manifest_entry, sync_revision_entry};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::calls::{noted, written_call};
use super::records::Writes;
use super::{GitError, records};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::realm::load_config::config_unavailable;
use crate::sync::outbox_record::{new_outbox_record, outbox_write_entry, schedule_drain_effect};

type Document = MetadataRegistryRecord;
type Row = (String, byteview::ByteView, byteview::ByteView);

fn config_call(document: &Document) -> IoCall {
    IoCall::child(GetConfigOperation::new(document.realm_id))
}

fn config_answer(result: IoResult) -> Result<RealmConfigDocument, GitError> {
    let config = result.child::<RealmConfigDocument, GetConfigError>()?;
    config.map_err(|error| {
        config_unavailable(&error);
        GitError::Unavailable
    })
}

/// The holders in `config`; a write takes its fence from the same config, so a cutover
/// between two reads cannot pair an old holder check with the new generation.
fn holders_in(
    config: &RealmConfigDocument,
    node: NodeId,
    document: &Document,
) -> Result<Vec<NodeId>, GitError> {
    let holders = resolve_shard_holders(config, &document.placement);
    if !holders.contains(&node) {
        return Err(GitError::NotHolder);
    }
    Ok(holders)
}

/// The document's current holders when this node is one of them.
pub fn holders(node: Option<NodeId>, document: &Document) -> HoldersOperation {
    HoldersOperation::new(node, document)
}

#[derive(Debug, PartialEq)]
pub struct HoldersOperation {
    node: Option<NodeId>,
    document: Document,
}

impl HoldersOperation {
    pub(super) fn new(node: Option<NodeId>, document: &Document) -> Self {
        Self {
            node,
            document: document.clone(),
        }
    }
}

impl Operation for HoldersOperation {
    type Output = Vec<NodeId>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Vec<NodeId>>, GitError> {
        self.node.ok_or(GitError::Unavailable)?;
        Ok(Next::Call(config_call(&self.document)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<NodeId>>, GitError> {
        let config = config_answer(result)?;
        let node = self.node.ok_or(GitError::Unavailable)?;
        Ok(Next::Done(holders_in(&config, node, &self.document)?))
    }
}

fn stored_call(pack: &GitPackRecord) -> IoCall {
    let key = pack.target().storage_key().to_vec();
    IoCall::child(records::load::<GitPackRecord>(GIT_PACK_KEYSPACE, key))
}

fn stored_answer(result: IoResult) -> Result<bool, GitError> {
    Ok(result
        .child::<Option<GitPackRecord>, GitError>()??
        .is_some())
}

fn commit_call(writes: Writes, fence: &WriteFence) -> IoCall {
    IoCall::child(records::commit(writes, fence))
}

/// What the pending call of storing a pack answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PackStep {
    Stored,
    Config,
    Commit,
    Noted,
}

/// Stores and replicates the bytes of a pack whose record is already stored.
pub fn publish_pack(
    node: Option<NodeId>,
    document: &Document,
    pack: GitPackRecord,
) -> PublishPackOperation {
    PublishPackOperation {
        node,
        document: document.clone(),
        pack,
        step: PackStep::Stored,
    }
}

#[derive(Debug, PartialEq)]
pub struct PublishPackOperation {
    node: Option<NodeId>,
    document: Document,
    pack: GitPackRecord,
    step: PackStep,
}

impl Operation for PublishPackOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        Ok(Next::Call(stored_call(&self.pack)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        match self.step {
            PackStep::Stored => {
                if stored_answer(result)? {
                    return Ok(Next::Done(()));
                }
                self.step = PackStep::Config;
                Ok(Next::Call(config_call(&self.document)))
            }
            PackStep::Config => {
                let config = config_answer(result)?;
                let node = self.node.ok_or(GitError::Unavailable)?;
                let document = &self.document;
                let peers = holders_in(&config, node, document)?;
                let mut fence = WriteFence::default();
                fence.add(document.realm_id, &config, [self.pack.placement]);
                let writes = pack_writes(&self.pack, document.realm_id, peers, &fence)?;
                self.step = PackStep::Commit;
                Ok(Next::Call(commit_call(writes, &fence)))
            }
            PackStep::Commit => {
                result.child::<(), GitError>()??;
                self.step = PackStep::Noted;
                let document_id = self.document.document_id;
                Ok(Next::Call(written_call(document_id, false)))
            }
            PackStep::Noted => {
                noted(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// The pack's bytes with the fields of `record`, which names it first.
pub fn pack_record(record: &GitRecord, bytes: bytes::Bytes) -> GitPackRecord {
    GitPackRecord {
        document_id: record.document_id,
        event_id: record.event_id,
        node_id: record.node_id,
        occurred_at_ms: record.occurred_at_ms,
        placement: record.placement,
        bytes,
    }
}

/// The pack row, its sync revision, shard manifest row and outbox publish.
fn pack_writes(
    pack: &GitPackRecord,
    realm_id: RealmId,
    peers: Vec<NodeId>,
    fence: &WriteFence,
) -> Result<Vec<Row>, GitError> {
    let target = pack.target();
    let change = pack.change();
    let bytes = postcard::to_allocvec(pack).map_err(|_| GitError::Invalid)?;
    let mut writes = vec![
        (
            GIT_PACK_KEYSPACE.to_string(),
            target.storage_key(),
            bytes.clone().into(),
        ),
        sync_revision_entry(&target, &change).map_err(|_| GitError::Invalid)?,
    ];
    writes.extend(shard_manifest_entry(&target, &change).map_err(|_| GitError::Invalid)?);
    let outbox = new_outbox_record(
        pack.node_id,
        target,
        peers,
        DocumentOutboxEvent::Upsert { bytes, change },
        pack.placement,
        false,
    )
    .fenced_at(fence.generation(&realm_id, &pack.placement));
    writes.push(outbox_write_entry(&outbox).map_err(|_| GitError::Invalid)?);
    Ok(writes)
}

/// Records the newest checkpoint chain does not cover, which a replay applies on top.
pub fn uncovered(records: &[GitRecord]) -> Vec<&GitRecord> {
    let (_, covered) = super::state::chain(records);
    records
        .iter()
        .filter(|record| !covered.contains(&record.event_id))
        .collect()
}

/// Writes the record, its sync revision, shard manifest row and outbox publish atomically.
pub fn publish(
    node: Option<NodeId>,
    document: &Document,
    user_id: UserId,
    change: GitChange,
) -> PublishRecordOperation {
    publish_with(node, document, user_id, change, (None, Vec::new()))
}

/// Like [`publish`], also storing the bytes of the pack the change names and writing `extra`
/// local rows, all in the same transaction.
pub fn publish_with(
    node: Option<NodeId>,
    document: &Document,
    user_id: UserId,
    change: GitChange,
    extras: (Option<bytes::Bytes>, Vec<Row>),
) -> PublishRecordOperation {
    PublishRecordOperation::new(node, document, user_id, change, extras)
}

/// What the pending call of publishing a record answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishStep {
    Config,
    Scan,
    PackStored,
    Commit,
    Stored,
    Wake,
}

#[derive(Debug, PartialEq)]
pub struct PublishRecordOperation {
    node: Option<NodeId>,
    document: Document,
    user_id: UserId,
    change: Option<GitChange>,
    pack: Option<bytes::Bytes>,
    extra: Vec<Row>,
    step: PublishStep,
    now_ms: u64,
    config: Option<RealmConfigDocument>,
    peers: Vec<NodeId>,
    record: Option<GitRecord>,
    fence: WriteFence,
    writes: Writes,
    pending: Option<GitPackRecord>,
}

type PublishTurn = Result<Next<GitRecord>, GitError>;

impl PublishRecordOperation {
    pub(super) fn new(
        node: Option<NodeId>,
        document: &Document,
        user_id: UserId,
        change: GitChange,
        (pack, extra): (Option<bytes::Bytes>, Vec<Row>),
    ) -> Self {
        Self {
            node,
            document: document.clone(),
            user_id,
            change: Some(change),
            pack,
            extra,
            step: PublishStep::Config,
            now_ms: 0,
            config: None,
            peers: Vec::new(),
            record: None,
            fence: WriteFence::default(),
            writes: Vec::new(),
            pending: None,
        }
    }

    /// Builds the record and its rows once the existing records allow one more.
    fn build(&mut self, existing: &[GitRecord]) -> PublishTurn {
        let change = self.change.take().ok_or(IoError::Unfinished)?;
        if !matches!(change, GitChange::Checkpoint(_)) && uncovered(existing).len() >= MAX_RECORDS {
            return Err(GitError::Full);
        }
        let config = self.config.as_ref().ok_or(IoError::Unfinished)?;
        let node_id = self.node.ok_or(GitError::Unavailable)?;
        let document = &self.document;
        let record = GitRecord {
            event_id: Ulid::generate(),
            realm_id: document.realm_id,
            group_id: document.group_id,
            document_id: document.document_id,
            placement: document.placement,
            user_id: self.user_id,
            node_id,
            occurred_at_ms: self.now_ms,
            change,
        };
        if !record.validate() {
            return Err(GitError::Invalid);
        }
        let invalid = |_| GitError::Invalid;
        self.fence.add(record.realm_id, config, [record.placement]);
        let target = DocumentTarget::GitRecord {
            document_id: record.document_id,
            event_id: record.event_id,
        };
        let change = record_change(&record);
        let bytes = postcard::to_allocvec(&record).map_err(|_| GitError::Invalid)?;
        let mut writes = vec![git_record_entry(&record).map_err(|_| GitError::Invalid)?];
        writes.push(sync_revision_entry(&target, &change).map_err(invalid)?);
        if let Some(entry) = shard_manifest_entry(&target, &change).map_err(invalid)? {
            writes.push(entry);
        }
        // Every record goes onto the document topic, even with no co-holder yet, so holders
        // that join later receive it through topic sync.
        let outbox = new_outbox_record(
            node_id,
            target,
            self.peers.clone(),
            DocumentOutboxEvent::Upsert { bytes, change },
            record.placement,
            false,
        )
        .fenced_at(self.fence.generation(&record.realm_id, &record.placement));
        writes.push(outbox_write_entry(&outbox).map_err(|_| GitError::Invalid)?);
        self.writes = writes;
        let pending = self.pack.take().map(|bytes| pack_record(&record, bytes));
        self.record = Some(record);
        match pending {
            Some(pack) => {
                let call = stored_call(&pack);
                self.pending = Some(pack);
                self.step = PublishStep::PackStored;
                Ok(Next::Call(call))
            }
            None => self.commit(),
        }
    }

    fn commit(&mut self) -> PublishTurn {
        let mut writes = std::mem::take(&mut self.writes);
        writes.append(&mut self.extra);
        self.step = PublishStep::Commit;
        Ok(Next::Call(commit_call(writes, &self.fence)))
    }

    fn finish(&mut self) -> PublishTurn {
        Ok(Next::Done(self.record.take().ok_or(IoError::Unfinished)?))
    }
}

impl Operation for PublishRecordOperation {
    type Output = GitRecord;
    type Error = GitError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PublishTurn {
        Ok(Next::Call(config_call(&self.document)))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn {
        match self.step {
            PublishStep::Config => {
                let config = config_answer(result)?;
                let node = self.node.ok_or(GitError::Unavailable)?;
                self.peers = holders_in(&config, node, &self.document)?;
                self.config = Some(config);
                self.step = PublishStep::Scan;
                let scan = records::scan(self.document.document_id);
                Ok(Next::Call(IoCall::child(scan)))
            }
            PublishStep::Scan => {
                let existing = result.child::<Vec<GitRecord>, GitError>()??;
                self.build(&existing)
            }
            PublishStep::PackStored => {
                let pack = self.pending.take().ok_or(IoError::Unfinished)?;
                if !stored_answer(result)? {
                    let realm_id = self.document.realm_id;
                    let peers = std::mem::take(&mut self.peers);
                    let writes = pack_writes(&pack, realm_id, peers, &self.fence)?;
                    self.writes.extend(writes);
                }
                self.commit()
            }
            PublishStep::Commit => {
                result.child::<(), GitError>()??;
                self.step = PublishStep::Stored;
                Ok(Next::Call(written_call(self.document.document_id, true)))
            }
            PublishStep::Stored => {
                noted(result)?;
                // The record is durable; a missed wake-up only delays replication.
                self.step = PublishStep::Wake;
                Ok(Next::Call(IoCall::from(schedule_drain_effect())))
            }
            PublishStep::Wake => self.finish(),
        }
    }
}

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
