//! Runs an offer of a local directory: checks the root, walks it, claims the bucket, records
//! every file and sweeps what vanished. Withdrawing an offer sweeps everything.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::StagingSourceError;
use aruna_core::events::{Event, StagingSourceEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, OFFERED_DIRECTORY_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, value, values};
use aruna_core::structs::execution::offered_directory::{OFFERED_DIRECTORY_ROOT, OfferedDirectory};
use aruna_core::structs::execution::source_access::{
    ListCursor, ListRequest, ResolvedSourceAccess, SourceEntry,
};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::storage::blob::{BlobHeadKey, BucketInfo};
use aruna_core::structs::storage::format::Compression;
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};

use super::super::offered_directory::{
    ObservationScope, ObservedFile, OfferDirectoryInput, OfferDirectoryResult,
    OfferedDirectoryError, WithdrawOfferInput,
};
use super::observe::{ObserveFileOperation, TombstoneOperation, entry_fingerprint};

type OfferError = OfferedDirectoryError;

/// Entries fetched per list round, so a large directory does not build one unbounded response.
const LIST_PAGE: usize = 512;

/// Upper bound on the files one offer registers. A directory beyond it is refused whole
/// rather than offered half.
const MAX_OFFERED_FILES: usize = 100_000;

/// Whether one more page would take the offer past the file cap.
fn over_cap(collected: usize, page: usize) -> bool {
    collected.saturating_add(page) > MAX_OFFERED_FILES
}

fn millis_since_epoch(time: SystemTime) -> Option<u64> {
    let since = time.duration_since(std::time::UNIX_EPOCH).ok()?;
    u64::try_from(since.as_millis()).ok()
}

fn local_access(root: &str) -> ResolvedSourceAccess {
    ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::LocalDirectory,
        config: HashMap::from([(OFFERED_DIRECTORY_ROOT.to_string(), root.to_string())]),
        path: String::new(),
        version: None,
    }
}

fn source_call(effect: StagingSourceEffect) -> IoCall {
    IoCall::from(Effect::StagingSource(effect))
}

/// The source's answer; a node without local directory access says so.
fn source_answer(result: IoResult) -> Result<StagingSourceEvent, OfferError> {
    match result.event()? {
        Event::StagingSource(StagingSourceEvent::Error { error }) => Err(match error {
            StagingSourceError::HandleMissing => OfferError::HandleMissing,
            other => other.into(),
        }),
        Event::StagingSource(event) => Ok(event),
        _ => Err(StagingSourceError::InvalidEffect.into()),
    }
}

fn offer_key(bucket: &str) -> Key {
    Key::from(bucket.as_bytes().to_vec())
}

/// Checks that `root` is a directory this node can read.
pub(crate) fn check_root(root: &str) -> CheckRootOperation {
    CheckRootOperation {
        root: root.to_string(),
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct CheckRootOperation {
    root: String,
}

impl Operation for CheckRootOperation {
    type Output = ();
    type Error = OfferError;

    fn first(&mut self) -> Result<Next<()>, OfferError> {
        let access = local_access(&self.root);
        let check = StagingSourceEffect::Check { access };
        Ok(Next::Call(source_call(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, OfferError> {
        match source_answer(result)? {
            StagingSourceEvent::CheckResult => Ok(Next::Done(())),
            _ => Err(StagingSourceError::InvalidEffect.into()),
        }
    }
}

/// Registers a root as a read-only bucket and mints one reference version per file. Offering
/// again refreshes the inventory: unchanged files keep their version, changed ones gain a
/// successor, and vanished files are tombstoned.
pub fn offer_directory(input: OfferDirectoryInput) -> OfferDirectoryOperation {
    OfferDirectoryOperation {
        input,
        step: OfferStep::Check,
        entries: Vec::new(),
        observed: Vec::new(),
    }
}

/// What the pending call of an offer answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OfferStep {
    Check,
    Walk,
    Register,
    Observe,
    Sweep,
}

#[derive(Debug, PartialEq)]
pub struct OfferDirectoryOperation {
    input: OfferDirectoryInput,
    step: OfferStep,
    entries: Vec<SourceEntry>,
    observed: Vec<ObservedFile>,
}

type OfferTurn = Result<Next<OfferDirectoryResult>, OfferError>;

impl OfferDirectoryOperation {
    fn list(&mut self, cursor: Option<ListCursor>) -> OfferTurn {
        self.step = OfferStep::Walk;
        let list = StagingSourceEffect::List {
            access: local_access(&self.input.root),
            request: ListRequest {
                cursor,
                limit: LIST_PAGE,
                recursive: true,
                files_only: true,
                ..Default::default()
            },
        };
        Ok(Next::Call(source_call(list)))
    }

    /// Takes one listed page. The walk ends before any write, so exceeding the cap cannot
    /// leave a partial inventory.
    fn walked(&mut self, event: StagingSourceEvent) -> OfferTurn {
        let StagingSourceEvent::ListResult {
            entries,
            next_cursor,
        } = event
        else {
            return Err(StagingSourceError::InvalidEffect.into());
        };
        if over_cap(self.entries.len(), entries.len()) {
            return Err(OfferError::TooManyFiles(MAX_OFFERED_FILES));
        }
        self.entries.extend(entries);
        if next_cursor.is_some() {
            return self.list(next_cursor);
        }
        self.step = OfferStep::Register;
        let register = RegisterBucketOperation::new(self.input.clone());
        Ok(Next::Call(IoCall::child(register)))
    }

    fn observe_next(&mut self) -> OfferTurn {
        let Some(entry) = self.entries.get(self.observed.len()) else {
            let offered: BTreeSet<String> = self
                .entries
                .iter()
                .map(|entry| entry.path.clone())
                .collect();
            self.step = OfferStep::Sweep;
            let sweep = SweepOperation::new(self.input.scope(), offered);
            return Ok(Next::Call(IoCall::child(sweep)));
        };
        self.step = OfferStep::Observe;
        let observe = ObserveFileOperation::new(self.input.scope(), entry);
        Ok(Next::Call(IoCall::child(observe)))
    }

    fn record(&mut self, version_id: Ulid) -> Result<(), IoError> {
        let entry = self.entries.get(self.observed.len());
        let entry = entry.ok_or(IoError::Unfinished)?;
        self.observed.push(ObservedFile {
            relative: entry.path.clone(),
            fingerprint: entry_fingerprint(entry),
            size: entry.size.unwrap_or_default(),
            modified_at_ms: entry.modified.and_then(millis_since_epoch),
            version_id,
            stat: entry.stat,
        });
        Ok(())
    }
}

impl Operation for OfferDirectoryOperation {
    type Output = OfferDirectoryResult;
    type Error = OfferError;

    fn first(&mut self) -> OfferTurn {
        Ok(Next::Call(IoCall::child(check_root(&self.input.root))))
    }

    fn next(&mut self, result: IoResult) -> OfferTurn {
        match self.step {
            OfferStep::Check => {
                result.child::<(), OfferError>()??;
                self.list(None)
            }
            OfferStep::Walk => self.walked(source_answer(result)?),
            OfferStep::Register => {
                result.child::<(), OfferError>()??;
                self.observe_next()
            }
            OfferStep::Observe => {
                self.record(result.child::<Ulid, OfferError>()??)?;
                self.observe_next()
            }
            OfferStep::Sweep => {
                let removed = result.child::<usize, OfferError>()??;
                Ok(Next::Done(OfferDirectoryResult {
                    bucket: self.input.bucket.clone(),
                    files: self.entries.len(),
                    removed,
                    observed: std::mem::take(&mut self.observed),
                }))
            }
        }
    }
}

/// What the pending call of claiming the bucket answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegisterStep {
    Rows,
    Create,
    Record,
}

/// Claims the bucket for an offer. An existing bucket that is not already this offer's is left
/// alone: an ordinary bucket must not become read-only.
#[derive(Debug, PartialEq)]
struct RegisterBucketOperation {
    input: OfferDirectoryInput,
    step: RegisterStep,
    created_at: Option<SystemTime>,
}

type RegisterTurn = Result<Next<()>, OfferError>;

impl RegisterBucketOperation {
    fn new(input: OfferDirectoryInput) -> Self {
        Self {
            input,
            step: RegisterStep::Rows,
            created_at: None,
        }
    }

    fn record(&mut self, now: SystemTime) -> RegisterTurn {
        let record = OfferedDirectory {
            bucket: self.input.bucket.clone(),
            group_id: self.input.group_id,
            root: self.input.root.clone(),
            created_at: self.created_at.unwrap_or(now),
            created_by: self.input.user_id,
        };
        let row = (
            OFFERED_DIRECTORY_KEYSPACE.to_string(),
            offer_key(&self.input.bucket),
            Value::from(record.to_bytes()?),
        );
        self.step = RegisterStep::Record;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn create(&mut self, now: SystemTime) -> RegisterTurn {
        let info = BucketInfo {
            group_id: self.input.group_id,
            created_at: now,
            created_by: self.input.user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        self.step = RegisterStep::Create;
        let create = CreateBucketOperation::new(self.input.bucket.clone(), info);
        Ok(Next::Call(IoCall::child(create)))
    }
}

impl Operation for RegisterBucketOperation {
    type Output = ();
    type Error = OfferError;

    fn first(&mut self) -> RegisterTurn {
        let key = offer_key(&self.input.bucket);
        Ok(Next::Call(IoCall::Read(vec![
            (OFFERED_DIRECTORY_KEYSPACE.to_string(), key.clone()),
            (S3_BUCKET_KEYSPACE.to_string(), key),
        ])))
    }

    fn next(&mut self, result: IoResult) -> RegisterTurn {
        let now = SystemTime::now();
        match self.step {
            RegisterStep::Rows => {
                let [offer, bucket] = values::<2>(result)?;
                match (offer, bucket) {
                    (Some(offer), _) => {
                        self.created_at = Some(OfferedDirectory::from_bytes(&offer)?.created_at);
                        self.record(now)
                    }
                    (None, Some(bucket)) => {
                        BucketInfo::from_bytes(&bucket)?;
                        Err(OfferError::BucketTaken(self.input.bucket.clone()))
                    }
                    (None, None) => self.create(now),
                }
            }
            RegisterStep::Create => {
                result.child::<BucketInfo, CreateBucketError>()??;
                self.record(now)
            }
            RegisterStep::Record => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// Tombstones every observation whose file the walk did not see: a file the owner deleted or
/// moved must stop being offered. Outputs how many live objects it removed.
#[derive(Debug, PartialEq)]
struct SweepOperation {
    scope: ObservationScope,
    offered: BTreeSet<String>,
    prefix: Option<Key>,
    after: Option<Key>,
    queue: VecDeque<String>,
    marking: bool,
    removed: usize,
}

type SweepTurn = Result<Next<usize>, OfferError>;

impl SweepOperation {
    fn new(scope: ObservationScope, offered: BTreeSet<String>) -> Self {
        Self {
            scope,
            offered,
            prefix: None,
            after: None,
            queue: VecDeque::new(),
            marking: false,
            removed: 0,
        }
    }

    fn page(&self) -> SweepTurn {
        Ok(Next::Call(IoCall::Page {
            space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: self.prefix.clone().ok_or(IoError::Unfinished)?,
            after: self.after.clone(),
            limit: LIST_PAGE,
        }))
    }

    fn advance(&mut self) -> SweepTurn {
        if let Some(key) = self.queue.pop_front() {
            self.marking = true;
            let mark = TombstoneOperation::new(self.scope.clone(), key);
            return Ok(Next::Call(IoCall::child(mark)));
        }
        self.marking = false;
        match self.after {
            Some(_) => self.page(),
            None => Ok(Next::Done(self.removed)),
        }
    }
}

impl Operation for SweepOperation {
    type Output = usize;
    type Error = OfferError;

    fn first(&mut self) -> SweepTurn {
        let prefix = BlobHeadKey::bucket_prefix(&self.scope.bucket)?;
        self.prefix = Some(Key::from(prefix.to_vec()));
        self.page()
    }

    fn next(&mut self, result: IoResult) -> SweepTurn {
        if self.marking {
            self.removed += usize::from(result.child::<bool, OfferError>()??);
            return self.advance();
        }
        let (rows, next) = result.page()?;
        for (key, _) in rows {
            let key = BlobHeadKey::from_bytes(key.as_ref())?.key;
            if !self.offered.contains(&key) {
                self.queue.push_back(key);
            }
        }
        self.after = next;
        self.advance()
    }
}

/// Withdraws one offer: the registration first, then observations become delete markers. An
/// interrupted sweep leaves the bucket unservable, and realm nodes can no longer resolve its
/// versions. Outputs the live count removed.
pub fn withdraw_offer(input: WithdrawOfferInput) -> WithdrawOfferOperation {
    WithdrawOfferOperation {
        input,
        step: WithdrawStep::Read,
        group_id: None,
    }
}

/// What the pending call of withdrawing an offer answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WithdrawStep {
    Read,
    Delete,
    Sweep,
}

#[derive(Debug, PartialEq)]
pub struct WithdrawOfferOperation {
    input: WithdrawOfferInput,
    step: WithdrawStep,
    group_id: Option<aruna_core::types::GroupId>,
}

impl Operation for WithdrawOfferOperation {
    type Output = usize;
    type Error = OfferError;

    fn first(&mut self) -> SweepTurn {
        let key = offer_key(&self.input.bucket);
        Ok(Next::Call(read_row(OFFERED_DIRECTORY_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> SweepTurn {
        match self.step {
            WithdrawStep::Read => {
                let bucket = &self.input.bucket;
                let row = value(result)?.ok_or_else(|| OfferError::NotOffered(bucket.clone()))?;
                self.group_id = Some(OfferedDirectory::from_bytes(&row)?.group_id);
                self.step = WithdrawStep::Delete;
                let row = (OFFERED_DIRECTORY_KEYSPACE.to_string(), offer_key(bucket));
                Ok(Next::Call(IoCall::Delete(vec![row])))
            }
            WithdrawStep::Delete => {
                result.done()?;
                let scope = ObservationScope {
                    bucket: self.input.bucket.clone(),
                    group_id: self.group_id.ok_or(IoError::Unfinished)?,
                    realm_id: self.input.realm_id,
                    node_id: self.input.node_id,
                    user_id: self.input.user_id,
                };
                self.step = WithdrawStep::Sweep;
                let sweep = SweepOperation::new(scope, BTreeSet::new());
                Ok(Next::Call(IoCall::child(sweep)))
            }
            WithdrawStep::Sweep => {
                let removed = result.child::<usize, OfferError>()??;
                Ok(Next::Done(removed))
            }
        }
    }
}

#[cfg(test)]
#[path = "run_tests.rs"]
mod tests;
