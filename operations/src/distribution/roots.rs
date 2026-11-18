//! Checks the source roots a member holds references of before it completes a transfer: each
//! root is probed once through the member's own connector of it in the set's group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// One pass reads one page of roots; a sweep that met an unreadable root starts over.

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{ConversionError, SourceResolutionError};
use aruna_core::events::{Event, StagingSourceEvent};
use aruna_core::keyspaces::SOURCE_ROOT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
use aruna_core::structs::storage::distribution::source_root::{RootEntry, checked_key, decode_key};
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::connectors::{ResolveBindingInput, ResolveBindingOperation};

/// Root rows one pass reads; a set has few roots, so one page is typical.
const ROOT_PAGE: usize = 256;
/// Unchecked roots one pass probes at most.
const MAX_PROBES: usize = 8;

/// Where an open sweep goes on: the last root row it read and whether all roots so far passed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sweep {
    pub after: Vec<u8>,
    pub clean: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootsOutcome {
    /// The page ended with rows left; the next pass goes on from the sweep.
    Continue(Sweep),
    /// The sweep ended; `true` only when it found every root checked.
    Done(bool),
}

#[derive(Debug, Error, PartialEq)]
pub enum RootsError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected answer while checking source roots: {0}")]
    UnexpectedEvent(&'static str),
    #[error("checking source roots did not finish")]
    NotFinished,
}

/// What the pending call of a pass answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Page,
    Resolve,
    Check,
    Mark,
}

type RootsNext = Result<Next<RootsOutcome>, RootsError>;

/// What the walk does once the running probe ends.
#[derive(Debug, PartialEq)]
enum Resume {
    /// Records the row whose unchecked root started the probe, then walks on.
    Row(Key, Value, [u8; 32]),
    End,
}

/// Whether every source root this node recorded for the set is checked: readable through
/// this node's connector of it and allowed for the set's group. One run reads one page of the
/// sweep and reports `Done(true)` only when a whole sweep found every root checked.
#[derive(Debug, PartialEq)]
pub struct RootsCheckOperation {
    set_id: Ulid,
    group_id: GroupId,
    after: Option<Vec<u8>>,
    clean: bool,
    state: State,
    rows: VecDeque<(Key, Value)>,
    full: bool,
    /// The last root row seen whose checked marker has not followed yet.
    unchecked: Option<([u8; 32], RootEntry)>,
    resume: Option<Vec<u8>>,
    previous: Option<Vec<u8>>,
    probes: usize,
    all_checked: bool,
    probing: Option<([u8; 32], RootEntry)>,
    then: Option<Resume>,
}

impl RootsCheckOperation {
    /// Reads the next page of `sweep`, or the first page of a new sweep.
    pub fn new(set_id: Ulid, group_id: GroupId, sweep: Option<Sweep>) -> Self {
        let (after, clean) = sweep.map_or((None, true), |sweep| (Some(sweep.after), sweep.clean));
        Self {
            set_id,
            group_id,
            previous: after.clone(),
            resume: after.clone(),
            after,
            clean,
            state: State::Page,
            rows: VecDeque::new(),
            full: false,
            unchecked: None,
            probes: 0,
            all_checked: true,
            probing: None,
            then: None,
        }
    }

    /// Walks the page rows until a probe starts or the page ends.
    fn walk(&mut self) -> RootsNext {
        while let Some((key, value)) = self.rows.pop_front() {
            let Some((digest, checked)) = decode_key(&key) else {
                self.previous = Some(key.to_vec());
                continue;
            };
            match (checked, self.unchecked.take()) {
                (true, Some((open, _))) if open == digest => {}
                (true, open) => self.unchecked = open,
                (false, open) => {
                    if let Some(open) = open
                        && let Some(probe) =
                            self.probe(open, Resume::Row(key.clone(), value.clone(), digest))
                    {
                        return Ok(Next::Call(probe));
                    }
                    self.record(&key, &value, digest)?;
                    continue;
                }
            }
            self.previous = Some(key.to_vec());
        }
        self.end()
    }

    /// Notes an unchecked root row as the open one.
    fn record(&mut self, key: &Key, value: &Value, digest: [u8; 32]) -> Result<(), RootsError> {
        let entry = RootEntry::from_bytes(value)?;
        self.unchecked = Some((digest, entry));
        self.resume = self.previous.clone();
        self.previous = Some(key.to_vec());
        Ok(())
    }

    fn finished(&self) -> RootsNext {
        let checked = self.clean && self.all_checked;
        Ok(Next::Done(RootsOutcome::Done(checked)))
    }

    fn end(&mut self) -> RootsNext {
        if self.full {
            let next = match self.unchecked.take() {
                Some(_) => self.resume.take(),
                None => self.previous.take(),
            };
            let sweep = Sweep {
                after: next.unwrap_or_default(),
                clean: self.clean && self.all_checked,
            };
            return Ok(Next::Done(RootsOutcome::Continue(sweep)));
        }
        if let Some(open) = self.unchecked.take()
            && let Some(probe) = self.probe(open, Resume::End)
        {
            return Ok(Next::Call(probe));
        }
        self.finished()
    }

    /// Starts probing one unchecked root while the pass has probes left; `None` when it has not.
    fn probe(&mut self, (digest, entry): ([u8; 32], RootEntry), then: Resume) -> Option<IoCall> {
        if self.probes >= MAX_PROBES {
            self.all_checked = false;
            return None;
        }
        self.probes += 1;
        let input = ResolveBindingInput {
            source: entry.probe(),
            group_id: Some(self.group_id),
        };
        self.probing = Some((digest, entry));
        self.then = Some(then);
        self.state = State::Resolve;
        Some(IoCall::child(ResolveBindingOperation::new(input)))
    }

    /// Ends the running probe; a failed one leaves its root unchecked for this sweep.
    fn probed(&mut self, failure: Option<String>) -> RootsNext {
        if let (Some(reason), Some((digest, entry))) = (failure, self.probing.as_ref()) {
            warn!(
                set_id = %self.set_id,
                root = %hex::encode(digest),
                kind = ?entry.root.kind,
                reason,
                "A source root of the set is not readable here; the transfer waits"
            );
            self.all_checked = false;
        }
        self.probing = None;
        match self.then.take() {
            Some(Resume::Row(key, value, digest)) => {
                self.record(&key, &value, digest)?;
                self.walk()
            }
            Some(Resume::End) | None => self.finished(),
        }
    }
}

impl Operation for RootsCheckOperation {
    type Output = RootsOutcome;
    type Error = RootsError;

    fn first(&mut self) -> RootsNext {
        Ok(Next::Call(IoCall::Page {
            space: SOURCE_ROOT_KEYSPACE.to_string(),
            prefix: self.set_id.to_bytes().to_vec().into(),
            after: self.after.clone().map(Key::from),
            limit: ROOT_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> RootsNext {
        match self.state {
            State::Page => {
                let (rows, _) = result.page()?;
                self.full = rows.len() == ROOT_PAGE;
                self.rows = rows.into();
                self.walk()
            }
            State::Resolve => {
                match result.child::<ResolvedSourceAccess, SourceResolutionError>()? {
                    Ok(access) => {
                        self.state = State::Check;
                        let check = StagingSourceEffect::Check { access };
                        let check = Effect::StagingSource(check);
                        Ok(Next::Call(IoCall::Effect(Box::new(check))))
                    }
                    Err(error) => self.probed(Some(error.to_string())),
                }
            }
            State::Check => match result.event()? {
                Event::StagingSource(StagingSourceEvent::CheckResult) => {
                    let (digest, _) = self.probing.as_ref().ok_or(RootsError::NotFinished)?;
                    let row = (
                        SOURCE_ROOT_KEYSPACE.to_string(),
                        checked_key(self.set_id, digest).into(),
                        Vec::new().into(),
                    );
                    self.state = State::Mark;
                    Ok(Next::Call(IoCall::Write(vec![row])))
                }
                Event::StagingSource(StagingSourceEvent::Error { error }) => {
                    self.probed(Some(error.to_string()))
                }
                other => Err(RootsError::UnexpectedEvent(other.label())),
            },
            State::Mark => {
                done(result)?;
                self.probed(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_store::{context, get, open};
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::{StagingSourceError, StorageError};
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::storage::distribution::source_root::{SourceRoot, root_key};
    use std::collections::BTreeMap;

    fn entry(bucket: &str) -> RootEntry {
        RootEntry {
            root: SourceRoot {
                kind: SourceConnectorKind::S3,
                public_config: BTreeMap::from([("bucket".to_string(), bucket.to_string())]),
                namespace: None,
            },
            probe_path: "a.fa".to_string(),
            probe_config: BTreeMap::new(),
        }
    }

    async fn put(handle: &aruna_storage::storage::StorageHandle, key: Vec<u8>, value: Vec<u8>) {
        let write = StorageEffect::Write {
            key_space: SOURCE_ROOT_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        assert!(matches!(
            handle.send_storage_effect(write).await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    /// One pass of the sweep, keeping its position as the transfer tending does.
    async fn pass(
        context: &DriverContext,
        set_id: Ulid,
        group_id: GroupId,
        sweep: &mut Option<Sweep>,
    ) -> bool {
        let check = RootsCheckOperation::new(set_id, group_id, sweep.take());
        match drive(check, context).await.unwrap() {
            RootsOutcome::Continue(next) => {
                *sweep = Some(next);
                false
            }
            RootsOutcome::Done(checked) => checked,
        }
    }

    #[tokio::test]
    async fn unreadable_root_holds() {
        // Checked roots pass; a root no connector of the group reads stays unchecked.
        let (_dir, handle) = open();
        let context = context(&handle);
        let (set_id, group_id) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
        let mut sweep = None;
        assert!(pass(&context, set_id, group_id, &mut sweep).await);
        let known = entry("known");
        let digest = known.root.digest().unwrap();
        put(
            &handle,
            root_key(set_id, &digest),
            known.to_bytes().unwrap(),
        )
        .await;
        put(&handle, checked_key(set_id, &digest), Vec::new()).await;
        assert!(pass(&context, set_id, group_id, &mut sweep).await);
        let unknown = entry("unknown");
        let digest = unknown.root.digest().unwrap();
        put(
            &handle,
            root_key(set_id, &digest),
            unknown.to_bytes().unwrap(),
        )
        .await;
        assert!(!pass(&context, set_id, group_id, &mut sweep).await);
        let marker = get(&handle, SOURCE_ROOT_KEYSPACE, checked_key(set_id, &digest)).await;
        assert_eq!(marker, None, "a failed probe marks nothing");
    }

    #[tokio::test]
    async fn sweep_reads_pages() {
        // Each pass reads one page; only a whole sweep over checked roots completes.
        let (_dir, handle) = open();
        let context = context(&handle);
        let (set_id, group_id) = (Ulid::from_bytes([4; 16]), Ulid::from_bytes([2; 16]));
        let mut sweep = None;
        for seed in 0..ROOT_PAGE {
            let root = entry(&format!("bucket-{seed}"));
            let digest = root.root.digest().unwrap();
            put(&handle, root_key(set_id, &digest), root.to_bytes().unwrap()).await;
            put(&handle, checked_key(set_id, &digest), Vec::new()).await;
        }
        let mut passes = 0;
        while !pass(&context, set_id, group_id, &mut sweep).await {
            passes += 1;
            assert!(passes < 8, "the sweep ends");
        }
        assert!(passes >= 1, "two rows per root need more than one page");
    }
    const SET: Ulid = Ulid::from_bytes([7; 16]);
    const GROUP: Ulid = Ulid::from_bytes([8; 16]);

    fn page_read(after: Option<Vec<u8>>) -> Next<RootsOutcome> {
        Next::Call(IoCall::Page {
            space: SOURCE_ROOT_KEYSPACE.to_string(),
            prefix: SET.to_bytes().to_vec().into(),
            after: after.map(Key::from),
            limit: ROOT_PAGE,
        })
    }

    fn rows(values: Vec<(Vec<u8>, Vec<u8>)>) -> IoResult {
        IoResult::Page {
            rows: (values.into_iter())
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            next: None,
        }
    }

    fn unchecked_row(name: &str) -> ((Vec<u8>, Vec<u8>), [u8; 32]) {
        let root = entry(name);
        let digest = root.root.digest().unwrap();
        ((root_key(SET, &digest), root.to_bytes().unwrap()), digest)
    }

    fn resolved(result: Result<ResolvedSourceAccess, SourceResolutionError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn access() -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::S3,
            config: Default::default(),
            path: "a.fa".to_string(),
            version: None,
        }
    }

    fn staged(event: StagingSourceEvent) -> IoResult {
        IoResult::Event(Box::new(Event::StagingSource(event)))
    }

    fn probing(row: (Vec<u8>, Vec<u8>)) -> RootsCheckOperation {
        let mut operation = RootsCheckOperation::new(SET, GROUP, None);
        operation.first().unwrap();
        let child = operation.next(rows(vec![row]));
        assert!(matches!(child, Ok(Next::Call(IoCall::Child(_)))));
        operation
    }

    // A readable root is resolved, checked and marked, and the short page ends the sweep.
    #[test]
    fn probes_unchecked_root() {
        let (row, digest) = unchecked_row("one");
        assert_eq!(
            RootsCheckOperation::new(SET, GROUP, None).first(),
            Ok(page_read(None))
        );
        let mut operation = probing(row);
        let check = StagingSourceEffect::Check { access: access() };
        let check = IoCall::Effect(Box::new(Effect::StagingSource(check)));
        assert_eq!(operation.next(resolved(Ok(access()))), Ok(Next::Call(check)));
        let mark = (
            SOURCE_ROOT_KEYSPACE.to_string(),
            checked_key(SET, &digest).into(),
            Vec::new().into(),
        );
        assert_eq!(
            operation.next(staged(StagingSourceEvent::CheckResult)),
            Ok(Next::Call(IoCall::Write(vec![mark])))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done(RootsOutcome::Done(true)))
        );
    }

    // A root that does not resolve or check stays unchecked, and the sweep reports it.
    #[test]
    fn failed_probe_waits() {
        let (row, _) = unchecked_row("one");
        let unchecked = Ok(Next::Done(RootsOutcome::Done(false)));
        let mut operation = probing(row.clone());
        let failed = resolved(Err(SourceResolutionError::ResolveFailed));
        assert_eq!(operation.next(failed), unchecked);

        let mut operation = probing(row);
        operation.next(resolved(Ok(access()))).unwrap();
        let error = StagingSourceError::AccessDenied;
        let denied = staged(StagingSourceEvent::Error { error });
        assert_eq!(operation.next(denied), unchecked);
    }

    // A full page continues the sweep after its last row; a dirty sweep stays dirty.
    #[test]
    fn full_page_continues() {
        let checked: Vec<_> = (0..ROOT_PAGE)
            .map(|seed| (checked_key(SET, &[seed as u8; 32]), Vec::new()))
            .collect();
        let last = checked.last().map(|(key, _)| key.clone()).unwrap();
        let sweep = Sweep {
            after: vec![1],
            clean: false,
        };
        let mut operation = RootsCheckOperation::new(SET, GROUP, Some(sweep));
        assert_eq!(operation.first(), Ok(page_read(Some(vec![1]))));
        let next = Sweep {
            after: last,
            clean: false,
        };
        assert_eq!(
            operation.next(rows(checked)),
            Ok(Next::Done(RootsOutcome::Continue(next)))
        );
    }

    // The runner's storage failures end the pass; a wrong answer to a check does too.
    #[test]
    fn failures_fail() {
        let failure = RootsError::from(IoError::Storage(StorageError::Timeout));
        assert_eq!(failure, RootsError::Io(IoError::Storage(StorageError::Timeout)));
        let (row, _) = unchecked_row("one");
        let mut operation = probing(row);
        operation.next(resolved(Ok(access()))).unwrap();
        let other = IoResult::Event(Box::new(Event::Search()));
        assert_eq!(
            operation.next(other),
            Err(RootsError::UnexpectedEvent("Event::Search"))
        );
    }
}
