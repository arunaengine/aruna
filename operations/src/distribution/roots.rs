//! Checks the source roots a member holds references of before it completes a transfer: each
//! root is probed once through the member's own connector of it in the set's group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// One pass reads one page of roots; a sweep that met an unreadable root starts over.

use std::collections::VecDeque;

use aruna_core::effects::{Effect, IterStart, StagingSourceEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StagingSourceEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::SOURCE_ROOT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::source_root::{RootEntry, checked_key, decode_key};
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::connectors::ResolveBindingInput;
use crate::connectors::resolver::resolve_binding_effect;

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
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event while checking source roots in state {state}: {received}")]
    UnexpectedEvent {
        state: &'static str,
        received: &'static str,
    },
    #[error("checking source roots did not finish")]
    NotFinished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Init,
    Page,
    Resolve,
    Check,
    Mark,
    Finish,
    Error,
}

impl State {
    fn name(self) -> &'static str {
        match self {
            State::Init => "init",
            State::Page => "page",
            State::Resolve => "resolve",
            State::Check => "check",
            State::Mark => "mark",
            State::Finish => "finish",
            State::Error => "error",
        }
    }
}

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
    output: Option<Result<RootsOutcome, RootsError>>,
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
            state: State::Init,
            rows: VecDeque::new(),
            full: false,
            unchecked: None,
            probes: 0,
            all_checked: true,
            probing: None,
            then: None,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<RootsOutcome, RootsError>) -> Effects {
        self.state = match output {
            Ok(_) => State::Finish,
            Err(_) => State::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    /// Walks the page rows until a probe starts or the page ends.
    fn walk(&mut self) -> Effects {
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
                        && let Some(effects) =
                            self.probe(open, Resume::Row(key.clone(), value.clone(), digest))
                    {
                        return effects;
                    }
                    if let Err(error) = self.record(&key, &value, digest) {
                        return self.finish(Err(error));
                    }
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

    fn end(&mut self) -> Effects {
        if self.full {
            let next = match self.unchecked.take() {
                Some(_) => self.resume.take(),
                None => self.previous.take(),
            };
            let sweep = Sweep {
                after: next.unwrap_or_default(),
                clean: self.clean && self.all_checked,
            };
            return self.finish(Ok(RootsOutcome::Continue(sweep)));
        }
        if let Some(open) = self.unchecked.take()
            && let Some(effects) = self.probe(open, Resume::End)
        {
            return effects;
        }
        let checked = self.clean && self.all_checked;
        self.finish(Ok(RootsOutcome::Done(checked)))
    }

    /// Starts probing one unchecked root while the pass has probes left; `None` when it has not.
    fn probe(&mut self, (digest, entry): ([u8; 32], RootEntry), then: Resume) -> Option<Effects> {
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
        Some(smallvec![resolve_binding_effect(input)])
    }

    /// Ends the running probe; a failed one leaves its root unchecked for this sweep.
    fn probed(&mut self, failure: Option<String>) -> Effects {
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
            Some(Resume::Row(key, value, digest)) => match self.record(&key, &value, digest) {
                Ok(()) => self.walk(),
                Err(error) => self.finish(Err(error)),
            },
            Some(Resume::End) | None => {
                let checked = self.clean && self.all_checked;
                self.finish(Ok(RootsOutcome::Done(checked)))
            }
        }
    }

    fn unexpected(&mut self, event: &Event) -> Effects {
        let state = self.state.name();
        self.finish(Err(RootsError::UnexpectedEvent {
            state,
            received: event.label(),
        }))
    }
}

impl Operation for RootsCheckOperation {
    type Output = RootsOutcome;
    type Error = RootsError;

    fn start(&mut self) -> Effects {
        self.state = State::Page;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: SOURCE_ROOT_KEYSPACE.to_string(),
            prefix: Some(self.set_id.to_bytes().to_vec().into()),
            start: self.after.clone().map(|key| IterStart::After(key.into())),
            limit: ROOT_PAGE,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (State::Page, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.full = values.len() == ROOT_PAGE;
                self.rows = values.into();
                self.walk()
            }
            (
                State::Resolve,
                Event::SubOperation(SubOperationEvent::VersionAccessResolved { result }),
            ) => match result {
                Ok(access) => {
                    self.state = State::Check;
                    smallvec![Effect::StagingSource(StagingSourceEffect::Check { access })]
                }
                Err(error) => self.probed(Some(error.to_string())),
            },
            (State::Check, Event::StagingSource(StagingSourceEvent::CheckResult)) => {
                let Some((digest, _)) = self.probing.as_ref() else {
                    return self.finish(Err(RootsError::NotFinished));
                };
                self.state = State::Mark;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: SOURCE_ROOT_KEYSPACE.to_string(),
                    key: checked_key(self.set_id, digest).into(),
                    value: Vec::new().into(),
                    txn_id: None,
                })]
            }
            (State::Check, Event::StagingSource(StagingSourceEvent::Error { error })) => {
                self.probed(Some(error.to_string()))
            }
            (State::Mark, Event::Storage(StorageEvent::WriteResult { .. })) => self.probed(None),
            (State::Page | State::Mark, Event::Storage(StorageEvent::Error { error })) => {
                self.finish(Err(error.into()))
            }
            (_, event) => self.unexpected(&event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finish | State::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RootsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_store::{context, get, open};
    use crate::driver::{DriverContext, drive};
    use aruna_core::errors::{SourceResolutionError, StagingSourceError};
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
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

    fn page_read(after: Option<Vec<u8>>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: SOURCE_ROOT_KEYSPACE.to_string(),
            prefix: Some(SET.to_bytes().to_vec().into()),
            start: after.map(|key| IterStart::After(key.into())),
            limit: ROOT_PAGE,
            txn_id: None,
        })
    }

    fn rows(values: Vec<(Vec<u8>, Vec<u8>)>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: (values.into_iter())
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            next_start_after: None,
        })
    }

    fn unchecked_row(name: &str) -> ((Vec<u8>, Vec<u8>), RootEntry, [u8; 32]) {
        let root = entry(name);
        let digest = root.root.digest().unwrap();
        let row = (root_key(SET, &digest), root.to_bytes().unwrap());
        (row, root, digest)
    }

    fn resolve(root: &RootEntry) -> Effect {
        resolve_binding_effect(ResolveBindingInput {
            source: root.probe(),
            group_id: Some(GROUP),
        })
    }

    fn resolved(result: Result<ResolvedSourceAccess, SourceResolutionError>) -> Event {
        Event::SubOperation(SubOperationEvent::VersionAccessResolved { result })
    }

    fn access() -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::S3,
            config: Default::default(),
            path: "a.fa".to_string(),
            version: None,
        }
    }

    fn mark(digest: &[u8; 32]) -> Effect {
        Effect::Storage(StorageEffect::Write {
            key_space: SOURCE_ROOT_KEYSPACE.to_string(),
            key: checked_key(SET, digest).into(),
            value: Vec::new().into(),
            txn_id: None,
        })
    }

    // A readable root is resolved, checked and marked, and the short page ends the sweep.
    #[test]
    fn probes_unchecked_root() {
        let (row, root, digest) = unchecked_row("one");
        let mut operation = RootsCheckOperation::new(SET, GROUP, None);
        check_trace(
            &mut operation,
            &[page_read(None)],
            vec![
                (rows(vec![row]), vec![resolve(&root)]),
                (
                    resolved(Ok(access())),
                    vec![Effect::StagingSource(StagingSourceEffect::Check {
                        access: access(),
                    })],
                ),
                (
                    Event::StagingSource(StagingSourceEvent::CheckResult),
                    vec![mark(&digest)],
                ),
                (
                    Event::Storage(StorageEvent::WriteResult {
                        key: checked_key(SET, &digest).into(),
                    }),
                    Vec::new(),
                ),
            ],
        );
        assert_eq!(operation.finalize(), Ok(RootsOutcome::Done(true)));
    }

    // A root that does not resolve or check stays unchecked, and the sweep reports it.
    #[test]
    fn failed_probe_waits() {
        let (row, _, _) = unchecked_row("one");
        let mut operation = RootsCheckOperation::new(SET, GROUP, None);
        operation.start();
        operation.step(rows(vec![row.clone()]));
        let effects = operation.step(resolved(Err(SourceResolutionError::ResolveFailed)));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Ok(RootsOutcome::Done(false)));

        let mut operation = RootsCheckOperation::new(SET, GROUP, None);
        operation.start();
        operation.step(rows(vec![row]));
        operation.step(resolved(Ok(access())));
        let error = StagingSourceError::AccessDenied;
        assert!(
            operation
                .step(Event::StagingSource(StagingSourceEvent::Error { error }))
                .is_empty()
        );
        assert_eq!(operation.finalize(), Ok(RootsOutcome::Done(false)));
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
        check_trace(
            &mut operation,
            &[page_read(Some(vec![1]))],
            vec![(rows(checked), Vec::new())],
        );
        let next = Sweep {
            after: last,
            clean: false,
        };
        assert_eq!(operation.finalize(), Ok(RootsOutcome::Continue(next)));
    }

    // Failed reads and writes end the pass with their error.
    #[test]
    fn storage_errors_fail() {
        let error = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        let mut operation = RootsCheckOperation::new(SET, GROUP, None);
        operation.start();
        operation.step(error());
        assert_eq!(operation.finalize(), Err(StorageError::Timeout.into()));

        let (row, _, _) = unchecked_row("one");
        let mut operation = RootsCheckOperation::new(SET, GROUP, None);
        operation.start();
        operation.step(rows(vec![row]));
        operation.step(resolved(Ok(access())));
        operation.step(Event::StagingSource(StagingSourceEvent::CheckResult));
        operation.step(error());
        assert_eq!(operation.finalize(), Err(StorageError::Timeout.into()));
    }

    // Events outside each phase are refused; abort has nothing to clean up.
    #[test]
    fn rejects_unexpected() {
        let make = || RootsCheckOperation::new(SET, GROUP, None);
        assert_eq!(
            rejects_events(make, sample_events, |_| false).len(),
            sample_events().len()
        );
        let started = || {
            let mut operation = make();
            operation.start();
            operation
        };
        let refused = rejects_events(started, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert_eq!(refused.len(), sample_events().len() - 2);
        let (row, _, _) = unchecked_row("one");
        let probing = || {
            let mut operation = started();
            operation.step(rows(vec![row.clone()]));
            operation
        };
        let refused = rejects_events(probing, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        assert!(probing().abort().is_empty());
    }

    #[test]
    fn rejects_early_finalize() {
        rejects_finalize(RootsCheckOperation::new(SET, GROUP, None));
        let mut operation = RootsCheckOperation::new(SET, GROUP, None);
        operation.start();
        rejects_finalize(operation);
    }
}
