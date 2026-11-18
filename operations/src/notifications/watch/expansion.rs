//! Expands watch events into inbox records in one write transaction: a page of watches, their
//! authorization, the inbox upsert, the retry row of the next page and the interest marker.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, WATCH_INTEREST_KEYSPACE, WATCH_SUBSCRIPTIONS_KEYSPACE};
use aruna_core::metrics::WatchMetricReason;
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::structs::execution::notification::{InboxWriteOutcome, NotificationRecord};
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventRetry, WatchSubscription, watch_retry_key,
    watch_retry_prefix, watch_subscription_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;

use crate::notifications::inbox::{InboxError, UpsertInboxOperation};
use crate::notifications::placement::filter_local_subscriptions;
use crate::notifications::protocol::{
    EVENT_BATCH_SIZE, EXPANSION_CANDIDATE_CAP, EXPANSION_RECORD_CAP, RETRY_BATCH_CAP,
    RETRY_BYTES_CAP, add_limit, expansion_budget, page_limit, valid_retry,
};
use crate::notifications::routing::route_watch_event;
use crate::notifications::watch::check::{WatchCheckError, WatchCheckOperation};
use crate::notifications::watch::digest::{InterestError, InterestOperation, dirty_marker_write};
use crate::notifications::watch::listing::{ListWatchesOperation, WatchPage, WatchScope};
use crate::notifications::watch::paths::WatchAuthorization;
use crate::notifications::watch::records::WatchSubscriptionError;

#[derive(Debug, Error, PartialEq)]
pub enum ExpandError {
    /// The watches or the retry row changed meanwhile; a fresh run may pass.
    #[error("watch event expansion conflicted")]
    Conflict,
    /// An authorization check could not run now; the delivery is retried.
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    Fatal(String),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for ExpandError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict | IoError::Storage(StorageError::TransactionConflict) => {
                Self::Conflict
            }
            IoError::Storage(error) => Self::Fatal(error.to_string()),
            error => Self::Io(error),
        }
    }
}

/// What one expansion stored, and whether it skipped stale or unauthorized watches.
#[derive(Debug, Default, PartialEq)]
pub struct ExpandOutcome {
    pub inbox: InboxWriteOutcome,
    pub dropped: bool,
}

/// One watch that routes one event into records, waiting for its authorization.
#[derive(Debug, PartialEq)]
struct Candidate {
    subscription: WatchSubscription,
    event: WatchEvent,
    routed: Vec<NotificationRecord>,
}

#[derive(Debug, Default, PartialEq)]
enum ExpandState {
    #[default]
    List,
    Guard,
    Scope,
    Event,
    Upsert(Box<UpsertInboxOperation>),
    RetryRead,
    RetryDelete,
    RetryPage,
    RetryWrite,
    Marker,
}

type Turn = Result<Next<ExpandOutcome>, ExpandError>;

fn fatal(error: impl ToString) -> ExpandError {
    ExpandError::Fatal(error.to_string())
}

/// Expands one bounded page of the realm's watches this node holds; a later page is kept as a
/// durable retry row. The watches read are guarded in the transaction, so a watch changed or
/// removed meanwhile is a conflict, and placement and authorization are checked again.
#[derive(Debug, PartialEq)]
pub struct WatchExpandOperation {
    realm_id: RealmId,
    realm_config: RealmConfigDocument,
    local: NodeId,
    events: Vec<WatchEvent>,
    /// The page cursor and the retry row of a drained retry; `None` for a fresh delivery.
    start: Option<Vec<u8>>,
    retry_key: Option<Vec<u8>>,
    /// The interest marker row, made with the operation so its steps stay deterministic.
    marker: (KeySpace, Key, Value),
    state: ExpandState,
    next: Option<Vec<u8>>,
    retry_value: Option<Vec<u8>>,
    found_stale: bool,
    guarded: Vec<WatchSubscription>,
    candidates: VecDeque<Candidate>,
    current: Option<Candidate>,
    records: Vec<NotificationRecord>,
    queue_changed: bool,
    /// The suppressed deliveries, recorded as metrics once the run ends either way.
    suppressed: Vec<WatchMetricReason>,
    outcome: ExpandOutcome,
}

impl WatchExpandOperation {
    pub fn new(
        realm_id: RealmId,
        realm_config: RealmConfigDocument,
        local: NodeId,
        events: Vec<WatchEvent>,
        retry: Option<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Self {
        let (retry_key, start) = match retry {
            Some((key, cursor)) => (Some(key), cursor),
            None => (None, None),
        };
        Self {
            realm_id,
            realm_config,
            local,
            events,
            start,
            retry_key,
            marker: dirty_marker_write(realm_id),
            state: ExpandState::List,
            next: None,
            retry_value: None,
            found_stale: false,
            guarded: Vec::new(),
            candidates: VecDeque::new(),
            current: None,
            records: Vec::new(),
            queue_changed: false,
            suppressed: Vec::new(),
            outcome: ExpandOutcome::default(),
        }
    }

    /// Routes the listed page and decides what this run must store.
    fn listed(&mut self, page: WatchPage) -> Turn {
        let (subscriptions, stale) =
            filter_local_subscriptions(page.subscriptions, &self.realm_config, self.local)
                .map_err(fatal)?;
        self.found_stale = stale;
        let work = expansion_budget(self.events.len(), subscriptions.len()).map_err(fatal)?;
        let mut candidates = VecDeque::with_capacity(work.min(EXPANSION_CANDIDATE_CAP));
        let mut record_count = 0;
        for event in &self.events {
            for subscription in &subscriptions {
                let routed = route_watch_event(event, std::slice::from_ref(subscription));
                if routed.is_empty() {
                    continue;
                }
                add_limit(candidates.len(), 1, EXPANSION_CANDIDATE_CAP, "candidate")
                    .map_err(fatal)?;
                record_count =
                    add_limit(record_count, routed.len(), EXPANSION_RECORD_CAP, "record")
                        .map_err(fatal)?;
                candidates.push_back(Candidate {
                    subscription: subscription.clone(),
                    event: event.clone(),
                    routed,
                });
            }
        }
        self.next = page.next;
        if self.retry_key.is_none() && self.next.is_some() {
            self.retry_key = Some(watch_retry_key(self.realm_id, self.events[0].event_id));
        }
        if let Some(cursor) = self.next.clone() {
            let retry = WatchEventRetry {
                events: self.events.clone(),
                cursor: Some(cursor),
            };
            let value = postcard::to_allocvec(&retry).map_err(fatal)?;
            if value.len() > RETRY_BYTES_CAP {
                return Err(fatal("watch retry event exceeds byte cap"));
            }
            self.retry_value = Some(value);
        }
        if candidates.is_empty() && self.retry_key.is_none() {
            self.outcome.dropped = stale;
            return Ok(Next::Done(std::mem::take(&mut self.outcome)));
        }
        if candidates.is_empty() {
            return self.retry_change();
        }
        let reads = self.guard_reads(&candidates);
        self.candidates = candidates;
        self.state = ExpandState::Guard;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// The watch rows of the candidates, compared after the read, and the realm's and groups'
    /// authorization rows, read so a change to them is a conflict at commit.
    fn guard_reads(&mut self, candidates: &VecDeque<Candidate>) -> Vec<(KeySpace, Key)> {
        let mut guarded: Vec<WatchSubscription> = Vec::new();
        for candidate in candidates {
            let watch_id = candidate.subscription.watch_id;
            if !guarded.iter().any(|current| current.watch_id == watch_id) {
                guarded.push(candidate.subscription.clone());
            }
        }
        let mut reads: Vec<(KeySpace, Key)> = (guarded.iter())
            .map(|subscription| {
                let key = watch_subscription_key(subscription.owner, subscription.watch_id);
                (WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(), key)
            })
            .collect();
        reads.push((
            AUTH_KEYSPACE.to_string(),
            self.realm_id.as_bytes().to_vec().into(),
        ));
        for candidate in candidates {
            let group_id = match &candidate.event.detail {
                WatchEventDetail::MetadataCreated { group_id, .. }
                | WatchEventDetail::DataUploaded { group_id, .. }
                | WatchEventDetail::SyncCompleted { group_id, .. }
                | WatchEventDetail::SyncFailed { group_id, .. } => *group_id,
            };
            let key: Key = group_id.to_bytes().to_vec().into();
            let known =
                (reads.iter()).any(|(space, current)| space == AUTH_KEYSPACE && *current == key);
            if !known {
                reads.push((AUTH_KEYSPACE.to_string(), key));
            }
        }
        self.guarded = guarded;
        reads
    }

    fn guard_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        if values.len() < self.guarded.len() {
            return Err(fatal(
                "watch authorization guard returned the wrong result count",
            ));
        }
        for (subscription, stored) in self.guarded.iter().zip(values) {
            let expected = subscription.to_bytes().map_err(fatal)?;
            if stored.as_deref() != Some(expected.as_slice()) {
                return Err(ExpandError::Conflict);
            }
        }
        self.next_candidate()
    }

    /// Checks the next candidate's watch scope, or upserts the authorized records.
    fn next_candidate(&mut self) -> Turn {
        let Some(candidate) = self.candidates.pop_front() else {
            return self.upsert();
        };
        let watch = &candidate.subscription;
        let check = WatchCheckOperation::delivery(
            self.realm_id,
            watch.owner,
            &watch.path_prefix,
            watch.event_mask,
            &watch.authorization,
        );
        self.current = Some(candidate);
        self.state = ExpandState::Scope;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn scope_checked(&mut self, check: Result<WatchAuthorization, WatchCheckError>) -> Turn {
        match check {
            Ok(WatchAuthorization::Authorized) => {
                let candidate = self.current.as_ref().ok_or(IoError::Unfinished)?;
                let watch = &candidate.subscription;
                let check =
                    WatchCheckOperation::event(watch.owner, &watch.authorization, &candidate.event);
                self.state = ExpandState::Event;
                Ok(Next::Call(IoCall::child(check)))
            }
            Ok(WatchAuthorization::Denied(reason)) => {
                self.suppressed.push(reason.metric_reason());
                self.outcome.dropped = true;
                self.next_candidate()
            }
            Ok(WatchAuthorization::Unavailable(error)) => Err(self.unavailable(error)),
            Err(error) => Err(self.unavailable(error.to_string())),
        }
    }

    fn event_checked(&mut self, check: Result<WatchAuthorization, WatchCheckError>) -> Turn {
        let candidate = self.current.take().ok_or(IoError::Unfinished)?;
        match check {
            Ok(WatchAuthorization::Authorized) => self.records.extend(candidate.routed),
            Ok(WatchAuthorization::Denied(reason)) => {
                self.suppressed.push(reason.metric_reason());
            }
            Ok(WatchAuthorization::Unavailable(error)) => return Err(self.unavailable(error)),
            Err(error) => return Err(self.unavailable(error.to_string())),
        }
        self.next_candidate()
    }

    /// Runs the inbox upsert inside this transaction.
    fn upsert(&mut self) -> Turn {
        if self.records.is_empty() {
            return self.retry_change();
        }
        let mut upsert = Box::new(UpsertInboxOperation::new(std::mem::take(&mut self.records)));
        let turn = upsert.first();
        self.upserted(upsert, turn)
    }

    fn upserted(
        &mut self,
        upsert: Box<UpsertInboxOperation>,
        turn: Result<Next<InboxWriteOutcome>, InboxError>,
    ) -> Turn {
        match turn {
            Ok(Next::Call(call)) => {
                self.state = ExpandState::Upsert(upsert);
                Ok(Next::Call(call))
            }
            Ok(Next::Done(outcome)) => {
                self.outcome.inbox = outcome;
                self.retry_change()
            }
            Ok(Next::Wait | Next::Then { .. }) => Err(fatal("the inbox upsert ended early")),
            Err(InboxError::Conflict) => Err(ExpandError::Conflict),
            Err(error) => Err(fatal(error)),
        }
    }

    /// Keeps the next page as the retry row, or removes the drained retry row.
    fn retry_change(&mut self) -> Turn {
        let Some(key) = self.retry_key.clone() else {
            return self.mark();
        };
        let space = WATCH_INTEREST_KEYSPACE.to_string();
        if self.retry_value.is_none() {
            self.state = ExpandState::RetryRead;
            return Ok(Next::Call(IoCall::Read(vec![(space, key.into())])));
        }
        self.state = ExpandState::RetryPage;
        Ok(Next::Call(IoCall::Page {
            space,
            prefix: watch_retry_prefix(self.realm_id).into(),
            after: None,
            limit: RETRY_BATCH_CAP.saturating_add(1),
        }))
    }

    /// Writes the retry row within the queue's row and byte caps; a stored row that is further
    /// along stays.
    fn write_retry(&mut self, rows: Vec<(Key, Value)>) -> Turn {
        let realm_id = self.realm_id;
        let key = self.retry_key.clone().ok_or(IoError::Unfinished)?;
        let mut value = self.retry_value.take().ok_or(IoError::Unfinished)?;
        if rows.len() > RETRY_BATCH_CAP {
            warn!(%realm_id, "Watch retry queue row cap reached");
            return Err(fatal("watch retry row cap reached"));
        }
        let current_bytes = rows
            .iter()
            .try_fold(0usize, |total, (_, row)| total.checked_add(row.len()));
        let current_bytes =
            current_bytes.ok_or_else(|| fatal("watch retry byte budget overflow"))?;
        let existing = rows
            .iter()
            .find(|(current, _)| current.as_ref() == key.as_slice());
        let replaced = existing.map_or(0, |(_, row)| row.len());
        if replaced == 0 && rows.len() >= RETRY_BATCH_CAP {
            warn!(%realm_id, "Watch retry queue row cap reached");
            return Err(fatal("watch retry row cap reached"));
        }
        if let Some((_, current)) = existing
            && let Ok(current_retry) = postcard::from_bytes::<WatchEventRetry>(current.as_ref())
            && let Ok(next_retry) = postcard::from_bytes::<WatchEventRetry>(&value)
            && let (Some(current_cursor), Some(next_cursor)) =
                (current_retry.cursor.as_ref(), next_retry.cursor.as_ref())
            && current_cursor >= next_cursor
        {
            value = current.to_vec();
        }
        let next_bytes = current_bytes
            .checked_sub(replaced)
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(|| fatal("watch retry byte budget overflow"))?;
        if next_bytes > RETRY_BYTES_CAP {
            warn!(%realm_id, "Watch retry queue byte cap reached");
            return Err(fatal("watch retry byte cap reached"));
        }
        self.state = ExpandState::RetryWrite;
        let row = (
            WATCH_INTEREST_KEYSPACE.to_string(),
            key.into(),
            value.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    /// Asks for a new interest publish when a page waits or a watch was dropped.
    fn mark(&mut self) -> Turn {
        self.outcome.dropped |= self.found_stale;
        if self.next.is_none() && !self.outcome.dropped {
            return self.finish();
        }
        self.state = ExpandState::Marker;
        Ok(Next::Call(IoCall::Write(vec![self.marker.clone()])))
    }

    /// A check that could not run now ends the run, recorded as one more suppression.
    fn unavailable(&mut self, error: String) -> ExpandError {
        self.suppressed
            .push(WatchMetricReason::AuthorizationUnavailable);
        ExpandError::Unavailable(error)
    }

    /// The metric records of the suppressed deliveries.
    fn suppression_calls(&mut self) -> Vec<IoCall> {
        let suppressed = std::mem::take(&mut self.suppressed).into_iter();
        let record = |reason| IoCall::from(Effect::Net(NetEffect::RecordSuppression(reason)));
        suppressed.map(record).collect()
    }

    /// Ends the run; after the commit it records the suppressions, and a queued next page asks
    /// for a publish.
    fn finish(&mut self) -> Turn {
        let output = std::mem::take(&mut self.outcome);
        let mut calls = self.suppression_calls();
        if self.queue_changed && self.next.is_some() {
            calls.push(IoCall::child(InterestOperation::publish()));
        }
        if calls.is_empty() {
            return Ok(Next::Done(output));
        }
        Ok(Next::Then { output, calls })
    }
}

impl Operation for WatchExpandOperation {
    type Output = ExpandOutcome;
    type Error = ExpandError;

    fn first(&mut self) -> Turn {
        if self.events.is_empty() {
            return Ok(Next::Done(ExpandOutcome::default()));
        }
        if self.events.len() > EVENT_BATCH_SIZE {
            let count = self.events.len();
            return Err(fatal(format!(
                "watch event batch count {count} exceeds cap {EVENT_BATCH_SIZE}"
            )));
        }
        let limit = page_limit(self.events.len()).map_err(fatal)?;
        let scope = WatchScope::Page {
            realm_id: self.realm_id,
            start: self.start.clone(),
            limit,
        };
        Ok(Next::Call(IoCall::child(ListWatchesOperation::new(scope))))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::take(&mut self.state) {
            ExpandState::List => {
                let page = result
                    .child::<WatchPage, WatchSubscriptionError>()?
                    .map_err(fatal)?;
                self.listed(page)
            }
            ExpandState::Guard => self.guard_checked(result.values()?),
            ExpandState::Scope => {
                self.scope_checked(result.child::<WatchAuthorization, WatchCheckError>()?)
            }
            ExpandState::Event => {
                self.event_checked(result.child::<WatchAuthorization, WatchCheckError>()?)
            }
            ExpandState::Upsert(mut upsert) => {
                let turn = upsert.next(result);
                self.upserted(upsert, turn)
            }
            ExpandState::RetryRead => match result.values()?.into_iter().next().flatten() {
                Some(_) => {
                    let key = self.retry_key.clone().ok_or(IoError::Unfinished)?;
                    self.state = ExpandState::RetryDelete;
                    let row = (WATCH_INTEREST_KEYSPACE.to_string(), key.into());
                    Ok(Next::Call(IoCall::Delete(vec![row])))
                }
                // Another run drained the row meanwhile.
                None => Err(ExpandError::Conflict),
            },
            ExpandState::RetryPage => {
                let (rows, _) = result.page()?;
                self.write_retry(rows)
            }
            ExpandState::RetryDelete | ExpandState::RetryWrite => {
                result.done()?;
                self.queue_changed = true;
                self.mark()
            }
            ExpandState::Marker => {
                result.done()?;
                self.finish()
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    /// A failed run still records the suppressions it found.
    fn abort(&mut self) -> Vec<IoCall> {
        self.suppression_calls()
    }

    /// One more run after a conflict, as the delivery did before.
    fn conflict_retries() -> u32 {
        1
    }
}

/// Why a retry drain left work behind; the realm's interest marker keeps it for the next run.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub struct RetryDrainError(pub String);

impl From<IoError> for RetryDrainError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug, Default, PartialEq)]
enum DrainState {
    #[default]
    Scan,
    Expand,
    Mark,
}

/// Runs the queued retry rows of one realm, each as an expansion that a lost conflict reruns
/// once. A malformed row, a row over the byte cap and a failed expansion stay queued, and the
/// realm's interest marker is set so the next publish run tries them again.
#[derive(Debug, PartialEq)]
pub struct RetryDrainOperation {
    realm_id: RealmId,
    realm_config: RealmConfigDocument,
    local: NodeId,
    state: DrainState,
    rows: VecDeque<(Vec<u8>, WatchEventRetry)>,
    failed: bool,
    error: Option<String>,
}

type DrainTurn = Result<Next<()>, RetryDrainError>;

impl RetryDrainOperation {
    pub fn new(realm_id: RealmId, realm_config: RealmConfigDocument, local: NodeId) -> Self {
        Self {
            realm_id,
            realm_config,
            local,
            state: DrainState::Scan,
            rows: VecDeque::new(),
            failed: false,
            error: None,
        }
    }

    fn scanned(&mut self, values: Vec<(Key, Value)>) -> DrainTurn {
        let realm_id = self.realm_id;
        if values.len() > RETRY_BATCH_CAP {
            warn!(%realm_id, "Watch retry queue row cap reached");
            self.failed = true;
        }
        for (key, value) in values.into_iter().take(RETRY_BATCH_CAP) {
            let key = key.to_vec();
            if value.len() > RETRY_BYTES_CAP {
                warn!(%realm_id, "Retaining watch retry row over byte cap");
                self.failed = true;
                continue;
            }
            match postcard::from_bytes::<WatchEventRetry>(&value) {
                Ok(retry) if valid_retry(&retry, realm_id, &key) => {
                    self.rows.push_back((key, retry))
                }
                Ok(_) | Err(_) => {
                    warn!(%realm_id, "Retaining malformed watch retry row");
                    self.failed = true;
                }
            }
        }
        self.next_row()
    }

    fn next_row(&mut self) -> DrainTurn {
        let Some((key, retry)) = self.rows.pop_front() else {
            if self.failed {
                return self.defer("watch retry drain deferred work".to_string());
            }
            return Ok(Next::Done(()));
        };
        let (realm_id, config, local) = (self.realm_id, self.realm_config.clone(), self.local);
        let seed = key.clone();
        let build = move || {
            let resume = Some((key.clone(), retry.cursor.clone()));
            let events = retry.events.clone();
            WatchExpandOperation::new(realm_id, config.clone(), local, events, resume)
        };
        let expand = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        self.state = DrainState::Expand;
        Ok(Next::Call(IoCall::Child(expand)))
    }

    /// Keeps the realm's interest marker, so the queued work runs again, and fails with `error`.
    fn defer(&mut self, error: String) -> DrainTurn {
        self.error = Some(error);
        self.state = DrainState::Mark;
        Ok(Next::Call(IoCall::child(InterestOperation::mark(
            self.realm_id,
        ))))
    }
}

impl Operation for RetryDrainOperation {
    type Output = ();
    type Error = RetryDrainError;

    fn first(&mut self) -> DrainTurn {
        // Sent as an effect, so a failed scan comes back as an event and still keeps the marker.
        let scan = StorageEffect::Iter {
            key_space: WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Some(watch_retry_prefix(self.realm_id).into()),
            start: None,
            limit: RETRY_BATCH_CAP.saturating_add(1),
            txn_id: None,
        };
        Ok(Next::Call(IoCall::from(Effect::Storage(scan))))
    }

    fn next(&mut self, result: IoResult) -> DrainTurn {
        match std::mem::take(&mut self.state) {
            DrainState::Scan => match result.event()? {
                Event::Storage(StorageEvent::IterResult { values, .. }) => self.scanned(values),
                Event::Storage(StorageEvent::Error { error }) => self.defer(error.to_string()),
                other => {
                    let error = format!("unexpected watch retry scan event: {}", other.label());
                    self.defer(error)
                }
            },
            DrainState::Expand => {
                if let Err(error) = result.child::<ExpandOutcome, ExpandError>()? {
                    let realm_id = self.realm_id;
                    warn!(%realm_id, %error, "Watch retry row deferred");
                    self.failed = true;
                }
                self.next_row()
            }
            DrainState::Mark => {
                let error = self.error.take().unwrap_or_default();
                match result.child::<bool, InterestError>()? {
                    Ok(_) => Err(RetryDrainError(error)),
                    Err(marker) => {
                        let realm_id = self.realm_id;
                        warn!(%realm_id, %marker, "Failed to retain watch retry marker");
                        Err(RetryDrainError(format!(
                            "{error}; failed to retain retry marker: {marker}"
                        )))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "expansion_tests.rs"]
mod tests;
