//! Revisits topic operations whose distribution snapshots could not be parked because the parking
//! bound was reached, in bounded pages, so no snapshot is skipped for good.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use byteview::ByteView;
use ulid::Ulid;

use super::distribution::{DistributionPlan, Handled, ReplayForkPlan};
use super::validate::{ConfigSnapshot, ValidateAdminPlan};
use super::{ApplyError, plan_call, topic_cursor_key};
use crate::admin_documents::{AdminDocumentEvent, AdminDocumentTarget};
use crate::calls::{IoCall, IoError, IoResult, Next};
use crate::config_authority::AdminEventValidation;
use crate::document::{
    DocumentEffect, DocumentEvent, DocumentNetEvent, DocumentTarget, OverflowPage,
};
use crate::effects::{Effect, NetEffect};
use crate::events::{Event, NetEvent};
use crate::keyspaces::APPLIED_OPS_KEYSPACE;
use crate::operation::Operation;
use crate::rows::{done, event, page, values};
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::PlacementRef;
use crate::types::{Key, Value};

const OVERFLOW_PREFIX: &[u8] = b"overflow-cursor/";
/// Where the current sweep over the overflowed operations continues.
const SCAN_PREFIX: &[u8] = b"overflow-scan/";
/// The first operation of the current sweep that still found parking full.
const FULL_PREFIX: &[u8] = b"overflow-full/";
/// Topics one pass revisits; snapshots ride the realm config topic, so one is typical.
const MAX_OVERFLOW_TOPICS: usize = 16;
/// Pages one pass replays per topic.
const REPLAY_PAGES: usize = 4;
/// Sets whose last replayed snapshot one pass keeps as fork evidence.
const MAX_EVIDENCE: usize = 64;

fn topic_key(prefix: &[u8], topic_id: irokle::TopicId) -> ByteView {
    let mut key = prefix.to_vec();
    key.extend_from_slice(topic_id.as_bytes());
    ByteView::from(key)
}

/// The row that remembers where an overflowed batch of the topic started.
pub fn overflow_key(topic_id: irokle::TopicId) -> ByteView {
    topic_key(OVERFLOW_PREFIX, topic_id)
}

fn write_row(key: Key, value: Value) -> IoCall {
    IoCall::Write(vec![(APPLIED_OPS_KEYSPACE.to_string(), key, value)])
}

fn delete_rows(keys: Vec<Key>) -> IoCall {
    IoCall::Delete(
        keys.into_iter()
            .map(|key| (APPLIED_OPS_KEYSPACE.to_string(), key))
            .collect(),
    )
}

/// One distribution snapshot of a replayed page.
#[derive(Debug, Clone, PartialEq)]
struct ReplayEvent {
    actor: irokle::ActorId,
    target: DocumentTarget,
    event: AdminDocumentEvent,
    placement: PlacementRef,
    signature: iroh::Signature,
}

/// The sweep over one overflowed topic.
#[derive(Debug, PartialEq)]
struct Sweep {
    topic_id: irokle::TopicId,
    key: Key,
    scan_key: Key,
    full_key: Key,
    /// The checkpoint this sweep started from.
    start: Value,
    full: Option<Value>,
    applied: Option<Value>,
    pages: usize,
    /// The encoded position of the page being replayed and of the page after it.
    at: Value,
    next: Value,
    events: VecDeque<ReplayEvent>,
    current: Option<ReplayEvent>,
    evidence: HashMap<Ulid, AdminDocumentEvent>,
    snapshot: Option<Arc<ConfigSnapshot>>,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Listing,
    Removing,
    Reading,
    Paging,
    Checking,
    Forking,
    Installing,
    MarkingFull,
    SavingScan,
    EndingScan,
    MovingCheckpoint,
    EndingFull,
}

/// Replays distribution snapshots after each overflow checkpoint, a few pages per pass. A
/// snapshot that still finds parking full is passed over, so later ones that can install or park
/// are not starved; the next sweep restarts at the first one passed over.
#[derive(Debug, PartialEq)]
pub struct ReplayOverflowOperation {
    realm_id: RealmId,
    checkpoints: VecDeque<(Key, Value)>,
    sweep: Option<Sweep>,
    phase: Phase,
}

type ReplayNext = Result<Next<()>, ApplyError>;

fn replay_event(event: DocumentEvent, actor: irokle::ActorId) -> Option<ReplayEvent> {
    let DocumentEvent::AdminOperation {
        target,
        event,
        placement,
        origin_signature,
    } = event
    else {
        return None;
    };
    matches!(event.target, AdminDocumentTarget::Distribution { .. }).then(|| ReplayEvent {
        actor,
        target,
        event: *event,
        placement,
        signature: origin_signature,
    })
}

impl ReplayOverflowOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            checkpoints: VecDeque::new(),
            sweep: None,
            phase: Phase::Listing,
        }
    }

    fn call(&mut self, phase: Phase, call: IoCall) -> ReplayNext {
        self.phase = phase;
        Ok(Next::Call(call))
    }

    fn sweep(&mut self) -> Result<&mut Sweep, ApplyError> {
        self.sweep
            .as_mut()
            .ok_or_else(|| IoError::Unfinished.into())
    }

    fn next_checkpoint(&mut self) -> ReplayNext {
        self.sweep = None;
        let Some((key, value)) = self.checkpoints.pop_front() else {
            return Ok(Next::Done(()));
        };
        let topic = key
            .get(OVERFLOW_PREFIX.len()..)
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .map(irokle::TopicId::from_bytes);
        let Some(topic_id) = topic else {
            return self.call(Phase::Removing, delete_rows(vec![key]));
        };
        let (scan_key, full_key) = (
            topic_key(SCAN_PREFIX, topic_id),
            topic_key(FULL_PREFIX, topic_id),
        );
        let reads = [
            scan_key.clone(),
            full_key.clone(),
            topic_cursor_key(topic_id),
        ]
        .into_iter()
        .map(|key| (APPLIED_OPS_KEYSPACE.to_string(), key))
        .collect();
        self.sweep = Some(Sweep {
            topic_id,
            key,
            scan_key,
            full_key,
            start: value,
            full: None,
            applied: None,
            pages: 0,
            at: Value::from(Vec::new()),
            next: Value::from(Vec::new()),
            events: VecDeque::new(),
            current: None,
            evidence: HashMap::new(),
            snapshot: None,
        });
        self.call(Phase::Reading, IoCall::Read(reads))
    }

    fn page_call(&mut self) -> ReplayNext {
        let sweep = self.sweep()?;
        let effect = DocumentEffect::OverflowPage {
            topic: sweep.topic_id,
            start: sweep.start.clone(),
            applied: sweep.applied.clone(),
        };
        let effect = Effect::Net(NetEffect::DocumentSync(effect));
        self.call(Phase::Paging, IoCall::Effect(Box::new(effect)))
    }

    fn on_page(&mut self, result: IoResult) -> ReplayNext {
        let page = match event(result)? {
            Event::Net(NetEvent::DocumentSync(DocumentNetEvent::OverflowPage(page))) => *page,
            Event::Net(NetEvent::DocumentSync(DocumentNetEvent::Error { error, .. })) => {
                return Err(ApplyError::Failed(error));
            }
            other => {
                return Err(IoError::Unexpected {
                    expected: "DocumentNetEvent::OverflowPage",
                    got: other.label(),
                }
                .into());
            }
        };
        match page {
            // A topic that is gone takes its sweep rows along; the checkpoint goes last.
            OverflowPage::TopicGone => {
                let sweep = self.sweep()?;
                let keys = vec![
                    sweep.scan_key.clone(),
                    sweep.full_key.clone(),
                    sweep.key.clone(),
                ];
                self.call(Phase::Removing, delete_rows(keys))
            }
            OverflowPage::Finished => self.end_sweep(),
            OverflowPage::Page { at, next, events } => {
                let sweep = self.sweep()?;
                sweep.at = at;
                sweep.next = next;
                sweep.events = events
                    .into_iter()
                    .filter_map(|(event, actor)| replay_event(event, actor))
                    .collect();
                self.next_event()
            }
        }
    }

    fn next_event(&mut self) -> ReplayNext {
        let realm_id = self.realm_id;
        let sweep = self.sweep()?;
        let Some(item) = sweep.events.pop_front() else {
            return self.end_page();
        };
        let plan = ValidateAdminPlan {
            topic_id: sweep.topic_id,
            actor_id: item.actor,
            target: item.target.clone(),
            event: item.event.clone(),
            realm_id,
            placement: item.placement,
            signature: item.signature,
            cache: sweep.snapshot.clone(),
        };
        sweep.current = Some(item);
        self.call(Phase::Checking, plan_call(plan))
    }

    fn end_page(&mut self) -> ReplayNext {
        let sweep = self.sweep()?;
        sweep.start = sweep.next.clone();
        sweep.pages += 1;
        if sweep.pages < REPLAY_PAGES {
            return self.page_call();
        }
        let call = write_row(sweep.scan_key.clone(), sweep.start.clone());
        self.call(Phase::SavingScan, call)
    }

    /// Ends a sweep at the topic cursor: the checkpoint moves to the first snapshot that found
    /// parking full, or goes once nothing was passed over.
    fn end_sweep(&mut self) -> ReplayNext {
        let call = delete_rows(vec![self.sweep()?.scan_key.clone()]);
        self.call(Phase::EndingScan, call)
    }

    fn on_checked(&mut self, validation: AdminEventValidation) -> ReplayNext {
        let sweep = self.sweep()?;
        let current = sweep.current.as_ref().ok_or(IoError::Unfinished)?;
        if validation != AdminEventValidation::Accepted {
            return self.next_event();
        }
        let AdminDocumentTarget::Distribution { set_id, .. } = current.event.target else {
            return self.next_event();
        };
        if let Some(earlier) = sweep.evidence.get(&set_id) {
            let plan = ReplayForkPlan {
                earlier: earlier.clone(),
                event: current.event.clone(),
            };
            return self.call(Phase::Forking, plan_call(plan));
        }
        self.install()
    }

    /// Keeps the snapshot as evidence of its set and installs it.
    fn install(&mut self) -> ReplayNext {
        let sweep = self.sweep()?;
        let current = sweep.current.take().ok_or(IoError::Unfinished)?;
        if let AdminDocumentTarget::Distribution { set_id, .. } = current.event.target
            && (sweep.evidence.len() < MAX_EVIDENCE || sweep.evidence.contains_key(&set_id))
        {
            sweep.evidence.insert(set_id, current.event.clone());
        }
        let plan = DistributionPlan {
            target: current.target,
            event: current.event,
        };
        self.call(Phase::Installing, plan_call(plan))
    }
}

impl Operation for ReplayOverflowOperation {
    type Output = ();
    type Error = ApplyError;

    fn first(&mut self) -> ReplayNext {
        Ok(Next::Call(IoCall::Page {
            space: APPLIED_OPS_KEYSPACE.to_string(),
            prefix: ByteView::from(OVERFLOW_PREFIX.to_vec()),
            after: None,
            limit: MAX_OVERFLOW_TOPICS,
        }))
    }

    fn next(&mut self, result: IoResult) -> ReplayNext {
        match self.phase {
            Phase::Listing => {
                let (rows, _) = page(result)?;
                self.checkpoints = rows.into();
                self.next_checkpoint()
            }
            Phase::Removing | Phase::SavingScan | Phase::EndingFull => {
                done(result)?;
                self.next_checkpoint()
            }
            Phase::Reading => {
                let [scan, full, applied] = values::<3>(result)?;
                let sweep = self.sweep()?;
                if let Some(scan) = scan {
                    sweep.start = scan;
                }
                sweep.full = full;
                sweep.applied = applied;
                self.page_call()
            }
            Phase::Paging => self.on_page(result),
            Phase::Checking => {
                let (validation, snapshot) = result
                    .child::<(AdminEventValidation, Option<Arc<ConfigSnapshot>>), ApplyError>()??;
                self.sweep()?.snapshot = snapshot;
                self.on_checked(validation)
            }
            Phase::Forking => {
                if result.child::<bool, ApplyError>()?? {
                    self.sweep()?.current = None;
                    return self.next_event();
                }
                self.install()
            }
            Phase::Installing => {
                let handled = result.child::<Handled, ApplyError>()??;
                let sweep = self.sweep()?;
                sweep.snapshot = None;
                if handled == Handled::Full && sweep.full.is_none() {
                    // The next sweep restarts at this page once parked snapshots have drained.
                    sweep.full = Some(sweep.at.clone());
                    let call = write_row(sweep.full_key.clone(), sweep.at.clone());
                    return self.call(Phase::MarkingFull, call);
                }
                self.next_event()
            }
            Phase::MarkingFull => {
                done(result)?;
                self.next_event()
            }
            Phase::EndingScan => {
                done(result)?;
                let sweep = self.sweep()?;
                let call = match sweep.full.clone() {
                    Some(value) => write_row(sweep.key.clone(), value),
                    None => delete_rows(vec![sweep.key.clone()]),
                };
                let moved = sweep.full.is_some();
                match moved {
                    true => self.call(Phase::MovingCheckpoint, call),
                    false => self.call(Phase::EndingFull, call),
                }
            }
            Phase::MovingCheckpoint => {
                done(result)?;
                let call = delete_rows(vec![self.sweep()?.full_key.clone()]);
                self.call(Phase::EndingFull, call)
            }
        }
    }
}

#[cfg(test)]
#[path = "overflow_tests.rs"]
mod tests;
