//! Runs one bounded pass over the job family outbox and offers each queued record to its holders.
//! The pass reports whether entries remain, so the timer re-arms instead of spinning.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::JobRecordFrame;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{FAMILY_OUTBOX_KEYSPACE, FAMILY_RECORD_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::execution::job::record_keys::{budget_key, id_key, record_key};
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::structs::execution::job::{
    ExpiryReason, JobFamilyRecord, JobRecordEnvelope, JobRecordKey, JobRecordKind, LogicalJobSpec,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::Key;
use tracing::warn;

use super::{OUTBOX_DRAIN_BATCH, PublishOutcome, PublishRecordConfig, PublishRecordOperation};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::cancel::PublishCancelOperation;
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::route::JobRouteError;

const OUTBOX_CURSOR_KEY: &[u8] = b"job_family_outbox_cursor";

/// One drain pass. A missing realm config ends the pass with "entries remain", so the timer
/// retries later; a failed storage call fails the pass, which the timer also retries.
#[derive(Debug, PartialEq)]
pub struct DrainOutboxOperation {
    realm_id: RealmId,
    local: NodeId,
    now_ms: u64,
    realm: Option<Box<RealmConfigDocument>>,
    entries: VecDeque<Key>,
    next_cursor: Option<Key>,
    phase: Phase,
}

/// The call the pass waits on.
#[derive(Debug, PartialEq)]
enum Phase {
    Config,
    Cursor,
    Scan,
    Record(Key),
    Clear,
    Publish(Box<PublishRecordOperation>),
    /// The launch a queued receipt binds, then its scheduler's budget, are read to go with it.
    Launch(Box<PublishRecordConfig>),
    Budget(Box<PublishRecordConfig>),
    /// The family's specs are read, to offer the record again with the one it binds.
    Spec(Box<PublishRecordConfig>),
    /// The specs of a family whose claim was refused as stale, to cancel an own spec.
    StaleSpec(Box<JobRecordEnvelope>),
    /// The expiry cancel of a family this node started with a stale spec.
    Expiring,
    WriteCursor,
    Rescan,
}

type Step = Result<Next<bool>, LifecycleError>;

/// The page of outbox entries after `after`.
fn scan(after: Option<Key>, limit: usize) -> Step {
    Ok(Next::Call(IoCall::Page {
        space: FAMILY_OUTBOX_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after,
        limit,
    }))
}

impl DrainOutboxOperation {
    /// One pass of `local`, a node of realm `realm_id`.
    pub fn new(realm_id: RealmId, local: NodeId) -> Self {
        Self {
            realm_id,
            local,
            now_ms: 0,
            realm: None,
            entries: VecDeque::new(),
            next_cursor: None,
            phase: Phase::Config,
        }
    }

    fn cursor_key() -> Key {
        Key::from(OUTBOX_CURSOR_KEY.to_vec())
    }

    /// Reads the record of the next entry with a valid key, or writes the cursor at the end.
    fn next_entry(&mut self) -> Step {
        while let Some(key) = self.entries.pop_front() {
            if JobRecordKey::from_bytes(&key).is_err() {
                continue;
            }
            self.phase = Phase::Record(key.clone());
            return Ok(Next::Call(IoCall::Read(vec![(
                FAMILY_RECORD_KEYSPACE.to_string(),
                key,
            )])));
        }
        self.phase = Phase::WriteCursor;
        let row = (NODE_STATE_KEYSPACE.to_string(), Self::cursor_key());
        Ok(Next::Call(match self.next_cursor.take() {
            Some(value) => IoCall::Write(vec![(row.0, row.1, value)]),
            None => IoCall::Delete(vec![row]),
        }))
    }

    /// Offers one stored record to the holders other than this node, or clears
    /// its entry when no other holder exists.
    fn offer(&mut self, key: Key, frame: JobRecordFrame) -> Step {
        let (Ok(record_key), Some(realm)) = (JobRecordKey::from_bytes(&key), self.realm.as_ref())
        else {
            return self.next_entry();
        };
        let placement = match realm.family_placement(record_key.family.submission_id) {
            Ok(placement) => placement,
            Err(error) => {
                warn!(error = %error, "Queued record has no derivable family placement");
                return self.next_entry();
            }
        };
        let local = self.local;
        let holders: Vec<NodeId> = resolve_shard_holders(realm, &placement)
            .into_iter()
            .filter(|holder| *holder != local)
            .collect();
        if holders.is_empty() {
            self.phase = Phase::Clear;
            return Ok(Next::Call(IoCall::Delete(vec![(
                FAMILY_OUTBOX_KEYSPACE.to_string(),
                key,
            )])));
        }
        let launch = launch_key(frame.envelope());
        let config = PublishRecordConfig {
            placement,
            holders,
            record: Box::new(frame),
            key: record_key,
            attached: Vec::new(),
        };
        match launch {
            Some(key) => self.read_bundled(Phase::Launch(Box::new(config)), key),
            None => self.publish(config),
        }
    }

    /// Reads one record that goes with the queued one.
    fn read_bundled(&mut self, phase: Phase, key: JobRecordKey) -> Step {
        self.phase = phase;
        let row = (FAMILY_RECORD_KEYSPACE.to_string(), record_key(&key));
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn publish(&mut self, config: PublishRecordConfig) -> Step {
        let mut publish = PublishRecordOperation::new(config);
        let step = publish.first();
        self.phase = Phase::Publish(Box::new(publish));
        self.after_publish(step)
    }

    /// Reads the family's specs for a record a holder wants together with its spec.
    fn load_spec(&mut self, config: PublishRecordConfig) -> Step {
        let specs = LoadFamilyOperation::kind(config.key.family, JobRecordKind::Spec);
        self.phase = Phase::Spec(Box::new(config));
        Ok(Next::Call(IoCall::child(specs)))
    }

    /// This node's own spec or claim was refused as stale, so its family never runs: the origin
    /// ends it with an expiry cancel, which also keeps its own witness from launching.
    fn expire_stale(&mut self, envelope: JobRecordEnvelope) -> Step {
        match &envelope.record {
            JobFamilyRecord::Spec(spec) => {
                let spec = spec.as_ref().clone();
                self.expire(spec)
            }
            JobFamilyRecord::Claim(_) => {
                let specs = LoadFamilyOperation::kind(envelope.family(), JobRecordKind::Spec);
                self.phase = Phase::StaleSpec(Box::new(envelope));
                Ok(Next::Call(IoCall::child(specs)))
            }
            _ => self.next_entry(),
        }
    }

    fn expire(&mut self, spec: LogicalJobSpec) -> Step {
        if spec.origin_node_id != self.local {
            return self.next_entry();
        }
        let serving = Some((self.local, self.realm_id));
        let reason = ExpiryReason::StaleSpec;
        let cancel = PublishCancelOperation::expiry(spec, reason, serving, self.now_ms);
        self.phase = Phase::Expiring;
        Ok(Next::Call(IoCall::child(cancel)))
    }

    /// Hands the publish's calls on, or moves to the next entry once it ended.
    fn after_publish(&mut self, step: Result<Next<PublishOutcome>, LifecycleError>) -> Step {
        match step {
            // A publish ends its entry without calls made after a commit.
            Ok(Next::Done(_) | Next::Then { .. }) => self.next_entry(),
            Ok(Next::Call(call)) => Ok(Next::Call(call)),
            Ok(Next::Wait) => Ok(Next::Wait),
            Err(error) => {
                warn!(error = %error, "Job record replication failed");
                self.next_entry()
            }
        }
    }
}

/// The key of the launch a receipt binds: a holder admits both, and the scheduler's budget, even
/// after the scheduler lost holder authority before its launch reached that holder.
fn launch_key(envelope: &JobRecordEnvelope) -> Option<JobRecordKey> {
    let JobFamilyRecord::Receipt(receipt) = &envelope.record else {
        return None;
    };
    let id = receipt.launch_id.to_bytes();
    Some(id_key(&envelope.family(), JobRecordKind::Launch, id, 0))
}

/// The stored spec `envelope` names by digest; an update or a stop names none, so it takes the
/// family's spec.
fn bound_spec(
    envelope: &JobRecordEnvelope,
    specs: Vec<JobRecordEnvelope>,
) -> Option<JobRecordFrame> {
    if matches!(envelope.record, JobFamilyRecord::Spec(_)) {
        return None;
    }
    let named = envelope.record.bound_spec_digest();
    let spec = specs.into_iter().find(|stored| match &stored.record {
        JobFamilyRecord::Spec(spec) => named.is_none_or(|digest| digest == spec.spec_digest),
        _ => false,
    })?;
    JobRecordFrame::new(spec).ok()
}

/// The stored record of one outbox entry as a frame, or `None` when it cannot be offered.
fn stored_frame(value: Option<&[u8]>) -> Option<JobRecordFrame> {
    let envelope = from_bytes::<JobRecordEnvelope>(value?).ok()?;
    JobRecordFrame::new(envelope).ok()
}

/// The single value a one-row read answered.
fn single(result: IoResult) -> Result<Option<aruna_core::types::Value>, LifecycleError> {
    match result {
        IoResult::Values(values) => Ok(values.into_iter().next().flatten()),
        _ => Err(LifecycleError::UnexpectedEvent {
            state: "drain".to_string(),
            expected: "one row",
            got: "another result",
        }),
    }
}

impl Operation for DrainOutboxOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Step {
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        let (space, key) = (config.storage_keyspace(), config.storage_key());
        Ok(Next::Call(aruna_core::rows::read_row(space, key)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match std::mem::replace(&mut self.phase, Phase::WriteCursor) {
            Phase::Config => {
                let realm = single(result)?.map(|value| RealmConfigDocument::from_bytes(&value));
                match realm {
                    Some(Ok(realm)) => {
                        self.realm = Some(Box::new(realm));
                        self.phase = Phase::Cursor;
                        Ok(Next::Call(IoCall::Read(vec![(
                            NODE_STATE_KEYSPACE.to_string(),
                            Self::cursor_key(),
                        )])))
                    }
                    Some(Err(error)) => {
                        warn!(error = %error, "Job family outbox realm config read failed");
                        Ok(Next::Done(true))
                    }
                    None => {
                        warn!("Job family outbox realm config is missing");
                        Ok(Next::Done(true))
                    }
                }
            }
            Phase::Cursor => {
                let cursor = single(result)?;
                self.phase = Phase::Scan;
                scan(cursor, OUTBOX_DRAIN_BATCH)
            }
            Phase::Scan => {
                let IoResult::Page { rows, next } = result else {
                    return Err(unfit("outbox page"));
                };
                self.entries = rows.into_iter().map(|(key, _)| key).collect();
                self.next_cursor = next;
                self.next_entry()
            }
            Phase::Record(key) => match stored_frame(single(result)?.as_deref()) {
                Some(frame) => self.offer(key, frame),
                None => self.next_entry(),
            },
            Phase::Clear => match result {
                IoResult::Done => self.next_entry(),
                _ => Err(unfit("outbox delete")),
            },
            Phase::Publish(mut publish) => {
                let step = publish.next(result);
                match step {
                    Ok(Next::Done(PublishOutcome::SpecNeeded)) => {
                        return self.load_spec(publish.config);
                    }
                    Ok(Next::Done(PublishOutcome::Stale)) => {
                        let envelope = publish.config.record.into_inner();
                        return self.expire_stale(envelope);
                    }
                    _ => {}
                }
                self.phase = Phase::Publish(publish);
                self.after_publish(step)
            }
            Phase::Launch(mut config) => {
                let Some(launch) = stored_frame(single(result)?.as_deref()) else {
                    return self.publish(*config);
                };
                let JobFamilyRecord::Launch(intent) = &launch.envelope().record else {
                    return self.publish(*config);
                };
                let budget = budget_key(&config.key.family, intent.scheduler_node_id);
                config.attached.push(launch);
                self.read_bundled(Phase::Budget(config), budget)
            }
            Phase::Budget(mut config) => {
                config
                    .attached
                    .extend(stored_frame(single(result)?.as_deref()));
                self.publish(*config)
            }
            Phase::Spec(mut config) => {
                let specs = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>();
                let specs = match specs {
                    Ok(Ok(specs)) => specs,
                    Ok(Err(error)) => {
                        warn!(error = %error, "The spec of a queued record is unreadable");
                        return self.next_entry();
                    }
                    Err(_) => return Err(unfit("the specs of a family")),
                };
                match bound_spec(config.record.envelope(), specs) {
                    Some(spec) => {
                        config.attached.push(spec);
                        self.publish(*config)
                    }
                    None => self.next_entry(),
                }
            }
            Phase::StaleSpec(claim) => {
                let specs = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>();
                let Ok(Ok(specs)) = specs else {
                    warn!("The spec of a stale claim is unreadable");
                    return self.next_entry();
                };
                let spec = bound_spec(&claim, specs).map(JobRecordFrame::into_inner);
                match spec.map(|envelope| envelope.record) {
                    Some(JobFamilyRecord::Spec(spec)) => self.expire(*spec),
                    _ => self.next_entry(),
                }
            }
            Phase::Expiring => {
                if let Err(error) = result.child::<(), JobRouteError>()? {
                    warn!(error = %error, "Expiry cancel of a stale spec was not published");
                }
                self.next_entry()
            }
            Phase::WriteCursor => match result {
                IoResult::Done => {
                    self.phase = Phase::Rescan;
                    scan(None, 1)
                }
                _ => Err(unfit("cursor write")),
            },
            Phase::Rescan => match result {
                IoResult::Page { rows, .. } => Ok(Next::Done(!rows.is_empty())),
                _ => Err(unfit("outbox rescan")),
            },
        }
    }
}

fn unfit(expected: &'static str) -> LifecycleError {
    LifecycleError::UnexpectedEvent {
        state: "drain".to_string(),
        expected,
        got: "another result",
    }
}

#[cfg(test)]
#[path = "drain_tests.rs"]
mod state_machine_tests;
