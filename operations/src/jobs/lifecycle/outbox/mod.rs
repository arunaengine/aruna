//! Replicates locally published job records to the other family holders.
//! Delivery is asynchronous and needs no quorum; an unreachable family leaves entries queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod drain;

use std::time::Duration;

use aruna_core::effects::{
    Effect, HolderList, JobRecordEffect, JobRecordFrame, MAX_RECORD_HOLDERS, NetEffect,
    StorageEffect,
};
use aruna_core::events::{Event, JobRecordEvent, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::FAMILY_OUTBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobRecordKey;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::Effects;
use smallvec::smallvec;
use tracing::{debug, warn};

use super::LifecycleError;
use crate::driver::DriverContext;
use crate::jobs::records::keys::record_key;
use crate::jobs::records::rows::{OutboxEntry, from_bytes, to_bytes};

/// Records one drain pass delivers before it re-arms.
pub const OUTBOX_DRAIN_BATCH: usize = 32;
/// Wall-clock budget of one record publish.
pub const PUBLISH_DEADLINE: Duration = Duration::from_secs(10);
/// Spacing between drain passes while entries remain undeliverable.
pub const OUTBOX_RETRY_AFTER: Duration = Duration::from_secs(5);
/// Definitive refusals one record is offered through before it is dropped.
/// Unreachable holders never count: only a holder's own refusal does.
pub const MAX_PUBLISH_REJECTIONS: u32 = 16;

/// Kicks the outbox drain without persisting a timer of its own.
pub fn schedule_outbox_drain(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainFamilyOutbox,
        after,
    })
}

/// Asks the drain to run now, after a record was queued for replication.
pub async fn kick(context: &DriverContext) {
    if let Some(task) = context.task_handle.as_ref() {
        let _ = task
            .send_effect(schedule_outbox_drain(Duration::ZERO))
            .await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecordConfig {
    pub realm_id: RealmId,
    pub placement: PlacementRef,
    pub holders: Vec<NodeId>,
    pub record: Box<JobRecordFrame>,
    pub key: JobRecordKey,
}

/// One queued record offered to the family holders. The entry is cleared only
/// after every current holder durably accepted it; an unreachable family keeps
/// it queued.
#[derive(Debug, PartialEq)]
pub struct PublishRecordOperation {
    config: PublishRecordConfig,
    queued_at_ms: u64,
    delivered: Vec<NodeId>,
    next_holder: u32,
    rejections: u32,
    state: PublishState,
    outcome: Option<Result<bool, LifecycleError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishState {
    Init,
    Read,
    Offer,
    Store,
    Clear,
    Finish,
    Error,
}

impl PublishRecordOperation {
    pub fn new(config: PublishRecordConfig) -> Self {
        Self {
            config,
            queued_at_ms: 0,
            delivered: Vec::new(),
            next_holder: 0,
            rejections: 0,
            state: PublishState::Init,
            outcome: None,
        }
    }

    fn clear(&mut self) -> Effects {
        self.state = PublishState::Clear;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: FAMILY_OUTBOX_KEYSPACE.to_string(),
            key: record_key(&self.config.key),
            txn_id: None,
        })]
    }

    fn offer(&mut self) -> Effects {
        let holder_count = self.config.holders.len();
        let start = self.next_holder as usize % holder_count.max(1);
        let mut pending = Vec::new();
        let mut last_index = None;
        for offset in 0..holder_count {
            let index = (start + offset) % holder_count;
            let holder = self.config.holders[index];
            if self.delivered.contains(&holder) {
                continue;
            }
            pending.push(holder);
            last_index = Some(index);
            if pending.len() == MAX_RECORD_HOLDERS {
                break;
            }
        }
        if pending.is_empty() {
            return self.clear();
        }
        if let Some(index) = last_index {
            self.next_holder = u32::try_from((index + 1) % holder_count).unwrap_or_default();
        }
        let Ok(holders) = HolderList::new(pending) else {
            return self.fail(LifecycleError::NotHolder);
        };
        self.state = PublishState::Offer;
        smallvec![Effect::Net(NetEffect::JobRecord(Box::new(
            JobRecordEffect::Publish {
                realm_id: self.config.realm_id,
                placement: self.config.placement,
                holders,
                record: self.config.record.clone(),
                deadline: PUBLISH_DEADLINE,
            }
        )))]
    }

    fn store(&mut self) -> Result<Effects, LifecycleError> {
        let value = to_bytes(&OutboxEntry {
            queued_at_ms: self.queued_at_ms,
            delivered: self.delivered.clone(),
            next_holder: self.next_holder,
            rejections: self.rejections,
        })?;
        self.state = PublishState::Store;
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: FAMILY_OUTBOX_KEYSPACE.to_string(),
            key: record_key(&self.config.key),
            value: value.into(),
            txn_id: None,
        })])
    }

    /// Notes the holders that accepted the record; clears the entry once every holder has it.
    fn delivered_to(&mut self, holders: Vec<NodeId>) -> Effects {
        for holder in holders {
            if self.config.holders.contains(&holder) && !self.delivered.contains(&holder) {
                self.delivered.push(holder);
            }
        }
        if self
            .config
            .holders
            .iter()
            .all(|holder| self.delivered.contains(holder))
        {
            return self.clear();
        }
        match self.store() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn settle(&mut self, delivered: bool) -> Effects {
        self.outcome = Some(Ok(delivered));
        self.state = PublishState::Finish;
        smallvec![]
    }

    fn fail(&mut self, error: LifecycleError) -> Effects {
        self.outcome = Some(Err(error));
        self.state = PublishState::Error;
        smallvec![]
    }
}

impl Operation for PublishRecordOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        self.state = PublishState::Read;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: FAMILY_OUTBOX_KEYSPACE.to_string(),
            key: record_key(&self.config.key),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PublishState::Read => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.settle(false);
                    };
                    match from_bytes::<OutboxEntry>(&value) {
                        Ok(entry) => {
                            self.queued_at_ms = entry.queued_at_ms;
                            self.delivered = entry
                                .delivered
                                .into_iter()
                                .filter(|holder| self.config.holders.contains(holder))
                                .collect();
                            self.next_holder = entry.next_holder;
                            self.rejections = entry.rejections;
                            self.offer()
                        }
                        Err(error) => self.fail(error.into()),
                    }
                }
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Read".to_string(),
                    expected: "outbox read result",
                    got: other.label(),
                }),
            },
            PublishState::Offer => match event {
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Published { holder })) => {
                    debug!(peer = %holder, "Job record replicated to a family holder");
                    self.delivered_to(vec![holder])
                }
                Event::Net(NetEvent::JobRecord(JobRecordEvent::PublishedMany { holders })) => {
                    self.delivered_to(holders)
                }
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Rejected { holder, reason })) => {
                    warn!(peer = %holder, reason = ?reason, "Family holder refused a record");
                    self.rejections = self.rejections.saturating_add(1);
                    // A record every holder stands behind refusing will never be
                    // accepted, so it stops consuming the queue.
                    if self.rejections >= MAX_PUBLISH_REJECTIONS {
                        warn!(
                            kind = ?self.config.key.kind,
                            rejections = self.rejections,
                            "Dropping a job record the family holders refuse"
                        );
                        return self.clear();
                    }
                    match self.store() {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Unavailable(message))) => {
                    debug!(message, "No family holder accepted the record yet");
                    match self.store() {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Offer".to_string(),
                    expected: "job record publish result",
                    got: other.label(),
                }),
            },
            PublishState::Store => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.settle(false),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Store".to_string(),
                    expected: "outbox write result",
                    got: other.label(),
                }),
            },
            PublishState::Clear => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => self.settle(true),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Clear".to_string(),
                    expected: "outbox delete result",
                    got: other.label(),
                }),
            },
            PublishState::Init | PublishState::Finish | PublishState::Error => {
                self.fail(LifecycleError::UnexpectedEvent {
                    state: format!("{:?}", self.state),
                    expected: "no event",
                    got: event.label(),
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PublishState::Finish | PublishState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Re-arms the replication and witness queues after a restart. Both queues are
/// durable rows, so the timers only have to be brought back, never the work.
pub async fn restore_lifecycle_timers(
    storage: &aruna_storage::StorageHandle,
    task_handle: &aruna_tasks::TaskHandle,
) {
    let pending = crate::jobs::store::iter_prefix_page(
        storage,
        aruna_core::keyspaces::WITNESS_DEADLINE_KEYSPACE,
        None,
        None,
        1,
        None,
    )
    .await
    .map(|(rows, _)| !rows.is_empty())
    .unwrap_or(false);
    let mut timers = vec![schedule_outbox_drain(OUTBOX_RETRY_AFTER)];
    if pending {
        timers.push(super::witness::schedule_witness_drain(Duration::from_secs(
            1,
        )));
    }
    for timer in timers {
        if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) =
            task_handle.send_effect(timer).await
        {
            warn!(message = %message, "Failed to restore a job lifecycle timer");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, node};
    use aruna_core::types::Value;

    #[test]
    fn drops_refused_record() {
        // Holders that stand behind refusing a record must not keep it queued
        // for every drain pass forever.
        let fixture = Family::new([12u8; 32]);
        let envelope = fixture.sign(
            &fixture.holder,
            aruna_core::structs::execution::job::JobFamilyRecord::Spec(Box::new(fixture.spec())),
        );
        let key = envelope.key();
        let frame = JobRecordFrame::new(envelope).expect("record frame");
        let mut operation = PublishRecordOperation::new(PublishRecordConfig {
            realm_id: fixture.config.realm_id,
            placement: fixture.placement,
            holders: vec![node(10)],
            record: Box::new(frame),
            key,
        });
        let _ = operation.start();
        let _ = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: record_key(&key),
            value: Some(Value::from(
                to_bytes(&OutboxEntry {
                    queued_at_ms: 1,
                    delivered: Vec::new(),
                    next_holder: 0,
                    rejections: MAX_PUBLISH_REJECTIONS - 1,
                })
                .expect("outbox entry"),
            )),
        }));

        let effects = operation.step(Event::Net(NetEvent::JobRecord(JobRecordEvent::Rejected {
            holder: node(10),
            reason: aruna_core::events::JobRecordRejection::Invalid,
        })));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { .. })]
        ));
    }

    #[test]
    fn batches_all_holders() {
        // Delivery state must advance beyond one bounded network fan-out.
        let fixture = Family::new([7u8; 32]);
        let spec = fixture.spec();
        let envelope = fixture.sign(
            &fixture.holder,
            aruna_core::structs::execution::job::JobFamilyRecord::Spec(Box::new(spec)),
        );
        let key = envelope.key();
        let frame = JobRecordFrame::new(envelope).expect("record frame");
        let holders: Vec<NodeId> = (10..20).map(node).collect();
        let config = PublishRecordConfig {
            realm_id: fixture.config.realm_id,
            placement: fixture.placement,
            holders: holders.clone(),
            record: Box::new(frame),
            key,
        };

        let mut first = PublishRecordOperation::new(config.clone());
        let _ = first.start();
        let effects = first.step(Event::Storage(StorageEvent::ReadResult {
            key: record_key(&key),
            value: Some(Value::from(
                to_bytes(&OutboxEntry {
                    queued_at_ms: 1,
                    delivered: Vec::new(),
                    next_holder: 0,
                    rejections: 0,
                })
                .expect("outbox entry"),
            )),
        }));
        let [Effect::Net(NetEffect::JobRecord(effect))] = effects.as_slice() else {
            panic!("expected first publish")
        };
        let JobRecordEffect::Publish {
            holders: offered, ..
        } = effect.as_ref()
        else {
            panic!("expected publish effect")
        };
        assert_eq!(offered.as_slice(), &holders[..MAX_RECORD_HOLDERS]);
        let effects = first.step(Event::Net(NetEvent::JobRecord(
            JobRecordEvent::Unavailable("unreachable".to_string()),
        )));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected cursor write")
        };
        let entry = from_bytes::<OutboxEntry>(value).expect("outbox entry decodes");
        assert!(entry.delivered.is_empty());
        assert_eq!(entry.next_holder as usize, MAX_RECORD_HOLDERS);

        let mut second = PublishRecordOperation::new(config);
        let _ = second.start();
        let effects = second.step(Event::Storage(StorageEvent::ReadResult {
            key: record_key(&key),
            value: Some(Value::from(to_bytes(&entry).expect("outbox entry"))),
        }));
        let [Effect::Net(NetEffect::JobRecord(effect))] = effects.as_slice() else {
            panic!("expected second publish")
        };
        let JobRecordEffect::Publish {
            holders: offered, ..
        } = effect.as_ref()
        else {
            panic!("expected publish effect")
        };
        assert_eq!(
            &offered.as_slice()[..holders.len() - MAX_RECORD_HOLDERS],
            &holders[MAX_RECORD_HOLDERS..]
        );
    }

    #[test]
    fn rejects_terminal_event() {
        // A publish that settled, or never started, refuses an event instead of ignoring it.
        let fixture = Family::new([9u8; 32]);
        let envelope = fixture.sign(
            &fixture.holder,
            aruna_core::structs::execution::job::JobFamilyRecord::Spec(Box::new(fixture.spec())),
        );
        let key = envelope.key();
        let config = PublishRecordConfig {
            realm_id: fixture.config.realm_id,
            placement: fixture.placement,
            holders: vec![node(10)],
            record: Box::new(JobRecordFrame::new(envelope).expect("record frame")),
            key,
        };
        let gone = || {
            Event::Storage(StorageEvent::ReadResult {
                key: record_key(&key),
                value: None,
            })
        };

        let mut early = PublishRecordOperation::new(config.clone());
        assert!(early.step(gone()).is_empty());
        assert!(matches!(
            early.finalize(),
            Err(LifecycleError::UnexpectedEvent { .. })
        ));

        let mut settled = PublishRecordOperation::new(config);
        let _ = settled.start();
        assert!(settled.step(gone()).is_empty());
        assert!(settled.is_complete());
        assert!(settled.step(gone()).is_empty());
        assert!(matches!(
            settled.finalize(),
            Err(LifecycleError::UnexpectedEvent { .. })
        ));
    }
}
