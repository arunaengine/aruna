//! Replicates locally published job records to the other family holders.
//! Delivery is asynchronous and needs no quorum; an unreachable family leaves entries queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod drain;
pub mod timers;

use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{
    Effect, EffectGroup, GroupForm, JobRecordFrame, MAX_RECORD_HOLDERS, MetadataRequestEffect,
    NetEffect,
};
use aruna_core::events::{
    Event, GroupEvent, GroupResult, JobRecordRejection, MetadataAnswer, NetEvent,
};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::FAMILY_OUTBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobRecordKey;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::task::{TaskEffect, TaskKey};
use tracing::{debug, warn};

use super::LifecycleError;
use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};
use aruna_core::structs::execution::job::record_keys::record_key;
use aruna_core::structs::execution::job::record_rows::{OutboxEntry, from_bytes, to_bytes};

/// Records one drain pass delivers before it re-arms.
pub const OUTBOX_DRAIN_BATCH: usize = 32;
/// Wall-clock budget of one record publish.
const PUBLISH_DEADLINE: Duration = Duration::from_secs(10);
/// Spacing between drain passes while entries remain undeliverable.
pub const OUTBOX_RETRY_AFTER: Duration = Duration::from_secs(5);
/// Definitive refusals one record is offered through before it is dropped.
/// Unreachable holders never count: only a holder's own refusal does.
const MAX_PUBLISH_REJECTIONS: u32 = 16;

/// Kicks the outbox drain without persisting a timer of its own.
pub fn schedule_outbox_drain(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainFamilyOutbox,
        after,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecordConfig {
    pub placement: PlacementRef,
    pub holders: Vec<NodeId>,
    pub record: Box<JobRecordFrame>,
    pub key: JobRecordKey,
}

/// One queued record offered to the family holders at once. The entry is cleared only
/// after every current holder durably accepted it; an unreachable family keeps it queued.
#[derive(Debug, PartialEq)]
pub struct PublishRecordOperation {
    config: PublishRecordConfig,
    entry: OutboxEntry,
    /// The holders asked in this offer, in the order of the group's members.
    asked: Vec<NodeId>,
    /// Whether the last call clears the entry instead of storing it.
    clearing: bool,
}

/// What the asked holders answered together.
enum Answers {
    Published(Vec<NodeId>),
    Rejected(NodeId, JobRecordRejection),
    Unavailable,
}

impl PublishRecordOperation {
    pub fn new(config: PublishRecordConfig) -> Self {
        Self {
            config,
            entry: OutboxEntry {
                queued_at_ms: 0,
                delivered: Vec::new(),
                next_holder: 0,
                rejections: 0,
            },
            asked: Vec::new(),
            clearing: false,
        }
    }

    fn row_key(&self) -> aruna_core::types::Key {
        record_key(&self.config.key)
    }

    fn clear(&mut self) -> Next<bool> {
        self.clearing = true;
        Next::Call(IoCall::Delete(vec![(
            FAMILY_OUTBOX_KEYSPACE.to_string(),
            self.row_key(),
        )]))
    }

    fn store(&mut self) -> Result<Next<bool>, LifecycleError> {
        let value = to_bytes(&self.entry)?;
        Ok(Next::Call(IoCall::Write(vec![(
            FAMILY_OUTBOX_KEYSPACE.to_string(),
            self.row_key(),
            value.into(),
        )])))
    }

    /// Asks the next bounded set of holders that do not have the record yet, all at once.
    fn offer(&mut self) -> Result<Next<bool>, LifecycleError> {
        let holder_count = self.config.holders.len();
        let start = self.entry.next_holder as usize % holder_count.max(1);
        let mut pending = Vec::new();
        let mut last_index = None;
        for offset in 0..holder_count {
            let index = (start + offset) % holder_count;
            let holder = self.config.holders[index];
            if self.entry.delivered.contains(&holder) {
                continue;
            }
            pending.push(holder);
            last_index = Some(index);
            if pending.len() == MAX_RECORD_HOLDERS {
                break;
            }
        }
        if pending.is_empty() {
            return Ok(self.clear());
        }
        if let Some(index) = last_index {
            self.entry.next_holder = u32::try_from((index + 1) % holder_count).unwrap_or_default();
        }
        let message = MetadataTransportMessage::ForwardJobRecord {
            placement: self.config.placement,
            record: self.config.record.clone(),
        };
        let frame = match encode_message(&message) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(error = %error, "Job record publish frame does not encode");
                return self.store();
            }
        };
        let members = pending
            .iter()
            .map(|holder| {
                Effect::Net(NetEffect::MetadataRequest(Box::new(
                    MetadataRequestEffect {
                        peer: *holder,
                        frame: frame.clone(),
                        timeout: PUBLISH_DEADLINE,
                    },
                )))
            })
            .collect();
        self.asked = pending;
        Ok(Next::Call(IoCall::Group(EffectGroup {
            limit: self.asked.len(),
            members,
            form: GroupForm::All {
                deadline: Some(PUBLISH_DEADLINE),
            },
        })))
    }

    /// Reads the holders' answers. A holder that is not a holder of the family
    /// may simply be stale, so the next drain can resolve the current set again.
    fn answers(&self, results: Vec<GroupResult>) -> Answers {
        let mut published = Vec::new();
        let mut rejected = None;
        for (holder, result) in self.asked.iter().copied().zip(results) {
            let GroupResult::Done(event) = result else {
                continue;
            };
            let Event::Net(NetEvent::MetadataAnswer(answer)) = *event else {
                continue;
            };
            let MetadataAnswer { result, .. } = *answer;
            let bytes = match result {
                Ok(bytes) => bytes,
                Err(error) => {
                    warn!(peer = %holder, error = ?error, "Job record publish failed");
                    continue;
                }
            };
            match decode_message(&bytes) {
                Ok(MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) }) => {
                    published.push(holder);
                }
                Ok(MetadataTransportMessage::ForwardedJobRecord {
                    result: Err(JobRecordRejection::NotHolder),
                })
                | Ok(MetadataTransportMessage::ForwardedWriteUnavailable) => {}
                Ok(MetadataTransportMessage::ForwardedJobRecord {
                    result: Err(reason),
                }) => {
                    rejected.get_or_insert((holder, reason));
                }
                Ok(other) => warn!(
                    peer = %holder,
                    reply = crate::metadata::transport_message_kind(&other),
                    "Unexpected job record publish reply"
                ),
                Err(error) => {
                    warn!(peer = %holder, error = %error, "Job record publish reply does not decode")
                }
            }
        }
        match (published.is_empty(), rejected) {
            (false, _) => Answers::Published(published),
            (true, Some((holder, reason))) => Answers::Rejected(holder, reason),
            (true, None) => Answers::Unavailable,
        }
    }

    /// Notes the holders that accepted the record; clears the entry once every holder has it.
    fn delivered_to(&mut self, holders: Vec<NodeId>) -> Result<Next<bool>, LifecycleError> {
        for holder in holders {
            debug!(peer = %holder, "Job record replicated to a family holder");
            if self.config.holders.contains(&holder) && !self.entry.delivered.contains(&holder) {
                self.entry.delivered.push(holder);
            }
        }
        if self
            .config
            .holders
            .iter()
            .all(|holder| self.entry.delivered.contains(holder))
        {
            return Ok(self.clear());
        }
        self.store()
    }

    fn rejected(
        &mut self,
        holder: NodeId,
        reason: JobRecordRejection,
    ) -> Result<Next<bool>, LifecycleError> {
        warn!(peer = %holder, reason = ?reason, "Family holder refused a record");
        self.entry.rejections = self.entry.rejections.saturating_add(1);
        // A record every holder stands behind refusing will never be accepted, so it
        // stops consuming the queue.
        if self.entry.rejections >= MAX_PUBLISH_REJECTIONS {
            warn!(
                kind = ?self.config.key.kind,
                rejections = self.entry.rejections,
                "Dropping a job record the family holders refuse"
            );
            return Ok(self.clear());
        }
        self.store()
    }
}

impl Operation for PublishRecordOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn first(&mut self) -> Result<Next<bool>, LifecycleError> {
        Ok(Next::Call(IoCall::Read(vec![(
            FAMILY_OUTBOX_KEYSPACE.to_string(),
            self.row_key(),
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, LifecycleError> {
        match result {
            IoResult::Values(values) => {
                let Some(Some(value)) = values.into_iter().next() else {
                    return Ok(Next::Done(false));
                };
                let mut entry = from_bytes::<OutboxEntry>(&value)?;
                entry
                    .delivered
                    .retain(|holder| self.config.holders.contains(holder));
                self.entry = entry;
                self.offer()
            }
            IoResult::Group(GroupEvent::Finished { results }) => match self.answers(results) {
                Answers::Published(holders) => self.delivered_to(holders),
                Answers::Rejected(holder, reason) => self.rejected(holder, reason),
                Answers::Unavailable => {
                    debug!("No family holder accepted the record yet");
                    self.store()
                }
            },
            IoResult::Done => Ok(Next::Done(self.clearing)),
            _ => Err(LifecycleError::UnexpectedEvent {
                state: "publish".to_string(),
                expected: "outbox row, holder answers or a write",
                got: "another result",
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, node};
    use aruna_core::types::Value;

    fn config(family: &Family, holders: Vec<NodeId>) -> PublishRecordConfig {
        let envelope = family.sign(
            &family.holder,
            aruna_core::structs::execution::job::JobFamilyRecord::Spec(Box::new(family.spec())),
        );
        let key = envelope.key();
        PublishRecordConfig {
            placement: family.placement,
            holders,
            record: Box::new(JobRecordFrame::new(envelope).expect("record frame")),
            key,
        }
    }

    fn stored(entry: &OutboxEntry) -> IoResult {
        IoResult::Values(vec![Some(Value::from(
            to_bytes(entry).expect("outbox entry").as_slice(),
        ))])
    }

    fn entry(next_holder: u32, rejections: u32) -> OutboxEntry {
        OutboxEntry {
            queued_at_ms: 1,
            delivered: Vec::new(),
            next_holder,
            rejections,
        }
    }

    fn asked(next: Result<Next<bool>, LifecycleError>) -> Vec<NodeId> {
        let Ok(Next::Call(IoCall::Group(group))) = next else {
            panic!("expected a group of publish requests")
        };
        group
            .members
            .into_iter()
            .map(|member| match member {
                Effect::Net(NetEffect::MetadataRequest(request)) => request.peer,
                other => panic!("unexpected member {other:?}"),
            })
            .collect()
    }

    fn refused(holder: NodeId) -> GroupResult {
        let reply = MetadataTransportMessage::ForwardedJobRecord {
            result: Err(JobRecordRejection::Invalid),
        };
        GroupResult::Done(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
            MetadataAnswer {
                peer: holder,
                elapsed: Duration::ZERO,
                result: Ok(encode_message(&reply).expect("reply encodes")),
            },
        )))))
    }

    #[test]
    fn drops_refused_record() {
        // Holders that stand behind refusing a record must not keep it queued
        // for every drain pass forever.
        let family = Family::new([12u8; 32]);
        let mut operation = PublishRecordOperation::new(config(&family, vec![node(10)]));
        operation.first().expect("outbox read");
        let offer = operation.next(stored(&entry(0, MAX_PUBLISH_REJECTIONS - 1)));
        assert_eq!(asked(offer), vec![node(10)]);
        let results = vec![refused(node(10))];
        assert!(matches!(
            operation.next(IoResult::Group(GroupEvent::Finished { results })),
            Ok(Next::Call(IoCall::Delete(_)))
        ));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn batches_all_holders() {
        // Delivery state must advance beyond one bounded network fan-out.
        let family = Family::new([7u8; 32]);
        let holders: Vec<NodeId> = (10..20).map(node).collect();
        let mut first = PublishRecordOperation::new(config(&family, holders.clone()));
        first.first().expect("outbox read");
        let offer = first.next(stored(&entry(0, 0)));
        assert_eq!(asked(offer), holders[..MAX_RECORD_HOLDERS].to_vec());
        let results = (0..MAX_RECORD_HOLDERS)
            .map(|_| GroupResult::Unfinished)
            .collect();
        let Ok(Next::Call(IoCall::Write(rows))) =
            first.next(IoResult::Group(GroupEvent::Finished { results }))
        else {
            panic!("expected the outbox row write")
        };
        let stored_entry = from_bytes::<OutboxEntry>(&rows[0].2).expect("outbox entry decodes");
        assert!(stored_entry.delivered.is_empty());
        assert_eq!(stored_entry.next_holder as usize, MAX_RECORD_HOLDERS);
        assert_eq!(first.next(IoResult::Done), Ok(Next::Done(false)));

        let mut second = PublishRecordOperation::new(config(&family, holders.clone()));
        second.first().expect("outbox read");
        let offer = second.next(stored(&stored_entry));
        assert_eq!(
            asked(offer)[..holders.len() - MAX_RECORD_HOLDERS],
            holders[MAX_RECORD_HOLDERS..]
        );
    }

    #[test]
    fn rejects_unfit_result() {
        let family = Family::new([9u8; 32]);
        let mut operation = PublishRecordOperation::new(config(&family, vec![node(10)]));
        operation.first().expect("outbox read");
        assert!(matches!(
            operation.next(IoResult::Woke),
            Err(LifecycleError::UnexpectedEvent { .. })
        ));
    }
}
