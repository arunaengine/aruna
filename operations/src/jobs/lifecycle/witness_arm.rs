//! Ranks the witnesses of a request family and arms this node's fallback deadline for it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::ExecutionTargetId;
use aruna_core::effects::Effect;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{DEADLINE_INDEX_KEYSPACE, WITNESS_DEADLINE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::scheduling::ExecutionPlan;
use aruna_core::structs::execution::job::record_rows::{from_bytes, to_bytes};
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordKind,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::LifecycleError;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::verify::FamilyView;

/// Domain of the stable witness order.
const WITNESS_RANK_DOMAIN: &[u8] = b"aruna-job-witness-v1";
pub(super) const DEADLINE_KEY_BYTES: usize = 8 + 64;

/// One family's persisted fallback deadline. The row exists while this node may
/// still have to launch, so a restart resumes the same schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WitnessDeadline {
    pub due_at_ms: u64,
    pub rank: u32,
}

/// The bounded plan this witness stored before it launched, with the targets
/// that already refused this request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WitnessExplain {
    pub sequence: u32,
    pub plan: ExecutionPlan,
    pub declined: Vec<ExecutionTargetId>,
    /// The previous launch was never confirmed, so this one may overlap it.
    pub overlapping: bool,
    pub stored_at_ms: u64,
}

/// Kicks the witness queue without persisting a timer of its own.
pub fn schedule_witness_drain(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::DrainWitnessQueue,
        after,
    })
}

/// Position of `node` in the witness order. The admitting node ranks first,
/// because it already holds the request; remaining witnesses follow the
/// domain-separated digest of the immutable identity, identical on every node.
pub fn witness_rank(
    holders: &[NodeId],
    family: &JobFamilyId,
    node: NodeId,
    admitting: Option<NodeId>,
) -> Option<u32> {
    let admitting = admitting.filter(|first| holders.contains(first));
    if admitting == Some(node) {
        return Some(0);
    }
    let mut ordered: Vec<([u8; 32], NodeId)> = holders
        .iter()
        .filter(|holder| Some(**holder) != admitting)
        .map(|holder| (rank_key(family, *holder), *holder))
        .collect();
    ordered.sort_unstable();
    let rank = ordered.iter().position(|(_, holder)| *holder == node)? as u32;
    Some(rank + u32::from(admitting.is_some()))
}

fn rank_key(family: &JobFamilyId, node: NodeId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(WITNESS_RANK_DOMAIN);
    hasher.update(&family.submission_id.0);
    hasher.update(&family.request_digest);
    hasher.update(node.as_bytes());
    *hasher.finalize().as_bytes()
}

/// Deterministic jitter below a quarter of the base delay, so equal ranks in
/// different families never fire in lockstep and a replay picks the same value.
fn jitter_ms(family: &JobFamilyId, node: NodeId, base_delay_ms: u64) -> u64 {
    let key = rank_key(family, node);
    let spread = base_delay_ms / 4 + 1;
    u64::from(u16::from_be_bytes([key[0], key[1]])) % spread
}

pub(super) fn deadline_key(family: &JobFamilyId, due_at_ms: u64) -> Key {
    let mut bytes = Vec::with_capacity(DEADLINE_KEY_BYTES);
    bytes.extend_from_slice(&due_at_ms.to_be_bytes());
    bytes.extend_from_slice(&family.to_bytes());
    Key::from(bytes.as_slice())
}

pub(super) fn deadline_index_key(family: &JobFamilyId) -> Key {
    Key::from(family.to_bytes().as_slice())
}

#[derive(Debug, PartialEq)]
enum ArmState {
    Config,
    Claims,
    Index,
    Write,
    Delete,
}

/// Arms this node's fallback for one family. Rank zero is due immediately; a
/// later rank waits `base_delay * rank` plus its own jitter. An earlier existing
/// deadline is kept, so re-arming can only ever bring a round forward. The deadline is read
/// and replaced in one write transaction, then the witness drain wakes.
#[derive(Debug, PartialEq)]
pub struct ArmFamilyOperation {
    family: JobFamilyId,
    local: NodeId,
    realm_id: RealmId,
    now_ms: u64,
    holders: Vec<NodeId>,
    base_delay_ms: u64,
    deadline: Option<WitnessDeadline>,
    deletes: Vec<(String, Key)>,
    state: ArmState,
}

type ArmStep = Result<Next<()>, LifecycleError>;

impl ArmFamilyOperation {
    pub fn new(family: JobFamilyId, local: NodeId, realm_id: RealmId, now_ms: u64) -> Self {
        Self {
            family,
            local,
            realm_id,
            now_ms,
            holders: Vec::new(),
            base_delay_ms: 0,
            deadline: None,
            deletes: Vec::new(),
            state: ArmState::Config,
        }
    }

    /// Ranks this node among the holders; the admitting node goes first.
    fn rank(&mut self, admitting: Option<NodeId>) -> ArmStep {
        let family = self.family;
        let Some(rank) = witness_rank(&self.holders, &family, self.local, admitting) else {
            debug!(?family, "Node is not a witness for this family");
            return Ok(Next::Done(()));
        };
        let base = self.base_delay_ms;
        let due_at_ms = match rank {
            0 => self.now_ms,
            rank => self.now_ms + base * u64::from(rank) + jitter_ms(&family, self.local, base),
        };
        self.deadline = Some(WitnessDeadline { due_at_ms, rank });
        self.state = ArmState::Index;
        let row = (
            DEADLINE_INDEX_KEYSPACE.to_string(),
            deadline_index_key(&family),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    /// Writes the new deadline unless an earlier one is already armed.
    fn replace(&mut self, existing: Option<WitnessDeadline>) -> ArmStep {
        let Some(next) = self.deadline else {
            return Ok(Next::Done(()));
        };
        if existing.is_some_and(|existing| existing.due_at_ms <= next.due_at_ms) {
            return Ok(Next::Done(()));
        }
        let family = &self.family;
        let key = deadline_key(family, next.due_at_ms);
        let value = Value::from(to_bytes(&next)?.as_slice());
        let writes = vec![
            (
                WITNESS_DEADLINE_KEYSPACE.to_string(),
                key.clone(),
                value.clone(),
            ),
            (
                DEADLINE_INDEX_KEYSPACE.to_string(),
                deadline_index_key(family),
                value,
            ),
        ];
        if let Some(previous) = existing.map(|previous| deadline_key(family, previous.due_at_ms))
            && previous != key
        {
            self.deletes
                .push((WITNESS_DEADLINE_KEYSPACE.to_string(), previous));
        }
        self.state = ArmState::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for ArmFamilyOperation {
    type Output = ();
    type Error = LifecycleError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ArmStep {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> ArmStep {
        let family = self.family;
        match std::mem::replace(&mut self.state, ArmState::Config) {
            ArmState::Config => {
                let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => config,
                    Err(error) => {
                        warn!(
                            ?family,
                            error = %error,
                            "Realm config unavailable; witness fallback not armed"
                        );
                        return Ok(Next::Done(()));
                    }
                };
                let Some(view) = FamilyView::resolve(&config, self.realm_id, family) else {
                    warn!(
                        ?family,
                        "Family placement unresolved; witness fallback not armed"
                    );
                    return Ok(Next::Done(()));
                };
                self.holders = view.holders().to_vec();
                self.base_delay_ms = config.compute.witness_delay_ms;
                self.state = ArmState::Claims;
                let claims = LoadFamilyOperation::kind(family, JobRecordKind::Claim);
                Ok(Next::Call(IoCall::child(claims)))
            }
            // The node that admitted the family: the committing node of the claim with the
            // lowest order key, which every holder reduces the same way. An unreadable
            // claim set leaves the order to the digest alone.
            ArmState::Claims => {
                let records = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()?;
                let admitting = records.ok().and_then(|records| {
                    records
                        .iter()
                        .filter_map(|envelope| match &envelope.record {
                            JobFamilyRecord::Claim(claim) => Some(claim),
                            _ => None,
                        })
                        .min_by_key(|claim| claim.order_key())
                        .map(|claim| claim.committing_node_id)
                });
                self.rank(admitting)
            }
            ArmState::Index => {
                let [value] = values::<1>(result)?;
                let existing = value.and_then(|bytes| from_bytes::<WitnessDeadline>(&bytes).ok());
                self.replace(existing)
            }
            ArmState::Write => {
                done(result)?;
                self.state = ArmState::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            ArmState::Delete => {
                done(result)?;
                let due_at_ms = self.deadline.map_or(self.now_ms, |next| next.due_at_ms);
                let after = Duration::from_millis(due_at_ms.saturating_sub(self.now_ms));
                let drain = IoCall::Effect(Box::new(schedule_witness_drain(after)));
                Ok(Next::Then {
                    output: (),
                    calls: vec![drain],
                })
            }
        }
    }
}

/// One change of a family's persisted deadline, applied only while the index row still holds the
/// deadline the drain read.
#[derive(Debug, PartialEq)]
pub(super) enum DeadlineChange {
    /// Removes the due deadline `expected` at `key` with its index row.
    Clear { key: Key, expected: WitnessDeadline },
    /// Removes a deadline row at `key` the index no longer names.
    Stale { key: Key },
    /// Moves the deadline from `expected` to `next`.
    Replace {
        expected: Option<WitnessDeadline>,
        next: WitnessDeadline,
    },
}

/// Applies one [`DeadlineChange`] in one write transaction. The answer is whether it changed
/// the rows; a deadline that moved meanwhile is left alone.
#[derive(Debug, PartialEq)]
enum ChangeState {
    Index,
    Write,
    Delete,
}

#[derive(Debug, PartialEq)]
pub(super) struct ChangeDeadlineOperation {
    family: JobFamilyId,
    change: DeadlineChange,
    deletes: Vec<(String, Key)>,
    state: ChangeState,
}

type ChangeStep = Result<Next<bool>, LifecycleError>;

impl ChangeDeadlineOperation {
    pub(super) fn new(family: JobFamilyId, change: DeadlineChange) -> Self {
        Self {
            family,
            change,
            deletes: Vec::new(),
            state: ChangeState::Index,
        }
    }

    fn apply(&mut self, current: Option<WitnessDeadline>) -> ChangeStep {
        let family = &self.family;
        let index = (
            DEADLINE_INDEX_KEYSPACE.to_string(),
            deadline_index_key(family),
        );
        let row = |key: &Key| (WITNESS_DEADLINE_KEYSPACE.to_string(), key.clone());
        match &self.change {
            DeadlineChange::Clear { key, expected } => {
                if current != Some(*expected) || deadline_key(family, expected.due_at_ms) != *key {
                    return Ok(Next::Done(false));
                }
                self.state = ChangeState::Delete;
                Ok(Next::Call(IoCall::Delete(vec![row(key), index])))
            }
            DeadlineChange::Stale { key } => {
                let named = current.map(|deadline| deadline_key(family, deadline.due_at_ms));
                if named.as_ref() == Some(key) {
                    return Ok(Next::Done(false));
                }
                self.state = ChangeState::Delete;
                Ok(Next::Call(IoCall::Delete(vec![row(key)])))
            }
            DeadlineChange::Replace { expected, next } => {
                if current != *expected {
                    return Ok(Next::Done(false));
                }
                let key = deadline_key(family, next.due_at_ms);
                let value = Value::from(to_bytes(next)?.as_slice());
                let writes = vec![
                    (
                        WITNESS_DEADLINE_KEYSPACE.to_string(),
                        key.clone(),
                        value.clone(),
                    ),
                    (index.0, index.1, value),
                ];
                let previous = expected.map(|previous| deadline_key(family, previous.due_at_ms));
                if let Some(previous) = previous.filter(|previous| *previous != key) {
                    self.deletes.push(row(&previous));
                }
                self.state = ChangeState::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
        }
    }
}

impl Operation for ChangeDeadlineOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ChangeStep {
        let index = (
            DEADLINE_INDEX_KEYSPACE.to_string(),
            deadline_index_key(&self.family),
        );
        Ok(Next::Call(IoCall::Read(vec![index])))
    }

    fn next(&mut self, result: IoResult) -> ChangeStep {
        match self.state {
            ChangeState::Index => {
                let [value] = values::<1>(result)?;
                let current = value.and_then(|bytes| from_bytes::<WitnessDeadline>(&bytes).ok());
                self.apply(current)
            }
            ChangeState::Write => {
                done(result)?;
                self.state = ChangeState::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            ChangeState::Delete => {
                done(result)?;
                Ok(Next::Done(true))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::Family;

    fn arming(existing: Option<WitnessDeadline>) -> Result<Next<()>, LifecycleError> {
        let fixture = Family::new([2u8; 32]);
        let family = fixture.family();
        let local = fixture.holder.public();
        let mut operation = ArmFamilyOperation::new(family, local, fixture.config.realm_id, 1_000);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.holders = vec![local];
        operation.base_delay_ms = 400;
        let read = operation.rank(Some(local));
        assert!(matches!(read, Ok(Next::Call(IoCall::Read(_)))));
        let value = existing.map(|deadline| Value::from(to_bytes(&deadline).unwrap().as_slice()));
        operation.next(IoResult::Values(vec![value]))
    }

    // An admitting node arms itself at once; an earlier armed deadline is kept.
    #[test]
    fn arms_earliest_deadline() {
        let Ok(Next::Call(IoCall::Write(rows))) = arming(None) else {
            panic!("a new deadline is written");
        };
        assert_eq!(rows.len(), 2);
        let armed: WitnessDeadline = from_bytes(&rows[1].2).unwrap();
        assert_eq!(
            armed,
            WitnessDeadline {
                due_at_ms: 1_000,
                rank: 0
            }
        );
        let earlier = WitnessDeadline {
            due_at_ms: 500,
            rank: 1,
        };
        assert_eq!(arming(Some(earlier)), Ok(Next::Done(())));
    }

    #[test]
    fn schedule_uses_shorten() {
        let Effect::Task(TaskEffect::ShortenTimer { key, after }) =
            schedule_witness_drain(Duration::from_secs(1))
        else {
            panic!("expected shortened witness timer")
        };
        assert_eq!(key, TaskKey::DrainWitnessQueue);
        assert_eq!(after, Duration::from_secs(1));
    }

    #[test]
    fn deadlines_ordered() {
        let family = JobFamilyId {
            submission_id: aruna_core::structs::execution::job::SubmissionId([1; 32]),
            request_digest: [2; 32],
        };
        assert!(deadline_key(&family, 1) < deadline_key(&family, 2));
    }

    fn deadline(due_at_ms: u64) -> WitnessDeadline {
        WitnessDeadline { due_at_ms, rank: 1 }
    }

    fn family() -> JobFamilyId {
        Family::new([3u8; 32]).family()
    }

    // A move writes the new rows and drops the old one; a deadline that moved meanwhile stays.
    #[test]
    fn replaces_expected_deadline() {
        use crate::jobs::store::mutation::{Rows, run_in_memory};
        let family = family();
        let mut rows = Rows::new();
        let arm = DeadlineChange::Replace {
            expected: None,
            next: deadline(20),
        };
        let operation = ChangeDeadlineOperation::new(family, arm);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(true));
        let moved = DeadlineChange::Replace {
            expected: Some(deadline(20)),
            next: deadline(10),
        };
        let operation = ChangeDeadlineOperation::new(family, moved);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(true));
        let space = WITNESS_DEADLINE_KEYSPACE.to_string();
        assert!(!rows.contains_key(&(space.clone(), deadline_key(&family, 20).to_vec())));
        assert!(rows.contains_key(&(space, deadline_key(&family, 10).to_vec())));

        let before = rows.clone();
        let stale = DeadlineChange::Replace {
            expected: Some(deadline(20)),
            next: deadline(5),
        };
        let operation = ChangeDeadlineOperation::new(family, stale);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(false));
        assert_eq!(rows, before);
    }

    // A clear removes only the deadline the index names; a stale row goes only when unnamed.
    #[test]
    fn clears_named_deadline() {
        use crate::jobs::store::mutation::{Rows, run_in_memory};
        let family = family();
        let mut rows = Rows::new();
        let arm = DeadlineChange::Replace {
            expected: None,
            next: deadline(20),
        };
        run_in_memory(ChangeDeadlineOperation::new(family, arm), &mut rows).expect("armed");
        let key = deadline_key(&family, 20);
        let named = DeadlineChange::Stale { key: key.clone() };
        let operation = ChangeDeadlineOperation::new(family, named);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(false));
        let wrong = DeadlineChange::Clear {
            key: key.clone(),
            expected: deadline(30),
        };
        let operation = ChangeDeadlineOperation::new(family, wrong);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(false));

        let clear = DeadlineChange::Clear {
            key: key.clone(),
            expected: deadline(20),
        };
        let operation = ChangeDeadlineOperation::new(family, clear);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(true));
        assert!(rows.is_empty());
        let unnamed = DeadlineChange::Stale { key };
        let operation = ChangeDeadlineOperation::new(family, unnamed);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(true));
    }
}
