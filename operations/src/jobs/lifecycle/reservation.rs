//! Holds exact local capacity for one accepted execution and releases it again.
//! Reservation, receipt and record commit together, so concurrent offers cannot oversubscribe.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::ResourceEnvelope;
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    FAMILY_OUTBOX_KEYSPACE, FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE,
    JOB_RESERVATION_KEYSPACE, RUN_LINK_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::execution::job::{
    EffectiveResources, JobFamilyRecord, JobId, JobPayload, JobRecord, LaunchIntent, RecordVerdict,
    RunLink, run_crate_key,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Key, Value};

use tracing::{debug, warn};
use ulid::Ulid;

use super::{LifecycleError, stored_view};
use crate::jobs::records::verify::{Evidence, FamilyView};
use aruna_core::structs::execution::job::job_insert_entries;
use aruna_core::structs::execution::job::record_keys::{family_prefix, record_key};
use aruna_core::structs::execution::job::record_rows::{
    OutboxEntry, ProjectionCache, from_bytes, to_bytes,
};

/// Reservations one capacity decision reads. A node cannot run more concurrent
/// executions than this without its backend ceiling stopping it first.
pub const MAX_RESERVATION_SCAN: usize = 512;

pub fn reservation_key(execution_id: Ulid) -> Key {
    Key::from(execution_id.to_bytes().as_slice())
}

/// Whether one more execution fits the backend's static ceilings. An unmeasured
/// ceiling never filters, and a zero concurrency ceiling admits nothing.
pub fn fits(
    held: &[JobReservationRecord],
    request: &EffectiveResources,
    envelope: &ResourceEnvelope,
) -> bool {
    let cpu: u64 = held
        .iter()
        .map(|row| u64::from(row.resources.cpu_cores))
        .sum::<u64>()
        + u64::from(request.cpu_cores);
    let ram: u64 = held
        .iter()
        .map(|row| row.resources.ram_bytes)
        .sum::<u64>()
        .saturating_add(request.ram_bytes);
    let disk: u64 = held
        .iter()
        .map(|row| row.resources.disk_bytes)
        .sum::<u64>()
        .saturating_add(request.disk_bytes);
    let concurrent = held.len() as u64 + 1;
    envelope
        .max_cpu_cores
        .is_none_or(|max| cpu <= u64::from(max))
        && envelope.max_ram_bytes.is_none_or(|max| ram <= max)
        && envelope.max_disk_bytes.is_none_or(|max| disk <= max)
        && envelope
            .max_concurrent
            .is_none_or(|max| concurrent <= u64::from(max))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveExecutionConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub envelope: ResourceEnvelope,
    /// The receipt this node signed for the exact launch it is accepting.
    pub receipt: JobRecordFrame,
    pub launch: Box<LaunchIntent>,
    pub job_id: JobId,
    pub logical_job_id: JobId,
    pub execution_id: Ulid,
    pub resources: EffectiveResources,
    /// Execution site the receipt stored, kept so the local attempt can fence
    /// itself against a subject this node no longer advertises.
    pub subject_generation: u64,
    pub subject_digest: [u8; 32],
    pub record: Box<JobRecord>,
    pub now_ms: u64,
}

/// Reserves exact capacity and makes the signed receipt durable in the same
/// transaction, or admits nothing at all. The realm view is read outside it.
#[derive(Debug, PartialEq)]
pub struct ReserveExecutionOperation {
    config: ReserveExecutionConfig,
    view: Option<FamilyView>,
    held: Vec<JobReservationRecord>,
    cache: Option<ProjectionCache>,
    state: ReserveState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReserveState {
    ReadConfig,
    Scan,
    ReadCache,
    Write,
}

type Step<T> = Result<Next<T>, LifecycleError>;

impl ReserveExecutionOperation {
    pub fn new(config: ReserveExecutionConfig) -> Self {
        Self {
            config,
            view: None,
            held: Vec::new(),
            cache: None,
            state: ReserveState::ReadConfig,
        }
    }

    fn family(&self) -> aruna_core::structs::execution::job::JobFamilyId {
        self.config.receipt.envelope().family()
    }

    fn scan(&mut self, after: Option<Key>) -> Step<Ulid> {
        self.state = ReserveState::Scan;
        Ok(Next::Call(IoCall::Page {
            space: JOB_RESERVATION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: MAX_RESERVATION_SCAN,
        }))
    }

    fn decide(&mut self) -> Step<Ulid> {
        // The same execution replaying its own reservation is not a second one.
        let replay = self
            .held
            .iter()
            .any(|row| row.execution_id == self.config.execution_id);
        if !replay && !fits(&self.held, &self.config.resources, &self.config.envelope) {
            return Err(LifecycleError::Capacity);
        }
        self.state = ReserveState::ReadCache;
        Ok(Next::Call(IoCall::Read(vec![(
            FAMILY_PROJECTION_KEYSPACE.to_string(),
            family_prefix(&self.family()),
        )])))
    }

    fn batch_writes(&self) -> Result<Vec<(String, Key, Value)>, LifecycleError> {
        let Some(view) = self.view.as_ref() else {
            return Err(LifecycleError::NotHolder);
        };
        let receipt = self.config.receipt.envelope();
        let evidence = Evidence {
            launch: Some(self.config.launch.as_ref()),
            ..Evidence::default()
        };
        if receipt.verify(&view.context(evidence, None))? != RecordVerdict::Authentic {
            return Err(LifecycleError::NotHolder);
        }
        let JobFamilyRecord::Receipt(_) = &receipt.record else {
            return Err(LifecycleError::NotHolder);
        };
        if self.config.record.job_id != self.config.job_id {
            return Err(LifecycleError::NotHolder);
        }
        let key = record_key(&receipt.key());
        let mut writes = vec![
            (
                JOB_RESERVATION_KEYSPACE.to_string(),
                reservation_key(self.config.execution_id),
                Value::from(
                    to_bytes(&JobReservationRecord {
                        execution_id: self.config.execution_id,
                        job_id: self.config.job_id,
                        logical_job_id: self.config.logical_job_id,
                        resources: self.config.resources,
                        created_at_ms: self.config.now_ms,
                        subject_generation: self.config.subject_generation,
                        subject_digest: self.config.subject_digest,
                    })?
                    .as_slice(),
                ),
            ),
            (
                FAMILY_RECORD_KEYSPACE.to_string(),
                key.clone(),
                Value::from(to_bytes(receipt)?.as_slice()),
            ),
            (
                FAMILY_OUTBOX_KEYSPACE.to_string(),
                key,
                Value::from(
                    to_bytes(&OutboxEntry {
                        queued_at_ms: self.config.now_ms,
                        delivered: Vec::new(),
                        next_holder: 0,
                        rejections: 0,
                    })?
                    .as_slice(),
                ),
            ),
            (
                FAMILY_PROJECTION_KEYSPACE.to_string(),
                family_prefix(&self.family()),
                Value::from(
                    to_bytes(&ProjectionCache::invalidated(self.cache.as_ref()))?.as_slice(),
                ),
            ),
        ];
        if let JobPayload::Execution(spec) = &self.config.record.payload {
            let link = RunLink {
                family: Some(self.family()),
                logical_job_id: Some(self.config.logical_job_id),
                group_id: spec.group_id,
                created_by: self.config.record.created_by,
                run_crate: None,
            };
            writes.push((
                RUN_LINK_KEYSPACE.to_string(),
                run_crate_key(self.config.job_id),
                Value::from(link.to_bytes()?.as_slice()),
            ));
        }
        writes.extend(
            job_insert_entries(self.config.record.as_ref())?
                .into_iter()
                .filter(|(key_space, _, _)| key_space != aruna_core::keyspaces::JOB_INDEX_KEYSPACE),
        );
        Ok(writes)
    }

    fn unexpected(&self, expected: &'static str, got: &'static str) -> LifecycleError {
        LifecycleError::unexpected(&self.state, expected, got)
    }
}

impl Operation for ReserveExecutionOperation {
    type Output = Ulid;
    type Error = LifecycleError;

    fn transaction(&self) -> bool {
        true
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            LifecycleError::Capacity
                | LifecycleError::NotHolder
                | LifecycleError::RealmConfigMissing
        )
    }

    fn first(&mut self) -> Step<Ulid> {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
            StorageEffect::Read {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                txn_id: None,
            },
        )))))
    }

    fn next(&mut self, result: IoResult) -> Step<Ulid> {
        match (self.state, result) {
            (ReserveState::ReadConfig, IoResult::Event(event)) => match *event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.view = stored_view(value, self.config.realm_id, self.family())?;
                    self.scan(None)
                }
                Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
                other => Err(self.unexpected("realm config read", other.label())),
            },
            (ReserveState::Scan, IoResult::Page { rows, next }) => {
                for (_, value) in rows {
                    self.held.push(from_bytes::<JobReservationRecord>(&value)?);
                }
                match next {
                    Some(after) => self.scan(Some(after)),
                    None => self.decide(),
                }
            }
            (ReserveState::ReadCache, result) => {
                let [value] = values::<1>(result)?;
                self.cache = value
                    .as_ref()
                    .and_then(|value| ProjectionCache::decode(value));
                if value.is_some() && self.cache.is_none() {
                    warn!("Job projection cache is unreadable");
                }
                self.state = ReserveState::Write;
                Ok(Next::Call(IoCall::Write(self.batch_writes()?)))
            }
            (ReserveState::Write, IoResult::Done) => {
                debug!(execution_id = %self.config.execution_id, "Execution capacity reserved");
                Ok(Next::Done(self.config.execution_id))
            }
            (state, other) => Err(LifecycleError::unexpected(
                &state,
                "a result of this step",
                other.label(),
            )),
        }
    }
}

/// Releases one execution's capacity once it is terminal. The read and the
/// delete share a transaction, so a still-running execution keeps its hold.
#[derive(Debug, PartialEq)]
pub struct ReleaseExecutionOperation {
    execution_id: Ulid,
    deleting: bool,
}

impl ReleaseExecutionOperation {
    pub fn new(execution_id: Ulid) -> Self {
        Self {
            execution_id,
            deleting: false,
        }
    }

    fn row(&self) -> (String, Key) {
        (
            JOB_RESERVATION_KEYSPACE.to_string(),
            reservation_key(self.execution_id),
        )
    }
}

impl Operation for ReleaseExecutionOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn transaction(&self) -> bool {
        true
    }

    fn first(&mut self) -> Step<bool> {
        Ok(Next::Call(IoCall::Read(vec![self.row()])))
    }

    fn next(&mut self, result: IoResult) -> Step<bool> {
        if self.deleting {
            return match result {
                IoResult::Done => Ok(Next::Done(true)),
                other => Err(LifecycleError::unexpected(
                    &"Delete",
                    "reservation delete result",
                    other.label(),
                )),
            };
        }
        let [value] = values::<1>(result)?;
        if value.is_none() {
            return Ok(Next::Done(false));
        }
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(vec![self.row()])))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    #[test]
    fn release_deletes_held() {
        let mut operation = ReleaseExecutionOperation::new(Ulid::from_parts(1, 1));
        assert!(operation.transaction());
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 1
        ));
        let held = IoResult::Values(vec![Some(Value::from(&[1u8][..]))]);
        assert!(matches!(
            operation.next(held),
            Ok(Next::Call(IoCall::Delete(deletes))) if deletes.len() == 1
        ));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn release_skips_missing() {
        let mut operation = ReleaseExecutionOperation::new(Ulid::from_parts(2, 2));
        operation.first().expect("reservation read");
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(false))
        );
    }

    #[test]
    fn release_rejects_result() {
        // A result this step cannot accept fails the release; the runner then
        // closes its transaction.
        let mut operation = ReleaseExecutionOperation::new(Ulid::from_parts(3, 3));
        operation.first().expect("reservation read");
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(LifecycleError::UnexpectedEvent { .. })
        ));
    }
}
