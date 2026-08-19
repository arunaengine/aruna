//! Exact local capacity held for one accepted execution.
//!
//! Advertised availability is stale telemetry that only ranks a target. This is
//! the authoritative admission: the reservation, the signed receipt, and the
//! record that makes both visible commit in one transaction, so two concurrent
//! offers can never oversubscribe the same backend and no work ever starts
//! before its receipt is durable.

use aruna_core::compute::ResourceEnvelope;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, IterStart, JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    JOB_FAMILY_OUTBOX_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE,
    JOB_RESERVATION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    EffectiveResources, JobFamilyRecord, JobId, JobRecord, LaunchIntent, RealmConfigDocument,
    RealmId, RecordVerdict,
};
use aruna_core::types::{Effects, Key, NodeId, TxnId, Value};

use smallvec::smallvec;
use tracing::debug;
use ulid::Ulid;

use super::LifecycleError;
use crate::driver::DriverContext;
use crate::jobs::records::keys::{family_prefix, record_key};
use crate::jobs::records::rows::{OutboxEntry, ProjectionCache, from_bytes, to_bytes};
use crate::jobs::records::verify::{Evidence, FamilyView};
use crate::jobs::store::{iter_prefix_page, job_insert_entries};

/// Reservations one capacity decision reads. A node cannot run more concurrent
/// executions than this without its backend ceiling stopping it first.
pub const MAX_RESERVATION_SCAN: usize = 512;

/// Capacity held for one physical execution: the shared core contract, written
/// with the receipt and released at that execution's terminal state.
pub use aruna_core::compute_quota::JobReservationRecord as ExecutionReservation;

pub fn reservation_key(execution_id: Ulid) -> Key {
    Key::from(execution_id.to_bytes().as_slice())
}

/// Whether one more execution fits the backend's static ceilings. An unmeasured
/// ceiling never filters, and a zero concurrency ceiling admits nothing.
pub fn fits(
    held: &[ExecutionReservation],
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

/// Reservations this node currently holds, oldest key first.
pub async fn held_reservations(
    context: &DriverContext,
) -> Result<Vec<ExecutionReservation>, String> {
    let mut held = Vec::new();
    let mut cursor = None;
    loop {
        let (rows, next) = iter_prefix_page(
            &context.storage_handle,
            JOB_RESERVATION_KEYSPACE,
            None,
            cursor,
            MAX_RESERVATION_SCAN,
            None,
        )
        .await?;
        for (_, value) in rows {
            held.push(
                from_bytes::<ExecutionReservation>(&value).map_err(|error| error.to_string())?,
            );
        }
        match next {
            Some(next) => cursor = Some(next),
            None => return Ok(held),
        }
    }
}

/// The reservation of one job's accepted execution, so the local attempt can
/// bind the exact ExecutionId the receipt named.
pub async fn job_reservation(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<ExecutionReservation>, String> {
    Ok(held_reservations(context)
        .await?
        .into_iter()
        .find(|row| row.job_id == job_id))
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
    /// Execution site the receipt sealed, kept so the local attempt can fence
    /// itself against a subject this node no longer advertises.
    pub subject_generation: u64,
    pub subject_digest: [u8; 32],
    pub record: Box<JobRecord>,
    pub now_ms: u64,
}

/// Reserves exact capacity and makes the signed receipt durable in the same
/// transaction, or admits nothing at all.
#[derive(Debug, PartialEq)]
pub struct ReserveExecutionOperation {
    config: ReserveExecutionConfig,
    view: Option<FamilyView>,
    held: Vec<ExecutionReservation>,
    cursor: Option<Key>,
    cache: Option<ProjectionCache>,
    state: ReserveState,
    outcome: Option<Result<Ulid, LifecycleError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReserveState {
    Init,
    ReadConfig,
    Begin,
    Scan { txn_id: TxnId },
    ReadCache { txn_id: TxnId },
    Write { txn_id: TxnId },
    Commit { txn_id: TxnId },
    Cancel { txn_id: TxnId },
    Finish,
    Error,
}

impl ReserveExecutionOperation {
    pub fn new(config: ReserveExecutionConfig) -> Self {
        Self {
            config,
            view: None,
            held: Vec::new(),
            cursor: None,
            cache: None,
            state: ReserveState::Init,
            outcome: None,
        }
    }

    fn txn(&self) -> Option<TxnId> {
        match self.state {
            ReserveState::Scan { txn_id }
            | ReserveState::ReadCache { txn_id }
            | ReserveState::Write { txn_id }
            | ReserveState::Commit { txn_id }
            | ReserveState::Cancel { txn_id } => Some(txn_id),
            _ => None,
        }
    }

    fn family(&self) -> aruna_core::structs::JobFamilyId {
        self.config.receipt.envelope().family()
    }

    fn read_config(&mut self) -> Effects {
        self.state = ReserveState::ReadConfig;
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })]
    }

    fn keep_view(&mut self, value: Option<Value>) -> Result<(), LifecycleError> {
        let Some(value) = value else {
            return Err(LifecycleError::RealmConfigMissing);
        };
        let config = RealmConfigDocument::from_bytes(&value)?;
        self.view = FamilyView::resolve(&config, self.config.realm_id, self.family());
        Ok(())
    }

    fn scan(&mut self, txn_id: TxnId) -> Effects {
        self.state = ReserveState::Scan { txn_id };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: JOB_RESERVATION_KEYSPACE.to_string(),
            prefix: None,
            start: self.cursor.clone().map(IterStart::After),
            limit: MAX_RESERVATION_SCAN,
            txn_id: Some(txn_id),
        })]
    }

    fn decide(&mut self, txn_id: TxnId) -> Effects {
        // The same execution replaying its own reservation is not a second one.
        let replay = self
            .held
            .iter()
            .any(|row| row.execution_id == self.config.execution_id);
        if !replay && !fits(&self.held, &self.config.resources, &self.config.envelope) {
            self.outcome = Some(Err(LifecycleError::Capacity));
            return self.cancel(txn_id);
        }
        self.state = ReserveState::ReadCache { txn_id };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: JOB_FAMILY_PROJECTION_KEYSPACE.to_string(),
            key: family_prefix(&self.family()),
            txn_id: Some(txn_id),
        })]
    }

    fn write(&mut self, txn_id: TxnId) -> Effects {
        let writes = match self.batch_writes() {
            Ok(writes) => writes,
            Err(error) => {
                self.outcome = Some(Err(error));
                return self.cancel(txn_id);
            }
        };
        self.state = ReserveState::Write { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
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
                    to_bytes(&ExecutionReservation {
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
                JOB_FAMILY_RECORD_KEYSPACE.to_string(),
                key.clone(),
                Value::from(to_bytes(receipt)?.as_slice()),
            ),
            (
                JOB_FAMILY_OUTBOX_KEYSPACE.to_string(),
                key,
                Value::from(
                    to_bytes(&OutboxEntry {
                        queued_at_ms: self.config.now_ms,
                    })?
                    .as_slice(),
                ),
            ),
            (
                JOB_FAMILY_PROJECTION_KEYSPACE.to_string(),
                family_prefix(&self.family()),
                Value::from(
                    to_bytes(&ProjectionCache::invalidated(self.cache.as_ref()))?.as_slice(),
                ),
            ),
        ];
        writes.extend(
            job_insert_entries(self.config.record.as_ref())?
                .into_iter()
                .filter(|(key_space, _, _)| {
                    key_space != aruna_core::keyspaces::JOB_OWNER_INDEX_KEYSPACE
                }),
        );
        Ok(writes)
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = ReserveState::Commit { txn_id };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn cancel(&mut self, txn_id: TxnId) -> Effects {
        self.state = ReserveState::Cancel { txn_id };
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn finish(&mut self) -> Effects {
        self.state = match &self.outcome {
            Some(Ok(_)) => ReserveState::Finish,
            _ => ReserveState::Error,
        };
        smallvec![]
    }

    fn fail(&mut self, error: LifecycleError) -> Effects {
        self.outcome = Some(Err(error));
        self.state = ReserveState::Error;
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(LifecycleError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for ReserveExecutionOperation {
    type Output = Ulid;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        self.read_config()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReserveState::ReadConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    if let Err(error) = self.keep_view(value) {
                        return self.fail(error);
                    }
                    self.state = ReserveState::Begin;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("realm config read", format!("{other:?}")),
            },
            ReserveState::Begin => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => self.scan(txn_id),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction start", format!("{other:?}")),
            },
            ReserveState::Scan { txn_id } => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (_, value) in values {
                        match from_bytes::<ExecutionReservation>(&value) {
                            Ok(reservation) => self.held.push(reservation),
                            Err(error) => {
                                self.outcome = Some(Err(error.into()));
                                return self.cancel(txn_id);
                            }
                        }
                    }
                    match next_start_after {
                        Some(cursor) => {
                            self.cursor = Some(cursor);
                            self.scan(txn_id)
                        }
                        None => self.decide(txn_id),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("reservation scan", format!("{other:?}")),
            },
            ReserveState::ReadCache { txn_id } => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.cache = value
                        .as_ref()
                        .and_then(|value| from_bytes::<ProjectionCache>(value).ok());
                    self.write(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("projection cache read", format!("{other:?}")),
            },
            ReserveState::Write { txn_id } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    debug!(execution_id = %self.config.execution_id, "Execution capacity reserved");
                    self.outcome = Some(Ok(self.config.execution_id));
                    self.commit(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("reservation write", format!("{other:?}")),
            },
            ReserveState::Commit { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => self.finish(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction commit", format!("{other:?}")),
            },
            ReserveState::Cancel { .. } => match event {
                Event::Storage(StorageEvent::TransactionAborted { .. }) => self.finish(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction abort", format!("{other:?}")),
            },
            ReserveState::Init | ReserveState::Finish | ReserveState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReserveState::Finish | ReserveState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn() {
            Some(txn_id) => {
                self.state = ReserveState::Error;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            LifecycleError::Capacity
                | LifecycleError::NotHolder
                | LifecycleError::RealmConfigMissing
        )
    }
}

/// Releases one execution's capacity once it is terminal. The read and the
/// delete share a transaction, so a still-running execution keeps its hold.
#[derive(Debug, PartialEq)]
pub struct ReleaseExecutionOperation {
    execution_id: Ulid,
    state: ReleaseState,
    outcome: Option<Result<bool, LifecycleError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleaseState {
    Init,
    Begin,
    Read { txn_id: TxnId },
    Delete { txn_id: TxnId },
    Commit { txn_id: TxnId },
    Cancel { txn_id: TxnId },
    Finish,
    Error,
}

impl ReleaseExecutionOperation {
    pub fn new(execution_id: Ulid) -> Self {
        Self {
            execution_id,
            state: ReleaseState::Init,
            outcome: None,
        }
    }
}

impl Operation for ReleaseExecutionOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        self.state = ReleaseState::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReleaseState::Begin => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.state = ReleaseState::Read { txn_id };
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: JOB_RESERVATION_KEYSPACE.to_string(),
                        key: reservation_key(self.execution_id),
                        txn_id: Some(txn_id),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.outcome = Some(Err(error.into()));
                    self.state = ReleaseState::Error;
                    smallvec![]
                }
                _ => smallvec![],
            },
            ReleaseState::Read { txn_id } => match event {
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    self.outcome = Some(Ok(false));
                    self.state = ReleaseState::Cancel { txn_id };
                    smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::ReadResult { .. }) => {
                    self.state = ReleaseState::Delete { txn_id };
                    smallvec![Effect::Storage(StorageEffect::Delete {
                        key_space: JOB_RESERVATION_KEYSPACE.to_string(),
                        key: reservation_key(self.execution_id),
                        txn_id: Some(txn_id),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.outcome = Some(Err(error.into()));
                    self.state = ReleaseState::Cancel { txn_id };
                    smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
                }
                _ => smallvec![],
            },
            ReleaseState::Delete { txn_id } => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.outcome = Some(Ok(true));
                    self.state = ReleaseState::Commit { txn_id };
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.outcome = Some(Err(error.into()));
                    self.state = ReleaseState::Cancel { txn_id };
                    smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
                }
                _ => smallvec![],
            },
            ReleaseState::Commit { .. } | ReleaseState::Cancel { .. } => {
                self.state = match &self.outcome {
                    Some(Ok(_)) => ReleaseState::Finish,
                    _ => ReleaseState::Error,
                };
                smallvec![]
            }
            ReleaseState::Init | ReleaseState::Finish | ReleaseState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReleaseState::Finish | ReleaseState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
