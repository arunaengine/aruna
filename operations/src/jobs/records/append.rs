//! The sans-I/O append of one immutable job record.
//!
//! Every check runs before the record becomes visible: frame bounds at decode,
//! the publisher signature, the record kind's author rule against this node's
//! own holder view, the family placement derived from the submission, the
//! dependency evidence already stored here, and the canonical digest. The
//! writes of one append commit in a single transaction.

use std::collections::{BTreeMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, IterStart, JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    JOB_FAMILY_ALIAS_KEYSPACE, JOB_FAMILY_CONFLICT_KEYSPACE, JOB_FAMILY_OUTBOX_KEYSPACE,
    JOB_FAMILY_PENDING_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    JobFamilyId, JobRecordEnvelope, JobRecordKey, JobRecordKind, LocalExecution,
    RealmConfigDocument, RealmId,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use tracing::debug;

use super::admit::{
    Admission, AppendPlan, FamilyState, MAX_PENDING_RECORDS, admitted_aliases, plan_append,
    relayable,
};
use super::keys::{alias_key, conflict_key, family_prefix, kind_prefix, record_key};
use super::rows::{OutboxEntry, PendingRecord, ProjectionCache, from_bytes, to_bytes};
use super::verify::{EvidencePlan, FamilyView};
use super::{MAX_EVIDENCE_ROWS, RECORD_PAGE_SIZE, RecordStoreError};

/// The rows a batch read found, dropping the keys that hold nothing.
fn stored_rows(values: Vec<(Key, Option<Value>)>) -> impl Iterator<Item = (Key, Value)> {
    values
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
}

/// Where one candidate record came from. The transport peer is authenticated
/// separately from the envelope's publisher and is audit data only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOrigin {
    /// Published by this node itself.
    Local,
    /// Relayed by an authenticated peer, which is never its author.
    Peer(NodeId),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppendRecordConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub record: JobRecordFrame,
    /// This node's own fenced execution, when it is publishing its own output
    /// record before the replicated launch chain exists.
    pub local: Option<LocalExecution>,
    pub origin: RecordOrigin,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOutcome {
    pub key: JobRecordKey,
    pub digest: [u8; 32],
    pub admission: Admission,
    /// Records that became visible, including pending ones this append admitted.
    pub admitted: usize,
    /// True when the local holder view could not be resolved, so the record was
    /// deferred rather than judged.
    pub deferred: bool,
}

#[derive(Debug, PartialEq)]
pub struct AppendRecordOperation {
    config: AppendRecordConfig,
    view: Option<FamilyView>,
    stored: BTreeMap<JobRecordKey, JobRecordEnvelope>,
    retained: Vec<(JobRecordKey, PendingRecord)>,
    cache: Option<ProjectionCache>,
    plan: Option<AppendPlan>,
    /// Record kinds still to be paged whole, in key order.
    scans: VecDeque<JobRecordKind>,
    cursor: Option<Key>,
    scanned: usize,
    state: AppendState,
    outcome: Option<Result<AppendOutcome, RecordStoreError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendState {
    Init,
    ReadConfig,
    Begin,
    ReadRow { txn_id: TxnId },
    ScanPending { txn_id: TxnId },
    ReadPending { txn_id: TxnId },
    ReadEvidence { txn_id: TxnId },
    ScanKind { txn_id: TxnId },
    Write { txn_id: TxnId },
    Clear { txn_id: TxnId },
    Commit { txn_id: TxnId },
    Cancel { txn_id: TxnId },
    Finish,
    Error,
}

impl AppendRecordOperation {
    pub fn new(config: AppendRecordConfig) -> Self {
        Self {
            config,
            view: None,
            stored: BTreeMap::new(),
            retained: Vec::new(),
            cache: None,
            plan: None,
            scans: VecDeque::new(),
            cursor: None,
            scanned: 0,
            state: AppendState::Init,
            outcome: None,
        }
    }

    fn envelope(&self) -> &JobRecordEnvelope {
        self.config.record.envelope()
    }

    fn family(&self) -> JobFamilyId {
        self.envelope().family()
    }

    fn txn(&self) -> Option<TxnId> {
        match self.state {
            AppendState::ReadRow { txn_id }
            | AppendState::ScanPending { txn_id }
            | AppendState::ReadPending { txn_id }
            | AppendState::ReadEvidence { txn_id }
            | AppendState::ScanKind { txn_id }
            | AppendState::Write { txn_id }
            | AppendState::Clear { txn_id }
            | AppendState::Commit { txn_id }
            | AppendState::Cancel { txn_id } => Some(txn_id),
            _ => None,
        }
    }

    /// The realm view is read outside the transaction: a config write is
    /// unrelated to this family and must not conflict every append.
    fn read_config(&mut self) -> Effects {
        self.state = AppendState::ReadConfig;
        let config = DocumentSyncTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: config.storage_keyspace().to_string(),
            key: config.storage_key(),
            txn_id: None,
        })]
    }

    fn keep_view(&mut self, value: Option<Value>) -> Result<(), RecordStoreError> {
        let Some(value) = value else {
            return Err(RecordStoreError::RealmConfigMissing);
        };
        let config = RealmConfigDocument::from_bytes(&value)?;
        self.view = FamilyView::resolve(&config, self.config.realm_id, self.family());
        Ok(())
    }

    fn read_row(&mut self, txn_id: TxnId) -> Effects {
        self.state = AppendState::ReadRow { txn_id };
        let reads = vec![
            (
                JOB_FAMILY_RECORD_KEYSPACE.to_string(),
                record_key(&self.envelope().key()),
            ),
            (
                JOB_FAMILY_PROJECTION_KEYSPACE.to_string(),
                family_prefix(&self.family()),
            ),
        ];
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    fn scan_pending(&mut self, txn_id: TxnId) -> Effects {
        self.state = AppendState::ScanPending { txn_id };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: JOB_FAMILY_PENDING_KEYSPACE.to_string(),
            prefix: Some(family_prefix(&self.family())),
            start: None,
            limit: MAX_PENDING_RECORDS,
            txn_id: Some(txn_id),
        })]
    }

    /// A pending record may already be stored under its own key by another
    /// path, so its row is read before it could ever be rewritten.
    fn read_pending(&mut self, txn_id: TxnId) -> Effects {
        let reads: Vec<(String, Key)> = self
            .retained
            .iter()
            .filter(|(key, _)| !self.stored.contains_key(key))
            .map(|(key, _)| (JOB_FAMILY_RECORD_KEYSPACE.to_string(), record_key(key)))
            .collect();
        if reads.is_empty() {
            return self.read_evidence(txn_id);
        }
        self.state = AppendState::ReadPending { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    /// The exact predecessors of every record this append judges: the candidate
    /// and each retained one. Retained records are re-judged here, so their
    /// evidence is read on every attempt, not only on first arrival.
    fn read_evidence(&mut self, txn_id: TxnId) -> Effects {
        let mut plan = EvidencePlan::default();
        plan.extend(&self.envelope().record);
        for (_, row) in &self.retained {
            plan.extend(&row.envelope.record);
        }
        self.scans = plan.kinds.into_iter().collect();
        let reads: Vec<(String, Key)> = plan
            .keys
            .iter()
            .filter(|key| !self.stored.contains_key(key))
            .map(|key| (JOB_FAMILY_RECORD_KEYSPACE.to_string(), record_key(key)))
            .collect();
        if reads.is_empty() {
            return self.scan_kind(txn_id);
        }
        self.state = AppendState::ReadEvidence { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    /// Pages one whole record kind. A predecessor selected by digest or by an
    /// id no key carries is absent only once its kind has been read to the end,
    /// so this follows the continuation instead of stopping at one page.
    fn scan_kind(&mut self, txn_id: TxnId) -> Effects {
        let Some(kind) = self.scans.front().copied() else {
            return self.plan_writes(txn_id);
        };
        self.state = AppendState::ScanKind { txn_id };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: JOB_FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: Some(kind_prefix(&self.family(), kind)),
            start: self.cursor.clone().map(IterStart::After),
            limit: RECORD_PAGE_SIZE,
            txn_id: Some(txn_id),
        })]
    }

    /// Continues the current kind, moves to the next one, or plans the writes.
    /// Reaching the row budget with pages left fails closed: a partial evidence
    /// set would read as a missing predecessor.
    fn next_page(&mut self, txn_id: TxnId, next: Option<Key>) -> Effects {
        match next {
            Some(_) if self.scanned >= MAX_EVIDENCE_ROWS => {
                self.fail(RecordStoreError::EvidenceIncomplete)
            }
            Some(cursor) => {
                self.cursor = Some(cursor);
                self.scan_kind(txn_id)
            }
            None => {
                self.cursor = None;
                self.scans.pop_front();
                self.scan_kind(txn_id)
            }
        }
    }

    /// Every loaded row is authority for this admission, so an undecodable key
    /// or value fails the append instead of silently shrinking the evidence.
    fn keep_records(
        &mut self,
        values: impl IntoIterator<Item = (Key, Value)>,
    ) -> Result<(), RecordStoreError> {
        for (key, value) in values {
            let record_key = JobRecordKey::from_bytes(&key)?;
            self.stored
                .insert(record_key, from_bytes::<JobRecordEnvelope>(&value)?);
        }
        Ok(())
    }

    fn keep_pending(&mut self, values: Vec<(Key, Value)>) {
        for (key, value) in values {
            let Ok(record_key) = JobRecordKey::from_bytes(&key) else {
                continue;
            };
            if let Ok(row) = from_bytes::<PendingRecord>(&value) {
                self.retained.push((record_key, row));
            }
        }
    }

    /// Builds the plan and emits its writes. A no-op admission writes nothing
    /// and aborts the transaction instead of committing an empty batch.
    fn plan_writes(&mut self, txn_id: TxnId) -> Effects {
        let relayed = match self.config.origin {
            RecordOrigin::Local => None,
            RecordOrigin::Peer(peer) => Some(peer),
        };
        let state = FamilyState {
            view: self.view.as_ref(),
            stored: &self.stored,
            local: self.config.local.as_ref(),
            now_ms: self.config.now_ms,
        };
        let (admission, plan) =
            plan_append(&state, &self.retained, self.envelope().clone(), relayed);
        let writes = match self.batch_writes(&plan) {
            Ok(writes) => writes,
            Err(error) => return self.fail(error),
        };
        self.plan = Some(plan);
        self.settle(admission);
        if writes.is_empty() {
            return self.cancel(txn_id);
        }
        self.state = AppendState::Write { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }

    fn batch_writes(
        &self,
        plan: &AppendPlan,
    ) -> Result<Vec<(String, Key, Value)>, RecordStoreError> {
        let mut writes: Vec<(String, Key, Value)> = Vec::new();
        for admitted in &plan.admitted {
            let key = admitted.envelope.key();
            writes.push((
                JOB_FAMILY_RECORD_KEYSPACE.to_string(),
                record_key(&key),
                Value::from(to_bytes(&admitted.envelope)?.as_slice()),
            ));
        }
        for (claim_key, job_id) in admitted_aliases(plan) {
            writes.push((
                JOB_FAMILY_ALIAS_KEYSPACE.to_string(),
                alias_key(job_id, &claim_key.family),
                Value::from(claim_key.to_bytes().as_slice()),
            ));
        }
        for key in relayable(plan, self.config.local_node_id) {
            writes.push((
                JOB_FAMILY_OUTBOX_KEYSPACE.to_string(),
                record_key(&key),
                Value::from(
                    to_bytes(&OutboxEntry {
                        queued_at_ms: self.config.now_ms,
                        delivered: Vec::new(),
                        next_holder: 0,
                        rejections: 0,
                    })?
                    .as_slice(),
                ),
            ));
        }
        for (key, row) in &plan.pending {
            writes.push((
                JOB_FAMILY_PENDING_KEYSPACE.to_string(),
                record_key(key),
                Value::from(to_bytes(row)?.as_slice()),
            ));
        }
        for (key, row) in &plan.conflicts {
            writes.push((
                JOB_FAMILY_CONFLICT_KEYSPACE.to_string(),
                conflict_key(key, &row.envelope.digest()?),
                Value::from(to_bytes(row)?.as_slice()),
            ));
        }
        if !plan.admitted.is_empty() {
            writes.push((
                JOB_FAMILY_PROJECTION_KEYSPACE.to_string(),
                family_prefix(&self.family()),
                Value::from(
                    to_bytes(&ProjectionCache::invalidated(self.cache.as_ref()))?.as_slice(),
                ),
            ));
        }
        Ok(writes)
    }

    fn clear_pending(&mut self, txn_id: TxnId) -> Effects {
        let deletes: Vec<(String, Key)> = self
            .plan
            .as_ref()
            .map(|plan| {
                plan.cleared
                    .iter()
                    .map(|key| (JOB_FAMILY_PENDING_KEYSPACE.to_string(), record_key(key)))
                    .collect()
            })
            .unwrap_or_default();
        if deletes.is_empty() {
            return self.commit(txn_id);
        }
        self.state = AppendState::Clear { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })]
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = AppendState::Commit { txn_id };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn cancel(&mut self, txn_id: TxnId) -> Effects {
        self.state = AppendState::Cancel { txn_id };
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn settle(&mut self, admission: Admission) {
        let admitted = self.plan.as_ref().map_or(0, |plan| plan.admitted.len());
        let digest = self.envelope().digest().unwrap_or_default();
        debug!(
            kind = ?self.envelope().kind(),
            admission = ?admission,
            admitted,
            "Job record admission decided"
        );
        self.outcome = Some(Ok(AppendOutcome {
            key: self.envelope().key(),
            digest,
            deferred: matches!(
                admission,
                Admission::Pending(super::rows::PendingNeed::LocalView)
            ),
            admission,
            admitted,
        }));
    }

    fn finish(&mut self) -> Effects {
        self.state = match &self.outcome {
            Some(Ok(_)) => AppendState::Finish,
            _ => AppendState::Error,
        };
        smallvec![]
    }

    fn fail(&mut self, error: RecordStoreError) -> Effects {
        self.outcome = Some(Err(error));
        self.state = AppendState::Error;
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(RecordStoreError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for AppendRecordOperation {
    type Output = AppendOutcome;
    type Error = RecordStoreError;

    fn start(&mut self) -> Effects {
        self.read_config()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AppendState::ReadConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    if let Err(error) = self.keep_view(value) {
                        return self.fail(error);
                    }
                    self.state = AppendState::Begin;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("realm config read", format!("{other:?}")),
            },
            AppendState::Begin => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.read_row(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction start", format!("{other:?}")),
            },
            AppendState::ReadRow { txn_id } => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let mut values = values.into_iter();
                    let row = values
                        .next()
                        .and_then(|(key, value)| value.map(|value| (key, value)));
                    if let Some((_, Some(value))) = values.next() {
                        self.cache = ProjectionCache::decode(&value);
                    }
                    if let Err(error) = self.keep_records(row) {
                        return self.fail(error);
                    }
                    self.scan_pending(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("record row read", format!("{other:?}")),
            },
            AppendState::ScanPending { txn_id } => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    self.keep_pending(values);
                    self.read_pending(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("pending scan", format!("{other:?}")),
            },
            AppendState::ReadPending { txn_id } => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    if let Err(error) = self.keep_records(stored_rows(values)) {
                        return self.fail(error);
                    }
                    self.read_evidence(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("pending row read", format!("{other:?}")),
            },
            AppendState::ReadEvidence { txn_id } => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    if let Err(error) = self.keep_records(stored_rows(values)) {
                        return self.fail(error);
                    }
                    self.scan_kind(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("evidence row read", format!("{other:?}")),
            },
            AppendState::ScanKind { txn_id } => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    self.scanned = self.scanned.saturating_add(values.len());
                    if let Err(error) = self.keep_records(values) {
                        return self.fail(error);
                    }
                    self.next_page(txn_id, next_start_after)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("evidence scan", format!("{other:?}")),
            },
            AppendState::Write { txn_id } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.clear_pending(txn_id),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("record write", format!("{other:?}")),
            },
            AppendState::Clear { txn_id } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.commit(txn_id),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("pending clear", format!("{other:?}")),
            },
            AppendState::Commit { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => self.finish(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction commit", format!("{other:?}")),
            },
            AppendState::Cancel { .. } => match event {
                Event::Storage(StorageEvent::TransactionAborted { .. }) => self.finish(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction abort", format!("{other:?}")),
            },
            AppendState::Init | AppendState::Finish | AppendState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AppendState::Finish | AppendState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(RecordStoreError::NotFinished))
    }

    /// An open transaction is rolled back, so a partial append never becomes
    /// visible.
    fn abort(&mut self) -> Effects {
        match self.txn() {
            Some(txn_id) => {
                self.state = AppendState::Error;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            RecordStoreError::RealmConfigMissing | RecordStoreError::UnknownAlias
        )
    }
}
