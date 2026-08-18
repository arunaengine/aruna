//! Local admission of one submission.
//!
//! A holder commits the immutable spec and its claim in one transaction, or it
//! commits nothing: a matching claim replays the canonical alias and a claim of
//! another request under the same key is a visible conflict. The candidate
//! records are signed before this runs, so the transaction only decides.

use aruna_core::compute_quota::QuotaDenied;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, IterStart, JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    JOB_FAMILY_ALIAS_KEYSPACE, JOB_FAMILY_OUTBOX_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE,
    JOB_FAMILY_RECORD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    JobFamilyId, JobFamilyRecord, JobId, JobRecordEnvelope, JobRecordKey, LogicalJobSpec,
    RealmConfigDocument, RealmId, RecordVerdict, SubmissionClaim, SubmissionId,
};
use aruna_core::types::{Effects, Key, NodeId, TxnId, Value};
use smallvec::smallvec;
use tracing::debug;

use super::{LifecycleError, MAX_SUBMISSION_SCAN};
use crate::jobs::records::keys::{alias_key, family_prefix, record_key, submission_prefix};
use crate::jobs::records::rows::{OutboxEntry, ProjectionCache, from_bytes, to_bytes};
use crate::jobs::records::verify::{Evidence, FamilyView};
use crate::jobs::records::{RECORD_PAGE_SIZE, RecordStoreError};

/// The alias this node would mint, with the records it already signed for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionCandidate {
    pub job_id: JobId,
    pub spec: JobRecordFrame,
    pub claim: JobRecordFrame,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmitSubmissionConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub candidate: Box<AdmissionCandidate>,
    pub now_ms: u64,
    /// A standing-quota refusal evaluated before the transaction. It applies
    /// only to a fresh admission: a replayed claim settles before this check.
    pub quota_refusal: Option<QuotaDenied>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedSubmission {
    /// The canonical alias of this request family at this node.
    pub job_id: JobId,
    pub family: JobFamilyId,
    /// False when a matching claim was already known here.
    pub created: bool,
}

#[derive(Debug, PartialEq)]
pub struct AdmitSubmissionOperation {
    config: AdmitSubmissionConfig,
    view: Option<FamilyView>,
    claims: Vec<(JobFamilyId, SubmissionClaim)>,
    cursor: Option<JobRecordKey>,
    scanned: usize,
    cache: Option<ProjectionCache>,
    state: AdmitState,
    outcome: Option<Result<AdmittedSubmission, LifecycleError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmitState {
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

impl AdmitSubmissionOperation {
    pub fn new(config: AdmitSubmissionConfig) -> Self {
        Self {
            config,
            view: None,
            claims: Vec::new(),
            cursor: None,
            scanned: 0,
            cache: None,
            state: AdmitState::Init,
            outcome: None,
        }
    }

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.config.submission_id,
            request_digest: self.config.request_digest,
        }
    }

    fn txn(&self) -> Option<TxnId> {
        match self.state {
            AdmitState::Scan { txn_id }
            | AdmitState::ReadCache { txn_id }
            | AdmitState::Write { txn_id }
            | AdmitState::Commit { txn_id }
            | AdmitState::Cancel { txn_id } => Some(txn_id),
            _ => None,
        }
    }

    fn read_config(&mut self) -> Effects {
        self.state = AdmitState::ReadConfig;
        let config = DocumentSyncTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: config.storage_keyspace().to_string(),
            key: config.storage_key(),
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

    /// Claims of every request family under this submission, so a replay and an
    /// idempotency conflict are both decided from the same bounded scan.
    fn scan(&mut self, txn_id: TxnId) -> Effects {
        self.state = AdmitState::Scan { txn_id };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: JOB_FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: Some(submission_prefix(self.config.submission_id)),
            start: self.cursor.map(|key| IterStart::After(record_key(&key))),
            limit: RECORD_PAGE_SIZE,
            txn_id: Some(txn_id),
        })]
    }

    fn keep_claims(&mut self, values: Vec<(Key, Value)>) -> bool {
        let full = values.len() >= RECORD_PAGE_SIZE;
        self.scanned = self.scanned.saturating_add(values.len());
        for (key, value) in values {
            if let Ok(record_key) = JobRecordKey::from_bytes(&key) {
                self.cursor = Some(record_key);
            }
            let Ok(envelope) = from_bytes::<JobRecordEnvelope>(&value) else {
                continue;
            };
            if let JobFamilyRecord::Claim(claim) = &envelope.record {
                self.claims.push((envelope.family(), *claim));
            }
        }
        full && self.scanned < MAX_SUBMISSION_SCAN
    }

    /// The canonical alias of one family: the smallest claim key, exactly the
    /// order the reducer selects, so a replay never depends on arrival order.
    fn canonical(&self, family: JobFamilyId) -> Option<JobId> {
        self.claims
            .iter()
            .filter(|(claimed, _)| *claimed == family)
            .min_by_key(|(_, claim)| claim.order_key())
            .map(|(_, claim)| claim.job_id)
    }

    fn conflicting(&self) -> Option<JobId> {
        let family = self.family();
        self.claims
            .iter()
            .filter(|(claimed, _)| *claimed != family)
            .min_by_key(|(_, claim)| claim.order_key())
            .map(|(_, claim)| claim.job_id)
    }

    fn decide(&mut self, txn_id: TxnId) -> Effects {
        if let Some(job_id) = self.canonical(self.family()) {
            self.settle(job_id, false);
            return self.cancel(txn_id);
        }
        if let Some(existing_job_id) = self.conflicting() {
            self.outcome = Some(Err(LifecycleError::IdempotencyConflict { existing_job_id }));
            return self.cancel(txn_id);
        }
        if let Some(denied) = self.config.quota_refusal.take() {
            self.outcome = Some(Err(LifecycleError::QuotaDenied(denied)));
            return self.cancel(txn_id);
        }
        self.state = AdmitState::ReadCache { txn_id };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: JOB_FAMILY_PROJECTION_KEYSPACE.to_string(),
            key: family_prefix(&self.family()),
            txn_id: Some(txn_id),
        })]
    }

    /// Verifies the two records this node signed against its own local view,
    /// then commits them, the alias row, and their replication entries together.
    fn write(&mut self, txn_id: TxnId) -> Effects {
        let writes = match self.batch_writes() {
            Ok(writes) => writes,
            Err(error) => {
                self.outcome = Some(Err(error));
                return self.cancel(txn_id);
            }
        };
        self.state = AdmitState::Write { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }

    fn batch_writes(&self) -> Result<Vec<(String, Key, Value)>, LifecycleError> {
        let Some(view) = self.view.as_ref() else {
            return Err(LifecycleError::NotHolder);
        };
        if !view.holds(self.config.local_node_id) {
            return Err(LifecycleError::NotHolder);
        }
        let spec = self.config.candidate.spec.envelope();
        let claim = self.config.candidate.claim.envelope();
        let JobFamilyRecord::Spec(sealed) = &spec.record else {
            return Err(LifecycleError::NotHolder);
        };
        authentic(spec.verify(&view.context(Evidence::default(), None))?)?;
        authentic(claim.verify(&view.context(spec_evidence(sealed), None))?)?;

        let mut writes: Vec<(String, Key, Value)> = Vec::new();
        for envelope in [spec, claim] {
            let key = record_key(&envelope.key());
            writes.push((
                JOB_FAMILY_RECORD_KEYSPACE.to_string(),
                key.clone(),
                Value::from(to_bytes(envelope)?.as_slice()),
            ));
            writes.push((
                JOB_FAMILY_OUTBOX_KEYSPACE.to_string(),
                key,
                Value::from(
                    to_bytes(&OutboxEntry {
                        queued_at_ms: self.config.now_ms,
                    })?
                    .as_slice(),
                ),
            ));
        }
        writes.push((
            JOB_FAMILY_ALIAS_KEYSPACE.to_string(),
            alias_key(self.config.candidate.job_id, &self.family()),
            Value::from(claim.key().to_bytes().as_slice()),
        ));
        writes.push((
            JOB_FAMILY_PROJECTION_KEYSPACE.to_string(),
            family_prefix(&self.family()),
            Value::from(to_bytes(&ProjectionCache::invalidated(self.cache.as_ref()))?.as_slice()),
        ));
        Ok(writes)
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = AdmitState::Commit { txn_id };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn cancel(&mut self, txn_id: TxnId) -> Effects {
        self.state = AdmitState::Cancel { txn_id };
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn settle(&mut self, job_id: JobId, created: bool) {
        debug!(job_id = %job_id, created, "Submission admission decided");
        self.outcome = Some(Ok(AdmittedSubmission {
            job_id,
            family: self.family(),
            created,
        }));
    }

    fn finish(&mut self) -> Effects {
        self.state = match &self.outcome {
            Some(Ok(_)) => AdmitState::Finish,
            _ => AdmitState::Error,
        };
        smallvec![]
    }

    fn fail(&mut self, error: LifecycleError) -> Effects {
        self.outcome = Some(Err(error));
        self.state = AdmitState::Error;
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

fn spec_evidence(spec: &LogicalJobSpec) -> Evidence<'_> {
    Evidence {
        spec: Some(spec),
        ..Evidence::default()
    }
}

/// A record this node signed itself must verify as replicated authority; local
/// evidence or a missing predecessor means it may not be admitted here.
fn authentic(verdict: RecordVerdict) -> Result<(), LifecycleError> {
    match verdict {
        RecordVerdict::Authentic => Ok(()),
        _ => Err(LifecycleError::NotHolder),
    }
}

impl Operation for AdmitSubmissionOperation {
    type Output = AdmittedSubmission;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        self.read_config()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AdmitState::ReadConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    if let Err(error) = self.keep_view(value) {
                        return self.fail(error);
                    }
                    if self.view.is_none() {
                        return self.fail(LifecycleError::NotHolder);
                    }
                    self.state = AdmitState::Begin;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("realm config read", format!("{other:?}")),
            },
            AdmitState::Begin => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => self.scan(txn_id),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction start", format!("{other:?}")),
            },
            AdmitState::Scan { txn_id } => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    match self.keep_claims(values) {
                        true => self.scan(txn_id),
                        false => self.decide(txn_id),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("submission scan", format!("{other:?}")),
            },
            AdmitState::ReadCache { txn_id } => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.cache = value
                        .as_ref()
                        .and_then(|value| from_bytes::<ProjectionCache>(value).ok());
                    self.write(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("projection cache read", format!("{other:?}")),
            },
            AdmitState::Write { txn_id } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    self.settle(self.config.candidate.job_id, true);
                    self.commit(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("admission write", format!("{other:?}")),
            },
            AdmitState::Commit { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => self.finish(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction commit", format!("{other:?}")),
            },
            AdmitState::Cancel { .. } => match event {
                Event::Storage(StorageEvent::TransactionAborted { .. }) => self.finish(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction abort", format!("{other:?}")),
            },
            AdmitState::Init | AdmitState::Finish | AdmitState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AdmitState::Finish | AdmitState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn() {
            Some(txn_id) => {
                self.state = AdmitState::Error;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            LifecycleError::IdempotencyConflict { .. }
                | LifecycleError::NotHolder
                | LifecycleError::RealmConfigMissing
                | LifecycleError::Store(RecordStoreError::UnknownAlias)
        )
    }
}
