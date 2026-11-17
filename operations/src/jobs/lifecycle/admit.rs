//! Admits one submission locally, committing the spec and the claim in a single transaction.
//! A matching claim replays the alias, and a conflicting claim stays visible to the caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::quota::QuotaDenied;
use aruna_core::document::DocumentTarget;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    ADMISSION_QUOTA_KEYSPACE, FAMILY_ALIAS_KEYSPACE, FAMILY_OUTBOX_KEYSPACE,
    FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE, JOB_INDEX_KEYSPACE, JOB_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobId, JobPayload, JobRecord, JobRecordEnvelope, LogicalJobSpec,
    RecordVerdict, SubmissionClaim, SubmissionId, WorkspaceMode, job_record_key, owner_index_key,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::rows::values;
use aruna_core::types::{Key, Value};
use tracing::{debug, warn};

use super::ids::workspace_of;
use super::{LifecycleError, stored_view};
use crate::jobs::records::verify::{Evidence, FamilyView};
use crate::jobs::records::{RECORD_PAGE_SIZE, RecordStoreError};
use aruna_core::structs::execution::job::record_keys::{
    alias_key, family_prefix, record_key, submission_prefix,
};
use aruna_core::structs::execution::job::record_rows::{
    OutboxEntry, ProjectionCache, from_bytes, to_bytes,
};

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
    /// The group revision that the successful quota check observed.
    pub quota_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedSubmission {
    /// The canonical alias of this request family at this node.
    pub job_id: JobId,
    pub family: JobFamilyId,
    /// False when a matching claim was already known here.
    pub created: bool,
}

/// Reads the realm view outside any transaction; the claim scan, the quota and the writes run
/// in one write transaction, which the runner opens at the first row call.
#[derive(Debug, PartialEq)]
pub struct AdmitSubmissionOperation {
    config: AdmitSubmissionConfig,
    view: Option<FamilyView>,
    claims: Vec<(JobFamilyId, SubmissionClaim)>,
    quota_revision: u64,
    cache: Option<ProjectionCache>,
    state: AdmitState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmitState {
    ReadConfig,
    Scan,
    ReadQuota,
    ReadCache,
    Write,
}

type Step = Result<Next<AdmittedSubmission>, LifecycleError>;

impl AdmitSubmissionOperation {
    pub fn new(config: AdmitSubmissionConfig) -> Self {
        Self {
            config,
            view: None,
            claims: Vec::new(),
            quota_revision: 0,
            cache: None,
            state: AdmitState::ReadConfig,
        }
    }

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.config.submission_id,
            request_digest: self.config.request_digest,
        }
    }

    /// Claims of every request family under this submission, so a replay and an
    /// idempotency conflict are both decided from the same transaction scan.
    fn scan(&mut self, after: Option<Key>) -> Step {
        self.state = AdmitState::Scan;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: submission_prefix(self.config.submission_id),
            after,
            limit: RECORD_PAGE_SIZE,
        }))
    }

    fn keep_claims(&mut self, values: Vec<(Key, Value)>) -> Result<(), LifecycleError> {
        for (_, value) in values {
            let envelope = from_bytes::<JobRecordEnvelope>(&value)?;
            if let JobFamilyRecord::Claim(claim) = &envelope.record {
                self.claims.push((envelope.family(), *claim));
            }
        }
        Ok(())
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

    fn decide(&mut self) -> Step {
        if let Some(job_id) = self.canonical(self.family()) {
            return Ok(Next::Done(self.settle(job_id, false)));
        }
        if let Some(existing_job_id) = self.conflicting() {
            return Err(LifecycleError::IdempotencyConflict { existing_job_id });
        }
        if let Some(denied) = self.config.quota_refusal.take() {
            return Err(LifecycleError::QuotaDenied(denied));
        }
        let JobFamilyRecord::Spec(spec) = &self.config.candidate.spec.envelope().record else {
            return Err(LifecycleError::NotHolder);
        };
        let quota = (
            ADMISSION_QUOTA_KEYSPACE.to_string(),
            Key::from(spec.group_id.to_bytes().as_slice()),
        );
        let fence = crate::groups::fence::group_fence_key(spec.group_id);
        self.state = AdmitState::ReadQuota;
        Ok(Next::Call(IoCall::Read(vec![quota, fence])))
    }

    /// Checks the group still accepts writes and that the quota revision is the one the
    /// caller's check observed, then reads the projection cache.
    fn read_cache(&mut self, result: IoResult) -> Step {
        let [quota, fence] = values::<2>(result)?;
        crate::groups::fence::check_group_write(fence.as_deref())?;
        self.quota_revision = match quota {
            Some(value) => from_bytes::<u64>(&value)?,
            None => 0,
        };
        if self
            .config
            .quota_revision
            .is_some_and(|expected| expected != self.quota_revision)
        {
            return Err(StorageError::TransactionConflict.into());
        }
        self.state = AdmitState::ReadCache;
        Ok(Next::Call(IoCall::Read(vec![(
            FAMILY_PROJECTION_KEYSPACE.to_string(),
            family_prefix(&self.family()),
        )])))
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
        let JobFamilyRecord::Spec(stored) = &spec.record else {
            return Err(LifecycleError::NotHolder);
        };
        authentic(spec.verify(&view.context(Evidence::default(), None))?)?;
        authentic(claim.verify(&view.context(spec_evidence(stored), None))?)?;

        let mut writes: Vec<(String, Key, Value)> = Vec::new();
        for envelope in [spec, claim] {
            let key = record_key(&envelope.key());
            writes.push((
                FAMILY_RECORD_KEYSPACE.to_string(),
                key.clone(),
                Value::from(to_bytes(envelope)?.as_slice()),
            ));
            writes.push((
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
            ));
        }
        writes.push((
            FAMILY_ALIAS_KEYSPACE.to_string(),
            alias_key(self.config.candidate.job_id, &self.family()),
            Value::from(claim.key().to_bytes().as_slice()),
        ));
        writes.push((
            FAMILY_PROJECTION_KEYSPACE.to_string(),
            family_prefix(&self.family()),
            Value::from(to_bytes(&ProjectionCache::invalidated(self.cache.as_ref()))?.as_slice()),
        ));
        let record = logical_record(stored);
        writes.push((
            JOB_KEYSPACE.to_string(),
            job_record_key(record.job_id),
            Value::from(record.to_bytes()?.as_slice()),
        ));
        writes.push((
            JOB_INDEX_KEYSPACE.to_string(),
            owner_index_key(record.created_by, record.created_at_ms, record.job_id),
            Value::from(Vec::<u8>::new().as_slice()),
        ));
        writes.push((
            ADMISSION_QUOTA_KEYSPACE.to_string(),
            Key::from(stored.group_id.to_bytes().as_slice()),
            Value::from(to_bytes(&self.quota_revision.saturating_add(1))?.as_slice()),
        ));
        Ok(writes)
    }

    fn settle(&self, job_id: JobId, created: bool) -> AdmittedSubmission {
        debug!(job_id = %job_id, created, "Submission admission decided");
        AdmittedSubmission {
            job_id,
            family: self.family(),
            created,
        }
    }

    fn unexpected(&self, expected: &'static str, got: &'static str) -> LifecycleError {
        LifecycleError::unexpected(&self.state, expected, got)
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

    fn transaction(&self) -> bool {
        true
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

    fn first(&mut self) -> Step {
        let config = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
            StorageEffect::Read {
                key_space: config.storage_keyspace().to_string(),
                key: config.storage_key(),
                txn_id: None,
            },
        )))))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match (self.state, result) {
            (AdmitState::ReadConfig, IoResult::Event(event)) => match *event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.view = stored_view(value, self.config.realm_id, self.family())?;
                    if self.view.is_none() {
                        return Err(LifecycleError::NotHolder);
                    }
                    self.scan(None)
                }
                Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
                other => Err(self.unexpected("realm config read", other.label())),
            },
            (AdmitState::Scan, IoResult::Page { rows, next }) => {
                self.keep_claims(rows)?;
                match next {
                    Some(after) => self.scan(Some(after)),
                    None => self.decide(),
                }
            }
            (AdmitState::ReadQuota, result) => self.read_cache(result),
            (AdmitState::ReadCache, result) => {
                let [value] = values::<1>(result)?;
                self.cache = value
                    .as_ref()
                    .and_then(|value| ProjectionCache::decode(value));
                if value.is_some() && self.cache.is_none() {
                    warn!("Job projection cache is unreadable");
                }
                // Verifies the two records this node signed against its own local view,
                // then commits them, the alias row, and their replication entries together.
                self.state = AdmitState::Write;
                Ok(Next::Call(IoCall::Write(self.batch_writes()?)))
            }
            (AdmitState::Write, IoResult::Done) => Ok(Next::Done(
                self.settle(self.config.candidate.job_id, true),
            )),
            (state, other) => Err(LifecycleError::unexpected(
                &state,
                "a result of this step",
                other.label(),
            )),
        }
    }
}

fn logical_record(spec: &LogicalJobSpec) -> JobRecord {
    let (mode, bucket) = workspace_of(&spec.payload);
    let mut record = JobRecord::new(
        spec.job_id,
        JobPayload::Execution(spec.payload.clone()),
        spec.created_by,
        spec.origin_node_id,
        spec.created_at_ms,
        spec.created_at_ms,
        None,
    );
    record.retention_ms = spec.retention_ms;
    record.workspace_mode = mode;
    record.workspace_bucket = match mode {
        WorkspaceMode::Existing => bucket,
        WorkspaceMode::None => None,
    };
    record
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::tests::records::{Family, REALM};

    // A result the current step cannot use fails the admission.
    #[test]
    fn rejects_unfit_result() {
        let family = Family::new([1u8; 32]);
        let spec = family.spec();
        let job_id = spec.job_id;
        let claim = family.claim(&spec);
        let sign = |record| {
            JobRecordFrame::new(family.sign(&family.holder, record)).expect("bounded record")
        };
        let mut operation = AdmitSubmissionOperation::new(AdmitSubmissionConfig {
            realm_id: REALM,
            local_node_id: family.holder.public(),
            submission_id: family.submission_id,
            request_digest: family.request_digest,
            candidate: Box::new(AdmissionCandidate {
                job_id,
                spec: sign(JobFamilyRecord::Spec(Box::new(spec.clone()))),
                claim: sign(JobFamilyRecord::Claim(claim.clone())),
            }),
            now_ms: 3_000,
            quota_refusal: None,
            quota_revision: None,
        });
        assert!(operation.transaction());
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(effect)))
                if matches!(*effect, Effect::Storage(StorageEffect::Read { txn_id: None, .. }))
        ));
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(LifecycleError::UnexpectedEvent { .. })
        ));
    }
}
