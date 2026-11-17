//! Appends one immutable job record after checking its frame, signature, and evidence.
//! All writes of one append commit together or not at all.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    FAMILY_ALIAS_KEYSPACE, FAMILY_CONFLICT_KEYSPACE, FAMILY_OUTBOX_KEYSPACE,
    FAMILY_PENDING_KEYSPACE, FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordKey, JobRecordKind, LocalExecution,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};
use tracing::debug;

use super::admit::{
    Admission, AppendPlan, FamilyState, MAX_PENDING_RECORDS, admitted_aliases, plan_append,
    relayable,
};
use super::verify::{EvidencePlan, FamilyView};
use super::{MAX_EVIDENCE_ROWS, RECORD_PAGE_SIZE, RecordStoreError};
use aruna_core::structs::execution::job::record_keys::{
    alias_key, conflict_key, family_prefix, kind_prefix, record_key,
};
use aruna_core::structs::execution::job::record_rows::{
    OutboxEntry, PendingRecord, ProjectionCache, from_bytes, to_bytes,
};

type Step = Result<Next<AppendOutcome>, RecordStoreError>;

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

/// Reads the realm view outside any transaction; every family read and write runs in one write
/// transaction, which the runner opens at the first row call and commits on success.
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
    /// The record rows of the pending batch read, in the order they were asked for.
    asked: Vec<Key>,
    outcome: Option<AppendOutcome>,
    state: AppendState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendState {
    ReadConfig,
    ReadRow,
    ScanPending,
    ReadPending,
    ReadEvidence,
    ScanKind,
    Write,
    Clear,
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
            asked: Vec::new(),
            outcome: None,
            state: AppendState::ReadConfig,
        }
    }

    fn envelope(&self) -> &JobRecordEnvelope {
        self.config.record.envelope()
    }

    fn family(&self) -> JobFamilyId {
        self.envelope().family()
    }

    /// The realm view is read outside the transaction: a config write is
    /// unrelated to this family and must not conflict every append.
    fn read_config(&mut self) -> Step {
        self.state = AppendState::ReadConfig;
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

    fn keep_view(&mut self, value: Option<Value>) -> Result<(), RecordStoreError> {
        let Some(value) = value else {
            return Err(RecordStoreError::RealmConfigMissing);
        };
        let config = RealmConfigDocument::from_bytes(&value)?;
        self.view = FamilyView::resolve(&config, self.config.realm_id, self.family());
        Ok(())
    }

    fn read_row(&mut self) -> Step {
        self.state = AppendState::ReadRow;
        let mut reads = vec![
            (
                FAMILY_RECORD_KEYSPACE.to_string(),
                record_key(&self.envelope().key()),
            ),
            (
                FAMILY_PROJECTION_KEYSPACE.to_string(),
                family_prefix(&self.family()),
            ),
        ];
        if let JobFamilyRecord::Spec(spec) = &self.envelope().record {
            reads.push(crate::groups::fence::group_fence_key(spec.group_id));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Keeps the candidate's own row and the projection cache, and checks that a spec's
    /// group still accepts writes.
    fn keep_row(&mut self, values: Vec<Option<Value>>) -> Result<(), RecordStoreError> {
        let mut values = values.into_iter();
        let row = values
            .next()
            .flatten()
            .map(|value| (record_key(&self.envelope().key()), value));
        if let Some(Some(value)) = values.next() {
            self.cache = ProjectionCache::decode(&value);
        }
        if matches!(self.envelope().record, JobFamilyRecord::Spec(_)) {
            let Some(value) = values.next() else {
                return Err(RecordStoreError::EvidenceIncomplete);
            };
            aruna_core::structs::identity::group_delete::check_group_write(value.as_deref())?;
        }
        self.keep_records(row)
    }

    fn scan_pending(&mut self) -> Step {
        self.state = AppendState::ScanPending;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_PENDING_KEYSPACE.to_string(),
            prefix: family_prefix(&self.family()),
            after: None,
            limit: MAX_PENDING_RECORDS,
        }))
    }

    /// Reads the record rows of `keys` this append has not loaded yet.
    fn read_records(&mut self, state: AppendState, keys: Vec<Key>) -> Step {
        self.state = state;
        let reads = keys
            .iter()
            .map(|key| (FAMILY_RECORD_KEYSPACE.to_string(), key.clone()))
            .collect();
        self.asked = keys;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// The rows the pending batch read found, dropping the keys that hold nothing.
    fn found(&mut self, values: Vec<Option<Value>>) -> Vec<(Key, Value)> {
        std::mem::take(&mut self.asked)
            .into_iter()
            .zip(values)
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect()
    }

    /// A pending record may already be stored under its own key by another
    /// path, so its row is read before it could ever be rewritten.
    fn read_pending(&mut self) -> Step {
        let keys: Vec<Key> = self
            .retained
            .iter()
            .filter(|(key, _)| !self.stored.contains_key(key))
            .map(|(key, _)| record_key(key))
            .collect();
        self.read_records(AppendState::ReadPending, keys)
    }

    /// The exact predecessors of every record this append judges: the candidate
    /// and each retained one. Retained records are re-judged here, so their
    /// evidence is read on every attempt, not only on first arrival.
    fn read_evidence(&mut self) -> Step {
        let mut plan = EvidencePlan::default();
        plan.extend(&self.envelope().record);
        for (_, row) in &self.retained {
            plan.extend(&row.envelope.record);
        }
        self.scans = plan.kinds.into_iter().collect();
        let keys: Vec<Key> = plan
            .keys
            .iter()
            .filter(|key| !self.stored.contains_key(key))
            .map(record_key)
            .collect();
        self.read_records(AppendState::ReadEvidence, keys)
    }

    /// Pages one whole record kind. A predecessor selected by digest or by an
    /// id no key carries is absent only once its kind has been read to the end,
    /// so this follows the continuation instead of stopping at one page.
    fn scan_kind(&mut self) -> Step {
        let Some(kind) = self.scans.front().copied() else {
            return self.plan_writes();
        };
        self.state = AppendState::ScanKind;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: kind_prefix(&self.family(), kind),
            after: self.cursor.clone(),
            limit: RECORD_PAGE_SIZE,
        }))
    }

    /// Continues the current kind, moves to the next one, or plans the writes.
    /// Reaching the row budget with pages left fails closed: a partial evidence
    /// set would read as a missing predecessor.
    fn next_page(&mut self, next: Option<Key>) -> Step {
        match next {
            Some(_) if self.scanned >= MAX_EVIDENCE_ROWS => {
                Err(RecordStoreError::EvidenceIncomplete)
            }
            Some(cursor) => {
                self.cursor = Some(cursor);
                self.scan_kind()
            }
            None => {
                self.cursor = None;
                self.scans.pop_front();
                self.scan_kind()
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

    /// Builds the plan and emits its writes. A no-op admission writes nothing, so its
    /// transaction commits only reads.
    fn plan_writes(&mut self) -> Step {
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
        let writes = self.batch_writes(&plan)?;
        self.plan = Some(plan);
        let outcome = self.settle(admission);
        if writes.is_empty() {
            return Ok(Next::Done(outcome));
        }
        self.outcome = Some(outcome);
        self.state = AppendState::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn batch_writes(
        &self,
        plan: &AppendPlan,
    ) -> Result<Vec<(String, Key, Value)>, RecordStoreError> {
        let mut writes: Vec<(String, Key, Value)> = Vec::new();
        for admitted in &plan.admitted {
            let key = admitted.envelope.key();
            writes.push((
                FAMILY_RECORD_KEYSPACE.to_string(),
                record_key(&key),
                Value::from(to_bytes(&admitted.envelope)?.as_slice()),
            ));
        }
        for (claim_key, job_id) in admitted_aliases(plan) {
            writes.push((
                FAMILY_ALIAS_KEYSPACE.to_string(),
                alias_key(job_id, &claim_key.family),
                Value::from(claim_key.to_bytes().as_slice()),
            ));
        }
        for key in relayable(plan, self.config.local_node_id) {
            writes.push((
                FAMILY_OUTBOX_KEYSPACE.to_string(),
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
                FAMILY_PENDING_KEYSPACE.to_string(),
                record_key(key),
                Value::from(to_bytes(row)?.as_slice()),
            ));
        }
        for (key, row) in &plan.conflicts {
            writes.push((
                FAMILY_CONFLICT_KEYSPACE.to_string(),
                conflict_key(key, &row.envelope.digest()?),
                Value::from(to_bytes(row)?.as_slice()),
            ));
        }
        if !plan.admitted.is_empty() {
            writes.push((
                FAMILY_PROJECTION_KEYSPACE.to_string(),
                family_prefix(&self.family()),
                Value::from(
                    to_bytes(&ProjectionCache::invalidated(self.cache.as_ref()))?.as_slice(),
                ),
            ));
        }
        Ok(writes)
    }

    fn clear_pending(&mut self) -> Step {
        let deletes: Vec<(String, Key)> = self
            .plan
            .as_ref()
            .map(|plan| {
                plan.cleared
                    .iter()
                    .map(|key| (FAMILY_PENDING_KEYSPACE.to_string(), record_key(key)))
                    .collect()
            })
            .unwrap_or_default();
        self.state = AppendState::Clear;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn settle(&self, admission: Admission) -> AppendOutcome {
        let admitted = self.plan.as_ref().map_or(0, |plan| plan.admitted.len());
        let digest = self.envelope().digest().unwrap_or_default();
        debug!(
            kind = ?self.envelope().kind(),
            admission = ?admission,
            admitted,
            "Job record admission decided"
        );
        AppendOutcome {
            key: self.envelope().key(),
            digest,
            deferred: matches!(
                admission,
                Admission::Pending(
                    aruna_core::structs::execution::job::record_rows::PendingNeed::LocalView
                )
            ),
            admission,
            admitted,
        }
    }

    fn unexpected(&self, expected: &'static str, got: &'static str) -> RecordStoreError {
        RecordStoreError::unexpected(&self.state, expected, got)
    }
}

impl Operation for AppendRecordOperation {
    type Output = AppendOutcome;
    type Error = RecordStoreError;

    fn transaction(&self) -> bool {
        true
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            RecordStoreError::RealmConfigMissing | RecordStoreError::UnknownAlias
        )
    }

    fn first(&mut self) -> Step {
        self.read_config()
    }

    fn next(&mut self, result: IoResult) -> Step {
        match (self.state, result) {
            (AppendState::ReadConfig, IoResult::Event(event)) => match *event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.keep_view(value)?;
                    self.read_row()
                }
                Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
                other => Err(self.unexpected("realm config read", other.label())),
            },
            (AppendState::ReadRow, IoResult::Values(values)) => {
                self.keep_row(values)?;
                self.scan_pending()
            }
            (AppendState::ScanPending, IoResult::Page { rows, .. }) => {
                self.keep_pending(rows);
                self.read_pending()
            }
            (AppendState::ReadPending, IoResult::Values(values)) => {
                let rows = self.found(values);
                self.keep_records(rows)?;
                self.read_evidence()
            }
            (AppendState::ReadEvidence, IoResult::Values(values)) => {
                let rows = self.found(values);
                self.keep_records(rows)?;
                self.scan_kind()
            }
            (AppendState::ScanKind, IoResult::Page { rows, next }) => {
                self.scanned = self.scanned.saturating_add(rows.len());
                self.keep_records(rows)?;
                self.next_page(next)
            }
            (AppendState::Write, IoResult::Done) => self.clear_pending(),
            (AppendState::Clear, IoResult::Done) => match self.outcome.take() {
                Some(outcome) => Ok(Next::Done(outcome)),
                None => Err(RecordStoreError::NotFinished),
            },
            (state, other) => Err(RecordStoreError::unexpected(
                &state,
                "a result of this step",
                other.label(),
            )),
        }
    }
}
