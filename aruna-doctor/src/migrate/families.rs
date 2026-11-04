//! Finds job family specs and launches stored in the alpha.134 layout, or that cannot be told
//! apart from it, and every row of their families and local runs, which the migration deletes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::jobs::LegacyInput;
use super::{Rewrites, decode_error, rewritten};
use crate::explorer::ExplorerError;
use aruna_core::compute::ExecutionTargetId;
use aruna_core::keyspaces::{
    FAMILY_ALIAS_KEYSPACE, FAMILY_RECORD_KEYSPACE, JOB_ENTRY_KEYSPACE, JOB_KEYSPACE,
    JOB_RESERVATION_KEYSPACE, RUN_CRATE_KEYSPACE,
};
use aruna_core::structs::execution::job::{
    EffectiveResources, ExecutionSpec, JobAdmissionRecord, JobFamilyId, JobFamilyRecord, JobId,
    JobPayload, JobRecord, JobRecordEnvelope, JobRetryPolicy, RunCrateStatus, SubmissionClaim,
    SubmissionId, WitnessBudgetRecord, cleanup_job_id, job_entry_prefix,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use aruna_operations::jobs::records::rows::{ConflictRecord, PendingNeed, PendingRecord};
use aruna_operations::jobs::store::prune_delete_entries;
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use ulid::Ulid;

/// Where a stored envelope names its record variant: after the 32 bytes of the realm id.
const VARIANT_AT: usize = 32;
/// Variant indexes of `JobFamilyRecord::Spec` and `JobFamilyRecord::Launch`.
const SPEC: u8 = 0;
const LAUNCH: u8 = 3;

/// A row of a family keyspace: an envelope first, then fields alpha.134 stored the same way.
pub(super) trait Held: Serialize + DeserializeOwned {
    type Tail: Serialize + DeserializeOwned;
    fn envelope(&self) -> &JobRecordEnvelope;
}

impl Held for JobRecordEnvelope {
    type Tail = ();
    fn envelope(&self) -> &JobRecordEnvelope {
        self
    }
}

impl Held for PendingRecord {
    type Tail = (PendingNeed, u64, u32);
    fn envelope(&self) -> &JobRecordEnvelope {
        &self.envelope
    }
}

impl Held for ConflictRecord {
    type Tail = ([u8; 32], u64, Option<NodeId>);
    fn envelope(&self) -> &JobRecordEnvelope {
        &self.envelope
    }
}

/// Spec digests that current specs and budgets bind by submission, and launch and spec digests
/// that receipts bind by launch.
#[derive(Default)]
pub(super) struct Evidence {
    specs: BTreeSet<(SubmissionId, [u8; 32])>,
    receipts: BTreeSet<(Ulid, [u8; 32], [u8; 32])>,
}

/// A spec or launch row: its envelope when it reads back unchanged in the current layout, and
/// its envelope when it reads back unchanged in the alpha.134 layout.
pub(super) struct Candidate {
    key: Vec<u8>,
    current: Option<JobRecordEnvelope>,
    historical: Option<OldEnvelope>,
}

/// What the migration deletes: the families of old specs and launches, with their local runs.
#[derive(Default)]
pub(super) struct Removal {
    families: BTreeSet<[u8; 64]>,
    jobs: BTreeSet<JobId>,
    executions: BTreeSet<Ulid>,
    /// Stored keys to delete, by keyspace.
    pub(super) rows: BTreeMap<String, BTreeSet<Vec<u8>>>,
    /// Run crate documents of deleted jobs, which stay.
    pub(super) run_crates: usize,
}

/// The spec and launch rows of one family keyspace; every current row adds its evidence.
pub(super) fn scan<T: Held>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    evidence: &mut Evidence,
) -> Result<Vec<Candidate>, ExplorerError> {
    let mut candidates = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        let current = exact::<T>(&value).map(|held| held.envelope().clone());
        if let Some(envelope) = &current {
            evidence.add(envelope);
        }
        if matches!(value.get(VARIANT_AT), Some(&SPEC | &LAUNCH)) {
            let historical = exact::<(OldEnvelope, T::Tail)>(&value).map(|(old, _)| old);
            candidates.push(Candidate {
                key: key.to_vec(),
                current,
                historical,
            });
        }
    }
    Ok(candidates)
}

/// The families of old specs and launches. Stops the migration before any write while one reads
/// in neither the current nor the alpha.134 layout, since then its family is unknown.
pub(super) fn old_families(
    keyspaces: &[(&str, Vec<Candidate>)],
    evidence: &Evidence,
) -> Result<Removal, ExplorerError> {
    let mut removal = Removal::default();
    let mut unread = Vec::new();
    for (name, rows) in keyspaces {
        let mut count = 0;
        for row in rows.iter().filter(|row| row.old(evidence)) {
            let Some((family, job_id)) = row.ids() else {
                count += 1;
                continue;
            };
            removal.families.insert(family.to_bytes());
            removal.jobs.insert(job_id);
            removal.add(name, &row.key);
        }
        if count > 0 {
            unread.push((*name, count));
        }
    }
    if unread.is_empty() {
        return Ok(removal);
    }
    let total: usize = unread.iter().map(|(_, count)| count).sum();
    let found: Vec<String> = unread
        .iter()
        .map(|(name, count)| format!("{name}: {count}"))
        .collect();
    Err(ExplorerError::Refused(format!(
        "{total} job family specs or launches read in neither the current nor the alpha.134 \
         layout ({}). Nothing was changed.",
        found.join(", ")
    )))
}

impl Removal {
    pub(super) fn is_empty(&self) -> bool {
        self.families.is_empty()
    }

    /// Rows whose key names a removed family at `offset`. Records and aliases also name the
    /// family's jobs and executions.
    pub(super) fn family_rows(
        &mut self,
        db: &OptimisticTxDatabase,
        name: &str,
        offset: usize,
    ) -> Result<(), ExplorerError> {
        if self.is_empty() {
            return Ok(());
        }
        let keyspace = db.keyspace(name, KeyspaceCreateOptions::default)?;
        for entry in db.read_tx().iter(&keyspace) {
            let (key, value) = entry.into_inner()?;
            let family = key.get(offset..offset + 64);
            if !family.is_some_and(|family| self.families.contains(family)) {
                continue;
            }
            match name {
                FAMILY_ALIAS_KEYSPACE => self.alias_job(&key),
                FAMILY_RECORD_KEYSPACE => self.record_jobs(&value),
                _ => {}
            }
            self.add(name, &key);
        }
        Ok(())
    }

    /// Local job rows of removed jobs and of their cleanup and run crate jobs, as a prune deletes
    /// them. Run crate status rows stay with their documents. `rewrites` are the upgraded rows.
    pub(super) fn job_rows(
        &mut self,
        db: &OptimisticTxDatabase,
        keyspace: &OptimisticTxKeyspace,
        rewrites: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), ExplorerError> {
        if self.is_empty() {
            return Ok(());
        }
        let rewrites: BTreeMap<&[u8], &[u8]> = rewrites
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect();
        let cleanups: BTreeSet<JobId> = self.jobs.iter().map(|job| cleanup_job_id(*job)).collect();
        let mut records = Vec::new();
        for entry in db.read_tx().iter(keyspace) {
            let (key, stored) = entry.into_inner()?;
            let value = rewrites.get(key.as_ref()).copied().unwrap_or(&stored);
            let record = JobRecord::from_bytes(value)
                .map_err(|error| decode_error(JOB_KEYSPACE, &key, error))?;
            let parent = match &record.payload {
                JobPayload::WriteRunCrate { for_job } => self.jobs.contains(for_job),
                _ => false,
            };
            if parent || self.jobs.contains(&record.job_id) || cleanups.contains(&record.job_id) {
                records.push(record);
            }
        }
        for record in records {
            for (name, key) in prune_delete_entries(&record) {
                if name == RUN_CRATE_KEYSPACE {
                    self.run_crates += usize::from(written_crate(db, &key)?);
                } else {
                    self.add_stored(db, &name, &key)?;
                }
            }
            let entries = db.keyspace(JOB_ENTRY_KEYSPACE, KeyspaceCreateOptions::default)?;
            for entry in db
                .read_tx()
                .prefix(&entries, job_entry_prefix(record.job_id))
            {
                self.add(JOB_ENTRY_KEYSPACE, &entry.into_inner()?.0);
            }
        }
        for execution in self.executions.clone() {
            self.add_stored(db, JOB_RESERVATION_KEYSPACE, &execution.to_bytes())?;
        }
        Ok(())
    }

    /// Rows of a family keyspace in an older shape, except the ones this removal deletes.
    pub(super) fn rewrites<Current, Legacy>(
        &self,
        db: &OptimisticTxDatabase,
        keyspace: &OptimisticTxKeyspace,
        name: &str,
    ) -> Result<Rewrites, ExplorerError>
    where
        Current: Serialize + for<'a> Deserialize<'a> + From<Legacy>,
        Legacy: for<'a> Deserialize<'a>,
    {
        let removed = self.rows.get(name);
        let mut scanned = 0;
        let mut rows = Vec::new();
        for entry in db.read_tx().iter(keyspace) {
            let (key, value) = entry.into_inner()?;
            scanned += 1;
            if removed.is_some_and(|keys| keys.contains(key.as_ref())) {
                continue;
            }
            match rewritten::<Current, Legacy>(&value) {
                Ok(Some(bytes)) => rows.push((key.to_vec(), bytes)),
                Ok(None) => {}
                Err(error) => return Err(decode_error(name, &key, error)),
            }
        }
        Ok(Rewrites { scanned, rows })
    }

    /// Whether this removal deletes `key` from `name`.
    pub(super) fn holds(&self, name: &str, key: &[u8]) -> bool {
        self.rows.get(name).is_some_and(|keys| keys.contains(key))
    }

    /// Deleted rows by keyspace.
    pub(super) fn counts(&self) -> BTreeMap<String, usize> {
        self.rows
            .iter()
            .map(|(name, keys)| (name.clone(), keys.len()))
            .collect()
    }

    pub(super) fn family_count(&self) -> usize {
        self.families.len()
    }

    fn add(&mut self, name: &str, key: &[u8]) {
        self.rows
            .entry(name.to_string())
            .or_default()
            .insert(key.to_vec());
    }

    /// Adds `key` when `name` stores it, so the counts name only real rows.
    fn add_stored(
        &mut self,
        db: &OptimisticTxDatabase,
        name: &str,
        key: &[u8],
    ) -> Result<(), ExplorerError> {
        let keyspace = db.keyspace(name, KeyspaceCreateOptions::default)?;
        if db.read_tx().get(&keyspace, key)?.is_some() {
            self.add(name, key);
        }
        Ok(())
    }

    /// An alias key starts with the job id it binds.
    fn alias_job(&mut self, key: &[u8]) {
        if let Some(Ok(id)) = key.get(..16).map(<[u8; 16]>::try_from) {
            self.jobs.insert(JobId::from_bytes(id));
        }
    }

    /// Claims name a logical job; receipts name the physical job and execution on this node.
    fn record_jobs(&mut self, value: &[u8]) {
        let Ok(envelope) = postcard::from_bytes::<JobRecordEnvelope>(value) else {
            return;
        };
        match envelope.record {
            JobFamilyRecord::Claim(claim) => {
                self.jobs.insert(claim.job_id);
            }
            JobFamilyRecord::Receipt(receipt) => {
                self.jobs.insert(receipt.physical_job_id);
                self.executions.insert(receipt.execution_id);
            }
            _ => {}
        }
    }
}

/// Whether a run crate status names a crate document that was or may have been written.
fn written_crate(db: &OptimisticTxDatabase, key: &[u8]) -> Result<bool, ExplorerError> {
    let keyspace = db.keyspace(RUN_CRATE_KEYSPACE, KeyspaceCreateOptions::default)?;
    let Some(value) = db.read_tx().get(&keyspace, key)? else {
        return Ok(false);
    };
    Ok(matches!(
        RunCrateStatus::from_bytes(&value),
        Ok(RunCrateStatus::Written { .. } | RunCrateStatus::Minted { .. })
    ))
}

impl Evidence {
    fn add(&mut self, envelope: &JobRecordEnvelope) {
        match &envelope.record {
            JobFamilyRecord::Spec(spec) if spec.verify_digest().is_ok() => {
                self.specs.insert((spec.submission_id, spec.spec_digest));
            }
            JobFamilyRecord::Budget(budget) => {
                self.specs
                    .insert((budget.submission_id, budget.source_spec_digest));
            }
            JobFamilyRecord::Receipt(receipt) => {
                let bound = (
                    receipt.launch_id,
                    receipt.launch_digest,
                    receipt.spec_digest,
                );
                self.receipts.insert(bound);
            }
            _ => {}
        }
    }
}

impl Candidate {
    /// A spec must reproduce its own digest. A launch that also reads as alpha.134 must match a
    /// current spec, budget or receipt, since shifted fields would not.
    fn old(&self, evidence: &Evidence) -> bool {
        let Some(envelope) = &self.current else {
            return true;
        };
        match &envelope.record {
            JobFamilyRecord::Spec(spec) => spec.verify_digest().is_err(),
            JobFamilyRecord::Launch(launch) if self.historical.is_some() => {
                let spec = (launch.submission_id, launch.spec_digest);
                let receipt = envelope.record.digest().is_ok_and(|digest| {
                    let bound = (launch.launch_id, digest, launch.spec_digest);
                    evidence.receipts.contains(&bound)
                });
                !evidence.specs.contains(&spec) && !receipt
            }
            _ => false,
        }
    }

    /// Family and job of the row, read as alpha.134 first. Both layouts store them before the
    /// fields they differ in.
    fn ids(&self) -> Option<(JobFamilyId, JobId)> {
        let old = self.historical.as_ref().and_then(|old| match &old.record {
            OldRecord::Spec(spec) => Some((spec.submission_id, spec.request_digest, spec.job_id)),
            OldRecord::Launch(launch) => {
                Some((launch.submission_id, launch.request_digest, launch.job_id))
            }
            OldRecord::Claim(_) | OldRecord::Budget(_) => None,
        });
        let current = || {
            self.current
                .as_ref()
                .and_then(|envelope| match &envelope.record {
                    JobFamilyRecord::Spec(spec) => {
                        Some((spec.submission_id, spec.request_digest, spec.job_id))
                    }
                    JobFamilyRecord::Launch(launch) => {
                        Some((launch.submission_id, launch.request_digest, launch.job_id))
                    }
                    _ => None,
                })
        };
        let (submission_id, request_digest, job_id) = old.or_else(current)?;
        let family = JobFamilyId {
            submission_id,
            request_digest,
        };
        Some((family, job_id))
    }
}

/// `T` read from `value` when it encodes back to the same bytes.
fn exact<T: Serialize + DeserializeOwned>(value: &[u8]) -> Option<T> {
    let decoded = postcard::from_bytes::<T>(value).ok()?;
    let bytes = postcard::to_allocvec(&decoded).ok()?;
    (bytes == value).then_some(decoded)
}

/// `PlannedInput` as alpha.134 stored it, before the content identity.
#[derive(Serialize, Deserialize)]
struct OldPlanned {
    destination_key: String,
    version_id: Ulid,
    blake3: [u8; 32],
    bytes: u64,
    policies: Vec<PlacementPolicyRef>,
    source_node_id: Option<NodeId>,
    transfer_ms: u64,
    known_link: bool,
}

/// `LaunchIntent` as alpha.134 stored it, before input pages.
#[derive(Serialize, Deserialize)]
struct OldLaunch {
    launch_id: Ulid,
    submission_id: SubmissionId,
    request_digest: [u8; 32],
    job_id: JobId,
    scheduler_node_id: NodeId,
    scheduler_seq: u32,
    witness_placement: PlacementRef,
    holder_generation: u64,
    target: ExecutionTargetId,
    inputs: Vec<OldPlanned>,
    output_policies: Vec<PlacementPolicyRef>,
    plan_digest: [u8; 32],
    spec_digest: [u8; 32],
    created_at_ms: u64,
}

/// `LogicalJobSpec` as alpha.134 stored it, before input pages and the input content identity.
#[derive(Serialize, Deserialize)]
struct OldSpec {
    submission_id: SubmissionId,
    job_id: JobId,
    origin_node_id: NodeId,
    ingress_node_id: NodeId,
    realm_id: RealmId,
    group_id: GroupId,
    created_by: UserId,
    created_at_ms: u64,
    payload: ExecutionSpec,
    request_digest: [u8; 32],
    spec_digest: [u8; 32],
    resources: EffectiveResources,
    retention_ms: u64,
    retry: JobRetryPolicy,
    admission: JobAdmissionRecord,
    captured_inputs: Vec<LegacyInput>,
    output_policies: Vec<PlacementPolicyRef>,
    placement: PlacementRef,
}

/// `JobFamilyRecord` up to its launch; only specs and launches are read in this layout.
#[derive(Serialize, Deserialize)]
enum OldRecord {
    Spec(Box<OldSpec>),
    Claim(SubmissionClaim),
    Budget(WitnessBudgetRecord),
    Launch(Box<OldLaunch>),
}

#[derive(Serialize, Deserialize)]
struct OldEnvelope {
    realm_id: RealmId,
    record: OldRecord,
    published_by: NodeId,
    signature: iroh::Signature,
}
