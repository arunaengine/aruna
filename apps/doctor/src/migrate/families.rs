//! Finds job family specs and launches stored in the alpha.134 layout, or that cannot be told
//! apart from it, and every row of their families and local runs, which the migration deletes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::compute;
use super::jobs::LegacyInput;
use super::prune_index::family_of;
use super::session_refs;
use super::tombstones::{Remains, Stated};
use super::{Rewrites, decode_error, rewritten};
use crate::explorer::ExplorerError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    FAMILY_ALIAS_KEYSPACE, FAMILY_RECORD_KEYSPACE, JOB_ENTRY_KEYSPACE, JOB_KEYSPACE,
    JOB_RESERVATION_KEYSPACE, RUN_CRATE_KEYSPACE,
};
use aruna_core::structs::execution::job::family::{
    EffectiveResources, JobAdmissionRecord, JobFamilyId, JobFamilyRecord, JobRecordEnvelope,
    JobRetryPolicy, SubmissionClaim, SubmissionId, WitnessBudgetRecord,
};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::input::{ExecutionSpec, JobPayload};
use aruna_core::structs::execution::job::local::{
    JobRecord, RunCrateStatus, cleanup_job_id, job_entry_prefix,
};
use aruna_core::structs::execution::job::record_rows::{
    ConflictRecord, PendingNeed, PendingRecord,
};
use aruna_core::structs::execution::job::rows::prune_delete_entries;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::user::id::UserId;
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use ulid::Ulid;

/// Where a stored envelope names its record variant: after the 32 bytes of the realm id.
const VARIANT_AT: usize = 32;
/// Variant indexes of `JobFamilyRecord::Spec`, `JobFamilyRecord::Launch` and the receipt.
const SPEC: u8 = 0;
const LAUNCH: u8 = 3;
const RECEIPT: u8 = 4;

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

/// A spec, launch or receipt row: its envelope when it reads back unchanged in the current
/// layout or in the alpha.134 layout, and its ids when it reads as alpha.206 stored it.
pub(super) struct Candidate {
    key: Vec<u8>,
    current: Option<JobRecordEnvelope>,
    historical: Option<OldEnvelope>,
    unnamed: Option<(SubmissionId, [u8; 32], JobId)>,
}

/// What the migration deletes: the families of old specs and launches, with their local runs.
#[derive(Default)]
pub(super) struct Removal {
    families: BTreeSet<[u8; 64]>,
    jobs: BTreeSet<JobId>,
    executions: BTreeSet<Ulid>,
    /// What each removed family with records leaves, as a finished prune leaves it.
    remains: BTreeMap<[u8; 64], Remains>,
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
        if matches!(value.get(VARIANT_AT), Some(&SPEC | &LAUNCH | &RECEIPT)) {
            let historical = old_envelope::<(OldEnvelope, T::Tail)>(&value).map(|(old, _)| old);
            candidates.push(Candidate {
                key: key.to_vec(),
                current,
                historical,
                unnamed: compute::family_ids::<T::Tail>(&value),
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

/// The tombstone and the two prune index rows of one removed family, as keyspace, key and value.
pub(super) type TombstoneRows = [(String, Key, Value); 3];

impl Removal {
    pub(super) fn is_empty(&self) -> bool {
        self.families.is_empty()
    }

    /// Rows whose key names a removed family at `offset`. Records and aliases also name the
    /// family's jobs and executions; records must go first, so aliases find their tombstone.
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
            let family = family.and_then(|bytes| <[u8; 64]>::try_from(bytes).ok());
            let Some(family) = family.filter(|family| self.families.contains(family)) else {
                continue;
            };
            match name {
                FAMILY_ALIAS_KEYSPACE if self.alias_job(&key, &family) => continue,
                FAMILY_RECORD_KEYSPACE => {
                    self.record_jobs(&value);
                    self.remains.entry(family).or_default().record(&value);
                }
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
        // An alpha.206 job record is always among the rewrites.
        if self.is_empty() && rewrites.is_empty() {
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
            // An execution of that release goes with its family, which cannot be re-signed;
            // any other job only gains the fields it lacks and stays.
            let execution = matches!(record.payload, JobPayload::Execution(_));
            let unnamed = execution && compute::old_job(&stored).is_some();
            if unnamed
                || parent
                || self.jobs.contains(&record.job_id)
                || cleanups.contains(&record.job_id)
            {
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

    /// The rows a finished prune at `now_ms` leaves of each removed family with records.
    pub(super) fn tombstones(&self, now_ms: u64) -> Result<Vec<TombstoneRows>, ExplorerError> {
        (self.remains.iter())
            .map(|(family, remains)| remains.rows(&family_of(*family), now_ms))
            .collect()
    }

    /// An alias key starts with the job id it binds. Answers whether the alias stays, as the
    /// tombstone of a family with records keeps answering it until both expire.
    fn alias_job(&mut self, key: &[u8], family: &[u8; 64]) -> bool {
        let Some(Ok(id)) = key.get(..16).map(<[u8; 16]>::try_from) else {
            return false;
        };
        let job_id = JobId::from_bytes(id);
        self.jobs.insert(job_id);
        let Some(remains) = self.remains.get_mut(family) else {
            return false;
        };
        remains.alias(job_id);
        true
    }

    /// Claims name a logical job; receipts name the physical job and execution on this node.
    fn record_jobs(&mut self, value: &[u8]) {
        if let Some((physical_job_id, execution_id)) = compute::receipt_jobs(value) {
            self.jobs.insert(physical_job_id);
            self.executions.insert(execution_id);
            return;
        }
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
        let (submission_id, request_digest, job_id) = old.or(self.unnamed).or_else(current)?;
        let family = JobFamilyId {
            submission_id,
            request_digest,
        };
        Some((family, job_id))
    }
}

/// `T` read from `value` when it encodes back to the same bytes.
pub(super) fn exact<T: Serialize + DeserializeOwned>(value: &[u8]) -> Option<T> {
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
    target: compute::Target,
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

/// The time an alpha.134 spec, claim or launch states, and a spec's submitter.
pub(super) fn old_stated(value: &[u8]) -> Option<Stated> {
    Some(match old_envelope::<OldEnvelope>(value)?.record {
        OldRecord::Spec(spec) => (Some(spec.created_at_ms), Some(spec.created_by)),
        OldRecord::Claim(claim) => (Some(claim.accepted_at_ms), None),
        OldRecord::Budget(_) => (None, None),
        OldRecord::Launch(launch) => (Some(launch.created_at_ms), None),
    })
}

/// An alpha.134 envelope: its job payload has no backend name yet, so `ExecutionSpec` is read
/// with the fields alpha.134 stored.
fn old_envelope<T: Serialize + DeserializeOwned>(value: &[u8]) -> Option<T> {
    exact::<T>(value).or_else(|| session_refs::decode::<T>(value, compute::UNNAMED).ok())
}

#[cfg(test)]
pub(super) mod tests {
    use crate::explorer::ExplorerError;
    use crate::migrate::tests::{read, write};
    use crate::migrate::{MigrateOutput, migrate_output};
    use aruna::tasks::runner::recover_stale_jobs;
    use aruna_core::compute::ExecutionTargetId;
    use aruna_core::id::NodeId;
    use aruna_core::keyspaces::{
        ACTIVE_USER_KEYSPACE, FAMILY_ALIAS_KEYSPACE, FAMILY_OUTBOX_KEYSPACE,
        FAMILY_RECORD_KEYSPACE, JOB_INDEX_KEYSPACE, JOB_KEYSPACE, JOB_RESERVATION_KEYSPACE,
        RUN_CRATE_KEYSPACE, SCHEDULE_INDEX_KEYSPACE, WITNESS_DEADLINE_KEYSPACE,
    };
    use aruna_core::structs::execution::job::family::{
        EffectiveResources, ExecutionReceipt, JobAdmissionRecord, JobFamilyId, JobFamilyRecord,
        JobRecordEnvelope, JobRecordKey, JobRecordKind, JobRetryPolicy, LaunchIntent,
        SubmissionClaim, SubmissionId,
    };
    use aruna_core::structs::execution::job::identity::JobId;
    use aruna_core::structs::execution::job::input::{ExecutionSpec, JobPayload};
    use aruna_core::structs::execution::job::local::{JobRecord, RunCrateStatus};
    use aruna_core::structs::execution::job::rows::job_insert_entries;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::GroupId;
    use aruna_core::user::id::UserId;
    use aruna_ops_work::jobs::store::list_jobs::ListJobsOperation;
    use aruna_runner::driver::{DriverContext, drive};
    use aruna_runner::jobs::runtime::JobsRuntime;
    use aruna_storage::storage::worker::FjallStorage;
    use serde::Serialize;
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::tempdir;
    use ulid::Ulid;

    const REALM: RealmId = RealmId([4u8; 32]);
    /// The family of every alpha.134 row below.
    const OLD: JobFamilyId = JobFamilyId {
        submission_id: SubmissionId([2u8; 32]),
        request_digest: [3u8; 32],
    };
    /// A current family that shares no row with the old one.
    const KEPT: JobFamilyId = JobFamilyId {
        submission_id: SubmissionId([12u8; 32]),
        request_digest: [13u8; 32],
    };

    /// `PlannedInput` as alpha.134 stored it.
    #[derive(Serialize)]
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
    #[derive(Serialize)]
    struct OldLaunch {
        launch_id: Ulid,
        submission_id: SubmissionId,
        request_digest: [u8; 32],
        job_id: JobId,
        scheduler_node_id: NodeId,
        scheduler_seq: u32,
        witness_placement: PlacementRef,
        holder_generation: u64,
        target: OldTarget,
        inputs: Vec<OldPlanned>,
        output_policies: Vec<PlacementPolicyRef>,
        plan_digest: [u8; 32],
        spec_digest: [u8; 32],
        created_at_ms: u64,
    }

    /// `ExecutionTargetId` as alpha.134 stored it, before the backend name.
    #[derive(Serialize)]
    struct OldTarget {
        node_id: NodeId,
        executor_kind: String,
    }

    /// `ExecutionSpec` as alpha.134 stored it: the current fields up to `collision_policy`.
    struct OldExecution(ExecutionSpec);

    impl Serialize for OldExecution {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeStruct;
            let spec = &self.0;
            let mut state = serializer.serialize_struct("ExecutionSpec", 16)?;
            state.serialize_field("group_id", &spec.group_id)?;
            state.serialize_field("name", &spec.name)?;
            state.serialize_field("description", &spec.description)?;
            state.serialize_field("tags", &spec.tags)?;
            state.serialize_field("image", &spec.image)?;
            state.serialize_field("entrypoint", &spec.entrypoint)?;
            state.serialize_field("command", &spec.command)?;
            state.serialize_field("workdir", &spec.workdir)?;
            state.serialize_field("env", &spec.env)?;
            state.serialize_field("resources", &spec.resources)?;
            state.serialize_field("executor_constraint", &spec.executor_constraint)?;
            state.serialize_field("inputs", &spec.inputs)?;
            state.serialize_field("file_outputs", &spec.file_outputs)?;
            state.serialize_field("workspace_outputs", &spec.workspace_outputs)?;
            state.serialize_field("output_prefixes", &spec.output_prefixes)?;
            state.serialize_field("collision_policy", &spec.collision_policy)?;
            state.end()
        }
    }

    /// `CapturedInput` as alpha.134 stored it.
    #[derive(Serialize)]
    struct OldInput {
        destination_key: String,
        source_node_id: NodeId,
        version_id: Ulid,
        blake3: [u8; 32],
        bytes: u64,
        policies: Vec<PlacementPolicyRef>,
    }

    /// `LogicalJobSpec` as alpha.134 stored it, before input pages.
    #[derive(Serialize)]
    struct OldSpec {
        submission_id: SubmissionId,
        job_id: JobId,
        origin_node_id: NodeId,
        ingress_node_id: NodeId,
        realm_id: RealmId,
        group_id: GroupId,
        created_by: UserId,
        created_at_ms: u64,
        payload: OldExecution,
        request_digest: [u8; 32],
        spec_digest: [u8; 32],
        resources: EffectiveResources,
        retention_ms: u64,
        retry: JobRetryPolicy,
        admission: JobAdmissionRecord,
        captured_inputs: Vec<OldInput>,
        output_policies: Vec<PlacementPolicyRef>,
        placement: PlacementRef,
    }

    /// `JobFamilyRecord` up to its launch variant; claims and budgets are never written here.
    #[derive(Serialize)]
    #[allow(dead_code)]
    enum OldRecord {
        Spec(Box<OldSpec>),
        Claim(()),
        Budget(()),
        Launch(Box<OldLaunch>),
    }

    #[derive(Serialize)]
    struct OldEnvelope {
        realm_id: RealmId,
        record: OldRecord,
        published_by: NodeId,
        signature: iroh::Signature,
    }

    fn secret() -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[7u8; 32])
    }

    fn user() -> UserId {
        UserId::new(Ulid::from_bytes([11u8; 16]), REALM)
    }

    fn target() -> ExecutionTargetId {
        ExecutionTargetId {
            node_id: secret().public(),
            backend: "kubernetes".to_string(),
            executor_kind: "kubernetes".to_string(),
        }
    }

    fn execution() -> ExecutionSpec {
        ExecutionSpec {
            group_id: Ulid::from_bytes([14u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
            backend: None,
        }
    }

    fn old_launch(seed: u8) -> OldLaunch {
        OldLaunch {
            launch_id: Ulid::from_bytes([seed; 16]),
            submission_id: OLD.submission_id,
            request_digest: OLD.request_digest,
            job_id: JobId::from_bytes([5u8; 16]),
            scheduler_node_id: secret().public(),
            scheduler_seq: 1,
            witness_placement: PlacementRef::NIL,
            holder_generation: 1,
            target: OldTarget {
                node_id: secret().public(),
                executor_kind: "kubernetes".to_string(),
            },
            inputs: vec![OldPlanned {
                destination_key: "inputs/table.csv".to_string(),
                version_id: Ulid::from_bytes([6u8; 16]),
                blake3: [7u8; 32],
                bytes: 42,
                policies: Vec::new(),
                source_node_id: None,
                transfer_ms: 0,
                known_link: false,
            }],
            output_policies: Vec::new(),
            plan_digest: [8u8; 32],
            spec_digest: [9u8; 32],
            created_at_ms: 1_000,
        }
    }

    fn old_spec() -> OldSpec {
        let resources = EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 1024,
            disk_bytes: 0,
            max_walltime_ms: 60_000,
            preemptible: false,
        };
        let group_id = Ulid::from_bytes([14u8; 16]);
        let job_id = JobId::from_bytes([5u8; 16]);
        OldSpec {
            submission_id: OLD.submission_id,
            job_id,
            origin_node_id: secret().public(),
            ingress_node_id: secret().public(),
            realm_id: REALM,
            group_id,
            created_by: user(),
            created_at_ms: 1_000,
            payload: OldExecution(execution()),
            request_digest: OLD.request_digest,
            spec_digest: [9u8; 32],
            resources,
            retention_ms: 60_000,
            retry: JobRetryPolicy {
                launches_per_witness: 1,
            },
            admission: JobAdmissionRecord {
                submission_id: OLD.submission_id,
                request_digest: OLD.request_digest,
                job_id,
                group_id,
                admitting_node_id: secret().public(),
                membership_generation: 1,
                resources,
                admitted_at_ms: 1_000,
            },
            captured_inputs: vec![OldInput {
                destination_key: "inputs/table.csv".to_string(),
                source_node_id: secret().public(),
                version_id: Ulid::from_bytes([6u8; 16]),
                blake3: [7u8; 32],
                bytes: 42,
                policies: Vec::new(),
            }],
            output_policies: Vec::new(),
            placement: PlacementRef::NIL,
        }
    }

    fn old_envelope(record: OldRecord) -> OldEnvelope {
        OldEnvelope {
            realm_id: REALM,
            record,
            published_by: secret().public(),
            signature: secret().sign(b"old"),
        }
    }

    fn old_bytes(record: OldRecord) -> Vec<u8> {
        postcard::to_allocvec(&old_envelope(record)).unwrap()
    }

    /// A launch in the current shape, which reads back unchanged.
    fn current_launch(spec_digest: [u8; 32], created_at_ms: u64) -> Vec<u8> {
        let launch = LaunchIntent {
            launch_id: Ulid::from_bytes([1u8; 16]),
            submission_id: OLD.submission_id,
            request_digest: OLD.request_digest,
            job_id: JobId::from_bytes([5u8; 16]),
            scheduler_node_id: secret().public(),
            scheduler_seq: 1,
            witness_placement: PlacementRef::NIL,
            holder_generation: 1,
            target: target(),
            inputs: Vec::new(),
            input_pages: None,
            output_policies: Vec::new(),
            plan_digest: [8u8; 32],
            spec_digest,
            created_at_ms,
            config_clock: Default::default(),
        };
        signed(JobFamilyRecord::Launch(Box::new(launch)))
    }

    fn signed(record: JobFamilyRecord) -> Vec<u8> {
        let envelope = JobRecordEnvelope::sign(REALM, record, &secret()).unwrap();
        postcard::to_allocvec(&envelope).unwrap()
    }

    fn claim(family: JobFamilyId, job_id: JobId) -> Vec<u8> {
        signed(JobFamilyRecord::Claim(SubmissionClaim {
            submission_id: family.submission_id,
            job_id,
            request_digest: family.request_digest,
            spec_digest: [9u8; 32],
            committing_node_id: secret().public(),
            accepted_at_ms: 2_000,
        }))
    }

    fn receipt(physical_job_id: JobId, execution_id: Ulid, launch_digest: [u8; 32]) -> Vec<u8> {
        let receipt = ExecutionReceipt {
            execution_id,
            physical_job_id,
            launch_id: Ulid::from_bytes([1u8; 16]),
            launch_digest,
            submission_id: OLD.submission_id,
            request_digest: OLD.request_digest,
            job_id: JobId::from_bytes([5u8; 16]),
            executor_node_id: secret().public(),
            target: target(),
            spec_digest: [9u8; 32],
            membership_generation: 1,
            subject_generation: 1,
            subject_digest: [12u8; 32],
            accepted_at_ms: 3_000,
            config_clock: Default::default(),
            config_generation: 0,
            config_digest: [0u8; 32],
        };
        signed(JobFamilyRecord::Receipt(Box::new(receipt)))
    }

    /// The storage key of a record of `kind` in `family`.
    pub(crate) fn key(family: JobFamilyId, kind: JobRecordKind, seed: u8) -> Vec<u8> {
        let key = JobRecordKey {
            family,
            kind,
            subject: [seed; 32],
            sequence: 0,
        };
        key.to_bytes().to_vec()
    }

    /// Stores a queued execution job with its index rows, as the node writes them.
    fn store_job(path: &Path, job_id: JobId) {
        let record = JobRecord::new(
            job_id,
            JobPayload::Execution(execution()),
            user(),
            secret().public(),
            1_000,
            1_000,
            None,
        );
        for (keyspace, key, value) in job_insert_entries(&record).unwrap() {
            write(path, &keyspace, vec![(key.as_ref(), value.to_vec())]);
        }
    }

    /// Every keyspace an old family or its runs may hold rows in.
    const TOUCHED: [&str; 9] = [
        FAMILY_RECORD_KEYSPACE,
        FAMILY_ALIAS_KEYSPACE,
        FAMILY_OUTBOX_KEYSPACE,
        WITNESS_DEADLINE_KEYSPACE,
        JOB_KEYSPACE,
        JOB_INDEX_KEYSPACE,
        SCHEDULE_INDEX_KEYSPACE,
        ACTIVE_USER_KEYSPACE,
        JOB_RESERVATION_KEYSPACE,
    ];

    fn snapshot(path: &Path) -> BTreeMap<&'static str, BTreeMap<Vec<u8>, Vec<u8>>> {
        TOUCHED
            .iter()
            .map(|name| (*name, read(path, name)))
            .collect()
    }

    fn deleted(output: &MigrateOutput, keyspace: &str) -> usize {
        output.old_runs_deleted.get(keyspace).copied().unwrap_or(0)
    }

    #[tokio::test]
    async fn deletes_old_families() {
        // An alpha.134 spec and launch take their family, local jobs and execution; the alias
        // stays with the tombstone. A current family, its job and the run crate stay, the node
        // lists and recovers jobs from what is left, and a second run changes nothing.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let kept_job = JobId::from_bytes([21u8; 16]);
        let kept_claim = key(KEPT, JobRecordKind::Claim, 21);
        write(
            &path,
            FAMILY_RECORD_KEYSPACE,
            vec![(&kept_claim, claim(KEPT, kept_job))],
        );
        let alias = [kept_job.to_bytes().as_slice(), &KEPT.to_bytes()].concat();
        write(&path, FAMILY_ALIAS_KEYSPACE, vec![(&alias, vec![1])]);
        store_job(&path, kept_job);
        let (logical, physical) = (JobId::from_bytes([5u8; 16]), JobId::from_bytes([22u8; 16]));
        let alias = [logical.to_bytes().as_slice(), &OLD.to_bytes()].concat();
        write(&path, FAMILY_ALIAS_KEYSPACE, vec![(&alias, vec![1])]);
        let kept = snapshot(&path);

        let execution = Ulid::from_bytes([23u8; 16]);
        let rows = [
            (
                key(OLD, JobRecordKind::Spec, 5),
                old_bytes(OldRecord::Spec(Box::new(old_spec()))),
            ),
            (
                key(OLD, JobRecordKind::Launch, 1),
                old_bytes(OldRecord::Launch(Box::new(old_launch(1)))),
            ),
            (key(OLD, JobRecordKind::Claim, 5), claim(OLD, logical)),
            (
                key(OLD, JobRecordKind::Receipt, 23),
                receipt(physical, execution, [10u8; 32]),
            ),
        ];
        let rows = rows
            .iter()
            .map(|(key, value)| (key.as_slice(), value.clone()));
        write(&path, FAMILY_RECORD_KEYSPACE, rows.collect());
        let outbox = key(OLD, JobRecordKind::Claim, 5);
        write(&path, FAMILY_OUTBOX_KEYSPACE, vec![(&outbox, vec![1])]);
        let deadline = [7u64.to_be_bytes().as_slice(), &OLD.to_bytes()].concat();
        write(&path, WITNESS_DEADLINE_KEYSPACE, vec![(&deadline, vec![1])]);
        write(
            &path,
            JOB_RESERVATION_KEYSPACE,
            vec![(&execution.to_bytes(), vec![1])],
        );
        let written = RunCrateStatus::Written {
            resource: "crate".to_string(),
        };
        let crate_key = physical.to_bytes();
        let crates = vec![(&crate_key[..], written.to_bytes().unwrap())];
        write(&path, RUN_CRATE_KEYSPACE, crates);
        store_job(&path, logical);
        store_job(&path, physical);
        let run_crate = read(&path, RUN_CRATE_KEYSPACE);

        let output = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(
            (output.old_families_deleted, output.run_crates_kept),
            (1, 1)
        );
        assert_eq!(output.families_tombstoned, 1);
        assert_eq!(deleted(&output, FAMILY_RECORD_KEYSPACE), 4);
        assert_eq!(deleted(&output, FAMILY_ALIAS_KEYSPACE), 0);
        assert_eq!(deleted(&output, JOB_KEYSPACE), 2);
        assert_eq!(deleted(&output, JOB_RESERVATION_KEYSPACE), 1);
        assert_eq!(snapshot(&path), kept);
        assert_eq!(read(&path, RUN_CRATE_KEYSPACE), run_crate);

        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(
            (again.old_families_deleted, again.families_tombstoned),
            (0, 0)
        );
        assert!(again.old_runs_deleted.is_empty());
        assert_eq!(snapshot(&path), kept);

        let context = DriverContext {
            storage_handle: FjallStorage::open(path.to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let (jobs, _) = drive(ListJobsOperation::new(user(), None, 10, |_| true), &context)
            .await
            .unwrap();
        let listed: Vec<JobId> = jobs.iter().map(|job| job.job_id).collect();
        assert_eq!(listed, [kept_job]);
        let recovered = recover_stale_jobs(&JobsRuntime::new(), &context).await;
        assert_eq!(recovered, Ok(0), "startup recovery reads what is left");
    }

    #[test]
    fn launches_never_alias() {
        // Launches cite their scheduler's realm config clock, so an alpha.134 launch no longer
        // reads as a current one, and a current launch no longer reads as alpha.134.
        let mut launch = old_launch(1);
        launch.inputs.clear();
        launch.plan_digest[0] = 0;
        let old = OldRecord::Launch(Box::new(launch));
        let bytes = postcard::to_allocvec(&old_envelope(old)).unwrap();
        assert!(super::exact::<JobRecordEnvelope>(&bytes).is_none());

        let launch = current_launch([0x99u8; 32], 1_730_000_000_000);
        assert!(super::exact::<(super::OldEnvelope, ())>(&launch).is_none());
    }

    #[test]
    fn refuses_unreadable_specs() {
        // A spec row that reads in neither layout names no family, so the migration refuses
        // before any write, also on a second run.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let value = [[4u8; 32].as_slice(), &[0u8, 0xff, 0xff, 0xff]].concat();
        let row = key(OLD, JobRecordKind::Spec, 1);
        write(&path, FAMILY_RECORD_KEYSPACE, vec![(&row, value)]);
        let rows = read(&path, FAMILY_RECORD_KEYSPACE);
        for _ in 0..2 {
            let error = migrate_output(path.to_str().unwrap()).unwrap_err();
            let ExplorerError::Refused(text) = error else {
                panic!("the migration refuses: {error}");
            };
            let message = "1 job family specs or launches read in neither";
            assert!(text.starts_with(message), "{text}");
            assert!(text.contains("job_family_records: 1"), "{text}");
            assert_eq!(read(&path, FAMILY_RECORD_KEYSPACE), rows);
        }
    }
}
