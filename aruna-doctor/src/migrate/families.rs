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

#[cfg(test)]
mod tests {
    use crate::explorer::ExplorerError;
    use crate::migrate::tests::{read, write};
    use crate::migrate::{MigrateOutput, migrate_output};
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::compute::ExecutionTargetId;
    use aruna_core::keyspaces::{
        ACTIVE_USER_KEYSPACE, FAMILY_ALIAS_KEYSPACE, FAMILY_OUTBOX_KEYSPACE,
        FAMILY_PENDING_KEYSPACE, FAMILY_RECORD_KEYSPACE, JOB_INDEX_KEYSPACE, JOB_KEYSPACE,
        JOB_RESERVATION_KEYSPACE, RUN_CRATE_KEYSPACE, SCHEDULE_INDEX_KEYSPACE,
        WITNESS_DEADLINE_KEYSPACE,
    };
    use aruna_core::structs::execution::job::{
        EffectiveResources, ExecutionReceipt, ExecutionSpec, JobAdmissionRecord, JobFamilyId,
        JobFamilyRecord, JobId, JobPayload, JobRecord, JobRecordEnvelope, JobRecordKey,
        JobRecordKind, JobRetryPolicy, LaunchIntent, RunCrateStatus, SubmissionClaim, SubmissionId,
        WitnessBudgetRecord,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::GroupId;
    use aruna_operations::jobs::records::rows::{PendingNeed, PendingRecord};
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_operations::jobs::store::{job_insert_entries, list_user_jobs};
    use aruna_storage::FjallStorage;
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
        target: ExecutionTargetId,
        inputs: Vec<OldPlanned>,
        output_policies: Vec<PlacementPolicyRef>,
        plan_digest: [u8; 32],
        spec_digest: [u8; 32],
        created_at_ms: u64,
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
        payload: ExecutionSpec,
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

    #[derive(Serialize)]
    struct OldPending {
        envelope: OldEnvelope,
        need: PendingNeed,
        first_seen_ms: u64,
        attempts: u32,
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
            target: target(),
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
            payload: execution(),
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
        };
        signed(JobFamilyRecord::Receipt(Box::new(receipt)))
    }

    /// The storage key of a record of `kind` in `family`.
    fn key(family: JobFamilyId, kind: JobRecordKind, seed: u8) -> Vec<u8> {
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
        // An alpha.134 spec and launch take their whole family, its local jobs and execution
        // with them. A current family and its job stay, the run crate stays, and the node lists
        // and recovers jobs from what is left. A second run changes nothing.
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
        let kept = snapshot(&path);

        let (logical, physical) = (JobId::from_bytes([5u8; 16]), JobId::from_bytes([22u8; 16]));
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
        let alias = [logical.to_bytes().as_slice(), &OLD.to_bytes()].concat();
        write(&path, FAMILY_ALIAS_KEYSPACE, vec![(&alias, vec![1])]);
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
        assert_eq!(deleted(&output, FAMILY_RECORD_KEYSPACE), 4);
        assert_eq!(deleted(&output, FAMILY_ALIAS_KEYSPACE), 1);
        assert_eq!(deleted(&output, JOB_KEYSPACE), 2);
        assert_eq!(deleted(&output, JOB_RESERVATION_KEYSPACE), 1);
        assert_eq!(snapshot(&path), kept);
        assert_eq!(read(&path, RUN_CRATE_KEYSPACE), run_crate);

        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.old_families_deleted, 0);
        assert!(again.old_runs_deleted.is_empty());
        assert_eq!(snapshot(&path), kept);

        let storage = FjallStorage::open(path.to_str().unwrap()).unwrap();
        let (jobs, _) = list_user_jobs(&storage, user(), None, 10, |_| true)
            .await
            .unwrap();
        let listed: Vec<JobId> = jobs.iter().map(|job| job.job_id).collect();
        assert_eq!(listed, [kept_job]);
        let recovered = JobsRuntime::new().recover_stale_jobs(&storage).await;
        assert_eq!(recovered, Ok(0), "startup recovery reads what is left");
    }

    #[test]
    fn deletes_receipted_aliases() {
        // An alpha.134 launch without inputs and a plan digest starting with zero reads back
        // unchanged as a current launch, with shifted digests. A receipt binding its aliased
        // digest but its true spec digest is no evidence, so the family goes, receipt included.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let mut launch = old_launch(1);
        launch.inputs.clear();
        launch.plan_digest[0] = 0;
        launch.created_at_ms = 1_730_000_000_000;
        let pending = |envelope| OldPending {
            envelope,
            need: PendingNeed::Evidence(JobRecordKind::Spec),
            first_seen_ms: 2_000,
            attempts: 1,
        };
        let launch = OldRecord::Launch(Box::new(launch));
        let bytes = postcard::to_allocvec(&pending(old_envelope(launch))).unwrap();
        let aliased = super::exact::<PendingRecord>(&bytes).expect("aliases a current row");
        let digest = aliased.envelope.record.digest().unwrap();
        let receipt = PendingRecord {
            envelope: postcard::from_bytes(&receipt(
                JobId::from_bytes([22u8; 16]),
                Ulid::nil(),
                digest,
            ))
            .unwrap(),
            need: PendingNeed::Evidence(JobRecordKind::Spec),
            first_seen_ms: 3_000,
            attempts: 1,
        };
        let rows = [
            (key(OLD, JobRecordKind::Launch, 1), bytes),
            (
                key(OLD, JobRecordKind::Receipt, 2),
                postcard::to_allocvec(&receipt).unwrap(),
            ),
        ];
        let rows = rows
            .iter()
            .map(|(key, value)| (key.as_slice(), value.clone()));
        write(&path, FAMILY_PENDING_KEYSPACE, rows.collect());

        let output = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(deleted(&output, FAMILY_PENDING_KEYSPACE), 2);
        assert!(read(&path, FAMILY_PENDING_KEYSPACE).is_empty());
    }

    #[test]
    fn keeps_bound_launches() {
        // A current launch whose bytes also read as alpha.134 counts as old until a budget binds
        // its spec digest; then it stays unchanged, also on a second run.
        let launch = current_launch([0x99u8; 32], 1_730_000_000_000);
        let historical = super::exact::<(super::OldEnvelope, ())>(&launch);
        assert!(historical.is_some(), "also reads as alpha.134");
        let budget = signed(JobFamilyRecord::Budget(WitnessBudgetRecord {
            submission_id: OLD.submission_id,
            request_digest: OLD.request_digest,
            scheduler_node_id: secret().public(),
            source_spec_digest: [0x99u8; 32],
            max_launches: 1,
        }));
        let launch_key = key(OLD, JobRecordKind::Launch, 1);
        let budget_key = key(OLD, JobRecordKind::Budget, 1);

        let temp = tempdir().unwrap();
        let alone = temp.path().join("alone");
        write(
            &alone,
            FAMILY_RECORD_KEYSPACE,
            vec![(&launch_key, launch.clone())],
        );
        let output = migrate_output(alone.to_str().unwrap()).unwrap();
        assert_eq!(output.old_families_deleted, 1);

        let bound = temp.path().join("bound");
        let rows = vec![(&launch_key[..], launch), (&budget_key[..], budget)];
        write(&bound, FAMILY_RECORD_KEYSPACE, rows);
        let rows = read(&bound, FAMILY_RECORD_KEYSPACE);
        for _ in 0..2 {
            let output = migrate_output(bound.to_str().unwrap()).unwrap();
            assert_eq!((output.records_scanned, output.records_rewritten), (2, 0));
            assert_eq!(output.old_families_deleted, 0);
            assert_eq!(read(&bound, FAMILY_RECORD_KEYSPACE), rows);
        }
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
