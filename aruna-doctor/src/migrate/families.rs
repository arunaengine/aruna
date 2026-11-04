//! Counts job family specs and launches stored in the alpha.134 layout, or that cannot be told
//! apart from it, and stops the migration while any exist: a rewrite would break their digests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::explorer::ExplorerError;
use aruna_core::NodeId;
use aruna_core::compute::ExecutionTargetId;
use aruna_core::structs::execution::job::{
    JobFamilyRecord, JobId, JobRecordEnvelope, LogicalJobSpec, SubmissionClaim, SubmissionId,
    WitnessBudgetRecord,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_operations::jobs::records::rows::{ConflictRecord, PendingNeed, PendingRecord};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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
/// whether it also reads back unchanged as an alpha.134 launch.
pub(super) struct Candidate {
    current: Option<JobRecordEnvelope>,
    historical: bool,
}

/// The spec and launch rows of one family keyspace; every current row adds its evidence.
pub(super) fn scan<T: Held>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    evidence: &mut Evidence,
) -> Result<Vec<Candidate>, ExplorerError> {
    let mut candidates = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (_, value) = entry.into_inner()?;
        let current = exact::<T>(&value).map(|held| held.envelope().clone());
        if let Some(envelope) = &current {
            evidence.add(envelope);
        }
        if matches!(value.get(VARIANT_AT), Some(&SPEC | &LAUNCH)) {
            let historical = exact::<(OldEnvelope, T::Tail)>(&value).is_some();
            candidates.push(Candidate {
                current,
                historical,
            });
        }
    }
    Ok(candidates)
}

/// Stops the migration before any write while a family keyspace holds old specs or launches.
/// Their spec digest and publisher signature cover the stored layout, so the operator decides.
pub(super) fn refuse_specs(
    keyspaces: &[(&str, Vec<Candidate>)],
    evidence: &Evidence,
) -> Result<(), ExplorerError> {
    let counts: Vec<(&str, usize)> = keyspaces
        .iter()
        .map(|(name, rows)| (*name, rows.iter().filter(|row| row.old(evidence)).count()))
        .filter(|(_, count)| *count > 0)
        .collect();
    if counts.is_empty() {
        return Ok(());
    }
    let total: usize = counts.iter().map(|(_, count)| count).sum();
    let found: Vec<String> = counts
        .iter()
        .map(|(name, count)| format!("{name}: {count}"))
        .collect();
    Err(ExplorerError::Refused(format!(
        "{total} job family specs or launches are stored in the alpha.134 layout or cannot be \
         told apart from it ({}). Their spec digest and publisher signature cover the stored \
         bytes, so this release cannot rewrite them. Nothing was changed.",
        found.join(", ")
    )))
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
            JobFamilyRecord::Launch(launch) if self.historical => {
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

/// `JobFamilyRecord` up to its launch, the only variant read in this layout.
#[derive(Serialize, Deserialize)]
enum OldRecord {
    Spec(Box<LogicalJobSpec>),
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
    use crate::migrate::migrate_output;
    use crate::migrate::tests::{read, write};
    use aruna_core::NodeId;
    use aruna_core::compute::ExecutionTargetId;
    use aruna_core::keyspaces::{
        FAMILY_CONFLICT_KEYSPACE, FAMILY_PENDING_KEYSPACE, FAMILY_RECORD_KEYSPACE,
    };
    use aruna_core::structs::execution::job::{
        ExecutionReceipt, JobFamilyRecord, JobId, JobRecordEnvelope, JobRecordKind, LaunchIntent,
        SubmissionId, WitnessBudgetRecord,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_operations::jobs::records::rows::{PendingNeed, PendingRecord};
    use serde::Serialize;
    use std::path::Path;
    use tempfile::tempdir;
    use ulid::Ulid;

    const REALM: RealmId = RealmId([4u8; 32]);

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

    /// `JobFamilyRecord` up to its launch variant; the earlier variants are never written here.
    #[derive(Serialize)]
    #[allow(dead_code)]
    enum OldRecord {
        Spec(()),
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

    #[derive(Serialize)]
    struct OldConflict {
        envelope: OldEnvelope,
        retained: [u8; 32],
        observed_at_ms: u64,
        relayed_by: Option<NodeId>,
    }

    fn secret() -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[7u8; 32])
    }

    fn target() -> ExecutionTargetId {
        ExecutionTargetId {
            node_id: secret().public(),
            executor_kind: "kubernetes".to_string(),
        }
    }

    fn old_launch(seed: u8) -> OldLaunch {
        OldLaunch {
            launch_id: Ulid::from_bytes([seed; 16]),
            submission_id: SubmissionId([2u8; 32]),
            request_digest: [3u8; 32],
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

    fn old_envelope(launch: OldLaunch) -> OldEnvelope {
        OldEnvelope {
            realm_id: REALM,
            record: OldRecord::Launch(Box::new(launch)),
            published_by: secret().public(),
            signature: secret().sign(b"launch"),
        }
    }

    /// A launch in the current shape, which the count leaves out.
    fn current_launch(spec_digest: [u8; 32], created_at_ms: u64) -> Vec<u8> {
        let launch = LaunchIntent {
            launch_id: Ulid::from_bytes([1u8; 16]),
            submission_id: SubmissionId([2u8; 32]),
            request_digest: [3u8; 32],
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

    /// Runs the migration twice; each run refuses with `message` and leaves `keyspace` as it was.
    fn refused_twice(path: &Path, keyspace: &str, message: &str) {
        let rows = read(path, keyspace);
        for _ in 0..2 {
            let error = migrate_output(path.to_str().unwrap()).unwrap_err();
            let ExplorerError::Refused(text) = error else {
                panic!("the migration refuses: {error}");
            };
            assert!(text.starts_with(message), "{text}");
            assert_eq!(read(path, keyspace), rows);
        }
    }

    #[test]
    fn refuses_old_launches() {
        // Current launches pass. Launches of alpha.134 stop the migration with their count by
        // keyspace, every row stays as it was, and a second run refuses the same way.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        write(
            &path,
            FAMILY_RECORD_KEYSPACE,
            vec![(b"current", current_launch([9u8; 32], 1_000))],
        );
        let output = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!((output.records_scanned, output.records_rewritten), (1, 0));

        let old = |seed| postcard::to_allocvec(&old_envelope(old_launch(seed))).unwrap();
        let conflict = OldConflict {
            envelope: old_envelope(old_launch(3)),
            retained: [1u8; 32],
            observed_at_ms: 2_000,
            relayed_by: None,
        };
        write(
            &path,
            FAMILY_RECORD_KEYSPACE,
            vec![(b"old-1", old(1)), (b"old-2", old(2))],
        );
        let conflict = postcard::to_allocvec(&conflict).unwrap();
        write(&path, FAMILY_CONFLICT_KEYSPACE, vec![(b"old", conflict)]);
        let records = read(&path, FAMILY_RECORD_KEYSPACE);
        let conflicts = read(&path, FAMILY_CONFLICT_KEYSPACE);

        for _ in 0..2 {
            let error = migrate_output(path.to_str().unwrap()).unwrap_err();
            let ExplorerError::Refused(message) = error else {
                panic!("the migration refuses: {error}");
            };
            assert!(message.starts_with("3 job family specs"), "{message}");
            let counts = "job_family_records: 2, job_family_conflicts: 1";
            assert!(message.contains(counts), "{message}");
            assert_eq!(read(&path, FAMILY_RECORD_KEYSPACE), records);
            assert_eq!(read(&path, FAMILY_CONFLICT_KEYSPACE), conflicts);
        }
    }

    #[test]
    fn refuses_aliased_launches() {
        // An alpha.134 launch without inputs or output policies and a plan digest starting with
        // zero reads back unchanged as a current launch, with shifted digests. It still counts.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let mut launch = old_launch(1);
        launch.inputs.clear();
        launch.plan_digest[0] = 0;
        launch.created_at_ms = 1_730_000_000_000;
        let pending = OldPending {
            envelope: old_envelope(launch),
            need: PendingNeed::Evidence(JobRecordKind::Spec),
            first_seen_ms: 2_000,
            attempts: 1,
        };
        let bytes = postcard::to_allocvec(&pending).unwrap();
        assert!(
            super::exact::<PendingRecord>(&bytes).is_some(),
            "aliases a current row"
        );
        write(&path, FAMILY_PENDING_KEYSPACE, vec![(b"old", bytes)]);

        let message = "1 job family specs or launches";
        refused_twice(&path, FAMILY_PENDING_KEYSPACE, message);
    }

    #[test]
    fn refuses_receipted_aliases() {
        // A historical receipt binds the aliased launch's digest but its true spec digest, so
        // it is no evidence for the shifted reading; the migration still refuses.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let mut launch = old_launch(1);
        launch.inputs.clear();
        launch.plan_digest[0] = 0;
        launch.created_at_ms = 1_730_000_000_000;
        let spec_digest = launch.spec_digest;
        let pending = |envelope| OldPending {
            envelope,
            need: PendingNeed::Evidence(JobRecordKind::Spec),
            first_seen_ms: 2_000,
            attempts: 1,
        };
        let bytes = postcard::to_allocvec(&pending(old_envelope(launch))).unwrap();
        let aliased = super::exact::<PendingRecord>(&bytes).expect("aliases a current row");
        let receipt = ExecutionReceipt {
            execution_id: Ulid::from_bytes([10u8; 16]),
            physical_job_id: JobId::from_bytes([11u8; 16]),
            launch_id: Ulid::from_bytes([1u8; 16]),
            launch_digest: aliased.envelope.record.digest().unwrap(),
            submission_id: SubmissionId([2u8; 32]),
            request_digest: [3u8; 32],
            job_id: JobId::from_bytes([5u8; 16]),
            executor_node_id: secret().public(),
            target: target(),
            spec_digest,
            membership_generation: 1,
            subject_generation: 1,
            subject_digest: [12u8; 32],
            accepted_at_ms: 3_000,
        };
        let record = JobFamilyRecord::Receipt(Box::new(receipt));
        let envelope = JobRecordEnvelope::sign(REALM, record, &secret()).unwrap();
        let receipt = PendingRecord {
            envelope,
            need: PendingNeed::Evidence(JobRecordKind::Spec),
            first_seen_ms: 3_000,
            attempts: 1,
        };
        let receipt = postcard::to_allocvec(&receipt).unwrap();
        let rows = vec![(&b"launch"[..], bytes), (&b"receipt"[..], receipt)];
        write(&path, FAMILY_PENDING_KEYSPACE, rows);

        refused_twice(
            &path,
            FAMILY_PENDING_KEYSPACE,
            "1 job family specs or launches",
        );
    }

    #[test]
    fn binds_ambiguous_launches() {
        // A current launch whose bytes also read as alpha.134 counts until a budget binds its
        // spec digest; then it passes unchanged, also on a second run.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let launch = current_launch([0x99u8; 32], 1_730_000_000_000);
        let historical = super::exact::<(super::OldEnvelope, ())>(&launch);
        assert!(historical.is_some(), "also reads as alpha.134");
        write(&path, FAMILY_RECORD_KEYSPACE, vec![(b"launch", launch)]);
        refused_twice(&path, FAMILY_RECORD_KEYSPACE, "1 job family specs");

        let budget = WitnessBudgetRecord {
            submission_id: SubmissionId([2u8; 32]),
            request_digest: [3u8; 32],
            scheduler_node_id: secret().public(),
            source_spec_digest: [0x99u8; 32],
            max_launches: 1,
        };
        let budget = signed(JobFamilyRecord::Budget(budget));
        write(&path, FAMILY_RECORD_KEYSPACE, vec![(b"budget", budget)]);
        let rows = read(&path, FAMILY_RECORD_KEYSPACE);
        for _ in 0..2 {
            let output = migrate_output(path.to_str().unwrap()).unwrap();
            assert_eq!((output.records_scanned, output.records_rewritten), (2, 0));
            assert_eq!(read(&path, FAMILY_RECORD_KEYSPACE), rows);
        }
    }
}
