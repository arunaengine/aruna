//! Counts job family specs and launches stored before input pages and stops the migration
//! while any exist, because a rewrite would break their signed digests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::explorer::ExplorerError;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Where a stored envelope names its record variant: after the 32 bytes of the realm id.
const VARIANT_AT: usize = 32;
/// Variant indexes of `JobFamilyRecord::Spec` and `JobFamilyRecord::Launch`.
const SPEC: u8 = 0;
const LAUNCH: u8 = 3;

/// Rows of one family keyspace holding a spec or launch that `T` cannot read back unchanged.
/// Records, pending and conflict rows all start with the envelope.
pub(super) fn old_specs<T: Serialize + DeserializeOwned>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
) -> Result<usize, ExplorerError> {
    let mut count = 0;
    for entry in db.read_tx().iter(keyspace) {
        let (_, value) = entry.into_inner()?;
        let spec = matches!(value.get(VARIANT_AT), Some(&SPEC | &LAUNCH));
        let current = postcard::from_bytes::<T>(&value)
            .ok()
            .and_then(|decoded| postcard::to_allocvec(&decoded).ok())
            .is_some_and(|bytes| bytes == *value);
        count += usize::from(spec && !current);
    }
    Ok(count)
}

/// Stops the migration before any write while a family keyspace holds old specs or launches.
/// Their spec digest and publisher signature cover the stored layout, so the operator decides.
pub(super) fn refuse_specs(counts: &[(&str, usize)]) -> Result<(), ExplorerError> {
    let found: Vec<String> = counts
        .iter()
        .filter(|(_, count)| *count > 0)
        .map(|(name, count)| format!("{name}: {count}"))
        .collect();
    if found.is_empty() {
        return Ok(());
    }
    let total: usize = counts.iter().map(|(_, count)| count).sum();
    Err(ExplorerError::Refused(format!(
        "{total} job family specs or launches were stored before input pages ({}). Their spec \
         digest and publisher signature cover the stored bytes, so this release cannot rewrite \
         them. Nothing was changed.",
        found.join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use crate::explorer::ExplorerError;
    use crate::migrate::migrate_output;
    use crate::migrate::tests::{read, write};
    use aruna_core::NodeId;
    use aruna_core::compute::ExecutionTargetId;
    use aruna_core::keyspaces::{FAMILY_CONFLICT_KEYSPACE, FAMILY_RECORD_KEYSPACE};
    use aruna_core::structs::execution::job::{
        JobFamilyRecord, JobId, JobRecordEnvelope, LaunchIntent, SubmissionId,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::placement::record::PlacementRef;
    use serde::Serialize;
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

    fn old_envelope(seed: u8) -> OldEnvelope {
        let launch = OldLaunch {
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
        };
        OldEnvelope {
            realm_id: REALM,
            record: OldRecord::Launch(Box::new(launch)),
            published_by: secret().public(),
            signature: secret().sign(b"launch"),
        }
    }

    /// A launch in the current shape, which the count leaves out.
    fn current_launch() -> Vec<u8> {
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
            spec_digest: [9u8; 32],
            created_at_ms: 1_000,
        };
        let record = JobFamilyRecord::Launch(Box::new(launch));
        let envelope = JobRecordEnvelope::sign(REALM, record, &secret()).unwrap();
        postcard::to_allocvec(&envelope).unwrap()
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
            vec![(b"current", current_launch())],
        );
        let output = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!((output.records_scanned, output.records_rewritten), (1, 0));

        let old = |seed| postcard::to_allocvec(&old_envelope(seed)).unwrap();
        let conflict = OldConflict {
            envelope: old_envelope(3),
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
}
