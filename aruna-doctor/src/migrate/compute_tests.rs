//! Tests the doctor's removal of rows stored before compute backends had names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::migrate::families::tests::key;
use crate::migrate::migrate_output;
use crate::migrate::tests::{read, strip, write};
use aruna_core::UserId;
use aruna_core::compute::{ExecutionTargetId, ExecutorCapability};
use aruna_core::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision};
use aruna_core::keyspaces::{
    FAMILY_RECORD_KEYSPACE, JOB_KEYSPACE, JOB_RESERVATION_KEYSPACE, LAUNCH_RECORD_KEYSPACE,
    NODE_INFO_KEYSPACE, PLAN_EXPLAIN_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
};
use aruna_core::scheduling::{ExecutionPlan, RejectedTarget};
use aruna_core::structs::execution::job::job_insert_entries;
use aruna_core::structs::execution::job::{
    AttemptIntent, EffectiveResources, ExecutionSpec, JobAdmissionRecord, JobRetryPolicy,
};
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobPayload, JobRecord, JobRecordEnvelope, JobRecordKind,
};
use aruna_core::structs::storage::node_info::node_info_key;
use aruna_operations::sync::outbox_record::new_outbox_record;
use std::path::Path;
use tempfile::tempdir;

const REALM: RealmId = RealmId([4u8; 32]);
const OLD: JobFamilyId = JobFamilyId {
    submission_id: SubmissionId([2u8; 32]),
    request_digest: [3u8; 32],
};
const KEPT: JobFamilyId = JobFamilyId {
    submission_id: SubmissionId([12u8; 32]),
    request_digest: [13u8; 32],
};

fn secret() -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[7u8; 32])
}

fn target() -> Target {
    Target {
        node_id: secret().public(),
        executor_kind: "docker".to_string(),
    }
}

fn unnamed(record: Record) -> Vec<u8> {
    let envelope = Envelope {
        realm_id: REALM,
        record,
        published_by: secret().public(),
        signature: secret().sign(b"old"),
    };
    postcard::to_allocvec(&envelope).unwrap()
}

fn launch() -> Record {
    Record::Launch(Box::new(Launch {
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
        spec_digest: [9u8; 32],
        created_at_ms: 1_000,
    }))
}

fn receipt(physical_job_id: JobId, execution_id: Ulid) -> Record {
    Record::Receipt(Box::new(Receipt {
        execution_id,
        physical_job_id,
        launch_id: Ulid::from_bytes([1u8; 16]),
        launch_digest: [10u8; 32],
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
    }))
}

fn claim(family: JobFamilyId) -> Vec<u8> {
    let record = JobFamilyRecord::Claim(SubmissionClaim {
        submission_id: family.submission_id,
        job_id: JobId::from_bytes([21u8; 16]),
        request_digest: family.request_digest,
        spec_digest: [9u8; 32],
        committing_node_id: secret().public(),
        accepted_at_ms: 2_000,
    });
    let envelope = JobRecordEnvelope::sign(REALM, record, &secret()).unwrap();
    postcard::to_allocvec(&envelope).unwrap()
}

/// Stores a local job row with its index rows; its payload is not an execution.
fn store_job(path: &Path, job_id: JobId) {
    let user = UserId::new(Ulid::from_bytes([11u8; 16]), REALM);
    let payload = JobPayload::WriteRunCrate {
        for_job: JobId::from_bytes([30u8; 16]),
    };
    let record = JobRecord::new(job_id, payload, user, secret().public(), 1_000, 1_000, None);
    for (keyspace, key, value) in job_insert_entries(&record).unwrap() {
        write(path, &keyspace, vec![(key.as_ref(), value.to_vec())]);
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
        executor_constraint: Some("docker".to_string()),
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
        backend: None,
    }
}

fn user() -> UserId {
    UserId::new(Ulid::from_bytes([11u8; 16]), REALM)
}

/// `value` as alpha.206 stored it: without the execution spec's empty backend name.
fn without_name(value: &[u8]) -> Vec<u8> {
    strip(value, &postcard::to_allocvec(&execution()).unwrap(), 1)
}

fn spec_row() -> Vec<u8> {
    let resources = EffectiveResources {
        cpu_cores: 1,
        ram_bytes: 1024,
        disk_bytes: 0,
        max_walltime_ms: 60_000,
        preemptible: false,
    };
    let job_id = JobId::from_bytes([5u8; 16]);
    let spec = LogicalJobSpec {
        submission_id: OLD.submission_id,
        job_id,
        origin_node_id: secret().public(),
        ingress_node_id: secret().public(),
        realm_id: REALM,
        group_id: execution().group_id,
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
            group_id: execution().group_id,
            admitting_node_id: secret().public(),
            membership_generation: 1,
            resources,
            admitted_at_ms: 1_000,
        },
        captured_inputs: Vec::new(),
        input_pages: None,
        output_policies: Vec::new(),
        placement: PlacementRef::NIL,
    };
    let record = JobFamilyRecord::Spec(Box::new(spec));
    let envelope = JobRecordEnvelope::sign(REALM, record, &secret()).unwrap();
    without_name(&postcard::to_allocvec(&envelope).unwrap())
}

#[test]
fn deletes_unnamed_jobs() {
    // Job, launch and reservation rows without the backend names, and a spec whose execution
    // spec lacks it, go with the job's index rows and the spec's family. Current rows stay.
    let temp = tempdir().unwrap();
    let path = temp.path().join("db");
    let intent = AttemptIntent {
        attempt_no: 1,
        external_name: "old-attempt".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: "alpine@sha256:digest".to_string(),
        attempt_epoch: 1,
        backend: "default".to_string(),
        config_generation: 0,
        home_digest: aruna_core::compute::identity::home_digest("docker", None),
    };
    // The fields from the backend name on are what alpha.206 did not store.
    let trailing = "default".len() + 1 + 1 + 32;
    let unnamed_intent =
        |value: &[u8]| strip(value, &postcard::to_allocvec(&intent).unwrap(), trailing);
    let old_id = JobId::from_bytes([31u8; 16]);
    let payload = JobPayload::Execution(execution());
    let mut old = JobRecord::new(
        old_id,
        payload,
        user(),
        secret().public(),
        1_000,
        1_000,
        None,
    );
    old.attempt_intent = Some(intent.clone());
    let mut old_keys = Vec::new();
    for (keyspace, key, value) in job_insert_entries(&old).unwrap() {
        let value = match keyspace == JOB_KEYSPACE {
            true => unnamed_intent(&without_name(&value)),
            false => value.to_vec(),
        };
        write(&path, &keyspace, vec![(key.as_ref(), value)]);
        old_keys.push((keyspace, key.to_vec()));
    }
    let launch = LaunchRecord {
        node_id: secret().public(),
        executor_kind: "docker".to_string(),
        location: String::new(),
        resources: Default::default(),
        workdir: None,
        env: BTreeMap::new(),
        started_at_ms: None,
        backend: "default".to_string(),
    };
    let launch = strip(&postcard::to_allocvec(&launch).unwrap(), b"\x07default", 8);
    write(
        &path,
        LAUNCH_RECORD_KEYSPACE,
        vec![(b"orphan".as_slice(), launch)],
    );
    let reservation = JobReservationRecord {
        execution_id: Ulid::from_bytes([32u8; 16]),
        job_id: old_id,
        logical_job_id: old_id,
        resources: EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 1,
            disk_bytes: 0,
            max_walltime_ms: 1,
            preemptible: false,
        },
        created_at_ms: 1,
        subject_generation: 1,
        subject_digest: [0u8; 32],
        config_clock: Default::default(),
        backend: "default".to_string(),
        config_generation: 0,
        config_digest: [0u8; 32],
    };
    let current = postcard::to_allocvec(&reservation).unwrap();
    let rows = vec![
        (b"current".as_slice(), current.clone()),
        (
            b"old".as_slice(),
            current[..current.len() - trailing].to_vec(),
        ),
    ];
    write(&path, JOB_RESERVATION_KEYSPACE, rows);
    let spec_key = key(OLD, JobRecordKind::Spec, 5);
    write(&path, FAMILY_RECORD_KEYSPACE, vec![(&spec_key, spec_row())]);
    let kept = JobId::from_bytes([24u8; 16]);
    store_job(&path, kept);

    let output = migrate_output(path.to_str().unwrap()).unwrap();

    assert_eq!(output.old_families_deleted, 1);
    assert!(read(&path, FAMILY_RECORD_KEYSPACE).is_empty());
    assert!(read(&path, LAUNCH_RECORD_KEYSPACE).is_empty());
    let reservations = read(&path, JOB_RESERVATION_KEYSPACE);
    assert_eq!(
        reservations.into_keys().collect::<Vec<_>>(),
        [b"current".to_vec()]
    );
    for (keyspace, key) in &old_keys {
        assert!(!read(&path, keyspace).contains_key(key), "{keyspace}");
    }
    let jobs = read(&path, JOB_KEYSPACE);
    assert_eq!(jobs.len(), 1);

    let again = migrate_output(path.to_str().unwrap()).unwrap();
    assert_eq!(again.old_families_deleted, 0);
    assert!(again.old_runs_deleted.is_empty());
    assert_eq!(read(&path, JOB_KEYSPACE), jobs);
}

#[test]
fn deletes_unnamed_families() {
    // An alpha.206 launch and receipt take their family, the physical job and its
    // reservation with them; another family and an unrelated job stay, also on a second run.
    let temp = tempdir().unwrap();
    let path = temp.path().join("db");
    let (physical, execution) = (JobId::from_bytes([22u8; 16]), Ulid::from_bytes([23u8; 16]));
    let rows = [
        (key(OLD, JobRecordKind::Launch, 1), unnamed(launch())),
        (
            key(OLD, JobRecordKind::Receipt, 23),
            unnamed(receipt(physical, execution)),
        ),
        (key(OLD, JobRecordKind::Claim, 5), claim(OLD)),
        (key(KEPT, JobRecordKind::Claim, 5), claim(KEPT)),
    ];
    let rows = rows
        .iter()
        .map(|(key, value)| (key.as_slice(), value.clone()));
    write(&path, FAMILY_RECORD_KEYSPACE, rows.collect());
    write(
        &path,
        JOB_RESERVATION_KEYSPACE,
        vec![(&execution.to_bytes(), vec![1])],
    );
    store_job(&path, physical);
    let kept_job = JobId::from_bytes([24u8; 16]);
    store_job(&path, kept_job);

    let output = migrate_output(path.to_str().unwrap()).unwrap();

    assert_eq!(output.old_families_deleted, 1);
    assert_eq!(
        output.old_runs_deleted.get(FAMILY_RECORD_KEYSPACE),
        Some(&3)
    );
    assert_eq!(output.old_runs_deleted.get(JOB_KEYSPACE), Some(&1));
    assert_eq!(
        output.old_runs_deleted.get(JOB_RESERVATION_KEYSPACE),
        Some(&1)
    );
    let records = read(&path, FAMILY_RECORD_KEYSPACE);
    assert_eq!(
        records.keys().cloned().collect::<Vec<_>>(),
        [key(KEPT, JobRecordKind::Claim, 5)]
    );
    let jobs = read(&path, JOB_KEYSPACE);
    assert_eq!(jobs.len(), 1);
    assert!(read(&path, JOB_RESERVATION_KEYSPACE).is_empty());

    let again = migrate_output(path.to_str().unwrap()).unwrap();
    assert_eq!(again.old_families_deleted, 0);
    assert_eq!(read(&path, FAMILY_RECORD_KEYSPACE), records);
    assert_eq!(read(&path, JOB_KEYSPACE), jobs);
}

fn subject(node_id: NodeId) -> PlacementSubject {
    PlacementSubject {
        node_id,
        generation: 3,
        location: "eu-west".to_string(),
        labels: BTreeMap::new(),
        executor_kind: None,
        local_to_controller: true,
    }
}

fn document(node_id: NodeId) -> NodeInfoDocument {
    let epoch = AdvertisementEpoch {
        membership_generation: 1,
        publisher_generation: 1,
        observed_at_ms: 1_000,
    };
    NodeInfoDocument {
        node_id,
        executors: vec![
            ExecutorCapability::new("lab".to_string(), "docker".to_string(), subject(node_id))
                .unwrap(),
        ],
        labels: BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 1,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 1_000,
            download_slots: None,
            storage_capacity_bytes: None,
        },
        updated_at_ms: 1_000,
        epoch,
        compute_draining: false,
        leaving: false,
        demand: ComputeDemandSnapshot {
            epoch,
            ..Default::default()
        },
        reservation: ComputeReservationSnapshot {
            epoch,
            ..Default::default()
        },
    }
}

/// The same document as alpha.206 stored it: every backend without its name.
fn unnamed_document(node_id: NodeId) -> Vec<u8> {
    let current = document(node_id);
    let executors = current
        .executors
        .iter()
        .map(|executor| Capability {
            kind: executor.kind.clone(),
            file_staging: executor.file_staging,
            direct_s3: executor.direct_s3,
            s3_mount: executor.s3_mount,
            network_policy: executor.network_policy,
            session: executor.session,
            subject: executor.subject.clone(),
            subject_digest: executor.subject_digest,
            policy_draining: executor.policy_draining,
            limits: executor.limits,
            availability: executor.availability,
            compute_priority: executor.compute_priority,
        })
        .collect();
    let old = Document {
        node_id,
        executors,
        labels: current.labels,
        urls: current.urls,
        utilization: current.utilization,
        updated_at_ms: current.updated_at_ms,
        epoch: current.epoch,
        compute_draining: current.compute_draining,
        leaving: current.leaving,
        demand: current.demand,
        reservation: current.reservation,
    };
    postcard::to_allocvec(&old).unwrap()
}

fn queued(node_id: NodeId, bytes: Vec<u8>) -> Vec<u8> {
    let change = DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_bytes([1u8; 16]),
            actor: node_id,
            updated_at_ms: 1,
        },
        kind: DocumentChangeKind::Upsert,
        placement: PlacementRef::NIL,
    };
    let target = DocumentTarget::NodeInfo {
        realm_id: REALM,
        node_id,
    };
    let event = DocumentOutboxEvent::Upsert { bytes, change };
    let record = new_outbox_record(node_id, target, Vec::new(), event, PlacementRef::NIL, false);
    postcard::to_allocvec(&record).unwrap()
}

fn explain(old: bool) -> Vec<u8> {
    let verdict = RejectionVerdict::ExecutorKind;
    match old {
        true => postcard::to_allocvec(&Explain {
            sequence: 1,
            plan: Plan {
                selected: None,
                retryable: false,
                alternatives: Vec::new(),
                rejected: vec![(target(), verdict)],
                omitted: 0,
            },
            declined: vec![target()],
            overlapping: false,
            stored_at_ms: 1,
        }),
        false => {
            let target = ExecutionTargetId {
                node_id: secret().public(),
                backend: "lab".to_string(),
                executor_kind: "docker".to_string(),
            };
            postcard::to_allocvec(&WitnessExplain {
                sequence: 1,
                plan: ExecutionPlan {
                    selected: None,
                    retryable: false,
                    alternatives: Vec::new(),
                    rejected: vec![RejectedTarget {
                        target: target.clone(),
                        verdict,
                    }],
                    omitted: 0,
                },
                declined: vec![target],
                overlapping: false,
                stored_at_ms: 1,
            })
        }
    }
    .unwrap()
}

#[test]
fn deletes_unnamed_documents() {
    // Node info documents, their queued copies and plan explains in the alpha.206 layout go;
    // the same rows in the current layout stay, also on a second run.
    let temp = tempdir().unwrap();
    let path = temp.path().join("db");
    let (old_node, new_node) = (
        secret().public(),
        iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
    );
    let current = document(new_node).to_bytes().unwrap();
    write(
        &path,
        NODE_INFO_KEYSPACE,
        vec![
            (&node_info_key(old_node), unnamed_document(old_node)),
            (&node_info_key(new_node), current.clone()),
        ],
    );
    write(
        &path,
        SYNC_OUTBOX_KEYSPACE,
        vec![
            (
                b"old".as_slice(),
                queued(old_node, unnamed_document(old_node)),
            ),
            (b"new".as_slice(), queued(new_node, current.clone())),
        ],
    );
    let old_key = [OLD.to_bytes().as_slice(), old_node.as_bytes()].concat();
    let new_key = [KEPT.to_bytes().as_slice(), old_node.as_bytes()].concat();
    write(
        &path,
        PLAN_EXPLAIN_KEYSPACE,
        vec![(&old_key, explain(true)), (&new_key, explain(false))],
    );

    let output = migrate_output(path.to_str().unwrap()).unwrap();

    for name in [
        NODE_INFO_KEYSPACE,
        SYNC_OUTBOX_KEYSPACE,
        PLAN_EXPLAIN_KEYSPACE,
    ] {
        assert_eq!(output.stale_rows_deleted.get(name), Some(&1), "{name}");
    }
    let kept = |name| read(&path, name).into_keys().collect::<Vec<_>>();
    assert_eq!(kept(NODE_INFO_KEYSPACE), [node_info_key(new_node)]);
    assert_eq!(kept(SYNC_OUTBOX_KEYSPACE), [b"new".to_vec()]);
    assert_eq!(kept(PLAN_EXPLAIN_KEYSPACE), std::slice::from_ref(&new_key));
    assert_eq!(
        read(&path, NODE_INFO_KEYSPACE)[&node_info_key(new_node)],
        current
    );

    let again = migrate_output(path.to_str().unwrap()).unwrap();
    for name in [
        NODE_INFO_KEYSPACE,
        SYNC_OUTBOX_KEYSPACE,
        PLAN_EXPLAIN_KEYSPACE,
    ] {
        assert_eq!(again.stale_rows_deleted.get(name), None, "{name}");
    }
    assert_eq!(kept(PLAN_EXPLAIN_KEYSPACE), [new_key]);
}
