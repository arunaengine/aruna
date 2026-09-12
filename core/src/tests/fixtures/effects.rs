use crate::compute::ExecutionTargetId;
use crate::structs::{JobRecordEnvelope, RealmId};
use ulid::Ulid;

/// Test fixture shared with the event frames: one signed output record whose
/// encoded size grows with the output count and key width.
pub(crate) fn sized_envelope(objects: usize, key_bytes: usize) -> JobRecordEnvelope {
    use crate::structs::{
        ExecutionOutputRecord, JobFamilyRecord, JobId, OutputObject, OutputSet, SubmissionId,
    };

    let secret = iroh::SecretKey::from_bytes(&[3u8; 32]);
    let execution_id = Ulid::from_bytes([7u8; 16]);
    let outputs = (0..objects)
        .map(|index| OutputObject {
            node_id: secret.public(),
            bucket: "bucket".to_string(),
            key: format!("{index:08}-{}", "k".repeat(key_bytes)),
            version_id: Ulid::from_bytes([9u8; 16]),
            execution_id,
            container_path: "/out".to_string(),
            size: 1,
            digest: None,
        })
        .collect();
    let record = JobFamilyRecord::Output(Box::new(ExecutionOutputRecord {
        execution_id,
        submission_id: SubmissionId([1u8; 32]),
        request_digest: [2u8; 32],
        job_id: JobId::from_bytes([5u8; 16]),
        executor_node_id: secret.public(),
        spec_digest: [3u8; 32],
        receipt_digest: [4u8; 32],
        outputs: OutputSet::canonical(outputs).expect("canonical outputs"),
        committed_at_ms: 1,
    }));
    JobRecordEnvelope::sign(RealmId([6u8; 32]), record, &secret).expect("record signs")
}

/// One signed launch whose encoded size grows with the executor-kind width.
pub(crate) fn sized_launch(kind_bytes: usize) -> JobRecordEnvelope {
    use crate::structs::{JobFamilyRecord, JobId, LaunchIntent, PlacementRef, SubmissionId};

    let secret = iroh::SecretKey::from_bytes(&[4u8; 32]);
    let record = JobFamilyRecord::Launch(Box::new(LaunchIntent {
        launch_id: Ulid::from_bytes([8u8; 16]),
        submission_id: SubmissionId([1u8; 32]),
        request_digest: [2u8; 32],
        job_id: JobId::from_bytes([5u8; 16]),
        scheduler_node_id: secret.public(),
        scheduler_seq: 0,
        witness_placement: PlacementRef {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            shard: 2,
        },
        holder_generation: 3,
        target: ExecutionTargetId {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            executor_kind: "d".repeat(kind_bytes),
        },
        inputs: Vec::new(),
        output_policies: Vec::new(),
        plan_digest: [6u8; 32],
        spec_digest: [7u8; 32],
        created_at_ms: 1,
    }));
    JobRecordEnvelope::sign(RealmId([6u8; 32]), record, &secret).expect("record signs")
}

/// One signed receipt whose encoded size grows with the executor-kind width.
pub(crate) fn sized_receipt(kind_bytes: usize) -> JobRecordEnvelope {
    use crate::structs::{ExecutionReceipt, JobFamilyRecord, JobId, SubmissionId};

    let secret = iroh::SecretKey::from_bytes(&[6u8; 32]);
    let record = JobFamilyRecord::Receipt(Box::new(ExecutionReceipt {
        execution_id: Ulid::from_bytes([8u8; 16]),
        physical_job_id: JobId::from_bytes([14u8; 16]),
        launch_id: Ulid::from_bytes([9u8; 16]),
        launch_digest: [1u8; 32],
        submission_id: SubmissionId([1u8; 32]),
        request_digest: [2u8; 32],
        job_id: JobId::from_bytes([5u8; 16]),
        executor_node_id: secret.public(),
        target: ExecutionTargetId {
            node_id: secret.public(),
            executor_kind: "d".repeat(kind_bytes),
        },
        spec_digest: [7u8; 32],
        membership_generation: 4,
        subject_generation: 5,
        subject_digest: [8u8; 32],
        accepted_at_ms: 1,
    }));
    JobRecordEnvelope::sign(RealmId([6u8; 32]), record, &secret).expect("record signs")
}
