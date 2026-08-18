use aruna_core::NodeId;
use aruna_core::effects::JobRecordFrame;
use aruna_core::structs::{
    AttemptControl, ExecutionOutputRecord, JobError, JobFamilyRecord, JobId, JobRecord,
    JobRecordEnvelope, LocalExecution, OutputObject, OutputSet, SubmissionId,
};
use tracing::{debug, warn};

use super::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin, RecordStoreError};
use super::store::{persist_output_record, read_output_record};
use crate::driver::{DriverContext, drive};

/// Build, sign, and make durable this execution's exact output record, then
/// return the digest terminal success must name. Success is refused when this
/// fails, so an unpublishable or unsigned output set never becomes a result.
pub async fn seal_outputs(
    context: &DriverContext,
    record: &JobRecord,
    control: &AttemptControl,
    outputs: &[OutputObject],
) -> Result<[u8; 32], JobError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| JobError::permanent("output record needs a net handle"))?;
    let request_digest = record
        .plan_digest
        .ok_or_else(|| JobError::permanent("execution job carries no plan digest"))?;
    let outputs = OutputSet::canonical(outputs.to_vec())
        .map_err(|error| JobError::permanent(format!("output set is not canonical: {error}")))?;
    // A replay re-appends the record it already sealed: the append is
    // idempotent and a family view that was unavailable before may resolve now.
    if let Some(sealed) = sealed_record(context, record.job_id, control, &outputs, net.node_id())
        .await?
        .map(JobRecordFrame::new)
        .transpose()
        .map_err(|error| JobError::permanent(format!("sealed record is unpublishable: {error}")))?
    {
        let digest = sealed.envelope().digest().map_err(|error| {
            JobError::permanent(format!("output record digest failed: {error}"))
        })?;
        append_output_record(context, record.job_id, control, sealed).await?;
        return Ok(digest);
    }
    let output = ExecutionOutputRecord {
        execution_id: control.execution_id,
        // Until the family rounds derive a replicated identity, the submission
        // and its sealed digests are the durable local equivalents.
        submission_id: SubmissionId::unkeyed(record.job_id.as_ulid()),
        request_digest,
        job_id: record.job_id,
        executor_node_id: net.node_id(),
        spec_digest: request_digest,
        receipt_digest: control.fence_digest(record.job_id),
        outputs,
        committed_at_ms: aruna_core::util::unix_timestamp_millis(),
    };
    let envelope = JobRecordEnvelope::signed_with(
        record.created_by.realm_id,
        JobFamilyRecord::Output(Box::new(output)),
        net.node_id(),
        |message| net.sign(message),
    )
    .map_err(|error| JobError::permanent(format!("output record signing failed: {error}")))?;
    let frame = JobRecordFrame::new(envelope)
        .map_err(|error| JobError::permanent(format!("output record is unpublishable: {error}")))?;
    publish_output_record(context, record.job_id, control, frame).await
}

/// The record this execution already sealed for the same exact output set. A
/// replayed finalize reuses it instead of re-signing a second record that
/// differs only by its commit timestamp.
///
/// The stored row is re-verified on read-back: a row whose signature or
/// publisher no longer proves this node sealed it is not evidence, so a correct
/// record is sealed again instead of trusting it.
async fn sealed_record(
    context: &DriverContext,
    job_id: JobId,
    control: &AttemptControl,
    outputs: &OutputSet,
    publisher: NodeId,
) -> Result<Option<JobRecordEnvelope>, JobError> {
    let Some(digest) = control.output_record else {
        return Ok(None);
    };
    let envelope = read_output_record(&context.storage_handle, job_id, control.attempt_epoch)
        .await
        .map_err(|error| JobError::retryable(format!("output record read failed: {error}")))?;
    let Some(envelope) = envelope else {
        return Ok(None);
    };
    if envelope.published_by != publisher || envelope.verify_signature().is_err() {
        warn!(
            job_id = %job_id,
            attempt_epoch = control.attempt_epoch,
            "Stored output record does not authenticate; sealing a new one"
        );
        return Ok(None);
    }
    let JobFamilyRecord::Output(sealed) = &envelope.record else {
        return Ok(None);
    };
    let matches = sealed.outputs == *outputs && envelope.digest().ok() == Some(digest);
    Ok(matches.then_some(envelope))
}

/// The one place this execution's signed output record leaves the workflow. It
/// is durable locally before terminal success may name its digest, and it is
/// appended to the family record store here, after the local write and before
/// the digest is returned. The append marks it for family replication only when
/// the replicated chain proves it: a record proven by this node's own fence
/// alone stays local until its receipt exists.
async fn publish_output_record(
    context: &DriverContext,
    job_id: JobId,
    control: &AttemptControl,
    frame: JobRecordFrame,
) -> Result<[u8; 32], JobError> {
    let digest = frame
        .envelope()
        .digest()
        .map_err(|error| JobError::permanent(format!("output record digest failed: {error}")))?;
    let bytes = postcard::to_allocvec(frame.envelope())
        .map_err(|error| JobError::permanent(format!("output record encoding failed: {error}")))?;
    persist_output_record(&context.storage_handle, job_id, control, digest, bytes)
        .await
        .map_err(|error| JobError::retryable(format!("output record write failed: {error}")))?;
    append_output_record(context, job_id, control, frame).await?;
    Ok(digest)
}

/// Appends the sealed record to the append-only store. A realm that cannot
/// resolve the family view yet defers the record instead of failing the
/// execution: the record is already durable and the append is idempotent.
async fn append_output_record(
    context: &DriverContext,
    job_id: JobId,
    control: &AttemptControl,
    frame: JobRecordFrame,
) -> Result<(), JobError> {
    let Some(net) = context.net_handle.as_ref() else {
        return Err(JobError::permanent("output record needs a net handle"));
    };
    let envelope = frame.envelope();
    let JobFamilyRecord::Output(output) = &envelope.record else {
        return Err(JobError::permanent("sealed record is not an output record"));
    };
    let config = AppendRecordConfig {
        realm_id: envelope.realm_id,
        local_node_id: net.node_id(),
        local: Some(LocalExecution {
            node_id: net.node_id(),
            execution_id: control.execution_id,
            fence_digest: control.fence_digest(job_id),
            spec_digest: output.spec_digest,
        }),
        record: frame.clone(),
        origin: RecordOrigin::Local,
        now_ms: aruna_core::util::unix_timestamp_millis(),
    };
    match drive(AppendRecordOperation::new(config), context).await {
        Ok(outcome) if outcome.deferred => {
            debug!(
                job_id = %job_id,
                "Output record deferred: the local job family view is unavailable"
            );
            Ok(())
        }
        Ok(_) => Ok(()),
        // A realm this node has no config for cannot derive a family placement
        // at all; the record stays durable locally instead of failing the run.
        Err(RecordStoreError::RealmConfigMissing) => {
            debug!(job_id = %job_id, "Output record deferred: realm config unavailable");
            Ok(())
        }
        Err(error) => Err(JobError::retryable(format!(
            "output record append failed: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::structs::{
        AttemptIntent, ComputeResources, ExecutionSpec, JobClaim, JobPayload, JobState, RealmId,
    };
    use aruna_core::types::UserId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;
    use ulid::Ulid;

    use super::super::store::{insert_job, read_attempt_control, reserve_output_commits};
    use super::*;

    const TOKEN: Ulid = Ulid(0x5EA1);

    fn spec() -> ExecutionSpec {
        ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        }
    }

    async fn fixture() -> (
        tempfile::TempDir,
        DriverContext,
        JobRecord,
        aruna_net::NetHandle,
    ) {
        let dir = tempdir().unwrap();
        let storage: StorageHandle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId([1u8; 32]);
        let net = aruna_net::NetHandle::new(
            aruna_net::NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                realm_id,
                discovery_method: aruna_net::DiscoveryMethod::None,
                relay_method: aruna_net::RelayMethod::None,
                ..aruna_net::NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let mut record = JobRecord::new(
            JobId::from_bytes([4u8; 16]),
            JobPayload::Execution(spec()),
            UserId::new(Ulid::from_bytes([2u8; 16]), realm_id),
            net.node_id(),
            1_000,
            1_000,
            None,
        );
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: net.node_id(),
            claim_token: TOKEN,
            lease_expires_at_ms: 10_000,
        });
        insert_job(&storage, &record).await.unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (dir, context, record, net)
    }

    fn output(execution_id: Ulid, version_id: Ulid) -> OutputObject {
        OutputObject {
            bucket: "ws".to_string(),
            key: "reports/a.txt".to_string(),
            version_id,
            execution_id,
            container_path: "/out/a.txt".to_string(),
            size: 3,
            digest: None,
        }
    }

    #[tokio::test]
    async fn seal_is_durable() {
        // The signed record must be readable back under the digest terminal
        // success will name, and a replay must not mint a second record.
        let (_dir, context, record, net) = fixture().await;
        super::super::store::record_attempt_intent(
            &context.storage_handle,
            record.job_id,
            TOKEN,
            AttemptIntent {
                attempt_no: 1,
                external_name: "aruna-test-a1".to_string(),
                executor_kind: "docker".to_string(),
                pinned_image: "alpine@sha256:0".to_string(),
                attempt_epoch: 0,
            },
            2_000,
        )
        .await
        .unwrap();
        let destinations = vec![("ws".to_string(), "reports/a.txt".to_string())];
        let control = reserve_output_commits(&context.storage_handle, record.job_id, &destinations)
            .await
            .unwrap();
        let version_id = control.output_commits[0].version_id;
        let outputs = vec![output(control.execution_id, version_id)];

        let digest = seal_outputs(&context, &record, &control, &outputs)
            .await
            .unwrap();

        let stored = read_attempt_control(
            &context.storage_handle,
            record.job_id,
            control.attempt_epoch,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(stored.output_record, Some(digest));

        let envelope = read_output_record(
            &context.storage_handle,
            record.job_id,
            control.attempt_epoch,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(envelope.digest().unwrap(), digest);
        assert!(envelope.verify_signature().is_ok());
        let JobFamilyRecord::Output(sealed) = &envelope.record else {
            panic!("the sealed record is an output record");
        };
        assert_eq!(sealed.outputs.as_slice(), outputs.as_slice());
        assert_eq!(sealed.execution_id, control.execution_id);

        let replayed = seal_outputs(&context, &record, &stored, &outputs)
            .await
            .unwrap();
        assert_eq!(replayed, digest);
        net.shutdown().await;
    }
}
