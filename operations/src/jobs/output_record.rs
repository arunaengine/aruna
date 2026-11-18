//! Builds, signs and stores an execution's output record and returns the digest success names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, NetEffect};
use aruna_core::events::{Event, ExecutionSignEvent, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    AttemptControl, ExecutionClaim, ExecutionOutputRecord, JobError, JobFamilyRecord, JobRecord,
    JobRecordEnvelope, LocalExecution, OutputObject, OutputSet, SubmissionId,
};
use aruna_core::structs::identity::realm::RealmId;
use tracing::{debug, warn};

use super::lifecycle::LifecycleError;
use super::lifecycle::chain::{ExecutionChain, ExecutionChainOperation};
use super::records::RecordStoreError;
use super::records::admit::Admission;
use super::records::append::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin,
};
use super::records::pages::ServingNode;
use super::store::JobMutationError;
use super::store::job_calls::persist_output_record;
use super::store::row_calls::read_output_record;
use crate::driver::{DriverContext, drive};

#[derive(Debug, PartialEq)]
enum OutputStep {
    Chain,
    Stored,
    Signed,
    Persisted(Box<JobRecordFrame>),
    Appended,
}

/// Builds, signs and makes durable this execution's exact output record, then answers the
/// digest terminal success must name. Success is refused when this fails, so an unpublishable
/// or unsigned output set never becomes a result.
#[derive(Debug, PartialEq)]
pub struct StoreOutputsOperation {
    record: JobRecord,
    control: AttemptControl,
    outputs: Vec<OutputObject>,
    serving: ServingNode,
    now_ms: u64,
    set: Option<OutputSet>,
    chain: Option<ExecutionChain>,
    receipted: bool,
    digest: [u8; 32],
    step: OutputStep,
}

type OutputNext = Result<Next<[u8; 32]>, JobError>;

impl StoreOutputsOperation {
    pub fn new(
        record: &JobRecord,
        control: &AttemptControl,
        outputs: &[OutputObject],
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        Self {
            record: record.clone(),
            control: control.clone(),
            outputs: outputs.to_vec(),
            serving,
            now_ms,
            set: None,
            chain: None,
            receipted: false,
            digest: [0; 32],
            step: OutputStep::Chain,
        }
    }

    fn node(&self) -> Result<(NodeId, RealmId), JobError> {
        self.serving
            .ok_or_else(|| JobError::retryable("output record needs a net handle"))
    }

    fn output_set(&self) -> Result<&OutputSet, JobError> {
        (self.set.as_ref()).ok_or_else(|| JobError::retryable("output set was lost"))
    }

    /// A replay re-appends an idempotent record and a previously unavailable family may resolve
    /// now; a receipted execution binds the real chain, else locals.
    fn chained(&mut self, chain: Option<ExecutionChain>) -> OutputNext {
        self.receipted = chain.is_some();
        self.chain = chain;
        if self.control.output_record.is_none() {
            return self.sign();
        }
        self.step = OutputStep::Stored;
        let read = read_output_record(self.record.job_id, self.control.attempt_epoch);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// The record this execution already stored for the same exact output set; a replayed
    /// finalize reuses it instead of re-signing a record that differs only by commit time.
    /// Re-verified on read-back, so a bad row is stored again.
    fn reusable(&self, envelope: JobRecordEnvelope) -> Result<Option<JobRecordEnvelope>, JobError> {
        let (publisher, _) = self.node()?;
        if envelope.published_by != publisher || envelope.verify_signature().is_err() {
            warn!(
                job_id = %self.record.job_id,
                attempt_epoch = self.control.attempt_epoch,
                "Stored output record does not authenticate; storing a new one"
            );
            return Ok(None);
        }
        let JobFamilyRecord::Output(stored) = &envelope.record else {
            return Ok(None);
        };
        let same = stored.outputs == *self.output_set()?
            && envelope.digest().ok() == self.control.output_record;
        Ok(same.then_some(envelope))
    }

    fn stored(&mut self, envelope: Option<JobRecordEnvelope>) -> OutputNext {
        let reused = match envelope {
            Some(envelope) => self.reusable(envelope)?,
            None => None,
        };
        let Some(envelope) = reused else {
            return self.sign();
        };
        let frame = JobRecordFrame::new(envelope).map_err(|error| {
            JobError::permanent(format!("stored record is unpublishable: {error}"))
        })?;
        self.digest = frame_digest(&frame)?;
        self.append(frame)
    }

    fn sign(&mut self) -> OutputNext {
        let (publisher, realm_id) = self.node()?;
        let (record, control, chain) = (&self.record, &self.control, self.chain);
        let request_digest = record
            .plan_digest
            .ok_or_else(|| JobError::permanent("execution job carries no plan digest"))?;
        let output = ExecutionOutputRecord {
            execution_id: control.execution_id,
            submission_id: chain
                .map(|chain| chain.family.submission_id)
                .unwrap_or_else(|| SubmissionId::unkeyed(record.job_id.as_ulid())),
            request_digest: chain
                .map(|chain| chain.family.request_digest)
                .unwrap_or(request_digest),
            // The receipt names the logical job; the local physical id addresses
            // nothing in the family and would fail the receipt evidence check.
            job_id: chain.map_or(record.job_id, |chain| chain.job_id),
            executor_node_id: publisher,
            spec_digest: chain
                .map(|chain| chain.spec_digest)
                .unwrap_or(request_digest),
            receipt_digest: chain
                .map(|chain| chain.receipt_digest)
                .unwrap_or_else(|| control.fence_digest(record.job_id)),
            outputs: self.output_set()?.clone(),
            committed_at_ms: self.now_ms,
        };
        let claim = ExecutionClaim {
            realm_id,
            record: JobFamilyRecord::Output(Box::new(output)),
            publisher,
        };
        self.step = OutputStep::Signed;
        let effect = Effect::Net(NetEffect::ExecutionSign(Box::new(claim)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    /// Durable locally before success may name its digest.
    fn signed(&mut self, result: IoResult) -> OutputNext {
        let expected = "Event::Net(NetEvent::ExecutionSign)";
        let IoResult::Event(event) = result else {
            let got = result.label();
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let envelope = match *event {
            Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Signed(envelope))) => *envelope,
            Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Unavailable(error))) => {
                return Err(JobError::retryable(format!(
                    "output record signing failed: {error}"
                )));
            }
            _ => {
                let got = "another event";
                return Err(IoError::Unexpected { expected, got }.into());
            }
        };
        let frame = JobRecordFrame::new(envelope).map_err(|error| {
            JobError::permanent(format!("output record is unpublishable: {error}"))
        })?;
        self.digest = frame_digest(&frame)?;
        let bytes = postcard::to_allocvec(frame.envelope()).map_err(|error| {
            JobError::permanent(format!("output record encoding failed: {error}"))
        })?;
        let job_id = self.record.job_id;
        let persist = persist_output_record(job_id, &self.control, self.digest, bytes);
        self.step = OutputStep::Persisted(Box::new(frame));
        Ok(Next::Call(IoCall::child(persist)))
    }

    /// Appends the stored record to the family store; only the chain proves it for replication,
    /// and the local fence is evidence while no receipt exists.
    fn append(&mut self, frame: JobRecordFrame) -> OutputNext {
        let (local_node_id, _) = self.node()?;
        let envelope = frame.envelope();
        let JobFamilyRecord::Output(output) = &envelope.record else {
            return Err(JobError::permanent("stored record is not an output record"));
        };
        let config = AppendRecordConfig {
            realm_id: envelope.realm_id,
            local_node_id,
            local: (!self.receipted).then(|| LocalExecution {
                node_id: local_node_id,
                execution_id: self.control.execution_id,
                fence_digest: self.control.fence_digest(self.record.job_id),
                spec_digest: output.spec_digest,
            }),
            record: frame.clone(),
            origin: RecordOrigin::Local,
            now_ms: self.now_ms,
        };
        self.step = OutputStep::Appended;
        Ok(Next::Call(IoCall::child(AppendRecordOperation::new(
            config,
        ))))
    }

    /// A realm that cannot resolve the family view yet defers the record instead of failing the
    /// execution: the record is already durable and the append is idempotent.
    fn appended(&self, result: Result<AppendOutcome, RecordStoreError>) -> OutputNext {
        let receipted = self.receipted;
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(RecordStoreError::RealmConfigMissing) if !receipted => {
                return Ok(Next::Done(self.digest));
            }
            Err(error) => {
                return Err(JobError::retryable(format!(
                    "output record append failed: {error}"
                )));
            }
        };
        match outcome.admission {
            Admission::Authentic if receipted => {}
            Admission::Local if !receipted => {}
            Admission::Duplicate => {}
            Admission::Pending(_) if outcome.deferred && !receipted => {
                debug!(
                    job_id = %self.record.job_id,
                    "Output record deferred: the local job family view is unavailable"
                );
            }
            Admission::Pending(_) | Admission::PendingFull => {
                return Err(JobError::retryable(
                    "output record admission is awaiting family evidence",
                ));
            }
            Admission::Conflict | Admission::Rejected(_) => {
                return Err(JobError::permanent(
                    "output record admission rejected the stored record",
                ));
            }
            admission => {
                return Err(JobError::permanent(format!(
                    "output record admission invalid for execution mode: {admission:?}"
                )));
            }
        }
        Ok(Next::Done(self.digest))
    }
}

fn frame_digest(frame: &JobRecordFrame) -> Result<[u8; 32], JobError> {
    frame
        .envelope()
        .digest()
        .map_err(|error| JobError::permanent(format!("output record digest failed: {error}")))
}

impl Operation for StoreOutputsOperation {
    type Output = [u8; 32];
    type Error = JobError;

    fn first(&mut self) -> OutputNext {
        self.node()?;
        if self.record.plan_digest.is_none() {
            return Err(JobError::permanent("execution job carries no plan digest"));
        }
        let set = OutputSet::canonical(std::mem::take(&mut self.outputs)).map_err(|error| {
            JobError::permanent(format!("output set is not canonical: {error}"))
        })?;
        self.set = Some(set);
        let chain =
            ExecutionChainOperation::of_attempt(self.record.job_id, self.control.execution_id);
        Ok(Next::Call(IoCall::child(chain)))
    }

    fn next(&mut self, result: IoResult) -> OutputNext {
        match std::mem::replace(&mut self.step, OutputStep::Appended) {
            OutputStep::Chain => {
                let chain = result.child::<Option<ExecutionChain>, LifecycleError>()?;
                self.chained(chain.ok().flatten())
            }
            OutputStep::Stored => {
                let read = result.child::<Option<JobRecordEnvelope>, JobMutationError>()?;
                self.stored(read.map_err(|error| {
                    JobError::retryable(format!("output record read failed: {error}"))
                })?)
            }
            OutputStep::Signed => self.signed(result),
            OutputStep::Persisted(frame) => {
                result
                    .child::<(), JobMutationError>()?
                    .map_err(|error| match error {
                        JobMutationError::OutputRecordConflict => {
                            JobError::permanent("execution output record digest is already stored")
                        }
                        error => {
                            JobError::retryable(format!("output record write failed: {error}"))
                        }
                    })?;
                self.append(*frame)
            }
            OutputStep::Appended => {
                self.appended(result.child::<AppendOutcome, RecordStoreError>()?)
            }
        }
    }
}

/// See [`StoreOutputsOperation`].
pub async fn store_outputs(
    context: &DriverContext,
    record: &JobRecord,
    control: &AttemptControl,
    outputs: &[OutputObject],
) -> Result<[u8; 32], JobError> {
    let serving = context.node_and_realm();
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let store = StoreOutputsOperation::new(record, control, outputs, serving, now_ms);
    drive(store, context).await
}

#[cfg(test)]
mod tests {
    use aruna_core::UserId;
    use aruna_core::structs::execution::job::{
        AttemptIntent, ComputeResources, ExecutionSpec, JobClaim, JobPayload, JobState,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;
    use ulid::Ulid;

    use super::super::store::attempt::{read_attempt_control, reserve_output_commits};
    use super::*;
    use crate::jobs::store::query::insert_job;

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
            lease_expires_ms: 10_000,
        });
        insert_job(&crate::testing::test_context(storage.clone()), &record)
            .await
            .unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (dir, context, record, net)
    }

    fn output(execution_id: Ulid, version_id: Ulid) -> OutputObject {
        OutputObject {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
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
    async fn store_is_durable() {
        // The signed record must be readable back under the digest terminal
        // success will name, and a replay must not mint a second record.
        let (_dir, context, record, net) = fixture().await;
        super::super::store::state::record_attempt_intent(
            &context,
            record.job_id,
            TOKEN,
            AttemptIntent {
                attempt_no: 1,
                external_name: "aruna-test-a1".to_string(),
                executor_kind: "docker".to_string(),
                pinned_image: "alpine@sha256:0".to_string(),
                attempt_epoch: 0,
            },
            None,
            None,
            2_000,
        )
        .await
        .unwrap();
        let destinations = vec![(net.node_id(), "ws".to_string(), "reports/a.txt".to_string())];
        let control =
            reserve_output_commits(&context, record.job_id, &destinations, &Default::default())
                .await
                .unwrap();
        let version_id = control.output_commits[0].version_id;
        let outputs = vec![output(control.execution_id, version_id)];

        let digest = store_outputs(&context, &record, &control, &outputs)
            .await
            .unwrap();

        let stored = read_attempt_control(&context, record.job_id, control.attempt_epoch, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.output_record, Some(digest));

        let envelope = read_output_record(&context, record.job_id, control.attempt_epoch)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(envelope.digest().unwrap(), digest);
        assert!(envelope.verify_signature().is_ok());
        let JobFamilyRecord::Output(output_record) = &envelope.record else {
            panic!("the stored record is an output record");
        };
        assert_eq!(output_record.outputs.as_slice(), outputs.as_slice());
        assert_eq!(output_record.execution_id, control.execution_id);

        let replayed = store_outputs(&context, &record, &stored, &outputs)
            .await
            .unwrap();
        assert_eq!(replayed, digest);
        net.shutdown().await;
    }
}
