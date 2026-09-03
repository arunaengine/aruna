//! One realm, one submission family, and signed records for it.

use aruna_core::NodeId;
use aruna_core::compute::ExecutionTargetId;
use aruna_core::structs::{
    CancelAuthority, CollisionPolicy, ComputeResources, EffectiveResources, ExecutionOutputRecord,
    ExecutionReceipt, ExecutionSpec, ExecutionUpdate, JobAdmissionRecord, JobCancelRecord,
    JobFamilyId, JobFamilyRecord, JobId, JobRecordBody, JobRecordEnvelope, JobRetryPolicy,
    LaunchIntent, LogicalJobSpec, OutputObject, OutputSet, PhysicalExecutionResult,
    PhysicalExecutionState, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind,
    ResultMessage, SubmissionClaim, SubmissionId, WitnessBudgetRecord,
};
use aruna_core::types::UserId;
use ulid::Ulid;

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::Actor;
use aruna_storage::{FjallStorage, StorageHandle};
use tempfile::TempDir;

use crate::driver::DriverContext;
use crate::jobs::records::verify::FamilyView;

pub const REALM: RealmId = RealmId([3u8; 32]);

pub fn secret(seed: u8) -> iroh::SecretKey {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    iroh::SecretKey::from_bytes(&bytes)
}

pub fn node(seed: u8) -> NodeId {
    secret(seed).public()
}

/// One realm whose four nodes all hold every bucket, so any of them may author
/// a holder-published record.
pub struct Family {
    pub config: RealmConfigDocument,
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub placement: PlacementRef,
    pub holder: iroh::SecretKey,
    pub target: iroh::SecretKey,
    pub job_id: JobId,
}

impl Family {
    pub fn new(request_digest: [u8; 32]) -> Self {
        Self::with_local(request_digest, None)
    }

    /// `local` joins the realm as a fifth node, so a serving node with its own
    /// key holds the family placement too.
    pub fn with_local(request_digest: [u8; 32], local: Option<NodeId>) -> Self {
        let mut config = RealmConfigDocument::new(REALM, Vec::new(), 5);
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        if let Some(local) = local {
            config.ensure_node(local, RealmNodeKind::Server);
        }
        config.seed_default_placement();
        config.snapshot_candidate_map();
        let submission_id = SubmissionId::keyed(user(), b"idempotency");
        let placement = config
            .family_placement(submission_id)
            .expect("family placement resolves");
        Self {
            config,
            submission_id,
            request_digest,
            placement,
            holder: secret(1),
            target: secret(4),
            job_id: JobId::from_bytes([9u8; 16]),
        }
    }

    pub fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    pub fn view(&self) -> FamilyView {
        FamilyView::resolve(&self.config, REALM, self.family()).expect("holder view resolves")
    }

    pub fn spec(&self) -> LogicalJobSpec {
        self.spec_for(self.job_id, self.holder.public())
    }

    pub fn spec_for(&self, job_id: JobId, origin: NodeId) -> LogicalJobSpec {
        let resources = EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 1024,
            disk_bytes: 2048,
            max_walltime_ms: 60_000,
            preemptible: false,
        };
        LogicalJobSpec {
            submission_id: self.submission_id,
            job_id,
            origin_node_id: origin,
            ingress_node_id: origin,
            realm_id: REALM,
            group_id: Ulid::from_bytes([2u8; 16]),
            created_by: user(),
            created_at_ms: 1_000,
            retention_ms: aruna_core::structs::DEFAULT_JOB_RETENTION_MS,
            payload: payload(),
            request_digest: self.request_digest,
            spec_digest: [0u8; 32],
            resources,
            retry: JobRetryPolicy {
                max_launches_per_witness: 2,
            },
            admission: JobAdmissionRecord {
                submission_id: self.submission_id,
                request_digest: self.request_digest,
                job_id,
                group_id: Ulid::from_bytes([2u8; 16]),
                admitting_node_id: origin,
                membership_generation: 1,
                resources,
                admitted_at_ms: 1_000,
            },
            captured_inputs: Vec::new(),
            output_policies: Vec::new(),
            placement: self.placement,
        }
        .store_digest()
        .expect("spec digest stored")
    }

    pub fn claim(&self, spec: &LogicalJobSpec) -> SubmissionClaim {
        SubmissionClaim {
            submission_id: self.submission_id,
            job_id: spec.job_id,
            request_digest: self.request_digest,
            spec_digest: spec.spec_digest,
            committing_node_id: spec.origin_node_id,
            accepted_at_ms: 1_001,
        }
    }

    pub fn budget(&self, spec: &LogicalJobSpec, scheduler: NodeId) -> WitnessBudgetRecord {
        WitnessBudgetRecord {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
            scheduler_node_id: scheduler,
            source_spec_digest: spec.spec_digest,
            max_launches: 2,
        }
    }

    pub fn launch(&self, spec: &LogicalJobSpec, scheduler: NodeId, seq: u32) -> LaunchIntent {
        LaunchIntent {
            launch_id: Ulid::from_bytes([seq as u8 + 20; 16]),
            submission_id: self.submission_id,
            request_digest: self.request_digest,
            job_id: spec.job_id,
            scheduler_node_id: scheduler,
            scheduler_seq: seq,
            witness_placement: self.placement,
            holder_generation: 1,
            target: self.target_id(),
            inputs: Vec::new(),
            output_policies: Vec::new(),
            plan_digest: [5u8; 32],
            spec_digest: spec.spec_digest,
            created_at_ms: 1_002,
        }
    }

    pub fn target_id(&self) -> ExecutionTargetId {
        ExecutionTargetId {
            node_id: self.target.public(),
            executor_kind: "docker".to_string(),
        }
    }

    pub fn receipt(&self, launch: &LaunchIntent, execution: u8) -> ExecutionReceipt {
        ExecutionReceipt {
            execution_id: Ulid::from_bytes([execution; 16]),
            launch_id: launch.launch_id,
            launch_digest: launch.digest().expect("launch digest"),
            submission_id: self.submission_id,
            request_digest: self.request_digest,
            job_id: launch.job_id,
            executor_node_id: self.target.public(),
            target: launch.target.clone(),
            spec_digest: launch.spec_digest,
            membership_generation: 1,
            subject_generation: 1,
            subject_digest: [6u8; 32],
            accepted_at_ms: 1_003,
        }
    }

    pub fn update(
        &self,
        receipt: &ExecutionReceipt,
        sequence: u64,
        previous: [u8; 32],
        state: PhysicalExecutionState,
        output_digest: Option<[u8; 32]>,
    ) -> ExecutionUpdate {
        ExecutionUpdate {
            execution_id: receipt.execution_id,
            submission_id: self.submission_id,
            request_digest: self.request_digest,
            executor_node_id: receipt.executor_node_id,
            sequence,
            previous_digest: previous,
            state,
            observed_at_ms: 1_004 + sequence,
            result: output_digest.map(|digest| PhysicalExecutionResult {
                exit_code: Some(0),
                output_digest: Some(digest),
                message: None,
                stdout: ResultMessage::tail("out tail"),
                stderr: ResultMessage::tail("err tail"),
            }),
        }
    }

    pub fn output(&self, receipt: &ExecutionReceipt) -> ExecutionOutputRecord {
        ExecutionOutputRecord {
            execution_id: receipt.execution_id,
            submission_id: self.submission_id,
            request_digest: self.request_digest,
            job_id: receipt.job_id,
            executor_node_id: receipt.executor_node_id,
            spec_digest: receipt.spec_digest,
            receipt_digest: receipt.digest().expect("receipt digest"),
            outputs: OutputSet::canonical(vec![OutputObject {
                node_id: receipt.executor_node_id,
                bucket: "ws".to_string(),
                key: "out.txt".to_string(),
                version_id: Ulid::from_bytes([7u8; 16]),
                execution_id: receipt.execution_id,
                container_path: "/out/out.txt".to_string(),
                size: 3,
                digest: None,
            }])
            .expect("canonical outputs"),
            committed_at_ms: 1_010,
        }
    }

    pub fn cancel(&self, spec: &LogicalJobSpec) -> JobCancelRecord {
        JobCancelRecord {
            cancel_id: Ulid::from_bytes([11u8; 16]),
            submission_id: self.submission_id,
            request_digest: self.request_digest,
            job_id: spec.job_id,
            spec_digest: spec.spec_digest,
            requested_by: spec.created_by,
            authority: CancelAuthority::Submitter,
            requested_at_ms: 1_020,
        }
    }

    pub fn sign(&self, key: &iroh::SecretKey, record: JobFamilyRecord) -> JobRecordEnvelope {
        JobRecordEnvelope::sign(REALM, record, key).expect("record signs")
    }

    /// Every record of one physical execution that reaches `terminal`, in
    /// dependency order: spec, claim, budget, launch, receipt, output, updates.
    pub fn run(
        &self,
        execution: u8,
        seq: u32,
        terminal: PhysicalExecutionState,
    ) -> Vec<JobRecordEnvelope> {
        let spec = self.spec();
        let claim = self.claim(&spec);
        let budget = self.budget(&spec, self.holder.public());
        let launch = self.launch(&spec, self.holder.public(), seq);
        let receipt = self.receipt(&launch, execution);
        let output = self.output(&receipt);
        let output_digest = output.digest().expect("output digest");
        let accepted = self.update(
            &receipt,
            0,
            receipt.digest().expect("receipt digest"),
            PhysicalExecutionState::Running,
            None,
        );
        let closing = self.update(
            &receipt,
            1,
            accepted.digest().expect("update digest"),
            terminal,
            (terminal == PhysicalExecutionState::Succeeded).then_some(output_digest),
        );
        vec![
            self.sign(&self.holder, JobFamilyRecord::Spec(Box::new(spec))),
            self.sign(&self.holder, JobFamilyRecord::Claim(claim)),
            self.sign(&self.holder, JobFamilyRecord::Budget(budget)),
            self.sign(&self.holder, JobFamilyRecord::Launch(Box::new(launch))),
            self.sign(&self.target, JobFamilyRecord::Receipt(Box::new(receipt))),
            self.sign(&self.target, JobFamilyRecord::Output(Box::new(output))),
            self.sign(&self.target, JobFamilyRecord::Update(Box::new(accepted))),
            self.sign(&self.target, JobFamilyRecord::Update(Box::new(closing))),
        ]
    }
}

pub fn user() -> UserId {
    UserId::new(Ulid::from_bytes([8u8; 16]), REALM)
}

pub fn payload() -> ExecutionSpec {
    ExecutionSpec {
        group_id: Ulid::from_bytes([2u8; 16]),
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
        collision_policy: CollisionPolicy::default(),
    }
}

pub fn actor(node_id: NodeId) -> Actor {
    Actor {
        node_id,
        user_id: user(),
        realm_id: REALM,
    }
}

/// One storage-backed context holding this realm config, so operations that
/// resolve the family view run against real rows.
pub async fn context(config: &RealmConfigDocument, publisher: NodeId) -> (TempDir, DriverContext) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage: StorageHandle =
        FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
    let target = DocumentSyncTarget::RealmConfig { realm_id: REALM };
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: config
                .to_bytes(&actor(publisher))
                .expect("config encodes")
                .into(),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    (
        dir,
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        },
    )
}
