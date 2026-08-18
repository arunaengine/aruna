//! Exact admission at the execution target.
//!
//! The target owns this decision alone. It re-fetches and verifies the sealed
//! spec, checks that the offering scheduler is a holder in its own current
//! view, re-authorizes the sealed submitter, re-evaluates placement against its
//! own execution subject, reserves exact local capacity, and only then signs and
//! persists the receipt that authorizes work. Replaying one launch returns the
//! same receipt instead of admitting a second execution.

use std::sync::Arc;

use aruna_compute::ExecutorRegistry;
use aruna_core::compute::{ExecutorCapability, ExecutorKind, ResourceEnvelope};
use aruna_core::effects::{
    Effect, HolderList, JobRecordEffect, JobRecordFrame, LaunchFrame, MAX_JOB_RECORD_HOLDERS,
    NetEffect, PageLimit, ReceiptFrame,
};
use aruna_core::events::{DeclinedPolicy, Event, JobRecordEvent, LaunchDecline, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AuthContext, ExecutionReceipt, JobFamilyId, JobFamilyRecord, JobPayload, JobRecord,
    JobRecordEnvelope, LaunchIntent, LogicalJobSpec, Permission, PlacementDecision,
    PlacementPolicyRef, PlacementSubject, PolicyResolution, RealmConfigDocument,
    blob_group_permission_path, evaluate_placement,
};
use aruna_core::types::{Effects, NodeId};
use aruna_core::util::unix_timestamp_millis;
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{debug, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::ids::workspace_of;
use super::reservation::{ReserveExecutionConfig, ReserveExecutionOperation};
use super::witness::load_family;
use crate::driver::{DriverContext, GateContextError, drive, gate_context, now_ms};
use crate::jobs::records::verify::FamilyView;
use crate::jobs::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::jobs::store::insert_job;
use crate::metadata::api::load_realm_config;
use crate::node_info::read_node_info_document;
use crate::placement::resolve_shard_holders;
use crate::placement_policy::{ResolvePolicyConfig, ResolvePolicyOperation};
use crate::request_authorization::authorize;
use crate::request_policy::PolicyRequestExtras;
use crate::s3::head_object::{HeadObjectInput, HeadObjectOperation};

/// Wall-clock budget of the record fetch that pulls a missing sealed spec.
const FETCH_DEADLINE: Duration = Duration::from_secs(10);

/// Admits one offered launch, or answers why it was refused. `None` means the
/// target could not decide at all, which is an availability answer and never a
/// refusal the scheduler should act on.
pub async fn admit_launch(
    context: &Arc<DriverContext>,
    launch: LaunchFrame,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let net = context.net_handle.as_ref()?;
    let realm_id = *net.realm_id();
    let local = net.node_id();
    let config = load_realm_config(context.as_ref(), realm_id).await?;
    let envelope = launch.envelope().clone();
    let JobFamilyRecord::Launch(intent) = &envelope.record else {
        return Some(Err(LaunchDecline::Unauthorized));
    };
    let intent = intent.as_ref().clone();
    if intent.target.node_id != local {
        // The offer names another node; this target has nothing to admit.
        return Some(Err(LaunchDecline::Unauthorized));
    }
    let family = envelope.family();
    let Some(view) = FamilyView::resolve(&config, realm_id, family) else {
        return None;
    };
    if !view.holds(intent.scheduler_node_id) {
        return Some(Err(LaunchDecline::NotHolder));
    }
    let mut records = load_family(context.as_ref(), family).await;
    if spec_of(&records, &intent).is_none() {
        fetch_family(context, &config, realm_id, family).await;
        records = load_family(context.as_ref(), family).await;
    }
    let Some(spec) = spec_of(&records, &intent) else {
        return None;
    };
    // The launch itself becomes a retained record before it can be receipted.
    if !append_record(context, realm_id, local, launch.envelope().clone()).await {
        return None;
    }
    records = load_family(context.as_ref(), family).await;
    match existing_receipt(&records, &intent) {
        Some(Ok(receipt)) => return Some(Ok(receipt)),
        Some(Err(decline)) => return Some(Err(decline)),
        None => {}
    }
    if cancelled(family, &records) {
        return Some(Err(LaunchDecline::Cancelled));
    }
    let Some(capability) = local_capability(context.as_ref(), local, &intent).await else {
        return Some(Err(LaunchDecline::Draining));
    };
    if capability.policy_draining {
        return Some(Err(LaunchDecline::Draining));
    }
    match gate_context(context.as_ref(), realm_id, now_ms()).await {
        Err(GateContextError::AdmissionStopped) => return Some(Err(LaunchDecline::Draining)),
        Err(GateContextError::Routing(_)) => return None,
        Ok(_) => {}
    }
    if let Err(decline) = authorize_submitter(context.as_ref(), &spec, local).await {
        return Some(Err(decline));
    }
    if let Some(decision) = placement_verdict(context.as_ref(), &spec, &capability.subject).await {
        return Some(Err(match DeclinedPolicy::new(decision) {
            Ok(policy) => LaunchDecline::Policy(policy),
            Err(_) => LaunchDecline::Unauthorized,
        }));
    }
    let limits = match backend_limits(context.as_ref(), &intent) {
        Some(limits) => limits,
        None => return Some(Err(LaunchDecline::Draining)),
    };
    if !limits.fits(&spec.resources) {
        return Some(Err(LaunchDecline::Capacity));
    }
    Some(
        reserve_and_run(
            context,
            ReceiptRound {
                realm_id,
                local,
                spec,
                intent,
                capability,
                limits,
            },
        )
        .await,
    )
}

/// Everything one receipt round decides over, resolved before it starts.
struct ReceiptRound {
    realm_id: aruna_core::structs::RealmId,
    local: NodeId,
    spec: LogicalJobSpec,
    intent: LaunchIntent,
    capability: ExecutorCapability,
    limits: ResourceEnvelope,
}

/// Signs the receipt, reserves capacity with it in one transaction, and only
/// then materializes the local execution row the existing runtime claims.
async fn reserve_and_run(
    context: &Arc<DriverContext>,
    round: ReceiptRound,
) -> Result<ReceiptFrame, LaunchDecline> {
    let now = unix_timestamp_millis();
    let execution_id = Ulid::generate();
    let membership_generation = read_node_info_document(&context.storage_handle, round.local)
        .await
        .ok()
        .flatten()
        .map(|document| document.epoch.membership_generation)
        .unwrap_or_default();
    let launch_digest = match aruna_core::structs::JobRecordBody::digest(&round.intent) {
        Ok(digest) => digest,
        Err(_) => return Err(LaunchDecline::Unauthorized),
    };
    let receipt = ExecutionReceipt {
        execution_id,
        launch_id: round.intent.launch_id,
        launch_digest,
        submission_id: round.spec.submission_id,
        request_digest: round.spec.request_digest,
        job_id: round.spec.job_id,
        executor_node_id: round.local,
        target: round.intent.target.clone(),
        spec_digest: round.spec.spec_digest,
        membership_generation,
        subject_generation: round.capability.subject.generation,
        subject_digest: round.capability.subject_digest,
        accepted_at_ms: now,
    };
    let Some(net) = context.net_handle.as_ref() else {
        return Err(LaunchDecline::Draining);
    };
    let envelope = JobRecordEnvelope::signed_with(
        round.realm_id,
        JobFamilyRecord::Receipt(Box::new(receipt)),
        round.local,
        |message| net.sign(message),
    )
    .map_err(|_| LaunchDecline::Unauthorized)?;
    let frame = JobRecordFrame::new(envelope.clone()).map_err(|_| LaunchDecline::Unauthorized)?;
    let reserved = drive(
        ReserveExecutionOperation::new(ReserveExecutionConfig {
            realm_id: round.realm_id,
            local_node_id: round.local,
            envelope: round.limits,
            receipt: frame,
            launch: Box::new(round.intent.clone()),
            job_id: round.spec.job_id,
            execution_id,
            resources: round.spec.resources,
            now_ms: now,
        }),
        context.as_ref(),
    )
    .await;
    match reserved {
        Ok(_) => {}
        Err(LifecycleError::Capacity) => return Err(LaunchDecline::Capacity),
        Err(error) => {
            warn!(error = %error, "Execution reservation failed");
            return Err(LaunchDecline::Capacity);
        }
    }
    super::outbox::kick(context.as_ref()).await;
    materialize_local(context.as_ref(), &round.spec, round.local, now).await;
    ReceiptFrame::new(envelope).map_err(|_| LaunchDecline::Unauthorized)
}

/// Writes the local execution row post-receipt, so the existing drain claims
/// and runs the sealed plan exactly like a locally submitted execution.
async fn materialize_local(
    context: &DriverContext,
    spec: &LogicalJobSpec,
    local: NodeId,
    now_ms: u64,
) {
    let (mode, bucket) = workspace_of(&spec.payload);
    let mut record = JobRecord::new(
        spec.job_id,
        JobPayload::Execution(spec.payload.clone()),
        spec.created_by,
        local,
        now_ms,
        now_ms,
        None,
    );
    record.workspace_mode = mode;
    record.workspace_bucket = match mode {
        aruna_core::structs::WorkspaceMode::Existing => bucket,
        aruna_core::structs::WorkspaceMode::Temporary
        | aruna_core::structs::WorkspaceMode::Kept => {
            Some(JobRecord::workspace_bucket_name(spec.job_id))
        }
        aruna_core::structs::WorkspaceMode::None => None,
    };
    if let Err(error) = insert_job(&context.storage_handle, &record).await {
        warn!(job_id = %spec.job_id, error = %error, "Receipted execution row could not be written");
    }
    if let Some(task) = context.task_handle.as_ref() {
        use aruna_core::handle::Handle;
        let _ = task
            .send_effect(crate::jobs::submit::schedule_job_drain_effect())
            .await;
    }
}

/// The sealed spec the launch names, by its exact digest.
fn spec_of(records: &[JobRecordEnvelope], intent: &LaunchIntent) -> Option<LogicalJobSpec> {
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Spec(spec) if spec.spec_digest == intent.spec_digest => {
            Some(spec.as_ref().clone())
        }
        _ => None,
    })
}

/// The receipt this target already issued for the same launch id. A different
/// launch digest under one id is a conflict, never a second acceptance.
fn existing_receipt(
    records: &[JobRecordEnvelope],
    intent: &LaunchIntent,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let digest = aruna_core::structs::JobRecordBody::digest(intent).ok()?;
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Receipt(receipt) if receipt.launch_id == intent.launch_id => {
            match receipt.launch_digest == digest {
                true => Some(
                    ReceiptFrame::new(envelope.clone()).map_err(|_| LaunchDecline::LaunchConflict),
                ),
                false => Some(Err(LaunchDecline::LaunchConflict)),
            }
        }
        _ => None,
    })
}

fn cancelled(family: JobFamilyId, records: &[JobRecordEnvelope]) -> bool {
    records.iter().any(
        |envelope| match (&envelope.record, envelope.family() == family) {
            (JobFamilyRecord::Cancel(_), true) => true,
            _ => false,
        },
    )
}

/// This node's own advertisement for the offered executor kind.
async fn local_capability(
    context: &DriverContext,
    local: NodeId,
    intent: &LaunchIntent,
) -> Option<ExecutorCapability> {
    let document = read_node_info_document(&context.storage_handle, local)
        .await
        .ok()
        .flatten()?;
    if document.leaving || document.compute_draining {
        return None;
    }
    document
        .executors
        .iter()
        .find(|capability| capability.kind == intent.target.executor_kind)
        .cloned()
}

fn backend_limits(context: &DriverContext, intent: &LaunchIntent) -> Option<ResourceEnvelope> {
    let registry: &ExecutorRegistry = context.compute_handle.as_ref()?;
    let backend = registry.get(&ExecutorKind::from_wire(&intent.target.executor_kind))?;
    Some(backend.capabilities().limits)
}

/// The sealed submitter must still hold WRITE on the group here. No bearer
/// token replicates: the check runs against this node's replicated permissions.
async fn authorize_submitter(
    context: &DriverContext,
    spec: &LogicalJobSpec,
    local: NodeId,
) -> Result<(), LaunchDecline> {
    let auth = AuthContext {
        user_id: spec.created_by,
        realm_id: spec.realm_id,
        path_restrictions: None,
    };
    authorize(
        context,
        spec.realm_id,
        &auth,
        &blob_group_permission_path(spec.realm_id, spec.group_id, local),
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await
    .map_err(|_| LaunchDecline::Unauthorized)
}

/// Re-evaluates the placement refs this target can resolve against its own
/// execution subject. Refs of an input it cannot resolve yet are enforced again
/// by the materialization gate when the bytes are staged.
async fn placement_verdict(
    context: &DriverContext,
    spec: &LogicalJobSpec,
    subject: &PlacementSubject,
) -> Option<PlacementDecision> {
    let mut refs: Vec<PlacementPolicyRef> = Vec::new();
    for input in &spec.payload.inputs {
        let aruna_core::structs::InputSource::S3 { bucket, key, .. } = &input.source;
        let head = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: None,
            }),
            context,
        )
        .await;
        if let Ok(Some(Ok(head))) = head {
            refs.extend(head.source_policies);
        }
    }
    refs.sort_unstable();
    refs.dedup();
    if refs.is_empty() {
        return None;
    }
    let mut policies: BTreeMap<Ulid, PolicyResolution> = BTreeMap::new();
    for policy_ref in &refs {
        let resolution = drive(
            ResolvePolicyOperation::new(ResolvePolicyConfig {
                realm_id: spec.realm_id,
                policy_ref: *policy_ref,
                local_node_id: subject.node_id,
                now_ms: now_ms(),
            }),
            context,
        )
        .await;
        policies.insert(
            policy_ref.policy_id,
            match resolution {
                Ok(resolved) => PolicyResolution::Known(resolved.policy),
                Err(_) => PolicyResolution::Unresolved,
            },
        );
    }
    match evaluate_placement(&refs, &policies, subject) {
        PlacementDecision::Allowed => None,
        decision => Some(decision),
    }
}

async fn append_record(
    context: &Arc<DriverContext>,
    realm_id: aruna_core::structs::RealmId,
    local: NodeId,
    envelope: JobRecordEnvelope,
) -> bool {
    let Ok(frame) = JobRecordFrame::new(envelope) else {
        return false;
    };
    drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id,
            local_node_id: local,
            record: frame,
            local: None,
            origin: RecordOrigin::Local,
            now_ms: unix_timestamp_millis(),
        }),
        context.as_ref(),
    )
    .await
    .is_ok()
}

/// Pulls one bounded page of the family from its holders, so a target that has
/// not synchronized the spec, claim, or budget yet can still admit the launch.
async fn fetch_family(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    realm_id: aruna_core::structs::RealmId,
    family: JobFamilyId,
) {
    let Ok(placement) = config.family_placement(family.submission_id) else {
        return;
    };
    let holders: Vec<NodeId> = resolve_shard_holders(config, &placement)
        .into_iter()
        .take(MAX_JOB_RECORD_HOLDERS)
        .collect();
    let Ok(holders) = HolderList::new(holders) else {
        return;
    };
    let fetched = drive(
        FetchFamilyOperation::new(JobRecordEffect::Fetch {
            realm_id,
            placement,
            holders,
            submission_id: family.submission_id,
            request_digest: Some(family.request_digest),
            cursor: None,
            limit: PageLimit::default(),
            deadline: FETCH_DEADLINE,
        }),
        context.as_ref(),
    )
    .await;
    let Ok(records) = fetched else {
        return;
    };
    let Some(net) = context.net_handle.as_ref() else {
        return;
    };
    let local = net.node_id();
    for frame in records {
        let _ = append_record(context, realm_id, local, frame.into_inner()).await;
    }
}

/// Reads one bounded page of a family from its current holders.
#[derive(Debug, PartialEq)]
struct FetchFamilyOperation {
    effect: Option<JobRecordEffect>,
    outcome: Option<Result<Vec<JobRecordFrame>, LifecycleError>>,
}

impl FetchFamilyOperation {
    fn new(effect: JobRecordEffect) -> Self {
        Self {
            effect: Some(effect),
            outcome: None,
        }
    }
}

impl Operation for FetchFamilyOperation {
    type Output = Vec<JobRecordFrame>;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        match self.effect.take() {
            Some(effect) => smallvec![Effect::Net(NetEffect::JobRecord(Box::new(effect)))],
            None => {
                self.outcome = Some(Err(LifecycleError::NotFinished));
                smallvec![]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        self.outcome = Some(match event {
            Event::Net(NetEvent::JobRecord(JobRecordEvent::Fetched { records, .. })) => {
                Ok(records.into_inner())
            }
            Event::Net(NetEvent::JobRecord(JobRecordEvent::Unavailable(message))) => {
                debug!(message, "No family holder answered the record fetch");
                Ok(Vec::new())
            }
            other => Err(LifecycleError::UnexpectedEvent {
                state: "Fetch".to_string(),
                expected: "job record page",
                got: format!("{other:?}"),
            }),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.outcome.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
