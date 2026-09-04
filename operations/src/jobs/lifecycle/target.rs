//! Exact admission at the execution target.
//!
//! The target owns this decision alone. It re-fetches and verifies the stored
//! spec, checks that the offering scheduler is a holder in its own current
//! view, re-authorizes the stored submitter, re-evaluates placement against its
//! own execution subject, reserves exact local capacity, and only then signs and
//! persists the receipt that authorizes work. Replaying one launch returns the
//! same receipt instead of admitting a second execution, and a launch for a
//! family this node already runs, or already ran successfully, is declined.

use std::sync::Arc;

use aruna_compute::ExecutorRegistry;
use aruna_core::compute::{ExecutorCapability, ExecutorKind, NetworkAccess, ResourceEnvelope};
use aruna_core::effects::{
    Effect, HolderList, JobRecordEffect, JobRecordFrame, LaunchFrame, MAX_JOB_RECORD_HOLDERS,
    NetEffect, PageLimit, ReceiptFrame,
};
use aruna_core::errors::StorageError;
use aruna_core::events::{DeclinedPolicy, Event, JobRecordEvent, LaunchDecline, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::scheduling::PlannedInput;
use aruna_core::structs::{
    AuthContext, CapturedInput, ExecutionReceipt, InputSource, JobFamilyId, JobFamilyRecord,
    JobPayload, JobRecord, JobRecordEnvelope, JobRecordKind, LaunchIntent, LogicalJobSpec,
    Permission, PhysicalExecutionState, PlacementDecision, PlacementPolicyRef, PlacementSubject,
    PolicyResolution, RealmConfigDocument, WorkspaceMode, blob_group_permission_path,
    evaluate_placement,
};
use aruna_core::types::{Effects, NodeId};
use aruna_core::util::unix_timestamp_millis;
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;
use tracing::{debug, info, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::ids::{self, workspace_of};
use super::plan::{REALM_STAGING, network_access};
use super::reservation::{ReserveExecutionConfig, ReserveExecutionOperation};
use crate::driver::{DriverContext, drive, gate_context, now_ms};
use crate::jobs::records::reduce::reduce_family;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::records::{
    Admission, AppendRecordConfig, AppendRecordOperation, RecordOrigin, load_family_complete,
    load_kind_complete,
};
use crate::jobs::service::mint_local_job;
use crate::metadata::api::load_realm_config;
use crate::node_info::{read_node_info_document, read_operator_drain};
use crate::placement::resolve_shard_holders;
use crate::placement_policy::{ResolvePolicyConfig, ResolvePolicyOperation};
use crate::request_authorization::authorize;
use crate::request_policy::PolicyRequestExtras;

/// Wall-clock budget of the record fetch that pulls a missing stored spec.
const FETCH_DEADLINE: Duration = Duration::from_secs(10);
/// Reservation attempts one offer makes while no commit of its own became
/// durable: a lost race or a write storage never accepted.
pub(crate) const RESERVE_ATTEMPTS: u32 = 3;

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
    let view = FamilyView::resolve(&config, realm_id, family)?;
    if !view.holds(intent.scheduler_node_id) {
        return Some(Err(LaunchDecline::NotHolder));
    }
    let mut records = family_records(context, family).await?;
    if spec_of(&records, &intent).is_none() {
        fetch_family(context, &config, realm_id, family, intent.scheduler_node_id).await;
        records = family_records(context, family).await?;
    }
    let spec = spec_of(&records, &intent)?;
    // The launch itself becomes a retained record before it can be receipted.
    // Missing family evidence is fetchable, so the family is pulled once more
    // before the offer is answered as undecidable.
    let origin = RecordOrigin::Peer(intent.scheduler_node_id);
    if !append_record(context, realm_id, local, launch.envelope().clone(), origin).await {
        fetch_family(context, &config, realm_id, family, intent.scheduler_node_id).await;
        if !append_record(context, realm_id, local, launch.envelope().clone(), origin).await {
            return None;
        }
    }
    records = family_records(context, family).await?;
    // A replayed offer re-arms the wakeups the first acceptance may have lost,
    // so the receipt still replicates and the execution still starts.
    if let Some(decision) = existing_receipt(&records, &intent) {
        return Some(accepted(context.as_ref(), decision).await);
    }
    if cancelled(family, &records) {
        return Some(Err(LaunchDecline::Cancelled));
    }
    if already_running(family, &records, local) {
        return Some(Err(LaunchDecline::AlreadyRunning));
    }
    let capability = match local_capability(context.as_ref(), &config, local, &intent, &spec).await
    {
        Ok(capability) => capability,
        Err(decline) => return Some(Err(decline)),
    };
    // A node that stopped admitting governed data declines every new execution
    // target categorically, whatever the offered work references.
    match gate_context(context.as_ref(), realm_id, now_ms()).await {
        Ok(Some(gate)) if !gate.admitting => return Some(Err(LaunchDecline::Draining)),
        Err(_) => return None,
        Ok(_) => {}
    }
    if let Err(decline) = authorize_submitter(context.as_ref(), &spec, local).await {
        return Some(Err(decline));
    }
    match placement_verdict(context.as_ref(), &spec, &intent, &capability.subject).await {
        Ok(Some(decision)) => {
            return Some(Err(match DeclinedPolicy::new(decision) {
                Ok(policy) => LaunchDecline::Policy(policy),
                Err(_) => LaunchDecline::Unauthorized,
            }));
        }
        Ok(None) => {}
        Err(decline) => return Some(Err(decline)),
    }
    let limits = match backend_limits(context.as_ref(), &intent) {
        Some(limits) => limits,
        None => return Some(Err(LaunchDecline::Draining)),
    };
    if !limits.fits(&spec.resources) {
        return Some(Err(LaunchDecline::Capacity));
    }
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
    .await
}

/// Every record of one family, or nothing at all. An incomplete read leaves the
/// offer undecided: a receipt, a cancellation, or the stored spec may be in the
/// part that did not load, so nothing here may be reserved or minted.
async fn family_records(
    context: &Arc<DriverContext>,
    family: JobFamilyId,
) -> Option<Vec<JobRecordEnvelope>> {
    load_family_complete(context.as_ref(), family)
        .await
        .inspect_err(|error| {
            warn!(error = %error, "Job family read is incomplete; launch stays undecided");
        })
        .ok()
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
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let config = match store_receipt(context, &round).await {
        Ok(config) => config,
        Err(decline) => return Some(Err(decline)),
    };
    let job_id = config.job_id;
    let execution_id = config.execution_id;
    let frame = match commit_receipt(context, config, &round.intent).await? {
        Ok(frame) => frame,
        Err(decline) => return Some(Err(decline)),
    };
    info!(
        job_id = %round.spec.job_id,
        physical_job_id = %job_id,
        execution_id = %execution_id,
        executor_kind = %round.intent.target.executor_kind,
        subject_generation = round.capability.subject.generation,
        "Target admitted a launch and stored its receipt"
    );
    Some(Ok(frame))
}

/// Mints the local execution identity and signs the receipt that authorizes it,
/// leaving the reservation transaction as the only thing still to commit.
async fn store_receipt(
    context: &Arc<DriverContext>,
    round: &ReceiptRound,
) -> Result<ReserveExecutionConfig, LaunchDecline> {
    let now = unix_timestamp_millis();
    let execution_id = Ulid::generate();
    let physical_job_id = mint_local_job(
        context.as_ref(),
        round.realm_id,
        round.local,
        Some(&execution_id.to_bytes()),
    )
    .await
    .map_err(|_| LaunchDecline::Draining)?;
    let record = materialize_local(
        &round.spec,
        &round.intent,
        physical_job_id,
        round.local,
        now,
    )?;
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
    let stored_subject = (receipt.subject_generation, receipt.subject_digest);
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
    let frame = JobRecordFrame::new(envelope).map_err(|_| LaunchDecline::Unauthorized)?;
    Ok(ReserveExecutionConfig {
        realm_id: round.realm_id,
        local_node_id: round.local,
        envelope: round.limits,
        receipt: frame,
        launch: Box::new(round.intent.clone()),
        job_id: physical_job_id,
        logical_job_id: round.spec.job_id,
        execution_id,
        resources: round.spec.resources,
        subject_generation: stored_subject.0,
        subject_digest: stored_subject.1,
        record: Box::new(record),
        now_ms: now,
    })
}

/// Commits one stored receipt with its reservation. `None` is undecidable: the
/// offer was neither admitted nor refused and the scheduler may ask again.
pub(crate) async fn commit_receipt(
    context: &Arc<DriverContext>,
    config: ReserveExecutionConfig,
    intent: &LaunchIntent,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let ctx: &DriverContext = context.as_ref();
    commit_with(context, config, intent, move |config| {
        drive(ReserveExecutionOperation::new(config), ctx)
    })
    .await
}

/// What one reservation attempt decided, and what it proves about the writes it
/// tried to commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitVerdict {
    /// Exact local capacity refused the execution.
    Capacity,
    /// Another admission committed first; its receipt answers this offer.
    Raced,
    /// Storage never accepted the write, so the reservation may be reattempted.
    Retry,
    /// The commit neither succeeded nor was refused; its writes may exist.
    Uncertain,
    /// This node cannot establish that it may admit here at all.
    Drained,
    /// A local fault that decides nothing about the offer.
    Faulted,
}

/// Classifies one reservation failure by what it means, never by convenience:
/// only a real capacity verdict may be reported as capacity, and only a lost
/// membership or missing realm config as a drain.
pub(crate) fn classify(error: &LifecycleError) -> CommitVerdict {
    match error {
        LifecycleError::Capacity => CommitVerdict::Capacity,
        LifecycleError::Storage(StorageError::TransactionConflict) => CommitVerdict::Raced,
        LifecycleError::Storage(storage) if storage.proves_no_commit() => CommitVerdict::Retry,
        LifecycleError::Storage(_) => CommitVerdict::Uncertain,
        LifecycleError::NotHolder | LifecycleError::RealmConfigMissing => CommitVerdict::Drained,
        LifecycleError::Conversion(_)
        | LifecycleError::Record(_)
        | LifecycleError::Family(_)
        | LifecycleError::Store(_)
        | LifecycleError::IdempotencyConflict { .. }
        | LifecycleError::QuotaDenied(_)
        | LifecycleError::NotFinished
        | LifecycleError::UnexpectedEvent { .. } => CommitVerdict::Faulted,
    }
}

/// Commits one stored receipt through `reserve`, which is the reservation
/// transaction in production and the injected outcome under test.
pub(crate) async fn commit_with<F, Fut>(
    context: &Arc<DriverContext>,
    config: ReserveExecutionConfig,
    intent: &LaunchIntent,
    mut reserve: F,
) -> Option<Result<ReceiptFrame, LaunchDecline>>
where
    F: FnMut(ReserveExecutionConfig) -> Fut,
    Fut: Future<Output = Result<Ulid, LifecycleError>>,
{
    let family = config.receipt.envelope().family();
    for attempt in 1..=RESERVE_ATTEMPTS {
        let error = match reserve(config.clone()).await {
            Ok(_) => {
                let frame = ReceiptFrame::new(config.receipt.envelope().clone())
                    .map_err(|_| LaunchDecline::Unauthorized);
                return Some(accepted(context.as_ref(), frame).await);
            }
            Err(error) => error,
        };
        match classify(&error) {
            CommitVerdict::Capacity => return Some(Err(LaunchDecline::Capacity)),
            // A commit conflict is two admissions racing one launch, never this
            // node refusing work. The winner's receipt answers the offer, so it
            // is searched for before the reservation is attempted again.
            CommitVerdict::Raced => {
                if let Some(committed) = committed_receipt(context, family, intent).await {
                    return Some(accepted(context.as_ref(), committed).await);
                }
                debug!(attempt, "Execution reservation lost a race and retries");
                tokio::task::yield_now().await;
            }
            CommitVerdict::Retry => {
                debug!(attempt, "Storage refused the reservation write; retrying");
                tokio::task::yield_now().await;
            }
            // An unknown commit outcome may already be durable, so the receipt
            // is reconciled from the store exactly once and never reserved
            // again: the writes may exist and must not be redone.
            CommitVerdict::Uncertain => {
                warn!(error = %error, "Execution commit outcome is unknown; reconciling");
                let committed = committed_receipt(context, family, intent).await?;
                return Some(accepted(context.as_ref(), committed).await);
            }
            CommitVerdict::Drained => return Some(Err(LaunchDecline::Draining)),
            CommitVerdict::Faulted => {
                warn!(error = %error, "Execution reservation failed locally");
                return None;
            }
        }
    }
    None
}

/// Answers one decided offer and, when it was admitted, re-arms the runtime the
/// acceptance owes: the receipt still has to replicate and the execution still
/// has to start. A decline wakes nothing.
async fn accepted(
    context: &DriverContext,
    decision: Result<ReceiptFrame, LaunchDecline>,
) -> Result<ReceiptFrame, LaunchDecline> {
    if decision.is_ok() {
        super::outbox::kick(context).await;
        schedule_local(context).await;
    }
    decision
}

/// The receipt already committed for this exact launch, whoever won the race.
async fn committed_receipt(
    context: &Arc<DriverContext>,
    family: JobFamilyId,
    intent: &LaunchIntent,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let records = load_kind_complete(context.as_ref(), family, JobRecordKind::Receipt)
        .await
        .inspect_err(|error| {
            warn!(error = %error, "Receipt read is incomplete after a reservation race");
        })
        .ok()?;
    existing_receipt(&records, intent)
}

fn materialize_local(
    spec: &LogicalJobSpec,
    intent: &LaunchIntent,
    job_id: aruna_core::structs::JobId,
    local: NodeId,
    now_ms: u64,
) -> Result<JobRecord, LaunchDecline> {
    let mut payload = spec.payload.clone();
    for input in &mut payload.inputs {
        let Some(pin) = intent
            .inputs
            .iter()
            .find(|pin| pin.destination_key == input.dest_key)
        else {
            return Err(LaunchDecline::Unauthorized);
        };
        let InputSource::S3 { version_id, .. } = &mut input.source;
        *version_id = Some(pin.version_id.to_string());
        // The plan picked which holder the bytes come from; no source means the
        // target already holds the compliant copy.
        input.source_node_id = pin.source_node_id.or(Some(local));
    }
    payload.resources.cpu_cores = Some(spec.resources.cpu_cores);
    payload.resources.ram_bytes = Some(spec.resources.ram_bytes);
    // A stored disk of zero is the absence of a request, not a zero-byte
    // ceiling: storing it as `Some(0)` is a spec every backend refuses.
    payload.resources.disk_bytes =
        (spec.resources.disk_bytes > 0).then_some(spec.resources.disk_bytes);
    payload.resources.max_walltime_ms = Some(spec.resources.max_walltime_ms);
    payload.resources.preemptible = spec.resources.preemptible;
    payload.executor_constraint = Some(intent.target.executor_kind.clone());
    let (mode, bucket) = workspace_of(&spec.payload);
    let (physical_mode, physical_bucket) = match mode {
        WorkspaceMode::Existing if local != spec.ingress_node_id => {
            return Err(LaunchDecline::Unauthorized);
        }
        WorkspaceMode::Existing => (mode, bucket),
        WorkspaceMode::None => (mode, None),
    };
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(payload),
        spec.created_by,
        local,
        now_ms,
        now_ms,
        None,
    );
    record.retention_ms = spec.retention_ms;
    record.workspace_mode = physical_mode;
    record.workspace_bucket = physical_bucket;
    record.captured_inputs = spec.captured_inputs.clone();
    Ok(record)
}

async fn schedule_local(context: &DriverContext) {
    if let Some(task) = context.task_handle.as_ref() {
        use aruna_core::handle::Handle;
        let _ = task
            .send_effect(crate::jobs::submit::schedule_job_drain_effect())
            .await;
    }
}

/// The stored spec the launch names, by its exact digest.
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
pub(crate) fn existing_receipt(
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

/// An execution of the same family this node already accepted. A second launch
/// is refused while that execution may still finish, and after it succeeded, so
/// one family never runs twice here. The refusal is retryable: another target
/// may still take the launch.
pub(crate) fn already_running(
    family: JobFamilyId,
    records: &[JobRecordEnvelope],
    local: NodeId,
) -> bool {
    let Ok(Some(projection)) = reduce_family(family, records) else {
        return false;
    };
    projection.executions.iter().any(|execution| {
        execution.executor_node_id == local
            && (!execution.state.is_terminal()
                || execution.state == PhysicalExecutionState::Succeeded)
    })
}

fn cancelled(family: JobFamilyId, records: &[JobRecordEnvelope]) -> bool {
    records.iter().any(|envelope| {
        matches!(&envelope.record, JobFamilyRecord::Cancel(_)) && envelope.family() == family
    })
}

/// This node's own advertisement for the offered executor kind.
pub(crate) async fn local_capability(
    context: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    intent: &LaunchIntent,
    spec: &LogicalJobSpec,
) -> Result<ExecutorCapability, LaunchDecline> {
    let document = read_node_info_document(&context.storage_handle, local)
        .await
        .map_err(|_| LaunchDecline::Draining)?
        .ok_or(LaunchDecline::Draining)?;
    if !config
        .sync_eligible_node_ids()
        .is_ok_and(|members| members.contains(&local))
    {
        return Err(LaunchDecline::Unauthorized);
    }
    let kind = config
        .nodes
        .iter()
        .find(|node| node.node_id == local.to_string())
        .map(|node| &node.kind)
        .ok_or(LaunchDecline::Unauthorized)?;
    if !kind.is_sync_eligible() {
        return Err(LaunchDecline::Unauthorized);
    }
    // The durable operator flag is checked directly: a stale heartbeat document
    // must never re-enable offers this node was drained out of.
    if document.leaving
        || document.compute_draining
        || read_operator_drain(context).await.unwrap_or(true)
        || config
            .placement_entry(local)
            .is_some_and(|entry| entry.draining)
    {
        return Err(LaunchDecline::Draining);
    }
    let capability = document
        .executors
        .iter()
        .find(|capability| capability.kind == intent.target.executor_kind)
        .cloned()
        .ok_or(LaunchDecline::Unauthorized)?;
    if capability.policy_draining {
        return Err(LaunchDecline::Draining);
    }
    if capability.validate(local).is_err()
        || spec
            .payload
            .executor_constraint
            .as_deref()
            .is_some_and(|kind| kind.trim() != capability.kind.trim())
        || !capability.supports(REALM_STAGING)
        || !capability.limits.fits(&spec.resources)
    {
        return Err(LaunchDecline::Unauthorized);
    }
    let labels = ids::required_labels(&spec.payload).map_err(|_| LaunchDecline::Unauthorized)?;
    if !labels.iter().all(|label| {
        capability
            .subject
            .labels
            .get(label.key.trim())
            .map(|value| value.trim())
            == Some(label.value.trim())
    }) {
        return Err(LaunchDecline::Unauthorized);
    }
    let protected = !intent.output_policies.is_empty()
        || intent.inputs.iter().any(|input| !input.policies.is_empty());
    if protected && network_access(spec) == NetworkAccess::Open && !capability.network_policy {
        return Err(LaunchDecline::Unauthorized);
    }
    Ok(capability)
}

fn backend_limits(context: &DriverContext, intent: &LaunchIntent) -> Option<ResourceEnvelope> {
    let registry: &ExecutorRegistry = context.compute_handle.as_ref()?;
    let backend = registry.get(&ExecutorKind::from_wire(&intent.target.executor_kind))?;
    Some(backend.capabilities().limits)
}

/// The stored submitter must still hold WRITE on the group here. No bearer
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
        session: None,
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

/// Whether one pinned input still describes the captured input it names. Any
/// node may be the pinned source, because a registered copy of the same bytes
/// serves the read; the captured version, hash and size still bind the content,
/// and a source naming this target itself would not be a remote read at all.
pub(crate) fn pin_matches(
    captured: &CapturedInput,
    pin: &PlannedInput,
    ingress: NodeId,
    local: NodeId,
) -> bool {
    captured.source_node_id == ingress
        && captured.version_id == pin.version_id
        && captured.blake3 == pin.blake3
        && captured.bytes == pin.bytes
        && pin.source_node_id != Some(local)
}

/// Re-evaluates the placement refs this target can resolve against its own
/// execution subject. Refs of an input it cannot resolve yet are enforced again
/// by the materialization gate when the bytes are staged.
async fn placement_verdict(
    context: &DriverContext,
    spec: &LogicalJobSpec,
    intent: &LaunchIntent,
    subject: &PlacementSubject,
) -> Result<Option<PlacementDecision>, LaunchDecline> {
    if intent.inputs.len() != spec.payload.inputs.len() {
        return Err(LaunchDecline::Unauthorized);
    }
    if spec.captured_inputs.len() != spec.payload.inputs.len() {
        return Err(LaunchDecline::Unauthorized);
    }
    let mut inputs: Vec<Vec<PlacementPolicyRef>> = Vec::new();
    for input in &spec.payload.inputs {
        let InputSource::S3 { version_id, .. } = &input.source;
        let pin = intent
            .inputs
            .iter()
            .find(|pin| pin.destination_key == input.dest_key)
            .ok_or(LaunchDecline::Unauthorized)?;
        let requested = version_id
            .as_deref()
            .map(Ulid::from_string)
            .transpose()
            .map_err(|_| LaunchDecline::Unauthorized)?;
        if requested.is_some_and(|requested| requested != pin.version_id) {
            return Err(LaunchDecline::Unauthorized);
        }
        let captured = spec
            .captured_inputs
            .iter()
            .find(|captured| captured.destination_key == input.dest_key)
            .ok_or(LaunchDecline::Unauthorized)?;
        if !pin_matches(captured, pin, spec.ingress_node_id, subject.node_id) {
            return Err(LaunchDecline::Unauthorized);
        }
        let version = Some(captured.version_id);
        let hash = captured.blake3;
        let mut policies = captured.policies.clone();
        policies.sort_unstable();
        policies.dedup();
        if version != Some(pin.version_id) || hash != pin.blake3 || policies != pin.policies {
            return Err(LaunchDecline::Unauthorized);
        }
        inputs.push(policies);
    }
    let mut output_policies = inputs.iter().flatten().copied().collect::<Vec<_>>();
    output_policies.extend(spec.output_policies.clone());
    output_policies.sort_unstable();
    output_policies.dedup();
    if output_policies != intent.output_policies {
        return Err(LaunchDecline::Unauthorized);
    }
    let mut refs = output_policies.clone();
    refs.extend(inputs.iter().flatten().copied());
    refs.sort_unstable();
    refs.dedup();
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
    for policy_set in
        std::iter::once(output_policies.as_slice()).chain(inputs.iter().map(Vec::as_slice))
    {
        match evaluate_placement(policy_set, &policies, subject) {
            PlacementDecision::Allowed => {}
            decision => return Ok(Some(decision)),
        }
    }
    Ok(None)
}

async fn append_record(
    context: &Arc<DriverContext>,
    realm_id: aruna_core::structs::RealmId,
    local: NodeId,
    envelope: JobRecordEnvelope,
    origin: RecordOrigin,
) -> bool {
    let Ok(frame) = JobRecordFrame::new(envelope) else {
        return false;
    };
    match drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id,
            local_node_id: local,
            record: frame,
            local: None,
            origin,
            now_ms: unix_timestamp_millis(),
        }),
        context.as_ref(),
    )
    .await
    {
        Ok(outcome) => matches!(
            outcome.admission,
            Admission::Authentic | Admission::Duplicate
        ),
        Err(_) => false,
    }
}

/// Pulls one bounded page of the family from its holders, so a target that has
/// not synchronized the spec, claim, or budget yet can still admit the launch.
async fn fetch_family(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    realm_id: aruna_core::structs::RealmId,
    family: JobFamilyId,
    scheduler: NodeId,
) {
    let Ok(placement) = config.family_placement(family.submission_id) else {
        return;
    };
    let mut holders = resolve_shard_holders(config, &placement);
    holders.retain(|holder| *holder != scheduler);
    holders.truncate(MAX_JOB_RECORD_HOLDERS.saturating_sub(1));
    holders.insert(0, scheduler);
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
    let Ok((source, records)) = fetched else {
        return;
    };
    let Some(net) = context.net_handle.as_ref() else {
        return;
    };
    let local = net.node_id();
    let origin = source.map_or(RecordOrigin::Local, RecordOrigin::Peer);
    for frame in records {
        let _ = append_record(context, realm_id, local, frame.into_inner(), origin).await;
    }
}

/// Reads one bounded page of a family from its current holders, with the holder
/// that answered, so the records keep their real relay in the audit.
type FetchedFamily = (Option<NodeId>, Vec<JobRecordFrame>);

#[derive(Debug, PartialEq)]
struct FetchFamilyOperation {
    effect: Option<JobRecordEffect>,
    outcome: Option<Result<FetchedFamily, LifecycleError>>,
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
    type Output = FetchedFamily;
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
            Event::Net(NetEvent::JobRecord(JobRecordEvent::Fetched {
                holder, records, ..
            })) => Ok((Some(holder), records.into_inner())),
            Event::Net(NetEvent::JobRecord(JobRecordEvent::Unavailable(message))) => {
                debug!(message, "No family holder answered the record fetch");
                Ok((None, Vec::new()))
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

#[cfg(test)]
mod tests {
    use aruna_core::scheduling::PlannedInput;
    use aruna_core::structs::{InputMode, InputSelection, JobId, WorkspaceOutput};

    use super::*;
    use crate::jobs::records::tests::fixture::Family;

    #[test]
    fn materializes_captured_inputs() {
        // The physical row must execute the exact input, resources, backend, and retention stored.
        let family = Family::new([8u8; 32]);
        let mut spec = family.spec();
        spec.payload.inputs.push(InputSelection {
            source: InputSource::S3 {
                bucket: "source".to_string(),
                key: "reads.fastq".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "reads.fastq".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        });
        let mut launch = family.launch(&spec, family.holder.public(), 0);
        let version_id = Ulid::from_bytes([11u8; 16]);
        launch.inputs.push(PlannedInput {
            destination_key: "reads.fastq".to_string(),
            version_id,
            blake3: [12u8; 32],
            bytes: 3,
            policies: Vec::new(),
            source_node_id: None,
            transfer_ms: 0,
            known_link: true,
        });

        let record = materialize_local(
            &spec,
            &launch,
            JobId::from_bytes([10u8; 16]),
            family.target.public(),
            4_000,
        )
        .expect("launch materializes");
        let JobPayload::Execution(payload) = record.payload else {
            panic!("expected execution payload");
        };
        let InputSource::S3 {
            version_id: pinned, ..
        } = &payload.inputs[0].source;
        let expected_version = version_id.to_string();
        assert_eq!(pinned.as_deref(), Some(expected_version.as_str()));
        assert_eq!(payload.resources.cpu_cores, Some(spec.resources.cpu_cores));
        assert_eq!(payload.resources.ram_bytes, Some(spec.resources.ram_bytes));
        assert_eq!(
            payload.resources.disk_bytes,
            Some(spec.resources.disk_bytes)
        );
        assert_eq!(payload.executor_constraint, Some("docker".to_string()));
        assert_eq!(record.retention_ms, spec.retention_ms);
    }

    #[test]
    fn disk_stays_none() {
        // A request without a disk ceiling stores zero, which no backend accepts
        // as a container limit.
        let family = Family::new([8u8; 32]);
        let mut spec = family.spec();
        spec.resources.disk_bytes = 0;
        let launch = family.launch(&spec, family.holder.public(), 0);

        let record = materialize_local(
            &spec,
            &launch,
            JobId::from_bytes([10u8; 16]),
            family.target.public(),
            4_000,
        )
        .expect("launch materializes");
        let JobPayload::Execution(payload) = record.payload else {
            panic!("expected execution payload");
        };
        assert_eq!(payload.resources.disk_bytes, None);
    }

    #[test]
    fn remote_existing_rejected() {
        // Existing workspace contents are node-local until whole-bucket staging exists.
        let family = Family::new([8u8; 32]);
        let mut spec = family.spec();
        let ingress = family.holder.public();
        spec.ingress_node_id = ingress;
        spec.payload.workspace_outputs.push(WorkspaceOutput {
            container_path: "/out/result.txt".to_string(),
            dest_key: "results/result.txt".to_string(),
        });
        ids::store_workspace(
            &mut spec.payload,
            WorkspaceMode::Existing,
            Some("existing".to_string()),
        )
        .expect("workspace stored");
        spec.payload.resolve_outputs("existing", ingress);
        let launch = family.launch(&spec, family.holder.public(), 0);
        let physical_id = JobId::from_bytes([10u8; 16]);

        assert_eq!(
            materialize_local(&spec, &launch, physical_id, family.target.public(), 4_000),
            Err(LaunchDecline::Unauthorized)
        );
    }
}
