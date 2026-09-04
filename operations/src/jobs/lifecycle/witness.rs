//! Leaderless witness scheduling.
//!
//! Every current holder of a submission family is a witness. Each computes the
//! same rank from the immutable identity, so the admitting node plans at once
//! and later ranks only step in after their own persisted delay. No witness
//! holds a lease, and a partition may therefore produce one execution per
//! participating witness.

use std::time::Duration;

use aruna_core::compute::ExecutionTargetId;
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame, LaunchOfferEffect, NetEffect};
use aruna_core::events::{Event, LaunchDecline, LaunchOfferEvent, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    JOB_PLAN_EXPLAIN_KEYSPACE, JOB_WITNESS_DEADLINE_INDEX_KEYSPACE, JOB_WITNESS_DEADLINE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::scheduling::{ExecutionPlan, MAX_PLAN_CANDIDATES};
use aruna_core::structs::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordKind, LaunchIntent, LogicalJobSpec,
    PhysicalExecutionState, PlacementDecision, RealmConfigDocument, WitnessBudgetRecord,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, Key, NodeId, TxnId};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use tracing::{debug, info, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::plan::build_plan;
use crate::driver::{DriverContext, drive};
use crate::jobs::records::reduce::reduce_family;
use crate::jobs::records::rows::{from_bytes, to_bytes};
use crate::jobs::records::verify::FamilyView;
use crate::jobs::records::{
    Admission, AppendRecordConfig, AppendRecordOperation, RecordOrigin, load_family_complete,
    load_kind_complete,
};
use crate::jobs::store::{batch_delete, iter_prefix_page};
use crate::metadata::api::load_realm_config;

/// Domain of the stable witness order.
pub const WITNESS_RANK_DOMAIN: &[u8] = b"aruna-job-witness-v1";
/// Families one drain pass considers.
pub const WITNESS_DRAIN_BATCH: usize = 64;
/// Spacing between drain passes while deadlines remain.
pub const WITNESS_RETRY_AFTER: Duration = Duration::from_secs(1);
/// Wall-clock budget of one launch offer.
pub const OFFER_DEADLINE: Duration = Duration::from_secs(30);
/// Offers a target must leave unanswered before it is excluded from the next
/// plan. One is never enough: a target that accepts slowly would be replaced by
/// a second launch while it is already running the work.
pub const LOST_TARGET_OFFERS: u32 = 2;
/// Declined targets one explain row retains.
pub const MAX_DECLINED_TARGETS: usize = MAX_PLAN_CANDIDATES;

const DEADLINE_KEY_BYTES: usize = 8 + 64;

/// One family's persisted fallback deadline. The row exists while this node may
/// still have to launch, so a restart resumes the same schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WitnessDeadline {
    pub due_at_ms: u64,
    pub rank: u32,
}

/// The bounded plan this witness stored before it launched, with the targets
/// that already refused this request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WitnessExplain {
    pub sequence: u32,
    pub plan: ExecutionPlan,
    pub declined: Vec<ExecutionTargetId>,
    /// The previous launch was never confirmed, so this one may overlap it.
    pub overlapping: bool,
    pub stored_at_ms: u64,
}

/// Kicks the witness queue without persisting a timer of its own.
pub fn schedule_witness_drain(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::DrainJobWitnessQueue,
        after,
    })
}

/// Position of `node` in the witness order. The admitting node ranks first,
/// because it already holds the request and every input decision it made; the
/// remaining witnesses follow the domain-separated digest of the immutable
/// identity, which is identical on every node and unbiasable by any publisher.
pub fn witness_rank(
    holders: &[NodeId],
    family: &JobFamilyId,
    node: NodeId,
    admitting: Option<NodeId>,
) -> Option<u32> {
    let admitting = admitting.filter(|first| holders.contains(first));
    if admitting == Some(node) {
        return Some(0);
    }
    let mut ordered: Vec<([u8; 32], NodeId)> = holders
        .iter()
        .filter(|holder| Some(**holder) != admitting)
        .map(|holder| (rank_key(family, *holder), *holder))
        .collect();
    ordered.sort_unstable();
    let rank = ordered.iter().position(|(_, holder)| *holder == node)? as u32;
    Some(rank + u32::from(admitting.is_some()))
}

fn rank_key(family: &JobFamilyId, node: NodeId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(WITNESS_RANK_DOMAIN);
    hasher.update(&family.submission_id.0);
    hasher.update(&family.request_digest);
    hasher.update(node.as_bytes());
    *hasher.finalize().as_bytes()
}

/// Deterministic jitter below a quarter of the base delay, so equal ranks in
/// different families never fire in lockstep and a replay picks the same value.
fn jitter_ms(family: &JobFamilyId, node: NodeId, base_delay_ms: u64) -> u64 {
    let key = rank_key(family, node);
    let spread = base_delay_ms / 4 + 1;
    u64::from(u16::from_be_bytes([key[0], key[1]])) % spread
}

/// Arms this node's fallback for one family. Rank zero is due immediately; a
/// later rank waits `base_delay * rank` plus its own jitter. An earlier existing
/// deadline is kept, so re-arming can only ever bring a round forward.
pub async fn arm_family(context: &DriverContext, family: JobFamilyId, now_ms: u64) {
    let Some(net) = context.net_handle.as_ref() else {
        debug!(?family, "No net handle; witness fallback not armed");
        return;
    };
    let realm_id = *net.realm_id();
    let local = net.node_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        warn!(
            ?family,
            "Realm config unavailable; witness fallback not armed"
        );
        return;
    };
    let Some(view) = FamilyView::resolve(&config, realm_id, family) else {
        warn!(
            ?family,
            "Family placement unresolved; witness fallback not armed"
        );
        return;
    };
    let admitting = admitting_node(context, family).await;
    let Some(rank) = witness_rank(view.holders(), &family, local, admitting) else {
        debug!(?family, "Node is not a witness for this family");
        return;
    };
    let base = config.compute.witness_base_delay_ms;
    let due_at_ms = match rank {
        0 => now_ms,
        rank => now_ms + base * u64::from(rank) + jitter_ms(&family, local, base),
    };
    let existing = match read_deadline_index(context, &family).await {
        Ok(existing) => existing,
        Err(error) => {
            warn!(?family, error = %error, "Witness deadline read failed");
            return;
        }
    };
    if existing
        .as_ref()
        .is_some_and(|existing| existing.due_at_ms <= due_at_ms)
    {
        return;
    }
    let deadline = WitnessDeadline { due_at_ms, rank };
    if !replace_deadline(context, &family, existing, deadline).await {
        return;
    }
    let after = Duration::from_millis(due_at_ms.saturating_sub(now_ms));
    if let Some(task) = context.task_handle.as_ref() {
        let _ = task.send_effect(schedule_witness_drain(after)).await;
    }
}

/// The node that admitted the family: the committing node of the claim with the
/// lowest order key, which every holder reduces the same way. An unreadable
/// claim set leaves the order to the digest alone.
async fn admitting_node(context: &DriverContext, family: JobFamilyId) -> Option<NodeId> {
    let records = load_kind_complete(context, family, JobRecordKind::Claim)
        .await
        .ok()?;
    records
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Claim(claim) => Some(claim),
            _ => None,
        })
        .min_by_key(|claim| claim.order_key())
        .map(|claim| claim.committing_node_id)
}

/// One bounded pass over the persisted deadlines. Returns true while rows
/// remain, so the caller re-arms instead of spinning.
pub async fn drain_witness_deadlines(context: &DriverContext, now_ms: u64) -> bool {
    let (rows, mut remaining) = match iter_prefix_page(
        &context.storage_handle,
        JOB_WITNESS_DEADLINE_KEYSPACE,
        None,
        None,
        WITNESS_DRAIN_BATCH,
        None,
    )
    .await
    {
        Ok((rows, next)) => (rows, next.is_some()),
        Err(error) => {
            warn!(error = %error, "Witness deadline scan failed");
            return true;
        }
    };
    for (key, _) in rows {
        let Some(family) = deadline_family(&key) else {
            continue;
        };
        let deadline = match read_deadline_index(context, &family).await {
            Ok(Some(deadline)) => deadline,
            Ok(None) => {
                if !remove_stale(context, &family, &key).await {
                    remaining = true;
                }
                continue;
            }
            Err(_) => {
                remaining = true;
                continue;
            }
        };
        if deadline_key(&family, deadline.due_at_ms) != key {
            if !remove_stale(context, &family, &key).await {
                remaining = true;
            }
            continue;
        }
        if deadline.due_at_ms > now_ms {
            remaining = true;
            continue;
        }
        match run_round(context, family, now_ms).await {
            RoundOutcome::Done => {
                if !clear_deadline(context, &family, &key, deadline).await {
                    remaining = true;
                }
            }
            RoundOutcome::Retry { after_ms } => {
                let next = WitnessDeadline {
                    due_at_ms: now_ms + after_ms,
                    rank: deadline.rank,
                };
                let _ = replace_deadline(context, &family, Some(deadline), next).await;
                remaining = true;
            }
        }
    }
    remaining
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::records::tests::fixture::{Family, context};

    #[tokio::test]
    async fn keeps_later_deadlines() {
        // A full pass must re-arm the drain for rows on its next page.
        let fixture = Family::new([1u8; 32]);
        let (_dir, ctx) = context(&fixture.config, fixture.holder.public()).await;
        for index in 0..=WITNESS_DRAIN_BATCH {
            let family = JobFamilyId {
                submission_id: aruna_core::structs::SubmissionId([index as u8; 32]),
                request_digest: [index as u8; 32],
            };
            assert!(
                replace_deadline(
                    &ctx,
                    &family,
                    None,
                    WitnessDeadline {
                        due_at_ms: 0,
                        rank: index as u32,
                    },
                )
                .await
            );
        }

        assert!(drain_witness_deadlines(&ctx, 1).await);
        let (rows, _) = iter_prefix_page(
            &ctx.storage_handle,
            JOB_WITNESS_DEADLINE_KEYSPACE,
            None,
            None,
            WITNESS_DRAIN_BATCH,
            None,
        )
        .await
        .expect("deadlines scan");
        assert_eq!(rows.len(), 1);
        assert!(!drain_witness_deadlines(&ctx, 1).await);
    }

    #[tokio::test]
    async fn moves_deadline_once() {
        let fixture = Family::new([2u8; 32]);
        let (_dir, ctx) = context(&fixture.config, fixture.holder.public()).await;
        let family = fixture.family();
        let first = WitnessDeadline {
            due_at_ms: 20,
            rank: 1,
        };
        let second = WitnessDeadline {
            due_at_ms: 10,
            rank: 1,
        };
        assert!(replace_deadline(&ctx, &family, None, first).await);
        assert!(replace_deadline(&ctx, &family, Some(first), second).await);
        assert_eq!(
            read_deadline_index(&ctx, &family)
                .await
                .expect("deadline index read"),
            Some(second)
        );
        let (rows, _) = iter_prefix_page(
            &ctx.storage_handle,
            JOB_WITNESS_DEADLINE_KEYSPACE,
            None,
            None,
            WITNESS_DRAIN_BATCH,
            None,
        )
        .await
        .expect("deadline scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, deadline_key(&family, second.due_at_ms));
    }

    #[test]
    fn schedule_uses_shorten() {
        let Effect::Task(TaskEffect::ShortenTimer { key, after }) =
            schedule_witness_drain(Duration::from_secs(1))
        else {
            panic!("expected shortened witness timer")
        };
        assert_eq!(key, TaskKey::DrainJobWitnessQueue);
        assert_eq!(after, Duration::from_secs(1));
    }

    #[test]
    fn deadlines_ordered() {
        let family = JobFamilyId {
            submission_id: aruna_core::structs::SubmissionId([1; 32]),
            request_digest: [2; 32],
        };
        assert!(deadline_key(&family, 1) < deadline_key(&family, 2));
    }

    #[test]
    fn lost_target_expires() {
        // One timed-out offer must never be enough to declare a target lost.
        let base = 5_000;
        let offer = OFFER_DEADLINE.as_millis() as u64;
        let window = lost_window_ms(base);
        assert!(window >= offer + base);
        assert!(!lost_target_expired(10, 10 + offer + base, base));
        assert!(!lost_target_expired(10, 9 + window, base));
        assert!(lost_target_expired(10, 10 + window, base));
        assert_eq!(lost_retry_ms(10, 10, base), base);
        assert_eq!(lost_retry_ms(10, 9 + window, base), 1);
    }
}

/// What one round leaves behind: nothing more to do for this family here, or a
/// re-armed deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundOutcome {
    Done,
    Retry { after_ms: u64 },
}

/// One witness round: suppression, budget, plan, launch, offer. Every step
/// reads only replicated records plus this node's own view.
pub async fn run_round(context: &DriverContext, family: JobFamilyId, now_ms: u64) -> RoundOutcome {
    let Some(net) = context.net_handle.as_ref() else {
        return RoundOutcome::Done;
    };
    let realm_id = *net.realm_id();
    let local = net.node_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return RoundOutcome::Retry { after_ms: 1_000 };
    };
    let base = config.compute.witness_base_delay_ms;
    let Some(view) = FamilyView::resolve(&config, realm_id, family) else {
        return RoundOutcome::Retry { after_ms: base };
    };
    if !view.holds(local) {
        return RoundOutcome::Done;
    }
    // An incomplete family read is undecided evidence: a suppression, a
    // cancellation, or an earlier launch may be in the part that did not load,
    // so this round stores no budget and offers no launch.
    let records = match load_family_complete(context, family).await {
        Ok(records) => records,
        Err(error) => {
            warn!(error = %error, "Job family read is incomplete; witness round deferred");
            return RoundOutcome::Retry { after_ms: base };
        }
    };
    if suppressed(family, &records) {
        return RoundOutcome::Done;
    }
    let Some(spec) = stored_spec(family, &records) else {
        return RoundOutcome::Retry { after_ms: base };
    };
    let budget = match stored_budget(context, &config, &spec, local, &records, now_ms).await {
        Some(budget) => budget,
        None => return RoundOutcome::Retry { after_ms: base },
    };
    let mine: Vec<&JobRecordEnvelope> = records
        .iter()
        .filter(|envelope| match &envelope.record {
            JobFamilyRecord::Launch(launch) => launch.scheduler_node_id == local,
            _ => false,
        })
        .collect();
    let sequence = mine.len() as u32;
    let mut explain = read_row::<WitnessExplain>(
        context,
        JOB_PLAN_EXPLAIN_KEYSPACE,
        &explain_key(&family, local),
    )
    .await
    .unwrap_or(WitnessExplain {
        sequence,
        plan: empty_plan(),
        declined: Vec::new(),
        overlapping: false,
        stored_at_ms: now_ms,
    });
    if let Some(envelope) = mine.iter().max_by_key(|envelope| match &envelope.record {
        JobFamilyRecord::Launch(launch) => launch.scheduler_seq,
        _ => 0,
    }) && let JobFamilyRecord::Launch(launch) = &envelope.record
        && !explain.declined.contains(&launch.target)
    {
        if lost_target_expired(launch.created_at_ms, now_ms, base) {
            record_decline(context, &family, local, launch.target.clone()).await;
            return RoundOutcome::Retry { after_ms: 0 };
        }
        let retry_after_ms = lost_retry_ms(launch.created_at_ms, now_ms, base);
        if launch.created_at_ms.saturating_add(base) > now_ms {
            return RoundOutcome::Retry {
                after_ms: retry_after_ms,
            };
        }
        let Ok(frame) = JobRecordFrame::new((*envelope).clone()) else {
            return RoundOutcome::Done;
        };
        return offer(
            context,
            Offer {
                realm_id,
                local,
                family,
                frame,
                target: launch.target.clone(),
                now_ms,
                retry_after_ms,
            },
        )
        .await;
    }
    if sequence >= budget.max_launches {
        debug!(sequence, "Witness budget is exhausted for this request");
        return RoundOutcome::Done;
    }
    let plan = match build_plan(context, &config, &spec, &explain.declined, now_ms).await {
        Ok(plan) => plan,
        Err(error) => {
            warn!(error = %error, "Execution planning failed");
            return RoundOutcome::Retry { after_ms: base };
        }
    };
    info!(
        sequence,
        selected = plan.selected.is_some(),
        alternatives = plan.alternatives.len(),
        rejected = plan.rejected.len(),
        omitted = plan.omitted,
        retryable = plan.retryable,
        "Witness planning round decided"
    );
    explain.sequence = sequence;
    explain.overlapping = !mine.is_empty();
    explain.plan = plan;
    explain.stored_at_ms = now_ms;
    // The explain record is durable before any launch exists, so every launch
    // has an auditable reason even if this node dies right after sending it.
    if write_row(
        context,
        JOB_PLAN_EXPLAIN_KEYSPACE,
        &explain_key(&family, local),
        &explain,
    )
    .await
    .is_err()
    {
        return RoundOutcome::Retry { after_ms: base };
    }
    let Some(selection) = explain.plan.selected.clone() else {
        let after_ms = match explain.plan.retryable {
            true => base,
            false => base.saturating_mul(4),
        };
        return RoundOutcome::Retry { after_ms };
    };
    let launch = LaunchIntent {
        launch_id: Ulid::generate(),
        submission_id: family.submission_id,
        request_digest: family.request_digest,
        job_id: spec.job_id,
        scheduler_node_id: local,
        scheduler_seq: sequence,
        witness_placement: view.placement,
        holder_generation: holder_generation(&config, &view),
        target: selection.target.clone(),
        inputs: selection.inputs,
        output_policies: selection.output_policies,
        plan_digest: selection.plan_digest,
        spec_digest: spec.spec_digest,
        created_at_ms: now_ms,
    };
    let Some(frame) = sign_record(context, realm_id, JobFamilyRecord::Launch(Box::new(launch)))
    else {
        return RoundOutcome::Retry { after_ms: base };
    };
    if !append_local(context, realm_id, local, frame.clone(), now_ms).await {
        return RoundOutcome::Retry { after_ms: base };
    }
    let offered = Offer {
        realm_id,
        local,
        family,
        frame,
        target: selection.target,
        now_ms,
        retry_after_ms: lost_retry_ms(now_ms, now_ms, base),
    };
    offer(context, offered).await
}

/// One launch offer round: the durable launch, its target, and the delays this
/// witness answers with.
struct Offer {
    realm_id: aruna_core::structs::RealmId,
    local: NodeId,
    family: JobFamilyId,
    frame: JobRecordFrame,
    target: ExecutionTargetId,
    now_ms: u64,
    retry_after_ms: u64,
}

/// Sends the durable launch to its target and settles what the answer means.
async fn offer(context: &DriverContext, offered: Offer) -> RoundOutcome {
    let Offer {
        realm_id,
        local,
        family,
        frame,
        target,
        now_ms,
        retry_after_ms,
    } = offered;
    let Ok(launch) = LaunchFrame::new(frame.into_inner()) else {
        return RoundOutcome::Done;
    };
    let operation = OfferLaunchOperation::new(LaunchOfferEffect {
        realm_id,
        target: target.clone(),
        launch: Box::new(launch),
        deadline: OFFER_DEADLINE,
    });
    match drive(operation, context).await {
        Ok(OfferOutcome::Accepted(receipt)) => {
            match append_local(context, realm_id, local, receipt, now_ms).await {
                true => RoundOutcome::Done,
                false => RoundOutcome::Retry {
                    after_ms: retry_after_ms,
                },
            }
        }
        Ok(OfferOutcome::Declined(reason)) => {
            if reason == LaunchDecline::Cancelled {
                return RoundOutcome::Done;
            }
            if permanent_decline(&reason) {
                record_decline(context, &family, local, target).await;
                RoundOutcome::Retry { after_ms: 0 }
            } else {
                RoundOutcome::Retry {
                    after_ms: retry_after_ms,
                }
            }
        }
        Ok(OfferOutcome::Unavailable) | Err(_) => RoundOutcome::Retry {
            after_ms: retry_after_ms,
        },
    }
}

fn permanent_decline(reason: &LaunchDecline) -> bool {
    match reason {
        LaunchDecline::Unauthorized | LaunchDecline::LaunchConflict => true,
        LaunchDecline::Policy(policy) => !matches!(
            policy.decision(),
            PlacementDecision::Required { .. } | PlacementDecision::Unavailable { .. }
        ),
        LaunchDecline::NotHolder
        | LaunchDecline::Capacity
        | LaunchDecline::Draining
        | LaunchDecline::Cancelled => false,
    }
}

async fn record_decline(
    context: &DriverContext,
    family: &JobFamilyId,
    local: NodeId,
    target: ExecutionTargetId,
) {
    let key = explain_key(family, local);
    let Some(mut explain) =
        read_row::<WitnessExplain>(context, JOB_PLAN_EXPLAIN_KEYSPACE, &key).await
    else {
        return;
    };
    if !explain.declined.contains(&target) && explain.declined.len() < MAX_DECLINED_TARGETS {
        explain.declined.push(target);
    }
    let _ = write_row(context, JOB_PLAN_EXPLAIN_KEYSPACE, &key, &explain).await;
}

/// Whether a launch is suppressed by a success, cancellation, permanent
/// failure, or an execution that may still finish.
pub(crate) fn suppressed(family: JobFamilyId, records: &[JobRecordEnvelope]) -> bool {
    let Ok(Some(projection)) = reduce_family(family, records) else {
        return false;
    };
    if projection.cancel_requested || projection.canonical_execution_id.is_some() {
        return true;
    }
    projection
        .executions
        .iter()
        .any(|execution| execution.state != PhysicalExecutionState::Error)
}

/// The stored spec of the family's canonical alias.
fn stored_spec(family: JobFamilyId, records: &[JobRecordEnvelope]) -> Option<LogicalJobSpec> {
    let projection = reduce_family(family, records).ok()??;
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Spec(spec) if spec.job_id == projection.canonical_job_id => {
            Some(spec.as_ref().clone())
        }
        _ => None,
    })
}

/// This node's immutable launch bound, stored once before its first launch.
async fn stored_budget(
    context: &DriverContext,
    config: &RealmConfigDocument,
    spec: &LogicalJobSpec,
    local: NodeId,
    records: &[JobRecordEnvelope],
    now_ms: u64,
) -> Option<WitnessBudgetRecord> {
    if let Some(budget) = records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Budget(budget) if budget.scheduler_node_id == local => Some(*budget),
        _ => None,
    }) {
        return Some(budget);
    }
    let budget = WitnessBudgetRecord {
        submission_id: spec.submission_id,
        request_digest: spec.request_digest,
        scheduler_node_id: local,
        source_spec_digest: spec.spec_digest,
        max_launches: spec.retry.max_launches_per_witness,
    };
    let frame = sign_record(context, config.realm_id, JobFamilyRecord::Budget(budget))?;
    match append_local(context, config.realm_id, local, frame, now_ms).await {
        true => Some(budget),
        false => None,
    }
}

/// The candidate map epoch this witness observed, recorded as audit evidence
/// only: it never proves that the scheduler held the family.
fn holder_generation(config: &RealmConfigDocument, view: &FamilyView) -> u64 {
    config
        .activation(&view.placement.strategy_id, view.placement.shard)
        .map(|activation| activation.candidate_map_epoch)
        .unwrap_or_default()
}

fn sign_record(
    context: &DriverContext,
    realm_id: aruna_core::structs::RealmId,
    record: JobFamilyRecord,
) -> Option<JobRecordFrame> {
    let net = context.net_handle.as_ref()?;
    let envelope = JobRecordEnvelope::signed_with(realm_id, record, net.node_id(), |message| {
        net.sign(message)
    })
    .ok()?;
    JobRecordFrame::new(envelope).ok()
}

async fn append_local(
    context: &DriverContext,
    realm_id: aruna_core::structs::RealmId,
    local: NodeId,
    record: JobRecordFrame,
    now_ms: u64,
) -> bool {
    let operation = AppendRecordOperation::new(AppendRecordConfig {
        realm_id,
        local_node_id: local,
        record,
        local: None,
        origin: RecordOrigin::Local,
        now_ms,
    });
    match drive(operation, context).await {
        Ok(outcome) => matches!(
            outcome.admission,
            Admission::Authentic | Admission::Duplicate
        ),
        Err(error) => {
            warn!(error = %error, "Witness record append failed");
            false
        }
    }
}

fn empty_plan() -> ExecutionPlan {
    ExecutionPlan {
        selected: None,
        retryable: true,
        alternatives: Vec::new(),
        rejected: Vec::new(),
        omitted: 0,
    }
}

fn deadline_key(family: &JobFamilyId, due_at_ms: u64) -> Key {
    let mut bytes = Vec::with_capacity(DEADLINE_KEY_BYTES);
    bytes.extend_from_slice(&due_at_ms.to_be_bytes());
    bytes.extend_from_slice(&family.to_bytes());
    Key::from(bytes.as_slice())
}

fn deadline_index_key(family: &JobFamilyId) -> Key {
    Key::from(family.to_bytes().as_slice())
}

fn deadline_family(key: &[u8]) -> Option<JobFamilyId> {
    if key.len() != DEADLINE_KEY_BYTES {
        return None;
    }
    let submission: [u8; 32] = key.get(8..40)?.try_into().ok()?;
    let request_digest: [u8; 32] = key.get(40..72)?.try_into().ok()?;
    Some(JobFamilyId {
        submission_id: aruna_core::structs::SubmissionId(submission),
        request_digest,
    })
}

fn explain_key(family: &JobFamilyId, node: NodeId) -> Key {
    let mut bytes = family.to_bytes().to_vec();
    bytes.extend_from_slice(node.as_bytes());
    Key::from(bytes.as_slice())
}

async fn clear_deadline(
    context: &DriverContext,
    family: &JobFamilyId,
    key: &Key,
    expected: WitnessDeadline,
) -> bool {
    let Ok(txn_id) = start_txn(context).await else {
        return false;
    };
    let current: Result<Option<WitnessDeadline>, LifecycleError> = read_row_txn(
        context,
        JOB_WITNESS_DEADLINE_INDEX_KEYSPACE,
        &deadline_index_key(family),
        Some(txn_id),
    )
    .await;
    let Ok(current) = current else {
        abort_txn(context, txn_id).await;
        return false;
    };
    if current != Some(expected) || deadline_key(family, expected.due_at_ms) != *key {
        abort_txn(context, txn_id).await;
        return false;
    }
    let deletes = vec![
        (JOB_WITNESS_DEADLINE_KEYSPACE.to_string(), key.clone()),
        (
            JOB_WITNESS_DEADLINE_INDEX_KEYSPACE.to_string(),
            deadline_index_key(family),
        ),
    ];
    if batch_delete(&context.storage_handle, deletes, Some(txn_id))
        .await
        .is_err()
    {
        abort_txn(context, txn_id).await;
        return false;
    }
    commit_txn(context, txn_id).await.is_ok()
}

async fn remove_stale(context: &DriverContext, family: &JobFamilyId, key: &Key) -> bool {
    let Ok(txn_id) = start_txn(context).await else {
        return false;
    };
    let current: Result<Option<WitnessDeadline>, LifecycleError> = read_row_txn(
        context,
        JOB_WITNESS_DEADLINE_INDEX_KEYSPACE,
        &deadline_index_key(family),
        Some(txn_id),
    )
    .await;
    let current_key = current
        .as_ref()
        .ok()
        .and_then(Option::as_ref)
        .map(|deadline| deadline_key(family, deadline.due_at_ms));
    if current_key.as_ref() == Some(key) {
        abort_txn(context, txn_id).await;
        return false;
    }
    if current.is_err()
        || batch_delete(
            &context.storage_handle,
            vec![(JOB_WITNESS_DEADLINE_KEYSPACE.to_string(), key.clone())],
            Some(txn_id),
        )
        .await
        .is_err()
    {
        abort_txn(context, txn_id).await;
        return false;
    }
    commit_txn(context, txn_id).await.is_ok()
}

async fn replace_deadline(
    context: &DriverContext,
    family: &JobFamilyId,
    expected: Option<WitnessDeadline>,
    next: WitnessDeadline,
) -> bool {
    let Ok(txn_id) = start_txn(context).await else {
        return false;
    };
    let current = read_row_txn(
        context,
        JOB_WITNESS_DEADLINE_INDEX_KEYSPACE,
        &deadline_index_key(family),
        Some(txn_id),
    )
    .await;
    let Ok(current) = current else {
        abort_txn(context, txn_id).await;
        return false;
    };
    if current != expected {
        abort_txn(context, txn_id).await;
        return false;
    }
    if write_row_txn(
        context,
        JOB_WITNESS_DEADLINE_KEYSPACE,
        &deadline_key(family, next.due_at_ms),
        &next,
        Some(txn_id),
    )
    .await
    .is_err()
        || write_row_txn(
            context,
            JOB_WITNESS_DEADLINE_INDEX_KEYSPACE,
            &deadline_index_key(family),
            &next,
            Some(txn_id),
        )
        .await
        .is_err()
    {
        abort_txn(context, txn_id).await;
        return false;
    }
    if let Some(previous) = expected {
        let previous_key = deadline_key(family, previous.due_at_ms);
        if previous_key != deadline_key(family, next.due_at_ms)
            && batch_delete(
                &context.storage_handle,
                vec![(JOB_WITNESS_DEADLINE_KEYSPACE.to_string(), previous_key)],
                Some(txn_id),
            )
            .await
            .is_err()
        {
            abort_txn(context, txn_id).await;
            return false;
        }
    }
    commit_txn(context, txn_id).await.is_ok()
}

pub(super) async fn has_deadline(context: &DriverContext, family: JobFamilyId) -> bool {
    match read_deadline_index(context, &family).await {
        Ok(deadline) => deadline.is_some(),
        Err(_) => true,
    }
}

/// How long a target keeps its launch: every offer's own deadline plus the
/// spacing of the rounds that make them.
fn lost_window_ms(base_delay_ms: u64) -> u64 {
    (OFFER_DEADLINE.as_millis() as u64)
        .saturating_mul(u64::from(LOST_TARGET_OFFERS))
        .saturating_add(base_delay_ms)
}

fn lost_target_expired(created_at_ms: u64, now_ms: u64, base_delay_ms: u64) -> bool {
    now_ms.saturating_sub(created_at_ms) >= lost_window_ms(base_delay_ms)
}

fn lost_retry_ms(created_at_ms: u64, now_ms: u64, base_delay_ms: u64) -> u64 {
    let elapsed = now_ms.saturating_sub(created_at_ms);
    let remaining = lost_window_ms(base_delay_ms).saturating_sub(elapsed);
    base_delay_ms.min(remaining)
}

async fn read_deadline_index(
    context: &DriverContext,
    family: &JobFamilyId,
) -> Result<Option<WitnessDeadline>, LifecycleError> {
    read_row_txn(
        context,
        JOB_WITNESS_DEADLINE_INDEX_KEYSPACE,
        &deadline_index_key(family),
        None,
    )
    .await
}

async fn start_txn(context: &DriverContext) -> Result<TxnId, LifecycleError> {
    match context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(aruna_core::events::StorageEvent::TransactionStarted { txn_id }) => {
            Ok(txn_id)
        }
        Event::Storage(aruna_core::events::StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LifecycleError::UnexpectedEvent {
            state: "start transaction".to_string(),
            expected: "transaction started",
            got: format!("{other:?}"),
        }),
    }
}

async fn commit_txn(context: &DriverContext, txn_id: TxnId) -> Result<(), LifecycleError> {
    match context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(aruna_core::events::StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(aruna_core::events::StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LifecycleError::UnexpectedEvent {
            state: "commit transaction".to_string(),
            expected: "transaction committed",
            got: format!("{other:?}"),
        }),
    }
}

async fn abort_txn(context: &DriverContext, txn_id: TxnId) {
    let _ = context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn read_row_txn<T: for<'a> Deserialize<'a>>(
    context: &DriverContext,
    key_space: &str,
    key: &Key,
    txn_id: Option<TxnId>,
) -> Result<Option<T>, LifecycleError> {
    let event = context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.clone(),
            txn_id,
        })
        .await;
    match event {
        Event::Storage(aruna_core::events::StorageEvent::ReadResult { value, .. }) => {
            Ok(value.and_then(|bytes| from_bytes::<T>(&bytes).ok()))
        }
        Event::Storage(aruna_core::events::StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LifecycleError::UnexpectedEvent {
            state: "read".to_string(),
            expected: "read result",
            got: format!("{other:?}"),
        }),
    }
}

async fn read_row<T: for<'a> Deserialize<'a>>(
    context: &DriverContext,
    key_space: &str,
    key: &Key,
) -> Option<T> {
    read_row_txn(context, key_space, key, None)
        .await
        .ok()
        .flatten()
}

async fn write_row_txn<T: Serialize>(
    context: &DriverContext,
    key_space: &str,
    key: &Key,
    row: &T,
    txn_id: Option<TxnId>,
) -> Result<(), LifecycleError> {
    let bytes = to_bytes(row)?;
    let event = context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.clone(),
            value: bytes.as_slice().into(),
            txn_id,
        })
        .await;
    match event {
        Event::Storage(aruna_core::events::StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(aruna_core::events::StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LifecycleError::UnexpectedEvent {
            state: "write".to_string(),
            expected: "write result",
            got: format!("{other:?}"),
        }),
    }
}

async fn write_row<T: Serialize>(
    context: &DriverContext,
    key_space: &str,
    key: &Key,
    row: &T,
) -> Result<(), LifecycleError> {
    write_row_txn(context, key_space, key, row, None).await
}

/// What a target answered to one offer.
#[derive(Debug, PartialEq)]
pub enum OfferOutcome {
    Accepted(JobRecordFrame),
    Declined(LaunchDecline),
    /// The target never answered; it may still have accepted the launch.
    Unavailable,
}

/// Offers one durable launch to its target and returns the answer verbatim.
#[derive(Debug, PartialEq)]
pub struct OfferLaunchOperation {
    effect: Option<LaunchOfferEffect>,
    outcome: Option<Result<OfferOutcome, LifecycleError>>,
    sent: bool,
}

impl OfferLaunchOperation {
    pub fn new(effect: LaunchOfferEffect) -> Self {
        Self {
            effect: Some(effect),
            outcome: None,
            sent: false,
        }
    }
}

impl Operation for OfferLaunchOperation {
    type Output = OfferOutcome;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        let Some(effect) = self.effect.take() else {
            self.outcome = Some(Err(LifecycleError::NotFinished));
            return smallvec![];
        };
        self.sent = true;
        smallvec![Effect::Net(NetEffect::LaunchOffer(Box::new(effect)))]
    }

    fn step(&mut self, event: Event) -> Effects {
        if !self.sent {
            self.outcome = Some(Err(LifecycleError::UnexpectedEvent {
                state: "Settled".to_string(),
                expected: "no further event",
                got: format!("{event:?}"),
            }));
            return smallvec![];
        }
        self.outcome = Some(match event {
            Event::Net(NetEvent::LaunchOffer(LaunchOfferEvent::Accepted { receipt, .. })) => {
                match JobRecordFrame::new(receipt.into_inner()) {
                    Ok(frame) => Ok(OfferOutcome::Accepted(frame)),
                    Err(error) => Err(LifecycleError::Store(error.into())),
                }
            }
            Event::Net(NetEvent::LaunchOffer(LaunchOfferEvent::Declined { target, reason })) => {
                debug!(peer = %target, reason = ?reason, "Execution target declined a launch");
                Ok(OfferOutcome::Declined(reason))
            }
            Event::Net(NetEvent::LaunchOffer(LaunchOfferEvent::Unavailable(message))) => {
                debug!(message, "Execution target did not answer the offer");
                Ok(OfferOutcome::Unavailable)
            }
            other => Err(LifecycleError::UnexpectedEvent {
                state: "Offer".to_string(),
                expected: "launch offer result",
                got: format!("{other:?}"),
            }),
        });
        self.sent = false;
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
