//! Runs leaderless witness scheduling: every family holder ranks itself and may offer a launch.
//! No witness holds a lease, so a partition can produce one execution per witness.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::ExecutionTargetId;
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame};
use aruna_core::events::{Event, LaunchDecline};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    DEADLINE_INDEX_KEYSPACE, PLAN_EXPLAIN_KEYSPACE, WITNESS_DEADLINE_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::scheduling::{ExecutionPlan, MAX_PLAN_CANDIDATES};
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordKind, LaunchIntent, LogicalJobSpec,
    PhysicalExecutionState, WitnessBudgetRecord,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::PlacementDecision;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, TxnId, Value};
use aruna_net::NetHandle;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::offer::{OfferLaunchOperation, OfferOutcome};
use super::plan::build_plan;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::records::admit::Admission;
use crate::jobs::records::append::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::reduce::reduce_family;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::store::query::batch_delete;
use crate::realm::load_config::load_realm_config;
use crate::storage_read::iter_prefix_page;
use aruna_core::structs::execution::job::record_rows::{from_bytes, to_bytes};

/// Domain of the stable witness order.
const WITNESS_RANK_DOMAIN: &[u8] = b"aruna-job-witness-v1";
/// Families one drain pass considers.
const WITNESS_DRAIN_BATCH: usize = 64;
/// Spacing between drain passes while deadlines remain.
pub const WITNESS_RETRY_AFTER: Duration = Duration::from_secs(1);
/// Wall-clock budget of one launch offer.
const OFFER_DEADLINE: Duration = Duration::from_secs(30);
/// Declined targets one explain row retains.
const MAX_DECLINED_TARGETS: usize = MAX_PLAN_CANDIDATES;

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
        key: TaskKey::DrainWitnessQueue,
        after,
    })
}

/// Position of `node` in the witness order. The admitting node ranks first,
/// because it already holds the request; remaining witnesses follow the
/// domain-separated digest of the immutable identity, identical on every node.
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

/// Arms this node's fallback for one family; see [`ArmFamilyOperation`].
pub async fn arm_family(context: &DriverContext, family: JobFamilyId, now_ms: u64) {
    let Some((local, realm_id)) = context.node_and_realm() else {
        debug!(?family, "No net handle; witness fallback not armed");
        return;
    };
    let operation = ArmFamilyOperation::new(family, local, realm_id, now_ms);
    if let Err(error) = drive(operation, context).await {
        warn!(?family, error = %error, "Witness deadline was not armed");
    }
}

#[derive(Debug, PartialEq)]
enum ArmState {
    Config,
    Claims,
    Index,
    Write,
    Delete,
}

/// Arms this node's fallback for one family. Rank zero is due immediately; a
/// later rank waits `base_delay * rank` plus its own jitter. An earlier existing
/// deadline is kept, so re-arming can only ever bring a round forward. The deadline is read
/// and replaced in one write transaction, then the witness drain wakes.
#[derive(Debug, PartialEq)]
pub struct ArmFamilyOperation {
    family: JobFamilyId,
    local: NodeId,
    realm_id: RealmId,
    now_ms: u64,
    holders: Vec<NodeId>,
    base_delay_ms: u64,
    deadline: Option<WitnessDeadline>,
    deletes: Vec<(String, Key)>,
    state: ArmState,
}

type ArmStep = Result<Next<()>, LifecycleError>;

impl ArmFamilyOperation {
    pub fn new(family: JobFamilyId, local: NodeId, realm_id: RealmId, now_ms: u64) -> Self {
        Self {
            family,
            local,
            realm_id,
            now_ms,
            holders: Vec::new(),
            base_delay_ms: 0,
            deadline: None,
            deletes: Vec::new(),
            state: ArmState::Config,
        }
    }

    /// Ranks this node among the holders; the admitting node goes first.
    fn rank(&mut self, admitting: Option<NodeId>) -> ArmStep {
        let family = self.family;
        let Some(rank) = witness_rank(&self.holders, &family, self.local, admitting) else {
            debug!(?family, "Node is not a witness for this family");
            return Ok(Next::Done(()));
        };
        let base = self.base_delay_ms;
        let due_at_ms = match rank {
            0 => self.now_ms,
            rank => self.now_ms + base * u64::from(rank) + jitter_ms(&family, self.local, base),
        };
        self.deadline = Some(WitnessDeadline { due_at_ms, rank });
        self.state = ArmState::Index;
        let row = (
            DEADLINE_INDEX_KEYSPACE.to_string(),
            deadline_index_key(&family),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    /// Writes the new deadline unless an earlier one is already armed.
    fn replace(&mut self, existing: Option<WitnessDeadline>) -> ArmStep {
        let Some(next) = self.deadline else {
            return Ok(Next::Done(()));
        };
        if existing.is_some_and(|existing| existing.due_at_ms <= next.due_at_ms) {
            return Ok(Next::Done(()));
        }
        let family = &self.family;
        let key = deadline_key(family, next.due_at_ms);
        let value = Value::from(to_bytes(&next)?.as_slice());
        let writes = vec![
            (
                WITNESS_DEADLINE_KEYSPACE.to_string(),
                key.clone(),
                value.clone(),
            ),
            (
                DEADLINE_INDEX_KEYSPACE.to_string(),
                deadline_index_key(family),
                value,
            ),
        ];
        if let Some(previous) = existing.map(|previous| deadline_key(family, previous.due_at_ms))
            && previous != key
        {
            self.deletes
                .push((WITNESS_DEADLINE_KEYSPACE.to_string(), previous));
        }
        self.state = ArmState::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for ArmFamilyOperation {
    type Output = ();
    type Error = LifecycleError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ArmStep {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> ArmStep {
        let family = self.family;
        match std::mem::replace(&mut self.state, ArmState::Config) {
            ArmState::Config => {
                let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => config,
                    Err(error) => {
                        warn!(
                            ?family,
                            error = %error,
                            "Realm config unavailable; witness fallback not armed"
                        );
                        return Ok(Next::Done(()));
                    }
                };
                let Some(view) = FamilyView::resolve(&config, self.realm_id, family) else {
                    warn!(
                        ?family,
                        "Family placement unresolved; witness fallback not armed"
                    );
                    return Ok(Next::Done(()));
                };
                self.holders = view.holders().to_vec();
                self.base_delay_ms = config.compute.witness_delay_ms;
                self.state = ArmState::Claims;
                let claims = LoadFamilyOperation::kind(family, JobRecordKind::Claim);
                Ok(Next::Call(IoCall::child(claims)))
            }
            // The node that admitted the family: the committing node of the claim with the
            // lowest order key, which every holder reduces the same way. An unreadable
            // claim set leaves the order to the digest alone.
            ArmState::Claims => {
                let records = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()?;
                let admitting = records.ok().and_then(|records| {
                    records
                        .iter()
                        .filter_map(|envelope| match &envelope.record {
                            JobFamilyRecord::Claim(claim) => Some(claim),
                            _ => None,
                        })
                        .min_by_key(|claim| claim.order_key())
                        .map(|claim| claim.committing_node_id)
                });
                self.rank(admitting)
            }
            ArmState::Index => {
                let [value] = values::<1>(result)?;
                let existing = value.and_then(|bytes| from_bytes::<WitnessDeadline>(&bytes).ok());
                self.replace(existing)
            }
            ArmState::Write => {
                done(result)?;
                self.state = ArmState::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            ArmState::Delete => {
                done(result)?;
                let due_at_ms = self.deadline.map_or(self.now_ms, |next| next.due_at_ms);
                let after = Duration::from_millis(due_at_ms.saturating_sub(self.now_ms));
                let drain = IoCall::Effect(Box::new(schedule_witness_drain(after)));
                Ok(Next::Then {
                    output: (),
                    calls: vec![drain],
                })
            }
        }
    }
}

/// One bounded pass over the persisted deadlines. Returns true while rows
/// remain, so the caller re-arms instead of spinning.
pub async fn drain_witness_deadlines(context: &DriverContext, now_ms: u64) -> bool {
    let (rows, mut remaining) = match iter_prefix_page(
        &context.storage_handle,
        WITNESS_DEADLINE_KEYSPACE,
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
    use crate::tests::records::{Family, context, node};

    fn arming(existing: Option<WitnessDeadline>) -> Result<Next<()>, LifecycleError> {
        let fixture = Family::new([2u8; 32]);
        let family = fixture.family();
        let local = fixture.holder.public();
        let mut operation = ArmFamilyOperation::new(family, local, fixture.config.realm_id, 1_000);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.holders = vec![local];
        operation.base_delay_ms = 400;
        let read = operation.rank(Some(local));
        assert!(matches!(read, Ok(Next::Call(IoCall::Read(_)))));
        let value = existing.map(|deadline| Value::from(to_bytes(&deadline).unwrap().as_slice()));
        operation.next(IoResult::Values(vec![value]))
    }

    // An admitting node arms itself at once; an earlier armed deadline is kept.
    #[test]
    fn arms_earliest_deadline() {
        let Ok(Next::Call(IoCall::Write(rows))) = arming(None) else {
            panic!("a new deadline is written");
        };
        assert_eq!(rows.len(), 2);
        let armed: WitnessDeadline = from_bytes(&rows[1].2).unwrap();
        assert_eq!(
            armed,
            WitnessDeadline {
                due_at_ms: 1_000,
                rank: 0
            }
        );
        let earlier = WitnessDeadline {
            due_at_ms: 500,
            rank: 1,
        };
        assert_eq!(arming(Some(earlier)), Ok(Next::Done(())));
    }

    #[tokio::test]
    async fn keeps_later_deadlines() {
        // A full pass must re-arm the drain for rows on its next page.
        let fixture = Family::new([1u8; 32]);
        let (_dir, ctx) = context(&fixture.config, fixture.holder.public()).await;
        for index in 0..=WITNESS_DRAIN_BATCH {
            let family = JobFamilyId {
                submission_id: aruna_core::structs::execution::job::SubmissionId([index as u8; 32]),
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
            WITNESS_DEADLINE_KEYSPACE,
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
            WITNESS_DEADLINE_KEYSPACE,
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
        assert_eq!(key, TaskKey::DrainWitnessQueue);
        assert_eq!(after, Duration::from_secs(1));
    }

    #[test]
    fn deadlines_ordered() {
        let family = JobFamilyId {
            submission_id: aruna_core::structs::execution::job::SubmissionId([1; 32]),
            request_digest: [2; 32],
        };
        assert!(deadline_key(&family, 1) < deadline_key(&family, 2));
    }

    /// One family with a launch by `holder` that no receipt answers.
    fn foreign(fixture: &Family) -> (LaunchIntent, Vec<JobRecordEnvelope>) {
        let spec = fixture.spec();
        let launch = fixture.launch(&spec, fixture.holder.public(), 0);
        let records = vec![fixture.sign(
            &fixture.holder,
            JobFamilyRecord::Launch(Box::new(launch.clone())),
        )];
        (launch, records)
    }

    #[test]
    fn defers_to_launch() {
        // Another witness's unreceipted launch holds the family until its
        // window ends; this node's own launch never defers the round.
        let fixture = Family::new([3u8; 32]);
        let (launch, records) = foreign(&fixture);
        let now = launch.created_at_ms + 10;

        assert_eq!(
            foreign_launch_wait(&records, node(2), now, 1_000),
            Some(990)
        );
        assert_eq!(
            foreign_launch_wait(&records, fixture.holder.public(), now, 1_000),
            None
        );
    }

    #[test]
    fn plans_after_window() {
        // Past the window, and once a receipt exists, the round plans again.
        let fixture = Family::new([4u8; 32]);
        let (launch, mut records) = foreign(&fixture);

        assert_eq!(
            foreign_launch_wait(&records, node(2), launch.created_at_ms + 1_000, 1_000),
            None
        );
        records.push(fixture.sign(
            &fixture.target,
            JobFamilyRecord::Receipt(Box::new(fixture.receipt(&launch, 1))),
        ));
        assert_eq!(
            foreign_launch_wait(&records, node(2), launch.created_at_ms + 10, 1_000),
            None
        );
    }

    #[test]
    fn lost_target_expires() {
        // A target keeps its launch for the whole configured catch-up window.
        let base = 5_000;
        let window = 300_000;
        assert!(!lost_target_expired(10, 9 + window, window));
        assert!(lost_target_expired(10, 10 + window, window));
        assert_eq!(lost_retry_ms(10, 10, base, window), base);
        assert_eq!(lost_retry_ms(10, 9 + window, base, window), 1);
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
async fn run_round(context: &DriverContext, family: JobFamilyId, now_ms: u64) -> RoundOutcome {
    let Some(net) = context.net_handle.as_ref() else {
        return RoundOutcome::Done;
    };
    let realm_id = *net.realm_id();
    let local = net.node_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return RoundOutcome::Retry { after_ms: 1_000 };
    };
    let base = config.compute.witness_delay_ms;
    let window = config.compute.catch_up_ms;
    let Some(view) = FamilyView::resolve(&config, realm_id, family) else {
        return RoundOutcome::Retry { after_ms: base };
    };
    if !view.holds(local) {
        return RoundOutcome::Done;
    }
    // An incomplete family read is undecided evidence: a suppression, cancel or
    // earlier launch may be in the unloaded part, so no budget and no offer.
    let records = match drive(LoadFamilyOperation::family(family), context).await {
        Ok(records) => records,
        Err(error) => {
            warn!(error = %error, "Job family read is incomplete; witness round deferred");
            return RoundOutcome::Retry { after_ms: base };
        }
    };
    let running = running_nodes(family, &records);
    let silent = silent_nodes(net, &running, window);
    if suppressed(family, &records, &silent) {
        // An unfinished execution suppresses only while its node reports, so look again later.
        return match suppressed(family, &records, &running) {
            true => RoundOutcome::Done,
            false => RoundOutcome::Retry { after_ms: window },
        };
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
    let mut explain =
        read_row::<WitnessExplain>(context, PLAN_EXPLAIN_KEYSPACE, &explain_key(&family, local))
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
        if lost_target_expired(launch.created_at_ms, now_ms, window) {
            record_decline(context, &family, local, launch.target.clone()).await;
            return RoundOutcome::Retry { after_ms: 0 };
        }
        let retry_after_ms = lost_retry_ms(launch.created_at_ms, now_ms, base, window);
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
    // Another witness already launched and its window has not run out, so this
    // round leaves the family to it instead of adding a second execution.
    if let Some(after_ms) = foreign_launch_wait(&records, local, now_ms, window) {
        debug!(after_ms, "Another witness holds a launch for this family");
        return RoundOutcome::Retry { after_ms };
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
        PLAN_EXPLAIN_KEYSPACE,
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
    let mut launch = LaunchIntent {
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
        input_pages: None,
        output_policies: selection.output_policies,
        plan_digest: selection.plan_digest,
        spec_digest: spec.spec_digest,
        created_at_ms: now_ms,
    };
    // Planned inputs too large for the launch record go to pages this node serves.
    if crate::jobs::records::pages::page_launch(context, &mut launch)
        .await
        .is_err()
    {
        return RoundOutcome::Retry { after_ms: base };
    }
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
        retry_after_ms: lost_retry_ms(now_ms, now_ms, base, window),
    };
    offer(context, offered).await
}

/// One launch offer round: the durable launch, its target, and the delays this
/// witness answers with.
struct Offer {
    realm_id: aruna_core::structs::identity::realm::RealmId,
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
    let target_node = target.node_id;
    let answered = match context.net_handle.as_ref() {
        // No transport dials the node it starts from, so a launch planned onto this
        // node is admitted in process; the decision is the one an inbound offer gets.
        Some(net) if net.node_id() == target_node => Ok(admit_here(context, launch).await),
        _ => {
            drive(
                OfferLaunchOperation::new(target_node, launch, OFFER_DEADLINE),
                context,
            )
            .await
        }
    };
    match answered {
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

/// Admits a launch planned onto this node, with the answer a remote target would give.
async fn admit_here(context: &DriverContext, launch: LaunchFrame) -> OfferOutcome {
    let context = std::sync::Arc::new(context.clone());
    match super::target::admit_launch(&context, launch).await {
        Some(Ok(receipt)) => match JobRecordFrame::new(receipt.into_inner()) {
            Ok(frame) => OfferOutcome::Accepted(frame),
            Err(_) => OfferOutcome::Unavailable,
        },
        Some(Err(reason)) => OfferOutcome::Declined(reason),
        None => OfferOutcome::Unavailable,
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
        | LaunchDecline::Cancelled
        | LaunchDecline::AlreadyRunning => false,
    }
}

async fn record_decline(
    context: &DriverContext,
    family: &JobFamilyId,
    local: NodeId,
    target: ExecutionTargetId,
) {
    let key = explain_key(family, local);
    let Some(mut explain) = read_row::<WitnessExplain>(context, PLAN_EXPLAIN_KEYSPACE, &key).await
    else {
        return;
    };
    if !explain.declined.contains(&target) && explain.declined.len() < MAX_DECLINED_TARGETS {
        explain.declined.push(target);
    }
    let _ = write_row(context, PLAN_EXPLAIN_KEYSPACE, &key, &explain).await;
}

/// Whether a launch is suppressed by a success, cancellation, permanent failure,
/// or an execution that may still finish. An unfinished execution on a `silent`
/// node no longer suppresses: that node stopped reporting.
pub(crate) fn suppressed(
    family: JobFamilyId,
    records: &[JobRecordEnvelope],
    silent: &BTreeSet<NodeId>,
) -> bool {
    let Ok(Some(projection)) = reduce_family(family, records) else {
        return false;
    };
    if projection.cancel_requested || projection.canonical_execution_id.is_some() {
        return true;
    }
    projection.executions.iter().any(|execution| {
        execution.state != PhysicalExecutionState::Error
            && (execution.state.is_terminal() || !silent.contains(&execution.executor_node_id))
    })
}

/// Executor nodes of the family's unfinished executions.
fn running_nodes(family: JobFamilyId, records: &[JobRecordEnvelope]) -> BTreeSet<NodeId> {
    let Ok(Some(projection)) = reduce_family(family, records) else {
        return BTreeSet::new();
    };
    projection
        .executions
        .iter()
        .filter(|execution| !execution.state.is_terminal())
        .map(|execution| execution.executor_node_id)
        .collect()
}

/// The `running` nodes that sent no heartbeat within the catch-up window. Without any heartbeat
/// the silence counts from this node's start, so a restart alone never makes a node silent early.
fn silent_nodes(net: &NetHandle, running: &BTreeSet<NodeId>, window_ms: u64) -> BTreeSet<NodeId> {
    running
        .iter()
        .copied()
        .filter(|node| net.heartbeat_silence(*node).as_millis() > u128::from(window_ms))
        .collect()
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
        max_launches: spec.retry.launches_per_witness,
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
    realm_id: aruna_core::structs::identity::realm::RealmId,
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
    realm_id: aruna_core::structs::identity::realm::RealmId,
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
        submission_id: aruna_core::structs::execution::job::SubmissionId(submission),
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
        DEADLINE_INDEX_KEYSPACE,
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
        (WITNESS_DEADLINE_KEYSPACE.to_string(), key.clone()),
        (
            DEADLINE_INDEX_KEYSPACE.to_string(),
            deadline_index_key(family),
        ),
    ];
    if batch_delete(context, deletes, Some(txn_id)).await.is_err() {
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
        DEADLINE_INDEX_KEYSPACE,
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
            context,
            vec![(WITNESS_DEADLINE_KEYSPACE.to_string(), key.clone())],
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
        DEADLINE_INDEX_KEYSPACE,
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
        WITNESS_DEADLINE_KEYSPACE,
        &deadline_key(family, next.due_at_ms),
        &next,
        Some(txn_id),
    )
    .await
    .is_err()
        || write_row_txn(
            context,
            DEADLINE_INDEX_KEYSPACE,
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
                context,
                vec![(WITNESS_DEADLINE_KEYSPACE.to_string(), previous_key)],
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

fn lost_target_expired(created_at_ms: u64, now_ms: u64, window_ms: u64) -> bool {
    now_ms.saturating_sub(created_at_ms) >= window_ms
}

fn lost_retry_ms(created_at_ms: u64, now_ms: u64, base_delay_ms: u64, window_ms: u64) -> u64 {
    let elapsed = now_ms.saturating_sub(created_at_ms);
    let remaining = window_ms.saturating_sub(elapsed);
    base_delay_ms.min(remaining)
}

/// How long this round must still leave a launch by another witness alone. It
/// is a soft lease: the launch keeps the family until the window ends without a
/// receipt, and then any witness may plan again.
fn foreign_launch_wait(
    records: &[JobRecordEnvelope],
    local: NodeId,
    now_ms: u64,
    window_ms: u64,
) -> Option<u64> {
    let receipted: std::collections::BTreeSet<Ulid> = records
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Receipt(receipt) => Some(receipt.launch_id),
            _ => None,
        })
        .collect();
    records
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Launch(launch)
                if launch.scheduler_node_id != local && !receipted.contains(&launch.launch_id) =>
            {
                Some(launch.created_at_ms)
            }
            _ => None,
        })
        .max()
        .map(|created_at_ms| window_ms.saturating_sub(now_ms.saturating_sub(created_at_ms)))
        .filter(|remaining| *remaining > 0)
}

async fn read_deadline_index(
    context: &DriverContext,
    family: &JobFamilyId,
) -> Result<Option<WitnessDeadline>, LifecycleError> {
    read_row_txn(
        context,
        DEADLINE_INDEX_KEYSPACE,
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
            got: other.label(),
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
            got: other.label(),
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
            got: other.label(),
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
            got: other.label(),
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
