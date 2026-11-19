//! Runs one witness round for a family: suppression, budget, plan, launch and offer.
//! Every step reads only replicated records plus this node's own view.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use aruna_core::admin_documents::AdminDocumentClock;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::ExecutionTargetId;
use aruna_core::compute::offer::BackendLimits;
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame, NetEffect};
use aruna_core::events::{Event, ExecutionSignEvent, LaunchDecline, NetEvent};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::PLAN_EXPLAIN_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::scheduling::{ExecutionPlan, MAX_PLAN_CANDIDATES};
use aruna_core::structs::execution::job::record_rows::{from_bytes, to_bytes};
use aruna_core::structs::execution::job::{
    ExecutionClaim, ExpiryReason, JobFamilyId, JobFamilyRecord, JobRecordEnvelope, LaunchIntent,
    LogicalJobSpec, NotRun, PhysicalExecutionState, WitnessBudgetRecord, work_settled,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::PlacementDecision;
use aruna_core::types::{Key, Value};
use tracing::{debug, info, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::cancel::PublishCancelOperation;
use super::closure::CloseLaunchesOperation;
use super::launch_admit::{AdmitLaunchOperation, LaunchDecision, LaunchNode, LaunchUndecided};
use super::membership::{MembershipViewOperation, family_not_run};
use super::offer::{OfferLaunchOperation, OfferOutcome};
use super::plan::{BuildPlanOperation, PlanBuildError, PlanRound};
use super::witness_arm::WitnessExplain;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::fence::{fence_read, fence_up};
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::admit::Admission;
use crate::jobs::records::append::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin,
};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::pages::{PageReadError, SpecInputsOperation, StoreEntriesOperation};
use crate::jobs::records::reduce::reduce_family;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::route::JobRouteError;
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::store_call::{RowBatch, RowRead};
use crate::realm::load_config::config_unavailable;
use crate::secrets::owner::WriterView;
use aruna_core::structs::execution::job::PageManifest;

/// Wall-clock budget of one launch offer.
const OFFER_DEADLINE: Duration = Duration::from_secs(30);
/// Declined targets one explain row retains.
const MAX_DECLINED_TARGETS: usize = MAX_PLAN_CANDIDATES;

/// What a witness round knows about this node.
#[derive(Clone, Debug, PartialEq)]
pub struct WitnessNode {
    pub local: NodeId,
    pub realm_id: RealmId,
    /// The fresh heartbeats whose telemetry overrides the advertised one.
    pub fresh: BTreeMap<NodeId, HeldHeartbeat>,
    /// Each local executor backend's wire kind and limits, by its name.
    pub limits: BackendLimits,
}

impl WitnessNode {
    /// This node as a launch planned onto it sees it.
    fn launch_node(&self, launch: &LaunchFrame, execution_id: Ulid, now_ms: u64) -> LaunchNode {
        let limit = match &launch.envelope().record {
            JobFamilyRecord::Launch(intent) => self.limits.get(&intent.target.backend),
            _ => None,
        };
        LaunchNode {
            local: self.local,
            realm_id: self.realm_id,
            now_ms,
            execution_id,
            limits: limit.map(|limit| limit.limits),
            config_digest: limit.map(|limit| limit.config_digest),
        }
    }
}

/// What one round leaves behind: nothing more to do for this family here, or a
/// re-armed deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundOutcome {
    Done,
    Retry { after_ms: u64 },
}

type RoundNext = Result<Next<RoundOutcome>, IoError>;

fn finished() -> RoundNext {
    Ok(Next::Done(RoundOutcome::Done))
}

fn retry(after_ms: u64) -> RoundNext {
    Ok(Next::Done(RoundOutcome::Retry { after_ms }))
}

#[derive(Debug, PartialEq)]
enum RoundStep {
    Config,
    /// Behind the start fence this node creates and offers no launch.
    Fence,
    Family,
    Interval,
    Silence,
    BudgetSigned,
    BudgetStored,
    Explain,
    /// The launch closures of a family whose budget is used up, then its expiry cancel.
    Closures,
    Expired,
    Inputs,
    Planned,
    ExplainStored,
    PagesStored,
    LaunchSigned,
    LaunchStored,
    Offered,
    AdmittedHere,
    ReceiptStored,
    DeclineRead,
    DeclineStored,
}

/// The offer under way: its target and the delay a missing answer retries after.
#[derive(Debug, PartialEq)]
struct OfferState {
    target: ExecutionTargetId,
    retry_after_ms: u64,
}

/// What the round has established so far.
#[derive(Debug, PartialEq)]
struct RoundFacts {
    config: RealmConfigDocument,
    view: FamilyView,
    base_ms: u64,
    window_ms: u64,
    records: Vec<JobRecordEnvelope>,
    /// This node's applied realm config clock, which a new launch cites.
    clock: AdminDocumentClock,
    not_run: NotRun,
    spec: Option<LogicalJobSpec>,
    max_launches: u32,
    sequence: u32,
    overlapping: bool,
    explain: Option<WitnessExplain>,
    launch: Option<LaunchIntent>,
    target: Option<ExecutionTargetId>,
    frame: Option<JobRecordFrame>,
    offer: Option<OfferState>,
    declining: Option<ExecutionTargetId>,
}

/// One witness round for one family. A node without a network has nothing to schedule.
#[derive(Debug, PartialEq)]
pub struct WitnessRoundOperation {
    family: JobFamilyId,
    node: Option<WitnessNode>,
    now_ms: u64,
    launch_id: Ulid,
    execution_id: Ulid,
    step: RoundStep,
    facts: Option<Box<RoundFacts>>,
}

fn explain_key(family: &JobFamilyId, node: NodeId) -> Key {
    let mut bytes = family.to_bytes().to_vec();
    bytes.extend_from_slice(node.as_bytes());
    Key::from(bytes.as_slice())
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

/// The stored explain row; an unreadable row reads as none.
fn explain_row(result: IoResult) -> Result<Option<WitnessExplain>, IoError> {
    Ok(result
        .child::<Option<WitnessExplain>, JobMutationError>()?
        .unwrap_or(None))
}

fn stored_row(result: IoResult) -> Result<bool, IoError> {
    Ok(result.child::<(), JobMutationError>()?.is_ok())
}

/// Whether the node may rely on the appended record.
fn appended(result: IoResult) -> Result<bool, IoError> {
    Ok(match result.child::<AppendOutcome, RecordStoreError>()? {
        Ok(outcome) => matches!(
            outcome.admission,
            Admission::Authentic | Admission::Duplicate
        ),
        Err(error) => {
            warn!(error = %error, "Witness record append failed");
            false
        }
    })
}

fn signed_frame(result: IoResult) -> Result<Option<JobRecordFrame>, IoError> {
    Ok(match event(result)? {
        Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Signed(envelope))) => {
            JobRecordFrame::new(*envelope).ok()
        }
        _ => None,
    })
}

impl WitnessRoundOperation {
    pub fn new(family: JobFamilyId, node: Option<WitnessNode>, now_ms: u64) -> Self {
        Self {
            family,
            node,
            now_ms,
            launch_id: Ulid::nil(),
            execution_id: Ulid::nil(),
            step: RoundStep::Config,
            facts: None,
        }
    }

    fn node(&self) -> Result<&WitnessNode, IoError> {
        self.node.as_ref().ok_or(IoError::Unfinished)
    }

    fn facts(&mut self) -> Result<&mut RoundFacts, IoError> {
        self.facts.as_deref_mut().ok_or(IoError::Unfinished)
    }

    fn base_ms(&mut self) -> Result<u64, IoError> {
        Ok(self.facts()?.base_ms)
    }

    fn call(&mut self, step: RoundStep, call: IoCall) -> RoundNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn configured(&mut self, config: RealmConfigDocument) -> RoundNext {
        let (local, realm_id) = {
            let node = self.node()?;
            (node.local, node.realm_id)
        };
        let base_ms = config.compute.witness_delay_ms;
        let window_ms = config.compute.catch_up_ms;
        let Some(view) = FamilyView::resolve(&config, realm_id, self.family) else {
            return retry(base_ms);
        };
        if !view.holds(local) {
            return finished();
        }
        self.facts = Some(Box::new(RoundFacts {
            config,
            view,
            base_ms,
            window_ms,
            records: Vec::new(),
            clock: AdminDocumentClock::default(),
            not_run: NotRun::default(),
            spec: None,
            max_launches: 0,
            sequence: 0,
            overlapping: false,
            explain: None,
            launch: None,
            target: None,
            frame: None,
            offer: None,
            declining: None,
        }));
        self.call(RoundStep::Fence, fence_read())
    }

    /// An incomplete family read is undecided evidence: a suppression, cancel or earlier
    /// launch may be in the unloaded part, so no budget and no offer.
    fn family_read(&mut self, result: IoResult) -> RoundNext {
        let records = match result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()? {
            Ok(records) => records,
            Err(error) => {
                warn!(error = %error, "Job family read is incomplete; witness round deferred");
                return retry(self.base_ms()?);
            }
        };
        self.facts()?.records = records;
        let read = MembershipViewOperation::new(self.node()?.realm_id);
        self.call(RoundStep::Interval, IoCall::child(read))
    }

    /// Open work whose nodes all left is not run, and such a family is not launched again.
    fn interval_read(&mut self, result: IoResult) -> RoundNext {
        let Ok(Some(view)) = result.child::<Option<WriterView>, IoError>()? else {
            return retry(self.base_ms()?);
        };
        let family = self.family;
        let facts = self.facts()?;
        let not_run = family_not_run(&facts.records, &view);
        if not_run.family {
            return finished();
        }
        facts.not_run = not_run;
        facts.clock = view.clock;
        let running = running_nodes(family, &facts.records);
        if running.is_empty() {
            return self.judged(&BTreeSet::new(), &running);
        }
        let ask = Effect::Net(NetEffect::HeartbeatSilence(
            running.iter().copied().collect(),
        ));
        self.call(RoundStep::Silence, IoCall::Effect(Box::new(ask)))
    }

    fn silence_read(&mut self, result: IoResult) -> RoundNext {
        let Event::Net(NetEvent::HeartbeatSilence(silence)) = event(result)? else {
            return retry(self.base_ms()?);
        };
        let window_ms = self.facts()?.window_ms;
        let running = running_nodes(self.family, &self.facts()?.records);
        let silent = silent_nodes(&silence, &running, window_ms);
        self.judged(&silent, &running)
    }

    fn judged(&mut self, silent: &BTreeSet<NodeId>, running: &BTreeSet<NodeId>) -> RoundNext {
        let family = self.family;
        let local = self.node()?.local;
        let facts = self.facts()?;
        if suppressed(family, &facts.records, silent) {
            // An unfinished execution suppresses only while its node reports, so look later.
            return match suppressed(family, &facts.records, running) {
                true => finished(),
                false => retry(facts.window_ms),
            };
        }
        let Some(spec) = stored_spec(family, &facts.records) else {
            return retry(facts.base_ms);
        };
        let stored = own_budget(&facts.records, local);
        let budget = WitnessBudgetRecord {
            submission_id: spec.submission_id,
            request_digest: spec.request_digest,
            scheduler_node_id: local,
            source_spec_digest: spec.spec_digest,
            max_launches: spec.retry.launches_per_witness,
        };
        facts.spec = Some(spec);
        if let Some(stored) = stored {
            facts.max_launches = stored.max_launches;
            return self.read_explain();
        }
        facts.max_launches = budget.max_launches;
        let realm_id = facts.config.realm_id;
        self.sign(
            realm_id,
            JobFamilyRecord::Budget(budget),
            RoundStep::BudgetSigned,
        )
    }

    fn sign(&mut self, realm_id: RealmId, record: JobFamilyRecord, step: RoundStep) -> RoundNext {
        let claim = ExecutionClaim {
            realm_id,
            record,
            publisher: self.node()?.local,
        };
        let effect = Effect::Net(NetEffect::ExecutionSign(Box::new(claim)));
        self.call(step, IoCall::Effect(Box::new(effect)))
    }

    fn append(&mut self, frame: JobRecordFrame, realm_id: RealmId, step: RoundStep) -> RoundNext {
        let append = AppendRecordOperation::new(AppendRecordConfig {
            realm_id,
            local_node_id: self.node()?.local,
            record: frame,
            local: None,
            origin: RecordOrigin::Local,
            now_ms: self.now_ms,
        });
        self.call(step, IoCall::child(append))
    }

    fn explain_read(&self) -> Result<IoCall, IoError> {
        let key = explain_key(&self.family, self.node()?.local);
        let read = RowRead::new(PLAN_EXPLAIN_KEYSPACE, key, |row: Option<Value>| {
            Ok(row.and_then(|bytes| from_bytes::<WitnessExplain>(&bytes).ok()))
        });
        Ok(IoCall::child(read))
    }

    fn explain_write(&self, explain: &WitnessExplain) -> Result<IoCall, IoError> {
        let key = explain_key(&self.family, self.node()?.local);
        let rows = to_bytes(explain)
            .map(|bytes| {
                let value = Value::from(bytes.as_slice());
                vec![(PLAN_EXPLAIN_KEYSPACE.to_string(), key, value)]
            })
            .map_err(|error| JobMutationError::Storage(error.to_string()));
        Ok(IoCall::child(RowBatch::write(rows)))
    }

    fn read_explain(&mut self) -> RoundNext {
        let read = self.explain_read()?;
        self.call(RoundStep::Explain, read)
    }

    fn explained(&mut self, stored: Option<WitnessExplain>) -> RoundNext {
        let (local, now_ms) = (self.node()?.local, self.now_ms);
        let serving = self.serving()?;
        let facts = self.facts()?;
        let mine = own_launches(&facts.records, local);
        let sequence = mine.len() as u32;
        let explain = stored.unwrap_or(WitnessExplain {
            sequence,
            plan: empty_plan(),
            declined: Vec::new(),
            overlapping: false,
            stored_at_ms: now_ms,
        });
        let (base_ms, window_ms) = (facts.base_ms, facts.window_ms);
        let latest = mine.iter().max_by_key(|(_, launch)| launch.scheduler_seq);
        if let Some((envelope, launch)) = latest
            && !explain.declined.contains(&launch.target)
        {
            let target = launch.target.clone();
            let created_at_ms = launch.created_at_ms;
            let frame = JobRecordFrame::new((*envelope).clone());
            if lost_target_expired(created_at_ms, now_ms, window_ms) {
                return self.decline(target);
            }
            let retry_after_ms = lost_retry_ms(created_at_ms, now_ms, base_ms, window_ms);
            if created_at_ms.saturating_add(base_ms) > now_ms {
                return retry(retry_after_ms);
            }
            let Ok(frame) = frame else {
                return finished();
            };
            return self.offer(frame, target, retry_after_ms);
        }
        let overlapping = !mine.is_empty();
        // Another witness already launched and its window has not run out, so this round
        // leaves the family to it instead of adding a second execution.
        if let Some(after_ms) = foreign_launch_wait(&facts.records, local, now_ms, window_ms) {
            debug!(after_ms, "Another witness holds a launch for this family");
            return retry(after_ms);
        }
        if sequence >= facts.max_launches {
            debug!(sequence, "Witness budget is exhausted for this request");
            let records = facts.records.clone();
            let close = CloseLaunchesOperation::new(self.family, records, serving);
            return self.call(RoundStep::Closures, IoCall::child(close));
        }
        facts.sequence = sequence;
        facts.overlapping = overlapping;
        facts.explain = Some(explain);
        let spec = facts.spec.as_ref().ok_or(IoError::Unfinished)?;
        let fill = SpecInputsOperation::new(spec, serving);
        self.call(RoundStep::Inputs, IoCall::child(fill))
    }

    /// A used-up budget ends the family with an expiry cancel, but only once every known launch
    /// is received or closed and every execution stopped, so the cancel never stops a running one.
    fn budget_used(&mut self, result: IoResult) -> RoundNext {
        let (now_ms, serving) = (self.now_ms, self.serving()?);
        let facts = self.facts()?;
        let window_ms = facts.window_ms;
        let closed = match result.child::<BTreeSet<Ulid>, LifecycleError>()? {
            Ok(closed) => closed,
            Err(error) => {
                warn!(error = %error, "Launch closures could not be read");
                return retry(window_ms);
            }
        };
        if !work_settled(&facts.records, &closed, &facts.not_run) {
            return retry(window_ms);
        }
        let spec = facts.spec.clone().ok_or(IoError::Unfinished)?;
        let reason = ExpiryReason::BudgetUsed;
        let cancel = PublishCancelOperation::expiry(spec, reason, serving, now_ms);
        self.call(RoundStep::Expired, IoCall::child(cancel))
    }

    fn serving(&self) -> Result<Option<(NodeId, RealmId)>, IoError> {
        let node = self.node()?;
        Ok(Some((node.local, node.realm_id)))
    }

    fn plan_failed(&mut self, error: impl std::fmt::Display) -> RoundNext {
        warn!(error = %error, "Execution planning failed");
        retry(self.base_ms()?)
    }

    fn inputs_read(&mut self, result: IoResult) -> RoundNext {
        let spec = match result.child::<LogicalJobSpec, PageReadError>()? {
            Ok(spec) => spec,
            Err(error) => {
                let error = PlanBuildError::Unavailable(error.to_string());
                return self.plan_failed(error);
            }
        };
        let (local, fresh) = {
            let node = self.node()?;
            (node.local, node.fresh.clone())
        };
        let now_ms = self.now_ms;
        let facts = self.facts()?;
        let explain = facts.explain.as_ref().ok_or(IoError::Unfinished)?;
        let round = PlanRound {
            config: facts.config.clone(),
            spec,
            excluded: explain.declined.clone(),
            now_ms,
            local,
            fresh,
        };
        self.call(
            RoundStep::Planned,
            IoCall::child(BuildPlanOperation::new(round)),
        )
    }

    /// The explain record is durable before any launch exists, so every launch has an
    /// auditable reason even if this node dies right after sending it.
    fn planned(&mut self, result: IoResult) -> RoundNext {
        let plan = match result.child::<ExecutionPlan, PlanBuildError>()? {
            Ok(plan) => plan,
            Err(error) => return self.plan_failed(error),
        };
        let now_ms = self.now_ms;
        let facts = self.facts()?;
        let sequence = facts.sequence;
        let overlapping = facts.overlapping;
        info!(
            sequence,
            selected = plan.selected.is_some(),
            alternatives = plan.alternatives.len(),
            rejected = plan.rejected.len(),
            omitted = plan.omitted,
            retryable = plan.retryable,
            "Witness planning round decided"
        );
        let explain = facts.explain.as_mut().ok_or(IoError::Unfinished)?;
        explain.sequence = sequence;
        explain.overlapping = overlapping;
        explain.plan = plan;
        explain.stored_at_ms = now_ms;
        let explain = explain.clone();
        let write = self.explain_write(&explain)?;
        self.call(RoundStep::ExplainStored, write)
    }

    fn explain_stored(&mut self, result: IoResult) -> RoundNext {
        let base_ms = self.base_ms()?;
        if !stored_row(result)? {
            return retry(base_ms);
        }
        let (family, now_ms, launch_id) = (self.family, self.now_ms, self.launch_id);
        let local = self.node()?.local;
        let facts = self.facts()?;
        let plan = &facts.explain.as_ref().ok_or(IoError::Unfinished)?.plan;
        let Some(selection) = plan.selected.clone() else {
            let after_ms = match plan.retryable {
                true => base_ms,
                false => base_ms.saturating_mul(4),
            };
            return retry(after_ms);
        };
        let spec = facts.spec.as_ref().ok_or(IoError::Unfinished)?;
        let launch = LaunchIntent {
            launch_id,
            submission_id: family.submission_id,
            request_digest: family.request_digest,
            job_id: spec.job_id,
            scheduler_node_id: local,
            scheduler_seq: facts.sequence,
            witness_placement: facts.view.placement,
            holder_generation: holder_generation(&facts.config, &facts.view),
            target: selection.target.clone(),
            inputs: selection.inputs,
            input_pages: None,
            output_policies: selection.output_policies,
            plan_digest: selection.plan_digest,
            spec_digest: spec.spec_digest,
            created_at_ms: now_ms,
            config_clock: facts.clock.clone(),
        };
        // Planned inputs too large for the launch record go to pages this node serves.
        let store = StoreEntriesOperation::launch(&launch);
        facts.target = Some(selection.target);
        facts.launch = Some(launch);
        self.call(RoundStep::PagesStored, IoCall::child(store))
    }

    fn pages_stored(&mut self, result: IoResult) -> RoundNext {
        let manifest = match result.child::<Option<PageManifest>, PageReadError>()? {
            Ok(manifest) => manifest,
            Err(_) => return retry(self.base_ms()?),
        };
        let realm_id = self.node()?.realm_id;
        let facts = self.facts()?;
        let mut launch = facts.launch.take().ok_or(IoError::Unfinished)?;
        if let Some(manifest) = manifest {
            launch.inputs.clear();
            launch.input_pages = Some(manifest);
        }
        let record = JobFamilyRecord::Launch(Box::new(launch));
        self.sign(realm_id, record, RoundStep::LaunchSigned)
    }

    fn launch_signed(&mut self, result: IoResult) -> RoundNext {
        let Some(frame) = signed_frame(result)? else {
            return retry(self.base_ms()?);
        };
        let realm_id = self.node()?.realm_id;
        self.facts()?.frame = Some(frame.clone());
        self.append(frame, realm_id, RoundStep::LaunchStored)
    }

    fn launch_stored(&mut self, result: IoResult) -> RoundNext {
        let now_ms = self.now_ms;
        let facts = self.facts()?;
        let (base_ms, window_ms) = (facts.base_ms, facts.window_ms);
        if !appended(result)? {
            return retry(base_ms);
        }
        let frame = facts.frame.take().ok_or(IoError::Unfinished)?;
        let target = facts.target.take().ok_or(IoError::Unfinished)?;
        let retry_after_ms = lost_retry_ms(now_ms, now_ms, base_ms, window_ms);
        self.offer(frame, target, retry_after_ms)
    }

    /// Sends the durable launch to its target. No transport dials the node it starts from, so
    /// a launch planned onto this node is admitted here, with the decision an offer gets.
    fn offer(
        &mut self,
        frame: JobRecordFrame,
        target: ExecutionTargetId,
        retry_after_ms: u64,
    ) -> RoundNext {
        let Ok(launch) = LaunchFrame::new(frame.into_inner()) else {
            return finished();
        };
        let target_node = target.node_id;
        self.facts()?.offer = Some(OfferState {
            target,
            retry_after_ms,
        });
        let node = self.node()?;
        if node.local == target_node {
            let local = node.launch_node(&launch, self.execution_id, self.now_ms);
            let admit = AdmitLaunchOperation::new(launch, local);
            return self.call(RoundStep::AdmittedHere, IoCall::child(admit));
        }
        let offer = OfferLaunchOperation::new(target_node, launch, OFFER_DEADLINE);
        self.call(RoundStep::Offered, IoCall::child(offer))
    }

    fn admitted_here(&mut self, result: IoResult) -> RoundNext {
        let outcome = match result.child::<LaunchDecision, LaunchUndecided>()? {
            Ok(Ok(receipt)) => match JobRecordFrame::new(receipt.into_inner()) {
                Ok(frame) => OfferOutcome::Accepted(frame),
                Err(_) => OfferOutcome::Unavailable,
            },
            Ok(Err(reason)) => OfferOutcome::Declined(reason),
            Err(_) => OfferOutcome::Unavailable,
        };
        self.settle(outcome)
    }

    fn offer_state(&mut self) -> Result<OfferState, IoError> {
        self.facts()?.offer.take().ok_or(IoError::Unfinished)
    }

    fn settle(&mut self, outcome: OfferOutcome) -> RoundNext {
        let offer = self.offer_state()?;
        match outcome {
            OfferOutcome::Accepted(receipt) => {
                let realm_id = self.node()?.realm_id;
                self.facts()?.offer = Some(offer);
                self.append(receipt, realm_id, RoundStep::ReceiptStored)
            }
            OfferOutcome::Declined(LaunchDecline::Cancelled) => finished(),
            OfferOutcome::Declined(reason) if permanent_decline(&reason) => {
                self.decline(offer.target)
            }
            OfferOutcome::Declined(_) | OfferOutcome::Unavailable => retry(offer.retry_after_ms),
        }
    }

    /// Remembers a target that refused for good, so the next round plans around it.
    fn decline(&mut self, target: ExecutionTargetId) -> RoundNext {
        self.facts()?.declining = Some(target);
        let read = self.explain_read()?;
        self.call(RoundStep::DeclineRead, read)
    }

    fn decline_read(&mut self, result: IoResult) -> RoundNext {
        let target = self.facts()?.declining.take();
        let (Some(mut explain), Some(target)) = (explain_row(result)?, target) else {
            return retry(0);
        };
        if !explain.declined.contains(&target) && explain.declined.len() < MAX_DECLINED_TARGETS {
            explain.declined.push(target);
        }
        let write = self.explain_write(&explain)?;
        self.call(RoundStep::DeclineStored, write)
    }
}

impl Operation for WitnessRoundOperation {
    type Output = RoundOutcome;
    type Error = IoError;

    /// Both ids stay fixed for the whole run.
    fn seed(&mut self, entropy: u128) {
        let mut ids = IdMint::new(entropy);
        self.launch_id = ids.mint(self.now_ms);
        self.execution_id = ids.mint(self.now_ms);
    }

    fn first(&mut self) -> RoundNext {
        let Some(node) = self.node.as_ref() else {
            return finished();
        };
        let read = GetConfigOperation::new(node.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> RoundNext {
        match self.step {
            RoundStep::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(config) => self.configured(config),
                Err(error) => {
                    config_unavailable(&error);
                    retry(1_000)
                }
            },
            RoundStep::Fence => match fence_up(result) {
                true => retry(self.base_ms()?),
                false => {
                    let read = LoadFamilyOperation::family(self.family);
                    self.call(RoundStep::Family, IoCall::child(read))
                }
            },
            RoundStep::Family => self.family_read(result),
            RoundStep::Interval => self.interval_read(result),
            RoundStep::Silence => self.silence_read(result),
            RoundStep::BudgetSigned => {
                let Some(frame) = signed_frame(result)? else {
                    return retry(self.base_ms()?);
                };
                let realm_id = self.facts()?.config.realm_id;
                self.append(frame, realm_id, RoundStep::BudgetStored)
            }
            RoundStep::BudgetStored => match appended(result)? {
                true => self.read_explain(),
                false => retry(self.base_ms()?),
            },
            RoundStep::Explain => {
                let stored = explain_row(result)?;
                self.explained(stored)
            }
            RoundStep::Closures => self.budget_used(result),
            RoundStep::Expired => match result.child::<(), JobRouteError>()? {
                Ok(()) => finished(),
                Err(error) => {
                    warn!(error = %error, "Expiry cancel of a used-up budget was not published");
                    retry(self.facts()?.window_ms)
                }
            },
            RoundStep::Inputs => self.inputs_read(result),
            RoundStep::Planned => self.planned(result),
            RoundStep::ExplainStored => self.explain_stored(result),
            RoundStep::PagesStored => self.pages_stored(result),
            RoundStep::LaunchSigned => self.launch_signed(result),
            RoundStep::LaunchStored => self.launch_stored(result),
            RoundStep::Offered => {
                let outcome = result
                    .child::<OfferOutcome, LifecycleError>()?
                    .unwrap_or(OfferOutcome::Unavailable);
                self.settle(outcome)
            }
            RoundStep::AdmittedHere => self.admitted_here(result),
            RoundStep::ReceiptStored => {
                let offer = self.offer_state()?;
                match appended(result)? {
                    true => finished(),
                    false => retry(offer.retry_after_ms),
                }
            }
            RoundStep::DeclineRead => self.decline_read(result),
            RoundStep::DeclineStored => retry(0),
        }
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
        | LaunchDecline::AlreadyRunning
        | LaunchDecline::Expired
        | LaunchDecline::WrongInterval => false,
    }
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
fn silent_nodes(
    silence: &BTreeMap<NodeId, u64>,
    running: &BTreeSet<NodeId>,
    window_ms: u64,
) -> BTreeSet<NodeId> {
    running
        .iter()
        .copied()
        .filter(|node| silence.get(node).is_some_and(|ms| *ms > window_ms))
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

/// This node's immutable launch bound, once it is stored.
fn own_budget(records: &[JobRecordEnvelope], local: NodeId) -> Option<WitnessBudgetRecord> {
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Budget(budget) if budget.scheduler_node_id == local => Some(*budget),
        _ => None,
    })
}

/// The launches this node already signed for the family.
fn own_launches(
    records: &[JobRecordEnvelope],
    local: NodeId,
) -> Vec<(JobRecordEnvelope, LaunchIntent)> {
    records
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Launch(launch) if launch.scheduler_node_id == local => {
                Some((envelope.clone(), launch.as_ref().clone()))
            }
            _ => None,
        })
        .collect()
}

/// The candidate map epoch this witness observed, recorded as audit evidence
/// only: it never proves that the scheduler held the family.
fn holder_generation(config: &RealmConfigDocument, view: &FamilyView) -> u64 {
    config
        .activation(&view.placement.strategy_id, view.placement.shard)
        .map(|activation| activation.candidate_map_epoch)
        .unwrap_or_default()
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
    let receipted: BTreeSet<Ulid> = records
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

#[cfg(test)]
#[path = "witness_round_tests.rs"]
mod tests;
