//! Bounded bulk application of a bucket default to current heads.
//!
//! A run seals one `(bucket identity, generation, target refs)` target in its
//! own transaction. Each object is minted through the same per-version
//! sub-operation the single-object mutation uses, which re-reads the sealed
//! default, the head and the intent inside its commit boundary. The application
//! is additive: it unions the sealed refs with the head re-read inside the mint
//! transaction, so applying a default never removes an object's constraints.

use crate::blob::blob_keyspace_helper::HeadAliasContext;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement_policy::resolve_set::{PolicySetResolver, ResolveMode, ResolveStep};
use crate::s3::policy_successor::{
    MintPolicySuccessorOperation, SealedDefault, SuccessorError, SuccessorOutcome, SuccessorPlan,
};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer,
    POLICY_BULK_INTENT_KEYSPACE, POLICY_BULK_RUN_KEYSPACE, Permission, PlacementPolicyRef,
    PlacementSubject, PolicyBlockedReason, PolicyBulkIntent, PolicyBulkIntentKey, PolicyBulkRun,
    PolicyBulkStatus, PolicyIntentOutcome, PolicyRefMode, PolicyResolution, VersionKey,
    policy_admin_path,
};
use aruna_core::types::{Effects, Key, TxnId};
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

/// Upper bound on the heads one pass touches.
pub const BULK_PAGE_LIMIT: usize = 128;

#[derive(Clone, Debug, PartialEq)]
pub struct BulkConfig {
    pub operation_id: Ulid,
    pub bucket: String,
    pub auth_context: AuthContext,
    pub subject: PlacementSubject,
    pub start_after: Option<Key>,
    pub limit: usize,
    pub now_ms: u64,
    pub created_at: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockedGap {
    pub key: String,
    pub reason: PolicyBlockedReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BulkReport {
    pub operation_id: Ulid,
    pub status: PolicyBulkStatus,
    pub generation: u64,
    pub target_refs: Vec<PlacementPolicyRef>,
    pub observed: usize,
    /// Heads this pass needed no successor for: already sealed, a delete
    /// marker, or an intent an earlier pass completed.
    pub covered: usize,
    pub minted: usize,
    /// Intents replanned from a newer head; the next pass mints them.
    pub replanned: usize,
    pub blocked: Vec<BlockedGap>,
    pub cursor: Option<Key>,
    /// True when this pass exhausted the bounded local iterator.
    pub complete: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum BulkError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Successor(#[from] SuccessorError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("caller may not administer the realm configuration")]
    Unauthorized,
    #[error("the run was sealed against a different bucket record")]
    BucketChanged,
    #[error("unexpected event during the bulk pass")]
    InvalidEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BulkState {
    Init,
    Authorize,
    StartSeal,
    ReadSeal,
    WriteRun,
    CommitSeal,
    Resolve,
    ScanHeads,
    ReadVersions,
    ResolveUnion,
    ReadIntents,
    Mint,
    /// The finished mint left a cleanup effect whose event must be consumed
    /// before the next object opens its own transaction.
    Settle,
    StartStatus,
    ReadStatus,
    WriteStatus,
    CommitStatus,
    Finish,
    Error,
}

/// One head this pass observed, with the durable intent it is minted under.
#[derive(Clone, Debug, PartialEq)]
struct Candidate {
    key: String,
    pointer: CurrentVersionPointer,
    /// Refs the observed head already carries; the union mint needs them
    /// resolved before it can decide.
    refs: Vec<PlacementPolicyRef>,
    intent: Option<PolicyBulkIntent>,
}

/// Runs one bounded pass over this responder's own heads. Nothing here claims
/// whole-bucket or realm-wide convergence: the report states what this node
/// observed and what stays a resumable gap.
#[derive(Debug, PartialEq)]
pub struct PolicyBulkOperation {
    config: BulkConfig,
    state: BulkState,
    txn_id: Option<TxnId>,
    group_id: Option<Ulid>,
    run: Option<PolicyBulkRun>,
    resolver: Option<PolicySetResolver>,
    resolved: BTreeMap<Ulid, PolicyResolution>,
    candidates: Vec<Candidate>,
    index: usize,
    mint: Option<MintPolicySuccessorOperation>,
    /// Outcome of a mint that is still cleaning up.
    settled: Option<Result<SuccessorOutcome, SuccessorError>>,
    /// The status the closing transaction writes once the pass has finished.
    pending_status: Option<PolicyBulkStatus>,
    report: BulkReport,
    output: Option<Result<BulkReport, BulkError>>,
}

impl PolicyBulkOperation {
    pub fn new(config: BulkConfig) -> Self {
        let report = BulkReport {
            operation_id: config.operation_id,
            status: PolicyBulkStatus::Active,
            generation: 0,
            target_refs: Vec::new(),
            observed: 0,
            covered: 0,
            minted: 0,
            replanned: 0,
            blocked: Vec::new(),
            cursor: None,
            complete: false,
        };
        Self {
            config,
            state: BulkState::Init,
            txn_id: None,
            group_id: None,
            run: None,
            resolver: None,
            resolved: BTreeMap::new(),
            candidates: Vec::new(),
            index: 0,
            mint: None,
            settled: None,
            pending_status: None,
            report,
            output: None,
        }
    }

    fn page_limit(&self) -> usize {
        self.config.limit.clamp(1, BULK_PAGE_LIMIT)
    }

    fn fail(&mut self, error: BulkError) -> Effects {
        let cleanup = self.abort();
        self.state = BulkState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn finish(&mut self) -> Effects {
        if let Some(run) = self.run.as_ref() {
            self.report.status = run.status;
        }
        self.output = Some(Ok(self.report.clone()));
        self.state = BulkState::Finish;
        smallvec![]
    }

    /// Rolls the run transaction back and reports what the pass observed.
    fn stop(&mut self) -> Effects {
        let mut effects = self.abort();
        effects.extend(self.finish());
        effects
    }

    /// The run's own transaction: sealing, superseding and completion are all
    /// compare-and-set writes against the row this pass just read.
    fn read_seal(&mut self, txn_id: TxnId) -> Result<Effects, BulkError> {
        self.txn_id = Some(txn_id);
        self.state = BulkState::ReadSeal;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    S3_BUCKET_KEYSPACE.to_string(),
                    Key::from(self.config.bucket.as_bytes().to_vec()),
                ),
                (
                    POLICY_BULK_RUN_KEYSPACE.to_string(),
                    Key::from(PolicyBulkRun::key(self.config.operation_id)?),
                ),
            ],
            txn_id: Some(txn_id),
        })])
    }

    fn handle_seal(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.fail(BulkError::InvalidEvent);
        };
        let [(_, bucket_value), (_, run_value)] = values.as_slice() else {
            return self.fail(BulkError::InvalidEvent);
        };
        let Some(bucket_value) = bucket_value.as_ref() else {
            return self.fail(BulkError::NoSuchBucket);
        };
        let bucket = match BucketInfo::from_bytes(bucket_value.as_ref()) {
            Ok(bucket) => bucket,
            Err(error) => return self.fail(error.into()),
        };
        let stored = match run_value
            .as_ref()
            .map(|value| PolicyBulkRun::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(stored) => stored,
            Err(error) => return self.fail(error.into()),
        };
        self.group_id = Some(bucket.group_id);
        self.report.generation = bucket.placement_policy_generation;
        self.report.target_refs = bucket.placement_policies.clone();

        let Some(run) = stored else {
            let run = PolicyBulkRun {
                operation_id: self.config.operation_id,
                bucket: self.config.bucket.clone(),
                bucket_identity: bucket.identity(),
                generation: bucket.placement_policy_generation,
                target_refs: bucket.placement_policies.clone(),
                status: PolicyBulkStatus::Active,
            };
            return self.write_run(run);
        };
        if run.bucket != self.config.bucket || run.bucket_identity != bucket.identity() {
            return self.fail(BulkError::BucketChanged);
        }
        self.report.generation = run.generation;
        self.report.target_refs = run.target_refs.clone();
        // A default change ends the run: one pass never mixes two policies.
        if run.generation != bucket.placement_policy_generation
            || run.target_refs != bucket.placement_policies
        {
            let mut superseded = run;
            superseded.status = PolicyBulkStatus::Superseded;
            return self.write_run(superseded);
        }
        if run.status != PolicyBulkStatus::Active {
            self.run = Some(run);
            return self.stop();
        }
        self.run = Some(run);
        match self.txn_id {
            Some(txn_id) => {
                self.state = BulkState::CommitSeal;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            None => self.fail(BulkError::InvalidEvent),
        }
    }

    fn write_run(&mut self, run: PolicyBulkRun) -> Effects {
        let (key, value) = match (PolicyBulkRun::key(run.operation_id), run.to_bytes()) {
            (Ok(key), Ok(value)) => (key, value),
            (Err(error), _) | (_, Err(error)) => return self.fail(error.into()),
        };
        self.run = Some(run);
        self.state = BulkState::WriteRun;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: POLICY_BULK_RUN_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    /// After the seal commits, the pass either stops or resolves its target.
    fn after_seal(&mut self) -> Effects {
        self.txn_id = None;
        let stopped = self
            .run
            .as_ref()
            .is_none_or(|run| run.status != PolicyBulkStatus::Active);
        if stopped {
            return self.finish();
        }
        self.resolve_refs()
    }

    fn resolve_refs(&mut self) -> Effects {
        let refs = self
            .run
            .as_ref()
            .map(|run| run.target_refs.clone())
            .unwrap_or_default();
        let mut resolver = PolicySetResolver::new(
            self.config.auth_context.realm_id,
            self.config.subject.node_id,
            self.config.now_ms,
            // A rule this node cannot obtain blocks its objects; the run stays
            // resumable instead of failing.
            ResolveMode::Lenient,
            &refs,
        );
        let step = resolver.start();
        self.resolver = Some(resolver);
        self.state = BulkState::Resolve;
        self.after_resolve(step)
    }

    fn after_resolve(&mut self, step: ResolveStep) -> Effects {
        match step {
            ResolveStep::Pending(effects) => effects,
            ResolveStep::Done | ResolveStep::Failed(..) => {
                if let Some(resolver) = self.resolver.take() {
                    self.resolved.extend(resolver.into_resolutions());
                }
                self.scan_heads()
            }
        }
    }

    fn scan_heads(&mut self) -> Effects {
        let prefix = match BlobHeadKey::bucket_prefix(&self.config.bucket) {
            Ok(prefix) => prefix,
            Err(error) => return self.fail(error.into()),
        };
        self.state = BulkState::ScanHeads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: self.config.start_after.clone().map(IterStart::After),
            limit: self.page_limit(),
            txn_id: None,
        })]
    }

    fn handle_heads(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.fail(BulkError::InvalidEvent);
        };
        self.report.cursor = next_start_after;
        self.report.complete = self.report.cursor.is_none();
        if values.is_empty() {
            return self.after_page();
        }
        let mut reads = Vec::with_capacity(values.len());
        for (key, value) in values {
            let head = match BlobHeadKey::from_bytes(key.as_ref()) {
                Ok(head) => head,
                Err(error) => return self.fail(error.into()),
            };
            let pointer = match CurrentVersionPointer::from_bytes(value.as_ref()) {
                Ok(pointer) => pointer,
                Err(error) => return self.fail(error.into()),
            };
            let version_key = VersionKey::new(&self.config.bucket, &head.key, pointer.version_id);
            let encoded = match version_key.to_bytes() {
                Ok(encoded) => encoded,
                Err(error) => return self.fail(error.into()),
            };
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(encoded)));
            self.candidates.push(Candidate {
                key: head.key,
                pointer,
                refs: Vec::new(),
                intent: None,
            });
        }
        self.state = BulkState::ReadVersions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn handle_versions(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.fail(BulkError::InvalidEvent);
        };
        if values.len() != self.candidates.len() {
            return self.fail(BulkError::InvalidEvent);
        }
        let target = self
            .run
            .as_ref()
            .map(|run| run.target_refs.clone())
            .unwrap_or_default();
        let candidates = std::mem::take(&mut self.candidates);
        let mut pending = Vec::with_capacity(candidates.len());
        for (mut candidate, (_, value)) in candidates.into_iter().zip(values) {
            self.report.observed += 1;
            let Some(value) = value else {
                // A head pointing at a version this node no longer stores is a
                // gap, never a covered object.
                self.report.blocked.push(BlockedGap {
                    key: candidate.key,
                    reason: PolicyBlockedReason::SourceUnavailable,
                });
                continue;
            };
            let version = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => version,
                Err(error) => return self.fail(error.into()),
            };
            if version.state == BlobVersionState::Deleted || covered(&version, &target) {
                self.report.covered += 1;
                continue;
            }
            candidate.refs = version.placement_policies;
            pending.push(candidate);
        }
        self.candidates = pending;
        if self.candidates.is_empty() {
            return self.after_page();
        }
        self.resolve_union()
    }

    /// The union a mint seals also contains the refs the head already carries,
    /// so they have to be authenticated too before any of them may be evaluated.
    fn resolve_union(&mut self) -> Effects {
        let mut refs = Vec::new();
        for candidate in &self.candidates {
            for policy_ref in &candidate.refs {
                if !self.resolved.contains_key(&policy_ref.policy_id) {
                    refs.push(*policy_ref);
                }
            }
        }
        if refs.is_empty() {
            return self.read_intents();
        }
        let mut resolver = PolicySetResolver::new(
            self.config.auth_context.realm_id,
            self.config.subject.node_id,
            self.config.now_ms,
            ResolveMode::Lenient,
            &refs,
        );
        let step = resolver.start();
        self.resolver = Some(resolver);
        self.state = BulkState::ResolveUnion;
        self.after_union(step)
    }

    fn after_union(&mut self, step: ResolveStep) -> Effects {
        match step {
            ResolveStep::Pending(effects) => effects,
            ResolveStep::Done | ResolveStep::Failed(..) => {
                if let Some(resolver) = self.resolver.take() {
                    self.resolved.extend(resolver.into_resolutions());
                }
                self.read_intents()
            }
        }
    }

    fn read_intents(&mut self) -> Effects {
        let mut reads = Vec::with_capacity(self.candidates.len());
        for candidate in &self.candidates {
            let key = PolicyBulkIntentKey::new(self.config.operation_id, candidate.key.clone());
            let encoded = match key.to_bytes() {
                Ok(encoded) => encoded,
                Err(error) => return self.fail(error.into()),
            };
            reads.push((POLICY_BULK_INTENT_KEYSPACE.to_string(), Key::from(encoded)));
        }
        self.state = BulkState::ReadIntents;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    /// Reuses the intent planned for this head, replans one whose head moved,
    /// and never re-mints one an earlier pass completed.
    fn handle_intents(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.fail(BulkError::InvalidEvent);
        };
        if values.len() != self.candidates.len() {
            return self.fail(BulkError::InvalidEvent);
        }
        let candidates = std::mem::take(&mut self.candidates);
        let mut pending = Vec::with_capacity(candidates.len());
        for (mut candidate, (_, value)) in candidates.into_iter().zip(values) {
            let stored = match value
                .map(|value| PolicyBulkIntent::from_bytes(value.as_ref()))
                .transpose()
            {
                Ok(stored) => stored,
                Err(error) => return self.fail(error.into()),
            };
            let intent = match stored {
                Some(intent) if matches!(intent.outcome, PolicyIntentOutcome::Completed { .. }) => {
                    self.report.covered += 1;
                    continue;
                }
                Some(intent) if intent.observed_head == candidate.pointer => intent,
                stale => {
                    if stale.is_some() {
                        self.report.replanned += 1;
                    }
                    PolicyBulkIntent {
                        operation_id: self.config.operation_id,
                        key: candidate.key.clone(),
                        observed_head: candidate.pointer.clone(),
                        successor_version_id: Ulid::generate(),
                        outcome: PolicyIntentOutcome::Planned,
                    }
                }
            };
            candidate.intent = Some(intent);
            pending.push(candidate);
        }
        self.candidates = pending;
        self.index = 0;
        self.mint_next()
    }

    fn mint_next(&mut self) -> Effects {
        let Some(candidate) = self.candidates.get(self.index) else {
            return self.after_page();
        };
        let (Some(intent), Some(run), Some(group_id)) =
            (candidate.intent.clone(), self.run.as_ref(), self.group_id)
        else {
            return self.fail(BulkError::InvalidEvent);
        };
        let plan = SuccessorPlan {
            context: HeadAliasContext::new(
                self.config.auth_context.realm_id,
                group_id,
                self.config.subject.node_id,
                self.config.bucket.clone(),
                candidate.key.clone(),
            ),
            // The preassigned successor is also the mutation identity, so a
            // retried pass replays onto the same version instead of minting
            // another.
            mutation_id: intent.successor_version_id,
            expected_head: intent.observed_head.clone(),
            bucket_identity: run.bucket_identity,
            target_refs: run.target_refs.clone(),
            mode: PolicyRefMode::Union,
            successor_version_id: intent.successor_version_id,
            created_at: self.config.created_at,
            auth_context: self.config.auth_context.clone(),
            subject: self.config.subject.clone(),
            resolved: self.resolved.clone(),
            sealed_default: Some(SealedDefault {
                generation: run.generation,
                refs: run.target_refs.clone(),
            }),
            intent: Some(intent),
        };
        let mut mint = MintPolicySuccessorOperation::new(plan);
        let effects = mint.start();
        self.mint = Some(mint);
        self.state = BulkState::Mint;
        effects
    }

    fn record_outcome(&mut self, result: Result<SuccessorOutcome, SuccessorError>) -> Effects {
        let key = self
            .candidates
            .get(self.index)
            .map(|candidate| candidate.key.clone())
            .unwrap_or_default();
        self.index += 1;
        match result {
            Ok(SuccessorOutcome::Minted { .. }) => self.report.minted += 1,
            Ok(SuccessorOutcome::Replayed { .. }) => self.report.covered += 1,
            Ok(SuccessorOutcome::Blocked(reason)) => {
                self.report.blocked.push(BlockedGap { key, reason })
            }
            // The default moved on: the run stops instead of committing an old
            // target against a new default.
            Err(SuccessorError::DefaultChanged { .. }) => {
                return self.close_run(PolicyBulkStatus::Superseded);
            }
            // A head that moved, an id another mutation took, an intent a
            // concurrent pass owns, and a lost commit race are all replanned by
            // the next pass.
            Err(
                SuccessorError::HeadConflict { .. }
                | SuccessorError::VersionCollision(_)
                | SuccessorError::IntentConflict
                | SuccessorError::Storage(StorageError::TransactionConflict),
            ) => self.report.replanned += 1,
            // A head that lost its version or became a delete marker is retained
            // as a blocked gap: the pass must not claim it as completed.
            Err(SuccessorError::HeadDeleted | SuccessorError::VersionMissing) => {
                self.report.blocked.push(BlockedGap {
                    key,
                    reason: PolicyBlockedReason::SourceUnavailable,
                })
            }
            Err(error) => return self.fail(error.into()),
        }
        self.mint_next()
    }

    /// Converged only when a full rescan from the start found nothing to do.
    fn after_page(&mut self) -> Effects {
        let converged = self.config.start_after.is_none()
            && self.report.complete
            && self.report.minted == 0
            && self.report.replanned == 0
            && self.report.blocked.is_empty();
        if !converged {
            return self.finish();
        }
        self.close_run(PolicyBulkStatus::Completed)
    }

    /// Status transitions are compare-and-set: only an active run moves on, so
    /// a replayed or concurrent pass cannot revive or downgrade a finished one.
    fn close_run(&mut self, status: PolicyBulkStatus) -> Effects {
        self.pending_status = Some(status);
        self.state = BulkState::StartStatus;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn read_status(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = BulkState::ReadStatus;
        let key = match PolicyBulkRun::key(self.config.operation_id) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: POLICY_BULK_RUN_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_status(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(BulkError::InvalidEvent);
        };
        let stored = match value
            .map(|value| PolicyBulkRun::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(stored) => stored,
            Err(error) => return self.fail(error.into()),
        };
        let (Some(mut run), Some(status)) = (stored, self.pending_status) else {
            return self.stop();
        };
        if run.status != PolicyBulkStatus::Active {
            self.run = Some(run);
            return self.stop();
        }
        run.status = status;
        let (key, value) = match (PolicyBulkRun::key(run.operation_id), run.to_bytes()) {
            (Ok(key), Ok(value)) => (key, value),
            (Err(error), _) | (_, Err(error)) => return self.fail(error.into()),
        };
        self.run = Some(run);
        self.state = BulkState::WriteStatus;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: POLICY_BULK_RUN_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }
}

fn covered(version: &BlobVersion, target: &[PlacementPolicyRef]) -> bool {
    target
        .iter()
        .all(|policy| version.placement_policies.contains(policy))
}

impl Operation for PolicyBulkOperation {
    type Output = BulkReport;
    type Error = BulkError;

    fn start(&mut self) -> Effects {
        self.state = BulkState::Authorize;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path: policy_admin_path(self.config.auth_context.realm_id),
            required_permission: Permission::WRITE,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(auth_config),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        // The mint and the resolver classify their own storage failures.
        let event = match (self.state, event) {
            (
                BulkState::Mint | BulkState::Settle | BulkState::Resolve | BulkState::ResolveUnion,
                event,
            ) => event,
            (_, Event::Storage(StorageEvent::Error { error })) => return self.fail(error.into()),
            (_, event) => event,
        };
        match self.state {
            BulkState::Init => self.start(),
            BulkState::Authorize => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.fail(BulkError::InvalidEvent);
                };
                match allowed {
                    Ok(true) => {
                        self.state = BulkState::StartSeal;
                        smallvec![Effect::Storage(StorageEffect::StartTransaction {
                            read: false
                        })]
                    }
                    Ok(false) => self.fail(BulkError::Unauthorized),
                    Err(error) => {
                        warn!(error = %error, "Bulk policy authorization check failed");
                        self.fail(BulkError::Unauthorized)
                    }
                }
            }
            BulkState::StartSeal => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(BulkError::InvalidEvent);
                };
                match self.read_seal(txn_id) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            BulkState::ReadSeal => self.handle_seal(event),
            BulkState::WriteRun => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(BulkError::InvalidEvent);
                };
                match self.txn_id {
                    Some(txn_id) => {
                        self.state = BulkState::CommitSeal;
                        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                    }
                    None => self.fail(BulkError::InvalidEvent),
                }
            }
            BulkState::CommitSeal => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(BulkError::InvalidEvent);
                };
                self.after_seal()
            }
            BulkState::Resolve => {
                let Some(resolver) = self.resolver.as_mut() else {
                    return self.fail(BulkError::InvalidEvent);
                };
                let step = resolver.step(event);
                self.after_resolve(step)
            }
            BulkState::ScanHeads => self.handle_heads(event),
            BulkState::ReadVersions => self.handle_versions(event),
            BulkState::ResolveUnion => {
                let Some(resolver) = self.resolver.as_mut() else {
                    return self.fail(BulkError::InvalidEvent);
                };
                let step = resolver.step(event);
                self.after_union(step)
            }
            BulkState::ReadIntents => self.handle_intents(event),
            BulkState::Mint => {
                let Some(mint) = self.mint.as_mut() else {
                    return self.fail(BulkError::InvalidEvent);
                };
                let effects = mint.step(event);
                if !mint.is_complete() {
                    return effects;
                }
                let Some(mint) = self.mint.take() else {
                    return self.fail(BulkError::InvalidEvent);
                };
                let outcome = mint.finalize();
                if effects.is_empty() {
                    return self.record_outcome(outcome);
                }
                self.settled = Some(outcome);
                self.state = BulkState::Settle;
                effects
            }
            // The cleanup event belongs to the finished mint, so it decides
            // nothing here.
            BulkState::Settle => match self.settled.take() {
                Some(outcome) => self.record_outcome(outcome),
                None => self.fail(BulkError::InvalidEvent),
            },
            BulkState::StartStatus => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(BulkError::InvalidEvent);
                };
                self.read_status(txn_id)
            }
            BulkState::ReadStatus => self.handle_status(event),
            BulkState::WriteStatus => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(BulkError::InvalidEvent);
                };
                match self.txn_id {
                    Some(txn_id) => {
                        self.state = BulkState::CommitStatus;
                        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                    }
                    None => self.fail(BulkError::InvalidEvent),
                }
            }
            BulkState::CommitStatus => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(BulkError::InvalidEvent);
                };
                self.txn_id = None;
                self.finish()
            }
            BulkState::Finish | BulkState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, BulkState::Finish | BulkState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(BulkError::InvalidEvent))
    }

    fn abort(&mut self) -> Effects {
        let mut effects: Effects = match self.resolver.as_mut() {
            Some(resolver) => resolver.abort(),
            None => smallvec![],
        };
        if let Some(mint) = self.mint.as_mut() {
            effects.extend(mint.abort());
        }
        if let Some(txn_id) = self.txn_id.take() {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        effects
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            BulkError::Unauthorized | BulkError::NoSuchBucket | BulkError::BucketChanged
        )
    }
}

/// Exit gate for step 6: a default applies to current heads by minting
/// successors, never by rewriting a stored version, and a run that cannot act
/// keeps the object as a resumable gap.
#[cfg(test)]
mod tests {
    use super::{BULK_PAGE_LIMIT, BulkConfig, BulkError, PolicyBulkOperation};
    use crate::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive, gate_context};
    use crate::placement_policy::cache::cache_key;
    use crate::placement_policy::fixtures::{seed_gate, subject};
    use crate::s3::bucket_placement::{PutBucketPlacementInput, PutBucketPlacementOperation};
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE,
        PLACEMENT_POLICY_CACHE_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        Actor, AuthContext, Backend, BackendConfig, BackendRef, BlobHeadKey, BlobVersion,
        BucketInfo, CurrentVersionPointer, ManagedCopyKey, ManagedCopyRecord,
        POLICY_BULK_INTENT_KEYSPACE, PlacementPolicy, PlacementPolicyRef, PlacementSelector,
        PolicyBlockedReason, PolicyBulkIntent, PolicyBulkIntentKey, PolicyBulkStatus,
        PolicyIntentOutcome, RealmId, RoutingSnapshot, VerifiedPolicy, VersionKey,
    };
    use aruna_core::types::{GroupId, Key, NodeId, UserId};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    const BUCKET: &str = "governed";
    const OBJECT: &str = "object.txt";
    const BODY: &[u8] = b"payload";

    struct Fixture {
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        user_id: UserId,
    }

    /// A realm with a claimed administrator, an advertised subject and the rule
    /// these tests attach already resolved, which is what production wiring
    /// establishes before a governed write is possible.
    async fn full_context() -> (TempDir, DriverContext, Fixture) {
        let temp_handle = tempdir().expect("temp dir");
        let temp_root = temp_handle.path().to_str().expect("utf8 path");
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).expect("blob root");
        let storage_handle = storage::FjallStorage::open(temp_root).expect("storage opens");
        let secret = iroh::SecretKey::from_bytes(&[3u8; 32]);
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                secret_key: Some(secret.clone()),
                realm_id,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .expect("net handle");
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100_000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .expect("blob handle");
        let fixture = Fixture {
            realm_id,
            group_id: Ulid::generate(),
            node_id: net_handle.node_id(),
            user_id: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
        };
        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let actor = Actor {
            node_id: fixture.node_id,
            user_id: fixture.user_id,
            realm_id,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "policy realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &context,
        )
        .await
        .expect("realm is created");
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput { actor }),
            &context,
        )
        .await
        .expect("admin is claimed");
        seed_gate(
            &context,
            realm_id,
            subject(fixture.node_id, "eu-west"),
            &[policy(fixture.node_id)],
        )
        .await;
        (temp_handle, context, fixture)
    }

    async fn write_bucket(context: &DriverContext, fixture: &Fixture) {
        let bucket = BucketInfo {
            group_id: fixture.group_id,
            created_at: UNIX_EPOCH,
            created_by: fixture.user_id,
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: BUCKET.as_bytes().to_vec().into(),
                value: bucket.to_bytes().expect("bucket encodes").into(),
                txn_id: None,
            })
            .await;
    }

    async fn put_object(context: &DriverContext, fixture: &Fixture, key: &str) -> Ulid {
        let gate = gate_context(context, fixture.realm_id, 1_000)
            .await
            .expect("subject reads");
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: fixture.user_id,
            group_id: fixture.group_id,
            realm_id: fixture.realm_id,
            node_id: fixture.node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                content_length: Some(BODY.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(BODY))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(fixture.group_id),
        });
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }
        drive(operation, context)
            .await
            .expect("put drives")
            .expect("put succeeds")
            .expect("put returns a result")
            .version_id
    }

    fn policy(node_id: NodeId) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([5u8; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: Some(node_id),
                location: None,
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn second_policy(node_id: NodeId) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([6u8; 16]),
            "second".to_string(),
            vec![PlacementSelector {
                node_id: Some(node_id),
                location: None,
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    async fn set_default(
        context: &DriverContext,
        fixture: &Fixture,
        refs: Vec<PlacementPolicyRef>,
    ) {
        drive(
            PutBucketPlacementOperation::new(PutBucketPlacementInput {
                bucket: BUCKET.to_string(),
                group_id: fixture.group_id,
                policies: refs,
                expected_generation: None,
                auth_context: auth(fixture),
                local_node_id: fixture.node_id,
                now_ms: 1_000,
            }),
            context,
        )
        .await
        .expect("default is set");
    }

    fn auth(fixture: &Fixture) -> AuthContext {
        AuthContext {
            user_id: fixture.user_id,
            realm_id: fixture.realm_id,
            path_restrictions: None,
        }
    }

    fn config(fixture: &Fixture, operation_id: Ulid) -> BulkConfig {
        BulkConfig {
            operation_id,
            bucket: BUCKET.to_string(),
            auth_context: auth(fixture),
            subject: subject(fixture.node_id, "eu-west"),
            start_after: None,
            limit: BULK_PAGE_LIMIT,
            now_ms: 1_000,
            created_at: SystemTime::now(),
        }
    }

    async fn read_head(context: &DriverContext, key: &str) -> CurrentVersionPointer {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(BUCKET, key)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        CurrentVersionPointer::from_bytes(value.expect("head exists").as_ref())
            .expect("pointer decodes")
    }

    async fn read_version(context: &DriverContext, key: &str, version_id: Ulid) -> BlobVersion {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(BUCKET, key, version_id)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        BlobVersion::from_bytes(value.expect("version exists").as_ref()).expect("version decodes")
    }

    /// Every stored version of one object, so a pass can be shown to mint at
    /// most one successor however often it runs.
    async fn count_versions(context: &DriverContext, key: &str) -> usize {
        let prefix: Key = VersionKey::object_prefix(BUCKET, key)
            .expect("prefix encodes")
            .into();
        let Event::Storage(StorageEvent::IterResult { values, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                prefix: Some(prefix),
                start: None,
                limit: 64,
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage iter result");
        };
        values.len()
    }

    async fn copy_key(key: &str, version_id: Ulid) -> ManagedCopyKey {
        ManagedCopyKey::new(
            VersionKey::new(BUCKET, key, version_id),
            BackendRef::node_default(),
        )
    }

    async fn read_copy(
        context: &DriverContext,
        key: &str,
        version_id: Ulid,
    ) -> Option<ManagedCopyRecord> {
        let managed_key = copy_key(key, version_id).await;
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: MANAGED_COPY_KEYSPACE.to_string(),
                key: managed_key.to_bytes().expect("key encodes").into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        value.map(|value| ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes"))
    }

    async fn take_copy(context: &DriverContext, key: &str, version_id: Ulid) -> ManagedCopyRecord {
        let record = read_copy(context, key, version_id)
            .await
            .expect("copy row exists");
        let managed_key = copy_key(key, version_id).await;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: MANAGED_COPY_KEYSPACE.to_string(),
                key: managed_key.to_bytes().expect("key encodes").into(),
                txn_id: None,
            })
            .await;
        record
    }

    async fn restore_copy(context: &DriverContext, record: &ManagedCopyRecord) {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: MANAGED_COPY_KEYSPACE.to_string(),
                key: record.key().to_bytes().expect("key encodes").into(),
                value: record.to_bytes().expect("record encodes").into(),
                txn_id: None,
            })
            .await;
    }

    async fn read_intent(
        context: &DriverContext,
        operation_id: Ulid,
        key: &str,
    ) -> Option<PolicyBulkIntent> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: POLICY_BULK_INTENT_KEYSPACE.to_string(),
                key: PolicyBulkIntentKey::new(operation_id, key)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        value.map(|value| PolicyBulkIntent::from_bytes(value.as_ref()).expect("intent decodes"))
    }

    #[tokio::test]
    async fn bulk_mints_successor() {
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let predecessor = put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let operation_id = Ulid::generate();

        let report = drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("pass runs");

        assert_eq!(report.minted, 1);
        assert!(report.blocked.is_empty());
        let head = read_head(&context, OBJECT).await;
        assert_ne!(head.version_id, predecessor);
        let successor = read_version(&context, OBJECT, head.version_id).await;
        assert_eq!(successor.placement_policies, vec![policy.policy_ref()]);
        assert_eq!(
            successor.state,
            read_version(&context, OBJECT, predecessor).await.state
        );
        // The predecessor keeps its own refs until retention retires it.
        assert!(
            read_version(&context, OBJECT, predecessor)
                .await
                .placement_policies
                .is_empty()
        );
        assert!(read_copy(&context, OBJECT, head.version_id).await.is_some());
        assert!(matches!(
            read_intent(&context, operation_id, OBJECT).await,
            Some(PolicyBulkIntent {
                outcome: PolicyIntentOutcome::Completed { .. },
                ..
            })
        ));

        // A second pass observes no gap and converges.
        let second = drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("second pass runs");
        assert_eq!(second.minted, 0);
        assert_eq!(second.covered, 1);
        assert_eq!(second.status, PolicyBulkStatus::Completed);
    }

    #[tokio::test]
    async fn denies_non_admin() {
        // Applying a bucket default is a realm-configuration change.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        put_object(&context, &fixture, OBJECT).await;
        let mut config = config(&fixture, Ulid::generate());
        config.auth_context.user_id = UserId::local(Ulid::from_bytes([9u8; 16]), fixture.realm_id);

        let denied = drive(PolicyBulkOperation::new(config), &context).await;

        assert_eq!(denied, Err(BulkError::Unauthorized));
    }

    /// Drops the durable cache entry, leaving a ref this node can no longer
    /// authenticate because no holder answers in a single-node realm.
    async fn evict_policy(context: &DriverContext, policy: &VerifiedPolicy) {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: PLACEMENT_POLICY_CACHE_KEYSPACE.to_string(),
                key: cache_key(&policy.policy_ref()),
                txn_id: None,
            })
            .await;
    }

    #[tokio::test]
    async fn bulk_blocks_unresolved() {
        // A ref the node can no longer obtain leaves the object a resumable gap
        // instead of granting anything.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let predecessor = put_object(&context, &fixture, OBJECT).await;
        let unknown = second_policy(fixture.node_id);
        seed_gate(
            &context,
            fixture.realm_id,
            subject(fixture.node_id, "eu-west"),
            std::slice::from_ref(&unknown),
        )
        .await;
        set_default(&context, &fixture, vec![unknown.policy_ref()]).await;
        evict_policy(&context, &unknown).await;
        let operation_id = Ulid::generate();

        let report = drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("pass runs");

        assert_eq!(report.minted, 0);
        assert_eq!(
            report.blocked.first().map(|gap| gap.reason),
            Some(PolicyBlockedReason::PolicyUnresolved)
        );
        assert_eq!(read_head(&context, OBJECT).await.version_id, predecessor);
        assert_eq!(report.status, PolicyBulkStatus::Active);
        assert!(matches!(
            read_intent(&context, operation_id, OBJECT).await,
            Some(PolicyBulkIntent {
                outcome: PolicyIntentOutcome::Blocked(PolicyBlockedReason::PolicyUnresolved),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn blocked_copy_resumes() {
        // No compliant local copy blocks the mint; once the ordinary movement
        // path has registered verified bytes again the same run completes it.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let predecessor = put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let operation_id = Ulid::generate();
        let copy = take_copy(&context, OBJECT, predecessor).await;

        let blocked = drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("first pass runs");

        assert_eq!(
            blocked.blocked.first().map(|gap| gap.reason),
            Some(PolicyBlockedReason::SourceUnavailable)
        );
        assert_eq!(read_head(&context, OBJECT).await.version_id, predecessor);
        let intent = read_intent(&context, operation_id, OBJECT)
            .await
            .expect("blocked intent is durable");
        assert_eq!(
            intent.outcome,
            PolicyIntentOutcome::Blocked(PolicyBlockedReason::SourceUnavailable)
        );

        restore_copy(&context, &copy).await;
        let resumed = drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("second pass runs");

        assert_eq!(resumed.minted, 1);
        // The blocked intent kept its successor id, so resuming mints exactly
        // the version the first pass had planned.
        assert_eq!(
            read_head(&context, OBJECT).await.version_id,
            intent.successor_version_id
        );
        assert_eq!(count_versions(&context, OBJECT).await, 2);
    }

    async fn write_intent(context: &DriverContext, intent: &PolicyBulkIntent) {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: POLICY_BULK_INTENT_KEYSPACE.to_string(),
                key: intent.key().to_bytes().expect("key encodes").into(),
                value: intent.to_bytes().expect("intent encodes").into(),
                txn_id: None,
            })
            .await;
    }

    async fn write_version(
        context: &DriverContext,
        key: &str,
        version_id: Ulid,
        version: &BlobVersion,
    ) {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(BUCKET, key, version_id)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                value: version.to_bytes().expect("version encodes").into(),
                txn_id: None,
            })
            .await;
    }

    #[tokio::test]
    async fn conflict_keeps_page() {
        // A collision on the first object must neither overwrite it nor abandon
        // the rest of the page.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let first = put_object(&context, &fixture, "a.txt").await;
        put_object(&context, &fixture, "b.txt").await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let operation_id = Ulid::generate();
        let taken = Ulid::generate();
        write_intent(
            &context,
            &PolicyBulkIntent {
                operation_id,
                key: "a.txt".to_string(),
                observed_head: read_head(&context, "a.txt").await,
                successor_version_id: taken,
                outcome: PolicyIntentOutcome::Planned,
            },
        )
        .await;
        let stored = read_version(&context, "a.txt", first).await;
        write_version(&context, "a.txt", taken, &stored).await;

        let report = drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("pass runs");

        assert_eq!(report.replanned, 1);
        assert_eq!(report.minted, 1);
        assert_eq!(read_head(&context, "a.txt").await.version_id, first);
        let head = read_head(&context, "b.txt").await;
        assert_eq!(
            read_version(&context, "b.txt", head.version_id)
                .await
                .placement_policies,
            vec![policy.policy_ref()]
        );
    }

    #[tokio::test]
    async fn bulk_stops_superseded() {
        // A changed default supersedes the run instead of mixing two policies.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let operation_id = Ulid::generate();
        drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("first pass runs");

        set_default(&context, &fixture, Vec::new()).await;
        let report = drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("second pass runs");

        assert_eq!(report.status, PolicyBulkStatus::Superseded);
        assert_eq!(report.observed, 0);
    }

    #[tokio::test]
    async fn bulk_reuses_successor() {
        // A lost response must reuse the assigned VersionId, not mint another.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let operation_id = Ulid::generate();
        drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("first pass runs");
        let successor = read_head(&context, OBJECT).await;

        let report = drive(
            PolicyBulkOperation::new(config(&fixture, operation_id)),
            &context,
        )
        .await
        .expect("second pass runs");

        assert_eq!(report.minted, 0);
        assert_eq!(read_head(&context, OBJECT).await, successor);
        assert_eq!(count_versions(&context, OBJECT).await, 2);
    }

    #[tokio::test]
    async fn concurrent_passes_agree() {
        // Two runs applying the same default may race, but only one successor
        // can exist: the loser sees a changed head and replans.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;

        let (first, second) = tokio::join!(
            drive(
                PolicyBulkOperation::new(config(&fixture, Ulid::generate())),
                &context
            ),
            drive(
                PolicyBulkOperation::new(config(&fixture, Ulid::generate())),
                &context
            ),
        );

        let minted =
            first.expect("first pass runs").minted + second.expect("second pass runs").minted;
        assert!(minted <= 1, "at most one successor may be minted");
        assert_eq!(count_versions(&context, OBJECT).await, 1 + minted);
        let head = read_head(&context, OBJECT).await;
        if minted == 1 {
            assert_eq!(
                read_version(&context, OBJECT, head.version_id)
                    .await
                    .placement_policies,
                vec![policy.policy_ref()]
            );
        }
    }

    #[tokio::test]
    async fn bulk_unions_existing() {
        // Applying a default may only add refs to the head it re-reads.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let first = policy(fixture.node_id);
        set_default(&context, &fixture, vec![first.policy_ref()]).await;
        put_object(&context, &fixture, OBJECT).await;

        let second = second_policy(fixture.node_id);
        seed_gate(
            &context,
            fixture.realm_id,
            subject(fixture.node_id, "eu-west"),
            std::slice::from_ref(&second),
        )
        .await;
        set_default(&context, &fixture, vec![second.policy_ref()]).await;

        let report = drive(
            PolicyBulkOperation::new(config(&fixture, Ulid::generate())),
            &context,
        )
        .await
        .expect("pass runs");

        assert_eq!(report.minted, 1);
        let head = read_head(&context, OBJECT).await;
        let successor = read_version(&context, OBJECT, head.version_id).await;
        assert!(successor.placement_policies.contains(&first.policy_ref()));
        assert!(successor.placement_policies.contains(&second.policy_ref()));
    }

    #[tokio::test]
    async fn bulk_skips_covered() {
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let version_id = put_object(&context, &fixture, OBJECT).await;

        let report = drive(
            PolicyBulkOperation::new(config(&fixture, Ulid::generate())),
            &context,
        )
        .await
        .expect("pass runs");

        assert_eq!(report.covered, 1);
        assert_eq!(report.minted, 0);
        assert_eq!(read_head(&context, OBJECT).await.version_id, version_id);
        assert_eq!(report.status, PolicyBulkStatus::Completed);
    }

    #[tokio::test]
    async fn successor_is_fresh() {
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let predecessor = put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;

        drive(
            PolicyBulkOperation::new(config(&fixture, Ulid::generate())),
            &context,
        )
        .await
        .expect("pass runs");

        let head = read_head(&context, OBJECT).await;
        let successor = read_version(&context, OBJECT, head.version_id).await;
        let before = read_version(&context, OBJECT, predecessor).await;
        assert!(successor.created_at >= before.created_at);
        assert!(successor.created_at <= SystemTime::now());
    }
}
