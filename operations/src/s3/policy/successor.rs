//! Mints a successor version that carries new policy refs and advances the head pointer.
//! It never rewrites a stored version; a retry with the same mutation id returns that version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::records::HeadAliasContext;
use crate::connectors::repository::connector_reference_write;
use crate::distribution::guard::{GuardError, authority_reads, write_epoch};
use crate::distribution::inventory::{
    AddedVersion, entry_of, stamp_head, stamp_version, version_written,
};
use crate::placement::policy::{PolicyGateError, split_drift_reads};
use crate::s3::object::put::abe::keyed;
use crate::s3::purge_fence::{PurgeFenceError, check_fence_row, fence_key};
use crate::s3::usage::{
    QuotaGate, QuotaGateError, RowChange, UsageCounterUpdate, UsageUpdateError,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE,
    PATHS_INDEX_KEYSPACE, PURGE_FENCE_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::managed_copy::{
    COPY_PAGE_LIMIT, CopyRegistration, CopyRequest, ManagedCopyError, register_entry,
    validate_registration, version_scope,
};
use aruna_core::operation::{Operation, SubOperation, TransactionMode, boxed_suboperation};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::node_subject::NODE_SUBJECT_KEY;
use aruna_core::structs::placement::policy::attachment::{
    BULK_INTENT_KEYSPACE, POLICY_MUTATION_KEYSPACE, PolicyBlockedReason, PolicyIntent,
    PolicyIntentOutcome, PolicyMutationParams, PolicyMutationRecord, PolicyRefMode,
};
use aruna_core::structs::placement::policy::{
    PlacementDecision, PlacementPolicyError, PlacementPolicyRef, PlacementSubject,
    PolicyResolution, evaluate_placement,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobVersion, BlobVersionState, BucketIdentity, CurrentVersionPointer,
    ManagedCopyKey, ManagedCopyRecord, VersionKey,
};
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_core::structs::storage::distribution::hint::SetHint;
use aruna_core::structs::storage::replication_queue::{
    LiveObligationRecord, live_obligation_entry,
};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::BTreeMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

/// The bucket default a bulk run captured. A mint that observes another default
/// in its own transaction supersedes instead of committing an old target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedDefault {
    pub generation: u64,
    pub refs: Vec<PlacementPolicyRef>,
}

/// Everything one successor mint is authorized by. Policy resolution is I/O, so
/// the caller supplies the resolved documents and this node's subject; the
/// decision itself is taken here and never delegated.
#[derive(Clone, Debug, PartialEq)]
pub struct SuccessorPlan {
    pub context: HeadAliasContext,
    pub mutation_id: Ulid,
    pub expected_head: CurrentVersionPointer,
    pub bucket_identity: BucketIdentity,
    pub target_refs: Vec<PlacementPolicyRef>,
    pub mode: PolicyRefMode,
    /// Assigned before the mint, so a replay or resume reuses one VersionId.
    pub successor_version_id: Ulid,
    pub created_at: SystemTime,
    pub auth_context: AuthContext,
    pub subject: PlacementSubject,
    pub resolved: BTreeMap<Ulid, PolicyResolution>,
    /// A bulk run's receipt for this object, committed in the same batch as the
    /// successor or the blocked reason it records.
    pub intent: Option<PolicyIntent>,
    /// Set by a bulk run, so the pass cannot commit against a default that
    /// moved on between the run's capture and this transaction.
    pub captured_default: Option<CapturedDefault>,
}

impl SuccessorPlan {
    fn params(&self) -> PolicyMutationParams {
        PolicyMutationParams {
            bucket: self.context.bucket.clone(),
            key: self.context.key.clone(),
            expected_head: self.expected_head.clone(),
            bucket_identity: self.bucket_identity,
            target_refs: self.target_refs.clone(),
            mode: self.mode,
        }
    }

    fn version_key(&self, version_id: Ulid) -> VersionKey {
        VersionKey::new(
            self.context.bucket.clone(),
            self.context.key.clone(),
            version_id,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuccessorOutcome {
    Minted {
        version_id: Ulid,
        refs: Vec<PlacementPolicyRef>,
        materialized: bool,
    },
    /// The recorded result of an earlier identical mutation.
    Replayed {
        version_id: Ulid,
        refs: Vec<PlacementPolicyRef>,
        materialized: bool,
    },
    /// Nothing was written; the caller may retry once the reason clears.
    Blocked(PolicyBlockedReason),
}

impl SuccessorOutcome {
    /// Refs the successor carries; empty for a blocked outcome, which wrote none.
    pub fn refs(&self) -> Vec<PlacementPolicyRef> {
        match self {
            Self::Minted { refs, .. } | Self::Replayed { refs, .. } => refs.clone(),
            Self::Blocked(_) => Vec::new(),
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum SuccessorError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Gate(#[from] PolicyGateError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
    #[error(transparent)]
    ManagedCopy(#[from] ManagedCopyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Purge(#[from] PurgeFenceError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    QuotaGate(#[from] QuotaGateError),
    /// The successor books its own bytes, so a mint that would push the owning
    /// group past its hard ceiling is refused before the counters commit.
    #[error("group quota ceiling {limit} exceeded by {usage} bytes")]
    QuotaExceeded { limit: u64, usage: u64 },
    #[error("mutation {0} is recorded with different parameters")]
    MutationConflict(Ulid),
    #[error("the object head is no longer the expected version")]
    HeadConflict {
        current: Option<CurrentVersionPointer>,
    },
    #[error("the bucket record is missing or changed identity")]
    BucketChanged,
    /// This node's subject moved between the plan and this transaction, so the
    /// evaluation the plan carries no longer describes the destination.
    #[error("the destination subject changed during this mutation")]
    SubjectDrift,
    /// The bucket default moved on, so this run's target is stale.
    #[error("the bucket default changed to generation {current}")]
    DefaultChanged { current: u64 },
    #[error("the head version record is missing")]
    VersionMissing,
    /// Another mutation already stored a version under the assigned id; the
    /// successor is never written over it.
    #[error("version {0} already exists with different bytes")]
    VersionCollision(Ulid),
    /// A concurrent pass owns this object's intent, so this one may not write.
    #[error("another pass owns the intent for this object")]
    IntentConflict,
    #[error("a delete marker carries no bytes to govern")]
    HeadDeleted,
    #[error("unexpected event during successor mint")]
    InvalidEvent,
}

impl From<IoError> for SuccessorError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MintState {
    ReadMutation,
    ReadIntent,
    ReadHead,
    ReadBucket,
    /// Reads a distributed bucket's record: only the key's coordinator writes the successor.
    ReadRecord,
    ReadVersion,
    ReadSuccessor,
    ScanCopies,
    WriteRecords,
    ReadQuota,
    EnforceQuota,
    UpdateUsage,
    Done,
}

/// The next call of a mint, or `None` once its outcome is decided.
type Step = Result<Option<IoCall>, SuccessorError>;

/// Embedded state machine so the single-object mutation and a bulk pass share
/// one implementation and one commit boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct SuccessorMint {
    plan: SuccessorPlan,
    state: MintState,
    predecessor: Option<BlobVersion>,
    effective_refs: Vec<PlacementPolicyRef>,
    outcome: Option<SuccessorOutcome>,
    /// Owner read from the bucket record inside this transaction.
    group_id: Option<GroupId>,
    /// The bucket's distribution binding, whose inventory the mint maintains.
    binding: Option<DistributionBinding>,
    /// The coordinator epoch the successor carries in a distributed bucket.
    epoch: aruna_core::structs::storage::distribution::OriginalEpoch,
    /// The successor's counter change, committed with its records.
    usage: Option<UsageCounterUpdate>,
    /// New logical bytes the successor books; only these need the quota gate.
    quota_bytes: u64,
    quota_gate: Option<QuotaGate>,
}

impl SuccessorMint {
    pub fn new(plan: SuccessorPlan) -> Self {
        Self {
            plan,
            state: MintState::ReadMutation,
            predecessor: None,
            effective_refs: Vec::new(),
            outcome: None,
            group_id: None,
            binding: None,
            epoch: Default::default(),
            usage: None,
            quota_bytes: 0,
            quota_gate: None,
        }
    }

    pub fn outcome(&self) -> Option<&SuccessorOutcome> {
        self.outcome.as_ref()
    }

    /// The first call, in the caller's transaction like every later one.
    pub fn start_calls(&mut self) -> Result<IoCall, SuccessorError> {
        self.state = MintState::ReadMutation;
        let key = PolicyMutationRecord::key(self.plan.mutation_id)?;
        Ok(read_row(POLICY_MUTATION_KEYSPACE, key))
    }

    /// The next call after `result`, or `None` once the outcome is decided; the caller
    /// then commits.
    pub fn step_calls(&mut self, result: IoResult) -> Step {
        match self.state {
            MintState::ReadMutation => self.handle_mutation(result.value()?),
            MintState::ReadIntent => self.handle_intent(result.value()?),
            MintState::ReadHead => self.handle_head(result.value()?),
            MintState::ReadBucket => self.handle_bucket(keyed(&result.values()?)),
            MintState::ReadRecord => self.handle_record(result.values()?),
            MintState::ReadVersion => self.handle_version(result.value()?),
            MintState::ReadSuccessor => self.handle_successor(result.value()?),
            MintState::ScanCopies => self.handle_copies(result.page()?.0),
            MintState::WriteRecords => {
                result.done()?;
                self.start_quota()
            }
            MintState::ReadQuota => self.handle_quota_config(result.value()?),
            MintState::EnforceQuota => self.handle_quota(result),
            MintState::UpdateUsage => self.handle_usage(result),
            MintState::Done => Err(SuccessorError::InvalidEvent),
        }
    }

    fn read(&mut self, state: MintState, space: &str, key: impl Into<Key>) -> Step {
        self.state = state;
        Ok(Some(read_row(space, key)))
    }

    fn handle_mutation(&mut self, value: Option<Value>) -> Step {
        if let Some(value) = value {
            let record = PolicyMutationRecord::from_bytes(value.as_ref())?;
            if record.params != self.plan.params() {
                return Err(SuccessorError::MutationConflict(self.plan.mutation_id));
            }
            self.outcome = Some(SuccessorOutcome::Replayed {
                version_id: record.successor_version_id,
                refs: record.effective_refs,
                materialized: record.materialized,
            });
            self.state = MintState::Done;
            return Ok(None);
        }
        let Some(intent) = self.plan.intent.as_ref() else {
            return self.read_head();
        };
        let key = intent.key().to_bytes()?;
        self.read(MintState::ReadIntent, BULK_INTENT_KEYSPACE, key)
    }

    /// Completed intent evidence never regresses: an intent another pass owns
    /// stops this one instead of overwriting its outcome.
    fn handle_intent(&mut self, value: Option<Value>) -> Step {
        let Some(value) = value else {
            return self.read_head();
        };
        let stored = PolicyIntent::from_bytes(value.as_ref())?;
        // A stale intent from an older head is replanned by this pass; only
        // completed evidence and a concurrent pass on the same head stop it.
        let same_head = stored.observed_head == self.plan.expected_head;
        if matches!(stored.outcome, PolicyIntentOutcome::Completed { .. })
            || (same_head && stored.successor_version_id != self.plan.successor_version_id)
        {
            return Err(SuccessorError::IntentConflict);
        }
        self.read_head()
    }

    fn read_head(&mut self) -> Step {
        let key = self.plan.context.head_key().to_bytes()?;
        self.read(MintState::ReadHead, BLOB_HEAD_KEYSPACE, key)
    }

    fn handle_head(&mut self, value: Option<Value>) -> Step {
        let Some(value) = value else {
            return Err(SuccessorError::HeadConflict { current: None });
        };
        let current = CurrentVersionPointer::from_bytes(value.as_ref())?;
        // Exact pointer, not just the version id: a concurrent write that landed
        // and was rolled back must not look like the observed head.
        if current != self.plan.expected_head {
            return Err(SuccessorError::HeadConflict {
                current: Some(current),
            });
        }
        self.state = MintState::ReadBucket;
        // The destination bucket and the local subject, so the commit re-checks both.
        let bucket = self.plan.context.bucket.as_bytes();
        Ok(Some(IoCall::Read(vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.into()),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
        ])))
    }

    /// Re-reads the destination details inside the commit boundary: the bucket
    /// the run captured, and the subject the plan was evaluated against.
    fn handle_bucket(&mut self, values: Vec<(Key, Option<Value>)>) -> Step {
        let (bucket, subject) = split_drift_reads(values)?;
        let Some(bucket) = bucket else {
            return Err(SuccessorError::BucketChanged);
        };
        if bucket.identity() != self.plan.bucket_identity {
            return Err(SuccessorError::BucketChanged);
        }
        // A successor in a distributed bucket must carry the set hint it was planned with.
        if let Some(binding) = bucket.distribution.as_ref()
            && SetHint::of_version(self.plan.successor_version_id) != binding.hint()
        {
            return Err(SuccessorError::BucketChanged);
        }
        self.group_id = Some(bucket.group_id);
        self.binding = bucket.distribution.clone();
        let admitted = subject.is_some_and(|record| {
            record.subject.generation == self.plan.subject.generation
                && !record.serving_blocked
                && !record.policy_draining
        });
        if !admitted {
            return Err(SuccessorError::SubjectDrift);
        }
        if let Some(captured) = self.plan.captured_default.as_ref()
            && (captured.generation != bucket.placement_policy_generation
                || captured.refs != bucket.placement_policies)
        {
            return Err(SuccessorError::DefaultChanged {
                current: bucket.placement_policy_generation,
            });
        }
        if let Some(binding) = self.binding.as_ref() {
            // Read inside the commit boundary, so a fence or record set meanwhile conflicts.
            self.state = MintState::ReadRecord;
            return Ok(Some(IoCall::Read(authority_reads(binding.set_id))));
        }
        self.read_version()
    }

    fn handle_record(&mut self, values: Vec<Option<Value>>) -> Step {
        let binding = self.binding.as_ref().ok_or(SuccessorError::InvalidEvent)?;
        let [record, fence] =
            <[_; 2]>::try_from(values).map_err(|_| SuccessorError::InvalidEvent)?;
        let key = &self.plan.context.key;
        self.epoch = write_epoch(binding, key, record.as_deref(), fence.as_deref())?;
        self.read_version()
    }

    fn read_version(&mut self) -> Step {
        let key = self.plan.version_key(self.plan.expected_head.version_id);
        self.read(
            MintState::ReadVersion,
            BLOB_VERSIONS_KEYSPACE,
            key.to_bytes()?,
        )
    }

    fn handle_version(&mut self, value: Option<Value>) -> Step {
        let Some(value) = value else {
            return Err(SuccessorError::VersionMissing);
        };
        let predecessor = BlobVersion::from_bytes(value.as_ref())?;
        self.effective_refs = match self.plan.mode {
            PolicyRefMode::Replace => PlacementPolicyRef::canonical_set(&self.plan.target_refs)?,
            PolicyRefMode::Union => {
                let mut refs = predecessor.placement_policies.clone();
                refs.extend(self.plan.target_refs.iter().copied());
                PlacementPolicyRef::canonical_set(&refs)?
            }
        };
        if predecessor.state == BlobVersionState::Deleted {
            return Err(SuccessorError::HeadDeleted);
        }
        if let Some(reason) = self.blocked_placement() {
            return self.blocked(reason);
        }
        self.predecessor = Some(predecessor);
        let key = self.plan.version_key(self.plan.successor_version_id);
        self.read(
            MintState::ReadSuccessor,
            BLOB_VERSIONS_KEYSPACE,
            key.to_bytes()?,
        )
    }

    /// The assigned VersionId must be free: an existing version under it
    /// belongs to another mutation and is never overwritten.
    fn handle_successor(&mut self, value: Option<Value>) -> Step {
        if value.is_some() {
            return Err(SuccessorError::VersionCollision(
                self.plan.successor_version_id,
            ));
        }
        let Some(predecessor) = self.predecessor.as_ref() else {
            return Err(SuccessorError::VersionMissing);
        };
        // A reference claims no Aruna-managed copy, so it needs no local
        // destination and registers nothing.
        if matches!(predecessor.state, BlobVersionState::Reference { .. }) {
            return self.write_records(None);
        }
        let scope = version_scope(&self.plan.version_key(self.plan.expected_head.version_id))?;
        self.state = MintState::ScanCopies;
        Ok(Some(IoCall::Page {
            space: MANAGED_COPY_KEYSPACE.to_string(),
            prefix: scope,
            after: None,
            limit: COPY_PAGE_LIMIT,
        }))
    }

    /// The successor's refs must admit this node before any byte is claimed for
    /// it. An unresolvable or denying policy blocks instead of granting.
    fn blocked_placement(&self) -> Option<PolicyBlockedReason> {
        match evaluate_placement(
            &self.effective_refs,
            &self.plan.resolved,
            &self.plan.subject,
        ) {
            PlacementDecision::Allowed => None,
            PlacementDecision::Denied { .. } => Some(PolicyBlockedReason::DestinationDenied),
            PlacementDecision::Required { .. }
            | PlacementDecision::Unavailable { .. }
            | PlacementDecision::DigestMismatch { .. }
            | PlacementDecision::Invalid { .. }
            | PlacementDecision::InvalidInput { .. } => Some(PolicyBlockedReason::PolicyUnresolved),
        }
    }

    /// Any local registration of the predecessor's exact bytes may back the
    /// successor. Without one the object stays a resumable gap: the ordinary
    /// movement path must stage and register compliant bytes first.
    fn handle_copies(&mut self, rows: Vec<(Key, Value)>) -> Step {
        let Some(predecessor) = self.predecessor.as_ref() else {
            return Err(SuccessorError::VersionMissing);
        };
        let Some(hash) = predecessor.blob_hash().copied() else {
            return Err(SuccessorError::VersionMissing);
        };
        let mut entries = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            entries.push((
                ManagedCopyKey::from_bytes(key.as_ref())?,
                ManagedCopyRecord::from_bytes(value.as_ref())?,
            ));
        }
        let version = self.plan.version_key(self.plan.expected_head.version_id);
        let refs = predecessor.placement_policies.clone();
        let reusable = entries.into_iter().find_map(|(key, record)| {
            reusable_copy(&key, &record, &version, &self.plan.subject, hash, &refs)
        });
        match reusable {
            Some(record) => self.write_records(Some(record)),
            None => self.blocked(PolicyBlockedReason::SourceUnavailable),
        }
    }

    /// A blocked object keeps its reason durable inside a run, so a later pass
    /// resumes it instead of rescanning from nothing.
    fn blocked(&mut self, reason: PolicyBlockedReason) -> Step {
        self.outcome = Some(SuccessorOutcome::Blocked(reason));
        let Some(intent) = self.plan.intent.as_ref() else {
            self.state = MintState::Done;
            return Ok(None);
        };
        let mut record = intent.clone();
        record.outcome = PolicyIntentOutcome::Blocked(reason);
        let writes = vec![(
            BULK_INTENT_KEYSPACE.to_string(),
            Key::from(record.key().to_bytes()?),
            Value::from(record.to_bytes()?),
        )];
        self.state = MintState::WriteRecords;
        Ok(Some(IoCall::Write(writes)))
    }

    /// One batch, so the successor, the advanced head, the idempotency record
    /// and the local registration become visible together or not at all.
    fn write_records(&mut self, source: Option<ManagedCopyRecord>) -> Step {
        let location = source.as_ref().map(|record| record.location.clone());
        let Some(predecessor) = self.predecessor.as_ref() else {
            return Err(SuccessorError::VersionMissing);
        };
        let version_id = self.plan.successor_version_id;
        let successor = successor_version(
            predecessor,
            self.plan.created_at,
            self.plan.auth_context.user_id,
        )
        .with_policies(self.effective_refs.clone())?;
        let next = CurrentVersionPointer::next_for(Some(&self.plan.expected_head), version_id)?;
        let next = stamp_head(next, self.binding.as_ref(), self.epoch);
        let successor = stamp_version(
            successor,
            self.binding.as_ref(),
            next.generation,
            self.epoch,
        );
        let mut writes: Vec<(String, Key, Value)> = Vec::with_capacity(7);
        let version_key = self.plan.version_key(version_id);
        writes.extend(connector_reference_write(&successor, &version_key)?);
        writes.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes()?.into(),
            successor.to_bytes()?.into(),
        ));
        // The expected pointer makes this transaction advance exactly one generation.
        writes.push((
            BLOB_HEAD_KEYSPACE.to_string(),
            self.plan.context.head_key().to_bytes()?.into(),
            next.to_bytes()?.into(),
        ));
        writes.push((
            POLICY_MUTATION_KEYSPACE.to_string(),
            PolicyMutationRecord::key(self.plan.mutation_id)?.into(),
            PolicyMutationRecord {
                mutation_id: self.plan.mutation_id,
                params: self.plan.params(),
                successor_version_id: version_id,
                effective_refs: self.effective_refs.clone(),
                materialized: location.is_some(),
            }
            .to_bytes()?
            .into(),
        ));
        if let Some(location) = location.as_ref() {
            let hash = successor
                .blob_hash()
                .copied()
                .ok_or(SuccessorError::VersionMissing)?;
            writes.push((
                PATHS_INDEX_KEYSPACE.to_string(),
                self.plan
                    .context
                    .path_index_key(hash, version_id)
                    .to_bytes()?
                    .into(),
                Vec::new().into(),
            ));
            // The successor is the same bytes under a new version, so it keeps
            // the provenance the copy it reuses already recorded.
            writes.push(register_entry(CopyRegistration {
                version: self.plan.version_key(version_id),
                node_id: self.plan.subject.node_id,
                location,
                policies: &self.effective_refs,
                origin: source
                    .as_ref()
                    .map(|record| record.origin)
                    .unwrap_or_default(),
                subject_generation: self.plan.subject.generation,
                registered_at_ms: version_id.timestamp_ms(),
            })?);
        }
        // The successor replicates like any other write, so peers converge on
        // the governed version instead of only this node holding it.
        writes.push(live_obligation_entry(&LiveObligationRecord::new(
            self.plan.subject.node_id,
            self.plan.auth_context.clone(),
            self.plan.context.bucket.clone(),
            self.plan.context.key.clone(),
            version_id,
            false,
        ))?);
        if let Some(intent) = self.plan.intent.as_ref() {
            let mut receipt = intent.clone();
            receipt.outcome = PolicyIntentOutcome::Completed {
                version_id,
                materialized: location.is_some(),
            };
            writes.push((
                BULK_INTENT_KEYSPACE.to_string(),
                receipt.key().to_bytes()?.into(),
                receipt.to_bytes()?.into(),
            ));
        }
        let Some(group_id) = self.group_id else {
            return Err(SuccessorError::BucketChanged);
        };
        let mut delta = successor_usage(&successor, location.as_ref());
        let size = u64::try_from(delta.logical_bytes + delta.referenced_bytes).unwrap_or_default();
        // An offered directory observation is charged only while it is the head.
        let released = predecessor.offered_size(&self.plan.context.bucket, &self.plan.context.key);
        delta.referenced_bytes -= i128::from(released.unwrap_or(0));
        self.quota_bytes = u64::try_from(delta.logical_bytes).unwrap_or_default();
        let added = AddedVersion {
            version_id,
            epoch: successor.original_epoch,
            generation: next.generation,
            entry: entry_of(&successor, size),
        };
        let key = &self.plan.context.key;
        let inventory = version_written(self.binding.as_ref(), key, added, true);
        let usage = UsageCounterUpdate::for_group(group_id, delta);
        let usage = usage.in_bucket(&self.plan.context.bucket, RowChange::AddedVersion);
        self.usage = Some(usage.with_inventory(inventory));
        self.outcome = Some(SuccessorOutcome::Minted {
            version_id,
            refs: self.effective_refs.clone(),
            materialized: location.is_some(),
        });
        self.state = MintState::WriteRecords;
        Ok(Some(IoCall::Write(writes)))
    }

    /// A materialized successor books new logical bytes, so the owning group's
    /// hard ceiling is enforced inside the mint's transaction.
    fn start_quota(&mut self) -> Step {
        if self.quota_bytes == 0 {
            return self.start_usage();
        }
        let key = DocumentTarget::RealmConfig {
            realm_id: self.plan.auth_context.realm_id,
        }
        .storage_key();
        self.read(MintState::ReadQuota, REALM_CONFIG_KEYSPACE, key)
    }

    fn handle_quota_config(&mut self, value: Option<Value>) -> Step {
        let Some(group_id) = self.group_id else {
            return Err(SuccessorError::BucketChanged);
        };
        let ceiling = value
            .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
            .transpose()?
            .and_then(|document| document.quota.effective_group_ceiling(&group_id));
        let Some(ceiling) = ceiling else {
            return self.start_usage();
        };
        let mut gate = QuotaGate::new_for_realm(
            ceiling,
            self.quota_bytes,
            group_id,
            self.plan.subject.node_id,
            self.plan.auth_context.realm_id,
        );
        self.state = MintState::EnforceQuota;
        let call = gate.start_calls();
        self.quota_gate = Some(gate);
        Ok(Some(call))
    }

    fn handle_quota(&mut self, result: IoResult) -> Step {
        let Some(gate) = self.quota_gate.as_mut() else {
            return Err(SuccessorError::InvalidEvent);
        };
        match gate.step_calls(result)? {
            Some(call) => Ok(Some(call)),
            None if gate.is_exceeded() => Err(SuccessorError::QuotaExceeded {
                limit: gate.ceiling(),
                usage: gate.projected_usage(),
            }),
            None => self.start_usage(),
        }
    }

    /// The counters move inside the mint's transaction, so a committed
    /// successor can never leave the owning group's usage short.
    fn start_usage(&mut self) -> Step {
        let Some(mut update) = self.usage.take().filter(|update| !update.is_noop()) else {
            self.state = MintState::Done;
            return Ok(None);
        };
        self.state = MintState::UpdateUsage;
        let call = update.start_calls().into_call();
        self.usage = Some(update);
        Ok(Some(call))
    }

    fn handle_usage(&mut self, result: IoResult) -> Step {
        let Some(update) = self.usage.as_mut() else {
            return Err(SuccessorError::InvalidEvent);
        };
        match update.step_calls(result)? {
            Some(call) => Ok(Some(call.into_call())),
            None => {
                self.state = MintState::Done;
                Ok(None)
            }
        }
    }
}

/// The successor is a second stored version of the same bytes: it books its own
/// size, the head only moves between live versions so the object count holds,
/// and nothing new is stored, so no `stored_*` counter and no quota gate apply.
fn successor_usage(successor: &BlobVersion, location: Option<&BackendLocation>) -> UsageDelta {
    match &successor.state {
        BlobVersionState::Reference {
            cached_metadata, ..
        } => UsageDelta {
            referenced_bytes: i128::from(cached_metadata.content_length),
            ..Default::default()
        },
        BlobVersionState::Materialized { .. } | BlobVersionState::PendingContent { .. } => {
            UsageDelta {
                logical_bytes: location.map_or(0, |location| i128::from(location.blob_size)),
                ..Default::default()
            }
        }
        BlobVersionState::Deleted => UsageDelta::default(),
    }
}

/// A row may back the successor only when it is this node's registration of
/// exactly these bytes under the predecessor's own refs. The successor's own
/// compliance is decided by the placement gate, not by this row.
fn reusable_copy(
    key: &ManagedCopyKey,
    record: &ManagedCopyRecord,
    version: &VersionKey,
    subject: &PlacementSubject,
    blob_hash: [u8; 32],
    refs: &[PlacementPolicyRef],
) -> Option<ManagedCopyRecord> {
    if key.version != *version {
        return None;
    }
    let request = CopyRequest {
        key,
        node_id: Some(subject.node_id),
        blake3: Some(blob_hash),
        refs,
        subject_generation: Some(subject.generation),
    };
    let bytes = record.to_bytes().ok()?;
    // The hash binds the content, so a zero-length object is reusable too.
    validate_registration(Some(bytes.as_slice()), &request).ok()
}

/// The successor carries the predecessor's content and metadata under a new
/// VersionId. The predecessor keeps its own refs until retention retires it.
fn successor_version(
    predecessor: &BlobVersion,
    created_at: SystemTime,
    created_by: aruna_core::UserId,
) -> BlobVersion {
    let mut successor = predecessor.clone();
    successor.created_at = created_at;
    successor.created_by = created_by;
    successor.published_by = None;
    successor
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationState {
    CheckPurgeFence,
    Minting,
}

/// The realm-admin mutation: one transaction, one durably assigned successor.
#[derive(Debug, PartialEq)]
pub struct MintSuccessorOperation {
    mint: SuccessorMint,
    state: OperationState,
}

type Turn = Result<Next<SuccessorOutcome>, SuccessorError>;

impl MintSuccessorOperation {
    pub fn new(plan: SuccessorPlan) -> Self {
        Self {
            mint: SuccessorMint::new(plan),
            state: OperationState::CheckPurgeFence,
        }
    }

    fn minted(&mut self, step: Option<IoCall>) -> Turn {
        match step {
            Some(call) => Ok(Next::Call(call)),
            None => {
                let outcome = self.mint.outcome().cloned();
                Ok(Next::Done(outcome.ok_or(SuccessorError::InvalidEvent)?))
            }
        }
    }
}

/// The mint as a child run, for an operation that steps it by hand.
pub(crate) fn mint_child(plan: SuccessorPlan) -> Box<dyn SubOperation> {
    let mint = MintSuccessorOperation::new(plan);
    boxed_suboperation(mint, |result| Event::Child(ChildOutcome::new(result)))
}

/// The outcome of a finished [`mint_child`].
pub(crate) fn mint_outcome(event: Event) -> Result<SuccessorOutcome, SuccessorError> {
    let Event::Child(outcome) = event else {
        return Err(SuccessorError::InvalidEvent);
    };
    outcome.take().unwrap_or(Err(SuccessorError::InvalidEvent))
}

impl Operation for MintSuccessorOperation {
    type Output = SuccessorOutcome;
    type Error = SuccessorError;

    fn first(&mut self) -> Turn {
        self.state = OperationState::CheckPurgeFence;
        let bucket = &self.mint.plan.context.bucket;
        Ok(Next::Call(read_row(
            PURGE_FENCE_KEYSPACE,
            fence_key(bucket),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            OperationState::CheckPurgeFence => {
                let context = &self.mint.plan.context;
                check_fence_row(result.value()?.as_deref(), &context.bucket, &context.key)?;
                self.state = OperationState::Minting;
                Ok(Next::Call(self.mint.start_calls()?))
            }
            OperationState::Minting => {
                let step = self.mint.step_calls(result)?;
                self.minted(step)
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            SuccessorError::HeadConflict { .. }
                | SuccessorError::BucketChanged
                | SuccessorError::DefaultChanged { .. }
                | SuccessorError::MutationConflict(_)
                | SuccessorError::VersionCollision(_)
                | SuccessorError::IntentConflict
                | SuccessorError::HeadDeleted
                | SuccessorError::VersionMissing
                | SuccessorError::Purge(PurgeFenceError::Suspended)
        )
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{
        CapturedDefault, MintState, MintSuccessorOperation, SuccessorError, SuccessorMint,
        SuccessorOutcome, SuccessorPlan, successor_version,
    };
    use crate::blob::records::HeadAliasContext;
    use crate::s3::purge_fence::PurgeFenceError;
    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::id::NodeId;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::execution::job::JobId;
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
    use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
    use aruna_core::structs::placement::policy::attachment::{
        BULK_INTENT_KEYSPACE, PolicyBlockedReason, PolicyIntent, PolicyIntentOutcome,
        PolicyMutationRecord, PolicyRefMode,
    };
    use aruna_core::structs::placement::policy::{
        PlacementPolicy, PlacementPolicyRef, PlacementSelector, PlacementSubject, PolicyResolution,
        VerifiedPolicy,
    };
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, BlobVersion, BucketInfo, CurrentVersionPointer,
        ManagedCopyRecord, ManagedCopyState, VersionKey,
    };
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
    use aruna_core::structs::storage::usage::{UsageCounters, usage_group_key};
    use aruna_core::types::{Key, Value};
    use std::collections::{BTreeMap, HashMap};
    use std::time::{SystemTime, UNIX_EPOCH};
    use ulid::Ulid;

    const BUCKET: &str = "bucket";
    const OBJECT: &str = "path/file.txt";
    const CONTENT: [u8; 32] = [6u8; 32];

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    fn realm_id() -> RealmId {
        RealmId::from_bytes([1u8; 32])
    }

    fn user_id() -> UserId {
        UserId::nil(realm_id())
    }

    fn context() -> HeadAliasContext {
        HeadAliasContext::new(
            realm_id(),
            Ulid::from_bytes([2u8; 16]),
            node_id(),
            BUCKET,
            OBJECT,
        )
    }

    fn bucket() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: UNIX_EPOCH,
            created_by: user_id(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 3,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn subject() -> PlacementSubject {
        PlacementSubject {
            node_id: node_id(),
            generation: 1,
            location: "eu-west".to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        }
    }

    fn verified(seed: u8, allowed_node: Option<NodeId>) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: allowed_node,
                location: allowed_node.is_none().then(|| "eu-west".to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn resolution(policy: &VerifiedPolicy) -> BTreeMap<Ulid, PolicyResolution> {
        BTreeMap::from([(
            policy.policy().policy_id,
            PolicyResolution::Known(policy.clone()),
        )])
    }

    fn head() -> CurrentVersionPointer {
        CurrentVersionPointer::new_with_generation(Ulid::from_bytes([7u8; 16]), 4)
    }

    fn plan(target: Vec<PlacementPolicyRef>, mode: PolicyRefMode) -> SuccessorPlan {
        SuccessorPlan {
            context: context(),
            mutation_id: Ulid::from_bytes([8u8; 16]),
            expected_head: head(),
            bucket_identity: bucket().identity(),
            target_refs: target,
            mode,
            successor_version_id: Ulid::from_bytes([9u8; 16]),
            created_at: UNIX_EPOCH,
            auth_context: AuthContext {
                user_id: user_id(),
                realm_id: realm_id(),
                path_restrictions: None,
                session: None,
            },
            subject: subject(),
            resolved: BTreeMap::new(),
            intent: None,
            captured_default: None,
        }
    }

    fn intent() -> PolicyIntent {
        PolicyIntent {
            operation_id: Ulid::from_bytes([3u8; 16]),
            key: OBJECT.to_string(),
            observed_head: head(),
            successor_version_id: Ulid::from_bytes([9u8; 16]),
            outcome: PolicyIntentOutcome::Planned,
        }
    }

    #[test]
    fn successor_obeys_fence() {
        let mut operation = MintSuccessorOperation::new(plan(Vec::new(), PolicyRefMode::Replace));
        operation.first().unwrap();
        let fence = StoragePurgeFence {
            job_id: JobId::from_bytes([12; 16]),
            scope: StoragePurgeScope::File {
                bucket: BUCKET.to_string(),
                key: OBJECT.to_string(),
            },
        };

        let turn = operation.next(read(Some(fence.to_bytes().unwrap())));

        assert_eq!(
            turn.err(),
            Some(SuccessorError::Purge(PurgeFenceError::Suspended))
        );
    }

    #[test]
    fn operation_commits_outcome() {
        // A replayed mutation ends the run; the runner commits its read transaction.
        let mut operation = MintSuccessorOperation::new(plan(Vec::new(), PolicyRefMode::Replace));
        operation.first().unwrap();
        operation.next(read(None)).unwrap();
        let record = PolicyMutationRecord {
            mutation_id: Ulid::from_bytes([8u8; 16]),
            params: plan(Vec::new(), PolicyRefMode::Replace).params(),
            successor_version_id: Ulid::from_bytes([4u8; 16]),
            effective_refs: Vec::new(),
            materialized: true,
        };
        let turn = operation.next(read(Some(record.to_bytes().unwrap())));
        assert!(matches!(
            turn,
            Ok(Next::Done(SuccessorOutcome::Replayed { .. }))
        ));
    }

    fn location() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "aruna".to_string(),
            backend_path: "objects/one".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 3,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), CONTENT.to_vec())]),
        }
    }

    fn materialized(refs: Vec<PlacementPolicyRef>) -> BlobVersion {
        BlobVersion::materialized(
            CONTENT,
            BackendRef::node_default(),
            EncodingClass::Raw,
            UNIX_EPOCH,
            user_id(),
            None,
        )
        .with_policies(refs)
        .expect("refs stored")
    }

    /// One registered local copy of the predecessor, as the write path leaves it.
    fn copy_row(refs: Vec<PlacementPolicyRef>, state: ManagedCopyState) -> (Key, Value) {
        let record = ManagedCopyRecord::new(
            VersionKey::new(BUCKET, OBJECT, head().version_id),
            node_id(),
            location(),
            refs,
            7,
            state,
        )
        .expect("record builds");
        (
            Key::from(record.key().to_bytes().expect("key encodes")),
            Value::from(record.to_bytes().expect("record encodes")),
        )
    }

    fn read(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    /// The bucket and the live subject the exposing transaction re-reads.
    fn drift(bucket: Option<Vec<u8>>, record: NodeSubjectRecord) -> IoResult {
        IoResult::Values(vec![
            bucket.map(Into::into),
            Some(Value::from(record.to_bytes().expect("record encodes"))),
        ])
    }

    fn advertised() -> NodeSubjectRecord {
        NodeSubjectRecord::seed(subject()).expect("subject is valid")
    }

    fn copies(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    /// Drives the mint through mutation, head, bucket, version and the free
    /// successor id, up to the copy scan.
    fn drive_to_copy(mint: &mut SuccessorMint, version: &BlobVersion) -> Option<IoCall> {
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");
        if mint.plan.intent.is_some() {
            mint.step_calls(read(None)).expect("intent absent");
        }
        mint.step_calls(read(Some(head().to_bytes().unwrap())))
            .expect("head matches");
        mint.step_calls(drift(Some(bucket().to_bytes().unwrap()), advertised()))
            .expect("bucket matches");
        let effects = mint
            .step_calls(read(Some(version.to_bytes().unwrap())))
            .expect("version decodes");
        match effects {
            Some(_) => mint.step_calls(read(None)).expect("successor id is free"),
            None => None,
        }
    }

    fn batch_writes(call: IoCall) -> Vec<(String, Vec<u8>, Vec<u8>)> {
        let IoCall::Write(writes) = call else {
            panic!("expected a batch write");
        };
        writes
            .into_iter()
            .map(|(key_space, key, value)| (key_space, key.to_vec(), value.to_vec()))
            .collect()
    }

    /// Runs the embedded counter update from the record batch to its deltas
    /// and returns the counters the owning group would commit from zero.
    fn group_counters(mint: &mut SuccessorMint) -> UsageCounters {
        let mut effect = first_effect(
            mint.step_calls(IoResult::Done)
                .expect("the usage update starts"),
        );
        // A materialized mint reads the realm config first; without one the
        // group is unlimited and the counter update starts right after.
        if mint.state == MintState::ReadQuota {
            effect = first_effect(mint.step_calls(read(None)).expect("no ceiling applies"));
        }
        let IoCall::AddUsage { deltas, .. } = effect else {
            panic!("expected the counter deltas");
        };
        let group_key = usage_group_key(bucket().group_id);
        let (_, delta) = deltas
            .into_iter()
            .find(|(key, _)| key.as_ref() == group_key.as_slice())
            .expect("the group counter is charged");
        let mut counters = UsageCounters::default();
        counters.apply(&delta).expect("the delta applies");
        counters
    }

    fn first_effect(call: Option<IoCall>) -> IoCall {
        call.expect("a call follows")
    }

    #[test]
    fn replays_recorded_mutation() {
        // A lost response must resolve to the recorded successor, not a new one.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Replace));
        let record = PolicyMutationRecord {
            mutation_id: Ulid::from_bytes([8u8; 16]),
            params: plan(Vec::new(), PolicyRefMode::Replace).params(),
            successor_version_id: Ulid::from_bytes([4u8; 16]),
            effective_refs: Vec::new(),
            materialized: true,
        };
        mint.start_calls().expect("start builds");

        assert_eq!(
            mint.step_calls(read(Some(record.to_bytes().unwrap())))
                .expect("replay decides"),
            None
        );
        assert_eq!(
            mint.outcome(),
            Some(&SuccessorOutcome::Replayed {
                version_id: Ulid::from_bytes([4u8; 16]),
                refs: Vec::new(),
                materialized: true,
            })
        );
    }

    #[test]
    fn rejects_reused_mutation() {
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        let record = PolicyMutationRecord {
            mutation_id: Ulid::from_bytes([8u8; 16]),
            params: plan(Vec::new(), PolicyRefMode::Replace).params(),
            successor_version_id: Ulid::from_bytes([4u8; 16]),
            effective_refs: Vec::new(),
            materialized: true,
        };
        mint.start_calls().expect("start builds");

        assert_eq!(
            mint.step_calls(read(Some(record.to_bytes().unwrap()))),
            Err(SuccessorError::MutationConflict(Ulid::from_bytes(
                [8u8; 16]
            )))
        );
    }

    #[test]
    fn rejects_stale_head() {
        // A concurrent write advanced the head, so the successor must not roll it
        // back to the version this mutation was planned against.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Replace));
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");
        let moved = CurrentVersionPointer::new_with_generation(Ulid::from_bytes([1u8; 16]), 5);

        assert_eq!(
            mint.step_calls(read(Some(moved.to_bytes().unwrap()))),
            Err(SuccessorError::HeadConflict {
                current: Some(moved)
            })
        );
    }

    #[test]
    fn rejects_changed_bucket() {
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Replace));
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");
        mint.step_calls(read(Some(head().to_bytes().unwrap())))
            .expect("head matches");
        let mut recreated = bucket();
        recreated.created_at = UNIX_EPOCH + std::time::Duration::from_secs(9);

        assert_eq!(
            mint.step_calls(drift(Some(recreated.to_bytes().unwrap()), advertised())),
            Err(SuccessorError::BucketChanged)
        );
    }

    #[test]
    fn needs_set_hint() {
        // A successor id planned without the set hint is refused in a distributed bucket.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Replace));
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");
        mint.step_calls(read(Some(head().to_bytes().unwrap())))
            .expect("head matches");
        let mut bound = bucket();
        bound.distribution = Some(super::DistributionBinding {
            set_id: Ulid::from_bytes([5; 16]),
            member_id: Ulid::from_bytes([6; 16]),
            incarnation: 1,
            revision: Default::default(),
        });
        assert_eq!(
            mint.step_calls(drift(Some(bound.to_bytes().unwrap()), advertised())),
            Err(SuccessorError::BucketChanged)
        );
    }

    #[test]
    fn reuses_empty_object() {
        // A zero-length governed object still gets a successor: the hash, not
        // the size, proves the registered bytes are the predecessor's.
        let policy = verified(1, Some(node_id()));
        let refs = vec![policy.policy_ref()];
        let version = VersionKey::new(BUCKET, OBJECT, head().version_id);
        let mut empty = location();
        empty.blob_size = 0;
        let record = ManagedCopyRecord::new(
            version.clone(),
            node_id(),
            empty,
            refs.clone(),
            7,
            ManagedCopyState::Registered,
        )
        .expect("record builds")
        .stored_under(subject().generation);

        assert!(
            super::reusable_copy(&record.key(), &record, &version, &subject(), CONTENT, &refs)
                .is_some()
        );
    }

    #[test]
    fn rejects_moved_subject() {
        // The plan was evaluated against the subject this node advertised then;
        // a drift before the commit must not expose bytes nothing evaluated.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Union));
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");
        mint.step_calls(read(Some(head().to_bytes().unwrap())))
            .expect("head matches");
        let mut moved = advertised();
        moved.subject.generation = 9;

        assert_eq!(
            mint.step_calls(drift(Some(bucket().to_bytes().unwrap()), moved)),
            Err(SuccessorError::SubjectDrift)
        );

        // A node that stopped serving is refused under the same generation.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Union));
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");
        mint.step_calls(read(Some(head().to_bytes().unwrap())))
            .expect("head matches");
        let mut blocked = advertised();
        blocked.serving_blocked = true;

        assert_eq!(
            mint.step_calls(drift(Some(bucket().to_bytes().unwrap()), blocked)),
            Err(SuccessorError::SubjectDrift)
        );
    }

    #[test]
    fn rejects_changed_default() {
        // A run's target is bound into its own transaction: a default that moved
        // on supersedes the run before any old target commits.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Union));
        mint.plan.captured_default = Some(CapturedDefault {
            generation: 2,
            refs: Vec::new(),
        });
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");
        mint.step_calls(read(Some(head().to_bytes().unwrap())))
            .expect("head matches");

        assert_eq!(
            mint.step_calls(drift(Some(bucket().to_bytes().unwrap()), advertised())),
            Err(SuccessorError::DefaultChanged { current: 3 })
        );
    }

    #[test]
    fn rejects_taken_version() {
        // The assigned successor id must be free; existing bytes are never
        // overwritten.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");
        mint.step_calls(read(Some(head().to_bytes().unwrap())))
            .expect("head matches");
        mint.step_calls(drift(Some(bucket().to_bytes().unwrap()), advertised()))
            .expect("bucket matches");
        mint.step_calls(read(Some(materialized(Vec::new()).to_bytes().unwrap())))
            .expect("version decodes");

        assert_eq!(
            mint.step_calls(read(Some(materialized(Vec::new()).to_bytes().unwrap()))),
            Err(SuccessorError::VersionCollision(Ulid::from_bytes(
                [9u8; 16]
            )))
        );
    }

    #[test]
    fn rejects_foreign_intent() {
        // Another pass owns this object's intent, so this one may not write.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Union));
        mint.plan.intent = Some(intent());
        let mut stored = intent();
        stored.successor_version_id = Ulid::from_bytes([4u8; 16]);
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");

        assert_eq!(
            mint.step_calls(read(Some(stored.to_bytes().unwrap()))),
            Err(SuccessorError::IntentConflict)
        );
    }

    #[test]
    fn keeps_completed_intent() {
        // Completed evidence never regresses to a replanned or blocked outcome.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Union));
        mint.plan.intent = Some(intent());
        let mut stored = intent();
        stored.outcome = PolicyIntentOutcome::Completed {
            version_id: Ulid::from_bytes([9u8; 16]),
            materialized: true,
        };
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");

        assert_eq!(
            mint.step_calls(read(Some(stored.to_bytes().unwrap()))),
            Err(SuccessorError::IntentConflict)
        );
    }

    #[test]
    fn replans_stale_intent() {
        // An intent planned against an older head is replaced by this pass, not
        // treated as a competing one.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Union));
        mint.plan.intent = Some(intent());
        let mut stored = intent();
        stored.observed_head =
            CurrentVersionPointer::new_with_generation(Ulid::from_bytes([1u8; 16]), 2);
        stored.successor_version_id = Ulid::from_bytes([4u8; 16]);
        mint.start_calls().expect("start builds");
        mint.step_calls(read(None)).expect("mutation absent");

        assert!(
            mint.step_calls(read(Some(stored.to_bytes().unwrap())))
                .expect("stale intent is replanned")
                .is_some()
        );
    }

    #[test]
    fn union_keeps_existing() {
        // A bucket-default application may only add refs.
        let existing = verified(1, Some(node_id()));
        let added = verified(2, None);
        let mut mint = SuccessorMint::new(plan(vec![added.policy_ref()], PolicyRefMode::Union));
        mint.plan.resolved = resolution(&existing);
        mint.plan.resolved.extend(resolution(&added));
        let effect = drive_to_copy(&mut mint, &materialized(vec![existing.policy_ref()]))
            .expect("copy scan follows");

        let IoCall::Page { space, .. } = effect else {
            panic!("expected the managed-copy scan");
        };
        assert_eq!(space, MANAGED_COPY_KEYSPACE);
        assert!(mint.effective_refs.contains(&existing.policy_ref()));
        assert!(mint.effective_refs.contains(&added.policy_ref()));
    }

    #[test]
    fn replace_can_relax() {
        // The realm-admin mutation is an exact replacement, unlike the bulk union.
        let existing = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Replace));
        drive_to_copy(&mut mint, &materialized(vec![existing.policy_ref()]));

        assert!(mint.effective_refs.is_empty());
    }

    #[test]
    fn blocks_unresolved_policy() {
        // Without the referenced document nothing is granted.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        assert_eq!(drive_to_copy(&mut mint, &materialized(Vec::new())), None);

        assert_eq!(
            mint.outcome(),
            Some(&SuccessorOutcome::Blocked(
                PolicyBlockedReason::PolicyUnresolved
            ))
        );
    }

    #[test]
    fn blocks_denied_destination() {
        let elsewhere = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let policy = verified(1, Some(elsewhere));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        assert_eq!(drive_to_copy(&mut mint, &materialized(Vec::new())), None);

        assert_eq!(
            mint.outcome(),
            Some(&SuccessorOutcome::Blocked(
                PolicyBlockedReason::DestinationDenied
            ))
        );
    }

    #[test]
    fn blocks_missing_copy() {
        // A materialized successor cannot complete without verified local bytes.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");

        // A blocked mutation without a run writes nothing.
        assert_eq!(
            mint.step_calls(copies(Vec::new())).expect("scan decides"),
            None
        );
        assert_eq!(
            mint.outcome(),
            Some(&SuccessorOutcome::Blocked(
                PolicyBlockedReason::SourceUnavailable
            ))
        );
    }

    #[test]
    fn blocks_quarantined_copy() {
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        let row = copy_row(Vec::new(), ManagedCopyState::UnresolvedDeparted);

        assert_eq!(
            mint.step_calls(copies(vec![row])).expect("scan decides"),
            None
        );
        assert_eq!(
            mint.outcome(),
            Some(&SuccessorOutcome::Blocked(
                PolicyBlockedReason::SourceUnavailable
            ))
        );
    }

    #[test]
    fn rejects_mismatched_copy() {
        // A registration that does not describe exactly the predecessor's own
        // registered bytes is no evidence for reusing them.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        // The predecessor carries no refs, so a row registered under one is not
        // its registration.
        let row = copy_row(vec![policy.policy_ref()], ManagedCopyState::Registered);

        assert_eq!(
            mint.step_calls(copies(vec![row])).expect("scan decides"),
            None
        );
        assert_eq!(
            mint.outcome(),
            Some(&SuccessorOutcome::Blocked(
                PolicyBlockedReason::SourceUnavailable
            ))
        );
    }

    #[test]
    fn blocked_persists_intent() {
        // Inside a run the reason is durable, so a later pass resumes it.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Union));
        mint.plan.resolved = resolution(&policy);
        mint.plan.intent = Some(intent());
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        let effect = first_effect(mint.step_calls(copies(Vec::new())).expect("scan decides"));
        let writes = batch_writes(effect);

        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, BULK_INTENT_KEYSPACE);
        let stored = PolicyIntent::from_bytes(&writes[0].2).expect("intent decodes");
        assert_eq!(
            stored.outcome,
            PolicyIntentOutcome::Blocked(PolicyBlockedReason::SourceUnavailable)
        );
    }

    #[test]
    fn mints_from_copy() {
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        let row = copy_row(Vec::new(), ManagedCopyState::Registered);

        let effect = first_effect(mint.step_calls(copies(vec![row])).expect("scan decides"));
        let writes = batch_writes(effect);

        let spaces: Vec<&str> = writes.iter().map(|(space, ..)| space.as_str()).collect();
        assert!(spaces.contains(&BLOB_VERSIONS_KEYSPACE));
        assert!(spaces.contains(&BLOB_HEAD_KEYSPACE));
        assert!(spaces.contains(&MANAGED_COPY_KEYSPACE));
        let (_, _, value) = writes
            .iter()
            .find(|(space, ..)| space == BLOB_VERSIONS_KEYSPACE)
            .expect("successor write");
        let successor = BlobVersion::from_bytes(value).expect("successor decodes");
        assert_eq!(successor.placement_policies, vec![policy.policy_ref()]);
        let (_, _, value) = writes
            .iter()
            .find(|(space, ..)| space == BLOB_HEAD_KEYSPACE)
            .expect("head write");
        let pointer = CurrentVersionPointer::from_bytes(value).expect("pointer decodes");
        assert_eq!(pointer.version_id, mint.plan.successor_version_id);
        // The head is read and written under one exact expected pointer.
        assert_eq!(pointer.generation, head().generation + 1);
    }

    #[test]
    fn books_successor_bytes() {
        // The successor stores a second version of the same bytes, so its size
        // is booked while the head only moves between two live versions.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        let row = copy_row(Vec::new(), ManagedCopyState::Registered);
        mint.step_calls(copies(vec![row])).expect("scan decides");

        let counters = group_counters(&mut mint);
        assert_eq!(counters.logical_bytes, location().blob_size);
        assert_eq!(counters.objects, 0);
        assert_eq!(counters.stored_bytes, 0);
        assert_eq!(counters.referenced_bytes, 0);
    }

    #[test]
    fn refuses_over_ceiling() {
        // Successor bytes must fit the group ceiling before counters commit.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        let row = copy_row(Vec::new(), ManagedCopyState::Registered);
        mint.step_calls(copies(vec![row])).expect("scan decides");
        mint.step_calls(IoResult::Done)
            .expect("the quota read follows");

        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        config.quota.default_quota_bytes = Some(1);
        config.quota.grace_factor_percent = 100;
        let actor = Actor {
            node_id: node_id(),
            user_id: user_id(),
            realm_id: realm_id(),
        };
        mint.step_calls(read(Some(config.to_bytes(&actor).expect("config encodes"))))
            .expect("the gate starts");
        mint.step_calls(read(None)).expect("no usage rebuild runs");
        mint.step_calls(read(None))
            .expect("the gate reads its own realm config");
        mint.step_calls(read(None))
            .expect("the group holds no counters yet");

        let error = mint
            .step_calls(copies(Vec::new()))
            .expect_err("the mint is refused");

        assert_eq!(
            error,
            SuccessorError::QuotaExceeded {
                limit: 1,
                usage: location().blob_size,
            }
        );
    }

    #[test]
    fn blocked_skips_usage() {
        // Nothing was minted, so the blocked receipt moves no counter.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Union));
        mint.plan.resolved = resolution(&policy);
        mint.plan.intent = Some(intent());
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        mint.step_calls(copies(Vec::new())).expect("scan decides");

        let effects = mint.step_calls(IoResult::Done).expect("the mint settles");
        assert_eq!(effects, None);
        assert_eq!(mint.state, MintState::Done);
    }

    const REFERENCE_BYTES: u64 = 1;

    fn reference() -> BlobVersion {
        BlobVersion::reference(
            aruna_core::structs::execution::staging::VersionSourceBinding {
                strategy: aruna_core::structs::execution::staging::StagingStrategy::Reference,
                descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
                    kind:
                        aruna_core::structs::execution::source_connector::SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "source".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                    namespace: None,
                    root: None,
                },
                connector_id: None,
                mirrors: Vec::new(),
            },
            aruna_core::structs::execution::source_access::SourceMetadata {
                content_length: REFERENCE_BYTES,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
                digests: Default::default(),
            },
            UNIX_EPOCH,
            user_id(),
            UNIX_EPOCH,
        )
    }

    #[test]
    fn books_reference_bytes() {
        // A reference successor carries no local bytes, so it books the source
        // size against the referenced counter instead of the logical one.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &reference()).expect("writes follow");

        let counters = group_counters(&mut mint);
        assert_eq!(counters.referenced_bytes, REFERENCE_BYTES);
        assert_eq!(counters.logical_bytes, 0);
        assert_eq!(counters.objects, 0);
    }

    #[test]
    fn offered_successor_uncharged() {
        // The successor of a head observation takes over its charge, so the
        // group is charged once and no group counter moves.
        let mut observation = reference();
        if let aruna_core::structs::storage::blob::BlobVersionState::Reference { source, .. } =
            &mut observation.state
        {
            source.descriptor.kind =
                aruna_core::structs::execution::source_connector::SourceConnectorKind::LocalDirectory;
            source.descriptor.source_path = OBJECT.to_string();
            source.descriptor.public_config.insert(
                aruna_core::structs::execution::offered_directory::OFFERED_DIRECTORY_BUCKET
                    .to_string(),
                BUCKET.to_string(),
            );
        }
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &observation).expect("writes follow");

        let written = || IoResult::Done;
        let effects = mint.step_calls(written()).expect("the write settles");
        // No group counter moves; only the bucket counts the successor's row.
        let Some(IoCall::AddUsage {
            deltas, buckets, ..
        }) = &effects
        else {
            panic!("expected the bucket row update, got {effects:?}");
        };
        assert!(deltas.iter().all(|(_, delta)| delta.is_zero()));
        let versions: Vec<_> = buckets.iter().map(|(_, delta)| delta.versions).collect();
        assert_eq!(versions, [1]);
        // The counter rows, then the dirty markers, are written.
        mint.step_calls(written()).expect("the counters settle");
        let done = mint.step_calls(written()).expect("the markers settle");
        assert!(done.is_none());
        assert_eq!(mint.state, MintState::Done);
    }

    #[test]
    fn reference_skips_registration() {
        // A reference claims no managed copy, so it registers nothing.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        let effect = drive_to_copy(&mut mint, &reference()).expect("writes follow");
        let writes = batch_writes(effect);

        assert!(
            writes
                .iter()
                .all(|(space, ..)| space != MANAGED_COPY_KEYSPACE)
        );
        assert_eq!(
            mint.outcome(),
            Some(&SuccessorOutcome::Minted {
                version_id: mint.plan.successor_version_id,
                refs: vec![policy.policy_ref()],
                materialized: false,
            })
        );
        assert_eq!(mint.state, MintState::WriteRecords);
    }

    #[test]
    fn successor_keeps_content() {
        let source = materialized(Vec::new());
        let successor = successor_version(&source, SystemTime::UNIX_EPOCH, user_id());

        assert_eq!(successor.state, source.state);
        assert_eq!(successor.published_by, None);
    }
}
