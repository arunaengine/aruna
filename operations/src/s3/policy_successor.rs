//! Explicit policy attachment as a successor version.
//!
//! Attaching, tightening or relaxing a policy never rewrites a stored version:
//! it mints one successor carrying the new effective refs and advances the head
//! from an exact expected pointer. The successor VersionId is durably assigned
//! under the caller's `mutation_id`, so a lost response or restart resolves to
//! that same version instead of minting another one.

use crate::blob::blob_keyspace_helper::HeadAliasContext;
use crate::blob::managed_copy::{
    COPY_PAGE_LIMIT, CopyRequest, ManagedCopyError, ManagedCopyPage, register_entry, scan_effect,
    validate_registration, version_scope,
};
use crate::replication::queue::{LiveReplicationObligationRecord, live_obligation_entry};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AuthContext, BackendLocation, BlobVersion, BlobVersionState, BucketIdentity, BucketInfo,
    CurrentVersionPointer, ManagedCopyKey, ManagedCopyRecord, POLICY_BULK_INTENT_KEYSPACE,
    POLICY_MUTATION_KEYSPACE, PlacementDecision, PlacementPolicyError, PlacementPolicyRef,
    PlacementSubject, PolicyBlockedReason, PolicyBulkIntent, PolicyIntentOutcome,
    PolicyMutationParams, PolicyMutationRecord, PolicyRefMode, PolicyResolution, VersionKey,
    evaluate_placement,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

/// The bucket default a bulk run sealed. A mint that observes another default
/// in its own transaction supersedes instead of committing an old target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedDefault {
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
    pub intent: Option<PolicyBulkIntent>,
    /// Set by a bulk run, so the pass cannot commit against a default that
    /// moved on between the run's seal and this transaction.
    pub sealed_default: Option<SealedDefault>,
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

#[derive(Debug, Error, PartialEq)]
pub enum SuccessorError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    ManagedCopy(#[from] ManagedCopyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("mutation {0} is recorded with different parameters")]
    MutationConflict(Ulid),
    #[error("the object head is no longer the expected version")]
    HeadConflict {
        current: Option<CurrentVersionPointer>,
    },
    #[error("the bucket record is missing or changed identity")]
    BucketChanged,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MintState {
    ReadMutation,
    ReadIntent,
    ReadHead,
    ReadBucket,
    ReadVersion,
    ReadSuccessor,
    ScanCopies,
    WriteRecords,
    Done,
}

/// Embedded state machine so the single-object mutation and a bulk pass share
/// one implementation and one commit boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct SuccessorMint {
    plan: SuccessorPlan,
    state: MintState,
    predecessor: Option<BlobVersion>,
    sealed_refs: Vec<PlacementPolicyRef>,
    outcome: Option<SuccessorOutcome>,
    /// Whether a batch write was emitted, so the caller knows to commit.
    wrote: bool,
}

impl SuccessorMint {
    pub fn new(plan: SuccessorPlan) -> Self {
        Self {
            plan,
            state: MintState::ReadMutation,
            predecessor: None,
            sealed_refs: Vec::new(),
            outcome: None,
            wrote: false,
        }
    }

    pub fn outcome(&self) -> Option<&SuccessorOutcome> {
        self.outcome.as_ref()
    }

    /// A transaction that emitted no write is rolled back instead of committed.
    pub fn wrote(&self) -> bool {
        self.wrote
    }

    pub fn start(&mut self, txn_id: Option<TxnId>) -> Result<Effects, SuccessorError> {
        self.state = MintState::ReadMutation;
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: POLICY_MUTATION_KEYSPACE.to_string(),
            key: PolicyMutationRecord::key(self.plan.mutation_id)?.into(),
            txn_id,
        })])
    }

    /// `Ok(None)` once the outcome is decided; the caller then commits or aborts.
    pub fn step(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(error.into());
        }
        match self.state {
            MintState::ReadMutation => self.handle_mutation(event, txn_id),
            MintState::ReadIntent => self.handle_intent(event, txn_id),
            MintState::ReadHead => self.handle_head(event, txn_id),
            MintState::ReadBucket => self.handle_bucket(event, txn_id),
            MintState::ReadVersion => self.handle_version(event, txn_id),
            MintState::ReadSuccessor => self.handle_successor(event, txn_id),
            MintState::ScanCopies => self.handle_copies(event, txn_id),
            MintState::WriteRecords => {
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return Err(SuccessorError::InvalidEvent);
                };
                self.state = MintState::Done;
                Ok(None)
            }
            MintState::Done => Err(SuccessorError::InvalidEvent),
        }
    }

    fn handle_mutation(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        let value = read_value(event)?;
        if let Some(value) = value {
            let record = PolicyMutationRecord::from_bytes(value.as_ref())?;
            if record.params != self.plan.params() {
                return Err(SuccessorError::MutationConflict(self.plan.mutation_id));
            }
            self.outcome = Some(SuccessorOutcome::Replayed {
                version_id: record.successor_version_id,
                refs: record.sealed_refs,
                materialized: record.materialized,
            });
            self.state = MintState::Done;
            return Ok(None);
        }
        let Some(intent) = self.plan.intent.as_ref() else {
            return Ok(Some(self.read_head(txn_id)?));
        };
        self.state = MintState::ReadIntent;
        Ok(Some(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: POLICY_BULK_INTENT_KEYSPACE.to_string(),
            key: intent.key().to_bytes()?.into(),
            txn_id,
        })]))
    }

    /// Completed intent evidence never regresses: an intent another pass owns
    /// stops this one instead of overwriting its outcome.
    fn handle_intent(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        let Some(value) = read_value(event)? else {
            return Ok(Some(self.read_head(txn_id)?));
        };
        let stored = PolicyBulkIntent::from_bytes(value.as_ref())?;
        if stored.successor_version_id != self.plan.successor_version_id
            || matches!(stored.outcome, PolicyIntentOutcome::Completed { .. })
        {
            return Err(SuccessorError::IntentConflict);
        }
        Ok(Some(self.read_head(txn_id)?))
    }

    fn read_head(&mut self, txn_id: Option<TxnId>) -> Result<Effects, SuccessorError> {
        self.state = MintState::ReadHead;
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: self.plan.context.head_key().to_bytes()?.into(),
            txn_id,
        })])
    }

    fn handle_head(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        let Some(value) = read_value(event)? else {
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
        Ok(Some(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.plan.context.bucket.as_bytes().to_vec().into(),
            txn_id,
        })]))
    }

    fn handle_bucket(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        let Some(value) = read_value(event)? else {
            return Err(SuccessorError::BucketChanged);
        };
        let bucket = BucketInfo::from_bytes(value.as_ref())?;
        if bucket.identity() != self.plan.bucket_identity {
            return Err(SuccessorError::BucketChanged);
        }
        if let Some(sealed) = self.plan.sealed_default.as_ref()
            && (sealed.generation != bucket.placement_policy_generation
                || sealed.refs != bucket.placement_policies)
        {
            return Err(SuccessorError::DefaultChanged {
                current: bucket.placement_policy_generation,
            });
        }
        self.state = MintState::ReadVersion;
        Ok(Some(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: self
                .plan
                .version_key(self.plan.expected_head.version_id)
                .to_bytes()?
                .into(),
            txn_id,
        })]))
    }

    fn handle_version(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        let Some(value) = read_value(event)? else {
            return Err(SuccessorError::VersionMissing);
        };
        let predecessor = BlobVersion::from_bytes(value.as_ref())?;
        self.sealed_refs = match self.plan.mode {
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
            return self.blocked(reason, txn_id);
        }
        self.predecessor = Some(predecessor);
        self.state = MintState::ReadSuccessor;
        Ok(Some(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: self
                .plan
                .version_key(self.plan.successor_version_id)
                .to_bytes()?
                .into(),
            txn_id,
        })]))
    }

    /// The assigned VersionId must be free: an existing version under it
    /// belongs to another mutation and is never overwritten.
    fn handle_successor(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        if read_value(event)?.is_some() {
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
            return self.write_records(txn_id, None);
        }
        self.state = MintState::ScanCopies;
        Ok(Some(smallvec![scan_effect(
            Some(version_scope(&self.plan.version_key(
                self.plan.expected_head.version_id
            ))?),
            None,
            COPY_PAGE_LIMIT,
            txn_id,
        )]))
    }

    /// The successor's refs must admit this node before any byte is claimed for
    /// it. An unresolvable or denying policy blocks instead of granting.
    fn blocked_placement(&self) -> Option<PolicyBlockedReason> {
        match evaluate_placement(&self.sealed_refs, &self.plan.resolved, &self.plan.subject) {
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
    fn handle_copies(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        let page = ManagedCopyPage::decode(event)?;
        let Some(predecessor) = self.predecessor.as_ref() else {
            return Err(SuccessorError::VersionMissing);
        };
        let Some(hash) = predecessor.blob_hash().copied() else {
            return Err(SuccessorError::VersionMissing);
        };
        let version = self.plan.version_key(self.plan.expected_head.version_id);
        let refs = predecessor.placement_policies.clone();
        let reusable = page.entries.into_iter().find_map(|(key, record)| {
            reusable_copy(&key, &record, &version, self.plan.subject.node_id, hash, &refs)
        });
        match reusable {
            Some(location) => self.write_records(txn_id, Some(location)),
            None => self.blocked(PolicyBlockedReason::SourceUnavailable, txn_id),
        }
    }

    /// A blocked object keeps its reason durable inside a run, so a later pass
    /// resumes it instead of rescanning from nothing.
    fn blocked(
        &mut self,
        reason: PolicyBlockedReason,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, SuccessorError> {
        self.outcome = Some(SuccessorOutcome::Blocked(reason));
        let Some(intent) = self.plan.intent.as_ref() else {
            self.state = MintState::Done;
            return Ok(None);
        };
        let mut record = intent.clone();
        record.outcome = PolicyIntentOutcome::Blocked(reason);
        let writes = vec![(
            POLICY_BULK_INTENT_KEYSPACE.to_string(),
            Key::from(record.key().to_bytes()?),
            Value::from(record.to_bytes()?),
        )];
        self.state = MintState::WriteRecords;
        self.wrote = true;
        Ok(Some(smallvec![Effect::Storage(
            StorageEffect::BatchWrite { writes, txn_id }
        )]))
    }

    /// One batch, so the successor, the advanced head, the idempotency record
    /// and the local registration become visible together or not at all.
    fn write_records(
        &mut self,
        txn_id: Option<TxnId>,
        location: Option<BackendLocation>,
    ) -> Result<Option<Effects>, SuccessorError> {
        let Some(predecessor) = self.predecessor.as_ref() else {
            return Err(SuccessorError::VersionMissing);
        };
        let version_id = self.plan.successor_version_id;
        let successor = successor_version(
            predecessor,
            self.plan.created_at,
            self.plan.auth_context.user_id,
        )
        .with_policies(self.sealed_refs.clone())?;
        let mut writes: Vec<(String, Key, Value)> = Vec::with_capacity(6);
        writes.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            self.plan.version_key(version_id).to_bytes()?.into(),
            successor.to_bytes()?.into(),
        ));
        // The head was read and written inside this transaction against the
        // exact expected pointer, so the successor always advances one
        // generation and can never contend with a local same-generation write.
        writes.push((
            BLOB_HEAD_KEYSPACE.to_string(),
            self.plan.context.head_key().to_bytes()?.into(),
            CurrentVersionPointer::next_for(Some(&self.plan.expected_head), version_id)?
                .to_bytes()?
                .into(),
        ));
        writes.push((
            POLICY_MUTATION_KEYSPACE.to_string(),
            PolicyMutationRecord::key(self.plan.mutation_id)?.into(),
            PolicyMutationRecord {
                mutation_id: self.plan.mutation_id,
                params: self.plan.params(),
                successor_version_id: version_id,
                sealed_refs: self.sealed_refs.clone(),
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
                HASH_PATHS_INDEX_KEYSPACE.to_string(),
                self.plan
                    .context
                    .hash_path_index_key(hash, version_id)
                    .to_bytes()?
                    .into(),
                Vec::new().into(),
            ));
            writes.push(register_entry(
                self.plan.version_key(version_id),
                self.plan.subject.node_id,
                location,
                &self.sealed_refs,
                version_id.timestamp_ms(),
            )?);
        }
        // The successor replicates like any other write, so peers converge on
        // the governed version instead of only this node holding it.
        writes.push(live_obligation_entry(
            &LiveReplicationObligationRecord::new(
                self.plan.subject.node_id,
                self.plan.auth_context.clone(),
                self.plan.context.bucket.clone(),
                self.plan.context.key.clone(),
                version_id,
                false,
            ),
        )?);
        if let Some(intent) = self.plan.intent.as_ref() {
            let mut receipt = intent.clone();
            receipt.outcome = PolicyIntentOutcome::Completed {
                version_id,
                materialized: location.is_some(),
            };
            writes.push((
                POLICY_BULK_INTENT_KEYSPACE.to_string(),
                receipt.key().to_bytes()?.into(),
                receipt.to_bytes()?.into(),
            ));
        }
        self.outcome = Some(SuccessorOutcome::Minted {
            version_id,
            refs: self.sealed_refs.clone(),
            materialized: location.is_some(),
        });
        self.state = MintState::WriteRecords;
        self.wrote = true;
        Ok(Some(smallvec![Effect::Storage(
            StorageEffect::BatchWrite { writes, txn_id }
        )]))
    }
}

/// A row may back the successor only when it is this node's registration of
/// exactly these bytes under the predecessor's own refs. The successor's own
/// compliance is decided by the placement gate, not by this row.
fn reusable_copy(
    key: &ManagedCopyKey,
    record: &ManagedCopyRecord,
    version: &VersionKey,
    node_id: aruna_core::NodeId,
    blob_hash: [u8; 32],
    refs: &[PlacementPolicyRef],
) -> Option<BackendLocation> {
    if key.version != *version {
        return None;
    }
    let request = CopyRequest {
        key,
        node_id: Some(node_id),
        blake3: Some(blob_hash),
        refs,
    };
    let bytes = record.to_bytes().ok()?;
    let validated = validate_registration(Some(bytes.as_slice()), &request).ok()?;
    (validated.location.blob_size > 0).then_some(validated.location)
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

fn read_value(event: Event) -> Result<Option<Value>, SuccessorError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(SuccessorError::InvalidEvent),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationState {
    Init,
    StartTransaction,
    Minting,
    CommitTransaction,
    Finish,
    Error,
}

/// The realm-admin mutation: one transaction, one durably assigned successor.
#[derive(Debug, PartialEq)]
pub struct MintPolicySuccessorOperation {
    mint: SuccessorMint,
    state: OperationState,
    txn_id: Option<TxnId>,
    output: Option<Result<SuccessorOutcome, SuccessorError>>,
}

impl MintPolicySuccessorOperation {
    pub fn new(plan: SuccessorPlan) -> Self {
        Self {
            mint: SuccessorMint::new(plan),
            state: OperationState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: SuccessorError) -> Effects {
        self.state = OperationState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    /// A transaction that wrote nothing is rolled back instead of committing an
    /// empty mutation; a persisted blocked reason still has to commit.
    fn settle(&mut self, outcome: SuccessorOutcome) -> Effects {
        self.output = Some(Ok(outcome));
        if !self.mint.wrote() {
            self.state = OperationState::Finish;
            return self.abort();
        }
        match self.txn_id {
            Some(txn_id) => {
                self.state = OperationState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            None => self.fail(SuccessorError::InvalidEvent),
        }
    }
}

impl Operation for MintPolicySuccessorOperation {
    type Output = SuccessorOutcome;
    type Error = SuccessorError;

    fn start(&mut self) -> Effects {
        self.state = OperationState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            OperationState::Init => self.start(),
            OperationState::StartTransaction => {
                if let Event::Storage(StorageEvent::Error { error }) = event {
                    return self.fail(error.into());
                }
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(SuccessorError::InvalidEvent);
                };
                self.txn_id = Some(txn_id);
                self.state = OperationState::Minting;
                match self.mint.start(Some(txn_id)) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            OperationState::Minting => match self.mint.step(event, self.txn_id) {
                Ok(Some(effects)) => effects,
                Ok(None) => match self.mint.outcome().cloned() {
                    Some(outcome) => self.settle(outcome),
                    None => self.fail(SuccessorError::InvalidEvent),
                },
                Err(error) => self.fail(error),
            },
            OperationState::CommitTransaction => {
                if let Event::Storage(StorageEvent::Error { error }) = event {
                    return self.fail(error.into());
                }
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(SuccessorError::InvalidEvent);
                };
                self.txn_id = None;
                self.state = OperationState::Finish;
                smallvec![]
            }
            OperationState::Finish => smallvec![],
            OperationState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, OperationState::Finish | OperationState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(result) => result,
            None => Err(SuccessorError::InvalidEvent),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
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
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MintState, SealedDefault, SuccessorError, SuccessorMint, SuccessorOutcome, SuccessorPlan,
        successor_version,
    };
    use crate::blob::blob_keyspace_helper::HeadAliasContext;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_CONTENDER_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
        MANAGED_COPY_KEYSPACE,
    };
    use aruna_core::structs::{
        AuthContext, BackendLocation, BackendRef, BlobVersion, BucketInfo, CurrentVersionPointer,
        ManagedCopyRecord, ManagedCopyState, POLICY_BULK_INTENT_KEYSPACE, PlacementPolicy,
        PlacementPolicyRef, PlacementSelector, PlacementSubject, PolicyBlockedReason,
        PolicyBulkIntent, PolicyIntentOutcome, PolicyMutationRecord, PolicyRefMode,
        PolicyResolution, RealmId, VerifiedPolicy, VersionKey, checksum::HASH_BLAKE3,
    };
    use aruna_core::types::{Key, NodeId, UserId, Value};
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
            replication: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 3,
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
            },
            subject: subject(),
            resolved: BTreeMap::new(),
            intent: None,
            sealed_default: None,
        }
    }

    fn intent() -> PolicyBulkIntent {
        PolicyBulkIntent {
            operation_id: Ulid::from_bytes([3u8; 16]),
            key: OBJECT.to_string(),
            observed_head: head(),
            successor_version_id: Ulid::from_bytes([9u8; 16]),
            outcome: PolicyIntentOutcome::Planned,
        }
    }

    fn location() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "aruna".to_string(),
            backend_path: "objects/one".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            compressed: false,
            encrypted: false,
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
            UNIX_EPOCH,
            user_id(),
            None,
        )
        .with_policies(refs)
        .expect("refs seal")
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

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    fn copies(rows: Vec<(Key, Value)>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: rows,
            next_start_after: None,
        })
    }

    /// Drives the mint through mutation, head, bucket, version and the free
    /// successor id, up to the copy scan.
    fn drive_to_copy(mint: &mut SuccessorMint, version: &BlobVersion) -> Option<Effect> {
        mint.start(None).expect("start builds");
        mint.step(read(None), None).expect("mutation absent");
        if mint.plan.intent.is_some() {
            mint.step(read(None), None).expect("intent absent");
        }
        mint.step(read(Some(head().to_bytes().unwrap())), None)
            .expect("head matches");
        mint.step(read(Some(bucket().to_bytes().unwrap())), None)
            .expect("bucket matches");
        let effects = mint
            .step(read(Some(version.to_bytes().unwrap())), None)
            .expect("version decodes");
        match effects {
            Some(_) => mint
                .step(read(None), None)
                .expect("successor id is free")
                .and_then(|effects| effects.into_iter().next()),
            None => None,
        }
    }

    fn batch_writes(effect: Effect) -> Vec<(String, Vec<u8>, Vec<u8>)> {
        let Effect::Storage(StorageEffect::BatchWrite { writes, .. }) = effect else {
            panic!("expected a batch write");
        };
        writes
            .into_iter()
            .map(|(key_space, key, value)| (key_space, key.to_vec(), value.to_vec()))
            .collect()
    }

    fn first_effect(effects: Option<aruna_core::types::Effects>) -> Effect {
        effects
            .expect("effects follow")
            .into_iter()
            .next()
            .expect("one effect")
    }

    #[test]
    fn replays_recorded_mutation() {
        // A lost response must resolve to the recorded successor, not a new one.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Replace));
        let record = PolicyMutationRecord {
            mutation_id: Ulid::from_bytes([8u8; 16]),
            params: plan(Vec::new(), PolicyRefMode::Replace).params(),
            successor_version_id: Ulid::from_bytes([4u8; 16]),
            sealed_refs: Vec::new(),
            materialized: true,
        };
        mint.start(None).expect("start builds");

        assert_eq!(
            mint.step(read(Some(record.to_bytes().unwrap())), None)
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
        assert!(!mint.wrote(), "a replay commits nothing");
    }

    #[test]
    fn rejects_reused_mutation() {
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        let record = PolicyMutationRecord {
            mutation_id: Ulid::from_bytes([8u8; 16]),
            params: plan(Vec::new(), PolicyRefMode::Replace).params(),
            successor_version_id: Ulid::from_bytes([4u8; 16]),
            sealed_refs: Vec::new(),
            materialized: true,
        };
        mint.start(None).expect("start builds");

        assert_eq!(
            mint.step(read(Some(record.to_bytes().unwrap())), None),
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
        mint.start(None).expect("start builds");
        mint.step(read(None), None).expect("mutation absent");
        let moved = CurrentVersionPointer::new_with_generation(Ulid::from_bytes([1u8; 16]), 5);

        assert_eq!(
            mint.step(read(Some(moved.to_bytes().unwrap())), None),
            Err(SuccessorError::HeadConflict {
                current: Some(moved)
            })
        );
    }

    #[test]
    fn rejects_changed_bucket() {
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Replace));
        mint.start(None).expect("start builds");
        mint.step(read(None), None).expect("mutation absent");
        mint.step(read(Some(head().to_bytes().unwrap())), None)
            .expect("head matches");
        let mut recreated = bucket();
        recreated.created_at = UNIX_EPOCH + std::time::Duration::from_secs(9);

        assert_eq!(
            mint.step(read(Some(recreated.to_bytes().unwrap())), None),
            Err(SuccessorError::BucketChanged)
        );
    }

    #[test]
    fn rejects_changed_default() {
        // A run's target is bound into its own transaction: a default that moved
        // on supersedes the run before any old target commits.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Union));
        mint.plan.sealed_default = Some(SealedDefault {
            generation: 2,
            refs: Vec::new(),
        });
        mint.start(None).expect("start builds");
        mint.step(read(None), None).expect("mutation absent");
        mint.step(read(Some(head().to_bytes().unwrap())), None)
            .expect("head matches");

        assert_eq!(
            mint.step(read(Some(bucket().to_bytes().unwrap())), None),
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
        mint.start(None).expect("start builds");
        mint.step(read(None), None).expect("mutation absent");
        mint.step(read(Some(head().to_bytes().unwrap())), None)
            .expect("head matches");
        mint.step(read(Some(bucket().to_bytes().unwrap())), None)
            .expect("bucket matches");
        mint.step(read(Some(materialized(Vec::new()).to_bytes().unwrap())), None)
            .expect("version decodes");

        assert_eq!(
            mint.step(read(Some(materialized(Vec::new()).to_bytes().unwrap())), None),
            Err(SuccessorError::VersionCollision(Ulid::from_bytes([9u8; 16])))
        );
    }

    #[test]
    fn rejects_foreign_intent() {
        // Another pass owns this object's intent, so this one may not write.
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Union));
        mint.plan.intent = Some(intent());
        let mut stored = intent();
        stored.successor_version_id = Ulid::from_bytes([4u8; 16]);
        mint.start(None).expect("start builds");
        mint.step(read(None), None).expect("mutation absent");

        assert_eq!(
            mint.step(read(Some(stored.to_bytes().unwrap())), None),
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
        mint.start(None).expect("start builds");
        mint.step(read(None), None).expect("mutation absent");

        assert_eq!(
            mint.step(read(Some(stored.to_bytes().unwrap())), None),
            Err(SuccessorError::IntentConflict)
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

        let Effect::Storage(StorageEffect::Iter { key_space, .. }) = effect else {
            panic!("expected the managed-copy scan");
        };
        assert_eq!(key_space, MANAGED_COPY_KEYSPACE);
        assert!(mint.sealed_refs.contains(&existing.policy_ref()));
        assert!(mint.sealed_refs.contains(&added.policy_ref()));
    }

    #[test]
    fn replace_can_relax() {
        // The realm-admin mutation is an exact replacement, unlike the bulk union.
        let existing = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(Vec::new(), PolicyRefMode::Replace));
        drive_to_copy(&mut mint, &materialized(vec![existing.policy_ref()]));

        assert!(mint.sealed_refs.is_empty());
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

        assert_eq!(mint.step(copies(Vec::new()), None).expect("scan decides"), None);
        assert_eq!(
            mint.outcome(),
            Some(&SuccessorOutcome::Blocked(
                PolicyBlockedReason::SourceUnavailable
            ))
        );
        assert!(!mint.wrote(), "a blocked mutation without a run writes nothing");
    }

    #[test]
    fn blocks_quarantined_copy() {
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        let row = copy_row(Vec::new(), ManagedCopyState::UnresolvedDeparted);

        assert_eq!(
            mint.step(copies(vec![row]), None).expect("scan decides"),
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
            mint.step(copies(vec![row]), None).expect("scan decides"),
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
        let effect = first_effect(mint.step(copies(Vec::new()), None).expect("scan decides"));
        let writes = batch_writes(effect);

        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, POLICY_BULK_INTENT_KEYSPACE);
        let stored = PolicyBulkIntent::from_bytes(&writes[0].2).expect("intent decodes");
        assert_eq!(
            stored.outcome,
            PolicyIntentOutcome::Blocked(PolicyBlockedReason::SourceUnavailable)
        );
        assert!(mint.wrote(), "the blocked reason has to commit");
    }

    #[test]
    fn mints_from_copy() {
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        drive_to_copy(&mut mint, &materialized(Vec::new())).expect("copy scan follows");
        let row = copy_row(Vec::new(), ManagedCopyState::Registered);

        let effect = first_effect(mint.step(copies(vec![row]), None).expect("scan decides"));
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
        // The head is read and written under one exact expected pointer, so the
        // successor advances one generation and records no contender.
        assert_eq!(pointer.generation, head().generation + 1);
        assert!(
            !spaces.contains(&BLOB_HEAD_CONTENDER_KEYSPACE),
            "a compare-and-set advance cannot contend with itself"
        );
    }

    #[test]
    fn reference_skips_registration() {
        // A reference claims no managed copy, so it registers nothing.
        let policy = verified(1, Some(node_id()));
        let mut mint = SuccessorMint::new(plan(vec![policy.policy_ref()], PolicyRefMode::Replace));
        mint.plan.resolved = resolution(&policy);
        let reference = BlobVersion::reference(
            aruna_core::structs::VersionSourceBinding {
                strategy: aruna_core::structs::StagingStrategy::Reference,
                descriptor: aruna_core::structs::PortableSourceDescriptor {
                    kind: aruna_core::structs::SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "source".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                },
                connector_id: None,
            },
            aruna_core::structs::SourceMetadata {
                content_length: 1,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
            },
            UNIX_EPOCH,
            user_id(),
            UNIX_EPOCH,
        );
        let effect = drive_to_copy(&mut mint, &reference).expect("writes follow");
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
