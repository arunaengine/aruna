//! Publishes a dirty stage as one S3 version in one foreground transaction with its receipt
//! and stage removal, and settles the last close of an instance: publish, delete or retain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::handles::{Held, advance_witness, rests_on};
use super::io::stage_key;
use super::mutate::{RemoveInput, VfsRemoveOperation};
use super::policy::{VfsAccess, VfsAccessContext, VfsGrant, VfsRequest, authorize_call, grant_of};
use super::recovery::{ReceiptInput, ReceiptOutcome, VfsReceiptOperation};
use super::{
    VfsError, record_of, state_delete, state_key, state_read, state_row, unexpected, vfs_call,
    vfs_event, watch_key,
};
use crate::replication::live_queue::{CompletePutInput, CompletePutOperation};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectOperation, PutObjectResult, RowFence,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, NFS_STATE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::vfs::{
    BucketScope, DetachReason, DirectoryWitness, FileIdentity, HandleIdentity, NfsStageRecord,
    QuotaRetry, ReceiptChange, StageBase, StageDisposition, StageOwner, StageScope, VfsHandleRow,
    VfsKey, VfsReceipt, encode_handle_id, encode_token,
};
use aruna_core::types::{Key, Value};
use aruna_core::vfs_effects::{CloseSummary, VfsEffect, VfsEvent, WitnessUpdate};
use ulid::Ulid;

/// First wait of a quota-blocked checkpoint; each retry doubles it up to [`QUOTA_WAIT_MAX`].
pub const QUOTA_WAIT_MS: u64 = 60_000;
pub const QUOTA_WAIT_MAX: u64 = 60 * 60 * 1000;

/// Whether a remaining stage may pass from one owner scope to another: never across attempts,
/// and never from a user to an attempt, which would shorten acknowledged user retention.
pub fn may_transfer(from: &StageScope, to: &StageScope) -> bool {
    match (from, to) {
        (StageScope::User, StageScope::User) => true,
        (StageScope::Attempt { job_id: from }, StageScope::Attempt { job_id: to }) => from == to,
        (StageScope::Attempt { .. }, StageScope::User) => true,
        (StageScope::User, StageScope::Attempt { .. }) => false,
    }
}

/// The stage as `next` owns it at the same generation, or `None` when its scope may not take
/// the stage over.
pub fn handed_over(
    stage: &NfsStageRecord,
    next: StageOwner,
    now_ms: u64,
) -> Option<NfsStageRecord> {
    if !may_transfer(&stage.owner.scope, &next.scope) {
        return None;
    }
    let mut moved = stage.clone();
    moved.owner = next;
    moved.updated_at_ms = now_ms;
    Some(moved)
}

/// The next retry of a writerless checkpoint the quota refused.
pub fn next_retry(previous: Option<QuotaRetry>, now_ms: u64) -> QuotaRetry {
    let attempts = previous.map_or(0, |retry| retry.attempts).saturating_add(1);
    let shift = attempts.saturating_sub(1).min(16);
    let wait = QUOTA_WAIT_MS.saturating_mul(1 << shift).min(QUOTA_WAIT_MAX);
    QuotaRetry {
        attempts,
        next_at_ms: now_ms.saturating_add(wait),
    }
}

/// Everything one publication needs besides its prepared write.
#[derive(Clone, Debug, PartialEq)]
pub struct PublishInput {
    pub context: VfsAccessContext,
    pub instance_id: u64,
    pub key: String,
    pub handle_id: Option<u64>,
    pub base: StageBase,
    /// A full independent replacement fences only the life, not the captured version.
    pub replaces: bool,
    /// The live `06` row the publication removes, as it was read.
    pub stage: Option<NfsStageRecord>,
    pub logical_size: u64,
    pub attempt_id: Ulid,
    /// Ancestor witnesses that rest on this name, advanced with the publication.
    pub updates: Vec<WitnessUpdate>,
    /// The edit generation the streamed image was read at; a later edit or another size under
    /// the guard refuses the publication, and the overlay clears only up to this edit.
    pub expected_edit: Option<u64>,
    /// A writerless retry refuses when a writer returned or the instance left `stage`.
    pub require_writerless: bool,
}

/// How a publication ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub version_id: Ulid,
    pub life_id: Option<Ulid>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublishStep {
    Authorize,
    Permit,
    Guard,
    Wait,
    Instance,
    Rows,
    Attempt,
    Put,
    Synced,
    Receipt,
    Finalized,
}

type PublishTurn = Result<Next<Published>, VfsError>;

/// Publishes one stage: re-admission, the namespace permit and file guard, a registered
/// attempt, one `PutObjectOperation` transaction whose fence guards and removes the stage row
/// and writes the receipt, a sync, then the receipt finalizer.
#[derive(Debug, PartialEq)]
pub struct VfsPublishOperation {
    input: PublishInput,
    put: Option<PutObjectConfig>,
    step: PublishStep,
    grant: Option<VfsGrant>,
    held: Held,
    guards: Vec<(String, aruna_core::types::Key, Option<Value>)>,
    /// The nfs rows as read, which a new name's publication rebinds in its transaction.
    rows: Vec<(VfsKey, Option<Value>)>,
    version_id: Option<Ulid>,
    begun: bool,
    unknown: bool,
    now_ms: u64,
    ids: IdMint,
}

impl VfsPublishOperation {
    /// `put` is the prepared write: its routing, quota ceiling and the stage image as its body
    /// stream, which the frontend reads in bounded chunks outside the namespace permit.
    pub fn new(input: PublishInput, put: PutObjectConfig) -> Self {
        Self {
            input,
            put: Some(put),
            step: PublishStep::Authorize,
            grant: None,
            held: Held::default(),
            guards: Vec::new(),
            rows: Vec::new(),
            version_id: None,
            begun: false,
            unknown: false,
            now_ms: 0,
            ids: IdMint::default(),
        }
    }

    /// The nfs rows the publication reads and guards. A new name also reads its reverse row
    /// and the ancestor handles whose witnesses rest on it, which its transaction advances.
    fn row_keys(&self) -> Result<Vec<VfsKey>, VfsError> {
        let scope = self.scope()?;
        let mut keys = vec![VfsKey::Receipt(self.input.attempt_id)];
        if let Some(stage) = &self.input.stage {
            keys.push(stage_key(&scope, stage));
        }
        if let Some(handle_id) = self.input.handle_id {
            keys.push(VfsKey::Handle(scope.clone(), handle_id));
        }
        if !matches!(self.input.base, StageBase::Unpublished { .. }) {
            return Ok(keys);
        }
        if self.input.handle_id.is_some() {
            keys.push(VfsKey::Name(scope.clone(), self.input.key.clone()));
        }
        let ancestors = self.input.updates.iter();
        let ancestors = ancestors.filter_map(|update| update.handle_id);
        keys.extend(ancestors.map(|handle_id| VfsKey::Handle(scope.clone(), handle_id)));
        Ok(keys)
    }

    /// The handle and witness rows that bind a new name to its first life, written with the
    /// version; the open witnesses learn the same advance after the commit.
    fn bound_rows(&mut self, life_id: Ulid) -> Result<Vec<(String, Key, Value)>, VfsError> {
        let key = self.input.key.clone();
        let replacement = DirectoryWitness::Descendant {
            key: key.clone(),
            life_id,
        };
        let mut writes = Vec::new();
        for (row_key, value) in &self.rows {
            if let (VfsKey::Name(..), None, Some(handle_id)) =
                (row_key, value, self.input.handle_id)
            {
                let name = Value::from(encode_handle_id(handle_id).to_vec());
                writes.push((NFS_STATE_KEYSPACE.to_string(), state_key(row_key)?, name));
                continue;
            }
            let VfsKey::Handle(_, handle_id) = row_key else {
                continue;
            };
            let Some(VfsHandleRow::Live(mut live)) = record_of::<VfsHandleRow>(value.as_ref())?
            else {
                continue;
            };
            let advanced = match &live.identity {
                HandleIdentity::File(_) if Some(*handle_id) == self.input.handle_id => {
                    Some(HandleIdentity::File(FileIdentity::Committed { life_id }))
                }
                HandleIdentity::Directory(witness) if rests_on(witness, &key) => {
                    advance_witness(witness, &key, Some(&replacement))
                        .map(HandleIdentity::Directory)
                }
                _ => None,
            };
            let Some(identity) = advanced.filter(|identity| *identity != live.identity) else {
                continue;
            };
            if let HandleIdentity::Directory(witness) = &identity {
                let updates = self.input.updates.iter_mut();
                for update in updates.filter(|update| update.handle_id == Some(*handle_id)) {
                    update.witness = witness.clone();
                }
            }
            live.identity = identity;
            writes.push(state_row(row_key, &VfsHandleRow::Live(live))?);
        }
        Ok(writes)
    }

    fn call(&mut self, step: PublishStep, call: IoCall) -> PublishTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        Ok(BucketScope::new(bucket, grant.incarnation))
    }

    fn guard(&mut self) -> PublishTurn {
        let scope = self.scope()?;
        let instances = vec![self.input.instance_id];
        let effect = VfsEffect::AcquireGuards { scope, instances };
        self.call(PublishStep::Guard, vfs_call(effect))
    }

    /// Under the guard, the instance must still be at the edit and size the image was read at,
    /// and a writerless retry must still find no writer on the same stage generation.
    fn guarded(&mut self) -> PublishTurn {
        if self.input.expected_edit.is_none() && !self.input.require_writerless {
            return self.read_rows();
        }
        let scope = self.scope()?;
        let effect = match self.input.handle_id {
            Some(handle_id) => VfsEffect::LookupHandle { scope, handle_id },
            None => VfsEffect::LookupInstance {
                scope,
                key: self.input.key.clone(),
            },
        };
        self.call(PublishStep::Instance, vfs_call(effect))
    }

    fn instance_read(&mut self, result: IoResult) -> PublishTurn {
        let summary = match vfs_event(result)? {
            VfsEvent::Instance(Some(summary)) if summary.instance_id == self.input.instance_id => {
                summary
            }
            VfsEvent::Instance(_) => return Err(VfsError::Conflict),
            other => return Err(unexpected(&other)),
        };
        let edited = self.input.expected_edit.is_some_and(|expected| {
            summary.edit_generation != expected || summary.logical_size != self.input.logical_size
        });
        let moved = self.input.stage.as_ref().is_some_and(|stage| {
            summary.stage_id != Some(stage.stage_id) || summary.generation != stage.generation
        });
        let written = self.input.require_writerless && (summary.writer_count > 0 || moved);
        if edited || written {
            return Err(VfsError::Conflict);
        }
        self.read_rows()
    }

    /// Reads the stage, handle and receipt rows the transaction must still find as they are.
    fn read_rows(&mut self) -> PublishTurn {
        let keys = self.row_keys()?;
        self.call(PublishStep::Rows, state_read(&keys)?)
    }

    fn rows_read(&mut self, result: IoResult) -> PublishTurn {
        let values = result.values()?;
        let scope = self.scope()?;
        let keys = self.row_keys()?;
        if values.len() != keys.len() {
            return Err(VfsError::Conflict);
        }
        if let Some(stage) = &self.input.stage {
            let current = record_of::<NfsStageRecord>(values.get(1).and_then(Option::as_ref))?;
            // The stage generation this publication streams must still be the live one.
            if current.as_ref() != Some(stage) {
                return Err(VfsError::Conflict);
            }
        }
        if values.first().is_some_and(Option::is_some) {
            return Err(VfsError::Conflict);
        }
        let space = NFS_STATE_KEYSPACE.to_string();
        self.guards = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| Ok((space.clone(), state_key(key)?, value.clone())))
            .collect::<Result<_, VfsError>>()?;
        self.rows = keys.into_iter().zip(values).collect();
        if let StageBase::Unpublished { token, .. } = self.input.base {
            let watch = watch_key(&scope.bucket, &self.input.key)?;
            let value = Value::from(encode_token(token).to_vec());
            self.guards
                .push((BLOB_HEAD_KEYSPACE.to_string(), watch, Some(value)));
        }
        let effect = VfsEffect::BeginAttempt {
            scope,
            attempt_id: self.input.attempt_id,
            instances: vec![self.input.instance_id],
        };
        self.call(PublishStep::Attempt, vfs_call(effect))
    }

    fn receipt(&self, version_id: Ulid, fresh: Option<Ulid>) -> Result<VfsReceipt, VfsError> {
        let life_id = match self.input.base {
            StageBase::Committed { life_id, .. } => life_id,
            // A new name takes the life minted for its publishing transaction.
            StageBase::Unpublished { .. } => fresh.unwrap_or_default(),
        };
        Ok(VfsReceipt {
            attempt_id: self.input.attempt_id,
            scope: self.scope()?,
            created_at_ms: self.now_ms,
            change: ReceiptChange::FilePublish {
                key: self.input.key.clone(),
                version_id,
                life_id,
                handle_id: self.input.handle_id,
                stage_id: self.input.stage.as_ref().map(|stage| stage.stage_id),
            },
        })
    }

    fn dispatch(&mut self) -> PublishTurn {
        let mut config = self.put.take().ok_or(VfsError::Conflict)?;
        let version_id = *config
            .preassigned_version_id
            .get_or_insert(self.input.attempt_id);
        self.version_id = Some(version_id);
        let fresh = match self.input.base {
            StageBase::Unpublished { .. } => Some(self.ids.mint(self.now_ms)),
            StageBase::Committed { .. } => None,
        };
        let receipt = self.receipt(version_id, fresh)?;
        let scope = self.scope()?;
        let mut writes = vec![state_row(
            &VfsKey::Receipt(self.input.attempt_id),
            &receipt,
        )?];
        if let Some(life_id) = fresh {
            writes.extend(self.bound_rows(life_id)?);
        }
        let deletes = match &self.input.stage {
            Some(stage) => vec![state_delete(&stage_key(&scope, stage))?],
            None => Vec::new(),
        };
        let fence = RowFence::new(std::mem::take(&mut self.guards), Vec::new())
            .with_writes(writes)
            .with_deletes(deletes);
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let mut put = PutObjectOperation::new(config)
            .with_bucket_guard(grant.bucket.clone())
            .with_preserve_envelope(true)
            .with_row_fence(fence);
        put = match self.input.base {
            StageBase::Committed { life_id, .. } if self.input.replaces => {
                put.with_expected_life(Some(life_id))
            }
            StageBase::Committed {
                version_id,
                life_id,
            } => put
                .with_expected_life(Some(life_id))
                .with_expected_head(Some(version_id)),
            StageBase::Unpublished { predecessor, .. } => put
                .with_expected_life(None)
                .with_expected_head(predecessor)
                .with_life(fresh.ok_or(VfsError::Conflict)?),
        };
        self.begun = true;
        self.call(PublishStep::Put, IoCall::child(put))
    }

    /// A definite refusal changes nothing; any other failure may have committed, so the
    /// receipt decides after a sync of the foreground queue.
    fn put_done(&mut self, result: IoResult) -> PublishTurn {
        match result.child::<PutObjectResult, PutObjectError>()? {
            Ok(_) => self.call(PublishStep::Synced, IoCall::Sync),
            // `abort` finishes the attempt of a definite refusal and releases the rest.
            Err(error) => match refusal(&error) {
                Some(refused) => Err(refused),
                None => {
                    self.unknown = true;
                    self.call(PublishStep::Synced, IoCall::Sync)
                }
            },
        }
    }

    fn finalize(&mut self) -> PublishTurn {
        let input = ReceiptInput {
            scope: self.scope()?,
            attempt_id: self.input.attempt_id,
            instance_id: Some(self.input.instance_id),
            updates: self.input.updates.clone(),
            logical_size: Some(self.input.logical_size),
            expected_edit: self.input.expected_edit,
        };
        self.call(
            PublishStep::Receipt,
            IoCall::child(VfsReceiptOperation::new(input)),
        )
    }

    fn finalized(&mut self, result: IoResult) -> PublishTurn {
        let outcome = result.child::<ReceiptOutcome, VfsError>()??;
        self.unknown = false;
        self.begun = false;
        let version_id = self.version_id.ok_or(VfsError::Conflict)?;
        let output = match outcome {
            ReceiptOutcome::Applied { life_id } => Published {
                version_id,
                life_id,
            },
            ReceiptOutcome::RolledBack => return Err(VfsError::Conflict),
        };
        self.step = PublishStep::Finalized;
        let mut calls = self.held.release();
        calls.push(self.completion(version_id)?);
        Ok(Next::Then { output, calls })
    }

    /// The ordinary post-PUT completion of a committed publication, after the permit and guard
    /// go: upload watchers and the prompt live replication queue, as for an S3 PUT.
    fn completion(&self, version_id: Ulid) -> Result<IoCall, VfsError> {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let context = &self.input.context;
        let complete = CompletePutOperation::new(CompletePutInput {
            realm_id: context.realm_id,
            node_id: context.node_id,
            auth: grant.context.clone(),
            group_id: grant.bucket.group_id,
            bucket: self.scope()?.bucket,
            key: self.input.key.clone(),
            version_id,
            size_bytes: self.input.logical_size,
        });
        Ok(IoCall::child(complete))
    }
}

/// A refusal before the commit, as the error the caller reports; `None` when the commit may
/// have happened.
pub(crate) fn refusal(error: &PutObjectError) -> Option<VfsError> {
    match error {
        PutObjectError::HeadChanged
        | PutObjectError::FenceTaken
        | PutObjectError::ObjectExists
        | PutObjectError::VersionExists
        | PutObjectError::StorageError(StorageError::TransactionConflict) => {
            Some(VfsError::Conflict)
        }
        PutObjectError::QuotaExceeded { .. } | PutObjectError::QuotaGateError(_) => {
            Some(VfsError::QuotaExceeded)
        }
        PutObjectError::StorageError(_) => None,
        other => match other.to_string() {
            text if text.to_ascii_lowercase().contains("quarantin") => Some(VfsError::Quarantined),
            text => Some(VfsError::Storage(text)),
        },
    }
}

impl Operation for VfsPublishOperation {
    type Output = Published;
    type Error = VfsError;

    fn first(&mut self) -> PublishTurn {
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let mut request = VfsRequest::new(VfsAccess::Put, bucket, Some(&self.input.key));
        request.length = Some(self.input.logical_size);
        let call = authorize_call(&self.input.context, request);
        self.call(PublishStep::Authorize, call)
    }

    fn next(&mut self, result: IoResult) -> PublishTurn {
        match self.step {
            PublishStep::Authorize => {
                self.grant = Some(grant_of(result)?);
                let scope = self.scope()?;
                self.call(
                    PublishStep::Permit,
                    vfs_call(VfsEffect::AcquirePermit { scope }),
                )
            }
            PublishStep::Permit => {
                self.held.permit_taken(result)?;
                self.guard()
            }
            PublishStep::Guard => match self.held.guard_taken(result)? {
                Some(attempt_id) => self.call(
                    PublishStep::Wait,
                    vfs_call(VfsEffect::WaitAttempt { attempt_id }),
                ),
                None => self.guarded(),
            },
            PublishStep::Wait => {
                vfs_event(result)?;
                self.guard()
            }
            PublishStep::Instance => self.instance_read(result),
            PublishStep::Rows => self.rows_read(result),
            PublishStep::Attempt => match vfs_event(result)? {
                VfsEvent::Done => self.dispatch(),
                other => Err(unexpected(&other)),
            },
            PublishStep::Put => self.put_done(result),
            PublishStep::Synced => {
                result.done()?;
                self.finalize()
            }
            PublishStep::Receipt => self.finalized(result),
            PublishStep::Finalized => Err(VfsError::Conflict),
        }
    }

    /// Releases what the run holds. An attempt whose outcome is unknown stays registered, so
    /// later callers wait for its receipt to be resolved.
    fn abort(&mut self) -> Vec<IoCall> {
        let resolved = (self.begun && !self.unknown).then_some(self.input.attempt_id);
        self.held.abandon(resolved)
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

/// How a stage row changes after a close, compared against the row as it was read.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StageMoveOperation {
    scope: BucketScope,
    expected: NfsStageRecord,
    /// The row that replaces it, under its own key; `None` removes the stage.
    replacement: Option<NfsStageRecord>,
    step: MoveStep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MoveStep {
    Read,
    Deleted,
    Written,
}

impl StageMoveOperation {
    pub(crate) fn new(
        scope: BucketScope,
        expected: NfsStageRecord,
        replacement: Option<NfsStageRecord>,
    ) -> Self {
        Self {
            scope,
            expected,
            replacement,
            step: MoveStep::Read,
        }
    }
}

impl Operation for StageMoveOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<()>, VfsError> {
        Ok(Next::Call(state_read(&[stage_key(
            &self.scope,
            &self.expected,
        )])?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, VfsError> {
        match self.step {
            MoveStep::Read => {
                let [row] = aruna_core::rows::values::<1>(result)?;
                if record_of::<NfsStageRecord>(row.as_ref())?.as_ref() != Some(&self.expected) {
                    return Err(VfsError::Conflict);
                }
                self.step = MoveStep::Deleted;
                let key = stage_key(&self.scope, &self.expected);
                Ok(Next::Call(IoCall::Delete(vec![state_delete(&key)?])))
            }
            MoveStep::Deleted => {
                result.done()?;
                let Some(replacement) = &self.replacement else {
                    return Ok(Next::Done(()));
                };
                self.step = MoveStep::Written;
                let key = stage_key(&self.scope, replacement);
                Ok(Next::Call(IoCall::Write(vec![state_row(
                    &key,
                    replacement,
                )?])))
            }
            MoveStep::Written => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// The retained copy of a live checkpoint, kept outside the namespace by its stage id.
pub fn retained(stage: &NfsStageRecord, reason: DetachReason, now_ms: u64) -> NfsStageRecord {
    let mut kept = stage.clone();
    kept.disposition = StageDisposition::Retained { reason };
    kept.quota_retry = None;
    kept.updated_at_ms = now_ms;
    kept
}

/// The live checkpoint with its next quota retry after one more refusal.
fn rescheduled(stage: &NfsStageRecord, now_ms: u64) -> NfsStageRecord {
    let mut waiting = stage.clone();
    waiting.quota_retry = Some(next_retry(stage.quota_retry, now_ms));
    waiting.updated_at_ms = now_ms;
    waiting
}

/// How a quota retry changes its writerless checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuotaChange {
    Reschedule,
    Retain(DetachReason),
}

/// Settles one quota retry of a writerless checkpoint: a still-full group moves its next retry
/// out, a name that can no longer take it keeps it outside the namespace by its stage id. Both
/// compare the row against the record as read, so a concurrent change refuses with a conflict.
#[derive(Debug, PartialEq)]
pub struct QuotaStageOperation {
    scope: BucketScope,
    stage: NfsStageRecord,
    change: QuotaChange,
    now_ms: u64,
    /// The live instance that still holds the checkpoint, detached once it is retained.
    instance_id: Option<u64>,
    synced: bool,
}

impl QuotaStageOperation {
    /// The group is still full: the checkpoint stays live with its next retry backed off.
    pub fn reschedule(scope: BucketScope, stage: NfsStageRecord) -> Self {
        Self {
            scope,
            stage,
            change: QuotaChange::Reschedule,
            now_ms: 0,
            instance_id: None,
            synced: false,
        }
    }

    /// The name can no longer take the checkpoint: it is retained for recovery.
    pub fn retain(scope: BucketScope, stage: NfsStageRecord, reason: DetachReason) -> Self {
        Self {
            change: QuotaChange::Retain(reason),
            ..Self::reschedule(scope, stage)
        }
    }

    /// The live instance of the checkpoint, which a synced retention detaches and releases.
    pub fn with_instance(mut self, instance_id: u64) -> Self {
        self.instance_id = Some(instance_id);
        self
    }

    fn stage_move(&self) -> StageMoveOperation {
        let replacement = match self.change {
            QuotaChange::Reschedule => rescheduled(&self.stage, self.now_ms),
            QuotaChange::Retain(reason) => retained(&self.stage, reason, self.now_ms),
        };
        StageMoveOperation::new(self.scope.clone(), self.stage.clone(), Some(replacement))
    }
}

impl Operation for QuotaStageOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<()>, VfsError> {
        let mut seed = self.stage.stage_id.to_bytes().to_vec();
        seed.extend(self.stage.generation.to_be_bytes());
        Ok(Next::Call(IoCall::retry(self.stage_move(), &seed)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, VfsError> {
        let (QuotaChange::Retain(reason), Some(instance_id)) = (self.change, self.instance_id)
        else {
            result.child::<(), VfsError>()??;
            return Ok(Next::Done(()));
        };
        match self.synced {
            false => {
                result.child::<(), VfsError>()??;
                self.synced = true;
                Ok(Next::Call(IoCall::Sync))
            }
            true => {
                result.done()?;
                let retain = VfsEffect::RetainInstance {
                    scope: self.scope.clone(),
                    instance_id,
                    stage_id: self.stage.stage_id,
                    generation: self.stage.generation,
                    reason,
                };
                Ok(Next::Then {
                    output: (),
                    calls: vec![vfs_call(retain)],
                })
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

/// What the last close of an open did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloseOutcome {
    /// Other opens remain, or nothing was dirty.
    Closed,
    Published(Published),
    /// The delete-pending name was removed without publishing its stage.
    Deleted,
    /// The acknowledged checkpoint was kept for recovery by its stage id.
    Retained(Ulid),
    /// The writerless checkpoint stays live and retries once quota frees.
    QuotaBlocked,
    /// A proven unlink discarded the detached stage.
    Discarded,
    /// Dirty volatile bytes remain and the frontend has not prepared their publication.
    Pending,
}

/// One close of one open.
#[derive(Debug, PartialEq)]
pub struct CloseInput {
    pub context: VfsAccessContext,
    pub scope: BucketScope,
    pub open_id: Ulid,
    /// The publication the frontend prepared in case this is the last dirty writer.
    pub publish: Option<(PublishInput, PutObjectConfig)>,
    /// The live checkpoint row of the instance, as last read.
    pub stage: Option<NfsStageRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloseStep {
    Close,
    Successor,
    Publish,
    Remove,
    Stage,
    Synced,
    Removed,
}

type CloseTurn = Result<Next<CloseOutcome>, VfsError>;

/// Closes one open. The last close publishes dirty bytes, completes a pending delete as its
/// authorized disposition owner without publishing, keeps an acknowledged checkpoint when the
/// name can no longer take it and records a quota retry for a full group.
#[derive(Debug, PartialEq)]
pub struct VfsCloseOperation {
    input: CloseInput,
    step: CloseStep,
    summary: Option<CloseSummary>,
    outcome: Option<CloseOutcome>,
    now_ms: u64,
    /// The prepared parent marker write a final delete passes on to its removal.
    marker: Option<PutObjectConfig>,
    /// The authorized writer that takes the stage when its owner closes while it stays open.
    successor: Option<StageOwner>,
    /// Why the stage was retained, which the synced move passes on to its instance.
    reason: Option<DetachReason>,
}

impl VfsCloseOperation {
    pub fn new(input: CloseInput) -> Self {
        Self {
            input,
            step: CloseStep::Close,
            summary: None,
            outcome: None,
            now_ms: 0,
            marker: None,
            successor: None,
            reason: None,
        }
    }

    /// The zero-length write of the parent marker, used when a final delete removes the last
    /// child of an implicit directory.
    pub fn with_parent_marker(mut self, put: PutObjectConfig) -> Self {
        self.marker = Some(put);
        self
    }

    /// A remaining writer with its own live scope, binding and credential, which takes the
    /// stage when the closing open owns it.
    pub fn with_successor(mut self, owner: StageOwner) -> Self {
        self.successor = Some(owner);
        self
    }

    /// The stage owner left while another writer stays: the stage moves to the earliest
    /// remaining writer at its exact generation once that writer's own credential is admitted
    /// to write the name and the scopes allow it, else it is retained for recovery.
    fn writer_left(&mut self) -> CloseTurn {
        let Some(stage) = self.input.stage.clone() else {
            return Ok(Next::Done(CloseOutcome::Closed));
        };
        if stage.owner.access_key != self.input.context.access_key {
            return Ok(Next::Done(CloseOutcome::Closed));
        }
        let successor = self.successor.take();
        let summary = self.summary.as_ref().ok_or(VfsError::Conflict)?;
        let (key, length) = (summary.key.clone(), summary.logical_size);
        let successor = successor.or_else(|| summary.successor_owner.clone());
        if successor.as_ref() == Some(&stage.owner) {
            return Ok(Next::Done(CloseOutcome::Closed));
        }
        let Some(next) = successor.filter(|next| may_transfer(&stage.owner.scope, &next.scope))
        else {
            return self.retain(DetachReason::LostLife);
        };
        let mut context = self.input.context.clone();
        context.access_key = next.access_key.clone();
        let bucket = self.input.scope.bucket.clone();
        let mut request = VfsRequest::new(VfsAccess::Put, &bucket, Some(&key));
        request.length = Some(length);
        self.successor = Some(next);
        self.call(CloseStep::Successor, authorize_call(&context, request))
    }

    fn successor_admitted(&mut self, result: IoResult) -> CloseTurn {
        let stage = self.input.stage.clone().ok_or(VfsError::Conflict)?;
        let next = self.successor.take().ok_or(VfsError::Conflict)?;
        match grant_of(result) {
            Ok(_) => {}
            Err(VfsError::Denied | VfsError::ReadOnly | VfsError::VaultLocked) => {
                return self.retain(DetachReason::LostLife);
            }
            Err(error) => return Err(error),
        }
        match handed_over(&stage, next, self.now_ms) {
            Some(moved) => self.change_stage(Some(moved), CloseOutcome::Closed),
            None => self.retain(DetachReason::LostLife),
        }
    }

    fn call(&mut self, step: CloseStep, call: IoCall) -> CloseTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn closed(&mut self, result: IoResult) -> CloseTurn {
        let summary = match vfs_event(result)? {
            VfsEvent::Closed(summary) => summary,
            other => return Err(unexpected(&other)),
        };
        let last = summary.remaining_opens == 0;
        let writerless = summary.remaining_writers == 0;
        let (pending, dirty, detached) = (summary.delete_pending, summary.dirty, summary.detached);
        self.summary = Some(summary);
        match (last, pending, detached) {
            (true, true, _) => self.final_delete(),
            (true, false, Some(DetachReason::Unlinked)) => self.discard(),
            (true, false, Some(reason)) => self.retain(reason),
            _ if dirty && writerless => self.publish(),
            (false, _, _) if !writerless => self.writer_left(),
            _ => Ok(Next::Done(CloseOutcome::Closed)),
        }
    }

    /// The fenced delete runs as the disposition owner; the dirty stage is never published.
    fn final_delete(&mut self) -> CloseTurn {
        let summary = self.summary.as_ref().ok_or(VfsError::Conflict)?;
        let Some(owner) = summary.disposition_owner.clone() else {
            return self.retain(DetachReason::LostLife);
        };
        let mut context = self.input.context.clone();
        context.access_key = owner.access_key;
        let remove = RemoveInput {
            context,
            key: summary.key.clone(),
            kind: summary.kind,
            handle_id: summary.handle_id,
            identity: summary.identity.clone(),
            instance_id: Some(summary.instance_id),
            attempt_id: self.input.open_id,
        };
        let mut remove = VfsRemoveOperation::new(remove);
        if let Some(put) = self.marker.take() {
            remove = remove.with_parent_marker(put);
        }
        self.call(CloseStep::Remove, IoCall::child(remove))
    }

    fn removed(&mut self, result: IoResult) -> CloseTurn {
        match result.child::<(), VfsError>()? {
            Ok(()) => self.discard_as(CloseOutcome::Deleted),
            // With no durable checkpoint to keep, the failed delete is the close's error.
            Err(error) if self.input.stage.is_none() => Err(error),
            // A lapsed delete authority or a new life leaves the name and keeps the bytes.
            Err(VfsError::Denied | VfsError::StaleHandle | VfsError::Conflict) => {
                self.retain(DetachReason::LostLife)
            }
            Err(error) => Err(error),
        }
    }

    fn publish(&mut self) -> CloseTurn {
        let Some((input, put)) = self.input.publish.take() else {
            return Ok(Next::Done(CloseOutcome::Pending));
        };
        let publish = VfsPublishOperation::new(input, put);
        self.call(CloseStep::Publish, IoCall::child(publish))
    }

    fn published(&mut self, result: IoResult) -> CloseTurn {
        match result.child::<Published, VfsError>()? {
            Ok(published) => Ok(Next::Done(CloseOutcome::Published(published))),
            Err(VfsError::QuotaExceeded) => self.quota_blocked(),
            // With no durable checkpoint to keep, the refused publication is the close's error.
            Err(error) if self.input.stage.is_none() => Err(error),
            Err(
                VfsError::Denied
                | VfsError::StaleHandle
                | VfsError::Conflict
                | VfsError::Quarantined
                | VfsError::ReadOnly
                | VfsError::VaultLocked,
            ) => self.retain(DetachReason::LostLife),
            // A transient failure keeps the operation and the last stable bytes.
            Err(error) => Err(error),
        }
    }

    fn change_stage(
        &mut self,
        replacement: Option<NfsStageRecord>,
        outcome: CloseOutcome,
    ) -> CloseTurn {
        let Some(stage) = self.input.stage.clone() else {
            return Ok(Next::Done(outcome));
        };
        self.outcome = Some(outcome);
        let change = StageMoveOperation::new(self.input.scope.clone(), stage, replacement);
        let seed = self.input.open_id.to_bytes();
        self.call(CloseStep::Stage, IoCall::retry(change, &seed))
    }

    fn retain(&mut self, reason: DetachReason) -> CloseTurn {
        let Some(stage) = self.input.stage.as_ref() else {
            return Ok(Next::Done(CloseOutcome::Closed));
        };
        let kept = retained(stage, reason, self.now_ms);
        let stage_id = kept.stage_id;
        self.reason = Some(reason);
        self.change_stage(Some(kept), CloseOutcome::Retained(stage_id))
    }

    fn quota_blocked(&mut self) -> CloseTurn {
        let Some(stage) = self.input.stage.as_ref() else {
            return Err(VfsError::QuotaExceeded);
        };
        let waiting = rescheduled(stage, self.now_ms);
        self.change_stage(Some(waiting), CloseOutcome::QuotaBlocked)
    }

    fn discard(&mut self) -> CloseTurn {
        self.discard_as(CloseOutcome::Discarded)
    }

    /// Only a proven unlink or final delete discards the stage row and its file.
    fn discard_as(&mut self, outcome: CloseOutcome) -> CloseTurn {
        self.change_stage(None, outcome)
    }

    fn stage_changed(&mut self, result: IoResult) -> CloseTurn {
        result.child::<(), VfsError>()??;
        self.call(CloseStep::Synced, IoCall::Sync)
    }

    fn synced(&mut self, result: IoResult) -> CloseTurn {
        result.done()?;
        let outcome = self.outcome.take().ok_or(VfsError::Conflict)?;
        let removes = matches!(outcome, CloseOutcome::Discarded | CloseOutcome::Deleted);
        match (removes, self.input.stage.as_ref()) {
            (true, Some(stage)) => {
                let (stage_id, generation) = (stage.stage_id, stage.generation);
                self.outcome = Some(outcome);
                let remove = VfsEffect::RemoveCheckpoint {
                    stage_id,
                    generation,
                };
                self.call(CloseStep::Removed, vfs_call(remove))
            }
            // The retained checkpoint leaves the instance, which drops it after its holders.
            (false, Some(stage)) if matches!(outcome, CloseOutcome::Retained(_)) => {
                let summary = self.summary.as_ref().ok_or(VfsError::Conflict)?;
                let retain = VfsEffect::RetainInstance {
                    scope: self.input.scope.clone(),
                    instance_id: summary.instance_id,
                    stage_id: stage.stage_id,
                    generation: stage.generation,
                    reason: self.reason.unwrap_or(DetachReason::LostLife),
                };
                Ok(Next::Then {
                    output: outcome,
                    calls: vec![vfs_call(retain)],
                })
            }
            _ => Ok(Next::Done(outcome)),
        }
    }
}

impl Operation for VfsCloseOperation {
    type Output = CloseOutcome;
    type Error = VfsError;

    fn first(&mut self) -> CloseTurn {
        let (scope, open_id) = (self.input.scope.clone(), self.input.open_id);
        self.call(
            CloseStep::Close,
            vfs_call(VfsEffect::CloseOpen { scope, open_id }),
        )
    }

    fn next(&mut self, result: IoResult) -> CloseTurn {
        match self.step {
            CloseStep::Close => self.closed(result),
            CloseStep::Successor => self.successor_admitted(result),
            CloseStep::Publish => self.published(result),
            CloseStep::Remove => self.removed(result),
            CloseStep::Stage => self.stage_changed(result),
            CloseStep::Synced => self.synced(result),
            CloseStep::Removed => {
                vfs_event(result)?;
                self.outcome
                    .take()
                    .map(Next::Done)
                    .ok_or(VfsError::Conflict)
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
