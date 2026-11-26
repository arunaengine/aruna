//! Namespace changes through the file servers: directory markers, rename and unlink. Each
//! change commits in one write transaction together with its receipt and handle rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::handles::{Held, position_key, retire_rows};
use super::path::{
    NameUse, check_component, creates_cycle, export_holds, marker_key, split_parent,
};
use super::policy::{VfsAccess, VfsAccessContext, VfsGrant, VfsRequest, authorize_call, grant_of};
use super::publish::refusal;
use super::recovery::{ReceiptInput, ReceiptOutcome, VfsReceiptOperation};
use super::{
    STATE_PAGE, VfsError, head_key, head_of, live_life, name_reads, record_of, state_delete,
    state_key, state_read, state_row, token_of, unexpected, vfs_call, vfs_event, watch_key,
    watch_row,
};
use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use crate::s3::object::put::{
    ParentMarker, PutObjectConfig, PutObjectError, PutObjectOperation, PutObjectResult, RangeGuard,
    RowFence, SourceDelete,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, NFS_STATE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
use aruna_core::structs::storage::vfs::{
    BucketScope, DetachReason, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind,
    LiveHandle, NfsStageRecord, ReceiptChange, RetireReason, StageBase, VfsHandleRow, VfsKey,
    VfsReceipt, decode_handle_id, encode_handle_id,
};
use aruna_core::types::{Key, Value};
use aruna_core::vfs_effects::{VfsEffect, VfsEvent, VfsProtocol};
use ulid::Ulid;

/// The refusal of a committed move whose caller prepared no destination write: a committed
/// source or a marker-backed directory moves only in the joint put and delete transaction.
pub const JOINT_MOVE: &str = "a committed source moves only with a joint put and delete";

/// One new directory marker `d/`.
#[derive(Debug, PartialEq)]
pub struct MkdirInput {
    pub context: VfsAccessContext,
    /// The directory key without its trailing slash.
    pub key: String,
    /// The prepared zero-length write of the marker key `d/`.
    pub put: PutObjectConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MkdirStep {
    Authorize,
    Permit,
    Rows,
    Staged,
    Put,
    Synced,
}

type MkdirTurn = Result<Next<Ulid>, VfsError>;

/// Creates a live `d/` marker after checking the name, collisions with a file, a directory or
/// a staged name, and pending deletion. Answers the marker's version id.
#[derive(Debug, PartialEq)]
pub struct VfsMkdirOperation {
    context: VfsAccessContext,
    key: String,
    put: Option<PutObjectConfig>,
    step: MkdirStep,
    grant: Option<VfsGrant>,
    held: Held,
    marker: Option<CurrentVersionPointer>,
    version_id: Option<Ulid>,
}

impl VfsMkdirOperation {
    pub fn new(input: MkdirInput) -> Self {
        Self {
            context: input.context,
            key: input.key,
            put: Some(input.put),
            step: MkdirStep::Authorize,
            grant: None,
            held: Held::default(),
            marker: None,
            version_id: None,
        }
    }

    fn call(&mut self, step: MkdirStep, call: IoCall) -> MkdirTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        scope_of(&self.context, self.grant.as_ref())
    }

    fn rows_read(&mut self, result: IoResult) -> MkdirTurn {
        let [file, marker] = aruna_core::rows::values::<2>(result)?;
        let (file, marker) = (head_of(file.as_ref())?, head_of(marker.as_ref())?);
        if live_life(file.as_ref()).is_some() || live_life(marker.as_ref()).is_some() {
            return Err(VfsError::Exists);
        }
        self.marker = marker;
        let (scope, prefix) = (self.scope()?, self.key.clone());
        self.call(
            MkdirStep::Staged,
            vfs_call(VfsEffect::ListStaged { scope, prefix }),
        )
    }

    fn staged_read(&mut self, result: IoResult) -> MkdirTurn {
        let entries = match vfs_event(result)? {
            VfsEvent::StagedList { entries, .. } => entries,
            other => return Err(unexpected(&other)),
        };
        let below = marker_key(&self.key);
        for entry in entries {
            if entry.key == self.key || entry.key.starts_with(&below) {
                return match entry.delete_pending {
                    true => Err(VfsError::DeletePending),
                    false => Err(VfsError::Exists),
                };
            }
        }
        let config = self.put.take().ok_or(VfsError::Conflict)?;
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let expected = self.marker.as_ref().map(|marker| marker.version_id);
        let put = PutObjectOperation::new(config)
            .with_bucket_guard(grant.bucket.clone())
            .with_expected_life(None)
            .with_expected_head(expected);
        self.call(MkdirStep::Put, IoCall::child(put))
    }

    fn put_done(&mut self, result: IoResult) -> MkdirTurn {
        match result.child::<PutObjectResult, PutObjectError>()? {
            Ok(written) => {
                self.version_id = Some(written.version_id);
                self.call(MkdirStep::Synced, IoCall::Sync)
            }
            Err(PutObjectError::HeadChanged | PutObjectError::ObjectExists) => {
                Err(VfsError::Exists)
            }
            Err(error) => Err(refusal(&error).unwrap_or(VfsError::Unavailable(error.to_string()))),
        }
    }
}

impl Operation for VfsMkdirOperation {
    type Output = Ulid;
    type Error = VfsError;

    fn first(&mut self) -> MkdirTurn {
        let (parent, name) = split_parent(&self.key).ok_or(VfsError::InvalidName)?;
        check_component(
            name,
            self.context.protocol,
            NameUse::Create,
            parent.is_empty(),
        )?;
        let bucket = bucket_of(&self.context)?;
        let marker = marker_key(&self.key);
        let mut request = VfsRequest::new(VfsAccess::Put, &bucket, Some(&marker));
        request.length = Some(0);
        let call = authorize_call(&self.context, request);
        self.call(MkdirStep::Authorize, call)
    }

    fn next(&mut self, result: IoResult) -> MkdirTurn {
        match self.step {
            MkdirStep::Authorize => {
                self.grant = Some(grant_of(result)?);
                let scope = self.scope()?;
                self.call(
                    MkdirStep::Permit,
                    vfs_call(VfsEffect::AcquirePermit { scope }),
                )
            }
            MkdirStep::Permit => {
                self.held.permit_taken(result)?;
                let bucket = bucket_of(&self.context)?;
                let space = || BLOB_HEAD_KEYSPACE.to_string();
                let reads = vec![
                    (space(), head_key(&bucket, &self.key)?),
                    (space(), head_key(&bucket, &marker_key(&self.key))?),
                ];
                self.call(MkdirStep::Rows, IoCall::Read(reads))
            }
            MkdirStep::Rows => self.rows_read(result),
            MkdirStep::Staged => self.staged_read(result),
            MkdirStep::Put => self.put_done(result),
            MkdirStep::Synced => {
                result.done()?;
                let output = self.version_id.ok_or(VfsError::Conflict)?;
                Ok(Next::Then {
                    output,
                    calls: self.held.release(),
                })
            }
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        self.held.release()
    }
}

fn bucket_of(context: &VfsAccessContext) -> Result<String, VfsError> {
    let bucket = context.bucket_scope().ok_or(VfsError::Denied)?.0;
    Ok(bucket.to_string())
}

fn scope_of(context: &VfsAccessContext, grant: Option<&VfsGrant>) -> Result<BucketScope, VfsError> {
    let grant = grant.ok_or(VfsError::Denied)?;
    Ok(BucketScope::new(bucket_of(context)?, grant.incarnation))
}

/// One unlink or empty-directory removal.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoveInput {
    pub context: VfsAccessContext,
    pub key: String,
    pub kind: HandleKind,
    pub handle_id: Option<u64>,
    /// The identity the caller resolved; another life or token refuses the removal.
    pub identity: HandleIdentity,
    /// The shared instance whose opens survive the unlink on their own bytes.
    pub instance_id: Option<u64>,
    pub attempt_id: Ulid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoveStep {
    Authorize,
    Marker,
    Permit,
    Staged,
    Probe,
    Rows,
    Change,
    Synced,
    Receipt,
}

type RemoveTurn = Result<Next<()>, VfsError>;

/// Removes one file or one empty directory. The delete, the detached or retired handle, the
/// reverse row, the old cookie position and the receipt commit in one transaction; surviving
/// opens keep reading their own bytes until the last close.
#[derive(Debug, PartialEq)]
pub struct VfsRemoveOperation {
    input: RemoveInput,
    step: RemoveStep,
    grant: Option<VfsGrant>,
    held: Held,
    begun: bool,
    unknown: bool,
    now_ms: u64,
    ids: aruna_core::id_mint::IdMint,
    /// The prepared write of the source parent's marker, used when it is admitted.
    marker: Option<PutObjectConfig>,
    /// The removal commits as the marker's put with the target's delete marker.
    joint: bool,
}

impl VfsRemoveOperation {
    pub fn new(input: RemoveInput) -> Self {
        Self {
            input,
            step: RemoveStep::Authorize,
            grant: None,
            held: Held::default(),
            begun: false,
            unknown: false,
            now_ms: 0,
            ids: aruna_core::id_mint::IdMint::default(),
            marker: None,
            joint: false,
        }
    }

    /// The zero-length write of the parent marker `p/`, which the removal creates in its own
    /// transaction when it removes the last child of an implicit directory `p`.
    pub fn with_parent_marker(mut self, put: PutObjectConfig) -> Self {
        self.marker = Some(put);
        self
    }

    fn call(&mut self, step: RemoveStep, call: IoCall) -> RemoveTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        scope_of(&self.input.context, self.grant.as_ref())
    }

    /// The key whose head the removal deletes: the file, or the directory's marker.
    fn target(&self) -> String {
        match self.input.kind {
            HandleKind::File => self.input.key.clone(),
            HandleKind::Directory => marker_key(&self.input.key),
        }
    }

    /// A directory with any staged name below it is not empty.
    fn staged_read(&mut self, result: IoResult) -> RemoveTurn {
        let entries = match vfs_event(result)? {
            VfsEvent::StagedList { entries, .. } => entries,
            other => return Err(unexpected(&other)),
        };
        let below = marker_key(&self.input.key);
        if entries.iter().any(|entry| entry.key.starts_with(&below)) {
            return Err(VfsError::NotEmpty);
        }
        let bucket = bucket_of(&self.input.context)?;
        let prefix = BlobHeadKey::object_prefix(&bucket, &below)?;
        self.call(
            RemoveStep::Probe,
            IoCall::Page {
                space: BLOB_HEAD_KEYSPACE.to_string(),
                prefix: prefix.into(),
                after: None,
                limit: STATE_PAGE,
            },
        )
    }

    /// Hidden S3 descendants still make a directory nonempty; only the marker may be live.
    fn probed(&mut self, result: IoResult) -> RemoveTurn {
        let marker = head_key(&bucket_of(&self.input.context)?, &self.target())?;
        if !page_empty(result, &marker)? {
            return Err(VfsError::NotEmpty);
        }
        self.read_rows()
    }

    /// The prepared parent marker write, when admitted and the parent is a directory.
    fn marker_write(&mut self) -> Option<PutObjectConfig> {
        let (parent, _) = split_parent(&self.input.key)?;
        let config = self.marker.take()?;
        let fits = !parent.is_empty() && config.request.key == marker_key(parent);
        fits.then_some(config)
    }

    /// The receipt, handle and reverse rows, then the target's head and watch rows.
    fn read_rows(&mut self) -> RemoveTurn {
        let scope = self.scope()?;
        let mut keys = vec![VfsKey::Receipt(self.input.attempt_id)];
        if let Some(handle_id) = self.input.handle_id {
            keys.push(VfsKey::Handle(scope.clone(), handle_id));
            keys.push(VfsKey::Name(scope.clone(), self.input.key.clone()));
        }
        let IoCall::Read(mut reads) = state_read(&keys)? else {
            return Err(VfsError::Conflict);
        };
        reads.extend(name_reads(&scope.bucket, &self.target())?);
        self.call(RemoveStep::Rows, IoCall::Read(reads))
    }

    /// Builds the one transaction of the removal from the rows as they were read.
    fn rows_read(&mut self, result: IoResult) -> RemoveTurn {
        let mut values = result.values()?;
        let (watch, head) = (values.pop().flatten(), values.pop().flatten());
        if values.first().is_some_and(Option::is_some) {
            return Err(VfsError::Conflict);
        }
        let scope = self.scope()?;
        let target = self.target();
        if !identity_present(&self.input.identity, head.as_ref(), watch.as_ref())? {
            return Err(VfsError::StaleHandle);
        }
        let mut plan = MovePlan::default();
        plan.guard(&VfsKey::Receipt(self.input.attempt_id), None)?;
        if let Some(handle_id) = self.input.handle_id {
            let row = values.get(1).cloned().flatten();
            let name = values.get(2).cloned().flatten();
            plan.guard(&VfsKey::Handle(scope.clone(), handle_id), row.clone())?;
            plan.guard(
                &VfsKey::Name(scope.clone(), self.input.key.clone()),
                name.clone(),
            )?;
            if let Some(VfsHandleRow::Live(live)) = record_of::<VfsHandleRow>(row.as_ref())? {
                let open = self.input.instance_id.is_some();
                let (rows, gone) =
                    unlink_rows(&scope, handle_id, &live, open, self.now_ms, name.as_ref())?;
                plan.writes.extend(rows);
                plan.deletes.extend(gone);
            }
        }
        let committed = !matches!(
            self.input.identity,
            HandleIdentity::File(FileIdentity::Unpublished { .. })
        );
        let mut marker = self.marker_write().filter(|_| committed);
        let marker_version = (marker.as_mut()).map(|config| {
            *config
                .preassigned_version_id
                .get_or_insert(self.input.attempt_id)
        });
        let receipt = VfsReceipt {
            attempt_id: self.input.attempt_id,
            scope: scope.clone(),
            created_at_ms: self.now_ms,
            change: ReceiptChange::Remove {
                key: self.input.key.clone(),
                marker_version,
                handle_id: self.input.handle_id,
            },
        };
        plan.writes.push(state_row(
            &VfsKey::Receipt(self.input.attempt_id),
            &receipt,
        )?);
        self.begun = true;
        let seed = self.input.attempt_id.to_bytes();
        match self.input.identity.clone() {
            HandleIdentity::File(FileIdentity::Unpublished { .. }) => {
                let space = BLOB_HEAD_KEYSPACE.to_string();
                plan.guards
                    .push((space.clone(), head_key(&scope.bucket, &target)?, head));
                plan.guards
                    .push((space.clone(), watch_key(&scope.bucket, &target)?, watch));
                plan.deletes
                    .push((space, watch_key(&scope.bucket, &target)?));
                let change = GuardedWriteOperation::new(plan);
                self.call(RemoveStep::Change, IoCall::retry(change, &seed))
            }
            HandleIdentity::File(FileIdentity::Committed { life_id })
            | HandleIdentity::Directory(DirectoryWitness::Marker { life_id }) => {
                let mut fence = RowFence::new(plan.guards, Vec::new())
                    .with_writes(plan.writes)
                    .with_deletes(plan.deletes);
                // The directory must still be empty when the delete commits.
                if self.input.kind == HandleKind::Directory {
                    let own = head_key(&scope.bucket, &target)?;
                    let below = marker_key(&self.input.key);
                    let range = head_range(&scope.bucket, &below, Some(own))?;
                    fence = fence.with_range_guards(vec![range]);
                }
                let grant = self.grant.clone().ok_or(VfsError::Denied)?;
                if let Some(config) = marker {
                    let source = SourceDelete {
                        key: target,
                        expected_life: Some(life_id),
                        expected_version: head_of(head.as_ref())?.map(|head| head.version_id),
                        deleted_by: grant.owner,
                        marker_version: self.ids.mint(self.now_ms),
                    };
                    let put = PutObjectOperation::new(config)
                        .with_bucket_guard(grant.bucket.clone())
                        .with_preserve_envelope(true)
                        .with_life(self.ids.mint(self.now_ms))
                        .with_expected_life(None)
                        .with_source_delete(source)
                        .with_row_fence(fence);
                    self.joint = true;
                    return self.call(RemoveStep::Change, IoCall::child(put));
                }
                let input = delete_input(&self.input.context, &grant, &target)?;
                let delete = DeleteObjectOperation::new(input)
                    .with_expected_life(Some(life_id))
                    .with_row_fence(fence);
                self.call(RemoveStep::Change, IoCall::child(delete))
            }
            // A directory without a marker exists only through children, so it is not empty.
            HandleIdentity::Directory(_) => Err(VfsError::NotEmpty),
        }
    }

    /// A definite refusal ends the removal; an unknown outcome is settled by the receipt.
    fn changed(&mut self, result: IoResult) -> RemoveTurn {
        let refused = match &self.input.identity {
            HandleIdentity::File(FileIdentity::Unpublished { .. }) => {
                match result.child::<(), VfsError>()? {
                    Ok(()) => None,
                    Err(VfsError::Conflict) => Some(VfsError::Conflict),
                    Err(_) => None,
                }
            }
            _ if self.joint => match result.child::<PutObjectResult, PutObjectError>()? {
                Ok(_) => None,
                Err(error) => move_refusal(&error),
            },
            _ => match result.child::<DeleteObjectResult, DeleteObjectError>()? {
                Ok(_) => None,
                Err(error) => delete_refusal(&error),
            },
        };
        if let Some(refused) = refused {
            return Err(refused);
        }
        self.unknown = true;
        self.call(RemoveStep::Synced, IoCall::Sync)
    }

    fn finalize(&mut self) -> RemoveTurn {
        let input = ReceiptInput {
            scope: self.scope()?,
            attempt_id: self.input.attempt_id,
            instance_id: self.input.instance_id,
            updates: Vec::new(),
            logical_size: None,
        };
        self.call(
            RemoveStep::Receipt,
            IoCall::child(VfsReceiptOperation::new(input)),
        )
    }
}

impl Operation for VfsRemoveOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> RemoveTurn {
        let bucket = bucket_of(&self.input.context)?;
        let target = self.target();
        let request = VfsRequest::new(VfsAccess::Delete, &bucket, Some(&target));
        let call = authorize_call(&self.input.context, request);
        self.call(RemoveStep::Authorize, call)
    }

    fn next(&mut self, result: IoResult) -> RemoveTurn {
        match self.step {
            RemoveStep::Authorize => {
                self.grant = Some(grant_of(result)?);
                match self
                    .marker
                    .as_ref()
                    .map(|config| config.request.key.clone())
                {
                    Some(marker) => {
                        let bucket = bucket_of(&self.input.context)?;
                        let mut request = VfsRequest::new(VfsAccess::Put, &bucket, Some(&marker));
                        request.length = Some(0);
                        let call = authorize_call(&self.input.context, request);
                        self.call(RemoveStep::Marker, call)
                    }
                    None => self.permit(),
                }
            }
            RemoveStep::Marker => {
                // A marker the caller may not write is left out, as an S3 delete leaves it.
                if !marker_admitted(result)? {
                    self.marker = None;
                }
                self.permit()
            }
            RemoveStep::Permit => {
                self.held.permit_taken(result)?;
                if self.input.kind == HandleKind::File {
                    return self.read_rows();
                }
                let (scope, prefix) = (self.scope()?, marker_key(&self.input.key));
                self.call(
                    RemoveStep::Staged,
                    vfs_call(VfsEffect::ListStaged { scope, prefix }),
                )
            }
            RemoveStep::Staged => self.staged_read(result),
            RemoveStep::Probe => self.probed(result),
            RemoveStep::Rows => self.rows_read(result),
            RemoveStep::Change => self.changed(result),
            RemoveStep::Synced => {
                result.done()?;
                self.finalize()
            }
            RemoveStep::Receipt => {
                let outcome = result.child::<ReceiptOutcome, VfsError>()??;
                (self.begun, self.unknown) = (false, false);
                match outcome {
                    ReceiptOutcome::Applied { .. } => Ok(Next::Then {
                        output: (),
                        calls: self.held.release(),
                    }),
                    ReceiptOutcome::RolledBack => Err(VfsError::Conflict),
                }
            }
        }
    }

    /// An attempt whose outcome is unknown stays registered until its receipt is resolved.
    fn abort(&mut self) -> Vec<IoCall> {
        let resolved = (self.begun && !self.unknown).then_some(self.input.attempt_id);
        self.held.abandon(resolved)
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = aruna_core::id_mint::IdMint::new(entropy);
    }
}

impl VfsRemoveOperation {
    fn permit(&mut self) -> RemoveTurn {
        let scope = self.scope()?;
        self.call(
            RemoveStep::Permit,
            vfs_call(VfsEffect::AcquirePermit { scope }),
        )
    }
}

/// Whether an authorization admitted the marker write; only a denial leaves it out.
fn marker_admitted(result: IoResult) -> Result<bool, VfsError> {
    match grant_of(result) {
        Ok(_) => Ok(true),
        Err(VfsError::Denied) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Whether one head page below a directory holds no live head besides `marker`.
fn page_empty(result: IoResult, marker: &Key) -> Result<bool, VfsError> {
    let (rows, next) = result.page()?;
    let live = rows.iter().any(|(key, value)| {
        let head = CurrentVersionPointer::from_bytes(value.as_ref());
        key != marker && head.is_ok_and(|head| head.life_id.is_some())
    });
    Ok(!live && next.is_none())
}

/// The head range below `prefix` of `bucket`, which must hold no live head but `exclude`.
fn head_range(bucket: &str, prefix: &str, exclude: Option<Key>) -> Result<RangeGuard, VfsError> {
    Ok(RangeGuard {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        prefix: BlobHeadKey::object_prefix(bucket, prefix)?.into(),
        exclude_key: exclude,
    })
}

/// A definite refusal of a joint put and delete; `None` when the commit may have happened.
fn move_refusal(error: &PutObjectError) -> Option<VfsError> {
    match error {
        PutObjectError::ObjectExists => Some(VfsError::Exists),
        other => refusal(other),
    }
}

/// Whether the target rows still hold the identity the caller resolved.
fn identity_present(
    identity: &HandleIdentity,
    head: Option<&Value>,
    watch: Option<&Value>,
) -> Result<bool, VfsError> {
    let (head, token) = (head_of(head)?, token_of(watch)?);
    Ok(super::handles::identity_holds(
        identity,
        head.as_ref(),
        token,
    ))
}

/// The delete of one exact key as the admitted caller.
fn delete_input(
    context: &VfsAccessContext,
    grant: &VfsGrant,
    key: &str,
) -> Result<DeleteObjectInput, VfsError> {
    Ok(DeleteObjectInput {
        bucket: bucket_of(context)?,
        key: key.to_string(),
        version_id: None,
        group_id: grant.bucket.group_id,
        realm_id: context.realm_id,
        node_id: context.node_id,
        deleted_by: grant.owner,
    })
}

/// A definite refusal of a delete; `None` when the commit may have happened.
fn delete_refusal(error: &DeleteObjectError) -> Option<VfsError> {
    match error {
        DeleteObjectError::VersionChanged
        | DeleteObjectError::FenceTaken
        | DeleteObjectError::StorageError(StorageError::TransactionConflict) => {
            Some(VfsError::Conflict)
        }
        DeleteObjectError::NoSuchVersion => Some(VfsError::StaleHandle),
        DeleteObjectError::StorageError(_) => None,
        other => Some(VfsError::Storage(other.to_string())),
    }
}

/// The handle rows of an unlink: an instance with opens stays readable as detached, any other
/// handle retires as stale; the reverse row goes and the cookie keeps its old position.
pub(crate) fn unlink_rows(
    scope: &BucketScope,
    handle_id: u64,
    live: &LiveHandle,
    open: bool,
    now_ms: u64,
    name: Option<&Value>,
) -> Result<super::StateChanges, VfsError> {
    let (mut writes, deletes) =
        retire_rows(scope, handle_id, live, RetireReason::Stale, now_ms, name)?;
    if open {
        let mut detached = live.clone();
        detached.detached = Some(DetachReason::Unlinked);
        let row = VfsHandleRow::Live(detached);
        writes[0] = state_row(&VfsKey::Handle(scope.clone(), handle_id), &row)?;
    }
    Ok((writes, deletes))
}

/// The exact rows one namespace transaction guards, writes and deletes.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MovePlan {
    pub guards: Vec<(String, Key, Option<Value>)>,
    pub writes: Vec<(String, Key, Value)>,
    pub deletes: Vec<(String, Key)>,
}

impl MovePlan {
    pub(crate) fn guard(&mut self, key: &VfsKey, value: Option<Value>) -> Result<(), VfsError> {
        self.guards
            .push((NFS_STATE_KEYSPACE.to_string(), state_key(key)?, value));
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GuardStep {
    Read,
    Written,
    Deleted,
}

/// One namespace transaction without an S3 version: every guard must still read as it was
/// read before, then the rows are written and deleted together.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GuardedWriteOperation {
    plan: MovePlan,
    step: GuardStep,
}

impl GuardedWriteOperation {
    pub(crate) fn new(plan: MovePlan) -> Self {
        Self {
            plan,
            step: GuardStep::Read,
        }
    }
}

impl Operation for GuardedWriteOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<()>, VfsError> {
        let reads = self.plan.guards.iter();
        let reads = reads.map(|(space, key, _)| (space.clone(), key.clone()));
        Ok(Next::Call(IoCall::Read(reads.collect())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, VfsError> {
        match self.step {
            GuardStep::Read => {
                let values = result.values()?;
                let expected = self.plan.guards.iter().map(|(_, _, value)| value);
                if values.len() != self.plan.guards.len() || !values.iter().eq(expected) {
                    return Err(VfsError::Conflict);
                }
                self.step = GuardStep::Written;
                Ok(Next::Call(IoCall::Write(self.plan.writes.clone())))
            }
            GuardStep::Written => {
                result.done()?;
                self.step = GuardStep::Deleted;
                Ok(Next::Call(IoCall::Delete(self.plan.deletes.clone())))
            }
            GuardStep::Deleted => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// One rename of a file or empty directory inside one export.
#[derive(Clone, Debug, PartialEq)]
pub struct RenameInput {
    pub context: VfsAccessContext,
    pub from: String,
    pub to: String,
    pub kind: HandleKind,
    /// The source identity the caller resolved and pinned.
    pub identity: HandleIdentity,
    pub handle_id: Option<u64>,
    pub instance_id: Option<u64>,
    /// SMB `ReplaceIfExists`; NFS replaces a compatible destination.
    pub replace: bool,
    pub attempt_id: Ulid,
    /// The source's live checkpoint row, moved with it.
    pub stage: Option<NfsStageRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenameStep {
    Source,
    Read,
    Target,
    Marker,
    Permit,
    Rows,
    Staged,
    /// Staged names below a moved directory make it nonempty.
    Emptied,
    Probe,
    Change,
    Synced,
    Receipt,
}

type RenameTurn = Result<Next<()>, VfsError>;

/// Renames inside one export in one transaction, never as separate put and delete. An
/// unpublished file rebinds its stage to a fresh destination token; a committed file or empty
/// marker directory publishes at the destination under a fresh life and deletes its source.
#[derive(Debug, PartialEq)]
pub struct VfsRenameOperation {
    input: RenameInput,
    step: RenameStep,
    ids: aruna_core::id_mint::IdMint,
    grant: Option<VfsGrant>,
    held: Held,
    rows: Vec<Option<Value>>,
    begun: bool,
    unknown: bool,
    now_ms: u64,
    /// The prepared write of the source's exact version under the destination key.
    destination: Option<PutObjectConfig>,
    /// The prepared write of the source parent's marker, used when it is admitted.
    marker: Option<PutObjectConfig>,
    /// The source is committed, so the move is one put with a source delete.
    committed: bool,
    /// The destination's handle the move displaced, detached once the move is settled.
    displaced: Option<u64>,
}

impl VfsRenameOperation {
    pub fn new(input: RenameInput) -> Self {
        Self {
            input,
            step: RenameStep::Source,
            ids: aruna_core::id_mint::IdMint::default(),
            grant: None,
            held: Held::default(),
            rows: Vec::new(),
            begun: false,
            unknown: false,
            now_ms: 0,
            destination: None,
            marker: None,
            committed: false,
            displaced: None,
        }
    }

    /// The write of a committed source's pinned version, or a directory's empty marker, under
    /// the destination key; its body streams the source bytes outside the namespace permit.
    pub fn with_destination_put(mut self, put: PutObjectConfig) -> Self {
        self.destination = Some(put);
        self
    }

    /// The zero-length write of the source parent's marker `p/`, which a move out of an
    /// implicit directory `p` creates in its own transaction when no live marker exists.
    pub fn with_parent_marker(mut self, put: PutObjectConfig) -> Self {
        self.marker = Some(put);
        self
    }

    /// The source and destination head keys: the names, or the directories' markers.
    fn heads(&self) -> (String, String) {
        match self.input.kind {
            HandleKind::File => (self.input.from.clone(), self.input.to.clone()),
            HandleKind::Directory => (marker_key(&self.input.from), marker_key(&self.input.to)),
        }
    }

    fn call(&mut self, step: RenameStep, call: IoCall) -> RenameTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        scope_of(&self.input.context, self.grant.as_ref())
    }

    fn authorize(&mut self, step: RenameStep, access: VfsAccess, key: &str) -> RenameTurn {
        let bucket = bucket_of(&self.input.context)?;
        let request = VfsRequest::new(access, &bucket, Some(key));
        let call = authorize_call(&self.input.context, request);
        self.call(step, call)
    }

    /// Paths, export and cycle checks that need no storage.
    fn checked(&self) -> Result<(), VfsError> {
        let context = &self.input.context;
        let bucket = bucket_of(context)?;
        let (from, to) = (&self.input.from, &self.input.to);
        if !export_holds(&context.target, &bucket, from)
            || !export_holds(&context.target, &bucket, to)
        {
            return Err(VfsError::CrossExport);
        }
        let (parent, name) = split_parent(to).ok_or(VfsError::InvalidName)?;
        check_component(name, context.protocol, NameUse::Create, parent.is_empty())?;
        if self.input.kind == HandleKind::Directory && creates_cycle(from, to) {
            return Err(VfsError::InvalidName);
        }
        Ok(())
    }

    /// Source and destination heads, watch keys, the destination marker and the nfs rows.
    fn read_rows(&mut self) -> RenameTurn {
        let scope = self.scope()?;
        let (source, _) = self.heads();
        let (from, to) = (&self.input.from, &self.input.to);
        let mut reads = name_reads(&scope.bucket, &source)?;
        reads.extend(name_reads(&scope.bucket, to)?);
        reads.push((
            BLOB_HEAD_KEYSPACE.to_string(),
            head_key(&scope.bucket, &marker_key(to))?,
        ));
        let mut keys = vec![
            VfsKey::Receipt(self.input.attempt_id),
            VfsKey::Name(scope.clone(), from.clone()),
            VfsKey::Name(scope.clone(), to.clone()),
        ];
        if let Some(handle_id) = self.input.handle_id {
            keys.push(VfsKey::Handle(scope.clone(), handle_id));
        }
        let IoCall::Read(state) = state_read(&keys)? else {
            return Err(VfsError::Conflict);
        };
        reads.extend(state);
        self.call(RenameStep::Rows, IoCall::Read(reads))
    }

    fn rows_read(&mut self, result: IoResult) -> RenameTurn {
        self.rows = result.values()?;
        let at = |index: usize| self.rows.get(index).cloned().flatten();
        let (to_head, to_watch, to_marker) = (at(2), at(3), at(4));
        if !identity_present(&self.input.identity, at(0).as_ref(), at(1).as_ref())? {
            return Err(VfsError::StaleHandle);
        }
        if at(5).is_some() {
            return Err(VfsError::Conflict);
        }
        match &self.input.identity {
            HandleIdentity::File(FileIdentity::Unpublished { .. }) => {}
            // A directory without a marker exists only through children, so it is not empty.
            HandleIdentity::Directory(witness)
                if !matches!(witness, DirectoryWitness::Marker { .. }) =>
            {
                return Err(VfsError::NotEmpty);
            }
            _ if self.destination.is_none() => {
                return Err(VfsError::Unavailable(JOINT_MOVE.to_string()));
            }
            _ => self.committed = true,
        }
        let marker = head_of(to_marker.as_ref())?;
        let head = head_of(to_head.as_ref())?;
        let directory = self.input.kind == HandleKind::Directory;
        if live_life(marker.as_ref()).is_some() {
            return Err(match directory {
                true => VfsError::Exists,
                false => VfsError::IsDirectory,
            });
        }
        if live_life(head.as_ref()).is_some() {
            // A committed file replaces a live destination in its own put transaction.
            match (self.input.replace, self.committed, directory) {
                (true, true, false) => return self.list_target(),
                (true, false, false) => {
                    return Err(VfsError::Unavailable(JOINT_MOVE.to_string()));
                }
                _ => return Err(VfsError::Exists),
            }
        }
        let staged = token_of(to_watch.as_ref())?;
        if staged.is_some()
            && !self.input.replace
            && self.input.context.protocol == VfsProtocol::Smb
        {
            return Err(VfsError::Exists);
        }
        self.list_target()
    }

    fn list_target(&mut self) -> RenameTurn {
        let (scope, prefix) = (self.scope()?, marker_key(&self.input.to));
        self.call(
            RenameStep::Staged,
            vfs_call(VfsEffect::ListStaged { scope, prefix }),
        )
    }

    /// A file never moves onto a projected directory, staged ones included; a directory never
    /// moves onto one either.
    fn staged_read(&mut self, result: IoResult) -> RenameTurn {
        if !staged_entries(result)?.is_empty() {
            return Err(match self.input.kind {
                HandleKind::File => VfsError::IsDirectory,
                HandleKind::Directory => VfsError::Exists,
            });
        }
        if self.input.kind == HandleKind::File {
            return self.change();
        }
        let (scope, prefix) = (self.scope()?, marker_key(&self.input.from));
        self.call(
            RenameStep::Emptied,
            vfs_call(VfsEffect::ListStaged { scope, prefix }),
        )
    }

    /// A moved directory must be empty of staged names and of live S3 descendants.
    fn emptied_read(&mut self, result: IoResult) -> RenameTurn {
        if !staged_entries(result)?.is_empty() {
            return Err(VfsError::NotEmpty);
        }
        let bucket = bucket_of(&self.input.context)?;
        let prefix = BlobHeadKey::object_prefix(&bucket, &marker_key(&self.input.from))?;
        self.call(
            RenameStep::Probe,
            IoCall::Page {
                space: BLOB_HEAD_KEYSPACE.to_string(),
                prefix: prefix.into(),
                after: None,
                limit: STATE_PAGE,
            },
        )
    }

    fn probed(&mut self, result: IoResult) -> RenameTurn {
        let (source, _) = self.heads();
        let marker = head_key(&bucket_of(&self.input.context)?, &source)?;
        if !page_empty(result, &marker)? {
            return Err(VfsError::NotEmpty);
        }
        self.change()
    }

    /// The source parent's marker, when admitted and the move leaves that parent.
    fn parent_marker(&mut self) -> Option<ParentMarker> {
        let (parent, _) = split_parent(&self.input.from)?;
        let (target, _) = split_parent(&self.input.to)?;
        let config = self.marker.take()?;
        if parent.is_empty() || parent == target || config.request.key != marker_key(parent) {
            return None;
        }
        Some(ParentMarker {
            key: config.request.key.clone(),
            version_id: (config.preassigned_version_id)
                .unwrap_or_else(|| self.ids.mint(self.now_ms)),
            life_id: self.ids.mint(self.now_ms),
            owner: config.user_id,
        })
    }

    /// One put transaction: the destination version under a fresh life, the source's delete
    /// marker, the moved handle, stage and reverse rows, both range checks and the receipt.
    fn joint_change(&mut self) -> RenameTurn {
        let scope = self.scope()?;
        let grant = self.grant.clone().ok_or(VfsError::Denied)?;
        let (source, target) = self.heads();
        let at = |index: usize| self.rows.get(index).cloned().flatten();
        let source_head = head_of(at(0).as_ref())?;
        let life_id = live_life(source_head.as_ref()).ok_or(VfsError::StaleHandle)?;
        let target_head = match self.input.kind {
            HandleKind::File => head_of(at(2).as_ref())?,
            HandleKind::Directory => head_of(at(4).as_ref())?,
        };
        let mut config = self.destination.take().ok_or(VfsError::Conflict)?;
        if config.request.key != target {
            return Err(VfsError::InvalidName);
        }
        let version_id = *config
            .preassigned_version_id
            .get_or_insert(self.input.attempt_id);
        let (fresh, marker_version) = (self.ids.mint(self.now_ms), self.ids.mint(self.now_ms));
        let plan = committed_move(
            &self.input,
            &scope,
            &self.rows,
            (version_id, fresh),
            self.now_ms,
        )?;
        self.displaced = plan.displaced;
        let mut ranges = vec![head_range(
            &scope.bucket,
            &marker_key(&self.input.to),
            None,
        )?];
        if self.input.kind == HandleKind::Directory {
            let own = head_key(&scope.bucket, &source)?;
            let below = marker_key(&self.input.from);
            ranges.push(head_range(&scope.bucket, &below, Some(own))?);
        }
        let fence = RowFence::new(plan.rows.guards, Vec::new())
            .with_writes(plan.rows.writes)
            .with_deletes(plan.rows.deletes)
            .with_range_guards(ranges);
        let delete = SourceDelete {
            key: source,
            expected_life: Some(life_id),
            expected_version: source_head.map(|head| head.version_id),
            deleted_by: grant.owner,
            marker_version,
        };
        let mut put = PutObjectOperation::new(config)
            .with_bucket_guard(grant.bucket.clone())
            .with_preserve_envelope(true)
            .with_life(fresh)
            .with_expected_head(target_head.as_ref().map(|head| head.version_id))
            .with_expected_life(live_life(target_head.as_ref()))
            .with_source_delete(delete)
            .with_row_fence(fence);
        if let Some(marker) = self.parent_marker() {
            put = put.with_parent_marker(marker);
        }
        self.begun = true;
        self.call(RenameStep::Change, IoCall::child(put))
    }

    /// The source token, handle, reverse rows and stage move to a fresh destination token.
    fn change(&mut self) -> RenameTurn {
        if self.committed {
            return self.joint_change();
        }
        let token = self.ids.mint(self.now_ms);
        let scope = self.scope()?;
        let plan = unpublished_move(&self.input, &scope, &self.rows, token, self.now_ms)?;
        self.begun = true;
        let seed = self.input.attempt_id.to_bytes();
        self.call(
            RenameStep::Change,
            IoCall::retry(GuardedWriteOperation::new(plan), &seed),
        )
    }

    /// A conflict is a definite refusal; any other end is settled by the receipt.
    fn changed(&mut self, result: IoResult) -> RenameTurn {
        let refused = match self.committed {
            true => match result.child::<PutObjectResult, PutObjectError>()? {
                Ok(_) => None,
                Err(error) => move_refusal(&error),
            },
            false => match result.child::<(), VfsError>()? {
                Err(VfsError::Conflict) => Some(VfsError::Conflict),
                _ => None,
            },
        };
        if let Some(refused) = refused {
            return Err(refused);
        }
        self.unknown = true;
        self.call(RenameStep::Synced, IoCall::Sync)
    }

    /// The settled move releases the permit and detaches the instance it displaced.
    fn settled(&mut self) -> Result<Vec<IoCall>, VfsError> {
        let mut calls = Vec::new();
        if let Some(handle_id) = self.displaced.take() {
            calls.push(vfs_call(VfsEffect::DetachInstance {
                scope: self.scope()?,
                handle_id,
                instance_id: None,
                reason: DetachReason::Displaced,
            }));
        }
        calls.extend(self.held.release());
        Ok(calls)
    }

    fn finalize(&mut self) -> RenameTurn {
        let input = ReceiptInput {
            scope: self.scope()?,
            attempt_id: self.input.attempt_id,
            instance_id: self.input.instance_id,
            updates: Vec::new(),
            logical_size: None,
        };
        self.call(
            RenameStep::Receipt,
            IoCall::child(VfsReceiptOperation::new(input)),
        )
    }
}

impl Operation for VfsRenameOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> RenameTurn {
        self.checked()?;
        let source = match self.input.kind {
            HandleKind::File => self.input.from.clone(),
            HandleKind::Directory => marker_key(&self.input.from),
        };
        self.authorize(RenameStep::Source, VfsAccess::Delete, &source)
    }

    fn next(&mut self, result: IoResult) -> RenameTurn {
        match self.step {
            RenameStep::Source => {
                grant_of(result)?;
                let from = self.input.from.clone();
                self.authorize(RenameStep::Read, VfsAccess::Get, &from)
            }
            RenameStep::Read => {
                grant_of(result)?;
                let to = self.input.to.clone();
                self.authorize(RenameStep::Target, VfsAccess::Copy, &to)
            }
            RenameStep::Target => {
                self.grant = Some(grant_of(result)?);
                // The same exact name is a no-op once its source is admitted.
                if self.input.from == self.input.to {
                    return Ok(Next::Done(()));
                }
                match self
                    .marker
                    .as_ref()
                    .map(|config| config.request.key.clone())
                {
                    Some(marker) => {
                        let bucket = bucket_of(&self.input.context)?;
                        let mut request = VfsRequest::new(VfsAccess::Put, &bucket, Some(&marker));
                        request.length = Some(0);
                        let call = authorize_call(&self.input.context, request);
                        self.call(RenameStep::Marker, call)
                    }
                    None => self.permit(),
                }
            }
            RenameStep::Marker => {
                if !marker_admitted(result)? {
                    self.marker = None;
                }
                self.permit()
            }
            RenameStep::Permit => {
                self.held.permit_taken(result)?;
                self.read_rows()
            }
            RenameStep::Rows => self.rows_read(result),
            RenameStep::Staged => self.staged_read(result),
            RenameStep::Emptied => self.emptied_read(result),
            RenameStep::Probe => self.probed(result),
            RenameStep::Change => self.changed(result),
            RenameStep::Synced => {
                result.done()?;
                self.finalize()
            }
            RenameStep::Receipt => {
                let outcome = result.child::<ReceiptOutcome, VfsError>()??;
                (self.begun, self.unknown) = (false, false);
                match outcome {
                    ReceiptOutcome::Applied { .. } => Ok(Next::Then {
                        output: (),
                        calls: self.settled()?,
                    }),
                    ReceiptOutcome::RolledBack => Err(VfsError::Conflict),
                }
            }
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        let resolved = (self.begun && !self.unknown).then_some(self.input.attempt_id);
        self.held.abandon(resolved)
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = aruna_core::id_mint::IdMint::new(entropy);
    }
}

impl VfsRenameOperation {
    fn permit(&mut self) -> RenameTurn {
        let scope = self.scope()?;
        self.call(
            RenameStep::Permit,
            vfs_call(VfsEffect::AcquirePermit { scope }),
        )
    }
}

fn staged_entries(result: IoResult) -> Result<Vec<aruna_core::vfs_effects::StagedEntry>, VfsError> {
    match vfs_event(result)? {
        VfsEvent::StagedList { entries, .. } => Ok(entries),
        other => Err(unexpected(&other)),
    }
}

/// The one transaction that moves an unpublished file to a fresh destination token. `rows`
/// are as read: source head and watch, destination head, watch and marker, the receipt, both
/// reverse rows and the source handle row.
pub(crate) fn unpublished_move(
    input: &RenameInput,
    scope: &BucketScope,
    rows: &[Option<Value>],
    token: Ulid,
    now_ms: u64,
) -> Result<MovePlan, VfsError> {
    let (from, to) = (input.from.clone(), input.to.clone());
    let at = |index: usize| rows.get(index).cloned().flatten();
    let head = head_of(at(2).as_ref())?;
    let predecessor = head.as_ref().map(|head| head.version_id);
    let space = BLOB_HEAD_KEYSPACE.to_string();
    let mut plan = MovePlan::default();
    for (index, (key, watch)) in [(&from, false), (&from, true), (&to, false), (&to, true)]
        .into_iter()
        .enumerate()
    {
        let row = match watch {
            true => watch_key(&scope.bucket, key)?,
            false => head_key(&scope.bucket, key)?,
        };
        plan.guards.push((space.clone(), row, at(index)));
    }
    plan.guard(&VfsKey::Receipt(input.attempt_id), None)?;
    plan.guard(&VfsKey::Name(scope.clone(), from.clone()), at(6))?;
    plan.guard(&VfsKey::Name(scope.clone(), to.clone()), at(7))?;
    plan.deletes
        .push((space.clone(), watch_key(&scope.bucket, &from)?));
    plan.writes.push(watch_row(&scope.bucket, &to, token)?);
    let identity = HandleIdentity::File(FileIdentity::Unpublished { token, predecessor });
    let displaced = moved_handle(&mut plan, input, scope, rows, identity, now_ms)?;
    if let Some(stage) = input.stage.clone() {
        let key = super::io::stage_key(scope, &stage);
        plan.guard(
            &key,
            Some(Value::from(
                aruna_core::structs::storage::vfs::VfsRecord::to_bytes(&stage)?,
            )),
        )?;
        let mut moved = stage;
        moved.key = to.clone();
        moved.base =
            aruna_core::structs::storage::vfs::StageBase::Unpublished { token, predecessor };
        moved.updated_at_ms = now_ms;
        plan.writes.push(state_row(&key, &moved)?);
    }
    let receipt = VfsReceipt {
        attempt_id: input.attempt_id,
        scope: scope.clone(),
        created_at_ms: now_ms,
        change: ReceiptChange::UnpublishedRename {
            from,
            to,
            token,
            handle_id: input.handle_id,
            displaced,
        },
    };
    plan.writes
        .push(state_row(&VfsKey::Receipt(input.attempt_id), &receipt)?);
    Ok(plan)
}

/// The rows of a committed move besides its put, and the destination handle it displaces.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CommittedMove {
    pub rows: MovePlan,
    pub displaced: Option<u64>,
}

/// The nfs rows a committed file or marker directory move commits in its put transaction:
/// the moved handle under the destination's new life, reverse rows, stage and receipt. `rows`
/// are as [`unpublished_move`] reads them; `target` is the destination version and life.
pub(crate) fn committed_move(
    input: &RenameInput,
    scope: &BucketScope,
    rows: &[Option<Value>],
    (version_id, life_id): (Ulid, Ulid),
    now_ms: u64,
) -> Result<CommittedMove, VfsError> {
    let (from, to) = (input.from.clone(), input.to.clone());
    let at = |index: usize| rows.get(index).cloned().flatten();
    let mut plan = MovePlan::default();
    // A staged destination name is displaced; its token goes with the head write.
    let watch = watch_key(&scope.bucket, &to)?;
    plan.guards
        .push((BLOB_HEAD_KEYSPACE.to_string(), watch, at(3)));
    plan.guard(&VfsKey::Receipt(input.attempt_id), None)?;
    plan.guard(&VfsKey::Name(scope.clone(), from.clone()), at(6))?;
    plan.guard(&VfsKey::Name(scope.clone(), to.clone()), at(7))?;
    let identity = match input.kind {
        HandleKind::File => HandleIdentity::File(FileIdentity::Committed { life_id }),
        HandleKind::Directory => HandleIdentity::Directory(DirectoryWitness::Marker { life_id }),
    };
    let displaced = moved_handle(&mut plan, input, scope, rows, identity, now_ms)?;
    let stage = input
        .stage
        .clone()
        .filter(|_| input.kind == HandleKind::File);
    let stage_id = stage.as_ref().map(|stage| stage.stage_id);
    // A dirty source keeps its overlay on top of the destination's new version.
    if let Some(stage) = stage {
        let key = super::io::stage_key(scope, &stage);
        let row = aruna_core::structs::storage::vfs::VfsRecord::to_bytes(&stage)?;
        plan.guard(&key, Some(Value::from(row)))?;
        let mut moved = stage;
        moved.key = to.clone();
        moved.base = StageBase::Committed {
            version_id,
            life_id,
        };
        moved.updated_at_ms = now_ms;
        plan.writes.push(state_row(&key, &moved)?);
    }
    let change = match input.kind {
        HandleKind::File => ReceiptChange::FilePublish {
            key: to,
            version_id,
            life_id,
            handle_id: input.handle_id,
            stage_id,
        },
        HandleKind::Directory => ReceiptChange::DirectoryRename {
            from,
            to,
            handle_id: input.handle_id,
        },
    };
    let receipt = VfsReceipt {
        attempt_id: input.attempt_id,
        scope: scope.clone(),
        created_at_ms: now_ms,
        change,
    };
    plan.writes
        .push(state_row(&VfsKey::Receipt(input.attempt_id), &receipt)?);
    Ok(CommittedMove {
        rows: plan,
        displaced,
    })
}

/// Moves the source handle to the destination with `identity`: its old cookie position, the
/// handle row and both reverse rows. Answers the other handle the destination named.
fn moved_handle(
    plan: &mut MovePlan,
    input: &RenameInput,
    scope: &BucketScope,
    rows: &[Option<Value>],
    identity: HandleIdentity,
    now_ms: u64,
) -> Result<Option<u64>, VfsError> {
    let (from, to) = (input.from.clone(), input.to.clone());
    let at = |index: usize| rows.get(index).cloned().flatten();
    let displaced = at(7)
        .map(|value| decode_handle_id(value.as_ref()))
        .transpose()?;
    if let Some(handle_id) = input.handle_id {
        let row = at(8);
        plan.guard(&VfsKey::Handle(scope.clone(), handle_id), row.clone())?;
        if let Some(VfsHandleRow::Live(mut live)) = record_of::<VfsHandleRow>(row.as_ref())? {
            let position = aruna_core::structs::storage::vfs::CookiePosition {
                directory_key: split_parent(&from).unwrap_or_default().0.to_string(),
                entry_key: position_key(&live),
                retired_at_ms: now_ms,
            };
            plan.writes.push(state_row(
                &VfsKey::Cookie(scope.clone(), live.cookie_id),
                &position,
            )?);
            live.key = to.clone();
            live.identity = identity.clone();
            plan.writes.push(state_row(
                &VfsKey::Handle(scope.clone(), handle_id),
                &VfsHandleRow::Live(live),
            )?);
        }
        let name = Value::from(encode_handle_id(handle_id).to_vec());
        plan.writes.push((
            NFS_STATE_KEYSPACE.to_string(),
            state_key(&VfsKey::Name(scope.clone(), to.clone()))?,
            name,
        ));
        plan.deletes
            .push(state_delete(&VfsKey::Name(scope.clone(), from))?);
    }
    Ok(displaced.filter(|displaced| Some(*displaced) != input.handle_id))
}

#[cfg(test)]
#[path = "mutate_tests.rs"]
pub(super) mod tests;
