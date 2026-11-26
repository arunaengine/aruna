//! Durable NFS handles and directory listings: lazy allocation under the namespace permit,
//! directory witnesses, retirement, detached handles and paged listings with staged names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::path::{children_prefix, marker_key, representable, split_parent};
use super::policy::{VfsAccess, VfsAccessContext, VfsGrant, VfsRequest, authorize_call, grant_of};
use super::{
    STATE_PAGE, VfsError, head_of, live_life, name_reads, record_of, state_delete, state_key,
    state_read, state_row, token_of, unexpected, vfs_call, vfs_event,
};
use crate::s3::object::list::{
    ListBucketError, ListBucketInput, ListContinuationToken, ListedObject,
};
use crate::s3::object::visible::{
    NextPage, RecordSource, ShowRule, Shown, SnapshotWitness, VisibleError, VisiblePage,
    VisiblePageOperation, after_entry,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, NFS_STATE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
use aruna_core::structs::storage::vfs::{
    BucketScope, CookiePosition, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind,
    LiveHandle, RetireReason, VfsBootState, VfsHandleRow, VfsKey, decode_handle_id,
    encode_handle_id,
};
use aruna_core::types::{Key, Value};
use aruna_core::vfs_effects::{InstanceSummary, StagedEntry, VfsEffect, VfsEvent};
use aruna_ops_identity::auth::scope::SubpathScope;
use std::collections::{BTreeMap, VecDeque};
use ulid::Ulid;

/// The namespace permit and file guard an operation holds; both are released on every end.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Held {
    pub permit: Option<Ulid>,
    pub guard: Option<Ulid>,
}

impl Held {
    /// Releases the guard before the permit, the reverse of taking them.
    pub(crate) fn release(&mut self) -> Vec<IoCall> {
        let guard = self.guard.take();
        let guard = guard.map(|guard_id| vfs_call(VfsEffect::ReleaseGuard { guard_id }));
        let permit = self.permit.take();
        let permit = permit.map(|permit_id| vfs_call(VfsEffect::ReleasePermit { permit_id }));
        guard.into_iter().chain(permit).collect()
    }

    /// The calls of an aborted run: a resolved attempt finishes, then the guard and permit go.
    pub(crate) fn abandon(&mut self, resolved: Option<Ulid>) -> Vec<IoCall> {
        let finish = resolved.map(|attempt_id| vfs_call(VfsEffect::FinishAttempt { attempt_id }));
        finish.into_iter().chain(self.release()).collect()
    }

    /// The permit an `AcquirePermit` answered.
    pub(crate) fn permit_taken(&mut self, result: IoResult) -> Result<(), VfsError> {
        match vfs_event(result)? {
            VfsEvent::PermitAcquired { permit_id } => {
                self.permit = Some(permit_id);
                Ok(())
            }
            other => Err(unexpected(&other)),
        }
    }

    /// The guard an `AcquireGuards` answered; a pending mutation makes the caller wait.
    pub(crate) fn guard_taken(&mut self, result: IoResult) -> Result<Option<Ulid>, VfsError> {
        match vfs_event(result)? {
            VfsEvent::GuardAcquired { guard_id } => {
                self.guard = Some(guard_id);
                Ok(None)
            }
            VfsEvent::PendingMutation { attempt_id } => Ok(Some(attempt_id)),
            other => Err(unexpected(&other)),
        }
    }
}

/// The key whose head and watch rows prove a handle's identity.
pub fn proof_key(key: &str, identity: &HandleIdentity) -> String {
    match identity {
        HandleIdentity::File(_) => key.to_string(),
        HandleIdentity::Directory(DirectoryWitness::Marker { .. }) => marker_key(key),
        HandleIdentity::Directory(DirectoryWitness::Descendant { key, .. })
        | HandleIdentity::Directory(DirectoryWitness::Unpublished { key, .. }) => key.clone(),
    }
}

/// Whether the proof key's head and watch token still prove `identity`.
pub fn identity_holds(
    identity: &HandleIdentity,
    head: Option<&CurrentVersionPointer>,
    token: Option<Ulid>,
) -> bool {
    match identity {
        HandleIdentity::File(FileIdentity::Committed { life_id })
        | HandleIdentity::Directory(DirectoryWitness::Marker { life_id })
        | HandleIdentity::Directory(DirectoryWitness::Descendant { life_id, .. }) => {
            live_life(head) == Some(*life_id)
        }
        HandleIdentity::File(FileIdentity::Unpublished {
            token: bound,
            predecessor,
        }) => {
            token == Some(*bound)
                && head.map(|head| head.version_id) == *predecessor
                && live_life(head).is_none()
        }
        HandleIdentity::Directory(DirectoryWitness::Unpublished { token: bound, .. }) => {
            token == Some(*bound) && live_life(head).is_none()
        }
    }
}

/// What one storage snapshot and the serialized staged names hold below a directory.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WitnessFacts {
    /// The life of the live `d/` marker.
    pub marker: Option<Ulid>,
    /// Live descendants by exact key and life.
    pub descendants: Vec<(String, Ulid)>,
    /// Unpublished descendants by exact key and watch token.
    pub unpublished: Vec<(String, Ulid)>,
}

/// The preferred witness: the marker, then the first live descendant, then the first
/// unpublished one; `None` when the directory no longer exists.
pub fn select_witness(facts: &WitnessFacts) -> Option<DirectoryWitness> {
    fn first_row(rows: &[(String, Ulid)]) -> Option<&(String, Ulid)> {
        rows.iter().min_by(|left, right| left.0.cmp(&right.0))
    }
    if let Some(life_id) = facts.marker {
        return Some(DirectoryWitness::Marker { life_id });
    }
    if let Some((key, life_id)) = first_row(&facts.descendants) {
        let (key, life_id) = (key.clone(), *life_id);
        return Some(DirectoryWitness::Descendant { key, life_id });
    }
    let (key, token) = first_row(&facts.unpublished)?;
    let (key, token) = (key.clone(), *token);
    Some(DirectoryWitness::Unpublished { key, token })
}

/// The witness key a VFS change removed: `None` when `witness` does not rest on it.
pub fn rests_on(witness: &DirectoryWitness, removed: &str) -> bool {
    match witness {
        DirectoryWitness::Marker { .. } => false,
        DirectoryWitness::Descendant { key, .. } | DirectoryWitness::Unpublished { key, .. } => {
            key == removed
        }
    }
}

/// The witness of an ancestor after a VFS change moved or removed `removed`: kept when it does
/// not rest on it, else the surviving replacement the change chose in its own transaction.
pub fn advance_witness(
    witness: &DirectoryWitness,
    removed: &str,
    replacement: Option<&DirectoryWitness>,
) -> Option<DirectoryWitness> {
    match rests_on(witness, removed) {
        false => Some(witness.clone()),
        true => replacement.cloned(),
    }
}

/// The listing snapshot check of a witness; an unpublished one is checked by its token only.
pub fn snapshot_of(directory: &str, witness: &DirectoryWitness) -> Option<SnapshotWitness> {
    match witness {
        DirectoryWitness::Marker { life_id } => Some(SnapshotWitness::Head {
            key: marker_key(directory),
            life_id: *life_id,
        }),
        DirectoryWitness::Descendant { key, life_id } => Some(SnapshotWitness::Head {
            key: key.clone(),
            life_id: *life_id,
        }),
        DirectoryWitness::Unpublished { .. } => None,
    }
}

/// How a detached handle is used; only the holder of a matching open keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandleUse {
    ExistingOpen,
    NewOpen,
    ClaimHandle,
    Anonymous,
}

/// Whether a detached instance serves `usage`: never a new open, a claim or anonymous data.
pub fn detached_allows(summary: &InstanceSummary, usage: HandleUse) -> bool {
    usage == HandleUse::ExistingOpen && summary.open_count > 0
}

/// What a handle row names right now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandleState {
    Live(LiveHandle),
    /// Off its name; it serves only its existing opens.
    Detached(LiveHandle, Box<InstanceSummary>),
}

/// The handle row `handle_id` as its state, or the error its tombstone or absence means.
pub fn row_state(row: Option<VfsHandleRow>) -> Result<LiveHandle, VfsError> {
    match row {
        Some(VfsHandleRow::Live(live)) => Ok(live),
        Some(VfsHandleRow::Tombstone {
            reason: RetireReason::Stale,
            ..
        }) => Err(VfsError::StaleHandle),
        // An authenticated id whose row was collected is an expired volatile identity.
        Some(VfsHandleRow::Tombstone { .. }) | None => Err(VfsError::ExpiredHandle),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResolveStep {
    Row,
    Instance,
    Proof,
    Probe,
}

/// Resolves a handle id and validates it against storage: a live binding whose proof still
/// holds, or a detached instance that still has opens. Reads only.
#[derive(Debug, PartialEq)]
pub struct ResolveHandleOperation {
    scope: BucketScope,
    handle_id: u64,
    step: ResolveStep,
    live: Option<LiveHandle>,
}

impl ResolveHandleOperation {
    pub fn new(scope: BucketScope, handle_id: u64) -> Self {
        Self {
            scope,
            handle_id,
            step: ResolveStep::Row,
            live: None,
        }
    }

    fn row_read(&mut self, result: IoResult) -> Result<Next<HandleState>, VfsError> {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let live = row_state(record_of::<VfsHandleRow>(row.as_ref())?)?;
        let next = match live.detached {
            Some(_) => {
                self.step = ResolveStep::Instance;
                let (scope, handle_id) = (self.scope.clone(), self.handle_id);
                vfs_call(VfsEffect::LookupHandle { scope, handle_id })
            }
            None => {
                self.step = ResolveStep::Proof;
                let key = proof_key(&live.key, &live.identity);
                IoCall::Read(name_reads(&self.scope.bucket, &key)?)
            }
        };
        self.live = Some(live);
        Ok(Next::Call(next))
    }

    fn instance_read(&mut self, result: IoResult) -> Result<Next<HandleState>, VfsError> {
        let live = self.live.take().ok_or(VfsError::StaleHandle)?;
        match vfs_event(result)? {
            VfsEvent::Instance(Some(summary)) if summary.open_count > 0 => {
                Ok(Next::Done(HandleState::Detached(live, Box::new(summary))))
            }
            VfsEvent::Instance(_) => Err(VfsError::StaleHandle),
            other => Err(unexpected(&other)),
        }
    }

    fn proof_read(&mut self, result: IoResult) -> Result<Next<HandleState>, VfsError> {
        let [head, token] = aruna_core::rows::values::<2>(result)?;
        let (head, token) = (head_of(head.as_ref())?, token_of(token.as_ref())?);
        let live = self.live.clone().ok_or(VfsError::StaleHandle)?;
        if identity_holds(&live.identity, head.as_ref(), token) {
            return Ok(Next::Done(HandleState::Live(live)));
        }
        if live.identity.kind() == HandleKind::File {
            return Err(VfsError::StaleHandle);
        }
        // A directory whose proof broke is expired while it still exists, else stale.
        self.step = ResolveStep::Probe;
        let prefix = BlobHeadKey::object_prefix(&self.scope.bucket, &marker_key(&live.key))?;
        Ok(Next::Call(IoCall::Page {
            space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: None,
            limit: STATE_PAGE,
        }))
    }

    fn probed(&mut self, result: IoResult) -> Result<Next<HandleState>, VfsError> {
        let (rows, next) = result.page()?;
        let live = rows.iter().any(|(_, value)| {
            let head = CurrentVersionPointer::from_bytes(value.as_ref());
            head.is_ok_and(|head| head.life_id.is_some())
        });
        match live || next.is_some() {
            true => Err(VfsError::ExpiredHandle),
            false => Err(VfsError::StaleHandle),
        }
    }
}

impl Operation for ResolveHandleOperation {
    type Output = HandleState;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<HandleState>, VfsError> {
        let key = VfsKey::Handle(self.scope.clone(), self.handle_id);
        Ok(Next::Call(state_read(&[key])?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HandleState>, VfsError> {
        match self.step {
            ResolveStep::Row => self.row_read(result),
            ResolveStep::Instance => self.instance_read(result),
            ResolveStep::Proof => self.proof_read(result),
            ResolveStep::Probe => self.probed(result),
        }
    }
}

/// The facts a new handle row is allocated under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandleClaim {
    pub scope: BucketScope,
    pub key: String,
    pub identity: HandleIdentity,
    pub mode: u32,
}

/// A handle id that is durable before anyone sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandleAllocation {
    pub handle_id: u64,
    pub cookie_id: u64,
    /// This call created the row; another caller's committed row is returned as it is.
    pub created: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClaimStep {
    Rows,
    Existing,
    Written,
}

/// Allocates or finds the handle of a name in one write transaction that reads the forward
/// and reverse rows and the observed identity, so a stale row never attaches to a new life.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ClaimHandleOperation {
    claim: HandleClaim,
    change: u64,
    step: ClaimStep,
    boot: VfsBootState,
    existing: Option<u64>,
    allocated: Option<HandleAllocation>,
    now_ms: u64,
}

impl ClaimHandleOperation {
    pub(crate) fn new(claim: HandleClaim, change: u64, now_ms: u64) -> Self {
        Self {
            claim,
            change,
            step: ClaimStep::Rows,
            boot: VfsBootState::default(),
            existing: None,
            allocated: None,
            now_ms,
        }
    }

    fn rows_read(&mut self, result: IoResult) -> Result<Next<HandleAllocation>, VfsError> {
        let [name, boot, head, token] = aruna_core::rows::values::<4>(result)?;
        let (head, token) = (head_of(head.as_ref())?, token_of(token.as_ref())?);
        if !identity_holds(&self.claim.identity, head.as_ref(), token) {
            return Err(VfsError::StaleHandle);
        }
        self.boot = record_of::<VfsBootState>(boot.as_ref())?.unwrap_or_default();
        let Some(name) = name else {
            return self.allocate(None);
        };
        let handle_id = decode_handle_id(name.as_ref())?;
        self.existing = Some(handle_id);
        self.step = ClaimStep::Existing;
        let key = VfsKey::Handle(self.claim.scope.clone(), handle_id);
        Ok(Next::Call(state_read(&[key])?))
    }

    fn existing_read(&mut self, result: IoResult) -> Result<Next<HandleAllocation>, VfsError> {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let handle_id = self.existing.ok_or(VfsError::Conflict)?;
        match record_of::<VfsHandleRow>(row.as_ref())? {
            Some(VfsHandleRow::Live(live))
                if live.key == self.claim.key
                    && live.identity == self.claim.identity
                    && live.detached.is_none() =>
            {
                Ok(Next::Done(HandleAllocation {
                    handle_id,
                    cookie_id: live.cookie_id,
                    created: false,
                }))
            }
            // A row bound to an earlier identity is stale; it never names the new life.
            Some(VfsHandleRow::Live(_)) => self.allocate(Some(handle_id)),
            _ => self.allocate(None),
        }
    }

    fn allocate(&mut self, stale: Option<u64>) -> Result<Next<HandleAllocation>, VfsError> {
        let (handle_id, boot) = self.boot.reserve_handles(1)?;
        let (cookie_id, boot) = boot.reserve_cookies(1)?;
        let scope = &self.claim.scope;
        let live = LiveHandle {
            key: self.claim.key.clone(),
            identity: self.claim.identity.clone(),
            mode: self.claim.mode,
            attribute_change: self.change,
            cookie_id,
            detached: None,
        };
        let name = VfsKey::Name(scope.clone(), self.claim.key.clone());
        let mut writes = vec![
            state_row(&VfsKey::Boot, &boot)?,
            state_row(
                &VfsKey::Handle(scope.clone(), handle_id),
                &VfsHandleRow::Live(live),
            )?,
            (
                NFS_STATE_KEYSPACE.to_string(),
                state_key(&name)?,
                Value::from(encode_handle_id(handle_id).to_vec()),
            ),
        ];
        if let Some(stale) = stale {
            let tombstone = VfsHandleRow::Tombstone {
                reason: RetireReason::Stale,
                retired_at_ms: self.now_ms,
            };
            writes.push(state_row(
                &VfsKey::Handle(scope.clone(), stale),
                &tombstone,
            )?);
        }
        self.allocated = Some(HandleAllocation {
            handle_id,
            cookie_id,
            created: true,
        });
        self.step = ClaimStep::Written;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for ClaimHandleOperation {
    type Output = HandleAllocation;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<HandleAllocation>, VfsError> {
        let scope = &self.claim.scope;
        let rows = [
            VfsKey::Name(scope.clone(), self.claim.key.clone()),
            VfsKey::Boot,
        ];
        let IoCall::Read(mut reads) = state_read(&rows)? else {
            return Err(VfsError::Conflict);
        };
        let proof = proof_key(&self.claim.key, &self.claim.identity);
        reads.extend(name_reads(&scope.bucket, &proof)?);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HandleAllocation>, VfsError> {
        match self.step {
            ClaimStep::Rows => self.rows_read(result),
            ClaimStep::Existing => self.existing_read(result),
            ClaimStep::Written => {
                result.done()?;
                self.allocated
                    .take()
                    .map(Next::Done)
                    .ok_or(VfsError::Conflict)
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AllocateStep {
    Permit,
    Change,
    Claim,
    Synced,
}

/// Allocates a handle lazily: under the namespace permit, one transaction guards the rows and
/// the observed identity, and a sync makes the id durable before it is returned.
#[derive(Debug, PartialEq)]
pub struct AllocateHandleOperation {
    claim: HandleClaim,
    step: AllocateStep,
    held: Held,
    allocation: Option<HandleAllocation>,
    now_ms: u64,
}

impl AllocateHandleOperation {
    pub fn new(claim: HandleClaim) -> Self {
        Self {
            claim,
            step: AllocateStep::Permit,
            held: Held::default(),
            allocation: None,
            now_ms: 0,
        }
    }

    fn call(
        &mut self,
        step: AllocateStep,
        call: IoCall,
    ) -> Result<Next<HandleAllocation>, VfsError> {
        self.step = step;
        Ok(Next::Call(call))
    }
}

impl Operation for AllocateHandleOperation {
    type Output = HandleAllocation;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<HandleAllocation>, VfsError> {
        let scope = self.claim.scope.clone();
        self.call(
            AllocateStep::Permit,
            vfs_call(VfsEffect::AcquirePermit { scope }),
        )
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HandleAllocation>, VfsError> {
        match self.step {
            AllocateStep::Permit => {
                self.held.permit_taken(result)?;
                let scope = self.claim.scope.clone();
                let key = Some(self.claim.key.clone());
                self.call(
                    AllocateStep::Change,
                    vfs_call(VfsEffect::NextChange { scope, key }),
                )
            }
            AllocateStep::Change => {
                let change = match vfs_event(result)? {
                    VfsEvent::ChangeValue { value } => value,
                    other => return Err(unexpected(&other)),
                };
                let claim = ClaimHandleOperation::new(self.claim.clone(), change, self.now_ms);
                let seed = self.claim.key.clone().into_bytes();
                self.call(AllocateStep::Claim, IoCall::retry(claim, &seed))
            }
            AllocateStep::Claim => {
                let allocation = result.child::<HandleAllocation, VfsError>()??;
                self.allocation = Some(allocation);
                // Another caller's committed row is synced too before its id leaves.
                self.call(AllocateStep::Synced, IoCall::Sync)
            }
            AllocateStep::Synced => {
                result.done()?;
                let output = self.allocation.take().ok_or(VfsError::Conflict)?;
                let calls = self.held.release();
                Ok(Next::Then { output, calls })
            }
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        self.held.release()
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

/// The rows that retire `handle_id`: its tombstone, its cookie's old position and the
/// removal of its reverse row when `name` still holds it.
pub(crate) fn retire_rows(
    scope: &BucketScope,
    handle_id: u64,
    live: &LiveHandle,
    reason: RetireReason,
    now_ms: u64,
    name: Option<&Value>,
) -> Result<super::StateChanges, VfsError> {
    let tombstone = VfsHandleRow::Tombstone {
        reason,
        retired_at_ms: now_ms,
    };
    let (directory, _) = split_parent(&live.key).unwrap_or_default();
    let position = CookiePosition {
        directory_key: directory.to_string(),
        entry_key: position_key(live),
        retired_at_ms: now_ms,
    };
    let writes = vec![
        state_row(&VfsKey::Handle(scope.clone(), handle_id), &tombstone)?,
        state_row(&VfsKey::Cookie(scope.clone(), live.cookie_id), &position)?,
    ];
    let named = name
        .map(|value| decode_handle_id(value.as_ref()))
        .transpose()?;
    let deletes = match named == Some(handle_id) {
        true => vec![state_delete(&VfsKey::Name(
            scope.clone(),
            live.key.clone(),
        ))?],
        false => Vec::new(),
    };
    Ok((writes, deletes))
}

/// The listing key a handle's cookie stood at: a directory sorts as `d/`.
pub fn position_key(live: &LiveHandle) -> String {
    match live.identity.kind() {
        HandleKind::File => live.key.clone(),
        HandleKind::Directory => marker_key(&live.key),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetireStep {
    Rows,
    Written,
    Deleted,
}

/// Retires one live handle row in one write transaction, unless it changed meanwhile.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RetireRowOperation {
    scope: BucketScope,
    handle_id: u64,
    expected: LiveHandle,
    reason: RetireReason,
    now_ms: u64,
    step: RetireStep,
    deletes: Vec<(String, Key)>,
}

impl RetireRowOperation {
    pub(crate) fn new(
        scope: BucketScope,
        handle_id: u64,
        expected: LiveHandle,
        reason: RetireReason,
        now_ms: u64,
    ) -> Self {
        Self {
            scope,
            handle_id,
            expected,
            reason,
            now_ms,
            step: RetireStep::Rows,
            deletes: Vec::new(),
        }
    }

    fn rows_read(&mut self, result: IoResult) -> Result<Next<bool>, VfsError> {
        let [row, name] = aruna_core::rows::values::<2>(result)?;
        let current = record_of::<VfsHandleRow>(row.as_ref())?;
        if current != Some(VfsHandleRow::Live(self.expected.clone())) {
            return Ok(Next::Done(false));
        }
        let (writes, deletes) = retire_rows(
            &self.scope,
            self.handle_id,
            &self.expected,
            self.reason,
            self.now_ms,
            name.as_ref(),
        )?;
        self.deletes = deletes;
        self.step = RetireStep::Written;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for RetireRowOperation {
    type Output = bool;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<bool>, VfsError> {
        let rows = [
            VfsKey::Handle(self.scope.clone(), self.handle_id),
            VfsKey::Name(self.scope.clone(), self.expected.key.clone()),
        ];
        Ok(Next::Call(state_read(&rows)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, VfsError> {
        match self.step {
            RetireStep::Rows => self.rows_read(result),
            RetireStep::Written => {
                result.done()?;
                self.step = RetireStep::Deleted;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            RetireStep::Deleted => {
                result.done()?;
                Ok(Next::Done(true))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermitStep {
    Permit,
    Child,
    Synced,
}

/// Retires a handle under the namespace permit: stale when its object is gone, expired when
/// a directory exists but its continuity is unknown. Answers whether the row changed.
#[derive(Debug, PartialEq)]
pub struct RetireHandleOperation {
    row: RetireRowOperation,
    step: PermitStep,
    held: Held,
    retired: bool,
}

impl RetireHandleOperation {
    pub fn new(
        scope: BucketScope,
        handle_id: u64,
        expected: LiveHandle,
        reason: RetireReason,
        now_ms: u64,
    ) -> Self {
        Self {
            row: RetireRowOperation::new(scope, handle_id, expected, reason, now_ms),
            step: PermitStep::Permit,
            held: Held::default(),
            retired: false,
        }
    }
}

impl Operation for RetireHandleOperation {
    type Output = bool;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<bool>, VfsError> {
        let scope = self.row.scope.clone();
        Ok(Next::Call(vfs_call(VfsEffect::AcquirePermit { scope })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, VfsError> {
        match self.step {
            PermitStep::Permit => {
                self.held.permit_taken(result)?;
                self.step = PermitStep::Child;
                let seed = self.row.handle_id.to_be_bytes();
                Ok(Next::Call(IoCall::retry(self.row.clone(), &seed)))
            }
            PermitStep::Child => {
                self.retired = result.child::<bool, VfsError>()??;
                self.step = PermitStep::Synced;
                Ok(Next::Call(IoCall::Sync))
            }
            PermitStep::Synced => {
                result.done()?;
                let calls = self.held.release();
                Ok(Next::Then {
                    output: self.retired,
                    calls,
                })
            }
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        self.held.release()
    }
}

/// Shows a listed object when the caller's scope allows its key and a live `s3.HeadObject`
/// admission of that exact key passes; a link is not served.
#[derive(Debug, PartialEq)]
pub(crate) struct VfsShows {
    pub scope: Option<SubpathScope>,
    pub context: VfsAccessContext,
    pub bucket: String,
}

impl ShowRule for VfsShows {
    type Object = ListedObject;
    type Error = VfsError;

    fn shows(&self, object: &ListedObject) -> Shown {
        let allowed = (self.scope.as_ref()).is_none_or(|scope| scope.allows_key(&object.head.key));
        if !allowed || object.link.is_some() {
            return Shown::Decided(false);
        }
        let key = Some(object.head.key.as_str());
        let request = VfsRequest::new(VfsAccess::Head, &self.bucket, key);
        Shown::Ask(authorize_call(&self.context, request))
    }

    fn answered(&self, result: IoResult) -> Result<Result<bool, VfsError>, IoError> {
        Ok(match result.child::<VfsGrant, VfsError>()? {
            Ok(_) => Ok(true),
            Err(error) => hidden(error).map(|()| false),
        })
    }

    fn groups(&self, prefix: &str) -> bool {
        (self.scope.as_ref()).is_none_or(|scope| scope.allows_prefix(prefix))
    }
}

/// Where a listing resumes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListResume {
    /// Cookie zero: the first entry.
    Start,
    /// After the last emitted order key returned by [`ListNext::After`].
    After(String),
    /// After the last emitted name and its cookie, or that cookie's old position.
    Cookie { cookie: u64, name: String },
    /// A sealed position among hidden entries, as an earlier page returned it.
    Sealed(Vec<u8>),
}

/// One listing request on a resolved directory.
#[derive(Clone, Debug, PartialEq)]
pub struct ListRequest {
    pub context: VfsAccessContext,
    /// The full key of the directory, such as `team` for a folder export's root; empty for
    /// the bucket root.
    pub directory: String,
    /// The directory handle's witness, checked around every listing read.
    pub witness: Option<DirectoryWitness>,
    pub resume: ListResume,
    pub max_entries: usize,
    pub deadline_ms: Option<u64>,
}

/// One shown name of a directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsEntry {
    pub name: String,
    pub kind: HandleKind,
    pub size: u64,
    pub version_id: Option<Ulid>,
    pub handle_id: Option<u64>,
    /// The name exists only as a stage, or its stage overlays the head.
    pub staged: bool,
}

impl VfsEntry {
    /// The key the entry sorts at: a directory as `d/`.
    fn order_key(&self) -> String {
        match self.kind {
            HandleKind::File => self.name.clone(),
            HandleKind::Directory => marker_key(&self.name),
        }
    }
}

/// Where the next page of a listing starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListNext {
    End,
    /// After this shown name.
    After(String),
    /// Among hidden entries, sealed; never reported as the end.
    Sealed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsListing {
    pub entries: Vec<VfsEntry>,
    pub next: ListNext,
    /// The change observer sequence the staged names were read at.
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListStep {
    Authorize,
    Cookie,
    CookieHandle,
    Staged,
    Page,
    Check,
    Descendant,
}

/// A denial hides one entry; any other failure ends the listing.
fn hidden(error: VfsError) -> Result<(), VfsError> {
    match error {
        VfsError::Denied | VfsError::CrossExport => Ok(()),
        error => Err(error),
    }
}

type ListTurn = Result<Next<VfsListing>, VfsError>;

/// Lists one directory: live S3 admission of `s3.ListObjectsV2`, one page of staged names,
/// then one visible page in exact key order. Every entry is admitted on its own, so a hidden
/// child adds no name, size or directory.
#[derive(Debug, PartialEq)]
pub struct VfsListOperation {
    request: ListRequest,
    step: ListStep,
    grant: Option<VfsGrant>,
    after: Option<String>,
    staged: Vec<StagedEntry>,
    /// The order key of the last staged name read when more follow; the page ends there.
    staged_cap: Option<String>,
    sequence: u64,
    queue: VecDeque<VfsEntry>,
    checking: Option<VfsEntry>,
    kept: Vec<VfsEntry>,
    next: Option<ListNext>,
    /// The staged keys below each directory that only staged names make, by its order key.
    descendants: BTreeMap<String, Vec<String>>,
    /// The staged keys still to try for the directory being checked.
    probes: VecDeque<String>,
}

impl VfsListOperation {
    pub fn new(request: ListRequest) -> Self {
        Self {
            request,
            step: ListStep::Authorize,
            grant: None,
            after: None,
            staged: Vec::new(),
            staged_cap: None,
            sequence: 0,
            queue: VecDeque::new(),
            checking: None,
            kept: Vec::new(),
            next: None,
            descendants: BTreeMap::new(),
            probes: VecDeque::new(),
        }
    }

    fn call(&mut self, step: ListStep, call: IoCall) -> ListTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn prefix(&self) -> String {
        children_prefix(&self.request.directory)
    }

    /// A full key as the name the listing sorts it by; a key outside the directory is no
    /// position of it.
    fn relative(&self, key: &str) -> Result<String, VfsError> {
        let name = key
            .strip_prefix(&self.prefix())
            .filter(|name| !name.is_empty());
        name.map(str::to_string).ok_or(VfsError::BadCookie)
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let bucket = (self.request.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        Ok(BucketScope::new(bucket, grant.incarnation))
    }

    /// The order key a committed page continues after.
    fn token_name(&self, token: &ListContinuationToken) -> Result<String, VfsError> {
        if let Some(group) = &token.last_common_prefix {
            return self.relative(group);
        }
        let bucket = format!("{}/", self.scope()?.bucket);
        let key = std::str::from_utf8(&token.last_key).map_err(|_| VfsError::BadCookie)?;
        self.relative(key.strip_prefix(&bucket).ok_or(VfsError::BadCookie)?)
    }

    fn authorized(&mut self, result: IoResult) -> ListTurn {
        self.grant = Some(grant_of(result)?);
        match self.request.resume.clone() {
            ListResume::Cookie { cookie: 0, .. } | ListResume::Start | ListResume::Sealed(_) => {
                self.read_staged()
            }
            ListResume::After(after) => {
                if after.is_empty() {
                    return Err(VfsError::BadCookie);
                }
                self.after = Some(after);
                self.read_staged()
            }
            ListResume::Cookie { cookie: 1 | 2, .. } => Err(VfsError::BadCookie),
            ListResume::Cookie { cookie, name } => {
                let scope = self.scope()?;
                let key = format!("{}{name}", self.prefix());
                let rows = [
                    VfsKey::Name(scope.clone(), key),
                    VfsKey::Cookie(scope, cookie),
                ];
                self.call(ListStep::Cookie, state_read(&rows)?)
            }
        }
    }

    /// Resumes after the cookie's current name, or after its old position in this directory.
    fn cookie_read(&mut self, result: IoResult) -> ListTurn {
        let [name, position] = aruna_core::rows::values::<2>(result)?;
        if let Some(name) = name {
            let scope = self.scope()?;
            let handle_id = decode_handle_id(name.as_ref())?;
            self.step = ListStep::CookieHandle;
            return Ok(Next::Call(state_read(&[VfsKey::Handle(scope, handle_id)])?));
        }
        self.after_position(position.as_ref())
    }

    fn after_position(&mut self, position: Option<&Value>) -> ListTurn {
        let position = record_of::<CookiePosition>(position)?.ok_or(VfsError::BadCookie)?;
        let directory = self.prefix();
        let directory = directory.strip_suffix('/').unwrap_or(&directory);
        if position.directory_key != directory {
            return Err(VfsError::BadCookie);
        }
        self.after = Some(self.relative(&position.entry_key)?);
        self.read_staged()
    }

    fn handle_read(&mut self, result: IoResult) -> ListTurn {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let ListResume::Cookie { cookie, .. } = self.request.resume else {
            return Err(VfsError::BadCookie);
        };
        match record_of::<VfsHandleRow>(row.as_ref())? {
            Some(VfsHandleRow::Live(live)) if live.cookie_id == cookie => {
                self.after = Some(self.relative(&position_key(&live))?);
                self.read_staged()
            }
            _ => Err(VfsError::BadCookie),
        }
    }

    /// One page of staged names after the resume point, as many as the listing returns. A
    /// resumed directory skips its whole subtree: no key below `d/` sorts after `d/` + U+10FFFF.
    fn read_staged(&mut self) -> ListTurn {
        let (scope, prefix) = (self.scope()?, self.prefix());
        let after = (self.after.as_ref()).map(|after| match after.ends_with('/') {
            true => format!("{prefix}{after}{}", char::MAX),
            false => format!("{prefix}{after}"),
        });
        let limit = Some(self.request.max_entries.max(1));
        let list = VfsEffect::ListStaged {
            scope,
            prefix,
            after,
            limit,
        };
        self.call(ListStep::Staged, vfs_call(list))
    }

    fn staged_read(&mut self, result: IoResult) -> ListTurn {
        let (entries, sequence, truncated) = match vfs_event(result)? {
            VfsEvent::StagedList {
                entries,
                sequence,
                truncated,
            } => (entries, sequence, truncated),
            other => return Err(unexpected(&other)),
        };
        let prefix = self.prefix();
        let last = entries
            .last()
            .and_then(|entry| entry.key.strip_prefix(&prefix));
        self.staged_cap = last
            .filter(|_| truncated)
            .map(|rest| match rest.split_once('/') {
                Some((name, _)) => marker_key(name),
                None => rest.to_string(),
            });
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        self.staged = entries
            .into_iter()
            .filter(|entry| grant.shows(&entry.key))
            .collect();
        self.sequence = sequence;
        let bucket = self.scope()?.bucket;
        let prefix = self.prefix();
        let continuation = (self.after.as_ref()).map(|after| {
            let group = after.ends_with('/');
            after_entry(&bucket, &format!("{prefix}{after}"), group)
        });
        let input = ListBucketInput {
            bucket,
            group_id: grant.bucket.group_id,
            continuation_token: continuation,
            max_keys: Some(self.request.max_entries.max(1)),
            prefix: Some(prefix),
            delimiter: Some("/".to_string()),
            start_after: None,
        };
        let rule = VfsShows {
            scope: grant.scope.clone(),
            context: self.request.context.clone(),
            bucket: input.bucket.clone(),
        };
        let mut page = VisiblePageOperation::new(RecordSource { bounds: None }, rule, input, None)
            .with_deadline(self.request.deadline_ms);
        let witness = self.request.witness.as_ref();
        if let Some(witness) = witness.and_then(|w| snapshot_of(&self.request.directory, w)) {
            page = page.with_witness(witness);
        }
        if let ListResume::Sealed(sealed) = &self.request.resume {
            page = page.opening(sealed.clone());
        }
        self.call(ListStep::Page, IoCall::child(page))
    }

    fn page_read(&mut self, result: IoResult) -> ListTurn {
        type Page = VisiblePage<ListedObject>;
        type Failure = VisibleError<VfsError, ListBucketError>;
        let page = match result.child::<Page, Failure>()? {
            Ok(page) => page,
            Err(VisibleError::Unopened | VisibleError::Cursor) => return Err(VfsError::BadCookie),
            Err(VisibleError::Io(error)) => return Err(error.into()),
            Err(error) => return Err(VfsError::Storage(format!("{error:?}"))),
        };
        // A witness that changed around a read discards the slice; the caller rebinds.
        if page.stale {
            return Err(VfsError::Conflict);
        }
        let prefix = self.prefix();
        let at_root = self.request.directory.is_empty();
        let protocol = self.request.context.protocol;
        let mut found = BTreeMap::new();
        for object in &page.objects {
            let Some(name) = object.head.key.strip_prefix(&prefix) else {
                continue;
            };
            if name.is_empty() || name.contains('/') || !representable(name, protocol, at_root) {
                continue;
            }
            let entry = VfsEntry {
                name: name.to_string(),
                kind: HandleKind::File,
                size: object
                    .location
                    .as_ref()
                    .map_or(0, |location| location.blob_size),
                version_id: Some(object.version_id),
                handle_id: None,
                staged: false,
            };
            found.insert(entry.order_key(), entry);
        }
        for group in &page.prefixes {
            let name = group
                .strip_prefix(&prefix)
                .and_then(|rest| rest.strip_suffix('/'));
            let Some(name) = name.filter(|name| representable(name, protocol, at_root)) else {
                continue;
            };
            // A directory shadows an exact file of the same name.
            found.remove(name);
            found.insert(marker_key(name), directory_entry(name, false));
        }
        // Each truncated source stops at its last name read; the page ends at the earlier one,
        // so neither source skips a name. A pause among hidden entries ends at its last shown.
        let committed_end = match &page.next {
            None => None,
            Some(NextPage::After(token)) => Some(self.token_name(token)?),
            Some(NextPage::Sealed(_)) => {
                Some(found.keys().next_back().cloned().unwrap_or_default())
            }
        };
        let horizon = match (committed_end.clone(), self.staged_cap.clone()) {
            (Some(committed), Some(staged)) => Some(committed.min(staged)),
            (committed, staged) => committed.or(staged),
        };
        let within = |key: &String| horizon.as_ref().is_none_or(|end| key <= end);
        found.retain(|key, _| within(key));
        self.descendants.clear();
        for staged in std::mem::take(&mut self.staged) {
            let Some(rest) = staged.key.strip_prefix(&prefix) else {
                continue;
            };
            let entry = match rest.split_once('/') {
                Some((name, _)) => directory_entry(name, true),
                None => VfsEntry {
                    name: rest.to_string(),
                    kind: staged.kind,
                    size: staged.size,
                    version_id: None,
                    handle_id: staged.handle_id,
                    staged: true,
                },
            };
            let key = entry.order_key();
            let before = self.after.as_ref().is_some_and(|after| key <= *after);
            if rest.is_empty() || before || !within(&key) {
                continue;
            }
            if !representable(&entry.name, protocol, at_root) {
                continue;
            }
            if entry.kind == HandleKind::Directory {
                found.remove(&entry.name);
            }
            let collapsed = rest.contains('/');
            if found.entry(key.clone()).or_insert(entry).staged && collapsed {
                self.descendants.entry(key).or_default().push(staged.key);
            }
        }
        let mut entries: Vec<VfsEntry> = found.into_values().collect();
        let cut = entries.len() > self.request.max_entries.max(1);
        entries.truncate(self.request.max_entries.max(1));
        let after_last = |entries: &[VfsEntry]| match entries.last() {
            Some(entry) => ListNext::After(entry.order_key()),
            None => ListNext::End,
        };
        // Even a page whose every name was hidden continues after its horizon, never ends.
        let next = match (page.next, horizon) {
            _ if cut => after_last(entries.as_slice()),
            (Some(NextPage::Sealed(sealed)), end) if end == committed_end => {
                ListNext::Sealed(sealed)
            }
            (_, Some(end)) => ListNext::After(end),
            (_, None) => ListNext::End,
        };
        self.next = Some(next);
        self.queue = entries.into();
        self.check_next()
    }

    /// Staged names and directories are admitted one by one before they show: a file by
    /// `s3.HeadObject` on its key, a directory by `s3.ListObjectsV2` on its prefix.
    fn check_next(&mut self) -> ListTurn {
        let bucket = self.scope()?.bucket;
        while let Some(entry) = self.queue.pop_front() {
            let key = format!("{}{}", self.prefix(), entry.name);
            let request = match (entry.kind, entry.staged) {
                (HandleKind::Directory, _) => {
                    let mut request = VfsRequest::new(VfsAccess::List, &bucket, None);
                    request.prefix = Some(marker_key(&key));
                    request
                }
                (HandleKind::File, true) => VfsRequest::new(VfsAccess::Head, &bucket, Some(&key)),
                // A committed file was admitted inside the visible page.
                (HandleKind::File, false) => {
                    self.kept.push(entry);
                    continue;
                }
            };
            self.checking = Some(entry);
            let call = authorize_call(&self.request.context, request);
            return self.call(ListStep::Check, call);
        }
        Ok(Next::Done(VfsListing {
            entries: std::mem::take(&mut self.kept),
            next: self.next.take().unwrap_or(ListNext::End),
            sequence: self.sequence,
        }))
    }

    fn checked(&mut self, result: IoResult) -> ListTurn {
        let entry = self.checking.take().ok_or(VfsError::Conflict)?;
        match grant_of(result) {
            Ok(_) => match self.descendants.remove(&entry.order_key()) {
                Some(keys) => {
                    self.probes = keys.into();
                    return self.next_probe(entry);
                }
                None => self.kept.push(entry),
            },
            Err(error) => hidden(error)?,
        }
        self.check_next()
    }

    /// A directory only staged names make shows through one of them the caller may head.
    fn next_probe(&mut self, entry: VfsEntry) -> ListTurn {
        let Some(key) = self.probes.pop_front() else {
            return self.check_next();
        };
        let bucket = self.scope()?.bucket;
        let request = VfsRequest::new(VfsAccess::Head, &bucket, Some(&key));
        self.checking = Some(entry);
        let call = authorize_call(&self.request.context, request);
        self.call(ListStep::Descendant, call)
    }

    fn descendant_checked(&mut self, result: IoResult) -> ListTurn {
        let entry = self.checking.take().ok_or(VfsError::Conflict)?;
        match grant_of(result) {
            Ok(_) => {
                self.probes.clear();
                self.kept.push(entry);
                self.check_next()
            }
            Err(error) => {
                hidden(error)?;
                self.next_probe(entry)
            }
        }
    }
}

fn directory_entry(name: &str, staged: bool) -> VfsEntry {
    VfsEntry {
        name: name.to_string(),
        kind: HandleKind::Directory,
        size: 0,
        version_id: None,
        handle_id: None,
        staged,
    }
}

impl Operation for VfsListOperation {
    type Output = VfsListing;
    type Error = VfsError;

    fn first(&mut self) -> ListTurn {
        let bucket = (self.request.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let mut request = VfsRequest::new(VfsAccess::List, bucket, None);
        request.prefix = Some(self.prefix());
        let call = authorize_call(&self.request.context, request);
        self.call(ListStep::Authorize, call)
    }

    fn next(&mut self, result: IoResult) -> ListTurn {
        match self.step {
            ListStep::Authorize => self.authorized(result),
            ListStep::Cookie => self.cookie_read(result),
            ListStep::CookieHandle => self.handle_read(result),
            ListStep::Staged => self.staged_read(result),
            ListStep::Page => self.page_read(result),
            ListStep::Check => self.checked(result),
            ListStep::Descendant => self.descendant_checked(result),
        }
    }
}
