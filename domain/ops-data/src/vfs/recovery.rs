//! Settles committed namespace changes from their receipts, recovers the file servers before
//! they serve after a start, and sweeps expired tombstones, cookies, receipts and tokens.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::handles::{advance_witness, rests_on};
use super::mutate::{GuardedWriteOperation, MovePlan};
use super::path::{ancestors, marker_key};
use super::policy::{VfsAccess, VfsAccessContext, VfsRequest, authorize_call, grant_of};
use super::publish::{StageMoveOperation, retained};
use super::{
    STAGE_RETENTION_MS, STATE_PAGE, TOMBSTONE_RETENTION_MS, VfsError, head_key, head_of, record_of,
    state_delete, state_read, state_row, token_of, unexpected, vfs_call, vfs_event,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BUCKET_CREATION_KEYSPACE, NFS_STATE_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::vfs::{
    BucketScope, CookiePosition, DetachReason, DirectoryWitness, EnrolledHostRecord, FileIdentity,
    HandleIdentity, NfsMountBinding, NfsMountTarget, NfsStageRecord, RECEIPT_TAG, ReceiptChange,
    STAGE_TAG, StageBase, StageDisposition, VfsBootState, VfsHandleRow, VfsKey, VfsReceipt,
    VfsWatchKey, decode_handle_id,
};
use aruna_core::types::{Key, Value};
use aruna_core::vfs_effects::{
    DiskStage, RebindRequest, RestoreRequest, VfsEffect, VfsEvent, VfsProtocol, WitnessUpdate,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use ulid::Ulid;

/// How one attempt ended once its receipt was read after a sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptOutcome {
    /// The change committed; the life its target now holds, when it has one.
    Applied { life_id: Option<Ulid> },
    /// No receipt after a successful sync: the change did not commit.
    RolledBack,
}

/// What finishing one attempt needs besides its durable receipt.
#[derive(Clone, Debug, PartialEq)]
pub struct ReceiptInput {
    pub scope: BucketScope,
    pub attempt_id: Ulid,
    /// The live instance the attempt changed, when one is open.
    pub instance_id: Option<u64>,
    /// Open directory witnesses the attempt advanced, applied after the commit.
    pub updates: Vec<WitnessUpdate>,
    /// The logical size a publication left, which becomes the instance's base.
    pub logical_size: Option<u64>,
    /// The edit generation a publication streamed; a later edit keeps the overlay dirty.
    pub expected_edit: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiptStep {
    Synced,
    Receipt,
    Head,
    Rows,
    Finalized,
}

type ReceiptTurn = Result<Next<ReceiptOutcome>, VfsError>;

/// Finishes one attempt: after a sync of the foreground queue an absent receipt is a rollback;
/// a present one fixes the outcome, and its rebind, detach and witness updates are applied
/// once from current rows before the reservations go and the receipt is retired.
#[derive(Debug, PartialEq)]
pub struct VfsReceiptOperation {
    input: ReceiptInput,
    step: ReceiptStep,
    receipt: Option<VfsReceipt>,
    life_id: Option<Ulid>,
    keys: Vec<VfsKey>,
}

impl VfsReceiptOperation {
    pub fn new(input: ReceiptInput) -> Self {
        Self {
            input,
            step: ReceiptStep::Synced,
            receipt: None,
            life_id: None,
            keys: Vec::new(),
        }
    }

    fn call(&mut self, step: ReceiptStep, call: IoCall) -> ReceiptTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn receipt_read(&mut self, result: IoResult) -> ReceiptTurn {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let Some(receipt) = record_of::<VfsReceipt>(row.as_ref())? else {
            return Ok(Next::Then {
                output: ReceiptOutcome::RolledBack,
                calls: self.release(),
            });
        };
        if receipt.scope != self.input.scope {
            return Err(VfsError::Conflict);
        }
        let target = match &receipt.change {
            ReceiptChange::FilePublish { key, .. } => key.clone(),
            ReceiptChange::UnpublishedRename { to, .. } => to.clone(),
            // A moved directory lives on as its destination marker.
            ReceiptChange::DirectoryRename { to, .. } => marker_key(to),
            ReceiptChange::Remove { key, .. } => key.clone(),
        };
        self.receipt = Some(receipt);
        let space = BLOB_HEAD_KEYSPACE.to_string();
        let read = (space, head_key(&self.input.scope.bucket, &target)?);
        self.call(ReceiptStep::Head, IoCall::Read(vec![read]))
    }

    /// The current head decides the committed life; then the handle row and the reverse rows of
    /// every ancestor are read to advance witnesses that rest on the changed name.
    fn head_read(&mut self, result: IoResult) -> ReceiptTurn {
        let [head] = aruna_core::rows::values::<1>(result)?;
        let head = head_of(head.as_ref())?;
        let receipt = self.receipt.as_ref().ok_or(VfsError::Conflict)?;
        let scope = self.input.scope.clone();
        self.keys.clear();
        if let ReceiptChange::DirectoryRename { .. } = &receipt.change {
            self.life_id = super::live_life(head.as_ref());
        }
        if let ReceiptChange::FilePublish {
            key,
            version_id,
            handle_id,
            ..
        } = &receipt.change
        {
            let committed = head.as_ref().filter(|head| head.version_id == *version_id);
            self.life_id = committed.and_then(|head| head.life_id);
            if let Some(handle_id) = handle_id {
                self.keys.push(VfsKey::Handle(scope.clone(), *handle_id));
            }
            let parents = ancestors(key).into_iter();
            self.keys
                .extend(parents.map(|parent| VfsKey::Name(scope.clone(), parent)));
        }
        if self.keys.is_empty() {
            return self.settle(Vec::new());
        }
        let keys = self.keys.clone();
        self.call(ReceiptStep::Rows, state_read(&keys)?)
    }

    /// Ancestor reverse rows name handles; their rows are read in a second pass.
    fn rows_read(&mut self, result: IoResult) -> ReceiptTurn {
        let values = result.values()?;
        let scope = self.input.scope.clone();
        let pending: Vec<VfsKey> = self
            .keys
            .iter()
            .zip(&values)
            .filter_map(|(key, value)| {
                let VfsKey::Name(..) = key else { return None };
                let handle_id = value
                    .as_ref()
                    .and_then(|value| decode_handle_id(value.as_ref()).ok())?;
                Some(VfsKey::Handle(scope.clone(), handle_id))
            })
            .collect();
        let own = self
            .keys
            .iter()
            .find(|key| matches!(key, VfsKey::Handle(..)))
            .cloned();
        self.keys = own.into_iter().chain(pending).collect();
        let keys = self.keys.clone();
        self.step = ReceiptStep::Finalized;
        Ok(Next::Call(state_read(&keys)?))
    }

    /// Advances the published handle to its committed life and every ancestor witness that
    /// rested on the unpublished name, in one guarded transaction.
    fn rows_finalized(&mut self, result: IoResult) -> ReceiptTurn {
        let values = result.values()?;
        let receipt = self.receipt.clone().ok_or(VfsError::Conflict)?;
        let ReceiptChange::FilePublish { key, .. } = &receipt.change else {
            return self.settle(Vec::new());
        };
        let Some(life_id) = self.life_id else {
            return self.settle(Vec::new());
        };
        let mut plan = MovePlan::default();
        let mut updates = Vec::new();
        for (row_key, value) in self.keys.clone().iter().zip(values) {
            let Some(VfsHandleRow::Live(mut live)) = record_of::<VfsHandleRow>(value.as_ref())?
            else {
                continue;
            };
            let VfsKey::Handle(_, handle_id) = row_key else {
                continue;
            };
            let advanced = match &live.identity {
                HandleIdentity::File(_) if live.key == *key => {
                    Some(HandleIdentity::File(FileIdentity::Committed { life_id }))
                }
                HandleIdentity::Directory(witness) if rests_on(witness, key) => {
                    let replacement = DirectoryWitness::Descendant {
                        key: key.clone(),
                        life_id,
                    };
                    let witness = advance_witness(witness, key, Some(&replacement));
                    witness.map(HandleIdentity::Directory)
                }
                _ => None,
            };
            let Some(identity) = advanced.filter(|identity| *identity != live.identity) else {
                continue;
            };
            plan.guard(row_key, value.clone())?;
            if let HandleIdentity::Directory(witness) = &identity {
                updates.push(WitnessUpdate {
                    key: live.key.clone(),
                    handle_id: Some(*handle_id),
                    witness: witness.clone(),
                });
            }
            live.identity = identity;
            plan.writes
                .push(state_row(row_key, &VfsHandleRow::Live(live))?);
        }
        if plan.writes.is_empty() {
            return self.settle(updates);
        }
        // The open registry learns the advanced witnesses once the rows committed.
        self.input.updates.extend(updates);
        self.keys.clear();
        let seed = receipt.attempt_id.to_bytes();
        let write = IoCall::retry(GuardedWriteOperation::new(plan), &seed);
        self.call(ReceiptStep::Rows, write)
    }

    /// The adapter calls after the outcome: rebind or detach, witnesses, then the attempt's
    /// reservations, and only then the receipt row itself.
    fn settle(&mut self, updates: Vec<WitnessUpdate>) -> ReceiptTurn {
        let receipt = self.receipt.clone().ok_or(VfsError::Conflict)?;
        let scope = self.input.scope.clone();
        let mut calls = Vec::new();
        match &receipt.change {
            ReceiptChange::FilePublish {
                key,
                handle_id,
                version_id,
                ..
            } => {
                if let Some(instance_id) = self.input.instance_id {
                    let effect = match self.life_id {
                        // Every open of the shared instance reads its base from the new version.
                        Some(life_id) => VfsEffect::RebindInstance(RebindRequest {
                            scope: scope.clone(),
                            instance_id,
                            key: key.clone(),
                            handle_id: *handle_id,
                            identity: HandleIdentity::File(FileIdentity::Committed { life_id }),
                            base_size: self.input.logical_size,
                            // A committed move carries a dirty overlay onto its new version.
                            clear_dirty: self.input.logical_size.is_some(),
                            stage_id: None,
                            generation: None,
                            base_version: Some(*version_id),
                            expected_edit: self.input.expected_edit,
                        }),
                        // A newer S3 write replaced the name meanwhile: the instance detaches.
                        None => VfsEffect::DetachInstance {
                            scope: scope.clone(),
                            handle_id: handle_id.unwrap_or_default(),
                            instance_id: Some(instance_id),
                            reason: DetachReason::LostLife,
                        },
                    };
                    calls.push(vfs_call(effect));
                }
            }
            ReceiptChange::UnpublishedRename {
                to,
                token,
                handle_id,
                displaced,
                ..
            } => {
                if let Some(displaced) = displaced {
                    calls.push(vfs_call(VfsEffect::DetachInstance {
                        scope: scope.clone(),
                        handle_id: *displaced,
                        instance_id: None,
                        reason: DetachReason::Displaced,
                    }));
                }
                if let Some(instance_id) = self.input.instance_id {
                    calls.push(vfs_call(VfsEffect::RebindInstance(RebindRequest {
                        scope: scope.clone(),
                        instance_id,
                        key: to.clone(),
                        handle_id: *handle_id,
                        identity: HandleIdentity::File(FileIdentity::Unpublished {
                            token: *token,
                            predecessor: None,
                        }),
                        base_size: None,
                        clear_dirty: false,
                        stage_id: None,
                        generation: None,
                        base_version: None,
                        expected_edit: None,
                    })));
                }
            }
            ReceiptChange::DirectoryRename { to, handle_id, .. } => {
                if let Some(instance_id) = self.input.instance_id {
                    calls.push(vfs_call(VfsEffect::RebindInstance(RebindRequest {
                        scope: scope.clone(),
                        instance_id,
                        key: to.clone(),
                        handle_id: *handle_id,
                        identity: HandleIdentity::Directory(DirectoryWitness::Marker {
                            life_id: self.life_id.unwrap_or_default(),
                        }),
                        base_size: None,
                        clear_dirty: false,
                        stage_id: None,
                        generation: None,
                        base_version: None,
                        expected_edit: None,
                    })));
                }
            }
            ReceiptChange::Remove { handle_id, .. } => {
                if handle_id.is_some() || self.input.instance_id.is_some() {
                    calls.push(vfs_call(VfsEffect::DetachInstance {
                        scope: scope.clone(),
                        handle_id: handle_id.unwrap_or(0),
                        instance_id: self.input.instance_id,
                        reason: DetachReason::Unlinked,
                    }));
                }
            }
        }
        let mut all = std::mem::take(&mut self.input.updates);
        all.extend(updates);
        if !all.is_empty() {
            calls.push(vfs_call(VfsEffect::UpdateWitnesses {
                scope,
                updates: all,
            }));
        }
        calls.extend(self.release());
        calls.push(IoCall::Delete(vec![state_delete(&VfsKey::Receipt(
            receipt.attempt_id,
        ))?]));
        Ok(Next::Then {
            output: ReceiptOutcome::Applied {
                life_id: self.life_id,
            },
            calls,
        })
    }

    /// The attempt is resolved: its waiters wake and its reservations go.
    fn release(&self) -> Vec<IoCall> {
        let attempt_id = self.input.attempt_id;
        vec![
            vfs_call(VfsEffect::FinishAttempt { attempt_id }),
            vfs_call(VfsEffect::ReleaseAttempt { attempt_id }),
        ]
    }
}

impl Operation for VfsReceiptOperation {
    type Output = ReceiptOutcome;
    type Error = VfsError;

    fn first(&mut self) -> ReceiptTurn {
        // Work submitted earlier on the foreground queue is drained before the receipt is read.
        self.call(ReceiptStep::Synced, IoCall::Sync)
    }

    fn next(&mut self, result: IoResult) -> ReceiptTurn {
        match self.step {
            ReceiptStep::Synced => {
                result.done()?;
                let read = state_read(&[VfsKey::Receipt(self.input.attempt_id)])?;
                self.call(ReceiptStep::Receipt, read)
            }
            ReceiptStep::Receipt => self.receipt_read(result),
            ReceiptStep::Head => self.head_read(result),
            ReceiptStep::Rows if self.keys.is_empty() => {
                result.child::<(), VfsError>()??;
                self.settle(Vec::new())
            }
            ReceiptStep::Rows => self.rows_read(result),
            ReceiptStep::Finalized => self.rows_finalized(result),
        }
    }
}

/// What a cold start found and did before the file servers serve.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColdStart {
    pub boot_generation: u64,
    pub verifier: [u8; 8],
    pub receipts: usize,
    pub restored: usize,
    pub retained: usize,
    /// Referenced checkpoints that are missing or corrupt: surfaced storage failures.
    pub corrupt: Vec<Ulid>,
    /// Checkpoint files no row names after the complete inventory; they were removed.
    pub orphans: Vec<(Ulid, u64)>,
}

/// The write verifier of a boot: its generation, which never repeats.
pub fn boot_verifier(boot_generation: u64) -> [u8; 8] {
    boot_generation.to_be_bytes()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColdStep {
    Boot,
    Written,
    Synced,
    Configured,
    Receipts,
    Receipt,
    Inventory,
    Stages,
    Bucket,
    Head,
    Mount,
    Host,
    Admit,
    Stage,
    Restored,
    Orphan,
}

type ColdTurn = Result<Next<ColdStart>, VfsError>;

/// Recovers the durable file server state before serving: a new synced boot generation and
/// write verifier, every receipt settled from durable rows, every stage row checked against
/// the checkpoint inventory and restored, retained or surfaced, then orphan files collected.
#[derive(Debug, PartialEq)]
pub struct ColdStartOperation {
    step: ColdStep,
    report: ColdStart,
    cursor: Option<Key>,
    receipts: VecDeque<VfsReceipt>,
    files: BTreeMap<(Ulid, u64), DiskStage>,
    referenced: BTreeSet<(Ulid, u64)>,
    stages: VecDeque<(VfsKey, NfsStageRecord)>,
    buckets: BTreeMap<String, Option<(u128, BucketInfo)>>,
    current: Option<(VfsKey, NfsStageRecord)>,
    orphans: VecDeque<(Ulid, u64)>,
    now_ms: u64,
    /// The realm and node a writer's credential is admitted on; without them no live stage
    /// shows again.
    admission: Option<(RealmId, NodeId)>,
    /// The mount binding of the current stage's NFS writer.
    binding: Option<NfsMountBinding>,
}

impl Default for ColdStartOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl ColdStartOperation {
    pub fn new() -> Self {
        Self {
            step: ColdStep::Boot,
            report: ColdStart::default(),
            cursor: None,
            receipts: VecDeque::new(),
            files: BTreeMap::new(),
            referenced: BTreeSet::new(),
            stages: VecDeque::new(),
            buckets: BTreeMap::new(),
            current: None,
            orphans: VecDeque::new(),
            now_ms: 0,
            admission: None,
            binding: None,
        }
    }

    /// The realm and node on which each live stage's writer is admitted again before it shows.
    pub fn with_admission(mut self, realm_id: RealmId, node_id: NodeId) -> Self {
        self.admission = Some((realm_id, node_id));
        self
    }

    fn call(&mut self, step: ColdStep, call: IoCall) -> ColdTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn page(&mut self, step: ColdStep, tag: u8) -> ColdTurn {
        let call = super::state_page(vec![tag], self.cursor.clone(), STATE_PAGE);
        self.call(step, call)
    }

    fn booted(&mut self, result: IoResult) -> ColdTurn {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let state = record_of::<VfsBootState>(row.as_ref())?.unwrap_or_default();
        let state = state.next_boot()?;
        self.report.boot_generation = state.boot_generation;
        self.report.verifier = boot_verifier(state.boot_generation);
        let write = state_row(&VfsKey::Boot, &state)?;
        self.call(ColdStep::Written, IoCall::Write(vec![write]))
    }

    fn receipts_read(&mut self, result: IoResult) -> ColdTurn {
        let (rows, next) = result.page()?;
        for (_, value) in &rows {
            if let Some(receipt) = record_of::<VfsReceipt>(Some(value))? {
                self.receipts.push_back(receipt);
            }
        }
        self.cursor = next;
        if self.cursor.is_some() {
            return self.page(ColdStep::Receipts, RECEIPT_TAG);
        }
        self.next_receipt()
    }

    /// Durable post-transaction rows are the authority: each receipt is applied, then retired.
    fn next_receipt(&mut self) -> ColdTurn {
        let Some(receipt) = self.receipts.pop_front() else {
            return self.call(ColdStep::Inventory, vfs_call(VfsEffect::ScanStages));
        };
        self.report.receipts += 1;
        let input = ReceiptInput {
            scope: receipt.scope,
            attempt_id: receipt.attempt_id,
            instance_id: None,
            updates: Vec::new(),
            logical_size: None,
            expected_edit: None,
        };
        self.call(
            ColdStep::Receipt,
            IoCall::child(VfsReceiptOperation::new(input)),
        )
    }

    fn inventory_read(&mut self, result: IoResult) -> ColdTurn {
        let files = match vfs_event(result)? {
            VfsEvent::StageInventory { files } => files,
            other => return Err(unexpected(&other)),
        };
        self.files = files
            .into_iter()
            .map(|file| ((file.stage_id, file.generation), file))
            .collect();
        self.cursor = None;
        self.page(ColdStep::Stages, STAGE_TAG)
    }

    fn stages_read(&mut self, result: IoResult) -> ColdTurn {
        let (rows, next) = result.page()?;
        for (key, value) in &rows {
            let key = VfsKey::from_bytes(key.as_ref())?;
            if let Some(record) = record_of::<NfsStageRecord>(Some(value))? {
                self.referenced.insert((record.stage_id, record.generation));
                self.stages.push_back((key, record));
            }
        }
        self.cursor = next;
        if self.cursor.is_some() {
            return self.page(ColdStep::Stages, STAGE_TAG);
        }
        self.next_stage()
    }

    fn scope_of(key: &VfsKey) -> Option<&BucketScope> {
        match key {
            VfsKey::LiveStage(scope, _) | VfsKey::RetainedStage(scope, _) => Some(scope),
            _ => None,
        }
    }

    fn next_stage(&mut self) -> ColdTurn {
        let Some((key, record)) = self.stages.pop_front() else {
            return self.collect_orphans();
        };
        let scope = Self::scope_of(&key)
            .cloned()
            .ok_or(VfsError::CorruptStage)?;
        let file = self.files.get(&(record.stage_id, record.generation));
        // A referenced checkpoint that is missing or corrupt is a surfaced storage failure.
        if file.is_none_or(|file| file.corrupt || file.bytes != record.logical_size) {
            self.report.corrupt.push(record.stage_id);
            return self.next_stage();
        }
        self.current = Some((key, record));
        if self.buckets.contains_key(&scope.bucket) {
            return self.check_head();
        }
        let bucket = Key::from(scope.bucket.as_bytes().to_vec());
        let reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
            (BUCKET_CREATION_KEYSPACE.to_string(), bucket),
        ];
        self.call(ColdStep::Bucket, IoCall::Read(reads))
    }

    fn bucket_read(&mut self, result: IoResult) -> ColdTurn {
        let [info, creation] = aruna_core::rows::values::<2>(result)?;
        let (key, _) = self.current.as_ref().ok_or(VfsError::Conflict)?;
        let bucket = Self::scope_of(key)
            .ok_or(VfsError::CorruptStage)?
            .bucket
            .clone();
        let info = info
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        let known = info.map(|info| (incarnation(creation.as_deref()), info));
        self.buckets.insert(bucket, known);
        self.check_head()
    }

    /// A live stage whose base no longer holds its name is retained for its owner; a stage of
    /// another bucket incarnation stays for the sweep and is not restored.
    fn check_head(&mut self) -> ColdTurn {
        let (key, record) = self.current.clone().ok_or(VfsError::Conflict)?;
        let scope = Self::scope_of(&key).ok_or(VfsError::CorruptStage)?;
        let bucket = self.buckets.get(&scope.bucket).cloned().flatten();
        if bucket.is_none_or(|(current, _)| current != scope.incarnation) {
            self.current = None;
            return self.next_stage();
        }
        let reads = super::name_reads(&scope.bucket, &record.key)?;
        self.call(ColdStep::Head, IoCall::Read(reads))
    }

    /// A derived checkpoint holds its name only on the exact base version it captured: an S3
    /// overwrite keeps the life, so the newer head stays visible and the bytes are retained.
    fn head_read(&mut self, result: IoResult) -> ColdTurn {
        let [head, token] = aruna_core::rows::values::<2>(result)?;
        let (head, token) = (head_of(head.as_ref())?, token_of(token.as_ref())?);
        let (key, record) = self.current.clone().ok_or(VfsError::Conflict)?;
        let holds = match record.base {
            StageBase::Committed {
                life_id,
                version_id,
            } => {
                let version = head.as_ref().map(|head| head.version_id);
                super::live_life(head.as_ref()) == Some(life_id) && version == Some(version_id)
            }
            StageBase::Unpublished { token: bound, .. } => token == Some(bound),
        };
        if !matches!(key, VfsKey::LiveStage(..)) {
            return self.restore(key, record);
        }
        if !holds {
            return self.retain_current();
        }
        // A writer's stage shows again only while its binding, host and credential still stand.
        match record.owner.mount_id.is_nil() {
            true => {
                let scope = Self::scope_of(&key).ok_or(VfsError::CorruptStage)?;
                let target = NfsMountTarget::for_bucket(&scope.bucket, scope.incarnation)?;
                self.admit(target, VfsProtocol::Smb, false)
            }
            false => {
                let read = state_read(&[VfsKey::Mount(record.owner.mount_id)])?;
                self.call(ColdStep::Mount, read)
            }
        }
    }

    fn mount_read(&mut self, result: IoResult) -> ColdTurn {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let (key, _) = self.current.as_ref().ok_or(VfsError::Conflict)?;
        let scope = Self::scope_of(key).ok_or(VfsError::CorruptStage)?;
        let binding = record_of::<NfsMountBinding>(row.as_ref())?;
        let live = binding.filter(|binding| binding.revoked_at_ms.is_none());
        let bound = live.filter(|binding| match &binding.target {
            NfsMountTarget::Bucket {
                bucket,
                bucket_incarnation,
            }
            | NfsMountTarget::Folder {
                bucket,
                bucket_incarnation,
                ..
            } => *bucket == scope.bucket && *bucket_incarnation == scope.incarnation,
            NfsMountTarget::JobInputView { .. } => false,
        });
        match bound {
            Some(binding) => {
                let read = state_read(&[VfsKey::Host(binding.host_id)])?;
                self.binding = Some(binding);
                self.call(ColdStep::Host, read)
            }
            None => self.retain_current(),
        }
    }

    fn host_read(&mut self, result: IoResult) -> ColdTurn {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let host = record_of::<EnrolledHostRecord>(row.as_ref())?;
        let binding = self.binding.take().ok_or(VfsError::Conflict)?;
        // A trusted worker pool host serves mounts of other owners too.
        let live = host.is_some_and(|host| {
            host.revoked_at_ms.is_none() && (binding.owner == host.owner || host.worker_pool)
        });
        if !live {
            return self.retain_current();
        }
        self.admit(binding.target, VfsProtocol::Nfs, binding.read_only)
    }

    /// The recorded writer's credential is admitted again to write the name at the stage's
    /// size through its export, exactly as a new write would be.
    fn admit(
        &mut self,
        target: NfsMountTarget,
        protocol: VfsProtocol,
        read_only: bool,
    ) -> ColdTurn {
        let Some((realm_id, node_id)) = self.admission else {
            return self.retain_current();
        };
        let (key, record) = self.current.as_ref().ok_or(VfsError::Conflict)?;
        let bucket = Self::scope_of(key)
            .ok_or(VfsError::CorruptStage)?
            .bucket
            .clone();
        let context = VfsAccessContext {
            realm_id,
            node_id,
            access_key: record.owner.access_key.clone(),
            protocol,
            target,
            read_only,
            now_ms: self.now_ms,
        };
        let mut request = VfsRequest::new(VfsAccess::Put, &bucket, Some(&record.key));
        request.length = Some(record.logical_size);
        self.call(ColdStep::Admit, authorize_call(&context, request))
    }

    /// A refused writer keeps its bytes by stage id; a transient failure ends the start.
    fn admitted(&mut self, result: IoResult) -> ColdTurn {
        match grant_of(result) {
            Ok(_) => {
                let (key, record) = self.current.clone().ok_or(VfsError::Conflict)?;
                self.restore(key, record)
            }
            Err(error @ (VfsError::Unavailable(_) | VfsError::Storage(_))) => Err(error),
            Err(_) => self.retain_current(),
        }
    }

    /// The live checkpoint that may not show again is kept by its stage id before anything else,
    /// then charged as the retained row it became.
    fn retain_current(&mut self) -> ColdTurn {
        let (key, record) = self.current.clone().ok_or(VfsError::Conflict)?;
        let scope = Self::scope_of(&key)
            .cloned()
            .ok_or(VfsError::CorruptStage)?;
        let kept = retained(&record, DetachReason::LostLife, self.now_ms);
        self.report.retained += 1;
        let change = StageMoveOperation::new(scope.clone(), record.clone(), Some(kept.clone()));
        self.current = Some((VfsKey::RetainedStage(scope, record.stage_id), kept));
        let seed = record.stage_id.to_bytes();
        self.call(ColdStep::Stage, IoCall::retry(change, &seed))
    }

    fn restore(&mut self, key: VfsKey, record: NfsStageRecord) -> ColdTurn {
        self.report.restored += 1;
        self.restore_call(key, record)
    }

    /// Restores `record` into memory; a retained one is only charged to the scratch budget.
    fn restore_call(&mut self, key: VfsKey, record: NfsStageRecord) -> ColdTurn {
        let scope = Self::scope_of(&key)
            .cloned()
            .ok_or(VfsError::CorruptStage)?;
        let group_id = self.buckets.get(&scope.bucket).cloned().flatten();
        let group_id = group_id
            .map(|(_, info)| info.group_id)
            .ok_or(VfsError::CorruptStage)?;
        let (identity, base_version) = match record.base {
            StageBase::Committed {
                life_id,
                version_id,
            } => (FileIdentity::Committed { life_id }, Some(version_id)),
            StageBase::Unpublished { token, predecessor } => {
                (FileIdentity::Unpublished { token, predecessor }, None)
            }
        };
        let detached = match record.disposition {
            StageDisposition::Retained { reason } => Some(reason),
            _ => None,
        };
        let request = RestoreRequest {
            scope,
            handle_id: record.handle_id,
            key: record.key.clone(),
            stage_id: record.stage_id,
            generation: record.generation,
            logical_size: record.logical_size,
            group_id,
            identity: HandleIdentity::File(identity),
            detached,
            owner: Some(record.owner.clone()),
            base_version,
        };
        self.call(
            ColdStep::Restored,
            vfs_call(VfsEffect::RestoreStage(request)),
        )
    }

    /// Only after the complete inventory and every row is an unnamed file an orphan.
    fn collect_orphans(&mut self) -> ColdTurn {
        let referenced = &self.referenced;
        let orphans = self.files.keys().filter(|file| !referenced.contains(file));
        self.orphans = orphans.copied().collect();
        self.report.orphans = self.orphans.iter().copied().collect();
        self.next_orphan()
    }

    fn next_orphan(&mut self) -> ColdTurn {
        let Some((stage_id, generation)) = self.orphans.pop_front() else {
            return Ok(Next::Done(std::mem::take(&mut self.report)));
        };
        let remove = VfsEffect::RemoveCheckpoint {
            stage_id,
            generation,
        };
        self.call(ColdStep::Orphan, vfs_call(remove))
    }
}

impl Operation for ColdStartOperation {
    type Output = ColdStart;
    type Error = VfsError;

    fn first(&mut self) -> ColdTurn {
        self.call(ColdStep::Boot, state_read(&[VfsKey::Boot])?)
    }

    fn next(&mut self, result: IoResult) -> ColdTurn {
        match self.step {
            ColdStep::Boot => self.booted(result),
            ColdStep::Written => {
                result.done()?;
                self.call(ColdStep::Synced, IoCall::Sync)
            }
            ColdStep::Synced => {
                result.done()?;
                let configure = VfsEffect::ConfigureBoot {
                    boot_generation: self.report.boot_generation,
                    verifier: self.report.verifier,
                };
                self.call(ColdStep::Configured, vfs_call(configure))
            }
            ColdStep::Configured => {
                vfs_event(result)?;
                self.cursor = None;
                self.page(ColdStep::Receipts, RECEIPT_TAG)
            }
            ColdStep::Receipts => self.receipts_read(result),
            ColdStep::Receipt => {
                result.child::<ReceiptOutcome, VfsError>()??;
                self.next_receipt()
            }
            ColdStep::Inventory => self.inventory_read(result),
            ColdStep::Stages => self.stages_read(result),
            ColdStep::Bucket => self.bucket_read(result),
            ColdStep::Head => self.head_read(result),
            ColdStep::Mount => self.mount_read(result),
            ColdStep::Host => self.host_read(result),
            ColdStep::Admit => self.admitted(result),
            ColdStep::Stage => {
                result.child::<(), VfsError>()??;
                let (key, record) = self.current.take().ok_or(VfsError::Conflict)?;
                self.restore_call(key, record)
            }
            // A live row reopens its instance; a retained one is only charged and answers done.
            ColdStep::Restored => match vfs_event(result)? {
                VfsEvent::Opened { .. } | VfsEvent::Done => {
                    self.current = None;
                    self.next_stage()
                }
                other => Err(unexpected(&other)),
            },
            ColdStep::Orphan => {
                vfs_event(result)?;
                self.next_orphan()
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

/// What one sweep page removed and what is due.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Where the next page starts; `None` once both passes are done.
    pub next: Option<Vec<u8>>,
    pub removed: usize,
    /// Writerless checkpoints whose quota retry is due, by scope and handle.
    pub due: Vec<(BucketScope, u64)>,
}

/// When the tombstone sweep runs again after a pass that finished, and after a failed one.
pub const TOMBSTONE_SWEEP_AFTER: std::time::Duration = std::time::Duration::from_secs(60 * 60);
pub const TOMBSTONE_SWEEP_RETRY: std::time::Duration = std::time::Duration::from_secs(60);

/// The first byte of a sweep cursor: which pass it continues.
const STATE_PASS: u8 = 0;
const TOKEN_PASS: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SweepStep {
    StatePage,
    Attempts,
    Receipt,
    Checkpoint,
    Delete,
    TokenPage,
    TokenRows,
    TokenInstance,
    TokenDelete,
}

type SweepTurn = Result<Next<SweepOutcome>, VfsError>;

/// One bounded page of the `SweepNfsTombstones` timer. The state pass drops expired tombstones,
/// cookie positions and retained (never live) checkpoints, settles finished receipts and reports
/// due quota retries; the token pass removes watch keys no stage, handle or instance holds.
#[derive(Clone, Debug, PartialEq)]
pub struct SweepTombstonesOperation {
    now_ms: u64,
    tombstone_retention_ms: u64,
    stage_retention_ms: u64,
    cursor: Option<Vec<u8>>,
    step: SweepStep,
    outcome: SweepOutcome,
    deletes: Vec<(String, Key)>,
    receipts: VecDeque<VfsReceipt>,
    /// Expired retained checkpoints: their row key and record.
    stages: VecDeque<(Key, NfsStageRecord)>,
    scopes: VecDeque<BucketScope>,
    active: BTreeSet<Ulid>,
    tokens: VecDeque<(VfsWatchKey, Value)>,
    token: Option<(VfsWatchKey, Value, BucketScope)>,
    next: Option<Vec<u8>>,
}

impl SweepTombstonesOperation {
    pub fn new(now_ms: u64, cursor: Option<Vec<u8>>) -> Self {
        Self {
            now_ms,
            tombstone_retention_ms: TOMBSTONE_RETENTION_MS,
            stage_retention_ms: STAGE_RETENTION_MS,
            cursor,
            step: SweepStep::StatePage,
            outcome: SweepOutcome::default(),
            deletes: Vec::new(),
            receipts: VecDeque::new(),
            stages: VecDeque::new(),
            scopes: VecDeque::new(),
            active: BTreeSet::new(),
            tokens: VecDeque::new(),
            token: None,
            next: None,
        }
    }

    /// How long tombstones and cookie positions, and retained checkpoints, stay before expiry.
    pub fn with_retention(mut self, tombstone_retention_ms: u64, stage_retention_ms: u64) -> Self {
        self.tombstone_retention_ms = tombstone_retention_ms;
        self.stage_retention_ms = stage_retention_ms;
        self
    }

    fn call(&mut self, step: SweepStep, call: IoCall) -> SweepTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn expired(&self, retired_at_ms: u64) -> bool {
        retired_at_ms.saturating_add(self.tombstone_retention_ms) <= self.now_ms
    }

    fn state_read(&mut self, result: IoResult) -> SweepTurn {
        let (rows, next) = result.page()?;
        for (key, value) in rows {
            let Ok(parsed) = VfsKey::from_bytes(key.as_ref()) else {
                continue;
            };
            match parsed {
                VfsKey::Handle(..) => {
                    let row = record_of::<VfsHandleRow>(Some(&value)).ok().flatten();
                    if let Some(VfsHandleRow::Tombstone { retired_at_ms, .. }) = row
                        && self.expired(retired_at_ms)
                    {
                        self.deletes.push((NFS_STATE_KEYSPACE.to_string(), key));
                    }
                }
                VfsKey::Cookie(..) => {
                    let position = record_of::<CookiePosition>(Some(&value)).ok().flatten();
                    if position.is_some_and(|position| self.expired(position.retired_at_ms)) {
                        self.deletes.push((NFS_STATE_KEYSPACE.to_string(), key));
                    }
                }
                VfsKey::Receipt(_) => {
                    if let Some(receipt) = record_of::<VfsReceipt>(Some(&value))? {
                        if !self.scopes.contains(&receipt.scope) {
                            self.scopes.push_back(receipt.scope.clone());
                        }
                        self.receipts.push_back(receipt);
                    }
                }
                VfsKey::LiveStage(scope, handle_id) => {
                    let record = record_of::<NfsStageRecord>(Some(&value))?;
                    let retry = record.and_then(|record| record.quota_retry);
                    if retry.is_some_and(|retry| retry.next_at_ms <= self.now_ms) {
                        self.outcome.due.push((scope, handle_id));
                    }
                }
                VfsKey::RetainedStage(..) => {
                    let record = record_of::<NfsStageRecord>(Some(&value)).ok().flatten();
                    if let Some(record) = record
                        && record.updated_at_ms.saturating_add(self.stage_retention_ms)
                            <= self.now_ms
                    {
                        self.stages.push_back((key, record));
                    }
                }
                _ => {}
            }
        }
        self.next = next.map(|key| [vec![STATE_PASS], key.to_vec()].concat());
        self.next_scope()
    }

    /// Receipts of attempts an active runner still owns are left to that runner.
    fn next_scope(&mut self) -> SweepTurn {
        if let Some(scope) = self.scopes.pop_front() {
            return self.call(
                SweepStep::Attempts,
                vfs_call(VfsEffect::ActiveAttempts { scope }),
            );
        }
        self.next_receipt()
    }

    fn next_receipt(&mut self) -> SweepTurn {
        while let Some(receipt) = self.receipts.pop_front() {
            if self.active.contains(&receipt.attempt_id) {
                continue;
            }
            let input = ReceiptInput {
                scope: receipt.scope,
                attempt_id: receipt.attempt_id,
                instance_id: None,
                updates: Vec::new(),
                logical_size: None,
                expected_edit: None,
            };
            let finish = VfsReceiptOperation::new(input);
            return self.call(SweepStep::Receipt, IoCall::child(finish));
        }
        self.next_stage()
    }

    /// An expired retained checkpoint loses its file first; only a removed file drops its row.
    fn next_stage(&mut self) -> SweepTurn {
        if let Some((_, record)) = self.stages.front() {
            let remove = VfsEffect::RemoveCheckpoint {
                stage_id: record.stage_id,
                generation: record.generation,
            };
            return self.call(SweepStep::Checkpoint, vfs_call(remove));
        }
        let deletes = std::mem::take(&mut self.deletes);
        self.outcome.removed += deletes.len();
        self.call(SweepStep::Delete, IoCall::Delete(deletes))
    }

    fn after_state(&mut self) -> SweepTurn {
        match self.next.take() {
            Some(next) => self.finish(Some(next)),
            None => self.token_page(None),
        }
    }

    fn token_page(&mut self, after: Option<Key>) -> SweepTurn {
        let call = IoCall::Page {
            space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: vec![aruna_core::structs::storage::vfs::WATCH_TAG].into(),
            after,
            limit: STATE_PAGE,
        };
        self.call(SweepStep::TokenPage, call)
    }

    fn tokens_read(&mut self, result: IoResult) -> SweepTurn {
        let (rows, next) = result.page()?;
        for (key, value) in rows {
            if let Ok(watch) = VfsWatchKey::from_bytes(key.as_ref()) {
                self.tokens.push_back((watch, value));
            }
        }
        self.next = next.map(|key| [vec![TOKEN_PASS], key.to_vec()].concat());
        self.next_token()
    }

    /// A token is kept while its bucket's reverse row names a handle bound to it.
    fn next_token(&mut self) -> SweepTurn {
        let Some((watch, value)) = self.tokens.pop_front() else {
            let next = self.next.take();
            return self.finish(next);
        };
        let bucket = Key::from(watch.bucket.as_bytes().to_vec());
        let reads = vec![(BUCKET_CREATION_KEYSPACE.to_string(), bucket)];
        self.token = Some((watch, value, BucketScope::new("", 0)));
        self.call(SweepStep::TokenRows, IoCall::Read(reads))
    }

    fn token_rows(&mut self, result: IoResult) -> SweepTurn {
        let [creation] = aruna_core::rows::values::<1>(result)?;
        let (watch, value, _) = self.token.take().ok_or(VfsError::Conflict)?;
        let scope = BucketScope::new(&watch.bucket, incarnation(creation.as_deref()));
        let lookup = VfsEffect::LookupInstance {
            scope: scope.clone(),
            key: watch.key.clone(),
        };
        self.token = Some((watch, value, scope));
        self.call(SweepStep::TokenInstance, vfs_call(lookup))
    }

    /// No live instance holds the token: it is removed only while it still reads the same.
    fn token_instance(&mut self, result: IoResult) -> SweepTurn {
        let (watch, value, scope) = self.token.take().ok_or(VfsError::Conflict)?;
        // Any answer but a definite absence keeps the token for a later sweep.
        let held = !matches!(vfs_event(result), Ok(VfsEvent::Instance(None)));
        if held {
            return self.next_token();
        }
        let space = BLOB_HEAD_KEYSPACE.to_string();
        let row = Key::from(watch.to_bytes()?);
        let mut plan = MovePlan::default();
        plan.guards.push((space.clone(), row.clone(), Some(value)));
        plan.guard(&VfsKey::Name(scope, watch.key.clone()), None)?;
        plan.deletes.push((space, row));
        let seed = watch.to_bytes()?;
        self.call(
            SweepStep::TokenDelete,
            IoCall::retry(GuardedWriteOperation::new(plan), &seed),
        )
    }

    fn finish(&mut self, next: Option<Vec<u8>>) -> SweepTurn {
        let mut outcome = std::mem::take(&mut self.outcome);
        outcome.next = next;
        Ok(Next::Done(outcome))
    }
}

impl Operation for SweepTombstonesOperation {
    type Output = SweepOutcome;
    type Error = VfsError;

    fn first(&mut self) -> SweepTurn {
        let cursor = self.cursor.take().unwrap_or_default();
        match cursor.split_first() {
            Some((&TOKEN_PASS, after)) => self.token_page(Some(Key::from(after.to_vec()))),
            Some((_, after)) if !after.is_empty() => {
                let call =
                    super::state_page(Vec::new(), Some(Key::from(after.to_vec())), STATE_PAGE);
                self.call(SweepStep::StatePage, call)
            }
            _ => self.call(
                SweepStep::StatePage,
                super::state_page(Vec::new(), None, STATE_PAGE),
            ),
        }
    }

    fn next(&mut self, result: IoResult) -> SweepTurn {
        match self.step {
            SweepStep::StatePage => self.state_read(result),
            SweepStep::Attempts => {
                match vfs_event(result) {
                    Ok(VfsEvent::Attempts { ids }) => self.active.extend(ids),
                    // Without the adapter's answer no receipt of the scope counts as finished.
                    _ => {
                        let active = self.receipts.iter().map(|receipt| receipt.attempt_id);
                        self.active.extend(active.collect::<Vec<_>>());
                    }
                }
                self.next_scope()
            }
            SweepStep::Receipt => {
                // A receipt that cannot be settled now stays for the next sweep.
                let _settled = result.child::<ReceiptOutcome, VfsError>()?;
                self.outcome.removed += 1;
                self.next_receipt()
            }
            SweepStep::Checkpoint => {
                let (key, _) = self.stages.pop_front().ok_or(VfsError::Conflict)?;
                // A checkpoint the adapter could not remove keeps its row for the next sweep.
                if vfs_event(result).is_ok() {
                    self.deletes.push((NFS_STATE_KEYSPACE.to_string(), key));
                }
                self.next_stage()
            }
            SweepStep::Delete => {
                result.done()?;
                self.after_state()
            }
            SweepStep::TokenPage => self.tokens_read(result),
            SweepStep::TokenRows => self.token_rows(result),
            SweepStep::TokenInstance => self.token_instance(result),
            SweepStep::TokenDelete => {
                // A token that changed meanwhile is simply kept.
                if result.child::<(), VfsError>()?.is_ok() {
                    self.outcome.removed += 1;
                }
                self.next_token()
            }
        }
    }
}
