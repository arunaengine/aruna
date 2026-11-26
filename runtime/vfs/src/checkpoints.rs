//! Immutable NFS checkpoint generations: planned, filled from the base, sealed with the overlay,
//! synced, promoted and collected, plus the cold-start inventory and restore of stage rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::BucketScope;
use aruna_core::types::GroupId;
use aruna_core::vfs_effects::{ByteRange, DiskStage, RestoreRequest, VfsEvent, VfsFailure};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use ulid::Ulid;

use crate::fence::WriteAdmission;
use crate::instances::{Instance, fenced, instance_mut, next_instance, release_stage};
use crate::stages::{Extents, Sealed, Stage, read_range, volatile_name, write_range};
use crate::{Answer, Reservation, Shared, State, VfsHandle};

/// Bytes copied per step while sealing, so no checkpoint is ever held in memory whole.
const COPY_CHUNK: u64 = 1 << 20;

/// One checkpoint generation known to this node and the scratch bytes it holds.
pub(crate) struct Checkpoint {
    pub(crate) scope: BucketScope,
    pub(crate) group_id: GroupId,
    /// Scratch bytes charged for this generation.
    pub(crate) bytes: u64,
    pub(crate) size: u64,
    pub(crate) sealed: bool,
    /// The instance edit count captured at seal time.
    edits: u64,
}

/// Which sealed generation an instance switches to, and which one it leaves.
pub(crate) struct Promotion {
    pub(crate) stage_id: Ulid,
    pub(crate) generation: u64,
    pub(crate) old_generation: Option<u64>,
}

pub(crate) fn sealed_name(stage_id: Ulid, generation: u64) -> String {
    format!("chk-{stage_id}-{generation}.bin")
}

fn draft_name(stage_id: Ulid, generation: u64) -> String {
    format!("chk-{stage_id}-{generation}.tmp")
}

/// Parses a checkpoint file name into its stage, generation and whether it is sealed.
pub(crate) fn parse_name(name: &str) -> Option<(Ulid, u64, bool)> {
    let rest = name.strip_prefix("chk-")?;
    let (rest, sealed) = match rest.strip_suffix(".bin") {
        Some(rest) => (rest, true),
        None => (rest.strip_suffix(".tmp")?, false),
    };
    let (stage_id, generation) = rest.split_once('-')?;
    Some((
        Ulid::from_string(stage_id).ok()?,
        generation.parse().ok()?,
        sealed,
    ))
}

/// Copies `[start, end)` of `source` to the same offsets of `target`, one chunk at a time.
fn copy_range(source: &mut File, target: &mut File, start: u64, end: u64) -> std::io::Result<()> {
    let mut offset = start;
    while offset < end {
        let length = COPY_CHUNK.min(end - offset);
        let data = read_range(source, offset, length)?;
        target.seek(SeekFrom::Start(offset))?;
        target.write_all(&data)?;
        offset += length;
    }
    Ok(())
}

/// What sealing writes on top of the base chunks already filled in.
struct SealPlan {
    draft: PathBuf,
    sealed: PathBuf,
    root: PathBuf,
    prior: Option<(PathBuf, u64)>,
    volatile: PathBuf,
    extents: Extents,
    size: u64,
}

/// A draft claim from begin until its file exists; dropping it armed ends the claim.
struct DraftClaim {
    shared: Arc<Shared>,
    scope: BucketScope,
    instance_id: u64,
    key: (Ulid, u64),
    armed: bool,
}

impl Drop for DraftClaim {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self.shared.state.lock();
            end_claim(&mut state, &self.scope, self.instance_id, self.key);
        }
    }
}

/// Ends the claim of an instance on draft `key` and gives back its charge; false if not held.
fn end_claim(state: &mut State, scope: &BucketScope, instance_id: u64, key: (Ulid, u64)) -> bool {
    let Ok(instance) = instance_mut(state, scope, instance_id) else {
        return false;
    };
    if instance.draft != Some(key) {
        return false;
    }
    instance.draft = None;
    if let Some(checkpoint) = state.checkpoints.remove(&key) {
        state.budget.release(checkpoint.group_id, checkpoint.bytes);
    }
    true
}

/// Whether `instance` reads, names or claims the generation `key`.
fn references(instance: &Instance, key: (Ulid, u64)) -> bool {
    let sealed = (instance.stage.sealed)
        .is_some_and(|sealed| sealed.stage_id == key.0 && sealed.generation == key.1);
    let named = instance.stage_id == Some(key.0) && instance.generation == key.1;
    sealed || named || instance.draft == Some(key)
}

fn seal_file(plan: &SealPlan) -> std::io::Result<()> {
    let mut target = OpenOptions::new().write(true).open(&plan.draft)?;
    if let Some((prior, limit)) = &plan.prior {
        let mut source = File::open(prior)?;
        copy_range(&mut source, &mut target, 0, (*limit).min(plan.size))?;
    }
    if !plan.extents.is_empty() {
        let mut source = File::open(&plan.volatile)?;
        for (start, end) in plan.extents.ranges() {
            copy_range(&mut source, &mut target, *start, (*end).min(plan.size))?;
        }
    }
    target.set_len(plan.size)?;
    target.sync_data()?;
    drop(target);
    std::fs::rename(&plan.draft, &plan.sealed)?;
    crate::sync_folder(&plan.root)
}

impl VfsHandle {
    /// Claims the one draft of the instance and charges its full size before creating its file,
    /// so a concurrent begin conflicts and the previous generation stays intact on no space.
    pub(crate) async fn begin_checkpoint(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
    ) -> Answer {
        let key = (stage_id, generation);
        let (missing, size, mut claim, _admission) = {
            let mut guard = self.shared.state.lock();
            let state = &mut *guard;
            if state.fences.closed(scope) {
                return Err(fenced());
            }
            if state.checkpoints.contains_key(&key) {
                return Err(VfsFailure::Conflict);
            }
            let bucket = state.buckets.get_mut(scope).ok_or(VfsFailure::NotFound)?;
            let instance = (bucket.instances.get_mut(&instance_id)).ok_or(VfsFailure::NotFound)?;
            if instance.draft.is_some() {
                return Err(VfsFailure::Conflict);
            }
            let missing = instance.stage.missing_base();
            let size = instance.stage.logical_size;
            let group_id = instance.group_id;
            state.budget.reserve(group_id, size)?;
            instance.draft = Some(key);
            let checkpoint = Checkpoint {
                scope: scope.clone(),
                group_id,
                bytes: size,
                size,
                sealed: false,
                edits: 0,
            };
            state.checkpoints.insert(key, checkpoint);
            let claim = DraftClaim {
                shared: Arc::clone(&self.shared),
                scope: scope.clone(),
                instance_id,
                key,
                armed: true,
            };
            let admission = WriteAdmission::enter(&self.shared, state, scope);
            (missing, size, claim, admission)
        };
        let draft = self.path(&draft_name(stage_id, generation));
        // Never opens an existing file: it may be another request's draft or a sealed generation.
        let created = crate::blocking(move || {
            match OpenOptions::new().write(true).create_new(true).open(&draft) {
                Ok(_) => Ok(true),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(false),
                Err(error) => Err(error),
            }
        })
        .await?;
        if !created {
            return Err(VfsFailure::Conflict);
        }
        claim.armed = false;
        let missing_base = (missing.into_iter())
            .map(|(start, end)| ByteRange {
                offset: start,
                length: end - start,
            })
            .collect();
        Ok(VfsEvent::CheckpointPlan {
            missing_base,
            logical_size: size,
        })
    }

    pub(crate) async fn fill_checkpoint(
        &self,
        stage_id: Ulid,
        generation: u64,
        offset: u64,
        data: Vec<u8>,
    ) -> Answer {
        let _admission = {
            let mut state = self.shared.state.lock();
            let checkpoint = state.checkpoints.get(&(stage_id, generation));
            let checkpoint = checkpoint.filter(|checkpoint| !checkpoint.sealed);
            let checkpoint = checkpoint.ok_or(VfsFailure::NotFound)?;
            let end = offset.checked_add(data.len() as u64);
            end.filter(|end| *end <= checkpoint.size)
                .ok_or(VfsFailure::Overflow)?;
            let scope = checkpoint.scope.clone();
            if state.fences.closed(&scope) {
                return Err(fenced());
            }
            WriteAdmission::enter(&self.shared, &mut state, &scope)
        };
        let draft = self.path(&draft_name(stage_id, generation));
        crate::blocking(move || write_range(&draft, offset, &data)).await?;
        Ok(VfsEvent::Done)
    }

    /// Lays the previous generation and the overlay onto the draft, syncs it and its folder,
    /// and only then gives it its sealed name.
    pub(crate) async fn seal_checkpoint(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
    ) -> Answer {
        let (plan, edits, group_id, charged, extra, _admission) = {
            let mut state = self.shared.state.lock();
            if state.fences.closed(scope) {
                return Err(fenced());
            }
            let checkpoint = state.checkpoints.get(&(stage_id, generation));
            let checkpoint = checkpoint.filter(|checkpoint| !checkpoint.sealed);
            let charged = checkpoint.ok_or(VfsFailure::NotFound)?.bytes;
            let instance = instance_mut(&mut state, scope, instance_id)?;
            if instance.draft != Some((stage_id, generation)) {
                return Err(VfsFailure::Conflict);
            }
            let stage = &mut instance.stage;
            stage.low_water = stage.logical_size;
            let plan = SealPlan {
                draft: self.path(&draft_name(stage_id, generation)),
                sealed: self.path(&sealed_name(stage_id, generation)),
                root: self.shared.root.clone(),
                prior: (stage.sealed).map(|prior| {
                    let name = sealed_name(prior.stage_id, prior.generation);
                    (self.path(&name), prior.limit)
                }),
                volatile: self.path(&volatile_name(instance_id)),
                extents: stage.extents.clone(),
                size: stage.logical_size,
            };
            let edits = stage.edits;
            let group_id = instance.group_id;
            let grown = plan.size.saturating_sub(charged);
            let extra = Reservation::take(&self.shared, &mut state, group_id, grown)?;
            let admission = WriteAdmission::enter(&self.shared, &mut state, scope);
            (plan, edits, group_id, charged, extra, admission)
        };
        let size = plan.size;
        let filename = sealed_name(stage_id, generation);
        crate::blocking(move || seal_file(&plan)).await?;
        let extra = extra.keep();
        let mut state = self.shared.state.lock();
        let held = charged + extra;
        state.budget.release(group_id, held.saturating_sub(size));
        if let Some(checkpoint) = state.checkpoints.get_mut(&(stage_id, generation)) {
            checkpoint.sealed = true;
            checkpoint.size = size;
            checkpoint.bytes = held.min(size);
            checkpoint.edits = edits;
        }
        Ok(VfsEvent::CheckpointWritten { filename, size })
    }

    /// Points the instance at its claimed sealed generation, drops the overlay if nothing changed
    /// since seal and unlinks the superseded generation unless something still references it.
    pub(crate) async fn promote_checkpoint(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        promotion: Promotion,
    ) -> Answer {
        let Promotion {
            stage_id,
            generation,
            old_generation,
        } = promotion;
        let (cleared, superseded) = {
            let mut state = self.shared.state.lock();
            let checkpoint = state.checkpoints.get(&(stage_id, generation));
            let checkpoint = checkpoint.filter(|checkpoint| checkpoint.sealed);
            let checkpoint = checkpoint.ok_or(VfsFailure::NotFound)?;
            let (size, edits) = (checkpoint.size, checkpoint.edits);
            let instance = instance_mut(&mut state, scope, instance_id)?;
            if instance.draft != Some((stage_id, generation)) {
                return Err(VfsFailure::Conflict);
            }
            instance.draft = None;
            let group_id = instance.group_id;
            instance.stage_id = Some(stage_id);
            instance.generation = generation;
            let stage = &mut instance.stage;
            stage.sealed = Some(Sealed {
                stage_id,
                generation,
                limit: size.min(stage.low_water).min(stage.logical_size),
            });
            let mut cleared = 0;
            if stage.edits == edits {
                cleared = stage.extents.total();
                stage.extents = Extents::default();
            }
            state.budget.release(group_id, cleared);
            let old = old_generation.filter(|old| *old != generation);
            let old = old.filter(|old| {
                let mut instances = state
                    .buckets
                    .values()
                    .flat_map(|bucket| bucket.instances.values());
                !instances.any(|instance| references(instance, (stage_id, *old)))
            });
            let superseded = old.and_then(|old| state.checkpoints.remove(&(stage_id, old)));
            if let Some(old) = superseded.as_ref() {
                state.budget.release(old.group_id, old.bytes);
            }
            (cleared > 0, old)
        };
        let volatile = self.path(&volatile_name(instance_id));
        let names = generation_files(self, stage_id, superseded);
        crate::blocking(move || {
            if cleared {
                crate::remove_quietly(&volatile);
            }
            for name in names {
                crate::remove_quietly(&name);
            }
            Ok(())
        })
        .await?;
        Ok(VfsEvent::Done)
    }

    /// Reads at most `length` bytes of a sealed generation.
    pub(crate) async fn read_checkpoint(
        &self,
        stage_id: Ulid,
        generation: u64,
        offset: u64,
        length: u32,
    ) -> Answer {
        let path = self.path(&sealed_name(stage_id, generation));
        crate::blocking(move || {
            let mut file = File::open(&path)?;
            let size = file.metadata()?.len();
            let end = offset.saturating_add(u64::from(length)).min(size);
            let data = match offset < end {
                true => read_range(&mut file, offset, end - offset)?,
                false => Vec::new(),
            };
            let eof = offset.saturating_add(data.len() as u64) >= size;
            Ok(VfsEvent::CheckpointData { data, eof })
        })
        .await
    }

    /// Drops the generation this instance claimed, sealed or not, and its files.
    pub(crate) async fn abort_checkpoint(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        key: (Ulid, u64),
    ) -> Answer {
        if !end_claim(&mut self.shared.state.lock(), scope, instance_id, key) {
            return Err(VfsFailure::NotFound);
        }
        let names = generation_files(self, key.0, Some(key.1));
        remove_files(names).await
    }

    /// Removes a generation no claim and no open or named instance references; a writerless
    /// detached instance gives up its reference and leaves once idle.
    pub(crate) async fn remove_checkpoint(&self, stage_id: Ulid, generation: u64) -> Answer {
        let key = (stage_id, generation);
        let mut names = generation_files(self, stage_id, Some(generation));
        {
            let mut state = self.shared.state.lock();
            let mut held = Vec::new();
            for (scope, bucket) in &state.buckets {
                for instance in bucket.instances.values() {
                    if !references(instance, key) {
                        continue;
                    }
                    let live = !instance.opens.is_empty() || instance.detached.is_none();
                    if live || instance.draft == Some(key) {
                        return Err(VfsFailure::Conflict);
                    }
                    held.push((scope.clone(), instance.id));
                }
            }
            for (scope, instance_id) in held {
                let volatile = release_stage(&mut state, &scope, instance_id);
                names.extend(volatile.map(|name| self.path(&name)));
            }
            if let Some(checkpoint) = state.checkpoints.remove(&key) {
                state.budget.release(checkpoint.group_id, checkpoint.bytes);
            }
        }
        remove_files(names).await
    }

    pub(crate) async fn scan_stages(&self) -> Answer {
        let root = self.shared.root.clone();
        let files = crate::blocking(move || inventory(&root)).await?;
        Ok(VfsEvent::StageInventory { files })
    }

    pub(crate) async fn check_stage(
        &self,
        stage_id: Ulid,
        generation: u64,
        expected_size: u64,
    ) -> Answer {
        let path = self.path(&sealed_name(stage_id, generation));
        crate::blocking(move || match std::fs::metadata(&path) {
            Ok(metadata) => Ok(VfsEvent::StageChecked {
                valid: metadata.is_file() && metadata.len() == expected_size,
                size: metadata.len(),
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(VfsEvent::StageChecked {
                    valid: false,
                    size: 0,
                })
            }
            Err(error) => Err(error),
        })
        .await
    }

    /// Rebuilds the in-memory instance and charges of a durable stage row; its bytes exist on
    /// disk already, so the charge may pass the budget. A detached row gets no instance.
    pub(crate) fn restore_stage(&self, request: RestoreRequest) -> Answer {
        let changes = &self.shared.changes;
        let mut state = self.shared.state.lock();
        let mut live = None;
        if request.detached.is_none() {
            let bucket = state.buckets.get(&request.scope);
            if bucket.is_some_and(|bucket| bucket.by_key.contains_key(&request.key)) {
                return Err(VfsFailure::Io(
                    "the key already has a live instance".to_string(),
                ));
            }
            let instance_id = next_instance(&mut state)?;
            live = Some((
                instance_id,
                changes.read(&request.scope.bucket, &request.key)?,
            ));
        }
        let checkpoint = Checkpoint {
            scope: request.scope.clone(),
            group_id: request.group_id,
            bytes: request.logical_size,
            size: request.logical_size,
            sealed: true,
            edits: 0,
        };
        let replaced = state
            .checkpoints
            .insert((request.stage_id, request.generation), checkpoint);
        if let Some(replaced) = replaced {
            state.budget.release(replaced.group_id, replaced.bytes);
        }
        state.budget.force(request.group_id, request.logical_size);
        let Some((instance_id, change)) = live else {
            return Ok(VfsEvent::Done);
        };
        let key = request.key.clone();
        let mut instance = Instance::new(instance_id, key, request.identity, request.group_id);
        instance.handle_id = Some(request.handle_id);
        instance.dirty = true;
        instance.restored = true;
        instance.change = change;
        instance.base_version = request.base_version;
        instance.stage = Stage::over(request.logical_size);
        instance.stage.base_limit = 0;
        instance.stage.sealed = Some(Sealed {
            stage_id: request.stage_id,
            generation: request.generation,
            limit: request.logical_size,
        });
        instance.stage_id = Some(request.stage_id);
        instance.generation = request.generation;
        let bucket = state.buckets.entry(request.scope).or_default();
        bucket.by_key.insert(request.key, instance_id);
        bucket.by_handle.insert(request.handle_id, instance_id);
        bucket.instances.insert(instance_id, instance);
        Ok(VfsEvent::Opened {
            instance_id,
            created: true,
            size: request.logical_size,
            change,
        })
    }
}

/// Unlinks files that may already be gone.
async fn remove_files(names: Vec<PathBuf>) -> Answer {
    crate::blocking(move || {
        for name in names {
            crate::remove_quietly(&name);
        }
        Ok(VfsEvent::Done)
    })
    .await
}

/// Both file names a generation may have.
fn generation_files(handle: &VfsHandle, stage_id: Ulid, generation: Option<u64>) -> Vec<PathBuf> {
    generation.map_or_else(Vec::new, |generation| {
        vec![
            handle.path(&sealed_name(stage_id, generation)),
            handle.path(&draft_name(stage_id, generation)),
        ]
    })
}

/// Every checkpoint file in the stage folder; an unsealed draft counts as corrupt.
pub(crate) fn inventory(root: &Path) -> std::io::Result<Vec<DiskStage>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some((stage_id, generation, sealed)) = parse_name(&name.to_string_lossy()) else {
            continue;
        };
        files.push(DiskStage {
            stage_id,
            generation,
            bytes: entry.metadata()?.len(),
            corrupt: !sealed,
        });
    }
    files.sort_by_key(|file| (file.stage_id, file.generation));
    Ok(files)
}
