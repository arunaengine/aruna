//! Immutable NFS checkpoint generations: planned, filled from the base, sealed with the overlay,
//! synced, promoted and collected, plus the cold-start inventory and restore of stage rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::BucketScope;
use aruna_core::types::GroupId;
use aruna_core::vfs_effects::{ByteRange, DiskStage, RestoreRequest, VfsEvent, VfsFailure};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use ulid::Ulid;

use crate::instances::{Instance, fenced, instance_mut, next_instance};
use crate::stages::{Extents, Sealed, Stage, read_range, volatile_name, write_range};
use crate::{Answer, Reservation, VfsHandle};

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
    /// Starts a new generation after charging its full size, so the previous generation
    /// stays intact when scratch space runs out.
    pub(crate) async fn begin_checkpoint(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
    ) -> Answer {
        let (missing, size, group_id, reservation) = {
            let mut state = self.shared.state.lock();
            if state.fences.closed(scope) {
                return Err(fenced());
            }
            let existing = state.checkpoints.get(&(stage_id, generation));
            if existing.is_some_and(|checkpoint| checkpoint.sealed) {
                return Err(VfsFailure::Io(
                    "the checkpoint generation exists".to_string(),
                ));
            }
            if let Some(draft) = state.checkpoints.remove(&(stage_id, generation)) {
                state.budget.release(draft.group_id, draft.bytes);
            }
            let instance = instance_mut(&mut state, scope, instance_id)?;
            let missing = instance.stage.missing_base();
            let size = instance.stage.logical_size;
            let group_id = instance.group_id;
            let reservation = Reservation::take(&self.shared, &mut state, group_id, size)?;
            (missing, size, group_id, reservation)
        };
        let draft = self.path(&draft_name(stage_id, generation));
        crate::blocking(move || File::create(&draft).map(drop)).await?;
        let bytes = reservation.keep();
        let checkpoint = Checkpoint {
            scope: scope.clone(),
            group_id,
            bytes,
            size,
            sealed: false,
            edits: 0,
        };
        self.shared
            .state
            .lock()
            .checkpoints
            .insert((stage_id, generation), checkpoint);
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
        {
            let state = self.shared.state.lock();
            let checkpoint = state.checkpoints.get(&(stage_id, generation));
            let checkpoint = checkpoint.filter(|checkpoint| !checkpoint.sealed);
            let checkpoint = checkpoint.ok_or(VfsFailure::NotFound)?;
            let end = offset.checked_add(data.len() as u64);
            end.filter(|end| *end <= checkpoint.size)
                .ok_or(VfsFailure::Overflow)?;
        }
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
        let (plan, edits, group_id, charged, extra) = {
            let mut state = self.shared.state.lock();
            let checkpoint = state.checkpoints.get(&(stage_id, generation));
            let checkpoint = checkpoint.filter(|checkpoint| !checkpoint.sealed);
            let charged = checkpoint.ok_or(VfsFailure::NotFound)?.bytes;
            let instance = instance_mut(&mut state, scope, instance_id)?;
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
            (plan, edits, group_id, charged, extra)
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

    /// Points the instance at a sealed generation, drops the overlay if nothing changed since
    /// seal and unlinks the superseded generation.
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

    pub(crate) async fn remove_checkpoint(&self, stage_id: Ulid, generation: u64) -> Answer {
        {
            let mut state = self.shared.state.lock();
            let in_use = (state.buckets.values())
                .flat_map(|bucket| bucket.instances.values())
                .filter_map(|instance| instance.stage.sealed)
                .any(|sealed| sealed.stage_id == stage_id && sealed.generation == generation);
            if in_use {
                return Err(VfsFailure::Io(
                    "a live instance reads this checkpoint".to_string(),
                ));
            }
            if let Some(checkpoint) = state.checkpoints.remove(&(stage_id, generation)) {
                state.budget.release(checkpoint.group_id, checkpoint.bytes);
            }
        }
        let names = generation_files(self, stage_id, Some(generation));
        crate::blocking(move || {
            for name in names {
                crate::remove_quietly(&name);
            }
            Ok(())
        })
        .await?;
        Ok(VfsEvent::Done)
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
    /// disk already, so the charge may pass the budget.
    pub(crate) fn restore_stage(&self, request: RestoreRequest) -> Answer {
        let changes = &self.shared.changes;
        let mut state = self.shared.state.lock();
        let instance_id = next_instance(&mut state)?;
        let change = changes.read(&request.scope.bucket, &request.key)?;
        let key = request.key.clone();
        let mut instance = Instance::new(instance_id, key, request.identity, request.group_id);
        instance.handle_id = Some(request.handle_id);
        instance.detached = request.detached;
        instance.dirty = true;
        instance.restored = true;
        instance.change = change;
        instance.stage = Stage::over(request.logical_size);
        instance.stage.base_limit = 0;
        instance.stage.sealed = Some(Sealed {
            stage_id: request.stage_id,
            generation: request.generation,
            limit: request.logical_size,
        });
        instance.stage_id = Some(request.stage_id);
        instance.generation = request.generation;
        let bucket = state.buckets.entry(request.scope.clone()).or_default();
        if request.detached.is_none() {
            if bucket.by_key.contains_key(&request.key) {
                return Err(VfsFailure::Io(
                    "the key already has a live instance".to_string(),
                ));
            }
            bucket.by_key.insert(request.key.clone(), instance_id);
        }
        bucket.by_handle.insert(request.handle_id, instance_id);
        bucket.instances.insert(instance_id, instance);
        let checkpoint = Checkpoint {
            scope: request.scope,
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
        Ok(VfsEvent::Opened {
            instance_id,
            created: true,
            size: request.logical_size,
            change,
        })
    }
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
