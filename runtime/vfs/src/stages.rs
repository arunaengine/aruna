//! Sparse volatile stages: written extents over a surviving base length, exact scratch
//! budgets per node and group, and staged reads, writes and resizes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::BucketScope;
use aruna_core::types::GroupId;
use aruna_core::vfs_effects::{StageSlice, VfsEvent, VfsFailure};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use ulid::Ulid;

use crate::checkpoints::sealed_name;
use crate::instances::{fenced, instance_mut};
use crate::{Answer, Reservation, VfsHandle};

/// The largest file offset the stage files can hold.
const OFFSET_LIMIT: u64 = i64::MAX as u64;

/// The volatile file of an instance; it never outlives the boot that wrote it.
pub(crate) fn volatile_name(instance_id: u64) -> String {
    format!("vol-{instance_id}.tmp")
}

pub(crate) fn is_volatile(name: &str) -> bool {
    volatile_id(name).is_some()
}

pub(crate) fn volatile_id(name: &str) -> Option<u64> {
    let id = name.strip_prefix("vol-")?.strip_suffix(".tmp")?;
    id.parse().ok()
}

/// Sorted, disjoint and non-adjacent written ranges `[start, end)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Extents(Vec<(u64, u64)>);

impl Extents {
    pub(crate) fn total(&self) -> u64 {
        self.0.iter().map(|(start, end)| end - start).sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn ranges(&self) -> &[(u64, u64)] {
        &self.0
    }

    /// Bytes of `[start, end)` already written.
    pub(crate) fn covered(&self, start: u64, end: u64) -> u64 {
        (self.0.iter())
            .map(|(from, to)| (*to).min(end).saturating_sub((*from).max(start)))
            .sum()
    }

    /// Marks `[start, end)` written and returns how many bytes are new.
    pub(crate) fn insert(&mut self, start: u64, end: u64) -> u64 {
        if start >= end {
            return 0;
        }
        let added = (end - start) - self.covered(start, end);
        let (mut from, mut to) = (start, end);
        let mut kept = Vec::with_capacity(self.0.len() + 1);
        for (left, right) in self.0.drain(..) {
            if right < from || left > to {
                kept.push((left, right));
            } else {
                from = from.min(left);
                to = to.max(right);
            }
        }
        kept.push((from, to));
        kept.sort_unstable();
        self.0 = kept;
        added
    }

    /// Drops everything at or past `limit` and returns how many bytes went.
    pub(crate) fn trim(&mut self, limit: u64) -> u64 {
        let before = self.total();
        self.0.retain_mut(|(start, end)| {
            *end = (*end).min(limit);
            *start < *end
        });
        before - self.total()
    }

    /// The parts of `[start, end)` no extent covers.
    pub(crate) fn gaps(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        let mut gaps = Vec::new();
        let mut cursor = start;
        for (from, to) in &self.0 {
            if *to <= cursor {
                continue;
            }
            if *from >= end {
                break;
            }
            if *from > cursor {
                gaps.push((cursor, (*from).min(end)));
            }
            cursor = cursor.max(*to);
        }
        if cursor < end {
            gaps.push((cursor, end));
        }
        gaps
    }
}

/// The immutable checkpoint generation a stage reads below its volatile overlay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sealed {
    pub(crate) stage_id: Ulid,
    pub(crate) generation: u64,
    /// Bytes of the checkpoint still part of the file after later truncation.
    pub(crate) limit: u64,
}

/// Sparse stage state: reads take the overlay, then the checkpoint, then the base, then zeros.
#[derive(Clone, Debug, Default)]
pub(crate) struct Stage {
    pub(crate) logical_size: u64,
    pub(crate) base_limit: u64,
    pub(crate) extents: Extents,
    pub(crate) sealed: Option<Sealed>,
    /// Counts writes and resizes, so a promotion knows whether the overlay changed since seal.
    pub(crate) edits: u64,
    /// The smallest size since the last seal began.
    pub(crate) low_water: u64,
}

impl Stage {
    pub(crate) fn over(base_size: u64) -> Self {
        Self {
            logical_size: base_size,
            base_limit: base_size,
            low_water: base_size,
            ..Self::default()
        }
    }

    /// The source of every byte of `[start, end)`, in order.
    fn plan(&self, start: u64, end: u64) -> Vec<(u64, u64, Source)> {
        let mut pieces: Vec<(u64, u64, Source)> = (self.extents.ranges().iter())
            .filter(|(from, to)| *from < end && *to > start)
            .map(|(from, to)| ((*from).max(start), (*to).min(end), Source::Volatile))
            .collect();
        for (from, to) in self.extents.gaps(start, end) {
            let mut cursor = from;
            if let Some(sealed) = self.sealed
                && cursor < sealed.limit
            {
                let stop = to.min(sealed.limit);
                pieces.push((cursor, stop, Source::Checkpoint(sealed)));
                cursor = stop;
            }
            if cursor < to && cursor < self.base_limit {
                let stop = to.min(self.base_limit);
                pieces.push((cursor, stop, Source::Base));
                cursor = stop;
            }
            if cursor < to {
                pieces.push((cursor, to, Source::Zero));
            }
        }
        pieces.sort_unstable_by_key(|(from, _, _)| *from);
        pieces
    }

    /// Untouched base ranges a new checkpoint must fetch: below the base limit and covered by
    /// neither the overlay nor the previous checkpoint.
    pub(crate) fn missing_base(&self) -> Vec<(u64, u64)> {
        let limit = self.base_limit.min(self.logical_size);
        let start = self.sealed.map_or(0, |sealed| sealed.limit.min(limit));
        self.extents.gaps(start, limit)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    Volatile,
    Checkpoint(Sealed),
    Base,
    Zero,
}

/// Actual scratch bytes charged to this node and each group.
pub(crate) struct Budget {
    node_limit: u64,
    group_limit: u64,
    node_used: u64,
    groups: HashMap<GroupId, u64>,
}

impl Budget {
    pub(crate) fn new(node_limit: u64, group_limit: u64) -> Self {
        Self {
            node_limit,
            group_limit,
            node_used: 0,
            groups: HashMap::new(),
        }
    }

    pub(crate) fn reserve(&mut self, group_id: GroupId, bytes: u64) -> Result<(), VfsFailure> {
        let group = self.groups.get(&group_id).copied().unwrap_or(0);
        let node = self
            .node_used
            .checked_add(bytes)
            .filter(|node| *node <= self.node_limit);
        let group = group
            .checked_add(bytes)
            .filter(|group| *group <= self.group_limit);
        let (Some(node), Some(group)) = (node, group) else {
            return Err(VfsFailure::NoSpace);
        };
        self.node_used = node;
        self.groups.insert(group_id, group);
        Ok(())
    }

    /// Charges bytes that already exist on disk, even past the limits.
    pub(crate) fn force(&mut self, group_id: GroupId, bytes: u64) {
        self.node_used = self.node_used.saturating_add(bytes);
        let group = self.groups.entry(group_id).or_insert(0);
        *group = group.saturating_add(bytes);
    }

    pub(crate) fn release(&mut self, group_id: GroupId, bytes: u64) {
        self.node_used = self.node_used.saturating_sub(bytes);
        if let Some(group) = self.groups.get_mut(&group_id) {
            *group = group.saturating_sub(bytes);
            if *group == 0 {
                self.groups.remove(&group_id);
            }
        }
    }
}

/// One staged write as the operation asked for it.
pub(crate) struct StageWrite {
    pub(crate) owner_id: u64,
    pub(crate) offset: u64,
    pub(crate) data: Vec<u8>,
    pub(crate) append: bool,
}

pub(crate) fn read_range(file: &mut File, offset: u64, length: u64) -> std::io::Result<Vec<u8>> {
    let size = usize::try_from(length).map_err(std::io::Error::other)?;
    let mut data = vec![0; size];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut data)?;
    Ok(data)
}

pub(crate) fn write_range(path: &Path, offset: u64, data: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)
}

impl VfsHandle {
    pub(crate) async fn read_stage(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        offset: u64,
        length: u32,
    ) -> Answer {
        let (plan, logical_size, base_limit) = {
            let mut state = self.shared.state.lock();
            let instance = instance_mut(&mut state, scope, instance_id)?;
            let stage = &instance.stage;
            let end = offset
                .saturating_add(u64::from(length))
                .min(stage.logical_size);
            let plan = match offset < end {
                true => stage.plan(offset, end),
                false => Vec::new(),
            };
            (plan, stage.logical_size, stage.base_limit)
        };
        let volatile = self.path(&volatile_name(instance_id));
        let root = self.shared.root.clone();
        let slices = crate::blocking(move || read_slices(&plan, &volatile, &root)).await?;
        Ok(VfsEvent::StageRead {
            slices,
            logical_size,
            base_limit,
        })
    }

    /// Writes to the volatile file only; a sealed checkpoint never changes.
    pub(crate) async fn write_stage(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        write: StageWrite,
    ) -> Answer {
        let length = u32::try_from(write.data.len()).map_err(|_| VfsFailure::Overflow)?;
        let (offset, end, group_id, reservation) = {
            let mut state = self.shared.state.lock();
            if state.fences.closed(scope) {
                return Err(fenced());
            }
            let instance = instance_mut(&mut state, scope, instance_id)?;
            let offset = match write.append {
                true => instance.stage.logical_size,
                false => write.offset,
            };
            let end = offset.checked_add(u64::from(length));
            let end = end
                .filter(|end| *end <= OFFSET_LIMIT)
                .ok_or(VfsFailure::Overflow)?;
            if crate::locks::write_conflict(&instance.locks, write.owner_id, offset, end).is_some()
            {
                return Err(VfsFailure::LockConflict);
            }
            let fresh = (end - offset) - instance.stage.extents.covered(offset, end);
            let group_id = instance.group_id;
            let reservation = Reservation::take(&self.shared, &mut state, group_id, fresh)?;
            (offset, end, group_id, reservation)
        };
        let path = self.path(&volatile_name(instance_id));
        let data = write.data;
        crate::blocking(move || write_range(&path, offset, &data)).await?;
        let reserved = reservation.keep();
        let mut state = self.shared.state.lock();
        // Settles the charge to the bytes this write actually added, even if the instance left.
        state.budget.release(group_id, reserved);
        let instance = instance_mut(&mut state, scope, instance_id)?;
        let added = instance.stage.extents.insert(offset, end);
        instance.stage.logical_size = instance.stage.logical_size.max(end);
        instance.stage.edits += 1;
        instance.dirty = true;
        let change = self
            .shared
            .changes
            .touch(&scope.bucket, &instance.key, false);
        if let Ok(value) = change {
            instance.change = value;
        }
        let new_size = instance.stage.logical_size;
        state.budget.force(group_id, added);
        let change = change?;
        Ok(VfsEvent::StageWritten {
            offset,
            written: length,
            new_size,
            change,
        })
    }

    /// Resizes the stage: the base and checkpoint limits only shrink, so shrinking then
    /// extending reads zeros.
    pub(crate) async fn truncate_stage(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        owner_id: u64,
        new_size: u64,
    ) -> Answer {
        if new_size > OFFSET_LIMIT {
            return Err(VfsFailure::Overflow);
        }
        let (change, trimmed) = {
            let mut state = self.shared.state.lock();
            if state.fences.closed(scope) {
                return Err(fenced());
            }
            let instance = instance_mut(&mut state, scope, instance_id)?;
            let stage = &instance.stage;
            let (low, high) = match new_size < stage.logical_size {
                true => (new_size, stage.logical_size),
                false => (stage.logical_size, new_size),
            };
            if crate::locks::write_conflict(&instance.locks, owner_id, low, high).is_some() {
                return Err(VfsFailure::LockConflict);
            }
            let stage = &mut instance.stage;
            stage.base_limit = stage.base_limit.min(new_size);
            stage.low_water = stage.low_water.min(new_size);
            if let Some(sealed) = stage.sealed.as_mut() {
                sealed.limit = sealed.limit.min(new_size);
            }
            let trimmed = stage.extents.trim(new_size);
            stage.logical_size = new_size;
            stage.edits += 1;
            instance.dirty = true;
            let group_id = instance.group_id;
            let change = self
                .shared
                .changes
                .touch(&scope.bucket, &instance.key, false);
            if let Ok(value) = change {
                instance.change = value;
            }
            state.budget.release(group_id, trimmed);
            (change?, trimmed)
        };
        if trimmed > 0 {
            let path = self.path(&volatile_name(instance_id));
            crate::blocking(move || shorten(&path, new_size)).await?;
        }
        Ok(VfsEvent::StageWritten {
            offset: new_size,
            written: 0,
            new_size,
            change,
        })
    }
}

/// Shortens a volatile file so trimmed bytes leave the disk; a missing file is already short.
fn shorten(path: &Path, size: u64) -> std::io::Result<()> {
    let file = match OpenOptions::new().write(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > size {
        file.set_len(size)?;
    }
    Ok(())
}

fn read_slices(
    plan: &[(u64, u64, Source)],
    volatile: &Path,
    root: &Path,
) -> std::io::Result<Vec<StageSlice>> {
    let mut open: HashMap<PathBuf, File> = HashMap::new();
    let mut slices = Vec::with_capacity(plan.len());
    for (start, end, source) in plan {
        let (start, end) = (*start, *end);
        let path = match source {
            Source::Base => {
                let length = end - start;
                slices.push(StageSlice::Base {
                    offset: start,
                    length,
                });
                continue;
            }
            Source::Zero => {
                let length = end - start;
                slices.push(StageSlice::Zero {
                    offset: start,
                    length,
                });
                continue;
            }
            Source::Volatile => volatile.to_path_buf(),
            Source::Checkpoint(sealed) => {
                root.join(sealed_name(sealed.stage_id, sealed.generation))
            }
        };
        let file = match open.entry(path.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => entry.insert(File::open(&path)?),
        };
        let data = read_range(file, start, end - start)?;
        slices.push(StageSlice::Written {
            offset: start,
            data,
        });
    }
    Ok(slices)
}
