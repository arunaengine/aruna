//! Byte-range locks shared by SMB and NFS opens of one instance: conflict rules, atomic
//! multi-range requests and protocol-specific unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::BucketScope;
use aruna_core::vfs_effects::{ByteLock, VfsEvent, VfsFailure, VfsProtocol};
use ulid::Ulid;

use crate::{Answer, VfsHandle};

/// The lock owner of an open whose locks end with it; SMB servers lock under this owner.
pub fn open_owner(open_id: &Ulid) -> u64 {
    let value = u128::from(*open_id);
    (value as u64) ^ ((value >> 64) as u64)
}

/// The bytes `[start, end)` a lock covers; `start == end` is an SMB zero-length lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Span {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl Span {
    /// NFS reads length zero or all ones as up to the end of the file.
    pub(crate) fn of(lock: &ByteLock) -> Result<Self, VfsFailure> {
        let open_end = lock.protocol == VfsProtocol::Nfs && matches!(lock.length, 0 | u64::MAX);
        let end = match open_end {
            true => u64::MAX,
            false => lock
                .offset
                .checked_add(lock.length)
                .ok_or(VfsFailure::Overflow)?,
        };
        Ok(Self {
            start: lock.offset,
            end,
        })
    }

    fn overlaps(self, other: Self) -> bool {
        match (self.start == self.end, other.start == other.end) {
            (true, true) => false,
            (true, false) => other.start <= self.start && self.start < other.end,
            (false, true) => self.start <= other.start && other.start < self.end,
            (false, false) => self.start < other.end && other.start < self.end,
        }
    }
}

fn same_owner(left: &ByteLock, right: &ByteLock) -> bool {
    left.owner == right.owner && left.protocol == right.protocol
}

/// The first held lock of another owner that blocks `wanted`.
pub(crate) fn conflict(
    held: &[ByteLock],
    wanted: &ByteLock,
) -> Result<Option<ByteLock>, VfsFailure> {
    let span = Span::of(wanted)?;
    for lock in held {
        if same_owner(lock, wanted) || !(lock.exclusive || wanted.exclusive) {
            continue;
        }
        if Span::of(lock)?.overlaps(span) {
            return Ok(Some(*lock));
        }
    }
    Ok(None)
}

/// The first lock of another owner over `[start, end)`; any lock of another owner blocks a write.
pub(crate) fn write_conflict(
    held: &[ByteLock],
    owner: u64,
    start: u64,
    end: u64,
) -> Option<ByteLock> {
    first_overlap(held.iter().filter(|lock| lock.owner != owner), start, end)
}

/// The first exclusive lock of another owner over `[start, end)`; shared locks admit reads.
pub(crate) fn read_conflict(
    held: &[ByteLock],
    owner: u64,
    start: u64,
    end: u64,
) -> Option<ByteLock> {
    let exclusive = held.iter().filter(|lock| lock.exclusive);
    first_overlap(exclusive.filter(|lock| lock.owner != owner), start, end)
}

fn first_overlap<'a>(
    mut locks: impl Iterator<Item = &'a ByteLock>,
    start: u64,
    end: u64,
) -> Option<ByteLock> {
    let span = Span { start, end };
    if start == end {
        return None;
    }
    locks
        .find(|lock| Span::of(lock).is_ok_and(|held| held.overlaps(span)))
        .copied()
}

/// Removes `released` from `held`: SMB needs the exact range, NFS may release any subrange.
fn unlock(held: &mut Vec<ByteLock>, released: &ByteLock) -> Result<(), VfsFailure> {
    if released.protocol == VfsProtocol::Smb {
        let index = held.iter().position(|lock| lock == released);
        held.remove(index.ok_or(VfsFailure::NotFound)?);
        return Ok(());
    }
    let cut = Span::of(released)?;
    let mut kept = Vec::with_capacity(held.len());
    for lock in held.drain(..) {
        let span = Span::of(&lock)?;
        if !same_owner(&lock, released) || !span.overlaps(cut) {
            kept.push(lock);
            continue;
        }
        if span.start < cut.start {
            kept.push(piece(&lock, span.start, cut.start));
        }
        if cut.end < span.end {
            kept.push(piece(&lock, cut.end, span.end));
        }
    }
    *held = kept;
    Ok(())
}

fn piece(lock: &ByteLock, start: u64, end: u64) -> ByteLock {
    let length = match end {
        u64::MAX => u64::MAX,
        end => end - start,
    };
    ByteLock {
        offset: start,
        length,
        ..*lock
    }
}

impl VfsHandle {
    /// Grants `locks` in order; `atomic` checks them all first so a conflict grants none.
    pub(crate) fn lock_ranges(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        locks: Vec<ByteLock>,
        atomic: bool,
    ) -> Answer {
        let mut state = self.shared.state.lock();
        let instance = crate::instances::instance_mut(&mut state, scope, instance_id)?;
        if atomic {
            let mut trial = instance.locks.clone();
            for lock in &locks {
                if let Some(holder) = conflict(&trial, lock)? {
                    return Ok(VfsEvent::LockConflict { holder });
                }
                trial.push(*lock);
            }
            instance.locks = trial;
            return Ok(VfsEvent::Done);
        }
        for lock in locks {
            if let Some(holder) = conflict(&instance.locks, &lock)? {
                return Ok(VfsEvent::LockConflict { holder });
            }
            instance.locks.push(lock);
        }
        Ok(VfsEvent::Done)
    }

    pub(crate) fn unlock_ranges(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        locks: Vec<ByteLock>,
    ) -> Answer {
        let mut state = self.shared.state.lock();
        let instance = crate::instances::instance_mut(&mut state, scope, instance_id)?;
        let mut held = instance.locks.clone();
        for lock in &locks {
            unlock(&mut held, lock)?;
        }
        instance.locks = held;
        Ok(VfsEvent::Done)
    }

    pub(crate) fn test_lock(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        lock: ByteLock,
    ) -> Answer {
        let mut state = self.shared.state.lock();
        let instance = crate::instances::instance_mut(&mut state, scope, instance_id)?;
        Ok(match conflict(&instance.locks, &lock)? {
            Some(holder) => VfsEvent::LockConflict { holder },
            None => VfsEvent::LockFree,
        })
    }
}
