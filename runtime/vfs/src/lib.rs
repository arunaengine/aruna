//! Node-local file server adapter owning bucket namespace permits, per-instance file guards,
//! shared SMB/NFS open and lock state, sparse stages, immutable checkpoints and change counters.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod changes;
mod checkpoints;
mod fence;
mod instances;
mod locks;
mod permits;
mod stages;

pub use locks::open_owner;

use aruna_core::structs::storage::vfs::{BucketScope, HeadChangeObserver};
use aruna_core::types::GroupId;
use aruna_core::vfs_effects::{VfsEffect, VfsEvent, VfsFailure};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::warn;
use ulid::Ulid;

use crate::changes::Changes;
use crate::checkpoints::Checkpoint;
use crate::fence::Fences;
use crate::instances::Bucket;
use crate::permits::Permits;
use crate::stages::Budget;

/// Revocations a slow subscriber may lag behind before it misses one and must resync.
const REVOCATION_BACKLOG: usize = 256;

type Answer = Result<VfsEvent, VfsFailure>;

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("the stage folder path is empty")]
    EmptyStage,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Where staged bytes live and how many of them this node and each group may keep.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsConfig {
    pub stage_dir: PathBuf,
    pub node_stage_bytes: u64,
    pub group_stage_bytes: u64,
}

/// A revoked host, mount or access key whose sessions, opens and connections must end now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantRevocation {
    pub host_id: Option<Ulid>,
    pub mount_id: Option<Ulid>,
    pub access_key: Option<String>,
}

/// A namespace permit or file guard a caller holds until it releases it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VfsHold {
    Permit(Ulid),
    Guard(Ulid),
}

impl VfsHold {
    /// The hold an answer hands to its caller.
    pub fn granted(event: &VfsEvent) -> Option<Self> {
        match event {
            VfsEvent::PermitAcquired { permit_id } => Some(Self::Permit(*permit_id)),
            VfsEvent::GuardAcquired { guard_id } => Some(Self::Guard(*guard_id)),
            _ => None,
        }
    }

    /// The hold a call gives back.
    pub fn released(effect: &VfsEffect) -> Option<Self> {
        match effect {
            VfsEffect::ReleasePermit { permit_id } => Some(Self::Permit(*permit_id)),
            VfsEffect::ReleaseGuard { guard_id } => Some(Self::Guard(*guard_id)),
            _ => None,
        }
    }
}

/// The file server state of this node; clones share it.
#[derive(Clone)]
pub struct VfsHandle {
    shared: Arc<Shared>,
}

pub(crate) struct Shared {
    root: PathBuf,
    state: Mutex<State>,
    changes: Changes,
    revocations: broadcast::Sender<GrantRevocation>,
}

/// Everything behind the one state lock; it is never held across an await or file I/O.
pub(crate) struct State {
    permits: Permits,
    buckets: HashMap<BucketScope, Bucket>,
    budget: Budget,
    checkpoints: HashMap<(Ulid, u64), Checkpoint>,
    fences: Fences,
    /// Stage writes per bucket between their fence check and their last file step.
    in_flight_writes: HashMap<BucketScope, usize>,
    next_instance: u64,
}

impl std::fmt::Debug for VfsHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VfsHandle")
            .field("root", &self.shared.root)
            .finish_non_exhaustive()
    }
}

impl VfsHandle {
    /// Opens the stage folder, creating it if needed, and drops volatile work of an earlier boot.
    pub fn open(config: VfsConfig) -> Result<Self, VfsError> {
        if config.stage_dir.as_os_str().is_empty() {
            return Err(VfsError::EmptyStage);
        }
        if !config.stage_dir.exists() {
            std::fs::create_dir_all(&config.stage_dir)?;
            let parent = config.stage_dir.parent();
            let parent = parent.filter(|parent| !parent.as_os_str().is_empty());
            sync_folder(parent.unwrap_or(Path::new(".")))?;
        }
        for entry in std::fs::read_dir(&config.stage_dir)? {
            let entry = entry?;
            if stages::is_volatile(&entry.file_name().to_string_lossy()) {
                std::fs::remove_file(entry.path())?;
            }
        }
        let (revocations, _) = broadcast::channel(REVOCATION_BACKLOG);
        let state = State {
            permits: Permits::default(),
            buckets: HashMap::new(),
            budget: Budget::new(config.node_stage_bytes, config.group_stage_bytes),
            checkpoints: HashMap::new(),
            fences: Fences::default(),
            in_flight_writes: HashMap::new(),
            next_instance: 1,
        };
        Ok(Self {
            shared: Arc::new(Shared {
                root: config.stage_dir,
                state: Mutex::new(state),
                changes: Changes::new(),
                revocations,
            }),
        })
    }

    /// Receives every later revocation so the servers drop matching sessions at once.
    pub fn subscribe_revocations(&self) -> broadcast::Receiver<GrantRevocation> {
        self.shared.revocations.subscribe()
    }

    /// Gives back a permit or guard whose owner went away without releasing it.
    pub fn release_hold(&self, hold: &VfsHold) {
        let released: Vec<_> = {
            let mut state = self.shared.state.lock();
            match hold {
                VfsHold::Permit(permit_id) => {
                    state.permits.take_permit(permit_id).into_iter().collect()
                }
                VfsHold::Guard(guard_id) => state.permits.take_guard(guard_id).unwrap_or_default(),
            }
        };
        drop(released);
    }

    /// Performs one call and answers it; failures come back as `VfsEvent::Failed`.
    pub async fn execute(&self, effect: VfsEffect) -> VfsEvent {
        let answer = match effect {
            VfsEffect::AcquirePermit { scope } => self.acquire_permit(scope).await,
            VfsEffect::ReleasePermit { permit_id } => self.release_permit(permit_id),
            VfsEffect::AcquireGuards { scope, instances } => {
                self.acquire_guards(scope, instances).await
            }
            VfsEffect::ReleaseGuard { guard_id } => self.release_guard(guard_id),
            VfsEffect::BeginAttempt {
                scope,
                attempt_id,
                instances,
            } => self.begin_attempt(scope, attempt_id, instances),
            VfsEffect::FinishAttempt { attempt_id } | VfsEffect::ReleaseAttempt { attempt_id } => {
                self.finish_attempt(attempt_id)
            }
            VfsEffect::WaitAttempt { attempt_id } => self.wait_attempt(attempt_id).await,
            VfsEffect::ActiveAttempts { scope } => self.active_attempts(&scope),
            VfsEffect::OpenInstance(request) => self.open_instance(request),
            VfsEffect::CloseOpen {
                scope,
                open_id,
                detach_unstable,
            } => self.close_open(&scope, open_id, detach_unstable).await,
            VfsEffect::LookupInstance { scope, key } => self.lookup_instance(&scope, &key),
            VfsEffect::LookupHandle { scope, handle_id } => self.lookup_handle(&scope, handle_id),
            VfsEffect::LookupById { scope, instance_id } => self.lookup_by_id(&scope, instance_id),
            VfsEffect::ListStaged {
                scope,
                prefix,
                after,
                limit,
            } => self.list_staged(&scope, &prefix, after.as_deref(), limit),
            VfsEffect::RebindInstance(request) => self.rebind_instance(request).await,
            VfsEffect::DetachInstance {
                scope,
                handle_id,
                instance_id,
                reason,
            } => {
                self.detach_instance(&scope, handle_id, instance_id, reason)
                    .await
            }
            VfsEffect::RetainInstance {
                scope,
                instance_id,
                stage_id,
                generation,
                reason,
            } => {
                self.retain_instance(&scope, instance_id, stage_id, generation, reason)
                    .await
            }
            VfsEffect::UpdateWitnesses { scope, updates } => self.update_witnesses(&scope, updates),
            VfsEffect::SetDisposition {
                scope,
                instance_id,
                open_id,
                pending,
                owner,
            } => self.set_disposition(&scope, instance_id, open_id, pending, owner),
            VfsEffect::ClearDisposition { scope, instance_id } => {
                self.clear_disposition(&scope, instance_id).await
            }
            VfsEffect::UpdateShare {
                scope,
                open_id,
                share,
            } => self.update_share(&scope, open_id, share),
            VfsEffect::LockRanges {
                scope,
                instance_id,
                locks,
                atomic,
            } => self.lock_ranges(&scope, instance_id, locks, atomic),
            VfsEffect::UnlockRanges {
                scope,
                instance_id,
                locks,
            } => self.unlock_ranges(&scope, instance_id, locks),
            VfsEffect::TestLock {
                scope,
                instance_id,
                lock,
            } => self.test_lock(&scope, instance_id, lock),
            VfsEffect::ReadStage {
                scope,
                instance_id,
                offset,
                length,
                lock_check,
            } => {
                self.read_stage(&scope, instance_id, offset, length, lock_check)
                    .await
            }
            VfsEffect::WriteStage {
                scope,
                instance_id,
                owner_id,
                offset,
                data,
                append,
                group_id: _,
            } => {
                let write = stages::StageWrite {
                    owner_id,
                    offset,
                    data,
                    append,
                };
                self.write_stage(&scope, instance_id, write).await
            }
            VfsEffect::TruncateStage {
                scope,
                instance_id,
                owner_id,
                new_size,
                group_id: _,
            } => {
                self.truncate_stage(&scope, instance_id, owner_id, new_size)
                    .await
            }
            VfsEffect::BeginCheckpoint {
                scope,
                instance_id,
                stage_id,
                generation,
                group_id: _,
            } => {
                self.begin_checkpoint(&scope, instance_id, stage_id, generation)
                    .await
            }
            VfsEffect::FillCheckpoint {
                stage_id,
                generation,
                offset,
                data,
            } => {
                self.fill_checkpoint(stage_id, generation, offset, data)
                    .await
            }
            VfsEffect::SealCheckpoint {
                scope,
                instance_id,
                stage_id,
                generation,
            } => {
                self.seal_checkpoint(&scope, instance_id, stage_id, generation)
                    .await
            }
            VfsEffect::PromoteCheckpoint {
                scope,
                instance_id,
                stage_id,
                generation,
                old_generation,
            } => {
                let promotion = checkpoints::Promotion {
                    stage_id,
                    generation,
                    old_generation,
                };
                self.promote_checkpoint(&scope, instance_id, promotion)
                    .await
            }
            VfsEffect::AbortCheckpoint {
                scope,
                instance_id,
                stage_id,
                generation,
            } => {
                self.abort_checkpoint(&scope, instance_id, (stage_id, generation))
                    .await
            }
            VfsEffect::ReleaseDraft {
                scope,
                instance_id,
                stage_id,
                generation,
            } => {
                self.release_draft(&scope, instance_id, stage_id, generation)
                    .await
            }
            VfsEffect::ReadCheckpoint {
                stage_id,
                generation,
                offset,
                length,
            } => {
                self.read_checkpoint(stage_id, generation, offset, length)
                    .await
            }
            VfsEffect::RemoveCheckpoint {
                stage_id,
                generation,
            } => self.remove_checkpoint(stage_id, generation).await,
            VfsEffect::NextChange { scope, key } => self.next_change(&scope, key.as_deref()),
            VfsEffect::ReadChange { scope, keys } => self.read_change(&scope, &keys),
            VfsEffect::WriteVerifier => self.shared.changes.verifier(),
            VfsEffect::AdvanceVerifier => self.shared.changes.advance_verifier(),
            VfsEffect::FenceVault { scope } => self.fence_vault(scope).await,
            VfsEffect::ReleaseFence {
                fence_id,
                keep_closed,
            } => self.release_fence(fence_id, keep_closed),
            VfsEffect::ScanStages => self.scan_stages().await,
            VfsEffect::CheckStage {
                stage_id,
                generation,
                expected_size,
            } => self.check_stage(stage_id, generation, expected_size).await,
            VfsEffect::ConfigureBoot {
                boot_generation,
                verifier,
            } => self.shared.changes.configure(boot_generation, verifier),
            VfsEffect::RestoreStage(request) => self.restore_stage(request),
            VfsEffect::RevokeGrant {
                host_id,
                mount_id,
                access_key,
            } => {
                let revocation = GrantRevocation {
                    host_id,
                    mount_id,
                    access_key,
                };
                // Sending fails only when no server listens, which leaves nothing to drop.
                let _ = self.shared.revocations.send(revocation);
                Ok(VfsEvent::Done)
            }
        };
        answer.unwrap_or_else(VfsEvent::Failed)
    }

    pub(crate) fn path(&self, name: &str) -> PathBuf {
        self.shared.root.join(name)
    }
}

impl HeadChangeObserver for VfsHandle {
    fn on_head_changed(&self, bucket: &str, key: &str) {
        self.shared.changes.observe(bucket, key);
    }
}

/// Scratch bytes taken from the budget for a write in flight; dropping it gives them back.
pub(crate) struct Reservation {
    shared: Arc<Shared>,
    group_id: GroupId,
    bytes: u64,
}

impl Reservation {
    pub(crate) fn take(
        shared: &Arc<Shared>,
        state: &mut State,
        group_id: GroupId,
        bytes: u64,
    ) -> Result<Self, VfsFailure> {
        state.budget.reserve(group_id, bytes)?;
        Ok(Self {
            shared: Arc::clone(shared),
            group_id,
            bytes,
        })
    }

    /// Keeps the bytes charged and returns how many; call it while the state lock is free.
    pub(crate) fn keep(mut self) -> u64 {
        std::mem::take(&mut self.bytes)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            let mut state = self.shared.state.lock();
            state.budget.release(self.group_id, self.bytes);
        }
    }
}

/// Runs file I/O off the async workers.
pub(crate) async fn blocking<T, F>(work: F) -> Result<T, VfsFailure>
where
    T: Send + 'static,
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(io_failure(&error)),
        Err(error) => Err(VfsFailure::Io(error.to_string())),
    }
}

pub(crate) fn io_failure(error: &std::io::Error) -> VfsFailure {
    match error.kind() {
        ErrorKind::StorageFull | ErrorKind::QuotaExceeded => VfsFailure::NoSpace,
        ErrorKind::NotFound => VfsFailure::NotFound,
        _ => VfsFailure::Io(error.to_string()),
    }
}

/// Removes a file that may already be gone.
pub(crate) fn remove_quietly(path: &Path) {
    if let Err(error) = std::fs::remove_file(path)
        && error.kind() != ErrorKind::NotFound
    {
        warn!(path = %path.display(), %error, "Could not remove a stage file");
    }
}

#[cfg(unix)]
pub(crate) fn sync_folder(folder: &Path) -> std::io::Result<()> {
    let handle = std::fs::File::open(folder)?;
    rustix::fs::fsync(&handle).map_err(std::io::Error::from)
}

/// Windows flushes only handles opened for writing, which folders never are.
#[cfg(not(unix))]
pub(crate) fn sync_folder(_: &Path) -> std::io::Result<()> {
    Ok(())
}
