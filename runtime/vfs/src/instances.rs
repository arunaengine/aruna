//! Shared open instances of each bucket: opens of both protocols, share modes, delete-pending
//! links, directory witnesses, detach and rebind after committed mutations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::{
    BucketScope, DetachReason, FileIdentity, HandleIdentity, HandleKind, StageOwner,
};
use aruna_core::types::GroupId;
use aruna_core::vfs_effects::{
    ByteLock, CloseSummary, InstanceSummary, OpenRequest, RebindRequest, ShareMode, StagedEntry,
    VfsEvent, VfsFailure, VfsProtocol, WitnessUpdate,
};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use ulid::Ulid;

use crate::locks::open_owner;
use crate::stages::{self, Stage};
use crate::{Answer, State, VfsHandle};

#[derive(Default)]
pub(crate) struct Bucket {
    pub(crate) instances: HashMap<u64, Instance>,
    pub(crate) by_key: BTreeMap<String, u64>,
    pub(crate) by_handle: HashMap<u64, u64>,
    pub(crate) by_open: HashMap<Ulid, u64>,
}

pub(crate) struct Open {
    protocol: VfsProtocol,
    share: ShareMode,
    delete_on_close: bool,
    disposition: bool,
    owner: Option<StageOwner>,
}

pub(crate) struct Instance {
    pub(crate) id: u64,
    pub(crate) key: String,
    pub(crate) kind: HandleKind,
    pub(crate) handle_id: Option<u64>,
    pub(crate) identity: HandleIdentity,
    pub(crate) opens: HashMap<Ulid, Open>,
    pub(crate) locks: Vec<ByteLock>,
    pub(crate) delete_pending: bool,
    pub(crate) disposition_owner: Option<StageOwner>,
    pub(crate) detached: Option<DetachReason>,
    pub(crate) group_id: GroupId,
    pub(crate) dirty: bool,
    /// Rebuilt from a durable stage row at cold start.
    pub(crate) restored: bool,
    pub(crate) change: u64,
    pub(crate) stage: Stage,
    pub(crate) stage_id: Option<Ulid>,
    pub(crate) generation: u64,
    pub(crate) base_version: Option<Ulid>,
    /// The one checkpoint generation this instance began and has not promoted or aborted.
    pub(crate) draft: Option<(Ulid, u64)>,
    /// Its checkpoint moved to retained recovery; the last close gives up the reference.
    pub(crate) retained: bool,
}

impl Instance {
    pub(crate) fn new(id: u64, key: String, identity: HandleIdentity, group_id: GroupId) -> Self {
        Self {
            id,
            key,
            kind: identity.kind(),
            handle_id: None,
            identity,
            opens: HashMap::new(),
            locks: Vec::new(),
            delete_pending: false,
            disposition_owner: None,
            detached: None,
            group_id,
            dirty: false,
            restored: false,
            change: 0,
            stage: Stage::default(),
            stage_id: None,
            generation: 0,
            base_version: None,
            draft: None,
            retained: false,
        }
    }

    fn writers(&self) -> usize {
        self.opens
            .values()
            .filter(|open| open.share.write_access)
            .count()
    }

    fn allows_delete(&self) -> bool {
        self.opens.values().all(|open| open.share.share_delete)
    }

    /// The owner of the earliest writer open that names one.
    fn successor(&self) -> Option<StageOwner> {
        let writers = (self.opens.iter()).filter(|(_, open)| open.share.write_access);
        let owned = writers.filter_map(|(open_id, open)| Some((open_id, open.owner.as_ref()?)));
        let earliest = owned.min_by_key(|(open_id, _)| **open_id);
        earliest.map(|(_, owner)| owner.clone())
    }

    /// Whether this instance holds plaintext that is not published yet.
    pub(crate) fn unpublished(&self) -> bool {
        let new_name = matches!(
            self.identity,
            HandleIdentity::File(FileIdentity::Unpublished { .. })
        );
        self.dirty || self.restored || self.stage_id.is_some() || new_name
    }

    /// Whether nothing keeps this instance in memory any more.
    fn idle(&self) -> bool {
        let unused = self.opens.is_empty() && self.stage_id.is_none();
        let detached = self.detached.is_some();
        unused && (detached || (!self.dirty && !self.delete_pending))
    }

    pub(crate) fn summary(&self) -> InstanceSummary {
        InstanceSummary {
            instance_id: self.id,
            key: self.key.clone(),
            kind: self.kind,
            handle_id: self.handle_id,
            identity: self.identity.clone(),
            logical_size: self.stage.logical_size,
            base_limit: self.stage.base_limit,
            dirty: self.dirty,
            delete_pending: self.delete_pending,
            disposition_owner: self.disposition_owner.clone(),
            open_count: self.opens.len(),
            writer_count: self.writers(),
            detached: self.detached,
            stage_id: self.stage_id,
            generation: self.generation,
            change: self.change,
            base_version: self.base_version,
            edit_generation: self.stage.edits,
            allows_delete: self.allows_delete(),
            sealed_edit: self.stage.sealed.map(|sealed| sealed.edits),
            draft: self.draft,
        }
    }

    fn close_summary(&self) -> CloseSummary {
        CloseSummary {
            instance_id: self.id,
            key: self.key.clone(),
            kind: self.kind,
            remaining_opens: self.opens.len(),
            remaining_writers: self.writers(),
            delete_pending: self.delete_pending,
            dirty: self.dirty,
            detached: self.detached,
            disposition_owner: self.disposition_owner.clone(),
            stage_id: self.stage_id,
            generation: self.generation,
            logical_size: self.stage.logical_size,
            handle_id: self.handle_id,
            identity: self.identity.clone(),
            base_version: self.base_version,
            edit_generation: self.stage.edits,
            successor_owner: self.successor(),
        }
    }
}

impl Bucket {
    /// Unmaps `instance_id` from its name, leaving its handle and opens in place.
    fn unname(&mut self, instance_id: u64) {
        let Some(instance) = self.instances.get(&instance_id) else {
            return;
        };
        if self.by_key.get(&instance.key) == Some(&instance_id) {
            self.by_key.remove(&instance.key);
        }
    }

    pub(crate) fn detach(&mut self, instance_id: u64, reason: DetachReason) {
        self.unname(instance_id);
        if let Some(instance) = self.instances.get_mut(&instance_id) {
            instance.detached = Some(reason);
        }
    }

    /// Whether `key` or any folder above it is marked delete-pending.
    fn blocked(&self, key: &str) -> bool {
        let mut names = vec![key];
        for (index, _) in key.match_indices('/') {
            names.push(&key[..index]);
            names.push(&key[..=index]);
        }
        names.into_iter().any(|name| {
            let instance = self.by_key.get(name).and_then(|id| self.instances.get(id));
            instance.is_some_and(|instance| instance.delete_pending)
        })
    }
}

/// Whether each open admits the access the other asks for.
fn shares_with(existing: &ShareMode, wanted: &ShareMode) -> bool {
    let admits = |share: &ShareMode, access: &ShareMode| {
        (!access.read_access || share.share_read)
            && (!access.write_access || share.share_write)
            && (!access.delete_access || share.share_delete)
    };
    admits(existing, wanted) && admits(wanted, existing)
}

/// Whether a request for `identity` joins `instance` rather than replacing it.
fn joins(instance: &Instance, identity: &HandleIdentity) -> bool {
    match (&instance.identity, identity) {
        (HandleIdentity::Directory(_), HandleIdentity::Directory(_)) => true,
        (held, wanted) => held == wanted,
    }
}

pub(crate) fn instance_mut<'a>(
    state: &'a mut State,
    scope: &BucketScope,
    instance_id: u64,
) -> Result<&'a mut Instance, VfsFailure> {
    let bucket = state.buckets.get_mut(scope).ok_or(VfsFailure::NotFound)?;
    bucket
        .instances
        .get_mut(&instance_id)
        .ok_or(VfsFailure::NotFound)
}

/// Allocates the next instance id of this node.
pub(crate) fn next_instance(state: &mut State) -> Result<u64, VfsFailure> {
    let id = state.next_instance;
    state.next_instance = id.checked_add(1).ok_or(VfsFailure::CounterExhausted)?;
    Ok(id)
}

/// Removes an idle instance and returns its volatile file name, which the caller unlinks.
fn reclaim(state: &mut State, scope: &BucketScope, instance_id: u64) -> Option<String> {
    let bucket = state.buckets.get_mut(scope)?;
    bucket.unname(instance_id);
    let instance = bucket.instances.remove(&instance_id)?;
    if let Some(handle_id) = instance.handle_id
        && bucket.by_handle.get(&handle_id) == Some(&instance_id)
    {
        bucket.by_handle.remove(&handle_id);
    }
    if bucket.instances.is_empty() {
        state.buckets.remove(scope);
    }
    let written = instance.stage.extents.total();
    state.budget.release(instance.group_id, written);
    state.permits.forget(instance_id);
    Some(stages::volatile_name(instance_id))
}

/// Drops the stage reference of a writerless detached instance and reclaims it once idle; the
/// caller unlinks the returned volatile file.
pub(crate) fn release_stage(
    state: &mut State,
    scope: &BucketScope,
    instance_id: u64,
) -> Option<String> {
    let instance = instance_mut(state, scope, instance_id).ok()?;
    instance.stage_id = None;
    instance.generation = 0;
    instance.stage.sealed = None;
    match instance.idle() {
        true => reclaim(state, scope, instance_id),
        false => None,
    }
}

impl VfsHandle {
    pub(crate) fn open_instance(&self, request: OpenRequest) -> Answer {
        let changes = &self.shared.changes;
        let mut state = self.shared.state.lock();
        let share = request.share;
        let writes = share.write_access || share.delete_access || request.delete_on_close;
        let creates = matches!(
            request.identity,
            HandleIdentity::File(FileIdentity::Unpublished { .. })
        );
        if (writes || creates) && state.fences.closed(&request.scope) {
            return Err(fenced());
        }
        let candidate = state.next_instance;
        let following = candidate
            .checked_add(1)
            .ok_or(VfsFailure::CounterExhausted)?;
        let bucket = state.buckets.entry(request.scope.clone()).or_default();
        if bucket.blocked(&request.key) {
            return Err(VfsFailure::DeletePending);
        }
        let existing = bucket.by_key.get(&request.key).copied();
        let joined = existing.filter(|id| {
            let instance = bucket.instances.get(id);
            instance.is_some_and(|instance| joins(instance, &request.identity))
        });
        if let Some(id) = joined {
            let instance = bucket.instances.get(&id).ok_or(VfsFailure::NotFound)?;
            if !instance
                .opens
                .values()
                .all(|open| shares_with(&open.share, &share))
            {
                return Err(VfsFailure::ShareViolation);
            }
        }
        let instance_id = joined.unwrap_or(candidate);
        if joined.is_none() {
            let change = changes.read(&request.scope.bucket, &request.key)?;
            if let Some(replaced) = existing {
                bucket.detach(replaced, DetachReason::LostLife);
            }
            let identity = request.identity.clone();
            let mut instance =
                Instance::new(candidate, request.key.clone(), identity, request.group_id);
            instance.stage = Stage::over(request.base_size);
            instance.change = change;
            instance.base_version = request.base_version;
            bucket.instances.insert(candidate, instance);
            bucket.by_key.insert(request.key.clone(), candidate);
        }
        let instance = bucket
            .instances
            .get_mut(&instance_id)
            .ok_or(VfsFailure::NotFound)?;
        if let Some(handle_id) = request.handle_id {
            instance.handle_id.get_or_insert(handle_id);
            bucket.by_handle.entry(handle_id).or_insert(instance_id);
        }
        let open = Open {
            protocol: request.protocol,
            share,
            delete_on_close: request.delete_on_close,
            disposition: false,
            owner: request.owner,
        };
        instance.opens.insert(request.open_id, open);
        bucket.by_open.insert(request.open_id, instance_id);
        let size = instance.stage.logical_size;
        let change = instance.change;
        if joined.is_none() {
            state.next_instance = following;
        }
        Ok(VfsEvent::Opened {
            instance_id,
            created: joined.is_none(),
            size,
            change,
        })
    }

    /// Removes one open; with `detach_unstable` the last writer of a dirty instance with no
    /// durable checkpoint also takes it off its name under the same lock.
    pub(crate) async fn close_open(
        &self,
        scope: &BucketScope,
        open_id: Ulid,
        detach_unstable: bool,
    ) -> Answer {
        let (summary, unused) = {
            let mut state = self.shared.state.lock();
            let bucket = state.buckets.get_mut(scope).ok_or(VfsFailure::NotFound)?;
            let instance_id = bucket
                .by_open
                .remove(&open_id)
                .ok_or(VfsFailure::NotFound)?;
            let instance = bucket
                .instances
                .get_mut(&instance_id)
                .ok_or(VfsFailure::NotFound)?;
            let open = instance
                .opens
                .remove(&open_id)
                .ok_or(VfsFailure::NotFound)?;
            let owner = open_owner(&open_id);
            instance
                .locks
                .retain(|lock| lock.owner != owner || lock.protocol != open.protocol);
            if instance.opens.is_empty() {
                instance.locks.clear();
            }
            if open.delete_on_close {
                instance.delete_pending = true;
                if instance.disposition_owner.is_none() {
                    instance.disposition_owner = open.owner;
                }
            }
            if instance.retained && instance.opens.is_empty() {
                instance.stage_id = None;
                instance.generation = 0;
                instance.stage.sealed = None;
            }
            if detach_unstable
                && instance.writers() == 0
                && instance.dirty
                && instance.stage_id.is_none()
                && instance.detached.is_none()
            {
                let key = instance.key.clone();
                bucket.detach(instance_id, DetachReason::LostLife);
                if let Some(instance) = bucket.instances.get_mut(&instance_id)
                    && let Ok(change) = self.shared.changes.touch(&scope.bucket, &key, true)
                {
                    instance.change = change;
                }
            }
            let instance = (bucket.instances.get(&instance_id)).ok_or(VfsFailure::NotFound)?;
            let summary = instance.close_summary();
            let unused = match instance.idle() {
                true => reclaim(&mut state, scope, instance_id),
                false => None,
            };
            (summary, unused)
        };
        if let Some(name) = unused {
            let path = self.path(&name);
            crate::blocking(move || {
                crate::remove_quietly(&path);
                Ok(())
            })
            .await?;
        }
        Ok(VfsEvent::Closed(summary))
    }

    pub(crate) fn lookup_instance(&self, scope: &BucketScope, key: &str) -> Answer {
        let state = self.shared.state.lock();
        let bucket = state.buckets.get(scope);
        let instance = bucket.and_then(|bucket| {
            let id = bucket.by_key.get(key)?;
            bucket.instances.get(id)
        });
        Ok(VfsEvent::Instance(instance.map(Instance::summary)))
    }

    pub(crate) fn lookup_handle(&self, scope: &BucketScope, handle_id: u64) -> Answer {
        let state = self.shared.state.lock();
        let bucket = state.buckets.get(scope);
        let instance = bucket.and_then(|bucket| {
            let id = bucket.by_handle.get(&handle_id)?;
            bucket.instances.get(id)
        });
        Ok(VfsEvent::Instance(instance.map(Instance::summary)))
    }

    pub(crate) fn lookup_by_id(&self, scope: &BucketScope, instance_id: u64) -> Answer {
        let state = self.shared.state.lock();
        let bucket = state.buckets.get(scope);
        let instance = bucket.and_then(|bucket| bucket.instances.get(&instance_id));
        Ok(VfsEvent::Instance(instance.map(Instance::summary)))
    }

    /// Staged and unpublished names under `prefix` in key order, strictly after `after` and at
    /// most `limit` of them, sampled after the observer sequence.
    pub(crate) fn list_staged(
        &self,
        scope: &BucketScope,
        prefix: &str,
        after: Option<&str>,
        limit: Option<usize>,
    ) -> Answer {
        let sequence = self.shared.changes.sequence();
        let state = self.shared.state.lock();
        let mut entries: Vec<StagedEntry> = Vec::new();
        let mut truncated = false;
        let Some(bucket) = state.buckets.get(scope) else {
            return Ok(VfsEvent::StagedList {
                entries,
                sequence,
                truncated,
            });
        };
        let start = match after {
            Some(after) if after >= prefix => Bound::Excluded(after),
            _ => Bound::Included(prefix),
        };
        let limit = limit.unwrap_or(usize::MAX);
        let keys = bucket.by_key.range::<str, _>((start, Bound::Unbounded));
        for (key, id) in keys.take_while(|(key, _)| key.starts_with(prefix)) {
            let Some(instance) = bucket.instances.get(id) else {
                continue;
            };
            if !instance.unpublished() {
                continue;
            }
            if entries.len() == limit {
                truncated = true;
                break;
            }
            let unpublished_token = match &instance.identity {
                HandleIdentity::File(FileIdentity::Unpublished { token, .. }) => Some(*token),
                _ => None,
            };
            entries.push(StagedEntry {
                key: key.clone(),
                instance_id: instance.id,
                handle_id: instance.handle_id,
                kind: instance.kind,
                size: instance.stage.logical_size,
                dirty: instance.dirty,
                delete_pending: instance.delete_pending,
                unpublished_token,
                change: instance.change,
            });
        }
        Ok(VfsEvent::StagedList {
            entries,
            sequence,
            truncated,
        })
    }

    /// Moves a surviving instance to its committed name, handle and identity.
    pub(crate) async fn rebind_instance(&self, request: RebindRequest) -> Answer {
        let bucket_name = request.scope.bucket.clone();
        let (summary, cleared) = {
            let mut state = self.shared.state.lock();
            let sealed = match (request.stage_id, request.generation) {
                (Some(stage_id), Some(generation)) => state
                    .checkpoints
                    .get(&(stage_id, generation))
                    .filter(|checkpoint| checkpoint.sealed)
                    .map(|checkpoint| (checkpoint.size, checkpoint.edits)),
                _ => None,
            };
            let bucket = state
                .buckets
                .get_mut(&request.scope)
                .ok_or(VfsFailure::NotFound)?;
            if !bucket.instances.contains_key(&request.instance_id) {
                return Err(VfsFailure::NotFound);
            }
            bucket.unname(request.instance_id);
            if let Some(displaced) = bucket.by_key.get(&request.key).copied()
                && displaced != request.instance_id
            {
                bucket.detach(displaced, DetachReason::Displaced);
            }
            bucket
                .by_key
                .insert(request.key.clone(), request.instance_id);
            if let Some(handle_id) = request.handle_id {
                bucket.by_handle.insert(handle_id, request.instance_id);
            }
            let instance =
                (bucket.instances.get_mut(&request.instance_id)).ok_or(VfsFailure::NotFound)?;
            let old_key = std::mem::replace(&mut instance.key, request.key.clone());
            instance.handle_id = request.handle_id.or(instance.handle_id);
            instance.identity = request.identity;
            instance.kind = instance.identity.kind();
            instance.detached = None;
            instance.retained = false;
            instance.base_version = request.base_version;
            let edits = instance.stage.edits;
            let current = request
                .expected_edit
                .is_none_or(|expected| expected == edits);
            let mut cleared = 0;
            if request.clear_dirty && current {
                let size = request.base_size.unwrap_or(instance.stage.logical_size);
                cleared = instance.stage.extents.total();
                instance.stage = Stage::over(size);
                instance.stage.edits = edits;
                instance.dirty = false;
                instance.stage_id = None;
                instance.generation = 0;
            } else if let Some(size) = request.base_size {
                // The new base matches the image only below every resize since it was captured.
                let stage = &mut instance.stage;
                stage.base_limit = size.min(stage.base_limit).min(stage.logical_size);
            }
            if let (Some(stage_id), Some(generation)) = (request.stage_id, request.generation) {
                instance.stage_id = Some(stage_id);
                instance.generation = generation;
                instance.stage.sealed = sealed.map(|(size, edits)| stages::Sealed {
                    stage_id,
                    generation,
                    limit: size.min(instance.stage.logical_size),
                    edits,
                });
            }
            let group_id = instance.group_id;
            let change = self
                .shared
                .changes
                .touch(&bucket_name, &request.key, true)?;
            self.shared.changes.touch(&bucket_name, &old_key, true)?;
            instance.change = change;
            let summary = instance.summary();
            state.budget.release(group_id, cleared);
            (summary, cleared > 0)
        };
        if cleared {
            let path = self.path(&stages::volatile_name(summary.instance_id));
            crate::blocking(move || {
                crate::remove_quietly(&path);
                Ok(())
            })
            .await?;
        }
        Ok(VfsEvent::Instance(Some(summary)))
    }

    /// Detaches the instance of a handle and reclaims it at once when nothing keeps it any more.
    pub(crate) async fn detach_instance(
        &self,
        scope: &BucketScope,
        handle_id: u64,
        instance_id: Option<u64>,
        reason: DetachReason,
    ) -> Answer {
        let (summary, unused) = {
            let mut state = self.shared.state.lock();
            let bucket = state.buckets.get_mut(scope).ok_or(VfsFailure::NotFound)?;
            let id = instance_id.or_else(|| bucket.by_handle.get(&handle_id).copied());
            let id = id.ok_or(VfsFailure::NotFound)?;
            if !bucket.instances.contains_key(&id) {
                return Err(VfsFailure::NotFound);
            }
            bucket.detach(id, reason);
            let instance = bucket.instances.get_mut(&id).ok_or(VfsFailure::NotFound)?;
            instance.change = self
                .shared
                .changes
                .touch(&scope.bucket, &instance.key, true)?;
            let summary = instance.summary();
            let unused = match instance.idle() {
                true => reclaim(&mut state, scope, id),
                false => None,
            };
            (summary, unused)
        };
        if let Some(name) = unused {
            let path = self.path(&name);
            crate::blocking(move || {
                crate::remove_quietly(&path);
                Ok(())
            })
            .await?;
        }
        Ok(VfsEvent::Instance(Some(summary)))
    }

    /// Detaches the instance still naming `stage_id` at `generation` and, once no open holds
    /// it, gives up its checkpoint reference; any other instance state is left alone.
    pub(crate) async fn retain_instance(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
        reason: DetachReason,
    ) -> Answer {
        let unused = {
            let mut state = self.shared.state.lock();
            let Some(bucket) = state.buckets.get_mut(scope) else {
                return Ok(VfsEvent::Done);
            };
            let named = (bucket.instances.get(&instance_id)).is_some_and(|instance| {
                instance.stage_id == Some(stage_id) && instance.generation == generation
            });
            if !named {
                return Ok(VfsEvent::Done);
            }
            bucket.detach(instance_id, reason);
            let instance = (bucket.instances.get_mut(&instance_id)).ok_or(VfsFailure::NotFound)?;
            instance.retained = true;
            instance.change = self
                .shared
                .changes
                .touch(&scope.bucket, &instance.key, true)?;
            let closed = instance.opens.is_empty();
            match closed {
                true => release_stage(&mut state, scope, instance_id),
                false => None,
            }
        };
        if let Some(name) = unused {
            let path = self.path(&name);
            crate::blocking(move || {
                crate::remove_quietly(&path);
                Ok(())
            })
            .await?;
        }
        Ok(VfsEvent::Done)
    }

    pub(crate) fn update_witnesses(
        &self,
        scope: &BucketScope,
        updates: Vec<WitnessUpdate>,
    ) -> Answer {
        let mut state = self.shared.state.lock();
        let Some(bucket) = state.buckets.get_mut(scope) else {
            return Ok(VfsEvent::Done);
        };
        for update in updates {
            let by_handle = update.handle_id.and_then(|id| bucket.by_handle.get(&id));
            let id = by_handle
                .or_else(|| bucket.by_key.get(&update.key))
                .copied();
            let instance = id.and_then(|id| bucket.instances.get_mut(&id));
            if let Some(instance) = instance
                && instance.kind == HandleKind::Directory
            {
                instance.identity = HandleIdentity::Directory(update.witness);
            }
        }
        Ok(VfsEvent::Done)
    }

    /// Sets or clears one open's delete disposition; clearing keeps the link pending while any
    /// other open still asks for deletion.
    pub(crate) fn set_disposition(
        &self,
        scope: &BucketScope,
        instance_id: u64,
        open_id: Ulid,
        pending: bool,
        owner: Option<StageOwner>,
    ) -> Answer {
        let mut state = self.shared.state.lock();
        let instance = instance_mut(&mut state, scope, instance_id)?;
        let open = instance
            .opens
            .get_mut(&open_id)
            .ok_or(VfsFailure::NotFound)?;
        open.disposition = pending;
        if pending {
            instance.delete_pending = true;
            instance.disposition_owner = owner.or(instance.disposition_owner.take());
        } else {
            let retained =
                (instance.opens.values()).any(|open| open.disposition || open.delete_on_close);
            instance.delete_pending &= retained;
            if !instance.delete_pending {
                instance.disposition_owner = None;
            }
        }
        Ok(VfsEvent::Instance(Some(instance.summary())))
    }

    /// Clears a delete disposition that outlived every open once deletion was refused, and
    /// reclaims the instance when nothing else keeps it. Unstable dirty bytes with no checkpoint
    /// are detached too so they cannot linger unnamed or block vault fencing.
    pub(crate) async fn clear_disposition(&self, scope: &BucketScope, instance_id: u64) -> Answer {
        let unused = {
            let mut state = self.shared.state.lock();
            let Some(bucket) = state.buckets.get_mut(scope) else {
                return Ok(VfsEvent::Done);
            };
            let Some(instance) = bucket.instances.get_mut(&instance_id) else {
                return Ok(VfsEvent::Done);
            };
            if !instance.opens.is_empty() {
                return Ok(VfsEvent::Done);
            }
            instance.delete_pending = false;
            instance.disposition_owner = None;
            if instance.dirty && instance.stage_id.is_none() && instance.detached.is_none() {
                let key = instance.key.clone();
                bucket.detach(instance_id, DetachReason::LostLife);
                if let Some(instance) = bucket.instances.get_mut(&instance_id)
                    && let Ok(change) = self.shared.changes.touch(&scope.bucket, &key, true)
                {
                    instance.change = change;
                }
            }
            let idle = bucket
                .instances
                .get(&instance_id)
                .is_some_and(Instance::idle);
            match idle {
                true => reclaim(&mut state, scope, instance_id),
                false => None,
            }
        };
        if let Some(name) = unused {
            let path = self.path(&name);
            crate::blocking(move || {
                crate::remove_quietly(&path);
                Ok(())
            })
            .await?;
        }
        Ok(VfsEvent::Done)
    }

    /// Replaces the share reservation of one open if every other open of its instance admits it.
    pub(crate) fn update_share(
        &self,
        scope: &BucketScope,
        open_id: Ulid,
        share: ShareMode,
    ) -> Answer {
        let mut state = self.shared.state.lock();
        if (share.write_access || share.delete_access) && state.fences.closed(scope) {
            return Err(fenced());
        }
        let bucket = state.buckets.get_mut(scope).ok_or(VfsFailure::NotFound)?;
        let instance_id = *bucket.by_open.get(&open_id).ok_or(VfsFailure::NotFound)?;
        let instance = (bucket.instances.get_mut(&instance_id)).ok_or(VfsFailure::NotFound)?;
        let mut others = (instance.opens.iter()).filter(|(id, _)| **id != open_id);
        if !others.all(|(_, other)| shares_with(&other.share, &share)) {
            return Err(VfsFailure::ShareViolation);
        }
        let open = instance
            .opens
            .get_mut(&open_id)
            .ok_or(VfsFailure::NotFound)?;
        open.share = share;
        Ok(VfsEvent::Done)
    }
}

pub(crate) fn fenced() -> VfsFailure {
    VfsFailure::VaultFenced("the bucket is fenced for a vault-locked transition".to_string())
}
