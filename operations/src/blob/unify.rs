//! Finds keys whose versions keep the same content in several linked files, lets those
//! versions read one file and hands the other files to the release of renamed files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::request_authorization::BucketAccess;
use crate::blob::cleanup::schedule_cleanup_effect;
use crate::blob::migration::move_routing;
use crate::driver::DriverContext;
use crate::jobs::store::iter_prefix_page;
use crate::s3::bucket::linked::JobStep;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE,
    BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DELETE_CLAIM_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE, LINKED_JOB_KEYSPACE, MANAGED_COPY_KEYSPACE,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, claim_key, object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobCleanupWork, BlobVersion, BlobVersionState, ManagedCopyKey,
    ManagedCopyRecord, VersionKey,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::linked_job::LinkedJob;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;
use ulid::Ulid;

/// Version rows one storage read returns, and one preview or job page passes over at most.
const VERSION_PAGE: usize = 1024;
const PAGE_SCAN: usize = 200_000;
const TOTAL_SCAN: usize = 2_000_000;
const JOB_SCAN: usize = 20_000;
/// Keys with copies one job page unifies at most.
const JOB_KEYS: usize = 64;
const RULE_PAGE: usize = 128;

/// The linked files with the same content of one key: the one its versions keep and the
/// copies the cleanup releases.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Duplicate {
    pub key: String,
    pub keep: BackendLocation,
    pub copies: Vec<BackendLocation>,
    /// The versions that read a copy now, with their stored rows.
    versions: Vec<(Ulid, Vec<u8>)>,
}

impl Duplicate {
    /// Bytes the release of the copies frees.
    pub fn freed(&self) -> u64 {
        self.copies.iter().map(|copy| copy.blob_size).sum()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnifyPage {
    /// Keys with copies, and the bytes their release frees; lower bounds unless `complete`.
    pub keys: u64,
    pub bytes: u64,
    pub complete: bool,
    pub entries: Vec<Duplicate>,
    /// The key after which the next page starts; a key the caller may read.
    pub next: Option<String>,
}

/// The versions of the keys of a bucket in storage order, one key at a time.
pub(crate) struct KeyWalk<'a> {
    context: &'a DriverContext,
    bucket: &'a str,
    start: Option<Key>,
    rows: std::collections::VecDeque<(Key, Value)>,
    end: bool,
    pub(crate) scanned: usize,
}

impl<'a> KeyWalk<'a> {
    /// Starts after every version of `after`.
    pub(crate) fn new(context: &'a DriverContext, bucket: &'a str, after: Option<&str>) -> Self {
        let start = after.and_then(|key| VersionKey::object_prefix(bucket, key).ok());
        // Every version of the key sorts before its prefix followed by these bytes.
        let start = start.map(|mut prefix| {
            prefix.extend([u8::MAX; 32]);
            prefix.into()
        });
        Self {
            context,
            bucket,
            start,
            rows: Default::default(),
            end: false,
            scanned: 0,
        }
    }

    async fn fill(&mut self) -> Result<(), String> {
        let prefix = VersionKey::bucket_prefix(self.bucket).map_err(|error| error.to_string())?;
        let page = iter_prefix_page(
            &self.context.storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            Some(prefix.into()),
            self.start.take(),
            VERSION_PAGE,
            None,
        );
        let (rows, next) = page.await?;
        self.end = next.is_none();
        self.start = rows.last().map(|(key, _)| key.clone());
        self.rows.extend(rows);
        Ok(())
    }

    /// The next key and its versions; `None` at the end of the bucket.
    pub(crate) async fn next(&mut self) -> Result<Option<(String, Vec<(Ulid, Vec<u8>)>)>, String> {
        let mut key = None::<String>;
        let mut versions = Vec::new();
        loop {
            if self.rows.is_empty() {
                if self.end {
                    return Ok(key.map(|key| (key, versions)));
                }
                self.fill().await?;
                continue;
            }
            let (raw, value) = &self.rows[0];
            let version = VersionKey::from_bytes(raw).map_err(|error| error.to_string())?;
            match key.as_deref() {
                Some(current) if current != version.key => return Ok(key.map(|k| (k, versions))),
                Some(_) => {}
                None => key = Some(version.key.clone()),
            }
            versions.push((version.version_id, value.to_vec()));
            self.scanned += 1;
            self.rows.pop_front();
        }
    }
}

/// A version with its stored row, and the linked file it reads.
type Member = (Ulid, Vec<u8>, Ulid);
/// A version with its stored row.
type Reader = (Ulid, Vec<u8>);

/// The linked files of `key` with the same content as another of its files, by content.
async fn duplicates(
    context: &DriverContext,
    routing: Option<&RoutingSnapshot>,
    (bucket, key): (&str, &str),
    versions: Vec<(Ulid, Vec<u8>)>,
) -> Result<Vec<Duplicate>, String> {
    let mut found = Vec::new();
    let mut groups: BTreeMap<([u8; 32], Vec<u8>), Vec<Member>> = BTreeMap::new();
    for (id, value) in versions {
        let version = BlobVersion::from_bytes(&value).map_err(|error| error.to_string())?;
        let BlobVersionState::Materialized {
            blob_hash,
            backend: backend @ BackendRef::Group(_),
            encoding: EncodingClass::Plain { location },
            ..
        } = &version.state
        else {
            continue;
        };
        let group = (*blob_hash, backend.key_bytes());
        groups
            .entry(group)
            .or_default()
            .push((id, value, *location));
    }
    for members in groups.into_values() {
        // Only content that two files hold needs its location rows read.
        let files: std::collections::HashSet<_> = members.iter().map(|(.., at)| *at).collect();
        if files.len() < 2 {
            continue;
        }
        let mut files: Vec<(BackendLocation, Vec<Reader>)> = Vec::new();
        for (id, value, _) in &members {
            let version = BlobVersion::from_bytes(value).map_err(|error| error.to_string())?;
            let Some(location_key) = version.location_key() else {
                continue;
            };
            let read = read(
                context,
                BLOB_LOCATIONS_KEYSPACE,
                location_key.to_bytes().into(),
            );
            let Some(row) = read.await? else {
                continue;
            };
            let location = BackendLocation::from_bytes(&row).map_err(|error| error.to_string())?;
            if !location.is_external() || location.staging || location.partial {
                continue;
            }
            match files
                .iter_mut()
                .find(|(file, _)| file.ulid == location.ulid)
            {
                Some((_, readers)) => readers.push((*id, value.clone())),
                None => files.push((location, vec![(*id, value.clone())])),
            }
        }
        if files.len() < 2 {
            continue;
        }
        // The file the current name pattern gives one of its versions is kept, else the newest.
        let named = |(file, readers): &(BackendLocation, Vec<(Ulid, Vec<u8>)>)| {
            readers.iter().any(|(id, _)| {
                let target = routing.and_then(|routing| routing.plain_target(bucket, key, *id));
                target.is_some_and(|target| target.path == file.backend_path)
            })
        };
        let newest = |(_, readers): &(BackendLocation, Vec<(Ulid, Vec<u8>)>)| {
            readers.iter().map(|(id, _)| *id).max()
        };
        let keep = match files.iter().position(named) {
            Some(at) => at,
            None => (0..files.len())
                .max_by_key(|at| newest(&files[*at]))
                .unwrap_or_default(),
        };
        let (keep, _) = files.remove(keep);
        let versions = files
            .iter()
            .flat_map(|(_, readers)| readers.clone())
            .collect();
        let copies = files.into_iter().map(|(file, _)| file).collect();
        found.push(Duplicate {
            key: key.to_string(),
            keep,
            copies,
            versions,
        });
    }
    Ok(found)
}

/// One preview page of the keys under `prefix` after `after` with copies, as far as the
/// caller may read them, and the totals.
pub async fn preview_page(
    context: &DriverContext,
    (bucket, prefix): (&str, &str),
    access: &BucketAccess,
    (filter, after, limit): (Option<&str>, Option<String>, usize),
) -> Result<UnifyPage, String> {
    let routing = move_routing(context, bucket).await?;
    let visible = |key: &str| access.allows(Some(key), &Permission::READ);
    let readable = |key: &str| {
        key.starts_with(prefix) && filter.is_none_or(|filter| key.contains(filter)) && visible(key)
    };
    let mut page = UnifyPage {
        complete: true,
        ..UnifyPage::default()
    };
    let mut walk = KeyWalk::new(context, bucket, None);
    while let Some((key, versions)) = walk.next().await? {
        if walk.scanned > TOTAL_SCAN {
            page.complete = false;
            break;
        }
        if !readable(&key) {
            continue;
        }
        let found = duplicates(context, routing.as_ref(), (bucket, &key), versions).await?;
        page.keys += u64::from(!found.is_empty());
        page.bytes += found.iter().map(Duplicate::freed).sum::<u64>();
    }
    let mut walk = KeyWalk::new(context, bucket, after.as_deref());
    let mut last = None;
    while let Some((key, versions)) = walk.next().await? {
        if page.entries.len() == limit || walk.scanned > PAGE_SCAN {
            page.next = page.entries.last().map(|entry| entry.key.clone()).or(last);
            if page.next.is_none() {
                return Err("too many keys in a row are hidden from the caller".to_string());
            }
            return Ok(page);
        }
        // A cursor may name any key the caller reads, also one the filter leaves out.
        if visible(&key) {
            last = Some(key.clone());
        }
        if !readable(&key) {
            continue;
        }
        let found = duplicates(context, routing.as_ref(), (bucket, &key), versions).await?;
        page.entries.extend(found);
    }
    Ok(page)
}

/// Unifies the next keys after the job's cursor; keys the caller may not write or written
/// after the start keep their files. Each key changes with the job's progress, and only while
/// the job is stored as read, so a paused or aborted job changes nothing more.
pub async fn job_page(
    context: &DriverContext,
    (bucket, prefix): (&str, &str),
    access: &BucketAccess,
    job: &LinkedJob,
) -> Result<JobStep, String> {
    let routing = move_routing(context, bucket).await?;
    let mut current = job.clone();
    let mut step = JobStep {
        cursor: job.cursor.clone(),
        ..JobStep::default()
    };
    let mut walk = KeyWalk::new(context, bucket, job.cursor.as_deref());
    let mut unified = 0;
    loop {
        if unified == JOB_KEYS || walk.scanned > JOB_SCAN {
            return Ok(step);
        }
        let Some((key, versions)) = walk.next().await? else {
            step.finished = true;
            return Ok(step);
        };
        if key.starts_with(prefix) {
            let fresh = versions
                .iter()
                .all(|(id, _)| id.timestamp_ms() <= job.started_at_ms);
            let found = duplicates(context, routing.as_ref(), (bucket, &key), versions).await?;
            let allowed = access.allows(Some(&key), &Permission::WRITE) && fresh;
            for found in found {
                let Some(routing) = routing.as_ref().filter(|_| allowed) else {
                    step.skipped += 1;
                    continue;
                };
                // The key's progress is stored with it; the cursor names the key before it.
                let mut next = current.clone();
                next.cursor = step.cursor.clone();
                next.done += 1;
                (next.skipped, next.failed) =
                    (next.skipped + step.skipped, next.failed + step.failed);
                let fence = Some((&current, &next));
                match unify(context, (bucket, routing.group_id), &found, fence).await? {
                    Unified::Done => {
                        (step.skipped, step.failed) = (0, 0);
                        current = next;
                    }
                    Unified::Kept => step.skipped += 1,
                    Unified::Changed => step.failed += 1,
                    Unified::Stopped => {
                        return Ok(JobStep {
                            cursor: current.cursor.clone(),
                            ..JobStep::default()
                        });
                    }
                }
                unified += 1;
            }
        }
        step.cursor = Some(key);
    }
}

/// What `unify` did with one key's copies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unified {
    Done,
    /// Another key or version records every copy, so the key keeps its files.
    Kept,
    /// The versions or the kept file changed meanwhile; nothing changed.
    Changed,
    /// The job was paused, aborted or replaced; nothing changed.
    Stopped,
}

/// Points the versions of each copy only this key records at the kept file, with their copy
/// registrations, and queues the copies' release, in one transaction that rereads them all.
/// With `fence`, the bucket's job must still be stored as its first value and gets its second.
pub async fn unify(
    context: &DriverContext,
    (bucket, group_id): (&str, GroupId),
    found: &Duplicate,
    fence: Option<(&LinkedJob, &LinkedJob)>,
) -> Result<Unified, String> {
    let rules = rules_on(context, group_id, &found.keep.backend).await?;
    let storage = &context.storage_handle;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(format!("could not start a transaction: {other:?}")),
    };
    let staged = Stage {
        context,
        txn_id,
        bucket,
        rules: &rules,
        found,
    };
    let result = staged.run(fence).await;
    let outcome = match result {
        Ok(outcome) => outcome,
        Err(error) => {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(error);
        }
    };
    if outcome != Unified::Done {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Ok(outcome);
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) if !error.proves_no_commit() => {
            return Err(format!("the unification may not have committed: {error}"));
        }
        _ => return Ok(Unified::Changed),
    }
    if let Some(tasks) = context.task_handle.as_ref() {
        use aruna_core::handle::Handle as _;
        tasks.send_effect(schedule_cleanup_effect()).await;
    }
    Ok(Unified::Done)
}

/// The reads and writes of one `unify` transaction.
struct Stage<'a> {
    context: &'a DriverContext,
    txn_id: Ulid,
    bucket: &'a str,
    rules: &'a [BackendImport],
    found: &'a Duplicate,
}

impl Stage<'_> {
    async fn batch(&self, reads: Vec<(String, Key)>) -> Result<Vec<Option<Value>>, String> {
        let read = StorageEffect::BatchRead {
            reads,
            txn_id: Some(self.txn_id),
        };
        match self.context.storage_handle.send_storage_effect(read).await {
            Event::Storage(StorageEvent::BatchReadResult { values }) => {
                Ok(values.into_iter().map(|(_, value)| value).collect())
            }
            other => Err(format!("could not read the copies: {other:?}")),
        }
    }

    async fn run(&self, fence: Option<(&LinkedJob, &LinkedJob)>) -> Result<Unified, String> {
        let found = self.found;
        let error = |error: aruna_core::errors::ConversionError| error.to_string();
        let mut writes = Vec::new();
        let name: Key = self.bucket.as_bytes().to_vec().into();
        if let Some((expected, next)) = fence {
            let read = vec![(LINKED_JOB_KEYSPACE.to_string(), name.clone())];
            let stored = self.batch(read).await?.pop().flatten();
            if stored.as_deref() != Some(expected.to_bytes().map_err(error)?.as_slice()) {
                return Ok(Unified::Stopped);
            }
            let next = next.to_bytes().map_err(error)?;
            writes.push((LINKED_JOB_KEYSPACE.to_string(), name, next.into()));
        }
        let keep_key = found.keep.location_key().map_err(error)?;
        let mut reads = vec![(
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            Key::from(keep_key.to_bytes()),
        )];
        for (id, _) in &found.versions {
            let version = VersionKey::new(self.bucket, &found.key, *id);
            let version = version.to_bytes().map_err(error)?;
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), version.into()));
        }
        for copy in &found.copies {
            reads.push((BACKEND_VERSION_KEYSPACE.to_string(), version_key(copy.ulid)));
            for rule in self.rules {
                let row = object_key(rule.import_id, &copy.backend_path);
                reads.push((IMPORTED_OBJECT_KEYSPACE.to_string(), row));
            }
        }
        let mut values = self.batch(reads).await?.into_iter();
        let kept = values.next().flatten();
        if kept.as_deref() != Some(found.keep.to_bytes().map_err(error)?.as_slice()) {
            return Ok(Unified::Changed);
        }
        // A kept file whose backend version a release is deleting takes no new reader.
        let read = vec![(
            BACKEND_VERSION_KEYSPACE.to_string(),
            version_key(found.keep.ulid),
        )];
        if let Some(provider) = self.batch(read).await?.pop().flatten() {
            let provider = String::from_utf8_lossy(&provider).into_owned();
            let claim = claim_key(&found.keep, &provider).map_err(error)?;
            let read = vec![(DELETE_CLAIM_KEYSPACE.to_string(), claim)];
            if self.batch(read).await?.pop().flatten().is_some() {
                return Ok(Unified::Changed);
            }
        }
        // Which copy each version reads now; any change refuses the whole key.
        let mut readers: HashMap<Ulid, Vec<(Ulid, BlobVersion)>> = HashMap::new();
        for (id, stored) in &found.versions {
            if values.next().flatten().as_deref() != Some(stored.as_slice()) {
                return Ok(Unified::Changed);
            }
            let version = BlobVersion::from_bytes(stored).map_err(error)?;
            if let BlobVersionState::Materialized {
                encoding: EncodingClass::Plain { location },
                ..
            } = &version.state
            {
                readers.entry(*location).or_default().push((*id, version));
            }
        }
        let encoding = found.keep.format.encoding();
        let mut changed = 0;
        for copy in &found.copies {
            let provider = values.next().flatten();
            let mine = readers.remove(&copy.ulid).unwrap_or_default();
            let mut owner = None;
            let mut foreign = false;
            for rule in self.rules {
                let row = values.next().flatten();
                let row = row.as_deref().map(ImportedObject::from_bytes).transpose();
                let Some(row) = row.map_err(error)? else {
                    continue;
                };
                // A row of another key or version makes the file that version's own.
                let ours = row.key.as_deref().is_none_or(|key| key == found.key)
                    && mine.iter().any(|(id, _)| *id == row.version_id);
                match ours {
                    true => owner = owner.or(Some((rule.import_id, row.version_id))),
                    false => foreign = true,
                }
            }
            if foreign || mine.is_empty() {
                continue;
            }
            for (id, mut version) in mine {
                if let BlobVersionState::Materialized { encoding: old, .. } = &mut version.state {
                    *old = encoding;
                }
                let key = VersionKey::new(self.bucket, &found.key, id);
                writes.extend(self.copies(&key, copy).await?);
                let value = version.to_bytes().map_err(error)?;
                let key = key.to_bytes().map_err(error)?;
                writes.push((BLOB_VERSIONS_KEYSPACE.to_string(), key.into(), value.into()));
            }
            writes.push(release(copy, provider, owner)?);
            changed += 1;
        }
        if changed == 0 {
            return Ok(Unified::Kept);
        }
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: Some(self.txn_id),
        };
        match self.context.storage_handle.send_storage_effect(write).await {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(Unified::Done),
            other => Err(format!("could not write the unification: {other:?}")),
        }
    }

    /// The copy registrations of `version` that name `copy`, moved to the kept file.
    async fn copies(
        &self,
        version: &VersionKey,
        copy: &BackendLocation,
    ) -> Result<Vec<(String, Key, Value)>, String> {
        let prefix = ManagedCopyKey::version_prefix(version).map_err(|error| error.to_string())?;
        let page = iter_prefix_page(
            &self.context.storage_handle,
            MANAGED_COPY_KEYSPACE,
            Some(prefix.into()),
            None,
            RULE_PAGE,
            Some(self.txn_id),
        );
        let (rows, _) = page.await?;
        let mut writes = Vec::new();
        for (key, value) in rows {
            let record =
                ManagedCopyRecord::from_bytes(&value).map_err(|error| error.to_string())?;
            if record.location.same_object(copy) {
                let record = ManagedCopyRecord {
                    location: self.found.keep.clone(),
                    ..record
                };
                let value = record.to_bytes().map_err(|error| error.to_string())?;
                writes.push((MANAGED_COPY_KEYSPACE.to_string(), key, value.into()));
            }
        }
        Ok(writes)
    }
}

/// The release of `copy` at its backend version, or without one the drop of its location row,
/// which leaves the file in place.
fn release(
    copy: &BackendLocation,
    provider: Option<Value>,
    owner: Option<(Ulid, Ulid)>,
) -> Result<(String, Key, Value), String> {
    let error = |error: aruna_core::errors::ConversionError| error.to_string();
    let Some(provider) = provider else {
        let location = copy.location_key().map_err(error)?;
        let candidate =
            ReclaimCandidateKey::new(location.backend, location.encoding, location.blake3_hash);
        let value = ReclaimCandidate {
            enqueued_at: SystemTime::now(),
        };
        let value = value.to_bytes().map_err(error)?;
        let key = candidate.to_bytes().into();
        return Ok((BLOB_RECLAIM_KEYSPACE.to_string(), key, value.into()));
    };
    let (import_id, version_id) = owner.unwrap_or((Ulid::nil(), Ulid::nil()));
    let work = BlobCleanupWork::ReleaseRenamed {
        location: copy.clone(),
        import_id,
        version_id,
        version: String::from_utf8(provider.to_vec()).map_err(|error| error.to_string())?,
    };
    let value = work.to_bytes().map_err(error)?;
    let key = Ulid::generate().to_bytes().to_vec().into();
    Ok((BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into()))
}

/// The group's rules that read or write on `backend`, whose rows may record a copy.
pub(crate) async fn rules_on(
    context: &DriverContext,
    group_id: GroupId,
    backend: &BackendRef,
) -> Result<Vec<BackendImport>, String> {
    let mut rules = Vec::new();
    let mut after = None;
    loop {
        let prefix = Some(group_id.to_bytes().to_vec().into());
        let page = iter_prefix_page(
            &context.storage_handle,
            BACKEND_IMPORT_KEYSPACE,
            prefix,
            after.take(),
            RULE_PAGE,
            None,
        );
        let (rows, next) = page.await?;
        for (_, value) in rows {
            let rule = BackendImport::from_bytes(&value).map_err(|error| error.to_string())?;
            let on = |id: Ulid| BackendRef::Group(id) == *backend;
            if on(rule.backend_id) || on(rule.write_backend()) {
                rules.push(rule);
            }
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(rules),
        }
    }
}

async fn read(context: &DriverContext, space: &str, key: Key) -> Result<Option<Vec<u8>>, String> {
    let read = StorageEffect::Read {
        key_space: space.to_string(),
        key,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.map(|v| v.to_vec())),
        other => Err(format!("could not read {space}: {other:?}")),
    }
}

#[cfg(test)]
#[path = "unify_tests.rs"]
mod tests;
