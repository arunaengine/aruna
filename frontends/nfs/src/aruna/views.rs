//! Immutable job input views over NFS: the live, nonterminal run record on every request, a
//! deterministic tree of its captured entries, per-entry visibility and exact-version reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::local::JobRecord;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::vfs::{NfsMountBinding, NfsMountTarget};
use aruna_ops_data::vfs::path::{JOB_ROOT, JobEntry, JobNodeKind, JobTree, JobTreeItem};
use aruna_ops_work::jobs::store::row_calls::read_job_record;
use aruna_ops_work::jobs::workflow::input_view::{
    ViewEntry, ViewEntryOperation, ViewError, ViewFrom, ViewLister, creator, view_entries,
};
use aruna_runner::driver::drive;
use bytes::Bytes;
use futures_util::StreamExt;

use super::{ArunaNfsBackend, view_status};
use crate::backend::{
    DirectoryEntry, DirectoryPage, HandleStatus, LockError, LockKind, LockRange, ObjectAttributes,
    ObjectRef, OwnerKey, ReadReply, ShareRequest,
};
use crate::types::{NfsStat4, NfsTime, NfsType4, access, share};

/// Cookies of a view are its node ids moved past the reserved cookies 0, 1 and 2.
const COOKIE_BASE: u64 = 2;

/// One request's view of a run: the record as stored now, its mounted entries and the tree
/// built from them before any visibility filter.
pub(super) struct RunView {
    pub record: JobRecord,
    pub entries: Vec<ViewEntry>,
    pub tree: JobTree,
    pub creator: AuthContext,
}

impl RunView {
    fn item(&self, object: ObjectRef) -> Result<&JobTreeItem, NfsStat4> {
        self.tree.item(object.object_id).ok_or(NfsStat4::Stale)
    }

    fn entry(&self, item: &JobTreeItem) -> Option<&ViewEntry> {
        let captured = self.tree.entry(item)?;
        self.entries.iter().find(|entry| entry.key == captured.path)
    }
}

fn job_of(binding: &NfsMountBinding) -> Result<JobId, NfsStat4> {
    match &binding.target {
        NfsMountTarget::JobInputView { job_id } => Ok(*job_id),
        _ => Err(NfsStat4::Invalid),
    }
}

/// The neutral entry list a view tree is built from; it never depends on the caller.
pub(super) fn tree_entries(entries: &[ViewEntry]) -> Vec<JobEntry> {
    let entry = |view: &ViewEntry| JobEntry {
        path: view.key.clone(),
        identity: format!("{}@{}", view.key, view.captured.version_id),
        size: view.captured.bytes,
    };
    entries.iter().map(entry).collect()
}

/// The time a run was created, which every entry of its immutable view shows.
fn created_time(record: &JobRecord) -> NfsTime {
    let millis = record.created_at_ms;
    NfsTime {
        seconds: i64::try_from(millis / 1000).unwrap_or(i64::MAX),
        nanos: u32::try_from(millis % 1000).unwrap_or(0) * 1_000_000,
    }
}

impl ArunaNfsBackend {
    /// Reads the run again; a missing, finished or ambiguous run serves nothing.
    pub(super) async fn view(&self, job_id: JobId) -> Result<RunView, NfsStat4> {
        let record = drive(read_job_record(job_id), &self.context)
            .await
            .map_err(|_| NfsStat4::Delay)?
            .ok_or(NfsStat4::Stale)?;
        let entries = view_entries(&record).map_err(|_| NfsStat4::Stale)?;
        let tree = JobTree::build(&tree_entries(&entries)).map_err(|_| NfsStat4::Io)?;
        let realm = self.context.serving_realm(record.created_by.realm_id);
        let creator = creator(&record, realm);
        Ok(RunView {
            record,
            entries,
            tree,
            creator,
        })
    }

    async fn binding_view(&self, binding: &NfsMountBinding) -> Result<RunView, NfsStat4> {
        self.view(job_of(binding)?).await
    }

    /// Whether the run's creator may still read `entry` at its source on this node; an entry
    /// held elsewhere is never read from a holder or through a grant.
    async fn entry_shows(&self, view: &RunView, entry: &ViewEntry) -> Result<bool, NfsStat4> {
        let head = ViewEntryOperation::head(view.creator.clone(), entry.clone(), self.node_id);
        let Some(head) = head.local_only() else {
            return Ok(false);
        };
        match drive(head, &self.context).await {
            Ok(_) => Ok(true),
            Err(ViewError::Denied | ViewError::NotFound) => Ok(false),
            Err(error) => Err(view_status(&error)),
        }
    }

    /// A file shows when its entry does; a directory when any entry below it does.
    async fn item_shows(&self, view: &RunView, item: &JobTreeItem) -> Result<bool, NfsStat4> {
        if item.node_id == JOB_ROOT {
            return Ok(true);
        }
        if item.kind == JobNodeKind::File {
            return match view.entry(item) {
                Some(entry) => self.entry_shows(view, entry).await,
                None => Ok(false),
            };
        }
        let below = format!("{}/", item.path);
        for entry in view
            .entries
            .iter()
            .filter(|entry| entry.key.starts_with(&below))
        {
            if self.entry_shows(view, entry).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The shown item of `object`; a hidden one is as missing as a deleted one.
    async fn shown_item<'a>(
        &self,
        view: &'a RunView,
        object: ObjectRef,
    ) -> Result<&'a JobTreeItem, NfsStat4> {
        let item = view.item(object)?;
        match self.item_shows(view, item).await? {
            true => Ok(item),
            false => Err(NfsStat4::Stale),
        }
    }

    pub(super) async fn view_resolve(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> HandleStatus {
        let Ok(view) = self.binding_view(binding).await else {
            return HandleStatus::Stale;
        };
        match self.shown_item(&view, object).await {
            Ok(_) => HandleStatus::Live,
            Err(_) => HandleStatus::Stale,
        }
    }

    pub(super) async fn view_lookup(
        &self,
        binding: &NfsMountBinding,
        directory: ObjectRef,
        name: &str,
    ) -> Result<ObjectRef, NfsStat4> {
        let view = self.binding_view(binding).await?;
        let parent = self.shown_item(&view, directory).await?;
        if parent.kind != JobNodeKind::Directory {
            return Err(NfsStat4::NotDirectory);
        }
        let item = view.tree.lookup(parent.node_id, name);
        let item = item.ok_or(NfsStat4::NoEntry)?;
        match self.item_shows(&view, item).await? {
            true => Ok(ObjectRef {
                mount_id: directory.mount_id,
                object_id: item.node_id,
            }),
            false => Err(NfsStat4::NoEntry),
        }
    }

    pub(super) async fn view_parent(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> Result<ObjectRef, NfsStat4> {
        let view = self.binding_view(binding).await?;
        let item = self.shown_item(&view, object).await?;
        let parent = match item.parent {
            0 => JOB_ROOT,
            parent => parent,
        };
        Ok(ObjectRef {
            mount_id: object.mount_id,
            object_id: parent,
        })
    }

    pub(super) async fn view_attributes(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> Result<ObjectAttributes, NfsStat4> {
        let view = self.binding_view(binding).await?;
        let item = self.shown_item(&view, object).await?;
        Ok(view_attributes(binding, &view, item))
    }

    pub(super) async fn view_access(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        requested: u32,
    ) -> Result<u32, NfsStat4> {
        let view = self.binding_view(binding).await?;
        let item = self.shown_item(&view, object).await?;
        let readable = match item.kind {
            JobNodeKind::File => access::READ | access::EXECUTE,
            JobNodeKind::Directory => access::READ | access::LOOKUP,
        };
        Ok(requested & readable)
    }

    pub(super) async fn view_directory(
        &self,
        binding: &NfsMountBinding,
        directory: ObjectRef,
        (cookie, verifier, limit): (u64, [u8; 8], usize),
    ) -> Result<DirectoryPage, NfsStat4> {
        let view = self.binding_view(binding).await?;
        let item = self.shown_item(&view, directory).await?;
        if item.kind != JobNodeKind::Directory {
            return Err(NfsStat4::NotDirectory);
        }
        let own_verifier = view_verifier(&view.record);
        if cookie != 0 && verifier != own_verifier {
            return Err(NfsStat4::NotSame);
        }
        let mut from = resume_from(&view, item, cookie)?;
        let prefix = match item.path.is_empty() {
            true => String::new(),
            false => format!("{}/", item.path),
        };
        let mut found: Vec<(String, u64)> = Vec::new();
        let eof = loop {
            let scope = (prefix.as_str(), Some("/"));
            let mut lister = ViewLister::new(&view.entries, scope, from.clone(), limit.max(1));
            while let Some(candidate) = lister.candidate() {
                let shown = self.entry_shows(&view, candidate).await?;
                lister.answer(shown);
            }
            let listing = lister.finish();
            let objects = listing
                .objects
                .iter()
                .map(|at| view.entries[*at].key.clone());
            let names = objects.chain(listing.prefixes.iter().cloned());
            for key in names {
                let name = key.strip_prefix(&prefix).unwrap_or(&key);
                let name = name.trim_end_matches('/');
                if let Some(child) = view.tree.lookup(item.node_id, name) {
                    found.push((key.clone(), child.node_id));
                }
            }
            match listing.next {
                // A page of only hidden entries continues instead of ending early.
                Some(next @ ViewFrom::Hidden { .. }) if found.is_empty() => from = Some(next),
                next => break next.is_none(),
            }
        };
        found.sort();
        let entries = found
            .into_iter()
            .filter_map(|(_, node_id)| {
                let child = view.tree.item(node_id)?;
                Some(DirectoryEntry {
                    name: child.name.clone(),
                    cookie: node_id + COOKIE_BASE,
                    object: ObjectRef {
                        mount_id: directory.mount_id,
                        object_id: node_id,
                    },
                    attributes: view_attributes(binding, &view, child),
                })
            })
            .collect();
        Ok(DirectoryPage {
            entries,
            verifier: own_verifier,
            eof,
        })
    }

    pub(super) async fn view_open(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        share: &ShareRequest,
    ) -> Result<(), NfsStat4> {
        if share.access & share::WRITE != 0 {
            return Err(NfsStat4::ReadOnly);
        }
        let view = self.binding_view(binding).await?;
        match self.shown_item(&view, object).await?.kind {
            JobNodeKind::File => Ok(()),
            JobNodeKind::Directory => Err(NfsStat4::IsDirectory),
        }
    }

    /// Reads the exact captured version at this node's source; a missing version, a lock or
    /// a needed key stays an error and nothing else is ever served in its place.
    pub(super) async fn view_read(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        offset: u64,
        count: u32,
    ) -> Result<ReadReply, NfsStat4> {
        let view = self.binding_view(binding).await?;
        let item = self.shown_item(&view, object).await?;
        if item.kind == JobNodeKind::Directory {
            return Err(NfsStat4::IsDirectory);
        }
        let entry = view.entry(item).ok_or(NfsStat4::Stale)?;
        let size = entry.captured.bytes;
        let end = offset.saturating_add(u64::from(count)).min(size);
        if offset >= end {
            return Ok(ReadReply {
                data: Bytes::new(),
                eof: true,
            });
        }
        let creator = view.creator.clone();
        let read =
            ViewEntryOperation::read(creator, entry.clone(), self.node_id, Some(offset..end));
        let read = read.local_only().ok_or(NfsStat4::Access)?;
        let stream = drive(read, &self.context)
            .await
            .map_err(|error| view_status(&error))?;
        let mut stream = stream.ok_or(NfsStat4::Io)?;
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.map_err(|_| NfsStat4::Io)?);
        }
        let eof = offset + data.len() as u64 >= size;
        Ok(ReadReply {
            data: data.into(),
            eof,
        })
    }

    /// Read locks never conflict on an immutable view; nobody can hold a write open.
    pub(super) fn view_lock(
        &self,
        object: ObjectRef,
        owner: &OwnerKey,
        kind: LockKind,
        range: LockRange,
    ) -> Result<(), LockError> {
        if kind == LockKind::Write {
            return Err(LockError::Status(NfsStat4::OpenMode));
        }
        self.remember_lock(object, owner, range);
        Ok(())
    }
}

/// Where a view listing resumes after `cookie`: past the entry or common prefix it named.
fn resume_from(
    view: &RunView,
    directory: &JobTreeItem,
    cookie: u64,
) -> Result<Option<ViewFrom>, NfsStat4> {
    match cookie {
        0 => Ok(None),
        1 | 2 => Err(NfsStat4::BadCookie),
        cookie => {
            let item = view.tree.item(cookie - COOKIE_BASE);
            let item = item.filter(|item| item.parent == directory.node_id);
            let item = item.ok_or(NfsStat4::BadCookie)?;
            let marker = match item.kind {
                JobNodeKind::File => item.path.clone(),
                JobNodeKind::Directory => format!("{}/", item.path),
            };
            Ok(Some(ViewFrom::After(marker)))
        }
    }
}

/// The cookie verifier of a view, fixed for the run.
fn view_verifier(record: &JobRecord) -> [u8; 8] {
    record.created_at_ms.to_be_bytes()
}

fn view_attributes(
    binding: &NfsMountBinding,
    view: &RunView,
    item: &JobTreeItem,
) -> ObjectAttributes {
    let time = created_time(&view.record);
    let (kind, size, mode, links) = match item.kind {
        JobNodeKind::File => {
            let size = view.entry(item).map_or(0, |entry| entry.captured.bytes);
            (NfsType4::Regular, size, binding.mode & 0o444, 1)
        }
        JobNodeKind::Directory => (NfsType4::Directory, 0, (binding.mode & 0o444) | 0o111, 2),
    };
    ObjectAttributes {
        kind,
        size,
        // The view never changes while it serves; a finished run serves nothing.
        change: 1,
        fileid: item.node_id,
        mode,
        links,
        uid: binding.display_uid,
        gid: binding.display_gid,
        space_used: size,
        accessed: time,
        modified: time,
        changed: time,
    }
}
