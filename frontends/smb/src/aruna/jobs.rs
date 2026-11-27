//! Read-only `in-<job_id>` shares: the live record of a nonterminal run read again on every
//! request, its immutable input tree, per-caller visibility and exact captured versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::local::JobRecord;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::vfs_effects::VfsProtocol;
use aruna_ops_data::vfs::path::{JOB_ROOT, Resolved, marker_key, representable, resolve_name};
use aruna_ops_data::vfs::{JobEntry, JobNodeKind, JobTree, JobTreeItem};
use aruna_ops_work::jobs::store::row_calls::read_job_record;
use aruna_ops_work::jobs::workflow::input_view::{
    MAX_VIEW_KEYS, ViewEntry, ViewEntryOperation, ViewError, ViewFrom, ViewLister, creator,
    view_bucket, view_entries, view_job,
};
use aruna_runner::driver::drive;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use ulid::Ulid;

use super::{ArunaSmbBackend, Entry, ViewOpen, file_info, view_failure};
use crate::access::{
    DELETE, FILE_APPEND_DATA, FILE_WRITE_ATTRIBUTES, FILE_WRITE_DATA, FILE_WRITE_EA, Rights,
};
use crate::backend::{
    BackendError, CreateAction, Credential, DirectoryEntry, FileInfo, ListPage, Lookup, ObjectKind,
    OpenGrant, OpenRequest, Scope, TreeGrant,
};

/// Rights an open of an immutable view may never hold.
const CHANGE_RIGHTS: u32 =
    FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_WRITE_ATTRIBUTES | FILE_WRITE_EA | DELETE;
const AFTER_MARK: char = 'a';
const HIDDEN_MARK: char = 'h';
const PENDING_MARK: char = 'p';

/// One request's reading of a run: its record, captured entries and their tree.
struct View {
    record: JobRecord,
    entries: Vec<ViewEntry>,
    tree: JobTree,
}

impl View {
    fn entry(&self, path: &str) -> Option<&ViewEntry> {
        self.entries.iter().find(|entry| entry.key == path)
    }

    /// The tree node of a share-relative path: the exact spelling first, then one fold.
    fn node(&self, path: &str) -> Option<&JobTreeItem> {
        let mut node = self.tree.item(JOB_ROOT)?;
        if path.is_empty() {
            return Some(node);
        }
        for part in path.split('/') {
            node = match self.tree.lookup(node.node_id, part) {
                Some(found) => found,
                None => {
                    let children: Vec<&JobTreeItem> = self
                        .tree
                        .children(node.node_id, |_: &JobEntry| true)
                        .collect();
                    let names = children.iter().map(|child| child.name.as_str());
                    let Resolved::Folded(name) = resolve_name(part, names, VfsProtocol::Smb) else {
                        return None;
                    };
                    children.into_iter().find(|child| child.name == name)?
                }
            };
        }
        Some(node)
    }
}

/// The tree entries of a run, built before any caller's visibility filter.
fn tree_entries(entries: &[ViewEntry]) -> Vec<JobEntry> {
    let entry = |view: &ViewEntry| JobEntry {
        path: view.key.clone(),
        identity: format!(
            "{}@{}",
            view.captured.destination_key, view.captured.version_id
        ),
        size: view.captured.bytes,
    };
    entries.iter().map(entry).collect()
}

fn job_kind(kind: JobNodeKind) -> ObjectKind {
    match kind {
        JobNodeKind::File => ObjectKind::File,
        JobNodeKind::Directory => ObjectKind::Directory,
    }
}

pub(super) fn view_cursor(from: &ViewFrom) -> String {
    match from {
        ViewFrom::After(marker) => format!("{AFTER_MARK}{marker}"),
        ViewFrom::Hidden { at, pending: None } => format!("{HIDDEN_MARK}{}:{at}", at.len()),
        ViewFrom::Hidden {
            at,
            pending: Some(pending),
        } => format!("{PENDING_MARK}{}:{at}{pending}", at.len()),
    }
}

pub(super) fn view_resume(cursor: &str) -> Result<ViewFrom, BackendError> {
    if let Some(marker) = cursor.strip_prefix(AFTER_MARK) {
        return Ok(ViewFrom::After(marker.to_string()));
    }
    let (pending, rest) = match (
        cursor.strip_prefix(HIDDEN_MARK),
        cursor.strip_prefix(PENDING_MARK),
    ) {
        (Some(rest), _) => (false, rest),
        (_, Some(rest)) => (true, rest),
        _ => return Err(BackendError::Invalid),
    };
    let (length, rest) = rest.split_once(':').ok_or(BackendError::Invalid)?;
    let length = length.parse::<usize>().map_err(|_| BackendError::Invalid)?;
    let (at, tail) = rest.split_at_checked(length).ok_or(BackendError::Invalid)?;
    Ok(ViewFrom::Hidden {
        at: at.to_string(),
        pending: pending.then(|| tail.to_string()),
    })
}

impl ArunaSmbBackend {
    /// The run behind an `in-<job_id>` share, only for that run's own attempt credential and
    /// only while the run is not finished.
    async fn view(&self, credential: &Credential, job: Ulid) -> Result<View, BackendError> {
        let job_id = JobId::from_bytes(job.to_bytes());
        if view_job(&credential.access_key, &view_bucket(job_id)) != Some(job_id) {
            return Err(BackendError::Denied);
        }
        let record = drive(read_job_record(job_id), &self.driver)
            .await
            .map_err(|error| BackendError::Internal(error.to_string()))?
            .ok_or(BackendError::NotFound)?;
        let entries = view_entries(&record).map_err(|_| BackendError::NotFound)?;
        // An ambiguous capture refuses the whole view rather than picking one of its readings.
        let tree = JobTree::build(&tree_entries(&entries)).map_err(|_| BackendError::Denied)?;
        Ok(View {
            record,
            entries,
            tree,
        })
    }

    fn view_auth(&self, record: &JobRecord) -> AuthContext {
        creator(
            record,
            self.driver.serving_realm(record.created_by.realm_id),
        )
    }

    /// Whether the run's creator may still read `entry` here; another node's copy is not served.
    async fn shows(&self, auth: &AuthContext, entry: &ViewEntry) -> Result<bool, BackendError> {
        let head = ViewEntryOperation::head(auth.clone(), entry.clone(), self.node_id);
        let Some(head) = head.local_only() else {
            return Ok(false);
        };
        match drive(head, &self.driver).await {
            Ok(_) => Ok(true),
            Err(ViewError::NotFound | ViewError::Denied) => Ok(false),
            Err(error) => Err(view_failure(error)),
        }
    }

    /// A file shows when its entry shows; a directory once any file below it shows.
    async fn node_shows(&self, view: &View, node: &JobTreeItem) -> Result<bool, BackendError> {
        let auth = self.view_auth(&view.record);
        if node.kind == JobNodeKind::File {
            let entry = view.entry(&node.path).ok_or(BackendError::NotFound)?;
            return self.shows(&auth, entry).await;
        }
        let below = marker_key(&node.path);
        for entry in &view.entries {
            let inside = node.path.is_empty() || entry.key.starts_with(&below);
            if inside && self.shows(&auth, entry).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) async fn connect_view(
        &self,
        credential: &Credential,
        job: Ulid,
    ) -> Result<TreeGrant, BackendError> {
        let view = self.view(credential, job).await?;
        self.admit_live(credential).await?;
        let total: u64 = view.entries.iter().map(|entry| entry.captured.bytes).sum();
        Ok(TreeGrant {
            read_only: true,
            capacity: total,
            available: 0,
        })
    }

    pub(super) async fn lookup_view(
        &self,
        scope: &Scope,
        job: Ulid,
        path: &str,
    ) -> Result<Lookup, BackendError> {
        let view = self.view(&scope.credential, job).await?;
        let hidden = Lookup {
            kind: None,
            rights: Rights::default(),
        };
        let Some(node) = view.node(path) else {
            return Ok(hidden);
        };
        if !self.node_shows(&view, node).await? {
            return Ok(hidden);
        }
        Ok(Lookup {
            kind: Some(job_kind(node.kind)),
            rights: Rights {
                read: true,
                write: false,
                delete: false,
            },
        })
    }

    pub(super) async fn open_view(
        &self,
        scope: &Scope,
        job: Ulid,
        request: OpenRequest,
    ) -> Result<OpenGrant, BackendError> {
        let changes = request.granted & CHANGE_RIGHTS != 0 || request.delete_on_close;
        if changes || request.disposition.truncates() {
            return Err(BackendError::ReadOnly);
        }
        let view = self.view(&scope.credential, job).await?;
        let found = view.node(&request.path);
        let shown = match found {
            Some(node) => self.node_shows(&view, node).await?,
            None => false,
        };
        let node = match found {
            Some(node) if shown => node,
            _ if request.disposition.creates() => return Err(BackendError::ReadOnly),
            _ => return Err(BackendError::NotFound),
        };
        if request.disposition == crate::backend::Disposition::Create {
            return Err(BackendError::ReadOnly);
        }
        let open = ViewOpen {
            access_key: scope.credential.access_key.clone(),
            job,
            path: node.path.clone(),
            kind: job_kind(node.kind),
        };
        let info = self.entry_info(&view, &open)?;
        let handle = self.store(Entry::Job(open));
        Ok(OpenGrant {
            handle,
            action: CreateAction::Opened,
            info,
        })
    }

    fn entry_info(&self, view: &View, open: &ViewOpen) -> Result<FileInfo, BackendError> {
        let created = view.record.created_at_ms;
        match open.kind {
            ObjectKind::Directory => Ok(file_info(ObjectKind::Directory, 0, created, false)),
            ObjectKind::File => {
                let entry = view.entry(&open.path).ok_or(BackendError::NotFound)?;
                let size = entry.captured.bytes;
                Ok(file_info(ObjectKind::File, size, created, false))
            }
        }
    }

    pub(super) async fn view_info(
        &self,
        scope: &Scope,
        open: &ViewOpen,
    ) -> Result<FileInfo, BackendError> {
        let view = self.view(&scope.credential, open.job).await?;
        self.entry_info(&view, open)
    }

    /// Reads a range of the exact captured version stored on this node, never a current head,
    /// another attempt's input or a remote copy.
    pub(super) async fn read_view(
        &self,
        scope: &Scope,
        open: &ViewOpen,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, BackendError> {
        if open.kind == ObjectKind::Directory {
            return Err(BackendError::IsDirectory);
        }
        let view = self.view(&scope.credential, open.job).await?;
        let entry = view.entry(&open.path).ok_or(BackendError::NotFound)?;
        let size = entry.captured.bytes;
        let end = offset.saturating_add(u64::from(length)).min(size);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let auth = self.view_auth(&view.record);
        let read = ViewEntryOperation::read(auth, entry.clone(), self.node_id, Some(offset..end));
        let read = read.local_only().ok_or(BackendError::Unsupported)?;
        let stream = drive(read, &self.driver).await.map_err(view_failure)?;
        let mut stream = stream.ok_or(BackendError::NotFound)?;
        let wanted = usize::try_from(end - offset).map_err(|_| BackendError::Invalid)?;
        let mut data = BytesMut::with_capacity(wanted);
        while data.len() < wanted {
            let Some(chunk) = stream.next().await else {
                break;
            };
            let chunk = chunk.map_err(|error| BackendError::Internal(error.to_string()))?;
            data.extend_from_slice(&chunk);
        }
        data.truncate(wanted);
        Ok(data.freeze())
    }

    /// One page of a view directory: names the caller may not read add nothing.
    pub(super) async fn list_view(
        &self,
        scope: &Scope,
        open: &ViewOpen,
        cursor: Option<String>,
    ) -> Result<ListPage, BackendError> {
        if open.kind != ObjectKind::Directory {
            return Err(BackendError::NotDirectory);
        }
        let view = self.view(&scope.credential, open.job).await?;
        let auth = self.view_auth(&view.record);
        let prefix = match open.path.is_empty() {
            true => String::new(),
            false => marker_key(&open.path),
        };
        let from = cursor.as_deref().map(view_resume).transpose()?;
        let slice = (prefix.as_str(), Some("/"));
        let mut lister = ViewLister::new(&view.entries, slice, from, MAX_VIEW_KEYS);
        while let Some(candidate) = lister.candidate() {
            let shown = self.shows(&auth, candidate).await?;
            lister.answer(shown);
        }
        let listing = lister.finish();
        let at_root = open.path.is_empty();
        let created = view.record.created_at_ms;
        let mut entries = Vec::new();
        for index in listing.objects {
            let entry = view.entries.get(index).ok_or(BackendError::NotFound)?;
            let Some(name) = entry.key.strip_prefix(&prefix) else {
                continue;
            };
            if representable(name, VfsProtocol::Smb, at_root) {
                let info = file_info(ObjectKind::File, entry.captured.bytes, created, false);
                let name = name.to_string();
                entries.push(DirectoryEntry { name, info });
            }
        }
        for group in listing.prefixes {
            let name = group
                .strip_prefix(&prefix)
                .and_then(|rest| rest.strip_suffix('/'));
            if let Some(name) = name.filter(|name| representable(name, VfsProtocol::Smb, at_root)) {
                let info = file_info(ObjectKind::Directory, 0, created, false);
                let name = name.to_string();
                entries.push(DirectoryEntry { name, info });
            }
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(ListPage {
            entries,
            next: listing.next.as_ref().map(view_cursor),
        })
    }
}
