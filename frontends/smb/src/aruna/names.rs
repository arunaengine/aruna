//! Resolves share-relative SMB paths to exact keys over the caller's visible listing: the exact
//! spelling first, otherwise the one case-insensitive match in each directory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::{DirectoryWitness, HandleKind};
use aruna_core::vfs_effects::VfsProtocol;
use aruna_ops_data::vfs::handles::{ListNext, ListRequest, ListResume, VfsEntry, VfsListing};
use aruna_ops_data::vfs::path::{
    NameUse, Resolved, check_component, child_key, fold_case, resolve_name,
};
use aruna_ops_data::vfs::{VfsAccessContext, VfsListOperation};
use aruna_runner::driver::drive;

use super::{ArunaSmbBackend, vfs_failure};
use crate::backend::BackendError;

/// Shown names one listing slice asks for.
pub(super) const LIST_PAGE: usize = 512;
const AFTER_MARK: char = 'a';
const SEALED_MARK: char = 's';

/// An existing object at its exact key.
#[derive(Clone, Debug)]
pub(super) struct Found {
    pub key: String,
    pub entry: VfsEntry,
}

/// What a path names: an existing object, or the exact key a new one would take.
#[derive(Clone, Debug)]
pub(super) enum Resolution {
    Found(Found),
    Missing(String),
}

/// The cursor text of where the next listing slice starts; `None` at the end.
pub(super) fn cursor_of(next: &ListNext) -> Option<String> {
    match next {
        ListNext::End => None,
        ListNext::After(name) => Some(format!("{AFTER_MARK}{name}")),
        ListNext::Sealed(sealed) => Some(format!("{SEALED_MARK}{sealed}")),
    }
}

/// Where a cursor this backend issued resumes; any other text is refused.
pub(super) fn resume_of(cursor: Option<&str>) -> Result<ListResume, BackendError> {
    let Some(cursor) = cursor else {
        return Ok(ListResume::Start);
    };
    if let Some(name) = cursor.strip_prefix(AFTER_MARK) {
        return Ok(ListResume::After(name.to_string()));
    }
    let sealed = cursor.strip_prefix(SEALED_MARK);
    let sealed = sealed.and_then(aruna_core::cursor::position_bytes);
    sealed.map(ListResume::Sealed).ok_or(BackendError::Invalid)
}

fn root_entry() -> VfsEntry {
    VfsEntry {
        name: String::new(),
        kind: HandleKind::Directory,
        size: 0,
        version_id: None,
        handle_id: None,
        staged: false,
    }
}

impl ArunaSmbBackend {
    /// One slice of a directory listing, admitted again for this request.
    pub(super) async fn list_slice(
        &self,
        context: &VfsAccessContext,
        directory: &str,
        witness: Option<DirectoryWitness>,
        cursor: Option<&str>,
    ) -> Result<VfsListing, BackendError> {
        let request = ListRequest {
            context: context.clone(),
            directory: directory.to_string(),
            witness,
            resume: resume_of(cursor)?,
            max_entries: LIST_PAGE,
            deadline_ms: None,
        };
        let list = VfsListOperation::new(request);
        drive(list, &self.driver).await.map_err(vfs_failure)
    }

    /// The visible child of `directory` that `wanted` names. Folding needs the whole
    /// directory, since several names may fold alike; an ambiguous fold names nothing.
    async fn find_child(
        &self,
        context: &VfsAccessContext,
        directory: &str,
        wanted: &str,
    ) -> Result<Option<VfsEntry>, BackendError> {
        let folded = fold_case(wanted);
        let mut alike = Vec::new();
        let mut cursor = None;
        loop {
            let listing = self
                .list_slice(context, directory, None, cursor.as_deref())
                .await?;
            for entry in listing.entries {
                if entry.name == wanted {
                    return Ok(Some(entry));
                }
                if fold_case(&entry.name) == folded {
                    alike.push(entry);
                }
            }
            cursor = cursor_of(&listing.next);
            if cursor.is_none() {
                break;
            }
        }
        let names = alike.iter().map(|entry| entry.name.as_str());
        match resolve_name(wanted, names, VfsProtocol::Smb) {
            Resolved::Exact(name) | Resolved::Folded(name) => {
                Ok(alike.into_iter().find(|entry| entry.name == name))
            }
            Resolved::Ambiguous | Resolved::Missing => Ok(None),
        }
    }

    /// Resolves every component in turn; a missing or file parent is a missing path.
    pub(super) async fn resolve(
        &self,
        context: &VfsAccessContext,
        path: &str,
    ) -> Result<Resolution, BackendError> {
        if path.is_empty() {
            return Ok(Resolution::Found(Found {
                key: String::new(),
                entry: root_entry(),
            }));
        }
        let parts: Vec<&str> = path.split('/').collect();
        let mut directory = String::new();
        for (index, part) in parts.iter().enumerate() {
            let at_root = directory.is_empty();
            check_component(part, VfsProtocol::Smb, NameUse::Lookup, at_root)
                .map_err(vfs_failure)?;
            let last = index + 1 == parts.len();
            match self.find_child(context, &directory, part).await? {
                Some(entry) if last => {
                    let key = child_key(&directory, &entry.name);
                    return Ok(Resolution::Found(Found { key, entry }));
                }
                Some(entry) if entry.kind == HandleKind::Directory => {
                    directory = child_key(&directory, &entry.name);
                }
                None if last => return Ok(Resolution::Missing(child_key(&directory, part))),
                _ => return Err(BackendError::PathNotFound),
            }
        }
        Err(BackendError::Invalid)
    }
}
