//! Projects S3 keys into file server names: component rules of both protocols, directory
//! projection, case resolution, export containment and the pinned job input tree.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::VfsError;
use aruna_core::structs::storage::blob::{HIDDEN_BLOB_PREFIX, MULTIPART_PART_PREFIX};
use aruna_core::structs::storage::vfs::NfsMountTarget;
use aruna_core::vfs_effects::VfsProtocol;
use std::collections::BTreeMap;

/// Most UTF-8 bytes of one NFS name component.
pub const NFS_NAME_MAX: usize = 255;
/// Most UTF-16 code units of one SMB name component.
pub const SMB_NAME_MAX: usize = 255;
/// The node id of a job input tree's root directory.
pub const JOB_ROOT: u64 = 1;

const SMB_FORBIDDEN: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
const DEVICE_NAMES: &[&str] = &["CON", "PRN", "AUX", "NUL"];
const NUMBERED_DEVICES: &[&str] = &["COM", "LPT"];

/// Whether a name is only looked up or is about to be created or renamed to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NameUse {
    Lookup,
    Create,
}

/// Checks one component of a path. Internal names are refused at the bucket or export root.
pub fn check_component(
    name: &str,
    protocol: VfsProtocol,
    usage: NameUse,
    at_root: bool,
) -> Result<(), VfsError> {
    let dots = name == "." || name == "..";
    let control = name.chars().any(|c| (c as u32) < 0x20 || c == '/');
    if name.is_empty() || dots || control {
        return Err(VfsError::InvalidName);
    }
    if at_root && (name == HIDDEN_BLOB_PREFIX || name == MULTIPART_PART_PREFIX) {
        return Err(VfsError::InvalidName);
    }
    match protocol {
        VfsProtocol::Nfs if name.len() > NFS_NAME_MAX => Err(VfsError::InvalidName),
        VfsProtocol::Nfs => Ok(()),
        VfsProtocol::Smb => smb_component(name, usage),
    }
}

/// The SMB rules on top of the shared ones: UTF-16 length, prohibited characters, device
/// names and, for a new name, no trailing dot or space.
fn smb_component(name: &str, usage: NameUse) -> Result<(), VfsError> {
    if name.encode_utf16().count() > SMB_NAME_MAX || name.contains(SMB_FORBIDDEN) {
        return Err(VfsError::InvalidName);
    }
    if device_name(name) {
        return Err(VfsError::InvalidName);
    }
    let trailing = name.ends_with('.') || name.ends_with(' ');
    match usage == NameUse::Create && trailing {
        true => Err(VfsError::InvalidName),
        false => Ok(()),
    }
}

/// A Windows device name, bare or with an extension, in any case: `con`, `COM1.txt`.
pub fn device_name(name: &str) -> bool {
    let base = name.split('.').next().unwrap_or(name).trim_end();
    let upper = base.to_ascii_uppercase();
    if DEVICE_NAMES.contains(&upper.as_str()) {
        return true;
    }
    NUMBERED_DEVICES.iter().any(|device| {
        upper
            .strip_prefix(device)
            .is_some_and(|digit| digit.len() == 1 && matches!(digit.as_bytes()[0], b'1'..=b'9'))
    })
}

/// Whether a listed component can be shown at all; others stay reachable only through S3.
pub fn representable(name: &str, protocol: VfsProtocol, at_root: bool) -> bool {
    check_component(name, protocol, NameUse::Lookup, at_root).is_ok()
}

/// The key of `name` inside directory `directory`, which is empty for the bucket root.
pub fn child_key(directory: &str, name: &str) -> String {
    match directory.is_empty() {
        true => name.to_string(),
        false => format!("{directory}/{name}"),
    }
}

/// The parent directory and last component of a key; the root has neither.
pub fn split_parent(key: &str) -> Option<(&str, &str)> {
    let key = key.strip_suffix('/').unwrap_or(key);
    if key.is_empty() {
        return None;
    }
    Some(key.rsplit_once('/').unwrap_or(("", key)))
}

/// The marker key that holds an empty directory: `d/`.
pub fn marker_key(directory: &str) -> String {
    format!("{directory}/")
}

/// The key prefix of every entry below `directory`; the bucket root lists all keys.
pub fn children_prefix(directory: &str) -> String {
    match directory.is_empty() {
        true => String::new(),
        false => marker_key(directory),
    }
}

/// Every ancestor directory of a key, nearest first, without the bucket root.
pub fn ancestors(key: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = key.strip_suffix('/').unwrap_or(key);
    while let Some((parent, _)) = rest.rsplit_once('/') {
        found.push(parent.to_string());
        rest = parent;
    }
    found
}

/// Whether `ancestor` is `key` itself or a directory above it, by whole components.
pub fn within(ancestor: &str, key: &str) -> bool {
    ancestor.is_empty() || key == ancestor || key.starts_with(&marker_key(ancestor))
}

/// Whether an export holds `key`, by whole components; `team/` never holds `team-old/a`.
pub fn export_holds(target: &NfsMountTarget, bucket: &str, key: &str) -> bool {
    match target {
        NfsMountTarget::Folder { .. } if !key.ends_with('/') => {
            target.contains(bucket, key) || target.contains(bucket, &marker_key(key))
        }
        _ => target.contains(bucket, key),
    }
}

/// What one name of a directory projects to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Projected {
    File,
    Directory,
    Missing,
}

/// What storage and the stage registry hold for one name.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NameFacts {
    /// A live head at the exact key.
    pub live_file: bool,
    /// A live `d/` marker.
    pub live_marker: bool,
    /// Any live key below `d/`.
    pub descendant: bool,
    /// An unpublished file staged at the exact key.
    pub staged_file: bool,
    /// A staged or unpublished entry below `d/`.
    pub staged_below: bool,
}

/// A marker or any live or staged descendant makes a directory, which shadows an exact file.
pub fn project_name(facts: NameFacts) -> Projected {
    if facts.live_marker || facts.descendant || facts.staged_below {
        return Projected::Directory;
    }
    match facts.live_file || facts.staged_file {
        true => Projected::File,
        false => Projected::Missing,
    }
}

/// How a requested name maps to the names that exist in its directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolved {
    /// The exact spelling exists.
    Exact(String),
    /// One name differs from the request only in case.
    Folded(String),
    /// Several names fold to the request; only their exact spellings resolve.
    Ambiguous,
    Missing,
}

/// The exact spelling first, otherwise the one case-insensitive match; NFS never folds case.
pub fn resolve_name<'a>(
    wanted: &str,
    names: impl IntoIterator<Item = &'a str>,
    protocol: VfsProtocol,
) -> Resolved {
    let folded = fold_case(wanted);
    let mut matches = Vec::new();
    for name in names {
        if name == wanted {
            return Resolved::Exact(name.to_string());
        }
        if protocol == VfsProtocol::Smb && fold_case(name) == folded {
            matches.push(name.to_string());
        }
    }
    matches.dedup();
    match matches.len() {
        0 => Resolved::Missing,
        1 => Resolved::Folded(matches.remove(0)),
        _ => Resolved::Ambiguous,
    }
}

/// The case-insensitive form SMB compares names in.
pub fn fold_case(name: &str) -> String {
    name.chars().flat_map(char::to_lowercase).collect()
}

/// Whether moving directory `source` to `target` would put it inside itself.
pub fn creates_cycle(source: &str, target: &str) -> bool {
    target != source && within(source, target)
}

/// One captured input of a job: its path in the view and the immutable identity it reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobEntry {
    pub path: String,
    /// The captured source and version, such as `bucket/key@version`.
    pub identity: String,
    pub size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobNodeKind {
    File,
    Directory,
}

/// One node of a job input tree; ids never depend on who looks at the tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobTreeItem {
    pub node_id: u64,
    pub parent: u64,
    pub name: String,
    pub path: String,
    pub kind: JobNodeKind,
    /// The captured entry of a file, by its index in the sorted entries.
    pub entry: Option<usize>,
}

/// The immutable tree of a job's captured inputs, built from the sorted neutral entry list
/// before any caller's visibility filter, so every caller sees the same handle ids.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobTree {
    entries: Vec<JobEntry>,
    items: Vec<JobTreeItem>,
    by_path: BTreeMap<String, usize>,
}

impl JobTree {
    /// Builds the tree; a path that is both a file and a directory, a path captured twice with
    /// different identities or an invalid component refuses the whole view.
    pub fn build(entries: &[JobEntry]) -> Result<Self, VfsError> {
        let mut sorted = entries.to_vec();
        sorted.sort_by(|left, right| left.path.cmp(&right.path));
        sorted.dedup();
        let mut tree = Self::default();
        tree.push_item(String::new(), String::new(), JobNodeKind::Directory, None)?;
        for (index, entry) in sorted.iter().enumerate() {
            tree.insert_entry(entry, index)?;
        }
        tree.entries = sorted;
        Ok(tree)
    }

    fn insert_entry(&mut self, entry: &JobEntry, index: usize) -> Result<(), VfsError> {
        let parts: Vec<&str> = entry.path.split('/').collect();
        for (depth, part) in parts.iter().enumerate() {
            check_component(part, VfsProtocol::Nfs, NameUse::Lookup, false)?;
            let path = parts[..=depth].join("/");
            let last = depth + 1 == parts.len();
            let kind = match last {
                true => JobNodeKind::File,
                false => JobNodeKind::Directory,
            };
            match self.by_path.get(&path).map(|at| &self.items[*at]) {
                // A file captured twice, or a file where a directory is, is ambiguous.
                Some(item) if item.kind != kind || last => return Err(VfsError::Conflict),
                Some(_) => {}
                None => {
                    let parent = parts[..depth].join("/");
                    let file = last.then_some(index);
                    self.push_item(parent, path, kind, file)?;
                }
            }
        }
        Ok(())
    }

    fn push_item(
        &mut self,
        parent: String,
        path: String,
        kind: JobNodeKind,
        entry: Option<usize>,
    ) -> Result<(), VfsError> {
        let node_id = JOB_ROOT + self.items.len() as u64;
        let parent = match path.is_empty() {
            true => 0,
            false => self.node_of(&parent).ok_or(VfsError::Conflict)?,
        };
        let name = path.rsplit('/').next().unwrap_or_default().to_string();
        self.by_path.insert(path.clone(), self.items.len());
        self.items.push(JobTreeItem {
            node_id,
            parent,
            name,
            path,
            kind,
            entry,
        });
        Ok(())
    }

    fn node_of(&self, path: &str) -> Option<u64> {
        self.by_path.get(path).map(|at| self.items[*at].node_id)
    }

    pub fn item(&self, node_id: u64) -> Option<&JobTreeItem> {
        let index = node_id.checked_sub(JOB_ROOT)?;
        self.items.get(usize::try_from(index).ok()?)
    }

    pub fn entry(&self, item: &JobTreeItem) -> Option<&JobEntry> {
        item.entry.and_then(|index| self.entries.get(index))
    }

    /// The child `name` of directory `parent`, by exact spelling only.
    pub fn lookup(&self, parent: u64, name: &str) -> Option<&JobTreeItem> {
        let parent = self.item(parent)?;
        let path = child_key(&parent.path, name);
        self.by_path.get(&path).map(|at| &self.items[*at])
    }

    /// The children of `parent` in name order, after the caller's filter of captured entries.
    pub fn children<'a>(
        &'a self,
        parent: u64,
        shows: impl Fn(&JobEntry) -> bool + 'a,
    ) -> impl Iterator<Item = &'a JobTreeItem> + 'a {
        self.items
            .iter()
            .filter(move |item| item.parent == parent && item.node_id != JOB_ROOT)
            .filter(move |item| self.shows_item(item, &shows))
    }

    /// A file shows when its entry shows; a directory when any file below it shows.
    fn shows_item(&self, item: &JobTreeItem, shows: &impl Fn(&JobEntry) -> bool) -> bool {
        match item.kind {
            JobNodeKind::File => self.entry(item).is_some_and(shows),
            JobNodeKind::Directory => {
                let below = marker_key(&item.path);
                let files = self.entries.iter();
                files
                    .filter(|entry| entry.path.starts_with(&below))
                    .any(shows)
            }
        }
    }
}
