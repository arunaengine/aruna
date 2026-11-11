//! Keeps the working state of one provenance export on disk: the ids read, the entities, the
//! root's parts and the links still to read. The store deletes itself when dropped.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
#[cfg(unix)]
use {
    std::fs::DirBuilder,
    std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
};

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use serde_json::{Map, Value, json};
use ulid::Ulid;

use super::{Hop, Target};

/// Properties a link may name; a stored link is read back to one of these.
const PROPERTIES: [&str; 6] = [
    "mainEntity",
    "isBasedOn",
    "result",
    "object",
    "hasPart",
    "subjectOf",
];

pub(super) struct Spool {
    // Keeps the database open; it removes its folder when dropped.
    _db: Database,
    labels: Keyspace,
    entities: Keyspace,
    parts: Keyspace,
    queue: Keyspace,
    pushed: u64,
    #[cfg(test)]
    pub(super) path: std::path::PathBuf,
}

impl std::fmt::Debug for Spool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spool")
            .field("pushed", &self.pushed)
            .finish()
    }
}

pub(super) type SpoolResult<T> = Result<T, fjall::Error>;

impl Spool {
    pub(super) fn open() -> SpoolResult<Self> {
        let path = scratch_dir()
            .map_err(fjall::Error::Io)?
            .join(scratch_name());
        let db = Database::builder(&path).temporary(true).open()?;
        let open = |name: &str| db.keyspace(name, KeyspaceCreateOptions::default);
        Ok(Self {
            labels: open("labels")?,
            entities: open("entities")?,
            parts: open("parts")?,
            queue: open("queue")?,
            _db: db,
            pushed: 0,
            #[cfg(test)]
            path,
        })
    }

    pub(super) fn label(&self, asked: &str) -> SpoolResult<Option<String>> {
        let value = self.labels.get(asked)?;
        Ok(value.map(|value| String::from_utf8_lossy(&value).into_owned()))
    }

    pub(super) fn set_label(&self, asked: &str, id: &str) -> SpoolResult<()> {
        self.labels.insert(asked, id)
    }

    pub(super) fn entity(&self, id: &str) -> SpoolResult<Option<Map<String, Value>>> {
        let value = self.entities.get(id)?;
        Ok(value.and_then(|value| serde_json::from_slice(&value).ok()))
    }

    pub(super) fn put_entity(&self, id: &str, entity: &Map<String, Value>) -> SpoolResult<()> {
        let bytes = serde_json::to_vec(entity).unwrap_or_default();
        self.entities.insert(id, bytes)
    }

    pub(super) fn add_part(&self, id: &str) -> SpoolResult<()> {
        self.parts.insert(id, [])
    }

    /// Queues a link behind every one queued before.
    pub(super) fn push(&mut self, hop: &Hop) -> SpoolResult<()> {
        let (kind, id) = match &hop.target {
            Target::Version(id) => ("version", id),
            Target::Run(id) => ("run", id),
            Target::Release(id) => ("release", id),
        };
        let record = json!({
            "kind": kind, "id": id, "from": hop.from, "property": hop.property, "digest": hop.digest,
        });
        self.pushed += 1;
        let bytes = serde_json::to_vec(&record).unwrap_or_default();
        self.queue.insert(self.pushed.to_be_bytes(), bytes)
    }

    /// The oldest queued link, removed from the queue.
    pub(super) fn pop(&mut self) -> SpoolResult<Option<Hop>> {
        let Some(first) = self.queue.first_key_value() else {
            return Ok(None);
        };
        let (key, value) = first.into_inner()?;
        self.queue.remove(key)?;
        let record: Value = serde_json::from_slice(&value).unwrap_or_default();
        let id = record["id"].as_str().unwrap_or_default().to_string();
        let target = match record["kind"].as_str() {
            Some("run") => Target::Run(id),
            Some("release") => Target::Release(id),
            _ => Target::Version(id),
        };
        let named = record["property"].as_str().unwrap_or_default();
        let property = PROPERTIES.into_iter().find(|known| *known == named);
        Ok(Some(Hop {
            target,
            from: record["from"].as_str().unwrap_or_default().to_string(),
            property: property.unwrap_or("isBasedOn"),
            digest: record["digest"].as_str().map(str::to_string),
        }))
    }

    pub(super) fn parts(&self) -> impl Iterator<Item = SpoolResult<String>> + '_ {
        self.parts.iter().map(|guard| {
            let key = guard.key()?;
            Ok(String::from_utf8_lossy(&key).into_owned())
        })
    }

    pub(super) fn entities(&self) -> impl Iterator<Item = SpoolResult<Value>> + '_ {
        self.entities.iter().map(|guard| {
            let (_, value) = guard.into_inner()?;
            Ok(serde_json::from_slice(&value).unwrap_or_default())
        })
    }
}

/// The private folder of export working state: this user's own, readable by nobody else.
/// Entries that crashed exports left behind are removed first.
#[cfg(unix)]
pub fn scratch_dir() -> io::Result<PathBuf> {
    let uid = rustix::process::geteuid().as_raw();
    let dir = std::env::temp_dir().join(format!("aruna-provenance-{uid}"));
    match DirBuilder::new().mode(0o700).create(&dir) {
        Err(error) if error.kind() != io::ErrorKind::AlreadyExists => return Err(error),
        _ => {}
    }
    // A folder of this name made by someone else, or a link, is never used.
    let found = std::fs::symlink_metadata(&dir)?;
    if !found.is_dir() || found.uid() != uid {
        return Err(io::Error::other(
            "the export scratch folder belongs to someone else",
        ));
    }
    if found.mode() & 0o077 != 0 {
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    }
    reclaim(&dir);
    Ok(dir)
}

/// The temporary folder is already private to its user on Windows.
#[cfg(not(unix))]
pub fn scratch_dir() -> io::Result<PathBuf> {
    let dir = std::env::temp_dir().join("aruna-provenance");
    match std::fs::create_dir(&dir) {
        Err(error) if error.kind() != io::ErrorKind::AlreadyExists => return Err(error),
        _ => {}
    }
    // A link of this name is never used.
    if !std::fs::symlink_metadata(&dir)?.is_dir() {
        return Err(io::Error::other(
            "the export scratch folder is not a folder",
        ));
    }
    reclaim(&dir);
    Ok(dir)
}

/// A new entry name of this process: `<pid>-<ulid>`, which tells whose it is.
pub fn scratch_name() -> String {
    format!("{}-{}", std::process::id(), Ulid::generate())
}

/// Creates a new scratch file only this user may read.
pub fn scratch_file(path: &std::path::Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    options.open(path)
}

/// Removes entries of processes that no longer run; anything else in the folder stays.
fn reclaim(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let stem = name.strip_suffix(".json").unwrap_or(&name);
        let Some((pid, id)) = stem.split_once('-') else {
            continue;
        };
        let Ok(pid) = pid.parse::<i32>() else {
            continue;
        };
        if Ulid::from_string(id).is_err() || pid == std::process::id() as i32 || alive(pid) {
            continue;
        }
        let path = entry.path();
        let _ = match entry.file_type() {
            Ok(kind) if kind.is_dir() => std::fs::remove_dir_all(&path),
            _ => std::fs::remove_file(&path),
        };
    }
}

/// Whether a process of this id runs; one that cannot be asked counts as running.
#[cfg(unix)]
fn alive(pid: i32) -> bool {
    match rustix::process::Pid::from_raw(pid) {
        Some(pid) => !matches!(
            rustix::process::test_kill_process(pid),
            Err(rustix::io::Errno::SRCH)
        ),
        None => true,
    }
}

#[cfg(not(unix))]
fn alive(_pid: i32) -> bool {
    true
}
