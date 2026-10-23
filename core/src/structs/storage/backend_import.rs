//! Defines backend import rules, the rows of imported objects and the import job records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::checksum::HASH_MD5;
use crate::structs::execution::job::JobId;
use crate::structs::identity::auth::AuthContext;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::blob::{BackendLocation, BackendRef};
use crate::structs::storage::group_backend::path_bucket;
use crate::types::{GroupId, Key};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use ulid::Ulid;

/// Failures one import run keeps for its report; later ones are only counted.
pub const MAX_IMPORT_ERRORS: usize = 100;

/// Write pattern of a rule that names none: a unique file per version, tagged like
/// [`tagged_path`].
pub const DEFAULT_PATTERN: &str = "{dir}{name}~{id}";

/// Write pattern of a provider bucket linked to a bucket: the bucket key, unchanged.
pub const PLAIN_PATTERN: &str = "{dir}{name}";

/// Bytes one provider copy part takes at most; the provider limit is 5 GiB.
pub const COPY_PART: u64 = 1 << 30;

/// Whether a provider copy of `source` without a provider version or recorded ETag still
/// proves its bytes: one part, whose ETag must be the stored MD5.
pub fn copy_provable(source: &BackendLocation) -> bool {
    source.blob_size <= COPY_PART && source.hashes.contains_key(HASH_MD5)
}

/// Registers the objects under `backend_prefix` of a group backend in `bucket` under
/// `key_prefix`. The bytes stay where they are and Aruna never deletes them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendImport {
    pub import_id: Ulid,
    pub group_id: GroupId,
    pub backend_id: Ulid,
    pub backend_prefix: String,
    pub bucket: String,
    pub key_prefix: String,
    /// Cron expression evaluated in UTC; `None` runs only on request.
    pub schedule: Option<String>,
    pub created_by: UserId,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub last_job: Option<JobId>,
    pub last_started_ms: Option<u64>,
    /// Group backend that new writes go to; `None` is `backend_id`.
    pub write_backend_id: Option<Ulid>,
    /// Folder that new writes go to; `None` is `backend_prefix`.
    pub write_prefix: Option<String>,
    /// Path of a new write below the write folder, see [`expand_pattern`].
    pub write_pattern: String,
}

impl BackendImport {
    pub fn key(&self) -> Key {
        import_key(self.group_id, self.import_id)
    }

    /// The bucket key of a backend key; `None` outside `backend_prefix`.
    pub fn aruna_key(&self, backend_key: &str) -> Option<String> {
        let rest = backend_key.strip_prefix(&self.backend_prefix)?;
        Some(format!("{}{rest}", self.key_prefix))
    }

    pub fn write_backend(&self) -> Ulid {
        self.write_backend_id.unwrap_or(self.backend_id)
    }

    pub fn write_folder(&self) -> &str {
        self.write_prefix.as_deref().unwrap_or(&self.backend_prefix)
    }

    /// Whether this rule reads or writes `bucket` on `backend_id`, a storage without a bucket.
    pub fn uses_bucket(&self, backend_id: Ulid, bucket: &str) -> bool {
        let names = |folder: &str| path_bucket(folder) == Some(bucket);
        (self.backend_id == backend_id && names(&self.backend_prefix))
            || (self.write_backend() == backend_id && names(self.write_folder()))
    }

    /// The provider bucket this rule links whole to its whole bucket, with plain keys.
    pub fn mapped_bucket(&self) -> Option<&str> {
        let bucket = path_bucket(&self.backend_prefix)?;
        (self.key_prefix.is_empty()
            && self.backend_prefix.len() == bucket.len() + 1
            && self.write_backend() == self.backend_id
            && self.write_folder() == self.backend_prefix
            && self.write_pattern == PLAIN_PATTERN)
            .then_some(bucket)
    }

    /// Without `{id}` a write replaces the file of the same name.
    pub fn same_name(&self) -> bool {
        !self.write_pattern.contains("{id}")
    }

    /// The backend path of a write of `key` as version `id`; `None` outside `key_prefix`,
    /// for a key without a file name except a folder key, and for a path that is not plain.
    pub fn plain_path(&self, key: &str, id: Ulid) -> Option<String> {
        let rest = key.strip_prefix(&self.key_prefix)?;
        let (dir, name) = match rest.rfind('/') {
            Some(slash) => rest.split_at(slash + 1),
            None => ("", rest),
        };
        // A folder key, like `reports/`, keeps its exact name under the plain pattern only.
        if name.is_empty() {
            let path = format!("{}{rest}", self.write_folder());
            path.strip_suffix('/').filter(|folder| plain_safe(folder))?;
            let plain = self.write_pattern == PLAIN_PATTERN && !rest.is_empty();
            return plain.then_some(path);
        }
        let expanded = expand_pattern(&self.write_pattern, dir, name, id)?;
        let path = format!("{}{expanded}", self.write_folder());
        plain_safe(&path).then_some(path)
    }

    /// What a backend path of this rule's folder says about the write that made it. Only a
    /// unique decoding that expands back to exactly `path` counts.
    pub fn decode_path(&self, path: &str) -> PathIdentity {
        let tokens = pattern_tokens(&self.write_pattern);
        let rest = path.strip_prefix(self.write_folder());
        let (Some(tokens), Some(rest)) = (tokens, rest) else {
            return PathIdentity::Unmatched;
        };
        let has_dir = tokens.contains(&Token::Dir);
        if self.write_backend() != self.backend_id || !tokens.contains(&Token::Id) {
            return PathIdentity::Unmatched;
        }
        let mut found = Vec::new();
        match_tokens(&tokens, rest, Parts::default(), &mut found);
        let mut decoded: Vec<PathIdentity> = found
            .into_iter()
            .filter(|(dir, ..)| dir.is_empty() || plain_safe(&dir[..dir.len() - 1]))
            // A move tags a taken name with an attempt id, which is no version id.
            .filter(|(_, name, _)| !ends_in_tag(name))
            .filter_map(|(dir, name, id)| {
                let key = has_dir.then(|| format!("{}{dir}{name}", self.key_prefix));
                let expanded = match &key {
                    Some(key) => self.plain_path(key, id),
                    None => expand_pattern(&self.write_pattern, "", name, id)
                        .map(|expanded| format!("{}{expanded}", self.write_folder()))
                        .filter(|expanded| plain_safe(expanded)),
                };
                (expanded.as_deref() == Some(path)).then(|| PathIdentity::Decoded {
                    key,
                    name: name.to_string(),
                    id,
                })
            })
            .collect();
        decoded.dedup();
        match decoded.len() {
            0 => PathIdentity::Unmatched,
            1 => decoded.remove(0),
            _ => PathIdentity::Ambiguous,
        }
    }

    /// Like [`Self::decode_path`], also with the rule's `earlier` patterns; different readings
    /// of the current and earlier patterns are ambiguous.
    pub fn decode_any(&self, path: &str, earlier: &[String]) -> PathIdentity {
        let mut found = Vec::new();
        let current = self.decode_path(path);
        if current != PathIdentity::Unmatched {
            found.push(current);
        }
        for pattern in earlier {
            let rule = BackendImport {
                write_pattern: pattern.clone(),
                ..self.clone()
            };
            let decoded = rule.decode_path(path);
            if decoded != PathIdentity::Unmatched && !found.contains(&decoded) {
                found.push(decoded);
            }
        }
        match found.len() {
            0 => PathIdentity::Unmatched,
            1 => found.remove(0),
            _ => PathIdentity::Ambiguous,
        }
    }

    /// The current name of the file at `path` that an `earlier` pattern named for version `id`
    /// of `key`; `None` for any other file, and for a file that has its current name.
    pub fn rename_target(
        &self,
        earlier: &[String],
        path: &str,
        key: &str,
        id: Ulid,
    ) -> Option<String> {
        let target = self.plain_path(key, id).filter(|target| target != path)?;
        match self.decode_any(path, earlier) {
            PathIdentity::Decoded {
                key: Some(found),
                id: found_id,
                ..
            } if found == key && found_id == id => Some(target),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The earlier patterns after `old` is replaced by `new`, newest first. Only a pattern with
/// `{id}` tells a version, so only such a pattern is kept.
pub fn retire_pattern(earlier: &[String], old: &str, new: &str) -> Vec<String> {
    let kept = earlier
        .iter()
        .filter(|pattern| *pattern != old && *pattern != new);
    let old = (old != new && old.contains("{id}")).then(|| old.to_string());
    old.into_iter().chain(kept.cloned()).collect()
}

/// The stored earlier patterns of a rule; none without a record.
pub fn read_patterns(stored: Option<&[u8]>) -> Result<Vec<String>, ConversionError> {
    Ok(stored
        .map(postcard::from_bytes)
        .transpose()?
        .unwrap_or_default())
}

pub fn write_patterns(earlier: &[String]) -> Result<Vec<u8>, ConversionError> {
    Ok(postcard::to_allocvec(earlier)?)
}

/// The identity a backend path encodes under its rule's write pattern.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PathIdentity {
    /// The path is no write of the pattern, or the pattern encodes no version id.
    Unmatched,
    /// More than one key or version id expands to the path.
    Ambiguous,
    /// `key` is `None` when the pattern drops the folder of the key, which the path cannot tell.
    Decoded {
        key: Option<String>,
        name: String,
        id: Ulid,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Token<'a> {
    Text(&'a str),
    Dir,
    Name,
    Id,
}

/// The parts of a pattern; `None` for an invalid brace or a placeholder used twice.
fn pattern_tokens(pattern: &str) -> Option<Vec<Token<'_>>> {
    let mut tokens = Vec::new();
    let mut rest = pattern;
    while let Some(start) = rest.find(['{', '}']) {
        if start > 0 {
            tokens.push(Token::Text(&rest[..start]));
        }
        let tail = &rest[start..];
        let (token, length) = if tail.starts_with("{dir}") {
            (Token::Dir, 5)
        } else if tail.starts_with("{name}") {
            (Token::Name, 6)
        } else if tail.starts_with("{id}") {
            (Token::Id, 4)
        } else {
            return None;
        };
        if tokens.contains(&token) {
            return None;
        }
        tokens.push(token);
        rest = &tail[length..];
    }
    if !rest.is_empty() {
        tokens.push(Token::Text(rest));
    }
    Some(tokens)
}

/// The folder, name and id one match has taken so far.
#[derive(Clone, Copy, Default)]
struct Parts<'a> {
    dir: &'a str,
    name: &'a str,
    id: Option<Ulid>,
}

/// Collects every way `path` splits into the tokens. A folder is empty or ends in `/`, a name
/// is a non-empty file name and an id is a ulid in its canonical form.
fn match_tokens<'a>(
    tokens: &[Token<'_>],
    path: &'a str,
    parts: Parts<'a>,
    found: &mut Vec<(&'a str, &'a str, Ulid)>,
) {
    let Some((token, rest)) = tokens.split_first() else {
        if let (true, Some(id)) = (path.is_empty(), parts.id) {
            found.push((parts.dir, parts.name, id));
        }
        return;
    };
    match token {
        Token::Text(text) => {
            if let Some(tail) = path.strip_prefix(text) {
                match_tokens(rest, tail, parts, found);
            }
        }
        Token::Id => {
            let Some((text, tail)) = path.split_at_checked(26) else {
                return;
            };
            let id = Ulid::from_string(text)
                .ok()
                .filter(|id| id.to_string() == text);
            if id.is_some() {
                match_tokens(rest, tail, Parts { id, ..parts }, found);
            }
        }
        Token::Dir => {
            let ends = std::iter::once(0).chain(path.match_indices('/').map(|(at, _)| at + 1));
            for end in ends {
                let dir = &path[..end];
                match_tokens(rest, &path[end..], Parts { dir, ..parts }, found);
            }
        }
        Token::Name => {
            let file = path.find('/').unwrap_or(path.len());
            let ends = path[..file].char_indices().map(|(at, _)| at).skip(1);
            for end in ends.chain([file]).filter(|end| *end > 0) {
                let name = &path[..end];
                match_tokens(rest, &path[end..], Parts { name, ..parts }, found);
            }
        }
    }
}

/// Replaces `{dir}`, `{name}` and `{id}` in one pass, so a name holding a placeholder stays
/// as it is. Any other brace is invalid.
pub fn expand_pattern(pattern: &str, dir: &str, name: &str, id: Ulid) -> Option<String> {
    let mut expanded = String::new();
    let mut rest = pattern;
    while let Some(start) = rest.find(['{', '}']) {
        expanded.push_str(&rest[..start]);
        let tail = &rest[start..];
        let (value, length) = if tail.starts_with("{dir}") {
            (dir.to_string(), 5)
        } else if tail.starts_with("{name}") {
            (name.to_string(), 6)
        } else if tail.starts_with("{id}") {
            (id.to_string(), 4)
        } else {
            return None;
        };
        expanded.push_str(&value);
        rest = &tail[length..];
    }
    expanded.push_str(rest);
    Some(expanded)
}

/// A pattern needs `{name}` and must give a plain relative path in and below the folder.
pub fn valid_pattern(pattern: &str) -> bool {
    pattern.contains("{name}")
        && ["", "dir/"].into_iter().all(|dir| {
            expand_pattern(pattern, dir, "name", Ulid::nil()).is_some_and(|path| plain_safe(&path))
        })
}

/// Relative, without empty, `.` or `..` parts and without control characters. OpenDAL
/// trims the ends of a path, so a path with outer whitespace would name another file.
fn plain_safe(path: &str) -> bool {
    path.trim() == path
        && path.split('/').all(|part| {
            !part.is_empty() && part != "." && part != ".." && !part.chars().any(char::is_control)
        })
}

/// Where a same-name write of version `id` goes when a file without versions holds `path`.
pub fn tagged_path(path: &str, id: Ulid) -> String {
    format!("{path}~{id}")
}

/// Whether `name` ends in a `~<id>` tag of [`tagged_path`].
fn ends_in_tag(name: &str) -> bool {
    name.rsplit_once('~')
        .is_some_and(|(_, tag)| Ulid::from_string(tag).is_ok_and(|id| id.to_string() == tag))
}

/// Rules are keyed by group first, so one group's rules are one prefix scan.
pub fn import_key(group_id: GroupId, import_id: Ulid) -> Key {
    let mut key = group_id.to_bytes().to_vec();
    key.extend_from_slice(&import_id.to_bytes());
    key.into()
}

pub fn object_key(import_id: Ulid, backend_key: &str) -> Key {
    let mut key = import_id.to_bytes().to_vec();
    key.extend_from_slice(backend_key.as_bytes());
    key.into()
}

/// Each write keeps its own intent; a path never holds a zero byte, so the prefix is exact.
pub fn intent_key(import_id: Ulid, backend_key: &str, version_id: Ulid) -> Key {
    let mut key = intent_prefix(import_id, backend_key).to_vec();
    key.extend_from_slice(&version_id.to_bytes());
    key.into()
}

/// The intents of every write to one backend key.
pub fn intent_prefix(import_id: Ulid, backend_key: &str) -> Key {
    let mut key = object_key(import_id, backend_key).to_vec();
    key.push(0);
    key.into()
}

/// Backend versions are keyed by the ulid of the location they belong to.
pub fn version_key(location: Ulid) -> Key {
    location.to_bytes().to_vec().into()
}

/// The claim of a delete of backend `version` of the file at `location`, keyed by the file
/// itself, so every location row of those bytes meets the same claim.
pub fn claim_key(
    location: &crate::structs::storage::blob::BackendLocation,
    version: &str,
) -> Result<Key, ConversionError> {
    let file = (
        &location.backend,
        &location.root,
        &location.storage_bucket,
        &location.backend_path,
        version,
    );
    Ok(postcard::to_allocvec(&file)?.into())
}

/// Whether the file name of `path` holds `id` in the id slot of Aruna's names, as
/// `{id}_{name}` or `{name}~{id}`; never a part of another name or a folder.
pub fn names_version(path: &str, id: Ulid) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    let id = id.to_string();
    let first = name
        .strip_prefix(&id)
        .and_then(|rest| rest.strip_prefix('_'));
    let last = name
        .strip_suffix(&id)
        .and_then(|rest| rest.strip_suffix('~'));
    first.or(last).is_some_and(|rest| !rest.is_empty())
}

pub fn object_prefix(import_id: Ulid) -> Key {
    import_id.to_bytes().to_vec().into()
}

/// A storage move asked to rename linked files: the move it belongs to and who asked. The move
/// renames only the files whose key that caller may still write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenameRequest {
    pub started_at_ms: u64,
    pub auth: AuthContext,
    pub node_id: NodeId,
    /// The bearer token the caller asked with, checked again before each page.
    pub grant: TokenGrant,
    /// The caller paused the rename; the move makes no progress until it resumes.
    pub paused: bool,
    /// When the caller stopped the rename for good; the move record then ends.
    pub aborted_at_ms: Option<u64>,
}

/// The non-secret identity of a bearer token: enough to tell later whether it was revoked,
/// cut off or expired, never enough to use it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenGrant {
    pub token_hash: String,
    pub issuer: RealmId,
    pub issued_at_secs: u64,
    pub expires_at_secs: u64,
    /// The linked login of another realm the token came through.
    pub via: Option<UserId>,
}

impl RenameRequest {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The files a rename planned to rename when it started, kept apart from its request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenameTotal {
    /// The start time of the move the rename belongs to.
    pub started_at_ms: u64,
    pub total: u64,
}

impl RenameTotal {
    /// The total, only for the move started at `started_at_ms`.
    pub fn for_move(&self, started_at_ms: u64) -> Option<u64> {
        (self.started_at_ms == started_at_ms).then_some(self.total)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// A write to a fixed path in the tenant's folders; `ulid` names its location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlainPath {
    pub path: String,
    pub ulid: Ulid,
    /// The version the write is for; an empty reservation of the path names it.
    pub version_id: Ulid,
    /// Fails the write with `PathTaken` if a file is already at the path.
    pub if_not_exists: bool,
    /// Only `path` itself may take the file: a storage that cannot create a file only while
    /// its name is free refuses the write instead of using the tagged path.
    pub exact: bool,
}

/// What a removal mark does after the rows: delete this provider bucket while it is empty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderCleanup {
    pub backend_id: Ulid,
    pub bucket: String,
}

/// The removal mark of an import rule: its bucket, which the settlement of its unfinished
/// moves names, and the provider bucket the removal deletes while it is empty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemovalMark {
    pub bucket: String,
    pub cleanup: Option<ProviderCleanup>,
}

impl RemovalMark {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(value: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(value)?)
    }
}

/// The identity a write to an import folder gets, stored before its bytes reach the path. It
/// outlives a version that never commits, so a later import registers the file as that version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriteIntent {
    pub key: String,
    pub version_id: Ulid,
    pub backend: BackendRef,
    pub created_by: UserId,
    /// The BLAKE3 hash of the bytes the write put at the path, recorded once it is done.
    /// Without it nothing proves that the file at the path holds this write's bytes.
    pub written: Option<[u8; 32]>,
    /// The start of the storage move that wrote the file, which owns the intent; an upload's
    /// intent has none.
    pub mover: Option<u64>,
}

impl WriteIntent {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// One object of a backend listing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExternalObject {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub modified_ms: Option<u64>,
}

/// What a run recorded for one backend key. It commits with the version it names, so a
/// failed import leaves the previous row in place.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImportedObject {
    pub version_id: Ulid,
    pub etag: Option<String>,
    pub size: u64,
    pub modified_ms: Option<u64>,
    /// The bucket key this backend key maps to; `None` on an older import row, which maps
    /// by the rule's folders.
    pub key: Option<String>,
}

impl ImportedObject {
    pub fn new(version_id: Ulid, object: &ExternalObject) -> Self {
        Self {
            version_id,
            etag: object.etag.clone(),
            size: object.size,
            modified_ms: object.modified_ms,
            key: None,
        }
    }

    /// A version Aruna wrote itself. Without the backend's ETag its first listing only records
    /// its ETag and time; with it, listings compare like any imported object.
    pub fn written(version_id: Ulid, key: String, size: u64, etag: Option<String>) -> Self {
        Self {
            version_id,
            etag,
            size,
            modified_ms: None,
            key: Some(key),
        }
    }

    /// Aruna wrote it and no listing has seen it since.
    pub fn unlisted(&self) -> bool {
        self.key.is_some() && self.etag.is_none() && self.modified_ms.is_none()
    }

    /// The first listing of Aruna's own write only records its ETag and time; `None` when
    /// the row is no such write or the listed object has another size.
    pub fn first_listing(&self, object: &ExternalObject) -> Option<Self> {
        (self.unlisted() && self.size == object.size).then(|| Self {
            etag: object.etag.clone(),
            modified_ms: object.modified_ms,
            ..self.clone()
        })
    }

    /// Without an ETag the modification time decides whether the object changed. Quotes
    /// around an ETag do not count, as write answers and listings differ there.
    pub fn matches(&self, object: &ExternalObject) -> bool {
        let unquoted = |etag: Option<&str>| etag.map(|etag| etag.trim_matches('"').to_string());
        // A row whose read showed no ETag records none; its modification time decides.
        let timed = self.etag.is_none() && self.modified_ms.is_some();
        self.size == object.size
            && (timed || unquoted(self.etag.as_deref()) == unquoted(object.etag.as_deref()))
            && ((object.etag.is_some() && !timed) || self.modified_ms == object.modified_ms)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// One run of an import rule. The rule is read when the run starts, so a removed
/// rule stops the run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendImportSpec {
    pub auth_context: AuthContext,
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub import_id: Ulid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImportFailure {
    pub key: String,
    pub error: String,
}

/// Progress of one run; `cursor` is the last backend key the run finished.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendImportCheckpoint {
    pub cursor: Option<String>,
    pub imported: u64,
    pub skipped: u64,
    pub failed: u64,
    pub errors: Vec<ImportFailure>,
}

impl BackendImportCheckpoint {
    pub fn fail(&mut self, key: &str, error: String) {
        self.failed = self.failed.saturating_add(1);
        if self.errors.len() < MAX_IMPORT_ERRORS {
            self.errors.push(ImportFailure {
                key: key.to_string(),
                error,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_id_slots() {
        // Only the whole slot of an id in the file name counts, never a part of a name.
        let id = Ulid::from_parts(5, 5);
        for path in [format!("raw/{id}_a.txt"), format!("raw/a.txt~{id}")] {
            assert!(names_version(&path, id), "{path}");
        }
        for path in [
            format!("raw/backup-{id}.txt"),
            format!("raw/{id}/a.txt"),
            format!("raw/{id}_"),
            format!("raw/x{id}_a.txt"),
            format!("raw/a.txt~{id}.bak"),
            format!("raw/{}_a.txt", Ulid::from_parts(6, 6)),
        ] {
            assert!(!names_version(&path, id), "{path}");
        }
    }

    fn object(etag: Option<&str>, modified_ms: Option<u64>) -> ExternalObject {
        ExternalObject {
            key: "raw/a.csv".to_string(),
            size: 4,
            etag: etag.map(str::to_string),
            modified_ms,
        }
    }

    fn rule(pattern: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::nil(),
            group_id: Ulid::nil(),
            backend_id: Ulid::nil(),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: "imported/".to_string(),
            schedule: None,
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: pattern.to_string(),
        }
    }

    #[test]
    fn maps_backend_keys() {
        let rule = rule(DEFAULT_PATTERN);
        assert_eq!(
            rule.aruna_key("raw/x/a.csv").as_deref(),
            Some("imported/x/a.csv")
        );
        assert_eq!(rule.aruna_key("other/a.csv"), None);
    }

    #[test]
    fn expands_write_patterns() {
        let id = Ulid::from_parts(1, 2);
        let path = |pattern: &str, key: &str| rule(pattern).plain_path(key, id);
        assert_eq!(
            path(DEFAULT_PATTERN, "imported/x/a.csv"),
            Some(format!("raw/x/a.csv~{id}"))
        );
        assert_eq!(
            path("{dir}{id}_{name}", "imported/x/a.csv"),
            Some(format!("raw/x/{id}_a.csv"))
        );
        assert_eq!(
            path("{dir}{id}/{name}", "imported/a.csv"),
            Some(format!("raw/{id}/a.csv"))
        );
        assert_eq!(
            path("{dir}{name}", "imported/x/y/a.csv").as_deref(),
            Some("raw/x/y/a.csv")
        );
        // A placeholder inside a name is kept as written.
        assert_eq!(
            path("{dir}{name}", "imported/{id}.csv").as_deref(),
            Some("raw/{id}.csv")
        );
        for key in [
            "imported/",
            "imported/x/",
            "other/a.csv",
            "imported/x//a.csv",
        ] {
            assert_eq!(path(DEFAULT_PATTERN, key), None, "{key}");
        }
        // Under the plain pattern a folder key keeps its exact name.
        assert_eq!(
            path(PLAIN_PATTERN, "imported/x/").as_deref(),
            Some("raw/x/")
        );
        for key in ["imported/", "imported/x//"] {
            assert_eq!(path(PLAIN_PATTERN, key), None, "{key}");
        }
        assert_eq!(path("{dir}{name}", "imported/../a.csv"), None);
        // A trimmed path would name another file.
        assert_eq!(path("{dir}{name}", "imported/a.csv "), None);
        assert!(!valid_pattern("{name} "));

        let mut moved = rule("{name}");
        moved.write_prefix = Some("new/".to_string());
        assert_eq!(
            moved.plain_path("imported/x/a.csv", id).as_deref(),
            Some("new/a.csv")
        );
        assert!(!rule(DEFAULT_PATTERN).same_name());
        assert!(moved.same_name());
    }

    #[test]
    fn decodes_written_paths() {
        let id = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let decoded = |key: Option<&str>, name: &str| PathIdentity::Decoded {
            key: key.map(str::to_string),
            name: name.to_string(),
            id,
        };
        for pattern in [DEFAULT_PATTERN, "{dir}{name}~{id}", "{dir}{id}/{name}"] {
            let rule = rule(pattern);
            for key in ["imported/a.csv", "imported/x/y/a_b.csv~1"] {
                let path = rule.plain_path(key, id).unwrap();
                let name = key.rsplit('/').next().unwrap();
                assert_eq!(rule.decode_path(&path), decoded(Some(key), name), "{path}");
            }
        }
        // Without `{dir}` the path keeps only the name, never the folder of the key.
        let path = format!("raw/{id}_a.csv");
        assert_eq!(
            rule("{id}_{name}").decode_path(&path),
            decoded(None, "a.csv")
        );
        let unmatched = [
            "raw/a.csv".to_string(),
            "other/x.csv".to_string(),
            format!("raw/{}_a.csv", id.to_string().to_lowercase()),
            format!("raw/x//{id}_a.csv"),
            format!("raw/{id}_"),
        ];
        for path in unmatched {
            let identity = rule(DEFAULT_PATTERN).decode_path(&path);
            assert_eq!(identity, PathIdentity::Unmatched, "{path}");
        }
        // A pattern without `{id}` names no version, and a write elsewhere is not this folder.
        let same = format!("raw/a.csv~{id}");
        assert_eq!(
            rule(PLAIN_PATTERN).decode_path(&same),
            PathIdentity::Unmatched
        );
        let mut moved = rule(DEFAULT_PATTERN);
        moved.write_backend_id = Some(Ulid::from_parts(9, 9));
        let path = format!("raw/a.csv~{id}");
        assert_eq!(moved.decode_path(&path), PathIdentity::Unmatched);
        // A moved copy of a taken name carries an attempt id after the version id.
        let other = Ulid::from_parts(3, 4);
        for (pattern, path) in [
            (DEFAULT_PATTERN, format!("raw/a.csv~{other}~{id}")),
            ("{dir}{id}_{name}", format!("raw/{id}_a.csv~{other}")),
        ] {
            assert_eq!(
                rule(pattern).decode_path(&path),
                PathIdentity::Unmatched,
                "{path}"
            );
        }
        // A placeholder used twice is not decoded.
        let path = format!("raw/a_{id}_a");
        let twice = rule("{name}_{id}_{name}").decode_path(&path);
        assert_eq!(twice, PathIdentity::Unmatched);
        // An id-like name stays part of the name.
        let other = Ulid::from_parts(3, 4);
        let path = format!("raw/{other}_{id}");
        let identity = rule("{name}_{id}").decode_path(&path);
        assert_eq!(identity, decoded(None, &other.to_string()));
        // Two keys expand to this path, so it names no single key.
        let path = format!("raw/axxb/{id}");
        let identity = rule("{name}x{dir}{id}").decode_path(&path);
        assert_eq!(identity, PathIdentity::Ambiguous);
    }

    #[test]
    fn decodes_earlier_patterns() {
        let id = Ulid::from_parts(1, 2);
        let prefix = "{dir}{id}_{name}".to_string();
        let decoded = PathIdentity::Decoded {
            key: Some("imported/a.csv".to_string()),
            name: "a.csv".to_string(),
            id,
        };
        // A file of the replaced pattern keeps its identity.
        let current = rule(DEFAULT_PATTERN);
        let old = format!("raw/{id}_a.csv");
        assert_eq!(current.decode_path(&old), PathIdentity::Unmatched);
        assert_eq!(
            current.decode_any(&old, std::slice::from_ref(&prefix)),
            decoded
        );
        let new = format!("raw/a.csv~{id}");
        assert_eq!(
            current.decode_any(&new, std::slice::from_ref(&prefix)),
            decoded
        );
        // Two earlier patterns that read one path differently name no single version.
        let path = format!("raw/{id}_{}", Ulid::from_parts(3, 4));
        let both = [
            "{dir}{id}_{name}".to_string(),
            "{dir}{name}_{id}".to_string(),
        ];
        assert_eq!(
            rule(PLAIN_PATTERN).decode_any(&path, &both),
            PathIdentity::Ambiguous
        );
        // The current pattern reads the path as another key and version: no guess.
        let current = rule("{dir}{name}_{id}");
        assert_eq!(
            current.decode_any(&path, &both[..1]),
            PathIdentity::Ambiguous
        );
    }

    #[test]
    fn finds_rename_targets() {
        let id = Ulid::from_parts(1, 2);
        let earlier = ["{dir}{id}_{name}".to_string()];
        let current = rule(DEFAULT_PATTERN);
        let target = |path: &str, key: &str, id| current.rename_target(&earlier, path, key, id);
        let old = format!("raw/x/{id}_a.csv");
        assert_eq!(
            target(&old, "imported/x/a.csv", id),
            Some(format!("raw/x/a.csv~{id}"))
        );
        // Only the version the old name tells is renamed; a current name stays.
        assert_eq!(
            target(&old, "imported/x/a.csv", Ulid::from_parts(3, 4)),
            None
        );
        assert_eq!(target(&old, "imported/x/b.csv", id), None);
        assert_eq!(
            target(&format!("raw/x/a.csv~{id}"), "imported/x/a.csv", id),
            None
        );
        // A plain or tagged name of a user's file names no version of an earlier pattern.
        assert_eq!(target("raw/x/a.csv", "imported/x/a.csv", id), None);
        let moved = format!("raw/x/a.csv~{}", Ulid::from_parts(5, 6));
        assert_eq!(target(&moved, "imported/x/a.csv", id), None);
    }

    #[test]
    fn retires_patterns() {
        let earlier = vec!["{dir}{name}_{id}".to_string()];
        let retired = retire_pattern(&earlier, "{dir}{id}_{name}", DEFAULT_PATTERN);
        assert_eq!(retired, ["{dir}{id}_{name}", "{dir}{name}_{id}"]);
        // A plain pattern tells no version; the current pattern is no earlier one.
        assert_eq!(
            retire_pattern(&retired, PLAIN_PATTERN, "{dir}{name}_{id}"),
            ["{dir}{id}_{name}"]
        );
        let stored = write_patterns(&retired).unwrap();
        assert_eq!(read_patterns(Some(&stored)).unwrap(), retired);
        assert!(read_patterns(None).unwrap().is_empty());
    }

    #[test]
    fn checks_write_patterns() {
        for pattern in [DEFAULT_PATTERN, "{dir}{id}/{name}", "{dir}{name}", "{name}"] {
            assert!(valid_pattern(pattern), "{pattern}");
        }
        for pattern in [
            "{dir}{id}",
            "{dir}/{name}",
            "{dir}{other}{name}",
            "../{name}",
            "{name}/",
        ] {
            assert!(!valid_pattern(pattern), "{pattern}");
        }
    }

    #[test]
    fn detects_changed_objects() {
        let row = ImportedObject::new(Ulid::nil(), &object(Some("e1"), Some(1)));
        assert!(row.matches(&object(Some("e1"), Some(2))));
        assert!(!row.matches(&object(Some("e2"), Some(1))));
        let untagged = ImportedObject::new(Ulid::nil(), &object(None, Some(1)));
        assert!(untagged.matches(&object(None, Some(1))));
        assert!(!untagged.matches(&object(None, Some(2))));
        // A row recorded without the listed ETag compares by time.
        assert!(untagged.matches(&object(Some("e1"), Some(1))));
        assert!(!untagged.matches(&object(Some("e1"), Some(2))));
    }

    #[test]
    fn notes_own_writes() {
        let written = ImportedObject::written(Ulid::nil(), "in/a.csv".to_string(), 4, None);
        let listed = written.first_listing(&object(Some("e1"), Some(5))).unwrap();
        assert_eq!(listed.key.as_deref(), Some("in/a.csv"));
        assert!(listed.matches(&object(Some("e1"), Some(5))));
        // Once listed, a later outside edit is a change like any other.
        assert_eq!(listed.first_listing(&object(Some("e2"), Some(6))), None);
        assert!(!listed.matches(&object(Some("e2"), Some(6))));
        // A different size means someone replaced the file before the first listing.
        let resized = ExternalObject {
            size: 9,
            ..object(Some("e1"), Some(5))
        };
        assert_eq!(written.first_listing(&resized), None);
        assert!(!ImportedObject::new(Ulid::nil(), &object(None, None)).unlisted());
        // A write that kept the backend's ETag compares by it, quotes aside.
        let tagged = ImportedObject::written(Ulid::nil(), "in/a.csv".into(), 4, Some("e1".into()));
        assert_eq!(tagged.first_listing(&object(Some("\"e1\""), Some(5))), None);
        assert!(tagged.matches(&object(Some("\"e1\""), Some(5))));
        assert!(!tagged.matches(&object(Some("\"e2\""), Some(5))));
    }

    #[test]
    fn finds_used_buckets() {
        let mut rule = rule(DEFAULT_PATTERN);
        rule.backend_prefix = "lab-raw/2024/".to_string();
        assert!(rule.uses_bucket(Ulid::nil(), "lab-raw"));
        assert!(!rule.uses_bucket(Ulid::nil(), "lab"));
        assert!(!rule.uses_bucket(Ulid::from_parts(1, 1), "lab-raw"));
        rule.write_prefix = Some("lab-new/".to_string());
        assert!(rule.uses_bucket(Ulid::nil(), "lab-new"));
    }

    #[test]
    fn caps_kept_errors() {
        let mut checkpoint = BackendImportCheckpoint::default();
        for _ in 0..=MAX_IMPORT_ERRORS {
            checkpoint.fail("key", "broken".to_string());
        }
        assert_eq!(checkpoint.failed, MAX_IMPORT_ERRORS as u64 + 1);
        assert_eq!(checkpoint.errors.len(), MAX_IMPORT_ERRORS);
    }
}
