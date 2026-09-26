//! Native repository bindings, LFS identities and Git adapter requests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::keyspaces::GIT_RECORD_KEYSPACE;
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::PlacementRef;
use crate::types::{GroupId, Key, KeySpace, Value};
use crate::{NodeId, UserId};
use bytes::Bytes;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// This node's own stored copies of Git packs and LFS content, keyed by document and SHA-256.
pub const LOCAL_OBJECTS: &str = "git_local_objects";
pub const STATUS: &str = "git_status";
pub const MAX_GIT_BYTES: usize = 64 * 1024 * 1024;
/// Upper bound for one replicated Git record.
pub const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;
/// Records not covered by a checkpoint; holders write a checkpoint well before this.
pub const MAX_RECORDS: usize = 1024;
pub const CHECKPOINT_AFTER: usize = 256;
pub const ZERO_OID: &str = "0000000000000000000000000000000000000000";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRepository {
    pub document_id: Ulid,
    pub group_id: Ulid,
    pub bucket: String,
    pub arc: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LfsObject {
    pub oid: String,
    pub size: u64,
}

impl LfsObject {
    pub fn valid(&self) -> bool {
        self.oid.len() == 64
            && self
                .oid
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }
}

pub struct GitRequest {
    pub repository: GitRepository,
    pub method: String,
    pub action: String,
    pub query: String,
    pub content_type: String,
    pub content_encoding: String,
    pub protocol: String,
    pub body: Bytes,
    pub token: String,
    pub lfs_url: String,
    pub metadata_url: String,
    /// Proves to the push endpoint that a call comes from this push's receive hook.
    pub push_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitSnapshot {
    pub document_id: Ulid,
    pub event_id: Ulid,
    pub occurred_at_ms: u64,
    pub jsonld: String,
    /// Aruna objects that File entities name, placed in the ARC as LFS pointers.
    pub objects: Vec<LinkedObject>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinkedObject {
    pub entity: String,
    pub object: StoredObject,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitStatus {
    pub event_id: Ulid,
    pub commit: Option<String>,
    pub error: Option<String>,
}

pub type Refs = std::collections::BTreeMap<String, String>;

/// One async lock per document, created on first use and dropped once nobody holds it.
#[derive(Debug, Default)]
pub struct DocumentLocks(
    std::sync::Mutex<std::collections::HashMap<Ulid, std::sync::Weak<tokio::sync::Mutex<()>>>>,
);

impl DocumentLocks {
    pub async fn lock(&self, document_id: Ulid) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks.get(&document_id).and_then(std::sync::Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
                    locks.insert(document_id, std::sync::Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }
}

/// One commit as read from the repository, without verifying its signature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitInfo {
    pub commit: String,
    pub parents: Vec<String>,
    pub author_name: String,
    pub author_email: String,
    pub committer_email: String,
    pub authored_at_s: i64,
    pub message: String,
    pub signed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileChangeKind {
    Added,
    Modified,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChange {
    pub path: String,
    pub change: FileChangeKind,
}

/// The result of merging a source commit into a target commit. No ref moves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    /// The target already contains the source.
    UpToDate,
    /// The target is an ancestor of the source, which becomes the new target.
    FastForward,
    Merged(String),
    /// Files both sides changed that no metadata merge can resolve.
    Conflicts(Vec<String>),
    /// The merged metadata cannot be represented as a valid ARC.
    Failed(String),
}

pub enum GitEffect {
    Initialize(Ulid),
    /// SHA-256 digests of the packs the local cache already holds.
    Imported(Ulid),
    /// Adds a verified pack's objects to the local cache under its SHA-256 digest.
    Import {
        document_id: Ulid,
        digest: String,
        pack: Bytes,
    },
    Refs(Ulid),
    /// Whether each first commit is an ancestor of the second; missing objects answer false.
    Ancestry {
        document_id: Ulid,
        pairs: Vec<(String, String)>,
    },
    /// Moves every local ref from `expected` to `target` in one transaction.
    SetRefs {
        document_id: Ulid,
        expected: Refs,
        target: Refs,
    },
    /// Packs the objects reachable from `include` but not from `exclude`.
    Pack {
        document_id: Ulid,
        include: Vec<String>,
        exclude: Vec<String>,
    },
    /// Builds signed snapshot commits on the given refs without moving any ref.
    Generate {
        snapshot: GitSnapshot,
        refs: Refs,
    },
    Export {
        document_id: Ulid,
        revision: String,
    },
    /// The commit a branch, tag or commit id names, if any.
    Resolve {
        document_id: Ulid,
        revision: String,
    },
    /// The best common ancestor of two commits, if any.
    MergeBase {
        document_id: Ulid,
        first: String,
        second: String,
    },
    /// Up to `limit` commits reachable from `revision` but not from `exclude`, newest first,
    /// after skipping `skip`.
    Log {
        document_id: Ulid,
        revision: String,
        exclude: Option<String>,
        skip: usize,
        limit: usize,
    },
    /// Files changed from `from` (or an empty tree) to `to`.
    Diff {
        document_id: Ulid,
        from: Option<String>,
        to: String,
    },
    /// Commits the snapshot's metadata on top of `head` without moving any ref.
    Edit {
        head: String,
        snapshot: GitSnapshot,
        message: String,
    },
    /// Merges `source` into `target` without moving any ref.
    Merge {
        document_id: Ulid,
        target: String,
        source: String,
        message: String,
    },
    /// Applies the metadata changed from `old` (or nothing) to `new` onto `graph` JSON-LD.
    MergeMetadata {
        document_id: Ulid,
        old: Option<String>,
        new: String,
        graph: String,
    },
    Http(Box<GitRequest>),
}

pub enum GitEvent {
    Initialized,
    Imported(std::collections::BTreeSet<String>),
    Refs(Refs),
    Ancestry(Vec<bool>),
    Packed(Bytes),
    /// The new `aruna` commit and, when main must follow, the new main commit.
    Generated {
        aruna: String,
        main: Option<String>,
    },
    /// The graph cannot be represented as a valid ARC yet; nothing was built.
    GenerateFailed(String),
    Exported(Bytes),
    Resolved(Option<String>),
    Log(Vec<CommitInfo>),
    Diff(Vec<FileChange>),
    /// The new commit, the unchanged head, or why the metadata cannot become an ARC.
    Edited(Result<String, String>),
    Merged(MergeOutcome),
    /// The merged JSON-LD, `None` when nothing changed, or why the merge failed.
    MetadataMerged(Result<Option<String>, String>),
    Response {
        status: u16,
        headers: Vec<(String, String)>,
        body: Bytes,
    },
}

/// One replicated Git fact of a metadata document. Every holder rebuilds its local
/// repository from these records, so the repository itself is a disposable cache.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRecord {
    pub event_id: Ulid,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub placement: PlacementRef,
    pub user_id: UserId,
    pub node_id: NodeId,
    pub occurred_at_ms: u64,
    pub change: GitChange,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitChange {
    /// A pack of new objects and the ref updates it makes. A server snapshot names the
    /// metadata event it represents in `revision` and the graph it captured in `digest`; a
    /// snapshot whose updates no longer match is dropped, since the server makes a new one.
    Objects {
        pack: Option<Box<StoredObject>>,
        refs: Vec<RefUpdate>,
        lfs: Vec<StoredObject>,
        revision: Option<Ulid>,
        digest: Option<[u8; 32]>,
        /// Commits the recording node made itself, whose Aruna trailers can be trusted.
        made: Vec<String>,
    },
    /// Claims an LFS lock; the earliest claim on a path wins.
    Lock {
        id: Ulid,
        path: String,
    },
    Unlock {
        id: Ulid,
    },
    /// The state after the records it and its previous checkpoints cover. Records the
    /// chain does not list still apply on top in order, so a late record is never lost.
    Checkpoint(Box<GitCheckpoint>),
}

/// Refs, locks and revision are complete; packs, LFS objects and covered records are those
/// added since `previous`, so a checkpoint stays small however long the history grows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitCheckpoint {
    pub previous: Option<Ulid>,
    pub packs: Vec<StoredObject>,
    /// Commits nodes made themselves since `previous`.
    pub made: Vec<String>,
    /// The graph digest of the newest applied snapshot.
    pub digest: Option<[u8; 32]>,
    pub refs: Vec<(String, String)>,
    pub lfs: Vec<StoredObject>,
    pub locks: Vec<LfsLock>,
    /// Claims that lost to a held lock and may still win if that lock was released earlier.
    pub waiting: Vec<LfsLock>,
    pub revision: Option<Ulid>,
    pub covered: Vec<Ulid>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LfsLock {
    pub id: Ulid,
    pub path: String,
    pub user_id: UserId,
    pub locked_at_ms: u64,
    /// The record that claimed the lock; the earliest claim on a path wins.
    pub claim: Ulid,
}

/// An exact Aruna object version, readable from its node through a routed get.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredObject {
    pub node_id: NodeId,
    /// The bucket's group when known; otherwise only the source node can check access.
    pub group_id: Option<GroupId>,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub size: u64,
    pub sha256: String,
    /// Lets any holder with a copy serve these bytes when the original node is gone.
    pub blake3: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefUpdate {
    pub name: String,
    pub old: String,
    pub new: String,
}

fn hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Accepts branch and tag names Git accepts. Only the server may write `aruna` and conflict refs.
pub fn valid_ref(name: &str, server: bool) -> bool {
    let Some(short) = name
        .strip_prefix("refs/heads/")
        .or_else(|| name.strip_prefix("refs/tags/"))
        .or_else(|| name.strip_prefix("refs/conflicts/").filter(|_| server))
    else {
        return false;
    };
    let reserved = name == "refs/heads/aruna" || name.starts_with("refs/heads/aruna/");
    name.len() <= 255
        && !short.is_empty()
        && (server || !reserved)
        && !short
            .split('/')
            .any(|part| part.is_empty() || part.starts_with('.') || part.ends_with(".lock"))
        && !name.contains("..")
        && !name.contains("@{")
        && !name
            .bytes()
            .any(|byte| byte <= b' ' || b"~^:?*[\\\x7f".contains(&byte))
        && !name.ends_with('.')
}

/// Whether two ref names cannot both exist, because one would be a folder of the other.
pub fn refs_clash(first: &str, second: &str) -> bool {
    first != second
        && (first
            .strip_prefix(second)
            .is_some_and(|rest| rest.starts_with('/'))
            || second
                .strip_prefix(first)
                .is_some_and(|rest| rest.starts_with('/')))
}

/// A repository-relative file path without traversal or Git internals.
pub fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path
            .split('/')
            .any(|part| matches!(part, "" | "." | ".." | ".git"))
}

impl StoredObject {
    fn valid(&self) -> bool {
        hex(&self.sha256, 64) && !self.bucket.is_empty() && self.key.len() <= 1024
    }
}

impl GitRecord {
    /// Checks shape and bounds; authorship and document state are checked by the writer and receiver.
    pub fn validate(&self) -> bool {
        let size = postcard::to_allocvec(self).map_or(usize::MAX, |bytes| bytes.len());
        size <= MAX_RECORD_BYTES
            && match &self.change {
                GitChange::Objects {
                    pack,
                    refs,
                    lfs,
                    revision,
                    made,
                    ..
                } => {
                    (!refs.is_empty() || revision.is_some())
                        && refs.iter().all(|update| {
                            // Users may discard a kept conflict, never write one.
                            let discard = update.new == ZERO_OID
                                && update.name.starts_with("refs/conflicts/");
                            valid_ref(&update.name, revision.is_some() || discard)
                                && hex(&update.old, 40)
                                && hex(&update.new, 40)
                                && update.old != update.new
                        })
                        && pack.as_deref().is_none_or(StoredObject::valid)
                        && lfs.iter().all(StoredObject::valid)
                        && made.iter().all(|commit| hex(commit, 40))
                }
                GitChange::Lock { path, .. } => valid_path(path),
                GitChange::Unlock { .. } => true,
                GitChange::Checkpoint(checkpoint) => {
                    checkpoint.packs.iter().all(StoredObject::valid)
                        && checkpoint
                            .previous
                            .is_none_or(|previous| previous < self.event_id)
                        && !checkpoint.covered.is_empty()
                        && checkpoint
                            .refs
                            .iter()
                            .all(|(name, oid)| valid_ref(name, true) && hex(oid, 40))
                        && checkpoint.lfs.iter().all(StoredObject::valid)
                        && checkpoint.locks.iter().all(|lock| valid_path(&lock.path))
                }
            }
    }
}

pub fn git_record_prefix(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

pub fn git_record_key(document_id: Ulid, event_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&document_id.to_bytes());
    bytes.extend_from_slice(&event_id.to_bytes());
    ByteView::from(bytes)
}

pub fn git_record_entry(record: &GitRecord) -> Result<(KeySpace, Key, Value), postcard::Error> {
    Ok((
        GIT_RECORD_KEYSPACE.to_string(),
        git_record_key(record.document_id, record.event_id),
        postcard::to_allocvec(record)?.into(),
    ))
}

/// The sync revision of an immutable record: it is only ever inserted, never replaced.
pub fn record_change(record: &GitRecord) -> crate::document::DocumentChange {
    crate::document::DocumentChange {
        base: None,
        current: crate::document::DocumentSyncRevision {
            generation: record.occurred_at_ms,
            event_id: record.event_id,
            actor: record.node_id,
            updated_at_ms: record.occurred_at_ms,
        },
        kind: crate::document::DocumentChangeKind::Upsert,
        placement: record.placement,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocumentTarget;

    fn record(change: GitChange) -> GitRecord {
        GitRecord {
            event_id: Ulid::from(2),
            realm_id: RealmId([1; 32]),
            group_id: Ulid::from(3),
            document_id: Ulid::from(4),
            placement: PlacementRef::NIL,
            user_id: UserId::new(Ulid::from(5), RealmId([1; 32])),
            node_id: iroh::SecretKey::from_bytes(&[7; 32]).public(),
            occurred_at_ms: 1,
            change,
        }
    }

    fn push(name: &str, revision: Option<Ulid>) -> GitRecord {
        record(GitChange::Objects {
            pack: None,
            refs: vec![RefUpdate {
                name: name.into(),
                old: ZERO_OID.into(),
                new: "a".repeat(40),
            }],
            lfs: Vec::new(),
            revision,
            digest: None,
            made: Vec::new(),
        })
    }

    #[test]
    fn ref_names() {
        for name in ["refs/heads/main", "refs/heads/feature/x", "refs/tags/v1.0"] {
            assert!(valid_ref(name, false), "{name}");
        }
        assert!(refs_clash("refs/heads/draft", "refs/heads/draft/sub"));
        assert!(refs_clash("refs/heads/draft/sub", "refs/heads/draft"));
        assert!(!refs_clash("refs/heads/draft", "refs/heads/drafts"));
        assert!(!refs_clash("refs/heads/draft", "refs/heads/draft"));
        for name in [
            "refs/heads/",
            "refs/heads/a..b",
            "refs/heads/x.lock",
            "refs/heads/.hidden",
            "refs/heads/a b",
            "refs/heads/a@{1}",
            "refs/notes/x",
            "HEAD",
        ] {
            assert!(!valid_ref(name, true), "{name}");
        }
        assert!(!valid_ref("refs/heads/aruna", false));
        assert!(valid_ref("refs/heads/aruna", true));
        assert!(!valid_ref("refs/conflicts/heads/main/x", false));
        assert!(valid_ref("refs/conflicts/heads/main/x", true));
    }

    #[test]
    fn record_bounds() {
        assert!(push("refs/heads/main", None).validate());
        assert!(!push("refs/heads/aruna", None).validate());
        assert!(push("refs/heads/aruna", Some(Ulid::from(9))).validate());
        let conflict = "refs/conflicts/heads/main/x";
        assert!(!push(conflict, None).validate());
        let mut discard = push(conflict, None);
        if let GitChange::Objects { refs, .. } = &mut discard.change {
            refs[0].old = "a".repeat(40);
            refs[0].new = ZERO_OID.into();
        }
        assert!(discard.validate());
        let mut invalid = push("refs/heads/main", None);
        if let GitChange::Objects { refs, .. } = &mut invalid.change {
            refs[0].new = "A".repeat(40);
        }
        assert!(!invalid.validate());
        assert!(
            !record(GitChange::Lock {
                id: Ulid::from(1),
                path: "../x".into()
            })
            .validate()
        );
        assert!(
            record(GitChange::Lock {
                id: Ulid::from(1),
                path: "data/x.bin".into()
            })
            .validate()
        );
        let mut large = push("refs/heads/main", None);
        if let GitChange::Objects { lfs, .. } = &mut large.change {
            let object = StoredObject {
                node_id: large.node_id,
                group_id: Some(large.group_id),
                bucket: "b".into(),
                key: "k".repeat(1000),
                version_id: Ulid::from(1),
                size: 1,
                sha256: "b".repeat(64),
                blake3: [0; 32],
            };
            lfs.extend(std::iter::repeat_n(object, 5000));
        }
        assert!(!large.validate());
    }

    #[test]
    fn record_placement() {
        let record = push("refs/heads/main", None);
        let target = DocumentTarget::GitRecord {
            document_id: record.document_id,
            event_id: record.event_id,
        };
        assert_eq!(
            target.topic_id(),
            crate::TopicId::metadata(record.document_id)
        );
        assert!(target.uses_shard_topic());
        let (keyspace, key, _) = git_record_entry(&record).expect("entry");
        assert_eq!(
            (keyspace.as_str(), key.clone()),
            (GIT_RECORD_KEYSPACE, target.storage_key())
        );
        assert!(key.starts_with(git_record_prefix(record.document_id)));
    }
}
