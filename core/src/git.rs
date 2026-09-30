//! Native repository bindings, LFS identities and Git adapter requests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::keyspaces::GIT_RECORD_KEYSPACE;
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::PlacementRef;
use crate::structs::storage::dataset_location::DatasetLocation;
use crate::types::{GroupId, Key, KeySpace, Value};
use crate::{NodeId, UserId};
use bytes::Bytes;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// This node's own stored copies of Git packs and LFS content, keyed by document and SHA-256.
pub const LOCAL_OBJECTS: &str = "git_local_objects";
pub const STATUS: &str = "git_status";
/// Accepted pushes to main whose metadata this node still has to apply.
pub const PENDING: &str = "git_pending_merges";
pub const MAX_GIT_BYTES: usize = 64 * 1024 * 1024;
/// Upper bound for one replicated Git record.
pub const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;
/// Upper bound for one pack; larger files belong in Git LFS.
pub const MAX_PACK_BYTES: usize = MAX_RECORD_BYTES;
/// Records not covered by a checkpoint; holders write a checkpoint well before this.
pub const MAX_RECORDS: usize = 1024;
pub const CHECKPOINT_AFTER: usize = 256;
pub const ZERO_OID: &str = "0000000000000000000000000000000000000000";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRepository {
    pub document_id: Ulid,
    pub group_id: Ulid,
    pub bucket: String,
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

    /// The object a Git LFS pointer file names, if `bytes` is one.
    pub fn from_pointer(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > 1024 {
            return None;
        }
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        if lines.next()? != "version https://git-lfs.github.com/spec/v1" {
            return None;
        }
        let (mut oid, mut size) = (None, None);
        for line in lines {
            if let Some(value) = line.strip_prefix("oid sha256:") {
                oid = Some(value.to_owned());
            } else if let Some(value) = line.strip_prefix("size ") {
                size = value.parse().ok();
            }
        }
        let object = Self {
            oid: oid?,
            size: size?,
        };
        object.valid().then_some(object)
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
    /// The author's commit message; `None` keeps the default one.
    pub message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinkedObject {
    pub entity: String,
    pub object: StoredObject,
    /// The repository path of a plain RO-Crate copy; `None` keeps a relative entity's own.
    pub path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitStatus {
    pub event_id: Ulid,
    pub commit: Option<String>,
    pub error: Option<String>,
}

pub type Refs = std::collections::BTreeMap<String, String>;

/// The metadata a push to main brings, applied after the push is recorded until it succeeds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingMerge {
    pub user_id: UserId,
    pub old: String,
    pub new: String,
}

/// One async lock per document, created on first use and dropped once nobody holds it.
#[derive(Debug, Default)]
pub struct DocumentLocks(
    std::sync::Mutex<std::collections::HashMap<Ulid, std::sync::Weak<tokio::sync::Mutex<()>>>>,
);

impl DocumentLocks {
    pub async fn lock(&self, document_id: Ulid) -> tokio::sync::OwnedMutexGuard<()> {
        self.mutex(document_id).lock_owned().await
    }

    /// The lock when no one holds it, without waiting.
    pub fn try_lock(&self, document_id: Ulid) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        self.mutex(document_id).try_lock_owned().ok()
    }

    fn mutex(&self, document_id: Ulid) -> std::sync::Arc<tokio::sync::Mutex<()>> {
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
    /// Notes a pack made from the local cache as imported, since it holds its objects already.
    MarkImported {
        document_id: Ulid,
        digest: String,
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
    /// Builds snapshot commits on the given refs without moving any ref.
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
    /// The commit each revision names, in order; `None` for one that names no commit.
    Peel {
        document_id: Ulid,
        revisions: Vec<String>,
    },
    /// The commits the revisions name, without their history; one entry per distinct commit.
    Commits {
        document_id: Ulid,
        revisions: Vec<String>,
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
    /// Paths also match entities stored under `location`.
    MergeMetadata {
        document_id: Ulid,
        old: Option<String>,
        new: String,
        graph: String,
        location: Option<DatasetLocation>,
    },
    /// The raw content of one file of a commit, `None` when the commit has no such file.
    ReadFile {
        document_id: Ulid,
        revision: String,
        path: String,
    },
    Http(Box<GitRequest>),
    /// The layout of the commit a revision names, if any.
    Layout {
        document_id: Ulid,
        revision: String,
    },
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
    Peeled(Vec<Option<String>>),
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
    Layout(Option<crate::repo_layout::Layout>),
    File(Option<Bytes>),
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
        pack: Option<GitPack>,
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
    /// The dataset's chosen storage location; the newest record wins. Checkpoints never
    /// cover the newest one, so it stays applied.
    Location(DatasetLocation),
}

/// Refs, locks and revision are complete; packs, LFS objects and covered records are those
/// added since `previous`, so a checkpoint stays small however long the history grows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitCheckpoint {
    pub previous: Option<Ulid>,
    pub packs: Vec<GitPack>,
    /// Commits nodes made themselves since `previous`.
    pub made: Vec<String>,
    /// The graph digest of the newest applied snapshot.
    pub digest: Option<[u8; 32]>,
    pub refs: Vec<(String, String)>,
    pub lfs: Vec<StoredObject>,
    pub locks: Vec<LfsLock>,
    /// Claims that lost to a held lock and may still win if that lock was released earlier.
    pub waiting: Vec<LfsLock>,
    /// Locks released since `previous`, with the unlock record, so a late claim made while
    /// one of them was held is refused as a full replay would refuse it.
    pub released: Vec<(LfsLock, Ulid)>,
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

/// A pack a record names; its bytes are the [`GitPackRecord`] stored under its SHA-256.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPack {
    pub sha256: String,
    pub size: u64,
}

impl GitPack {
    fn valid(&self) -> bool {
        hex(&self.sha256, 64) && usize::try_from(self.size).is_ok_and(|size| size <= MAX_PACK_BYTES)
    }

    /// The raw digest that keys the pack's bytes.
    pub fn digest(&self) -> Option<[u8; 32]> {
        hex::decode(&self.sha256).ok()?.try_into().ok()
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
                        && pack.as_ref().is_none_or(GitPack::valid)
                        && lfs.iter().all(StoredObject::valid)
                        && made.iter().all(|commit| hex(commit, 40))
                }
                GitChange::Lock { path, .. } => valid_path(path),
                GitChange::Unlock { .. } => true,
                GitChange::Checkpoint(checkpoint) => {
                    checkpoint.packs.iter().all(GitPack::valid)
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
                GitChange::Location(location) => location.valid(),
            }
    }
}

/// Documents share these counters, which only costs an extra refresh.
static RECORD_WRITES: [std::sync::atomic::AtomicU64; 1024] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 1024];

fn write_counter(document_id: Ulid) -> &'static std::sync::atomic::AtomicU64 {
    &RECORD_WRITES[(u128::from(document_id) % 1024) as usize]
}

/// Changes whenever this process stores a Git record of the document, local or replicated.
pub fn record_writes(document_id: Ulid) -> u64 {
    write_counter(document_id).load(std::sync::atomic::Ordering::Acquire)
}

/// Called after a Git record of the document is durably stored.
pub fn record_written(document_id: Ulid) {
    write_counter(document_id).fetch_add(1, std::sync::atomic::Ordering::AcqRel);
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

pub fn git_pack_key(document_id: Ulid, sha256: &[u8; 32]) -> Key {
    ByteView::from([document_id.to_bytes().as_slice(), sha256].concat())
}

/// The bytes of one pack, replicated with the fields of the record that first named it, so
/// every node that stores the same pack writes identical rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPackRecord {
    pub document_id: Ulid,
    pub event_id: Ulid,
    pub node_id: NodeId,
    pub occurred_at_ms: u64,
    pub placement: PlacementRef,
    pub bytes: Bytes,
}

impl GitPackRecord {
    pub fn sha256(&self) -> [u8; 32] {
        use sha2::Digest;
        sha2::Sha256::digest(&self.bytes).into()
    }

    /// Whether the bytes fit the pack limit and hash to `sha256`.
    pub fn valid(&self, sha256: &[u8; 32]) -> bool {
        self.bytes.len() <= MAX_PACK_BYTES && self.sha256() == *sha256
    }

    pub fn target(&self) -> crate::document::DocumentTarget {
        crate::document::DocumentTarget::GitPack {
            document_id: self.document_id,
            sha256: self.sha256(),
        }
    }

    /// Like a record, a pack is only ever inserted, never replaced.
    pub fn change(&self) -> crate::document::DocumentChange {
        crate::document::DocumentChange {
            base: None,
            current: crate::document::DocumentSyncRevision {
                generation: self.occurred_at_ms,
                event_id: self.event_id,
                actor: self.node_id,
                updated_at_ms: self.occurred_at_ms,
            },
            kind: crate::document::DocumentChangeKind::Upsert,
            placement: self.placement,
        }
    }
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

    #[tokio::test]
    async fn reads_skip_busy() {
        let locks = DocumentLocks::default();
        let id = Ulid::from(1);
        let held = locks.lock(id).await;
        assert!(
            locks.try_lock(id).is_none(),
            "a held lock is not handed out"
        );
        assert!(
            locks.try_lock(Ulid::from(2)).is_some(),
            "other documents stay free"
        );
        drop(held);
        assert!(locks.try_lock(id).is_some());
    }
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
    fn reads_pointers() {
        let oid = "a".repeat(64);
        let pointer =
            format!("version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize 12\n");
        let object = LfsObject::from_pointer(pointer.as_bytes()).expect("pointer");
        assert_eq!((object.oid, object.size), (oid, 12));
        assert!(LfsObject::from_pointer(b"plain,csv\n1,2\n").is_none());
        let short = "version https://git-lfs.github.com/spec/v1\noid sha256:ab\nsize 1\n";
        assert!(LfsObject::from_pointer(short.as_bytes()).is_none());
    }

    #[test]
    fn change_tags() {
        // Postcard encodes the variant index first; replicated records depend on it.
        let tag = |change: GitChange| postcard::to_allocvec(&change).unwrap()[0];
        let unlock = GitChange::Unlock { id: Ulid::from(1) };
        let location = DatasetLocation::new("lab-data", "runs").unwrap();
        assert_eq!(tag(unlock), 2);
        assert_eq!(tag(GitChange::Location(location.clone())), 4);
        assert!(record(GitChange::Location(location)).validate());
        let raw = DatasetLocation {
            bucket: "lab-data".into(),
            prefix: "../x".into(),
        };
        assert!(!record(GitChange::Location(raw)).validate());
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
