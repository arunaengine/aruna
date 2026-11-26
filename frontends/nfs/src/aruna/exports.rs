//! Live bucket and folder exports: durable handles, lookups, attributes, per-bit access and
//! directory listings, all admitted from live state on each request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::{
    BucketScope, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind, NfsMountBinding,
    NfsMountTarget, StageScope,
};
use aruna_core::vfs_effects::{InstanceSummary, ShareMode, VfsProtocol};
use aruna_ops_data::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use aruna_ops_data::vfs::handles::{HandleClaim, HandleState, ListNext, ListRequest, ListResume};
use aruna_ops_data::vfs::io::{OpenInput, OpenResult};
use aruna_ops_data::vfs::path::{
    NameUse, check_component, child_key, children_prefix, export_holds, marker_key, split_parent,
};
use aruna_ops_data::vfs::policy::{AuthorizeVfsOperation, VfsAccess, VfsGrant, VfsRequest};
use aruna_ops_data::vfs::publish::CloseInput;
use aruna_ops_data::vfs::{
    AllocateHandleOperation, InstanceTarget, LookupInstanceOperation, ReadChangeOperation,
    ResolveHandleOperation, VfsCloseOperation, VfsError, VfsListOperation, VfsOpenOperation,
};
use aruna_runner::driver::drive;
use tracing::debug;
use ulid::Ulid;

use super::bodies::bucket_name;
use super::{ArunaNfsBackend, LIVE_ROOT, vfs_status};
use crate::backend::{DirectoryEntry, DirectoryPage, HandleStatus, ObjectAttributes, ObjectRef};
use crate::types::{NfsStat4, NfsTime, NfsType4, access, share};

/// The file id the export root reports; handle ids never reach it.
const ROOT_FILEID: u64 = u64::MAX;

/// What a live handle names now.
pub(super) struct Located {
    pub key: String,
    pub kind: HandleKind,
    /// `None` for the export root, which is never bound to a handle row.
    pub identity: Option<HandleIdentity>,
    /// Off its name, serving only its existing opens.
    pub detached: Option<Box<InstanceSummary>>,
}

/// A name's identity and its effective size and change, as one admitted read found it.
pub(super) struct Probe {
    pub identity: HandleIdentity,
    pub kind: HandleKind,
    pub size: u64,
    pub change: u64,
}

/// A durable handle of a name together with what it was allocated for.
pub(super) struct Claimed {
    pub handle_id: u64,
    pub cookie_id: u64,
    pub probe: Probe,
}

/// The bucket and incarnation every handle of a binding belongs to.
pub(super) fn bucket_scope(binding: &NfsMountBinding) -> Result<BucketScope, VfsError> {
    match &binding.target {
        NfsMountTarget::Bucket {
            bucket,
            bucket_incarnation,
        }
        | NfsMountTarget::Folder {
            bucket,
            bucket_incarnation,
            ..
        } => Ok(BucketScope::new(bucket.clone(), *bucket_incarnation)),
        NfsMountTarget::JobInputView { .. } => Err(VfsError::ReadOnly),
    }
}

/// The key of the export root: empty for a bucket, the folder without its slash.
pub(super) fn root_key(binding: &NfsMountBinding) -> String {
    match &binding.target {
        NfsMountTarget::Folder { prefix, .. } => prefix.trim_end_matches('/').to_string(),
        _ => String::new(),
    }
}

/// Whether the export holds `key` by whole components; `team/` never holds `team-old/a`.
pub(super) fn holds(binding: &NfsMountBinding, key: &str) -> bool {
    let Ok(bucket) = bucket_name(binding) else {
        return false;
    };
    match &binding.target {
        NfsMountTarget::Folder { .. } if key == root_key(binding) => true,
        _ => export_holds(&binding.target, bucket, key),
    }
}

/// The key an action on an object is admitted on: a file, a directory's marker, or the
/// bucket itself for the bucket root.
pub(super) fn object_key(key: &str, kind: HandleKind) -> Option<String> {
    match (kind, key.is_empty()) {
        (HandleKind::File, _) => Some(key.to_string()),
        (HandleKind::Directory, true) => None,
        (HandleKind::Directory, false) => Some(marker_key(key)),
    }
}

/// The mode a binding shows: no write bits on a read-only one, search bits on directories.
pub(super) fn display_mode(binding: &NfsMountBinding, kind: HandleKind) -> u32 {
    let mut mode = binding.mode & 0o777;
    if binding.read_only {
        mode &= !0o222;
    }
    match kind {
        HandleKind::File => mode & !0o111,
        HandleKind::Directory => mode | 0o111,
    }
}

/// The creation time an identity carries in its ULID, or `fallback_ms` without one.
fn identity_time(identity: Option<&HandleIdentity>, fallback_ms: u64) -> NfsTime {
    let id = match identity {
        Some(HandleIdentity::File(FileIdentity::Committed { life_id }))
        | Some(HandleIdentity::Directory(DirectoryWitness::Marker { life_id }))
        | Some(HandleIdentity::Directory(DirectoryWitness::Descendant { life_id, .. })) => {
            Some(*life_id)
        }
        Some(HandleIdentity::File(FileIdentity::Unpublished { token, .. }))
        | Some(HandleIdentity::Directory(DirectoryWitness::Unpublished { token, .. })) => {
            Some(*token)
        }
        None => None,
    };
    let millis = id.map_or(fallback_ms, |id| id.timestamp_ms());
    NfsTime {
        seconds: i64::try_from(millis / 1000).unwrap_or(i64::MAX),
        nanos: u32::try_from(millis % 1000).unwrap_or(0) * 1_000_000,
    }
}

fn object_attributes(
    binding: &NfsMountBinding,
    object_id: u64,
    (kind, size, change): (HandleKind, u64, u64),
    time: NfsTime,
) -> ObjectAttributes {
    let (file_kind, links) = match kind {
        HandleKind::File => (NfsType4::Regular, 1),
        HandleKind::Directory => (NfsType4::Directory, 2),
    };
    ObjectAttributes {
        kind: file_kind,
        size,
        change,
        fileid: match object_id {
            LIVE_ROOT => ROOT_FILEID,
            object_id => object_id,
        },
        mode: display_mode(binding, kind),
        links,
        uid: binding.display_uid,
        gid: binding.display_gid,
        space_used: size,
        accessed: time,
        modified: time,
        changed: time,
    }
}

pub(super) fn share_mode(access: u32, deny: u32) -> ShareMode {
    ShareMode {
        read_access: access & share::READ != 0,
        write_access: access & share::WRITE != 0,
        delete_access: false,
        share_read: deny & share::READ == 0,
        share_write: deny & share::WRITE == 0,
        share_delete: true,
    }
}

/// Where a scan continues after a page whose entries were all hidden, or `None` at its end.
/// A plain position must move strictly forward, so the scan always ends.
pub(super) fn resume_after(
    next: ListNext,
    previous: &ListResume,
) -> Result<Option<ListResume>, NfsStat4> {
    match (next, previous) {
        (ListNext::End, _) => Ok(None),
        (ListNext::Sealed(sealed), _) => Ok(Some(ListResume::Sealed(sealed.into_bytes()))),
        (ListNext::After(after), ListResume::After(before)) if after <= *before => {
            Err(NfsStat4::ServerFault)
        }
        (ListNext::After(after), _) => Ok(Some(ListResume::After(after))),
    }
}

/// A protected name answers as a missing one.
pub(super) fn lookup_status(error: &VfsError) -> NfsStat4 {
    match error {
        VfsError::Denied | VfsError::InvalidName => NfsStat4::NoEntry,
        error => vfs_status(error),
    }
}

/// Refusals that only withhold an access bit; anything else fails the ACCESS call.
fn withheld(error: &VfsError) -> bool {
    matches!(
        error,
        VfsError::Denied
            | VfsError::ReadOnly
            | VfsError::VaultLocked
            | VfsError::KeyRequired
            | VfsError::Quarantined
            | VfsError::CrossExport
            | VfsError::Distributed
    )
}

pub(super) fn status_of(error: VfsError) -> NfsStat4 {
    vfs_status(&error)
}

impl ArunaNfsBackend {
    pub(super) fn object_ref(&self, binding: &NfsMountBinding, object_id: u64) -> ObjectRef {
        ObjectRef {
            mount_id: binding.mount_id,
            object_id,
        }
    }

    pub(super) async fn live_resolve(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> HandleStatus {
        if object.object_id == LIVE_ROOT {
            return HandleStatus::Live;
        }
        let Ok(scope) = bucket_scope(binding) else {
            return HandleStatus::Stale;
        };
        let resolve = ResolveHandleOperation::new(scope, object.object_id);
        match drive(resolve, &self.context).await {
            Ok(HandleState::Live(live) | HandleState::Detached(live, _))
                if holds(binding, &live.key) =>
            {
                HandleStatus::Live
            }
            Ok(_) => HandleStatus::Stale,
            Err(VfsError::ExpiredHandle) => HandleStatus::Expired,
            Err(VfsError::StaleHandle | VfsError::NotFound) => HandleStatus::Stale,
            // A transient failure is reported by the first operation that uses the handle.
            Err(_) => HandleStatus::Live,
        }
    }

    /// The key and identity a handle names now, inside the export.
    pub(super) async fn locate(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> Result<Located, NfsStat4> {
        if object.object_id == LIVE_ROOT {
            return Ok(Located {
                key: root_key(binding),
                kind: HandleKind::Directory,
                identity: None,
                detached: None,
            });
        }
        let scope = bucket_scope(binding).map_err(status_of)?;
        let resolve = ResolveHandleOperation::new(scope, object.object_id);
        let state = drive(resolve, &self.context).await.map_err(status_of)?;
        let (live, detached) = match state {
            HandleState::Live(live) => (live, None),
            HandleState::Detached(live, summary) => (live, Some(summary)),
        };
        if !holds(binding, &live.key) {
            return Err(NfsStat4::Stale);
        }
        Ok(Located {
            kind: live.identity.kind(),
            key: live.key,
            identity: Some(live.identity),
            detached,
        })
    }

    pub(super) async fn directory_of(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> Result<Located, NfsStat4> {
        let located = self.locate(binding, object).await?;
        match located.kind {
            HandleKind::Directory => Ok(located),
            HandleKind::File => Err(NfsStat4::NotDirectory),
        }
    }

    /// Admits one action on `key` (or on the bucket) from live state.
    pub(super) async fn authorize(
        &self,
        binding: &NfsMountBinding,
        access: VfsAccess,
        key: Option<String>,
        prefix: Option<String>,
    ) -> Result<VfsGrant, VfsError> {
        let bucket = bucket_name(binding).map_err(|_| VfsError::ReadOnly)?;
        let mut request = VfsRequest::new(access, bucket, key.as_deref());
        request.prefix = prefix;
        let operation = AuthorizeVfsOperation::new(self.access_context(binding), request);
        drive(operation, &self.context).await
    }

    /// A directory without a live marker shows only while the caller can list one child of it,
    /// so children hidden by policy never reveal their parent.
    async fn implicit_shows(
        &self,
        binding: &NfsMountBinding,
        key: &str,
        identity: Option<&HandleIdentity>,
    ) -> Result<(), VfsError> {
        let implicit = matches!(
            identity,
            Some(HandleIdentity::Directory(
                DirectoryWitness::Descendant { .. } | DirectoryWitness::Unpublished { .. }
            ))
        );
        if !implicit {
            return Ok(());
        }
        let mut resume = ListResume::Start;
        loop {
            let request = ListRequest {
                context: self.access_context(binding),
                directory: key.to_string(),
                witness: None,
                resume,
                max_entries: 1,
                deadline_ms: None,
            };
            let listing = drive(VfsListOperation::new(request), &self.context).await?;
            if !listing.entries.is_empty() {
                return Ok(());
            }
            resume = match listing.next {
                ListNext::End => return Err(VfsError::NotFound),
                ListNext::After(name) => ListResume::After(name),
                ListNext::Sealed(sealed) => ListResume::Sealed(sealed.into_bytes()),
            };
        }
    }

    /// The change counter of `key`; also advances with staged and descendant changes.
    pub(super) async fn change_of(
        &self,
        binding: &NfsMountBinding,
        key: &str,
    ) -> Result<u64, NfsStat4> {
        let scope = bucket_scope(binding).map_err(status_of)?;
        let read = ReadChangeOperation::new(scope, vec![key.to_string()]);
        let snapshot = drive(read, &self.context).await.map_err(status_of)?;
        Ok(snapshot.values.first().copied().unwrap_or_default())
    }

    /// The current head version and size of `key`; no head reads as `(None, 0)`.
    pub(super) async fn head_info(
        &self,
        binding: &NfsMountBinding,
        key: &str,
    ) -> Result<(Option<Ulid>, u64), NfsStat4> {
        let input = HeadObjectInput {
            bucket: bucket_name(binding)?.to_string(),
            key: key.to_string(),
            version_id: None,
        };
        match drive(HeadObjectOperation::new(input), &self.context).await {
            Ok(head) => {
                let size = head
                    .location
                    .as_ref()
                    .map_or(0, |location| location.blob_size);
                Ok((head.version_id, size))
            }
            Err(HeadObjectError::NoSuchKey | HeadObjectError::DeleteMarker) => Ok((None, 0)),
            Err(error) => {
                debug!(error = %error, "NFS head read failed");
                Err(NfsStat4::Delay)
            }
        }
    }

    /// The live instance at `key` while it still has `identity`.
    pub(super) async fn instance_at(
        &self,
        binding: &NfsMountBinding,
        key: &str,
        identity: Option<&HandleIdentity>,
    ) -> Result<Option<InstanceSummary>, NfsStat4> {
        let scope = bucket_scope(binding).map_err(status_of)?;
        let lookup = LookupInstanceOperation::new(scope, InstanceTarget::Key(key.to_string()));
        let summary = drive(lookup, &self.context).await.map_err(status_of)?;
        Ok(summary.filter(|summary| {
            summary.detached.is_none()
                && identity.is_none_or(|identity| summary.identity == *identity)
        }))
    }

    /// Opens `key` without access or deny bits to learn its identity, then closes again.
    async fn probe_open(
        &self,
        binding: &NfsMountBinding,
        key: &str,
        kind: HandleKind,
    ) -> Result<OpenResult, VfsError> {
        let open_id = Ulid::generate();
        let input = OpenInput {
            context: self.access_context(binding),
            key: key.to_string(),
            kind,
            create: false,
            exclusive: false,
            write: false,
            share: share_mode(0, 0),
            delete_on_close: false,
            open_id,
            stable: false,
            mode: display_mode(binding, kind),
            mount_id: binding.mount_id,
            stage_scope: StageScope::User,
        };
        let opened = drive(VfsOpenOperation::new(input), &self.context).await?;
        self.discard_open(binding, open_id).await;
        Ok(opened)
    }

    /// Closes an open of the backend's own that never wrote.
    pub(super) async fn discard_open(&self, binding: &NfsMountBinding, open_id: Ulid) {
        let Ok(scope) = bucket_scope(binding) else {
            return;
        };
        let input = CloseInput {
            context: self.access_context(binding),
            scope,
            open_id,
            publish: None,
            stage: None,
        };
        if let Err(error) = drive(VfsCloseOperation::new(input), &self.context).await {
            debug!(error = %error, "NFS probe close failed");
        }
    }

    /// The identity, kind, size and change of `key` after live `s3.HeadObject` admission.
    async fn probe(
        &self,
        binding: &NfsMountBinding,
        key: &str,
        kind: Option<HandleKind>,
    ) -> Result<Probe, VfsError> {
        let scope = bucket_scope(binding)?;
        let lookup = LookupInstanceOperation::new(scope, InstanceTarget::Key(key.to_string()));
        let summary = drive(lookup, &self.context).await?;
        if let Some(summary) = summary.filter(|summary| summary.detached.is_none()) {
            let admitted = object_key(key, summary.kind);
            self.authorize(binding, VfsAccess::Head, admitted, None)
                .await?;
            self.implicit_shows(binding, key, Some(&summary.identity))
                .await?;
            return Ok(Probe {
                identity: summary.identity,
                kind: summary.kind,
                size: summary.logical_size,
                change: summary.change,
            });
        }
        let kinds = match kind {
            Some(kind) => vec![kind],
            None => vec![HandleKind::File, HandleKind::Directory],
        };
        for kind in kinds {
            match self.probe_open(binding, key, kind).await {
                Ok(opened) => {
                    self.implicit_shows(binding, key, Some(&opened.identity))
                        .await?;
                    return Ok(Probe {
                        identity: opened.identity,
                        kind,
                        size: opened.size,
                        change: opened.change,
                    });
                }
                Err(VfsError::IsDirectory | VfsError::NotDirectory) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(VfsError::NotFound)
    }

    /// The durable handle of `key`, allocated lazily for the identity a probe found.
    pub(super) async fn claim(
        &self,
        binding: &NfsMountBinding,
        key: &str,
        kind: Option<HandleKind>,
    ) -> Result<Claimed, VfsError> {
        let probe = self.probe(binding, key, kind).await?;
        let claim = HandleClaim {
            scope: bucket_scope(binding)?,
            key: key.to_string(),
            identity: probe.identity.clone(),
            mode: display_mode(binding, probe.kind),
        };
        let allocation = drive(AllocateHandleOperation::new(claim), &self.context).await?;
        Ok(Claimed {
            handle_id: allocation.handle_id,
            cookie_id: allocation.cookie_id,
            probe,
        })
    }

    pub(super) async fn live_lookup(
        &self,
        binding: &NfsMountBinding,
        directory: ObjectRef,
        name: &str,
    ) -> Result<ObjectRef, NfsStat4> {
        let parent = self.directory_of(binding, directory).await?;
        let at_root = parent.key.is_empty();
        check_component(name, VfsProtocol::Nfs, NameUse::Lookup, at_root)
            .map_err(|error| lookup_status(&error))?;
        let key = child_key(&parent.key, name);
        if !holds(binding, &key) {
            return Err(NfsStat4::NoEntry);
        }
        let claimed = self.claim(binding, &key, None).await;
        let claimed = claimed.map_err(|error| lookup_status(&error))?;
        Ok(self.object_ref(binding, claimed.handle_id))
    }

    pub(super) async fn live_parent(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> Result<ObjectRef, NfsStat4> {
        let located = self.locate(binding, object).await?;
        let root = root_key(binding);
        let parent =
            split_parent(&located.key).map_or_else(String::new, |(parent, _)| parent.to_string());
        if parent == root || parent.is_empty() {
            return Ok(self.object_ref(binding, LIVE_ROOT));
        }
        if !holds(binding, &parent) {
            return Err(NfsStat4::Access);
        }
        let claimed = self
            .claim(binding, &parent, Some(HandleKind::Directory))
            .await;
        let claimed = claimed.map_err(|error| lookup_status(&error))?;
        Ok(self.object_ref(binding, claimed.handle_id))
    }

    pub(super) async fn live_attributes(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> Result<ObjectAttributes, NfsStat4> {
        let located = self.locate(binding, object).await?;
        let time = identity_time(located.identity.as_ref(), binding.created_at_ms);
        if let Some(summary) = &located.detached {
            let facts = (summary.kind, summary.logical_size, summary.change);
            return Ok(object_attributes(binding, object.object_id, facts, time));
        }
        let admitted = object_key(&located.key, located.kind);
        self.authorize(binding, VfsAccess::Head, admitted, None)
            .await
            .map_err(status_of)?;
        let identity = located.identity.as_ref();
        self.implicit_shows(binding, &located.key, identity)
            .await
            .map_err(|error| lookup_status(&error))?;
        let (size, change) = match located.kind {
            HandleKind::Directory => (0, self.change_of(binding, &located.key).await?),
            HandleKind::File => {
                let identity = located.identity.as_ref();
                match self.instance_at(binding, &located.key, identity).await? {
                    Some(summary) => (summary.logical_size, summary.change),
                    None => {
                        let (_, size) = self.head_info(binding, &located.key).await?;
                        (size, self.change_of(binding, &located.key).await?)
                    }
                }
            }
        };
        let facts = (located.kind, size, change);
        Ok(object_attributes(binding, object.object_id, facts, time))
    }

    /// Each requested bit is admitted on its own S3 action against the live policy.
    pub(super) async fn live_access(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        requested: u32,
    ) -> Result<u32, NfsStat4> {
        let located = self.locate(binding, object).await?;
        let key = located.key.clone();
        if located.detached.is_none() {
            let identity = located.identity.as_ref();
            self.implicit_shows(binding, &key, identity)
                .await
                .map_err(|error| lookup_status(&error))?;
        }
        let own = object_key(&key, located.kind);
        let children = (!key.is_empty()).then(|| children_prefix(&key));
        type Check = (u32, VfsAccess, Option<String>, Option<String>);
        let checks: Vec<Check> = match located.kind {
            HandleKind::File => vec![
                (access::READ, VfsAccess::Get, own.clone(), None),
                (access::EXECUTE, VfsAccess::Head, own.clone(), None),
                (access::MODIFY | access::EXTEND, VfsAccess::Put, own, None),
            ],
            HandleKind::Directory => vec![
                (
                    access::READ,
                    VfsAccess::List,
                    None,
                    Some(children_prefix(&key)),
                ),
                (access::LOOKUP, VfsAccess::Head, own, None),
                (
                    access::MODIFY | access::EXTEND,
                    VfsAccess::Put,
                    children.clone(),
                    None,
                ),
                (access::DELETE, VfsAccess::Delete, children, None),
            ],
        };
        let mut granted = 0;
        for (bits, action, key, prefix) in checks {
            if requested & bits == 0 || (action.writes() && binding.read_only) {
                continue;
            }
            match self.authorize(binding, action, key, prefix).await {
                Ok(_) => granted |= requested & bits,
                Err(error) if withheld(&error) => {}
                Err(error) => return Err(vfs_status(&error)),
            }
        }
        Ok(granted)
    }

    pub(super) async fn live_directory(
        &self,
        binding: &NfsMountBinding,
        directory: ObjectRef,
        (cookie, verifier, limit): (u64, [u8; 8], usize),
    ) -> Result<DirectoryPage, NfsStat4> {
        let located = self.directory_of(binding, directory).await?;
        if cookie != 0 && verifier != self.verifier {
            return Err(NfsStat4::NotSame);
        }
        let witness = match &located.identity {
            Some(HandleIdentity::Directory(witness)) => Some(witness.clone()),
            _ => None,
        };
        let mut resume = match cookie {
            0 => ListResume::Start,
            1 | 2 => return Err(NfsStat4::BadCookie),
            cookie => {
                let name = self.tables.lock().cookies.get(&(binding.mount_id, cookie));
                ListResume::Cookie {
                    cookie,
                    name: name.unwrap_or_default(),
                }
            }
        };
        let limit = limit.max(1);
        let mut entries = Vec::new();
        let eof = loop {
            let request = ListRequest {
                context: self.access_context(binding),
                directory: located.key.clone(),
                witness: witness.clone(),
                resume: resume.clone(),
                max_entries: limit,
                deadline_ms: None,
            };
            let listing = drive(VfsListOperation::new(request), &self.context).await;
            let listing = listing.map_err(status_of)?;
            let mut full = false;
            for entry in listing.entries {
                // Entries past the page get no handle; the next page lists them again.
                if entries.len() == limit {
                    full = true;
                    break;
                }
                let key = child_key(&located.key, &entry.name);
                let claimed = match self.claim(binding, &key, Some(entry.kind)).await {
                    Ok(claimed) => claimed,
                    // A name that went away or became hidden since the page was read.
                    Err(
                        VfsError::NotFound
                        | VfsError::Denied
                        | VfsError::StaleHandle
                        | VfsError::ExpiredHandle
                        | VfsError::IsDirectory
                        | VfsError::NotDirectory,
                    ) => continue,
                    Err(error) => return Err(vfs_status(&error)),
                };
                let cookie = (binding.mount_id, claimed.cookie_id);
                let name = entry.name.clone();
                self.tables.lock().cookies.insert(cookie, name);
                let probe = &claimed.probe;
                let time = identity_time(Some(&probe.identity), binding.created_at_ms);
                let facts = (probe.kind, probe.size, probe.change);
                entries.push(DirectoryEntry {
                    name: entry.name,
                    cookie: claimed.cookie_id,
                    object: self.object_ref(binding, claimed.handle_id),
                    attributes: object_attributes(binding, claimed.handle_id, facts, time),
                });
            }
            if full || !entries.is_empty() {
                break !full && listing.next == ListNext::End;
            }
            // A page of hidden entries alone continues the scan instead of ending the page.
            match resume_after(listing.next, &resume)? {
                Some(next) => resume = next,
                None => break true,
            }
        };
        Ok(DirectoryPage {
            entries,
            verifier: self.verifier,
            eof,
        })
    }
}
