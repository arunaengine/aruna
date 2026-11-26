//! Opens and byte access of one shared file instance: open and create, exact-version reads,
//! staged writes and resizes, durable NFS checkpoints and byte-range locks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::handles::{
    ClaimHandleOperation, HandleAllocation, HandleClaim, HandleUse, Held, WitnessFacts,
    detached_allows, select_witness,
};
use super::path::{NameFacts, NameUse, Projected, check_component, marker_key, project_name};
use super::policy::{VfsAccess, VfsAccessContext, VfsGrant, VfsRequest, authorize_call, grant_of};
use super::{
    CHUNK_BYTES, STATE_PAGE, VfsError, head_of, live_life, name_reads, record_of, state_key,
    state_row, token_of, unexpected, vfs_call, vfs_event, watch_row,
};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::BlobEvent;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, NFS_STATE_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::encryption::EncryptionMode;
use aruna_core::structs::storage::object_read::ObjectRangeRequest;
use aruna_core::structs::storage::vfs::{
    BucketScope, FileIdentity, HandleIdentity, HandleKind, NfsStageRecord, StageBase, StageOwner,
    StageScope, VfsKey,
};
use aruna_core::types::Key;
use aruna_core::vfs_effects::{
    ByteLock, ByteRange, InstanceSummary, OpenRequest, ShareMode, StageSlice, VfsEffect, VfsEvent,
};
use ulid::Ulid;

/// How durable a write must be before it is acknowledged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteStable {
    Unstable,
    DataSync,
    FileSync,
}

/// A failed exact-version read as the file server error it means.
pub(crate) fn read_error(error: GetObjectError) -> VfsError {
    match error {
        GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker => VfsError::StaleHandle,
        GetObjectError::PendingContent(_) | GetObjectError::Abe(_) => VfsError::KeyRequired,
        GetObjectError::GovernedUnavailable | GetObjectError::HolderAccessDenied => {
            VfsError::Denied
        }
        // A quarantined copy fails closed inside the read; its refusal names it.
        other => match other.to_string() {
            text if text.to_ascii_lowercase().contains("quarantin") => VfsError::Quarantined,
            text => VfsError::Unavailable(text),
        },
    }
}

/// One exact-version range read of `[start, start + length)` for `grant`'s caller.
pub(crate) fn range_read(
    context: &VfsAccessContext,
    grant: &VfsGrant,
    key: &str,
    version_id: Ulid,
    range: ByteRange,
) -> IoCall {
    let end = range.offset + range.length.max(1) - 1;
    let input = GetObjectInput {
        bucket: (context.bucket_scope()).map_or_else(String::new, |(bucket, _)| bucket.to_string()),
        key: key.to_string(),
        version_id: Some(version_id),
        range: Some(ObjectRangeRequest::StartEnd {
            start: range.offset,
            end,
        }),
        group_id: grant.bucket.group_id,
        user_identity: grant.access.user_identity,
        realm_id: context.realm_id,
        node_id: context.node_id,
    };
    let restrictions = grant.access.path_restrictions.clone();
    IoCall::child(GetObjectOperation::new(input).with_restrictions(restrictions))
}

/// The stream of a range read, collected in memory up to the requested length; an empty or
/// past-the-end range reads as nothing.
pub(crate) fn collect_call(result: IoResult, length: u64) -> Result<Option<IoCall>, VfsError> {
    match result.child::<GetObjectResult, GetObjectError>()? {
        Ok(read) => {
            let collect = BlobEffect::CollectStream {
                blob: read.blob,
                limit: Some(length),
            };
            Ok(Some(Effect::blob(collect).into()))
        }
        Err(GetObjectError::InvalidRange) => Ok(None),
        Err(error) => Err(read_error(error)),
    }
}

/// The bytes a `CollectStream` answered.
pub(crate) fn collected(result: IoResult) -> Result<Vec<u8>, VfsError> {
    match result.blob()? {
        BlobEvent::StreamCollected { bytes: Ok(bytes) } => Ok(bytes),
        BlobEvent::StreamCollected { bytes: Err(reason) } => Err(VfsError::Unavailable(reason)),
        BlobEvent::StreamTooLarge { .. } => Err(VfsError::Conflict),
        other => Err(VfsError::Unavailable(format!(
            "unexpected blob answer {other:?}"
        ))),
    }
}

/// The chunks of `ranges`, each at most [`CHUNK_BYTES`], in order.
pub fn chunked(ranges: &[ByteRange]) -> Vec<ByteRange> {
    let mut chunks = Vec::new();
    for range in ranges {
        let mut offset = range.offset;
        let end = range.offset.saturating_add(range.length);
        while offset < end {
            let length = (end - offset).min(CHUNK_BYTES);
            chunks.push(ByteRange { offset, length });
            offset += length;
        }
    }
    chunks
}

/// What an open request asks for.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenInput {
    pub context: VfsAccessContext,
    pub key: String,
    pub kind: HandleKind,
    pub create: bool,
    /// Refuses an existing name.
    pub exclusive: bool,
    pub write: bool,
    pub share: ShareMode,
    pub delete_on_close: bool,
    pub open_id: Ulid,
    /// NFS needs a durable handle; SMB opens stay volatile.
    pub stable: bool,
    pub mode: u32,
    /// The mount binding of an NFS writer; SMB uses the nil id.
    pub mount_id: Ulid,
    pub stage_scope: StageScope,
}

/// An admitted open on one shared instance.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenResult {
    pub instance_id: u64,
    pub identity: HandleIdentity,
    pub handle: Option<HandleAllocation>,
    pub size: u64,
    pub created: bool,
    pub change: u64,
    pub grant: VfsGrant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenStep {
    Authorize,
    Permit,
    Rows,
    Probe,
    Staged,
    Instance,
    Version,
    Location,
    Watch,
    Opened,
    Handle,
    Synced,
}

type OpenTurn = Result<Next<OpenResult>, VfsError>;

/// Opens or creates one name: live admission, path projection under the namespace permit, a
/// create-only watch token for a new name and a durable handle where NFS needs stability.
#[derive(Debug, PartialEq)]
pub struct VfsOpenOperation {
    input: OpenInput,
    step: OpenStep,
    ids: IdMint,
    now_ms: u64,
    grant: Option<VfsGrant>,
    held: Held,
    head: Option<CurrentVersionPointer>,
    facts: NameFacts,
    witness: WitnessFacts,
    joined: Option<InstanceSummary>,
    identity: Option<HandleIdentity>,
    base_size: u64,
    opened: Option<(u64, bool, u64, u64)>,
    handle: Option<HandleAllocation>,
}

impl VfsOpenOperation {
    pub fn new(input: OpenInput) -> Self {
        Self {
            input,
            step: OpenStep::Authorize,
            ids: IdMint::default(),
            now_ms: 0,
            grant: None,
            held: Held::default(),
            head: None,
            facts: NameFacts::default(),
            witness: WitnessFacts::default(),
            joined: None,
            identity: None,
            base_size: 0,
            opened: None,
            handle: None,
        }
    }

    fn call(&mut self, step: OpenStep, call: IoCall) -> OpenTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn bucket(&self) -> String {
        let scope = self.input.context.bucket_scope();
        scope.map_or_else(String::new, |(bucket, _)| bucket.to_string())
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        Ok(BucketScope::new(self.bucket(), grant.incarnation))
    }

    fn rows_read(&mut self, result: IoResult) -> OpenTurn {
        let [head, _watch, marker] = aruna_core::rows::values::<3>(result)?;
        self.head = head_of(head.as_ref())?;
        let marker = head_of(marker.as_ref())?;
        self.facts.live_file = live_life(self.head.as_ref()).is_some();
        self.facts.live_marker = live_life(marker.as_ref()).is_some();
        self.witness.marker = live_life(marker.as_ref());
        let below = marker_key(&self.input.key);
        let prefix = BlobHeadKey::object_prefix(&self.bucket(), &below)?;
        self.call(
            OpenStep::Probe,
            IoCall::Page {
                space: BLOB_HEAD_KEYSPACE.to_string(),
                prefix: prefix.into(),
                after: None,
                limit: STATE_PAGE,
            },
        )
    }

    /// One bounded page of raw heads below `d/` decides whether a live descendant exists.
    fn probed(&mut self, result: IoResult) -> OpenTurn {
        let (rows, _) = result.page()?;
        for (key, value) in rows {
            let head = CurrentVersionPointer::from_bytes(value.as_ref())?;
            let name = BlobHeadKey::from_bytes(key.as_ref())?;
            if let Some(life_id) = head.life_id {
                self.witness.descendants.push((name.key, life_id));
            }
        }
        self.facts.descendant = !self.witness.descendants.is_empty();
        let (scope, prefix) = (self.scope()?, self.input.key.clone());
        self.call(
            OpenStep::Staged,
            vfs_call(VfsEffect::ListStaged { scope, prefix }),
        )
    }

    fn staged_read(&mut self, result: IoResult) -> OpenTurn {
        let entries = match vfs_event(result)? {
            VfsEvent::StagedList { entries, .. } => entries,
            other => return Err(unexpected(&other)),
        };
        let below = marker_key(&self.input.key);
        for entry in entries {
            if entry.key == self.input.key && entry.kind == HandleKind::File {
                self.facts.staged_file = true;
            } else if entry.key.starts_with(&below) {
                self.facts.staged_below = true;
                if let Some(token) = entry.unpublished_token {
                    self.witness.unpublished.push((entry.key, token));
                }
            }
        }
        self.project()
    }

    fn project(&mut self) -> OpenTurn {
        let projected = project_name(self.facts);
        match (projected, self.input.kind) {
            (Projected::Missing, _) if !self.input.create => Err(VfsError::NotFound),
            (Projected::Missing, HandleKind::Directory) => Err(VfsError::NotFound),
            (Projected::Missing, HandleKind::File) => self.create(),
            (_, _) if self.input.exclusive => Err(VfsError::Exists),
            (Projected::Directory, HandleKind::File) => Err(VfsError::IsDirectory),
            (Projected::File, HandleKind::Directory) => Err(VfsError::NotDirectory),
            (Projected::Directory, HandleKind::Directory) => {
                let witness = select_witness(&self.witness).ok_or(VfsError::NotFound)?;
                self.identity = Some(HandleIdentity::Directory(witness));
                self.open_instance()
            }
            (Projected::File, HandleKind::File) => {
                let (scope, key) = (self.scope()?, self.input.key.clone());
                self.call(
                    OpenStep::Instance,
                    vfs_call(VfsEffect::LookupInstance { scope, key }),
                )
            }
        }
    }

    /// An existing stage is joined with its own identity; a committed head opens its life.
    fn instance_read(&mut self, result: IoResult) -> OpenTurn {
        match vfs_event(result)? {
            VfsEvent::Instance(Some(summary)) if summary.detached.is_none() => {
                self.identity = Some(summary.identity.clone());
                self.base_size = summary.base_limit;
                self.joined = Some(summary);
                self.open_instance()
            }
            VfsEvent::Instance(_) => {
                let head = self.head.clone().ok_or(VfsError::NotFound)?;
                let life_id = head.life_id.ok_or(VfsError::NotFound)?;
                self.identity = Some(HandleIdentity::File(FileIdentity::Committed { life_id }));
                let version = VersionKey::new(self.bucket(), &self.input.key, head.version_id);
                let read = (
                    BLOB_VERSIONS_KEYSPACE.to_string(),
                    Key::from(version.to_bytes()?),
                );
                self.call(OpenStep::Version, IoCall::Read(vec![read]))
            }
            other => Err(unexpected(&other)),
        }
    }

    fn version_read(&mut self, result: IoResult) -> OpenTurn {
        let [version] = aruna_core::rows::values::<1>(result)?;
        let version = version.ok_or(VfsError::StaleHandle)?;
        let version = BlobVersion::from_bytes(version.as_ref())?;
        let Some(location) = version.location_key() else {
            return self.open_instance();
        };
        let read = (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            Key::from(location.to_bytes()),
        );
        self.call(OpenStep::Location, IoCall::Read(vec![read]))
    }

    fn location_read(&mut self, result: IoResult) -> OpenTurn {
        let [location] = aruna_core::rows::values::<1>(result)?;
        let location = location.map(|value| BackendLocation::from_bytes(value.as_ref()));
        self.base_size = location
            .transpose()?
            .map_or(0, |location| location.blob_size);
        self.open_instance()
    }

    /// A new name binds a fresh watch token over its exact absent or marker predecessor.
    fn create(&mut self) -> OpenTurn {
        let protocol = self.input.context.protocol;
        let (parent, name) =
            super::path::split_parent(&self.input.key).ok_or(VfsError::InvalidName)?;
        check_component(name, protocol, NameUse::Create, parent.is_empty())?;
        if !self.input.write {
            return Err(VfsError::Denied);
        }
        let token = self.ids.mint(self.now_ms);
        let predecessor = self.head.as_ref().map(|head| head.version_id);
        let install =
            WatchInstallOperation::new(&self.bucket(), &self.input.key, token, predecessor);
        self.identity = Some(HandleIdentity::File(FileIdentity::Unpublished {
            token,
            predecessor,
        }));
        let seed = token.to_bytes();
        self.call(OpenStep::Watch, IoCall::retry(install, &seed))
    }

    fn open_instance(&mut self) -> OpenTurn {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let identity = self.identity.clone().ok_or(VfsError::NotFound)?;
        let owner = self.input.write.then(|| StageOwner {
            user: grant.owner,
            access_key: self.input.context.access_key.clone(),
            mount_id: self.input.mount_id,
            scope: self.input.stage_scope.clone(),
        });
        let request = OpenRequest {
            scope: self.scope()?,
            key: self.input.key.clone(),
            kind: self.input.kind,
            handle_id: self.joined.as_ref().and_then(|summary| summary.handle_id),
            identity,
            open_id: self.input.open_id,
            protocol: self.input.context.protocol,
            share: self.input.share,
            delete_on_close: self.input.delete_on_close,
            owner,
            base_size: self.base_size,
            group_id: grant.bucket.group_id,
        };
        self.call(OpenStep::Opened, vfs_call(VfsEffect::OpenInstance(request)))
    }

    fn instance_opened(&mut self, result: IoResult) -> OpenTurn {
        let (instance_id, created, size, change) = match vfs_event(result)? {
            VfsEvent::Opened {
                instance_id,
                created,
                size,
                change,
            } => (instance_id, created, size, change),
            other => return Err(unexpected(&other)),
        };
        self.opened = Some((instance_id, created, size, change));
        let known = self.joined.as_ref().and_then(|summary| summary.handle_id);
        if !self.input.stable || known.is_some() {
            return self.finish();
        }
        // The permit is held, so the claim runs as a plain child, never through its own permit.
        let claim = HandleClaim {
            scope: self.scope()?,
            key: self.input.key.clone(),
            identity: self.identity.clone().ok_or(VfsError::NotFound)?,
            mode: self.input.mode,
        };
        let claim = ClaimHandleOperation::new(claim, change, self.now_ms);
        let seed = self.input.open_id.to_bytes();
        self.call(OpenStep::Handle, IoCall::retry(claim, &seed))
    }

    fn finish(&mut self) -> OpenTurn {
        let (instance_id, created, size, change) = self.opened.ok_or(VfsError::NotFound)?;
        let output = OpenResult {
            instance_id,
            identity: self.identity.clone().ok_or(VfsError::NotFound)?,
            handle: self.handle,
            size,
            created,
            change,
            grant: self.grant.clone().ok_or(VfsError::Denied)?,
        };
        self.opened = None;
        let calls = self.held.release();
        Ok(Next::Then { output, calls })
    }
}

impl Operation for VfsOpenOperation {
    type Output = OpenResult;
    type Error = VfsError;

    fn first(&mut self) -> OpenTurn {
        let access = match self.input.write || self.input.create {
            true => VfsAccess::Put,
            false => VfsAccess::Head,
        };
        let bucket = self.bucket();
        let request = VfsRequest::new(access, &bucket, Some(&self.input.key));
        let call = authorize_call(&self.input.context, request);
        self.call(OpenStep::Authorize, call)
    }

    fn next(&mut self, result: IoResult) -> OpenTurn {
        match self.step {
            OpenStep::Authorize => {
                self.grant = Some(grant_of(result)?);
                let scope = self.scope()?;
                self.call(
                    OpenStep::Permit,
                    vfs_call(VfsEffect::AcquirePermit { scope }),
                )
            }
            OpenStep::Permit => {
                self.held.permit_taken(result)?;
                let (bucket, key) = (self.bucket(), self.input.key.clone());
                let mut reads = name_reads(&bucket, &key)?;
                reads.push((
                    BLOB_HEAD_KEYSPACE.to_string(),
                    super::head_key(&bucket, &marker_key(&key))?,
                ));
                self.call(OpenStep::Rows, IoCall::Read(reads))
            }
            OpenStep::Rows => self.rows_read(result),
            OpenStep::Probe => self.probed(result),
            OpenStep::Staged => self.staged_read(result),
            OpenStep::Instance => self.instance_read(result),
            OpenStep::Version => self.version_read(result),
            OpenStep::Location => self.location_read(result),
            OpenStep::Watch => {
                result.child::<(), VfsError>()??;
                self.open_instance()
            }
            OpenStep::Opened => self.instance_opened(result),
            OpenStep::Handle => {
                self.handle = Some(result.child::<HandleAllocation, VfsError>()??);
                self.call(OpenStep::Synced, IoCall::Sync)
            }
            OpenStep::Synced => {
                result.done()?;
                self.finish()
            }
        }
    }

    /// Releases the permit and closes an open this run registered but could not finish.
    fn abort(&mut self) -> Vec<IoCall> {
        let mut calls = Vec::new();
        if self.opened.take().is_some()
            && let Ok(scope) = self.scope()
        {
            let open_id = self.input.open_id;
            calls.push(vfs_call(VfsEffect::CloseOpen { scope, open_id }));
        }
        calls.extend(self.held.release());
        calls
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WatchStep {
    Read,
    Written,
}

/// Installs the watch token of a new name only while its head is still the exact absent or
/// marker predecessor and no other token holds the name.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WatchInstallOperation {
    pub bucket: String,
    pub key: String,
    pub token: Ulid,
    pub predecessor: Option<Ulid>,
    step: WatchStep,
}

impl WatchInstallOperation {
    pub(crate) fn new(bucket: &str, key: &str, token: Ulid, predecessor: Option<Ulid>) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            token,
            predecessor,
            step: WatchStep::Read,
        }
    }
}

impl Operation for WatchInstallOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<()>, VfsError> {
        Ok(Next::Call(IoCall::Read(name_reads(
            &self.bucket,
            &self.key,
        )?)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, VfsError> {
        if self.step == WatchStep::Written {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let [head, token] = aruna_core::rows::values::<2>(result)?;
        let head = head_of(head.as_ref())?;
        let same = head.as_ref().map(|head| head.version_id) == self.predecessor;
        if !same || live_life(head.as_ref()).is_some() || token_of(token.as_ref())?.is_some() {
            return Err(VfsError::Exists);
        }
        self.step = WatchStep::Written;
        let row = watch_row(&self.bucket, &self.key, self.token)?;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// One read of a file's effective image.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadInput {
    pub context: VfsAccessContext,
    pub key: String,
    /// The shared instance whose stage overlays the base; `None` reads the committed head.
    pub instance_id: Option<u64>,
    /// The exact version the base bytes come from.
    pub version_id: Option<Ulid>,
    pub offset: u64,
    pub length: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadResult {
    pub data: Vec<u8>,
    pub eof: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadStep {
    Authorize,
    Unlocked,
    Stage,
    Base,
    Collect,
}

type ReadTurn = Result<Next<ReadResult>, VfsError>;

/// Reads one range: live `s3.GetObject` admission, a live unlock of a `vault_locked` bucket,
/// staged slices and exact-version base bytes, checked on every read and never cached.
#[derive(Debug, PartialEq)]
pub struct VfsReadOperation {
    input: ReadInput,
    step: ReadStep,
    grant: Option<VfsGrant>,
    slices: Vec<StageSlice>,
    parts: Vec<Option<Vec<u8>>>,
    pending: Vec<(usize, ByteRange)>,
    size: u64,
}

impl VfsReadOperation {
    pub fn new(input: ReadInput) -> Self {
        Self {
            input,
            step: ReadStep::Authorize,
            grant: None,
            slices: Vec::new(),
            parts: Vec::new(),
            pending: Vec::new(),
            size: u64::MAX,
        }
    }

    fn call(&mut self, step: ReadStep, call: IoCall) -> ReadTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn authorized(&mut self, result: IoResult) -> ReadTurn {
        let grant = grant_of(result)?;
        let locked = grant.encryption.mode == EncryptionMode::VaultLocked;
        let bucket_id = grant.encryption.bucket_id;
        self.grant = Some(grant);
        match (locked, bucket_id) {
            (true, Some(bucket_id)) => {
                let status = Effect::blob(BlobEffect::ReadKeyStatus { bucket_id });
                self.call(ReadStep::Unlocked, status.into())
            }
            (true, None) => Err(VfsError::VaultLocked),
            (false, _) => self.read_stage(),
        }
    }

    /// A `vault_locked` bucket serves reads only while its active key is unlocked here.
    fn unlocked(&mut self, result: IoResult) -> ReadTurn {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let active = grant.encryption.active_key();
        match result.blob()? {
            BlobEvent::KeyStatus { generations }
                if generations
                    .iter()
                    .any(|status| Some(status.key) == active && status.active) =>
            {
                self.read_stage()
            }
            _ => Err(VfsError::VaultLocked),
        }
    }

    fn read_stage(&mut self) -> ReadTurn {
        let (offset, length) = (self.input.offset, self.input.length);
        let Some(instance_id) = self.input.instance_id else {
            let base = StageSlice::Base {
                offset,
                length: u64::from(length),
            };
            return self.plan(vec![base]);
        };
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let scope = BucketScope::new(bucket, grant.incarnation);
        let read = VfsEffect::ReadStage {
            scope,
            instance_id,
            offset,
            length,
        };
        self.call(ReadStep::Stage, vfs_call(read))
    }

    fn stage_read(&mut self, result: IoResult) -> ReadTurn {
        match vfs_event(result)? {
            VfsEvent::StageRead {
                slices,
                logical_size,
                ..
            } => {
                self.size = logical_size;
                self.plan(slices)
            }
            other => Err(unexpected(&other)),
        }
    }

    fn plan(&mut self, slices: Vec<StageSlice>) -> ReadTurn {
        self.parts = vec![None; slices.len()];
        for (index, slice) in slices.iter().enumerate() {
            if let StageSlice::Base { offset, length } = slice {
                let range = ByteRange {
                    offset: *offset,
                    length: *length,
                };
                self.pending
                    .extend(chunked(&[range]).into_iter().map(|chunk| (index, chunk)));
            }
        }
        self.slices = slices;
        self.next_base()
    }

    fn next_base(&mut self) -> ReadTurn {
        let Some((_, range)) = self.pending.first().copied() else {
            return self.assemble();
        };
        let version_id = self.input.version_id.ok_or(VfsError::StaleHandle)?;
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let call = range_read(
            &self.input.context,
            grant,
            &self.input.key,
            version_id,
            range,
        );
        self.call(ReadStep::Base, call)
    }

    fn base_read(&mut self, result: IoResult) -> ReadTurn {
        let (_, range) = self.pending.first().copied().ok_or(VfsError::Conflict)?;
        match collect_call(result, range.length)? {
            Some(call) => self.call(ReadStep::Collect, call),
            // Past the end of the base: nothing more is there.
            None => {
                self.pending.clear();
                self.assemble()
            }
        }
    }

    fn base_collected(&mut self, result: IoResult) -> ReadTurn {
        let bytes = collected(result)?;
        let (index, range) = self.pending.remove(0);
        let short = (bytes.len() as u64) < range.length;
        let part = self.parts[index].get_or_insert_with(Vec::new);
        part.extend_from_slice(&bytes);
        if short {
            self.pending.retain(|(at, _)| *at != index);
        }
        self.next_base()
    }

    fn assemble(&mut self) -> ReadTurn {
        let mut data = Vec::new();
        for (index, slice) in std::mem::take(&mut self.slices).into_iter().enumerate() {
            match slice {
                StageSlice::Written { data: bytes, .. } => data.extend_from_slice(&bytes),
                StageSlice::Zero { length, .. } => {
                    data.resize(data.len() + usize::try_from(length).unwrap_or(0), 0)
                }
                StageSlice::Base { .. } => {
                    let Some(part) = self.parts.get_mut(index).and_then(Option::take) else {
                        break;
                    };
                    data.extend_from_slice(&part);
                }
            }
        }
        let end = self.input.offset.saturating_add(data.len() as u64);
        let eof = end >= self.size || data.len() < self.input.length as usize;
        Ok(Next::Done(ReadResult { data, eof }))
    }
}

impl Operation for VfsReadOperation {
    type Output = ReadResult;
    type Error = VfsError;

    fn first(&mut self) -> ReadTurn {
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let mut request = VfsRequest::new(VfsAccess::Get, bucket, Some(&self.input.key));
        request.version_id = self.input.version_id;
        let call = authorize_call(&self.input.context, request);
        self.call(ReadStep::Authorize, call)
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        match self.step {
            ReadStep::Authorize => self.authorized(result),
            ReadStep::Unlocked => self.unlocked(result),
            ReadStep::Stage => self.stage_read(result),
            ReadStep::Base => self.base_read(result),
            ReadStep::Collect => self.base_collected(result),
        }
    }
}

/// A staged write or resize of one shared instance.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteInput {
    pub context: VfsAccessContext,
    pub key: String,
    pub instance_id: u64,
    pub owner_id: u64,
    pub change: StageChange,
    pub stable: WriteStable,
    /// A size change with no active writer publishes before it succeeds.
    pub anonymous: bool,
    /// The durable checkpoint a stable NFS change writes; SMB leaves it out.
    pub checkpoint: Option<CommitInput>,
}

/// The byte change itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageChange {
    Write {
        offset: u64,
        data: Vec<u8>,
        append: bool,
    },
    Truncate {
        size: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteResult {
    pub written: u32,
    pub new_size: u64,
    pub change: u64,
    pub verifier: [u8; 8],
    /// The change is durable in a checkpoint.
    pub committed: bool,
    /// The frontend publishes before it reports success.
    pub publish: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteStep {
    Instance,
    Authorize,
    Guard,
    Wait,
    Staged,
    Released,
    Commit,
    Verifier,
}

type WriteTurn = Result<Next<WriteResult>, VfsError>;

/// Writes or resizes the stage of one instance under its file guard, after live
/// `s3.PutObject` admission with the resulting logical length.
#[derive(Debug, PartialEq)]
pub struct VfsWriteOperation {
    input: WriteInput,
    step: WriteStep,
    grant: Option<VfsGrant>,
    held: Held,
    result: Option<WriteResult>,
}

/// A resize is a write of no bytes; see [`VfsWriteOperation`].
pub type VfsTruncateOperation = VfsWriteOperation;

impl VfsWriteOperation {
    pub fn new(input: WriteInput) -> Self {
        Self {
            input,
            step: WriteStep::Instance,
            grant: None,
            held: Held::default(),
            result: None,
        }
    }

    fn call(&mut self, step: WriteStep, call: IoCall) -> WriteTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        let bucket = (self.input.context.bucket_scope()).ok_or(VfsError::Denied)?;
        match &self.grant {
            Some(grant) => Ok(BucketScope::new(bucket.0, grant.incarnation)),
            None => Ok(BucketScope::new(bucket.0, bucket.1)),
        }
    }

    /// The logical length the change leaves, checked as the write's content length.
    fn instance_read(&mut self, result: IoResult) -> WriteTurn {
        let summary = match vfs_event(result)? {
            VfsEvent::Instance(Some(summary)) => summary,
            VfsEvent::Instance(None) => return Err(VfsError::StaleHandle),
            other => return Err(unexpected(&other)),
        };
        if summary.instance_id != self.input.instance_id || summary.delete_pending {
            return Err(VfsError::DeletePending);
        }
        let length = match &self.input.change {
            StageChange::Write {
                data, append: true, ..
            } => summary.logical_size.checked_add(data.len() as u64),
            StageChange::Write { offset, data, .. } => offset
                .checked_add(data.len() as u64)
                .map(|end| end.max(summary.logical_size)),
            StageChange::Truncate { size } => Some(*size),
        };
        let length = length.ok_or(VfsError::Vfs(aruna_core::vfs_effects::VfsFailure::Overflow))?;
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let mut request = VfsRequest::new(VfsAccess::Put, bucket, Some(&self.input.key));
        request.length = Some(length);
        let call = authorize_call(&self.input.context, request);
        self.call(WriteStep::Authorize, call)
    }

    fn guard(&mut self) -> WriteTurn {
        let scope = self.scope()?;
        let instances = vec![self.input.instance_id];
        self.call(
            WriteStep::Guard,
            vfs_call(VfsEffect::AcquireGuards { scope, instances }),
        )
    }

    fn guarded(&mut self, result: IoResult) -> WriteTurn {
        // An unfinished namespace change is waited for without holding the guard.
        if let Some(attempt_id) = self.held.guard_taken(result)? {
            return self.call(
                WriteStep::Wait,
                vfs_call(VfsEffect::WaitAttempt { attempt_id }),
            );
        }
        let (scope, instance_id, owner_id) =
            (self.scope()?, self.input.instance_id, self.input.owner_id);
        let group_id = (self.grant.as_ref())
            .ok_or(VfsError::Denied)?
            .bucket
            .group_id;
        let effect = match self.input.change.clone() {
            StageChange::Write {
                offset,
                data,
                append,
            } => VfsEffect::WriteStage {
                scope,
                instance_id,
                owner_id,
                offset,
                data,
                append,
                group_id,
            },
            StageChange::Truncate { size } => VfsEffect::TruncateStage {
                scope,
                instance_id,
                owner_id,
                new_size: size,
                group_id,
            },
        };
        self.call(WriteStep::Staged, vfs_call(effect))
    }

    fn staged(&mut self, result: IoResult) -> WriteTurn {
        let (written, new_size, change) = match vfs_event(result)? {
            VfsEvent::StageWritten {
                written,
                new_size,
                change,
                ..
            } => (written, new_size, change),
            other => return Err(unexpected(&other)),
        };
        let resize = matches!(self.input.change, StageChange::Truncate { .. });
        self.result = Some(WriteResult {
            written,
            new_size,
            change,
            verifier: [0; 8],
            committed: false,
            publish: self.input.anonymous && resize,
        });
        let release = self.held.release();
        let release = release.into_iter().next().ok_or(VfsError::Conflict)?;
        self.call(WriteStep::Released, release)
    }

    /// A stable NFS write or resize is durable in a checkpoint before it is acknowledged.
    fn released(&mut self, result: IoResult) -> WriteTurn {
        vfs_event(result)?;
        let resize = matches!(self.input.change, StageChange::Truncate { .. });
        let durable = self.input.stable != WriteStable::Unstable || resize;
        match (durable, self.input.checkpoint.clone()) {
            (true, Some(mut checkpoint)) => {
                if let Some(result) = &self.result {
                    checkpoint.record.logical_size = result.new_size;
                }
                let commit = VfsCommitOperation::new(checkpoint);
                self.call(WriteStep::Commit, IoCall::child(commit))
            }
            _ => self.call(WriteStep::Verifier, vfs_call(VfsEffect::WriteVerifier)),
        }
    }

    fn finish(&mut self, verifier: [u8; 8], committed: bool) -> WriteTurn {
        let mut output = self.result.take().ok_or(VfsError::Conflict)?;
        output.verifier = verifier;
        output.committed = committed;
        Ok(Next::Done(output))
    }
}

impl Operation for VfsWriteOperation {
    type Output = WriteResult;
    type Error = VfsError;

    fn first(&mut self) -> WriteTurn {
        let scope = self.scope()?;
        let key = self.input.key.clone();
        self.call(
            WriteStep::Instance,
            vfs_call(VfsEffect::LookupInstance { scope, key }),
        )
    }

    fn next(&mut self, result: IoResult) -> WriteTurn {
        match self.step {
            WriteStep::Instance => self.instance_read(result),
            WriteStep::Authorize => {
                self.grant = Some(grant_of(result)?);
                self.guard()
            }
            WriteStep::Wait => {
                vfs_event(result)?;
                self.guard()
            }
            WriteStep::Guard => self.guarded(result),
            WriteStep::Staged => self.staged(result),
            WriteStep::Released => self.released(result),
            WriteStep::Commit => {
                let verifier = result.child::<[u8; 8], VfsError>()??;
                self.finish(verifier, true)
            }
            WriteStep::Verifier => match vfs_event(result)? {
                VfsEvent::Verifier { verifier } => self.finish(verifier, false),
                other => Err(unexpected(&other)),
            },
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        self.held.release()
    }
}

/// One durable checkpoint of an instance's stage.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitInput {
    pub context: VfsAccessContext,
    pub instance_id: u64,
    /// The stage row as it stands with the new generation filled in; its `base` names the
    /// exact version untouched ranges are copied from.
    pub record: NfsStageRecord,
    /// The generation the row named before, which stays until the new one is synced.
    pub previous: Option<NfsStageRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitStep {
    Authorize,
    Begin,
    Base,
    Collect,
    Fill,
    Seal,
    Row,
    Synced,
    Promoted,
    Verifier,
}

type CommitTurn = Result<Next<[u8; 8]>, VfsError>;

/// NFS `COMMIT`: a fresh immutable checkpoint generation with the missing base ranges copied
/// in bounded chunks, sealed and synced, its `06` row written and synced, then promoted.
#[derive(Debug, PartialEq)]
pub struct VfsCommitOperation {
    input: CommitInput,
    step: CommitStep,
    grant: Option<VfsGrant>,
    pending: Vec<ByteRange>,
    begun: bool,
    written: bool,
}

impl VfsCommitOperation {
    pub fn new(input: CommitInput) -> Self {
        Self {
            input,
            step: CommitStep::Authorize,
            grant: None,
            pending: Vec::new(),
            begun: false,
            written: false,
        }
    }

    fn call(&mut self, step: CommitStep, call: IoCall) -> CommitTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        Ok(BucketScope::new(bucket, grant.incarnation))
    }

    fn begin(&mut self) -> CommitTurn {
        let record = &self.input.record;
        let effect = VfsEffect::BeginCheckpoint {
            scope: self.scope()?,
            instance_id: self.input.instance_id,
            stage_id: record.stage_id,
            generation: record.generation,
            group_id: (self.grant.as_ref())
                .ok_or(VfsError::Denied)?
                .bucket
                .group_id,
        };
        self.begun = true;
        self.call(CommitStep::Begin, vfs_call(effect))
    }

    fn planned(&mut self, result: IoResult) -> CommitTurn {
        match vfs_event(result)? {
            VfsEvent::CheckpointPlan {
                missing_base,
                logical_size,
            } => {
                self.input.record.logical_size = logical_size;
                self.pending = chunked(&missing_base);
                self.next_base()
            }
            other => Err(unexpected(&other)),
        }
    }

    fn next_base(&mut self) -> CommitTurn {
        let Some(range) = self.pending.first().copied() else {
            let record = &self.input.record;
            let effect = VfsEffect::SealCheckpoint {
                scope: self.scope()?,
                instance_id: self.input.instance_id,
                stage_id: record.stage_id,
                generation: record.generation,
            };
            return self.call(CommitStep::Seal, vfs_call(effect));
        };
        let StageBase::Committed { version_id, .. } = self.input.record.base else {
            return Err(VfsError::CorruptStage);
        };
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let call = range_read(
            &self.input.context,
            grant,
            &self.input.record.key,
            version_id,
            range,
        );
        self.call(CommitStep::Base, call)
    }

    fn base_read(&mut self, result: IoResult) -> CommitTurn {
        let range = self.pending.first().copied().ok_or(VfsError::Conflict)?;
        match collect_call(result, range.length)? {
            Some(call) => self.call(CommitStep::Collect, call),
            None => Err(VfsError::StaleHandle),
        }
    }

    fn base_collected(&mut self, result: IoResult) -> CommitTurn {
        let data = collected(result)?;
        let range = self.pending.remove(0);
        if data.len() as u64 != range.length {
            return Err(VfsError::StaleHandle);
        }
        let record = &self.input.record;
        let fill = VfsEffect::FillCheckpoint {
            stage_id: record.stage_id,
            generation: record.generation,
            offset: range.offset,
            data,
        };
        self.call(CommitStep::Fill, vfs_call(fill))
    }

    fn sealed(&mut self, result: IoResult) -> CommitTurn {
        let (filename, size) = match vfs_event(result)? {
            VfsEvent::CheckpointWritten { filename, size } => (filename, size),
            other => return Err(unexpected(&other)),
        };
        self.input.record.checkpoint = filename;
        self.input.record.logical_size = size;
        let (record, previous) = (self.input.record.clone(), self.input.previous.clone());
        let write = StageWriteOperation::new(self.scope()?, record, previous);
        let seed = self.input.record.stage_id.to_bytes();
        self.call(CommitStep::Row, IoCall::retry(write, &seed))
    }

    fn promote(&mut self) -> CommitTurn {
        let record = &self.input.record;
        let effect = VfsEffect::PromoteCheckpoint {
            scope: self.scope()?,
            instance_id: self.input.instance_id,
            stage_id: record.stage_id,
            generation: record.generation,
            old_generation: self
                .input
                .previous
                .as_ref()
                .map(|previous| previous.generation),
        };
        self.call(CommitStep::Promoted, vfs_call(effect))
    }
}

impl Operation for VfsCommitOperation {
    type Output = [u8; 8];
    type Error = VfsError;

    fn first(&mut self) -> CommitTurn {
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let mut request = VfsRequest::new(VfsAccess::Get, bucket, Some(&self.input.record.key));
        if let StageBase::Committed { version_id, .. } = self.input.record.base {
            request.version_id = Some(version_id);
        }
        // An unpublished stage has no base to copy; its writer's own put grant suffices.
        if matches!(self.input.record.base, StageBase::Unpublished { .. }) {
            request = VfsRequest::new(VfsAccess::Put, bucket, Some(&self.input.record.key));
            request.length = Some(self.input.record.logical_size);
        }
        let call = authorize_call(&self.input.context, request);
        self.call(CommitStep::Authorize, call)
    }

    fn next(&mut self, result: IoResult) -> CommitTurn {
        match self.step {
            CommitStep::Authorize => {
                self.grant = Some(grant_of(result)?);
                self.begin()
            }
            CommitStep::Begin => self.planned(result),
            CommitStep::Base => self.base_read(result),
            CommitStep::Collect => self.base_collected(result),
            CommitStep::Fill => {
                vfs_event(result)?;
                self.next_base()
            }
            CommitStep::Seal => self.sealed(result),
            CommitStep::Row => {
                result.child::<(), VfsError>()??;
                self.written = true;
                self.call(CommitStep::Synced, IoCall::Sync)
            }
            CommitStep::Synced => {
                result.done()?;
                self.promote()
            }
            CommitStep::Promoted => {
                vfs_event(result)?;
                self.call(CommitStep::Verifier, vfs_call(VfsEffect::WriteVerifier))
            }
            CommitStep::Verifier => match vfs_event(result)? {
                VfsEvent::Verifier { verifier } => Ok(Next::Done(verifier)),
                other => Err(unexpected(&other)),
            },
        }
    }

    /// A generation no row names yet is removed; the last acknowledged one always stays.
    fn abort(&mut self) -> Vec<IoCall> {
        if !self.begun || self.written {
            return Vec::new();
        }
        let record = &self.input.record;
        let (stage_id, generation) = (record.stage_id, record.generation);
        vec![vfs_call(VfsEffect::RemoveCheckpoint {
            stage_id,
            generation,
        })]
    }
}

/// Points the `06` row at a new generation only while it still names the previous one.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StageWriteOperation {
    pub scope: BucketScope,
    pub record: NfsStageRecord,
    pub previous: Option<NfsStageRecord>,
    written: bool,
}

impl StageWriteOperation {
    pub(crate) fn new(
        scope: BucketScope,
        record: NfsStageRecord,
        previous: Option<NfsStageRecord>,
    ) -> Self {
        Self {
            scope,
            record,
            previous,
            written: false,
        }
    }

    fn key(&self) -> VfsKey {
        stage_key(&self.scope, &self.record)
    }
}

/// The row key of a stage: live by handle, retained by stage id.
pub(crate) fn stage_key(scope: &BucketScope, record: &NfsStageRecord) -> VfsKey {
    match record.disposition {
        aruna_core::structs::storage::vfs::StageDisposition::Retained { .. } => {
            VfsKey::RetainedStage(scope.clone(), record.stage_id)
        }
        _ => VfsKey::LiveStage(scope.clone(), record.handle_id),
    }
}

impl Operation for StageWriteOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<()>, VfsError> {
        let space = NFS_STATE_KEYSPACE.to_string();
        Ok(Next::Call(IoCall::Read(vec![(
            space,
            state_key(&self.key())?,
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, VfsError> {
        if self.written {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let [row] = aruna_core::rows::values::<1>(result)?;
        let current = record_of::<NfsStageRecord>(row.as_ref())?;
        if current != self.previous {
            return Err(VfsError::Conflict);
        }
        self.written = true;
        let row = state_row(&self.key(), &self.record)?;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// A byte-range lock request of one open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockRequest {
    Lock { locks: Vec<ByteLock>, atomic: bool },
    Unlock { locks: Vec<ByteLock> },
    Test { lock: ByteLock },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockOutcome {
    Done,
    Free,
    Conflict(ByteLock),
}

#[derive(Clone, Debug, PartialEq)]
pub struct LockInput {
    pub context: VfsAccessContext,
    pub key: String,
    pub instance_id: u64,
    /// The durable handle of a detached instance; its holder must still have an open.
    pub detached: Option<u64>,
    pub request: LockRequest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LockStep {
    Authorize,
    Detached,
    Lock,
}

/// Byte-range locks across both protocols, after live admission of the path; a detached
/// instance serves only the holders of its existing opens.
#[derive(Debug, PartialEq)]
pub struct VfsLockOperation {
    input: LockInput,
    step: LockStep,
    scope: Option<BucketScope>,
}

impl VfsLockOperation {
    pub fn new(input: LockInput) -> Self {
        Self {
            input,
            step: LockStep::Authorize,
            scope: None,
        }
    }

    fn lock_call(&mut self) -> Result<Next<LockOutcome>, VfsError> {
        let scope = self.scope.clone().ok_or(VfsError::Denied)?;
        let instance_id = self.input.instance_id;
        let effect = match self.input.request.clone() {
            LockRequest::Lock { locks, atomic } => VfsEffect::LockRanges {
                scope,
                instance_id,
                locks,
                atomic,
            },
            LockRequest::Unlock { locks } => VfsEffect::UnlockRanges {
                scope,
                instance_id,
                locks,
            },
            LockRequest::Test { lock } => VfsEffect::TestLock {
                scope,
                instance_id,
                lock,
            },
        };
        self.step = LockStep::Lock;
        Ok(Next::Call(vfs_call(effect)))
    }
}

impl Operation for VfsLockOperation {
    type Output = LockOutcome;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<LockOutcome>, VfsError> {
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let request = VfsRequest::new(VfsAccess::Head, bucket, Some(&self.input.key));
        Ok(Next::Call(authorize_call(&self.input.context, request)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LockOutcome>, VfsError> {
        match self.step {
            LockStep::Authorize => {
                let grant = grant_of(result)?;
                let bucket = (self.input.context.bucket_scope())
                    .ok_or(VfsError::Denied)?
                    .0;
                let scope = BucketScope::new(bucket, grant.incarnation);
                self.scope = Some(scope.clone());
                let Some(handle_id) = self.input.detached else {
                    return self.lock_call();
                };
                self.step = LockStep::Detached;
                let lookup = VfsEffect::LookupHandle { scope, handle_id };
                Ok(Next::Call(vfs_call(lookup)))
            }
            LockStep::Detached => match vfs_event(result)? {
                VfsEvent::Instance(Some(summary))
                    if summary.instance_id == self.input.instance_id
                        && detached_allows(&summary, HandleUse::ExistingOpen) =>
                {
                    self.lock_call()
                }
                VfsEvent::Instance(_) => Err(VfsError::StaleHandle),
                other => Err(unexpected(&other)),
            },
            LockStep::Lock => match vfs_event(result)? {
                VfsEvent::Done => Ok(Next::Done(LockOutcome::Done)),
                VfsEvent::LockFree => Ok(Next::Done(LockOutcome::Free)),
                VfsEvent::LockConflict { holder } => Ok(Next::Done(LockOutcome::Conflict(holder))),
                other => Err(unexpected(&other)),
            },
        }
    }
}

/// Inputs for setting or clearing SMB delete-pending disposition on an open link.
#[derive(Clone, Debug, PartialEq)]
pub struct DispositionInput {
    pub context: VfsAccessContext,
    pub key: String,
    pub kind: HandleKind,
    pub instance_id: u64,
    pub open_id: Ulid,
    pub pending: bool,
    pub owner: Option<StageOwner>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DispositionStep {
    Authorize,
    Permit,
    Staged,
    Probe,
    Apply,
}

/// Sets or clears delete-pending disposition after live delete authorization and, for directory
/// dispositions, an emptiness check over both staged names and raw heads.
#[derive(Debug, PartialEq)]
pub struct VfsDispositionOperation {
    input: DispositionInput,
    step: DispositionStep,
    grant: Option<VfsGrant>,
    held: Held,
    probe_after: Option<Key>,
}

impl VfsDispositionOperation {
    pub fn new(input: DispositionInput) -> Self {
        Self {
            input,
            step: DispositionStep::Authorize,
            grant: None,
            held: Held::default(),
            probe_after: None,
        }
    }

    fn scope(&self) -> Result<BucketScope, VfsError> {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        Ok(BucketScope::new(bucket, grant.incarnation))
    }

    fn apply_call(&mut self) -> Result<Next<()>, VfsError> {
        let scope = self.scope()?;
        self.step = DispositionStep::Apply;
        Ok(Next::Call(vfs_call(VfsEffect::SetDisposition {
            scope,
            instance_id: self.input.instance_id,
            open_id: self.input.open_id,
            pending: self.input.pending,
            owner: self.input.owner.clone(),
        })))
    }

    fn probe_call(&mut self) -> Result<Next<()>, VfsError> {
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let below = marker_key(&self.input.key);
        let prefix = BlobHeadKey::object_prefix(bucket, &below)
            .map_err(|error| VfsError::Storage(error.to_string()))?;
        self.step = DispositionStep::Probe;
        Ok(Next::Call(IoCall::Page {
            space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: self.probe_after.clone(),
            limit: STATE_PAGE,
        }))
    }
}

impl Operation for VfsDispositionOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<()>, VfsError> {
        let bucket = (self.input.context.bucket_scope())
            .ok_or(VfsError::Denied)?
            .0;
        let request = VfsRequest::new(VfsAccess::Delete, bucket, Some(&self.input.key));
        Ok(Next::Call(authorize_call(&self.input.context, request)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, VfsError> {
        match self.step {
            DispositionStep::Authorize => {
                self.grant = Some(grant_of(result)?);
                let scope = self.scope()?;
                self.step = DispositionStep::Permit;
                Ok(Next::Call(vfs_call(VfsEffect::AcquirePermit { scope })))
            }
            DispositionStep::Permit => {
                self.held.permit_taken(result)?;
                if !self.input.pending || self.input.kind != HandleKind::Directory {
                    return self.apply_call();
                }
                let scope = self.scope()?;
                let prefix = marker_key(&self.input.key);
                self.step = DispositionStep::Staged;
                Ok(Next::Call(vfs_call(VfsEffect::ListStaged {
                    scope,
                    prefix,
                })))
            }
            DispositionStep::Staged => {
                let entries = match vfs_event(result)? {
                    VfsEvent::StagedList { entries, .. } => entries,
                    other => return Err(unexpected(&other)),
                };
                let below = marker_key(&self.input.key);
                if entries
                    .iter()
                    .any(|entry| entry.key != below && entry.key.starts_with(&below))
                {
                    return Err(VfsError::NotEmpty);
                }
                self.probe_call()
            }
            DispositionStep::Probe => {
                let (rows, next) = result.page()?;
                let full = rows.len() >= STATE_PAGE;
                let mut last = None;
                let below = marker_key(&self.input.key);
                let bucket = (self.input.context.bucket_scope())
                    .ok_or(VfsError::Denied)?
                    .0;
                let marker_bytes = BlobHeadKey::new(bucket, &below)
                    .to_bytes()
                    .map_err(|error| VfsError::Storage(error.to_string()))?;
                for (key, value) in rows {
                    last = Some(key.clone());
                    if key.as_ref() == marker_bytes.as_slice() {
                        continue;
                    }
                    let head = head_of(Some(&value))?;
                    if live_life(head.as_ref()).is_some() {
                        return Err(VfsError::NotEmpty);
                    }
                }
                if next.is_some() && full {
                    self.probe_after = last;
                    return self.probe_call();
                }
                self.apply_call()
            }
            DispositionStep::Apply => match vfs_event(result)? {
                VfsEvent::Instance(Some(_)) => Ok(Next::Then {
                    output: (),
                    calls: self.held.release(),
                }),
                other => Err(unexpected(&other)),
            },
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        self.held.release()
    }
}

/// Identifies a live shared instance either by exact key or by durable NFS handle id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstanceTarget {
    Key(String),
    Handle(u64),
}

/// Looks up one live or detached shared instance without issuing raw VFS effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LookupInstanceOperation {
    scope: BucketScope,
    target: InstanceTarget,
}

impl LookupInstanceOperation {
    pub fn new(scope: BucketScope, target: InstanceTarget) -> Self {
        Self { scope, target }
    }
}

impl Operation for LookupInstanceOperation {
    type Output = Option<InstanceSummary>;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<Option<InstanceSummary>>, VfsError> {
        let effect = match &self.target {
            InstanceTarget::Key(key) => VfsEffect::LookupInstance {
                scope: self.scope.clone(),
                key: key.clone(),
            },
            InstanceTarget::Handle(handle_id) => VfsEffect::LookupHandle {
                scope: self.scope.clone(),
                handle_id: *handle_id,
            },
        };
        Ok(Next::Call(vfs_call(effect)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<InstanceSummary>>, VfsError> {
        match vfs_event(result)? {
            VfsEvent::Instance(summary) => Ok(Next::Done(summary)),
            other => Err(unexpected(&other)),
        }
    }
}

/// Sampled change counters together with the head-observer sequence token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeSnapshot {
    pub sequence: u64,
    pub values: Vec<u64>,
}

/// Samples current boot-fenced change counters for a set of keys without raw VFS effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadChangeOperation {
    scope: BucketScope,
    keys: Vec<String>,
}

impl ReadChangeOperation {
    pub fn new(scope: BucketScope, keys: Vec<String>) -> Self {
        Self { scope, keys }
    }
}

impl Operation for ReadChangeOperation {
    type Output = ChangeSnapshot;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<ChangeSnapshot>, VfsError> {
        Ok(Next::Call(vfs_call(VfsEffect::ReadChange {
            scope: self.scope.clone(),
            keys: self.keys.clone(),
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ChangeSnapshot>, VfsError> {
        match vfs_event(result)? {
            VfsEvent::ChangeSnapshot { sequence, values } => {
                Ok(Next::Done(ChangeSnapshot { sequence, values }))
            }
            other => Err(unexpected(&other)),
        }
    }
}
