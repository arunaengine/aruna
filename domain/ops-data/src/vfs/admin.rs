//! Admin rows of the file servers: enrolled NFS hosts, mount bindings and the retained stages
//! a bucket admin recovers. Revocations also end the live grants that depend on them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::io::stage_key;
use super::policy::{VfsAccess, VfsAccessContext, VfsGrant, VfsRequest, authorize_call, grant_of};
use super::publish::refusal;
use super::{
    STATE_PAGE, VfsError, record_of, state_delete, state_key, state_page, state_read, state_row,
    vfs_call, vfs_event,
};
use crate::s3::access::admit::{
    AdmitOwnerConfig, AdmitOwnerError, AdmitOwnerOperation, check_credential_usable,
    is_local_access,
};
use crate::s3::access::get::{GetAccessError, GetAccessOperation};
use crate::s3::bucket::key::rows::{authority_keys, parse_authority};
use crate::s3::object::put::abe::keyed;
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectOperation, PutObjectResult, RowFence,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{BUCKET_CREATION_KEYSPACE, NFS_STATE_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, incarnation};
use aruna_core::structs::storage::vfs::{
    BucketScope, EnrolledHostRecord, HOST_TAG, MOUNT_TAG, NfsMountBinding, NfsMountTarget,
    NfsStageRecord, RETAINED_STAGE, VfsKey,
};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::{VfsEffect, VfsEvent};
use std::net::IpAddr;
use std::time::{Duration, SystemTime};
use ulid::Ulid;

/// Whether `text` is a network in CIDR notation, such as `10.0.0.0/8` or `fd00::/8`.
pub fn valid_network(text: &str) -> bool {
    let Some((address, prefix)) = text.split_once('/') else {
        return false;
    };
    let (Ok(address), Ok(prefix)) = (address.parse::<IpAddr>(), prefix.parse::<u8>()) else {
        return false;
    };
    match address {
        IpAddr::V4(_) => prefix <= 32,
        IpAddr::V6(_) => prefix <= 128,
    }
}

fn check_networks(networks: &[String]) -> Result<(), VfsError> {
    match networks.iter().all(|network| valid_network(network)) {
        true => Ok(()),
        false => Err(VfsError::InvalidName),
    }
}

/// A host's proof of possession must name one of its accepted certificate keys.
fn check_proof(fingerprints: &[[u8; 32]], proven: &[u8; 32]) -> Result<(), VfsError> {
    match fingerprints.contains(proven) {
        true => Ok(()),
        false => Err(VfsError::Denied),
    }
}

/// The host row of `host_id`, live and owned by `caller`.
fn owned_host(value: Option<&Value>, caller: UserId) -> Result<EnrolledHostRecord, VfsError> {
    let host = record_of::<EnrolledHostRecord>(value)?.ok_or(VfsError::NotFound)?;
    if host.revoked_at_ms.is_some() {
        return Err(VfsError::NotFound);
    }
    match host.owner == caller {
        true => Ok(host),
        false => Err(VfsError::Denied),
    }
}

fn revoke_call(host_id: Option<Ulid>, mount_id: Option<Ulid>) -> IoCall {
    vfs_call(VfsEffect::RevokeGrant {
        host_id,
        mount_id,
        access_key: None,
    })
}

/// A new NFS client host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollInput {
    pub owner: UserId,
    pub fingerprints: Vec<[u8; 32]>,
    /// The key whose possession the API verified with a signed challenge.
    pub proven: [u8; 32],
    pub allowed_networks: Vec<String>,
    pub worker_pool: bool,
}

/// Enrolls a host for its verified owner after the proof of possession; one transaction
/// writes its row under a fresh host id.
#[derive(Debug, PartialEq)]
pub struct EnrollHostOperation {
    input: EnrollInput,
    ids: IdMint,
    now_ms: u64,
    host: Option<EnrolledHostRecord>,
    written: bool,
}

impl EnrollHostOperation {
    pub fn new(input: EnrollInput) -> Self {
        Self {
            input,
            ids: IdMint::default(),
            now_ms: 0,
            host: None,
            written: false,
        }
    }
}

impl Operation for EnrollHostOperation {
    type Output = EnrolledHostRecord;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<EnrolledHostRecord>, VfsError> {
        check_proof(&self.input.fingerprints, &self.input.proven)?;
        check_networks(&self.input.allowed_networks)?;
        let host_id = self.ids.mint(self.now_ms);
        let host = EnrolledHostRecord {
            host_id,
            owner: self.input.owner,
            fingerprints: self.input.fingerprints.clone(),
            allowed_networks: self.input.allowed_networks.clone(),
            worker_pool: self.input.worker_pool,
            created_at_ms: self.now_ms,
            revoked_at_ms: None,
        };
        self.host = Some(host);
        Ok(Next::Call(state_read(&[VfsKey::Host(host_id)])?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<EnrolledHostRecord>, VfsError> {
        if self.written {
            result.done()?;
            return self.host.take().map(Next::Done).ok_or(VfsError::Conflict);
        }
        let [row] = aruna_core::rows::values::<1>(result)?;
        if row.is_some() {
            return Err(VfsError::Exists);
        }
        let host = self.host.as_ref().ok_or(VfsError::Conflict)?;
        self.written = true;
        let write = state_row(&VfsKey::Host(host.host_id), host)?;
        Ok(Next::Call(IoCall::Write(vec![write])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

/// What a host change does to the row of its verified owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostChange {
    /// New certificate keys, proven by one of them; established connections are revoked.
    Rotate {
        fingerprints: Vec<[u8; 32]>,
        proven: [u8; 32],
    },
    /// Ends the host and every connection and grant it holds.
    Revoke,
}

/// Changes a host for its verified owner only, then revokes the live grants it held.
#[derive(Debug, PartialEq)]
pub struct HostChangeOperation {
    host_id: Ulid,
    caller: UserId,
    change: HostChange,
    now_ms: u64,
    host: Option<EnrolledHostRecord>,
}

/// Rotates the certificate keys of a host; see [`HostChangeOperation`].
pub type RotateHostOperation = HostChangeOperation;
/// Revokes a host; see [`HostChangeOperation`].
pub type RevokeHostOperation = HostChangeOperation;

impl HostChangeOperation {
    pub fn new(host_id: Ulid, caller: UserId, change: HostChange) -> Self {
        Self {
            host_id,
            caller,
            change,
            now_ms: 0,
            host: None,
        }
    }
}

impl Operation for HostChangeOperation {
    type Output = EnrolledHostRecord;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<EnrolledHostRecord>, VfsError> {
        if let HostChange::Rotate {
            fingerprints,
            proven,
        } = &self.change
        {
            check_proof(fingerprints, proven)?;
        }
        Ok(Next::Call(state_read(&[VfsKey::Host(self.host_id)])?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<EnrolledHostRecord>, VfsError> {
        if let Some(host) = self.host.take() {
            result.done()?;
            let revoke = revoke_call(Some(host.host_id), None);
            return Ok(Next::Then {
                output: host,
                calls: vec![revoke],
            });
        }
        let [row] = aruna_core::rows::values::<1>(result)?;
        let mut host = owned_host(row.as_ref(), self.caller)?;
        match &self.change {
            HostChange::Rotate { fingerprints, .. } => host.fingerprints = fingerprints.clone(),
            HostChange::Revoke => host.revoked_at_ms = Some(self.now_ms),
        }
        let write = state_row(&VfsKey::Host(self.host_id), &host)?;
        self.host = Some(host);
        Ok(Next::Call(IoCall::Write(vec![write])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

/// Reads one host or one mount of its owner.
#[derive(Debug, PartialEq)]
pub struct GetHostOperation {
    host_id: Ulid,
    caller: UserId,
}

impl GetHostOperation {
    pub fn new(host_id: Ulid, caller: UserId) -> Self {
        Self { host_id, caller }
    }
}

impl Operation for GetHostOperation {
    type Output = EnrolledHostRecord;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<EnrolledHostRecord>, VfsError> {
        Ok(Next::Call(state_read(&[VfsKey::Host(self.host_id)])?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<EnrolledHostRecord>, VfsError> {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let host = record_of::<EnrolledHostRecord>(row.as_ref())?.ok_or(VfsError::NotFound)?;
        match host.owner == self.caller {
            true => Ok(Next::Done(host)),
            false => Err(VfsError::NotFound),
        }
    }
}

/// One page of a caller's rows and where the next one starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminPage<T> {
    pub items: Vec<T>,
    pub next: Option<Ulid>,
}

/// Pages through the rows of one tag, keeping those `keep` admits; no fixed cap applies.
#[derive(Debug, PartialEq)]
pub struct OwnedRowsOperation<T> {
    tag: u8,
    caller: UserId,
    host_id: Option<Ulid>,
    after: Option<Ulid>,
    limit: usize,
    found: Vec<T>,
}

/// Lists a caller's hosts; see [`OwnedRowsOperation`].
pub type ListHostsOperation = OwnedRowsOperation<EnrolledHostRecord>;
/// Lists a caller's mounts, optionally of one host; see [`OwnedRowsOperation`].
pub type ListMountsOperation = OwnedRowsOperation<NfsMountBinding>;

/// The owner and host of a listed row.
pub trait OwnedRow: aruna_core::structs::storage::vfs::VfsRecord {
    const TAG: u8;
    fn owner(&self) -> UserId;
    fn host(&self) -> Ulid;
    fn row_id(&self) -> Ulid;
}

impl OwnedRow for EnrolledHostRecord {
    const TAG: u8 = HOST_TAG;
    fn owner(&self) -> UserId {
        self.owner
    }
    fn host(&self) -> Ulid {
        self.host_id
    }
    fn row_id(&self) -> Ulid {
        self.host_id
    }
}

impl OwnedRow for NfsMountBinding {
    const TAG: u8 = MOUNT_TAG;
    fn owner(&self) -> UserId {
        self.owner
    }
    fn host(&self) -> Ulid {
        self.host_id
    }
    fn row_id(&self) -> Ulid {
        self.mount_id
    }
}

impl<T: OwnedRow> OwnedRowsOperation<T> {
    pub fn new(caller: UserId, host_id: Option<Ulid>, after: Option<Ulid>, limit: usize) -> Self {
        Self {
            tag: T::TAG,
            caller,
            host_id,
            after,
            limit: limit.clamp(1, STATE_PAGE),
            found: Vec::new(),
        }
    }

    fn page(&self) -> IoCall {
        let after = self
            .after
            .map(|id| Key::from([&[self.tag][..], &id.to_bytes()].concat()));
        state_page(vec![self.tag], after, self.limit)
    }
}

impl<T> Operation for OwnedRowsOperation<T>
where
    T: OwnedRow + Send + std::fmt::Debug + PartialEq + 'static,
{
    type Output = AdminPage<T>;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<AdminPage<T>>, VfsError> {
        Ok(Next::Call(self.page()))
    }

    /// Keeps reading pages until one holds a caller's row or the rows end.
    fn next(&mut self, result: IoResult) -> Result<Next<AdminPage<T>>, VfsError> {
        let (rows, next) = result.page()?;
        let full = rows.len() >= self.limit;
        for (_, value) in rows {
            let Some(row) = record_of::<T>(Some(&value))? else {
                continue;
            };
            self.after = Some(row.row_id());
            let host = self.host_id.is_none_or(|host_id| row.host() == host_id);
            if row.owner() == self.caller && host {
                self.found.push(row);
            }
        }
        let more = next.is_some() && full;
        if self.found.is_empty() && more {
            return Ok(Next::Call(self.page()));
        }
        Ok(Next::Done(AdminPage {
            items: std::mem::take(&mut self.found),
            next: more.then_some(self.after).flatten(),
        }))
    }
}

/// A new mount binding of an enrolled host.
#[derive(Clone, Debug, PartialEq)]
pub struct MountInput {
    /// The caller's live credential whose grants admit every request of the mount.
    pub context: VfsAccessContext,
    pub caller: UserId,
    pub host_id: Ulid,
    pub group_id: GroupId,
    /// Target bucket for bucket/folder mounts; empty when `job_id` is set.
    pub bucket: String,
    /// A folder prefix, canonicalized; `None` exports the whole bucket.
    pub prefix: Option<String>,
    /// Target job id for an immutable read-only job input view mount.
    pub job_id: Option<JobId>,
    pub read_only: bool,
    pub allowed_networks: Vec<String>,
    pub display_uid: u32,
    pub display_gid: u32,
    pub mode: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MountStep {
    Authorize,
    Access,
    Owner,
    Host,
    Written,
}

/// Binds a mount to its owner's host: the credential, owner and local ordinary bucket (or
/// read-only job input view) are admitted live, the target is canonical and the host is checked
/// again in the write.
#[derive(Debug, PartialEq)]
pub struct CreateMountOperation {
    input: MountInput,
    step: MountStep,
    ids: IdMint,
    now_ms: u64,
    binding: Option<NfsMountBinding>,
}

impl CreateMountOperation {
    pub fn new(input: MountInput) -> Self {
        Self {
            input,
            step: MountStep::Authorize,
            ids: IdMint::default(),
            now_ms: 0,
            binding: None,
        }
    }

    fn target(&self, incarnation: u128) -> Result<NfsMountTarget, VfsError> {
        if let Some(job_id) = self.input.job_id {
            return Ok(NfsMountTarget::JobInputView { job_id });
        }
        let bucket = &self.input.bucket;
        let target = match self.input.prefix.as_deref() {
            Some(prefix) => NfsMountTarget::for_folder(bucket, incarnation, prefix),
            None => NfsMountTarget::for_bucket(bucket, incarnation),
        };
        target.map_err(|_| VfsError::InvalidName)
    }

    fn start_host(&mut self, target: NfsMountTarget) -> Result<Next<NfsMountBinding>, VfsError> {
        self.binding = Some(NfsMountBinding {
            mount_id: self.ids.mint(self.now_ms),
            host_id: self.input.host_id,
            owner: self.input.caller,
            access_key: self.input.context.access_key.clone(),
            group_id: self.input.group_id,
            target,
            read_only: self.input.read_only,
            allowed_networks: self.input.allowed_networks.clone(),
            display_uid: self.input.display_uid,
            display_gid: self.input.display_gid,
            mode: self.input.mode,
            created_at_ms: self.now_ms,
        });
        self.step = MountStep::Host;
        Ok(Next::Call(state_read(&[VfsKey::Host(self.input.host_id)])?))
    }

    fn authorized(&mut self, grant: VfsGrant) -> Result<Next<NfsMountBinding>, VfsError> {
        if grant.owner != self.input.caller || grant.access.group_id != self.input.group_id {
            return Err(VfsError::Denied);
        }
        let target = self.target(grant.incarnation)?;
        self.start_host(target)
    }

    fn access_loaded(&mut self, result: IoResult) -> Result<Next<NfsMountBinding>, VfsError> {
        let access = match result.child::<UserAccess, GetAccessError>()? {
            Ok(access) => access,
            Err(GetAccessError::NotFound) => return Err(VfsError::Denied),
            Err(error) => return Err(VfsError::Storage(error.to_string())),
        };
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.input.context.now_ms);
        if !is_local_access(&access)
            || check_credential_usable(&access, now).is_err()
            || access.user_identity != self.input.caller
            || access.group_id != self.input.group_id
        {
            return Err(VfsError::Denied);
        }
        let owner = AdmitOwnerOperation::new(AdmitOwnerConfig {
            realm_id: self.input.context.realm_id,
            access_key: self.input.context.access_key.clone(),
            user_id: access.user_identity,
            issued_by: access.issued_by,
            revoke: false,
        });
        self.step = MountStep::Owner;
        Ok(Next::Call(IoCall::child(owner)))
    }
}

impl Operation for CreateMountOperation {
    type Output = NfsMountBinding;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<NfsMountBinding>, VfsError> {
        check_networks(&self.input.allowed_networks)?;
        if self.input.job_id.is_some() {
            if !self.input.read_only {
                return Err(VfsError::ReadOnly);
            }
            if !self.input.bucket.is_empty() || self.input.prefix.is_some() {
                return Err(VfsError::InvalidName);
            }
            self.step = MountStep::Access;
            let access = GetAccessOperation::new(self.input.context.access_key.clone());
            return Ok(Next::Call(IoCall::child(access)));
        }
        let bucket = self.input.bucket.clone();
        let mut request = VfsRequest::new(VfsAccess::List, &bucket, None);
        request.prefix = self.input.prefix.clone();
        Ok(Next::Call(authorize_call(&self.input.context, request)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NfsMountBinding>, VfsError> {
        match self.step {
            MountStep::Authorize => self.authorized(grant_of(result)?),
            MountStep::Access => self.access_loaded(result),
            MountStep::Owner => {
                match result.child::<(), AdmitOwnerError>()? {
                    Ok(()) => {}
                    Err(
                        AdmitOwnerError::IssuerUnknown
                        | AdmitOwnerError::Revoked
                        | AdmitOwnerError::Deactivated
                        | AdmitOwnerError::MalformedNode,
                    ) => return Err(VfsError::Denied),
                    Err(error) => {
                        return Err(VfsError::Storage(error.to_string()));
                    }
                }
                let target = self.target(0)?;
                self.start_host(target)
            }
            MountStep::Host => {
                let [row] = aruna_core::rows::values::<1>(result)?;
                owned_host(row.as_ref(), self.input.caller)?;
                let binding = self.binding.as_ref().ok_or(VfsError::Conflict)?;
                let write = state_row(&VfsKey::Mount(binding.mount_id), binding)?;
                // The host row is read in the same transaction, so a revocation conflicts.
                self.step = MountStep::Written;
                Ok(Next::Call(IoCall::Write(vec![write])))
            }
            MountStep::Written => {
                result.done()?;
                self.binding
                    .take()
                    .map(Next::Done)
                    .ok_or(VfsError::Conflict)
            }
        }
    }

    /// The authorization child runs on its own; the host read and the write share one
    /// transaction that opens at the host read.
    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

/// Reads or deletes one mount of its owner; a deletion revokes the mount's live state only.
#[derive(Debug, PartialEq)]
pub struct MountRowOperation {
    mount_id: Ulid,
    caller: UserId,
    delete: bool,
    found: Option<NfsMountBinding>,
}

/// Reads one mount; see [`MountRowOperation`].
pub type GetMountOperation = MountRowOperation;
/// Deletes one mount; see [`MountRowOperation`].
pub type DeleteMountOperation = MountRowOperation;

impl MountRowOperation {
    pub fn get(mount_id: Ulid, caller: UserId) -> Self {
        Self {
            mount_id,
            caller,
            delete: false,
            found: None,
        }
    }

    pub fn delete(mount_id: Ulid, caller: UserId) -> Self {
        Self {
            delete: true,
            ..Self::get(mount_id, caller)
        }
    }
}

impl Operation for MountRowOperation {
    type Output = NfsMountBinding;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<NfsMountBinding>, VfsError> {
        Ok(Next::Call(state_read(&[VfsKey::Mount(self.mount_id)])?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NfsMountBinding>, VfsError> {
        if let Some(binding) = self.found.take() {
            result.done()?;
            return Ok(Next::Then {
                output: binding,
                calls: vec![revoke_call(None, Some(self.mount_id))],
            });
        }
        let [row] = aruna_core::rows::values::<1>(result)?;
        let binding = record_of::<NfsMountBinding>(row.as_ref())?.ok_or(VfsError::NotFound)?;
        if binding.owner != self.caller {
            return Err(VfsError::NotFound);
        }
        if !self.delete {
            return Ok(Next::Done(binding));
        }
        let delete = state_delete(&VfsKey::Mount(self.mount_id))?;
        self.found = Some(binding);
        Ok(Next::Call(IoCall::Delete(vec![delete])))
    }

    fn transaction(&self) -> TransactionMode {
        match self.delete {
            true => TransactionMode::Write,
            false => TransactionMode::None,
        }
    }
}

/// A bucket admin's request on the retained stages of the current bucket incarnation.
#[derive(Clone, Debug, PartialEq)]
pub struct StageAdmin {
    /// The admin's live credential on the bucket, with the bucket export as its target.
    pub context: VfsAccessContext,
    pub bucket: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdminStep {
    Authorize,
    Authority,
    Work,
    Done,
}

/// Admits a bucket admin with permitted copy access on the bucket, then reads its authority.
#[derive(Debug, PartialEq)]
struct AdminGate {
    step: AdminStep,
    grant: Option<VfsGrant>,
}

impl AdminGate {
    fn new() -> Self {
        Self {
            step: AdminStep::Authorize,
            grant: None,
        }
    }

    fn first(admin: &StageAdmin) -> IoCall {
        let request = VfsRequest::new(VfsAccess::Copy, &admin.bucket, None);
        authorize_call(&admin.context, request)
    }

    /// The grant, then the bucket and authority rows that name its admins.
    fn authorized(&mut self, admin: &StageAdmin, result: IoResult) -> Result<IoCall, VfsError> {
        let grant = grant_of(result)?;
        let reads = authority_keys(&admin.bucket, admin.context.realm_id, grant.bucket.group_id);
        self.grant = Some(grant);
        self.step = AdminStep::Authority;
        Ok(IoCall::Read(reads))
    }

    fn admitted(&mut self, admin: &StageAdmin, result: IoResult) -> Result<BucketScope, VfsError> {
        let grant = self.grant.as_ref().ok_or(VfsError::Denied)?;
        let values = result.values()?;
        let state = parse_authority(
            keyed(&values),
            admin.context.realm_id,
            grant.bucket.group_id,
        )
        .map_err(|_| VfsError::Denied)?;
        if !state.admins.contains(&grant.owner) {
            return Err(VfsError::Denied);
        }
        self.step = AdminStep::Work;
        Ok(BucketScope::new(&admin.bucket, grant.incarnation))
    }
}

/// Lists the retained stages of the current bucket incarnation, a page at a time.
#[derive(Debug, PartialEq)]
pub struct ListStagesOperation {
    admin: StageAdmin,
    gate: AdminGate,
    after: Option<Ulid>,
    limit: usize,
}

impl ListStagesOperation {
    pub fn new(admin: StageAdmin, after: Option<Ulid>, limit: usize) -> Self {
        Self {
            admin,
            gate: AdminGate::new(),
            after,
            limit: limit.clamp(1, STATE_PAGE),
        }
    }
}

impl Operation for ListStagesOperation {
    type Output = AdminPage<NfsStageRecord>;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<Self::Output>, VfsError> {
        Ok(Next::Call(AdminGate::first(&self.admin)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, VfsError> {
        match self.gate.step {
            AdminStep::Authorize => Ok(Next::Call(self.gate.authorized(&self.admin, result)?)),
            AdminStep::Authority => {
                let scope = self.gate.admitted(&self.admin, result)?;
                let prefix = VfsKey::stage_prefix(&scope, RETAINED_STAGE)?;
                let after = self
                    .after
                    .map(|id| VfsKey::RetainedStage(scope.clone(), id));
                let after = after.map(|key| state_key(&key)).transpose()?;
                Ok(Next::Call(state_page(prefix, after, self.limit)))
            }
            AdminStep::Work | AdminStep::Done => {
                let (rows, next) = result.page()?;
                let items = rows
                    .iter()
                    .map(|(_, value)| record_of::<NfsStageRecord>(Some(value)))
                    .collect::<Result<Vec<_>, _>>()?;
                let items: Vec<NfsStageRecord> = items.into_iter().flatten().collect();
                let next = next.and(items.last().map(|stage| stage.stage_id));
                Ok(Next::Done(AdminPage { items, next }))
            }
        }
    }
}

/// Deletes one retained stage the admin saw at `generation`, then its checkpoint file.
#[derive(Debug, PartialEq)]
pub struct DeleteStageOperation {
    admin: StageAdmin,
    gate: AdminGate,
    stage_id: Ulid,
    generation: u64,
    scope: Option<BucketScope>,
    deleted: bool,
}

impl DeleteStageOperation {
    pub fn new(admin: StageAdmin, stage_id: Ulid, generation: u64) -> Self {
        Self {
            admin,
            gate: AdminGate::new(),
            stage_id,
            generation,
            scope: None,
            deleted: false,
        }
    }
}

impl Operation for DeleteStageOperation {
    type Output = ();
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<()>, VfsError> {
        Ok(Next::Call(AdminGate::first(&self.admin)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, VfsError> {
        match self.gate.step {
            AdminStep::Authorize => Ok(Next::Call(self.gate.authorized(&self.admin, result)?)),
            AdminStep::Authority => {
                let scope = self.gate.admitted(&self.admin, result)?;
                let key = VfsKey::RetainedStage(scope.clone(), self.stage_id);
                self.scope = Some(scope);
                Ok(Next::Call(state_read(&[key])?))
            }
            AdminStep::Work => {
                let [row] = aruna_core::rows::values::<1>(result)?;
                let stage = record_of::<NfsStageRecord>(row.as_ref())?.ok_or(VfsError::NotFound)?;
                // The explicit conflict fence: the generation the admin saw.
                if stage.generation != self.generation {
                    return Err(VfsError::Conflict);
                }
                let scope = self.scope.clone().ok_or(VfsError::Denied)?;
                self.gate.step = AdminStep::Done;
                let delete = state_delete(&stage_key(&scope, &stage))?;
                self.deleted = true;
                Ok(Next::Call(IoCall::Delete(vec![delete])))
            }
            AdminStep::Done => {
                result.done()?;
                let (stage_id, generation) = (self.stage_id, self.generation);
                let remove = vfs_call(VfsEffect::RemoveCheckpoint {
                    stage_id,
                    generation,
                });
                Ok(Next::Then {
                    output: (),
                    calls: vec![remove],
                })
            }
        }
    }

    /// The authorization reads run on their own; the stage read and delete share one
    /// transaction that opens at the stage read.
    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Publishes one retained stage at its name or at a separate recovery destination.
#[derive(Debug, PartialEq)]
pub struct PublishStageInput {
    pub admin: StageAdmin,
    pub stage_id: Ulid,
    /// The generation the admin saw; another one refuses the publication.
    pub generation: u64,
    /// The head the admin saw at the destination: the explicit conflict fence.
    pub expected_head: Option<Ulid>,
    /// The object life the admin saw at the destination, when fencing against a newer life.
    pub expected_life: Option<Ulid>,
    /// A recovery key used instead of the stage's own name, so no newer life is overwritten.
    pub destination: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StagePublishStep {
    Gate,
    Stage,
    Target,
    Put,
    Synced,
}

/// Publishes a retained stage as an admin: target rights with the stage's length, quota and
/// placement through the ordinary write, and the retained row removed in its transaction.
#[derive(Debug, PartialEq)]
pub struct PublishStageOperation {
    input: PublishStageInput,
    put: Option<PutObjectConfig>,
    gate: AdminGate,
    step: StagePublishStep,
    scope: Option<BucketScope>,
    stage: Option<(NfsStageRecord, Value)>,
}

impl PublishStageOperation {
    /// `put` writes the checkpoint image the frontend streams to the destination key.
    pub fn new(input: PublishStageInput, put: PutObjectConfig) -> Self {
        Self {
            input,
            put: Some(put),
            gate: AdminGate::new(),
            step: StagePublishStep::Gate,
            scope: None,
            stage: None,
        }
    }

    fn destination(&self, stage: &NfsStageRecord) -> String {
        self.input
            .destination
            .clone()
            .unwrap_or_else(|| stage.key.clone())
    }

    fn stage_read(&mut self, result: IoResult) -> Result<Next<Ulid>, VfsError> {
        let [row] = aruna_core::rows::values::<1>(result)?;
        let value = row.ok_or(VfsError::NotFound)?;
        let stage = record_of::<NfsStageRecord>(Some(&value))?.ok_or(VfsError::NotFound)?;
        if stage.generation != self.input.generation {
            return Err(VfsError::Conflict);
        }
        let destination = self.destination(&stage);
        let mut request =
            VfsRequest::new(VfsAccess::Put, &self.input.admin.bucket, Some(&destination));
        request.length = Some(stage.logical_size);
        self.stage = Some((stage, value));
        self.step = StagePublishStep::Target;
        Ok(Next::Call(authorize_call(
            &self.input.admin.context,
            request,
        )))
    }

    fn target_admitted(&mut self, result: IoResult) -> Result<Next<Ulid>, VfsError> {
        let grant = grant_of(result)?;
        let (stage, value) = self.stage.clone().ok_or(VfsError::Conflict)?;
        let scope = self.scope.clone().ok_or(VfsError::Denied)?;
        let mut config = self.put.take().ok_or(VfsError::Conflict)?;
        if config.request.key != self.destination(&stage) {
            return Err(VfsError::InvalidName);
        }
        config.preassigned_version_id.get_or_insert(stage.stage_id);
        let key = stage_key(&scope, &stage);
        let guard = (
            NFS_STATE_KEYSPACE.to_string(),
            state_key(&key)?,
            Some(value),
        );
        let fence = RowFence::new(vec![guard], Vec::new()).with_deletes(vec![state_delete(&key)?]);
        let mut put = PutObjectOperation::new(config)
            .with_bucket_guard(grant.bucket)
            .with_preserve_envelope(true)
            .with_expected_head(self.input.expected_head)
            .with_row_fence(fence);
        if let Some(expected_life) = self.input.expected_life {
            put = put.with_expected_life(Some(expected_life));
        }
        self.step = StagePublishStep::Put;
        Ok(Next::Call(IoCall::child(put)))
    }
}

impl Operation for PublishStageOperation {
    type Output = Ulid;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<Ulid>, VfsError> {
        Ok(Next::Call(AdminGate::first(&self.input.admin)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Ulid>, VfsError> {
        match (self.step, self.gate.step) {
            (StagePublishStep::Gate, AdminStep::Authorize) => {
                Ok(Next::Call(self.gate.authorized(&self.input.admin, result)?))
            }
            (StagePublishStep::Gate, _) => {
                let scope = self.gate.admitted(&self.input.admin, result)?;
                let key = VfsKey::RetainedStage(scope.clone(), self.input.stage_id);
                self.scope = Some(scope);
                self.step = StagePublishStep::Stage;
                Ok(Next::Call(state_read(&[key])?))
            }
            (StagePublishStep::Stage, _) => self.stage_read(result),
            (StagePublishStep::Target, _) => self.target_admitted(result),
            (StagePublishStep::Put, _) => {
                match result.child::<PutObjectResult, PutObjectError>()? {
                    Ok(written) => {
                        self.step = StagePublishStep::Synced;
                        let (stage, _) = self.stage.as_ref().ok_or(VfsError::Conflict)?;
                        let remove = vfs_call(VfsEffect::RemoveCheckpoint {
                            stage_id: stage.stage_id,
                            generation: stage.generation,
                        });
                        Ok(Next::Then {
                            output: written.version_id,
                            calls: vec![IoCall::Sync, remove],
                        })
                    }
                    Err(error) => {
                        Err(refusal(&error).unwrap_or(VfsError::Unavailable(error.to_string())))
                    }
                }
            }
            (StagePublishStep::Synced, _) => {
                vfs_event(result)?;
                Err(VfsError::Conflict)
            }
        }
    }
}

/// Reads the live incarnation scope of a local bucket so frontends and REST handlers can bind
/// mounts or inspect retained stages without issuing raw storage effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketScopeOperation {
    bucket: String,
}

impl BucketScopeOperation {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
        }
    }
}

impl Operation for BucketScopeOperation {
    type Output = BucketScope;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<BucketScope>, VfsError> {
        let key = Key::from(self.bucket.as_bytes());
        Ok(Next::Call(IoCall::Read(vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (BUCKET_CREATION_KEYSPACE.to_string(), key),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BucketScope>, VfsError> {
        let [bucket, creation] = aruna_core::rows::values::<2>(result)?;
        let Some(bucket) = bucket else {
            return Err(VfsError::NotFound);
        };
        let info = BucketInfo::from_bytes(&bucket)
            .map_err(|error| VfsError::Storage(error.to_string()))?;
        if info.distribution.is_some() {
            return Err(VfsError::Distributed);
        }
        Ok(Next::Done(BucketScope::new(
            self.bucket.clone(),
            incarnation(creation.as_deref()),
        )))
    }
}

/// Reads one retained stage record for the current bucket incarnation as a bucket admin.
#[derive(Debug, PartialEq)]
pub struct GetStageOperation {
    admin: StageAdmin,
    stage_id: Ulid,
    gate: AdminGate,
    reading: bool,
}

impl GetStageOperation {
    pub fn new(admin: StageAdmin, stage_id: Ulid) -> Self {
        Self {
            admin,
            stage_id,
            gate: AdminGate::new(),
            reading: false,
        }
    }
}

impl Operation for GetStageOperation {
    type Output = NfsStageRecord;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<NfsStageRecord>, VfsError> {
        Ok(Next::Call(AdminGate::first(&self.admin)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NfsStageRecord>, VfsError> {
        if self.reading {
            let [row] = aruna_core::rows::values::<1>(result)?;
            let stage = record_of::<NfsStageRecord>(row.as_ref())?.ok_or(VfsError::NotFound)?;
            return Ok(Next::Done(stage));
        }
        match self.gate.step {
            AdminStep::Authorize => Ok(Next::Call(self.gate.authorized(&self.admin, result)?)),
            AdminStep::Authority => {
                let scope = self.gate.admitted(&self.admin, result)?;
                let key = VfsKey::RetainedStage(scope, self.stage_id);
                self.reading = true;
                Ok(Next::Call(state_read(&[key])?))
            }
            AdminStep::Work | AdminStep::Done => Err(VfsError::Conflict),
        }
    }
}

/// One bounded slice of a sealed checkpoint file returned by [`ReadCheckpointOperation`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointChunk {
    pub data: Vec<u8>,
    pub eof: bool,
}

/// Reads one bounded byte slice from a sealed live or retained checkpoint file through the file
/// server adapter so frontends can stream bodies without issuing raw VFS effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadCheckpointOperation {
    stage_id: Ulid,
    generation: u64,
    offset: u64,
    length: u32,
}

impl ReadCheckpointOperation {
    pub fn new(stage_id: Ulid, generation: u64, offset: u64, length: u32) -> Self {
        Self {
            stage_id,
            generation,
            offset,
            length,
        }
    }
}

impl Operation for ReadCheckpointOperation {
    type Output = CheckpointChunk;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<CheckpointChunk>, VfsError> {
        Ok(Next::Call(vfs_call(VfsEffect::ReadCheckpoint {
            stage_id: self.stage_id,
            generation: self.generation,
            offset: self.offset,
            length: self.length,
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CheckpointChunk>, VfsError> {
        match vfs_event(result)? {
            VfsEvent::CheckpointData { data, eof } => Ok(Next::Done(CheckpointChunk { data, eof })),
            other => Err(super::unexpected(&other)),
        }
    }
}

/// Finds the first unrevoked enrolled host carrying a certificate SPKI SHA-256 fingerprint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindHostOperation {
    fingerprint: [u8; 32],
    after: Option<Key>,
}

impl FindHostOperation {
    pub fn new(fingerprint: [u8; 32]) -> Self {
        Self {
            fingerprint,
            after: None,
        }
    }

    fn page(&self) -> IoCall {
        state_page(vec![HOST_TAG], self.after.clone(), STATE_PAGE)
    }
}

impl Operation for FindHostOperation {
    type Output = Option<EnrolledHostRecord>;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<Option<EnrolledHostRecord>>, VfsError> {
        Ok(Next::Call(self.page()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<EnrolledHostRecord>>, VfsError> {
        let (rows, next) = result.page()?;
        let full = rows.len() >= STATE_PAGE;
        for (key, value) in rows {
            self.after = Some(key);
            let Some(host) = record_of::<EnrolledHostRecord>(Some(&value))? else {
                continue;
            };
            if host.revoked_at_ms.is_none() && host.fingerprints.contains(&self.fingerprint) {
                return Ok(Next::Done(Some(host)));
            }
        }
        if next.is_none() || !full {
            return Ok(Next::Done(None));
        }
        Ok(Next::Call(self.page()))
    }
}

/// Reads one mount binding by mount id without requiring a REST admin caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LookupMountOperation {
    mount_id: Ulid,
}

impl LookupMountOperation {
    pub fn new(mount_id: Ulid) -> Self {
        Self { mount_id }
    }
}

impl Operation for LookupMountOperation {
    type Output = Option<NfsMountBinding>;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<Option<NfsMountBinding>>, VfsError> {
        Ok(Next::Call(state_read(&[VfsKey::Mount(self.mount_id)])?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<NfsMountBinding>>, VfsError> {
        let [row] = aruna_core::rows::values::<1>(result)?;
        Ok(Next::Done(record_of::<NfsMountBinding>(row.as_ref())?))
    }
}

/// Reads one live NFS stage row for `(scope, handle_id)` without raw storage effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveStageOperation {
    scope: BucketScope,
    handle_id: u64,
}

impl LiveStageOperation {
    pub fn new(scope: BucketScope, handle_id: u64) -> Self {
        Self { scope, handle_id }
    }
}

impl Operation for LiveStageOperation {
    type Output = Option<NfsStageRecord>;
    type Error = VfsError;

    fn first(&mut self) -> Result<Next<Option<NfsStageRecord>>, VfsError> {
        let key = VfsKey::LiveStage(self.scope.clone(), self.handle_id);
        Ok(Next::Call(state_read(&[key])?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<NfsStageRecord>>, VfsError> {
        let [row] = aruna_core::rows::values::<1>(result)?;
        Ok(Next::Done(record_of::<NfsStageRecord>(row.as_ref())?))
    }
}
