//! Resolves one export input to a local candidate and opens it again before its bytes are
//! read: its version, its location and the caller's right to read it, each in one read snapshot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::AuthorizationError;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::managed_copy::{
    CopyRequest, serve_reads, split_serve_reads, validate_registration,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values;
use aruna_core::structs::execution::job::ExportReportSource;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobVersion, BucketInfo, HashIndex, ManagedCopyKey, VersionKey,
    object_permission_path,
};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRef, ReadLease,
};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::{GroupId, Key, Value};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::replication::plaintext::{HolderCheckOperation, PlaintextError};

#[derive(Debug, PartialEq)]
pub(super) enum ResolveResult {
    Candidate(ExportCandidate),
    Denied,
    Missing { hash: Option<[u8; 32]> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ExportCandidate {
    pub(super) source: CandidateSource,
    pub(super) report_source: ExportReportSource,
    pub(super) resolved_version: Option<Ulid>,
    pub(super) expected_blake3: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum CandidateSource {
    Local {
        location: BackendLocation,
        group_id: GroupId,
        permission_path: String,
        node_id: NodeId,
        bucket: String,
        key: String,
    },
    RemoteExact {
        node_id: NodeId,
        target: VersionedObjectArn,
    },
    RemoteHash {
        node_id: NodeId,
        hash: [u8; 32],
    },
}

/// Why a local resolve could not answer.
#[derive(Debug, PartialEq)]
pub(super) enum ResolveError {
    Retryable(String),
    Permanent(String),
}

impl From<IoError> for ResolveError {
    fn from(error: IoError) -> Self {
        Self::Retryable(error.to_string())
    }
}

/// The local version a resolve reads.
#[derive(Debug, PartialEq)]
pub(super) enum LocalSource {
    /// An exact version, named by its ARN.
    Exact(VersionedObjectArn),
    /// A hash alias whose path the caller may already read.
    Alias(HashIndex),
}

#[derive(Debug, PartialEq)]
enum ResolveStep {
    Bucket,
    Authorization,
    Version,
    Location,
}

/// Resolves one [`LocalSource`] to a candidate, or to denied or missing. The bucket of an exact
/// version names its group; the caller's grants and the request policies of that group must
/// allow the read.
#[derive(Debug, PartialEq)]
pub(super) struct ResolveLocalOperation {
    auth: AuthContext,
    source: LocalSource,
    group_id: Option<GroupId>,
    permission_path: String,
    check: Option<CheckPermissionsConfig>,
    hash: Option<[u8; 32]>,
    step: ResolveStep,
}

type ResolveNext = Result<Next<ResolveResult>, ResolveError>;

fn retryable(error: impl std::fmt::Display) -> ResolveError {
    ResolveError::Retryable(error.to_string())
}

/// Missing authorization state denies; it must not retry the job.
fn denied(error: AuthorizationError) -> Result<bool, ResolveError> {
    match error {
        AuthorizationError::DocNotFound | AuthorizationError::GroupNotFound => Ok(false),
        error => Err(retryable(error)),
    }
}

impl ResolveLocalOperation {
    pub(super) fn new(auth: AuthContext, source: LocalSource) -> Self {
        let (group_id, permission_path) = match &source {
            LocalSource::Exact(_) => (None, String::new()),
            LocalSource::Alias(alias) => (Some(alias.group_id), alias.permission_path()),
        };
        Self {
            auth,
            source,
            group_id,
            permission_path,
            check: None,
            hash: None,
            step: ResolveStep::Bucket,
        }
    }

    fn missing(&self) -> ResolveNext {
        let hash = match self.source {
            LocalSource::Exact(_) => self.hash,
            LocalSource::Alias(_) => None,
        };
        Ok(Next::Done(ResolveResult::Missing { hash }))
    }

    fn version_key(&self) -> (&str, &str, Ulid) {
        match &self.source {
            LocalSource::Exact(exact) => (&exact.bucket, &exact.key, exact.version),
            LocalSource::Alias(alias) => (&alias.bucket, &alias.key, alias.version_id),
        }
    }

    fn bucket(&mut self, value: Option<Value>) -> ResolveNext {
        let LocalSource::Exact(exact) = &self.source else {
            return Err(ResolveError::Permanent("an alias reads no bucket".into()));
        };
        let Some(value) = value else {
            return Ok(Next::Done(ResolveResult::Missing { hash: None }));
        };
        let bucket = BucketInfo::from_bytes(value.as_ref()).map_err(retryable)?;
        let (realm_id, group_id) = (self.auth.realm_id, bucket.group_id);
        let path =
            object_permission_path(realm_id, group_id, exact.node_id, &exact.bucket, &exact.key);
        self.permission_path = path;
        self.group_id = Some(group_id);
        self.authorize()
    }

    /// Reads the caller's grants on the path and the request policies of its group at once.
    fn authorize(&mut self) -> ResolveNext {
        let check = CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: self.permission_path.clone(),
            required_permission: Permission::READ,
        };
        let mut reads = match check.reads() {
            Ok(reads) => reads,
            Err(error) => {
                denied(error)?;
                return Ok(Next::Done(ResolveResult::Denied));
            }
        };
        reads.extend(policy_reads(self.auth.realm_id, self.group_id));
        self.check = Some(check);
        self.step = ResolveStep::Authorization;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn authorized(&mut self, mut rows: Vec<Option<Value>>) -> ResolveNext {
        let check = self
            .check
            .take()
            .ok_or_else(|| retryable("the check was lost"))?;
        let split = check.reads().map_err(retryable)?.len();
        let policies = rows.split_off(split.min(rows.len()));
        let evaluator =
            read_policies(self.auth.realm_id, self.group_id, policies).map_err(retryable)?;
        let allowed = check.allowed(rows).or_else(denied)?;
        if !allowed {
            return Ok(Next::Done(ResolveResult::Denied));
        }
        let request = policy_request_with(
            &self.permission_path,
            &Permission::READ,
            Some(&self.auth),
            PolicyRequestExtras::operation("s3.GetObject"),
        );
        match evaluator.evaluate(&request) {
            Ok(()) => {}
            Err(PolicyEnforcementError::Denied { .. }) => {
                return Ok(Next::Done(ResolveResult::Denied));
            }
            Err(PolicyEnforcementError::Unavailable(error)) => return Err(retryable(error)),
        }
        let (bucket, key, version) = self.version_key();
        let key = VersionKey::new(bucket.to_string(), key.to_string(), version)
            .to_bytes()
            .map_err(|error| ResolveError::Permanent(error.to_string()))?;
        self.step = ResolveStep::Version;
        let row = (BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(key));
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn version(&mut self, value: Option<Value>) -> ResolveNext {
        let Some(value) = value else {
            return Ok(Next::Done(ResolveResult::Missing { hash: None }));
        };
        let version = BlobVersion::from_bytes(value.as_ref()).map_err(retryable)?;
        let hash = version.blob_hash().copied();
        let expected = match &self.source {
            LocalSource::Exact(_) => hash,
            LocalSource::Alias(alias) => Some(alias.blake3_hash),
        };
        if hash.is_none() || hash != expected {
            return Ok(Next::Done(ResolveResult::Missing { hash: None }));
        }
        self.hash = hash;
        let Some(location_key) = version.location_key() else {
            return self.missing();
        };
        self.step = ResolveStep::Location;
        let row = (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            Key::from(location_key.to_bytes()),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn location(&mut self, value: Option<Value>) -> ResolveNext {
        let Some(value) = value else {
            return self.missing();
        };
        let location = BackendLocation::from_bytes(value.as_ref()).map_err(retryable)?;
        let Some(hash) = self
            .hash
            .filter(|hash| location.get_blake3() == Some(hash.as_slice()))
        else {
            return self.missing();
        };
        let group_id = self
            .group_id
            .ok_or_else(|| retryable("the group was lost"))?;
        let permission_path = std::mem::take(&mut self.permission_path);
        let (node_id, bucket, key, version, report_source) = match &self.source {
            LocalSource::Exact(exact) => {
                let found = (exact.node_id, &exact.bucket, &exact.key, exact.version);
                (
                    found.0,
                    found.1,
                    found.2,
                    found.3,
                    ExportReportSource::Local,
                )
            }
            LocalSource::Alias(alias) => {
                let found = (alias.node_id, &alias.bucket, &alias.key, alias.version_id);
                (found.0, found.1, found.2, found.3, ExportReportSource::Hash)
            }
        };
        Ok(Next::Done(ResolveResult::Candidate(ExportCandidate {
            source: CandidateSource::Local {
                location,
                group_id,
                permission_path,
                node_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            report_source,
            resolved_version: Some(version),
            expected_blake3: Some(hash),
        })))
    }
}

impl Operation for ResolveLocalOperation {
    type Output = ResolveResult;
    type Error = ResolveError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> ResolveNext {
        let LocalSource::Exact(exact) = &self.source else {
            return self.authorize();
        };
        let row = (
            S3_BUCKET_KEYSPACE.to_string(),
            Key::from(exact.bucket.as_bytes().to_vec()),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> ResolveNext {
        match self.step {
            ResolveStep::Bucket => {
                let [value] = values::<1>(result)?;
                self.bucket(value)
            }
            ResolveStep::Authorization => {
                let IoResult::Values(rows) = result else {
                    return Err(IoError::Unexpected {
                        expected: "IoResult::Values",
                        got: result.label(),
                    }
                    .into());
                };
                self.authorized(rows)
            }
            ResolveStep::Version => {
                let [value] = values::<1>(result)?;
                self.version(value)
            }
            ResolveStep::Location => {
                let [value] = values::<1>(result)?;
                self.location(value)
            }
        }
    }
}

/// What opening a local candidate decided before its bytes are read.
#[derive(Debug, PartialEq)]
pub(super) enum LocalOpen {
    /// The version, its location or its bucket no longer match the candidate.
    Missing,
    Denied,
    /// An export into another realm needs a bucket key of this copy that the caller lacks.
    NotHolder,
    /// The bucket key the read needs is locked; the job waits for it.
    Locked(BucketKeyRef),
    /// The read may start, under this lease when the copy needs a bucket key.
    Admitted(Option<ReadLease>),
}

/// The local candidate one open checks again.
#[derive(Debug, PartialEq)]
pub(super) struct LocalCandidate {
    pub(super) location: BackendLocation,
    pub(super) group_id: GroupId,
    pub(super) permission_path: String,
    pub(super) node_id: NodeId,
    pub(super) bucket: String,
    pub(super) key: String,
    pub(super) blake3: [u8; 32],
    pub(super) version_id: Ulid,
}

#[derive(Debug, PartialEq)]
enum OpenStep {
    Bucket,
    Version,
    Location,
    Authorization,
    Serve,
    Encryption,
    Holder,
    Admission,
}

/// Checks a local candidate again in one read snapshot before its bytes are read: the bucket,
/// version and location still match, the caller may read the path, this node may still serve
/// a governed version, an export into another realm reads only as a key holder, and an
/// encrypting bucket admits the read under a lease.
#[derive(Debug, PartialEq)]
pub(super) struct OpenLocalOperation {
    auth: AuthContext,
    /// The export goes into a realm, so the holder rule applies.
    selection: bool,
    candidate: LocalCandidate,
    version: Option<BlobVersion>,
    check: Option<CheckPermissionsConfig>,
    copy_key: Option<ManagedCopyKey>,
    bucket_key: Option<BucketKeyRef>,
    step: OpenStep,
}

type OpenNext = Result<Next<LocalOpen>, ResolveError>;

impl OpenLocalOperation {
    pub(super) fn new(auth: AuthContext, selection: bool, candidate: LocalCandidate) -> Self {
        Self {
            auth,
            selection,
            candidate,
            version: None,
            check: None,
            copy_key: None,
            bucket_key: None,
            step: OpenStep::Bucket,
        }
    }

    fn version_key(&self) -> VersionKey {
        let candidate = &self.candidate;
        let (bucket, key) = (candidate.bucket.clone(), candidate.key.clone());
        VersionKey::new(bucket, key, candidate.version_id)
    }

    fn read(space: &str, key: impl Into<Key>) -> OpenNext {
        Ok(Next::Call(IoCall::Read(vec![(
            space.to_string(),
            key.into(),
        )])))
    }

    fn bucket(&mut self, value: Option<Value>) -> OpenNext {
        let Some(value) = value else {
            return Ok(Next::Done(LocalOpen::Missing));
        };
        let info = BucketInfo::from_bytes(value.as_ref()).map_err(retryable)?;
        let candidate = &self.candidate;
        let path = object_permission_path(
            self.auth.realm_id,
            candidate.group_id,
            candidate.node_id,
            &candidate.bucket,
            &candidate.key,
        );
        if info.group_id != candidate.group_id || candidate.permission_path != path {
            return Ok(Next::Done(LocalOpen::Missing));
        }
        let key = self
            .version_key()
            .to_bytes()
            .map_err(|error| ResolveError::Permanent(error.to_string()))?;
        self.step = OpenStep::Version;
        Self::read(BLOB_VERSIONS_KEYSPACE, key)
    }

    fn version(&mut self, value: Option<Value>) -> OpenNext {
        let Some(value) = value else {
            return Ok(Next::Done(LocalOpen::Missing));
        };
        let version = BlobVersion::from_bytes(value.as_ref()).map_err(retryable)?;
        let Some(location_key) = version.location_key() else {
            return Ok(Next::Done(LocalOpen::Missing));
        };
        if version.blob_hash() != Some(&self.candidate.blake3) {
            return Ok(Next::Done(LocalOpen::Missing));
        }
        self.version = Some(version);
        self.step = OpenStep::Location;
        Self::read(BLOB_LOCATIONS_KEYSPACE, location_key.to_bytes())
    }

    fn location(&mut self, value: Option<Value>) -> OpenNext {
        let Some(value) = value else {
            return Ok(Next::Done(LocalOpen::Missing));
        };
        let current = BackendLocation::from_bytes(value.as_ref()).map_err(retryable)?;
        let candidate = &self.candidate;
        if current.get_blake3() != Some(candidate.blake3.as_slice())
            || !current.same_object(&candidate.location)
        {
            return Ok(Next::Done(LocalOpen::Missing));
        }
        let check = CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: candidate.permission_path.clone(),
            required_permission: Permission::READ,
        };
        let mut reads = match check.reads() {
            Ok(reads) => reads,
            Err(error) => {
                denied(error)?;
                return Ok(Next::Done(LocalOpen::Denied));
            }
        };
        reads.extend(policy_reads(self.auth.realm_id, Some(candidate.group_id)));
        self.check = Some(check);
        self.step = OpenStep::Authorization;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn authorized(&mut self, mut rows: Vec<Option<Value>>) -> OpenNext {
        let check = self
            .check
            .take()
            .ok_or_else(|| retryable("the check was lost"))?;
        let split = check.reads().map_err(retryable)?.len();
        let policies = rows.split_off(split.min(rows.len()));
        let group_id = Some(self.candidate.group_id);
        let evaluator = read_policies(self.auth.realm_id, group_id, policies).map_err(retryable)?;
        if !check.allowed(rows).or_else(denied)? {
            return Ok(Next::Done(LocalOpen::Denied));
        }
        let request = policy_request_with(
            &check.path,
            &Permission::READ,
            Some(&self.auth),
            PolicyRequestExtras::operation("s3.GetObject"),
        );
        match evaluator.evaluate(&request) {
            Ok(()) => {}
            Err(PolicyEnforcementError::Denied { .. }) => {
                return Ok(Next::Done(LocalOpen::Denied));
            }
            Err(PolicyEnforcementError::Unavailable(error)) => return Err(retryable(error)),
        }
        self.serve()
    }

    /// A local governed read is a serve like any other: this node's own registration and
    /// subject must still allow it. An ungoverned version never consults the inventory.
    fn serve(&mut self) -> OpenNext {
        let governed = self
            .version
            .as_ref()
            .is_some_and(|version| !version.placement_policies.is_empty());
        if !governed {
            return self.encryption();
        }
        let backend = self.candidate.location.backend.clone();
        let key = ManagedCopyKey::new(self.version_key(), backend);
        let Ok(Effect::Storage(StorageEffect::BatchRead { reads, .. })) = serve_reads(&key, None)
        else {
            return Err(retryable("serve reads unavailable"));
        };
        self.copy_key = Some(key);
        self.step = OpenStep::Serve;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn served(&self, rows: Vec<Option<Value>>) -> bool {
        let (Some(key), Some(version)) = (self.copy_key.as_ref(), self.version.as_ref()) else {
            return false;
        };
        let rows = rows
            .into_iter()
            .map(|value| (Key::default(), value))
            .collect();
        let Ok((copy, subject)) = split_serve_reads(rows) else {
            return false;
        };
        let location = &self.candidate.location;
        let request = CopyRequest {
            key,
            node_id: Some(subject.subject.node_id),
            blake3: location.get_blake3().and_then(|hash| hash.try_into().ok()),
            refs: &version.placement_policies,
            subject_generation: Some(subject.subject.generation),
        };
        validate_registration(copy.as_deref(), &request).is_ok()
    }

    /// The bucket key the read needs: the copy's own key for a sealed copy, the active bucket
    /// key for a plain copy of an encrypting bucket, none for a plain bucket.
    fn encryption(&mut self) -> OpenNext {
        if let Some(key) = self.candidate.location.format.bucket_key() {
            return self.holder(Some(key));
        }
        self.step = OpenStep::Encryption;
        let bucket = self.candidate.bucket.as_bytes().to_vec();
        Self::read(BUCKET_ENCRYPTION_KEYSPACE, bucket)
    }

    /// An export into another realm reads a copy that needs a bucket key only as a current
    /// key holder of its bucket.
    fn holder(&mut self, key: Option<BucketKeyRef>) -> OpenNext {
        self.bucket_key = key;
        if !self.selection || key.is_none() {
            return self.admission();
        }
        self.step = OpenStep::Holder;
        let (realm_id, user_id) = (self.auth.realm_id, self.auth.user_id);
        let check = HolderCheckOperation::new(realm_id, &self.candidate.bucket, user_id);
        Ok(Next::Call(IoCall::child(check)))
    }

    /// An encrypting bucket admits plaintext only under a read lease.
    fn admission(&mut self) -> OpenNext {
        let Some(key) = self.bucket_key else {
            return Ok(Next::Done(LocalOpen::Admitted(None)));
        };
        self.step = OpenStep::Admission;
        let archive = ArchiveKey::of(&self.candidate.location);
        let admit = Effect::Blob(BlobEffect::AdmitRead { key, archive });
        Ok(Next::Call(IoCall::Effect(Box::new(admit))))
    }
}

impl Operation for OpenLocalOperation {
    type Output = LocalOpen;
    type Error = ResolveError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> OpenNext {
        let bucket = self.candidate.bucket.as_bytes().to_vec();
        Self::read(S3_BUCKET_KEYSPACE, bucket)
    }

    fn next(&mut self, result: IoResult) -> OpenNext {
        match self.step {
            OpenStep::Bucket => {
                let [value] = values::<1>(result)?;
                self.bucket(value)
            }
            OpenStep::Version => {
                let [value] = values::<1>(result)?;
                self.version(value)
            }
            OpenStep::Location => {
                let [value] = values::<1>(result)?;
                self.location(value)
            }
            OpenStep::Authorization => self.authorized(rows(result)?),
            OpenStep::Serve => match self.served(rows(result)?) {
                true => self.encryption(),
                false => Ok(Next::Done(LocalOpen::Denied)),
            },
            OpenStep::Encryption => {
                let [row] = values::<1>(result)?;
                let settings = BucketEncryption::from_row(row.as_deref()).map_err(retryable)?;
                self.holder(settings.active_key())
            }
            OpenStep::Holder => match result.child::<bool, PlaintextError>()? {
                Ok(true) => self.admission(),
                Ok(false) => Ok(Next::Done(LocalOpen::NotHolder)),
                Err(error) => Err(retryable(error)),
            },
            OpenStep::Admission => {
                let IoResult::Event(event) = result else {
                    return Err(retryable(format!(
                        "read admission failed: {}",
                        result.label()
                    )));
                };
                match *event {
                    Event::Blob(BlobEvent::ReadAdmitted { lease }) => {
                        Ok(Next::Done(LocalOpen::Admitted(Some(lease))))
                    }
                    Event::Blob(BlobEvent::Error(BlobError::BucketKey(
                        BucketKeyError::Locked(_),
                    ))) => {
                        let key = self
                            .bucket_key
                            .ok_or_else(|| retryable("the key was lost"))?;
                        Ok(Next::Done(LocalOpen::Locked(key)))
                    }
                    event => Err(retryable(format!(
                        "read admission failed: {}",
                        event.label()
                    ))),
                }
            }
        }
    }
}

fn rows(result: IoResult) -> Result<Vec<Option<Value>>, ResolveError> {
    match result {
        IoResult::Values(values) => Ok(values),
        other => Err(IoError::Unexpected {
            expected: "IoResult::Values",
            got: other.label(),
        }
        .into()),
    }
}

