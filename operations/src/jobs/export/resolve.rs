//! Resolves one export input to a local candidate: its version, its location and the caller's
//! right to read it, all in one read snapshot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values;
use aruna_core::structs::execution::job::ExportReportSource;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobVersion, BucketInfo, HashIndex, VersionKey, object_permission_path,
};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::{GroupId, Key, Value};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};

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
