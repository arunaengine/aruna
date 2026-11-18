//! Captures inputs this node holds: each is read like an S3 GET by the caller, pinned to an
//! exact version and granted for later exact reads, with its bucket's metadata and releases.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, NetEvent};
use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, value};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::{
    CapturedInput, InputContent, InputSelection, SubmissionId,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{
    BlobVersion, BlobVersionState, BucketInfo, VersionKey, bucket_permission_path,
};
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, CapturedDatabase, ReleaseRef};
use aruna_core::structs::storage::dataset_description::{DatasetDescription, DescriptionError};
use aruna_core::structs::storage::origin::SourcePin;
use aruna_core::structs::storage::read_grant::{
    GrantScope, HEAD_OPERATION, READ_OPERATION, ReadGrant, SignedGrant,
};
use aruna_core::structs::storage::release::MembershipQuery;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use ulid::Ulid;

use super::input_walk::{InputCaller, reference_of, source_parts};
use super::submission_rules::{admitted_description, reference_content, source_error};
use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError, check_error};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::jobs::read_access::{SourceObject, SourceReadError, SourceReadOperation};
use crate::jobs::submit::SubmitJobError;
use crate::s3::bucket::dataset::{
    BucketMetadataError, BucketMetadataOperation, DatasetSummaryOperation,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::bucket::release::access::MembersCheckOperation;
use crate::s3::bucket::release::{BucketScope, MembersOutput, MembershipCaller, ReleaseError};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};

/// Who captures inputs here, and what their grants and release checks need.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct CaptureCaller {
    pub(super) input: InputCaller,
    pub(super) submission_id: SubmissionId,
    pub(super) expires_at: u64,
    /// Lets range coordinators answer release membership.
    pub(super) bearer: Option<String>,
}

/// The blake3 of a version this node does not hold locally, from its replicated record.
#[derive(Debug, PartialEq)]
struct VersionHashOperation {
    key: Option<Key>,
}

impl VersionHashOperation {
    fn new(bucket: &str, key: &str, version: Ulid) -> Self {
        let key = VersionKey::new(bucket, key, version).to_bytes().ok();
        Self {
            key: key.map(Key::from),
        }
    }
}

impl Operation for VersionHashOperation {
    type Output = Option<[u8; 32]>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        Ok(match self.key.take() {
            Some(key) => Next::Call(read_row(BLOB_VERSIONS_KEYSPACE, key)),
            None => Next::Done(None),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        let version = value(result)?.and_then(|row| postcard::from_bytes::<BlobVersion>(&row).ok());
        Ok(Next::Done(version.and_then(
            |version| match version.state {
                BlobVersionState::Materialized { blob_hash, .. } => Some(blob_hash),
                _ => None,
            },
        )))
    }
}

/// Why the metadata a run captures could not be read.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
struct DatabaseError(String);

impl From<IoError> for DatabaseError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum DatabaseStep {
    Check,
    Metadata,
    Summary(BucketMetadata),
}

/// The bucket's metadata and dataset description when the caller may read the bucket itself, as
/// the metadata route checks; READ on one key alone shows none of it and the run still starts.
/// An unreadable dataset leaves the description empty; an unavailable one fails the admission.
#[derive(Debug, PartialEq)]
struct ReadableDatabaseOperation {
    auth: AuthContext,
    bucket: String,
    info: BucketInfo,
    bearer: Option<String>,
    check: CheckPermissionsConfig,
    step: DatabaseStep,
}

type DatabaseNext = Result<Next<Option<CapturedDatabase>>, DatabaseError>;

impl ReadableDatabaseOperation {
    fn new(caller: &CaptureCaller, bucket: &str, info: &BucketInfo) -> Self {
        let auth = caller.input.auth();
        let path = bucket_permission_path(auth.realm_id, info.group_id, caller.input.local, bucket);
        Self {
            check: CheckPermissionsConfig {
                auth_context: auth.clone(),
                path,
                required_permission: Permission::READ,
            },
            auth,
            bucket: bucket.to_string(),
            info: info.clone(),
            bearer: caller.bearer.clone(),
            step: DatabaseStep::Check,
        }
    }

    /// Only a refusal omits the metadata; unreadable state stays a retryable failure.
    fn refused(error: AuthorizeError) -> DatabaseNext {
        match error {
            AuthorizeError::PermissionDenied
            | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }) => Ok(Next::Done(None)),
            error => Err(DatabaseError(error.to_string())),
        }
    }

    fn rule_failure(error: AuthorizationError) -> DatabaseNext {
        Self::refused(check_error(error))
    }

    fn decide(&mut self, mut rows: Vec<Option<Value>>) -> DatabaseNext {
        let split = match self.check.reads() {
            Ok(reads) => reads.len(),
            Err(error) => return Self::rule_failure(error),
        };
        let policies = rows.split_off(split.min(rows.len()));
        match self.check.allowed(rows) {
            Ok(true) => {}
            Ok(false) => return Self::refused(AuthorizeError::PermissionDenied),
            Err(error) => return Self::rule_failure(error),
        }
        let group = Some(self.info.group_id);
        let verdict = read_policies(self.auth.realm_id, group, policies).and_then(|evaluator| {
            evaluator.evaluate(&policy_request_with(
                &self.check.path,
                &Permission::READ,
                Some(&self.auth),
                PolicyRequestExtras::rest(),
            ))
        });
        if let Err(error) = verdict {
            return Self::refused(error.into());
        }
        self.step = DatabaseStep::Metadata;
        let read = BucketMetadataOperation::new(&self.bucket, &self.info);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn described(&mut self, metadata: Option<BucketMetadata>) -> DatabaseNext {
        let Some(metadata) = metadata else {
            return Ok(Next::Done(None));
        };
        let Some(dataset) = metadata.dataset else {
            let database = CapturedDatabase::new(metadata, DatasetDescription::default());
            return Ok(Next::Done(Some(database)));
        };
        let token = (self.bearer.as_deref()).and_then(|token| AuthToken::bearer(token).ok());
        let caller = (Some(&self.auth), token);
        let read = DatasetSummaryOperation::new(self.auth.realm_id, dataset, caller);
        self.step = DatabaseStep::Summary(metadata);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for ReadableDatabaseOperation {
    type Output = Option<CapturedDatabase>;
    type Error = DatabaseError;

    fn first(&mut self) -> DatabaseNext {
        let mut reads = match self.check.reads() {
            Ok(reads) => reads,
            Err(error) => return Self::rule_failure(error),
        };
        reads.extend(policy_reads(self.auth.realm_id, Some(self.info.group_id)));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> DatabaseNext {
        match std::mem::replace(&mut self.step, DatabaseStep::Check) {
            DatabaseStep::Check => {
                let IoResult::Values(rows) = result else {
                    let got = result.label();
                    let expected = "IoResult::Values";
                    return Err(IoError::Unexpected { expected, got }.into());
                };
                self.decide(rows)
            }
            DatabaseStep::Metadata => {
                let read = result.child::<Option<BucketMetadata>, BucketMetadataError>()?;
                self.described(read.map_err(|error| DatabaseError(error.to_string()))?)
            }
            DatabaseStep::Summary(metadata) => {
                let read = result.child::<DatasetDescription, DescriptionError>()?;
                let described = admitted_description(read).map_err(DatabaseError)?;
                let database = CapturedDatabase::new(metadata, described);
                Ok(Next::Done(Some(database)))
            }
        }
    }
}
