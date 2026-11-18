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

/// What the version head settled about an input.
#[derive(Debug, PartialEq)]
struct Located {
    version: Ulid,
    content: InputContent,
    source_pin: Option<SourcePin>,
    bytes: u64,
    policies: Vec<PlacementPolicyRef>,
}

/// An input the caller may read, with whether everyone may.
#[derive(Debug, PartialEq)]
struct Readable {
    located: Located,
    group_id: GroupId,
    public: bool,
}

#[derive(Debug, PartialEq)]
enum CaptureStep {
    Head,
    Hash(Box<HeadObjectResult>, Ulid),
    Access(Located),
    Public(Located, GroupId),
    Bucket(Readable),
    Database(Readable, Box<BucketInfo>),
    Releases(Readable, Option<CapturedDatabase>),
    Grant(Readable, Option<CapturedDatabase>, Vec<ReleaseRef>, bool),
    Finished,
}

/// Captures one input held here. `known` is its bucket's metadata when an earlier input of the
/// same bucket already read it.
#[derive(Debug, PartialEq)]
struct CaptureInputOperation {
    caller: CaptureCaller,
    input: InputSelection,
    reference: String,
    known: Option<Option<CapturedDatabase>>,
    step: CaptureStep,
}

type CaptureNext = Result<Next<CapturedInput>, SubmitJobError>;

impl CaptureInputOperation {
    fn new(
        caller: CaptureCaller,
        input: InputSelection,
        known: Option<Option<CapturedDatabase>>,
    ) -> Self {
        Self {
            reference: reference_of(&input),
            caller,
            input,
            known,
            step: CaptureStep::Head,
        }
    }

    fn unavailable(&self, error: impl std::fmt::Display) -> SubmitJobError {
        SubmitJobError::PlacementUnavailable(format!("{}: {error}", self.reference))
    }

    fn invalid(&self, error: impl std::fmt::Display) -> SubmitJobError {
        SubmitJobError::InvalidWorkspace(format!("{}: {error}", self.reference))
    }

    /// A definitive miss is the submitter's error, never a retryable 503.
    fn head_failure(&self, error: HeadObjectError) -> SubmitJobError {
        match error {
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker => self.invalid("input object not found"),
            other => self.unavailable(other),
        }
    }

    fn headed(&mut self, head: HeadObjectResult) -> CaptureNext {
        let version = head.resolved_version_id.or(head.version_id);
        let version = version.ok_or_else(|| self.invalid("input has no version"))?;
        let hash = (head.location.as_ref())
            .and_then(|location| location.hashes.get(HASH_BLAKE3))
            .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok());
        if hash.is_some() {
            return self.located(&head, version, hash);
        }
        let (bucket, key, _) = source_parts(&self.input);
        let read = VersionHashOperation::new(bucket, key, version);
        self.step = CaptureStep::Hash(Box::new(head), version);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn located(
        &mut self,
        head: &HeadObjectResult,
        version: Ulid,
        hash: Option<[u8; 32]>,
    ) -> CaptureNext {
        let (content, source_pin) = match hash {
            Some(hash) => (InputContent::Blake3(hash), None),
            None => reference_content(&self.reference, head)?,
        };
        let stored = head.location.as_ref().map(|location| location.blob_size);
        let described = head.source_metadata.as_ref();
        let bytes = stored
            .or_else(|| described.map(|metadata| metadata.content_length))
            .unwrap_or_default();
        let policies = PlacementPolicyRef::canonical_set(&head.source_policies)
            .map_err(|error| self.invalid(error))?;
        // The caller must read the input like an S3 GET of it, in whatever group it lives.
        let (bucket, key, _) = source_parts(&self.input);
        let check = self.caller.input.read_check(bucket, key);
        self.step = CaptureStep::Access(Located {
            version,
            content,
            source_pin,
            bytes,
            policies,
        });
        Ok(Next::Call(IoCall::child(check)))
    }

    fn public_check(&self, group_id: GroupId) -> SourceReadOperation {
        let (bucket, key, _) = source_parts(&self.input);
        let object = SourceObject {
            node_id: self.caller.input.local,
            bucket: bucket.to_string(),
            key: key.to_string(),
            operation: READ_OPERATION.to_string(),
        };
        let anonymous = AuthContext::anonymous(self.caller.input.realm_id);
        SourceReadOperation::in_group(anonymous, object, group_id)
    }

    fn release_check(&self, version: Ulid, info: &BucketInfo) -> MembersCheckOperation {
        let (bucket, key, _) = source_parts(&self.input);
        let scope = BucketScope {
            realm_id: self.caller.input.realm_id,
            node_id: self.caller.input.local,
            bucket,
            info,
        };
        let auth = self.caller.input.auth();
        let caller = MembershipCaller {
            auth: &auth,
            bearer: self.caller.bearer.as_deref(),
        };
        let query = MembershipQuery {
            key: key.to_string(),
            version_id: version,
        };
        MembersCheckOperation::new(scope, caller, &[query])
    }

    fn with_database(
        &mut self,
        readable: Readable,
        info: &BucketInfo,
        database: Option<CapturedDatabase>,
    ) -> CaptureNext {
        let check = self.release_check(readable.located.version, info);
        self.step = CaptureStep::Releases(readable, database);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn with_bucket(&mut self, readable: Readable, info: BucketInfo) -> CaptureNext {
        if let Some(database) = self.known.take() {
            return self.with_database(readable, &info, database);
        }
        let (bucket, _, _) = source_parts(&self.input);
        let read = ReadableDatabaseOperation::new(&self.caller, bucket, &info);
        self.step = CaptureStep::Database(readable, Box::new(info));
        Ok(Next::Call(IoCall::child(read)))
    }

    /// This node validated the caller itself, so its grant vouches for exact reads later.
    fn grant(&self, version: Ulid) -> ReadGrant {
        let (bucket, key, _) = source_parts(&self.input);
        let caller = &self.caller.input;
        ReadGrant {
            realm_id: caller.realm_id,
            issuer: caller.local,
            submission_id: self.caller.submission_id,
            user_id: caller.created_by,
            restrictions: None,
            scope: GrantScope::Version(VersionedObjectArn {
                realm_id: caller.realm_id,
                node_id: caller.local,
                bucket: bucket.to_string(),
                key: key.to_string(),
                version,
            }),
            operations: vec![READ_OPERATION.to_string(), HEAD_OPERATION.to_string()],
            expires_at: self.caller.expires_at,
        }
    }

    fn finish(
        &self,
        readable: Readable,
        database: Option<CapturedDatabase>,
        (releases, releases_complete): (Vec<ReleaseRef>, bool),
        grant: Option<SignedGrant>,
    ) -> CapturedInput {
        let Readable {
            located,
            group_id,
            public,
        } = readable;
        CapturedInput {
            destination_key: self.input.dest_key.clone(),
            source_node_id: self.caller.input.local,
            version_id: located.version,
            content: located.content,
            bytes: located.bytes,
            policies: located.policies,
            source_group_id: group_id,
            public,
            link: None,
            source_pin: located.source_pin,
            database,
            releases,
            releases_complete,
            grant,
            served_by: None,
        }
    }
}

/// The signed grant an answer carries; a node that cannot sign gives none.
fn signed_grant(result: IoResult) -> Result<Option<SignedGrant>, IoError> {
    let expected = "Event::Net(NetEvent::GrantSign)";
    let IoResult::Event(event) = result else {
        let got = result.label();
        return Err(IoError::Unexpected { expected, got });
    };
    let Event::Net(NetEvent::GrantSign(grant)) = *event else {
        let got = "another event";
        return Err(IoError::Unexpected { expected, got });
    };
    Ok(grant.map(|grant| *grant))
}

impl Operation for CaptureInputOperation {
    type Output = CapturedInput;
    type Error = SubmitJobError;

    fn first(&mut self) -> CaptureNext {
        let (bucket, key, version_id) = source_parts(&self.input);
        // Every refusal names the input it is about: the object store's own
        // message says only that some key is missing.
        let version_id = version_id
            .map(Ulid::from_string)
            .transpose()
            .map_err(|error| self.invalid(error))?;
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
        });
        Ok(Next::Call(IoCall::child(head)))
    }

    fn next(&mut self, result: IoResult) -> CaptureNext {
        match std::mem::replace(&mut self.step, CaptureStep::Finished) {
            CaptureStep::Head => {
                let head = result.child::<HeadObjectResult, HeadObjectError>()?;
                self.headed(head.map_err(|error| self.head_failure(error))?)
            }
            // A version record that cannot be read gives no hash, like one without any.
            CaptureStep::Hash(head, version) => {
                let hash = result.child::<Option<[u8; 32]>, IoError>()?;
                self.located(&head, version, hash.ok().flatten())
            }
            CaptureStep::Access(located) => {
                let read = result.child::<GroupId, SourceReadError>()?;
                let group_id = read.map_err(|error| source_error(&self.reference, error))?;
                self.step = CaptureStep::Public(located, group_id);
                Ok(Next::Call(IoCall::child(self.public_check(group_id))))
            }
            CaptureStep::Public(located, group_id) => {
                let public = match result.child::<GroupId, SourceReadError>()? {
                    Ok(_) => true,
                    Err(SourceReadError::Denied) => false,
                    Err(error) => return Err(source_error(&self.reference, error)),
                };
                self.step = CaptureStep::Bucket(Readable {
                    located,
                    group_id,
                    public,
                });
                let (bucket, _, _) = source_parts(&self.input);
                let read = GetBucketOperation::new(bucket.to_string());
                Ok(Next::Call(IoCall::child(read)))
            }
            CaptureStep::Bucket(readable) => {
                let info = result.child::<BucketInfo, GetBucketError>()?;
                let info = info.map_err(|error| self.unavailable(error))?;
                self.with_bucket(readable, info)
            }
            CaptureStep::Database(readable, info) => {
                let read = result.child::<Option<CapturedDatabase>, DatabaseError>()?;
                let database = read.map_err(|error| self.unavailable(error))?;
                self.with_database(readable, &info, database)
            }
            // A check that fails leaves the releases unproven rather than refusing the run.
            CaptureStep::Releases(readable, database) => {
                let found = result.child::<MembersOutput, ReleaseError>()?;
                let answer = found
                    .ok()
                    .and_then(|found| found.answers.into_iter().next());
                let (releases, complete) = answer.map_or((Vec::new(), false), |answer| {
                    (answer.releases, answer.complete)
                });
                let grant = self.grant(readable.located.version);
                self.step = CaptureStep::Grant(readable, database, releases, complete);
                let effect = Effect::Net(NetEffect::GrantSign(Box::new(grant)));
                Ok(Next::Call(IoCall::Effect(Box::new(effect))))
            }
            CaptureStep::Grant(readable, database, releases, complete) => {
                let grant = signed_grant(result)?;
                let releases = (releases, complete);
                Ok(Next::Done(self.finish(readable, database, releases, grant)))
            }
            CaptureStep::Finished => Err(IoError::Unfinished.into()),
        }
    }
}

/// Captures inputs held here in order. Inputs of one bucket share its metadata and its
/// dataset's description, read once.
#[derive(Debug, PartialEq)]
pub(super) struct CaptureInputsOperation {
    caller: CaptureCaller,
    inputs: Vec<InputSelection>,
    captured: Vec<CapturedInput>,
    databases: BTreeMap<String, Option<CapturedDatabase>>,
}

type InputsNext = Result<Next<Vec<CapturedInput>>, SubmitJobError>;

impl CaptureInputsOperation {
    pub(super) fn new(caller: CaptureCaller, inputs: Vec<InputSelection>) -> Self {
        Self {
            caller,
            captured: Vec::with_capacity(inputs.len()),
            inputs,
            databases: BTreeMap::new(),
        }
    }

    fn bucket_of(input: &InputSelection) -> &str {
        source_parts(input).0
    }

    fn capture_next(&mut self) -> InputsNext {
        let Some(input) = self.inputs.get(self.captured.len()) else {
            return Ok(Next::Done(std::mem::take(&mut self.captured)));
        };
        let known = self.databases.get(Self::bucket_of(input)).cloned();
        let capture = CaptureInputOperation::new(self.caller.clone(), input.clone(), known);
        Ok(Next::Call(IoCall::child(capture)))
    }
}

impl Operation for CaptureInputsOperation {
    type Output = Vec<CapturedInput>;
    type Error = SubmitJobError;

    fn first(&mut self) -> InputsNext {
        self.capture_next()
    }

    fn next(&mut self, result: IoResult) -> InputsNext {
        let captured = result.child::<CapturedInput, SubmitJobError>()??;
        if let Some(input) = self.inputs.get(self.captured.len()) {
            let bucket = Self::bucket_of(input).to_string();
            self.databases
                .entry(bucket)
                .or_insert_with(|| captured.database.clone());
        }
        self.captured.push(captured);
        self.capture_next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, captured_input, node, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::{InputMode, InputSource};
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::format::{Compression, StoredFormat};
    use aruna_core::structs::storage::multipart::MultipartChecksumType;

    const GROUP: Ulid = Ulid::from_parts(2, 2);
    const VERSION: Ulid = Ulid::from_parts(7, 7);

    fn caller() -> CaptureCaller {
        CaptureCaller {
            input: InputCaller {
                created_by: user(),
                realm_id: REALM,
                local: node(1),
            },
            submission_id: SubmissionId([4; 32]),
            expires_at: 10,
            bearer: None,
        }
    }

    fn input(key: &str) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: "b".to_string(),
                key: key.to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: key.to_string(),
            mode: InputMode::Snapshot,
            container_path: Some(format!("/in/{key}")),
            name: None,
            description: None,
        }
    }

    fn head() -> HeadObjectResult {
        HeadObjectResult {
            location: None,
            metadata: Default::default(),
            source_metadata: None,
            last_refresh: None,
            version_created_at: None,
            version_id: Some(VERSION),
            resolved_version_id: None,
            checksum_type: MultipartChecksumType::FullObject,
            composite_hashes: Default::default(),
            part_count: None,
            source_policies: Vec::new(),
            source_binding: None,
        }
    }

    fn info() -> BucketInfo {
        BucketInfo {
            group_id: GROUP,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn capture() -> CaptureInputOperation {
        CaptureInputOperation::new(caller(), input("k"), None)
    }

    fn calls_child<T: std::fmt::Debug>(next: Result<Next<T>, SubmitJobError>) {
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))), "{next:?}");
    }

    // One input passes every step in order, and the grant is what this node signs.
    #[test]
    fn capture_walks_steps() {
        let mut operation = capture();
        calls_child(operation.first());
        calls_child(operation.next(child(Ok::<_, HeadObjectError>(head()))));
        let hash = Ok::<_, IoError>(Some([9u8; 32]));
        calls_child(operation.next(child(hash)));
        calls_child(operation.next(child(Ok::<_, SourceReadError>(GROUP))));
        calls_child(operation.next(child(Err::<GroupId, _>(SourceReadError::Denied))));
        calls_child(operation.next(child(Ok::<_, GetBucketError>(info()))));
        let database = Ok::<Option<CapturedDatabase>, DatabaseError>(None);
        calls_child(operation.next(child(database)));
        let failed = ReleaseError::Unavailable("down".to_string());
        let signing = operation.next(child(Err::<MembersOutput, _>(failed)));
        let Ok(Next::Call(IoCall::Effect(effect))) = signing else {
            panic!("the grant is signed last: {signing:?}");
        };
        let Effect::Net(NetEffect::GrantSign(grant)) = *effect else {
            panic!("expected a grant");
        };
        assert_eq!((grant.issuer, grant.expires_at), (node(1), 10));
        let answer = IoResult::Event(Box::new(Event::Net(NetEvent::GrantSign(None))));
        let Ok(Next::Done(captured)) = operation.next(answer) else {
            panic!("the capture is complete");
        };
        assert_eq!(captured.version_id, VERSION);
        assert_eq!(captured.content, InputContent::Blake3([9; 32]));
        assert_eq!(captured.source_group_id, GROUP);
        assert!(!captured.public && !captured.releases_complete);
    }

    // A definite miss is the submitter's error; any other head failure can be retried.
    #[test]
    fn head_failure_kinds() {
        let failed = |error| {
            let mut operation = capture();
            operation.first().expect("head");
            operation.next(child(Err::<HeadObjectResult, _>(error)))
        };
        let missing = failed(HeadObjectError::NoSuchKey);
        assert!(
            matches!(missing, Err(SubmitJobError::InvalidWorkspace(reason))
            if reason.contains("s3://b/k"))
        );
        let broken = failed(HeadObjectError::HeadObjectFailed);
        assert!(matches!(
            broken,
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    // An unreadable version record counts as no hash, so only a reference could still pass.
    #[test]
    fn unread_hash_needs_reference() {
        let mut operation = capture();
        operation.first().expect("head");
        calls_child(operation.next(child(Ok::<_, HeadObjectError>(head()))));
        let unread = operation.next(child(Err::<Option<[u8; 32]>, _>(IoError::Unfinished)));
        assert!(matches!(unread, Err(SubmitJobError::InvalidWorkspace(_))));
    }

    // Metadata an earlier input of the bucket read is not read again.
    #[test]
    fn known_database_skips_read() {
        let mut operation = CaptureInputOperation::new(caller(), input("k"), Some(None));
        operation.step = CaptureStep::Bucket(Readable {
            located: Located {
                version: VERSION,
                content: InputContent::Blake3([1; 32]),
                source_pin: None,
                bytes: 1,
                policies: Vec::new(),
            },
            group_id: GROUP,
            public: true,
        });
        calls_child(operation.next(child(Ok::<_, GetBucketError>(info()))));
        assert!(matches!(operation.step, CaptureStep::Releases(_, None)));
    }

    // Inputs are captured in order and remember what their bucket's metadata was.
    #[test]
    fn inputs_share_database() {
        let inputs = vec![input("a"), input("b")];
        let mut operation = CaptureInputsOperation::new(caller(), inputs.clone());
        calls_child(operation.first());
        let first = captured_input(&inputs[0], node(1));
        calls_child(operation.next(child(Ok::<_, SubmitJobError>(first.clone()))));
        assert_eq!(operation.databases.get("b"), Some(&None));
        let second = captured_input(&inputs[1], node(1));
        let done = operation.next(child(Ok::<_, SubmitJobError>(second.clone())));
        assert_eq!(done, Ok(Next::Done(vec![first, second])));
    }

    fn database() -> ReadableDatabaseOperation {
        ReadableDatabaseOperation::new(&caller(), "b", &info())
    }

    // A caller without the documents' grant sees no metadata, and the run still starts.
    #[test]
    fn denied_omits_metadata() {
        let mut operation = database();
        let Ok(Next::Call(IoCall::Read(reads))) = operation.first() else {
            panic!("the grants and policies are read");
        };
        let rows = IoResult::Values(vec![None; reads.len()]);
        assert_eq!(operation.next(rows), Ok(Next::Done(None)));
    }

    // Metadata without a dataset is complete; with one, the summary is read next.
    #[test]
    fn dataset_read_follows() {
        let read = |metadata: BucketMetadata| {
            let mut operation = database();
            operation.step = DatabaseStep::Metadata;
            let answer = child(Ok::<_, BucketMetadataError>(Some(metadata)));
            (operation.next(answer), operation)
        };
        let (plain, _) = read(BucketMetadata::default());
        assert!(matches!(plain, Ok(Next::Done(Some(_)))));
        let dataset = Some(Ulid::from_parts(5, 5));
        let linked = BucketMetadata {
            dataset,
            ..BucketMetadata::default()
        };
        let (next, operation) = read(linked);
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
        assert!(matches!(operation.step, DatabaseStep::Summary(_)));
    }

    // An unreadable dataset leaves the description empty; an unavailable one fails.
    #[test]
    fn summary_failure_kinds() {
        let read = |error| {
            let mut operation = database();
            operation.step = DatabaseStep::Summary(BucketMetadata::default());
            operation.next(child(Err::<DatasetDescription, _>(error)))
        };
        assert!(matches!(
            read(DescriptionError::Unreadable),
            Ok(Next::Done(Some(_)))
        ));
        assert!(read(DescriptionError::Unavailable).is_err());
    }

    // The hash comes from a materialized version; other rows give none.
    #[test]
    fn version_hash_decodes() {
        let answer = |row: Option<Vec<u8>>| {
            let mut operation = VersionHashOperation::new("b", "k", VERSION);
            assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
            operation.next(IoResult::Values(vec![row.map(Value::from)]))
        };
        let version = BlobVersion::materialized(
            [3; 32],
            BackendRef::node_default(),
            StoredFormat::default().encoding(),
            std::time::SystemTime::UNIX_EPOCH,
            user(),
            None,
        );
        let row = postcard::to_allocvec(&version).unwrap();
        assert_eq!(answer(Some(row)), Ok(Next::Done(Some([3; 32]))));
        assert_eq!(answer(Some(vec![255])), Ok(Next::Done(None)));
        assert_eq!(answer(None), Ok(Next::Done(None)));
    }
}
