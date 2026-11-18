//! Copies bytes from an existing object into one part of an open multipart upload.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::placement::policy::{
    GateContext, GateContextError, GateOperation, PolicyGateError, PolicyGateOutcome,
    ReadPolicyError, gate_decision, union_refs, write_gate,
};
use crate::s3::fence_store::FenceCheckOperation;
use crate::s3::multipart::part_upload::{
    UploadPartError, UploadPartInput, UploadPartOperation, UploadPartResult,
};
use crate::s3::object::copy::{
    CopySourceConditions, evaluate_source_conditions, source_etag, source_modified,
};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectResult, ObjectRangeRequest};
use crate::s3::object::read::{ReadSourceOperation, TokenRead};
use crate::s3::purge_fence::PurgeFenceError;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::UPLOAD_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::multipart::{MultipartUpload, MultipartUploadStatus};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartCopyInput {
    pub source_bucket: String,
    pub source_key: String,
    pub source_version_id: Option<Ulid>,
    pub source_group_id: GroupId,
    pub dest_bucket: String,
    pub dest_key: String,
    pub upload_id: Ulid,
    pub part_number: u16,
    pub range: Option<ObjectRangeRequest>,
    pub user_id: UserId,
    pub node_id: NodeId,
    pub source_auth_context: AuthContext,
    pub conditions: CopySourceConditions,
}

#[derive(Debug, PartialEq)]
pub struct PartCopyResult {
    pub part_location: BackendLocation,
    pub source_version_id: Option<Ulid>,
    pub source_last_modified: Option<SystemTime>,
}

#[derive(Debug, Error, PartialEq)]
pub enum PartCopyError {
    #[error(transparent)]
    Get(#[from] GetObjectError),
    #[error(transparent)]
    UploadPart(#[from] UploadPartError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Gate(#[from] GateContextError),
    #[error("At least one of the preconditions you specified did not hold.")]
    PreconditionFailed,
}

impl From<IoError> for PartCopyError {
    fn from(error: IoError) -> Self {
        Self::UploadPart(error.into())
    }
}

type CopyTurn<T> = Result<Next<T>, PartCopyError>;

fn read_upload(upload_id: Ulid) -> IoCall {
    read_row(UPLOAD_KEYSPACE, upload_id.to_bytes().to_vec())
}

fn upload_record(value: Option<Value>) -> Result<MultipartUpload, PartCopyError> {
    let value = value.ok_or(UploadPartError::NoSuchUpload)?;
    Ok(MultipartUpload::from_bytes(value.as_ref()).map_err(UploadPartError::from)?)
}

/// Refuses an upload of another bucket or key, or one that is no longer open.
fn check_target(
    record: &MultipartUpload,
    (bucket, key): (&str, &str),
) -> Result<(), PartCopyError> {
    if record.bucket != bucket || record.key != key {
        return Err(UploadPartError::UploadTargetMismatch.into());
    }
    if record.status != MultipartUploadStatus::Open {
        return Err(UploadPartError::UploadNotOpen.into());
    }
    Ok(())
}

/// Evaluates this node against every ref the finished part carries. An ungoverned copy resolves
/// nothing further.
#[derive(Debug, PartialEq)]
struct GatePartOperation {
    realm_id: RealmId,
    refs: Vec<PlacementPolicyRef>,
    group_id: GroupId,
    now_ms: u64,
    checking: bool,
}

impl GatePartOperation {
    fn new(realm_id: RealmId, refs: Vec<PlacementPolicyRef>, group_id: GroupId) -> Self {
        Self {
            realm_id,
            refs,
            group_id,
            now_ms: 0,
            checking: false,
        }
    }
}

impl Operation for GatePartOperation {
    type Output = ();
    type Error = PartCopyError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> CopyTurn<()> {
        let gate = GateOperation::new(self.realm_id, self.now_ms);
        Ok(Next::Call(IoCall::child(gate)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<()> {
        if self.checking {
            let outcome = result.child::<PolicyGateOutcome, ReadPolicyError>()?;
            let outcome =
                outcome.map_err(|error| UploadPartError::from(PolicyGateError::from(error)))?;
            gate_decision(outcome).map_err(UploadPartError::from)?;
            return Ok(Next::Done(()));
        }
        let destination = result.child::<Option<GateContext>, GateContextError>()??;
        let gate = write_gate(destination.as_ref(), &self.refs, Some(self.group_id))
            .map_err(UploadPartError::from)?;
        let Some(gate) = gate else {
            return Ok(Next::Done(()));
        };
        self.checking = true;
        Ok(Next::Call(IoCall::child(gate)))
    }
}

/// Merges a source's refs into the destination upload record in one transaction.
#[derive(Debug, PartialEq)]
struct StorePoliciesOperation {
    bucket: String,
    key: String,
    upload_id: Ulid,
    policies: Vec<PlacementPolicyRef>,
    writing: bool,
}

impl StorePoliciesOperation {
    fn new((bucket, key): (&str, &str), upload_id: Ulid, policies: &[PlacementPolicyRef]) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id,
            policies: policies.to_vec(),
            writing: false,
        }
    }
}

impl Operation for StorePoliciesOperation {
    type Output = ();
    type Error = PartCopyError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> CopyTurn<()> {
        Ok(Next::Call(read_upload(self.upload_id)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<()> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let mut record = upload_record(result.value()?)?;
        check_target(&record, (&self.bucket, &self.key))?;
        if !record.merge_policies(&self.policies)? {
            return Ok(Next::Done(()));
        }
        let value = record.to_bytes().map_err(UploadPartError::from)?;
        self.writing = true;
        let key = Key::from(self.upload_id.to_bytes().to_vec());
        let row = (UPLOAD_KEYSPACE.to_string(), key, Value::from(value));
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

#[derive(Debug, PartialEq)]
enum KeepStep {
    Upload,
    Gate,
    Store,
}

/// Admits this node for the refs a part copied on another member carries and merges them into
/// the upload before its bytes land, as a part copied here does.
#[derive(Debug, PartialEq)]
pub struct KeepPoliciesOperation {
    realm_id: RealmId,
    upload_id: Ulid,
    bucket: String,
    key: String,
    policies: Vec<PlacementPolicyRef>,
    step: KeepStep,
}

impl KeepPoliciesOperation {
    pub fn new(
        (realm_id, upload_id): (RealmId, Ulid),
        (bucket, key): (&str, &str),
        policies: &[PlacementPolicyRef],
    ) -> Self {
        Self {
            realm_id,
            upload_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            policies: policies.to_vec(),
            step: KeepStep::Upload,
        }
    }
}

impl Operation for KeepPoliciesOperation {
    type Output = ();
    type Error = PartCopyError;

    fn first(&mut self) -> CopyTurn<()> {
        if self.policies.is_empty() {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(read_upload(self.upload_id)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<()> {
        match self.step {
            KeepStep::Upload => {
                let upload = upload_record(result.value()?)?;
                check_target(&upload, (&self.bucket, &self.key))?;
                let refs = union_refs(&upload.placement_policies, &self.policies)
                    .map_err(UploadPartError::from)?;
                let gate = GatePartOperation::new(self.realm_id, refs, upload.group_id);
                self.step = KeepStep::Gate;
                Ok(Next::Call(IoCall::child(gate)))
            }
            KeepStep::Gate => {
                result.child::<(), PartCopyError>()??;
                let target = (self.bucket.as_str(), self.key.as_str());
                let store = StorePoliciesOperation::new(target, self.upload_id, &self.policies);
                self.step = KeepStep::Store;
                Ok(Next::Call(IoCall::child(store)))
            }
            KeepStep::Store => {
                result.child::<(), PartCopyError>()??;
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PartCopyStep {
    Fence,
    Upload,
    Source,
    Gate,
    Store,
    Part,
}

/// Copies bytes of an existing object into one part of an open upload; `token` admits the source
/// read while its bucket key is locked.
#[derive(Debug, PartialEq)]
pub struct PartCopyOperation {
    input: PartCopyInput,
    token: Option<TokenRead>,
    upload: Option<MultipartUpload>,
    source: Option<GetObjectResult>,
    modified: Option<SystemTime>,
    source_version_id: Option<Ulid>,
    step: PartCopyStep,
}

impl PartCopyOperation {
    pub fn new(input: PartCopyInput, token: Option<TokenRead>) -> Self {
        Self {
            input,
            token,
            upload: None,
            source: None,
            modified: None,
            source_version_id: None,
            step: PartCopyStep::Fence,
        }
    }

    fn read_source(&mut self) -> CopyTurn<PartCopyResult> {
        let input = &self.input;
        let source = GetObjectInput {
            bucket: input.source_bucket.clone(),
            key: input.source_key.clone(),
            version_id: input.source_version_id,
            range: input.range.clone(),
            group_id: input.source_group_id,
            user_identity: input.source_auth_context.user_id,
            realm_id: input.source_auth_context.realm_id,
            node_id: input.node_id,
        };
        let restrictions = input.source_auth_context.path_restrictions.clone();
        let read = ReadSourceOperation::new(source, restrictions, self.token.take());
        self.step = PartCopyStep::Source;
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Checks the conditions before the lazy source stream is consumed. No byte of a governed
    /// source lands here before this node is admitted for the union the part will carry.
    fn source_read(&mut self, source: GetObjectResult) -> CopyTurn<PartCopyResult> {
        let (location, metadata) = (source.location.as_ref(), source.source_metadata.as_ref());
        self.modified = source_modified(source.info.version_created_at, location, metadata);
        self.source_version_id = source.version_id;
        let etag = source_etag(location, metadata);
        evaluate_source_conditions(&self.input.conditions, etag.as_deref(), self.modified, true)
            .map_err(|_| PartCopyError::PreconditionFailed)?;
        let upload = self.upload.as_ref().ok_or(IoError::Unfinished)?;
        let refs = union_refs(&upload.placement_policies, &source.source_policies)
            .map_err(UploadPartError::from)?;
        let realm_id = self.input.source_auth_context.realm_id;
        let gate = GatePartOperation::new(realm_id, refs, upload.group_id);
        self.source = Some(source);
        self.step = PartCopyStep::Gate;
        Ok(Next::Call(IoCall::child(gate)))
    }

    /// Stored before the bytes land: a lost merge must not let the completed object drop the
    /// refs its source carried.
    fn store_policies(&mut self) -> CopyTurn<PartCopyResult> {
        let source = self.source.as_ref().ok_or(IoError::Unfinished)?;
        if source.source_policies.is_empty() {
            return self.write_part();
        }
        let input = &self.input;
        let target = (input.dest_bucket.as_str(), input.dest_key.as_str());
        let store = StorePoliciesOperation::new(target, input.upload_id, &source.source_policies);
        self.step = PartCopyStep::Store;
        Ok(Next::Call(IoCall::child(store)))
    }

    fn write_part(&mut self) -> CopyTurn<PartCopyResult> {
        let source = self.source.take().ok_or(IoError::Unfinished)?;
        let content_length = (source.resolved_range.as_ref())
            .map(|range| range.content_length as u64)
            .or_else(|| source.location.as_ref().map(|location| location.blob_size));
        let input = &self.input;
        let part = UploadPartOperation::new(UploadPartInput {
            bucket: input.dest_bucket.clone(),
            key: input.dest_key.clone(),
            upload_id: input.upload_id,
            part_number: input.part_number,
            content_length,
            body: Some(source.blob),
            created_by: input.user_id,
            expected_checksums: Vec::new(),
        });
        self.step = PartCopyStep::Part;
        Ok(Next::Call(IoCall::child(part)))
    }
}

impl Operation for PartCopyOperation {
    type Output = PartCopyResult;
    type Error = PartCopyError;

    fn first(&mut self) -> CopyTurn<PartCopyResult> {
        let input = &self.input;
        let fence = FenceCheckOperation::new(&input.dest_bucket, &input.dest_key);
        Ok(Next::Call(IoCall::child(fence)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<PartCopyResult> {
        match self.step {
            PartCopyStep::Fence => {
                (result.child::<(), PurgeFenceError>()?).map_err(UploadPartError::PurgeFence)?;
                self.step = PartCopyStep::Upload;
                Ok(Next::Call(read_upload(self.input.upload_id)))
            }
            PartCopyStep::Upload => {
                let upload = upload_record(result.value()?)?;
                check_target(&upload, (&self.input.dest_bucket, &self.input.dest_key))?;
                self.upload = Some(upload);
                self.read_source()
            }
            PartCopyStep::Source => {
                self.source_read(result.child::<GetObjectResult, GetObjectError>()??)
            }
            PartCopyStep::Gate => {
                result.child::<(), PartCopyError>()??;
                self.store_policies()
            }
            PartCopyStep::Store => {
                result.child::<(), PartCopyError>()??;
                self.write_part()
            }
            PartCopyStep::Part => {
                let part = result.child::<UploadPartResult, UploadPartError>()??;
                Ok(Next::Done(PartCopyResult {
                    part_location: part.location,
                    source_version_id: self.source_version_id,
                    source_last_modified: self.modified,
                }))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use crate::tests::policy::{seed_gate, subject};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE};
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::checksum::Hasher;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig, BackendRef};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::multipart::{
        MultipartPart, MultipartPartKey, MultipartUpload, MultipartUploadStatus,
    };
    use aruna_core::structs::storage::routing::RoutingSnapshot;
    use aruna_net::NetHandle;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::{TempDir, tempdir};

    fn test_node_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[7; 32]).public()
    }

    async fn upload_part_copy(
        context: &DriverContext,
        input: PartCopyInput,
    ) -> Result<PartCopyResult, PartCopyError> {
        drive(PartCopyOperation::new(input, None), context).await
    }

    async fn keep_part_policies(
        context: &DriverContext,
        upload: (RealmId, Ulid),
        target: (&str, &str),
        policies: &[PlacementPolicyRef],
    ) -> Result<(), PartCopyError> {
        drive(
            KeepPoliciesOperation::new(upload, target, policies),
            context,
        )
        .await
    }

    async fn stored_upload(context: &DriverContext, upload_id: Ulid) -> MultipartUpload {
        let read = StorageEffect::Read {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: upload_id.to_bytes().to_vec().into(),
            txn_id: None,
        };
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = context.storage_handle.send_storage_effect(read).await
        else {
            panic!("the upload record is stored");
        };
        MultipartUpload::from_bytes(value.as_ref()).unwrap()
    }

    fn step_input() -> PartCopyInput {
        let realm_id = RealmId::from_bytes([1; 32]);
        PartCopyInput {
            source_bucket: "source".to_string(),
            source_key: "a".to_string(),
            source_version_id: None,
            source_group_id: Ulid::from_bytes([2; 16]),
            dest_bucket: "dest".to_string(),
            dest_key: "b".to_string(),
            upload_id: Ulid::from_bytes([3; 16]),
            part_number: 1,
            range: None,
            user_id: UserId::local(Ulid::from_bytes([4; 16]), realm_id),
            node_id: test_node_id(),
            source_auth_context: AuthContext::anonymous(realm_id),
            conditions: CopySourceConditions::default(),
        }
    }

    #[test]
    fn missing_upload_refused() {
        let mut copy = PartCopyOperation::new(step_input(), None);
        copy.first().unwrap();
        let fenced: Result<(), PurgeFenceError> = Ok(());
        let outcome = aruna_core::calls::ChildOutcome::new(fenced);
        assert!(matches!(
            copy.next(IoResult::Child(outcome)),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let answer = copy.next(IoResult::Values(vec![None]));
        assert_eq!(
            answer,
            Err(PartCopyError::UploadPart(UploadPartError::NoSuchUpload))
        );
    }

    #[test]
    fn no_policies_finish() {
        let upload = (RealmId::from_bytes([1; 32]), Ulid::from_bytes([3; 16]));
        let mut keep = KeepPoliciesOperation::new(upload, ("dest", "b"), &[]);
        assert_eq!(keep.first(), Ok(Next::Done(())));
    }

    #[test]
    fn store_uses_transaction() {
        let store = StorePoliciesOperation::new(("dest", "b"), Ulid::from_bytes([3; 16]), &[]);
        assert_eq!(store.transaction(), TransactionMode::Write);
    }

    async fn full_context() -> (TempDir, DriverContext) {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (temp_handle, context)
    }

    async fn put_source(
        context: &DriverContext,
        realm_id: RealmId,
        group_id: GroupId,
        node_id: aruna_core::NodeId,
        bucket: &str,
        key: &str,
        data: &'static [u8],
    ) {
        let gate = drive(GateOperation::new(realm_id, 1_000), context)
            .await
            .expect("subject reads");
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: UserId::local(Ulid::generate(), realm_id),
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content_length: Some(data.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(data))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        });
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }
        drive(operation, context).await.unwrap();
    }

    async fn seed_multipart_upload(
        context: &DriverContext,
        upload_id: Ulid,
        bucket: &str,
        key: &str,
        group_id: GroupId,
        user_id: UserId,
    ) {
        seed_upload_policies(
            context,
            upload_id,
            bucket,
            key,
            group_id,
            user_id,
            Vec::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_upload_policies(
        context: &DriverContext,
        upload_id: Ulid,
        bucket: &str,
        key: &str,
        group_id: GroupId,
        user_id: UserId,
        policies: Vec<PlacementPolicyRef>,
    ) {
        let record = MultipartUpload {
            backend: BackendRef::node_default(),
            storage_class: None,
            upload_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id,
            created_by: user_id,
            created_at: SystemTime::now(),
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: policies,
            subject_generation: 1,
            completing_since_ms: None,
            backend_upload: None,
            encryption: None,
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: UPLOAD_KEYSPACE.to_string(),
                key: upload_id.to_bytes().to_vec().into(),
                value: record.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn range_copy_persists() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let upload_id = Ulid::generate();

        put_source(
            &context,
            realm_id,
            group_id,
            node_id,
            "bucket",
            "source.txt",
            b"0123456789",
        )
        .await;
        seed_multipart_upload(&context, upload_id, "bucket", "dest.txt", group_id, user_id).await;

        let result = upload_part_copy(
            &context,
            PartCopyInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                upload_id,
                part_number: 1,
                range: Some(ObjectRangeRequest::StartEnd { start: 2, end: 5 }),
                user_id,
                node_id: test_node_id(),
                source_auth_context: AuthContext::anonymous(realm_id),
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap();

        assert_eq!(result.part_location.blob_size, 4);
        let expected = Hasher::new_with_bytes(b"2345").to_map();
        assert_eq!(
            result.part_location.hashes.get(HASH_MD5),
            expected.get(HASH_MD5)
        );

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: UPLOAD_PART_KEYSPACE.to_string(),
                key: MultipartPartKey::new(upload_id, 1)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing part record");
        };
        let part = MultipartPart::from_bytes(value.expect("missing part record").as_ref()).unwrap();
        assert_eq!(part.part_number, 1);
        assert_eq!(part.location.blob_size, 4);
        assert_eq!(part.location.created_by, user_id);
    }

    #[tokio::test]
    async fn copy_stores_policies() {
        // A part copied from a governed source stores its refs on the upload, so
        // the composed object cannot be less constrained than what it copied.
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let upload_id = Ulid::generate();
        let rule = aruna_core::structs::placement::policy::VerifiedPolicy::verify(
            aruna_core::structs::placement::policy::PlacementPolicy::new(
                Ulid::from_bytes([4u8; 16]),
                "residency".to_string(),
                vec![aruna_core::structs::placement::policy::PlacementSelector {
                    node_id: Some(node_id),
                    location: None,
                    labels: Vec::new(),
                    executor_kind: None,
                }],
            )
            .expect("policy is valid"),
        )
        .expect("policy verifies");
        let policy = rule.policy_ref();
        seed_gate(
            &context,
            realm_id,
            user_id,
            subject(node_id, "eu-west"),
            &[rule],
        )
        .await;
        seed_governed_bucket(&context, group_id, user_id, vec![policy]).await;

        put_source(
            &context,
            realm_id,
            group_id,
            node_id,
            "bucket",
            "source.txt",
            b"0123456789",
        )
        .await;
        seed_multipart_upload(&context, upload_id, "bucket", "dest.txt", group_id, user_id).await;

        upload_part_copy(
            &context,
            PartCopyInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                upload_id,
                part_number: 1,
                range: None,
                user_id,
                node_id,
                source_auth_context: AuthContext::anonymous(realm_id),
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap();

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: UPLOAD_KEYSPACE.to_string(),
                key: upload_id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing upload record");
        };
        let record =
            MultipartUpload::from_bytes(value.expect("missing upload record").as_ref()).unwrap();
        assert_eq!(record.placement_policies, vec![policy]);
    }

    #[tokio::test]
    async fn refuses_denied_part() {
        // A part may only land after the destination is admitted for every ref
        // the finished object carries; the stored refs alone are not a gate.
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let upload_id = Ulid::generate();
        let elsewhere = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let rule = aruna_core::structs::placement::policy::VerifiedPolicy::verify(
            aruna_core::structs::placement::policy::PlacementPolicy::new(
                Ulid::from_bytes([7u8; 16]),
                "residency".to_string(),
                vec![aruna_core::structs::placement::policy::PlacementSelector {
                    node_id: Some(elsewhere),
                    location: None,
                    labels: Vec::new(),
                    executor_kind: None,
                }],
            )
            .expect("policy is valid"),
        )
        .expect("policy verifies");
        let policy = rule.policy_ref();
        seed_gate(
            &context,
            realm_id,
            user_id,
            subject(node_id, "eu-west"),
            &[rule],
        )
        .await;

        put_source(
            &context,
            realm_id,
            group_id,
            node_id,
            "bucket",
            "source.txt",
            b"0123456789",
        )
        .await;
        seed_upload_policies(
            &context,
            upload_id,
            "bucket",
            "dest.txt",
            group_id,
            user_id,
            vec![policy],
        )
        .await;

        let error = upload_part_copy(
            &context,
            PartCopyInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                upload_id,
                part_number: 1,
                range: None,
                user_id,
                node_id,
                source_auth_context: AuthContext::anonymous(realm_id),
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            PartCopyError::UploadPart(UploadPartError::PolicyGateError(PolicyGateError::Denied {
                policy_ids: vec![policy.policy_id]
            }))
        );
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: UPLOAD_PART_KEYSPACE.to_string(),
                key: MultipartPartKey::new(upload_id, 1)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing part read");
        };
        assert!(value.is_none(), "a refused part must write no bytes");
    }

    // A part copied on another member keeps its source's refs on the upload once this node is
    // admitted for them, and is refused when it is not.
    #[tokio::test]
    async fn keeps_forwarded_policies() {
        use aruna_core::structs::placement::policy::{
            PlacementPolicy, PlacementSelector, VerifiedPolicy,
        };
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let rule = |seed: u8, node| {
            let selector = PlacementSelector {
                node_id: Some(node),
                location: None,
                labels: Vec::new(),
                executor_kind: None,
            };
            let policy = PlacementPolicy::new(
                Ulid::from_bytes([seed; 16]),
                "residency".to_string(),
                vec![selector],
            );
            VerifiedPolicy::verify(policy.expect("policy is valid")).expect("policy verifies")
        };
        let here = rule(8, node_id);
        let elsewhere = rule(9, iroh::SecretKey::from_bytes(&[9u8; 32]).public());
        let (allowed, denied) = (here.policy_ref(), elsewhere.policy_ref());
        let subject = subject(node_id, "eu-west");
        seed_gate(&context, realm_id, user_id, subject, &[here, elsewhere]).await;
        let upload_id = Ulid::generate();
        seed_multipart_upload(&context, upload_id, "bucket", "dest.txt", group_id, user_id).await;

        let target = ("bucket", "dest.txt");
        keep_part_policies(&context, (realm_id, upload_id), target, &[allowed])
            .await
            .unwrap();
        let record = stored_upload(&context, upload_id).await;
        assert_eq!(record.placement_policies, vec![allowed]);
        let error = keep_part_policies(&context, (realm_id, upload_id), target, &[denied])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            PartCopyError::UploadPart(UploadPartError::PolicyGateError(_))
        ));
        let missing = (realm_id, Ulid::generate());
        assert_eq!(
            keep_part_policies(&context, missing, target, &[]).await,
            Ok(())
        );
    }

    // An upload id of another bucket or key is refused before its refs change, also inside
    // the merge transaction itself.
    #[tokio::test]
    async fn merge_checks_target() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let group_id = Ulid::generate();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let upload_id = Ulid::generate();
        seed_multipart_upload(&context, upload_id, "bucket", "dest.txt", group_id, user_id).await;
        let policy = PlacementPolicyRef {
            policy_id: Ulid::from_bytes([8; 16]),
            digest: [8; 32],
        };
        let mismatch = Err(PartCopyError::UploadPart(
            UploadPartError::UploadTargetMismatch,
        ));

        let other = ("bucket", "other.txt");
        let kept = keep_part_policies(&context, (realm_id, upload_id), other, &[policy]).await;
        assert_eq!(kept, mismatch);
        let other = ("other", "dest.txt");
        let store = StorePoliciesOperation::new(other, upload_id, &[policy]);
        let stored = drive(store, &context).await;
        assert_eq!(stored, mismatch);
        let record = stored_upload(&context, upload_id).await;
        assert!(record.placement_policies.is_empty());
    }

    async fn seed_governed_bucket(
        context: &DriverContext,
        group_id: GroupId,
        user_id: UserId,
        policies: Vec<aruna_core::structs::placement::policy::PlacementPolicyRef>,
    ) {
        let bucket = aruna_core::structs::storage::blob::BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: policies,
            placement_policy_generation: 1,
            compression: Compression::Off,
            distribution: None,
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
                key: b"bucket".to_vec().into(),
                value: bucket.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    #[tokio::test]
    async fn upload_precedes_lookup() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::generate();
        let user_id = UserId::local(Ulid::generate(), realm_id);

        let error = upload_part_copy(
            &context,
            PartCopyInput {
                source_bucket: "missing-source".to_string(),
                source_key: "missing.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "dest".to_string(),
                dest_key: "object.txt".to_string(),
                upload_id: Ulid::generate(),
                part_number: 1,
                range: None,
                user_id,
                node_id: test_node_id(),
                source_auth_context: AuthContext {
                    user_id,
                    realm_id,
                    path_restrictions: None,
                    session: None,
                },
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            PartCopyError::UploadPart(UploadPartError::NoSuchUpload)
        );
    }

    #[tokio::test]
    async fn unsatisfiable_range_errors() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let upload_id = Ulid::generate();

        put_source(
            &context,
            realm_id,
            group_id,
            node_id,
            "bucket",
            "source.txt",
            b"0123456789",
        )
        .await;
        seed_multipart_upload(&context, upload_id, "bucket", "dest.txt", group_id, user_id).await;

        let error = upload_part_copy(
            &context,
            PartCopyInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                upload_id,
                part_number: 1,
                range: Some(ObjectRangeRequest::StartEnd {
                    start: 100,
                    end: 200,
                }),
                user_id,
                node_id: test_node_id(),
                source_auth_context: AuthContext {
                    user_id,
                    realm_id,
                    path_restrictions: None,
                    session: None,
                },
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error, PartCopyError::Get(GetObjectError::InvalidRange));
    }
}
