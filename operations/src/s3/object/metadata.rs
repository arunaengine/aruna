//! Queues and schedules reference metadata refresh jobs for object versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{REFRESH_DUE_KEYSPACE, REFRESH_JOB_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

// Queue writes that lose to a racing drain or request reread the job, up to this often.
const QUEUE_CONFLICT_RETRIES: u32 = 8;

pub const REFRESH_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceRefresh {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    /// `None` asks the worker to observe the source first.
    pub metadata: Option<SourceMetadata>,
    pub refreshed_at: SystemTime,
    /// The node and caller a confirmed change is recorded and replicated for.
    pub node_id: NodeId,
    pub auth_context: AuthContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceRefreshRecord {
    pub refresh: ReferenceRefresh,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRefreshResult {
    pub queued: bool,
    pub scheduled: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReferenceRefreshError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("reference metadata refresh failed: {0}")]
    Refresh(String),
    #[error("unexpected event while processing reference metadata refresh queue: {0}")]
    UnexpectedEvent(String),
}

impl From<IoError> for ReferenceRefreshError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type QueueTurn = Result<Next<QueueRefreshResult>, ReferenceRefreshError>;

#[derive(Serialize)]
struct ReferenceRefreshIdentity<'a> {
    bucket: &'a str,
    key: &'a str,
    version_id: Ulid,
}

impl ReferenceRefreshRecord {
    pub fn new(refresh: ReferenceRefresh, due_at_ms: u64) -> Self {
        Self {
            refresh,
            due_at_ms,
            attempts: 0,
            last_error: None,
        }
    }
}

/// One job per version: a newer request replaces the payload of the queued one.
pub fn job_key(refresh: &ReferenceRefresh) -> Result<Key, ConversionError> {
    let identity = ReferenceRefreshIdentity {
        bucket: &refresh.bucket,
        key: &refresh.key,
        version_id: refresh.version_id,
    };
    let mut key = b"v2".to_vec();
    key.extend(postcard::to_allocvec(&identity)?);
    Ok(ByteView::from(key))
}

/// Big-endian due milliseconds, then the job key, so the due index sorts by due time.
pub(super) fn due_key(due_at_ms: u64, job_key: &[u8]) -> Key {
    let mut key = due_at_ms.to_be_bytes().to_vec();
    key.extend_from_slice(job_key);
    ByteView::from(key)
}

pub(super) fn split_due(row: &[u8]) -> Option<(u64, &[u8])> {
    let (due, job_key) = row.split_first_chunk::<8>()?;
    Some((u64::from_be_bytes(*due), job_key))
}

/// The job row and its due index row.
pub(super) fn job_entries(
    record: &ReferenceRefreshRecord,
) -> Result<Vec<(String, Key, ByteView)>, ConversionError> {
    let key = job_key(&record.refresh)?;
    Ok(vec![
        (
            REFRESH_DUE_KEYSPACE.to_string(),
            due_key(record.due_at_ms, &key),
            ByteView::from(Vec::new()),
        ),
        (
            REFRESH_JOB_KEYSPACE.to_string(),
            key,
            ByteView::from(postcard::to_allocvec(record)?),
        ),
    ])
}

pub fn schedule_drain() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainRefreshQueue,
        after: Duration::ZERO,
    })
}

/// Merges one refresh request into its queued job, in one transaction.
#[derive(Debug, PartialEq)]
struct RefreshJobWrite {
    refresh: ReferenceRefresh,
    writing: bool,
}

impl RefreshJobWrite {
    /// A queued job keeps its due time and attempts; only a newer request replaces its payload.
    fn merged(&self, existing: Option<&[u8]>) -> Option<ReferenceRefreshRecord> {
        match existing.map(postcard::from_bytes::<ReferenceRefreshRecord>) {
            Some(Ok(existing)) if existing.refresh.refreshed_at >= self.refresh.refreshed_at => {
                None
            }
            Some(Ok(existing)) => Some(ReferenceRefreshRecord {
                refresh: self.refresh.clone(),
                ..existing
            }),
            Some(Err(_)) | None => Some(ReferenceRefreshRecord::new(
                self.refresh.clone(),
                unix_timestamp_millis(),
            )),
        }
    }
}

impl Operation for RefreshJobWrite {
    type Output = ();
    type Error = ReferenceRefreshError;

    fn first(&mut self) -> Result<Next<()>, ReferenceRefreshError> {
        let key = job_key(&self.refresh)?;
        Ok(Next::Call(read_row(REFRESH_JOB_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ReferenceRefreshError> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let existing = result.value()?;
        let Some(record) = self.merged(existing.as_deref()) else {
            return Ok(Next::Done(()));
        };
        self.writing = true;
        Ok(Next::Call(IoCall::Write(job_entries(&record)?)))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Debug, PartialEq)]
pub struct QueueRefreshOperation {
    refresh: ReferenceRefresh,
    conflicts: u32,
    scheduling: bool,
}

impl QueueRefreshOperation {
    pub fn new(refresh: ReferenceRefresh) -> Self {
        Self {
            refresh,
            conflicts: 0,
            scheduling: false,
        }
    }

    fn write_job(&self) -> QueueTurn {
        Ok(Next::Call(IoCall::child(RefreshJobWrite {
            refresh: self.refresh.clone(),
            writing: false,
        })))
    }

    fn job_written(&mut self, written: Result<(), ReferenceRefreshError>) -> QueueTurn {
        match written {
            Ok(()) => {
                self.scheduling = true;
                Ok(Next::Call(schedule_drain().into()))
            }
            Err(ReferenceRefreshError::Storage(StorageError::TransactionConflict))
                if self.conflicts < QUEUE_CONFLICT_RETRIES =>
            {
                self.conflicts += 1;
                self.write_job()
            }
            Err(error) => Err(error),
        }
    }
}

impl Operation for QueueRefreshOperation {
    type Output = QueueRefreshResult;
    type Error = ReferenceRefreshError;

    fn first(&mut self) -> QueueTurn {
        self.write_job()
    }

    fn next(&mut self, result: IoResult) -> QueueTurn {
        if !self.scheduling {
            return self.job_written(result.child::<(), ReferenceRefreshError>()?);
        }
        // The job is durable, so a drain timer that cannot be set is only reported.
        let scheduled = match result.event() {
            Ok(Event::Task(TaskEvent::TimerScheduled { .. })) => true,
            Ok(Event::Task(TaskEvent::Error { .. })) => false,
            other => {
                warn!(event = ?other, "Reference metadata refresh job persisted but drain scheduling returned an unexpected event");
                false
            }
        };
        Ok(Next::Done(QueueRefreshResult {
            queued: true,
            scheduled,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, VersionKey};
    use aruna_storage::StorageHandle;

    use crate::driver::DriverContext;
    use crate::s3::object::metadata_drain::{
        jobs_exist, process_batch, refresh_with_context, settle_job,
    };
    use aruna_core::UserId;
    use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    struct TestState {
        _storage_dir: tempfile::TempDir,
        context: DriverContext,
        bucket: String,
        key: String,
        version_id: Ulid,
        created_by: UserId,
    }

    fn setup_state() -> TestState {
        let storage_dir = tempdir().unwrap();
        let storage_handle = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        TestState {
            _storage_dir: storage_dir,
            context: DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                graph_handle: None,
                task_handle: None,
                compute_handle: None,
                outbound_handle: None,
            },
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::generate(),
            created_by: UserId::local(Ulid::generate(), realm_id),
        }
    }

    fn source_binding() -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: std::collections::HashMap::new(),
                source_path: "source/path".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        }
    }

    fn source_metadata(content_length: u64, etag: &str) -> SourceMetadata {
        SourceMetadata {
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some(etag.to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(content_length)),
            source_version: None,
            digests: Default::default(),
        }
    }

    fn refresh(
        test: &TestState,
        metadata: SourceMetadata,
        refreshed_at: SystemTime,
    ) -> ReferenceRefresh {
        ReferenceRefresh {
            bucket: test.bucket.clone(),
            key: test.key.clone(),
            version_id: test.version_id,
            metadata: Some(metadata),
            refreshed_at,
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            auth_context: aruna_core::structs::identity::auth::AuthContext {
                user_id: Default::default(),
                realm_id: aruna_core::structs::identity::realm::RealmId([9u8; 32]),
                path_restrictions: None,
                session: None,
            },
        }
    }

    async fn write_reference_version(
        test: &TestState,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
        advance_count: u16,
    ) {
        let version = BlobVersion::reference(
            source_binding(),
            cached_metadata,
            SystemTime::UNIX_EPOCH,
            test.created_by,
            last_refresh,
        )
        .with_metadata(std::collections::HashMap::from([(
            "mtime".to_string(),
            "1753272000.123456789".to_string(),
        )]))
        .with_advance_count(advance_count);
        match test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&test.bucket, &test.key, test.version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected version write event: {other:?}"),
        }
    }

    async fn read_reference(test: &TestState) -> BlobVersion {
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&test.bucket, &test.key, test.version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected version read event");
        };
        BlobVersion::from_bytes(value.as_ref()).unwrap()
    }

    async fn assert_reference_state(
        test: &TestState,
        expected_metadata: &SourceMetadata,
        expected_last_refresh: SystemTime,
    ) {
        let version = read_reference(test).await;
        assert_eq!(
            version.metadata.get("mtime").map(String::as_str),
            Some("1753272000.123456789")
        );
        let BlobVersionState::Reference {
            cached_metadata,
            last_refresh,
            ..
        } = version.state
        else {
            panic!("version was not a reference");
        };
        assert_eq!(cached_metadata, *expected_metadata);
        assert_eq!(last_refresh, expected_last_refresh);
    }

    async fn read_jobs(storage: &StorageHandle) -> Vec<ReferenceRefreshRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REFRESH_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| postcard::from_bytes(&value).expect("refresh job decodes"))
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn write_rows(storage: &StorageHandle, writes: Vec<(String, Key, ByteView)>) {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected refresh job write event: {other:?}"),
        }
    }

    async fn write_corrupt_job(storage: &StorageHandle, key: &str) {
        let writes = vec![
            (
                REFRESH_DUE_KEYSPACE.to_string(),
                due_key(0, key.as_bytes()),
                ByteView::from(Vec::new()),
            ),
            (
                REFRESH_JOB_KEYSPACE.to_string(),
                ByteView::from(key.as_bytes().to_vec()),
                ByteView::from(Vec::new()),
            ),
        ];
        write_rows(storage, writes).await;
    }

    fn written(result: Result<(), ReferenceRefreshError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn conflict() -> Result<(), ReferenceRefreshError> {
        Err(ReferenceRefreshError::from(IoError::Conflict))
    }

    #[test]
    fn job_write_merges() {
        // A newer request replaces the payload and keeps the due time; an older one writes nothing.
        let test = setup_state();
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let queued = ReferenceRefreshRecord::new(refresh(&test, source_metadata(1, "a"), at), 77);
        let stored = postcard::to_allocvec(&queued).unwrap();
        let later = at + Duration::from_secs(1);
        let newer = refresh(&test, source_metadata(2, "b"), later);
        let mut write = RefreshJobWrite {
            refresh: newer.clone(),
            writing: false,
        };
        assert_eq!(write.transaction(), TransactionMode::Write);
        assert_eq!(
            write.first(),
            Ok(Next::Call(read_row(
                REFRESH_JOB_KEYSPACE,
                job_key(&newer).unwrap()
            )))
        );
        let expected = ReferenceRefreshRecord::new(newer, 77);
        assert_eq!(
            write.next(IoResult::Values(vec![Some(stored.clone().into())])),
            Ok(Next::Call(IoCall::Write(job_entries(&expected).unwrap())))
        );
        assert_eq!(write.next(IoResult::Done), Ok(Next::Done(())));

        let mut older = RefreshJobWrite {
            refresh: refresh(&test, source_metadata(3, "c"), at),
            writing: false,
        };
        older.first().unwrap();
        let answer = IoResult::Values(vec![Some(stored.into())]);
        assert_eq!(older.next(answer), Ok(Next::Done(())));
    }

    #[test]
    fn queue_retries_conflicts() {
        // A lost conflict rereads the job, up to the retry limit.
        let test = setup_state();
        let request = refresh(&test, source_metadata(1, "a"), SystemTime::UNIX_EPOCH);
        let mut operation = QueueRefreshOperation::new(request);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        for _ in 0..QUEUE_CONFLICT_RETRIES {
            assert!(matches!(
                operation.next(written(conflict())),
                Ok(Next::Call(IoCall::Child(_)))
            ));
        }
        assert_eq!(
            operation.next(written(conflict())),
            Err(conflict().unwrap_err())
        );
    }

    #[test]
    fn queue_schedules_drain() {
        let test = setup_state();
        let request = refresh(&test, source_metadata(1, "a"), SystemTime::UNIX_EPOCH);
        let mut operation = QueueRefreshOperation::new(request);
        operation.first().unwrap();
        assert_eq!(
            operation.next(written(Ok(()))),
            Ok(Next::Call(schedule_drain().into()))
        );
        let refused = IoResult::Event(Box::new(Event::Task(TaskEvent::Error {
            key: None,
            message: "stopped".to_string(),
        })));
        assert_eq!(
            operation.next(refused),
            Ok(Next::Done(QueueRefreshResult {
                queued: true,
                scheduled: false
            }))
        );
    }

    #[tokio::test]
    async fn drain_preserves_guard() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        let original_metadata = source_metadata(10, "original");
        write_reference_version(&test, original_metadata.clone(), last_refresh, 0).await;

        crate::driver::drive(
            QueueRefreshOperation::new(refresh(
                &test,
                source_metadata(20, "older"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(10),
            )),
            &test.context,
        )
        .await
        .expect("queue succeeds");
        let result = process_batch(&test.context).await.expect("drain succeeds");

        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&test.context.storage_handle).await.is_empty());
        assert_reference_state(&test, &original_metadata, last_refresh).await;
    }

    // Fields under a version id never change: a changed observation is no refresh.
    #[tokio::test]
    async fn changed_refresh_ignored() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let original = source_metadata(10, "original");
        write_reference_version(&test, original.clone(), last_refresh, 0).await;

        crate::driver::drive(
            QueueRefreshOperation::new(refresh(&test, source_metadata(20, "newer"), refreshed_at)),
            &test.context,
        )
        .await
        .expect("queue succeeds");
        process_batch(&test.context).await.expect("drain succeeds");

        assert_reference_state(&test, &original, last_refresh).await;
    }

    // A confirmed change of the current version is recorded as a successor that becomes the
    // head; the refreshed version keeps its fields.
    #[tokio::test]
    async fn change_mints_successor() {
        use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, S3_BUCKET_KEYSPACE};
        use aruna_core::structs::storage::blob::{BlobHeadKey, BucketInfo, CurrentVersionPointer};
        let test = setup_state();
        let original = source_metadata(10, "original");
        write_reference_version(&test, original.clone(), SystemTime::UNIX_EPOCH, 0).await;
        let bucket = BucketInfo {
            group_id: Ulid::from_bytes([1; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: test.created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        };
        let head_key = BlobHeadKey::new(&test.bucket, &test.key)
            .to_bytes()
            .unwrap();
        let writes = vec![
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                head_key.clone().into(),
                CurrentVersionPointer::new(test.version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
            (
                S3_BUCKET_KEYSPACE.to_string(),
                test.bucket.as_bytes().to_vec().into(),
                bucket.to_bytes().unwrap().into(),
            ),
        ];
        test.context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        let changed = source_metadata(20, "changed");
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);

        refresh_with_context(&test.context, refresh(&test, changed.clone(), refreshed_at))
            .await
            .unwrap();

        assert_reference_state(&test, &original, SystemTime::UNIX_EPOCH).await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(head), ..
        }) = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: head_key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("the head must exist");
        };
        let head = CurrentVersionPointer::from_bytes(&head).unwrap();
        assert_ne!(head.version_id, test.version_id);
        assert_eq!(head.generation, 2);
    }

    // A version bound by recorded digests stays the head when its source changed: a successor
    // from the fresh observation would drop the digests that guard its reads.
    #[tokio::test]
    async fn digest_bound_kept() {
        use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
        use aruna_core::structs::checksum::ChecksumAlgorithm;
        use aruna_core::structs::execution::source_access::{
            DigestOrigin, DigestScope, SourceDigest,
        };
        use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
        let test = setup_state();
        let mut original = source_metadata(10, "original");
        let digest = SourceDigest {
            digest: vec![7; 16],
            scope: DigestScope::FullObject,
            origin: DigestOrigin::Manifest,
        };
        original.digests.insert(ChecksumAlgorithm::Md5, digest);
        write_reference_version(&test, original.clone(), SystemTime::UNIX_EPOCH, 0).await;
        let head_key = BlobHeadKey::new(&test.bucket, &test.key)
            .to_bytes()
            .unwrap();
        let pointer = CurrentVersionPointer::new(test.version_id)
            .to_bytes()
            .unwrap();
        // The bucket row lets a successor be minted, as in `change_mints_successor`.
        let bucket = aruna_core::structs::storage::blob::BucketInfo {
            group_id: Ulid::from_bytes([1; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: test.created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        };
        let writes = vec![
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                head_key.clone().into(),
                pointer.into(),
            ),
            (
                aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
                test.bucket.as_bytes().to_vec().into(),
                bucket.to_bytes().unwrap().into(),
            ),
        ];
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        test.context.storage_handle.send_storage_effect(write).await;
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let changed = refresh(&test, source_metadata(20, "changed"), refreshed_at);

        refresh_with_context(&test.context, changed).await.unwrap();

        assert_reference_state(&test, &original, SystemTime::UNIX_EPOCH).await;
        let read = StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: head_key.into(),
            txn_id: None,
        };
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(head), ..
        }) = test.context.storage_handle.send_storage_effect(read).await
        else {
            panic!("the head must exist");
        };
        let head = CurrentVersionPointer::from_bytes(&head).unwrap();
        assert_eq!(head.version_id, test.version_id);
    }

    // A request for a version that no longer exists is finished instead of retried forever.
    #[tokio::test]
    async fn obsolete_request_finished() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let mut request = refresh(&test, source_metadata(1, "unused"), refreshed_at);
        request.metadata = None;
        crate::driver::drive(QueueRefreshOperation::new(request), &test.context)
            .await
            .expect("queue succeeds");

        let result = process_batch(&test.context).await.expect("drain runs");

        assert_eq!((result.succeeded, result.failed), (1, 0));
        assert!(read_jobs(&test.context.storage_handle).await.is_empty());
    }

    #[tokio::test]
    async fn same_refresh_advances() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let original = source_metadata(10, "original");
        write_reference_version(&test, original.clone(), SystemTime::UNIX_EPOCH, 0).await;
        let mut unknown = original.clone();
        unknown.etag = None;
        let mut listed = original.clone();
        listed.content_type = None;

        for (observation, expected_refresh) in
            [(unknown, SystemTime::UNIX_EPOCH), (listed, refreshed_at)]
        {
            crate::driver::drive(
                QueueRefreshOperation::new(refresh(&test, observation, refreshed_at)),
                &test.context,
            )
            .await
            .expect("queue succeeds");
            process_batch(&test.context).await.expect("drain succeeds");

            assert_reference_state(&test, &original, expected_refresh).await;
        }
    }

    // A metadata refresh is not a rebind, so it must not hand the binding a fresh
    // automatic advance budget.
    #[tokio::test]
    async fn refresh_keeps_count() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let original = source_metadata(10, "original");
        write_reference_version(&test, original.clone(), SystemTime::UNIX_EPOCH, 7).await;

        crate::driver::drive(
            QueueRefreshOperation::new(refresh(&test, original.clone(), refreshed_at)),
            &test.context,
        )
        .await
        .expect("queue succeeds");
        process_batch(&test.context).await.expect("drain succeeds");

        assert_reference_state(&test, &original, refreshed_at).await;
        assert_eq!(read_reference(&test).await.advance_count(), Some(7));
    }

    #[tokio::test]
    async fn newest_job_kept() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH;
        let newer = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let older = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let original = source_metadata(1, "original");
        write_reference_version(&test, original.clone(), last_refresh, 0).await;

        crate::driver::drive(
            QueueRefreshOperation::new(refresh(&test, original.clone(), newer)),
            &test.context,
        )
        .await
        .expect("newer queue succeeds");
        crate::driver::drive(
            QueueRefreshOperation::new(refresh(&test, original.clone(), older)),
            &test.context,
        )
        .await
        .expect("older queue succeeds");

        let jobs = read_jobs(&test.context.storage_handle).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].refresh.refreshed_at, newer);

        let result = process_batch(&test.context).await.expect("drain succeeds");
        assert_eq!(result.succeeded, 1);
        assert_reference_state(&test, &original, newer).await;
    }

    // A drain that finishes a request a newer one replaced keeps the newer job queued.
    #[tokio::test]
    async fn replaced_job_kept() {
        let test = setup_state();
        let original = source_metadata(1, "original");
        let older = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let newer = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        for refreshed_at in [older, newer] {
            crate::driver::drive(
                QueueRefreshOperation::new(refresh(&test, original.clone(), refreshed_at)),
                &test.context,
            )
            .await
            .expect("queue succeeds");
        }
        let storage = &test.context.storage_handle;
        let queued = read_jobs(storage).await;
        let older_job = ReferenceRefreshRecord {
            refresh: refresh(&test, original, older),
            ..queued[0].clone()
        };
        let key = job_key(&older_job.refresh).unwrap();

        settle_job(storage, &key, &older_job, None).await.unwrap();

        assert_eq!(read_jobs(storage).await, queued);
        assert_eq!(queued[0].refresh.refreshed_at, newer);
        assert!(jobs_exist(storage).await.unwrap());
    }

    #[tokio::test]
    async fn future_retry_preserved() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let refresh = refresh(&test, source_metadata(30, "future"), refreshed_at);
        let future_job = ReferenceRefreshRecord {
            refresh: refresh.clone(),
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 1,
            last_error: Some("transient".to_string()),
        };
        let writes = job_entries(&future_job).expect("future job serializes");
        write_rows(&test.context.storage_handle, writes).await;

        crate::driver::drive(QueueRefreshOperation::new(refresh), &test.context)
            .await
            .expect("duplicate queue succeeds");

        assert_eq!(
            read_jobs(&test.context.storage_handle).await,
            vec![future_job]
        );
    }

    #[tokio::test]
    async fn future_not_due() {
        let test = setup_state();
        let due_at_ms = unix_timestamp_millis().saturating_add(60_000);
        let record = ReferenceRefreshRecord::new(
            refresh(
                &test,
                source_metadata(20, "future"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            ),
            due_at_ms,
        );
        write_rows(&test.context.storage_handle, job_entries(&record).unwrap()).await;

        let result = process_batch(&test.context)
            .await
            .expect("future-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_some());
    }

    #[tokio::test]
    async fn corrupt_job_deleted() {
        let test = setup_state();
        write_corrupt_job(&test.context.storage_handle, "000-corrupt-refresh-job").await;

        let result = process_batch(&test.context)
            .await
            .expect("corrupt-only refresh drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_none());
        assert!(!jobs_exist(&test.context.storage_handle).await.unwrap());
    }

    #[tokio::test]
    async fn corrupt_precedes_valid() {
        let test = setup_state();
        write_corrupt_job(&test.context.storage_handle, "000-corrupt-refresh-job").await;
        crate::driver::drive(
            QueueRefreshOperation::new(refresh(
                &test,
                source_metadata(20, "valid"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            )),
            &test.context,
        )
        .await
        .expect("queue succeeds");

        let result = process_batch(&test.context)
            .await
            .expect("mixed corrupt/valid refresh drain succeeds");

        assert_eq!(result.processed, 1);
        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&test.context.storage_handle).await.is_empty());
    }
}
