//! List multipart uploads for a bucket. Scans the global multipart keyspace and
//! filters, sorts, and paginates in memory because no per-bucket index exists.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::UPLOAD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::multipart::{MultipartUpload, MultipartUploadStatus};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::s3::listing::{ListMarker, build_page, retain_after_marker};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListUploadsState {
    Init,
    StartTransaction,
    ReadUploads,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListUploadsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("multipart upload scan budget exhausted")]
    ScanBudgetExceeded,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListUploadsState,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("ListMultipartUploads failed")]
    ListUploadsFailed,
    #[error("operation did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
pub struct ListUploadsInput {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<Ulid>,
    pub max_uploads: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListUploadsResult {
    pub uploads: Vec<MultipartUpload>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_marker: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
pub struct ListUploadsOperation {
    input: ListUploadsInput,
    state: ListUploadsState,
    txn_id: Option<Ulid>,
    uploads: Vec<MultipartUpload>,
    scan_cursor: Option<Key>,
    scan_rows: usize,
    scan_round_limit: usize,
    max_scan_rows: usize,
    include_in_progress: bool,
    output: Option<Result<ListUploadsResult, ListUploadsError>>,
}

impl ListUploadsOperation {
    pub const DEFAULT_MAX_UPLOADS: usize = 1_000;
    const SCAN_ROUND_LIMIT: usize = 100;
    const MAX_SCAN_ROWS: usize = 10_000;

    pub fn new(input: ListUploadsInput) -> Self {
        Self {
            input,
            state: ListUploadsState::Init,
            txn_id: None,
            uploads: Vec::new(),
            scan_cursor: None,
            scan_rows: 0,
            scan_round_limit: Self::SCAN_ROUND_LIMIT,
            max_scan_rows: Self::MAX_SCAN_ROWS,
            include_in_progress: false,
            output: None,
        }
    }

    /// Internal purge inventory must also see Completing/Aborting rows so its
    /// final emptiness proof cannot race a multipart state transition.
    pub fn including_in_progress(mut self) -> Self {
        self.include_in_progress = true;
        self
    }

    /// Deletion inventory cannot mistake the global scan safety budget for a
    /// complete bucket result. Callers still bound the returned page.
    pub fn complete_scan(mut self) -> Self {
        self.max_scan_rows = usize::MAX;
        self
    }

    #[cfg(test)]
    fn with_scan_limit(mut self, scan_round_limit: usize) -> Self {
        self.scan_round_limit = scan_round_limit;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_scan_budget(mut self, max_scan_rows: usize) -> Self {
        self.max_scan_rows = max_scan_rows;
        self
    }

    fn emit_error(&mut self, error: ListUploadsError) -> Effects {
        self.state = ListUploadsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        // Mirror the key listings: max_uploads=0 returns an empty,
        // non-truncated result instead of truncating without a resume marker.
        if self.input.max_uploads == 0 {
            self.state = ListUploadsState::Finish;
            self.output = Some(Ok(ListUploadsResult {
                uploads: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_upload_marker: None,
            }));
            return smallvec![];
        }

        self.state = ListUploadsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(ListUploadsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.issue_upload_round()
    }

    fn issue_upload_round(&mut self) -> Effects {
        let Some(remaining) = self.max_scan_rows.checked_sub(self.scan_rows) else {
            return self.emit_error(ListUploadsError::ScanBudgetExceeded);
        };
        if remaining == 0 {
            return self.emit_error(ListUploadsError::ScanBudgetExceeded);
        }
        self.state = ListUploadsState::ReadUploads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: UPLOAD_KEYSPACE.to_string(),
            prefix: None,
            start: self.scan_cursor.take().map(IterStart::After),
            limit: self.scan_round_limit.min(remaining),
            txn_id: self.txn_id,
        })]
    }

    fn prefix(&self) -> Option<&str> {
        self.input
            .prefix
            .as_deref()
            .filter(|prefix| !prefix.is_empty())
    }

    fn handle_uploads_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.emit_error(ListUploadsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let Some(remaining) = self.max_scan_rows.checked_sub(self.scan_rows) else {
            return self.emit_error(ListUploadsError::ScanBudgetExceeded);
        };
        if values.len() > remaining {
            return self.emit_error(ListUploadsError::ScanBudgetExceeded);
        }
        self.scan_rows += values.len();

        for (_key, value) in values {
            let record = match MultipartUpload::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(err) => return self.emit_error(err.into()),
            };
            if record.bucket != self.input.bucket
                || (!self.include_in_progress && record.status != MultipartUploadStatus::Open)
            {
                continue;
            }
            if let Some(prefix) = self.prefix()
                && !record.key.starts_with(prefix)
            {
                continue;
            }
            self.uploads.push(record);
        }
        let mut uploads = std::mem::take(&mut self.uploads);
        self.apply_marker(&mut uploads);
        self.uploads = uploads;

        if next_start_after.is_none() {
            return self.finish_upload_scan();
        }
        if self.scan_rows >= self.max_scan_rows {
            return self.emit_error(ListUploadsError::ScanBudgetExceeded);
        }

        self.scan_cursor = next_start_after;
        self.issue_upload_round()
    }

    fn finish_upload_scan(&mut self) -> Effects {
        let mut uploads = std::mem::take(&mut self.uploads);
        uploads.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.upload_id.cmp(&right.upload_id))
        });
        self.apply_marker(&mut uploads);

        self.finish(uploads)
    }

    fn apply_marker(&self, uploads: &mut Vec<MultipartUpload>) {
        let marker = self.input.key_marker.as_deref().map(|key| ListMarker {
            key,
            id: self.input.upload_id_marker,
        });
        retain_after_marker(
            uploads,
            marker,
            |upload| upload.key.as_str(),
            |upload| upload.upload_id,
        );
    }

    fn finish(&mut self, uploads: Vec<MultipartUpload>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(ListUploadsError::NoTransactionFound);
        };

        let page = build_page(
            &uploads,
            self.input.max_uploads,
            self.input.prefix.as_deref(),
            self.input.delimiter.as_deref(),
            |upload| upload.key.as_str(),
        );
        let (next_key_marker, next_upload_marker) =
            match page.last_index.filter(|_| page.truncated) {
                Some(index) => (
                    Some(uploads[index].key.clone()),
                    Some(uploads[index].upload_id),
                ),
                None => (None, None),
            };

        self.state = ListUploadsState::CommitTransaction;
        self.output = Some(Ok(ListUploadsResult {
            uploads: page.entries,
            common_prefixes: page.prefixes,
            is_truncated: page.truncated,
            next_key_marker,
            next_upload_marker,
        }));
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(ListUploadsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.state = ListUploadsState::Finish;
        smallvec![]
    }
}

impl Operation for ListUploadsOperation {
    type Output = ListUploadsResult;
    type Error = ListUploadsError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(ListUploadsError::StorageError(error));
        }

        match self.state {
            ListUploadsState::Init => self.handle_init(),
            ListUploadsState::StartTransaction => self.handle_transaction_started(event),
            ListUploadsState::ReadUploads => self.handle_uploads_read(event),
            ListUploadsState::CommitTransaction => self.handle_transaction_committed(event),
            ListUploadsState::Finish | ListUploadsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListUploadsState::Finish | ListUploadsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(ListUploadsError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_storage::storage;
    use std::time::{Duration, SystemTime};
    use tempfile::tempdir;

    fn driver_context(storage_handle: storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn upload_record(upload_id: Ulid, bucket: &str, key: &str) -> MultipartUpload {
        upload_record_at(upload_id, bucket, key, SystemTime::UNIX_EPOCH)
    }

    fn upload_record_at(
        upload_id: Ulid,
        bucket: &str,
        key: &str,
        created_at: SystemTime,
    ) -> MultipartUpload {
        MultipartUpload {
            backend: BackendRef::node_default(),
            storage_class: None,
            upload_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id: Ulid::generate(),
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            created_at,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: Default::default(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
        }
    }

    async fn seed_upload(storage_handle: &storage::StorageHandle, record: &MultipartUpload) {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: UPLOAD_KEYSPACE.to_string(),
                key: record.upload_id.to_bytes().to_vec().into(),
                value: record.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    fn input(bucket: &str, max_uploads: usize) -> ListUploadsInput {
        ListUploadsInput {
            bucket: bucket.to_string(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads,
        }
    }

    #[tokio::test]
    async fn bucket_filters() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        seed_upload(
            &storage_handle,
            &upload_record(Ulid::generate(), "bucket", "a"),
        )
        .await;
        seed_upload(
            &storage_handle,
            &upload_record(Ulid::generate(), "other", "b"),
        )
        .await;
        seed_upload(
            &storage_handle,
            &upload_record(Ulid::generate(), "bucket", "c"),
        )
        .await;

        let result = drive(
            ListUploadsOperation::new(input("bucket", ListUploadsOperation::DEFAULT_MAX_UPLOADS)),
            &driver_ctx,
        )
        .await
        .unwrap();

        let keys: Vec<&str> = result
            .uploads
            .iter()
            .map(|upload| upload.key.as_str())
            .collect();
        assert_eq!(keys, vec!["a", "c"]);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn scans_foreign_rounds() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        seed_upload(
            &storage_handle,
            &upload_record(Ulid::from_bytes([1; 16]), "other", "a"),
        )
        .await;
        seed_upload(
            &storage_handle,
            &upload_record(Ulid::from_bytes([2; 16]), "other", "b"),
        )
        .await;
        seed_upload(
            &storage_handle,
            &upload_record(Ulid::from_bytes([3; 16]), "bucket", "target"),
        )
        .await;

        let result = drive(
            ListUploadsOperation::new(input("bucket", 1)).with_scan_limit(1),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result.uploads.len(), 1);
        assert_eq!(result.uploads[0].key, "target");
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn scan_budget_fails() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for (index, key) in ["a", "b", "c"].into_iter().enumerate() {
            seed_upload(
                &storage_handle,
                &upload_record(Ulid::from_bytes([(index + 1) as u8; 16]), "other", key),
            )
            .await;
        }

        let result = drive(
            ListUploadsOperation::new(input("bucket", 1)).with_scan_budget(2),
            &driver_ctx,
        )
        .await;

        assert_eq!(result, Err(ListUploadsError::ScanBudgetExceeded));
    }

    #[tokio::test]
    async fn sorts_before_cutoff() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        seed_upload(
            &storage_handle,
            &upload_record(Ulid::from_bytes([1; 16]), "bucket", "z"),
        )
        .await;
        seed_upload(
            &storage_handle,
            &upload_record(Ulid::from_bytes([2; 16]), "bucket", "y"),
        )
        .await;
        seed_upload(
            &storage_handle,
            &upload_record(Ulid::from_bytes([3; 16]), "bucket", "a"),
        )
        .await;

        let result = drive(
            ListUploadsOperation::new(input("bucket", 1)).with_scan_limit(2),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result.uploads.len(), 1);
        assert_eq!(result.uploads[0].key, "a");
        assert!(result.is_truncated);
    }

    #[tokio::test]
    async fn zero_limit_empty() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        seed_upload(
            &storage_handle,
            &upload_record(Ulid::generate(), "bucket", "a"),
        )
        .await;

        let result = drive(ListUploadsOperation::new(input("bucket", 0)), &driver_ctx)
            .await
            .unwrap();

        assert!(result.uploads.is_empty());
        assert!(!result.is_truncated);
        assert_eq!(result.next_key_marker, None);
        assert_eq!(result.next_upload_marker, None);
    }

    #[tokio::test]
    async fn key_order_stable() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let first = Ulid::generate();
        let second = Ulid::generate();
        let (low, high) = if first < second {
            (first, second)
        } else {
            (second, first)
        };
        // "b" comes after "aa" lexicographically; two uploads share key "aa".
        seed_upload(&storage_handle, &upload_record(high, "bucket", "aa")).await;
        seed_upload(&storage_handle, &upload_record(low, "bucket", "aa")).await;
        seed_upload(
            &storage_handle,
            &upload_record(Ulid::generate(), "bucket", "b"),
        )
        .await;

        let result = drive(
            ListUploadsOperation::new(input("bucket", ListUploadsOperation::DEFAULT_MAX_UPLOADS)),
            &driver_ctx,
        )
        .await
        .unwrap();

        let ordered: Vec<(&str, Ulid)> = result
            .uploads
            .iter()
            .map(|upload| (upload.key.as_str(), upload.upload_id))
            .collect();
        assert_eq!(
            ordered,
            vec![
                ("aa", low),
                ("aa", high),
                ("b", result.uploads[2].upload_id)
            ]
        );
    }

    #[tokio::test]
    async fn orders_same_key() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let versions = {
            let first = Ulid::generate();
            let second = Ulid::generate();
            if first < second {
                (first, second)
            } else {
                (second, first)
            }
        };
        let newer_lower_upload_id = versions.0;
        let older_higher_upload_id = versions.1;
        seed_upload(
            &storage_handle,
            &upload_record_at(
                newer_lower_upload_id,
                "bucket",
                "same-key",
                SystemTime::UNIX_EPOCH + Duration::from_secs(2),
            ),
        )
        .await;
        seed_upload(
            &storage_handle,
            &upload_record_at(
                older_higher_upload_id,
                "bucket",
                "same-key",
                SystemTime::UNIX_EPOCH + Duration::from_secs(1),
            ),
        )
        .await;

        let result = drive(
            ListUploadsOperation::new(input("bucket", ListUploadsOperation::DEFAULT_MAX_UPLOADS)),
            &driver_ctx,
        )
        .await
        .unwrap();

        let ordered: Vec<Ulid> = result
            .uploads
            .iter()
            .map(|upload| upload.upload_id)
            .collect();
        assert_eq!(ordered, vec![newer_lower_upload_id, older_higher_upload_id]);
    }

    #[tokio::test]
    async fn markers_paginate() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for key in ["a", "b", "c", "d"] {
            seed_upload(
                &storage_handle,
                &upload_record(Ulid::generate(), "bucket", key),
            )
            .await;
        }

        let mut key_marker = None;
        let mut upload_id_marker = None;
        let mut collected = Vec::new();
        loop {
            let result = drive(
                ListUploadsOperation::new(ListUploadsInput {
                    bucket: "bucket".to_string(),
                    prefix: None,
                    delimiter: None,
                    key_marker: key_marker.clone(),
                    upload_id_marker,
                    max_uploads: 2,
                }),
                &driver_ctx,
            )
            .await
            .unwrap();

            collected.extend(result.uploads.iter().map(|upload| upload.key.clone()));
            if result.is_truncated {
                key_marker = result.next_key_marker.clone();
                upload_id_marker = result.next_upload_marker;
                assert!(key_marker.is_some());
            } else {
                break;
            }
        }

        assert_eq!(collected, vec!["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn resumes_same_key() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let ids = [
            Ulid::from_bytes([1; 16]),
            Ulid::from_bytes([2; 16]),
            Ulid::from_bytes([3; 16]),
        ];
        for (index, upload_id) in ids.iter().enumerate() {
            seed_upload(
                &storage_handle,
                &upload_record_at(
                    *upload_id,
                    "bucket",
                    "same-key",
                    SystemTime::UNIX_EPOCH + Duration::from_secs((ids.len() - index) as u64),
                ),
            )
            .await;
        }

        let first = drive(
            ListUploadsOperation::new(ListUploadsInput {
                bucket: "bucket".to_string(),
                prefix: None,
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 2,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();
        assert!(first.is_truncated);
        assert_eq!(
            first
                .uploads
                .iter()
                .map(|upload| upload.upload_id)
                .collect::<Vec<_>>(),
            ids[..2]
        );

        let second = drive(
            ListUploadsOperation::new(ListUploadsInput {
                bucket: "bucket".to_string(),
                prefix: None,
                delimiter: None,
                key_marker: first.next_key_marker.clone(),
                upload_id_marker: first.next_upload_marker,
                max_uploads: 2,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(second.uploads.len(), 1);
        assert_eq!(second.uploads[0].upload_id, ids[2]);
        assert!(!second.is_truncated);
    }

    #[tokio::test]
    async fn resumes_missing_marker() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let lower = Ulid::from_bytes([1; 16]);
        let marker = Ulid::from_bytes([2; 16]);
        let higher = Ulid::from_bytes([3; 16]);
        seed_upload(&storage_handle, &upload_record(lower, "bucket", "same-key")).await;
        seed_upload(
            &storage_handle,
            &upload_record(higher, "bucket", "same-key"),
        )
        .await;

        let result = drive(
            ListUploadsOperation::new(ListUploadsInput {
                bucket: "bucket".to_string(),
                prefix: None,
                delimiter: None,
                key_marker: Some("same-key".to_string()),
                upload_id_marker: Some(marker),
                max_uploads: 2,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(
            result
                .uploads
                .iter()
                .map(|upload| upload.upload_id)
                .collect::<Vec<_>>(),
            vec![higher]
        );
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn prefix_filters() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for key in ["docs/1", "docs/2", "images/1", "readme"] {
            seed_upload(
                &storage_handle,
                &upload_record(Ulid::generate(), "bucket", key),
            )
            .await;
        }

        let result = drive(
            ListUploadsOperation::new(ListUploadsInput {
                bucket: "bucket".to_string(),
                prefix: Some("docs/".to_string()),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: ListUploadsOperation::DEFAULT_MAX_UPLOADS,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        let keys: Vec<&str> = result
            .uploads
            .iter()
            .map(|upload| upload.key.as_str())
            .collect();
        assert_eq!(keys, vec!["docs/1", "docs/2"]);
    }

    #[tokio::test]
    async fn delimiter_groups() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for key in ["a.txt", "dir/1", "dir/2", "z.txt"] {
            seed_upload(
                &storage_handle,
                &upload_record(Ulid::generate(), "bucket", key),
            )
            .await;
        }

        let result = drive(
            ListUploadsOperation::new(ListUploadsInput {
                bucket: "bucket".to_string(),
                prefix: None,
                delimiter: Some("/".to_string()),
                key_marker: None,
                upload_id_marker: None,
                max_uploads: ListUploadsOperation::DEFAULT_MAX_UPLOADS,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        let keys: Vec<&str> = result
            .uploads
            .iter()
            .map(|upload| upload.key.as_str())
            .collect();
        assert_eq!(keys, vec!["a.txt", "z.txt"]);
        assert_eq!(result.common_prefixes, vec!["dir/"]);
        assert!(!result.is_truncated);
    }
}
