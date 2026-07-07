//! List multipart uploads for a bucket.
//!
//! The multipart upload keyspace is not indexed by bucket, so this operation
//! performs a single capped scan (up to [`ListMultipartUploadsOperation::SCAN_LIMIT`])
//! over the whole keyspace and filters/sorts/paginates in memory. This is an
//! accepted tradeoff for the current keyspace layout: multipart upload counts
//! are small and short-lived, so no dedicated per-bucket index is maintained.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_MULTIPART_UPLOAD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{MultipartUpload, MultipartUploadStatus};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::s3::listing::common_prefix_of;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListMultipartUploadsState {
    Init,
    StartTransaction,
    ReadUploads,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListMultipartUploadsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListMultipartUploadsState,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("ListMultipartUploads failed")]
    ListMultipartUploadsFailed,
}

#[derive(Debug, PartialEq)]
pub struct ListMultipartUploadsInput {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<Ulid>,
    pub max_uploads: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListMultipartUploadsResult {
    pub uploads: Vec<MultipartUpload>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
pub struct ListMultipartUploadsOperation {
    input: ListMultipartUploadsInput,
    state: ListMultipartUploadsState,
    txn_id: Option<Ulid>,
    output: Option<Result<ListMultipartUploadsResult, ListMultipartUploadsError>>,
}

impl ListMultipartUploadsOperation {
    pub const DEFAULT_MAX_UPLOADS: usize = 1_000;
    const SCAN_LIMIT: usize = 10_000;

    pub fn new(input: ListMultipartUploadsInput) -> Self {
        Self {
            input,
            state: ListMultipartUploadsState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListMultipartUploadsError) -> Effects {
        self.state = ListMultipartUploadsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ListMultipartUploadsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(ListMultipartUploadsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.state = ListMultipartUploadsState::ReadUploads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: Self::SCAN_LIMIT,
            txn_id: Some(txn_id),
        })]
    }

    fn prefix(&self) -> Option<&str> {
        self.input
            .prefix
            .as_deref()
            .filter(|prefix| !prefix.is_empty())
    }

    fn after_marker(&self, key: &str, upload_id: Ulid) -> bool {
        let Some(key_marker) = self.input.key_marker.as_deref() else {
            return true;
        };
        match key.cmp(key_marker) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => self
                .input
                .upload_id_marker
                .is_some_and(|marker| upload_id > marker),
        }
    }

    fn handle_uploads_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(ListMultipartUploadsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let mut uploads = Vec::new();
        for (_key, value) in values {
            let record = match MultipartUpload::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(err) => return self.emit_error(err.into()),
            };
            if record.bucket != self.input.bucket || record.status != MultipartUploadStatus::Open {
                continue;
            }
            if let Some(prefix) = self.prefix()
                && !record.key.starts_with(prefix)
            {
                continue;
            }
            if !self.after_marker(&record.key, record.upload_id) {
                continue;
            }
            uploads.push(record);
        }
        uploads.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.upload_id.cmp(&right.upload_id))
        });

        self.finish(uploads)
    }

    fn finish(&mut self, uploads: Vec<MultipartUpload>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(ListMultipartUploadsError::NoTransactionFound);
        };

        let prefix = self.input.prefix.as_deref();
        let delimiter = self.input.delimiter.as_deref();
        let max_uploads = self.input.max_uploads;

        let mut result_uploads = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut emitted = 0usize;
        let mut is_truncated = false;
        let mut next_key_marker = None;
        let mut next_upload_id_marker = None;

        let mut index = 0;
        while index < uploads.len() {
            if emitted >= max_uploads {
                is_truncated = true;
                break;
            }
            let upload = &uploads[index];
            match common_prefix_of(&upload.key, prefix, delimiter) {
                Some(group) => {
                    let mut last = index;
                    while last + 1 < uploads.len()
                        && common_prefix_of(&uploads[last + 1].key, prefix, delimiter).as_deref()
                            == Some(group.as_str())
                    {
                        last += 1;
                    }
                    next_key_marker = Some(uploads[last].key.clone());
                    next_upload_id_marker = Some(uploads[last].upload_id);
                    common_prefixes.push(group);
                    emitted += 1;
                    index = last + 1;
                }
                None => {
                    next_key_marker = Some(upload.key.clone());
                    next_upload_id_marker = Some(upload.upload_id);
                    result_uploads.push(upload.clone());
                    emitted += 1;
                    index += 1;
                }
            }
        }

        if !is_truncated {
            next_key_marker = None;
            next_upload_id_marker = None;
        }

        self.state = ListMultipartUploadsState::CommitTransaction;
        self.output = Some(Ok(ListMultipartUploadsResult {
            uploads: result_uploads,
            common_prefixes,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        }));
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(ListMultipartUploadsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.state = ListMultipartUploadsState::Finish;
        smallvec![]
    }
}

impl Operation for ListMultipartUploadsOperation {
    type Output = Option<Result<ListMultipartUploadsResult, ListMultipartUploadsError>>;
    type Error = ListMultipartUploadsError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(ListMultipartUploadsError::StorageError(error));
        }

        match self.state {
            ListMultipartUploadsState::Init => self.handle_init(),
            ListMultipartUploadsState::StartTransaction => self.handle_transaction_started(event),
            ListMultipartUploadsState::ReadUploads => self.handle_uploads_read(event),
            ListMultipartUploadsState::CommitTransaction => {
                self.handle_transaction_committed(event)
            }
            ListMultipartUploadsState::Finish | ListMultipartUploadsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListMultipartUploadsState::Finish | ListMultipartUploadsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListMultipartUploadsState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListMultipartUploadsError::ListMultipartUploadsFailed);
        }
        Ok(self.output)
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
    use aruna_core::structs::RealmId;
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn driver_context(storage_handle: storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    fn upload_record(upload_id: Ulid, bucket: &str, key: &str) -> MultipartUpload {
        MultipartUpload {
            upload_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id: Ulid::new(),
            created_by: UserId::local(Ulid::new(), RealmId::from_bytes([1u8; 32])),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
        }
    }

    async fn seed_upload(storage_handle: &storage::StorageHandle, record: &MultipartUpload) {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
                key: record.upload_id.to_bytes().to_vec().into(),
                value: record.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    fn input(bucket: &str, max_uploads: usize) -> ListMultipartUploadsInput {
        ListMultipartUploadsInput {
            bucket: bucket.to_string(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads,
        }
    }

    #[tokio::test]
    async fn list_multipart_uploads_filters_by_bucket() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        seed_upload(&storage_handle, &upload_record(Ulid::new(), "bucket", "a")).await;
        seed_upload(&storage_handle, &upload_record(Ulid::new(), "other", "b")).await;
        seed_upload(&storage_handle, &upload_record(Ulid::new(), "bucket", "c")).await;

        let result = drive(
            ListMultipartUploadsOperation::new(input(
                "bucket",
                ListMultipartUploadsOperation::DEFAULT_MAX_UPLOADS,
            )),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
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
    async fn list_multipart_uploads_orders_by_key_then_upload_id() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let first = Ulid::new();
        let second = Ulid::new();
        let (low, high) = if first < second {
            (first, second)
        } else {
            (second, first)
        };
        // "b" comes after "aa" lexicographically; two uploads share key "aa".
        seed_upload(&storage_handle, &upload_record(high, "bucket", "aa")).await;
        seed_upload(&storage_handle, &upload_record(low, "bucket", "aa")).await;
        seed_upload(&storage_handle, &upload_record(Ulid::new(), "bucket", "b")).await;

        let result = drive(
            ListMultipartUploadsOperation::new(input(
                "bucket",
                ListMultipartUploadsOperation::DEFAULT_MAX_UPLOADS,
            )),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
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
    async fn list_multipart_uploads_paginates_with_markers() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for key in ["a", "b", "c", "d"] {
            seed_upload(&storage_handle, &upload_record(Ulid::new(), "bucket", key)).await;
        }

        let mut key_marker = None;
        let mut upload_id_marker = None;
        let mut collected = Vec::new();
        loop {
            let result = drive(
                ListMultipartUploadsOperation::new(ListMultipartUploadsInput {
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
            .unwrap()
            .unwrap()
            .unwrap();

            collected.extend(result.uploads.iter().map(|upload| upload.key.clone()));
            if result.is_truncated {
                key_marker = result.next_key_marker.clone();
                upload_id_marker = result.next_upload_id_marker;
                assert!(key_marker.is_some());
            } else {
                break;
            }
        }

        assert_eq!(collected, vec!["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn list_multipart_uploads_filters_by_prefix() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for key in ["docs/1", "docs/2", "images/1", "readme"] {
            seed_upload(&storage_handle, &upload_record(Ulid::new(), "bucket", key)).await;
        }

        let result = drive(
            ListMultipartUploadsOperation::new(ListMultipartUploadsInput {
                bucket: "bucket".to_string(),
                prefix: Some("docs/".to_string()),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: ListMultipartUploadsOperation::DEFAULT_MAX_UPLOADS,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let keys: Vec<&str> = result
            .uploads
            .iter()
            .map(|upload| upload.key.as_str())
            .collect();
        assert_eq!(keys, vec!["docs/1", "docs/2"]);
    }

    #[tokio::test]
    async fn list_multipart_uploads_groups_by_delimiter() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for key in ["a.txt", "dir/1", "dir/2", "z.txt"] {
            seed_upload(&storage_handle, &upload_record(Ulid::new(), "bucket", key)).await;
        }

        let result = drive(
            ListMultipartUploadsOperation::new(ListMultipartUploadsInput {
                bucket: "bucket".to_string(),
                prefix: None,
                delimiter: Some("/".to_string()),
                key_marker: None,
                upload_id_marker: None,
                max_uploads: ListMultipartUploadsOperation::DEFAULT_MAX_UPLOADS,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
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
