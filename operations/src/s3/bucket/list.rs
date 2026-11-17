//! Lists a group's buckets page by page, with an encoded cursor for the next page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::cursor;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ListBucketsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("ListBuckets failed")]
    ListBucketsFailed,
    #[error("could not list buckets: {0}")]
    Io(IoError),
}

impl From<IoError> for ListBucketsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<ListBucketsResult>, ListBucketsError>;

#[derive(Debug, Clone, PartialEq)]
pub struct ListBucketsInput {
    pub group_id: GroupId,
    pub prefix: Option<String>,
    pub continuation_token: Option<String>,
    pub max_buckets: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListBucketsResult {
    pub buckets: Vec<(String, BucketInfo)>,
    pub continuation_token: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct ListBucketsOperation {
    input: ListBucketsInput,
    matches: Vec<(String, BucketInfo)>,
    storage_start_after: Option<Key>,
    scanned_rows: usize,
}

impl ListBucketsOperation {
    const DEFAULT_MAX_BUCKETS: usize = 10_000;
    const MAX_BUCKETS: usize = 10_000;
    const SCAN_LIMIT: usize = 1_000;
    const MAX_SCAN_ROWS: usize = 10_000;

    pub fn new(input: ListBucketsInput) -> Self {
        Self {
            input,
            matches: Vec::new(),
            storage_start_after: None,
            scanned_rows: 0,
        }
    }

    fn max_buckets(&self) -> usize {
        self.input
            .max_buckets
            .filter(|limit| *limit > 0)
            .unwrap_or(Self::DEFAULT_MAX_BUCKETS)
            .min(Self::MAX_BUCKETS)
    }

    fn emit_scan(&mut self) -> Step {
        let after = match (&self.storage_start_after, &self.input.continuation_token) {
            (Some(key), _) => Some(key.clone()),
            (None, Some(token)) => Some(decode_cursor(token)?),
            (None, None) => None,
        };
        let remaining = Self::MAX_SCAN_ROWS.saturating_sub(self.scanned_rows);
        if remaining == 0 {
            return Err(ListBucketsError::ListBucketsFailed);
        }
        let prefix = self.input.prefix.clone().unwrap_or_default().into_bytes();
        Ok(Next::Call(IoCall::Page {
            space: S3_BUCKET_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after,
            limit: Self::SCAN_LIMIT.min(remaining),
        }))
    }

    fn handle_bucket_list(&mut self, result: IoResult) -> Step {
        let (values, next_start_after) = result.page()?;
        let remaining = Self::MAX_SCAN_ROWS.saturating_sub(self.scanned_rows);
        if values.len() > remaining {
            return Err(ListBucketsError::ListBucketsFailed);
        }
        self.scanned_rows += values.len();
        let max_buckets = self.max_buckets();

        for (key, value) in values {
            let bucket_info = BucketInfo::from_bytes(value.as_ref())?;
            if bucket_info.group_id != self.input.group_id {
                continue;
            }
            let bucket_name = String::from_utf8(key.to_vec())
                .map_err(|err| ListBucketsError::ConversionError(err.into()))?;
            self.matches.push((bucket_name, bucket_info));
        }

        if self.matches.len() > max_buckets {
            let continuation_key = self
                .matches
                .get(max_buckets - 1)
                .map(|(bucket, _)| Key::from(bucket.as_bytes().to_vec()));
            self.matches.truncate(max_buckets);
            return self.finish_cursor(continuation_key.as_ref());
        }

        // The group page is not yet full: follow the storage cursor into the next
        // raw page so group buckets past the first page stay reachable.
        if let Some(next) = next_start_after {
            self.storage_start_after = Some(next);
            if self.scanned_rows == Self::MAX_SCAN_ROWS {
                let cursor = self.storage_start_after.clone();
                return self.finish_cursor(cursor.as_ref());
            }
            return self.emit_scan();
        }

        Ok(self.finish(None))
    }

    fn finish(&mut self, continuation_token: Option<String>) -> Next<ListBucketsResult> {
        Next::Done(ListBucketsResult {
            buckets: std::mem::take(&mut self.matches),
            continuation_token,
        })
    }

    fn finish_cursor(&mut self, key: Option<&Key>) -> Step {
        let token = key.map(encode_cursor).transpose()?;
        Ok(self.finish(token))
    }
}

/// The continuation token that resumes a listing right after `bucket`.
pub fn bucket_cursor(bucket: &str) -> Result<String, ConversionError> {
    encode_cursor(&Key::from(bucket.as_bytes().to_vec()))
}

fn encode_cursor(key: &Key) -> Result<String, ConversionError> {
    Ok(cursor::encode_value(&key.to_vec())?)
}

fn decode_cursor(token: &str) -> Result<Key, ConversionError> {
    Ok(cursor::decode_value::<Vec<u8>>(token)?.into())
}

impl Operation for ListBucketsOperation {
    type Output = ListBucketsResult;
    type Error = ListBucketsError;

    fn first(&mut self) -> Step {
        self.emit_scan()
    }

    fn next(&mut self, result: IoResult) -> Step {
        self.handle_bucket_list(result)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::storage::format::Compression;
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_list_buckets() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let group_id = Ulid::generate();
        for (bucket, bucket_info) in [
            (
                "alpha".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                    distribution: None,
                },
            ),
            (
                "beta".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                    distribution: None,
                },
            ),
            (
                "foreign".to_string(),
                BucketInfo {
                    group_id: Ulid::generate(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                    distribution: None,
                },
            ),
        ] {
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: bucket.into(),
                    value: bucket_info.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
        }

        let result = drive(
            ListBucketsOperation::new(ListBucketsInput {
                group_id,
                prefix: None,
                continuation_token: None,
                max_buckets: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result.buckets.len(), 2);
        assert_eq!(
            result
                .buckets
                .into_iter()
                .map(|(bucket, _)| bucket)
                .collect::<Vec<_>>(),
            vec!["alpha".to_string(), "beta".to_string()]
        );
        assert_eq!(result.continuation_token, None);
    }

    #[test]
    fn scans_across_pages() {
        // Group buckets split across two raw pages must all surface; the first
        // page's storage cursor drives a second scan.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let other = Ulid::from_bytes([2u8; 16]);
        let info = |group_id| BucketInfo {
            group_id,
            created_at: SystemTime::now(),
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let entry = |name: &str, group_id| {
            (
                super::Key::from(name.as_bytes().to_vec()),
                info(group_id).to_bytes().unwrap().into(),
            )
        };

        let mut op = ListBucketsOperation::new(ListBucketsInput {
            group_id,
            prefix: None,
            continuation_token: None,
            max_buckets: Some(10),
        });
        let _ = op.first();

        let cursor: super::Key = "beta".as_bytes().to_vec().into();
        let next = op.next(IoResult::Page {
            rows: vec![entry("alpha", group_id), entry("beta", other)],
            next: Some(cursor.clone()),
        });
        match next {
            Ok(Next::Call(IoCall::Page {
                after: Some(key), ..
            })) => assert_eq!(key, cursor),
            other => panic!("unexpected call: {other:?}"),
        }

        let done = op.next(IoResult::Page {
            rows: vec![entry("gamma", group_id)],
            next: None,
        });
        let result = listed(done);
        let names: Vec<_> = result.buckets.into_iter().map(|(name, _)| name).collect();
        assert_eq!(names, vec!["alpha".to_string(), "gamma".to_string()]);
        assert_eq!(result.continuation_token, None);
    }

    fn listed(done: Step) -> ListBucketsResult {
        match done {
            Ok(Next::Done(result)) => result,
            other => panic!("expected the listing, got {other:?}"),
        }
    }

    #[test]
    fn caps_bucket_limit() {
        let operation = ListBucketsOperation::new(ListBucketsInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            prefix: None,
            continuation_token: None,
            max_buckets: Some(usize::MAX),
        });

        assert_eq!(operation.max_buckets(), ListBucketsOperation::MAX_BUCKETS);
    }

    #[test]
    fn output_cursor_resumes() {
        let group_id = Ulid::from_bytes([3u8; 16]);
        let info = |group_id| BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let entry = |name: &str, group_id| {
            (
                Key::from(name.as_bytes().to_vec()),
                info(group_id).to_bytes().unwrap().into(),
            )
        };
        let mut operation = ListBucketsOperation::new(ListBucketsInput {
            group_id,
            prefix: None,
            continuation_token: None,
            max_buckets: Some(1),
        });
        let _ = operation.first();
        let first = listed(operation.next(IoResult::Page {
            rows: vec![entry("alpha", group_id), entry("beta", group_id)],
            next: Some(Key::from(b"beta".to_vec())),
        }));
        assert_eq!(first.buckets.len(), 1);
        let token = first.continuation_token.unwrap();
        assert_eq!(decode_cursor(&token).unwrap(), Key::from(b"alpha".to_vec()));

        let mut next = ListBucketsOperation::new(ListBucketsInput {
            group_id,
            prefix: None,
            continuation_token: Some(token),
            max_buckets: Some(1),
        });
        let _ = next.first();
        let second = listed(next.next(IoResult::Page {
            rows: vec![entry("beta", group_id)],
            next: None,
        }));
        assert_eq!(
            second
                .buckets
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["beta"]
        );
        assert!(second.continuation_token.is_none());
    }

    #[test]
    fn resumes_scan_cap() {
        let group_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = ListBucketsOperation::new(ListBucketsInput {
            group_id,
            prefix: None,
            continuation_token: None,
            max_buckets: Some(1),
        });
        operation.scanned_rows = ListBucketsOperation::MAX_SCAN_ROWS - 1;
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Page { limit: 1, .. }))
        ));
        let done = operation.next(IoResult::Page {
            rows: vec![(
                Key::from(b"foreign".to_vec()),
                BucketInfo {
                    group_id: Ulid::from_bytes([6u8; 16]),
                    created_at: SystemTime::UNIX_EPOCH,
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                    distribution: None,
                }
                .to_bytes()
                .unwrap()
                .into(),
            )],
            next: Some(Key::from(b"foreign".to_vec())),
        });

        let result = listed(done);
        assert!(result.buckets.is_empty());
        let token = result.continuation_token.unwrap();
        assert_ne!(token, "foreign");
        assert_eq!(
            decode_cursor(&token).unwrap(),
            Key::from(b"foreign".to_vec())
        );
    }
}
