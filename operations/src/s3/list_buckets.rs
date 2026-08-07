use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::BucketInfo;
use aruna_core::types::{Effects, GroupId, Key};
use base64::Engine;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListBucketsState {
    Init,
    ReadBuckets,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListBucketsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListBucketsState,
        expected: &'static str,
        received: Event,
    },
    #[error("ListBuckets failed")]
    ListBucketsFailed,
}

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
    state: ListBucketsState,
    matches: Vec<(String, BucketInfo)>,
    next_storage_start_after: Option<Key>,
    scanned_rows: usize,
    output: Option<Result<ListBucketsResult, ListBucketsError>>,
}

impl ListBucketsOperation {
    const DEFAULT_MAX_BUCKETS: usize = 10_000;
    const MAX_BUCKETS: usize = 10_000;
    const SCAN_LIMIT: usize = 1_000;
    const MAX_SCAN_ROWS: usize = 10_000;

    pub fn new(input: ListBucketsInput) -> Self {
        Self {
            input,
            state: ListBucketsState::Init,
            matches: Vec::new(),
            next_storage_start_after: None,
            scanned_rows: 0,
            output: None,
        }
    }

    fn max_buckets(&self) -> usize {
        self.input
            .max_buckets
            .filter(|limit| *limit > 0)
            .unwrap_or(Self::DEFAULT_MAX_BUCKETS)
            .min(Self::MAX_BUCKETS)
    }

    fn emit_error(&mut self, error: ListBucketsError) -> Effects {
        self.state = ListBucketsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn emit_scan(&mut self) -> Effects {
        self.state = ListBucketsState::ReadBuckets;
        let start = if let Some(key) = &self.next_storage_start_after {
            Some(IterStart::After(key.clone()))
        } else if let Some(token) = self.input.continuation_token.as_deref() {
            match decode_cursor(token) {
                Ok(key) => Some(IterStart::After(key)),
                Err(error) => return self.emit_error(error.into()),
            }
        } else {
            None
        };
        let remaining = Self::MAX_SCAN_ROWS.saturating_sub(self.scanned_rows);
        if remaining == 0 {
            return self.emit_error(ListBucketsError::ListBucketsFailed);
        }
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            prefix: self.input.prefix.clone().map(Into::into),
            start,
            limit: Self::SCAN_LIMIT.min(remaining),
            txn_id: None,
        })]
    }

    fn handle_init(&mut self) -> Effects {
        self.emit_scan()
    }

    fn handle_bucket_list(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.emit_error(ListBucketsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let remaining = Self::MAX_SCAN_ROWS.saturating_sub(self.scanned_rows);
        if values.len() > remaining {
            return self.emit_error(ListBucketsError::ListBucketsFailed);
        }
        self.scanned_rows += values.len();
        let max_buckets = self.max_buckets();

        for (key, value) in values {
            let bucket_info = match BucketInfo::from_bytes(value.as_ref()) {
                Ok(bucket_info) => bucket_info,
                Err(err) => return self.emit_error(err.into()),
            };

            if bucket_info.group_id != self.input.group_id {
                continue;
            }

            let bucket_name = match String::from_utf8(key.to_vec()) {
                Ok(bucket_name) => bucket_name,
                Err(err) => return self.emit_error(ListBucketsError::ConversionError(err.into())),
            };
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
            self.next_storage_start_after = Some(next);
            if self.scanned_rows == Self::MAX_SCAN_ROWS {
                return self.finish_cursor(self.next_storage_start_after.as_ref());
            }
            return self.emit_scan();
        }

        self.finish(None)
    }

    fn finish(&mut self, continuation_token: Option<String>) -> Effects {
        self.state = ListBucketsState::Finish;
        self.output = Some(Ok(ListBucketsResult {
            buckets: std::mem::take(&mut self.matches),
            continuation_token,
        }));
        smallvec![]
    }

    fn finish_cursor(&mut self, key: Option<&Key>) -> Effects {
        match key.map(encode_cursor).transpose() {
            Ok(token) => self.finish(token),
            Err(error) => self.emit_error(error.into()),
        }
    }
}

fn encode_cursor(key: &Key) -> Result<String, ConversionError> {
    let bytes = postcard::to_allocvec(&key.to_vec())?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(token: &str) -> Result<Key, ConversionError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token)?;
    let key = postcard::from_bytes::<Vec<u8>>(&bytes)?;
    Ok(key.into())
}

impl Operation for ListBucketsOperation {
    type Output = Option<Result<ListBucketsResult, ListBucketsError>>;
    type Error = ListBucketsError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListBucketsState::Init => self.handle_init(),
            ListBucketsState::ReadBuckets => self.handle_bucket_list(event),
            ListBucketsState::Finish => smallvec![],
            ListBucketsState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListBucketsState::Finish | ListBucketsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListBucketsState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListBucketsError::ListBucketsFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
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
            task_handle: None,
            compute_handle: None,
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
                    replication: None,
                    storage_routing: Vec::new(),
                },
            ),
            (
                "beta".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    replication: None,
                    storage_routing: Vec::new(),
                },
            ),
            (
                "foreign".to_string(),
                BucketInfo {
                    group_id: Ulid::generate(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    replication: None,
                    storage_routing: Vec::new(),
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
        .unwrap()
        .unwrap()
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
            replication: None,
            storage_routing: Vec::new(),
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
        op.start();

        let cursor: super::Key = "beta".as_bytes().to_vec().into();
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry("alpha", group_id), entry("beta", other)],
            next_start_after: Some(cursor.clone()),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                start: Some(IterStart::After(key)),
                ..
            }) => assert_eq!(key, &cursor),
            other => panic!("unexpected effect: {other:?}"),
        }
        assert!(!op.is_complete());

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry("gamma", group_id)],
            next_start_after: None,
        }));
        assert!(effects.is_empty());

        let result = op.finalize().unwrap().unwrap().unwrap();
        let names: Vec<_> = result.buckets.into_iter().map(|(name, _)| name).collect();
        assert_eq!(names, vec!["alpha".to_string(), "gamma".to_string()]);
        assert_eq!(result.continuation_token, None);
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
            replication: None,
            storage_routing: Vec::new(),
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
        operation.start();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry("alpha", group_id), entry("beta", group_id)],
            next_start_after: Some(Key::from(b"beta".to_vec())),
        }));

        let first = operation.finalize().unwrap().unwrap().unwrap();
        assert_eq!(first.buckets.len(), 1);
        let token = first.continuation_token.unwrap();
        assert_eq!(decode_cursor(&token).unwrap(), Key::from(b"alpha".to_vec()));

        let mut next = ListBucketsOperation::new(ListBucketsInput {
            group_id,
            prefix: None,
            continuation_token: Some(token),
            max_buckets: Some(1),
        });
        next.start();
        next.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry("beta", group_id)],
            next_start_after: None,
        }));
        let second = next.finalize().unwrap().unwrap().unwrap();
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
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { limit: 1, .. })]
        ));
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                Key::from(b"foreign".to_vec()),
                BucketInfo {
                    group_id: Ulid::from_bytes([6u8; 16]),
                    created_at: SystemTime::UNIX_EPOCH,
                    created_by: Default::default(),
                    cors_configuration: None,
                    replication: None,
                    storage_routing: Vec::new(),
                }
                .to_bytes()
                .unwrap()
                .into(),
            )],
            next_start_after: Some(Key::from(b"foreign".to_vec())),
        }));

        let result = operation.finalize().unwrap().unwrap().unwrap();
        assert!(result.buckets.is_empty());
        let token = result.continuation_token.unwrap();
        assert_ne!(token, "foreign");
        assert_eq!(
            decode_cursor(&token).unwrap(),
            Key::from(b"foreign".to_vec())
        );
    }
}
