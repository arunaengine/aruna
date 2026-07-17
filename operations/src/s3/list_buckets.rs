use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::BucketInfo;
use aruna_core::types::{Effects, GroupId, Key};
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
    output: Option<Result<ListBucketsResult, ListBucketsError>>,
}

impl ListBucketsOperation {
    const DEFAULT_MAX_BUCKETS: usize = 10_000;
    const SCAN_LIMIT: usize = 10_000;

    pub fn new(input: ListBucketsInput) -> Self {
        Self {
            input,
            state: ListBucketsState::Init,
            matches: Vec::new(),
            next_storage_start_after: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListBucketsError) -> Effects {
        self.state = ListBucketsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn emit_scan(&mut self) -> Effects {
        self.state = ListBucketsState::ReadBuckets;
        let start = match &self.next_storage_start_after {
            Some(key) => Some(IterStart::After(key.clone())),
            None => self
                .input
                .continuation_token
                .clone()
                .map(Into::into)
                .map(IterStart::After),
        };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            prefix: self.input.prefix.clone().map(Into::into),
            start,
            limit: Self::SCAN_LIMIT,
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

        let max_buckets = self
            .input
            .max_buckets
            .filter(|limit| *limit > 0)
            .unwrap_or(Self::DEFAULT_MAX_BUCKETS);

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
            let continuation_token = self
                .matches
                .get(max_buckets - 1)
                .map(|(bucket, _)| bucket.clone());
            self.matches.truncate(max_buckets);
            return self.finish(continuation_token);
        }

        // The group page is not yet full: follow the storage cursor into the next
        // raw page so group buckets past the first page stay reachable.
        if let Some(next) = next_start_after {
            self.next_storage_start_after = Some(next);
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

        let group_id = Ulid::r#gen();
        for (bucket, bucket_info) in [
            (
                "alpha".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                },
            ),
            (
                "beta".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                },
            ),
            (
                "foreign".to_string(),
                BucketInfo {
                    group_id: Ulid::r#gen(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
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
}
