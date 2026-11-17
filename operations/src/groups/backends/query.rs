//! Reads one group storage backend record and lists a group's backends page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{RecordReadError, backend_key, index_prefix, parse_iter, parse_read};
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{BACKEND_INDEX_KEYSPACE, STORAGE_BACKEND_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const LIST_PAGE_SIZE: usize = 128;

#[derive(Debug, Error, PartialEq)]
pub enum GroupQueryError {
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("query never completed")]
    Incomplete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QueryState {
    Init,
    Reading,
    Finish,
    Error,
}

/// Reads one backend record. The caller checks that the record's group matches
/// the authorized one; the key itself carries no group.
#[derive(Debug, PartialEq)]
pub struct GetBackendOperation {
    backend_id: Ulid,
    state: QueryState,
    output: Option<Result<Option<GroupStorage>, GroupQueryError>>,
}

impl GetBackendOperation {
    pub fn new(backend_id: Ulid) -> Self {
        Self {
            backend_id,
            state: QueryState::Init,
            output: None,
        }
    }

    fn reject(&mut self) -> Effects {
        self.state = QueryState::Error;
        self.output = Some(Err(RecordReadError::Unexpected.into()));
        smallvec![]
    }
}

impl EffectOperation for GetBackendOperation {
    type Output = Option<GroupStorage>;
    type Error = GroupQueryError;

    fn start(&mut self) -> Effects {
        self.state = QueryState::Reading;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_key(self.backend_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueryState::Init => self.start(),
            QueryState::Reading => {
                match parse_read(event, GroupStorage::from_bytes) {
                    Ok(record) => {
                        self.state = QueryState::Finish;
                        self.output = Some(Ok(record));
                    }
                    Err(error) => {
                        self.state = QueryState::Error;
                        self.output = Some(Err(error.into()));
                    }
                }
                smallvec![]
            }
            QueryState::Finish | QueryState::Error => self.reject(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, QueryState::Finish | QueryState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GroupQueryError::Incomplete))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Lists a group's backends from the group-prefixed index, so one tenant's
/// listing never walks another's records.
#[derive(Debug, PartialEq)]
pub struct ListBackendsOperation {
    group_id: GroupId,
    state: QueryState,
    found: Vec<GroupStorage>,
    output: Option<Result<Vec<GroupStorage>, GroupQueryError>>,
}

impl ListBackendsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            state: QueryState::Init,
            found: Vec::new(),
            output: None,
        }
    }

    fn iter_effect(&self, start_after: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: BACKEND_INDEX_KEYSPACE.to_string(),
            prefix: Some(index_prefix(self.group_id)),
            start: start_after.map(IterStart::After),
            limit: LIST_PAGE_SIZE,
            txn_id: None,
        })
    }

    fn reject(&mut self) -> Effects {
        self.state = QueryState::Error;
        self.output = Some(Err(RecordReadError::Unexpected.into()));
        smallvec![]
    }
}

impl EffectOperation for ListBackendsOperation {
    type Output = Vec<GroupStorage>;
    type Error = GroupQueryError;

    fn start(&mut self) -> Effects {
        self.state = QueryState::Reading;
        smallvec![self.iter_effect(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueryState::Init => self.start(),
            QueryState::Reading => match parse_iter(event, GroupStorage::from_bytes) {
                Ok((records, next_start_after)) => {
                    self.found.extend(
                        records
                            .into_iter()
                            .filter(|record| record.group_id == self.group_id),
                    );
                    if let Some(start_after) = next_start_after {
                        return smallvec![self.iter_effect(Some(start_after))];
                    }
                    self.state = QueryState::Finish;
                    self.output = Some(Ok(std::mem::take(&mut self.found)));
                    smallvec![]
                }
                Err(error) => {
                    self.state = QueryState::Error;
                    self.output = Some(Err(error.into()));
                    smallvec![]
                }
            },
            QueryState::Finish | QueryState::Error => self.reject(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, QueryState::Finish | QueryState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GroupQueryError::Incomplete))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Whether `bucket` exists on the storage without a bucket `backend_id`, and for an existing
/// bucket whether Aruna may write and delete there. Nothing is created.
#[derive(Debug, PartialEq)]
pub struct BucketCheckOperation {
    backend_id: Ulid,
    bucket: String,
    started: bool,
    output: Option<Result<(bool, Option<bool>), BlobError>>,
}

impl BucketCheckOperation {
    pub fn new(backend_id: Ulid, bucket: &str) -> Self {
        Self {
            backend_id,
            bucket: bucket.to_string(),
            started: false,
            output: None,
        }
    }
}

impl EffectOperation for BucketCheckOperation {
    type Output = (bool, Option<bool>);
    type Error = BlobError;

    fn start(&mut self) -> Effects {
        self.started = true;
        smallvec![Effect::Blob(BlobEffect::HeadBucket {
            backend: BackendRef::Group(self.backend_id),
            bucket: self.bucket.clone(),
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let started = std::mem::replace(&mut self.started, false);
        self.output = Some(match event {
            Event::Blob(BlobEvent::BucketExists { exists, writable }) if started => {
                Ok((exists, writable))
            }
            // Kept from the direct handle call: a node without blob access says so this way.
            Event::Blob(BlobEvent::Error(BlobError::HandleMissing)) if started => Err(
                BlobError::UnknownBackend("blob handle unavailable".to_string()),
            ),
            Event::Blob(BlobEvent::Error(error)) if started => Err(error),
            other => Err(BlobError::UnknownBackend(format!(
                "unexpected bucket check answer: {}",
                other.label()
            ))),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(BlobError::UnknownBackend(
            "bucket check never completed".to_string(),
        )))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod pure_tests {
    use super::super::RecordReadError;
    use super::super::{index_key, index_prefix};
    use super::{
        BucketCheckOperation, GetBackendOperation, GroupQueryError, ListBackendsOperation,
    };
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::errors::BlobError;
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::BACKEND_INDEX_KEYSPACE;
    use aruna_core::operation::EffectOperation;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn record(group_id: Ulid) -> GroupStorage {
        GroupStorage {
            backend_id: Ulid::from_bytes([9u8; 16]),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::B2,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: false,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
    }

    #[test]
    fn reads_absent_record() {
        let mut operation = GetBackendOperation::new(Ulid::from_bytes([9u8; 16]));
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: None,
        }));

        assert_eq!(operation.finalize().unwrap(), None);
    }

    #[test]
    fn scopes_by_group() {
        // The index prefix is what keeps another tenant's records out.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mine = record(group_id);
        let mut operation = ListBackendsOperation::new(group_id);
        let effects = operation.start();

        let [
            Effect::Storage(StorageEffect::Iter {
                key_space,
                prefix: Some(prefix),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a prefixed scan, got {effects:?}")
        };
        assert_eq!(key_space, BACKEND_INDEX_KEYSPACE);
        assert_eq!(prefix, &index_prefix(group_id));

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                index_key(group_id, mine.backend_id),
                mine.to_bytes().unwrap().into(),
            )],
            next_start_after: None,
        }));

        assert_eq!(operation.finalize().unwrap(), vec![mine]);
    }

    // A state that expects no event must reject one instead of ignoring it.
    #[test]
    fn terminal_rejects_event() {
        let stray = || {
            Event::Storage(StorageEvent::ReadResult {
                key: b"x".to_vec().into(),
                value: None,
            })
        };
        let get = || {
            let mut get = GetBackendOperation::new(Ulid::from_bytes([9u8; 16]));
            get.start();
            get.step(stray());
            get
        };
        for (_, effects, error) in rejects_events(get, sample_events, |_| false) {
            assert!(effects.is_empty());
            assert!(
                matches!(error, GroupQueryError::Read(RecordReadError::Unexpected)),
                "{error:?}"
            );
        }

        let group_id = Ulid::from_bytes([1u8; 16]);
        let list = || {
            let mut list = ListBackendsOperation::new(group_id);
            list.start();
            list.step(Event::Storage(StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            }));
            list
        };
        for (_, effects, error) in rejects_events(list, sample_events, |_| false) {
            assert!(effects.is_empty());
            assert!(
                matches!(error, GroupQueryError::Read(RecordReadError::Unexpected)),
                "{error:?}"
            );
        }
    }

    fn bucket_check() -> BucketCheckOperation {
        BucketCheckOperation::new(Ulid::from_bytes([9u8; 16]), "data")
    }

    #[test]
    fn checks_one_bucket() {
        // One head request on the group storage; its answer is the result.
        let mut operation = bucket_check();
        let head = Effect::Blob(BlobEffect::HeadBucket {
            backend: BackendRef::Group(Ulid::from_bytes([9u8; 16])),
            bucket: "data".to_string(),
        });
        let answer = Event::Blob(BlobEvent::BucketExists {
            exists: true,
            writable: Some(false),
        });
        check_trace(&mut operation, &[head], vec![(answer, vec![])]);
        assert_eq!(operation.finalize(), Ok((true, Some(false))));
    }

    #[test]
    fn bucket_errors_returned() {
        // A storage error is returned as is; a node without blob access names that.
        let mut operation = bucket_check();
        operation.start();
        let down = || BlobError::ConnectionFailed("down".to_string());
        operation.step(Event::Blob(BlobEvent::Error(down())));
        assert_eq!(operation.finalize(), Err(down()));
        let mut operation = bucket_check();
        operation.start();
        operation.step(Event::Blob(BlobEvent::Error(BlobError::HandleMissing)));
        assert_eq!(
            operation.finalize(),
            Err(BlobError::UnknownBackend(
                "blob handle unavailable".to_string()
            ))
        );
    }

    #[test]
    fn bucket_rejects_events() {
        // Any other event, or an answer before start, fails the check without effects.
        for (label, effects, error) in rejects_events(bucket_check, sample_events, |_| false) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(matches!(error, BlobError::UnknownBackend(_)), "{label}");
        }
        let mut operation = bucket_check();
        operation.step(Event::Blob(BlobEvent::BucketExists {
            exists: true,
            writable: None,
        }));
        assert!(operation.finalize().is_err());
    }

    #[test]
    fn bucket_abort_finalize() {
        // The check only reads: abort has nothing to clean up, and an early finalize fails.
        let mut operation = bucket_check();
        operation.start();
        assert!(operation.abort().is_empty());
        rejects_finalize(operation);
    }
}
