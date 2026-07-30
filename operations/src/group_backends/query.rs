use super::{RecordReadError, backend_key, parse_iter, parse_read};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::Event;
use aruna_core::keyspaces::GROUP_STORAGE_BACKEND_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::GroupStorageBackend;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const LIST_PAGE_SIZE: usize = 128;

#[derive(Debug, Error, PartialEq)]
pub enum GroupBackendQueryError {
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
pub struct GetGroupBackendOperation {
    backend_id: Ulid,
    state: QueryState,
    output: Option<Result<Option<GroupStorageBackend>, GroupBackendQueryError>>,
}

impl GetGroupBackendOperation {
    pub fn new(backend_id: Ulid) -> Self {
        Self {
            backend_id,
            state: QueryState::Init,
            output: None,
        }
    }
}

impl Operation for GetGroupBackendOperation {
    type Output = Option<GroupStorageBackend>;
    type Error = GroupBackendQueryError;

    fn start(&mut self) -> Effects {
        self.state = QueryState::Reading;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_key(self.backend_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueryState::Init => self.start(),
            QueryState::Reading => {
                match parse_read(event, GroupStorageBackend::from_bytes) {
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
            QueryState::Finish | QueryState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, QueryState::Finish | QueryState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(GroupBackendQueryError::Incomplete))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Lists a group's backends. Records are keyed by id, so the scan filters on
/// the record's own group; tenant backends are few by construction.
#[derive(Debug, PartialEq)]
pub struct ListGroupBackendsOperation {
    group_id: GroupId,
    state: QueryState,
    found: Vec<GroupStorageBackend>,
    output: Option<Result<Vec<GroupStorageBackend>, GroupBackendQueryError>>,
}

impl ListGroupBackendsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            state: QueryState::Init,
            found: Vec::new(),
            output: None,
        }
    }

    fn iter_effect(start_after: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: LIST_PAGE_SIZE,
            txn_id: None,
        })
    }
}

impl Operation for ListGroupBackendsOperation {
    type Output = Vec<GroupStorageBackend>;
    type Error = GroupBackendQueryError;

    fn start(&mut self) -> Effects {
        self.state = QueryState::Reading;
        smallvec![Self::iter_effect(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueryState::Init => self.start(),
            QueryState::Reading => match parse_iter(event, GroupStorageBackend::from_bytes) {
                Ok((records, next_start_after)) => {
                    self.found.extend(
                        records
                            .into_iter()
                            .filter(|record| record.group_id == self.group_id),
                    );
                    if let Some(start_after) = next_start_after {
                        return smallvec![Self::iter_effect(Some(start_after))];
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
            QueryState::Finish | QueryState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, QueryState::Finish | QueryState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(GroupBackendQueryError::Incomplete))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{GetGroupBackendOperation, ListGroupBackendsOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{GroupBackendKind, GroupStorageBackend};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn record(group_id: Ulid) -> GroupStorageBackend {
        GroupStorageBackend {
            backend_id: Ulid::from_bytes([9u8; 16]),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::B2,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            retiring: false,
        }
    }

    #[test]
    fn reads_absent_record() {
        let mut operation = GetGroupBackendOperation::new(Ulid::from_bytes([9u8; 16]));
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: None,
        }));

        assert_eq!(operation.finalize().unwrap(), None);
    }

    #[test]
    fn list_filters_group() {
        // Records are keyed by id, so another group's backend must be dropped.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mine = record(group_id);
        let other = record(Ulid::from_bytes([2u8; 16]));
        let mut operation = ListGroupBackendsOperation::new(group_id);
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { prefix: None, .. })]
        ));

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (b"a".to_vec().into(), mine.to_bytes().unwrap().into()),
                (b"b".to_vec().into(), other.to_bytes().unwrap().into()),
            ],
            next_start_after: None,
        }));

        assert_eq!(operation.finalize().unwrap(), vec![mine]);
    }
}
