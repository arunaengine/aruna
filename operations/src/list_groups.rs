use aruna_core::consts::GROUP_KEYSPACE;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::Group;
use aruna_core::types::{Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub struct ListGroupOperation {
    txn_id: Option<Ulid>,
    output: Option<Result<Vec<Group>, ListGroupError>>,
    state: ListGroupState,
    limit: usize,
    offset: usize,
}

impl Default for ListGroupOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl ListGroupOperation {
    const DEFAULT_LIMIT: usize = 10_000;

    pub fn new() -> Self {
        Self::with_pagination(Self::DEFAULT_LIMIT, 0)
    }

    pub fn with_pagination(limit: usize, offset: usize) -> Self {
        ListGroupOperation {
            txn_id: None,
            output: None,
            state: ListGroupState::Init,
            limit,
            offset,
        }
    }

    fn emit_list_groups(&mut self) -> aruna_core::types::Effects {
        let scan_limit = self.offset.saturating_add(self.limit).max(1);
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: GROUP_KEYSPACE.to_string(),
            prefix: None,
            start_after: None,
            limit: scan_limit,
            txn_id: self.txn_id,
        })]
    }

    fn emit_parse_groups(
        &mut self,
        values: Vec<(Key, Value)>,
    ) -> Result<aruna_core::types::Effects, ListGroupError> {
        self.output = Some(
            values
                .into_iter()
                .skip(self.offset)
                .take(self.limit)
                .map(|(_, value)| -> Result<Group, ListGroupError> {
                    Group::from_bytes(&value).map_err(|err| err.into())
                })
                .collect(),
        );
        match self.txn_id {
            Some(txn_id) => Ok(smallvec![Effect::Storage(
                StorageEffect::CommitTransaction { txn_id }
            )]),
            None => Err(ListGroupError::NoTransactionFound),
        }
    }

    fn fail(&mut self, err: ListGroupError) -> aruna_core::types::Effects {
        self.state = ListGroupState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(
        &mut self,
        err: ListGroupError,
        cleanup_effects: aruna_core::types::Effects,
    ) -> aruna_core::types::Effects {
        self.state = ListGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: ListGroupState,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            ListGroupError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, aruna_core::types::Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    fn handle_start_transaction(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                ListGroupState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        self.state = ListGroupState::ListGroups;
        self.txn_id = Some(txn_id);
        self.emit_list_groups()
    }

    fn handle_list_groups(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.unexpected_event(
                ListGroupState::ListGroups,
                "Event::Storage(StorageEvent::IterResult)",
                got,
            );
        };

        match self.emit_parse_groups(values) {
            Ok(effects) => {
                self.state = ListGroupState::CommitTransaction;
                effects
            }
            Err(err) => self.fail(err),
        }
    }

    fn handle_commit_transaction(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                ListGroupState::CommitTransaction,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };

        self.state = ListGroupState::Finish;
        smallvec![]
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ListGroupState {
    Init,
    StartTransaction,
    ListGroups,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("No group found")]
    GroupNotFound,
    #[error("No group found")]
    AuthDocNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: ListGroupState,
        expected: &'static str,
        got: String,
    },
}

impl Operation for ListGroupOperation {
    type Output = Vec<Group>;

    type Error = ListGroupError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = ListGroupState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            ListGroupState::StartTransaction => self.handle_start_transaction(event),
            ListGroupState::ListGroups => self.handle_list_groups(event),
            ListGroupState::CommitTransaction => self.handle_commit_transaction(event),
            ListGroupState::Init | ListGroupState::Finish | ListGroupState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListGroupState::Finish | ListGroupState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListGroupError::NotFinished)?
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::driver::{DriverContext, drive};
    use crate::list_groups::ListGroupOperation;
    use aruna_core::structs::Actor;
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_list_group() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                use_dns_discovery: false,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            automerge_handle: None,
            task_handle: Some(task_handle),
        };

        let mut groups = Vec::new();
        for i in 0..100 {
            let group_config = CreateGroupConfig {
                actor: Actor {
                    user_id: Ulid::new(),
                    realm_id: aruna_core::structs::RealmId([0u8; 32]),
                    node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                },
                display_name: format!("Test group {i}"),
            };
            let group_operation = CreateGroupOperation::new(group_config.clone());
            let create_result = drive(group_operation, &context).await.unwrap();
            groups.push(create_result.0);
        }

        let list_operation = ListGroupOperation::new();
        let mut list_result = drive(list_operation, &context).await.unwrap();
        list_result.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));

        assert_eq!(list_result, groups);

        net_handle.shutdown().await;
    }
}
