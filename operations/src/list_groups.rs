use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::StorageEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::Group;
use aruna_core::types::{Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug)]
pub struct ListGroupOperation {
    txn_id: Option<Ulid>,
    output: Option<Result<Vec<Group>, ListGroupError>>,
    state: ListGroupState,
}

impl ListGroupOperation {
    pub fn new() -> Self {
        ListGroupOperation {
            txn_id: None,
            output: None,
            state: ListGroupState::Init,
        }
    }
    fn emit_list_groups(&mut self) -> aruna_core::types::Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: "groups".to_string(),
            prefix: None,
            start_after: None,
            limit: 10_000,
            txn_id: self.txn_id,
        })]
    }

    fn emit_parse_groups(
        &mut self,
        values: Vec<(Key, Value)>,
    ) -> Result<aruna_core::types::Effects, ListGroupError> {
        self.output = Some(
            values
                .iter()
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
}

#[derive(Debug)]
pub enum ListGroupState {
    Init,
    StartTransaction,
    ListGroups,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error)]
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

    fn step(&mut self, events: aruna_core::events::Event) -> aruna_core::types::Effects {
        match (events, &self.state) {
            (
                aruna_core::events::Event::Storage(StorageEvent::TransactionStarted { txn_id }),
                ListGroupState::StartTransaction,
            ) => {
                self.state = ListGroupState::ListGroups;
                self.txn_id = Some(txn_id);
                self.emit_list_groups()
            }
            (
                aruna_core::events::Event::Storage(StorageEvent::IterResult { values, .. }),
                ListGroupState::ListGroups,
            ) => match self.emit_parse_groups(values) {
                Ok(effects) => {
                    self.state = ListGroupState::CommitTransaction;
                    effects
                }
                Err(err) => {
                    self.state = ListGroupState::Error;
                    self.output = Some(Err(err));
                    smallvec![]
                }
            },
            (
                aruna_core::events::Event::Storage(StorageEvent::TransactionCommitted { .. }),
                ListGroupState::CommitTransaction,
            ) => {
                self.state = ListGroupState::Finish;
                smallvec![]
            }
            (aruna_core::events::Event::Storage(StorageEvent::Error { error }), _) => {
                self.state = ListGroupState::Error;
                self.output = Some(Err(error.into()));
                smallvec![]
            }
            _ => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListGroupState::Finish | ListGroupState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| ListGroupError::NotFinished)?
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
    use aruna_storage::storage;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_list_group() {
        let random_path = format!("/dev/shm/{}", Ulid::new().to_string());
        let storage_handle = storage::FjallStorage::open(&random_path).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
        };

        let mut groups = Vec::new();
        for i in 0..100 {
            let group_config = CreateGroupConfig {
                user_id: Ulid::new(),
                realm_id: aruna_core::structs::RealmId([0u8; 32]),
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
    }
}
