use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{AuthorizationDocument, Group};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug)]
pub struct GetGroupConfig {
    pub group_id: GroupId,
}

#[derive(Debug)]
pub struct GetGroupOperation {
    config: GetGroupConfig,
    txn_id: Option<Ulid>,
    group: Option<Group>,
    auth_doc: Option<AuthorizationDocument>,
    output: Option<Result<(Group, AuthorizationDocument), GetGroupError>>,
    state: GetGroupState,
}

impl GetGroupOperation {
    pub fn new(config: GetGroupConfig) -> Self {
        GetGroupOperation {
            config,
            txn_id: None,
            output: None,
            group: None,
            auth_doc: None,
            state: GetGroupState::Init,
        }
    }
    fn emit_get_group(&mut self) -> aruna_core::types::Effects {
        self.config.group_id;
        let key = self.config.group_id.to_bytes().into();

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: "groups".to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn emit_parse_group(&mut self, value: Option<byteview::ByteView>) -> Result<(), GetGroupError> {
        let value = value.ok_or_else(|| GetGroupError::GroupNotFound)?;
        let group = Group::from_bytes(&value)?;
        self.group = Some(group);
        Ok(())
    }

    fn emit_get_auth_doc(&mut self) -> aruna_core::types::Effects {
        self.config.group_id;
        let key = self.config.group_id.to_bytes().into();

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: "auth".to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }
    fn emit_parse_auth_doc(
        &mut self,
        value: Option<byteview::ByteView>,
    ) -> Result<Effects, GetGroupError> {
        let value = value.ok_or_else(|| GetGroupError::AuthDocNotFound)?;
        let auth_doc = AuthorizationDocument::from_bytes(&value)?;
        self.auth_doc = Some(auth_doc);
        let txn_id = self
            .txn_id
            .ok_or_else(|| GetGroupError::NoTransactionFound)?;
        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    fn fail(&mut self, err: GetGroupError) -> Effects {
        self.state = GetGroupState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(&mut self, err: GetGroupError, cleanup_effects: Effects) -> Effects {
        self.state = GetGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: GetGroupState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            GetGroupError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                GetGroupState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        self.state = GetGroupState::GetGroup;
        self.txn_id = Some(txn_id);
        self.emit_get_group()
    }

    fn handle_get_group(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                GetGroupState::GetGroup,
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.emit_parse_group(value) {
            Ok(_) => {
                self.state = GetGroupState::GetAuthDoc;
                self.emit_get_auth_doc()
            }
            Err(err) => self.fail(err),
        }
    }

    fn handle_get_auth_doc(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                GetGroupState::GetAuthDoc,
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.emit_parse_auth_doc(value) {
            Ok(effects) => {
                self.state = GetGroupState::CommitTransaction;
                effects
            }
            Err(err) => self.fail(err),
        }
    }

    fn handle_commit_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                GetGroupState::CommitTransaction,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };

        if let Some(group) = &self.group
            && let Some(auth) = &self.auth_doc
        {
            self.state = GetGroupState::Finish;
            self.output = Some(Ok((group.clone(), auth.clone())));
            smallvec![]
        } else {
            self.fail(GetGroupError::GroupNotFound)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum GetGroupState {
    Init,
    StartTransaction,
    GetGroup,
    GetAuthDoc,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum GetGroupError {
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
        state: GetGroupState,
        expected: &'static str,
        got: String,
    },
}

impl Operation for GetGroupOperation {
    type Output = (Group, AuthorizationDocument);

    type Error = GetGroupError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = GetGroupState::StartTransaction;

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
            GetGroupState::StartTransaction => self.handle_start_transaction(event),
            GetGroupState::GetGroup => self.handle_get_group(event),
            GetGroupState::GetAuthDoc => self.handle_get_auth_doc(event),
            GetGroupState::CommitTransaction => self.handle_commit_transaction(event),
            GetGroupState::Init | GetGroupState::Finish | GetGroupState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetGroupState::Finish | GetGroupState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| GetGroupError::NotFinished)?
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
    use crate::get_group::{GetGroupConfig, GetGroupOperation};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_get_group() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
        };

        let group_config = CreateGroupConfig {
            user_id: Ulid::new(),
            realm_id: aruna_core::structs::RealmId([0u8; 32]),
            display_name: "Test group".to_string(),
        };
        let group_operation = CreateGroupOperation::new(group_config.clone());
        let create_result = drive(group_operation, &context).await.unwrap();

        let get_group_config = GetGroupConfig {
            group_id: create_result.0.group_id,
        };
        let group_operation = GetGroupOperation::new(get_group_config.clone());
        let get_result = drive(group_operation, &context).await.unwrap();

        assert_eq!(create_result.0, get_result.0);
    }
}
