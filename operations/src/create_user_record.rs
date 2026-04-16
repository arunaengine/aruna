use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{boxed_suboperation, Operation};
use aruna_core::structs::{Actor, RealmConfigDocument, User};
use aruna_core::types::{Effects, TxnId, UserId};
use aruna_core::{AutomergeDocumentVariant, USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::VecDeque;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::automerge::repository::{read_effect, write_effect};

#[derive(Clone, Debug, PartialEq)]
pub struct CreateUserRecordInput {
    pub actor: Actor,
    pub user_id: UserId,
    pub name: String,
    pub subject_ids: Vec<String>,
}

#[derive(PartialEq)]
pub struct CreateUserRecordOperation {
    input: CreateUserRecordInput,
    pending_subject_ids: VecDeque<String>,
    state: CreateUserRecordState,
    output: Option<Result<User, CreateUserRecordError>>,
}

impl std::fmt::Debug for CreateUserRecordOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateUserRecordOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateUserRecordState {
    Init,
    StartTxn,
    CreateUser {
        txn_id: TxnId,
        user: User,
    },
    WriteSubjectIndex {
        txn_id: TxnId,
        user: User,
        subject_id: String,
    },
    ReadRealmConfig {
        txn_id: TxnId,
        user: User,
    },
    WriteRealmConfig {
        txn_id: TxnId,
        user: User,
    },
    CommitTxn {
        user: User,
    },
    AnnounceUser {
        user: User,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateUserRecordError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("AnnouncementError: `{0}`")]
    AnnouncementError(String),
    #[error("No realm config found")]
    NoRealmConfigFound,
    #[error("Create user record did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: CreateUserRecordState,
        expected: &'static str,
        got: String,
    },
}

impl CreateUserRecordOperation {
    pub fn new(input: CreateUserRecordInput) -> Self {
        CreateUserRecordOperation {
            input,
            pending_subject_ids: VecDeque::new(),
            state: CreateUserRecordState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: CreateUserRecordError) -> Effects {
        self.state = CreateUserRecordState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(
        &mut self,
        err: CreateUserRecordError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = CreateUserRecordState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: CreateUserRecordState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            CreateUserRecordError::UnexpectedEvent {
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

    fn handle_start_txn(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        match self.emit_create_user(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_create_user(&mut self, txn_id: TxnId) -> Result<Effects, CreateUserRecordError> {
        let user = User {
            user_id: self.input.user_id,
            name: self.input.name.clone(),
            subject_ids: self.input.subject_ids.clone(),
        };
        self.pending_subject_ids = self.input.subject_ids.iter().cloned().collect();

        let key = self.input.user_id.to_bytes().into();
        let value = user.to_bytes(&self.input.actor)?.into();

        self.state = CreateUserRecordState::CreateUser {
            txn_id,
            user: user.clone(),
        };

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_write_subject_index(&mut self, txn_id: TxnId, user: User) -> Option<Effects> {
        let subject_id = self.pending_subject_ids.pop_front()?;
        self.state = CreateUserRecordState::WriteSubjectIndex {
            txn_id,
            user: user.clone(),
            subject_id: subject_id.clone(),
        };
        Some(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            key: ByteView::from(subject_id.into_bytes()),
            value: ByteView::from(user.user_id.to_string().into_bytes()),
            txn_id: Some(txn_id),
        })])
    }

    fn realm_config_ref(&self) -> AutomergeDocumentVariant {
        AutomergeDocumentVariant::RealmConfig {
            realm_id: self.input.actor.realm_id.clone(),
        }
    }

    fn handle_create_user(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::WriteResult { .. })",
                got,
            );
        };

        if let Some(effects) = self.emit_write_subject_index(txn_id, user.clone()) {
            effects
        } else {
            self.state = CreateUserRecordState::ReadRealmConfig {
                txn_id,
                user: user.clone(),
            };
            smallvec![read_effect(&self.realm_config_ref(), Some(txn_id))]
        }
    }

    fn handle_write_subject_index(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::WriteResult { .. })",
                got,
            );
        };

        if let Some(effects) = self.emit_write_subject_index(txn_id, user.clone()) {
            effects
        } else {
            self.state = CreateUserRecordState::ReadRealmConfig {
                txn_id,
                user: user.clone(),
            };
            smallvec![read_effect(&self.realm_config_ref(), Some(txn_id))]
        }
    }

    fn handle_read_realm_config(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::ReadResult { .. })",
                got,
            );
        };

        match self.emit_write_realm_config(txn_id, user, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_write_realm_config(
        &mut self,
        txn_id: TxnId,
        user: User,
        value: Option<ByteView>,
    ) -> Result<Effects, CreateUserRecordError> {
        let mut document = RealmConfigDocument::from_bytes(
            &value.ok_or_else(|| CreateUserRecordError::NoRealmConfigFound)?,
        )?;
        if !document
            .users
            .iter()
            .any(|existing| existing.user_id == user.user_id)
        {
            document.users.push(user.clone());
        }

        let bytes = document.to_bytes(&self.input.actor)?;

        self.state = CreateUserRecordState::WriteRealmConfig {
            txn_id,
            user: user.clone(),
        };
        Ok(smallvec![write_effect(
            &self.realm_config_ref(),
            bytes,
            Some(txn_id)
        )])
    }

    fn handle_write_realm_config(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::WriteResult { .. })",
                got,
            );
        };

        self.state = CreateUserRecordState::CommitTxn { user: user.clone() };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_txn(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionCommitted { .. })",
                got,
            );
        };

        self.state = CreateUserRecordState::AnnounceUser { user: user.clone() };
        let suboperation = boxed_suboperation(
            AnnounceTopicOperation::new(
                AutomergeDocumentVariant::RealmConfig {
                    realm_id: self.input.actor.realm_id.clone(),
                }
                .topic_id(),
                self.input.actor.node_id,
            ),
            |result| {
                Event::SubOperation(SubOperationEvent::TopicAnnouncementResult {
                    result: result.map_err(|error| error.to_string()),
                })
            },
        );
        smallvec![Effect::SubOperation(suboperation)]
    }

    fn handle_announce_user(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) = event
        else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result })",
                got,
            );
        };

        self.state = CreateUserRecordState::Finish;
        self.output = Some(Ok(user));
        match result {
            Ok(_) => smallvec![],
            Err(err) => self.fail(CreateUserRecordError::AnnouncementError(err)),
        }
    }
}

impl Operation for CreateUserRecordOperation {
    type Output = User;
    type Error = CreateUserRecordError;

    fn start(&mut self) -> Effects {
        self.state = CreateUserRecordState::StartTxn;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            CreateUserRecordState::StartTxn => self.handle_start_txn(event),
            CreateUserRecordState::CreateUser { txn_id, user } => {
                self.handle_create_user(event, txn_id, user)
            }
            CreateUserRecordState::WriteSubjectIndex { txn_id, user, .. } => {
                self.handle_write_subject_index(event, txn_id, user)
            }
            CreateUserRecordState::ReadRealmConfig { txn_id, user } => {
                self.handle_read_realm_config(event, txn_id, user)
            }
            CreateUserRecordState::WriteRealmConfig { txn_id, user } => {
                self.handle_write_realm_config(event, txn_id, user)
            }
            CreateUserRecordState::CommitTxn { user } => self.handle_commit_txn(event, user),
            CreateUserRecordState::AnnounceUser { user } => self.handle_announce_user(event, user),
            CreateUserRecordState::Init
            | CreateUserRecordState::Finish
            | CreateUserRecordState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateUserRecordState::Finish | CreateUserRecordState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| CreateUserRecordError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            CreateUserRecordState::CreateUser { txn_id, .. }
            | CreateUserRecordState::WriteSubjectIndex { txn_id, .. }
            | CreateUserRecordState::ReadRealmConfig { txn_id, .. }
            | CreateUserRecordState::WriteRealmConfig { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}
