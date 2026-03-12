use std::collections::HashSet;

use aruna_core::TopicId;
use aruna_core::consts::{
    AUTH_KEYSPACE, GOSSIP_SUBSCRIPTIONS_KEYSPACE, METADATA_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::{
    parse_auth_document, parse_metadata_key, parse_realm_config_document,
};
use crate::automerge_announce::AnnounceAutomergeDocumentOperation;

#[derive(Debug, PartialEq)]
pub struct RestoreAutomergeSubscriptionsOperation {
    state: RestoreAutomergeSubscriptionsState,
    documents: Vec<aruna_core::automerge::AutomergeDocumentVariant>,
    subscriptions: HashSet<TopicId>,
    output: Option<Result<(), RestoreAutomergeSubscriptionsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum RestoreAutomergeSubscriptionsState {
    Init,
    ListMetadata,
    ListAuth,
    ListRealmConfig,
    ReadSubscriptions,
    WaitAnnouncement,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RestoreAutomergeSubscriptionsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("automerge announcement failed: {0}")]
    AutomergeState(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl RestoreAutomergeSubscriptionsOperation {
    pub fn new() -> Self {
        Self {
            state: RestoreAutomergeSubscriptionsState::Init,
            documents: Vec::new(),
            subscriptions: HashSet::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: RestoreAutomergeSubscriptionsError) -> aruna_core::types::Effects {
        self.state = RestoreAutomergeSubscriptionsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(RestoreAutomergeSubscriptionsError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Default for RestoreAutomergeSubscriptionsOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for RestoreAutomergeSubscriptionsOperation {
    type Output = ();
    type Error = RestoreAutomergeSubscriptionsError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = RestoreAutomergeSubscriptionsState::ListMetadata;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: METADATA_KEYSPACE.to_string(),
            prefix: None,
            start_after: None,
            limit: usize::MAX,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            RestoreAutomergeSubscriptionsState::ListMetadata => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    for (key, _) in values {
                        match parse_metadata_key(&key) {
                            Ok(document) => self.documents.push(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.state = RestoreAutomergeSubscriptionsState::ListAuth;
                    smallvec![Effect::Storage(StorageEffect::Iter {
                        key_space: AUTH_KEYSPACE.to_string(),
                        prefix: None,
                        start_after: None,
                        limit: usize::MAX,
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreAutomergeSubscriptionsState::ListAuth => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    for (key, _value) in values {
                        match parse_auth_document(&key) {
                            Ok(document) => self.documents.push(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.state = RestoreAutomergeSubscriptionsState::ListRealmConfig;
                    smallvec![Effect::Storage(StorageEffect::Iter {
                        key_space: REALM_CONFIG_KEYSPACE.to_string(),
                        prefix: None,
                        start_after: None,
                        limit: usize::MAX,
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreAutomergeSubscriptionsState::ListRealmConfig => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    for (key, _) in values {
                        match parse_realm_config_document(&key) {
                            Ok(document) => self.documents.push(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.state = RestoreAutomergeSubscriptionsState::ReadSubscriptions;
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: GOSSIP_SUBSCRIPTIONS_KEYSPACE.to_string(),
                        key: ByteView::from(b"topics".as_slice()),
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreAutomergeSubscriptionsState::ReadSubscriptions => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    if let Some(value) = value {
                        let topics: Vec<TopicId> = match postcard::from_bytes(&value) {
                            Ok(topics) => topics,
                            Err(error) => return self.fail(ConversionError::from(error).into()),
                        };
                        self.subscriptions = topics.into_iter().collect();
                    }

                    self.documents
                        .retain(|document| self.subscriptions.contains(&document.topic_id()));

                    if let Some(document) = self.documents.pop() {
                        self.state = RestoreAutomergeSubscriptionsState::WaitAnnouncement;
                        smallvec![Effect::SubOperation(boxed_suboperation(
                            AnnounceAutomergeDocumentOperation::new(document),
                            |result| {
                                Event::SubOperation(SubOperationEvent::AutomergeStateResult {
                                    result: result.map_err(|error| error.to_string()),
                                })
                            },
                        ))]
                    } else {
                        self.state = RestoreAutomergeSubscriptionsState::Finish;
                        self.output = Some(Ok(()));
                        smallvec![]
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            RestoreAutomergeSubscriptionsState::WaitAnnouncement => match event {
                Event::SubOperation(SubOperationEvent::AutomergeStateResult { result }) => {
                    match result {
                        Ok(()) => {
                            if let Some(document) = self.documents.pop() {
                                self.state = RestoreAutomergeSubscriptionsState::WaitAnnouncement;
                                smallvec![Effect::SubOperation(boxed_suboperation(
                                    AnnounceAutomergeDocumentOperation::new(document),
                                    |result| {
                                        Event::SubOperation(
                                            SubOperationEvent::AutomergeStateResult {
                                                result: result.map_err(|error| error.to_string()),
                                            },
                                        )
                                    },
                                ))]
                            } else {
                                self.state = RestoreAutomergeSubscriptionsState::Finish;
                                self.output = Some(Ok(()));
                                smallvec![]
                            }
                        }
                        Err(error) => {
                            self.fail(RestoreAutomergeSubscriptionsError::AutomergeState(error))
                        }
                    }
                }
                other => {
                    self.unexpected_event("automerge announcement result", format!("{other:?}"))
                }
            },
            RestoreAutomergeSubscriptionsState::Finish
            | RestoreAutomergeSubscriptionsState::Error
            | RestoreAutomergeSubscriptionsState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RestoreAutomergeSubscriptionsState::Finish | RestoreAutomergeSubscriptionsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
