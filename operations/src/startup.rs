use std::collections::HashSet;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE,
    USER_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::types::UserId;
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::document_repository::{
    parse_auth_document, parse_group_document, parse_realm_config_document,
};
use crate::metadata::repository::parse_registry_iter;

#[derive(Debug, PartialEq)]
pub struct RestoreTopicSubscriptionsOperation {
    local_node_id: NodeId,
    state: RestoreTopicSubscriptionsState,
    documents: Vec<DocumentSyncTarget>,
    discovered_documents: HashSet<DocumentSyncTarget>,
    output: Option<Result<(), RestoreTopicSubscriptionsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum RestoreTopicSubscriptionsState {
    Init,
    ListAuth,
    ListGroups,
    ListRealmConfig,
    ListMetadata,
    ListUsers,
    WaitAnnouncement,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RestoreTopicSubscriptionsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl RestoreTopicSubscriptionsOperation {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            state: RestoreTopicSubscriptionsState::Init,
            documents: Vec::new(),
            discovered_documents: HashSet::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: RestoreTopicSubscriptionsError) -> aruna_core::types::Effects {
        self.state = RestoreTopicSubscriptionsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(RestoreTopicSubscriptionsError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn push_document(&mut self, document: DocumentSyncTarget) {
        if self.discovered_documents.insert(document.clone()) {
            self.documents.push(document);
        }
    }

    fn next_announcement(&mut self) -> aruna_core::types::Effects {
        if let Some(document) = self.documents.pop() {
            self.state = RestoreTopicSubscriptionsState::WaitAnnouncement;
            smallvec![Effect::SubOperation(boxed_suboperation(
                AnnounceTopicOperation::new_for_document(
                    document.topic_id(),
                    self.local_node_id,
                    Some(document),
                ),
                |result| {
                    Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                        result: result.map_err(|error| error.to_string()),
                    })
                },
            ))]
        } else {
            self.state = RestoreTopicSubscriptionsState::Finish;
            self.output = Some(Ok(()));
            smallvec![]
        }
    }
}

impl Default for RestoreTopicSubscriptionsOperation {
    fn default() -> Self {
        Self::new(iroh::SecretKey::from_bytes(&[0u8; 32]).public())
    }
}

impl Operation for RestoreTopicSubscriptionsOperation {
    type Output = ();
    type Error = RestoreTopicSubscriptionsError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = RestoreTopicSubscriptionsState::ListAuth;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: AUTH_KEYSPACE.to_string(),
            prefix: None,
            start_after: None,
            limit: usize::MAX,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            RestoreTopicSubscriptionsState::ListAuth => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    for (key, _) in values {
                        match parse_auth_document(&key) {
                            Ok(document) => self.push_document(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.state = RestoreTopicSubscriptionsState::ListGroups;
                    smallvec![Effect::Storage(StorageEffect::Iter {
                        key_space: GROUP_KEYSPACE.to_string(),
                        prefix: None,
                        start_after: None,
                        limit: usize::MAX,
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::ListGroups => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    for (key, _) in values {
                        match parse_group_document(&key) {
                            Ok(document) => self.push_document(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.state = RestoreTopicSubscriptionsState::ListRealmConfig;
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
            RestoreTopicSubscriptionsState::ListRealmConfig => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    for (key, _) in values {
                        match parse_realm_config_document(&key) {
                            Ok(document) => self.push_document(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.state = RestoreTopicSubscriptionsState::ListMetadata;
                    smallvec![Effect::Storage(StorageEffect::Iter {
                        key_space: METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
                        prefix: None,
                        start_after: None,
                        limit: usize::MAX,
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::ListMetadata => match event {
                event => match parse_registry_iter(event) {
                    Ok((records, _)) => {
                        for record in records {
                            self.push_document(DocumentSyncTarget::MetadataRegistry {
                                group_id: record.group_id,
                                document_id: record.document_id,
                            });
                        }
                        self.state = RestoreTopicSubscriptionsState::ListUsers;
                        smallvec![Effect::Storage(StorageEffect::Iter {
                            key_space: USER_KEYSPACE.to_string(),
                            prefix: None,
                            start_after: None,
                            limit: usize::MAX,
                            txn_id: None,
                        })]
                    }
                    Err(crate::metadata::repository::StorageReadError::Storage(error)) => {
                        self.fail(error.into())
                    }
                    Err(crate::metadata::repository::StorageReadError::Conversion(error)) => {
                        self.fail(error.into())
                    }
                },
            },
            RestoreTopicSubscriptionsState::ListUsers => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    for (key, _) in values {
                        match UserId::from_storage_key(&key) {
                            Ok(user_id) => self.push_document(DocumentSyncTarget::User { user_id }),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.next_announcement()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::WaitAnnouncement => match event {
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) => {
                    match result {
                        Ok(()) => self.next_announcement(),
                        Err(error) => {
                            self.fail(RestoreTopicSubscriptionsError::TopicAnnouncement(error))
                        }
                    }
                }
                other => self.unexpected_event("document sync result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::Finish
            | RestoreTopicSubscriptionsState::Error
            | RestoreTopicSubscriptionsState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RestoreTopicSubscriptionsState::Finish | RestoreTopicSubscriptionsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
