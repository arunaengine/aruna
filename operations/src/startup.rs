use std::collections::HashSet;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
    METADATA_GRAPH_LIFECYCLE_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_KEYSPACE,
};
use aruna_core::metadata::MetadataGraphLifecycleRecord;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::RealmId;
use aruna_core::types::{Key, UserId};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::announce::AnnounceTopicOperation;
use crate::document_repository::{
    parse_auth_document, parse_group_document, parse_realm_config_document,
};
use crate::metadata::repository::parse_registry_iter;

const STARTUP_DOCUMENT_PAGE_SIZE: usize = 256;

#[derive(Debug, PartialEq)]
pub struct RestoreTopicSubscriptionsOperation {
    realm_id: RealmId,
    local_node_id: NodeId,
    state: RestoreTopicSubscriptionsState,
    scan_state: RestoreTopicSubscriptionsState,
    documents: Vec<DocumentSyncTarget>,
    discovered_documents: HashSet<DocumentSyncTarget>,
    next_start_after: Option<Key>,
    output: Option<Result<(), RestoreTopicSubscriptionsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum RestoreTopicSubscriptionsState {
    Init,
    ListAuth,
    ListGroups,
    ListRealmConfig,
    ListMetadata,
    ListMetadataLifecycle,
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
    pub fn new(local_node_id: NodeId, realm_id: RealmId) -> Self {
        Self {
            realm_id,
            local_node_id,
            state: RestoreTopicSubscriptionsState::Init,
            scan_state: RestoreTopicSubscriptionsState::ListAuth,
            documents: Vec::new(),
            discovered_documents: HashSet::new(),
            next_start_after: None,
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
            self.continue_scan_or_advance(self.scan_state.clone())
        }
    }

    fn emit_iter(&mut self, state: RestoreTopicSubscriptionsState) -> aruna_core::types::Effects {
        let key_space = match state {
            RestoreTopicSubscriptionsState::ListAuth => AUTH_KEYSPACE,
            RestoreTopicSubscriptionsState::ListGroups => GROUP_KEYSPACE,
            RestoreTopicSubscriptionsState::ListRealmConfig => REALM_CONFIG_KEYSPACE,
            RestoreTopicSubscriptionsState::ListMetadata => METADATA_DOCUMENT_INDEX_KEYSPACE,
            RestoreTopicSubscriptionsState::ListMetadataLifecycle => {
                METADATA_GRAPH_LIFECYCLE_KEYSPACE
            }
            RestoreTopicSubscriptionsState::ListUsers => USER_KEYSPACE,
            _ => {
                self.state = RestoreTopicSubscriptionsState::Finish;
                self.output = Some(Ok(()));
                return smallvec![];
            }
        };
        self.scan_state = state.clone();
        self.state = state;
        let prefix = if matches!(self.state, RestoreTopicSubscriptionsState::ListUsers) {
            Some(UserId::storage_prefix(self.realm_id))
        } else {
            None
        };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix,
            start: self.next_start_after.take().map(IterStart::After),
            limit: STARTUP_DOCUMENT_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn continue_scan_or_advance(
        &mut self,
        current: RestoreTopicSubscriptionsState,
    ) -> aruna_core::types::Effects {
        if self.next_start_after.is_some() {
            return self.emit_iter(current);
        }
        self.next_start_after = None;
        match current {
            RestoreTopicSubscriptionsState::ListAuth => {
                self.emit_iter(RestoreTopicSubscriptionsState::ListGroups)
            }
            RestoreTopicSubscriptionsState::ListGroups => {
                self.emit_iter(RestoreTopicSubscriptionsState::ListRealmConfig)
            }
            RestoreTopicSubscriptionsState::ListRealmConfig => {
                self.emit_iter(RestoreTopicSubscriptionsState::ListMetadata)
            }
            RestoreTopicSubscriptionsState::ListMetadata => {
                self.emit_iter(RestoreTopicSubscriptionsState::ListMetadataLifecycle)
            }
            RestoreTopicSubscriptionsState::ListMetadataLifecycle => {
                self.emit_iter(RestoreTopicSubscriptionsState::ListUsers)
            }
            RestoreTopicSubscriptionsState::ListUsers => {
                self.state = RestoreTopicSubscriptionsState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            _ => smallvec![],
        }
    }
}

impl Default for RestoreTopicSubscriptionsOperation {
    fn default() -> Self {
        Self::new(
            iroh::SecretKey::from_bytes(&[0u8; 32]).public(),
            RealmId::from_bytes([0u8; 32]),
        )
    }
}

impl Operation for RestoreTopicSubscriptionsOperation {
    type Output = ();
    type Error = RestoreTopicSubscriptionsError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.next_start_after = None;
        self.emit_iter(RestoreTopicSubscriptionsState::ListAuth)
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            RestoreTopicSubscriptionsState::ListAuth => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (key, _) in values {
                        match parse_auth_document(&key) {
                            Ok(document) => self.push_document(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.next_start_after = next_start_after;
                    self.next_announcement()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::ListGroups => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (key, _) in values {
                        match parse_group_document(&key) {
                            Ok(document) => self.push_document(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.next_start_after = next_start_after;
                    self.next_announcement()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::ListRealmConfig => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (key, _) in values {
                        match parse_realm_config_document(&key) {
                            Ok(document) => self.push_document(document),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.next_start_after = next_start_after;
                    self.next_announcement()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::ListMetadata => match parse_registry_iter(event) {
                Ok((records, next_start_after)) => {
                    for record in records {
                        self.push_document(DocumentSyncTarget::MetadataRegistry {
                            group_id: record.group_id,
                            document_id: record.document_id,
                        });
                    }
                    self.next_start_after = next_start_after;
                    self.next_announcement()
                }
                Err(crate::metadata::repository::StorageReadError::Storage(error)) => {
                    self.fail(error.into())
                }
                Err(crate::metadata::repository::StorageReadError::Conversion(error)) => {
                    self.fail(error.into())
                }
            },
            RestoreTopicSubscriptionsState::ListMetadataLifecycle => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (_, value) in values {
                        let record: MetadataGraphLifecycleRecord =
                            match postcard::from_bytes(&value) {
                                Ok(record) => record,
                                Err(error) => {
                                    return self.fail(ConversionError::from(error).into());
                                }
                            };
                        if record.realm_id == self.realm_id {
                            self.push_document(DocumentSyncTarget::MetadataGraphLifecycle {
                                graph_iri: record.graph_iri,
                            });
                        }
                    }
                    self.next_start_after = next_start_after;
                    self.next_announcement()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::ListUsers => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (key, _) in values {
                        match UserId::from_storage_key(&key) {
                            Ok(user_id) if user_id.realm_id == self.realm_id => {
                                self.push_document(DocumentSyncTarget::User { user_id })
                            }
                            Ok(_) => {}
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    self.next_start_after = next_start_after;
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
                            warn!(error = %error, "Failed to restore topic subscription; continuing best-effort");
                            self.next_announcement()
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
