use std::collections::VecDeque;

use aruna_core::document::{DocumentSyncTarget, IrokleEvent};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::metadata::MetadataError;
use aruna_core::operation::Operation;
use aruna_core::structs::RealmId;
use aruna_core::types::{Effects, Key, UserId};
use aruna_core::{IrokleEffect, NodeId, TopicId, USER_KEYSPACE};
use smallvec::smallvec;
use thiserror::Error;

use crate::document_repository;

const USER_SYNC_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
enum PendingDocumentSync {
    Document(DocumentSyncTarget),
    UserPage {
        realm_id: RealmId,
        start_after: Option<Key>,
    },
}

#[derive(Debug, PartialEq)]
pub struct AnnounceTopicOperation {
    topic: TopicId,
    document: Option<DocumentSyncTarget>,
    peers: Vec<NodeId>,
    state: AnnounceTopicState,
    pending: VecDeque<PendingDocumentSync>,
    current: Option<DocumentSyncTarget>,
    output: Option<Result<(), AnnounceTopicError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum AnnounceTopicState {
    Init,
    ReadDocument,
    ListUsers,
    Publish,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AnnounceTopicError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl AnnounceTopicOperation {
    pub fn new(topic: TopicId, _local_node_id: NodeId) -> Self {
        Self::new_for_document(topic, _local_node_id, None)
    }

    pub fn new_for_document(
        topic: TopicId,
        local_node_id: NodeId,
        document: Option<DocumentSyncTarget>,
    ) -> Self {
        Self::new_for_document_with_peers(topic, local_node_id, document, Vec::new())
    }

    pub fn new_for_document_with_peers(
        topic: TopicId,
        _local_node_id: NodeId,
        document: Option<DocumentSyncTarget>,
        peers: Vec<NodeId>,
    ) -> Self {
        Self {
            topic,
            document,
            peers,
            state: AnnounceTopicState::Init,
            pending: VecDeque::new(),
            current: None,
            output: None,
        }
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.state = AnnounceTopicState::Error;
        self.output = Some(Err(AnnounceTopicError::UnexpectedEvent {
            state,
            expected,
            got,
        }));
        smallvec![]
    }

    fn fail(&mut self, error: AnnounceTopicError) -> Effects {
        self.state = AnnounceTopicState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn finish(&mut self) -> Effects {
        self.state = AnnounceTopicState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn queue_topic_documents(&mut self) {
        if !self.pending.is_empty() {
            return;
        }

        if let Some(document) = self.document.clone() {
            self.pending
                .push_back(PendingDocumentSync::Document(document));
            return;
        }

        match &self.topic {
            TopicId::Realm(realm_id) => {
                self.pending.push_back(PendingDocumentSync::Document(
                    DocumentSyncTarget::RealmAuthorization {
                        realm_id: *realm_id,
                    },
                ));
                self.pending.push_back(PendingDocumentSync::Document(
                    DocumentSyncTarget::RealmConfig {
                        realm_id: *realm_id,
                    },
                ));
            }
            TopicId::Group(group_id) => {
                self.pending
                    .push_back(PendingDocumentSync::Document(DocumentSyncTarget::Group {
                        group_id: *group_id,
                    }));
                self.pending.push_back(PendingDocumentSync::Document(
                    DocumentSyncTarget::GroupAuthorization {
                        group_id: *group_id,
                    },
                ));
            }
            TopicId::Users(realm_id) => self.pending.push_back(PendingDocumentSync::UserPage {
                realm_id: *realm_id,
                start_after: None,
            }),
            TopicId::Metadata(_) | TopicId::Node(_) => {}
        }
    }

    fn next_effect(&mut self) -> Effects {
        match self.pending.pop_front() {
            Some(PendingDocumentSync::Document(document)) => {
                self.current = Some(document.clone());
                self.state = AnnounceTopicState::ReadDocument;
                smallvec![document_repository::read_effect(&document, None)]
            }
            Some(PendingDocumentSync::UserPage {
                realm_id: _,
                start_after,
            }) => {
                self.state = AnnounceTopicState::ListUsers;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: USER_KEYSPACE.to_string(),
                    prefix: None,
                    start_after,
                    limit: USER_SYNC_PAGE_SIZE,
                    txn_id: None,
                })]
            }
            None => self.finish(),
        }
    }
}

impl Operation for AnnounceTopicOperation {
    type Output = ();
    type Error = AnnounceTopicError;

    fn start(&mut self) -> Effects {
        self.queue_topic_documents();
        self.next_effect()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AnnounceTopicState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(document) = self.current.clone() else {
                        return self.unexpected_event(
                            "tracked document sync target",
                            "missing current document".to_string(),
                        );
                    };
                    let Some(bytes) = value else {
                        return self.next_effect();
                    };
                    self.state = AnnounceTopicState::Publish;
                    smallvec![Effect::Net(NetEffect::Irokle(
                        IrokleEffect::PublishDocument {
                            target: document,
                            bytes: bytes.to_vec(),
                            peers: self.peers.clone(),
                        }
                    ))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            AnnounceTopicState::ListUsers => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    let TopicId::Users(realm_id) = self.topic else {
                        return self.unexpected_event(
                            "users topic",
                            format!("unexpected topic {:?}", self.topic),
                        );
                    };
                    for (key, _) in values {
                        let user_id = match UserId::from_storage_key(&key) {
                            Ok(user_id) => user_id,
                            Err(error) => return self.fail(error.into()),
                        };
                        if user_id.realm_id == realm_id {
                            self.pending.push_back(PendingDocumentSync::Document(
                                DocumentSyncTarget::User { user_id },
                            ));
                        }
                    }
                    if let Some(start_after) = next_start_after {
                        self.pending.push_back(PendingDocumentSync::UserPage {
                            realm_id,
                            start_after: Some(start_after),
                        });
                    }
                    self.next_effect()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iter result", format!("{other:?}")),
            },
            AnnounceTopicState::Publish => match event {
                Event::Net(NetEvent::Irokle(IrokleEvent::DocumentPublished { .. })) => {
                    self.current = None;
                    self.next_effect()
                }
                Event::Net(NetEvent::Irokle(IrokleEvent::Error { error, .. })) => {
                    self.fail(AnnounceTopicError::DocumentSync(error))
                }
                Event::Net(NetEvent::Error(error)) => {
                    self.fail(AnnounceTopicError::DocumentSync(format!("{error:?}")))
                }
                other => {
                    self.unexpected_event("irokle document publish result", format!("{other:?}"))
                }
            },
            AnnounceTopicState::Finish | AnnounceTopicState::Error | AnnounceTopicState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AnnounceTopicState::Finish | AnnounceTopicState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
