use std::collections::HashSet;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GOSSIP_SUBSCRIPTIONS_KEYSPACE, GROUP_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{boxed_suboperation, Operation};
use aruna_core::{NodeId, TopicId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::automerge::repository::{
    parse_auth_document, parse_group_document, parse_realm_config_document,
};
use crate::automerge_announce::AnnounceTopicOperation;

#[derive(Debug, PartialEq)]
pub struct RestoreTopicSubscriptionsOperation {
    local_node_id: NodeId,
    state: RestoreTopicSubscriptionsState,
    topics: Vec<TopicId>,
    discovered_topics: HashSet<TopicId>,
    subscriptions: HashSet<TopicId>,
    output: Option<Result<(), RestoreTopicSubscriptionsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum RestoreTopicSubscriptionsState {
    Init,
    ListAuth,
    ListGroups,
    ListRealmConfig,
    ListMetadata,
    ReadSubscriptions,
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
            topics: Vec::new(),
            discovered_topics: HashSet::new(),
            subscriptions: HashSet::new(),
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

    fn push_topic(&mut self, topic: TopicId) {
        if self.discovered_topics.insert(topic.clone()) {
            self.topics.push(topic);
        }
    }

    fn next_announcement(&mut self) -> aruna_core::types::Effects {
        if let Some(topic) = self.topics.pop() {
            self.state = RestoreTopicSubscriptionsState::WaitAnnouncement;
            smallvec![Effect::SubOperation(boxed_suboperation(
                AnnounceTopicOperation::new(topic, self.local_node_id),
                |result| {
                    Event::SubOperation(SubOperationEvent::TopicAnnouncementResult {
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
                            Ok(document) => self.push_topic(document.topic_id()),
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
                            Ok(document) => self.push_topic(document.topic_id()),
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
                            Ok(document) => self.push_topic(document.topic_id()),
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
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    for (key, _) in values {
                        if key.len() != 16 {
                            return self.fail(
                                ConversionError::InvalidLength(format!(
                                    "unexpected metadata document index key length {}",
                                    key.len()
                                ))
                                .into(),
                            );
                        }
                        let mut document_id = [0u8; 16];
                        document_id.copy_from_slice(&key);
                        self.push_topic(TopicId::metadata(Ulid::from_bytes(document_id)));
                    }
                    self.state = RestoreTopicSubscriptionsState::ReadSubscriptions;
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: GOSSIP_SUBSCRIPTIONS_KEYSPACE.to_string(),
                        key: ByteView::from(b"topics".as_slice()),
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::ReadSubscriptions => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    if let Some(value) = value {
                        match postcard::from_bytes::<Vec<TopicId>>(&value) {
                            Ok(topics) => {
                                self.subscriptions = topics.into_iter().collect();
                            }
                            Err(_) => {
                                self.subscriptions = self.discovered_topics.clone();
                            }
                        }
                    } else {
                        self.subscriptions = self.discovered_topics.clone();
                    }

                    self.topics
                        .retain(|topic| self.subscriptions.contains(topic));

                    self.next_announcement()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            RestoreTopicSubscriptionsState::WaitAnnouncement => match event {
                Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) => {
                    match result {
                        Ok(()) => self.next_announcement(),
                        Err(error) => {
                            self.fail(RestoreTopicSubscriptionsError::TopicAnnouncement(error))
                        }
                    }
                }
                other => {
                    self.unexpected_event("automerge announcement result", format!("{other:?}"))
                }
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
