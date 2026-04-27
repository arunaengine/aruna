use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, GossipEffect, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, GossipError};
use aruna_core::events::{Event, GossipEvent, NetEvent, StorageEvent};
use aruna_core::gossip::{TopicMessage, TopicMessageKind, TopicMessageVersion};
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, UserId};
use aruna_core::{NodeId, TopicId, USER_KEYSPACE};
use smallvec::smallvec;
use thiserror::Error;
use tracing::trace;
use ulid::Ulid;

use crate::automerge::repository::{automerge_clock, read_effect};
use crate::metadata::repository::read_registry_by_document_effect;
use crate::telemetry::current_trace_id;

pub const TOPIC_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(30);
pub const TOPIC_ANNOUNCE_SHORT_INTERVAL: Duration = Duration::from_secs(5);
const USER_ANNOUNCE_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
enum PendingTopicAnnouncement {
    Automerge(AutomergeDocumentVariant),
    Metadata {
        document_id: Ulid,
    },
    UserPage {
        realm_id: RealmId,
        start_after: Option<Key>,
    },
}

#[derive(Debug, PartialEq)]
pub struct AnnounceTopicOperation {
    topic: TopicId,
    local_node_id: NodeId,
    document: Option<AutomergeDocumentVariant>,
    state: AnnounceTopicState,
    pending: VecDeque<PendingTopicAnnouncement>,
    current: Option<PendingTopicAnnouncement>,
    current_message_id: Option<Ulid>,
    output: Option<Result<(), AnnounceTopicError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum AnnounceTopicState {
    Init,
    ResetTimer,
    Subscribe,
    ReadAutomergeDocument,
    ReadMetadataRecord,
    ReadMetadataClock,
    ListUsers,
    Broadcast,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AnnounceTopicError {
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    GossipError(#[from] GossipError),
    #[error(transparent)]
    MetadataError(#[from] aruna_core::metadata::MetadataError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl AnnounceTopicOperation {
    pub fn new(topic: TopicId, local_node_id: NodeId) -> Self {
        Self::new_for_document(topic, local_node_id, None)
    }

    pub fn new_for_document(
        topic: TopicId,
        local_node_id: NodeId,
        document: Option<AutomergeDocumentVariant>,
    ) -> Self {
        Self {
            topic,
            local_node_id,
            document,
            state: AnnounceTopicState::Init,
            pending: VecDeque::new(),
            current: None,
            current_message_id: None,
            output: None,
        }
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.state = AnnounceTopicState::Error;
        self.output = Some(Err(AnnounceTopicError::UnexpectedEvent {
            state,
            expected,
            got,
        }));
        smallvec![]
    }

    fn fail(&mut self, error: AnnounceTopicError) -> aruna_core::types::Effects {
        self.state = AnnounceTopicState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn queue_topic_documents(&mut self) {
        if !self.pending.is_empty() {
            return;
        }

        if let Some(document) = self.document.clone() {
            self.pending
                .push_back(PendingTopicAnnouncement::Automerge(document));
            return;
        }

        match &self.topic {
            TopicId::Realm(realm_id) => {
                self.pending.push_back(PendingTopicAnnouncement::Automerge(
                    AutomergeDocumentVariant::RealmAuthorization {
                        realm_id: *realm_id,
                    },
                ));
                self.pending.push_back(PendingTopicAnnouncement::Automerge(
                    AutomergeDocumentVariant::RealmConfig {
                        realm_id: *realm_id,
                    },
                ));
            }
            TopicId::Group(group_id) => {
                self.pending.push_back(PendingTopicAnnouncement::Automerge(
                    AutomergeDocumentVariant::Group {
                        group_id: *group_id,
                    },
                ));
                self.pending.push_back(PendingTopicAnnouncement::Automerge(
                    AutomergeDocumentVariant::GroupAuthorization {
                        group_id: *group_id,
                    },
                ));
            }
            TopicId::Metadata(document_id) => {
                self.pending.push_back(PendingTopicAnnouncement::Metadata {
                    document_id: *document_id,
                });
            }
            TopicId::Users(realm_id) => {
                self.pending.push_back(PendingTopicAnnouncement::UserPage {
                    realm_id: *realm_id,
                    start_after: None,
                });
            }
            TopicId::Node(_) => {}
        }
    }

    fn next_effect(&mut self) -> aruna_core::types::Effects {
        self.current = self.pending.pop_front();
        match self.current.clone() {
            Some(PendingTopicAnnouncement::Automerge(document)) => {
                self.state = AnnounceTopicState::ReadAutomergeDocument;
                smallvec![read_effect(&document, None)]
            }
            Some(PendingTopicAnnouncement::Metadata { document_id }) => {
                self.state = AnnounceTopicState::ReadMetadataRecord;
                smallvec![read_registry_by_document_effect(document_id, None)]
            }
            Some(PendingTopicAnnouncement::UserPage { start_after, .. }) => {
                self.state = AnnounceTopicState::ListUsers;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: USER_KEYSPACE.to_string(),
                    prefix: None,
                    start_after,
                    limit: USER_ANNOUNCE_PAGE_SIZE,
                    txn_id: None,
                })]
            }
            None => {
                self.state = AnnounceTopicState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
        }
    }

    fn broadcast_message(
        &mut self,
        kind: TopicMessageKind,
        version: TopicMessageVersion,
    ) -> aruna_core::types::Effects {
        let message_id = Ulid::new();
        let message = TopicMessage::new(
            kind,
            message_id,
            self.local_node_id,
            current_trace_id(),
            version,
        );
        let bytes = match postcard::to_allocvec(&message) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(ConversionError::from(error).into()),
        };
        trace!(
            event = "gossip.broadcast",
            topic = %self.topic,
            message_id = %message_id,
            "Broadcasting topic gossip message"
        );
        self.current_message_id = Some(message_id);
        self.state = AnnounceTopicState::Broadcast;
        smallvec![Effect::Net(NetEffect::Gossip(GossipEffect::Broadcast {
            topic: self.topic.clone(),
            message: bytes,
        }))]
    }
}

impl Operation for AnnounceTopicOperation {
    type Output = ();
    type Error = AnnounceTopicError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.queue_topic_documents();
        self.state = AnnounceTopicState::ResetTimer;
        smallvec![Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::TopicAnnounce(self.topic.clone()),
            after: TOPIC_ANNOUNCE_INTERVAL,
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            AnnounceTopicState::ResetTimer => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = AnnounceTopicState::Subscribe;
                    smallvec![Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
                        topic: self.topic.clone(),
                    }))]
                }
                Event::Task(TaskEvent::Error { .. }) => {
                    self.state = AnnounceTopicState::Subscribe;
                    smallvec![Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
                        topic: self.topic.clone(),
                    }))]
                }
                other => self.unexpected_event("task timer acknowledgement", format!("{other:?}")),
            },
            AnnounceTopicState::Subscribe => match event {
                Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. }))
                | Event::Net(NetEvent::Gossip(GossipEvent::Error {
                    error: GossipError::AlreadySubscribed,
                })) => self.next_effect(),
                Event::Net(NetEvent::Gossip(GossipEvent::Error { .. }))
                | Event::Net(NetEvent::Error(_)) => {
                    self.state = AnnounceTopicState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                other => {
                    self.unexpected_event("gossip subscribe acknowledgement", format!("{other:?}"))
                }
            },
            AnnounceTopicState::ReadAutomergeDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(PendingTopicAnnouncement::Automerge(document)) = self.current.as_ref()
                    else {
                        return self.unexpected_event(
                            "tracked topic document",
                            "missing current document".to_string(),
                        );
                    };
                    let Some(value) = value else {
                        return self.next_effect();
                    };
                    let clock = match automerge_clock(&value) {
                        Ok(clock) => clock,
                        Err(error) => return self.fail(error.into()),
                    };
                    self.broadcast_message(
                        document.message_kind(),
                        TopicMessageVersion::Automerge {
                            heads: clock.heads,
                            change_count: clock.change_count,
                        },
                    )
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            AnnounceTopicState::ReadMetadataRecord => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(PendingTopicAnnouncement::Metadata { .. }) = self.current.as_ref()
                    else {
                        return self.unexpected_event(
                            "tracked metadata document",
                            "missing metadata document".to_string(),
                        );
                    };
                    let Some(value) = value else {
                        return self.next_effect();
                    };
                    let record: aruna_core::structs::MetadataRegistryRecord =
                        match postcard::from_bytes(&value) {
                            Ok(record) => record,
                            Err(error) => return self.fail(ConversionError::from(error).into()),
                        };
                    self.state = AnnounceTopicState::ReadMetadataClock;
                    smallvec![Effect::Metadata(MetadataEffect::VectorClock {
                        graph_iri: record.graph_iri,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            AnnounceTopicState::ReadMetadataClock => match event {
                Event::Metadata(MetadataEvent::VectorClockResult { clock, .. }) => self
                    .broadcast_message(
                        TopicMessageKind::Metadata,
                        TopicMessageVersion::Metadata { clock },
                    ),
                Event::Metadata(MetadataEvent::Error { error, .. }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("metadata vector clock result", format!("{other:?}"))
                }
            },
            AnnounceTopicState::ListUsers => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    let Some(PendingTopicAnnouncement::UserPage { realm_id, .. }) =
                        self.current.as_ref()
                    else {
                        return self.unexpected_event(
                            "tracked user page",
                            "missing current user page".to_string(),
                        );
                    };
                    let realm_id = *realm_id;
                    for (key, _) in values {
                        let user_id = match UserId::from_storage_key(&key) {
                            Ok(user_id) => user_id,
                            Err(error) => return self.fail(error.into()),
                        };
                        if user_id.realm_id == realm_id {
                            self.pending.push_back(PendingTopicAnnouncement::Automerge(
                                AutomergeDocumentVariant::User { user_id },
                            ));
                        }
                    }
                    if let Some(start_after) = next_start_after {
                        self.pending.push_back(PendingTopicAnnouncement::UserPage {
                            realm_id,
                            start_after: Some(start_after),
                        });
                    }
                    self.next_effect()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iter result", format!("{other:?}")),
            },
            AnnounceTopicState::Broadcast => match event {
                Event::Net(NetEvent::Gossip(GossipEvent::BroadcastComplete { .. })) => {
                    self.next_effect()
                }
                Event::Net(NetEvent::Gossip(GossipEvent::Error { .. }))
                | Event::Net(NetEvent::Error(_)) => {
                    self.state = AnnounceTopicState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                other => {
                    self.unexpected_event("gossip broadcast acknowledgement", format!("{other:?}"))
                }
            },
            AnnounceTopicState::Finish | AnnounceTopicState::Error => smallvec![],
            AnnounceTopicState::Init => smallvec![],
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

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{AnnounceTopicOperation, USER_ANNOUNCE_PAGE_SIZE};
    use aruna_core::effects::{Effect, GossipEffect, NetEffect, StorageEffect};
    use aruna_core::events::{Event, GossipEvent, NetEvent, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::RealmId;
    use aruna_core::task::TaskEvent;
    use aruna_core::types::UserId;
    use aruna_core::{TopicId, USER_KEYSPACE};
    use ulid::Ulid;

    fn node_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    fn subscribed_users_operation(realm_id: RealmId) -> AnnounceTopicOperation {
        let mut operation = AnnounceTopicOperation::new(TopicId::users(realm_id), node_id());
        assert!(matches!(operation.start().first(), Some(Effect::Task(_))));
        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: aruna_core::task::TaskKey::TopicAnnounce(TopicId::users(realm_id)),
            after: super::TOPIC_ANNOUNCE_INTERVAL,
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Net(NetEffect::Gossip(
                GossipEffect::Subscribe { .. }
            )))
        ));
        operation
    }

    #[test]
    fn users_topic_lists_user_keyspace_after_subscribe() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let mut operation = subscribed_users_operation(realm_id);

        let effects = operation.step(Event::Net(NetEvent::Gossip(GossipEvent::Subscribed {
            topic: TopicId::users(realm_id),
        })));

        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                key_space,
                prefix,
                start_after,
                limit,
                txn_id,
            }) => {
                assert_eq!(key_space, USER_KEYSPACE);
                assert_eq!(prefix, &None);
                assert_eq!(start_after, &None);
                assert_eq!(*limit, USER_ANNOUNCE_PAGE_SIZE);
                assert_eq!(txn_id, &None);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn users_topic_filters_realm_users_and_continues_pages() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let foreign_realm_id = RealmId::from_bytes([6u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([7u8; 16]), realm_id);
        let foreign_user_id = UserId::local(Ulid::from_bytes([8u8; 16]), foreign_realm_id);
        let cursor = foreign_user_id.to_storage_key();
        let mut operation = subscribed_users_operation(realm_id);
        operation.step(Event::Net(NetEvent::Gossip(GossipEvent::Subscribed {
            topic: TopicId::users(realm_id),
        })));

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (foreign_user_id.to_storage_key().into(), Vec::new().into()),
                (user_id.to_storage_key().into(), Vec::new().into()),
            ],
            next_start_after: Some(cursor.clone().into()),
        }));

        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Read { key, .. }) => {
                assert_eq!(key.as_ref(), user_id.to_storage_key().as_slice());
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: user_id.to_storage_key().into(),
            value: None,
        }));

        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                start_after: Some(start_after),
                ..
            }) => assert_eq!(start_after.as_ref(), cursor.as_slice()),
            other => panic!("unexpected effect: {other:?}"),
        }
    }
}
