use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, GossipEffect, NetEffect};
use aruna_core::errors::{ConversionError, GossipError};
use aruna_core::events::{Event, GossipEvent, NetEvent, StorageEvent};
use aruna_core::gossip::{TopicMessage, TopicMessageKind, TopicMessageVersion};
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::operation::Operation;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::{NodeId, TopicId};
use smallvec::smallvec;
use thiserror::Error;
use tracing::trace;
use ulid::Ulid;

use crate::automerge::repository::{automerge_clock, read_effect};
use crate::metadata::repository::read_registry_by_document_effect;
use crate::telemetry::current_trace_id;

pub const TOPIC_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(30);
pub const TOPIC_ANNOUNCE_SHORT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq)]
enum PendingTopicAnnouncement {
    Automerge(AutomergeDocumentVariant),
    Metadata { document_id: Ulid },
}

#[derive(Debug, PartialEq)]
pub struct AnnounceTopicOperation {
    topic: TopicId,
    local_node_id: NodeId,
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
        Self {
            topic,
            local_node_id,
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

        match &self.topic {
            TopicId::Realm(realm_id) => {
                self.pending.push_back(PendingTopicAnnouncement::Automerge(
                    AutomergeDocumentVariant::RealmAuthorization {
                        realm_id: realm_id.clone(),
                    },
                ));
                self.pending.push_back(PendingTopicAnnouncement::Automerge(
                    AutomergeDocumentVariant::RealmConfig {
                        realm_id: realm_id.clone(),
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
