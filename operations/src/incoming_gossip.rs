use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::gossip::{TopicMessage, TopicMessageVersion};
use aruna_core::metadata::{
    MetadataClockRelation, MetadataEffect, MetadataEvent, compare_metadata_clocks,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Effects;
use aruna_core::{NodeId, TopicId};
use craqle::VectorClock;
use smallvec::smallvec;
use thiserror::Error;
use tracing::trace;
use ulid::Ulid;

use crate::announce::{TOPIC_ANNOUNCE_INTERVAL, TOPIC_ANNOUNCE_SHORT_INTERVAL};
use crate::automerge::repository::{automerge_clock, read_effect};
use crate::metadata::repository::read_registry_by_document_effect;
use crate::outgoing_automerge::OutgoingAutomergeOperation;

#[derive(Debug, PartialEq)]
pub struct IncomingGossipOperation {
    topic: TopicId,
    sender: NodeId,
    local_node_id: NodeId,
    data: Vec<u8>,
    message: Option<TopicMessage>,
    state: IncomingGossipState,
    pending_timer: Option<TaskEffect>,
    metadata_sync_needs_reannounce: bool,
    output: Option<Result<(), IncomingGossipError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum IncomingGossipState {
    Init,
    ReadAutomergeDocument,
    ReadMetadataRecord,
    ReadMetadataClock,
    ScheduleTimer,
    WaitForAutomergeSync,
    WaitForMetadataSync,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum IncomingGossipError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("automerge sync failed: {0}")]
    AutomergeSync(String),
    #[error("metadata sync failed: {0}")]
    MetadataSync(String),
    #[error("failed to schedule topic timer: {0}")]
    ScheduleFailed(String),
    #[error("invalid gossip announcement")]
    InvalidAnnouncement,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl IncomingGossipOperation {
    pub fn new(topic: TopicId, sender: NodeId, local_node_id: NodeId, data: Vec<u8>) -> Self {
        Self {
            topic,
            sender,
            local_node_id,
            data,
            message: None,
            state: IncomingGossipState::Init,
            pending_timer: None,
            metadata_sync_needs_reannounce: false,
            output: None,
        }
    }

    fn fail(&mut self, error: IncomingGossipError) -> Effects {
        self.state = IncomingGossipState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(IncomingGossipError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn reset_timer(&mut self, after: std::time::Duration) -> Effects {
        self.pending_timer = Some(TaskEffect::ResetTimer {
            key: TaskKey::TopicAnnounce(self.topic.clone()),
            after,
        });
        self.state = IncomingGossipState::ScheduleTimer;
        smallvec![Effect::Task(
            self.pending_timer.clone().expect("pending timer set")
        )]
    }

    fn shorten_timer(&mut self, after: std::time::Duration) -> Effects {
        self.pending_timer = Some(TaskEffect::ShortenTimer {
            key: TaskKey::TopicAnnounce(self.topic.clone()),
            after,
        });
        self.state = IncomingGossipState::ScheduleTimer;
        smallvec![Effect::Task(
            self.pending_timer.clone().expect("pending timer set")
        )]
    }

    fn metadata_document_id(&self) -> Option<Ulid> {
        match self.topic {
            TopicId::Metadata(document_id) => Some(document_id),
            _ => None,
        }
    }

    fn message_id(&self) -> Option<Ulid> {
        self.message.as_ref().map(|message| message.message_id)
    }
}

impl Operation for IncomingGossipOperation {
    type Output = ();
    type Error = IncomingGossipError;

    fn start(&mut self) -> Effects {
        let Ok(message) = postcard::from_bytes::<TopicMessage>(&self.data) else {
            self.state = IncomingGossipState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        };

        if message.node_id != self.sender || !message.is_valid_for(&self.topic) {
            self.state = IncomingGossipState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        }

        if let Some(document) =
            AutomergeDocumentVariant::from_topic_message(&self.topic, &message.kind)
        {
            self.message = Some(message);
            self.state = IncomingGossipState::ReadAutomergeDocument;
            return smallvec![read_effect(&document, None)];
        }

        if matches!(message.version, TopicMessageVersion::Metadata { .. }) {
            let Some(document_id) = self.metadata_document_id() else {
                self.state = IncomingGossipState::Finish;
                self.output = Some(Ok(()));
                return smallvec![];
            };
            self.message = Some(message);
            self.state = IncomingGossipState::ReadMetadataRecord;
            return smallvec![read_registry_by_document_effect(document_id, None)];
        }

        self.state = IncomingGossipState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            IncomingGossipState::ReadAutomergeDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(message) = self.message.as_ref() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    let Some(document) =
                        AutomergeDocumentVariant::from_topic_message(&self.topic, &message.kind)
                    else {
                        self.state = IncomingGossipState::Finish;
                        self.output = Some(Ok(()));
                        return smallvec![];
                    };
                    let TopicMessageVersion::Automerge {
                        heads,
                        change_count,
                    } = &message.version
                    else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };

                    let local_bytes = value.map(|value| value.to_vec()).unwrap_or_default();
                    let local_clock = match automerge_clock(&local_bytes) {
                        Ok(clock) => clock,
                        Err(error) => return self.fail(error.into()),
                    };
                    let same_heads = local_clock.heads == *heads;
                    let message_id = self.message_id();

                    if same_heads && local_clock.change_count == *change_count {
                        trace!(
                            event = "gossip.state_matched",
                            topic = %self.topic,
                            sender = %self.sender,
                            message_id = message_id.as_ref().map(Ulid::to_string),
                            "Received gossip announcement with matching local state"
                        );
                        return self.reset_timer(TOPIC_ANNOUNCE_INTERVAL);
                    }

                    if *change_count < local_clock.change_count {
                        trace!(
                            event = "gossip.state_remote_behind",
                            topic = %self.topic,
                            sender = %self.sender,
                            message_id = message_id.as_ref().map(Ulid::to_string),
                            "Received gossip announcement from peer behind local state"
                        );
                        return self.shorten_timer(TOPIC_ANNOUNCE_SHORT_INTERVAL);
                    }

                    trace!(
                        event = "gossip.sync_requested",
                        topic = %self.topic,
                        sender = %self.sender,
                        message_id = message_id.as_ref().map(Ulid::to_string),
                        "Received newer gossip announcement and starting sync"
                    );
                    self.state = IncomingGossipState::WaitForAutomergeSync;
                    smallvec![Effect::SubOperation(boxed_suboperation(
                        OutgoingAutomergeOperation::new_with_local_node(
                            self.sender,
                            document,
                            self.local_node_id,
                        ),
                        |result| {
                            Event::SubOperation(SubOperationEvent::AutomergeSyncResult {
                                result: result.map_err(|error| error.to_string()),
                            })
                        },
                    ))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            IncomingGossipState::ReadMetadataRecord => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(message) = self.message.as_ref() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    let TopicMessageVersion::Metadata { .. } = &message.version else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    let Some(document_id) = self.metadata_document_id() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };

                    let Some(value) = value else {
                        self.metadata_sync_needs_reannounce = false;
                        self.state = IncomingGossipState::WaitForMetadataSync;
                        return smallvec![Effect::Metadata(MetadataEffect::SyncFromPeer {
                            node_id: self.sender,
                            document_id,
                            known_clock: VectorClock::default(),
                        })];
                    };

                    let record: aruna_core::structs::MetadataRegistryRecord =
                        match postcard::from_bytes(&value) {
                            Ok(record) => record,
                            Err(error) => return self.fail(ConversionError::from(error).into()),
                        };
                    self.state = IncomingGossipState::ReadMetadataClock;
                    smallvec![Effect::Metadata(MetadataEffect::VectorClock {
                        graph_iri: record.graph_iri,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            IncomingGossipState::ReadMetadataClock => match event {
                Event::Metadata(MetadataEvent::VectorClockResult { clock, .. }) => {
                    let Some(message) = self.message.as_ref() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    let TopicMessageVersion::Metadata {
                        clock: remote_clock,
                    } = &message.version
                    else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    let Some(document_id) = self.metadata_document_id() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };

                    match compare_metadata_clocks(&clock, remote_clock) {
                        MetadataClockRelation::Equal => self.reset_timer(TOPIC_ANNOUNCE_INTERVAL),
                        MetadataClockRelation::LocalAhead => {
                            self.shorten_timer(TOPIC_ANNOUNCE_SHORT_INTERVAL)
                        }
                        MetadataClockRelation::RemoteAhead => {
                            self.metadata_sync_needs_reannounce = false;
                            self.state = IncomingGossipState::WaitForMetadataSync;
                            smallvec![Effect::Metadata(MetadataEffect::SyncFromPeer {
                                node_id: self.sender,
                                document_id,
                                known_clock: clock.clone(),
                            })]
                        }
                        MetadataClockRelation::Concurrent => {
                            self.metadata_sync_needs_reannounce = true;
                            self.state = IncomingGossipState::WaitForMetadataSync;
                            smallvec![Effect::Metadata(MetadataEffect::SyncFromPeer {
                                node_id: self.sender,
                                document_id,
                                known_clock: clock.clone(),
                            })]
                        }
                    }
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    self.fail(IncomingGossipError::MetadataSync(error.to_string()))
                }
                other => {
                    self.unexpected_event("metadata vector clock result", format!("{other:?}"))
                }
            },
            IncomingGossipState::ScheduleTimer => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = IncomingGossipState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    self.fail(IncomingGossipError::ScheduleFailed(message))
                }
                other => self.unexpected_event("task timer acknowledgement", format!("{other:?}")),
            },
            IncomingGossipState::WaitForAutomergeSync => match event {
                Event::SubOperation(SubOperationEvent::AutomergeSyncResult { result }) => {
                    match result {
                        Ok(()) => {
                            self.state = IncomingGossipState::Finish;
                            self.output = Some(Ok(()));
                            smallvec![]
                        }
                        Err(error) => self.fail(IncomingGossipError::AutomergeSync(error)),
                    }
                }
                other => self.unexpected_event("automerge sync result", format!("{other:?}")),
            },
            IncomingGossipState::WaitForMetadataSync => match event {
                Event::Metadata(MetadataEvent::PeerSyncApplied { .. }) => {
                    if self.metadata_sync_needs_reannounce {
                        self.metadata_sync_needs_reannounce = false;
                        self.shorten_timer(TOPIC_ANNOUNCE_SHORT_INTERVAL)
                    } else {
                        self.state = IncomingGossipState::Finish;
                        self.output = Some(Ok(()));
                        smallvec![]
                    }
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    self.fail(IncomingGossipError::MetadataSync(error.to_string()))
                }
                other => self.unexpected_event("metadata sync result", format!("{other:?}")),
            },
            IncomingGossipState::Finish
            | IncomingGossipState::Error
            | IncomingGossipState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingGossipState::Finish | IncomingGossipState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use craqle::{ActorId, VectorClock};

    use super::*;
    use aruna_core::events::StorageEvent;
    use aruna_core::gossip::{TopicMessage, TopicMessageKind, TopicMessageVersion};
    use aruna_core::structs::RealmId;
    use aruna_core::types::GroupId;
    use byteview::ByteView;

    use crate::automerge::repository::automerge_clock;

    fn make_node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn make_document() -> aruna_core::automerge::AutomergeDocumentVariant {
        aruna_core::automerge::AutomergeDocumentVariant::Group {
            group_id: GroupId::from_bytes([1u8; 16]),
        }
    }

    fn read_event(value: Vec<u8>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(b"doc".as_slice()),
            value: Some(value.into()),
        })
    }

    fn group_bytes(seed: u8) -> Vec<u8> {
        let actor = aruna_core::structs::Actor {
            node_id: make_node(seed),
            user_id: aruna_core::UserId::local(
                GroupId::from_bytes([seed; 16]),
                RealmId::from_bytes([seed; 32]),
            ),
            realm_id: RealmId::from_bytes([seed; 32]),
        };
        aruna_core::structs::Group {
            display_name: format!("group-{seed}"),
            group_id: GroupId::from_bytes([1u8; 16]),
            realm_id: RealmId::from_bytes([seed; 32]),
            roles: std::collections::HashSet::new(),
        }
        .to_bytes(&actor)
        .expect("group bytes")
    }

    fn announcement(clock: aruna_core::AutomergeClock, node_id: aruna_core::NodeId) -> Vec<u8> {
        postcard::to_allocvec(&TopicMessage::new(
            TopicMessageKind::Group,
            Ulid::new(),
            node_id,
            TopicMessageVersion::Automerge {
                heads: clock.heads,
                change_count: clock.change_count,
            },
        ))
        .expect("announcement bytes")
    }

    #[test]
    fn matching_state_resets_normal_timer() {
        let document = make_document();
        let remote_node = make_node(9);
        let local_bytes = group_bytes(3);
        let clock = automerge_clock(&local_bytes).expect("clock");
        let mut op = IncomingGossipOperation::new(
            document.topic_id(),
            remote_node,
            make_node(1),
            announcement(clock, remote_node),
        );

        let start = op.start();
        assert_eq!(start.len(), 1);

        let effects = op.step(read_event(local_bytes));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Task(TaskEffect::ResetTimer { after, .. })]
            if *after == TOPIC_ANNOUNCE_INTERVAL
        ));
    }

    #[test]
    fn older_remote_state_shortens_timer() {
        let document = make_document();
        let remote_node = make_node(10);
        let local_bytes = group_bytes(4);
        let mut op = IncomingGossipOperation::new(
            document.topic_id(),
            remote_node,
            make_node(1),
            announcement(aruna_core::AutomergeClock::new(Vec::new(), 0), remote_node),
        );

        let start = op.start();
        assert_eq!(start.len(), 1);

        let effects = op.step(read_event(local_bytes));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Task(TaskEffect::ShortenTimer { after, .. })]
            if *after == TOPIC_ANNOUNCE_SHORT_INTERVAL
        ));
    }

    #[test]
    fn newer_remote_state_starts_sync() {
        let document = make_document();
        let remote_node = make_node(11);
        let remote_bytes = group_bytes(5);
        let remote_clock = automerge_clock(&remote_bytes).expect("clock");
        let mut op = IncomingGossipOperation::new(
            document.topic_id(),
            remote_node,
            make_node(1),
            announcement(remote_clock, remote_node),
        );

        let start = op.start();
        assert_eq!(start.len(), 1);

        let effects = op.step(read_event(Vec::new()));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    }

    #[test]
    fn mismatched_announcement_node_is_ignored() {
        let document = make_document();
        let sender = make_node(12);
        let announced_by = make_node(13);
        let payload = announcement(aruna_core::AutomergeClock::new(Vec::new(), 0), announced_by);
        let mut op =
            IncomingGossipOperation::new(document.topic_id(), sender, make_node(1), payload);

        let effects = op.start();
        assert!(effects.is_empty());
        assert!(op.is_complete());
    }

    #[test]
    fn invalid_metadata_version_is_rejected_for_group_topic() {
        let message = TopicMessage::new(
            TopicMessageKind::Metadata,
            Ulid::new(),
            make_node(14),
            TopicMessageVersion::Metadata {
                clock: VectorClock(BTreeMap::<ActorId, u64>::new()),
            },
        );
        let mut op = IncomingGossipOperation::new(
            TopicId::group(GroupId::from_bytes([1u8; 16])),
            message.node_id,
            make_node(1),
            postcard::to_allocvec(&message).expect("message bytes"),
        );

        let effects = op.start();
        assert!(effects.is_empty());
        assert!(op.is_complete());
    }
}
