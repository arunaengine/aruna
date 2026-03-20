use aruna_core::automerge::AutomergeState;
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{boxed_suboperation, Operation};
use aruna_core::task::{TaskEffect, TaskEvent};
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::{automerge_clock, read_effect};
use crate::automerge_announce::{AUTOMERGE_ANNOUNCE_INTERVAL, AUTOMERGE_ANNOUNCE_SHORT_INTERVAL};
use crate::outgoing_automerge::OutgoingAutomergeOperation;

#[derive(Debug, PartialEq)]
pub struct IncomingGossipOperation {
    topic: aruna_core::TopicId,
    sender: aruna_core::NodeId,
    data: Vec<u8>,
    announcement: Option<AutomergeState>,
    document: Option<aruna_core::automerge::AutomergeDocumentVariant>,
    state: IncomingGossipState,
    output: Option<Result<(), IncomingGossipError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum IncomingGossipState {
    Init,
    ReadDocument,
    ScheduleTimer,
    WaitForSync,
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
    #[error("failed to schedule automerge timer: {0}")]
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
    pub fn new(topic: aruna_core::TopicId, sender: aruna_core::NodeId, data: Vec<u8>) -> Self {
        Self {
            topic,
            sender,
            data,
            announcement: None,
            document: None,
            state: IncomingGossipState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: IncomingGossipError) -> aruna_core::types::Effects {
        self.state = IncomingGossipState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(IncomingGossipError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for IncomingGossipOperation {
    type Output = ();
    type Error = IncomingGossipError;

    fn start(&mut self) -> aruna_core::types::Effects {
        let announcement: AutomergeState = match postcard::from_bytes(&self.data) {
            Ok(announcement) => announcement,
            Err(_) => {
                self.state = IncomingGossipState::Finish;
                self.output = Some(Ok(()));
                return smallvec![];
            }
        };

        let Some(document) =
            aruna_core::automerge::AutomergeDocumentVariant::from_topic_id(&self.topic)
        else {
            self.state = IncomingGossipState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        };

        if announcement.node_id != self.sender {
            self.state = IncomingGossipState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        }

        self.announcement = Some(announcement);
        self.document = Some(document.clone());
        self.state = IncomingGossipState::ReadDocument;
        smallvec![read_effect(&document, None)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            IncomingGossipState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(announcement) = self.announcement.as_ref() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    let Some(document) = self.document.as_ref() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    let value = value.map(|value| value.to_vec()).unwrap_or_default();
                    let local_clock = match automerge_clock(&value) {
                        Ok(clock) => clock,
                        Err(error) => return self.fail(error.into()),
                    };
                    let same_heads = local_clock.heads == announcement.heads;

                    if same_heads && local_clock.change_count == announcement.change_count {
                        self.state = IncomingGossipState::ScheduleTimer;
                        return smallvec![Effect::Task(TaskEffect::ResetTimer {
                            key: document.announce_timer_key(),
                            after: AUTOMERGE_ANNOUNCE_INTERVAL,
                        })];
                    }

                    if announcement.change_count < local_clock.change_count {
                        self.state = IncomingGossipState::ScheduleTimer;
                        return smallvec![Effect::Task(TaskEffect::ShortenTimer {
                            key: document.announce_timer_key(),
                            after: AUTOMERGE_ANNOUNCE_SHORT_INTERVAL,
                        })];
                    }

                    self.state = IncomingGossipState::WaitForSync;
                    smallvec![Effect::SubOperation(boxed_suboperation(
                        OutgoingAutomergeOperation::new(self.sender, document.clone()),
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
            IncomingGossipState::WaitForSync => match event {
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

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::automerge::AutomergeClock;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::{Actor, MetadataDocument, RealmId};
    use aruna_core::types::GroupId;
    use byteview::ByteView;

    use crate::automerge::repository::automerge_clock;

    fn make_node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn make_document() -> aruna_core::automerge::AutomergeDocumentVariant {
        aruna_core::automerge::AutomergeDocumentVariant::Metadata {
            group_id: GroupId::from_bytes([1u8; 16]),
            document_id: GroupId::from_bytes([2u8; 16]),
        }
    }

    fn read_event(value: Vec<u8>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(b"doc".as_slice()),
            value: Some(value.into()),
        })
    }

    fn metadata_bytes(seed: u8) -> Vec<u8> {
        let actor = Actor {
            node_id: make_node(seed),
            user_id: GroupId::from_bytes([seed; 16]),
            realm_id: RealmId::from_bytes([seed; 32]),
        };
        MetadataDocument::new(
            GroupId::from_bytes([1u8; 16]),
            GroupId::from_bytes([2u8; 16]),
            format!("https://example.org/{seed}"),
        )
        .to_bytes(&actor)
        .expect("metadata bytes")
    }

    fn announcement(clock: AutomergeClock, node_id: aruna_core::NodeId) -> Vec<u8> {
        postcard::to_allocvec(&AutomergeState::new(
            clock.heads,
            clock.change_count,
            node_id,
        ))
        .expect("announcement bytes")
    }

    #[test]
    fn matching_state_resets_normal_timer() {
        let document = make_document();
        let remote_node = make_node(9);
        let local_bytes = metadata_bytes(3);
        let clock = automerge_clock(&local_bytes).expect("clock");
        let mut op = IncomingGossipOperation::new(
            document.topic_id(),
            remote_node,
            announcement(clock, remote_node),
        );

        let start = op.start();
        assert_eq!(start.len(), 1);

        let effects = op.step(read_event(local_bytes));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Task(TaskEffect::ResetTimer { after, .. })]
            if *after == AUTOMERGE_ANNOUNCE_INTERVAL
        ));
    }

    #[test]
    fn older_remote_state_shortens_timer() {
        let document = make_document();
        let remote_node = make_node(10);
        let local_bytes = metadata_bytes(4);
        let mut op = IncomingGossipOperation::new(
            document.topic_id(),
            remote_node,
            announcement(AutomergeClock::new(Vec::new(), 0), remote_node),
        );

        let start = op.start();
        assert_eq!(start.len(), 1);

        let effects = op.step(read_event(local_bytes));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Task(TaskEffect::ShortenTimer { after, .. })]
            if *after == AUTOMERGE_ANNOUNCE_SHORT_INTERVAL
        ));
    }

    #[test]
    fn newer_remote_state_starts_sync() {
        let document = make_document();
        let remote_node = make_node(11);
        let remote_bytes = metadata_bytes(5);
        let remote_clock = automerge_clock(&remote_bytes).expect("clock");
        let mut op = IncomingGossipOperation::new(
            document.topic_id(),
            remote_node,
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
        let payload = announcement(AutomergeClock::new(Vec::new(), 0), announced_by);
        let mut op = IncomingGossipOperation::new(document.topic_id(), sender, payload);

        let effects = op.start();
        assert!(effects.is_empty());
        assert!(op.is_complete());
    }
}
