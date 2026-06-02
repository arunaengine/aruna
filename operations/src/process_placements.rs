use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, PendingTopicPlacement};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{RealmConfigDocument, RealmId};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::document_repository::read_effect;
use crate::sync_placement::{
    decode_placement, delete_placement_effect, new_placement,
    select_sync_peers, sort_node_ids, write_placement_effect,
};

const PENDING_PLACEMENT_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub struct PlacementConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
}

#[derive(Debug, PartialEq)]
pub struct ProcessPlacementsOperation {
    config: PlacementConfig,
    state: PlacementState,
    realm_nodes: Vec<NodeId>,
    records: Vec<PendingTopicPlacement>,
    next_start_after: Option<Key>,
    current: Option<CurrentPlacement>,
    output: Option<Result<(), PlacementError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum PlacementState {
    Init,
    LoadRealmConfig,
    ListPending,
    Publish,
    StorePlacement,
    Finish,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
struct CurrentPlacement {
    target: DocumentSyncTarget,
    desired_peer_count: usize,
    selected_peers: Vec<NodeId>,
    newly_selected: Vec<NodeId>,
}

#[derive(Debug, Error, PartialEq)]
pub enum PlacementError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("pending placement decode failed: {0}")]
    Decode(String),
    #[error("realm config document not found")]
    RealmConfigNotFound,
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("placement persistence failed: {0}")]
    Placement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ProcessPlacementsOperation {
    pub fn new(config: PlacementConfig) -> Self {
        Self {
            config,
            state: PlacementState::Init,
            realm_nodes: Vec::new(),
            records: Vec::new(),
            next_start_after: None,
            current: None,
            output: None,
        }
    }

    fn fail(&mut self, error: PlacementError) -> Effects {
        self.state = PlacementState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(PlacementError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn emit_list_pending(&mut self) -> Effects {
        self.state = PlacementState::ListPending;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
            prefix: None,
            start_after: self.next_start_after.take(),
            limit: PENDING_PLACEMENT_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn emit_next_record(&mut self) -> Effects {
        let Some(record) = self.records.pop() else {
            if self.next_start_after.is_some() {
                return self.emit_list_pending();
            }
            self.state = PlacementState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        };

        let newly_selected = select_sync_peers(
            &record.target,
            self.config.local_node_id,
            &self.realm_nodes,
            &record.selected_peers,
            record.missing_peer_count,
        );
        self.current = Some(CurrentPlacement {
            target: record.target.clone(),
            desired_peer_count: record.desired_peer_count,
            selected_peers: record.selected_peers,
            newly_selected: newly_selected.clone(),
        });

        if newly_selected.is_empty() {
            return self.emit_placement_update();
        }

        self.state = PlacementState::Publish;
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceTopicOperation::new_for_document_with_peers(
                record.target.topic_id(),
                self.config.local_node_id,
                Some(record.target),
                newly_selected,
            ),
            |result| Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                result: result.map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn emit_placement_update(&mut self) -> Effects {
        let Some(mut current) = self.current.take() else {
            return self.emit_next_record();
        };
        current.selected_peers.append(&mut current.newly_selected);
        sort_node_ids(&mut current.selected_peers);

        self.state = PlacementState::StorePlacement;
        if current.selected_peers.len() >= current.desired_peer_count {
            return smallvec![delete_placement_effect(&current.target)];
        }

        let record = new_placement(
            current.target,
            current.desired_peer_count,
            current.selected_peers,
        );
        match write_placement_effect(&record) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(PlacementError::Placement(
                error.to_string(),
            )),
        }
    }
}

impl Operation for ProcessPlacementsOperation {
    type Output = ();
    type Error = PlacementError;

    fn start(&mut self) -> Effects {
        self.state = PlacementState::LoadRealmConfig;
        smallvec![read_effect(
            &DocumentSyncTarget::RealmConfig {
                realm_id: self.config.realm_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PlacementState::LoadRealmConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(PlacementError::RealmConfigNotFound);
                    };
                    let document = match RealmConfigDocument::from_bytes(&value) {
                        Ok(document) => document,
                        Err(error) => return self.fail(error.into()),
                    };
                    let mut nodes = match document.node_ids() {
                        Ok(nodes) => nodes,
                        Err(error) => return self.fail(error.into()),
                    };
                    nodes.retain(|node_id| *node_id != self.config.local_node_id);
                    sort_node_ids(&mut nodes);
                    self.realm_nodes = nodes;
                    self.emit_list_pending()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
            },
            PlacementState::ListPending => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    self.next_start_after = next_start_after;
                    self.records.clear();
                    for (_, value) in values.into_iter().rev() {
                        let record = match decode_placement(&value) {
                            Ok(record) => record,
                            Err(error) => {
                                return self.fail(PlacementError::Decode(
                                    error.to_string(),
                                ));
                            }
                        };
                        self.records.push(record);
                    }
                    self.emit_next_record()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("pending placement iter result", format!("{other:?}"))
                }
            },
            PlacementState::Publish => match event {
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) => {
                    match result {
                        Ok(()) => self.emit_placement_update(),
                        Err(error) => {
                            self.fail(PlacementError::DocumentSync(error))
                        }
                    }
                }
                other => self.unexpected_event("document sync result", format!("{other:?}")),
            },
            PlacementState::StorePlacement => match event {
                Event::Storage(StorageEvent::WriteResult { .. })
                | Event::Storage(StorageEvent::DeleteResult { .. }) => self.emit_next_record(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("placement storage result", format!("{other:?}")),
            },
            PlacementState::Init
            | PlacementState::Finish
            | PlacementState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PlacementState::Finish | PlacementState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
