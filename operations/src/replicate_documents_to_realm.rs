use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::RealmId;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::get_realm_nodes::GetRealmNodesOperation;

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicateDocumentsToRealmConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub documents: Vec<DocumentSyncTarget>,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateDocumentsToRealmOperation {
    config: ReplicateDocumentsToRealmConfig,
    state: ReplicateDocumentsToRealmState,
    pending_documents: Vec<DocumentSyncTarget>,
    realm_nodes: Vec<NodeId>,
    output: Option<Result<(), ReplicateDocumentsToRealmError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReplicateDocumentsToRealmState {
    Init,
    LoadRealmNodes,
    Publish,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicateDocumentsToRealmError {
    #[error("failed to load realm nodes: {0}")]
    RealmNodes(String),
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ReplicateDocumentsToRealmOperation {
    pub fn new(config: ReplicateDocumentsToRealmConfig) -> Self {
        Self {
            pending_documents: config.documents.clone().into_iter().rev().collect(),
            config,
            state: ReplicateDocumentsToRealmState::Init,
            realm_nodes: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: ReplicateDocumentsToRealmError) -> Effects {
        self.state = ReplicateDocumentsToRealmState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ReplicateDocumentsToRealmError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn finish_success(&mut self) -> Effects {
        self.state = ReplicateDocumentsToRealmState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn emit_next_publish(&mut self) -> Effects {
        let Some(document) = self.pending_documents.pop() else {
            return self.finish_success();
        };

        self.state = ReplicateDocumentsToRealmState::Publish;
        smallvec![aruna_core::effects::Effect::SubOperation(
            boxed_suboperation(
                AnnounceTopicOperation::new_for_document_with_peers(
                    document.topic_id(),
                    self.config.local_node_id,
                    Some(document),
                    self.realm_nodes.clone(),
                ),
                |result| Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                    result: result.map_err(|error| error.to_string()),
                }),
            )
        )]
    }
}

impl Operation for ReplicateDocumentsToRealmOperation {
    type Output = ();
    type Error = ReplicateDocumentsToRealmError;

    fn start(&mut self) -> Effects {
        self.state = ReplicateDocumentsToRealmState::LoadRealmNodes;
        smallvec![aruna_core::effects::Effect::SubOperation(
            boxed_suboperation(
                GetRealmNodesOperation::new(self.config.realm_id),
                |result| Event::SubOperation(SubOperationEvent::RealmNodesResult {
                    result: result
                        .map(|nodes| {
                            let mut nodes: Vec<_> = nodes.into_iter().collect();
                            nodes.sort_by_key(|node_id| *node_id.as_bytes());
                            nodes
                        })
                        .map_err(|error| error.to_string()),
                }),
            )
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplicateDocumentsToRealmState::LoadRealmNodes => match event {
                Event::SubOperation(SubOperationEvent::RealmNodesResult { result }) => {
                    let realm_nodes = match result {
                        Ok(nodes) => nodes,
                        Err(error) => {
                            return self.fail(ReplicateDocumentsToRealmError::RealmNodes(error));
                        }
                    };
                    self.realm_nodes = realm_nodes
                        .into_iter()
                        .filter(|node_id| *node_id != self.config.local_node_id)
                        .collect();
                    if self.realm_nodes.is_empty() {
                        return self.finish_success();
                    }
                    self.emit_next_publish()
                }
                other => self.unexpected_event("realm node lookup result", format!("{other:?}")),
            },
            ReplicateDocumentsToRealmState::Publish => match event {
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) => {
                    match result {
                        Ok(()) => self.emit_next_publish(),
                        Err(error) => {
                            self.fail(ReplicateDocumentsToRealmError::DocumentSync(error))
                        }
                    }
                }
                other => self.unexpected_event("document sync result", format!("{other:?}")),
            },
            ReplicateDocumentsToRealmState::Init
            | ReplicateDocumentsToRealmState::Finish
            | ReplicateDocumentsToRealmState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplicateDocumentsToRealmState::Finish | ReplicateDocumentsToRealmState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
