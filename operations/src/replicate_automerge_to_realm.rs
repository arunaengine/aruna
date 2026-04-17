use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{boxed_suboperation, Operation};
use aruna_core::structs::RealmId;
use aruna_core::types::Effects;
use aruna_core::NodeId;
use smallvec::smallvec;
use thiserror::Error;
use tracing::{trace, warn};

use crate::get_realm_nodes::GetRealmNodesOperation;
use crate::outgoing_automerge::OutgoingAutomergeOperation;

const MAX_SYNC_RETRIES: u8 = 3;

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicateAutomergeDocumentsToRealmConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub documents: Vec<AutomergeDocumentVariant>,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateAutomergeDocumentsToRealmOperation {
    config: ReplicateAutomergeDocumentsToRealmConfig,
    state: ReplicateAutomergeDocumentsToRealmState,
    pending_documents: Vec<AutomergeDocumentVariant>,
    realm_nodes: Vec<NodeId>,
    current_document: Option<AutomergeDocumentVariant>,
    current_document_successes: usize,
    current_target: Option<NodeId>,
    current_attempt: u8,
    current_targets: Vec<NodeId>,
    output: Option<Result<(), ReplicateAutomergeDocumentsToRealmError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReplicateAutomergeDocumentsToRealmState {
    Init,
    LoadRealmNodes,
    Replicate,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicateAutomergeDocumentsToRealmError {
    #[error("failed to load realm nodes: {0}")]
    RealmNodes(String),
    #[error("automerge replication failed: {0}")]
    AutomergeSync(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ReplicateAutomergeDocumentsToRealmOperation {
    pub fn new(config: ReplicateAutomergeDocumentsToRealmConfig) -> Self {
        Self {
            pending_documents: config.documents.clone().into_iter().rev().collect(),
            config,
            state: ReplicateAutomergeDocumentsToRealmState::Init,
            realm_nodes: Vec::new(),
            current_document: None,
            current_document_successes: 0,
            current_target: None,
            current_attempt: 0,
            current_targets: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: ReplicateAutomergeDocumentsToRealmError) -> Effects {
        self.state = ReplicateAutomergeDocumentsToRealmState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ReplicateAutomergeDocumentsToRealmError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn finish_success(&mut self) -> Effects {
        self.state = ReplicateAutomergeDocumentsToRealmState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn emit_next_replication(&mut self) -> Effects {
        loop {
            if self.current_document.is_none() {
                let Some(document) = self.pending_documents.pop() else {
                    return self.finish_success();
                };

                trace!(
                    event = "automerge.realm_replication.started",
                    topic = %document.topic_id(),
                    target_count = self.realm_nodes.len(),
                    "Replicating automerge document to realm nodes"
                );

                self.current_targets = self.realm_nodes.clone();
                self.current_targets.reverse();
                self.current_document_successes = 0;
                self.current_document = Some(document);
            }

            let Some(target) = self.current_targets.pop() else {
                if let Some(document) = self.current_document.take() {
                    if self.current_document_successes == 0 {
                        return self.fail(ReplicateAutomergeDocumentsToRealmError::AutomergeSync(
                            format!(
                                "no successful automerge replication for {}",
                                document.topic_id()
                            ),
                        ));
                    }
                    self.current_target = None;
                    self.current_attempt = 0;
                    trace!(
                        event = "automerge.realm_replication.completed",
                        topic = %document.topic_id(),
                        target_count = self.realm_nodes.len(),
                        success_count = self.current_document_successes,
                        "Replicated automerge document to realm nodes"
                    );
                }
                continue;
            };

            let Some(document) = self.current_document.clone() else {
                continue;
            };

            self.current_target = Some(target);
            self.current_attempt = 0;
            return self.emit_replication(document);
        }
    }

    fn emit_replication(&mut self, document: AutomergeDocumentVariant) -> Effects {
        let Some(target) = self.current_target else {
            return self.finish_success();
        };

        self.state = ReplicateAutomergeDocumentsToRealmState::Replicate;
        smallvec![aruna_core::effects::Effect::SubOperation(
            boxed_suboperation(
                OutgoingAutomergeOperation::new(target, document),
                |result| Event::SubOperation(SubOperationEvent::AutomergeSyncResult {
                    result: result.map_err(|error| error.to_string()),
                }),
            )
        )]
    }
}

impl Operation for ReplicateAutomergeDocumentsToRealmOperation {
    type Output = ();
    type Error = ReplicateAutomergeDocumentsToRealmError;

    fn start(&mut self) -> Effects {
        self.state = ReplicateAutomergeDocumentsToRealmState::LoadRealmNodes;
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
            ReplicateAutomergeDocumentsToRealmState::LoadRealmNodes => match event {
                Event::SubOperation(SubOperationEvent::RealmNodesResult { result }) => {
                    let realm_nodes = match result {
                        Ok(nodes) => nodes,
                        Err(error) => {
                            return self
                                .fail(ReplicateAutomergeDocumentsToRealmError::RealmNodes(error));
                        }
                    };

                    self.realm_nodes = realm_nodes
                        .into_iter()
                        .filter(|node_id| *node_id != self.config.local_node_id)
                        .collect();

                    if self.realm_nodes.is_empty() {
                        return self.finish_success();
                    }

                    self.emit_next_replication()
                }
                other => self.unexpected_event("realm node lookup result", format!("{other:?}")),
            },
            ReplicateAutomergeDocumentsToRealmState::Replicate => match event {
                Event::SubOperation(SubOperationEvent::AutomergeSyncResult { result }) => {
                    match result {
                        Ok(()) => {
                            self.current_document_successes += 1;
                            self.current_target = None;
                            self.current_attempt = 0;
                            self.emit_next_replication()
                        }
                        Err(error) => {
                            if self.current_attempt + 1 < MAX_SYNC_RETRIES {
                                self.current_attempt += 1;
                                if let (Some(target), Some(document)) =
                                    (self.current_target, self.current_document.clone())
                                {
                                    warn!(
                                        event = "automerge.realm_replication.retry",
                                        topic = %document.topic_id(),
                                        target = %target,
                                        attempt = self.current_attempt + 1,
                                        error = %error,
                                        "Retrying automerge replication to realm node"
                                    );
                                    return self.emit_replication(document);
                                }
                            }

                            if let (Some(target), Some(document)) =
                                (self.current_target, self.current_document.clone())
                            {
                                warn!(
                                    event = "automerge.realm_replication.skipped_target",
                                    topic = %document.topic_id(),
                                    target = %target,
                                    error = %error,
                                    "Skipping failed automerge replication target"
                                );
                            }

                            self.current_target = None;
                            self.current_attempt = 0;
                            self.emit_next_replication()
                        }
                    }
                }
                other => self.unexpected_event("automerge sync result", format!("{other:?}")),
            },
            ReplicateAutomergeDocumentsToRealmState::Init
            | ReplicateAutomergeDocumentsToRealmState::Finish
            | ReplicateAutomergeDocumentsToRealmState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplicateAutomergeDocumentsToRealmState::Finish
                | ReplicateAutomergeDocumentsToRealmState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
