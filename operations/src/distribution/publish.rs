//! Accepts one request on the set's publisher: validates it against the current head, checks
//! group admin WRITE for admin requests and commits the signed revision, the accepted request
//! and the outbox row in one transaction. Only committed revisions are published.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::{
    AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, SnapshotSignEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    DISTRIBUTION_ACCEPT_KEYSPACE, DISTRIBUTION_KEYSPACE, DOCUMENT_STATE_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminApplyStatus, AdminDocumentError, AdminDocumentState};
use aruna_core::storage_entries::{reducer_state_entry, reducer_state_key};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::distribution::snapshot::SnapshotClaim;
use aruna_core::structs::storage::distribution::{
    DistributionError, DistributionRequest, DistributionRevision, DistributionSnapshot,
    DistributionView, SignedSnapshot, next_snapshot,
};
use aruna_core::types::{Effects, TxnId, Value};
use aruna_core::{TaskEvent, UserId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

const EVENT_DOMAIN: &[u8] = b"aruna-distribution-event-v1";

#[derive(Debug, Clone, PartialEq)]
pub struct PublishConfig {
    pub realm_id: RealmId,
    /// This node; it must be the set's publisher.
    pub node_id: NodeId,
    pub set_id: Ulid,
    /// The identity of the request; a retry of an accepted one returns its revision.
    pub request_id: Ulid,
    pub request: DistributionRequest,
    /// The requester's own token context; admin requests need group admin WRITE with it.
    pub auth_context: Option<AuthContext>,
}

#[derive(Debug, Error, PartialEq)]
pub enum PublishError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Distribution(#[from] DistributionError),
    #[error(transparent)]
    Reducer(#[from] AdminDocumentError),
    #[error("caller may not administer the distributed bucket's owner group")]
    Unauthorized,
    #[error("this node is not the publisher of the distributed bucket")]
    NotPublisher,
    #[error("the publisher signed two revisions of one number; configuration changes stopped")]
    Conflicted,
    #[error("a newer admin revision made this automatic request stale")]
    Dropped,
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("snapshot signing unavailable: {0}")]
    SignUnavailable(String),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: got {got}")]
    UnexpectedEvent { state: &'static str, got: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishState {
    Init,
    StartTransaction,
    ReadState,
    Authorize,
    Sign,
    Write,
    Commit,
    Drain,
    Replay,
    Finish,
    Error,
}

/// What the read phase found in the publisher's own storage.
#[derive(Debug, Default, PartialEq)]
struct Stored {
    view: Option<DistributionView>,
    config: Option<RealmConfigDocument>,
    reducer: Option<AdminDocumentState>,
}

#[derive(Debug, PartialEq)]
pub struct PublishOperation {
    config: PublishConfig,
    state: PublishState,
    txn_id: Option<TxnId>,
    stored: Stored,
    next: Option<DistributionSnapshot>,
    output: Option<Result<DistributionRevision, PublishError>>,
}

/// The key of an accepted request: set id then request id.
pub fn accept_key(set_id: Ulid, request_id: Ulid) -> Vec<u8> {
    let mut key = set_id.to_bytes().to_vec();
    key.extend_from_slice(&request_id.to_bytes());
    key
}

/// One event id per revision, so a retried publication rewrites one outbox row.
fn revision_event(set_id: Ulid, number: u64) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(EVENT_DOMAIN);
    hasher.update(&set_id.to_bytes());
    hasher.update(&number.to_be_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

impl PublishOperation {
    pub fn new(config: PublishConfig) -> Self {
        Self {
            config,
            state: PublishState::Init,
            txn_id: None,
            stored: Stored::default(),
            next: None,
            output: None,
        }
    }

    fn target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::Distribution {
            realm_id: self.config.realm_id,
            set_id: self.config.set_id,
        }
    }

    fn config_target(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        }
    }

    fn read_state(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = PublishState::ReadState;
        let set_id = self.config.set_id;
        let document = self.config_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    DISTRIBUTION_KEYSPACE.to_string(),
                    set_id.to_bytes().to_vec().into(),
                ),
                (
                    DISTRIBUTION_ACCEPT_KEYSPACE.to_string(),
                    accept_key(set_id, self.config.request_id).into(),
                ),
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key()
                ),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&self.target()),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    /// Validates the request against the head; admin requests continue to authorization.
    fn decide(&mut self, values: Vec<Option<Value>>) -> Result<Effects, PublishError> {
        let [view, accepted, config, reducer] =
            <[Option<Value>; 4]>::try_from(values).map_err(|_| PublishError::UnexpectedEvent {
                state: "read state",
                got: "a batch of another length".to_string(),
            })?;
        if let Some(accepted) = accepted {
            let revision: DistributionRevision =
                postcard::from_bytes(accepted.as_ref()).map_err(ConversionError::from)?;
            self.output = Some(Ok(revision));
            return Ok(self.abort_replay());
        }
        self.stored = Stored {
            view: view
                .map(|value| DistributionView::from_bytes(value.as_ref()))
                .transpose()?,
            config: config
                .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
                .transpose()?,
            reducer: reducer
                .map(|value| aruna_core::reducer::decode_reducer_state(value.as_ref()))
                .transpose()
                .map_err(ConversionError::from)?,
        };
        if self.stored.config.is_none() {
            return Err(PublishError::ConfigMissing);
        }
        let head = self.stored.view.as_ref();
        if head.is_some_and(DistributionView::conflicted) {
            return Err(PublishError::Conflicted);
        }
        let head = head.map(DistributionView::snapshot);
        let request = &self.config.request;
        let next = match next_snapshot(self.config.realm_id, head, request) {
            Ok(next) => next,
            Err(DistributionError::StaleRevision) if !request.admin() => {
                return Err(PublishError::Dropped);
            }
            Err(error) => return Err(error.into()),
        };
        if next.root.publisher != self.config.node_id || next.set_id() != self.config.set_id {
            return Err(PublishError::NotPublisher);
        }
        let owner = next.root.owner_group;
        self.next = Some(next);
        if !request.admin() {
            return Ok(self.sign());
        }
        let Some(auth_context) = self.config.auth_context.clone() else {
            return Err(PublishError::Unauthorized);
        };
        self.state = PublishState::Authorize;
        Ok(smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context,
                path: group_admin_path(self.config.realm_id, owner),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))])
    }

    fn sign(&mut self) -> Effects {
        let Some(snapshot) = self.next.clone() else {
            return self.fail(PublishError::NotFinished);
        };
        self.state = PublishState::Sign;
        smallvec![Effect::Net(NetEffect::SnapshotSign(Box::new(
            SnapshotClaim {
                realm_id: self.config.realm_id,
                snapshot,
            }
        )))]
    }

    /// The head, the reducer state, the accepted request and the outbox row, in one batch.
    fn write(&mut self, signed: SignedSnapshot) -> Result<Effects, PublishError> {
        let txn_id = self.txn_id.ok_or(PublishError::NotFinished)?;
        let revision = signed.snapshot.revision();
        let set_id = self.config.set_id;
        let user_id = self
            .config
            .auth_context
            .as_ref()
            .map_or(UserId::nil(self.config.realm_id), |auth| auth.user_id);
        let event = AdminDocumentEvent {
            event_id: revision_event(set_id, revision.number),
            target: self.target(),
            origin_node_id: self.config.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: self.config.node_id,
                user_id,
                realm_id: self.config.realm_id,
            },
            op: AdminDocumentOperation::DistributionPublished {
                snapshot: Box::new(signed),
            },
        };
        let mut reducer = self
            .stored
            .reducer
            .take()
            .unwrap_or_else(|| AdminDocumentState::new(self.target()));
        if reducer.apply(&event)? != AdminApplyStatus::Applied {
            return Err(DistributionError::StaleRevision.into());
        }
        let view = reducer
            .distribution_view()
            .ok_or(DistributionError::UnknownSet)?;
        let config = self
            .stored
            .config
            .as_ref()
            .ok_or(PublishError::ConfigMissing)?;
        let document = self.config_target();
        let placement = target_placement_ref(config, &document, Default::default());
        let record = new_identified_record(
            event.event_id,
            self.config.node_id,
            document,
            Vec::new(),
            DocumentOutboxEvent::admin(event),
            placement,
            false,
        );
        let accepted = postcard::to_allocvec(&revision).map_err(ConversionError::from)?;
        let writes = vec![
            (
                DISTRIBUTION_KEYSPACE.to_string(),
                set_id.to_bytes().to_vec().into(),
                view.to_bytes()?.into(),
            ),
            reducer_state_entry(&reducer)?,
            (
                DISTRIBUTION_ACCEPT_KEYSPACE.to_string(),
                accept_key(set_id, self.config.request_id).into(),
                accepted.into(),
            ),
            outbox_write_entry(&record).map_err(ConversionError::from)?,
        ];
        self.output = Some(Ok(revision));
        self.state = PublishState::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn abort_replay(&mut self) -> Effects {
        self.state = PublishState::Replay;
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => {
                self.state = PublishState::Finish;
                smallvec![]
            }
        }
    }

    fn fail(&mut self, error: PublishError) -> Effects {
        let cleanup = self.abort();
        self.state = PublishState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, state: &'static str, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        self.fail(PublishError::UnexpectedEvent {
            state,
            got: format!("{event:?}"),
        })
    }

    fn settle(&mut self, result: Result<Effects, PublishError>) -> Effects {
        result.unwrap_or_else(|error| self.fail(error))
    }
}

impl Operation for PublishOperation {
    type Output = DistributionRevision;
    type Error = PublishError;

    fn start(&mut self) -> Effects {
        self.state = PublishState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (
                PublishState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => self.read_state(txn_id),
            (PublishState::ReadState, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let values = values.into_iter().map(|(_, value)| value).collect();
                let decided = self.decide(values);
                self.settle(decided)
            }
            (
                PublishState::Authorize,
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
            ) => match allowed {
                Ok(true) => self.sign(),
                Ok(false) => self.fail(PublishError::Unauthorized),
                Err(AuthorizationError::StorageError(error)) => self.fail(error.into()),
                Err(_) => self.fail(PublishError::Unauthorized),
            },
            (PublishState::Sign, Event::Net(NetEvent::SnapshotSign(signed))) => match signed {
                SnapshotSignEvent::Signed(signed) => {
                    let written = self.write(*signed);
                    self.settle(written)
                }
                SnapshotSignEvent::Unavailable(reason) => {
                    self.fail(PublishError::SignUnavailable(reason))
                }
            },
            (PublishState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                match self.txn_id {
                    Some(txn_id) => {
                        self.state = PublishState::Commit;
                        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                    }
                    None => self.fail(PublishError::NotFinished),
                }
            }
            (PublishState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = PublishState::Drain;
                smallvec![schedule_drain_effect()]
            }
            (PublishState::Drain, Event::Task(TaskEvent::TimerScheduled { .. }))
            | (PublishState::Drain, Event::Task(TaskEvent::Error { .. })) => {
                // A missed timer leaves the durable outbox row for the next drain.
                self.state = PublishState::Finish;
                smallvec![]
            }
            (PublishState::Replay, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                self.state = PublishState::Finish;
                smallvec![]
            }
            (PublishState::Finish | PublishState::Error | PublishState::Init, _) => smallvec![],
            (state, event) => {
                let name = match state {
                    PublishState::StartTransaction => "start transaction",
                    PublishState::ReadState => "read state",
                    PublishState::Authorize => "authorize",
                    PublishState::Sign => "sign",
                    PublishState::Write => "write",
                    PublishState::Commit => "commit",
                    PublishState::Drain => "drain",
                    PublishState::Replay => "replay",
                    PublishState::Init | PublishState::Finish | PublishState::Error => "done",
                };
                self.unexpected(name, event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PublishState::Finish | PublishState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(PublishError::NotFinished);
        }
        self.output.unwrap_or(Err(PublishError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}
