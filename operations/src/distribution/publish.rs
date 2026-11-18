//! Accepts one request on the set's publisher: validates it against the current head, checks
//! group admin WRITE for admin requests and commits it in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// That transaction holds the signed revision, the accepted request and the outbox row.
// Only committed revisions are published.

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::admin_documents::{
    AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, SnapshotSignEvent};
use aruna_core::keyspaces::{
    CATCH_UP_KEYSPACE, DISTRIBUTION_ACCEPT_KEYSPACE, DISTRIBUTION_KEYSPACE,
    DISTRIBUTION_LIFECYCLE_KEYSPACE, DISTRIBUTION_PUBLISHER_KEYSPACE, DOCUMENT_STATE_KEYSPACE,
    NODE_STATS_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminApplyStatus, AdminDocumentError, AdminDocumentState};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{reducer_state_entry, reducer_state_key};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::distribution::snapshot::SnapshotClaim;
use aruna_core::structs::storage::distribution::{
    DistributionError, DistributionRequest, DistributionRevision, DistributionSnapshot,
    DistributionView, SetPhase, SignedSnapshot, next_snapshot, publisher_key,
};
use aruna_core::structs::storage::usage::dirty_share_key;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

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
    #[error("the request id was already used for another request or requester")]
    RequestReused,
    #[error("snapshot signing unavailable: {0}")]
    SignUnavailable(String),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected answer to the signing request: {0}")]
    UnexpectedEvent(&'static str),
}

impl From<IoError> for PublishError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// What the pending call of a publication answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishState {
    ReadState,
    Authorize,
    Sign,
    Write,
}

type PublishNext = Result<Next<DistributionRevision>, PublishError>;

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
    stored: Stored,
    next: Option<DistributionSnapshot>,
    /// The head revision an already applied activation answers with.
    current: Option<DistributionRevision>,
    /// The admin check whose rows the acceptance transaction reads.
    check: Option<CheckPermissionsConfig>,
    /// The revision the commit publishes.
    revision: Option<DistributionRevision>,
}

/// What the publisher keeps per accepted request, so a retry replays only the same request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedRequest {
    pub revision: DistributionRevision,
    /// BLAKE3 over the canonical request encoding.
    pub fingerprint: [u8; 32],
    pub requester: Option<UserId>,
}

/// The key of an accepted request: set id then request id.
fn accept_key(set_id: Ulid, request_id: Ulid) -> Vec<u8> {
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
            state: PublishState::ReadState,
            stored: Stored::default(),
            next: None,
            current: None,
            check: None,
            revision: None,
        }
    }

    fn fingerprint(&self) -> [u8; 32] {
        let bytes = postcard::to_allocvec(&self.config.request).unwrap_or_default();
        *blake3::hash(&bytes).as_bytes()
    }

    fn requester(&self) -> Option<UserId> {
        self.config.auth_context.as_ref().map(|auth| auth.user_id)
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

    fn call(&mut self, state: PublishState, call: IoCall) -> PublishNext {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read_state(&self) -> IoCall {
        let set_id = self.config.set_id;
        let document = self.config_target();
        IoCall::Read(vec![
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
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&self.target()),
            ),
        ])
    }

    /// Validates the request against the head; admin requests continue to authorization.
    fn decide(&mut self, result: IoResult) -> PublishNext {
        let [view, accepted, config, reducer] = values::<4>(result)?;
        if let Some(accepted) = accepted {
            let accepted: AcceptedRequest =
                postcard::from_bytes(accepted.as_ref()).map_err(ConversionError::from)?;
            // A reused request id only replays the same request from the same requester.
            if accepted.fingerprint != self.fingerprint() || accepted.requester != self.requester()
            {
                return Err(PublishError::RequestReused);
            }
            return Ok(Next::Done(accepted.revision));
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
        let Some(config) = self.stored.config.as_ref() else {
            return Err(PublishError::ConfigMissing);
        };
        // A publisher removed from the realm, or never able to publish, accepts nothing.
        let member = config
            .sync_eligible_nodes()
            .is_ok_and(|nodes| nodes.contains(&self.config.node_id));
        if !member {
            return Err(PublishError::NotPublisher);
        }
        let head = self.stored.view.as_ref();
        if head.is_some_and(DistributionView::conflicted) {
            return Err(PublishError::Conflicted);
        }
        let head = head.map(DistributionView::snapshot);
        let request = &self.config.request;
        let owner = match (next_snapshot(self.config.realm_id, head, request), head) {
            (Ok(next), _) => {
                if next.root.publisher != self.config.node_id || next.set_id() != self.config.set_id
                {
                    return Err(PublishError::NotPublisher);
                }
                let owner = next.root.owner_group;
                self.next = Some(next);
                owner
            }
            (Err(DistributionError::StaleRevision), _) if !request.admin() => {
                return Err(PublishError::Dropped);
            }
            // Activating an active set again answers, once authorized, with its head revision.
            (Err(DistributionError::NotPending), Some(head))
                if matches!(request, DistributionRequest::Activate)
                    && head.phase == SetPhase::Active =>
            {
                self.current = Some(head.revision());
                head.root.owner_group
            }
            (Err(error), _) => return Err(error.into()),
        };
        if !request.admin() {
            return self.sign();
        }
        let Some(auth_context) = self.config.auth_context.clone() else {
            return Err(PublishError::Unauthorized);
        };
        // Roles are read in the acceptance transaction, so a revoked role conflicts the commit.
        let check = CheckPermissionsConfig {
            auth_context,
            path: group_admin_path(self.config.realm_id, owner),
            required_permission: Permission::WRITE,
        };
        let reads = check.reads().map_err(|_| PublishError::Unauthorized)?;
        self.check = Some(check);
        self.call(PublishState::Authorize, IoCall::Read(reads))
    }

    /// Accepts the request once the admin check allowed it; an already applied one replays.
    fn authorized(&mut self, allowed: Result<bool, AuthorizationError>) -> PublishNext {
        match allowed {
            Ok(true) => match self.current {
                Some(revision) => Ok(Next::Done(revision)),
                None => self.sign(),
            },
            Ok(false) => Err(PublishError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(_) => Err(PublishError::Unauthorized),
        }
    }

    fn sign(&mut self) -> PublishNext {
        let snapshot = self.next.clone().ok_or(PublishError::NotFinished)?;
        let claim = SnapshotClaim {
            realm_id: self.config.realm_id,
            snapshot,
        };
        let sign = Effect::Net(NetEffect::SnapshotSign(Box::new(claim)));
        self.call(PublishState::Sign, IoCall::Effect(Box::new(sign)))
    }

    /// The head, the reducer state, the accepted request and the outbox row, in one batch.
    fn write(&mut self, signed: SignedSnapshot) -> PublishNext {
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
        let accepted = AcceptedRequest {
            revision,
            fingerprint: self.fingerprint(),
            requester: self.requester(),
        };
        let accepted = postcard::to_allocvec(&accepted).map_err(ConversionError::from)?;
        let catch_up = view.snapshot().bootstrap_open();
        let mut writes = vec![
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
            (
                DISTRIBUTION_PUBLISHER_KEYSPACE.to_string(),
                publisher_key(&self.config.node_id, set_id).into(),
                postcard::to_allocvec(&revision)
                    .map_err(ConversionError::from)?
                    .into(),
            ),
        ];
        // A marker carries its revision, so a pass that settled an older one keeps it.
        let tag = revision.number.to_be_bytes().to_vec();
        // The publisher may coordinate ranges too; it finds the members it walks like any node.
        if catch_up {
            let key = set_id.to_bytes().to_vec().into();
            writes.push((CATCH_UP_KEYSPACE.to_string(), key, tag.clone().into()));
        }
        // Nodes find the sets whose transfers, deletion or departures they act on through it.
        if view.snapshot().lifecycle_open() {
            let key = set_id.to_bytes().to_vec().into();
            writes.push((DISTRIBUTION_LIFECYCLE_KEYSPACE.to_string(), key, tag.into()));
        }
        // This node's replica share may shrink with the record: its quota gate stops deducting it.
        let share = dirty_share_key(view.snapshot().config.owner_group).into();
        let generation = Ulid::generate().to_bytes().to_vec().into();
        writes.push((NODE_STATS_KEYSPACE.to_string(), share, generation));
        self.revision = Some(revision);
        self.call(PublishState::Write, IoCall::Write(writes))
    }
}

impl Operation for PublishOperation {
    type Output = DistributionRevision;
    type Error = PublishError;

    fn first(&mut self) -> PublishNext {
        Ok(Next::Call(self.read_state()))
    }

    fn next(&mut self, result: IoResult) -> PublishNext {
        match self.state {
            PublishState::ReadState => self.decide(result),
            PublishState::Authorize => {
                let values = result.values()?;
                let allowed = match self.check.take() {
                    Some(check) => check.allowed(values),
                    None => Err(AuthorizationError::NotFinished),
                };
                self.authorized(allowed)
            }
            PublishState::Sign => match result.event()? {
                Event::Net(NetEvent::SnapshotSign(SnapshotSignEvent::Signed(signed))) => {
                    self.write(*signed)
                }
                Event::Net(NetEvent::SnapshotSign(SnapshotSignEvent::Unavailable(reason))) => {
                    Err(PublishError::SignUnavailable(reason))
                }
                other => Err(PublishError::UnexpectedEvent(other.label())),
            },
            // A missed drain timer leaves the durable outbox row for the next drain.
            PublishState::Write => {
                done(result)?;
                let output = self.revision.take().ok_or(PublishError::NotFinished)?;
                Ok(Next::Then {
                    output,
                    calls: vec![IoCall::Effect(Box::new(schedule_drain_effect()))],
                })
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
