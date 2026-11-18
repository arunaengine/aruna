//! Seeds and refreshes device replicas by joining a holder's graph snapshot as an OR-Set.
//! Unpublished local edits survive, and a repeated refresh changes nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::placement::read_holder_sets;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;
use rand::seq::SliceRandom;
use tracing::debug;
use ulid::Ulid;

use super::install_graph::{InstallError, InstallGraphOperation};
use super::replica::{ReplicaRecord, ReplicaScanError, ReplicaState};
use super::replica_store::{list_replicas, read_replica, store_replica};
use crate::document_repository::{config_of, realm_config};
use aruna_core::structs::identity::realm::NodeRealm;
use crate::forward::transport::{peer_call_within, timed_answer};
use crate::driver::DriverContext;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::protocol::{GraphState, MetadataTransportMessage};
use crate::metadata::transport_message_kind;
use crate::realm::mutate_placement::node_kind;

/// Replicas one pass refreshes. The rest follow on the next beat, so a large
/// working set converges instead of blocking one pass.
const MAX_REFRESH_PASS: usize = 32;

/// Bounds one document's exchange with the realm.
const REFRESH_BUDGET: Duration = Duration::from_secs(10);

/// Who this device is and whom it may ask.
#[derive(Clone, Debug, PartialEq)]
struct RefreshPlan {
    realm_id: RealmId,
    owner: UserId,
    config: RealmConfigDocument,
}

impl RefreshPlan {
    fn of(ids: NodeRealm, config: Option<RealmConfigDocument>) -> Option<Self> {
        let (node_id, realm_id) = ids?;
        let config = config?;
        let owner = node_kind(&config, node_id).and_then(|kind| kind.owner())?;
        Some(Self {
            realm_id,
            owner,
            config,
        })
    }
}

/// What one refresh achieved. Silence is not a refusal: the replica keeps
/// serving what it already holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshOutcome {
    Installed,
    Refused(SyncRefusal),
    Unreachable,
}

/// What the owner is told about a refusal. The variant alone is the message;
/// an invalid one carries the holder's own words.
pub fn refusal_reason(refusal: &SyncRefusal) -> String {
    match refusal {
        SyncRefusal::Unauthorized => "the realm did not accept this device".to_string(),
        SyncRefusal::Forbidden => "the owner may not read this document".to_string(),
        SyncRefusal::NotFound => "the realm does not know this document".to_string(),
        SyncRefusal::Invalid(reason) => reason.clone(),
        SyncRefusal::Unavailable => "no holder could serve this document".to_string(),
        SyncRefusal::Coordinator(node) => format!("node {node} coordinates this key"),
    }
}

/// The document's holders, in a different order every time so one lagging
/// holder never owns this device's view.
fn holders_for(plan: &RefreshPlan, document_id: Ulid) -> Vec<NodeId> {
    let Ok(placement) = resolve_metadata_id(&plan.config, plan.realm_id, None, document_id) else {
        return Vec::new();
    };
    let mut holders = read_holder_sets(&plan.config, &placement).unwrap_or_default();
    holders.shuffle(&mut rand::rng());
    holders
}

/// What the pending call of a planned refresh answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlannedStep {
    Holder,
    Install,
    Failed,
}

/// Asks the document's holders in turn until one answers, then joins its graph into the replica.
/// An authoritative refusal is what the owner has to see; silence only means the realm is out of
/// reach and the replica keeps serving.
#[derive(Debug, PartialEq)]
struct PlannedRefreshOperation {
    plan: RefreshPlan,
    replica: ReplicaRecord,
    holders: VecDeque<NodeId>,
    budget: Duration,
    step: PlannedStep,
    refusal: Option<SyncRefusal>,
}

type Refreshing = Result<Next<RefreshOutcome>, IoError>;

impl PlannedRefreshOperation {
    fn new(plan: RefreshPlan, replica: ReplicaRecord) -> Self {
        let holders = holders_for(&plan, replica.document_id).into();
        Self {
            plan,
            replica,
            holders,
            budget: REFRESH_BUDGET,
            step: PlannedStep::Holder,
            refusal: None,
        }
    }

    fn ask_next(&mut self) -> Refreshing {
        let document_id = self.replica.document_id;
        while !self.budget.is_zero() {
            let Some(holder) = self.holders.pop_front() else {
                break;
            };
            let auth = AuthContext {
                user_id: self.plan.owner,
                realm_id: self.plan.realm_id,
                path_restrictions: None,
                session: None,
            };
            let message = MetadataTransportMessage::FetchGraphState {
                auth_token: AuthToken::internal(auth),
                document_id,
            };
            match peer_call_within(holder, &message, self.budget) {
                Ok(call) => return Ok(Next::Call(call)),
                Err(error) => {
                    debug!(%document_id, %error, "Could not reach a holder for the graph state");
                }
            }
        }
        Ok(Next::Done(RefreshOutcome::Unreachable))
    }

    fn install(&mut self, state: GraphState) -> Refreshing {
        let replica = self.replica.clone();
        let install = InstallGraphOperation::new(replica, state, unix_timestamp_millis());
        self.step = PlannedStep::Install;
        Ok(Next::Call(IoCall::child(install)))
    }

    fn refused(&mut self, refusal: SyncRefusal) -> Refreshing {
        self.replica.state = ReplicaState::Failed;
        self.replica.last_error = Some(refusal_reason(&refusal));
        self.refusal = Some(refusal);
        self.step = PlannedStep::Failed;
        Ok(Next::Call(IoCall::child(store_replica(&self.replica))))
    }

    fn answered(&mut self, result: IoResult) -> Refreshing {
        let document_id = self.replica.document_id;
        let (answer, elapsed) = timed_answer(result)?;
        self.budget = self.budget.saturating_sub(elapsed);
        match answer {
            Ok(MetadataTransportMessage::FetchedGraphState { result: Ok(state) }) => {
                return self.install(*state);
            }
            Ok(MetadataTransportMessage::FetchedGraphState {
                result: Err(refusal),
            }) => {
                // An authorization verdict is the same at every holder.
                if matches!(refusal, SyncRefusal::Unauthorized | SyncRefusal::Forbidden) {
                    return self.refused(refusal);
                }
                debug!(%document_id, refusal = ?refusal, "A holder refused the graph state");
            }
            Ok(other) => {
                let message = transport_message_kind(&other);
                debug!(%document_id, %message, "Unexpected answer to a graph state fetch");
            }
            Err(error) => {
                debug!(%document_id, %error, "Could not reach a holder for the graph state");
            }
        }
        self.ask_next()
    }
}

impl Operation for PlannedRefreshOperation {
    type Output = RefreshOutcome;
    type Error = IoError;

    fn first(&mut self) -> Refreshing {
        self.ask_next()
    }

    fn next(&mut self, result: IoResult) -> Refreshing {
        match self.step {
            PlannedStep::Holder => self.answered(result),
            PlannedStep::Install => Ok(Next::Done(match result.child::<bool, InstallError>()? {
                Ok(true) => RefreshOutcome::Installed,
                _ => RefreshOutcome::Unreachable,
            })),
            // The refusal stands even when it could not be noted on the replica.
            PlannedStep::Failed => {
                result.child::<(), IoError>()?.ok();
                let refusal = self.refusal.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(RefreshOutcome::Refused(refusal)))
            }
        }
    }
}

/// What the pending call of a replica refresh answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OneStep {
    Config,
    Replica,
    Refresh,
}

/// Refreshes one replica now.
#[derive(Debug, PartialEq)]
pub struct RefreshReplicaOperation {
    ids: NodeRealm,
    document_id: Ulid,
    step: OneStep,
    plan: Option<RefreshPlan>,
}

impl RefreshReplicaOperation {
    pub(crate) fn new(ids: NodeRealm, document_id: Ulid) -> Self {
        Self {
            ids,
            document_id,
            step: OneStep::Config,
            plan: None,
        }
    }
}

impl Operation for RefreshReplicaOperation {
    type Output = RefreshOutcome;
    type Error = IoError;

    fn first(&mut self) -> Refreshing {
        match self.ids {
            Some((_, realm_id)) => Ok(Next::Call(realm_config(realm_id))),
            None => Ok(Next::Done(RefreshOutcome::Unreachable)),
        }
    }

    fn next(&mut self, result: IoResult) -> Refreshing {
        match self.step {
            OneStep::Config => {
                self.plan = RefreshPlan::of(self.ids, config_of(result)?);
                if self.plan.is_none() {
                    return Ok(Next::Done(RefreshOutcome::Unreachable));
                }
                self.step = OneStep::Replica;
                Ok(Next::Call(IoCall::child(read_replica(self.document_id))))
            }
            OneStep::Replica => {
                let Some(replica) = result.child::<Option<ReplicaRecord>, IoError>()?? else {
                    return Ok(Next::Done(RefreshOutcome::Unreachable));
                };
                let plan = self.plan.take().ok_or(IoError::Unfinished)?;
                self.step = OneStep::Refresh;
                let refresh = PlannedRefreshOperation::new(plan, replica);
                Ok(Next::Call(IoCall::child(refresh)))
            }
            OneStep::Refresh => Ok(Next::Done(result.child::<RefreshOutcome, IoError>()??)),
        }
    }
}

/// What the pending call of a refresh pass answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PassStep {
    Config,
    List,
    Refresh,
}

/// Refreshes the least recently synced selected replicas, and answers how many of them a holder
/// served.
#[derive(Debug, PartialEq)]
pub struct RefreshReplicasOperation {
    ids: NodeRealm,
    step: PassStep,
    plan: Option<RefreshPlan>,
    replicas: VecDeque<ReplicaRecord>,
    refreshed: usize,
}

/// One refresh pass of this device; see [`RefreshReplicasOperation`].
pub fn refresh_replicas(context: &DriverContext) -> RefreshReplicasOperation {
    RefreshReplicasOperation {
        ids: context.node_and_realm(),
        step: PassStep::Config,
        plan: None,
        replicas: VecDeque::new(),
        refreshed: 0,
    }
}

impl RefreshReplicasOperation {
    fn next_replica(&mut self) -> Result<Next<usize>, IoError> {
        let Some(replica) = self.replicas.pop_front() else {
            return Ok(Next::Done(self.refreshed));
        };
        let plan = self.plan.clone().ok_or(IoError::Unfinished)?;
        self.step = PassStep::Refresh;
        let refresh = PlannedRefreshOperation::new(plan, replica);
        Ok(Next::Call(IoCall::child(refresh)))
    }
}

impl Operation for RefreshReplicasOperation {
    type Output = usize;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<usize>, IoError> {
        match self.ids {
            Some((_, realm_id)) => Ok(Next::Call(realm_config(realm_id))),
            None => Ok(Next::Done(0)),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, IoError> {
        match self.step {
            PassStep::Config => {
                self.plan = RefreshPlan::of(self.ids, config_of(result)?);
                if self.plan.is_none() {
                    return Ok(Next::Done(0));
                }
                self.step = PassStep::List;
                Ok(Next::Call(IoCall::child(list_replicas())))
            }
            PassStep::List => {
                let Ok(mut replicas) = result.child::<Vec<ReplicaRecord>, ReplicaScanError>()?
                else {
                    return Ok(Next::Done(0));
                };
                replicas.retain(|replica| replica.selected);
                replicas.sort_by_key(|replica| replica.last_synced_ms.unwrap_or(0));
                replicas.truncate(MAX_REFRESH_PASS);
                self.replicas = replicas.into();
                self.next_replica()
            }
            PassStep::Refresh => {
                if result.child::<RefreshOutcome, IoError>()?? == RefreshOutcome::Installed {
                    self.refreshed += 1;
                }
                self.next_replica()
            }
        }
    }
}

#[cfg(test)]
#[path = "refresh_tests.rs"]
mod tests;
