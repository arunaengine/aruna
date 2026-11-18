//! Creates a distributed bucket on this node, its publisher, and admits its members: the create
//! revision, one member creation per node under the caller's bearer token, signed admissions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The set activates once every coordinator and replica is admitted.

use std::collections::BTreeSet;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::distribution::capacity::{RangeSize, with_unopened};
use aruna_core::structs::storage::distribution::presets::{
    Layout, add_edges, replica_layout, shard_layout,
};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionConfig, DistributionError, DistributionMember, DistributionRequest,
    DistributionRevision, DistributionSettings, DistributionSnapshot, DistributionView, SetPhase,
    SetRoot, SignedSnapshot, SignedStatement,
};
use aruna_core::types::GroupId;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::forward::{
    AdmitMemberOperation, MemberRefusal, PublishRefusal, config_of, realm_config,
};
use super::here::{Here, here};
use super::peer_call::{peer_answer, peer_call};
use super::status_view::{ReadViewOperation, StatusError};
use super::submit::{PublishLocalOperation, SendAdminOperation, SubmitOperation};
use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::peer_trust::is_sync_eligible;
use aruna_core::metadata::AuthToken;

/// How often and how far apart admission rereads a record that lags the accepted revision.
const RECORD_POLLS: u32 = 50;
const RECORD_POLL: Duration = Duration::from_millis(200);

/// How the key space is laid out over the chosen nodes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LayoutKind {
    /// One range; the first node coordinates and every other node keeps a full copy.
    Replica,
    /// Ranges split at the boundaries, coordinators in turn, each with further copies.
    Sharding {
        boundaries: Vec<String>,
        replicas: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateInput {
    pub bucket: String,
    pub owner_group: GroupId,
    /// Nodes that coordinate or replicate ranges, in layout order.
    pub nodes: Vec<NodeId>,
    pub layout: LayoutKind,
    /// Members that hold every record and keep bytes by their strategy, never coordinating.
    pub edges: Vec<(NodeId, DataStrategy)>,
    pub settings: DistributionSettings,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateError {
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Distribution(#[from] DistributionError),
    #[error(transparent)]
    Publish(#[from] PublishRefusal),
    #[error("distributed bucket {0} is not known on this node yet")]
    UnknownSet(Ulid),
    #[error("the publisher signed conflicting records; configuration changes stopped")]
    Conflicted,
    #[error(transparent)]
    Status(#[from] StatusError),
    /// An operation that needs an unreachable coordinator waits until it is reachable again.
    #[error("{0}")]
    Blocked(String),
}

impl From<IoError> for CreateError {
    fn from(error: IoError) -> Self {
        Self::Status(error.into())
    }
}

fn no_transport() -> CreateError {
    CreateError::Invalid("this node has no realm transport".to_string())
}

/// One member after an admission attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberOutcome {
    pub member_id: Ulid,
    pub node_id: NodeId,
    pub admitted: bool,
    /// Why this attempt failed; the set stays pending and the admission can be retried.
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreationReport {
    pub set_id: Ulid,
    pub active: bool,
    pub members: Vec<MemberOutcome>,
    /// Why admission or activation stopped after the set was committed; it can be retried.
    pub error: Option<String>,
}

/// Plans the members and ranges of a new set; every member gets a fresh id.
pub fn plan_layout(set_id: Ulid, input: &CreateInput) -> Result<Layout, DistributionError> {
    let assign = |nodes: &[NodeId]| -> Vec<(Ulid, NodeId)> {
        nodes.iter().map(|node| (Ulid::generate(), *node)).collect()
    };
    let nodes = assign(&input.nodes);
    let mut layout = match &input.layout {
        LayoutKind::Replica => replica_layout(set_id, &nodes)?,
        // Each range has one coordinator, and every sharding member must coordinate one.
        LayoutKind::Sharding { boundaries, .. } if boundaries.len() + 1 < nodes.len() => {
            return Err(DistributionError::Malformed(
                "every member needs a range: give one boundary per member after the first",
            ));
        }
        LayoutKind::Sharding {
            boundaries,
            replicas,
        } => shard_layout(set_id, &nodes, boundaries, *replicas)?,
    };
    for (node, strategy) in &input.edges {
        add_edges(&mut layout, &assign(&[*node]), *strategy)?;
    }
    Ok(layout)
}

/// What the pending call of a set creation answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateStep {
    Config,
    Publish,
    Admit,
}

/// Creates the set with this node as its publisher, then admits every member it can.
#[derive(Debug, PartialEq)]
pub struct CreateSetOperation {
    here: Here,
    auth: AuthContext,
    bearer: Option<AuthToken>,
    input: Option<CreateInput>,
    set_id: Option<Ulid>,
    step: CreateStep,
}

/// The creation of a set on this node; see [`CreateSetOperation`].
pub fn create_set(
    context: &DriverContext,
    auth: &AuthContext,
    bearer: Option<AuthToken>,
    input: CreateInput,
) -> CreateSetOperation {
    CreateSetOperation {
        here: here(context),
        auth: auth.clone(),
        bearer,
        input: Some(input),
        set_id: None,
        step: CreateStep::Config,
    }
}

impl CreateSetOperation {
    /// Publishes the create revision once every chosen node may hold a member.
    fn publish(&mut self, config: Option<RealmConfigDocument>) -> Creating {
        let config = config.ok_or_else(|| {
            PublishRefusal::Unavailable("the realm configuration is not available".to_string())
        })?;
        let input = self.input.take().ok_or(IoError::Unfinished)?;
        let (publisher, _) = self.here.ids.ok_or_else(no_transport)?;
        let nodes = (input.nodes.iter()).chain(input.edges.iter().map(|(node, _)| node));
        if let Some(node) = nodes.into_iter().find(|n| !is_sync_eligible(&config, **n)) {
            return Err(CreateError::Invalid(format!(
                "node {node} is not a realm infrastructure node and cannot hold a member"
            )));
        }
        let root = SetRoot {
            publisher,
            owner_group: input.owner_group,
            nonce: rand::random(),
        };
        let set_id = root.set_id();
        let layout = plan_layout(set_id, &input)?;
        let request = DistributionRequest::Create {
            root,
            config: DistributionConfig {
                bucket: input.bucket,
                owner_group: input.owner_group,
                node_set: None,
                members: layout.members,
                ranges: layout.ranges,
                settings: input.settings,
            },
        };
        let ids = (set_id, Ulid::generate());
        let auth = Some(self.auth.clone());
        let publish = PublishLocalOperation::new(self.here.ids, ids, request, auth);
        self.set_id = Some(set_id);
        self.step = CreateStep::Publish;
        Ok(Next::Call(IoCall::child(publish)))
    }
}

type Creating = Result<Next<CreationReport>, CreateError>;

impl Operation for CreateSetOperation {
    type Output = CreationReport;
    type Error = CreateError;

    fn first(&mut self) -> Creating {
        let (_, realm_id) = self.here.ids.ok_or_else(no_transport)?;
        Ok(Next::Call(realm_config(realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Creating {
        let set_id = self.set_id;
        match self.step {
            CreateStep::Config => self.publish(config_of(result)?),
            CreateStep::Publish => {
                result.child::<DistributionRevision, PublishRefusal>()??;
                let set_id = set_id.ok_or(IoError::Unfinished)?;
                // The ranges of a new set are empty.
                let admit = AdmitPendingOperation::new(
                    self.here.clone(),
                    (self.auth.clone(), self.bearer.take()),
                    (set_id, None),
                    Vec::new(),
                );
                self.step = CreateStep::Admit;
                Ok(Next::Call(IoCall::child(admit)))
            }
            // The set exists from here on, so every later failure still names it.
            CreateStep::Admit => match result.child::<CreationReport, CreateError>()? {
                Ok(report) => Ok(Next::Done(report)),
                Err(error) => Ok(Next::Done(CreationReport {
                    set_id: set_id.ok_or(IoError::Unfinished)?,
                    active: false,
                    members: Vec::new(),
                    error: Some(error.to_string()),
                })),
            },
        }
    }
}

/// What the pending call of an admission round answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmitStep {
    View,
    Wait,
    Member,
    Submit,
    Activate,
}

/// Admits unadmitted members against `sizes`, where a range no admitted member holds is empty,
/// and activates the set once all are in; a failure keeps it pending. A member with a new copy
/// is admitted again, once this node's record reached `accepted`.
#[derive(Debug, PartialEq)]
pub struct AdmitPendingOperation {
    here: Here,
    auth: AuthContext,
    bearer: Option<AuthToken>,
    set_id: Ulid,
    accepted: Option<DistributionRevision>,
    sizes: Vec<RangeSize>,
    polls: u32,
    step: AdmitStep,
    signed: Option<SignedSnapshot>,
    next_member: usize,
    /// The node of the member whose admission was sent to it.
    asked: Option<NodeId>,
    admitted: BTreeSet<Ulid>,
    members: Vec<MemberOutcome>,
}

type Admitting = Result<Next<CreationReport>, CreateError>;

/// The admission of a set's pending members; see [`AdmitPendingOperation`].
pub fn admit_pending(
    context: &DriverContext,
    auth: &AuthContext,
    bearer: Option<AuthToken>,
    accepted: (Ulid, Option<DistributionRevision>),
    sizes: &[RangeSize],
) -> AdmitPendingOperation {
    let caller = (auth.clone(), bearer);
    AdmitPendingOperation::new(here(context), caller, accepted, sizes.to_vec())
}

impl AdmitPendingOperation {
    pub(super) fn new(
        here: Here,
        (auth, bearer): (AuthContext, Option<AuthToken>),
        (set_id, accepted): (Ulid, Option<DistributionRevision>),
        sizes: Vec<RangeSize>,
    ) -> Self {
        Self {
            here,
            auth,
            bearer,
            set_id,
            accepted,
            sizes,
            polls: 0,
            step: AdmitStep::View,
            signed: None,
            next_member: 0,
            asked: None,
            admitted: BTreeSet::new(),
            members: Vec::new(),
        }
    }

    fn local(&self) -> Result<NodeId, CreateError> {
        self.here
            .ids
            .map(|(local, _)| local)
            .ok_or_else(no_transport)
    }

    fn read_view(&mut self) -> Admitting {
        self.step = AdmitStep::View;
        let read = ReadViewOperation {
            set_id: self.set_id,
        };
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Goes on with this node's record once it reached the accepted revision, waiting a
    /// bounded time for it.
    fn viewed(&mut self, view: Option<DistributionView>) -> Admitting {
        match (view, &self.accepted) {
            (Some(view), None) => return self.start(view),
            (Some(view), Some(revision)) if reached(&view.head.snapshot, revision) => {
                return self.start(view);
            }
            (None, None) => return Err(CreateError::UnknownSet(self.set_id)),
            _ => {}
        }
        self.polls += 1;
        if self.polls < RECORD_POLLS {
            self.step = AdmitStep::Wait;
            return Ok(Next::Call(IoCall::Delay(RECORD_POLL)));
        }
        let number = self.accepted.as_ref().map(|r| r.number).unwrap_or_default();
        let set_id = self.set_id;
        Err(CreateError::Blocked(format!(
            "the publisher accepted the change as revision {number} of distributed bucket \
             {set_id}, but this node has not received it yet; retry the admission shortly"
        )))
    }

    fn start(&mut self, view: DistributionView) -> Admitting {
        if view.conflicted() {
            return Err(CreateError::Conflicted);
        }
        self.sizes = with_unopened(&view.head.snapshot, &self.sizes);
        self.signed = Some(view.head);
        self.admit_next()
    }

    fn snapshot(&self) -> Result<&DistributionSnapshot, IoError> {
        let signed = self.signed.as_ref().ok_or(IoError::Unfinished)?;
        Ok(&signed.snapshot)
    }

    /// The next member that needs an admission; members already in count as admitted.
    fn admit_next(&mut self) -> Admitting {
        let local = self.local()?;
        loop {
            let snapshot = self.snapshot()?;
            let Some(member) = snapshot.config.members.get(self.next_member).cloned() else {
                return self.finish();
            };
            let filling = (snapshot.lifecycle.fills.iter())
                .any(|fill| fill.member_id == member.member_id && !fill.admitted);
            if member.incarnation.is_some() && !filling {
                self.record(Ok(()))?;
                continue;
            }
            if member.node_id == local {
                return self.admit_here(&member);
            }
            let Some(auth_token) = self.bearer.clone() else {
                self.record(Err(refusal_text(&SyncRefusal::Unauthorized)))?;
                continue;
            };
            let signed = self.signed.as_ref().ok_or(IoError::Unfinished)?;
            let message = MetadataTransportMessage::ForwardAdmitMember {
                auth_token,
                snapshot: Box::new(signed.clone()),
                member_id: member.member_id,
                sizes: self.sizes.clone(),
            };
            match peer_call(member.node_id, &message) {
                Ok(call) => {
                    self.asked = Some(member.node_id);
                    self.step = AdmitStep::Member;
                    return Ok(Next::Call(call));
                }
                Err(error) => {
                    let node_id = member.node_id;
                    warn!(%node_id, %error, "A distributed bucket member node is unreachable");
                    self.record(Err(refusal_text(&SyncRefusal::Unavailable)))?;
                }
            }
        }
    }

    fn admit_here(&mut self, member: &DistributionMember) -> Admitting {
        let signed = self.signed.clone().ok_or(IoError::Unfinished)?;
        let admit = AdmitMemberOperation::new(
            (self.here.ids, self.here.capacity),
            self.auth.clone(),
            signed,
            member.member_id,
            self.sizes.clone(),
        );
        self.asked = None;
        self.step = AdmitStep::Member;
        Ok(Next::Call(IoCall::child(admit)))
    }

    /// The statement of the member just asked, from its node or from this one.
    fn statement(
        &mut self,
        result: IoResult,
    ) -> Result<Result<SignedStatement, SyncRefusal>, IoError> {
        let Some(node_id) = self.asked.take() else {
            let admitted = result.child::<SignedStatement, MemberRefusal>()?;
            return Ok(admitted.map_err(|refused| refused.0));
        };
        Ok(match peer_answer(result)? {
            Ok(MetadataTransportMessage::AdmittedMember { result }) => result.map(|signed| *signed),
            Ok(_) => Err(SyncRefusal::Invalid(
                "unexpected admission answer".to_string(),
            )),
            Err(error) => {
                warn!(%node_id, %error, "A distributed bucket member node is unreachable");
                Err(SyncRefusal::Unavailable)
            }
        })
    }

    /// Records how the current member's admission ended and moves past it.
    fn record(&mut self, outcome: Result<(), String>) -> Result<(), IoError> {
        let snapshot = self.snapshot()?;
        let member = (snapshot.config.members.get(self.next_member)).ok_or(IoError::Unfinished)?;
        let (member_id, node_id) = (member.member_id, member.node_id);
        if outcome.is_ok() {
            self.admitted.insert(member_id);
        }
        self.members.push(MemberOutcome {
            member_id,
            node_id,
            admitted: outcome.is_ok(),
            error: outcome.err(),
        });
        self.next_member += 1;
        Ok(())
    }

    /// Activation is an admin request: the publisher checks the caller's group admin WRITE
    /// again. It follows once every coordinator and replica is admitted.
    fn finish(&mut self) -> Admitting {
        let snapshot = self.snapshot()?;
        let required = (snapshot.config.ranges.iter())
            .flat_map(|range| std::iter::once(&range.coordinator).chain(&range.replicas));
        let all_in = required
            .into_iter()
            .all(|member| self.admitted.contains(member));
        if snapshot.phase == SetPhase::Active || !all_in {
            let active = snapshot.phase == SetPhase::Active;
            return Ok(self.report(active, None));
        }
        let publisher = snapshot.root.publisher;
        let activate = SendAdminOperation::new(
            (self.here.ids, publisher),
            self.set_id,
            (self.auth.clone(), self.bearer.clone()),
            DistributionRequest::Activate,
        );
        self.step = AdmitStep::Activate;
        Ok(Next::Call(IoCall::child(activate)))
    }

    fn report(&mut self, active: bool, error: Option<String>) -> Next<CreationReport> {
        Next::Done(CreationReport {
            set_id: self.set_id,
            active,
            members: std::mem::take(&mut self.members),
            error,
        })
    }
}

impl Operation for AdmitPendingOperation {
    type Output = CreationReport;
    type Error = CreateError;

    fn first(&mut self) -> Admitting {
        self.local()?;
        self.read_view()
    }

    fn next(&mut self, result: IoResult) -> Admitting {
        match self.step {
            AdmitStep::View => {
                let view = result.child::<Option<DistributionView>, StatusError>()??;
                self.viewed(view)
            }
            AdmitStep::Wait => self.read_view(),
            AdmitStep::Member => match self.statement(result)? {
                Ok(statement) => {
                    let publisher = self.snapshot()?.root.publisher;
                    let submit = SubmitOperation::new(self.here.ids, publisher, statement);
                    self.step = AdmitStep::Submit;
                    Ok(Next::Call(IoCall::child(submit)))
                }
                Err(refusal) => {
                    self.record(Err(refusal_text(&refusal)))?;
                    self.admit_next()
                }
            },
            AdmitStep::Submit => {
                let submitted = result.child::<(), PublishRefusal>()?;
                self.record(submitted.map_err(|refusal| refusal.to_string()))?;
                self.admit_next()
            }
            AdmitStep::Activate => Ok(
                match result.child::<DistributionRevision, PublishRefusal>()? {
                    Ok(_) => self.report(true, None),
                    Err(refusal) => self.report(false, Some(refusal.to_string())),
                },
            ),
        }
    }
}

/// Whether `head` is `revision` or a later revision.
fn reached(head: &DistributionSnapshot, revision: &DistributionRevision) -> bool {
    match head.number.cmp(&revision.number) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => head.revision() == *revision,
        std::cmp::Ordering::Greater => true,
    }
}

/// A member refusal in words, with the permission hint where the member's node denied it.
fn refusal_text(refusal: &SyncRefusal) -> String {
    match refusal {
        SyncRefusal::Unauthorized => "the member node did not accept the bearer token".to_string(),
        SyncRefusal::Forbidden => "the caller has no WRITE on the bucket path of the member node; \
             grant it with a node glob such as /{realm}/g/{group}/data/*/{bucket}/**"
            .to_string(),
        SyncRefusal::NotFound => "the member node does not know this member".to_string(),
        SyncRefusal::Invalid(reason) => reason.clone(),
        SyncRefusal::Unavailable => "the member node is unreachable or busy; retry".to_string(),
        SyncRefusal::Coordinator(node) => format!("node {node} coordinates this key"),
    }
}

#[cfg(test)]
#[path = "create_tests.rs"]
mod tests;
