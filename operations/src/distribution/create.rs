//! Creates a distributed bucket on this node, its publisher, and admits its members: the create
//! revision, one member creation per node under the caller's bearer token, signed admissions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The set activates once every coordinator and replica is admitted.

use std::collections::BTreeSet;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::distribution::capacity::RangeSize;
use aruna_core::structs::storage::distribution::presets::{
    Layout, add_edges, replica_layout, shard_layout,
};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionConfig, DistributionError, DistributionRequest, DistributionRevision,
    DistributionSettings, SetPhase, SetRoot, SignedSnapshot, SignedStatement,
};
use aruna_core::types::GroupId;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::forward::{PublishRefusal, admit_member, publish_local};
use super::status::{StatusError, read_view};
use crate::driver::DriverContext;
use crate::forward::authorize::is_sync_eligible;
use crate::metadata::protocol::{AuthToken, MetadataTransportMessage};
use crate::placement::process_placements::load_realm_config;

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

/// Creates the set with this node as its publisher, then admits every member it can.
pub async fn create_set(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    bearer: Option<AuthToken>,
    input: CreateInput,
) -> Result<CreationReport, CreateError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| CreateError::Invalid("this node has no realm transport".to_string()))?;
    let (realm_id, publisher) = (*net.realm_id(), net.node_id());
    let config = load_realm_config(context, realm_id).await.ok_or_else(|| {
        PublishRefusal::Unavailable("the realm configuration is not available".to_string())
    })?;
    let nodes = input
        .nodes
        .iter()
        .chain(input.edges.iter().map(|(node, _)| node));
    if let Some(node) = nodes
        .into_iter()
        .find(|node| !is_sync_eligible(&config, **node))
    {
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
    publish_local(
        context,
        set_id,
        Ulid::generate(),
        request,
        Some(auth.clone()),
    )
    .await?;
    // The set exists from here on, so every later failure still names it.
    // The ranges of a new set are empty.
    match admit_pending(context, auth, bearer, set_id, &[]).await {
        Ok(report) => Ok(report),
        Err(error) => Ok(CreationReport {
            set_id,
            active: false,
            members: Vec::new(),
            error: Some(error.to_string()),
        }),
    }
}

/// Admits every member the record has not admitted yet and activates the set once every
/// coordinator and replica is in. A failed member keeps the set pending; nothing is deleted.
/// Each member checks its node's capacity against the ranges' expected `sizes`.
pub async fn admit_pending(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    bearer: Option<AuthToken>,
    set_id: Ulid,
    sizes: &[RangeSize],
) -> Result<CreationReport, CreateError> {
    let local = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or_else(|| CreateError::Invalid("this node has no realm transport".to_string()))?;
    let view = read_view(context, set_id)
        .await?
        .ok_or(CreateError::UnknownSet(set_id))?;
    if view.conflicted() {
        return Err(CreateError::Conflicted);
    }
    let signed = view.head;
    let snapshot = &signed.snapshot;
    let publisher = snapshot.root.publisher;
    let mut admitted = BTreeSet::new();
    let mut members = Vec::with_capacity(snapshot.config.members.len());
    for member in &snapshot.config.members {
        let outcome = match member.incarnation {
            Some(_) => Ok(()),
            None => {
                let statement = match (member.node_id == local, bearer.clone()) {
                    (true, _) => {
                        admit_member(context, auth, &signed, (member.member_id, sizes)).await
                    }
                    (false, Some(token)) => {
                        let member = (member.node_id, member.member_id, sizes);
                        request_admission(context, member, token, &signed).await
                    }
                    (false, None) => Err(SyncRefusal::Unauthorized),
                };
                match statement {
                    Ok(statement) => submit(context, publisher, local, statement)
                        .await
                        .map_err(|refusal| refusal.to_string()),
                    Err(refusal) => Err(refusal_text(&refusal)),
                }
            }
        };
        if outcome.is_ok() {
            admitted.insert(member.member_id);
        }
        members.push(MemberOutcome {
            member_id: member.member_id,
            node_id: member.node_id,
            admitted: outcome.is_ok(),
            error: outcome.err(),
        });
    }
    let required = snapshot
        .config
        .ranges
        .iter()
        .flat_map(|range| std::iter::once(&range.coordinator).chain(&range.replicas));
    let mut active = snapshot.phase == SetPhase::Active;
    let mut error = None;
    if !active && required.into_iter().all(|member| admitted.contains(member)) {
        match activate(context, (publisher, local), set_id, auth, bearer).await {
            Ok(()) => active = true,
            Err(refusal) => error = Some(refusal.to_string()),
        }
    }
    Ok(CreationReport {
        set_id,
        active,
        members,
        error,
    })
}

/// Hands a member's statement to the publisher; one it already embedded counts as done.
pub(crate) async fn submit(
    context: &Arc<DriverContext>,
    publisher: NodeId,
    local: NodeId,
    statement: SignedStatement,
) -> Result<(), PublishRefusal> {
    let (set_id, id) = (statement.statement.set_id, statement.statement.statement_id);
    let statement = Box::new(statement);
    let result = match publisher == local {
        true => {
            publish_local(
                context,
                set_id,
                id,
                DistributionRequest::Statement(statement),
                None,
            )
            .await
        }
        false => {
            let message = MetadataTransportMessage::SubmitDistributionStatement { statement };
            publish_remote(context, publisher, message).await
        }
    };
    match result {
        Ok(_) | Err(PublishRefusal::Replayed) => Ok(()),
        Err(refusal) => Err(refusal),
    }
}

/// Activation is an admin request: the publisher checks the caller's group admin WRITE again.
async fn activate(
    context: &Arc<DriverContext>,
    nodes: (NodeId, NodeId),
    set_id: Ulid,
    auth: &AuthContext,
    bearer: Option<AuthToken>,
) -> Result<(), PublishRefusal> {
    let request = DistributionRequest::Activate;
    send_admin(context, nodes, set_id, (auth, bearer), request)
        .await
        .map(|_| ())
}

/// Sends an admin request to the set's publisher: accepted here when this node publishes,
/// otherwise forwarded with the caller's bearer token, which the publisher checks again.
pub(crate) async fn send_admin(
    context: &Arc<DriverContext>,
    (publisher, local): (NodeId, NodeId),
    set_id: Ulid,
    (auth, bearer): (&AuthContext, Option<AuthToken>),
    request: DistributionRequest,
) -> Result<DistributionRevision, PublishRefusal> {
    let request_id = Ulid::generate();
    if publisher == local {
        return publish_local(context, set_id, request_id, request, Some(auth.clone())).await;
    }
    let auth_token = bearer.ok_or(PublishRefusal::Unauthorized)?;
    let message = MetadataTransportMessage::ForwardDistributionRequest {
        auth_token,
        set_id,
        request_id,
        request: Box::new(request),
    };
    publish_remote(context, publisher, message).await
}

/// Sends one request to the set's publisher; an unreachable publisher means configuration
/// changes are unavailable, never a queued request.
pub(crate) async fn publish_remote(
    context: &Arc<DriverContext>,
    publisher: NodeId,
    message: MetadataTransportMessage,
) -> Result<DistributionRevision, PublishRefusal> {
    let unavailable = |reason: String| PublishRefusal::Unavailable(reason);
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| unavailable("no transport to the publisher".to_string()))?;
    match metadata.request_forwarded_write(publisher, message).await {
        Ok(MetadataTransportMessage::DistributionAccepted { result }) => result,
        Ok(other) => Err(unavailable(format!(
            "the publisher answered {}",
            crate::metadata::transport_message_kind(&other)
        ))),
        Err(error) => {
            warn!(%publisher, %error, "The distributed bucket publisher is unreachable");
            Err(unavailable(format!(
                "publisher node {publisher} is unreachable"
            )))
        }
    }
}

/// Asks a member's node to create and admit its local bucket under the caller's bearer token.
pub(crate) async fn request_admission(
    context: &Arc<DriverContext>,
    (node_id, member_id, sizes): (NodeId, Ulid, &[RangeSize]),
    auth_token: AuthToken,
    snapshot: &SignedSnapshot,
) -> Result<SignedStatement, SyncRefusal> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let message = MetadataTransportMessage::ForwardAdmitMember {
        auth_token,
        snapshot: Box::new(snapshot.clone()),
        member_id,
        sizes: sizes.to_vec(),
    };
    match metadata.request_forwarded_write(node_id, message).await {
        Ok(MetadataTransportMessage::AdmittedMember { result }) => result.map(|signed| *signed),
        Ok(_) => Err(SyncRefusal::Invalid(
            "unexpected admission answer".to_string(),
        )),
        Err(error) => {
            warn!(%node_id, %error, "A distributed bucket member node is unreachable");
            Err(SyncRefusal::Unavailable)
        }
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
