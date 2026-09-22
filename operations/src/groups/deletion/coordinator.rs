//! Runs a resumable group deletion decision across the current realm members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::group_delete::{
    GroupDeleteAction, GroupDeleteCertificate, GroupDeletePhase, GroupDeletePlan, GroupDeleteProof,
    GroupDeleteRecord, GroupDeletionError,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::GroupId;
use aruna_core::{NodeId, admin_documents::AdminDocumentEvent};
use futures_util::{StreamExt, stream};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::forward::authorize::{ForwardAuthError, authorize_write};
use crate::metadata::protocol::{AuthToken, MetadataTransportMessage};
use crate::placement::process_placements::load_realm_config;

use super::commit::CommitOperation;
use super::prepare::PrepareOperation;

pub(super) fn coordinator(config: &RealmConfigDocument) -> Result<NodeId, GroupDeletionError> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .map(|node| node.node_id.parse::<NodeId>())
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))?
        .first()
        .copied()
        .ok_or_else(|| GroupDeletionError::Unavailable("realm has no group coordinator".into()))
}

async fn load_group(
    context: &DriverContext,
    group_id: GroupId,
) -> Result<(Option<Group>, Option<GroupDeleteRecord>), GroupDeletionError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads: vec![
                (GROUP_KEYSPACE.to_string(), group_id.to_bytes().into()),
                (
                    GROUP_DELETE_KEYSPACE.to_string(),
                    group_id.to_bytes().into(),
                ),
            ],
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            let [(_, group), (_, record)] = values.as_slice() else {
                return Err(GroupDeletionError::Unavailable("invalid group read".into()));
            };
            Ok((
                group.as_deref().map(Group::from_bytes).transpose()?,
                record
                    .as_deref()
                    .map(GroupDeleteRecord::from_bytes)
                    .transpose()?,
            ))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(GroupDeletionError::Unavailable("invalid group read".into())),
    }
}

async fn authorize(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    group_id: GroupId,
    record: Option<&GroupDeleteRecord>,
    exists: bool,
) -> Result<(), GroupDeletionError> {
    if auth.user_id.is_nil() || auth.path_restrictions.is_some() {
        return Err(GroupDeletionError::Unauthorized);
    }
    if record.is_some_and(|record| {
        record.phase == GroupDeletePhase::Deleted
            && record.plan.realm_id == auth.realm_id
            && (record.plan.owner == auth.user_id
                || record.plan.requested_by == auth.user_id
                || record.deleted_by == Some(auth.user_id))
    }) {
        return Ok(());
    }
    let mut paths = vec![format!("/{}/admin/groups", auth.realm_id)];
    if exists {
        paths.push(format!("/{}/g/{group_id}/admin", auth.realm_id));
    }
    for path in paths {
        match authorize_write(context, auth.clone(), path).await {
            Ok(()) => return Ok(()),
            Err(ForwardAuthError::Unavailable(reason)) => {
                return Err(GroupDeletionError::Unavailable(reason));
            }
            Err(ForwardAuthError::Forbidden | ForwardAuthError::Unauthorized) => {}
        }
    }
    Err(GroupDeletionError::Unauthorized)
}

pub async fn delete_group(
    context: &Arc<DriverContext>,
    auth: AuthContext,
    token: Option<AuthToken>,
    group_id: GroupId,
) -> Result<(), GroupDeletionError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| GroupDeletionError::Unavailable("network handle is missing".into()))?;
    if auth.realm_id != *net.realm_id() {
        return Err(GroupDeletionError::Unauthorized);
    }
    let config = load_realm_config(context, auth.realm_id)
        .await
        .ok_or_else(|| GroupDeletionError::Unavailable("realm configuration is missing".into()))?;
    let (group, record) = load_group(context, group_id).await?;
    authorize(context, &auth, group_id, record.as_ref(), group.is_some()).await?;
    let target = match record
        .as_ref()
        .filter(|record| record.phase != GroupDeletePhase::Cancelled)
    {
        Some(record) => record.plan.coordinator,
        None => coordinator(&config)?,
    };
    if target != net.node_id() {
        request_node(
            context,
            target,
            token,
            GroupDeleteAction::Start { group_id },
        )
        .await?;
        return Ok(());
    }
    if let Some(record) = record
        .as_ref()
        .filter(|record| record.phase == GroupDeletePhase::Deleted)
    {
        let event = record.event.as_deref().ok_or_else(|| {
            GroupDeletionError::Unavailable("committed deletion event is missing".into())
        })?;
        return publish_decision(context, &record.plan, event).await;
    }
    let group = group.ok_or(GroupDeletionError::NotFound)?;
    if group.realm_id != auth.realm_id || group.group_id != group_id {
        return Err(GroupDeletionError::Unauthorized);
    }
    let nodes = config
        .nodes
        .iter()
        .map(|node| node.node_id.parse::<NodeId>())
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))?;
    let plan = record
        .as_ref()
        .filter(|record| record.phase != GroupDeletePhase::Cancelled)
        .map(|record| record.plan.clone())
        .unwrap_or_else(|| GroupDeletePlan {
            request_id: Ulid::generate(),
            group_id,
            realm_id: auth.realm_id,
            owner: group.owner,
            requested_by: auth.user_id,
            coordinator: net.node_id(),
            nodes,
        });
    if record
        .as_ref()
        .is_some_and(|record| record.phase == GroupDeletePhase::Aborting)
        || !plan.matches_config(&config)
        || plan.requested_by != auth.user_id
    {
        cancel_plan(context, &plan).await?;
        return Err(GroupDeletionError::Conflict(
            "previous attempt cancelled; retry deletion".into(),
        ));
    }
    if let Err(error) = drive(
        PrepareOperation::new(
            plan.clone(),
            GroupDeletePhase::Preparing,
            net.node_id(),
            SystemTime::now(),
        ),
        context.as_ref(),
    )
    .await
    {
        cancel_plan(context, &plan).await?;
        return Err(error);
    }
    let replies = stream::iter(plan.nodes.iter().copied().map(|node| {
        let plan = plan.clone();
        async move {
            (
                node,
                request_node(
                    context,
                    node,
                    None,
                    GroupDeleteAction::Prepare {
                        plan: Box::new(plan),
                    },
                )
                .await,
            )
        }
    }))
    .buffer_unordered(4)
    .collect::<Vec<_>>()
    .await;
    let mut proofs = Vec::new();
    let mut failure = None;
    for (node, reply) in replies {
        match reply {
            Ok(Some(proof)) if proof.node_id == node => proofs.push(proof),
            Ok(_) => {
                failure = Some(GroupDeletionError::Unavailable(format!(
                    "node {node} did not confirm emptiness"
                )))
            }
            Err(GroupDeletionError::NotEmpty(reason)) => {
                failure = Some(GroupDeletionError::NotEmpty(format!(
                    "node {node}: {reason}"
                )))
            }
            Err(error) => failure = Some(error),
        }
    }
    if let Some(error) = failure {
        cancel_plan(context, &plan).await?;
        return Err(error);
    }
    if let Err(error) = authorize(context, &auth, group_id, None, true).await {
        cancel_plan(context, &plan).await?;
        return Err(error);
    }
    let actor = Actor {
        node_id: net.node_id(),
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    };
    let certificate = GroupDeleteCertificate {
        plan: plan.clone(),
        proofs,
    };
    if !certificate.verify() {
        cancel_plan(context, &plan).await?;
        return Err(GroupDeletionError::Unavailable(
            "a node supplied an invalid confirmation".into(),
        ));
    }
    let committed = drive(
        CommitOperation::new(certificate, actor, Some(auth), None),
        context.as_ref(),
    )
    .await;
    let event = match committed {
        Ok(event) => event,
        Err(
            error @ (GroupDeletionError::Unauthorized
            | GroupDeletionError::NotFound
            | GroupDeletionError::Invalid(_)
            | GroupDeletionError::Conflict(_)),
        ) => {
            cancel_plan(context, &plan).await?;
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    publish_decision(context, &plan, &event).await
}

async fn cancel_plan(
    context: &Arc<DriverContext>,
    plan: &GroupDeletePlan,
) -> Result<(), GroupDeletionError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| GroupDeletionError::Unavailable("network handle is missing".into()))?;
    drive(
        PrepareOperation::new(
            plan.clone(),
            GroupDeletePhase::Aborting,
            net.node_id(),
            SystemTime::now(),
        ),
        context.as_ref(),
    )
    .await?;
    let config = load_realm_config(context, plan.realm_id)
        .await
        .ok_or_else(|| GroupDeletionError::Unavailable("realm configuration is missing".into()))?;
    let mut failure = None;
    for node in &plan.nodes {
        if *node != net.node_id()
            && config
                .nodes
                .iter()
                .any(|member| member.node_id == node.to_string())
        {
            if let Err(error) = request_node(
                context,
                *node,
                None,
                GroupDeleteAction::Abort {
                    plan: Box::new(plan.clone()),
                },
            )
            .await
            {
                failure = Some(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    drive(
        PrepareOperation::new(
            plan.clone(),
            GroupDeletePhase::Cancelled,
            net.node_id(),
            SystemTime::now(),
        ),
        context.as_ref(),
    )
    .await
}

async fn publish_decision(
    context: &Arc<DriverContext>,
    plan: &GroupDeletePlan,
    event: &AdminDocumentEvent,
) -> Result<(), GroupDeletionError> {
    sync_decision(context.as_ref()).await?;
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| GroupDeletionError::Unavailable("network handle is missing".into()))?;
    let config = load_realm_config(context, plan.realm_id)
        .await
        .ok_or_else(|| GroupDeletionError::Unavailable("realm configuration is missing".into()))?;
    for node in &plan.nodes {
        if *node != net.node_id()
            && config
                .nodes
                .iter()
                .any(|member| member.node_id == node.to_string())
        {
            request_node(
                context,
                *node,
                None,
                GroupDeleteAction::Commit {
                    event: Box::new(event.clone()),
                },
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn sync_decision(context: &DriverContext) -> Result<(), GroupDeletionError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::SyncAll)
        .await
    {
        Event::Storage(StorageEvent::SyncAllFinished) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(GroupDeletionError::Unavailable(
            "deletion decision was not persisted".into(),
        )),
    }
}

pub(super) async fn request_node(
    context: &Arc<DriverContext>,
    node: NodeId,
    token: Option<AuthToken>,
    action: GroupDeleteAction,
) -> Result<Option<GroupDeleteProof>, GroupDeletionError> {
    if context
        .net_handle
        .as_ref()
        .is_some_and(|net| net.node_id() == node)
    {
        return super::peer::apply_control(context, node, action).await;
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| GroupDeletionError::Unavailable("metadata transport is missing".into()))?;
    match metadata
        .request_forwarded_write(
            node,
            MetadataTransportMessage::GroupDeletion {
                auth_token: token,
                action,
            },
        )
        .await
    {
        Ok(MetadataTransportMessage::GroupDeletionResult { result }) => result,
        Ok(_) => Err(GroupDeletionError::Unavailable(format!(
            "node {node} refused the deletion request"
        ))),
        Err(error) => Err(GroupDeletionError::Unavailable(format!(
            "node {node}: {error}"
        ))),
    }
}
