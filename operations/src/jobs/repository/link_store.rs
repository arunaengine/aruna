//! Reads stored repository links and applies one link change on the link's owner node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::keyspaces::{LINK_SECRET_KEYSPACE, REPOSITORY_LINK_KEYSPACE};
use aruna_core::repository::{
    LinkStatus, RepositoryCredential, RepositoryLink, RepositoryRecord, link_key,
};
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use ulid::Ulid;

use super::link_reads::{ListLinksOperation, StoredRowOperation};
use super::links::{ChangeLinkOperation, LinkChange, LinkError, id_key};
use crate::driver::{DriverContext, drive};
use crate::metadata::persistent_id::mapping_route;

/// Applies a change on the link's owner node and replicates it to the document's holders.
pub async fn change_link(
    context: &DriverContext,
    link: &RepositoryLink,
    change: LinkChange,
) -> Result<Option<RepositoryLink>, LinkError> {
    let route = match context.net_handle.as_ref() {
        Some(net) if net.node_id() != link.owner_node => {
            return Err(LinkError::NotOwner(link.owner_node_url.clone()));
        }
        Some(net) => mapping_route(context, *net.realm_id(), link.document_id)
            .await
            .map_err(|error| LinkError::Unexpected(error.to_string()))?,
        None => None,
    };
    // These changes can end a failure and enable the link again.
    let revives = matches!(link.status, LinkStatus::Failed { .. })
        && matches!(
            change,
            LinkChange::Rotate(_) | LinkChange::Begin(_) | LinkChange::Accept(_)
        );
    let enabled = match &change {
        LinkChange::Create { link, .. } => Some(link.as_ref()),
        LinkChange::Patch(patch) if patch.paused == Some(false) => Some(link),
        _ if revives => Some(link),
        _ => None,
    };
    if let Some(enabled) = enabled {
        Box::pin(ensure_lineage(context, enabled)).await?;
    }
    if let LinkChange::Accept(state) = &change
        && let Some(record) = state.latest.as_ref().filter(|record| record.published)
    {
        register_accepted(context, link, record).await?;
    }
    let operation =
        ChangeLinkOperation::new(link.document_id, link.link_id, change, SystemTime::now())
            .routed(route);
    drive(operation, context).await
}

/// Queues a published record that the link adopts outside a push, such as after a community
/// review, as `Published` identifiers of the dataset.
async fn register_accepted(
    context: &DriverContext,
    link: &RepositoryLink,
    record: &RepositoryRecord,
) -> Result<(), LinkError> {
    let identifiers = record.identifiers.clone();
    if identifiers.is_empty() {
        return Ok(());
    }
    let owner = context
        .net_handle
        .as_ref()
        .map_or(link.owner_node, |net| net.node_id());
    let spec = RegisterIdentifiersSpec {
        document_id: link.document_id,
        identifiers,
        auth_context: super::push::creator_auth(context, link),
    };
    let key = format!("identifiers/{}/{}", link.link_id, record.id);
    crate::jobs::service::submit_identifiers(context, spec, owner, key)
        .await
        .map(|_| ())
        .map_err(|error| LinkError::Submit(error.to_string()))
}

/// One lineage cannot have an enabled push link and an enabled pull link on one dataset.
pub(super) async fn ensure_lineage(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<(), LinkError> {
    let pulls = link.pull().is_some();
    let conflict = list_links(context, link.document_id)
        .await?
        .into_iter()
        .any(|(other, _)| {
            other.link_id != link.link_id
                && other.status == LinkStatus::Enabled
                && other.pull().is_some() != pulls
                && other.same_lineage(link)
        });
    if conflict {
        return Err(LinkError::Lineage);
    }
    Ok(())
}

/// The document's links with whether each still has a push check queued.
pub async fn list_links(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Vec<(RepositoryLink, bool)>, LinkError> {
    drive(ListLinksOperation::new(document_id), context).await
}

pub async fn read_link(
    context: &DriverContext,
    document_id: Ulid,
    link_id: Ulid,
) -> Result<Option<RepositoryLink>, LinkError> {
    let key = link_key(document_id, link_id);
    drive(
        StoredRowOperation::new(REPOSITORY_LINK_KEYSPACE, key),
        context,
    )
    .await
}

pub(crate) async fn read_secret(
    context: &DriverContext,
    link_id: Ulid,
) -> Result<Option<RepositoryCredential>, LinkError> {
    drive(
        StoredRowOperation::new(LINK_SECRET_KEYSPACE, id_key(link_id)),
        context,
    )
    .await
}
