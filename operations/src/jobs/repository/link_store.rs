//! Reads stored repository links and applies one link change on the link's owner node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{LINK_QUEUE_KEYSPACE, LINK_SECRET_KEYSPACE, REPOSITORY_LINK_KEYSPACE};
use aruna_core::repository::{
    LinkStatus, RepositoryCredential, RepositoryLink, RepositoryRecord, link_key, link_prefix,
};
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use ulid::Ulid;

use super::links::{ChangeLinkOperation, LinkChange, LinkError, id_key};
use crate::driver::{DriverContext, drive};
use crate::metadata::persistent_id::mapping_route;

const LINK_PAGE: usize = 256;

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
        Box::pin(ensure_lineage(&context.storage_handle, enabled)).await?;
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
    storage: &StorageHandle,
    link: &RepositoryLink,
) -> Result<(), LinkError> {
    let pulls = link.pull().is_some();
    let conflict = list_links(storage, link.document_id)
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

async fn send(storage: &StorageHandle, effect: StorageEffect) -> Result<Event, LinkError> {
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Ok(event),
    }
}

/// The document's links with whether each still has a push check queued.
pub async fn list_links(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<Vec<(RepositoryLink, bool)>, LinkError> {
    let links = document_links(storage, document_id).await?;
    let reads = links
        .iter()
        .map(|link| (LINK_QUEUE_KEYSPACE.to_string(), id_key(link.link_id)))
        .collect::<Vec<_>>();
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let event = send(
        storage,
        StorageEffect::BatchRead {
            reads,
            txn_id: None,
        },
    )
    .await?;
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        return Err(LinkError::Unexpected(event.label().to_string()));
    };
    Ok(links
        .into_iter()
        .zip(values)
        .map(|(link, (_, queued))| (link, queued.is_some()))
        .collect())
}

/// All links of the document, read page by page.
pub(crate) async fn document_links(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<Vec<RepositoryLink>, LinkError> {
    let mut links = Vec::new();
    let mut start = None;
    loop {
        let event = send(
            storage,
            StorageEffect::Iter {
                key_space: REPOSITORY_LINK_KEYSPACE.to_string(),
                prefix: Some(ByteView::from(link_prefix(document_id))),
                start: start.take().map(IterStart::After),
                limit: LINK_PAGE,
                txn_id: None,
            },
        )
        .await?;
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return Err(LinkError::Unexpected(event.label().to_string()));
        };
        for (_, value) in values {
            links.push(RepositoryLink::from_bytes(&value)?);
        }
        match next_start_after {
            Some(next) => start = Some(next),
            None => return Ok(links),
        }
    }
}

pub async fn read_link(
    storage: &StorageHandle,
    document_id: Ulid,
    link_id: Ulid,
) -> Result<Option<RepositoryLink>, LinkError> {
    let event = send(
        storage,
        StorageEffect::Read {
            key_space: REPOSITORY_LINK_KEYSPACE.to_string(),
            key: ByteView::from(link_key(document_id, link_id)),
            txn_id: None,
        },
    )
    .await?;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .map(|bytes| RepositoryLink::from_bytes(&bytes))
            .transpose()?),
        other => Err(LinkError::Unexpected(other.label().to_string())),
    }
}

pub(crate) async fn read_secret(
    storage: &StorageHandle,
    link_id: Ulid,
) -> Result<Option<RepositoryCredential>, LinkError> {
    let event = send(
        storage,
        StorageEffect::Read {
            key_space: LINK_SECRET_KEYSPACE.to_string(),
            key: id_key(link_id),
            txn_id: None,
        },
    )
    .await?;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .map(|bytes| postcard::from_bytes(&bytes).map_err(ConversionError::from))
            .transpose()?),
        other => Err(LinkError::Unexpected(other.label().to_string())),
    }
}
