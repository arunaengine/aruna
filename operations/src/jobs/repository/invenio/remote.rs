//! Reads and checks a link's record lineage in an Invenio repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::repository::invenio::validate_id;
use aruna_core::repository::{
    LinkFailure, LinkReview, LinkTarget, RemoteState, RepositoryDestination, RepositoryLink,
};
use aruna_core::structs::execution::job::ExportRoCrateSpec;
use aruna_core::structs::identity::auth::Permission;
use http::Method;
use serde_json::Value;

use super::connect;
use super::export::{file_keys, record_from};
use crate::driver::DriverContext;
use crate::jobs::executor::JobContext;
use crate::jobs::repository::TransferError;
use crate::jobs::repository::links::read_secret;
use crate::jobs::repository::push::creator_auth;

/// Fails as remote changed when the lineage moved outside Aruna; returns a parent's latest version.
pub(crate) async fn check_lineage(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &RepositoryDestination,
    target: &LinkTarget,
) -> Result<Option<String>, TransferError> {
    let client = connect(
        &ctx.driver,
        &spec.auth_context,
        destination.group_id,
        destination.connector_id,
        Permission::WRITE,
        spec.limits.metadata_bytes,
        destination.credential.as_ref(),
    )
    .await?;
    let changed = || TransferError::Refused(LinkFailure::RemoteChanged);
    let read = async |url| match client.json(Method::GET, url, None).await {
        Err(InvenioError::Status(404 | 410)) => Err(changed()),
        result => result.map_err(TransferError::from),
    };
    if let Some(draft) = &destination.draft_id {
        validate_id(draft)?;
        let current = read(client.url(&["records", draft, "draft"])?).await?;
        // Another revision than this link's last write means someone edited the draft.
        if target
            .revision_id
            .is_some_and(|revision| current["revision_id"].as_u64() != Some(revision))
        {
            return Err(changed());
        }
    }
    // The record's own version flag comes from the database, so no search index lag applies.
    if let Some(published) = &target.published_id {
        validate_id(published)?;
        let record = read(client.url(&["records", published])?).await?;
        if record["versions"]["is_latest"] != true {
            return Err(changed());
        }
        return Ok(None);
    }
    let Some(parent) = target
        .parent_id
        .as_ref()
        .filter(|_| destination.draft_id.is_none())
    else {
        return Ok(None);
    };
    validate_id(parent)?;
    let mut url = client.url(&["records"])?;
    url.query_pairs_mut()
        .append_pair("q", &format!("parent.id:{parent}"))
        .append_pair("size", "1");
    let page = read(url).await?;
    let hit = &page["hits"]["hits"][0];
    match hit["id"].as_str() {
        Some(id) if hit["parent"]["id"] == parent.as_str() => {
            validate_id(id)?;
            Ok(Some(id.to_string()))
        }
        _ => Err(changed()),
    }
}

async fn link_client<'a>(
    context: &'a DriverContext,
    link: &RepositoryLink,
) -> Result<InvenioClient<'a>, TransferError> {
    let credential = read_secret(&context.storage_handle, link.link_id)
        .await
        .map_err(|error| TransferError::Retryable(error.to_string()))?
        .filter(|secret| secret.link_id == Some(link.link_id) && secret.endpoint == link.endpoint)
        .ok_or_else(|| TransferError::Permanent("the link token was removed".into()))?;
    let auth = creator_auth(link);
    connect(
        context,
        &auth,
        link.group_id,
        link.connector_id,
        Permission::WRITE,
        link.limits.metadata_bytes,
        Some(&credential),
    )
    .await
}

async fn read_optional(
    client: &InvenioClient<'_>,
    path: &[&str],
) -> Result<Option<Value>, TransferError> {
    match client.json(Method::GET, client.url(path)?, None).await {
        Ok(value) => Ok(Some(value)),
        Err(InvenioError::Status(404 | 410)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn review_of(record: &Value) -> Option<LinkReview> {
    match record["parent"]["review"]["status"].as_str()? {
        "created" | "submitted" => Some(LinkReview::Pending),
        "accepted" => Some(LinkReview::Accepted),
        "declined" | "cancelled" | "expired" => Some(LinkReview::Declined),
        _ => None,
    }
}

/// The repository's answer to a pending review; `None` while the community still decides.
pub(crate) async fn review_state(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<Option<RemoteState>, TransferError> {
    let Some(draft_id) = link.remote.draft_id.as_deref() else {
        return Ok(None);
    };
    validate_id(draft_id)?;
    let client = link_client(context, link).await?;
    if let Some(draft) = read_optional(&client, &["records", draft_id, "draft"]).await? {
        let review = review_of(&draft).unwrap_or(LinkReview::Declined);
        if review == LinkReview::Pending {
            return Ok(None);
        }
        return Ok(Some(RemoteState {
            draft: Some(record_from(&client, &draft)?),
            latest: None,
            review,
            files: file_keys(&client, draft_id, false).await?,
        }));
    }
    // An accepted review publishes the draft, which then leaves the draft endpoint.
    let Some(record) = read_optional(&client, &["records", draft_id]).await? else {
        return Err(TransferError::Refused(LinkFailure::RemoteChanged));
    };
    Ok(Some(RemoteState {
        draft: None,
        latest: Some(record_from(&client, &record)?),
        review: LinkReview::Accepted,
        files: file_keys(&client, draft_id, true).await?,
    }))
}

/// The repository's current draft and latest published version, for accepting remote edits.
pub async fn remote_state(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<RemoteState, TransferError> {
    let client = link_client(context, link).await?;
    let draft = match link.remote.draft_id.as_deref() {
        Some(id) => {
            validate_id(id)?;
            read_optional(&client, &["records", id, "draft"]).await?
        }
        None => None,
    };
    let anchor = link
        .remote
        .record_id
        .as_deref()
        .or(link.remote.draft_id.as_deref());
    let mut latest = None;
    if let Some(anchor) = anchor {
        validate_id(anchor)?;
        let mut url = client.url(&["records", anchor, "versions"])?;
        url.query_pairs_mut()
            .append_pair("size", "1")
            .append_pair("sort", "version");
        let page = match client.json(Method::GET, url, None).await {
            Ok(page) => page,
            Err(InvenioError::Status(404 | 410)) => Value::Null,
            Err(error) => return Err(error.into()),
        };
        let hit = &page["hits"]["hits"][0];
        if hit.is_object() {
            latest = Some(record_from(&client, hit)?);
        }
    }
    let review = match &draft {
        Some(draft) => review_of(draft).unwrap_or(LinkReview::None),
        None if link.remote.review == LinkReview::Pending && latest.is_some() => {
            LinkReview::Accepted
        }
        None if link.remote.review == LinkReview::Pending => LinkReview::None,
        None => link.remote.review,
    };
    let files = match (&draft, &latest) {
        (Some(draft), _) => file_keys(&client, &record_from(&client, draft)?.id, false).await?,
        (None, Some(latest)) => file_keys(&client, &latest.id, true).await?,
        (None, None) => Vec::new(),
    };
    Ok(RemoteState {
        draft: draft
            .map(|draft| record_from(&client, &draft))
            .transpose()?,
        latest,
        review,
        files,
    })
}
