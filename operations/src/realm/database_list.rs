//! Lists the realm's databases for one reader: entries whose bucket the reader may read and
//! whose dataset links that bucket readably. Curators also see inactive entries and why.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::database::{DatabaseEntry, DatabaseTarget, database_admin_path};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::distribution::DistributionView;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;
use crate::driver::drive;
use crate::metadata::api::{LinkNodeRequest, LinkQuery, links_distributed};
use crate::realm::database_publish::PublishDatabaseError;
use crate::realm::get_config::GetConfigOperation;

/// Pages of linking datasets read to find one dataset, at most.
const LINK_PAGES: usize = 10;

#[derive(Debug, Error)]
pub enum DatabaseServiceError {
    #[error("caller may not do this")]
    Forbidden,
    #[error("{0}")]
    NotFound(String),
    #[error("dataset {0} does not link this bucket as a Dataset-typed part of its root")]
    NotLinked(Ulid),
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    BadRequest(String),
    #[error(transparent)]
    Publish(#[from] PublishDatabaseError),
    #[error("{0}")]
    Internal(String),
}
/// The caller's credentials, as the transport passed them.
#[derive(Clone, Debug)]
pub struct Caller {
    pub auth: AuthContext,
    pub bearer: Option<String>,
}
pub(crate) async fn allowed(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    path: &str,
    permission: Permission,
) -> Result<bool, DatabaseServiceError> {
    match authorize(
        context,
        realm_id,
        auth,
        path,
        &permission,
        PolicyRequestExtras::rest(),
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(AuthorizeError::Storage(error)) => {
            Err(DatabaseServiceError::Unavailable(error.to_string()))
        }
        Err(_) => Ok(false),
    }
}
pub(crate) async fn read(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, DatabaseServiceError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|value| value.to_vec()))
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(DatabaseServiceError::Unavailable(error.to_string()))
        }
        other => Err(DatabaseServiceError::Internal(format!("{other:?}"))),
    }
}

/// Whether the dataset's live root names the target bucket as a linked part, as the caller
/// sees it. `Ok(None)` when the answer is incomplete and the dataset was not seen.
pub async fn dataset_links(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    caller: &Caller,
    entry: &DatabaseEntry,
) -> Result<Option<LinkedDataset>, DatabaseServiceError> {
    let link = entry.target.root_link(realm_id);
    let mut after = None;
    let mut reliable = true;
    let mut hidden = false;
    for _ in 0..LINK_PAGES {
        let page = links_distributed(
            context,
            realm_id,
            node_id,
            Some(caller.auth.clone()),
            caller.bearer.clone(),
            LinkNodeRequest {
                queries: vec![LinkQuery::Exact(link.clone())],
                parts_only: true,
                after,
                limit: 100,
            },
        )
        .await
        .map_err(|error| DatabaseServiceError::Unavailable(error.to_string()))?;
        if let Some(document) = page
            .documents
            .iter()
            .find(|document| document.document_id == entry.dataset_id)
        {
            return Ok(Some(LinkedDataset::Linked {
                title: document.title.clone(),
                graph_iri: document.graph_iri.clone(),
                document_path: document.document_path.clone(),
            }));
        }
        reliable &= page.nodes_failed == 0
            && page.pending_materializations == 0
            && page.failed_materializations == 0;
        hidden |= page.hidden;
        match page.next_after {
            Some(next) => after = Some(next),
            None if reliable && page.complete && hidden => return Ok(Some(LinkedDataset::Hidden)),
            None if reliable && page.complete => return Ok(Some(LinkedDataset::Unlinked)),
            None => return Ok(None),
        }
    }
    Ok(None)
}

/// How a database's dataset stands for one reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkedDataset {
    Linked {
        title: Option<String>,
        graph_iri: String,
        document_path: String,
    },
    /// Every node answered and no readable dataset links the bucket, but an unreadable one may.
    Hidden,
    /// Every node answered and nothing links the bucket: the entry is inactive.
    Unlinked,
}

/// Whether the caller may read the database's bucket on some node that holds it.
pub async fn can_read_bucket(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    entry: &DatabaseEntry,
) -> Result<Option<String>, DatabaseServiceError> {
    let (bucket, nodes) = match &entry.target {
        DatabaseTarget::Ordinary { node_id, bucket } => (bucket.clone(), vec![*node_id]),
        DatabaseTarget::Distributed { set_id } => {
            let Some(view) = read(context, DISTRIBUTION_KEYSPACE, set_id.to_bytes().to_vec())
                .await?
                .and_then(|value| DistributionView::from_bytes(&value).ok())
            else {
                return Ok(None);
            };
            let config = &view.snapshot().config;
            (
                config.bucket.clone(),
                config.members.iter().map(|member| member.node_id).collect(),
            )
        }
    };
    for node in nodes {
        let path = bucket_permission_path(realm_id, entry.owner_group, node, &bucket);
        if allowed(context, realm_id, auth, &path, Permission::READ).await? {
            return Ok(Some(bucket));
        }
    }
    Ok(None)
}

/// How one listed database stands for the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseState {
    /// The dataset links the bucket and the caller may read it.
    Active,
    /// Not every metadata node answered; the description is temporarily unavailable.
    Unavailable,
    /// Curator view only: the caller may not read the dataset.
    DatasetHidden,
    /// Curator view only: the dataset no longer links the bucket, or is gone.
    Unlinked,
}

#[derive(Clone, Debug)]
pub struct DatabaseListing {
    pub entry: DatabaseEntry,
    pub bucket: String,
    pub state: DatabaseState,
    pub title: Option<String>,
    pub graph_iri: Option<String>,
    pub document_path: Option<String>,
}

/// Databases after `after` the caller may see: entries whose bucket the caller may read and
/// whose dataset links it readably. The curator view also shows inactive entries.
pub async fn list_databases(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    caller: &Caller,
    after: Option<Ulid>,
    limit: usize,
    curator_view: bool,
) -> Result<(Vec<DatabaseListing>, Option<Ulid>), DatabaseServiceError> {
    if curator_view
        && !allowed(
            context,
            realm_id,
            &caller.auth,
            &database_admin_path(realm_id),
            Permission::WRITE,
        )
        .await?
    {
        return Err(DatabaseServiceError::Forbidden);
    }
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .map_err(|error| DatabaseServiceError::Unavailable(error.to_string()))?;
    let mut entries = config.databases;
    entries.sort_by_key(|entry| entry.entry_id);
    let limit = limit.clamp(1, 100);
    let mut listed = Vec::new();
    let mut examined = 0usize;
    let mut last = None;
    for entry in entries
        .into_iter()
        .filter(|entry| after.is_none_or(|after| entry.entry_id > after))
    {
        if listed.len() >= limit || examined >= limit.saturating_mul(4) {
            return Ok((listed, last));
        }
        examined += 1;
        last = Some(entry.entry_id);
        let bucket = match can_read_bucket(context, realm_id, &caller.auth, &entry).await? {
            Some(bucket) => bucket,
            None if curator_view => entry_bucket(&entry),
            None => continue,
        };
        let (state, details) =
            match dataset_links(context, realm_id, node_id, caller, &entry).await? {
                Some(LinkedDataset::Linked {
                    title,
                    graph_iri,
                    document_path,
                }) => (
                    DatabaseState::Active,
                    Some((title, graph_iri, document_path)),
                ),
                None => (DatabaseState::Unavailable, None),
                Some(LinkedDataset::Hidden) => (DatabaseState::DatasetHidden, None),
                Some(LinkedDataset::Unlinked) => (DatabaseState::Unlinked, None),
            };
        if !curator_view
            && matches!(
                state,
                DatabaseState::DatasetHidden | DatabaseState::Unlinked
            )
        {
            continue;
        }
        let (title, graph_iri, document_path) = match details {
            Some((title, graph_iri, document_path)) => {
                (title, Some(graph_iri), Some(document_path))
            }
            None => (None, None, None),
        };
        listed.push(DatabaseListing {
            entry,
            bucket,
            state,
            title,
            graph_iri,
            document_path,
        });
    }
    Ok((listed, None))
}

/// The bucket name an entry names without reading its record; empty for an unknown set.
fn entry_bucket(entry: &DatabaseEntry) -> String {
    match &entry.target {
        DatabaseTarget::Ordinary { bucket, .. } => bucket.clone(),
        DatabaseTarget::Distributed { .. } => String::new(),
    }
}
