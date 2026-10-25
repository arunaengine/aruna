//! Serves the database list: marks, owner consent, removal and the reader-filtered listing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A mark needs the dataset to link the bucket root as a Dataset-typed `hasPart` already;
// the entry itself grants nothing, so every listing checks the caller's own permissions.

pub mod list;
pub mod publish;

use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentDot;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DATABASE_REQUEST_KEYSPACE, DISTRIBUTION_KEYSPACE};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::database::{
    ConsentClaim, DatabaseConsent, DatabaseEntry, DatabaseRequest, DatabaseTarget,
    database_admin_path, owner_admin_path,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::types::GroupId;
use ulid::Ulid;

use self::list::{Caller, DatabaseServiceError, allowed, read};
use crate::driver::{DriverContext, drive};
use crate::metadata::api::forwarded_bearer;
use crate::realm::database::publish::{
    DatabaseChange, PublishDatabaseConfig, PublishDatabaseOperation,
};
use crate::realm::get_config::GetConfigOperation;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Pending requests one node keeps and lists, at most.
const MAX_REQUESTS: usize = 1024;

/// What a curator asks for: a new mark, or a retarget of `entry_id`.
#[derive(Clone, Debug)]
pub struct MarkInput {
    pub entry_id: Option<Ulid>,
    pub dataset_id: Ulid,
    pub target: DatabaseTarget,
}

#[derive(Clone, Debug)]
pub enum MarkOutcome {
    /// The caller administers the owning group too; the entry is published.
    Published(DatabaseEntry),
    /// An admin of the owning group has to approve the request first.
    Pending(DatabaseRequest),
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// The group that owns the target bucket: from the set's record, from this node's bucket, or
/// from the bucket's node under the caller's token.
pub async fn bucket_owner(
    context: &DriverContext,
    node_id: NodeId,
    caller: &Caller,
    target: &DatabaseTarget,
) -> Result<GroupId, DatabaseServiceError> {
    match target {
        DatabaseTarget::Distributed { set_id } => {
            let view = read(context, DISTRIBUTION_KEYSPACE, set_id.to_bytes().to_vec())
                .await?
                .map(|value| DistributionView::from_bytes(&value))
                .transpose()
                .map_err(|error| DatabaseServiceError::Internal(error.to_string()))?
                .ok_or_else(|| {
                    DatabaseServiceError::NotFound(format!(
                        "distributed bucket {set_id} is not known on this node"
                    ))
                })?;
            Ok(view.snapshot().config.owner_group)
        }
        DatabaseTarget::Ordinary {
            node_id: bucket_node,
            bucket,
        } if *bucket_node == node_id => {
            match drive(GetBucketOperation::new(bucket.clone()), context).await {
                Ok(info) => Ok(info.group_id),
                Err(GetBucketError::NotFound) => Err(DatabaseServiceError::NotFound(format!(
                    "bucket {bucket} not found"
                ))),
                Err(error) => Err(DatabaseServiceError::Internal(error.to_string())),
            }
        }
        DatabaseTarget::Ordinary {
            node_id: bucket_node,
            bucket,
        } => {
            let handle = context.metadata_handle.as_ref().ok_or_else(|| {
                DatabaseServiceError::Unavailable("metadata transport unavailable".to_string())
            })?;
            let token = forwarded_bearer(caller.bearer.as_deref())
                .map_err(|_| DatabaseServiceError::BadRequest("invalid bearer".to_string()))?;
            handle
                .request_bucket_owner(*bucket_node, token, bucket.clone())
                .await
                .map_err(|error| {
                    DatabaseServiceError::Unavailable(format!(
                        "the bucket's node did not name its owner: {error:?}"
                    ))
                })
        }
    }
}

/// Marks a bucket as a database or retargets an entry. Publishes at once when the caller
/// also administers the owning group, otherwise keeps a pending request for its admins.
pub async fn mark_database(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    caller: &Caller,
    input: MarkInput,
) -> Result<MarkOutcome, DatabaseServiceError> {
    let auth = &caller.auth;
    let curator_path = database_admin_path(realm_id);
    if !allowed(context, realm_id, auth, &curator_path, Permission::WRITE).await? {
        return Err(DatabaseServiceError::Forbidden);
    }
    let owner_group = bucket_owner(context, node_id, caller, &input.target).await?;
    let entry = DatabaseEntry {
        entry_id: input.entry_id.unwrap_or_else(Ulid::generate),
        dataset_id: input.dataset_id,
        target: input.target,
        owner_group,
    };
    entry
        .validate()
        .map_err(|error| DatabaseServiceError::BadRequest(error.to_string()))?;
    require_link(context, realm_id, node_id, caller, &entry).await?;
    let owner_path = owner_admin_path(realm_id, owner_group);
    if allowed(context, realm_id, auth, &owner_path, Permission::WRITE).await? {
        let actor = Actor {
            node_id,
            user_id: auth.user_id,
            realm_id,
        };
        drive(
            PublishDatabaseOperation::new(PublishDatabaseConfig {
                actor,
                checks: vec![(auth.clone(), curator_path), (auth.clone(), owner_path)],
                change: DatabaseChange::Set {
                    entry: entry.clone(),
                    consent: None,
                },
            }),
            context,
        )
        .await?;
        return Ok(MarkOutcome::Published(entry));
    }
    let request = DatabaseRequest {
        request_id: Ulid::generate(),
        predecessor: register_version(context, realm_id, entry.entry_id).await?,
        entry,
        curator: auth.user_id,
        created_at_ms: now_ms(),
    };
    write_request(context, &request).await?;
    Ok(MarkOutcome::Pending(request))
}

/// Refuses an entry whose dataset does not link the bucket root now.
async fn require_link(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    caller: &Caller,
    entry: &DatabaseEntry,
) -> Result<(), DatabaseServiceError> {
    match list::dataset_links(context, realm_id, node_id, caller, entry).await? {
        Some(list::LinkedDataset::Linked { .. }) => Ok(()),
        Some(_) => Err(DatabaseServiceError::NotLinked(entry.dataset_id)),
        None => Err(DatabaseServiceError::Unavailable(
            "not every metadata node answered; the dataset link cannot be confirmed".to_string(),
        )),
    }
}

/// An admin of the owning group approves a pending request: this node signs the consent for
/// the approver's own authenticated request and publishes the curator's mark with it. The
/// bucket's owner and the dataset link are checked again, since both may have changed.
pub async fn approve_request(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    caller: &Caller,
    request_id: Ulid,
) -> Result<DatabaseEntry, DatabaseServiceError> {
    let request = read_request(context, request_id).await?;
    let auth = &caller.auth;
    let owner_path = owner_admin_path(realm_id, request.entry.owner_group);
    if !allowed(context, realm_id, auth, &owner_path, Permission::WRITE).await? {
        return Err(DatabaseServiceError::Forbidden);
    }
    let owner = bucket_owner(context, node_id, caller, &request.entry.target).await?;
    if owner != request.entry.owner_group {
        return Err(DatabaseServiceError::BadRequest(format!(
            "the bucket now belongs to group {owner}; mark it again"
        )));
    }
    require_link(context, realm_id, node_id, caller, &request.entry).await?;
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| DatabaseServiceError::Unavailable("node key unavailable".to_string()))?;
    let claim = ConsentClaim {
        realm_id,
        entry: request.entry.clone(),
        predecessor: request.predecessor,
        approver: auth.user_id,
    };
    let bytes = claim
        .signing_bytes()
        .map_err(|error| DatabaseServiceError::Internal(error.to_string()))?;
    let consent = DatabaseConsent {
        signature: net.sign(&bytes),
        signer: net.node_id(),
        claim,
    };
    // The curator's rights were checked with their credential; receivers check them again.
    let curator = AuthContext {
        user_id: request.curator,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    drive(
        PublishDatabaseOperation::new(PublishDatabaseConfig {
            actor: Actor {
                node_id,
                user_id: request.curator,
                realm_id,
            },
            checks: vec![
                (auth.clone(), owner_path),
                (curator, database_admin_path(realm_id)),
            ],
            change: DatabaseChange::Set {
                entry: request.entry.clone(),
                consent: Some(Box::new(consent)),
            },
        }),
        context,
    )
    .await?;
    delete_request(context, request_id).await?;
    Ok(request.entry)
}

/// The requester or an admin of the owning group drops a pending request.
pub async fn reject_request(
    context: &DriverContext,
    realm_id: RealmId,
    caller: &Caller,
    request_id: Ulid,
) -> Result<(), DatabaseServiceError> {
    let request = read_request(context, request_id).await?;
    let owner_path = owner_admin_path(realm_id, request.entry.owner_group);
    if request.curator != caller.auth.user_id
        && !allowed(
            context,
            realm_id,
            &caller.auth,
            &owner_path,
            Permission::WRITE,
        )
        .await?
    {
        return Err(DatabaseServiceError::Forbidden);
    }
    delete_request(context, request_id).await
}

/// The pending requests the caller made or may approve.
pub async fn list_requests(
    context: &DriverContext,
    realm_id: RealmId,
    caller: &Caller,
) -> Result<Vec<DatabaseRequest>, DatabaseServiceError> {
    let rows = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: DATABASE_REQUEST_KEYSPACE.to_string(),
            prefix: None,
            start: None::<IterStart>,
            limit: MAX_REQUESTS,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(DatabaseServiceError::Unavailable(error.to_string()));
        }
        other => return Err(DatabaseServiceError::Internal(format!("{other:?}"))),
    };
    let mut visible = Vec::new();
    for (_, value) in rows {
        let Ok(request) = DatabaseRequest::from_bytes(&value) else {
            continue;
        };
        let owner_path = owner_admin_path(realm_id, request.entry.owner_group);
        if request.curator == caller.auth.user_id
            || allowed(
                context,
                realm_id,
                &caller.auth,
                &owner_path,
                Permission::WRITE,
            )
            .await?
        {
            visible.push(request);
        }
    }
    Ok(visible)
}

/// Removes an entry: a curator or an admin of the owning group may.
pub async fn remove_database(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    caller: &Caller,
    entry_id: Ulid,
) -> Result<(), DatabaseServiceError> {
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .map_err(|error| DatabaseServiceError::Unavailable(error.to_string()))?;
    let entry = config
        .databases
        .iter()
        .find(|entry| entry.entry_id == entry_id)
        .ok_or_else(|| DatabaseServiceError::NotFound(format!("database {entry_id} not found")))?;
    let auth = &caller.auth;
    let curator_path = database_admin_path(realm_id);
    let path = if allowed(context, realm_id, auth, &curator_path, Permission::WRITE).await? {
        curator_path
    } else {
        owner_admin_path(realm_id, entry.owner_group)
    };
    drive(
        PublishDatabaseOperation::new(PublishDatabaseConfig {
            actor: Actor {
                node_id,
                user_id: auth.user_id,
                realm_id,
            },
            checks: vec![(auth.clone(), path)],
            change: DatabaseChange::Remove { entry_id },
        }),
        context,
    )
    .await?;
    Ok(())
}

async fn register_version(
    context: &DriverContext,
    realm_id: RealmId,
    entry_id: Ulid,
) -> Result<Option<AdminDocumentDot>, DatabaseServiceError> {
    let state = list::config_state(context, realm_id).await?;
    Ok(state.and_then(|state| state.database_version(entry_id)))
}

async fn read_request(
    context: &DriverContext,
    request_id: Ulid,
) -> Result<DatabaseRequest, DatabaseServiceError> {
    read(
        context,
        DATABASE_REQUEST_KEYSPACE,
        request_id.to_bytes().to_vec(),
    )
    .await?
    .map(|value| DatabaseRequest::from_bytes(&value))
    .transpose()
    .map_err(|error| DatabaseServiceError::Internal(error.to_string()))?
    .ok_or_else(|| DatabaseServiceError::NotFound(format!("request {request_id} not found")))
}

async fn write_request(
    context: &DriverContext,
    request: &DatabaseRequest,
) -> Result<(), DatabaseServiceError> {
    let value = request
        .to_bytes()
        .map_err(|error| DatabaseServiceError::Internal(error.to_string()))?;
    expect_done(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: DATABASE_REQUEST_KEYSPACE.to_string(),
                key: request.request_id.to_bytes().to_vec().into(),
                value: value.into(),
                txn_id: None,
            })
            .await,
    )
}

async fn delete_request(
    context: &DriverContext,
    request_id: Ulid,
) -> Result<(), DatabaseServiceError> {
    expect_done(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: DATABASE_REQUEST_KEYSPACE.to_string(),
                key: request_id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await,
    )
}

fn expect_done(event: Event) -> Result<(), DatabaseServiceError> {
    match event {
        Event::Storage(StorageEvent::WriteResult { .. })
        | Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(DatabaseServiceError::Unavailable(error.to_string()))
        }
        other => Err(DatabaseServiceError::Internal(format!("{other:?}"))),
    }
}
