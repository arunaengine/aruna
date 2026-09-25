//! Publishes signed ARC snapshots and checkpoints as Git records on document holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::project::{Projection, author, lock, project};
use super::{GitError, document, objects, publish, records};
use crate::driver::DriverContext;
use crate::driver::drive;
use crate::replication::bao_read::{BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::get::{GetObjectInput, get_object_info};
use aruna_blob::git::GitStore;
use aruna_core::git::{
    CHECKPOINT_AFTER, GitChange, GitCheckpoint, GitEffect, GitEvent, GitSnapshot, GitStatus,
    LinkedObject, RefUpdate, STATUS, StoredObject, ZERO_OID,
};
use aruna_core::keyspaces::{EVENT_LOG_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE};
use aruna_core::metadata::{
    MaterializationState, MaterializationStatusRecord, MetadataEventRecord, MetadataRawRevision,
};
use aruna_core::storage_entries::{event_log_key, materialization_status_key};
use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_SHA256};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::{NodeId, UserId};
use bytes::Bytes;
use ulid::Ulid;

/// How long other holders leave a new revision to the first holder before generating it.
const FAILOVER_MS: u64 = 60_000;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

fn first(context: &DriverContext, holders: &[NodeId]) -> bool {
    context.net_handle.as_ref().map(|net| net.node_id()) == holders.first().copied()
}

pub(super) async fn execute(
    store: &GitStore,
    effect: GitEffect,
    actor: UserId,
) -> Result<GitEvent, GitError> {
    store
        .execute(effect, actor)
        .await
        .map_err(|_| GitError::Unavailable)
}

/// The materialized graph this node would snapshot, if it is ready. During materialization
/// the graph is current before its status says so, which `materializing` states.
pub(super) async fn current(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    revision: Option<&MetadataRawRevision>,
    materializing: bool,
) -> Result<Option<(Ulid, String)>, GitError> {
    let raw = match revision {
        Some(revision) => Some(revision.clone()),
        None => crate::metadata::raw_revision::load_raw_view(context, document.document_id, None)
            .await
            .map_err(|_| GitError::Unavailable)?
            .map(|view| view.revision),
    };
    if let Some(raw) = raw {
        return Ok(Some((raw.winning_event_id, raw.jsonld)));
    }
    let projected: Option<MaterializationStatusRecord> = records::load(
        context,
        MATERIALIZATION_STATUS_KEYSPACE,
        materialization_status_key(document.document_id).to_vec(),
    )
    .await?;
    if !projected.is_some_and(|status| {
        (materializing || status.state == MaterializationState::Materialized)
            && status.event_id == document.last_event_id
    }) {
        return Ok(None);
    }
    let handle = context
        .metadata_handle
        .as_ref()
        .ok_or(GitError::Unavailable)?;
    let jsonld = handle
        .export_rocrate_jsonld(document.graph_iri.clone())
        .await
        .map_err(|_| GitError::Unavailable)?;
    Ok(Some((document.last_event_id, jsonld)))
}

/// Size and digests of an exact object version, read here or from its node.
async fn resolve(
    context: &DriverContext,
    exact: &VersionedObjectArn,
    user: UserId,
) -> Result<StoredObject, GitError> {
    let node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    let (size, hashes, group_id) = if exact.node_id == node {
        let bucket = drive(GetBucketOperation::new(exact.bucket.clone()), context)
            .await
            .map_err(|_| GitError::NotFound)?;
        let input = GetObjectInput {
            bucket: exact.bucket.clone(),
            key: exact.key.clone(),
            version_id: Some(exact.version),
            range: None,
            group_id: bucket.group_id,
            user_identity: user,
            node_id: node,
        };
        let info = get_object_info(context, input, None)
            .await
            .map_err(|_| GitError::NotFound)?;
        (info.size, info.hashes, Some(bucket.group_id))
    } else {
        let request = BaoReadRequest {
            auth_context: author(user),
            realm_id: exact.realm_id,
            target: BaoReadTarget::ExactVersion(exact.clone()),
            expected_blake3: None,
            metadata_only: true,
            destination: None,
            known_refs: Vec::new(),
        };
        match managed_read(context, exact.node_id, request).await {
            Ok(BaoReadOutput::Metadata { size, hashes, .. }) => (size, hashes, None),
            _ => return Err(GitError::NotFound),
        }
    };
    Ok(StoredObject {
        node_id: exact.node_id,
        group_id,
        bucket: exact.bucket.clone(),
        key: exact.key.clone(),
        version_id: exact.version,
        size,
        sha256: hashes
            .get(HASH_SHA256)
            .map(hex::encode)
            .ok_or(GitError::Unavailable)?,
        blake3: hashes
            .get(HASH_BLAKE3)
            .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok())
            .ok_or(GitError::Unavailable)?,
    })
}

/// File entities that name an exact Aruna object version. An object the author cannot
/// read stays out of the ARC; its entity still describes it in the metadata.
pub(super) async fn linked(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    jsonld: &str,
    user: UserId,
) -> Vec<LinkedObject> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(jsonld) else {
        return Vec::new();
    };
    let mut linked = Vec::new();
    for entity in value["@graph"].as_array().into_iter().flatten() {
        let listed = |value: &serde_json::Value| -> Vec<String> {
            let values = value
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![value.clone()]);
            values
                .iter()
                .filter_map(|item| item.as_str().or_else(|| item["@id"].as_str()))
                .map(str::to_owned)
                .collect()
        };
        let Some(id) = entity["@id"].as_str() else {
            continue;
        };
        // RO-Crate `File` is an alias of schema.org MediaObject, which exports may spell out.
        let file = |kind: &String| {
            let local = kind
                .trim_start_matches("http://schema.org/")
                .trim_start_matches("https://schema.org/");
            matches!(local, "File" | "MediaObject")
        };
        if !listed(&entity["@type"]).iter().any(file) {
            continue;
        }
        let urls = listed(&entity["contentUrl"]);
        let Some(exact) = crate::jobs::export::entity_identity(id, &urls).exact else {
            continue;
        };
        if exact.realm_id != document.realm_id {
            continue;
        }
        match resolve(context, &exact, user).await {
            Ok(object) => linked.push(LinkedObject {
                entity: id.to_string(),
                object,
            }),
            Err(error) => {
                tracing::warn!(entity = id, %error, "Leaving unreadable object out of the ARC")
            }
        }
        if linked.len() >= 10_000 {
            break;
        }
    }
    linked
}

async fn generate(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
    projection: &Projection,
    source: (Ulid, String),
) -> Result<(), GitError> {
    let (event_id, jsonld) = source;
    let event: Option<MetadataEventRecord> = records::load(
        context,
        EVENT_LOG_KEYSPACE,
        event_log_key(document.document_id, event_id).to_vec(),
    )
    .await?;
    let user = event
        .as_ref()
        .map_or(UserId::nil(document.realm_id), |event| event.user_id);
    let occurred_at_ms = event.map_or(document.updated_at_ms, |event| event.occurred_at_ms);
    let objects = linked(context, document, &jsonld, user).await;
    let lfs: Vec<StoredObject> = objects.iter().map(|linked| linked.object.clone()).collect();
    let refs = &projection.state.refs;
    let effect = GitEffect::Generate {
        snapshot: GitSnapshot {
            document_id: document.document_id,
            event_id,
            occurred_at_ms,
            jsonld,
            objects,
        },
        refs: refs.clone(),
    };
    let status_key = document.document_id.to_bytes().to_vec();
    let (aruna, main) = match execute(store, effect, user).await? {
        GitEvent::Generated { aruna, main } => (aruna, main),
        GitEvent::GenerateFailed(error) => {
            let status = GitStatus {
                event_id,
                commit: refs.get("refs/heads/aruna").cloned(),
                error: Some(error),
            };
            return records::save(context, STATUS, status_key, &status).await;
        }
        _ => return Err(GitError::Unavailable),
    };
    let unchanged = refs.get("refs/heads/aruna") == Some(&aruna) && main.is_none();
    let pack = if unchanged {
        None
    } else {
        let mut include = vec![aruna.clone()];
        include.extend(main.iter().cloned());
        let effect = GitEffect::Pack {
            document_id: document.document_id,
            include,
            exclude: refs.values().cloned().collect(),
        };
        let GitEvent::Packed(pack) = execute(store, effect, user).await? else {
            return Err(GitError::Unavailable);
        };
        Some(objects::store_pack(context, &author(user), document, pack).await?)
    };
    let update = |name: &str, new: String| RefUpdate {
        name: name.into(),
        old: refs.get(name).cloned().unwrap_or_else(|| ZERO_OID.into()),
        new,
    };
    let mut updates = vec![update("refs/heads/aruna", aruna.clone())];
    updates.extend(main.map(|main| update("refs/heads/main", main)));
    updates.retain(|update| update.old != update.new);
    let change = GitChange::Objects {
        pack: pack.map(Box::new),
        refs: updates,
        lfs,
        revision: Some(event_id),
    };
    publish::publish(context, document, user, change).await?;
    let status = GitStatus {
        event_id,
        commit: Some(aruna),
        error: None,
    };
    records::save(context, STATUS, status_key, &status).await
}

/// Folds the records covered so far into one checkpoint so history never hits its cap.
async fn checkpoint(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
    projection: &Projection,
) -> Result<(), GitError> {
    let state = &projection.state;
    let user = UserId::nil(document.realm_id);
    let effect = GitEffect::Pack {
        document_id: document.document_id,
        include: state.refs.values().cloned().collect(),
        exclude: Vec::new(),
    };
    let GitEvent::Packed(pack) = execute(store, effect, user).await? else {
        return Err(GitError::Unavailable);
    };
    let owner = projection
        .records
        .last()
        .map_or(user, |record| record.user_id);
    let pack = objects::store_pack(context, &author(owner), document, pack).await?;
    let change = GitChange::Checkpoint(Box::new(GitCheckpoint {
        pack,
        refs: state.refs.clone().into_iter().collect(),
        lfs: state.lfs.values().cloned().collect(),
        locks: state.locks.values().cloned().collect(),
        revision: state.revision,
        covered: state.applied.clone(),
    }));
    publish::publish(context, document, owner, change)
        .await
        .map(|_| ())
}

/// Projects the records, then publishes a missing snapshot or a due checkpoint. The first
/// holder acts at once; the others take over when a revision stays unpublished too long.
/// The caller holds the document lock.
pub async fn refresh(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
) -> Result<Projection, GitError> {
    update(context, store, document, None, false).await
}

async fn update(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
    revision: Option<&MetadataRawRevision>,
    materializing: bool,
) -> Result<Projection, GitError> {
    let mut projection = project(context, store, document).await?;
    let leading = first(context, &projection.holders);
    let overdue = now_ms().saturating_sub(document.updated_at_ms) > FAILOVER_MS;
    if let Some(source) = current(context, document, revision, materializing).await?
        && projection
            .state
            .revision
            .is_none_or(|applied| applied < source.0)
        && (leading || overdue)
    {
        generate(context, store, document, &projection, source).await?;
        projection = project(context, store, document).await?;
    }
    let uncovered = publish::uncovered(&projection.records).len();
    if uncovered > CHECKPOINT_AFTER && (leading || uncovered > 2 * CHECKPOINT_AFTER) {
        checkpoint(context, store, document, &projection).await?;
        projection = project(context, store, document).await?;
    }
    Ok(projection)
}

/// Called after materialization; nodes that do not hold the document have nothing to do.
pub async fn capture(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
    revision: Option<&MetadataRawRevision>,
) -> Result<(), GitError> {
    let Some(store) = context
        .metadata_handle
        .as_ref()
        .and_then(|handle| handle.git())
    else {
        return Ok(());
    };
    let _guard = lock(record.document_id).await;
    match update(context, store, record, revision, true).await {
        Ok(_) | Err(GitError::NotHolder) => Ok(()),
        Err(error) => Err(error),
    }
}

pub async fn status(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
) -> Result<(Option<GitStatus>, Projection), GitError> {
    let (document, _) = super::repository(context, auth, id, Permission::READ).await?;
    let _guard = lock(id).await;
    let projection = refresh(context, store, &document).await?;
    let status = records::load(context, STATUS, id.to_bytes().to_vec()).await?;
    Ok((status, projection))
}

pub async fn export(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    revision: String,
) -> Result<Bytes, GitError> {
    let document = document(context, auth, id, Permission::READ).await?;
    let _guard = lock(id).await;
    refresh(context, store, &document).await?;
    let effect = GitEffect::Export {
        document_id: id,
        revision,
    };
    let GitEvent::Exported(bytes) = execute(store, effect, auth.user_id).await? else {
        return Err(GitError::Unavailable);
    };
    let result: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| GitError::Unavailable)?;
    if result.get("error").is_some() {
        return Err(GitError::Invalid);
    }
    Ok(bytes)
}
