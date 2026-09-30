//! Removes Git packs and LFS copies stored as objects before packs moved into Fjall.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::state::{Ancestry, reduce};
use super::{GitError, lfs, objects, records};
use crate::driver::{DriverContext, drive};
use crate::metadata::get_document::load_document_record;
use crate::s3::object::delete::{DeleteObjectInput, DeleteObjectOperation};
use aruna_core::UserId;
use aruna_core::git::{
    GitChange, GitPack, GitPackRecord, GitRecord, LOCAL_OBJECTS, StoredObject, git_pack_key,
};
use aruna_core::keyspaces::GIT_PACK_KEYSPACE;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::{info, warn};
use ulid::Ulid;

/// Deletes each old pack or copy once its bytes are safe: a pack or pack copy after its bytes
/// are in Fjall, an LFS copy while its original stays readable or once nothing names it, and
/// everything of a deleted dataset. Kept objects are logged with the reason and retried later.
pub async fn remove_objects(context: &DriverContext) {
    let Some(realm_id) = context.net_handle.as_ref().map(|net| *net.realm_id()) else {
        return;
    };
    let rows = match records::prefixed::<StoredObject>(context, LOCAL_OBJECTS, Vec::new()).await {
        Ok(rows) => rows,
        Err(error) => return warn!(%error, "Local Git objects unreadable"),
    };
    for (key, object) in rows {
        if !object.key.starts_with("git-packs/") && !object.key.starts_with("git-copies/") {
            continue;
        }
        let Some(id) = key.get(..16).and_then(|id| <[u8; 16]>::try_from(id).ok()) else {
            continue;
        };
        let id = Ulid::from_bytes(id);
        let settled = match load_document_record(context, id).await {
            // No dataset is left whose history could need the object.
            Ok(None) => Ok(Ok(())),
            Ok(Some(document)) => settled(context, &document, &object).await,
            Err(_) => continue,
        };
        match settled {
            Ok(Ok(())) => {}
            Ok(Err(reason)) => {
                info!(document_id = %id, key = %object.key, reason, "Old Git object kept");
                continue;
            }
            Err(error) => {
                warn!(document_id = %id, key = %object.key, %error, "Old Git object kept");
                continue;
            }
        }
        match delete(context, realm_id, &object, key).await {
            Ok(()) => info!(document_id = %id, key = %object.key, "Deleted old Git object"),
            Err(error) => {
                warn!(document_id = %id, key = %object.key, %error, "Old Git object not deleted");
            }
        }
    }
}

/// `Ok(())` when the object may go, or the reason it must stay.
async fn settled(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    object: &StoredObject,
) -> Result<Result<(), &'static str>, GitError> {
    let history = records::scan(context, document.document_id).await?;
    let pack = GitPack {
        sha256: object.sha256.clone(),
        size: object.size,
    };
    let named = |record: &GitRecord| match &record.change {
        GitChange::Objects {
            pack: Some(own), ..
        } => *own == pack,
        GitChange::Checkpoint(checkpoint) => checkpoint.packs.contains(&pack),
        _ => false,
    };
    // Copies of remote packs were stored under git-copies/ too.
    if object.key.starts_with("git-packs/") || history.iter().any(named) {
        return moved(context, document, &pack, &history).await;
    }
    original(context, document, object, &history).await
}

/// Whether the pack's bytes are in Fjall, moved there now, or named by no record.
async fn moved(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    pack: &GitPack,
    history: &[GitRecord],
) -> Result<Result<(), &'static str>, GitError> {
    let digest = pack.digest().ok_or(GitError::Invalid)?;
    let key = git_pack_key(document.document_id, &digest).to_vec();
    if records::load::<GitPackRecord>(context, GIT_PACK_KEYSPACE, key)
        .await?
        .is_some()
    {
        return Ok(Ok(()));
    }
    let owner = history.iter().find(|record| {
        matches!(&record.change, GitChange::Objects { pack: Some(own), .. } if own == pack)
    });
    match owner {
        Some(owner) => objects::pack_bytes(context, document, pack, owner)
            .await
            .map(|_| Ok(())),
        None => Ok(Ok(())),
    }
}

/// Whether an LFS copy is not the last one: its original is still readable, or no record
/// names the content at all.
async fn original(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    copy: &StoredObject,
    history: &[GitRecord],
) -> Result<Result<(), &'static str>, GitError> {
    let Some(original) = reduce(history, &Ancestry::new()).0.lfs.remove(&copy.sha256) else {
        return Ok(Ok(()));
    };
    if original.key == copy.key && original.node_id == copy.node_id {
        return Ok(Err("the copy is the recorded original"));
    }
    // The pushing user reads the original, as a download by that user would.
    let pusher = history
        .iter()
        .find(|record| match &record.change {
            GitChange::Objects { lfs, .. } => lfs.contains(&original),
            GitChange::Checkpoint(checkpoint) => checkpoint.lfs.contains(&original),
            _ => false,
        })
        .map_or(UserId::nil(document.realm_id), |record| record.user_id);
    let auth = super::project::author(pusher);
    Ok(match lfs::readable(context, &auth, &original).await {
        true => Ok(()),
        false => Err("the original is unreadable, so this copy may be the last one"),
    })
}

/// Deletes the exact version through the normal S3 delete; a version that is already gone
/// only loses its index row.
async fn delete(
    context: &DriverContext,
    realm_id: RealmId,
    object: &StoredObject,
    key: Vec<u8>,
) -> Result<(), GitError> {
    let group_id = object.group_id.ok_or(GitError::Invalid)?;
    let found = (object.bucket.as_str(), group_id);
    let exists = objects::described(context, found, &object.key, Some(object.version_id))
        .await
        .is_ok_and(|described| described.is_some());
    if exists {
        drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: object.bucket.clone(),
                key: object.key.clone(),
                version_id: Some(object.version_id),
                group_id,
                realm_id,
                node_id: object.node_id,
                deleted_by: UserId::nil(realm_id),
            }),
            context,
        )
        .await
        .map_err(|_| GitError::Unavailable)?;
    }
    records::remove(context, LOCAL_OBJECTS, key).await
}
