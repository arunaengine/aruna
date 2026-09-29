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
    GitChange, GitPack, GitPackRecord, LOCAL_OBJECTS, StoredObject, git_pack_key,
};
use aruna_core::keyspaces::GIT_PACK_KEYSPACE;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::{info, warn};
use ulid::Ulid;

/// Moves each old pack into Fjall, then deletes the exact object version and its index row.
/// An LFS copy is deleted only while its original stays readable. Failures wait for the next
/// run; once nothing old is left, a run only reads the index.
pub async fn remove_objects(context: &DriverContext) {
    let rows = match records::prefixed::<StoredObject>(context, LOCAL_OBJECTS, Vec::new()).await {
        Ok(rows) => rows,
        Err(error) => return warn!(%error, "Local Git objects unreadable"),
    };
    for (key, object) in rows {
        let pack = object.key.starts_with("git-packs/");
        if !pack && !object.key.starts_with("git-copies/") {
            continue;
        }
        let Some(id) = key.get(..16).and_then(|id| <[u8; 16]>::try_from(id).ok()) else {
            continue;
        };
        let id = Ulid::from_bytes(id);
        let Ok(Some(document)) = load_document_record(context, id).await else {
            continue;
        };
        let kept = match pack {
            true => moved(context, &document, &object).await,
            false => original(context, &document, &object).await,
        };
        match kept {
            Ok(true) => {}
            Ok(false) => continue,
            Err(error) => {
                warn!(document_id = %id, key = %object.key, %error, "Old Git object kept");
                continue;
            }
        }
        if let Err(error) = delete(context, &document, &object, key).await {
            warn!(document_id = %id, key = %object.key, %error, "Old Git object not deleted");
        } else {
            info!(document_id = %id, key = %object.key, "Deleted old Git object");
        }
    }
}

/// Whether the pack's bytes are in Fjall, moved there now, or named by no record.
async fn moved(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    object: &StoredObject,
) -> Result<bool, GitError> {
    let pack = GitPack {
        sha256: object.sha256.clone(),
        size: object.size,
    };
    let digest = pack.digest().ok_or(GitError::Invalid)?;
    let key = git_pack_key(document.document_id, &digest).to_vec();
    if records::load::<GitPackRecord>(context, GIT_PACK_KEYSPACE, key)
        .await?
        .is_some()
    {
        return Ok(true);
    }
    let history = records::scan(context, document.document_id).await?;
    let owner = history.iter().find(|record| {
        matches!(&record.change, GitChange::Objects { pack: Some(own), .. } if *own == pack)
    });
    match owner {
        Some(owner) => objects::pack_bytes(context, document, &pack, owner)
            .await
            .map(|_| true),
        None => Ok(true),
    }
}

/// Whether the original of an LFS copy is still readable, so the copy is not the last one.
async fn original(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    copy: &StoredObject,
) -> Result<bool, GitError> {
    let history = records::scan(context, document.document_id).await?;
    let Some(original) = reduce(&history, &Ancestry::new())
        .0
        .lfs
        .remove(&copy.sha256)
    else {
        return Ok(false);
    };
    if original.key == copy.key && original.node_id == copy.node_id {
        return Ok(false);
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
    Ok(lfs::readable(context, &auth, &original).await)
}

async fn delete(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    object: &StoredObject,
    key: Vec<u8>,
) -> Result<(), GitError> {
    let group_id = object.group_id.ok_or(GitError::Invalid)?;
    drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version_id: Some(object.version_id),
            group_id,
            realm_id: document.realm_id,
            node_id: object.node_id,
            deleted_by: UserId::nil(document.realm_id),
        }),
        context,
    )
    .await
    .map_err(|_| GitError::Unavailable)?;
    records::remove(context, LOCAL_OBJECTS, key).await
}
