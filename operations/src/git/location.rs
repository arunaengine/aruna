//! Reads and chooses a dataset's storage location, which replicates as a Git record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::state::{Ancestry, reduce};
use super::{GitError, objects, publish, records};
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::git::GitChange;
use aruna_core::handle::Handle;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::reducer::decode_reducer_state;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::dataset_location::{DatasetLocation, default_bucket};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use ulid::Ulid;

/// The chosen location, or the default one; `true` marks the default.
pub async fn effective(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
) -> Result<(DatasetLocation, bool), GitError> {
    let records = records::scan(context, document.document_id).await?;
    Ok(chosen(
        document,
        reduce(&records, &Ancestry::new()).0.location,
    ))
}

/// The location a reduced state chose, or the default one; `true` marks the default.
pub fn chosen(
    document: &MetadataRegistryRecord,
    location: Option<(Ulid, DatasetLocation)>,
) -> (DatasetLocation, bool) {
    match location {
        Some((_, location)) => (location, false),
        None => (
            DatasetLocation::default_for(document.group_id, document.document_id),
            true,
        ),
    }
}

pub async fn get(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
) -> Result<(DatasetLocation, bool), GitError> {
    let (document, _) = super::repository(context, auth, id, Permission::READ).await?;
    effective(context, &document).await
}

/// Records `location` as the dataset's choice after WRITE on the dataset and on the location.
pub async fn set(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    location: DatasetLocation,
) -> Result<DatasetLocation, GitError> {
    let (document, _) = super::repository(context, auth, id, Permission::WRITE).await?;
    record(context, auth, &document, location).await
}

/// Records `location` for a document whose WRITE the caller already proved, as the create
/// that returned `document` did.
pub async fn record(
    context: &DriverContext,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    location: DatasetLocation,
) -> Result<DatasetLocation, GitError> {
    writable(context, auth, document, &location).await?;
    // A location moves no ref, so it needs no document lock and never waits for a snapshot.
    let change = GitChange::Location(location.clone());
    publish::publish(context, document, auth.user_id, change).await?;
    Ok(location)
}

/// This node's bucket of `location` when `auth` may write under its prefix. The default
/// bucket is created on first use; any other bucket must already exist here.
pub async fn writable(
    context: &DriverContext,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    location: &DatasetLocation,
) -> Result<BucketInfo, GitError> {
    if !location.valid() {
        return Err(GitError::Invalid);
    }
    let bucket = if location.bucket == default_bucket(document.group_id) {
        objects::bucket(context, document, auth).await?
    } else {
        existing(context, &location.bucket).await?
    };
    permit(context, auth, &bucket, location).await?;
    Ok(bucket)
}

/// Checks a location chosen for a dataset that does not exist yet: a bucket other than the
/// group's default one must exist here and allow `auth` to write under the prefix.
pub async fn precheck(
    context: &DriverContext,
    auth: &AuthContext,
    group_id: GroupId,
    location: &DatasetLocation,
) -> Result<(), GitError> {
    if location.bucket == default_bucket(group_id) {
        return Ok(());
    }
    let bucket = existing(context, &location.bucket).await?;
    permit(context, auth, &bucket, location).await
}

/// Where the group's new datasets store their files, as stored on this node. Groups record
/// their generated bucket at creation; a group without a record uses the same.
pub async fn group_default(
    context: &DriverContext,
    group_id: GroupId,
) -> Result<DatasetLocation, GitError> {
    let read = StorageEffect::Read {
        key_space: DOCUMENT_STATE_KEYSPACE.into(),
        key: reducer_state_key(&AdminDocumentTarget::Group { group_id }),
        txn_id: None,
    };
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_effect(Effect::Storage(read))
        .await
    else {
        return Err(GitError::Unavailable);
    };
    let state = value
        .map(|bytes| decode_reducer_state(&bytes))
        .transpose()
        .map_err(|_| GitError::Unavailable)?;
    Ok(state
        .and_then(|state| state.group_location())
        .unwrap_or_else(|| DatasetLocation::group_default(group_id)))
}

/// Checks a group default here: its bucket exists on this node, belongs to the group and
/// allows `auth` to write under the prefix. The generated group bucket is created on first use.
pub async fn check_default(
    context: &DriverContext,
    auth: &AuthContext,
    group_id: GroupId,
    location: &DatasetLocation,
) -> Result<(), GitError> {
    if location.bucket == default_bucket(group_id) {
        return Ok(());
    }
    let bucket = match existing(context, &location.bucket).await {
        Err(GitError::NotFound) => Err(GitError::Refused(format!(
            "bucket {} does not exist on this node; choose another storage location",
            location.bucket
        ))),
        found => found,
    }?;
    if bucket.group_id != group_id {
        return Err(GitError::Refused(format!(
            "bucket {} belongs to another group; choose a bucket of this group",
            location.bucket
        )));
    }
    permit(context, auth, &bucket, location).await
}

/// Where a new dataset stores its files: the explicit choice, else the group default, which
/// gets `<document id>/` appended to its prefix at create (`true`).
pub async fn resolve(
    context: &DriverContext,
    auth: &AuthContext,
    group_id: GroupId,
    chosen: Option<DatasetLocation>,
) -> Result<(DatasetLocation, bool), GitError> {
    if let Some(chosen) = chosen {
        precheck(context, auth, group_id, &chosen).await?;
        return Ok((chosen, false));
    }
    let default = group_default(context, group_id).await?;
    check_default(context, auth, group_id, &default).await?;
    Ok((default, true))
}

/// Records the resolved location of a just-created dataset, so later group default
/// changes never move it. A location not recorded here can be set with PUT later.
pub async fn establish(
    context: &DriverContext,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    (location, shared): (DatasetLocation, bool),
) {
    let location = match shared {
        true => location.for_dataset(document.document_id),
        false => location,
    };
    // A dataset without a record already uses exactly this location, so none is written.
    if location == DatasetLocation::default_for(document.group_id, document.document_id) {
        return;
    }
    if let Err(error) = record(context, auth, document, location).await {
        tracing::warn!(document_id = %document.document_id, %error,
            "Chosen storage location not recorded at create");
    }
}

async fn existing(context: &DriverContext, name: &str) -> Result<BucketInfo, GitError> {
    match drive(GetBucketOperation::new(name.to_string()), context).await {
        Ok(bucket) => Ok(bucket),
        Err(GetBucketError::NotFound) => Err(GitError::NotFound),
        Err(_) => Err(GitError::Unavailable),
    }
}

async fn permit(
    context: &DriverContext,
    auth: &AuthContext,
    bucket: &BucketInfo,
    location: &DatasetLocation,
) -> Result<(), GitError> {
    let node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    let prefix = location.prefix.trim_end_matches('/');
    let name = &location.bucket;
    let path = if prefix.is_empty() {
        bucket_permission_path(auth.realm_id, bucket.group_id, node, name)
    } else {
        object_permission_path(auth.realm_id, bucket.group_id, node, name, prefix)
    };
    authorize(
        context,
        auth.realm_id,
        auth,
        &path,
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await?;
    Ok(())
}
