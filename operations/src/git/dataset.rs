//! Stores the data files of plain RO-Crate pushes in the dataset's storage location.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::objects::{self, Blob};
use super::project::author;
use super::snapshot::execute;
use super::{GitError, lfs, location, publish};
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::realm::get_config::GetConfigOperation;
use crate::s3::object::copy::{CopyObjectInput, CopyReferences, CopySourceConditions, copy_object};
use aruna_blob::git::GitStore;
use aruna_core::git::{FileChangeKind, GitEffect, GitEvent, LfsObject, PendingMerge, StoredObject};
use aruna_core::repo_layout::{Layout, StoredFile};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::data_identity::ObjectLocation;
use aruna_core::structs::storage::dataset_location::DatasetLocation;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

/// What a push to main stores: the location, its bucket here, and the changed files.
pub(super) struct PushTarget {
    location: DatasetLocation,
    bucket: Option<BucketInfo>,
    changed: BTreeSet<String>,
    /// Files stored so far, reused when the metadata update is retried.
    pub(super) stored: BTreeMap<String, StoredFile>,
}

/// The storage target of a plain RO-Crate push, or `None` for an ARC push. A location whose
/// bucket is missing here or not writable leaves the files unstored until a later push.
pub(super) async fn target(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
    merge: &PendingMerge,
) -> Result<Option<PushTarget>, GitError> {
    let layout = GitEffect::Layout {
        document_id: document.document_id,
        revision: merge.new.clone(),
    };
    if !matches!(
        execute(store, layout, merge.user_id).await?,
        GitEvent::Layout(Some(Layout::RoCrate))
    ) {
        return Ok(None);
    }
    let (location, _) = location::effective(context, document).await?;
    let auth = author(merge.user_id);
    let bucket = match location::writable(context, &auth, document, &location).await {
        Ok(bucket) => Some(bucket),
        Err(error) => {
            tracing::warn!(document_id = %document.document_id, %error,
                "Pushed files stay unstored; the storage location is not writable here");
            None
        }
    };
    let diff = GitEffect::Diff {
        document_id: document.document_id,
        from: (merge.old != aruna_core::git::ZERO_OID).then(|| merge.old.clone()),
        to: merge.new.clone(),
    };
    let GitEvent::Diff(changes) = execute(store, diff, merge.user_id).await? else {
        return Err(GitError::Unavailable);
    };
    let changed = changes
        .into_iter()
        .filter(|change| change.change != FileChangeKind::Deleted)
        .map(|change| change.path)
        .collect();
    Ok(Some(PushTarget {
        location,
        bucket,
        changed,
        stored: BTreeMap::new(),
    }))
}

impl PushTarget {
    /// The location of this push, for matching pushed paths to stored entities.
    pub(super) fn location(&self) -> &DatasetLocation {
        &self.location
    }

    /// Stores every file the merged graph still needs and names the entities by content.
    /// Returns the rewritten graph when anything changed.
    pub(super) async fn apply(
        &mut self,
        context: &DriverContext,
        store: &GitStore,
        document: &MetadataRegistryRecord,
        merge: &PendingMerge,
        jsonld: &str,
    ) -> Result<Option<String>, GitError> {
        let Some(bucket) = self.bucket.clone() else {
            return Ok(None);
        };
        let mut value: serde_json::Value =
            serde_json::from_str(jsonld).map_err(|_| GitError::Invalid)?;
        let paths = aruna_core::repo_layout::unstored_paths(&value, &self.changed);
        for path in paths {
            if self.stored.contains_key(&path) {
                continue;
            }
            let file = Box::pin(self.store_file(context, store, document, merge, &bucket, &path));
            if let Some(file) = file.await? {
                self.stored.insert(path, file);
            }
        }
        Ok(
            aruna_core::repo_layout::name_stored(&mut value, &self.stored)
                .then(|| value.to_string()),
        )
    }

    /// Stores one repository file at its key; `None` when the commit has no such file or
    /// its LFS content is unknown here.
    async fn store_file(
        &self,
        context: &DriverContext,
        store: &GitStore,
        document: &MetadataRegistryRecord,
        merge: &PendingMerge,
        bucket: &BucketInfo,
        path: &str,
    ) -> Result<Option<StoredFile>, GitError> {
        let read = GitEffect::ReadFile {
            document_id: document.document_id,
            revision: merge.new.clone(),
            path: path.to_string(),
        };
        let GitEvent::File(bytes) = execute(store, read, merge.user_id).await? else {
            return Err(GitError::Unavailable);
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let auth = author(merge.user_id);
        let key = self.location.key(path);
        let name = self.location.bucket.as_str();
        let node = context
            .net_handle
            .as_ref()
            .map(|net| net.node_id())
            .ok_or(GitError::Unavailable)?;
        let target = object_permission_path(auth.realm_id, bucket.group_id, node, name, &key);
        let extras = PolicyRequestExtras::operation("s3.PutObject");
        authorize(
            context,
            auth.realm_id,
            &auth,
            &target,
            &Permission::WRITE,
            extras,
        )
        .await?;
        let content = match LfsObject::from_pointer(&bytes) {
            Some(pointer) => match lfs::locate(context, document, &pointer.oid).await? {
                Some(source) => Content::Lfs(source),
                None => return Ok(None),
            },
            None => Content::Blob(bytes),
        };
        let sha256 = content.sha256();
        let existing = objects::described(context, (name, bucket.group_id), &key, None).await?;
        let stored = match existing.filter(|existing| existing.sha256 == sha256) {
            Some(existing) => existing,
            None => {
                let target = (name, bucket.clone(), key.clone());
                Box::pin(content.write(context, &auth, document, target)).await?
            }
        };
        Ok(Some(StoredFile {
            hash: stored.blake3,
            location: ObjectLocation {
                bucket: stored.bucket,
                key: stored.key,
            },
            size: stored.size,
        }))
    }
}

/// The bytes of a pushed file: a plain Git blob, or the LFS object its pointer names.
enum Content {
    Blob(Bytes),
    Lfs(StoredObject),
}

impl Content {
    fn sha256(&self) -> String {
        match self {
            Self::Blob(bytes) => aruna_blob::hash::Hasher::new_with_bytes(bytes)
                .to_map()
                .get("sha256")
                .map(hex::encode)
                .unwrap_or_default(),
            Self::Lfs(object) => object.sha256.clone(),
        }
    }

    /// Copies LFS content held here on the server; other content streams through a put.
    async fn write(
        self,
        context: &DriverContext,
        auth: &AuthContext,
        document: &MetadataRegistryRecord,
        (name, bucket, key): (&str, BucketInfo, String),
    ) -> Result<StoredObject, GitError> {
        let sha256 = self.sha256();
        let (size, body): (u64, Blob) = match self {
            Self::Blob(bytes) => {
                let size = bytes.len() as u64;
                let body =
                    BackendStream::new(futures_util::stream::iter([Ok::<_, StreamError>(bytes)]));
                (size, body)
            }
            Self::Lfs(source) => {
                let own = objects::copy(context, document, &source.sha256).await?;
                let node = context.net_handle.as_ref().map(|net| net.node_id());
                let local = own.or((Some(source.node_id) == node).then_some(source.clone()));
                if let Some(local) = local
                    && let Some(group_id) = local.group_id
                {
                    return copied(context, auth, (&local, group_id), (name, bucket, key)).await;
                }
                let holders = publish::holders(context, document).await?;
                let blob = objects::open(context, auth, document, &source, &holders).await?;
                (source.size, blob)
            }
        };
        objects::put(context, auth, (name, bucket), (key, size, &sha256), body).await
    }
}

/// A server-side copy of an object of this node, authorized as a read of the source.
async fn copied(
    context: &DriverContext,
    auth: &AuthContext,
    (source, source_group): (&StoredObject, aruna_core::types::GroupId),
    (name, bucket, key): (&str, BucketInfo, String),
) -> Result<StoredObject, GitError> {
    let node = source.node_id;
    let path = object_permission_path(
        auth.realm_id,
        source_group,
        node,
        &source.bucket,
        &source.key,
    );
    let extras = PolicyRequestExtras::operation("s3.GetObject");
    authorize(
        context,
        auth.realm_id,
        auth,
        &path,
        &Permission::READ,
        extras,
    )
    .await?;
    let quota = drive(GetConfigOperation::new(auth.realm_id), context)
        .await
        .map_err(|_| GitError::Unavailable)?
        .quota
        .effective_group_ceiling(&bucket.group_id);
    let input = CopyObjectInput {
        source_bucket: source.bucket.clone(),
        source_key: source.key.clone(),
        source_version_id: Some(source.version_id),
        source_group_id: source_group,
        source_auth_context: auth.clone(),
        dest_bucket: name.to_string(),
        dest_key: key.clone(),
        user_id: auth.user_id,
        group_id: bucket.group_id,
        realm_id: auth.realm_id,
        node_id: node,
        quota_ceiling: quota,
        conditions: CopySourceConditions::default(),
        metadata: None,
        restrictions: auth.path_restrictions.clone(),
        references: CopyReferences::Materialize,
    };
    let result = copy_object(context, input)
        .await
        .map_err(|_| GitError::Unavailable)?;
    objects::described(
        context,
        (name, bucket.group_id),
        &key,
        Some(result.version_id),
    )
    .await?
    .ok_or(GitError::Unavailable)
}
