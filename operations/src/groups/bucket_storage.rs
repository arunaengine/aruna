//! Tells where the buckets of a group store new uploads, one page of buckets at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::{DriverContext, drive};
use crate::groups::backends::query::GetBackendOperation;
use crate::groups::imports::list_imports;
use crate::groups::storage_routing::GroupRoutingOperation;
use crate::s3::bucket::list::{ListBucketsInput, ListBucketsOperation};
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::group_backend::{GroupStorage, path_bucket};
use aruna_core::structs::storage::routing::RoutingTarget;
use aruna_core::types::GroupId;
use std::collections::HashMap;
use ulid::Ulid;

/// Where one bucket stores new uploads.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketStorage {
    pub bucket: String,
    /// The group storage; `None` is Aruna storage on this node.
    pub backend_id: Option<Ulid>,
    pub provider_bucket: Option<String>,
    /// The storage folder of a whole-bucket link.
    pub folder: Option<String>,
    /// Aruna names the files, in the storage's shared bucket or fixed container.
    pub shared: bool,
    pub import_id: Option<Ulid>,
    pub write_pattern: Option<String>,
}

/// One page of the group's buckets with their storage, and the cursor of the next page.
pub async fn list_bucket_storage(
    context: &DriverContext,
    group_id: GroupId,
    (after, limit): (Option<String>, usize),
) -> Result<(Vec<BucketStorage>, Option<String>), String> {
    let input = ListBucketsInput {
        group_id,
        prefix: None,
        continuation_token: after,
        max_buckets: Some(limit),
    };
    let page = drive(ListBucketsOperation::new(input), context)
        .await
        .map_err(|error| error.to_string())?;
    let imports = list_imports(context, group_id)
        .await
        .map_err(|error| error.to_string())?;
    let routing = drive(GroupRoutingOperation::new(group_id), context)
        .await
        .map_err(|error| error.to_string())?;
    let default = routing.and_then(|routing| routing.default_target);
    let mut storages = HashMap::new();
    let mut found = Vec::with_capacity(page.buckets.len());
    for (bucket, info) in page.buckets {
        let target = bucket_target(&info).or(default.as_ref());
        let backend = match target {
            Some(RoutingTarget::Backend(BackendRef::Group(id))) => Some(*id),
            _ => None,
        };
        for id in imports.iter().map(|rule| rule.backend_id).chain(backend) {
            if let std::collections::hash_map::Entry::Vacant(entry) = storages.entry(id) {
                let record = drive(GetBackendOperation::new(id), context).await;
                let record = record.map_err(|error| error.to_string())?;
                entry.insert(record.filter(|record| record.group_id == group_id));
            }
        }
        found.push(resolve(&bucket, (&imports, target), &storages));
    }
    Ok((found, page.continuation_token))
}

/// The bucket's own rule for all of its keys, if it has one.
fn bucket_target(info: &BucketInfo) -> Option<&RoutingTarget> {
    let rules = info.storage_routing.iter();
    rules
        .filter(|rule| rule.key_prefix.is_empty() && !rule.exact)
        .map(|rule| &rule.target)
        .next()
}

/// A whole-bucket link first, else the bucket's or the group's target.
fn resolve(
    bucket: &str,
    (imports, target): (&[BackendImport], Option<&RoutingTarget>),
    storages: &HashMap<Ulid, Option<GroupStorage>>,
) -> BucketStorage {
    let storage = |id: &Ulid| storages.get(id).and_then(Option::as_ref);
    let link = imports
        .iter()
        .find(|rule| rule.bucket == bucket && rule.key_prefix.is_empty());
    if let Some(rule) = link {
        let provider = match storage(&rule.backend_id) {
            Some(record) if record.splits() => path_bucket(&rule.backend_prefix),
            Some(record) => record.container(),
            None => None,
        };
        return BucketStorage {
            bucket: bucket.to_string(),
            backend_id: Some(rule.backend_id),
            provider_bucket: provider.map(str::to_string),
            folder: Some(rule.backend_prefix.clone()),
            shared: false,
            import_id: Some(rule.import_id),
            write_pattern: Some(rule.write_pattern.clone()),
        };
    }
    let Some(RoutingTarget::Backend(BackendRef::Group(id))) = target else {
        return BucketStorage {
            bucket: bucket.to_string(),
            ..BucketStorage::default()
        };
    };
    let provider = storage(id).and_then(|record| match record.splits() {
        true => record.default_bucket(),
        false => record.container(),
    });
    BucketStorage {
        bucket: bucket.to_string(),
        backend_id: Some(*id),
        provider_bucket: provider.map(str::to_string),
        shared: true,
        ..BucketStorage::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::backend_import::PLAIN_PATTERN;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use std::time::SystemTime;

    const BACKEND: Ulid = Ulid::from_parts(2, 2);

    fn storage() -> GroupStorage {
        GroupStorage {
            backend_id: BACKEND,
            group_id: Ulid::from_parts(1, 1),
            name: "spire".to_string(),
            kind: GroupBackendKind::S3,
            public_config: [("default_bucket".to_string(), "aruna-spire".to_string())].into(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        }
    }

    fn link(bucket: &str, folder: &str, key_prefix: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::from_parts(3, 3),
            group_id: Ulid::from_parts(1, 1),
            backend_id: BACKEND,
            backend_prefix: folder.to_string(),
            bucket: bucket.to_string(),
            key_prefix: key_prefix.to_string(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: PLAIN_PATTERN.to_string(),
        }
    }

    #[test]
    fn resolves_bucket_storage() {
        let storages = HashMap::from([(BACKEND, Some(storage()))]);
        let imports = [link("genes", "genes/", ""), link("lab", "raw/", "in/")];
        let group = RoutingTarget::Backend(BackendRef::Group(BACKEND));
        let found = resolve("genes", (&imports, Some(&group)), &storages);
        assert_eq!(
            (
                found.provider_bucket.as_deref(),
                found.import_id,
                found.shared
            ),
            (Some("genes"), Some(Ulid::from_parts(3, 3)), false)
        );
        // A folder link is no whole-bucket link: the bucket uses the shared bucket.
        let found = resolve("lab", (&imports, Some(&group)), &storages);
        assert_eq!(
            (
                found.provider_bucket.as_deref(),
                found.import_id,
                found.shared
            ),
            (Some("aruna-spire"), None, true)
        );
        let class = RoutingTarget::Class("cold".to_string());
        let found = resolve("scratch", (&imports, Some(&class)), &storages);
        assert_eq!((found.backend_id, found.shared), (None, false));
    }
}
