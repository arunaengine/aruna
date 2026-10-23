//! Tells where the buckets of a group store new uploads, one page of buckets at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::{DriverContext, drive};
use crate::effect_adapters::routing::node_routing;
use crate::groups::backends::query::GetBackendOperation;
use crate::groups::bucket_jobs::{ActiveJob, active_job};
use crate::groups::storage_routing::GroupInputsOperation;
use crate::s3::bucket::list::{ListBucketsInput, ListBucketsOperation};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::group_backend::{GroupStorage, path_bucket};
use aruna_core::structs::storage::routing::{RoutingSnapshot, resolve_backend};
use aruna_core::types::GroupId;
use std::collections::HashMap;
use ulid::Ulid;

/// Where one bucket stores new uploads, and the job that runs or is paused on it.
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
    pub job: Option<ActiveJob>,
}

/// Links and mapping rows of a group the listing reads at most.
const LINK_CAP: usize = 10_000;

/// One page of the group's buckets with their storage and job, and the next page's cursor. The
/// group's routing inputs load once, as for one upload; each bucket resolves like an upload.
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
    let inputs = GroupInputsOperation::new(group_id).capped(LINK_CAP);
    let inputs = drive(inputs, context)
        .await
        .map_err(|error| error.to_string())?;
    let encrypted = encrypted_buckets(context, &page.buckets).await?;
    // Jobs show for every rule, also one whose write storage routing leaves out.
    let rules = crate::groups::imports::list_imports(context, group_id).await;
    let rules = rules.map_err(|error| error.to_string())?;
    let mut snapshot = node_routing(context)
        .snapshot(group_id)
        .with_group_inputs(inputs);
    let mut storages = HashMap::new();
    let mut found = Vec::with_capacity(page.buckets.len());
    for ((bucket, info), encrypted) in page.buckets.into_iter().zip(encrypted) {
        snapshot.bucket_rules = info.storage_routing;
        snapshot.compression = info.compression;
        let encoded = encrypted || info.compression != Compression::Off;
        let destination = destination(&snapshot, &bucket, encoded);
        if let Some(id) = destination.backend_id
            && let std::collections::hash_map::Entry::Vacant(entry) = storages.entry(id)
        {
            let record = drive(GetBackendOperation::new(id), context).await;
            let record = record.map_err(|error| error.to_string())?;
            entry.insert(record.filter(|record| record.group_id == group_id));
        }
        let job = active_job(context, &bucket, &rules).await?;
        found.push(BucketStorage {
            job,
            ..named(destination, &storages)
        });
    }
    Ok((found, page.continuation_token))
}

/// Whether each bucket of the page encrypts its new uploads.
async fn encrypted_buckets(
    context: &DriverContext,
    buckets: &[(String, BucketInfo)],
) -> Result<Vec<bool>, String> {
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
    let reads = (buckets.iter())
        .map(|(bucket, _)| {
            let key = bucket.as_bytes().to_vec().into();
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key)
        })
        .collect();
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    let Event::Storage(StorageEvent::BatchReadResult { values }) =
        context.storage_handle.send_storage_effect(read).await
    else {
        return Err("could not read the encryption of the buckets".to_string());
    };
    values
        .into_iter()
        .map(|(_, row)| {
            let settings = BucketEncryption::from_row(row.as_deref());
            settings.map(|settings| settings.is_encrypted())
        })
        .collect::<Result<_, _>>()
        .map_err(|error| error.to_string())
}

/// Where uploads of `bucket` go: a whole-bucket link's write folder, else the backend an
/// upload resolves to. Encoded uploads keep Aruna's layout and skip the link.
fn destination(snapshot: &RoutingSnapshot, bucket: &str, encoded: bool) -> BucketStorage {
    let link =
        (snapshot.imports.iter()).find(|rule| rule.bucket == bucket && rule.key_prefix.is_empty());
    if let Some(rule) = link.filter(|_| !encoded) {
        return BucketStorage {
            bucket: bucket.to_string(),
            backend_id: Some(rule.write_backend()),
            folder: Some(rule.write_folder().to_string()),
            import_id: Some(rule.import_id),
            write_pattern: Some(rule.write_pattern.clone()),
            ..BucketStorage::default()
        };
    }
    let backend = resolve_backend(snapshot, bucket, "").map(|resolved| resolved.backend);
    BucketStorage {
        bucket: bucket.to_string(),
        backend_id: match backend {
            Ok(BackendRef::Group(id)) => Some(id),
            _ => None,
        },
        shared: matches!(backend, Ok(BackendRef::Group(_))),
        import_id: link.map(|rule| rule.import_id),
        write_pattern: link.map(|rule| rule.write_pattern.clone()),
        ..BucketStorage::default()
    }
}

/// Adds the S3 bucket or container the files go to, where the storage record tells it.
fn named(
    mut found: BucketStorage,
    storages: &HashMap<Ulid, Option<GroupStorage>>,
) -> BucketStorage {
    let record = found
        .backend_id
        .and_then(|id| storages.get(&id))
        .and_then(Option::as_ref);
    found.provider_bucket = record
        .and_then(|record| match (record.splits(), found.folder.as_deref()) {
            (true, Some(folder)) => path_bucket(folder),
            (true, None) => record.default_bucket(),
            (false, _) => record.container(),
        })
        .map(str::to_string);
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::backend_import::{BackendImport, PLAIN_PATTERN};
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use aruna_core::structs::storage::routing::{
        BackendCatalog, GroupRoutingInputs, RoutingTarget, StorageRoutingRule,
    };
    use std::time::SystemTime;

    const BACKEND: Ulid = Ulid::from_parts(2, 2);
    const WRITES: Ulid = Ulid::from_parts(4, 4);

    fn storage(id: Ulid) -> GroupStorage {
        GroupStorage {
            backend_id: id,
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
            write_backend_id: Some(WRITES),
            write_prefix: Some("out/".to_string()),
            write_pattern: PLAIN_PATTERN.to_string(),
        }
    }

    #[test]
    fn shows_write_destinations() {
        let storages = HashMap::from([(WRITES, Some(storage(WRITES)))]);
        let catalog = BackendCatalog::new("default").with_backend("default", None);
        let inputs = GroupRoutingInputs {
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(WRITES))),
            backend_ids: [BACKEND, WRITES].into(),
            imports: vec![link("genes", "genes/", ""), link("lab", "raw/", "in/")],
            ..GroupRoutingInputs::default()
        };
        let snapshot =
            RoutingSnapshot::new(Ulid::from_parts(1, 1), catalog).with_group_inputs(inputs);
        // A redirected link shows where its uploads go, not where it reads.
        let found = named(destination(&snapshot, "genes", false), &storages);
        assert_eq!(
            (
                found.backend_id,
                found.folder.as_deref(),
                found.provider_bucket.as_deref()
            ),
            (Some(WRITES), Some("out/"), Some("out"))
        );
        assert_eq!(found.import_id, Some(Ulid::from_parts(3, 3)));
        // An encoded bucket writes Aruna's layout to the shared bucket, not to its link.
        let found = named(destination(&snapshot, "genes", true), &storages);
        assert_eq!(
            (found.folder, found.shared, found.provider_bucket.as_deref()),
            (None, true, Some("aruna-spire"))
        );
        // A bucket rule naming a class this node lacks falls back to the group storage.
        let rule = StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Class("cold".to_string()),
        };
        let lab = snapshot.with_bucket_rules(vec![rule]);
        let found = named(destination(&lab, "lab", false), &storages);
        assert_eq!(
            (
                found.backend_id,
                found.shared,
                found.provider_bucket.as_deref()
            ),
            (Some(WRITES), true, Some("aruna-spire"))
        );
    }
}
