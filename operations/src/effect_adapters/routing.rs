//! Builds the routing snapshot, gate context and quota-marked catalog for a config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::routing::{
    BackendCatalog, GroupRoutingInputs, NodeRouting, RoutingSnapshot, StorageRoutingRule,
};
use aruna_core::structs::storage::usage::{UsageCounters, usage_backend_keys};
use aruna_core::types::GroupId;
use thiserror::Error;
use tracing::warn;

use crate::driver::DriverContext;
use crate::groups::backends::{RecordReadError, parse_read};
use crate::groups::storage_routing::{GroupInputsError, GroupInputsOperation};
use crate::placement::policy::GateContext;

/// Node-local routing inputs for a caller assembling an operation config.
/// Pure in-memory state: operations never fetch this from inside a step.
pub fn node_routing(context: &DriverContext) -> NodeRouting {
    context
        .blob_handle
        .as_ref()
        .map(|handle| handle.routing())
        .unwrap_or_default()
}

/// Why a write could not learn where it belongs. Absent records are not an
/// error; only an unreadable or undecodable one is.
#[derive(Debug, Error, PartialEq)]
pub enum RoutingInputsError {
    #[error("group routing inputs unavailable: {0}")]
    GroupInputs(#[from] GroupInputsError),
    #[error("bucket routing rules unavailable: {0}")]
    BucketRules(#[from] RecordReadError),
    /// Not `#[from]`: `BucketRules` already owns the conversion from a read.
    #[error("backend usage counters unavailable: {0}")]
    BackendUsage(#[source] RecordReadError),
    #[error("node placement subject unavailable: {0}")]
    NodeSubject(#[source] RecordReadError),
}

impl RoutingInputsError {
    /// The underlying storage failure, so retrying callers can tell a transient
    /// read failure from a record that will never decode.
    pub fn storage(&self) -> Option<&StorageError> {
        let read = match self {
            Self::GroupInputs(GroupInputsError::Read(read)) => read,
            Self::BucketRules(read) | Self::BackendUsage(read) | Self::NodeSubject(read) => read,
            Self::GroupInputs(GroupInputsError::Incomplete) => return None,
        };
        match read {
            RecordReadError::Storage(error) => Some(error),
            RecordReadError::Conversion(_) | RecordReadError::Unexpected => None,
        }
    }
}

/// The group's default target plus the ids of the backends it registered. Only
/// the named group's ids are ever loaded.
async fn group_inputs(
    context: &DriverContext,
    group_id: GroupId,
) -> Result<GroupRoutingInputs, RoutingInputsError> {
    Ok(crate::driver::drive(GroupInputsOperation::new(group_id), context).await?)
}

/// Bucket rules for callers that do not already hold the bucket record. A
/// bucket without a record simply has no rules.
async fn bucket_rules(
    context: &DriverContext,
    bucket: &str,
) -> Result<Vec<StorageRoutingRule>, RoutingInputsError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await;
    Ok(parse_read(event, BucketInfo::from_bytes)?
        .map(|info| info.storage_routing)
        .unwrap_or_default())
}

/// Wall clock for the cache freshness a gate is built with. Operations stay
/// sans-I/O by taking it as configuration.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or_default()
}

/// The destination this node evaluates governed writes and serves against. `None`
/// means no subject was ever advertised, failing governed operations closed; a node
/// revalidating inventory reports `admitting: false` to stop governed writes.
pub async fn gate_context(
    context: &DriverContext,
    realm_id: RealmId,
    now_ms: u64,
) -> Result<Option<GateContext>, GateContextError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
            key: NODE_SUBJECT_KEY.to_vec().into(),
            txn_id: None,
        })
        .await;
    let Some(record) = parse_read(event, NodeSubjectRecord::from_bytes)
        .map_err(RoutingInputsError::NodeSubject)?
    else {
        return Ok(None);
    };
    let admitting = !record.serving_blocked && !record.policy_draining;
    Ok(Some(GateContext {
        realm_id,
        subject: record.subject,
        now_ms,
        admitting,
    }))
}

/// Why a caller could not build a destination gate. An admission stop is not
/// one: the gate is built either way and `write_gate` refuses a governed write.
#[derive(Debug, Error, PartialEq)]
pub enum GateContextError {
    #[error(transparent)]
    Routing(#[from] RoutingInputsError),
}

/// Routing inputs for one bucket write, assembled before the operation starts.
/// Failing here fails the write: a partial snapshot would route it to the node
/// default and D3/D4 record that choice for good.
pub async fn routing_snapshot(
    context: &DriverContext,
    group_id: GroupId,
    bucket: &str,
) -> Result<RoutingSnapshot, RoutingInputsError> {
    let snapshot = node_routing(context)
        .snapshot(group_id)
        .with_group_inputs(group_inputs(context, group_id).await?)
        .with_bucket_rules(bucket_rules(context, bucket).await?);
    mark_full_backends(context, snapshot).await
}

/// The same inputs when the caller already holds the bucket record, as the S3
/// surface does from its auth middleware.
pub async fn bucket_snapshot(
    context: &DriverContext,
    bucket: &BucketInfo,
) -> Result<RoutingSnapshot, RoutingInputsError> {
    let snapshot = node_routing(context)
        .snapshot(bucket.group_id)
        .with_group_inputs(group_inputs(context, bucket.group_id).await?)
        .with_bucket_rules(bucket.storage_routing.clone());
    mark_full_backends(context, snapshot).await
}

/// Node routing whose capped backends already carry their fullness, for the
/// background writers that build their own snapshot later. Replication reads
/// the same catalog, so an unreadable counter refuses it too and it retries.
pub async fn quota_marked_routing(
    context: &DriverContext,
) -> Result<NodeRouting, RoutingInputsError> {
    let routing = node_routing(context);
    let catalog = mark_full_catalog(context, routing.catalog.clone()).await?;
    Ok(NodeRouting { catalog, ..routing })
}

async fn mark_full_backends(
    context: &DriverContext,
    snapshot: RoutingSnapshot,
) -> Result<RoutingSnapshot, RoutingInputsError> {
    let catalog = mark_full_catalog(context, snapshot.catalog.clone()).await?;
    Ok(RoutingSnapshot {
        catalog,
        ..snapshot
    })
}

/// Freezes each capped backend's fullness for one request, exactly like the
/// group quota ceiling. Concurrent writes can overshoot by their own bytes; an
/// unreadable counter fails the caller rather than routing past the cap.
async fn mark_full_catalog(
    context: &DriverContext,
    catalog: BackendCatalog,
) -> Result<BackendCatalog, RoutingInputsError> {
    let quotas = catalog.quotas();
    if quotas.is_empty() {
        return Ok(catalog);
    }
    let mut catalog = catalog;
    for (name, quota) in quotas {
        let used = backend_used_bytes(context, &BackendRef::Node(name.clone()))
            .await
            .map_err(RoutingInputsError::BackendUsage)?;
        if used >= quota {
            warn!(backend = %name, quota_bytes = quota, "Storage backend reached its quota");
            catalog = catalog.mark_full(&name);
        }
    }
    Ok(catalog)
}

/// Sums one backend's stored-byte shards. A missing row reads as zero, so a node
/// whose counters were never built reports no usage; an unreadable or
/// undecodable shard is an error, never a zero.
pub async fn backend_used_bytes(
    context: &DriverContext,
    backend: &BackendRef,
) -> Result<u64, RecordReadError> {
    let reads = usage_backend_keys(backend)
        .into_iter()
        .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
        .collect::<Vec<_>>();
    let values = match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(RecordReadError::Unexpected),
    };
    let mut total = 0u64;
    for (_, value) in values {
        let Some(value) = value else { continue };
        total = total.saturating_add(UsageCounters::from_bytes(value.as_ref())?.stored_bytes);
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::staging::setup_driver_context;
    use aruna_core::UserId;
    use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, STORAGE_ROUTING_KEYSPACE};
    use aruna_core::structs::placement::policy::PlacementSubject;
    use aruna_core::structs::storage::blob::{BackendRef, ResolvedBackend};
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::structs::storage::routing::{
        GroupStorageRouting, RoutingTarget, StorageRoutingRule, resolve_backend,
    };
    use aruna_core::structs::storage::usage::{UsageCounters, usage_backend_key};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    async fn write_value(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn register(context: &DriverContext, group_id: Ulid) -> Ulid {
        let record = GroupStorage {
            backend_id: Ulid::generate(),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        };
        for (key_space, key, value) in crate::groups::backends::record_writes(&record).unwrap() {
            write_value(context, &key_space, key.to_vec(), value.to_vec()).await;
        }
        record.backend_id
    }

    async fn set_default(context: &DriverContext, group_id: Ulid, backend_id: Ulid) {
        let record = GroupStorageRouting {
            group_id,
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
            updated_at: SystemTime::UNIX_EPOCH,
            updated_by: Default::default(),
        };
        write_value(
            context,
            STORAGE_ROUTING_KEYSPACE,
            group_id.to_bytes().to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;
    }

    fn bucket(group_id: Ulid) -> BucketInfo {
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
    }

    #[tokio::test]
    async fn routes_group_backend() {
        // The catalog is built the way production builds it, so a group default
        // naming a registered backend has to resolve rather than fail.
        let test = setup_driver_context().await;
        let group_id = Ulid::generate();
        let backend_id = register(&test.driver_context, group_id).await;
        set_default(&test.driver_context, group_id, backend_id).await;

        let snapshot = routing_snapshot(&test.driver_context, group_id, "b")
            .await
            .unwrap();

        assert_eq!(
            resolve_backend(&snapshot, "b", "k").unwrap(),
            ResolvedBackend::new(BackendRef::Group(backend_id), None)
        );
    }

    #[tokio::test]
    async fn scopes_catalog() {
        // Another group's backend must never enter this group's catalog.
        let test = setup_driver_context().await;
        let group_id = Ulid::generate();
        let foreign = register(&test.driver_context, Ulid::generate()).await;
        set_default(&test.driver_context, group_id, foreign).await;

        let snapshot = routing_snapshot(&test.driver_context, group_id, "b")
            .await
            .unwrap();

        assert!(resolve_backend(&snapshot, "b", "k").is_err());
    }

    #[tokio::test]
    async fn snapshot_loads_group() {
        // A caller holding the bucket record still has to pick up the group's
        // default target and backend ids.
        let test = setup_driver_context().await;
        let group_id = Ulid::generate();
        let backend_id = register(&test.driver_context, group_id).await;
        set_default(&test.driver_context, group_id, backend_id).await;

        let snapshot = bucket_snapshot(&test.driver_context, &bucket(group_id))
            .await
            .unwrap();

        assert_eq!(
            resolve_backend(&snapshot, "b", "k").unwrap(),
            ResolvedBackend::new(BackendRef::Group(backend_id), None)
        );
    }

    async fn direct_context() -> (tempfile::TempDir, DriverContext) {
        let dir = tempfile::tempdir().unwrap();
        let storage_handle =
            aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (dir, context)
    }

    #[tokio::test]
    async fn snapshot_reads_rules() {
        // The snapshot seam has to pick up both stored scopes, not stay empty.
        let (_dir, context) = direct_context().await;
        let group_id = Ulid::generate();
        let rule = StorageRoutingRule {
            key_prefix: "archive/".to_string(),
            exact: false,
            target: RoutingTarget::Class("cold".to_string()),
        };
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            cors_configuration: None,
            storage_routing: vec![rule.clone()],
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        let record = GroupStorageRouting {
            group_id,
            default_target: Some(RoutingTarget::Class("archive".to_string())),
            updated_at: SystemTime::UNIX_EPOCH,
            updated_by: UserId::default(),
        };
        write_value(
            &context,
            S3_BUCKET_KEYSPACE,
            b"routed".to_vec(),
            info.to_bytes().unwrap(),
        )
        .await;
        write_value(
            &context,
            STORAGE_ROUTING_KEYSPACE,
            group_id.to_bytes().to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;

        let snapshot = routing_snapshot(&context, group_id, "routed")
            .await
            .unwrap();
        assert_eq!(snapshot.bucket_rules, vec![rule.clone()]);
        assert_eq!(snapshot.group_default, record.default_target);

        let known = bucket_snapshot(&context, &info).await.unwrap();
        assert_eq!(known.bucket_rules, vec![rule]);
        assert_eq!(known.group_default, record.default_target);

        // An unwritten group and bucket are normal empty state, never an error.
        let absent = routing_snapshot(&context, Ulid::generate(), "missing")
            .await
            .unwrap();
        assert!(absent.bucket_rules.is_empty());
        assert_eq!(absent.group_default, None);
    }

    #[tokio::test]
    async fn sums_backend_shards() {
        // Fullness is measured over every shard of one backend, and only that one.
        let (_dir, context) = direct_context().await;
        let counters = |bytes| UsageCounters {
            stored_bytes: bytes,
            ..Default::default()
        };
        for (backend, shard, bytes) in [
            (BackendRef::node_default(), 0, 10u64),
            (BackendRef::node_default(), 5, 7),
            (BackendRef::Node("cold".to_string()), 0, 100),
        ] {
            write_value(
                &context,
                USAGE_STATS_KEYSPACE,
                usage_backend_key(&backend, shard),
                counters(bytes).to_bytes().unwrap(),
            )
            .await;
        }

        assert_eq!(
            backend_used_bytes(&context, &BackendRef::node_default())
                .await
                .unwrap(),
            17
        );
        assert_eq!(
            backend_used_bytes(&context, &BackendRef::Node("gone".to_string()))
                .await
                .unwrap(),
            0
        );

        // One undecodable shard must fail the read, not read as zero usage.
        write_value(
            &context,
            USAGE_STATS_KEYSPACE,
            usage_backend_key(&BackendRef::node_default(), 1),
            vec![0xff; 8],
        )
        .await;
        assert!(
            backend_used_bytes(&context, &BackendRef::node_default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn blocked_subject_reads() {
        // A blocked node still reports its subject: only a write carrying refs
        // is stopped, and that decision belongs to the gate.
        let (_dir, context) = direct_context().await;
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let subject = PlacementSubject {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            generation: 1,
            location: "eu-west".to_string(),
            labels: Default::default(),
            executor_kind: None,
            local_to_controller: true,
        };
        // A node that never advertised a subject has no gate at all.
        assert_eq!(gate_context(&context, realm_id, 0).await.unwrap(), None);

        let mut record = NodeSubjectRecord::seed(subject).unwrap();
        record.serving_blocked = true;
        record.policy_draining = true;
        write_value(
            &context,
            NODE_SUBJECT_KEYSPACE,
            NODE_SUBJECT_KEY.to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;

        let gate = gate_context(&context, realm_id, 0)
            .await
            .unwrap()
            .expect("subject is advertised");
        assert!(!gate.admitting);
    }

    #[tokio::test]
    async fn snapshot_fails_corrupt() {
        // A bucket record that will not decode must fail the write instead of
        // routing it to the node default.
        let (_dir, context) = direct_context().await;
        write_value(
            &context,
            S3_BUCKET_KEYSPACE,
            b"corrupt".to_vec(),
            vec![0xff; 8],
        )
        .await;

        let result = routing_snapshot(&context, Ulid::generate(), "corrupt").await;

        assert!(matches!(result, Err(RoutingInputsError::BucketRules(_))));
    }
}
