//! Owns a document's persistent identifier state, moving it from requested to active.
//! Each step is a compare-and-set that also queues its publish, so replay cannot mint twice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "persistent_id_forward.rs"]
pub mod forward;

use aruna_core::UserId;
use aruna_core::handle::Handle;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::{PersistentIdFailure, PersistentIdMapping};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::id_mapping::{
    MappingRoute, PersistentIdError, mapping_route_for, parse_mapping_read, read_mapping_effect,
};
use crate::metadata::pid_transition::{PidTransition, PidTransitionOperation};
use crate::realm::load_config::load_realm_config;
use crate::sync::outbox_record::schedule_drain_effect;

pub async fn read_mapping(
    ctx: &DriverContext,
    document_id: Ulid,
) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    let event = ctx
        .storage_handle
        .send_effect(read_mapping_effect(document_id, None))
        .await;
    parse_mapping_read(event)
}

/// Activate the Conceptual PID selected by the create transaction. Returns the
/// authoritative mapping and whether this call activated it; if projection has
/// not produced the registry row, `Processing` is committed and the caller defers.
pub async fn mint_persistent_id(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
    minted_by: aruna_core::UserId,
    minted_at_ms: u64,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let route = mapping_route(ctx, realm_id, document_id).await?;
    let transition = PidTransition::Mint {
        minted_by,
        minted_at_ms,
    };
    run_transition(ctx, route, document_id, transition).await
}

/// Adds external identifiers on this node's row; callers route to the PID authority first.
pub(crate) async fn add_secondary_ids(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
    identifiers: Vec<aruna_core::structs::secondary_id::SecondaryIdentifier>,
    occurred_at_ms: u64,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let route = mapping_route(ctx, realm_id, document_id).await?;
    let result = crate::driver::drive(
        crate::metadata::secondary_ids::AddIdentifiersOperation::new(
            crate::metadata::secondary_ids::AddIdentifiersInput {
                document_id,
                identifiers,
                route,
                occurred_at_ms,
            },
        ),
        ctx,
    )
    .await?;
    if result.1 {
        schedule_drain(ctx).await;
    }
    Ok(result)
}

/// Record a terminal provider failure on the same intent. Projection absence
/// must never call this path; the mint handler uses the non-terminal Processing
/// transition above for that condition.
pub(crate) async fn fail_persistent_id(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
    failure: PersistentIdFailure,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let route = mapping_route(ctx, realm_id, document_id).await?;
    run_transition(ctx, route, document_id, PidTransition::Fail { failure }).await
}

/// Exceptional administrator-only withdrawal. Authorization is enforced at
/// both routing hops; the transition stores the actor and required reason and
/// writes the generic metadata audit row in the same transaction.
pub(crate) async fn admin_withdraw_pid(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
    withdrawn_by: UserId,
    reason: String,
    withdrawn_at_ms: u64,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let route = mapping_route(ctx, realm_id, document_id).await?;
    let transition = PidTransition::Withdraw {
        withdrawn_by,
        reason,
        withdrawn_at_ms,
    };
    run_transition(ctx, route, document_id, transition).await
}

/// Drives one transition and schedules the outbox drain after its commit.
async fn run_transition(
    ctx: &DriverContext,
    route: Option<MappingRoute>,
    document_id: Ulid,
    transition: PidTransition,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let operation = PidTransitionOperation::new(route, document_id, transition);
    let outcome = drive(operation, ctx).await?;
    if outcome.committed {
        schedule_drain(ctx).await;
    }
    Ok((outcome.mapping, outcome.applied))
}

pub(crate) async fn mapping_route(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<Option<MappingRoute>, PersistentIdError> {
    let Some(net_handle) = ctx.net_handle.as_ref() else {
        return Ok(None);
    };
    let config = load_realm_config(ctx, realm_id).await.ok_or_else(|| {
        PersistentIdError::Unavailable("realm placement config is unavailable".to_string())
    })?;
    mapping_route_for(&config, realm_id, document_id, net_handle.node_id())
        .ok_or_else(|| {
            PersistentIdError::Unavailable("persistent id placement is unavailable".to_string())
        })
        .map(Some)
}

async fn schedule_drain(ctx: &DriverContext) {
    if let Some(task_handle) = ctx.task_handle.as_ref() {
        task_handle.send_effect(schedule_drain_effect()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::id_mapping::{TransitionEntry, mapping_revision, transition_entries};
    use crate::metadata::repository::metadata_audit_key;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::METADATA_AUDIT_KEYSPACE;
    use aruna_core::storage_entries::registry_write_entries;
    use aruna_core::structs::PersistentIdStatus;
    use aruna_core::structs::execution::job::JobId;
    use aruna_core::structs::identity::realm::RealmConfigDocument;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::{
        MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord,
    };
    use aruna_storage::storage;
    use tempfile::tempdir;

    fn context() -> (DriverContext, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let context = context_at(dir.path().to_str().unwrap());
        (context, dir)
    }

    fn context_at(path: &str) -> DriverContext {
        DriverContext {
            storage_handle: storage::FjallStorage::open(path).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn realm() -> RealmId {
        RealmId([3; 32])
    }

    fn user() -> aruna_core::UserId {
        aruna_core::UserId::local(Ulid::from_bytes([2; 16]), realm())
    }

    fn record(document_id: Ulid) -> MetadataRegistryRecord {
        let group_id = Ulid::from_bytes([8; 16]);
        MetadataRegistryRecord {
            realm_id: realm(),
            group_id,
            document_id,
            document_path: "doc".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm(),
                group_id,
                "doc",
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_bytes([1; 16]),
            last_event_id: Ulid::from_bytes([1; 16]),
        }
    }

    async fn seed_record(ctx: &DriverContext, document_id: Ulid) {
        let writes = registry_write_entries(&record(document_id)).unwrap();
        match ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected registry seed event: {other:?}"),
        }
    }

    async fn seed_intent(ctx: &DriverContext, document_id: Ulid, profile: bool) {
        let mapping = PersistentIdMapping::requested(
            document_id,
            profile,
            user(),
            JobId::from_bytes([7; 16]),
            true,
            record(document_id).permission_path,
            mapping_revision(&None, 1),
        );
        match ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes: transition_entries(&None, &mapping).unwrap(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected intent seed event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mint_is_idempotent() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([5; 16]);
        seed_record(&ctx, id).await;
        seed_intent(&ctx, id, false).await;
        let (first, minted_first) = mint_persistent_id(&ctx, realm(), id, user(), 1)
            .await
            .unwrap();
        assert!(minted_first);
        let (second, minted_second) = mint_persistent_id(&ctx, realm(), id, user(), 9)
            .await
            .unwrap();
        assert!(!minted_second);
        assert_eq!(first.pid, second.pid);
        assert_eq!(second.minted_at_ms, Some(1));
    }

    #[tokio::test]
    async fn absence_stays_processing() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([6; 16]);
        seed_intent(&ctx, id, false).await;
        let (mapping, activated) = mint_persistent_id(&ctx, realm(), id, user(), 2)
            .await
            .unwrap();
        assert!(!activated);
        assert_eq!(mapping.status, PersistentIdStatus::Processing);
        assert!(
            mapping
                .failure
                .as_ref()
                .is_some_and(|failure| failure.retryable)
        );
        assert_eq!(read_mapping(&ctx, id).await.unwrap(), Some(mapping));
    }

    #[tokio::test]
    async fn admin_withdraw_needs() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([12; 16]);
        seed_intent(&ctx, id, false).await;
        assert_eq!(
            admin_withdraw_pid(&ctx, realm(), id, user(), "reason".into(), 10)
                .await
                .unwrap_err(),
            PersistentIdError::DocumentMissing
        );
        assert_eq!(
            read_mapping(&ctx, id).await.unwrap().unwrap().status,
            PersistentIdStatus::Requested
        );
    }

    #[tokio::test]
    async fn admin_withdraw_records() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([7; 16]);
        seed_record(&ctx, id).await;
        seed_intent(&ctx, id, false).await;
        let (mapping, changed) =
            admin_withdraw_pid(&ctx, realm(), id, user(), "operator reason".into(), 10)
                .await
                .unwrap();
        assert!(changed);
        assert_eq!(mapping.status, PersistentIdStatus::AdminWithdrawn);
        assert_eq!(mapping.minted_at_ms, None);
        assert_eq!(
            mapping.withdrawal_reason.as_deref(),
            Some("operator reason")
        );
        let audit_key = metadata_audit_key(record(id).group_id, id, mapping.revision.event_id);
        let audit = match ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: METADATA_AUDIT_KEYSPACE.to_string(),
                key: audit_key,
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => postcard::from_bytes::<MetadataAuditRecord>(&bytes).unwrap(),
            other => panic!("unexpected audit read event: {other:?}"),
        };
        assert_eq!(
            audit.operation,
            MetadataAuditOperation::WithdrawPersistentId
        );
        assert_eq!(audit.user_id, user());
        assert_eq!(
            audit.details.as_deref(),
            Some("provider=w3id; reason=operator reason")
        );

        let (again, changed_again) =
            admin_withdraw_pid(&ctx, realm(), id, user(), "another reason".into(), 20)
                .await
                .unwrap();
        assert!(!changed_again);
        assert_eq!(again.withdrawn_at_ms, Some(10));
    }

    #[tokio::test]
    async fn mint_keeps_tombstone() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([8; 16]);
        seed_record(&ctx, id).await;
        seed_intent(&ctx, id, false).await;
        admin_withdraw_pid(&ctx, realm(), id, user(), "reason".into(), 5)
            .await
            .unwrap();

        let (mapping, minted) = mint_persistent_id(&ctx, realm(), id, user(), 9)
            .await
            .unwrap();
        assert!(!minted);
        assert_eq!(mapping.status, PersistentIdStatus::AdminWithdrawn);
        assert_eq!(mapping.minted_at_ms, None);
    }

    // A restart re-reads the row it committed: the tombstone is durable, and the
    // mint that reopens the store still cannot replace it.
    #[tokio::test]
    async fn mapping_survives_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        let id = Ulid::from_bytes([11; 16]);
        {
            let ctx = context_at(&path);
            seed_record(&ctx, id).await;
            seed_intent(&ctx, id, false).await;
            mint_persistent_id(&ctx, realm(), id, user(), 1)
                .await
                .unwrap();
            admin_withdraw_pid(&ctx, realm(), id, user(), "reason".into(), 4)
                .await
                .unwrap();
            // Dropping the handle does not wait for the worker to release the file lock.
            ctx.storage_handle.close().await;
        }
        let ctx = context_at(&path);
        let mapping = read_mapping(&ctx, id).await.unwrap().unwrap();
        assert_eq!(mapping.status, PersistentIdStatus::AdminWithdrawn);
        assert_eq!(mapping.withdrawn_at_ms, Some(4));

        let (after, minted) = mint_persistent_id(&ctx, realm(), id, user(), 9)
            .await
            .unwrap();
        assert!(!minted);
        assert_eq!(after, mapping);
    }

    #[tokio::test]
    async fn withdraw_flips_active() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([9; 16]);
        seed_record(&ctx, id).await;
        seed_intent(&ctx, id, false).await;
        mint_persistent_id(&ctx, realm(), id, user(), 1)
            .await
            .unwrap();

        let (mapping, changed) = admin_withdraw_pid(&ctx, realm(), id, user(), "reason".into(), 10)
            .await
            .unwrap();
        assert!(changed);
        assert_eq!(mapping.status, PersistentIdStatus::AdminWithdrawn);
        assert_eq!(mapping.minted_at_ms, Some(1));
        assert_eq!(mapping.withdrawn_at_ms, Some(10));
        assert_eq!(read_mapping(&ctx, id).await.unwrap().unwrap(), mapping);
    }

    fn node() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    /// A realm whose buckets are activated at generation one, so a mapping
    /// transition resolves a generation and takes the bucket's fence.
    fn activated_config() -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
        config.seed_default_placement();
        config.ensure_node(
            node(),
            aruna_core::structs::identity::realm::RealmNodeKind::Server,
        );
        config.snapshot_candidate_map();
        config
    }

    fn outbox_row(writes: &[TransitionEntry]) -> aruna_core::document::DocumentOutboxRecord {
        let entry = writes
            .iter()
            .find(|(key_space, _, _)| key_space == aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE)
            .expect("the transition publishes an outbox row");
        postcard::from_bytes(entry.2.as_ref()).expect("outbox row decodes")
    }

    #[tokio::test]
    async fn fence_rejects_mint() {
        // The departing holder closed generation one: a mapping transition that
        // resolved the old holders must not commit its outbox row afterwards.
        let (ctx, _dir) = context();
        let config = activated_config();
        use aruna_core::StructuredId;
        let document_id = aruna_core::MetaResourceId::from_parts(
            13,
            aruna_core::structured_id::PlacementHandle::new(
                aruna_core::structs::placement::record::METADATA_HANDLE,
            )
            .unwrap(),
            aruna_core::structured_id::BucketId::new(3).unwrap(),
            13,
        )
        .expect("the structured id builds")
        .as_ulid();
        let route =
            mapping_route_for(&config, realm(), document_id, node()).expect("the route resolves");
        assert_eq!(route.generation, 1);

        let mapping = PersistentIdMapping::requested(
            document_id,
            false,
            user(),
            JobId::from_bytes([7; 16]),
            true,
            record(document_id).permission_path,
            mapping_revision(&Some(route.clone()), 1),
        );
        let writes = transition_entries(&Some(route.clone()), &mapping).expect("entries build");
        assert_eq!(outbox_row(&writes).generation, 1);

        assert!(fence_open(&ctx, &route).await);

        crate::placement::process_transitions::close(
            &ctx.storage_handle,
            &realm(),
            &route.placement,
            1,
        )
        .await
        .expect("the departing holder closes the fence");
        assert!(!fence_open(&ctx, &route).await);
    }

    /// Whether the mapping bucket's write fence still admits the route's generation.
    async fn fence_open(ctx: &DriverContext, route: &MappingRoute) -> bool {
        let (key_space, key) =
            crate::placement::fence::fence_read(&route.realm_id, &route.placement);
        let read = StorageEffect::Read {
            key_space,
            key,
            txn_id: None,
        };
        match ctx.storage_handle.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                crate::placement::fence::admits(value.as_ref(), route.generation)
            }
            other => panic!("fence read failed: {}", other.label()),
        }
    }
}
