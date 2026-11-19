//! Owns a document's persistent identifier state, moving it from requested to active.
//! Each step is a compare-and-set that also queues its publish, so replay cannot mint twice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "persistent_id_forward.rs"]
pub mod forward;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::keyspaces::ID_MAPPING_KEYSPACE;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::rows::{read_row, value};
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::persistent_id_key;
use aruna_core::structs::secondary_id::SecondaryIdentifier;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::id_mapping::{MappingRoute, PersistentIdError, mapping_route_for};
use crate::metadata::pid_transition::{PidOutcome, PidTransition, PidTransitionOperation};
use crate::metadata::secondary_ids::{AddIdentifiersInput, AddIdentifiersOperation};
use crate::realm::load_config::config_unavailable;
use crate::sync::outbox_record::schedule_drain_effect;

/// Reads one document's persistent identifier mapping, if it has one.
#[derive(Debug, PartialEq)]
pub struct MappingReadOperation {
    document_id: Ulid,
}

impl MappingReadOperation {
    pub fn new(document_id: Ulid) -> Self {
        Self { document_id }
    }
}

impl Operation for MappingReadOperation {
    type Output = Option<PersistentIdMapping>;
    type Error = PersistentIdError;

    fn first(&mut self) -> Result<Next<Option<PersistentIdMapping>>, PersistentIdError> {
        let key = persistent_id_key(self.document_id);
        Ok(Next::Call(read_row(ID_MAPPING_KEYSPACE, key)))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<Option<PersistentIdMapping>>, PersistentIdError> {
        let mapping = value(result)?
            .map(|bytes| PersistentIdMapping::from_bytes(bytes.as_ref()))
            .transpose()
            .map_err(PersistentIdError::Conversion)?;
        Ok(Next::Done(mapping))
    }
}

/// One change of a document's mapping on this node.
#[derive(Clone, Debug, PartialEq)]
pub enum PidChange {
    /// Mint, fail or withdraw the mapping. A mint without a projected registry row commits
    /// `Processing` and the caller defers.
    Transition(PidTransition),
    /// Adds external identifiers; callers route to the PID authority first.
    AddIdentifiers {
        identifiers: Vec<SecondaryIdentifier>,
        occurred_at_ms: u64,
    },
}

#[derive(Debug, PartialEq)]
enum ChangeStage {
    Config,
    Change,
}

/// Applies one [`PidChange`] in a transaction that reruns after a lost conflict, fenced by the
/// mapping's placement when this node has a network, and wakes the outbox drain after a commit.
/// The output is the authoritative mapping and whether this change applied.
#[derive(Debug, PartialEq)]
pub struct PidChangeOperation {
    realm_id: RealmId,
    local_node: Option<NodeId>,
    document_id: Ulid,
    change: Option<PidChange>,
    adding: bool,
    stage: ChangeStage,
}

impl PidChangeOperation {
    /// `local_node` is this node on the network; without one the change is not fenced.
    pub fn new(
        (realm_id, local_node): (RealmId, Option<NodeId>),
        document_id: Ulid,
        change: PidChange,
    ) -> Self {
        Self {
            realm_id,
            local_node,
            document_id,
            adding: matches!(change, PidChange::AddIdentifiers { .. }),
            change: Some(change),
            stage: ChangeStage::Config,
        }
    }

    fn route(
        &self,
        config: &RealmConfigDocument,
        node: NodeId,
    ) -> Result<MappingRoute, PersistentIdError> {
        mapping_route_for(config, self.realm_id, self.document_id, node).ok_or_else(|| {
            PersistentIdError::Unavailable("persistent id placement is unavailable".to_string())
        })
    }

    fn change(
        &mut self,
        route: Option<MappingRoute>,
    ) -> Result<Next<(PersistentIdMapping, bool)>, PersistentIdError> {
        let change = self.change.take().ok_or_else(|| {
            PersistentIdError::Unavailable("the change was already started".to_string())
        })?;
        let document_id = self.document_id;
        let seed = document_id.to_bytes();
        let child = match change {
            PidChange::Transition(transition) => boxed_retry(
                move || PidTransitionOperation::new(route.clone(), document_id, transition.clone()),
                &seed,
                |result| Event::Child(ChildOutcome::new(result)),
            ),
            PidChange::AddIdentifiers {
                identifiers,
                occurred_at_ms,
            } => {
                let input = AddIdentifiersInput {
                    document_id,
                    identifiers,
                    route,
                    occurred_at_ms,
                };
                boxed_retry(
                    move || AddIdentifiersOperation::new(input.clone()),
                    &seed,
                    |result| Event::Child(ChildOutcome::new(result)),
                )
            }
        };
        self.stage = ChangeStage::Change;
        Ok(Next::Call(IoCall::Child(child)))
    }

    /// The result, with a drain wake once a change committed.
    fn changed(
        &self,
        (mapping, applied): (PersistentIdMapping, bool),
        committed: bool,
    ) -> Next<(PersistentIdMapping, bool)> {
        let output = (mapping, applied);
        match committed {
            true => Next::Then {
                output,
                calls: vec![IoCall::from(schedule_drain_effect())],
            },
            false => Next::Done(output),
        }
    }
}

impl Operation for PidChangeOperation {
    type Output = (PersistentIdMapping, bool);
    type Error = PersistentIdError;

    fn first(&mut self) -> Result<Next<(PersistentIdMapping, bool)>, PersistentIdError> {
        if self.local_node.is_none() {
            return self.change(None);
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<(PersistentIdMapping, bool)>, PersistentIdError> {
        match self.stage {
            ChangeStage::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| {
                    config_unavailable(&error);
                    PersistentIdError::Unavailable(
                        "realm placement config is unavailable".to_string(),
                    )
                })?;
                let node = self.local_node.ok_or_else(|| {
                    PersistentIdError::Unavailable(
                        "persistent id placement is unavailable".to_string(),
                    )
                })?;
                let route = self.route(&config, node)?;
                self.change(Some(route))
            }
            // Added identifiers commit exactly when they change the mapping.
            ChangeStage::Change if self.adding => {
                let added = result.child::<(PersistentIdMapping, bool), PersistentIdError>()??;
                let committed = added.1;
                Ok(self.changed(added, committed))
            }
            ChangeStage::Change => {
                let outcome = result.child::<PidOutcome, PersistentIdError>()??;
                let committed = outcome.committed;
                Ok(self.changed((outcome.mapping, outcome.applied), committed))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::handle::Handle;

    async fn change(
        ctx: &DriverContext,
        (realm_id, document_id): (RealmId, Ulid),
        transition: PidTransition,
    ) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
        let change = PidChange::Transition(transition);
        drive(
            PidChangeOperation::new((realm_id, None), document_id, change),
            ctx,
        )
        .await
    }

    async fn mint_persistent_id(
        ctx: &DriverContext,
        realm_id: RealmId,
        document_id: Ulid,
        minted_by: UserId,
        minted_at_ms: u64,
    ) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
        let transition = PidTransition::Mint {
            minted_by,
            minted_at_ms,
        };
        change(ctx, (realm_id, document_id), transition).await
    }

    async fn admin_withdraw_pid(
        ctx: &DriverContext,
        realm_id: RealmId,
        document_id: Ulid,
        withdrawn_by: UserId,
        reason: String,
        withdrawn_at_ms: u64,
    ) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
        let transition = PidTransition::Withdraw {
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        };
        change(ctx, (realm_id, document_id), transition).await
    }

    async fn read_mapping(
        ctx: &DriverContext,
        document_id: Ulid,
    ) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
        drive(MappingReadOperation::new(document_id), ctx).await
    }
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
            outbound_handle: None,
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

        crate::driver::drive(
            crate::placement::process_transitions::CloseFenceOperation::new(
                realm(),
                route.placement,
                1,
            ),
            &crate::driver::DriverContext::storage_only(ctx.storage_handle.clone()),
        )
        .await
        .expect("the departing holder closes the fence");
        assert!(!fence_open(&ctx, &route).await);
    }

    /// Whether the mapping bucket's write fence still admits the route's generation.
    async fn fence_open(ctx: &DriverContext, route: &MappingRoute) -> bool {
        let (key_space, key) =
            aruna_core::placement::fence::fence_read(&route.realm_id, &route.placement);
        let read = StorageEffect::Read {
            key_space,
            key,
            txn_id: None,
        };
        match ctx.storage_handle.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                aruna_core::placement::fence::admits(value.as_ref(), route.generation)
            }
            other => panic!("fence read failed: {}", other.label()),
        }
    }

    fn requested(document_id: Ulid) -> PersistentIdMapping {
        PersistentIdMapping::requested(
            document_id,
            false,
            user(),
            JobId::from_bytes([7; 16]),
            true,
            record(document_id).permission_path,
            mapping_revision(&None, 1),
        )
    }

    #[test]
    fn mapping_read_decodes() {
        let id = Ulid::from_bytes([5; 16]);
        let mut read = MappingReadOperation::new(id);
        let key = persistent_id_key(id);
        assert_eq!(
            read.first(),
            Ok(Next::Call(read_row(ID_MAPPING_KEYSPACE, key)))
        );
        assert_eq!(
            read.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
        let mapping = requested(id);
        let bytes = aruna_core::types::Value::from(mapping.to_bytes().unwrap());
        assert_eq!(
            read.next(IoResult::Values(vec![Some(bytes)])),
            Ok(Next::Done(Some(mapping)))
        );
    }

    #[test]
    fn commit_wakes_drain() {
        let id = Ulid::from_bytes([5; 16]);
        let transition = PidTransition::Mint {
            minted_by: user(),
            minted_at_ms: 1,
        };
        let change = PidChange::Transition(transition);
        // Each mapping gets a fresh revision id, so one mapping is built and compared.
        let mapping = requested(id);
        let mut pid = PidChangeOperation::new((realm(), None), id, change.clone());
        assert!(matches!(pid.first(), Ok(Next::Call(IoCall::Child(_)))));
        let outcome = PidOutcome {
            mapping: mapping.clone(),
            applied: true,
            committed: true,
        };
        let done = pid.next(IoResult::Child(ChildOutcome::new(
            Ok::<_, PersistentIdError>(outcome),
        )));
        assert_eq!(
            done,
            Ok(Next::Then {
                output: (mapping.clone(), true),
                calls: vec![IoCall::from(schedule_drain_effect())],
            })
        );

        let mut replay = PidChangeOperation::new((realm(), None), id, change);
        replay.first().unwrap();
        let outcome = PidOutcome {
            mapping: mapping.clone(),
            applied: false,
            committed: false,
        };
        let done = replay.next(IoResult::Child(ChildOutcome::new(
            Ok::<_, PersistentIdError>(outcome),
        )));
        assert_eq!(done, Ok(Next::Done((mapping, false))));
    }

    #[test]
    fn network_needs_config() {
        let id = Ulid::from_bytes([5; 16]);
        let change = PidChange::AddIdentifiers {
            identifiers: Vec::new(),
            occurred_at_ms: 1,
        };
        let mut pid = PidChangeOperation::new((realm(), Some(node())), id, change);
        assert!(matches!(pid.first(), Ok(Next::Call(IoCall::Child(_)))));
        let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
        assert!(matches!(
            pid.next(IoResult::Child(ChildOutcome::new(missing))),
            Err(PersistentIdError::Unavailable(_))
        ));
    }
}
