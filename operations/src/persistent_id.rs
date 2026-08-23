//! The document-scoped PID authority.
//!
//! Creation writes `Requested` with its fenced job. The authority advances that
//! exact typed intent through `Processing` to `Active`; projection absence keeps
//! it retryable. `AdminWithdrawn` and deletion `Tombstoned` are distinct terminal
//! states. Every transition is a compare-and-set transaction that also enqueues
//! its durable sync publish, so replay cannot mint twice or revive a retirement.
//! Routing lives in [`crate::metadata::forward`].

use aruna_core::document::DocumentSyncOutboxEvent;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{METADATA_AUDIT_KEYSPACE, PERSISTENT_ID_MAPPING_KEYSPACE};
use aruna_core::storage_entries::{document_sync_revision_write_entry, shard_manifest_write_entry};
use aruna_core::structs::{
    MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord, PersistentIdFailure,
    PersistentIdMapping, PersistentIdRevision, PlacementRef, RealmConfigDocument, RealmId,
    persistent_id_change, persistent_id_key, persistent_id_target,
};
use aruna_core::types::{TxnId, UserId};
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::create_metadata_document::resolve_metadata_id;
use crate::document_sync_outbox::{
    new_outbox_record, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::metadata::repository::{metadata_audit_key, read_registry_by_document_effect};
use crate::placement::resolve_shard_holders;

/// Storage conflicts are optimistic and short-lived; a caller that exhausts these
/// gets a retryable error rather than a lost transition.
const TRANSITION_ATTEMPTS: usize = 4;

#[derive(Debug, Error, PartialEq)]
pub enum PersistentIdError {
    #[error(transparent)]
    Storage(StorageError),
    #[error(transparent)]
    Conversion(ConversionError),
    /// A mint whose document has no live registry row on this authority.
    #[error("persistent id target document is absent")]
    DocumentMissing,
    #[error("persistent id intent has not reached this authority yet")]
    IntentMissing,
    #[error("persistent id authority is unavailable: {0}")]
    Unavailable(String),
    /// The mapping's bucket cut over to a new holder set mid-transition.
    #[error("persistent id mapping bucket cut over; retry the transition")]
    PlacementFenced,
}

pub fn read_mapping_effect(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: PERSISTENT_ID_MAPPING_KEYSPACE.to_string(),
        key: ByteView::from(persistent_id_key(document_id)),
        txn_id,
    })
}

pub fn parse_mapping_read(event: Event) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                PersistentIdMapping::from_bytes(bytes.as_ref())
                    .map_err(PersistentIdError::Conversion)
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        _ => Err(PersistentIdError::Storage(StorageError::ReadError)),
    }
}

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
/// authoritative mapping and whether this call activated it. If projection has
/// not produced the live registry row yet, `Processing` is committed and the
/// caller must defer without spending a terminal retry.
pub async fn mint_persistent_id(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
    minted_by: aruna_core::types::UserId,
    minted_at_ms: u64,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let route = mapping_route(ctx, realm_id, document_id).await?;
    for attempt in 0..TRANSITION_ATTEMPTS {
        let txn_id = start_transaction(ctx).await?;
        let outcome = mint_in_txn(ctx, &route, document_id, minted_by, minted_at_ms, txn_id).await;
        let Some((mapping, activated)) = (match outcome {
            Ok(mapping) => mapping,
            Err(error) => {
                abort_transaction(ctx, txn_id).await;
                return Err(error);
            }
        }) else {
            abort_transaction(ctx, txn_id).await;
            let existing = read_mapping(ctx, document_id)
                .await?
                .ok_or(PersistentIdError::DocumentMissing)?;
            return Ok((existing, false));
        };
        match commit_transaction(ctx, txn_id).await {
            TransitionCommit::Committed => {
                schedule_drain(ctx).await;
                return Ok((mapping, activated));
            }
            TransitionCommit::Conflict if attempt + 1 < TRANSITION_ATTEMPTS => continue,
            TransitionCommit::Conflict => {
                return Err(PersistentIdError::Storage(
                    StorageError::TransactionConflict,
                ));
            }
            TransitionCommit::Failed(error) => return Err(PersistentIdError::Unavailable(error)),
        }
    }
    Err(PersistentIdError::Storage(
        StorageError::TransactionConflict,
    ))
}

/// Record a terminal provider failure on the same intent. Projection absence
/// must never call this path; the mint handler uses the non-terminal Processing
/// transition above for that condition.
pub async fn fail_persistent_id(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
    failure: PersistentIdFailure,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let route = mapping_route(ctx, realm_id, document_id).await?;
    for attempt in 0..TRANSITION_ATTEMPTS {
        let txn_id = start_transaction(ctx).await?;
        let outcome = async {
            let Some(mut mapping) = mapping_in_txn(ctx, document_id, txn_id).await? else {
                return Err(PersistentIdError::IntentMissing);
            };
            if !fence_admits(ctx, &route, txn_id).await? {
                return Err(PersistentIdError::PlacementFenced);
            }
            if !mapping.fail(
                failure.clone(),
                mapping_revision(&route, failure.recorded_at_ms),
            ) {
                return Ok(None);
            }
            write_transition(ctx, &route, &mapping, txn_id).await?;
            Ok(Some(mapping))
        }
        .await;
        let Some(mapping) = (match outcome {
            Ok(mapping) => mapping,
            Err(error) => {
                abort_transaction(ctx, txn_id).await;
                return Err(error);
            }
        }) else {
            abort_transaction(ctx, txn_id).await;
            let existing = read_mapping(ctx, document_id)
                .await?
                .ok_or(PersistentIdError::IntentMissing)?;
            return Ok((existing, false));
        };
        match commit_transaction(ctx, txn_id).await {
            TransitionCommit::Committed => {
                schedule_drain(ctx).await;
                return Ok((mapping, true));
            }
            TransitionCommit::Conflict if attempt + 1 < TRANSITION_ATTEMPTS => continue,
            TransitionCommit::Conflict => {
                return Err(PersistentIdError::Storage(
                    StorageError::TransactionConflict,
                ));
            }
            TransitionCommit::Failed(error) => return Err(PersistentIdError::Unavailable(error)),
        }
    }
    Err(PersistentIdError::Storage(
        StorageError::TransactionConflict,
    ))
}

/// Exceptional administrator-only withdrawal. Authorization is enforced at
/// both routing hops; the transition stores the actor and required reason and
/// writes the generic metadata audit row in the same transaction.
pub async fn admin_withdraw_persistent_id(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
    withdrawn_by: UserId,
    reason: String,
    withdrawn_at_ms: u64,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let route = mapping_route(ctx, realm_id, document_id).await?;
    for attempt in 0..TRANSITION_ATTEMPTS {
        let txn_id = start_transaction(ctx).await?;
        let outcome = admin_withdraw_in_txn(
            ctx,
            &route,
            document_id,
            withdrawn_by,
            &reason,
            withdrawn_at_ms,
            txn_id,
        )
        .await;
        let mapping = match outcome {
            Ok(Some(mapping)) => mapping,
            Ok(None) => {
                abort_transaction(ctx, txn_id).await;
                let existing = read_mapping(ctx, document_id)
                    .await?
                    .ok_or(PersistentIdError::Storage(StorageError::ReadError))?;
                return Ok((existing, false));
            }
            Err(error) => {
                abort_transaction(ctx, txn_id).await;
                return Err(error);
            }
        };
        match commit_transaction(ctx, txn_id).await {
            TransitionCommit::Committed => {
                schedule_drain(ctx).await;
                return Ok((mapping, true));
            }
            TransitionCommit::Conflict if attempt + 1 < TRANSITION_ATTEMPTS => continue,
            TransitionCommit::Conflict => {
                return Err(PersistentIdError::Storage(
                    StorageError::TransactionConflict,
                ));
            }
            TransitionCommit::Failed(error) => return Err(PersistentIdError::Unavailable(error)),
        }
    }
    Err(PersistentIdError::Storage(
        StorageError::TransactionConflict,
    ))
}

/// Where a mapping row lives and who replicates it: the document-lifecycle
/// placement, never the deletable registry row, so a withdrawal still routes and
/// replicates after the document is gone.
#[derive(Debug, Clone, PartialEq)]
pub struct MappingRoute {
    pub realm_id: RealmId,
    pub placement: PlacementRef,
    pub peers: Vec<aruna_core::NodeId>,
    pub actor: aruna_core::NodeId,
    /// Activation generation the mapping's bucket resolved at. `0` means the
    /// realm has no activation yet, so no transition can be in flight.
    pub generation: u64,
}

async fn mapping_route(
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

/// The route for a caller that already holds the realm config, such as the delete
/// transaction that writes the tombstone alongside the registry row it removes.
pub fn mapping_route_for(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
    actor: aruna_core::NodeId,
) -> Option<MappingRoute> {
    let placement = mapping_placement(config, realm_id, document_id).ok()?;
    Some(MappingRoute {
        realm_id,
        placement,
        peers: resolve_shard_holders(config, &placement),
        actor,
        generation: crate::placement::fence::write_generation(config, &placement).unwrap_or(0),
    })
}

pub fn mapping_placement(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<PlacementRef, PersistentIdError> {
    resolve_metadata_id(config, realm_id, None, document_id)
        .map_err(|error| PersistentIdError::Unavailable(error.to_string()))
}

/// A context without a net handle replicates nothing, so its transitions record a
/// fixed placeholder actor rather than inventing an identity.
fn transition_actor(route: &Option<MappingRoute>) -> aruna_core::NodeId {
    route
        .as_ref()
        .map(|route| route.actor)
        .unwrap_or_else(|| iroh::SecretKey::from_bytes(&[0u8; 32]).public())
}

pub fn mapping_revision(route: &Option<MappingRoute>, occurred_at_ms: u64) -> PersistentIdRevision {
    PersistentIdRevision {
        event_id: Ulid::generate(),
        actor: transition_actor(route),
        occurred_at_ms,
    }
}

/// `Ok(None)` means the transition is a no-op and the caller must abort.
async fn mint_in_txn(
    ctx: &DriverContext,
    route: &Option<MappingRoute>,
    document_id: Ulid,
    minted_by: aruna_core::types::UserId,
    minted_at_ms: u64,
    txn_id: TxnId,
) -> Result<Option<(PersistentIdMapping, bool)>, PersistentIdError> {
    let Some(mut mapping) = mapping_in_txn(ctx, document_id, txn_id).await? else {
        return Err(PersistentIdError::IntentMissing);
    };
    if mapping.is_active() || mapping.is_retired() {
        return Ok(None);
    }
    if !fence_admits(ctx, route, txn_id).await? {
        return Err(PersistentIdError::PlacementFenced);
    }
    if registry_missing_txn(ctx, document_id, txn_id).await? {
        let failure = PersistentIdFailure {
            message: "metadata projection is not readable yet".to_string(),
            retryable: true,
            recorded_at_ms: minted_at_ms,
        };
        mapping.processing(mapping_revision(route, minted_at_ms), Some(failure));
        write_transition(ctx, route, &mapping, txn_id).await?;
        return Ok(Some((mapping, false)));
    }
    mapping.activate(minted_by, mapping_revision(route, minted_at_ms));
    write_transition(ctx, route, &mapping, txn_id).await?;
    Ok(Some((mapping, true)))
}

/// `Ok(None)` means the mapping is already terminal and the caller must abort.
async fn admin_withdraw_in_txn(
    ctx: &DriverContext,
    route: &Option<MappingRoute>,
    document_id: Ulid,
    withdrawn_by: UserId,
    reason: &str,
    withdrawn_at_ms: u64,
    txn_id: TxnId,
) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    let record = registry_in_txn(ctx, document_id, txn_id)
        .await?
        .ok_or(PersistentIdError::DocumentMissing)?;
    if !fence_admits(ctx, route, txn_id).await? {
        return Err(PersistentIdError::PlacementFenced);
    }
    let Some(mut mapping) = mapping_in_txn(ctx, document_id, txn_id).await? else {
        return Err(PersistentIdError::IntentMissing);
    };
    let revision = mapping_revision(route, withdrawn_at_ms);
    if !mapping.admin_withdraw(withdrawn_by, reason.to_string(), revision) {
        return Ok(None);
    }
    let mut writes = transition_entries(route, &mapping)?;
    let audit = MetadataAuditRecord {
        realm_id: record.realm_id,
        group_id: record.group_id,
        document_id,
        graph_iri: record.graph_iri,
        user_id: withdrawn_by,
        node_id: transition_actor(route),
        operation: MetadataAuditOperation::WithdrawPersistentId,
        occurred_at_ms: withdrawn_at_ms,
        details: Some(format!("provider=w3id; reason={reason}")),
    };
    writes.push((
        METADATA_AUDIT_KEYSPACE.to_string(),
        metadata_audit_key(record.group_id, document_id, revision.event_id),
        ByteView::from(
            postcard::to_allocvec(&audit)
                .map_err(|error| PersistentIdError::Conversion(error.into()))?,
        ),
    ));
    write_entries(ctx, writes, txn_id).await?;
    Ok(Some(mapping))
}

/// Deletion transition shared with the document delete. It is composed into the
/// transaction that removes the registry row; an absent mapping stays absent.
pub fn tombstone_transition(
    existing: Option<&PersistentIdMapping>,
    route: &Option<MappingRoute>,
    document_id: Ulid,
    deleted_at_ms: u64,
) -> Result<Option<(PersistentIdMapping, Vec<TransitionEntry>)>, PersistentIdError> {
    let Some(mapping) = existing else {
        return Ok(None);
    };
    debug_assert_eq!(mapping.target, document_id);
    let mut mapping = mapping.clone();
    if !mapping.mark_tombstoned(mapping_revision(route, deleted_at_ms)) {
        return Ok(None);
    }
    let writes = transition_entries(route, &mapping)?;
    Ok(Some((mapping, writes)))
}

pub type TransitionEntry = (String, ByteView, ByteView);

/// Row, sync sidecar, shard-manifest entry, and outbox publish, so an accepted
/// transition is either fully durable and replicated or not taken.
pub fn transition_entries(
    route: &Option<MappingRoute>,
    mapping: &PersistentIdMapping,
) -> Result<Vec<TransitionEntry>, PersistentIdError> {
    let target = persistent_id_target(mapping.target);
    let mut writes = vec![(
        PERSISTENT_ID_MAPPING_KEYSPACE.to_string(),
        ByteView::from(persistent_id_key(mapping.target)),
        ByteView::from(mapping.to_bytes().map_err(PersistentIdError::Conversion)?),
    )];
    if let Some(route) = route {
        let change = persistent_id_change(mapping, route.placement);
        writes.push(
            document_sync_revision_write_entry(&target, &change)
                .map_err(PersistentIdError::Conversion)?,
        );
        if let Some(entry) =
            shard_manifest_write_entry(&target, &change).map_err(PersistentIdError::Conversion)?
        {
            writes.push(entry);
        }
        if !route.peers.is_empty() {
            let record = new_outbox_record(
                route.actor,
                target,
                route.peers.clone(),
                DocumentSyncOutboxEvent::Upsert {
                    bytes: mapping.to_bytes().map_err(PersistentIdError::Conversion)?,
                    change,
                },
                route.placement,
                false,
            )
            .fenced_at(route.generation);
            let entry = outbox_write_entry(&record)
                .map_err(|error| PersistentIdError::Conversion(error.into()))?;
            writes.push(entry);
        }
    }
    Ok(writes)
}

async fn write_transition(
    ctx: &DriverContext,
    route: &Option<MappingRoute>,
    mapping: &PersistentIdMapping,
    txn_id: TxnId,
) -> Result<(), PersistentIdError> {
    write_entries(ctx, transition_entries(route, mapping)?, txn_id).await
}

async fn write_entries(
    ctx: &DriverContext,
    writes: Vec<TransitionEntry>,
    txn_id: TxnId,
) -> Result<(), PersistentIdError> {
    match ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        }))
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        other => Err(PersistentIdError::Unavailable(format!(
            "unexpected persistent id write event: {other:?}"
        ))),
    }
}

/// Whether the document's registry row is absent inside `txn_id`.
async fn registry_missing_txn(
    ctx: &DriverContext,
    document_id: Ulid,
    txn_id: TxnId,
) -> Result<bool, PersistentIdError> {
    match ctx
        .storage_handle
        .send_effect(read_registry_by_document_effect(document_id, Some(txn_id)))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_none()),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        other => Err(PersistentIdError::Unavailable(format!(
            "unexpected registry fence event: {other:?}"
        ))),
    }
}

async fn registry_in_txn(
    ctx: &DriverContext,
    document_id: Ulid,
    txn_id: TxnId,
) -> Result<Option<MetadataRegistryRecord>, PersistentIdError> {
    match ctx
        .storage_handle
        .send_effect(read_registry_by_document_effect(document_id, Some(txn_id)))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes(bytes.as_ref())
                    .map_err(|error| PersistentIdError::Conversion(error.into()))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        other => Err(PersistentIdError::Unavailable(format!(
            "unexpected registry read event: {other:?}"
        ))),
    }
}

/// Reads the mapping bucket's write fence inside the transaction, so a
/// departing holder's close either rejects this transition or conflicts it.
async fn fence_admits(
    ctx: &DriverContext,
    route: &Option<MappingRoute>,
    txn_id: TxnId,
) -> Result<bool, PersistentIdError> {
    let Some(route) = route.as_ref().filter(|route| route.generation > 0) else {
        return Ok(true);
    };
    let (key_space, key) = crate::placement::fence::fence_read(&route.realm_id, &route.placement);
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space,
            key,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(
            crate::placement::fence::admits(value.as_ref(), route.generation),
        ),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        other => Err(PersistentIdError::Unavailable(format!(
            "unexpected mapping fence event: {other:?}"
        ))),
    }
}

async fn mapping_in_txn(
    ctx: &DriverContext,
    document_id: Ulid,
    txn_id: TxnId,
) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    let event = ctx
        .storage_handle
        .send_effect(read_mapping_effect(document_id, Some(txn_id)))
        .await;
    parse_mapping_read(event)
}

enum TransitionCommit {
    Committed,
    Conflict,
    Failed(String),
}

async fn start_transaction(ctx: &DriverContext) -> Result<TxnId, PersistentIdError> {
    match ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        }))
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        other => Err(PersistentIdError::Unavailable(format!(
            "unexpected persistent id transaction event: {other:?}"
        ))),
    }
}

async fn commit_transaction(ctx: &DriverContext, txn_id: TxnId) -> TransitionCommit {
    match ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => TransitionCommit::Committed,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => TransitionCommit::Conflict,
        Event::Storage(StorageEvent::Error { error }) => {
            TransitionCommit::Failed(error.to_string())
        }
        other => {
            TransitionCommit::Failed(format!("unexpected persistent id commit event: {other:?}"))
        }
    }
}

async fn abort_transaction(ctx: &DriverContext, txn_id: TxnId) {
    if let Event::Storage(StorageEvent::Error { error }) = ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::AbortTransaction { txn_id }))
        .await
    {
        tracing::warn!(%error, "failed to abort a persistent id transaction");
    }
}

async fn schedule_drain(ctx: &DriverContext) {
    if let Some(task_handle) = ctx.task_handle.as_ref() {
        task_handle
            .send_effect(schedule_outbox_drain_effect())
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::storage_entries::metadata_registry_write_entries;
    use aruna_core::structs::{
        JobId, MetadataAuditRecord, MetadataRegistryRecord, PersistentIdStatus, RealmId,
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
            task_handle: None,
            compute_handle: None,
        }
    }

    fn realm() -> RealmId {
        RealmId([3; 32])
    }

    fn user() -> aruna_core::types::UserId {
        aruna_core::types::UserId::local(Ulid::from_bytes([2; 16]), realm())
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
        let writes = metadata_registry_write_entries(&record(document_id)).unwrap();
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
    async fn projection_absence_stays_processing() {
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
    async fn admin_withdraw_needs_record() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([12; 16]);
        seed_intent(&ctx, id, false).await;
        assert_eq!(
            admin_withdraw_persistent_id(&ctx, realm(), id, user(), "reason".into(), 10)
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
    async fn admin_withdraw_records_reason() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([7; 16]);
        seed_record(&ctx, id).await;
        seed_intent(&ctx, id, false).await;
        let (mapping, changed) =
            admin_withdraw_persistent_id(&ctx, realm(), id, user(), "operator reason".into(), 10)
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
            admin_withdraw_persistent_id(&ctx, realm(), id, user(), "another reason".into(), 20)
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
        admin_withdraw_persistent_id(&ctx, realm(), id, user(), "reason".into(), 5)
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
            admin_withdraw_persistent_id(&ctx, realm(), id, user(), "reason".into(), 4)
                .await
                .unwrap();
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

        let (mapping, changed) =
            admin_withdraw_persistent_id(&ctx, realm(), id, user(), "reason".into(), 10)
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
        config.ensure_node(node(), aruna_core::structs::RealmNodeKind::Server);
        config.snapshot_candidate_map();
        config
    }

    fn outbox_row(writes: &[TransitionEntry]) -> aruna_core::document::DocumentSyncOutboxRecord {
        let entry = writes
            .iter()
            .find(|(key_space, _, _)| {
                key_space == aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE
            })
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
            aruna_core::structured_id::PlacementHandle::new(aruna_core::structs::METADATA_HANDLE)
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

        let txn_id = start_transaction(&ctx).await.unwrap();
        assert!(
            fence_admits(&ctx, &Some(route.clone()), txn_id)
                .await
                .unwrap()
        );
        abort_transaction(&ctx, txn_id).await;

        crate::placement::fence::close(&ctx.storage_handle, &realm(), &route.placement, 1)
            .await
            .expect("the departing holder closes the fence");
        let txn_id = start_transaction(&ctx).await.unwrap();
        assert!(!fence_admits(&ctx, &Some(route), txn_id).await.unwrap());
        abort_transaction(&ctx, txn_id).await;
    }
}
