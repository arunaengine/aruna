//! The document-scoped PID authority.
//!
//! Transitions are `Absent -> Active`, `Absent -> Withdrawn`, and
//! `Active -> Withdrawn`; `Withdrawn` is terminal. Every accepted transition is a
//! compare-and-set inside one storage transaction that also enqueues the durable
//! sync publish, so a frozen holder converges on the same row and a withdrawal can
//! never be overwritten by an accepted-but-not-yet-executed mint job. Callers
//! reach these on the document's single authority only; routing lives in
//! [`crate::metadata::forward`].

use aruna_core::document::DocumentSyncOutboxEvent;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::PERSISTENT_ID_MAPPING_KEYSPACE;
use aruna_core::storage_entries::{document_sync_revision_write_entry, shard_manifest_write_entry};
use aruna_core::structs::{
    PersistentIdMapping, PersistentIdRevision, PlacementRef, RealmConfigDocument, RealmId,
    persistent_id_change, persistent_id_key, persistent_id_target,
};
use aruna_core::types::TxnId;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::create_metadata_document::resolve_metadata_id;
use crate::document_sync_outbox::{
    new_outbox_record, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::metadata::repository::read_registry_by_document_effect;
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
    #[error("persistent id authority is unavailable: {0}")]
    Unavailable(String),
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

/// Register a Conceptual PID for a document. Returns the mapping and whether this
/// call is the one that created it. A `Withdrawn` mapping is returned untouched:
/// mint never revives a tombstone.
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
                .ok_or(PersistentIdError::DocumentMissing)?;
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

/// Flip a live document's PID mapping to `Withdrawn`, writing the tombstone even
/// when nothing was ever minted, so an accepted-but-unexecuted mint job cannot
/// land after it. A document whose registry row is already gone is
/// `DocumentMissing`: its tombstone was written by the delete itself. Returns the
/// resulting mapping and whether this call performed the transition.
pub async fn withdraw_persistent_id(
    ctx: &DriverContext,
    realm_id: RealmId,
    document_id: Ulid,
    withdrawn_at_ms: u64,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    let route = mapping_route(ctx, realm_id, document_id).await?;
    for attempt in 0..TRANSITION_ATTEMPTS {
        let txn_id = start_transaction(ctx).await?;
        let outcome = withdraw_in_txn(ctx, &route, document_id, withdrawn_at_ms, txn_id).await;
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
    pub placement: PlacementRef,
    pub peers: Vec<aruna_core::NodeId>,
    pub actor: aruna_core::NodeId,
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
        placement,
        peers: resolve_shard_holders(config, &placement),
        actor,
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

fn revision(route: &Option<MappingRoute>, occurred_at_ms: u64) -> PersistentIdRevision {
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
) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    // Fence on a live registry row inside the transaction: a mint may not
    // activate a PID for a document a concurrent delete is removing.
    if registry_missing_txn(ctx, document_id, txn_id).await? {
        return Err(PersistentIdError::DocumentMissing);
    }
    if mapping_in_txn(ctx, document_id, txn_id).await?.is_some() {
        return Ok(None);
    }
    let mapping =
        PersistentIdMapping::conceptual(document_id, minted_by, revision(route, minted_at_ms));
    write_transition(ctx, route, &mapping, txn_id).await?;
    Ok(Some(mapping))
}

/// `Ok(None)` means the mapping is already withdrawn and the caller must abort.
async fn withdraw_in_txn(
    ctx: &DriverContext,
    route: &Option<MappingRoute>,
    document_id: Ulid,
    withdrawn_at_ms: u64,
    txn_id: TxnId,
) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    // An explicit withdrawal is an operation on a live document, so it fences on
    // the registry row exactly as a mint does. The tombstone of a document that is
    // already gone belongs to the transaction that removed it, not here.
    if registry_missing_txn(ctx, document_id, txn_id).await? {
        return Err(PersistentIdError::DocumentMissing);
    }
    let existing = mapping_in_txn(ctx, document_id, txn_id).await?;
    let Some((mapping, writes)) =
        withdrawal_transition(existing.as_ref(), route, document_id, withdrawn_at_ms)?
    else {
        return Ok(None);
    };
    write_entries(ctx, writes, txn_id).await?;
    Ok(Some(mapping))
}

/// The mapping and the writes that retire it, or `None` when it is already
/// withdrawn. Shared with the document delete, which writes the tombstone inside
/// the transaction that removes the registry row so it cannot be lost afterwards.
pub fn withdrawal_transition(
    existing: Option<&PersistentIdMapping>,
    route: &Option<MappingRoute>,
    document_id: Ulid,
    withdrawn_at_ms: u64,
) -> Result<Option<(PersistentIdMapping, Vec<TransitionEntry>)>, PersistentIdError> {
    let revision = revision(route, withdrawn_at_ms);
    let mapping = match existing {
        Some(mapping) => {
            let mut mapping = mapping.clone();
            if !mapping.withdraw(revision) {
                return Ok(None);
            }
            mapping
        }
        None => PersistentIdMapping::tombstone(document_id, revision),
    };
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
            );
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
    use aruna_core::structs::{MetadataRegistryRecord, PersistentIdStatus, RealmId};
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

    #[tokio::test]
    async fn mint_is_idempotent() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([5; 16]);
        seed_record(&ctx, id).await;
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
    async fn mint_needs_record() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([6; 16]);
        assert_eq!(
            mint_persistent_id(&ctx, realm(), id, user(), 1)
                .await
                .unwrap_err(),
            PersistentIdError::DocumentMissing
        );
        assert!(read_mapping(&ctx, id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn withdraw_needs_record() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([12; 16]);
        assert_eq!(
            withdraw_persistent_id(&ctx, realm(), id, 10)
                .await
                .unwrap_err(),
            PersistentIdError::DocumentMissing
        );
        assert!(read_mapping(&ctx, id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn withdraw_tombstones_unminted() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([7; 16]);
        seed_record(&ctx, id).await;
        let (mapping, changed) = withdraw_persistent_id(&ctx, realm(), id, 10).await.unwrap();
        assert!(changed);
        assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
        assert_eq!(mapping.minted_at_ms, None);

        let (again, changed_again) = withdraw_persistent_id(&ctx, realm(), id, 20).await.unwrap();
        assert!(!changed_again);
        assert_eq!(again.withdrawn_at_ms, Some(10));
    }

    #[tokio::test]
    async fn mint_keeps_tombstone() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([8; 16]);
        seed_record(&ctx, id).await;
        withdraw_persistent_id(&ctx, realm(), id, 5).await.unwrap();

        let (mapping, minted) = mint_persistent_id(&ctx, realm(), id, user(), 9)
            .await
            .unwrap();
        assert!(!minted);
        assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
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
            mint_persistent_id(&ctx, realm(), id, user(), 1)
                .await
                .unwrap();
            withdraw_persistent_id(&ctx, realm(), id, 4).await.unwrap();
        }
        let ctx = context_at(&path);
        let mapping = read_mapping(&ctx, id).await.unwrap().unwrap();
        assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
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
        mint_persistent_id(&ctx, realm(), id, user(), 1)
            .await
            .unwrap();

        let (mapping, changed) = withdraw_persistent_id(&ctx, realm(), id, 10).await.unwrap();
        assert!(changed);
        assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
        assert_eq!(mapping.minted_at_ms, Some(1));
        assert_eq!(mapping.withdrawn_at_ms, Some(10));
        assert_eq!(read_mapping(&ctx, id).await.unwrap().unwrap(), mapping);
    }
}
