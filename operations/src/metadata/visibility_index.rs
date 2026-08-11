//! Timestamp-ordered index of the records an anonymous caller may read.
//!
//! OAI-PMH enumeration is unauthenticated, so it must neither scan the registry
//! nor evaluate a request policy per candidate. A background pass rebuilds the
//! index into a fresh generation and publishes it only once complete; readers
//! page the published generation under a fixed candidate budget and re-check
//! authorization on every record before it is rendered.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_VISIBILITY_INDEX_KEYSPACE, METADATA_VISIBILITY_STATE_KEYSPACE,
};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::{MetadataRegistryRecord, Permission};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::repository::{StorageReadError, delete_index_keys};
use crate::metadata::timestamp_index::enumerate_updated;
use crate::request_policy::{PolicyEvaluator, PolicyRequestExtras, policy_request_with};

/// Registry rows evaluated per rebuild batch, and index rows per scan batch.
const BUILD_BATCH: usize = 256;
const SCAN_BATCH: usize = 256;
/// Index rows a single reader request may inspect before it must stop and hand
/// the caller a cursor. Bounds request work independently of registry size.
pub const CANDIDATE_BUDGET: usize = 512;
/// Continuations `earliest_visible` may follow through denied candidates.
const EARLIEST_PAGES: usize = 64;
const PRUNE_BATCH: usize = 256;
/// Registry batches one maintenance pass may walk before it yields, so shutdown
/// is observed within a fixed amount of work.
const PASS_BATCHES: usize = 4;
/// Passes `rebuild_index` may drive before giving up on reaching a quiet state.
const DRIVE_PASSES: usize = 100_000;
const REBUILD_INTERVAL: Duration = Duration::from_secs(60);
/// Pacing between the passes of a cycle that still has work to do.
const WORK_INTERVAL: Duration = Duration::from_secs(1);

/// The single row naming the generation readers may serve, plus the resumable
/// state of the maintenance cycle.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct VisibilityState {
    generation: u64,
    ready: bool,
    /// Fingerprint of the visible set the published generation was built from.
    /// A pass whose fingerprint matches it publishes nothing.
    digest: [u8; 32],
    /// The registry walk in flight, if any.
    pass: Option<PassState>,
    /// Superseded generations may still hold rows; resume deleting after this key.
    prune_after: Option<Vec<u8>>,
    pruning: bool,
}

/// A resumable registry walk. It compares the registry against the published
/// generation until a difference shows up, and only then writes a new one.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PassState {
    /// Timestamp-index cursor to resume the walk after.
    cursor: Option<Vec<u8>>,
    /// Fingerprint accumulated so far.
    digest: [u8; 32],
    /// The generation being written; `None` while the walk only compares.
    building: Option<u64>,
}

/// What one bounded maintenance pass accomplished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassOutcome {
    /// Work remains; run the next pass without waiting out the idle interval.
    Continue,
    /// The published generation matches the registry and nothing is left to prune.
    Idle,
    /// Shutdown was observed; no further storage mutation was attempted.
    Cancelled,
}

/// Per-pass work limits, so cancellation latency stays independent of registry
/// size. Tests shrink them to drive many passes over a small registry.
#[derive(Debug, Clone, Copy)]
struct PassBounds {
    /// Registry records per scan batch.
    batch: usize,
    /// Scan batches per pass.
    batches: usize,
    /// Index rows inspected per prune pass.
    prune: usize,
}

impl Default for PassBounds {
    fn default() -> Self {
        Self {
            batch: BUILD_BATCH,
            batches: PASS_BATCHES,
            prune: PRUNE_BATCH,
        }
    }
}

#[derive(Debug)]
pub enum VisibilityError {
    /// No complete generation exists, or the policy evaluation behind a page
    /// failed. Anonymous enumeration must fail rather than widen its scan.
    Unavailable,
    Storage(StorageReadError),
}

impl From<StorageReadError> for VisibilityError {
    fn from(error: StorageReadError) -> Self {
        Self::Storage(error)
    }
}

/// One page of anonymously visible records with the index cursor of each, so a
/// caller stopping early can resume at exactly the record it emitted last.
pub struct VisiblePage {
    pub entries: Vec<(Key, MetadataRegistryRecord)>,
    /// Index cursor to resume after. It names the last key the scan inspected,
    /// which may be a candidate the policy re-check discarded, so a page emptied
    /// by authorization still continues instead of ending the enumeration.
    pub next_after: Option<Key>,
    /// Index rows remain inside the window beyond `next_after`. False only when
    /// the window is genuinely exhausted.
    pub more: bool,
    /// The scan stopped on the candidate budget rather than on `limit` or the end
    /// of the window.
    pub budget_hit: bool,
}

fn index_key(generation: u64, updated_at_ms: u64, document_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&generation.to_be_bytes());
    bytes.extend_from_slice(&updated_at_ms.to_be_bytes());
    bytes.extend_from_slice(&document_id.to_bytes());
    ByteView::from(bytes)
}

fn parse_index_key(key: &[u8]) -> Result<(u64, u64, Ulid), ConversionError> {
    if key.len() != 32 {
        return Err(ConversionError::InvalidLength(format!(
            "expected 32-byte visibility index key, got {}",
            key.len()
        )));
    }
    let generation = u64::from_be_bytes(key[..8].try_into()?);
    let updated_at_ms = u64::from_be_bytes(key[8..16].try_into()?);
    let document_id = Ulid::from_bytes(key[16..32].try_into()?);
    Ok((generation, updated_at_ms, document_id))
}

fn state_key() -> Key {
    ByteView::from(b"visibility".to_vec())
}

/// Whether an anonymous caller may read `record` right now. Public documents are
/// still subject to the group's `metadata.read` request policies.
fn record_visible(record: &MetadataRegistryRecord, evaluators: &Evaluators) -> bool {
    if !record.public {
        return false;
    }
    let request = policy_request_with(
        &record.permission_path,
        &Permission::READ,
        None,
        PolicyRequestExtras::operation("metadata.read"),
    );
    evaluators
        .get(&(record.realm_id, record.group_id))
        .is_some_and(|evaluator| evaluator.evaluate(&request).is_ok())
}

type Evaluators = std::collections::HashMap<
    (aruna_core::structs::RealmId, aruna_core::types::GroupId),
    PolicyEvaluator,
>;

async fn load_evaluators(
    context: &DriverContext,
    records: &[MetadataRegistryRecord],
) -> Result<Evaluators, VisibilityError> {
    PolicyEvaluator::load_bulk(
        context,
        records
            .iter()
            .map(|record| (record.realm_id, record.group_id)),
    )
    .await
    .map_err(|_| VisibilityError::Unavailable)
}

/// Whether an anonymous caller may read one record right now, for the read-by-id
/// path that does not go through the index.
pub async fn anon_readable(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<bool, VisibilityError> {
    if !record.public {
        return Ok(false);
    }
    let evaluators = load_evaluators(context, std::slice::from_ref(record)).await?;
    Ok(record_visible(record, &evaluators))
}

/// Maintenance variant: one group whose policy state cannot be read is left out
/// of the index instead of blocking every other group's records from it.
async fn build_evaluators(
    context: &DriverContext,
    records: &[MetadataRegistryRecord],
) -> Evaluators {
    if let Ok(evaluators) = load_evaluators(context, records).await {
        return evaluators;
    }
    let mut scopes: Vec<_> = records
        .iter()
        .map(|record| (record.realm_id, record.group_id))
        .collect();
    scopes.sort();
    scopes.dedup();
    let mut evaluators = Evaluators::new();
    for (realm_id, group_id) in scopes {
        if let Ok(evaluator) = PolicyEvaluator::load(context, realm_id, Some(group_id)).await {
            evaluators.insert((realm_id, group_id), evaluator);
        }
    }
    evaluators
}

async fn read_state(context: &DriverContext) -> Result<Option<VisibilityState>, StorageReadError> {
    let event = context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: METADATA_VISIBILITY_STATE_KEYSPACE.to_string(),
            key: state_key(),
            txn_id: None,
        }))
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(
            value.and_then(|value| postcard::from_bytes::<VisibilityState>(value.as_ref()).ok())
        ),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::ReadError)),
    }
}

async fn write_state(
    context: &DriverContext,
    state: &VisibilityState,
) -> Result<(), StorageReadError> {
    let value: Value = postcard::to_allocvec(state)
        .map_err(|error| StorageReadError::Conversion(ConversionError::PostcardError(error)))?
        .into();
    let event = context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: METADATA_VISIBILITY_STATE_KEYSPACE.to_string(),
            key: state_key(),
            value,
            txn_id: None,
        }))
        .await;
    match event {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::WriteError)),
    }
}

fn scan_effect(start: IterStart, limit: usize) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: METADATA_VISIBILITY_INDEX_KEYSPACE.to_string(),
        prefix: None,
        start: Some(start),
        limit,
        txn_id: None,
    })
}

/// A scanned index batch: its entries and the storage cursor to resume after.
type IndexBatch = (Vec<(Key, Value)>, Option<Key>);

fn parse_scan(event: Event) -> Result<IndexBatch, StorageReadError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::ReadError)),
    }
}

/// Rewrites a cursor minted against an older generation onto the current one, so
/// a resumption token survives a rebuild without restarting the enumeration.
fn rebase_cursor(cursor: &Key, generation: u64) -> Option<Key> {
    let (_, updated_at_ms, document_id) = parse_index_key(cursor.as_ref()).ok()?;
    Some(index_key(generation, updated_at_ms, document_id))
}

/// Page anonymously visible records in `[from_ms, until_ms]` after `after`.
///
/// Fails closed: without a published generation, or when the per-record re-check
/// cannot be evaluated, the caller gets `Unavailable` rather than a wider scan.
pub async fn visible_page(
    context: &DriverContext,
    from_ms: u64,
    until_ms: u64,
    after: Option<Key>,
    limit: usize,
) -> Result<VisiblePage, VisibilityError> {
    let state = read_state(context)
        .await?
        .ok_or(VisibilityError::Unavailable)?;
    if !state.ready {
        return Err(VisibilityError::Unavailable);
    }
    let generation = state.generation;
    let mut start = match after
        .as_ref()
        .and_then(|key| rebase_cursor(key, generation))
    {
        Some(cursor) => IterStart::After(cursor),
        None => IterStart::At(index_key(generation, from_ms, Ulid::nil())),
    };

    let mut entries: Vec<(Key, MetadataRegistryRecord)> = Vec::new();
    let mut pending: Vec<(Key, MetadataRegistryRecord)> = Vec::new();
    let mut cursor: Option<Key> = None;
    let mut scanned = 0usize;
    let mut budget_hit = false;
    let mut more = true;

    'scan: loop {
        let event = context
            .storage_handle
            .send_effect(scan_effect(start.clone(), SCAN_BATCH))
            .await;
        let (batch, next) = parse_scan(event)?;
        if batch.is_empty() {
            more = false;
            break;
        }
        for (key, _) in batch {
            let (key_generation, updated_at_ms, document_id) =
                parse_index_key(key.as_ref()).map_err(StorageReadError::Conversion)?;
            if key_generation != generation || updated_at_ms > until_ms {
                more = false;
                break 'scan;
            }
            scanned += 1;
            cursor = Some(key.clone());
            if updated_at_ms >= from_ms
                && let Some(record) =
                    crate::get_metadata_document::load_metadata_record_by_document(
                        context,
                        document_id,
                    )
                    .await?
                && record.updated_at_ms == updated_at_ms
            {
                pending.push((key, record));
            }
            // Re-check as soon as the candidates in hand could fill the page, so
            // denied leading candidates are replaced instead of ending the page.
            if entries.len() + pending.len() >= limit {
                admit_visible(context, &mut pending, &mut entries).await?;
                if entries.len() >= limit {
                    break 'scan;
                }
            }
            if scanned >= CANDIDATE_BUDGET {
                budget_hit = true;
                break 'scan;
            }
        }
        match next {
            Some(next) => start = IterStart::After(next),
            None => {
                more = false;
                break;
            }
        }
    }
    admit_visible(context, &mut pending, &mut entries).await?;

    // A batch that authorized past `limit` resumes at the last kept entry; the
    // surplus is served by the next request rather than dropped.
    if entries.len() > limit {
        entries.truncate(limit);
        cursor = entries.last().map(|(key, _)| key.clone());
        more = true;
    }
    Ok(VisiblePage {
        entries,
        next_after: cursor,
        more,
        budget_hit,
    })
}

/// Applies the anonymous policy re-check to the candidates in hand and moves the
/// survivors into `entries`.
async fn admit_visible(
    context: &DriverContext,
    pending: &mut Vec<(Key, MetadataRegistryRecord)>,
    entries: &mut Vec<(Key, MetadataRegistryRecord)>,
) -> Result<(), VisibilityError> {
    if pending.is_empty() {
        return Ok(());
    }
    let records: Vec<MetadataRegistryRecord> =
        pending.iter().map(|(_, record)| record.clone()).collect();
    let evaluators = load_evaluators(context, &records).await?;
    entries.extend(
        pending
            .drain(..)
            .filter(|(_, record)| record_visible(record, &evaluators)),
    );
    Ok(())
}

/// The datestamp of the oldest anonymously visible record, for `earliestDatestamp`.
///
/// Follows continuations so a run of denied leading candidates cannot report a
/// repository as empty, and fails closed rather than reporting a datestamp that
/// is not the oldest once the walk budget is spent.
pub async fn earliest_visible(context: &DriverContext) -> Result<Option<u64>, VisibilityError> {
    let mut after = None;
    for _ in 0..EARLIEST_PAGES {
        let page = visible_page(context, 0, u64::MAX, after, 1).await?;
        if let Some((_, record)) = page.entries.first() {
            return Ok(Some(record.updated_at_ms));
        }
        if !page.more {
            return Ok(None);
        }
        after = page.next_after;
        if after.is_none() {
            return Ok(None);
        }
    }
    Err(VisibilityError::Unavailable)
}

/// Folds one visible record into a pass fingerprint. XOR keeps the fold
/// order-independent and resumable, so a pass can persist it and continue later.
fn fold_visible(digest: &mut [u8; 32], updated_at_ms: u64, document_id: Ulid) {
    let mut bytes = [0u8; 24];
    bytes[..8].copy_from_slice(&updated_at_ms.to_be_bytes());
    bytes[8..].copy_from_slice(&document_id.to_bytes());
    for (slot, byte) in digest.iter_mut().zip(blake3::hash(&bytes).as_bytes()) {
        *slot ^= byte;
    }
}

/// One bounded step of index maintenance.
///
/// A cycle walks the registry comparing it against the published generation and
/// writes a new one only once the visible set actually differs, so a steady-state
/// pass rewrites nothing. Every storage mutation is gated on `token`, and the
/// walk keeps a persisted cursor so a pass never spans the whole registry.
pub async fn visibility_pass(
    context: &DriverContext,
    token: &CancellationToken,
) -> Result<PassOutcome, VisibilityError> {
    pass_bounded(context, token, PassBounds::default()).await
}

async fn pass_bounded(
    context: &DriverContext,
    token: &CancellationToken,
    bounds: PassBounds,
) -> Result<PassOutcome, VisibilityError> {
    if token.is_cancelled() {
        return Ok(PassOutcome::Cancelled);
    }
    let mut state = read_state(context).await?.unwrap_or_default();
    if state.pruning {
        return prune_pass(context, token, state, bounds).await;
    }
    let mut pass = state.pass.clone().unwrap_or_else(|| PassState {
        cursor: None,
        digest: [0u8; 32],
        // Nothing is published yet, so there is nothing to compare against.
        building: (!state.ready).then_some(state.generation + 1),
    });

    for _ in 0..bounds.batches {
        if token.is_cancelled() {
            return Ok(PassOutcome::Cancelled);
        }
        let cursor = pass.cursor.clone().map(Key::from);
        let page = enumerate_updated(context, 0, u64::MAX, cursor, bounds.batch).await?;
        let evaluators = build_evaluators(context, &page.records).await;
        let visible: Vec<&MetadataRegistryRecord> = page
            .records
            .iter()
            .filter(|record| record_visible(record, &evaluators))
            .collect();
        for record in &visible {
            fold_visible(&mut pass.digest, record.updated_at_ms, record.document_id);
        }
        if let Some(generation) = pass.building {
            let writes: Vec<(String, Key, Value)> = visible
                .iter()
                .map(|record| {
                    (
                        METADATA_VISIBILITY_INDEX_KEYSPACE.to_string(),
                        index_key(generation, record.updated_at_ms, record.document_id),
                        ByteView::from(Vec::new()),
                    )
                })
                .collect();
            if !writes.is_empty() {
                if token.is_cancelled() {
                    return Ok(PassOutcome::Cancelled);
                }
                write_index_keys(context, writes).await?;
            }
        }
        match page.next_after {
            Some(next) => pass.cursor = Some(next.as_ref().to_vec()),
            None => return finish_pass(context, token, state, pass).await,
        }
    }

    if token.is_cancelled() {
        return Ok(PassOutcome::Cancelled);
    }
    state.pass = Some(pass);
    write_state(context, &state).await?;
    Ok(PassOutcome::Continue)
}

/// Closes a walk that reached the end of the registry: publish what it built,
/// start building when the comparison found a difference, or go quiet.
async fn finish_pass(
    context: &DriverContext,
    token: &CancellationToken,
    mut state: VisibilityState,
    pass: PassState,
) -> Result<PassOutcome, VisibilityError> {
    if token.is_cancelled() {
        return Ok(PassOutcome::Cancelled);
    }
    match pass.building {
        Some(generation) => {
            state.generation = generation;
            state.ready = true;
            state.digest = pass.digest;
            state.pass = None;
            state.pruning = true;
            state.prune_after = None;
            write_state(context, &state).await?;
            Ok(PassOutcome::Continue)
        }
        None if pass.digest == state.digest => {
            if state.pass.is_some() {
                state.pass = None;
                write_state(context, &state).await?;
            }
            Ok(PassOutcome::Idle)
        }
        None => {
            state.pass = Some(PassState {
                cursor: None,
                digest: [0u8; 32],
                building: Some(state.generation + 1),
            });
            write_state(context, &state).await?;
            Ok(PassOutcome::Continue)
        }
    }
}

/// Deletes one bounded batch of index rows outside the published generation.
async fn prune_pass(
    context: &DriverContext,
    token: &CancellationToken,
    mut state: VisibilityState,
    bounds: PassBounds,
) -> Result<PassOutcome, VisibilityError> {
    let start = match state.prune_after.clone() {
        Some(cursor) => IterStart::After(Key::from(cursor)),
        None => IterStart::At(index_key(0, 0, Ulid::nil())),
    };
    let event = context
        .storage_handle
        .send_effect(scan_effect(start, bounds.prune))
        .await;
    let (batch, next) = parse_scan(event)?;
    let mut stale = Vec::new();
    for (key, _) in batch {
        let (key_generation, _, _) =
            parse_index_key(key.as_ref()).map_err(StorageReadError::Conversion)?;
        if key_generation != state.generation {
            stale.push(key);
        }
    }
    if token.is_cancelled() {
        return Ok(PassOutcome::Cancelled);
    }
    delete_index_keys(context, METADATA_VISIBILITY_INDEX_KEYSPACE, stale).await?;
    match next {
        Some(next) => state.prune_after = Some(next.as_ref().to_vec()),
        None => {
            state.pruning = false;
            state.prune_after = None;
        }
    }
    if token.is_cancelled() {
        return Ok(PassOutcome::Cancelled);
    }
    write_state(context, &state).await?;
    Ok(if state.pruning {
        PassOutcome::Continue
    } else {
        PassOutcome::Idle
    })
}

/// Drives maintenance to a quiet state and returns the published generation.
/// Bootstrap and test helper; the background task runs one pass per tick.
pub async fn rebuild_index(context: &DriverContext) -> Result<u64, VisibilityError> {
    drive_index(context, &CancellationToken::new(), PassBounds::default()).await
}

async fn drive_index(
    context: &DriverContext,
    token: &CancellationToken,
    bounds: PassBounds,
) -> Result<u64, VisibilityError> {
    for _ in 0..DRIVE_PASSES {
        if pass_bounded(context, token, bounds).await? != PassOutcome::Continue {
            break;
        }
    }
    Ok(read_state(context)
        .await?
        .map(|state| state.generation)
        .unwrap_or(0))
}

async fn write_index_keys(
    context: &DriverContext,
    writes: Vec<(String, Key, Value)>,
) -> Result<(), StorageReadError> {
    let event = context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        }))
        .await;
    match event {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::WriteError)),
    }
}

/// Keeps the index current on the shutdown supervisor, one bounded pass per tick.
/// A record whose visibility or governing policy changed is picked up by the next
/// cycle; until then the reader's per-record re-check keeps a newly-hidden record
/// from being served.
pub fn spawn_visibility_index(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            let delay = match visibility_pass(&context, &token).await {
                Ok(PassOutcome::Continue) => WORK_INTERVAL,
                Ok(PassOutcome::Idle) => REBUILD_INTERVAL,
                Ok(PassOutcome::Cancelled) => return,
                Err(error) => {
                    warn!(error = ?error, "Anonymous visibility index pass failed");
                    REBUILD_INTERVAL
                }
            };
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::repository::create_records_and_outbox_write_entries;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, MetadataAuditOperation, MetadataAuditRecord,
        PlacementRef, RealmConfigDocument, RealmId,
    };
    use aruna_core::{NodeId, UserId};
    use aruna_storage::storage;
    use std::collections::{HashMap, HashSet};

    const REALM: RealmId = RealmId([1; 32]);

    fn actor() -> Actor {
        Actor {
            node_id: NodeId::from_bytes(&[1u8; 32]).expect("node id"),
            user_id: UserId::local(Ulid::from_bytes([2; 16]), REALM),
            realm_id: REALM,
        }
    }

    fn deny_policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([6u8; 16]),
            name: "oai".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    fn record(
        group_id: Ulid,
        document_id: Ulid,
        updated_at_ms: u64,
        public: bool,
    ) -> MetadataRegistryRecord {
        MetadataRegistryRecord {
            realm_id: REALM,
            group_id,
            document_id,
            document_path: format!("doc/{document_id}"),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &REALM,
                group_id,
                &format!("doc/{document_id}"),
                document_id,
            ),
            placement: PlacementRef {
                strategy_id: Ulid::nil(),
                epoch: 0,
                shard: 0,
            },
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms,
            establishing_event_id: Ulid::from_bytes([8; 16]),
            last_event_id: Ulid::from_bytes([9; 16]),
        }
    }

    fn audit(record: &MetadataRegistryRecord) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id: record.realm_id,
            group_id: record.group_id,
            document_id: record.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: Default::default(),
            node_id: NodeId::from_bytes(&[1u8; 32]).expect("node id"),
            operation: MetadataAuditOperation::Create,
            occurred_at_ms: record.updated_at_ms,
            details: None,
        }
    }

    fn context() -> (DriverContext, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (context, dir)
    }

    async fn write(context: &DriverContext, writes: Vec<(String, Key, Value)>) {
        let event = context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    async fn seed_realm(context: &DriverContext, policies: Vec<RequestPolicy>) {
        let mut config = RealmConfigDocument::new(REALM, Vec::new(), 1);
        config.request_policies = policies;
        let target = aruna_core::document::DocumentSyncTarget::RealmConfig { realm_id: REALM };
        write(
            context,
            vec![(
                target.storage_keyspace().to_string(),
                target.storage_key(),
                config.to_bytes(&actor()).unwrap().into(),
            )],
        )
        .await;
    }

    async fn seed_group(context: &DriverContext, group_id: Ulid, policies: Vec<RequestPolicy>) {
        let group = Group {
            display_name: "g".to_string(),
            group_id,
            realm_id: REALM,
            roles: HashSet::new(),
            owner: actor().user_id,
        };
        let auth = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::new(),
            policies,
        };
        write(
            context,
            vec![
                (
                    GROUP_KEYSPACE.to_string(),
                    ByteView::from(group_id.to_bytes().to_vec()),
                    group.to_bytes(&actor()).unwrap().into(),
                ),
                (
                    AUTH_KEYSPACE.to_string(),
                    ByteView::from(group_id.to_bytes().to_vec()),
                    postcard::to_allocvec(&auth).unwrap().into(),
                ),
            ],
        )
        .await;
    }

    async fn seed_record(context: &DriverContext, record: &MetadataRegistryRecord) {
        let writes =
            create_records_and_outbox_write_entries(record, &audit(record), Ulid::generate(), None)
                .unwrap();
        write(context, writes).await;
    }

    async fn seed_many(context: &DriverContext, group_id: Ulid, range: std::ops::Range<u64>) {
        for index in range {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&index.to_be_bytes());
            seed_record(
                context,
                &record(group_id, Ulid::from_bytes(bytes), 100 + index, true),
            )
            .await;
        }
    }

    // More than one budget of leading candidates denied after publication must
    // not end the scan: the page continues and the later group still enumerates.
    #[tokio::test]
    async fn scan_passes_denied() {
        let (context, _dir) = context();
        let denied = Ulid::from_bytes([91; 16]);
        let allowed = Ulid::from_bytes([92; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(&context, denied, Vec::new()).await;
        seed_group(&context, allowed, Vec::new()).await;
        seed_many(&context, denied, 0..(CANDIDATE_BUDGET as u64 + 8)).await;
        let first_allowed = CANDIDATE_BUDGET as u64 + 8;
        seed_many(&context, allowed, first_allowed..first_allowed + 3).await;
        rebuild_index(&context).await.unwrap();

        seed_group(
            &context,
            denied,
            vec![deny_policy("operation == 'metadata.read'")],
        )
        .await;

        let first = visible_page(&context, 0, u64::MAX, None, 10).await.unwrap();
        assert!(first.entries.is_empty());
        assert!(first.more);
        assert!(first.budget_hit);
        assert!(first.next_after.is_some());

        let second = visible_page(&context, 0, u64::MAX, first.next_after, 10)
            .await
            .unwrap();
        assert_eq!(second.entries.len(), 3);
        assert!(!second.more);
        assert_eq!(
            earliest_visible(&context).await.unwrap(),
            Some(100 + first_allowed)
        );
    }

    // A page cut at `limit` inside one re-check batch must resume at its last
    // kept entry, so the surplus is served next instead of being skipped.
    #[tokio::test]
    async fn limit_keeps_surplus() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([93; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(&context, group_id, Vec::new()).await;
        seed_many(&context, group_id, 0..6).await;
        rebuild_index(&context).await.unwrap();

        let mut after = None;
        let mut stamps = Vec::new();
        for _ in 0..6 {
            let page = visible_page(&context, 0, u64::MAX, after, 2).await.unwrap();
            stamps.extend(page.entries.iter().map(|(_, record)| record.updated_at_ms));
            after = page.next_after;
            if !page.more {
                break;
            }
        }
        assert_eq!(stamps, vec![100, 101, 102, 103, 104, 105]);
    }

    #[tokio::test]
    async fn public_allowed_indexed() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([11; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(&context, group_id, Vec::new()).await;
        let public = record(group_id, Ulid::from_bytes([12; 16]), 100, true);
        let private = record(group_id, Ulid::from_bytes([13; 16]), 200, false);
        seed_record(&context, &public).await;
        seed_record(&context, &private).await;

        rebuild_index(&context).await.unwrap();
        let page = visible_page(&context, 0, u64::MAX, None, 10).await.unwrap();
        let ids: Vec<Ulid> = page
            .entries
            .iter()
            .map(|(_, record)| record.document_id)
            .collect();
        assert_eq!(ids, vec![public.document_id]);
    }

    #[tokio::test]
    async fn denied_stays_hidden() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([21; 16]);
        seed_realm(&context, vec![deny_policy("operation == 'metadata.read'")]).await;
        seed_group(&context, group_id, Vec::new()).await;
        let denied = record(group_id, Ulid::from_bytes([22; 16]), 100, true);
        seed_record(&context, &denied).await;

        rebuild_index(&context).await.unwrap();
        let page = visible_page(&context, 0, u64::MAX, None, 10).await.unwrap();
        assert!(page.entries.is_empty());
    }

    // A policy that starts denying after the pass must not be served from the
    // stale index; the reader re-checks every candidate.
    #[tokio::test]
    async fn recheck_drops_denied() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([31; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(&context, group_id, Vec::new()).await;
        let visible = record(group_id, Ulid::from_bytes([32; 16]), 100, true);
        seed_record(&context, &visible).await;
        rebuild_index(&context).await.unwrap();
        assert_eq!(
            visible_page(&context, 0, u64::MAX, None, 10)
                .await
                .unwrap()
                .entries
                .len(),
            1
        );

        seed_group(
            &context,
            group_id,
            vec![deny_policy("operation == 'metadata.read'")],
        )
        .await;
        let page = visible_page(&context, 0, u64::MAX, None, 10).await.unwrap();
        assert!(page.entries.is_empty());
    }

    #[tokio::test]
    async fn unbuilt_fails_closed() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([41; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(&context, group_id, Vec::new()).await;
        seed_record(
            &context,
            &record(group_id, Ulid::from_bytes([42; 16]), 100, true),
        )
        .await;

        assert!(matches!(
            visible_page(&context, 0, u64::MAX, None, 10).await,
            Err(VisibilityError::Unavailable)
        ));
        assert!(matches!(
            earliest_visible(&context).await,
            Err(VisibilityError::Unavailable)
        ));
    }

    // Losing the realm policy state must not downgrade to serving the index.
    #[tokio::test]
    async fn loss_fails_closed() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([51; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(&context, group_id, Vec::new()).await;
        seed_record(
            &context,
            &record(group_id, Ulid::from_bytes([52; 16]), 100, true),
        )
        .await;
        rebuild_index(&context).await.unwrap();

        let target = aruna_core::document::DocumentSyncTarget::RealmConfig { realm_id: REALM };
        let event = context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Delete {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));

        assert!(matches!(
            visible_page(&context, 0, u64::MAX, None, 10).await,
            Err(VisibilityError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn earliest_from_index() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([61; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(&context, group_id, Vec::new()).await;
        seed_record(
            &context,
            &record(group_id, Ulid::from_bytes([62; 16]), 50, false),
        )
        .await;
        seed_record(
            &context,
            &record(group_id, Ulid::from_bytes([63; 16]), 400, true),
        )
        .await;
        rebuild_index(&context).await.unwrap();

        assert_eq!(earliest_visible(&context).await.unwrap(), Some(400));
    }

    // A rebuild republishes into a new generation and drops the previous one, so
    // a cursor minted before the flip still resumes in the right place.
    #[tokio::test]
    async fn rebuild_rotates_generation() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([71; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(&context, group_id, Vec::new()).await;
        for seed in 0..3u8 {
            seed_record(
                &context,
                &record(
                    group_id,
                    Ulid::from_bytes([80 + seed; 16]),
                    100 + seed as u64,
                    true,
                ),
            )
            .await;
        }
        assert_eq!(rebuild_index(&context).await.unwrap(), 1);
        let first = visible_page(&context, 0, u64::MAX, None, 1).await.unwrap();
        let cursor = first.entries.last().map(|(key, _)| key.clone());

        seed_record(
            &context,
            &record(group_id, Ulid::from_bytes([83; 16]), 103, true),
        )
        .await;
        assert_eq!(rebuild_index(&context).await.unwrap(), 2);
        let page = visible_page(&context, 0, u64::MAX, cursor, 10)
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 3);
    }

    fn small_bounds() -> PassBounds {
        PassBounds {
            batch: 4,
            batches: 1,
            prune: 4,
        }
    }

    async fn index_keys(context: &DriverContext) -> Vec<Vec<u8>> {
        let mut start = IterStart::At(index_key(0, 0, Ulid::nil()));
        let mut keys = Vec::new();
        loop {
            let event = context
                .storage_handle
                .send_effect(scan_effect(start.clone(), SCAN_BATCH))
                .await;
            let (batch, next) = parse_scan(event).unwrap();
            keys.extend(batch.into_iter().map(|(key, _)| key.as_ref().to_vec()));
            match next {
                Some(next) => start = IterStart::After(next),
                None => break,
            }
        }
        keys
    }

    async fn seed_realm_group(context: &DriverContext, group_id: Ulid, count: u64) {
        seed_realm(context, Vec::new()).await;
        seed_group(context, group_id, Vec::new()).await;
        seed_many(context, group_id, 0..count).await;
    }

    // One pass yields at its bound instead of walking the whole registry, so
    // cancellation is observed within a fixed amount of work.
    #[tokio::test]
    async fn pass_yields_bound() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([94; 16]);
        seed_realm_group(&context, group_id, 20).await;

        let token = CancellationToken::new();
        assert_eq!(
            pass_bounded(&context, &token, small_bounds())
                .await
                .unwrap(),
            PassOutcome::Continue
        );
        let state = read_state(&context).await.unwrap().unwrap();
        assert!(!state.ready);
        assert!(state.pass.is_some_and(|pass| pass.cursor.is_some()));
        assert!(matches!(
            visible_page(&context, 0, u64::MAX, None, 10).await,
            Err(VisibilityError::Unavailable)
        ));
    }

    // A cancelled token stops the build before any further storage mutation.
    #[tokio::test]
    async fn build_honors_cancel() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([95; 16]);
        seed_realm_group(&context, group_id, 20).await;

        let token = CancellationToken::new();
        pass_bounded(&context, &token, small_bounds())
            .await
            .unwrap();
        let before = index_keys(&context).await;
        assert!(!before.is_empty());

        token.cancel();
        for _ in 0..4 {
            assert_eq!(
                pass_bounded(&context, &token, small_bounds())
                    .await
                    .unwrap(),
                PassOutcome::Cancelled
            );
        }
        assert_eq!(index_keys(&context).await, before);
    }

    // The same holds once the cycle has moved on to deleting superseded rows.
    #[tokio::test]
    async fn prune_honors_cancel() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([96; 16]);
        seed_realm_group(&context, group_id, 20).await;
        let token = CancellationToken::new();
        drive_index(&context, &token, small_bounds()).await.unwrap();

        seed_many(&context, group_id, 20..24).await;
        let mut pruning = false;
        for _ in 0..64 {
            pass_bounded(&context, &token, small_bounds())
                .await
                .unwrap();
            if read_state(&context).await.unwrap().unwrap().pruning {
                pruning = true;
                break;
            }
        }
        assert!(pruning);

        let before = index_keys(&context).await;
        token.cancel();
        assert_eq!(
            pass_bounded(&context, &token, small_bounds())
                .await
                .unwrap(),
            PassOutcome::Cancelled
        );
        assert_eq!(index_keys(&context).await, before);
        assert!(read_state(&context).await.unwrap().unwrap().pruning);
    }

    // A pass over an unchanged registry must publish nothing, so steady state
    // costs no index writes at all.
    #[tokio::test]
    async fn steady_rewrites_nothing() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([97; 16]);
        seed_realm_group(&context, group_id, 20).await;
        let generation = rebuild_index(&context).await.unwrap();
        let before = index_keys(&context).await;
        assert_eq!(before.len(), 20);

        assert_eq!(rebuild_index(&context).await.unwrap(), generation);
        assert_eq!(index_keys(&context).await, before);

        // A real change still publishes, and the superseded rows are pruned.
        seed_many(&context, group_id, 20..21).await;
        assert_eq!(rebuild_index(&context).await.unwrap(), generation + 1);
        let after = index_keys(&context).await;
        assert_eq!(after.len(), 21);
        assert!(
            after
                .iter()
                .all(|key| parse_index_key(key).unwrap().0 == generation + 1)
        );
    }

    // A policy change alone is a change of the visible set.
    #[tokio::test]
    async fn policy_change_republishes() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([98; 16]);
        seed_realm_group(&context, group_id, 5).await;
        let generation = rebuild_index(&context).await.unwrap();
        assert_eq!(index_keys(&context).await.len(), 5);

        seed_group(
            &context,
            group_id,
            vec![deny_policy("operation == 'metadata.read'")],
        )
        .await;
        assert_eq!(rebuild_index(&context).await.unwrap(), generation + 1);
        assert!(index_keys(&context).await.is_empty());
    }

    #[test]
    fn index_key_roundtrips() {
        let document_id = Ulid::from_bytes([3; 16]);
        let key = index_key(7, 1_234, document_id);
        assert_eq!(
            parse_index_key(key.as_ref()).unwrap(),
            (7, 1_234, document_id)
        );
        assert!(parse_index_key(&[0u8; 24]).is_err());
    }

    #[test]
    fn cursor_rebases_generation() {
        let document_id = Ulid::from_bytes([4; 16]);
        let old = index_key(1, 900, document_id);
        let rebased = rebase_cursor(&old, 5).unwrap();
        assert_eq!(rebased.as_ref(), index_key(5, 900, document_id).as_ref());
    }

    // Ordering must be generation-major then datestamp so a published generation
    // is one contiguous range.
    #[test]
    fn keys_order_generation() {
        let document_id = Ulid::from_bytes([5; 16]);
        assert!(
            index_key(1, u64::MAX, document_id).as_ref() < index_key(2, 0, document_id).as_ref()
        );
        assert!(index_key(2, 1, document_id).as_ref() < index_key(2, 2, document_id).as_ref());
    }
}
