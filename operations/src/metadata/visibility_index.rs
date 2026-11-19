//! Keeps a timestamp-ordered index of the records an anonymous caller may read.
//! A background pass publishes a generation that readers re-check instead of scanning.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{VISIBILITY_INDEX_KEYSPACE, VISIBILITY_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{all_values, event};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{GroupId, Key, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::auth::enforcement::{PolicyBulkOperation, PolicyEvaluator, PolicyLoadOperation};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::repository::StorageReadError;
use crate::metadata::updated_page::{UpdatedPageError, UpdatedPageOperation, UpdatedRecordsPage};
use crate::storage_read::parse_storage_scan;

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
/// Pause after a pass that found nothing to do, or failed.
pub const REBUILD_INTERVAL: Duration = Duration::from_secs(60);
/// Pacing between the passes of a cycle that still has work to do.
pub const WORK_INTERVAL: Duration = Duration::from_secs(1);

/// The single row naming the generation readers may serve, plus the resumable
/// state of the maintenance cycle.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct VisibilityState {
    generation: u64,
    ready: bool,
    /// Fingerprint of the visible set the published generation was built from.
    /// A pass whose fingerprint matches it publishes nothing.
    digest: [u8; 32],
    /// Largest effective OAI datestamp lifted above a registry timestamp because
    /// the record was newly exposed by a visibility change.
    max_uplift_ms: u64,
    /// The registry walk in flight, if any.
    pass: Option<PassState>,
    /// Superseded generations may still hold rows; resume deleting after this key.
    prune_after: Option<Vec<u8>>,
    pruning: bool,
}

/// A resumable registry walk. It compares the registry against the published
/// generation until a difference shows up, and only then writes a new one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PassState {
    /// Timestamp-index cursor to resume the walk after.
    cursor: Option<Vec<u8>>,
    /// Fingerprint accumulated so far.
    digest: [u8; 32],
    /// The generation being written; `None` while the walk only compares.
    building: Option<u64>,
    /// Time the visible-set change was detected. Initial construction preserves
    /// registry timestamps; later additions use this as their OAI creation time.
    changed_at_ms: Option<u64>,
    /// Largest uplift copied or created while building this generation.
    max_uplift_ms: u64,
}

/// What one bounded maintenance pass accomplished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassOutcome {
    /// Work remains; run the next pass without waiting out the idle interval.
    Continue,
    /// The published generation matches the registry and nothing is left to prune.
    Idle,
}

/// Per-pass work limits, so shutdown latency stays independent of registry size. Tests
/// shrink them to drive many passes over a small registry.
#[derive(Debug, Clone, Copy, PartialEq)]
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

#[derive(Debug, PartialEq)]
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

impl From<UpdatedPageError> for VisibilityError {
    fn from(error: UpdatedPageError) -> Self {
        Self::Storage(match error {
            UpdatedPageError::Storage(error) => StorageReadError::Storage(error),
            UpdatedPageError::Conversion(error) => StorageReadError::Conversion(error),
            error => StorageReadError::Storage(StorageError::ReadError(error.to_string())),
        })
    }
}

/// One page of anonymously visible records with the index cursor of each, so a
/// caller stopping early can resume at exactly the record it emitted last.
#[derive(Debug, PartialEq)]
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

fn index_datestamp(value: &[u8]) -> Result<u64, ConversionError> {
    if value.len() != 8 {
        return Err(ConversionError::InvalidLength(format!(
            "expected 8-byte visibility datestamp, got {}",
            value.len()
        )));
    }
    Ok(u64::from_be_bytes(value.try_into()?))
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

type Evaluators = std::collections::HashMap<(RealmId, GroupId), PolicyEvaluator>;

fn scan_effect(start: IterStart, limit: usize) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: VISIBILITY_INDEX_KEYSPACE.to_string(),
        prefix: None,
        start: Some(start),
        limit,
        txn_id: None,
    })
}

/// Rewrites a cursor minted against an older generation onto the current one, so
/// a resumption token survives a rebuild without restarting the enumeration.
fn rebase_cursor(cursor: &Key, generation: u64) -> Option<Key> {
    let (_, updated_at_ms, document_id) = parse_index_key(cursor.as_ref()).ok()?;
    Some(index_key(generation, updated_at_ms, document_id))
}

impl From<IoError> for VisibilityError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

fn state_read() -> IoCall {
    IoCall::Read(vec![(VISIBILITY_STATE_KEYSPACE.to_string(), state_key())])
}

/// The visibility state a [`state_read`] answered; an unreadable row reads as none.
fn read_state(result: IoResult) -> Result<Option<VisibilityState>, VisibilityError> {
    let value = result.value()?;
    Ok(value.and_then(|value| postcard::from_bytes::<VisibilityState>(value.as_ref()).ok()))
}

fn state_write(state: &VisibilityState) -> Result<IoCall, VisibilityError> {
    let value: Value = postcard::to_allocvec(state)
        .map_err(|error| StorageReadError::Conversion(ConversionError::PostcardError(error)))?
        .into();
    let row = (VISIBILITY_STATE_KEYSPACE.to_string(), state_key(), value);
    Ok(IoCall::Write(vec![row]))
}

fn index_row(generation: u64, updated_at_ms: u64, document_id: Ulid) -> (String, Key) {
    let key = index_key(generation, updated_at_ms, document_id);
    (VISIBILITY_INDEX_KEYSPACE.to_string(), key)
}

fn datestamp_value(value: Option<Value>) -> Result<Option<u64>, VisibilityError> {
    let datestamp = value
        .map(|value| index_datestamp(value.as_ref()))
        .transpose();
    Ok(datestamp.map_err(StorageReadError::Conversion)?)
}

fn scan_call(start: IterStart, limit: usize) -> IoCall {
    IoCall::Effect(Box::new(scan_effect(start, limit)))
}

fn scanned(result: IoResult) -> Result<(Vec<(Key, Value)>, Option<Key>), VisibilityError> {
    Ok(parse_storage_scan(event(result)?)?)
}

fn evaluators_call(records: &[MetadataRegistryRecord]) -> IoCall {
    let scopes = records
        .iter()
        .map(|record| (record.realm_id, record.group_id));
    IoCall::child(PolicyBulkOperation::new(scopes))
}

/// The evaluators of an [`evaluators_call`]; a failed load fails closed.
fn evaluators(result: IoResult) -> Result<Evaluators, VisibilityError> {
    let loaded = result.child::<Evaluators, PolicyEnforcementError>()?;
    loaded.map_err(|_| VisibilityError::Unavailable)
}

/// Whether an anonymous caller may read one record right now, for the read-by-id path that
/// does not go through the index.
#[derive(Debug, PartialEq)]
pub struct AnonReadableOperation {
    record: MetadataRegistryRecord,
}

impl AnonReadableOperation {
    pub fn new(record: MetadataRegistryRecord) -> Self {
        Self { record }
    }
}

impl Operation for AnonReadableOperation {
    type Output = bool;
    type Error = VisibilityError;

    fn first(&mut self) -> Result<Next<bool>, VisibilityError> {
        if !self.record.public {
            return Ok(Next::Done(false));
        }
        Ok(Next::Call(evaluators_call(std::slice::from_ref(
            &self.record,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, VisibilityError> {
        let evaluators = evaluators(result)?;
        Ok(Next::Done(record_visible(&self.record, &evaluators)))
    }
}

/// Effective OAI datestamp for a currently published record. A missing row means the
/// visibility generation has not yet caught up, so GetRecord must fail closed rather than
/// expose an old datestamp that an incremental harvest cannot find.
#[derive(Debug, PartialEq)]
pub struct EffectiveDatestampOperation {
    record: MetadataRegistryRecord,
    state_read: bool,
}

impl EffectiveDatestampOperation {
    pub fn new(record: MetadataRegistryRecord) -> Self {
        Self {
            record,
            state_read: false,
        }
    }
}

impl Operation for EffectiveDatestampOperation {
    type Output = u64;
    type Error = VisibilityError;

    fn first(&mut self) -> Result<Next<u64>, VisibilityError> {
        Ok(Next::Call(state_read()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, VisibilityError> {
        if !self.state_read {
            let state = read_state(result)?.filter(|state| state.ready);
            let state = state.ok_or(VisibilityError::Unavailable)?;
            self.state_read = true;
            let record = &self.record;
            let row = index_row(state.generation, record.updated_at_ms, record.document_id);
            return Ok(Next::Call(IoCall::Read(vec![row])));
        }
        let effective = datestamp_value(result.value()?)?.ok_or(VisibilityError::Unavailable)?;
        if effective < self.record.updated_at_ms {
            return Err(VisibilityError::Unavailable);
        }
        Ok(Next::Done(effective))
    }
}

#[derive(Debug, PartialEq)]
enum PageStage {
    State,
    Scan,
    Record(Key, u64),
    Admit,
}

/// Pages anonymously visible records in `[from_ms, until_ms]` after `after`. Fails closed: no
/// published generation or an unevaluable per-record check yields `Unavailable` rather than a
/// wider scan.
#[derive(Debug, PartialEq)]
pub struct VisiblePageOperation {
    window: (u64, u64),
    after: Option<Key>,
    limit: usize,
    generation: u64,
    start: Option<IterStart>,
    batch: std::collections::VecDeque<(Key, Value)>,
    next: Option<Key>,
    entries: Vec<(Key, MetadataRegistryRecord)>,
    pending: Vec<(Key, MetadataRegistryRecord)>,
    cursor: Option<Key>,
    scanned: usize,
    budget_hit: bool,
    more: bool,
    /// The scan ended; the page waits only for its last re-check.
    ended: bool,
    stage: PageStage,
}

type PageTurn = Result<Next<VisiblePage>, VisibilityError>;

impl VisiblePageOperation {
    pub fn new((from_ms, until_ms): (u64, u64), after: Option<Key>, limit: usize) -> Self {
        Self {
            window: (from_ms, until_ms),
            after,
            limit,
            generation: 0,
            start: None,
            batch: std::collections::VecDeque::new(),
            next: None,
            entries: Vec::new(),
            pending: Vec::new(),
            cursor: None,
            scanned: 0,
            budget_hit: false,
            more: true,
            ended: false,
            stage: PageStage::State,
        }
    }

    fn scan(&mut self) -> PageTurn {
        let start = (self.start.take()).ok_or(VisibilityError::Unavailable)?;
        self.stage = PageStage::Scan;
        Ok(Next::Call(scan_call(start, SCAN_BATCH)))
    }

    /// Ends the scan and re-checks the candidates still in hand.
    fn end(&mut self) -> PageTurn {
        self.ended = true;
        self.admit()
    }

    fn admit(&mut self) -> PageTurn {
        if self.pending.is_empty() {
            return self.admitted();
        }
        let records: Vec<_> = self
            .pending
            .iter()
            .map(|(_, record)| record.clone())
            .collect();
        self.stage = PageStage::Admit;
        Ok(Next::Call(evaluators_call(&records)))
    }

    fn admitted(&mut self) -> PageTurn {
        if self.ended || self.entries.len() >= self.limit {
            return Ok(Next::Done(self.page()));
        }
        if let Some(turn) = self.budget() {
            return turn;
        }
        self.rows()
    }

    /// Walks the batch in hand until a row needs its record or the scan ends.
    fn rows(&mut self) -> PageTurn {
        let (from_ms, until_ms) = self.window;
        while let Some((key, value)) = self.batch.pop_front() {
            let (generation, updated_at_ms, document_id) =
                parse_index_key(key.as_ref()).map_err(StorageReadError::Conversion)?;
            if generation != self.generation || updated_at_ms > until_ms {
                self.more = false;
                return self.end();
            }
            let effective_ms =
                index_datestamp(value.as_ref()).map_err(StorageReadError::Conversion)?;
            if effective_ms < updated_at_ms {
                return Err(VisibilityError::Unavailable);
            }
            self.scanned += 1;
            self.cursor = Some(key.clone());
            if effective_ms >= from_ms && effective_ms <= until_ms {
                self.stage = PageStage::Record(key, effective_ms);
                let read = DocumentRecordOperation::new(document_id);
                return Ok(Next::Call(IoCall::child(read)));
            }
            if let Some(turn) = self.checked() {
                return turn;
            }
        }
        match self.next.take() {
            Some(next) => {
                self.start = Some(IterStart::After(next));
                self.scan()
            }
            None => {
                self.more = false;
                self.end()
            }
        }
    }

    /// Re-checks as soon as the candidates in hand could fill the page, so denied leading
    /// candidates are replaced instead of ending the page; stops at the candidate budget.
    fn checked(&mut self) -> Option<PageTurn> {
        if self.entries.len() + self.pending.len() >= self.limit {
            return Some(self.admit());
        }
        self.budget()
    }

    /// Stops the scan once it inspected [`CANDIDATE_BUDGET`] index rows.
    fn budget(&mut self) -> Option<PageTurn> {
        if self.scanned < CANDIDATE_BUDGET {
            return None;
        }
        self.budget_hit = true;
        Some(self.end())
    }

    fn page(&mut self) -> VisiblePage {
        let mut entries = std::mem::take(&mut self.entries);
        let mut cursor = self.cursor.take();
        let mut more = self.more;
        // A batch that authorized past `limit` resumes at the last kept entry; the surplus is
        // served by the next request rather than dropped.
        if entries.len() > self.limit {
            entries.truncate(self.limit);
            cursor = entries.last().map(|(key, _)| key.clone());
            more = true;
        }
        VisiblePage {
            entries,
            next_after: cursor,
            more,
            budget_hit: self.budget_hit,
        }
    }
}

impl Operation for VisiblePageOperation {
    type Output = VisiblePage;
    type Error = VisibilityError;

    fn first(&mut self) -> PageTurn {
        Ok(Next::Call(state_read()))
    }

    fn next(&mut self, result: IoResult) -> PageTurn {
        match std::mem::replace(&mut self.stage, PageStage::Scan) {
            PageStage::State => {
                let state = read_state(result)?.ok_or(VisibilityError::Unavailable)?;
                if !state.ready {
                    return Err(VisibilityError::Unavailable);
                }
                self.generation = state.generation;
                let from_ms = self.window.0;
                let scan_from_ms = if from_ms <= state.max_uplift_ms {
                    0
                } else {
                    from_ms
                };
                let rebased =
                    (self.after.as_ref()).and_then(|key| rebase_cursor(key, state.generation));
                self.start = Some(match rebased {
                    Some(cursor) => IterStart::After(cursor),
                    None => IterStart::At(index_key(state.generation, scan_from_ms, Ulid::nil())),
                });
                self.scan()
            }
            PageStage::Scan => {
                let (batch, next) = scanned(result)?;
                if batch.is_empty() {
                    self.more = false;
                    return self.end();
                }
                self.batch = batch.into();
                self.next = next;
                self.rows()
            }
            PageStage::Record(key, effective_ms) => {
                let record =
                    result.child::<Option<MetadataRegistryRecord>, StorageReadError>()??;
                let (_, updated_at_ms, _) =
                    parse_index_key(key.as_ref()).map_err(StorageReadError::Conversion)?;
                if let Some(mut record) =
                    record.filter(|record| record.updated_at_ms == updated_at_ms)
                {
                    record.updated_at_ms = effective_ms;
                    self.pending.push((key, record));
                }
                if let Some(turn) = self.checked() {
                    return turn;
                }
                self.rows()
            }
            PageStage::Admit => {
                let evaluators = evaluators(result)?;
                let pending = std::mem::take(&mut self.pending);
                let visible = pending
                    .into_iter()
                    .filter(|(_, record)| record_visible(record, &evaluators));
                self.entries.extend(visible);
                self.admitted()
            }
        }
    }
}

/// The datestamp of the oldest anonymously visible record, for `earliestDatestamp`. Follows
/// continuations so denied leading candidates do not report an empty repository, and fails
/// closed once the walk budget is spent.
#[derive(Debug, PartialEq, Default)]
pub struct EarliestVisibleOperation {
    pages: usize,
}

impl EarliestVisibleOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn page(&mut self, after: Option<Key>) -> Result<Next<Option<u64>>, VisibilityError> {
        if self.pages == EARLIEST_PAGES {
            return Err(VisibilityError::Unavailable);
        }
        self.pages += 1;
        let page = VisiblePageOperation::new((0, u64::MAX), after, 1);
        Ok(Next::Call(IoCall::child(page)))
    }
}

impl Operation for EarliestVisibleOperation {
    type Output = Option<u64>;
    type Error = VisibilityError;

    fn first(&mut self) -> Result<Next<Option<u64>>, VisibilityError> {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<u64>>, VisibilityError> {
        let page = result.child::<VisiblePage, VisibilityError>()??;
        if let Some((_, record)) = page.entries.first() {
            return Ok(Next::Done(Some(record.updated_at_ms)));
        }
        match page.next_after.filter(|_| page.more) {
            Some(after) => self.page(Some(after)),
            None => Ok(Next::Done(None)),
        }
    }
}

/// Folds one visible record into a pass fingerprint. XOR keeps the fold order-independent
/// and resumable, so a pass can persist it and continue later.
fn fold_visible(digest: &mut [u8; 32], updated_at_ms: u64, document_id: Ulid) {
    let mut bytes = [0u8; 24];
    bytes[..8].copy_from_slice(&updated_at_ms.to_be_bytes());
    bytes[8..].copy_from_slice(&document_id.to_bytes());
    for (slot, byte) in digest.iter_mut().zip(blake3::hash(&bytes).as_bytes()) {
        *slot ^= byte;
    }
}

#[derive(Debug, PartialEq)]
enum PassStage {
    State,
    Page,
    /// The bulk policy load failed, so each group loads alone; a group whose policy state
    /// cannot be read is left out of the index instead of blocking every other group.
    Groups(Box<GroupLoads>),
    Evaluators(Vec<MetadataRegistryRecord>),
    Datestamps(Vec<MetadataRegistryRecord>),
    Written,
    Saved(PassOutcome),
    PruneScan,
    Pruned(Option<Key>),
}

/// The per-group policy loads of one page after its bulk load failed.
#[derive(Debug, PartialEq)]
struct GroupLoads {
    records: Vec<MetadataRegistryRecord>,
    scopes: std::collections::VecDeque<(RealmId, GroupId)>,
    loading: (RealmId, GroupId),
    evaluators: Evaluators,
}

/// One bounded step of index maintenance. A cycle writes a new generation only once the
/// visible set differs, so a steady-state pass rewrites nothing; a persisted cursor keeps
/// the walk bounded.
#[derive(Debug, PartialEq)]
pub struct VisibilityPassOperation {
    bounds: PassBounds,
    state: VisibilityState,
    pass: Option<PassState>,
    batches: usize,
    next_after: Option<Key>,
    now_ms: u64,
    stage: PassStage,
}

type PassTurn = Result<Next<PassOutcome>, VisibilityError>;

impl Default for VisibilityPassOperation {
    fn default() -> Self {
        Self::bounded(PassBounds::default())
    }
}

impl VisibilityPassOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn bounded(bounds: PassBounds) -> Self {
        Self {
            bounds,
            state: VisibilityState::default(),
            pass: None,
            batches: 0,
            next_after: None,
            now_ms: 0,
            stage: PassStage::State,
        }
    }

    fn pass(&mut self) -> Result<&mut PassState, VisibilityError> {
        (self.pass.as_mut()).ok_or(VisibilityError::Unavailable)
    }

    fn page(&mut self) -> PassTurn {
        if self.batches == self.bounds.batches {
            let pass = self.pass.take();
            self.state.pass = pass;
            return self.save(PassOutcome::Continue);
        }
        self.batches += 1;
        let cursor = self.pass()?.cursor.clone().map(Key::from);
        self.stage = PassStage::Page;
        let page = UpdatedPageOperation::new(0, u64::MAX, cursor, self.bounds.batch);
        Ok(Next::Call(IoCall::child(page)))
    }

    fn save(&mut self, outcome: PassOutcome) -> PassTurn {
        self.stage = PassStage::Saved(outcome);
        Ok(Next::Call(state_write(&self.state)?))
    }

    /// Folds the visible records of one page and, while building, reads their datestamps in
    /// the published generation when the change needs them.
    fn evaluated(
        &mut self,
        records: Vec<MetadataRegistryRecord>,
        evaluators: &Evaluators,
    ) -> PassTurn {
        let visible: Vec<_> = (records.into_iter())
            .filter(|record| record_visible(record, evaluators))
            .collect();
        let pass = self.pass()?;
        for record in &visible {
            fold_visible(&mut pass.digest, record.updated_at_ms, record.document_id);
        }
        let (building, changed) = (pass.building, pass.changed_at_ms);
        if building.is_none() || visible.is_empty() {
            return self.batched();
        }
        if changed.is_none() {
            let datestamps = vec![None; visible.len()];
            return self.build(visible, datestamps);
        }
        let generation = self.state.generation;
        let rows = (visible.iter())
            .map(|record| index_row(generation, record.updated_at_ms, record.document_id))
            .collect();
        self.stage = PassStage::Datestamps(visible);
        Ok(Next::Call(IoCall::Read(rows)))
    }

    /// Writes the visible records into the generation being built.
    fn build(
        &mut self,
        visible: Vec<MetadataRegistryRecord>,
        datestamps: Vec<Option<u64>>,
    ) -> PassTurn {
        let pass = self.pass()?;
        let generation = pass.building.ok_or(VisibilityError::Unavailable)?;
        let mut writes = Vec::with_capacity(visible.len());
        for (record, published) in visible.iter().zip(datestamps) {
            let effective_ms = match pass.changed_at_ms {
                Some(changed_at_ms) => {
                    published.unwrap_or_else(|| record.updated_at_ms.max(changed_at_ms))
                }
                None => record.updated_at_ms,
            };
            if effective_ms < record.updated_at_ms {
                return Err(VisibilityError::Unavailable);
            }
            if effective_ms > record.updated_at_ms {
                pass.max_uplift_ms = pass.max_uplift_ms.max(effective_ms);
            }
            let (key_space, key) = index_row(generation, record.updated_at_ms, record.document_id);
            writes.push((
                key_space,
                key,
                ByteView::from(effective_ms.to_be_bytes().to_vec()),
            ));
        }
        self.stage = PassStage::Written;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Loads the next group's policies alone, then evaluates the page with those that loaded.
    fn load_group(&mut self, mut loads: GroupLoads) -> PassTurn {
        let Some((realm_id, group_id)) = loads.scopes.pop_front() else {
            let GroupLoads {
                records,
                evaluators,
                ..
            } = loads;
            return self.evaluated(records, &evaluators);
        };
        loads.loading = (realm_id, group_id);
        self.stage = PassStage::Groups(Box::new(loads));
        let load = PolicyLoadOperation::new(realm_id, Some(group_id));
        Ok(Next::Call(IoCall::child(load)))
    }

    fn batched(&mut self) -> PassTurn {
        match self.next_after.take() {
            Some(next) => {
                self.pass()?.cursor = Some(next.as_ref().to_vec());
                self.page()
            }
            None => self.finish(),
        }
    }

    /// Closes a walk that reached the end of the registry: publish what it built, start
    /// building when the comparison found a difference, or go quiet.
    fn finish(&mut self) -> PassTurn {
        let pass = self.pass.take().ok_or(VisibilityError::Unavailable)?;
        match pass.building {
            Some(generation) => {
                let state = &mut self.state;
                state.generation = generation;
                state.ready = true;
                state.digest = pass.digest;
                state.max_uplift_ms = pass.max_uplift_ms;
                state.pass = None;
                state.pruning = true;
                state.prune_after = None;
                self.save(PassOutcome::Continue)
            }
            None if pass.digest == self.state.digest => {
                if self.state.pass.is_none() {
                    return Ok(Next::Done(PassOutcome::Idle));
                }
                self.state.pass = None;
                self.save(PassOutcome::Idle)
            }
            None => {
                self.state.pass = Some(PassState {
                    cursor: None,
                    digest: [0u8; 32],
                    building: Some(self.state.generation + 1),
                    changed_at_ms: Some(self.now_ms),
                    max_uplift_ms: 0,
                });
                self.save(PassOutcome::Continue)
            }
        }
    }

    /// Deletes one bounded batch of index rows outside the published generation.
    fn prune(&mut self, result: IoResult) -> PassTurn {
        let (batch, next) = scanned(result)?;
        let mut stale = Vec::new();
        for (key, _) in batch {
            let (generation, _, _) =
                parse_index_key(key.as_ref()).map_err(StorageReadError::Conversion)?;
            if generation != self.state.generation {
                stale.push((VISIBILITY_INDEX_KEYSPACE.to_string(), key));
            }
        }
        self.stage = PassStage::Pruned(next);
        Ok(Next::Call(IoCall::Delete(stale)))
    }
}

impl Operation for VisibilityPassOperation {
    type Output = PassOutcome;
    type Error = VisibilityError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PassTurn {
        Ok(Next::Call(state_read()))
    }

    fn next(&mut self, result: IoResult) -> PassTurn {
        match std::mem::replace(&mut self.stage, PassStage::Written) {
            PassStage::State => {
                self.state = read_state(result)?.unwrap_or_default();
                if self.state.pruning {
                    let start = match self.state.prune_after.clone() {
                        Some(cursor) => IterStart::After(Key::from(cursor)),
                        None => IterStart::At(index_key(0, 0, Ulid::nil())),
                    };
                    self.stage = PassStage::PruneScan;
                    return Ok(Next::Call(scan_call(start, self.bounds.prune)));
                }
                let state = &self.state;
                self.pass = Some(state.pass.clone().unwrap_or_else(|| PassState {
                    cursor: None,
                    digest: [0u8; 32],
                    // Nothing is published yet, so there is nothing to compare against.
                    building: (!state.ready).then_some(state.generation + 1),
                    changed_at_ms: None,
                    max_uplift_ms: 0,
                }));
                self.page()
            }
            PassStage::Page => {
                let page = result.child::<UpdatedRecordsPage, UpdatedPageError>()??;
                self.next_after = page.next_after;
                self.stage = PassStage::Evaluators(page.records.clone());
                Ok(Next::Call(evaluators_call(&page.records)))
            }
            PassStage::Evaluators(records) => {
                match result.child::<Evaluators, PolicyEnforcementError>()? {
                    Ok(evaluators) => self.evaluated(records, &evaluators),
                    Err(_) => {
                        let mut scopes: Vec<_> = (records.iter())
                            .map(|record| (record.realm_id, record.group_id))
                            .collect();
                        scopes.sort();
                        scopes.dedup();
                        let loads = GroupLoads {
                            records,
                            scopes: scopes.into(),
                            loading: (RealmId::from_bytes([0; 32]), GroupId::nil()),
                            evaluators: Evaluators::new(),
                        };
                        self.load_group(loads)
                    }
                }
            }
            PassStage::Groups(mut loads) => {
                if let Ok(evaluator) = result.child::<PolicyEvaluator, PolicyEnforcementError>()? {
                    loads.evaluators.insert(loads.loading, evaluator);
                }
                self.load_group(*loads)
            }
            PassStage::Datestamps(visible) => {
                let values = all_values(result)?;
                let datestamps = (values.into_iter())
                    .map(datestamp_value)
                    .collect::<Result<Vec<_>, _>>()?;
                self.build(visible, datestamps)
            }
            PassStage::Written => {
                result.done()?;
                self.batched()
            }
            PassStage::Saved(outcome) => {
                result.done()?;
                Ok(Next::Done(outcome))
            }
            PassStage::PruneScan => self.prune(result),
            PassStage::Pruned(next) => {
                result.done()?;
                match next {
                    Some(next) => self.state.prune_after = Some(next.as_ref().to_vec()),
                    None => {
                        self.state.pruning = false;
                        self.state.prune_after = None;
                    }
                }
                let outcome = match self.state.pruning {
                    true => PassOutcome::Continue,
                    false => PassOutcome::Idle,
                };
                self.save(outcome)
            }
        }
    }
}

/// Drives maintenance to a quiet state and returns the published generation; a bootstrap
/// and test step, as the background task runs one pass per tick.
#[derive(Debug, PartialEq, Default)]
pub struct RebuildIndexOperation {
    bounds: PassBounds,
    passes: usize,
    reading: bool,
}

impl RebuildIndexOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn pass(&mut self) -> Result<Next<u64>, VisibilityError> {
        self.passes += 1;
        let pass = VisibilityPassOperation::bounded(self.bounds);
        Ok(Next::Call(IoCall::child(pass)))
    }
}

impl Operation for RebuildIndexOperation {
    type Output = u64;
    type Error = VisibilityError;

    fn first(&mut self) -> Result<Next<u64>, VisibilityError> {
        self.pass()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, VisibilityError> {
        if self.reading {
            let generation = read_state(result)?.map(|state| state.generation);
            return Ok(Next::Done(generation.unwrap_or(0)));
        }
        let outcome = result.child::<PassOutcome, VisibilityError>()??;
        if outcome == PassOutcome::Continue && self.passes < DRIVE_PASSES {
            return self.pass();
        }
        self.reading = true;
        Ok(Next::Call(state_read()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::metadata::repository::create_outbox_entries;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::time::unix_timestamp_millis;
    use aruna_core::{NodeId, UserId};
    use aruna_storage::storage;
    use std::collections::{HashMap, HashSet};

    const REALM: RealmId = RealmId([1; 32]);

    async fn read_state(
        context: &DriverContext,
    ) -> Result<Option<VisibilityState>, StorageReadError> {
        let read = StorageEffect::Read {
            key_space: VISIBILITY_STATE_KEYSPACE.to_string(),
            key: state_key(),
            txn_id: None,
        };
        match context
            .storage_handle
            .send_effect(Effect::Storage(read))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
                .and_then(|value| postcard::from_bytes::<VisibilityState>(value.as_ref()).ok())),
            other => Err(StorageReadError::Storage(StorageError::ReadError(
                other.label().to_string(),
            ))),
        }
    }

    async fn visible_page(
        context: &DriverContext,
        from_ms: u64,
        until_ms: u64,
        after: Option<Key>,
        limit: usize,
    ) -> Result<VisiblePage, VisibilityError> {
        drive(
            VisiblePageOperation::new((from_ms, until_ms), after, limit),
            context,
        )
        .await
    }

    async fn earliest_visible(context: &DriverContext) -> Result<Option<u64>, VisibilityError> {
        drive(EarliestVisibleOperation::new(), context).await
    }

    async fn effective_datestamp(
        context: &DriverContext,
        record: &MetadataRegistryRecord,
    ) -> Result<u64, VisibilityError> {
        drive(EffectiveDatestampOperation::new(record.clone()), context).await
    }

    async fn rebuild_index(context: &DriverContext) -> Result<u64, VisibilityError> {
        drive(RebuildIndexOperation::new(), context).await
    }

    async fn pass_bounded(
        context: &DriverContext,
        bounds: PassBounds,
    ) -> Result<PassOutcome, VisibilityError> {
        drive(VisibilityPassOperation::bounded(bounds), context).await
    }

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
                shard: 0,
            },
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms,
            establishing_event_id: Ulid::from_bytes([8; 16]),
            last_event_id: Ulid::from_bytes([9; 16]),
        }
    }

    fn context() -> (DriverContext, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
        let target = aruna_core::document::DocumentTarget::RealmConfig { realm_id: REALM };
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
        let writes = create_outbox_entries(
            record,
            &crate::tests::metadata::create_audit(record),
            Ulid::generate(),
            None,
        )
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

        let target = aruna_core::document::DocumentTarget::RealmConfig { realm_id: REALM };
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
            let (batch, next) = parse_storage_scan(event).unwrap();
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
    // shutdown is observed within a fixed amount of work.
    #[tokio::test]
    async fn pass_yields_bound() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([94; 16]);
        seed_realm_group(&context, group_id, 20).await;

        assert_eq!(
            pass_bounded(&context, small_bounds()).await.unwrap(),
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

    #[tokio::test]
    async fn newly_allowed_record() {
        let (context, _dir) = context();
        let group_id = Ulid::from_bytes([99; 16]);
        seed_realm(&context, Vec::new()).await;
        seed_group(
            &context,
            group_id,
            vec![deny_policy("operation == 'metadata.read'")],
        )
        .await;
        let record = record(group_id, Ulid::from_bytes([100; 16]), 100, true);
        seed_record(&context, &record).await;
        rebuild_index(&context).await.unwrap();

        let exposed_after = unix_timestamp_millis();
        seed_group(&context, group_id, Vec::new()).await;
        rebuild_index(&context).await.unwrap();

        let page = visible_page(&context, exposed_after, u64::MAX, None, 10)
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 1);
        let effective = page.entries[0].1.updated_at_ms;
        assert!(effective >= exposed_after);
        assert_eq!(
            effective_datestamp(&context, &record).await.unwrap(),
            effective
        );
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
        assert_eq!(index_datestamp(&1_234u64.to_be_bytes()).unwrap(), 1_234);
        assert!(index_datestamp(&[0u8; 7]).is_err());
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

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::placement::record::PlacementRef;

    fn record(public: bool) -> MetadataRegistryRecord {
        let document_id = Ulid::from_parts(3, 3);
        MetadataRegistryRecord {
            realm_id: RealmId::from_bytes([1; 32]),
            group_id: Ulid::from_parts(2, 2),
            document_id,
            document_path: "doc".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public,
            permission_path: String::new(),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 10,
            establishing_event_id: Ulid::from_parts(4, 4),
            last_event_id: Ulid::from_parts(4, 4),
        }
    }

    fn state(state: &VisibilityState) -> IoResult {
        let value = postcard::to_allocvec(state).unwrap();
        IoResult::Values(vec![Some(ByteView::from(value))])
    }

    #[test]
    fn private_never_anonymous() {
        let mut readable = AnonReadableOperation::new(record(false));
        assert_eq!(readable.first(), Ok(Next::Done(false)));
    }

    #[test]
    fn unpublished_datestamp_unavailable() {
        let mut datestamp = EffectiveDatestampOperation::new(record(true));
        assert_eq!(datestamp.first(), Ok(Next::Call(state_read())));
        let unready = VisibilityState::default();
        assert_eq!(
            datestamp.next(state(&unready)),
            Err(VisibilityError::Unavailable)
        );
    }

    #[test]
    fn published_datestamp_read() {
        let mut datestamp = EffectiveDatestampOperation::new(record(true));
        datestamp.first().unwrap();
        let ready = VisibilityState {
            generation: 2,
            ready: true,
            ..VisibilityState::default()
        };
        let row = index_row(2, 10, Ulid::from_parts(3, 3));
        assert_eq!(
            datestamp.next(state(&ready)),
            Ok(Next::Call(IoCall::Read(vec![row])))
        );
        let value = IoResult::Values(vec![Some(ByteView::from(12u64.to_be_bytes().to_vec()))]);
        assert_eq!(datestamp.next(value), Ok(Next::Done(12)));
    }

    #[test]
    fn missing_state_unavailable() {
        let mut page = VisiblePageOperation::new((0, u64::MAX), None, 5);
        page.first().unwrap();
        let missing = IoResult::Values(vec![None]);
        assert_eq!(page.next(missing), Err(VisibilityError::Unavailable));
    }

    #[test]
    fn prune_ends_idle() {
        let mut pass = VisibilityPassOperation::new();
        pass.first().unwrap();
        let pruning = VisibilityState {
            generation: 3,
            ready: true,
            pruning: true,
            ..VisibilityState::default()
        };
        assert!(matches!(
            pass.next(state(&pruning)),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let stale = index_key(2, 10, Ulid::from_parts(3, 3));
        let scan = Event::Storage(StorageEvent::IterResult {
            values: vec![(stale.clone(), ByteView::from(10u64.to_be_bytes().to_vec()))],
            next_start_after: None,
        });
        let deleted = pass.next(IoResult::Event(Box::new(scan)));
        let row = (VISIBILITY_INDEX_KEYSPACE.to_string(), stale);
        assert_eq!(deleted, Ok(Next::Call(IoCall::Delete(vec![row]))));
        assert!(matches!(
            pass.next(IoResult::Done),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        assert_eq!(pass.next(IoResult::Done), Ok(Next::Done(PassOutcome::Idle)));
    }

    #[test]
    fn rebuild_reads_generation() {
        let mut rebuild = RebuildIndexOperation::new();
        assert!(matches!(rebuild.first(), Ok(Next::Call(IoCall::Child(_)))));
        let idle: Result<PassOutcome, VisibilityError> = Ok(PassOutcome::Idle);
        let answered = rebuild.next(IoResult::Child(ChildOutcome::new(idle)));
        assert_eq!(answered, Ok(Next::Call(state_read())));
        let published = VisibilityState {
            generation: 7,
            ..VisibilityState::default()
        };
        assert_eq!(rebuild.next(state(&published)), Ok(Next::Done(7)));
    }
}
