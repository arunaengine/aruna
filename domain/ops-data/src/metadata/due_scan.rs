//! Scans due materialization jobs, reports whether any remain and prunes stale job rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

/// Due jobs, whether more are due beyond the limit, and when the next one falls due.
pub(super) type DueJobs = (ScannedJobs, bool, Option<u64>);

#[derive(Debug, PartialEq)]
pub(super) enum ScanStage {
    Page,
    Sidecars(Vec<DueCandidate>),
    Live(ScannedJobs),
    Prune(bool),
}

// The due index is due-ordered and a row is valid only when the sidecar row matches its due
// time.
#[derive(Debug, PartialEq)]
pub(super) struct DueJobsOperation {
    pub(super) now_ms: u64,
    pub(super) limit: usize,
    pub(super) jobs: ScannedJobs,
    pub(super) candidates: VecDeque<DueCandidate>,
    pub(super) stale: Vec<StaleJob>,
    pub(super) next_due_ms: Option<u64>,
    pub(super) next: Option<Key>,
    pub(super) stage: ScanStage,
}

impl DueJobsOperation {
    pub(super) fn new(now_ms: u64, limit: usize) -> Self {
        Self {
            now_ms,
            limit,
            jobs: Vec::new(),
            candidates: VecDeque::new(),
            stale: Vec::new(),
            next_due_ms: None,
            next: None,
            stage: ScanStage::Page,
        }
    }

    pub(super) fn page(&mut self, after: Option<Key>) -> MaterializationTurn<DueJobs> {
        self.stage = ScanStage::Page;
        Ok(Next::Call(IoCall::Page {
            space: MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: MATERIALIZATION_PAGE_SIZE,
        }))
    }

    pub(super) fn scanned(&mut self, rows: Vec<(Key, Value)>) -> MaterializationTurn<DueJobs> {
        for (key, _value) in rows {
            let key = key.to_vec();
            let Some((due_at_ms, document_id, event_id)) = job_key_parts(&key) else {
                warn!(key = ?key, "Deleting malformed metadata materialization index row");
                self.stale.push(StaleJob {
                    index_key: key,
                    seen: None,
                    drop_sidecar: false,
                });
                continue;
            };
            if due_at_ms > self.now_ms {
                self.next_due_ms = Some(due_at_ms);
                break;
            }
            (self.candidates).push_back((key, due_at_ms, document_id, event_id));
        }
        self.resolve()
    }

    // Resolved in slices of the outstanding limit so a probe for a single
    // due job does not read a whole page of sidecars, events and statuses.
    pub(super) fn resolve(&mut self) -> MaterializationTurn<DueJobs> {
        if self.candidates.is_empty() {
            return self.prune(false);
        }
        let remaining = self.limit.saturating_sub(self.jobs.len()).max(1);
        let end = self.candidates.len().min(remaining);
        let slice: Vec<DueCandidate> = self.candidates.drain(..end).collect();
        let reads = (slice.iter())
            .map(|(_, _, document_id, event_id)| sidecar_read(*document_id, *event_id))
            .collect();
        self.stage = ScanStage::Sidecars(slice);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    pub(super) fn sidecars(
        &mut self,
        slice: Vec<DueCandidate>,
        sidecars: Vec<Option<Value>>,
    ) -> MaterializationTurn<DueJobs> {
        let mut due = Vec::with_capacity(slice.len());
        for ((key, due_at_ms, _, _), sidecar) in slice.into_iter().zip(sidecars) {
            let sidecar = sidecar.and_then(|value| {
                postcard::from_bytes::<MetadataMaterializationRecord>(&value).ok()
            });
            match sidecar {
                Some(job) if job.due_at_ms == due_at_ms => due.push((key, job)),
                // The sidecar is authoritative: an index row it does not match is
                // stale. A missing or malformed sidecar is dropped with it.
                sidecar => self.stale.push(StaleJob {
                    index_key: key,
                    drop_sidecar: sidecar.is_none(),
                    seen: sidecar,
                }),
            }
        }
        if due.is_empty() {
            return self.resolve();
        }
        let documents: BTreeSet<Ulid> = due.iter().map(|(_, job)| job.document_id).collect();
        let statuses = documents
            .iter()
            .map(|document_id| status_read(*document_id));
        let events = (due.iter()).map(|(_, job)| event_read(job.document_id, job.event_id));
        let reads = statuses.chain(events).collect();
        self.stage = ScanStage::Live(due);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    // Live means the create event still exists and no status has advanced past the
    // job. Dead jobs are pruned with both of their rows.
    pub(super) fn live(
        &mut self,
        due: ScannedJobs,
        values: Vec<Option<Value>>,
    ) -> MaterializationTurn<DueJobs> {
        let documents: BTreeSet<Ulid> = due.iter().map(|(_, job)| job.document_id).collect();
        let mut values = values.into_iter();
        let mut statuses = HashMap::new();
        for (document_id, value) in documents.into_iter().zip(values.by_ref()) {
            if let Some(status) = decode_status(value)? {
                statuses.insert(document_id, status);
            }
        }
        let mut live = Vec::with_capacity(due.len());
        for ((key, job), event) in due.into_iter().zip(values) {
            let merge = (event.as_ref())
                .and_then(|event| postcard::from_bytes::<MetadataEventRecord>(event).ok())
                .is_some_and(|event| merges(&event));
            let advanced = (statuses.get(&job.document_id))
                .is_some_and(|status| job_superseded(status, &job, merge));
            if event.is_none() || advanced {
                self.stale.push(StaleJob {
                    index_key: key,
                    seen: Some(job),
                    drop_sidecar: true,
                });
            } else {
                live.push(job);
            }
        }
        for job in live {
            self.jobs
                .push((materialization_job_key(&job).to_vec(), job));
            if self.jobs.len() >= self.limit {
                return self.prune(true);
            }
        }
        self.resolve()
    }

    pub(super) fn prune(&mut self, full: bool) -> MaterializationTurn<DueJobs> {
        let stale = std::mem::take(&mut self.stale);
        if stale.is_empty() {
            return self.after_prune(full);
        }
        self.stage = ScanStage::Prune(full);
        Ok(Next::Call(IoCall::child(PruneJobsOperation::new(stale))))
    }

    pub(super) fn after_prune(&mut self, full: bool) -> MaterializationTurn<DueJobs> {
        if full {
            return Ok(Next::Done((std::mem::take(&mut self.jobs), true, None)));
        }
        if let Some(due_at_ms) = self.next_due_ms {
            let jobs = std::mem::take(&mut self.jobs);
            return Ok(Next::Done((jobs, false, Some(due_at_ms))));
        }
        match self.next.take() {
            Some(after) => self.page(Some(after)),
            None => Ok(Next::Done((std::mem::take(&mut self.jobs), false, None))),
        }
    }
}

impl Operation for DueJobsOperation {
    type Output = DueJobs;
    type Error = MetadataMaterializationError;

    fn first(&mut self) -> MaterializationTurn<DueJobs> {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<DueJobs> {
        match std::mem::replace(&mut self.stage, ScanStage::Page) {
            ScanStage::Page => {
                let (rows, next) = result.page()?;
                self.next = next;
                self.scanned(rows)
            }
            ScanStage::Sidecars(slice) => self.sidecars(slice, result.values()?),
            ScanStage::Live(due) => self.live(due, result.values()?),
            ScanStage::Prune(full) => {
                pruned(result)?;
                self.after_prune(full)
            }
        }
    }
}

/// One index row seen by the scan: its key, the due time encoded in it and the
/// sidecar it points at.
pub(super) type DueCandidate = (Vec<u8>, u64, Ulid, Ulid);

pub(super) type ScannedJobs = Vec<(Vec<u8>, MetadataMaterializationRecord)>;

#[derive(Debug, PartialEq)]
pub(super) enum ExistStage {
    Page,
    Sidecar(Vec<u8>, u64),
    Live(Vec<u8>, MetadataMaterializationRecord),
    Pruned,
    Removed,
}

/// Whether a live job is queued. Dead and stale rows met on the way are deleted, so a queue
/// of only dead jobs answers `false` and is empty afterwards.
#[derive(Debug, PartialEq)]
pub struct JobsExistOperation {
    pub(super) next: Option<Key>,
    pub(super) stage: ExistStage,
}

impl JobsExistOperation {
    pub fn new() -> Self {
        Self {
            next: None,
            stage: ExistStage::Page,
        }
    }

    pub(super) fn page(&mut self, after: Option<Key>) -> MaterializationTurn<bool> {
        self.stage = ExistStage::Page;
        Ok(Next::Call(IoCall::Page {
            space: MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: 1,
        }))
    }

    pub(super) fn advance(&mut self) -> MaterializationTurn<bool> {
        match self.next.take() {
            Some(after) => self.page(Some(after)),
            None => Ok(Next::Done(false)),
        }
    }

    pub(super) fn remove(&mut self, deletes: Vec<(String, ByteView)>) -> MaterializationTurn<bool> {
        self.stage = ExistStage::Removed;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

impl Default for JobsExistOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for JobsExistOperation {
    type Output = bool;
    type Error = MetadataMaterializationError;

    fn first(&mut self) -> MaterializationTurn<bool> {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<bool> {
        match std::mem::replace(&mut self.stage, ExistStage::Removed) {
            ExistStage::Page => {
                let (rows, next) = result.page()?;
                self.next = next;
                let Some((key, _value)) = rows.into_iter().next() else {
                    return Ok(Next::Done(false));
                };
                let key = key.to_vec();
                let Some((due_at_ms, document_id, event_id)) = job_key_parts(&key) else {
                    return self.remove(vec![global_row(key)]);
                };
                self.stage = ExistStage::Sidecar(key, due_at_ms);
                Ok(Next::Call(IoCall::Read(vec![sidecar_read(
                    document_id,
                    event_id,
                )])))
            }
            ExistStage::Sidecar(key, due_at_ms) => {
                let value = result.value()?;
                let job = value
                    .map(|value| postcard::from_bytes::<MetadataMaterializationRecord>(&value));
                match job {
                    Some(Ok(job)) if job.due_at_ms == due_at_ms => {
                        let reads = vec![
                            event_read(job.document_id, job.event_id),
                            status_read(job.document_id),
                        ];
                        self.stage = ExistStage::Live(key, job);
                        Ok(Next::Call(IoCall::Read(reads)))
                    }
                    Some(Err(error)) => {
                        let sidecar = job_key_target(&key).map(|(document_id, event_id)| {
                            warn!(error = %error, document_id = %document_id, event_id = %event_id, "Deleting malformed metadata materialization document job");
                            sidecar_read(document_id, event_id)
                        });
                        let deletes = sidecar.into_iter().chain([global_row(key)]).collect();
                        self.remove(deletes)
                    }
                    _ => self.remove(vec![global_row(key)]),
                }
            }
            ExistStage::Live(key, job) => {
                let [event, status] = values::<2>(result)?;
                let live = match logged_event(job.document_id, job.event_id, event) {
                    Ok(event) => !(decode_status(status)?)
                        .is_some_and(|status| job_superseded(&status, &job, merges(&event))),
                    Err(MetadataMaterializationError::CreateEventMissing { .. }) => false,
                    Err(error) => return Err(error),
                };
                if live {
                    return Ok(Next::Done(true));
                }
                let stale = StaleJob {
                    index_key: key,
                    seen: Some(job),
                    drop_sidecar: true,
                };
                self.stage = ExistStage::Pruned;
                Ok(Next::Call(IoCall::child(PruneJobsOperation::new(vec![
                    stale,
                ]))))
            }
            ExistStage::Pruned => {
                pruned(result)?;
                self.advance()
            }
            ExistStage::Removed => {
                result.done()?;
                self.advance()
            }
        }
    }
}

pub(super) fn global_row(key: Vec<u8>) -> (String, ByteView) {
    (
        MATERIALIZATION_JOB_KEYSPACE.to_string(),
        ByteView::from(key),
    )
}

/// An index row a scan found stale or dead, the sidecar it read for it and whether that
/// sidecar, with its capture mark, goes too.
#[derive(Debug, PartialEq)]
pub(super) struct StaleJob {
    pub(super) index_key: Vec<u8>,
    pub(super) seen: Option<MetadataMaterializationRecord>,
    pub(super) drop_sidecar: bool,
}

#[derive(Debug, PartialEq)]
pub(super) enum PruneStage {
    Read,
    Delete,
}

/// Deletes stale job rows after reading their sidecars again in one transaction. A sidecar a
/// newer retry rewrote keeps its rows and capture mark; a conflict leaves all to the next scan.
#[derive(Debug, PartialEq)]
pub(super) struct PruneJobsOperation {
    pub(super) stale: Vec<StaleJob>,
    pub(super) stage: PruneStage,
}

impl PruneJobsOperation {
    pub(super) fn new(stale: Vec<StaleJob>) -> Self {
        Self {
            stale,
            stage: PruneStage::Read,
        }
    }

    pub(super) fn delete(&mut self, sidecars: Vec<Option<Value>>) -> MaterializationTurn<()> {
        let mut sidecars = sidecars.into_iter();
        let mut deletes = Vec::new();
        for row in std::mem::take(&mut self.stale) {
            let target = job_key_parts(&row.index_key);
            let index = global_row(row.index_key);
            let Some((due_at_ms, document_id, event_id)) = target else {
                deletes.push(index);
                continue;
            };
            let current = (sidecars.next().flatten()).and_then(|value| {
                postcard::from_bytes::<MetadataMaterializationRecord>(&value).ok()
            });
            let changed = current != row.seen;
            // A retry that rewrote the sidecar onto this very index row keeps it.
            if !(changed && current.is_some_and(|job| job.due_at_ms == due_at_ms)) {
                deletes.push(index);
            }
            if row.drop_sidecar && !changed {
                let key = document_job_key(document_id, event_id);
                deletes.push((DOCUMENT_JOB_KEYSPACE.to_string(), key.clone()));
                deletes.push((MATERIALIZATION_CAPTURE_KEYSPACE.to_string(), key));
            }
        }
        if deletes.is_empty() {
            return Ok(Next::Done(()));
        }
        self.stage = PruneStage::Delete;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

impl Operation for PruneJobsOperation {
    type Output = ();
    type Error = MetadataMaterializationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> MaterializationTurn<()> {
        let reads: Vec<_> = (self.stale.iter())
            .filter_map(|row| job_key_parts(&row.index_key))
            .map(|(_, document_id, event_id)| sidecar_read(document_id, event_id))
            .collect();
        if reads.is_empty() {
            return self.delete(Vec::new());
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<()> {
        match self.stage {
            PruneStage::Read => self.delete(result.values()?),
            PruneStage::Delete => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// A prune's answer; one that lost its transaction leaves the rows to the next scan.
pub(super) fn pruned(result: IoResult) -> Result<(), MetadataMaterializationError> {
    match result.child::<(), MetadataMaterializationError>()? {
        Ok(()) | Err(MetadataMaterializationError::Storage(StorageError::TransactionConflict)) => {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(super) fn job_key_target(key: &[u8]) -> Option<(Ulid, Ulid)> {
    job_key_parts(key).map(|(_, document_id, event_id)| (document_id, event_id))
}

pub(super) fn job_key_parts(key: &[u8]) -> Option<(u64, Ulid, Ulid)> {
    if key.len() != 40 {
        return None;
    }
    let mut due_at_ms = [0u8; 8];
    due_at_ms.copy_from_slice(&key[..8]);
    let mut document_id = [0u8; 16];
    document_id.copy_from_slice(&key[8..24]);
    let mut event_id = [0u8; 16];
    event_id.copy_from_slice(&key[24..40]);
    Some((
        u64::from_be_bytes(due_at_ms),
        Ulid::from_bytes(document_id),
        Ulid::from_bytes(event_id),
    ))
}
