//! Reads the due graph prune jobs and reschedules failed ones, one job per graph under its key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::PRUNE_JOB_KEYSPACE;
use aruna_core::metadata::GraphPruneRecord;
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, done};
use aruna_core::storage_entries::{graph_prune_entry, graph_prune_key};
use aruna_core::types::{Key, KeySpace, Value};

use crate::queue_backoff::{min_due_at, retry_delay_ms};

const PRUNE_PAGE: usize = 512;

/// The due jobs in due order with their keys, whether more may be due, and the next due time.
#[derive(Debug, PartialEq)]
pub(crate) struct DuePrune {
    pub jobs: Vec<(Vec<u8>, GraphPruneRecord)>,
    pub has_more_due: bool,
    pub next_due_ms: Option<u64>,
}

#[derive(Debug, PartialEq)]
enum Step {
    Scan,
    Write,
    Delete,
}

/// Reads every prune job, keeps the preferred job of each graph under its canonical key and
/// deletes malformed rows and the other jobs of a graph, then answers the jobs due at `now_ms`.
/// At `limit` due jobs it stops and reports that more may be due.
#[derive(Debug, PartialEq)]
pub struct DuePruneOperation {
    now_ms: u64,
    limit: usize,
    pages: PrefixPages,
    rows: Vec<(Vec<u8>, GraphPruneRecord)>,
    malformed: Vec<Vec<u8>>,
    deletes: Vec<(KeySpace, Key)>,
    step: Step,
    due: Option<DuePrune>,
}

type DueTurn = Result<Next<DuePrune>, IoError>;

impl DuePruneOperation {
    pub(crate) fn new(now_ms: u64, limit: usize) -> Self {
        Self {
            now_ms,
            limit,
            pages: PrefixPages::new(PRUNE_JOB_KEYSPACE, Key::from(Vec::new()), PRUNE_PAGE),
            rows: Vec::new(),
            malformed: Vec::new(),
            deletes: Vec::new(),
            step: Step::Scan,
            due: None,
        }
    }

    fn scanned(&mut self, result: IoResult) -> DueTurn {
        for (key, value) in self.pages.take(result)? {
            match postcard::from_bytes::<GraphPruneRecord>(&value) {
                Ok(job) => self.rows.push((key.to_vec(), job)),
                Err(_) => self.malformed.push(key.to_vec()),
            }
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        let (writes, due) = self.plan()?;
        self.due = Some(due);
        self.step = Step::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// The repair writes and the due jobs; the deletes wait for the writes.
    fn plan(&mut self) -> Result<(Vec<(KeySpace, Key, Value)>, DuePrune), IoError> {
        let rows = std::mem::take(&mut self.rows);
        let mut preferred: BTreeMap<&str, usize> = BTreeMap::new();
        for (index, (_, job)) in rows.iter().enumerate() {
            let current = preferred.entry(job.graph_iri.as_str()).or_insert(index);
            if preferred_job(job, &rows[*current].1) {
                *current = index;
            }
        }
        let kept: BTreeSet<usize> = preferred.into_values().collect();
        let mut deletes: Vec<Vec<u8>> = std::mem::take(&mut self.malformed);
        let mut writes = Vec::new();
        let mut jobs = Vec::new();
        for (index, (key, job)) in rows.iter().enumerate() {
            let canonical = graph_prune_key(job).to_vec();
            if !kept.contains(&index) {
                deletes.push(key.clone());
                continue;
            }
            if *key != canonical {
                let entry = graph_prune_entry(job).map_err(|e| IoError::Codec(e.to_string()))?;
                writes.push(entry);
                deletes.push(key.clone());
            }
            jobs.push((canonical, job.clone()));
        }
        jobs.sort_by(|left, right| left.0.cmp(&right.0));
        self.deletes = deletes
            .into_iter()
            .map(|key| (PRUNE_JOB_KEYSPACE.to_string(), Key::from(key)))
            .collect();
        Ok((writes, self.due_jobs(jobs)))
    }

    fn due_jobs(&self, jobs: Vec<(Vec<u8>, GraphPruneRecord)>) -> DuePrune {
        let mut due = Vec::new();
        let mut next_due_ms = None;
        for (key, job) in jobs {
            if job.due_at_ms > self.now_ms {
                next_due_ms = min_due_at(next_due_ms, job.due_at_ms);
                continue;
            }
            due.push((key, job));
            if due.len() >= self.limit {
                return DuePrune {
                    jobs: due,
                    has_more_due: true,
                    next_due_ms,
                };
            }
        }
        DuePrune {
            jobs: due,
            has_more_due: false,
            next_due_ms,
        }
    }
}

/// Whether `candidate` replaces `current` as the job of its graph: more attempts, then later.
fn preferred_job(candidate: &GraphPruneRecord, current: &GraphPruneRecord) -> bool {
    (candidate.attempts, candidate.due_at_ms) > (current.attempts, current.due_at_ms)
}

impl Operation for DuePruneOperation {
    type Output = DuePrune;
    type Error = IoError;

    fn first(&mut self) -> DueTurn {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> DueTurn {
        match self.step {
            Step::Scan => self.scanned(result),
            Step::Write => {
                done(result)?;
                self.step = Step::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            Step::Delete => {
                done(result)?;
                let unfinished = IoError::Unfinished;
                Ok(Next::Done(self.due.take().ok_or(unfinished)?))
            }
        }
    }
}

/// Moves a failed job to its next attempt in one transaction: writes the job due after the
/// retry delay and deletes its old rows.
#[derive(Debug, PartialEq)]
pub(crate) struct RescheduleOperation {
    write: (KeySpace, Key, Value),
    deletes: Vec<(KeySpace, Key)>,
    written: bool,
}

impl RescheduleOperation {
    pub(crate) fn new(
        old_keys: &[Vec<u8>],
        job: &GraphPruneRecord,
        error: String,
        now_ms: u64,
    ) -> Result<Self, IoError> {
        let attempts = job.attempts.saturating_add(1);
        let next_job = GraphPruneRecord {
            graph_iri: job.graph_iri.clone(),
            due_at_ms: now_ms.saturating_add(retry_delay_ms(attempts)),
            attempts,
            last_error: Some(error),
        };
        let write = graph_prune_entry(&next_job).map_err(|e| IoError::Codec(e.to_string()))?;
        let deletes = old_keys
            .iter()
            .filter(|key| key.as_slice() != write.1.as_ref())
            .map(|key| (PRUNE_JOB_KEYSPACE.to_string(), Key::from(key.clone())))
            .collect();
        Ok(Self {
            write,
            deletes,
            written: false,
        })
    }
}

impl Operation for RescheduleOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> aruna_core::operation::TransactionMode {
        aruna_core::operation::TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Write(vec![self.write.clone()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        done(result)?;
        if self.written {
            return Ok(Next::Done(()));
        }
        self.written = true;
        Ok(Next::Call(IoCall::Delete(std::mem::take(
            &mut self.deletes,
        ))))
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;

    const NOW_MS: u64 = 1_000_000;

    fn job(graph_iri: &str, due_at_ms: u64, attempts: u32) -> GraphPruneRecord {
        GraphPruneRecord {
            graph_iri: graph_iri.to_string(),
            due_at_ms,
            attempts,
            last_error: None,
        }
    }

    fn canonical(job: &GraphPruneRecord) -> (Key, Value) {
        let (_, key, value) = graph_prune_entry(job).unwrap();
        (key, value)
    }

    fn misplaced(key: u8, job: &GraphPruneRecord) -> (Key, Value) {
        (vec![key].into(), postcard::to_allocvec(job).unwrap().into())
    }

    fn row_key(key: &[u8]) -> (KeySpace, Key) {
        (PRUNE_JOB_KEYSPACE.to_string(), Key::from(key.to_vec()))
    }

    /// Scans `rows` as one page and returns the repair writes, deletes and the outcome.
    fn scan(
        rows: Vec<(Key, Value)>,
        limit: usize,
    ) -> (Vec<(KeySpace, Key, Value)>, Vec<(KeySpace, Key)>, DuePrune) {
        let mut operation = DuePruneOperation::new(NOW_MS, limit);
        operation.first().unwrap();
        let page = IoResult::Page { rows, next: None };
        let Ok(Next::Call(IoCall::Write(writes))) = operation.next(page) else {
            panic!("expected the repair writes");
        };
        let Ok(Next::Call(IoCall::Delete(deletes))) = operation.next(IoResult::Done) else {
            panic!("expected the deletes");
        };
        let Ok(Next::Done(due)) = operation.next(IoResult::Done) else {
            panic!("expected the due jobs");
        };
        (writes, deletes, due)
    }

    #[test]
    fn pages_whole_queue() {
        let mut operation = DuePruneOperation::new(NOW_MS, 8);
        let first = PrefixPages::new(PRUNE_JOB_KEYSPACE, Key::from(Vec::new()), PRUNE_PAGE);
        assert_eq!(operation.first(), Ok(Next::Call(first.call())));
        let more = IoResult::Page {
            rows: vec![canonical(&job("urn:a", 1, 0))],
            next: Some(Key::from(vec![9])),
        };
        let Ok(Next::Call(IoCall::Page { after, .. })) = operation.next(more) else {
            panic!("expected the next page");
        };
        assert_eq!(after, Some(Key::from(vec![9])));
    }

    #[test]
    fn repairs_future_misplaced() {
        // A misplaced future job moves to its key; the due job is answered.
        let future = job("urn:future", NOW_MS + 60_000, 0);
        let due = job("urn:due", 1, 0);
        let (writes, deletes, outcome) = scan(vec![misplaced(0, &future), canonical(&due)], 8);
        assert_eq!(writes, vec![graph_prune_entry(&future).unwrap()]);
        assert_eq!(deletes, vec![row_key(&[0])]);
        let due_key = graph_prune_key(&due).to_vec();
        assert_eq!(outcome.jobs, vec![(due_key, due)]);
        assert!(!outcome.has_more_due);
        assert_eq!(outcome.next_due_ms, Some(future.due_at_ms));
    }

    #[test]
    fn due_misplaced_answered() {
        let future = job("urn:future", NOW_MS + 60_000, 0);
        let due = job("urn:due", 1, 0);
        let (writes, deletes, outcome) = scan(vec![canonical(&future), misplaced(255, &due)], 8);
        assert_eq!(writes, vec![graph_prune_entry(&due).unwrap()]);
        assert_eq!(deletes, vec![row_key(&[255])]);
        assert_eq!(outcome.jobs, vec![(graph_prune_key(&due).to_vec(), due)]);
    }

    #[test]
    fn retried_job_wins() {
        // Of two jobs for one graph the one with more attempts stays, misplaced or not.
        let retried = job("urn:graph", NOW_MS + 60_000, 1);
        let stale = job("urn:graph", 1, 0);
        for stale_row in [misplaced(0, &stale), canonical(&stale)] {
            let stale_key = stale_row.0.to_vec();
            let (writes, deletes, outcome) = scan(vec![stale_row, canonical(&retried)], 8);
            assert!(writes.is_empty());
            assert_eq!(deletes, vec![row_key(&stale_key)]);
            assert!(outcome.jobs.is_empty() && !outcome.has_more_due);
            assert_eq!(outcome.next_due_ms, Some(retried.due_at_ms));
        }
    }

    #[test]
    fn malformed_rows_deleted() {
        let (writes, deletes, outcome) = scan(vec![(vec![0].into(), vec![1, 2, 3].into())], 8);
        assert!(writes.is_empty());
        assert_eq!(deletes, vec![row_key(&[0])]);
        assert_eq!(
            outcome,
            DuePrune {
                jobs: Vec::new(),
                has_more_due: false,
                next_due_ms: None,
            }
        );
    }

    #[test]
    fn limit_reports_more() {
        let rows = vec![
            canonical(&job("urn:a", 1, 0)),
            canonical(&job("urn:b", 2, 0)),
        ];
        let (_, _, outcome) = scan(rows, 1);
        assert_eq!(outcome.jobs.len(), 1);
        assert_eq!(outcome.jobs[0].1.graph_iri, "urn:a");
        assert!(outcome.has_more_due);
    }

    #[test]
    fn reschedule_moves_job() {
        let failed = job("urn:graph", 1, 0);
        let old_key = graph_prune_key(&failed).to_vec();
        let mut operation =
            RescheduleOperation::new(&[old_key.clone()], &failed, "down".into(), NOW_MS).unwrap();
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let next_job = GraphPruneRecord {
            due_at_ms: NOW_MS + retry_delay_ms(1),
            attempts: 1,
            last_error: Some("down".to_string()),
            ..failed
        };
        let write = graph_prune_entry(&next_job).unwrap();
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Write(vec![write])))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(vec![row_key(&old_key)])))
        );
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }
}
