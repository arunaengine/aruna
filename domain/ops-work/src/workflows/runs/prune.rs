//! Prunes expired terminal workflow runs with their task, event, log and crate rows, and drops
//! expired entries from the workflow task cache during node upkeep.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::keys::active_key;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    AUTOMATION_RUNS_KEYSPACE, RUN_EVENT_KEYSPACE, RUN_LOG_KEYSPACE, WORKFLOW_CACHE_KEYSPACE,
    WORKFLOW_CRATE_KEYSPACE, WORKFLOW_RUN_KEYSPACE, WORKFLOW_TASK_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::identity::RETENTION_MS;
use aruna_core::structs::execution::workflow_run::{
    WorkflowCacheRecord, WorkflowRunRecord, run_key, run_prefix,
};
use aruna_core::types::{Key, KeySpace};
use ulid::Ulid;

use crate::workflows::CatalogError;

/// Rows one storage page scans or deletes.
const PAGE: usize = 256;
/// Expired workflow runs dropped in one upkeep pass at most.
const RUN_CAP: usize = 64;
/// Expired cache rows dropped in one upkeep pass at most.
const CACHE_CAP: usize = 512;

/// Counts of expired workflow rows dropped by one prune pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PruneReport {
    pub runs: usize,
    pub cache: usize,
}

#[derive(Debug, PartialEq)]
enum PruneStep {
    ScanRuns,
    CheckFiring(Ulid),
    DropRows,
    DropRun,
    ScanCache,
    DropCache(Option<Key>),
}

/// Drops expired terminal workflow runs and expired cache entries in bounded passes.
#[derive(Debug, PartialEq)]
pub struct PruneRunsOperation {
    now_ms: u64,
    retention_ms: u64,
    run_cap: usize,
    cache_cap: usize,
    pending: Vec<(Ulid, Option<Ulid>)>,
    current: Option<Ulid>,
    spaces: Vec<KeySpace>,
    report: PruneReport,
    step: PruneStep,
}

type PruneNext = Result<Next<PruneReport>, CatalogError>;

impl PruneRunsOperation {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            retention_ms: RETENTION_MS,
            run_cap: RUN_CAP,
            cache_cap: CACHE_CAP,
            pending: Vec::new(),
            current: None,
            spaces: Vec::new(),
            report: PruneReport::default(),
            step: PruneStep::ScanRuns,
        }
    }

    /// Overrides the retention window, mainly for tests.
    pub fn with_retention(mut self, retention_ms: u64) -> Self {
        self.retention_ms = retention_ms;
        self
    }

    fn expired(&self, stamped_ms: u64) -> bool {
        stamped_ms.saturating_add(self.retention_ms) <= self.now_ms
    }

    fn scan_runs(&mut self, after: Option<Key>) -> PruneNext {
        self.step = PruneStep::ScanRuns;
        Ok(Next::Call(IoCall::Page {
            space: WORKFLOW_RUN_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: PAGE,
        }))
    }

    fn scan_cache(&mut self, after: Option<Key>) -> PruneNext {
        if self.report.cache >= self.cache_cap {
            return Ok(Next::Done(self.report));
        }
        self.step = PruneStep::ScanCache;
        Ok(Next::Call(IoCall::Page {
            space: WORKFLOW_CACHE_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: PAGE,
        }))
    }

    fn start_run_drop(&mut self, run_id: Ulid) -> PruneNext {
        self.current = Some(run_id);
        self.spaces = vec![
            WORKFLOW_CRATE_KEYSPACE.to_string(),
            RUN_LOG_KEYSPACE.to_string(),
            RUN_EVENT_KEYSPACE.to_string(),
            WORKFLOW_TASK_KEYSPACE.to_string(),
        ];
        self.next_space()
    }

    fn next_run(&mut self) -> PruneNext {
        let Some((run_id, firing_id)) = self.pending.pop() else {
            return self.scan_cache(None);
        };
        if let Some(firing_id) = firing_id {
            self.step = PruneStep::CheckFiring(run_id);
            return Ok(Next::Call(IoCall::Read(vec![(
                AUTOMATION_RUNS_KEYSPACE.to_string(),
                active_key(firing_id),
            )])));
        }
        self.start_run_drop(run_id)
    }

    fn next_space(&mut self) -> PruneNext {
        let Some(run_id) = self.current else {
            return self.next_run();
        };
        let Some(space) = self.spaces.last() else {
            // Delete the parent run row last so an interrupted pass can finish dropping child rows.
            self.step = PruneStep::DropRun;
            return Ok(Next::Call(IoCall::Delete(vec![(
                WORKFLOW_RUN_KEYSPACE.to_string(),
                run_key(run_id),
            )])));
        };
        self.step = PruneStep::DropRows;
        Ok(Next::Call(IoCall::Page {
            space: space.clone(),
            prefix: run_prefix(run_id),
            after: None,
            limit: PAGE,
        }))
    }
}

impl Operation for PruneRunsOperation {
    type Output = PruneReport;
    type Error = CatalogError;

    fn first(&mut self) -> PruneNext {
        self.scan_runs(None)
    }

    fn next(&mut self, result: IoResult) -> PruneNext {
        match std::mem::replace(&mut self.step, PruneStep::ScanCache) {
            PruneStep::ScanRuns => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    if self.pending.len() >= self.run_cap {
                        break;
                    }
                    let Ok(record) = WorkflowRunRecord::from_bytes(&value) else {
                        continue;
                    };
                    let ended = record.finished_at_ms.unwrap_or(record.created_at_ms);
                    if record.state.is_terminal() && self.expired(ended) {
                        let firing = record.automation.as_ref().map(|o| o.firing_id);
                        self.pending.push((record.run_id, firing));
                    }
                }
                if self.pending.len() < self.run_cap
                    && let Some(after) = next
                {
                    return self.scan_runs(Some(after));
                }
                self.next_run()
            }
            PruneStep::CheckFiring(run_id) => {
                if result.value()?.is_some() {
                    return self.next_run();
                }
                self.start_run_drop(run_id)
            }
            PruneStep::DropRun => {
                result.done()?;
                self.current = None;
                self.report.runs += 1;
                self.next_run()
            }
            PruneStep::DropRows => match result {
                IoResult::Done => self.next_space(),
                page => {
                    let (rows, _) = page.page()?;
                    if rows.is_empty() {
                        self.spaces.pop();
                        return self.next_space();
                    }
                    let space = self.spaces.last().cloned().unwrap_or_default();
                    let deletes: Vec<_> = rows
                        .into_iter()
                        .map(|(key, _)| (space.clone(), key))
                        .collect();
                    self.step = PruneStep::DropRows;
                    Ok(Next::Call(IoCall::Delete(deletes)))
                }
            },
            PruneStep::ScanCache => {
                let (rows, next) = result.page()?;
                let room = self.cache_cap.saturating_sub(self.report.cache);
                let mut deletes = Vec::new();
                for (key, value) in rows {
                    if deletes.len() >= room {
                        break;
                    }
                    let Ok(entry) = WorkflowCacheRecord::from_bytes(&value) else {
                        continue;
                    };
                    if self.expired(entry.created_at_ms) {
                        deletes.push((WORKFLOW_CACHE_KEYSPACE.to_string(), key));
                    }
                }
                if !deletes.is_empty() {
                    self.report.cache += deletes.len();
                    self.step = PruneStep::DropCache(next);
                    return Ok(Next::Call(IoCall::Delete(deletes)));
                }
                match next {
                    Some(after) => self.scan_cache(Some(after)),
                    None => Ok(Next::Done(self.report)),
                }
            }
            PruneStep::DropCache(next) => {
                result.done()?;
                match next {
                    Some(after) => self.scan_cache(Some(after)),
                    None => Ok(Next::Done(self.report)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflows::runs::tests::sample_run;
    use aruna_core::structs::execution::workflow_run::{
        RunEventRecord, RunState, cache_row, event_key, task_key,
    };
    use aruna_runner::driver::drive;
    use aruna_runner::testing::{memory_context, read_value, scan_prefix, write_value};

    fn cache_entry(key: &str, created_at_ms: u64, run_id: Ulid) -> WorkflowCacheRecord {
        WorkflowCacheRecord {
            task_key: key.to_string(),
            scope_namespace: "group:g".to_string(),
            exit_code: 0,
            outputs: Vec::new(),
            stdout_tail: Vec::new(),
            stderr_tail: Vec::new(),
            created_at_ms,
            producer_group_id: Ulid::from_bytes([1; 16]),
            producer_run_id: run_id,
            deterministic: true,
            has_secrets: false,
        }
    }

    #[tokio::test]
    async fn prunes_expired_rows() {
        let ctx = memory_context();
        let store = &ctx.storage_handle;

        let mut old_run = sample_run();
        old_run.run_id = Ulid::from_bytes([1; 16]);
        old_run.state = RunState::Succeeded;
        old_run.finished_at_ms = Some(1_000);
        write_value(
            store,
            WORKFLOW_RUN_KEYSPACE,
            run_key(old_run.run_id),
            old_run.to_bytes().unwrap(),
        )
        .await;
        let event = RunEventRecord {
            seq: 1,
            event_type: "state".into(),
            payload_json: "{}".into(),
        };
        write_value(
            store,
            RUN_EVENT_KEYSPACE,
            event_key(old_run.run_id, 1),
            event.to_bytes().unwrap(),
        )
        .await;
        write_value(
            store,
            WORKFLOW_TASK_KEYSPACE,
            task_key(old_run.run_id, 1),
            b"task".to_vec(),
        )
        .await;

        let mut active_run = sample_run();
        active_run.run_id = Ulid::from_bytes([2; 16]);
        active_run.state = RunState::Running;
        active_run.created_at_ms = 500;
        write_value(
            store,
            WORKFLOW_RUN_KEYSPACE,
            run_key(active_run.run_id),
            active_run.to_bytes().unwrap(),
        )
        .await;

        let mut fresh_run = sample_run();
        fresh_run.run_id = Ulid::from_bytes([3; 16]);
        fresh_run.state = RunState::Failed;
        fresh_run.finished_at_ms = Some(9_500);
        write_value(
            store,
            WORKFLOW_RUN_KEYSPACE,
            run_key(fresh_run.run_id),
            fresh_run.to_bytes().unwrap(),
        )
        .await;

        let old_cache = cache_entry("old", 1_000, old_run.run_id);
        let fresh_cache = cache_entry("fresh", 9_500, fresh_run.run_id);
        write_value(
            store,
            WORKFLOW_CACHE_KEYSPACE,
            cache_row("group:g", "old"),
            old_cache.to_bytes().unwrap(),
        )
        .await;
        write_value(
            store,
            WORKFLOW_CACHE_KEYSPACE,
            cache_row("group:g", "fresh"),
            fresh_cache.to_bytes().unwrap(),
        )
        .await;

        let report = drive(PruneRunsOperation::new(10_000).with_retention(5_000), &ctx)
            .await
            .unwrap();
        assert_eq!(report, PruneReport { runs: 1, cache: 1 });
        assert!(
            read_value(
                store,
                WORKFLOW_RUN_KEYSPACE,
                run_key(old_run.run_id).to_vec()
            )
            .await
            .is_none()
        );
        assert!(
            scan_prefix(store, RUN_EVENT_KEYSPACE, &run_prefix(old_run.run_id))
                .await
                .is_empty()
        );
        assert!(
            scan_prefix(store, WORKFLOW_TASK_KEYSPACE, &run_prefix(old_run.run_id))
                .await
                .is_empty()
        );
        assert!(
            read_value(
                store,
                WORKFLOW_RUN_KEYSPACE,
                run_key(active_run.run_id).to_vec()
            )
            .await
            .is_some()
        );
        assert!(
            read_value(
                store,
                WORKFLOW_RUN_KEYSPACE,
                run_key(fresh_run.run_id).to_vec()
            )
            .await
            .is_some()
        );
        assert!(
            read_value(
                store,
                WORKFLOW_CACHE_KEYSPACE,
                cache_row("group:g", "old").to_vec()
            )
            .await
            .is_none()
        );
        assert!(
            read_value(
                store,
                WORKFLOW_CACHE_KEYSPACE,
                cache_row("group:g", "fresh").to_vec()
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn drops_run_last() {
        use aruna_core::structs::execution::workflow_run::AutomationOrigin;

        // Child keyspaces are scanned/deleted before WORKFLOW_RUN_KEYSPACE is deleted.
        let run_id = Ulid::from_bytes([40; 16]);
        let mut run = sample_run();
        run.run_id = run_id;
        run.state = RunState::Succeeded;
        run.finished_at_ms = Some(100);
        let mut op = PruneRunsOperation::new(10_000).with_retention(1_000);
        op.first().unwrap();
        let next = op
            .next(IoResult::Page {
                rows: vec![(run_key(run_id), run.to_bytes().unwrap().into())],
                next: None,
            })
            .unwrap();
        let mut deleted_spaces = Vec::new();
        let mut task_sent = false;
        let mut step = next;
        for _ in 0..16 {
            match step {
                Next::Call(IoCall::Page { space, .. }) if space == WORKFLOW_CACHE_KEYSPACE => {
                    break;
                }
                Next::Call(IoCall::Page { space, .. }) => {
                    if space == WORKFLOW_TASK_KEYSPACE && !task_sent {
                        task_sent = true;
                        step = op
                            .next(IoResult::Page {
                                rows: vec![(task_key(run_id, 1), b"t".to_vec().into())],
                                next: None,
                            })
                            .unwrap();
                    } else {
                        step = op
                            .next(IoResult::Page {
                                rows: Vec::new(),
                                next: None,
                            })
                            .unwrap();
                    }
                }
                Next::Call(IoCall::Delete(keys)) => {
                    for (space, _) in &keys {
                        deleted_spaces.push(space.clone());
                    }
                    step = op.next(IoResult::Done).unwrap();
                }
                other => panic!("unexpected step: {other:?}"),
            }
        }
        assert_eq!(
            deleted_spaces,
            vec![
                WORKFLOW_TASK_KEYSPACE.to_string(),
                WORKFLOW_RUN_KEYSPACE.to_string()
            ]
        );

        // An automation child run is retained while its firing's active row still exists.
        let ctx = memory_context();
        let store = &ctx.storage_handle;
        let firing_id = Ulid::from_bytes([41; 16]);
        let auto_id = Ulid::from_bytes([42; 16]);
        run.automation = Some(AutomationOrigin {
            automation_id: auto_id,
            firing_id,
            stage: "run".into(),
            applet_id: None,
            outside_input: false,
            depth: 0,
        });
        write_value(
            store,
            WORKFLOW_RUN_KEYSPACE,
            run_key(run_id),
            run.to_bytes().unwrap(),
        )
        .await;
        write_value(
            store,
            AUTOMATION_RUNS_KEYSPACE,
            active_key(firing_id),
            auto_id.to_bytes().to_vec(),
        )
        .await;
        let kept = drive(PruneRunsOperation::new(10_000).with_retention(1_000), &ctx)
            .await
            .unwrap();
        assert_eq!(kept.runs, 0);
        assert!(
            read_value(store, WORKFLOW_RUN_KEYSPACE, run_key(run_id).to_vec())
                .await
                .is_some()
        );
    }
}
