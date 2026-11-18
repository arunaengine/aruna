//! Reads import schedules and starts their due runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::cron::next_due;
use super::remove::RemoveImportOperation;
use super::{ImportRuleError, ListImportsOperation};
use crate::jobs::records::pages::ServingNode;
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::jobs::submit_local::SubmitLocalOperation;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::IMPORT_REMOVAL_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::backend_import::{BackendImport, BackendImportSpec};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Key;
use std::collections::VecDeque;
use std::time::{Duration, UNIX_EPOCH};
use tracing::warn;
use ulid::Ulid;

/// How long a due rule waits while its last run is still going.
const BUSY_RETRY_MS: u64 = 60_000;

/// Interrupted removals finished per schedule run; the rest wait for the next one.
const REMOVAL_PAGE: usize = 64;

/// Who starts a run and how the job is kept.
#[derive(Clone, Debug, PartialEq)]
pub struct ImportRunInput {
    pub auth_context: AuthContext,
    pub node_id: NodeId,
    pub dedup: Option<String>,
    pub retention_ms: u64,
    pub serving: ServingNode,
    pub now_ms: u64,
}

/// Starts one run unless the last one is still going. The job and the rule's record of it commit
/// in one transaction. Outputs `None` while a run is going.
#[derive(Debug, PartialEq)]
pub struct StartRunOperation {
    group_id: aruna_core::types::GroupId,
    import_id: Ulid,
    input: Option<ImportRunInput>,
}

impl StartRunOperation {
    pub fn new(rule: &BackendImport, input: ImportRunInput) -> Self {
        Self {
            group_id: rule.group_id,
            import_id: rule.import_id,
            input: Some(input),
        }
    }
}

impl Operation for StartRunOperation {
    type Output = Option<JobId>;
    type Error = ImportRuleError;

    fn first(&mut self) -> Result<Next<Option<JobId>>, ImportRuleError> {
        let input = self.input.take();
        let input = input.ok_or(ImportRuleError::Unexpected("StartRun"))?;
        let spec = BackendImportSpec {
            auth_context: input.auth_context,
            node_id: input.node_id,
            group_id: self.group_id,
            import_id: self.import_id,
        };
        let submit = SubmitLocalOperation::backend_import(
            spec,
            input.node_id,
            (input.dedup, input.retention_ms),
            input.serving,
            input.now_ms,
        );
        Ok(Next::Call(IoCall::child(submit)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<JobId>>, ImportRuleError> {
        match result.child::<SubmitJobResult, SubmitJobError>()? {
            Ok(submitted) => Ok(Next::Done(Some(submitted.job_id))),
            Err(SubmitJobError::ImportRunning) => Ok(Next::Done(None)),
            Err(SubmitJobError::ImportMissing) => Err(ImportRuleError::NotFound),
            Err(error) => Err(error.into()),
        }
    }
}

/// One page of interrupted removals, and whether more follow.
#[derive(Debug, PartialEq)]
struct RemovalMarks;

impl Operation for RemovalMarks {
    type Output = (Vec<Key>, bool);
    type Error = IoError;

    fn first(&mut self) -> Result<Next<(Vec<Key>, bool)>, IoError> {
        Ok(Next::Call(IoCall::Page {
            space: IMPORT_REMOVAL_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: REMOVAL_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<(Vec<Key>, bool)>, IoError> {
        let (rows, next) = result.page()?;
        let marks = rows.into_iter().map(|(key, _)| key).collect();
        Ok(Next::Done((marks, next.is_none())))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum DueStep {
    Marks,
    Removal(Ulid),
    Rules,
    Start,
}

type DueTurn = Result<Next<Option<Duration>>, IoError>;

/// Starts every scheduled rule that is due, as its creator without a session, after finishing
/// interrupted removals. Outputs when to look again, or `None` while no rule has a schedule and
/// no removal is left. Missed times run once.
#[derive(Debug, PartialEq)]
pub struct RunDueOperation {
    node_id: NodeId,
    retention_ms: u64,
    now_ms: u64,
    serving: ServingNode,
    step: DueStep,
    marks: VecDeque<Key>,
    removed: bool,
    rules: VecDeque<BackendImport>,
    /// The schedule of the rule being started.
    schedule: String,
    next: Option<u64>,
}

impl RunDueOperation {
    pub fn new(node_id: NodeId, retention_ms: u64, now_ms: u64, serving: ServingNode) -> Self {
        Self {
            node_id,
            retention_ms,
            now_ms,
            serving,
            step: DueStep::Marks,
            marks: VecDeque::new(),
            removed: true,
            rules: VecDeque::new(),
            schedule: String::new(),
            next: None,
        }
    }

    fn call(&mut self, step: DueStep, call: IoCall) -> DueTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn retry_at(&self) -> u64 {
        self.now_ms.saturating_add(BUSY_RETRY_MS)
    }

    fn marks_read(&mut self, marks: Result<(Vec<Key>, bool), IoError>) -> DueTurn {
        match marks {
            Ok((marks, last)) => {
                self.marks = marks.into();
                self.removed = last;
            }
            Err(error) => {
                warn!(error = %error, "Backend import removals could not be read");
                self.removed = false;
            }
        }
        self.next_removal()
    }

    /// Finishes removals that stopped after their rule was deleted.
    fn next_removal(&mut self) -> DueTurn {
        while let Some(key) = self.marks.pop_front() {
            let Ok(bytes) = <[u8; 16]>::try_from(key.as_ref()) else {
                warn!("Backend import removal mark has an invalid key");
                continue;
            };
            let import_id = Ulid::from_bytes(bytes);
            let resume = RemoveImportOperation::resume(import_id);
            return self.call(DueStep::Removal(import_id), IoCall::child(resume));
        }
        self.call(DueStep::Rules, IoCall::child(ListImportsOperation::all()))
    }

    fn removal_ended(&mut self, import_id: Ulid, ended: Result<(), ImportRuleError>) -> DueTurn {
        if let Err(error) = ended {
            warn!(import_id = %import_id, error = %error, "Backend import removal did not finish");
            self.removed = false;
        }
        self.next_removal()
    }

    fn rules_read(&mut self, rules: Result<Vec<BackendImport>, ImportRuleError>) -> DueTurn {
        let rules = match rules {
            Ok(rules) => rules,
            Err(error) => {
                warn!(error = %error, "Backend import schedule could not read its rules");
                return Ok(Next::Done(Some(Duration::from_millis(BUSY_RETRY_MS))));
            }
        };
        self.rules = rules.into();
        self.next = (!self.removed).then(|| self.retry_at());
        self.next_rule()
    }

    fn note_due(&mut self, due: u64) {
        self.next = Some(self.next.map_or(due, |next| next.min(due)));
    }

    fn next_rule(&mut self) -> DueTurn {
        while let Some(rule) = self.rules.pop_front() {
            let Some(schedule) = rule.schedule.clone() else {
                continue;
            };
            let since = rule.last_started_ms.unwrap_or_else(|| {
                rule.created_at
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as u64)
            });
            let Some(due) = next_due(&schedule, since) else {
                continue;
            };
            if due > self.now_ms {
                self.note_due(due);
                continue;
            }
            let realm_id = self
                .serving
                .map_or(rule.created_by.realm_id, |(_, realm)| realm);
            let start = StartRunOperation::new(
                &rule,
                ImportRunInput {
                    auth_context: AuthContext {
                        user_id: rule.created_by,
                        realm_id,
                        path_restrictions: None,
                        session: None,
                    },
                    node_id: self.node_id,
                    dedup: Some(format!("backend-import/{}/{due}", rule.import_id)),
                    retention_ms: self.retention_ms,
                    serving: self.serving,
                    now_ms: self.now_ms,
                },
            );
            self.schedule = schedule;
            return self.call(DueStep::Start, IoCall::child(start));
        }
        let now_ms = self.now_ms;
        let after = (self.next).map(|at| Duration::from_millis(at.saturating_sub(now_ms)));
        Ok(Next::Done(after))
    }

    fn run_started(&mut self, started: Result<Option<JobId>, ImportRuleError>) -> DueTurn {
        let due = match started {
            Ok(Some(_)) => next_due(&self.schedule, self.now_ms).unwrap_or(u64::MAX),
            Ok(None) => self.retry_at(),
            Err(error) => {
                warn!(error = %error, "Scheduled backend import did not start");
                self.retry_at()
            }
        };
        self.note_due(due);
        self.next_rule()
    }
}

impl Operation for RunDueOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> DueTurn {
        self.call(DueStep::Marks, IoCall::child(RemovalMarks))
    }

    fn next(&mut self, result: IoResult) -> DueTurn {
        match self.step {
            DueStep::Marks => self.marks_read(result.child::<(Vec<Key>, bool), IoError>()?),
            DueStep::Removal(import_id) => {
                self.removal_ended(import_id, result.child::<(), ImportRuleError>()?)
            }
            DueStep::Rules => {
                self.rules_read(result.child::<Vec<BackendImport>, ImportRuleError>()?)
            }
            DueStep::Start => self.run_started(result.child::<Option<JobId>, ImportRuleError>()?),
        }
    }
}

/// Makes the schedule look again now, since a changed rule may be due sooner. The task adapter
/// persists the timer first; a failure only warns.
#[derive(Debug, PartialEq)]
pub struct WakeScheduleOperation;

impl Operation for WakeScheduleOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let wake = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::RunBackendImports,
            after: Duration::ZERO,
        });
        Ok(Next::Call(wake.into()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match result.event()? {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {}
            other => warn!(
                event = other.label(),
                "Failed to wake the backend import schedule"
            ),
        }
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::imports::cron::parse_schedule;

    const HOUR_MS: u64 = 3_600_000;

    #[test]
    fn refuses_bad_patterns() {
        assert!(parse_schedule("0 3 * * *").is_ok());
        for pattern in ["", "every day", "0 0 3 * * *", "61 * * * *"] {
            assert!(parse_schedule(pattern).is_err(), "{pattern}");
        }
    }

    #[test]
    fn computes_next_due() {
        // 1970-01-01 00:30 UTC: the next 03:00 is the same day, then the day after.
        let after = HOUR_MS / 2;
        assert_eq!(next_due("0 3 * * *", after), Some(3 * HOUR_MS));
        assert_eq!(next_due("0 3 * * *", 3 * HOUR_MS), Some(27 * HOUR_MS));
        assert_eq!(next_due("*/15 * * * *", after), Some(after + HOUR_MS / 4));
        assert_eq!(next_due("not cron", after), None);
    }

    use crate::s3::bucket::rename::tests::rule;
    use aruna_core::calls::ChildOutcome;

    const NOW: u64 = 10 * HOUR_MS;

    fn due() -> RunDueOperation {
        let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let mut operation = RunDueOperation::new(node, 1, NOW, None);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn no_marks() -> IoResult {
        child(Ok::<_, IoError>((Vec::<Key>::new(), true)))
    }

    fn scheduled(schedule: &str) -> BackendImport {
        let mut rule = rule();
        rule.schedule = Some(schedule.to_string());
        rule.created_at = UNIX_EPOCH;
        rule.last_started_ms = Some(NOW - 1);
        rule
    }

    #[test]
    fn idle_schedule_sleeps() {
        let mut operation = due();
        operation.next(no_marks()).unwrap();
        let rules = child(Ok::<_, ImportRuleError>(vec![rule()]));
        assert_eq!(operation.next(rules), Ok(Next::Done(None)));
    }

    #[test]
    fn unread_marks_retry() {
        let mut operation = due();
        let failed = child(Err::<(Vec<Key>, bool), _>(IoError::Conflict));
        operation.next(failed).unwrap();
        let rules = child(Ok::<_, ImportRuleError>(Vec::<BackendImport>::new()));
        let retry = Duration::from_millis(BUSY_RETRY_MS);
        assert_eq!(operation.next(rules), Ok(Next::Done(Some(retry))));
    }

    #[test]
    fn future_rule_waits() {
        let mut operation = due();
        operation.next(no_marks()).unwrap();
        let rules = vec![scheduled("0 3 * * *")];
        let turn = operation.next(child(Ok::<_, ImportRuleError>(rules)));
        let at = next_due("0 3 * * *", NOW - 1).unwrap();
        let wait = Duration::from_millis(at - NOW);
        assert_eq!(turn, Ok(Next::Done(Some(wait))));
    }

    #[test]
    fn busy_rule_retries() {
        let mut operation = due();
        operation.next(no_marks()).unwrap();
        let mut rule = scheduled("* * * * *");
        rule.last_started_ms = Some(0);
        let turn = operation.next(child(Ok::<_, ImportRuleError>(vec![rule])));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let running = child(Ok::<Option<JobId>, ImportRuleError>(None));
        let retry = Duration::from_millis(BUSY_RETRY_MS);
        assert_eq!(operation.next(running), Ok(Next::Done(Some(retry))));
    }

    #[test]
    fn wake_only_warns() {
        let mut wake = WakeScheduleOperation;
        assert!(matches!(wake.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let answer = Event::Storage(aruna_core::events::StorageEvent::SyncAllFinished);
        assert_eq!(
            wake.next(IoResult::Event(Box::new(answer))),
            Ok(Next::Done(()))
        );
    }
}
