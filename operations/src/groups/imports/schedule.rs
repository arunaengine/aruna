//! Reads import schedules, five-field cron expressions in UTC, and starts due runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::remove::RemoveImportOperation;
use super::{ImportRuleError, all_imports};
use crate::driver::{DriverContext, drive};
use crate::jobs::service::submit_import_job;
use crate::jobs::store::iter_prefix_page;
use crate::jobs::submit::SubmitJobError;
use crate::tasks::task_persistence::persist_task_effect;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::IMPORT_REMOVAL_KEYSPACE;
use aruna_core::structs::execution::job::{JobId, JobState};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::backend_import::{BackendImport, BackendImportSpec};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use chrono::{DateTime, Utc};
use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};
use std::time::{Duration, UNIX_EPOCH};
use tracing::warn;
use ulid::Ulid;

/// How long a due rule waits while its last run is still going.
const BUSY_RETRY_MS: u64 = 60_000;

/// Interrupted removals finished per schedule run; the rest wait for the next one.
const REMOVAL_PAGE: usize = 64;

/// Seconds and years are refused, so no schedule fires more than once a minute.
pub fn parse_schedule(pattern: &str) -> Result<Cron, ImportRuleError> {
    CronParser::builder()
        .seconds(Seconds::Disallowed)
        .year(Year::Disallowed)
        .build()
        .parse(pattern)
        .map_err(|error| ImportRuleError::Invalid(format!("schedule is not valid cron: {error}")))
}

/// The first time strictly after `after_ms` that `schedule` fires, in Unix milliseconds.
pub fn next_due(schedule: &str, after_ms: u64) -> Option<u64> {
    let cron = parse_schedule(schedule).ok()?;
    let after = DateTime::<Utc>::from_timestamp_millis(i64::try_from(after_ms).ok()?)?;
    let next = cron.find_next_occurrence(&after, false).ok()?;
    u64::try_from(next.timestamp_millis()).ok()
}

/// A run counts as going until it ends; an undecided run does not hold the schedule.
pub fn is_running(state: JobState) -> bool {
    !state.is_terminal() && state != JobState::Indeterminate
}

/// Starts one run unless the last one is still going. The job and the rule's record of
/// it commit in one transaction. Answers `None` while a run is going.
pub async fn start_run(
    context: &DriverContext,
    rule: &BackendImport,
    auth_context: AuthContext,
    node_id: NodeId,
    dedup: Option<String>,
    retention_ms: u64,
) -> Result<Option<JobId>, ImportRuleError> {
    let spec = BackendImportSpec {
        auth_context,
        node_id,
        group_id: rule.group_id,
        import_id: rule.import_id,
    };
    match submit_import_job(context, spec, node_id, dedup, retention_ms).await {
        Ok(submitted) => Ok(Some(submitted.job_id)),
        Err(SubmitJobError::ImportRunning) => Ok(None),
        Err(SubmitJobError::ImportMissing) => Err(ImportRuleError::NotFound),
        Err(error) => Err(error.into()),
    }
}

/// Finishes removals that stopped after their rule was deleted; `false` while one is left.
async fn finish_removals(context: &DriverContext) -> bool {
    let page = iter_prefix_page(
        &context.storage_handle,
        IMPORT_REMOVAL_KEYSPACE,
        None,
        None,
        REMOVAL_PAGE,
        None,
    )
    .await;
    let (marks, rest) = match page {
        Ok(page) => page,
        Err(error) => {
            warn!(error = %error, "Backend import removals could not be read");
            return false;
        }
    };
    let mut done = rest.is_none();
    for (key, _) in marks {
        let Ok(bytes) = <[u8; 16]>::try_from(key.as_ref()) else {
            warn!("Backend import removal mark has an invalid key");
            continue;
        };
        let import_id = Ulid::from_bytes(bytes);
        if let Err(error) = drive(RemoveImportOperation::resume(import_id), context).await {
            warn!(import_id = %import_id, error = %error, "Backend import removal did not finish");
            done = false;
        }
    }
    done
}

/// Starts every scheduled rule that is due, as its creator without a session, after
/// finishing interrupted removals. Answers when to look again, or `None` while no rule
/// has a schedule and no removal is left. Missed times run once.
pub async fn run_due(
    context: &DriverContext,
    node_id: NodeId,
    retention_ms: u64,
    now_ms: u64,
) -> Option<Duration> {
    let removed = finish_removals(context).await;
    let rules = match all_imports(context).await {
        Ok(rules) => rules,
        Err(error) => {
            warn!(error = %error, "Backend import schedule could not read its rules");
            return Some(Duration::from_millis(BUSY_RETRY_MS));
        }
    };
    let mut next = (!removed).then(|| now_ms.saturating_add(BUSY_RETRY_MS));
    for rule in rules {
        let Some(schedule) = rule.schedule.as_deref() else {
            continue;
        };
        let since = rule.last_started_ms.unwrap_or_else(|| {
            rule.created_at
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_millis() as u64)
        });
        let Some(mut due) = next_due(schedule, since) else {
            continue;
        };
        if due <= now_ms {
            let auth = AuthContext {
                user_id: rule.created_by,
                realm_id: context.serving_realm(rule.created_by.realm_id),
                path_restrictions: None,
                session: None,
            };
            let dedup = format!("backend-import/{}/{due}", rule.import_id);
            due = match start_run(context, &rule, auth, node_id, Some(dedup), retention_ms).await {
                Ok(Some(_)) => next_due(schedule, now_ms).unwrap_or(u64::MAX),
                Ok(None) => now_ms.saturating_add(BUSY_RETRY_MS),
                Err(error) => {
                    warn!(import_id = %rule.import_id, error = %error, "Scheduled backend import did not start");
                    now_ms.saturating_add(BUSY_RETRY_MS)
                }
            };
        }
        next = Some(next.map_or(due, |next| next.min(due)));
    }
    next.map(|at| Duration::from_millis(at.saturating_sub(now_ms)))
}

/// Makes the schedule look again now, since a changed rule may be due sooner.
pub async fn wake_schedule(context: &DriverContext) {
    let effect = TaskEffect::ShortenTimer {
        key: TaskKey::RunBackendImports,
        after: Duration::ZERO,
    };
    if let Err(message) = persist_task_effect(&context.storage_handle, &effect).await {
        warn!(message = %message, "Failed to persist the backend import timer");
        return;
    }
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    match task_handle.send_effect(Effect::Task(effect)).await {
        Event::Task(TaskEvent::TimerScheduled { .. }) => {}
        other => warn!(event = ?other, "Failed to wake the backend import schedule"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
