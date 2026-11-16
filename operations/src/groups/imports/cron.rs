//! Parses five-field cron import schedules in UTC and finds their next due time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::JobState;
use chrono::{DateTime, Utc};
use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};

use super::ImportRuleError;

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
