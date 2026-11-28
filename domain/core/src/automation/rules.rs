//! Automation start conditions evaluated purely against bundled IANA time zones and file payloads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use super::firing::TriggerPayload;
use super::glob::glob_match;
use super::schedule::{ClockTime, Weekday, days_text, parse_zone};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionMode {
    #[default]
    All,
    Any,
}

impl ConditionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Any => "any",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "all" => Some(Self::All),
            "any" => Some(Self::Any),
            _ => None,
        }
    }
}

/// One guard condition evaluated after a trigger matches and before rate/concurrency limits.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Condition {
    TimeOfDay {
        from: ClockTime,
        to: ClockTime,
        zone: String,
    },
    Weekdays {
        days: Vec<Weekday>,
        zone: String,
    },
    File {
        pattern: Option<String>,
        min_bytes: Option<u64>,
        max_bytes: Option<u64>,
    },
}

/// All conditions attached to an automation spec, combined by `mode`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conditions {
    pub mode: ConditionMode,
    pub items: Vec<Condition>,
}

impl Conditions {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Kind of condition or limit recorded on a firing's `conditions_checked` list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConditionCheckKind {
    TimeOfDay,
    Weekdays,
    File,
    Starts,
    ActiveLimit,
}

impl ConditionCheckKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TimeOfDay => "time_of_day",
            Self::Weekdays => "weekdays",
            Self::File => "file",
            Self::Starts => "starts",
            Self::ActiveLimit => "one_at_a_time",
        }
    }
}

/// Recorded outcome of evaluating one condition or limit for a firing or skipped start.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionCheck {
    pub kind: ConditionCheckKind,
    pub text: String,
    pub held: bool,
    pub why: String,
}

/// Aggregated result of evaluating `Conditions` at a point in time for a trigger payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConditionEvaluation {
    pub ok: bool,
    pub reason: Option<String>,
    pub checked: Vec<ConditionCheck>,
}

/// Formats binary byte counts cleanly for condition descriptions and skip reasons.
pub fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let amount = bytes as f64;
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GiB", amount / GIB)
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MiB", amount / MIB)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", amount / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Validates every condition item on an automation spec.
pub fn validate_conditions(conditions: &Conditions) -> Result<(), String> {
    for condition in &conditions.items {
        match condition {
            Condition::TimeOfDay { from, to, zone } => {
                parse_zone(zone, "time_of_day condition")?;
                if !from.is_valid() {
                    return Err("time_of_day `from` must be HH:MM (00:00..23:59)".into());
                }
                if !to.is_valid() {
                    return Err("time_of_day `to` must be HH:MM (00:00..23:59)".into());
                }
                if from == to {
                    return Err("time_of_day `from` and `to` must differ".into());
                }
            }
            Condition::Weekdays { days, zone } => {
                parse_zone(zone, "weekdays condition")?;
                if days.is_empty() {
                    return Err("weekdays condition needs at least one day".into());
                }
            }
            Condition::File {
                pattern,
                min_bytes,
                max_bytes,
            } => {
                if pattern.is_none() && min_bytes.is_none() && max_bytes.is_none() {
                    return Err("file condition needs pattern, min_bytes or max_bytes".into());
                }
                if let Some(glob) = pattern
                    && (glob.is_empty() || glob.contains('/'))
                {
                    return Err("file condition pattern must be non-empty without `/`".into());
                }
                if let (Some(minimum), Some(maximum)) = (min_bytes, max_bytes)
                    && minimum > maximum
                {
                    return Err("file condition min_bytes must not exceed max_bytes".into());
                }
            }
        }
    }
    Ok(())
}

/// Human-readable description of one condition.
pub fn condition_text(condition: &Condition) -> String {
    match condition {
        Condition::TimeOfDay { from, to, zone } => {
            let overnight = if from > to { ", over midnight" } else { "" };
            format!("Between {from} and {to} {zone}{overnight}")
        }
        Condition::Weekdays { days, zone } => format!("{} ({zone})", days_text(days)),
        Condition::File {
            pattern,
            min_bytes,
            max_bytes,
        } => {
            let mut sizes = Vec::new();
            if let Some(minimum) = min_bytes {
                sizes.push(format!("at least {}", format_bytes(*minimum)));
            }
            if let Some(maximum) = max_bytes {
                sizes.push(format!("at most {}", format_bytes(*maximum)));
            }
            match (pattern, sizes.is_empty()) {
                (Some(glob), true) => format!("File name matches {glob}"),
                (Some(glob), false) => {
                    format!(
                        "File name matches {glob} and file is {}",
                        sizes.join(" and ")
                    )
                }
                (None, false) => format!("File is {}", sizes.join(" and ")),
                (None, true) => "Any file".into(),
            }
        }
    }
}

/// Evaluates all conditions on `conditions` at `now_ms` against `payload`.
pub fn evaluate_conditions(
    conditions: &Conditions,
    now_ms: u64,
    payload: &TriggerPayload,
) -> ConditionEvaluation {
    if conditions.items.is_empty() {
        return ConditionEvaluation {
            ok: true,
            reason: None,
            checked: Vec::new(),
        };
    }
    let timestamp = i64::try_from(now_ms)
        .ok()
        .and_then(|ms| Timestamp::from_millisecond(ms).ok());
    let mut checked = Vec::with_capacity(conditions.items.len());
    let mut applicable = Vec::with_capacity(conditions.items.len());
    for condition in &conditions.items {
        let text = condition_text(condition);
        let (kind, held, not_applicable, why) = match condition {
            Condition::TimeOfDay { from, to, zone } => {
                let outcome = timestamp.and_then(|instant| {
                    let time_zone = parse_zone(zone, "time_of_day condition").ok()?;
                    let local = time_zone.to_datetime(instant);
                    let current = ClockTime::new(local.hour() as u8, local.minute() as u8)?;
                    let inside = if from <= to {
                        current >= *from && current < *to
                    } else {
                        current >= *from || current < *to
                    };
                    let why = if inside {
                        format!("It was {current} {zone}.")
                    } else {
                        format!("It was {current} {zone}, outside {from} to {to} {zone}.")
                    };
                    Some((inside, why))
                });
                let (held, why) =
                    outcome.unwrap_or_else(|| (false, format!("Invalid time zone `{zone}`.")));
                (ConditionCheckKind::TimeOfDay, held, false, why)
            }
            Condition::Weekdays { days, zone } => {
                let outcome = timestamp.and_then(|instant| {
                    let time_zone = parse_zone(zone, "weekdays condition").ok()?;
                    let weekday = Weekday::from_jiff(time_zone.to_datetime(instant).weekday());
                    let allowed = days.contains(&weekday);
                    let why = if allowed {
                        format!("It was {} in {zone}.", weekday.label())
                    } else {
                        format!(
                            "It was {} in {zone}, and it runs only {}.",
                            weekday.label(),
                            days_text(days)
                        )
                    };
                    Some((allowed, why))
                });
                let (held, why) =
                    outcome.unwrap_or_else(|| (false, format!("Invalid time zone `{zone}`.")));
                (ConditionCheckKind::Weekdays, held, false, why)
            }
            Condition::File {
                pattern,
                min_bytes,
                max_bytes,
            } => match payload {
                TriggerPayload::DataUploaded {
                    key, size_bytes, ..
                } => {
                    let name = key.rsplit('/').next().unwrap_or(key);
                    let needs_size = min_bytes.is_some() || max_bytes.is_some();
                    let (held, why) = if let Some(glob) = pattern
                        && !glob_match(glob, name)
                    {
                        (false, format!("The file {name} does not match {glob}."))
                    } else if needs_size && size_bytes.is_none() {
                        (false, format!("The size of file {name} is unavailable."))
                    } else if let (Some(minimum), Some(actual)) = (min_bytes, size_bytes)
                        && actual < minimum
                    {
                        (
                            false,
                            format!(
                                "The file {name} is {}, less than {}.",
                                format_bytes(*actual),
                                format_bytes(*minimum)
                            ),
                        )
                    } else if let (Some(maximum), Some(actual)) = (max_bytes, size_bytes)
                        && actual > maximum
                    {
                        (
                            false,
                            format!(
                                "The file {name} is {}, more than {}.",
                                format_bytes(*actual),
                                format_bytes(*maximum)
                            ),
                        )
                    } else {
                        match size_bytes {
                            Some(actual) => (
                                true,
                                format!("The file {name} is {}.", format_bytes(*actual)),
                            ),
                            None => (true, format!("The file {name} matches.")),
                        }
                    };
                    (ConditionCheckKind::File, held, false, why)
                }
                _ => (
                    ConditionCheckKind::File,
                    true,
                    true,
                    "This start has no file, so the condition does not apply.".into(),
                ),
            },
        };
        checked.push(ConditionCheck {
            kind,
            text,
            held,
            why,
        });
        applicable.push(!not_applicable);
    }
    let evaluated: Vec<&ConditionCheck> = checked
        .iter()
        .zip(&applicable)
        .filter_map(|(item, &applies)| applies.then_some(item))
        .collect();
    let ok = match conditions.mode {
        ConditionMode::All => checked.iter().all(|item| item.held),
        ConditionMode::Any => !evaluated.is_empty() && evaluated.iter().any(|item| item.held),
    };
    let reason = if ok {
        None
    } else {
        let failed: Vec<&str> = checked
            .iter()
            .filter(|item| !item.held)
            .map(|item| item.why.as_str())
            .collect();
        match conditions.mode {
            ConditionMode::All => failed.first().map(|text| (*text).to_string()),
            ConditionMode::Any if evaluated.is_empty() => {
                Some("None of the conditions applied to this start.".into())
            }
            ConditionMode::Any => {
                Some(format!("None of the conditions held. {}", failed.join(" ")))
            }
        }
    };
    ConditionEvaluation {
        ok,
        reason,
        checked,
    }
}

fn canonical_condition(condition: &Condition) -> Condition {
    match condition {
        Condition::TimeOfDay { from, to, zone } => Condition::TimeOfDay {
            from: *from,
            to: *to,
            zone: zone.trim().to_string(),
        },
        Condition::Weekdays { days, zone } => {
            let mut normalized = days.clone();
            normalized.sort_by_key(|day| day.ordinal());
            normalized.dedup();
            Condition::Weekdays {
                days: normalized,
                zone: zone.trim().to_string(),
            }
        }
        Condition::File { .. } => condition.clone(),
    }
}

/// Whether changing conditions from `before` to `after` widens when the automation can start.
pub fn conditions_widen(before: &Conditions, after: &Conditions) -> bool {
    let before_set: BTreeSet<Condition> = before.items.iter().map(canonical_condition).collect();
    let after_set: BTreeSet<Condition> = after.items.iter().map(canonical_condition).collect();
    if before_set.is_empty() {
        return false;
    }
    if after_set.is_empty() {
        return true;
    }
    match (before.mode, after.mode) {
        (ConditionMode::All, ConditionMode::All) => !before_set.is_subset(&after_set),
        (ConditionMode::Any, ConditionMode::Any) => !after_set.is_subset(&before_set),
        (ConditionMode::Any, ConditionMode::All) => {
            // Switching Any -> All narrows only if at least one always-applicable condition from
            // `before` is required in `after`; otherwise a non-file start could pass All([File])
            // when Any([File]) would refuse it.
            !before_set
                .intersection(&after_set)
                .any(|item| !matches!(item, Condition::File { .. }))
        }
        (ConditionMode::All, ConditionMode::Any) => {
            !(before_set.len() == 1 && before_set == after_set)
        }
    }
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
