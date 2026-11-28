//! Automation start conditions evaluated purely against IANA time zones and file payloads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use jiff::Timestamp;
use jiff::tz::TimeZone;
use serde::{Deserialize, Serialize};

use super::firing::TriggerPayload;
use super::glob::glob_match;
pub use super::schedule::{
    LIVE_GRACE_MS, ScheduleEvery, ScheduleSpec, Weekday, days_text, due_slots, next_start,
    parse_clock_time, schedule_due_slots, validate_schedule,
};
use super::schedule::{format_clock_time, parse_zone};

/// Maximum condition items allowed on one automation spec.
pub const MAX_CONDITIONS: usize = 16;

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Condition {
    TimeOfDay {
        from: String,
        to: String,
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

/// Recorded outcome of evaluating one condition or limit for a firing or skipped start.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionCheck {
    pub kind: String,
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

/// Formats byte counts cleanly for condition descriptions and skip reasons.
pub fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let amount = bytes as f64;
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", amount / GIB)
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", amount / MIB)
    } else if bytes >= 1024 {
        format!("{:.1} KB", amount / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Validates every condition item on an automation spec.
pub fn validate_conditions(conditions: &Conditions) -> Result<(), String> {
    if conditions.items.len() > MAX_CONDITIONS {
        return Err(format!(
            "automations allow at most {MAX_CONDITIONS} conditions"
        ));
    }
    for condition in &conditions.items {
        match condition {
            Condition::TimeOfDay { from, to, zone } => {
                parse_zone(zone, "time_of_day condition")?;
                let Some(start) = parse_clock_time(from) else {
                    return Err("time_of_day `from` must be HH:MM (00:00..23:59)".into());
                };
                let Some(end) = parse_clock_time(to) else {
                    return Err("time_of_day `to` must be HH:MM (00:00..23:59)".into());
                };
                if start == end {
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
            let overnight = match (parse_clock_time(from), parse_clock_time(to)) {
                (Some(start), Some(end)) if start > end => ", over midnight",
                _ => "",
            };
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
                    let tz = TimeZone::get(zone.trim()).ok()?;
                    let local = tz.to_datetime(instant);
                    let current =
                        u16::from(local.hour() as u8) * 60 + u16::from(local.minute() as u8);
                    let start = parse_clock_time(from)?;
                    let end = parse_clock_time(to)?;
                    let inside = if start <= end {
                        current >= start && current < end
                    } else {
                        current >= start || current < end
                    };
                    let why = if inside {
                        format!("It was {} {zone}.", format_clock_time(current))
                    } else {
                        format!(
                            "It was {} {zone}, outside {from} to {to} {zone}.",
                            format_clock_time(current)
                        )
                    };
                    Some((inside, why))
                });
                let (held, why) =
                    outcome.unwrap_or_else(|| (false, format!("Invalid time zone `{zone}`.")));
                ("time_of_day", held, false, why)
            }
            Condition::Weekdays { days, zone } => {
                let outcome = timestamp.and_then(|instant| {
                    let tz = TimeZone::get(zone.trim()).ok()?;
                    let weekday = Weekday::from_jiff(tz.to_datetime(instant).weekday());
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
                ("weekdays", held, false, why)
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
                    let (held, why) = if let Some(glob) = pattern
                        && !glob_match(glob, name)
                    {
                        (false, format!("The file {name} does not match {glob}."))
                    } else if let Some(minimum) = min_bytes
                        && size_bytes < minimum
                    {
                        (
                            false,
                            format!(
                                "The file {name} is {}, less than {}.",
                                format_bytes(*size_bytes),
                                format_bytes(*minimum)
                            ),
                        )
                    } else if let Some(maximum) = max_bytes
                        && size_bytes > maximum
                    {
                        (
                            false,
                            format!(
                                "The file {name} is {}, more than {}.",
                                format_bytes(*size_bytes),
                                format_bytes(*maximum)
                            ),
                        )
                    } else {
                        (
                            true,
                            format!("The file {name} is {}.", format_bytes(*size_bytes)),
                        )
                    };
                    ("file", held, false, why)
                }
                _ => (
                    "file",
                    true,
                    true,
                    "This start has no file, so the condition does not apply.".into(),
                ),
            },
        };
        checked.push(ConditionCheck {
            kind: kind.into(),
            text,
            held,
            why,
        });
        applicable.push(!not_applicable);
    }
    let ok = match conditions.mode {
        ConditionMode::All => checked.iter().all(|item| item.held),
        ConditionMode::Any => {
            let evaluated: Vec<&ConditionCheck> = checked
                .iter()
                .zip(&applicable)
                .filter_map(|(item, &applies)| applies.then_some(item))
                .collect();
            evaluated.is_empty() || evaluated.iter().any(|item| item.held)
        }
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

/// Whether changing conditions from `before` to `after` widens when the automation can start.
pub fn conditions_widen(before: &Conditions, after: &Conditions) -> bool {
    if before.items.is_empty() {
        return false;
    }
    if after.items.is_empty() {
        return true;
    }
    if before.mode == ConditionMode::All && after.mode == ConditionMode::All {
        return before.items.iter().any(|item| !after.items.contains(item));
    }
    before != after
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
