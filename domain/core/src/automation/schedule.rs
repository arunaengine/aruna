//! Recurring wall-clock schedule triggers evaluated purely against IANA time zones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use jiff::civil::{Date, DateTime, Weekday as JiffDay};
use jiff::tz::TimeZone;
use jiff::{Timestamp, ToSpan};
use serde::{Deserialize, Serialize};

use super::record::{Spec, Trigger};

/// Grace window for firing a due schedule slot live rather than recording it as missed.
pub const LIVE_GRACE_MS: u64 = 120_000;

/// Maximum calendar-day span scanned for missed slots after an extended outage.
const MAX_SCAN_DAYS: i64 = 90;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Weekday {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl Weekday {
    pub const ALL: [Self; 7] = [
        Self::Monday,
        Self::Tuesday,
        Self::Wednesday,
        Self::Thursday,
        Self::Friday,
        Self::Saturday,
        Self::Sunday,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Monday => "mon",
            Self::Tuesday => "tue",
            Self::Wednesday => "wed",
            Self::Thursday => "thu",
            Self::Friday => "fri",
            Self::Saturday => "sat",
            Self::Sunday => "sun",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Monday => "Monday",
            Self::Tuesday => "Tuesday",
            Self::Wednesday => "Wednesday",
            Self::Thursday => "Thursday",
            Self::Friday => "Friday",
            Self::Saturday => "Saturday",
            Self::Sunday => "Sunday",
        }
    }

    pub const fn ordinal(self) -> u8 {
        match self {
            Self::Monday => 0,
            Self::Tuesday => 1,
            Self::Wednesday => 2,
            Self::Thursday => 3,
            Self::Friday => 4,
            Self::Saturday => 5,
            Self::Sunday => 6,
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|day| day.as_str().eq_ignore_ascii_case(text))
    }

    pub(crate) fn from_jiff(day: JiffDay) -> Self {
        match day {
            JiffDay::Monday => Self::Monday,
            JiffDay::Tuesday => Self::Tuesday,
            JiffDay::Wednesday => Self::Wednesday,
            JiffDay::Thursday => Self::Thursday,
            JiffDay::Friday => Self::Friday,
            JiffDay::Saturday => Self::Saturday,
            JiffDay::Sunday => Self::Sunday,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScheduleEvery {
    Day,
    Week,
    Hours,
}

impl ScheduleEvery {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Week => "week",
            Self::Hours => "hours",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "day" => Some(Self::Day),
            "week" => Some(Self::Week),
            "hours" => Some(Self::Hours),
            _ => None,
        }
    }
}

/// Recurring wall-clock schedule evaluated in an explicit IANA time zone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleSpec {
    pub every: ScheduleEvery,
    /// Wall-clock anchor `"HH:MM"` (`00:00`..=`23:59`).
    pub at: String,
    /// Selected weekdays when `every` is `Week`.
    pub days: Vec<Weekday>,
    /// Hour interval (`1..=24`) when `every` is `Hours`.
    pub hours: Option<u8>,
    /// Required IANA time zone identifier, such as `"Europe/Berlin"` or `"UTC"`.
    pub zone: String,
}

/// Parses canonical `"HH:MM"` (`00:00`..=`23:59`) into minutes since midnight.
pub fn parse_clock_time(text: &str) -> Option<u16> {
    let bytes = text.as_bytes();
    if bytes.len() != 5 || bytes[2] != b':' {
        return None;
    }
    let hour: u16 = text[..2].parse().ok()?;
    let minute: u16 = text[3..].parse().ok()?;
    (hour < 24 && minute < 60).then_some(hour * 60 + minute)
}

pub(crate) fn format_clock_time(minutes: u16) -> String {
    format!("{:02}:{:02}", minutes / 60, minutes % 60)
}

pub(crate) fn parse_zone(zone: &str, label: &str) -> Result<TimeZone, String> {
    let trimmed = zone.trim();
    if trimmed.is_empty() {
        return Err(format!("{label} needs an IANA time zone"));
    }
    TimeZone::get(trimmed).map_err(|_| format!("{label} has unknown IANA time zone `{trimmed}`"))
}

/// Formats selected weekdays into readable text (`"Monday to Friday"`, `"Every day"`, etc.).
pub fn days_text(days: &[Weekday]) -> String {
    let mut unique: Vec<Weekday> = days.to_vec();
    unique.sort_by_key(|day| day.ordinal());
    unique.dedup();
    if unique.is_empty() {
        return "no day".into();
    }
    if unique.len() == 7 {
        return "Every day".into();
    }
    let contiguous = unique.len() >= 3
        && usize::from(unique[unique.len() - 1].ordinal() - unique[0].ordinal())
            == unique.len() - 1;
    if contiguous {
        return format!(
            "{} to {}",
            unique[0].label(),
            unique[unique.len() - 1].label()
        );
    }
    let labels: Vec<&str> = unique.iter().map(|day| day.label()).collect();
    match labels.as_slice() {
        [only] => (*only).to_string(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
        [] => "no day".into(),
    }
}

/// Validates a schedule trigger definition.
pub fn validate_schedule(schedule: &ScheduleSpec) -> Result<(), String> {
    parse_zone(&schedule.zone, "schedule trigger")?;
    if parse_clock_time(&schedule.at).is_none() {
        return Err("schedule trigger `at` must be HH:MM (00:00..23:59)".into());
    }
    match schedule.every {
        ScheduleEvery::Day => {
            if !schedule.days.is_empty() {
                return Err("daily schedule must not specify `days`".into());
            }
            if schedule.hours.is_some() {
                return Err("daily schedule must not specify `hours`".into());
            }
        }
        ScheduleEvery::Week => {
            if schedule.days.is_empty() {
                return Err("weekly schedule needs at least one day in `days`".into());
            }
            if schedule.hours.is_some() {
                return Err("weekly schedule must not specify `hours`".into());
            }
        }
        ScheduleEvery::Hours => {
            if !schedule.days.is_empty() {
                return Err("hourly schedule must not specify `days`".into());
            }
            match schedule.hours {
                Some(1..=24) => {}
                _ => return Err("hourly schedule needs `hours` in 1..=24".into()),
            }
        }
    }
    Ok(())
}

fn slot_minutes(schedule: &ScheduleSpec) -> Vec<u16> {
    let anchor = parse_clock_time(&schedule.at).unwrap_or(0);
    if schedule.every != ScheduleEvery::Hours {
        return vec![anchor];
    }
    let step = u16::from(schedule.hours.unwrap_or(1).clamp(1, 24)) * 60;
    let mut minutes = Vec::new();
    let mut current = anchor % step;
    while current < 1440 {
        minutes.push(current);
        current += step;
    }
    minutes
}

fn local_slot_ms(date: Date, minutes: u16, zone: &TimeZone) -> Option<u64> {
    let datetime = DateTime::new(
        date.year(),
        date.month(),
        date.day(),
        (minutes / 60) as i8,
        (minutes % 60) as i8,
        0,
        0,
    )
    .ok()?;
    let timestamp = zone.to_ambiguous_timestamp(datetime).compatible().ok()?;
    u64::try_from(timestamp.as_millisecond()).ok()
}

/// Returns every scheduled slot timestamp in `(after_ms, now_ms]` for one schedule trigger.
pub fn due_slots(schedule: &ScheduleSpec, after_ms: u64, now_ms: u64) -> Vec<u64> {
    if after_ms >= now_ms {
        return Vec::new();
    }
    let Ok(zone) = TimeZone::get(schedule.zone.trim()) else {
        return Vec::new();
    };
    let Some(end_timestamp) = i64::try_from(now_ms)
        .ok()
        .and_then(|ms| Timestamp::from_millisecond(ms).ok())
    else {
        return Vec::new();
    };
    let end_date = zone.to_datetime(end_timestamp).date();
    let floor_date = end_date
        .checked_sub(MAX_SCAN_DAYS.days())
        .unwrap_or(end_date);
    let raw_start = i64::try_from(after_ms)
        .ok()
        .and_then(|ms| Timestamp::from_millisecond(ms).ok())
        .map(|timestamp| zone.to_datetime(timestamp).date())
        .unwrap_or(floor_date);
    let start_date = raw_start.yesterday().unwrap_or(raw_start).max(floor_date);
    let stop_date = end_date.tomorrow().unwrap_or(end_date);
    let times = slot_minutes(schedule);
    let mut slots = BTreeSet::new();
    let mut current = start_date;
    while current <= stop_date {
        let weekday = Weekday::from_jiff(current.weekday());
        let day_ok = schedule.every != ScheduleEvery::Week || schedule.days.contains(&weekday);
        if day_ok {
            for &minutes in &times {
                if let Some(ms) = local_slot_ms(current, minutes, &zone)
                    && ms > after_ms
                    && ms <= now_ms
                {
                    slots.insert(ms);
                }
            }
        }
        let Ok(next) = current.tomorrow() else {
            break;
        };
        current = next;
    }
    slots.into_iter().collect()
}

/// Merges every schedule trigger in `spec` into sorted unique due slots in `(after_ms, now_ms]`.
pub fn schedule_due_slots(spec: &Spec, after_ms: u64, now_ms: u64) -> Vec<u64> {
    let mut all = BTreeSet::new();
    for trigger in &spec.triggers {
        if let Trigger::Schedule(schedule) = trigger {
            all.extend(due_slots(schedule, after_ms, now_ms));
        }
    }
    all.into_iter().collect()
}

/// Next scheduled start strictly after `after_ms` across all schedule triggers in `spec`.
pub fn next_start(spec: &Spec, after_ms: u64) -> Option<u64> {
    let mut best: Option<u64> = None;
    for trigger in &spec.triggers {
        if let Trigger::Schedule(schedule) = trigger
            && let Some(ms) = next_slot(schedule, after_ms)
        {
            best = Some(best.map_or(ms, |previous| previous.min(ms)));
        }
    }
    best
}

fn next_slot(schedule: &ScheduleSpec, after_ms: u64) -> Option<u64> {
    let zone = TimeZone::get(schedule.zone.trim()).ok()?;
    let timestamp = Timestamp::from_millisecond(i64::try_from(after_ms).ok()?).ok()?;
    let start = zone.to_datetime(timestamp).date();
    let times = slot_minutes(schedule);
    let mut current = start.yesterday().unwrap_or(start);
    for _ in 0..10 {
        let weekday = Weekday::from_jiff(current.weekday());
        let day_ok = schedule.every != ScheduleEvery::Week || schedule.days.contains(&weekday);
        if day_ok {
            for &minutes in &times {
                if let Some(ms) = local_slot_ms(current, minutes, &zone)
                    && ms > after_ms
                {
                    return Some(ms);
                }
            }
        }
        current = current.tomorrow().ok()?;
    }
    None
}
