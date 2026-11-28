//! Recurring wall-clock schedule triggers evaluated purely against bundled IANA time zones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::fmt;
use std::sync::LazyLock;

use jiff::Timestamp;
use jiff::civil::{Date, DateTime, Weekday as JiffDay};
use jiff::tz::{TimeZone, TimeZoneDatabase};
use serde::{Deserialize, Serialize};

use super::record::{Spec, Trigger};
use crate::errors::ConversionError;

/// Grace window for firing a due schedule slot live rather than recording it as missed.
pub const LIVE_GRACE_MS: u64 = 120_000;

const DIRECT_EVAL_DAYS: i64 = 21;
const BOUNDARY_DAYS: i64 = 7;

static BUNDLED_TZDB: LazyLock<TimeZoneDatabase> = LazyLock::new(TimeZoneDatabase::bundled);

/// Validated wall-clock hour and minute (`00:00`..=`23:59`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClockTime {
    hour: u8,
    minute: u8,
}

impl ClockTime {
    pub fn new(hour: u8, minute: u8) -> Option<Self> {
        (hour < 24 && minute < 60).then_some(Self { hour, minute })
    }

    /// Parses canonical `"HH:MM"` (`00:00`..=`23:59`).
    pub fn parse(text: &str) -> Option<Self> {
        let bytes = text.as_bytes();
        if bytes.len() != 5 || bytes[2] != b':' {
            return None;
        }
        let hour: u8 = text[..2].parse().ok()?;
        let minute: u8 = text[3..].parse().ok()?;
        Self::new(hour, minute)
    }

    pub const fn hour(self) -> u8 {
        self.hour
    }

    pub const fn minute(self) -> u8 {
        self.minute
    }

    pub const fn is_valid(self) -> bool {
        self.hour < 24 && self.minute < 60
    }

    pub const fn minutes_since_midnight(self) -> u16 {
        (self.hour as u16) * 60 + (self.minute as u16)
    }
}

impl fmt::Display for ClockTime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:02}:{:02}", self.hour, self.minute)
    }
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

/// Recurring wall-clock schedule evaluated in an explicit bundled IANA time zone.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScheduleSpec {
    pub every: ScheduleEvery,
    /// Wall-clock anchor (`00:00`..=`23:59`).
    pub at: ClockTime,
    /// Selected weekdays when `every` is `Week`.
    pub days: Vec<Weekday>,
    /// Hour interval (`1..=24`) when `every` is `Hours`.
    pub hours: Option<u8>,
    /// Required IANA time zone identifier, such as `"Europe/Berlin"` or `"UTC"`.
    pub zone: String,
}

/// Last processed timestamp tracked for one schedule trigger on an active automation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleAnchor {
    pub schedule: ScheduleSpec,
    pub last_ms: u64,
}

/// Persisted due-time cursor for one active scheduled automation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleCursor {
    /// Earliest next scheduled instant strictly after each anchor's `last_ms`.
    pub due_ms: u64,
    pub anchors: Vec<ScheduleAnchor>,
}

impl ScheduleCursor {
    /// Builds or updates the schedule cursor for `spec`, preserving existing per-schedule anchors
    /// when the automation was already active so unrelated edits never erase already-due slots.
    pub fn for_spec(
        spec: &Spec,
        existing: Option<&Self>,
        was_active: bool,
        now_ms: u64,
    ) -> Option<Self> {
        let schedules = unique_schedules(spec);
        if schedules.is_empty() {
            return None;
        }
        let mut anchors = Vec::with_capacity(schedules.len());
        for schedule in schedules {
            let preserved = if was_active {
                existing.and_then(|cursor| {
                    cursor
                        .anchors
                        .iter()
                        .find(|anchor| anchor.schedule == schedule)
                        .map(|anchor| anchor.last_ms)
                })
            } else {
                None
            };
            anchors.push(ScheduleAnchor {
                schedule,
                last_ms: preserved.unwrap_or(now_ms),
            });
        }
        let due_ms = anchors
            .iter()
            .filter_map(|anchor| next_slot(&anchor.schedule, anchor.last_ms))
            .min()?;
        Some(Self { due_ms, anchors })
    }

    /// Advances every anchor up to `processed_ms` and recomputes the earliest next due instant.
    pub fn advance(&self, spec: &Spec, processed_ms: u64, now_ms: u64) -> Option<Self> {
        let schedules = unique_schedules(spec);
        if schedules.is_empty() {
            return None;
        }
        let mut anchors = Vec::with_capacity(schedules.len());
        for schedule in schedules {
            let previous = self
                .anchors
                .iter()
                .find(|anchor| anchor.schedule == schedule)
                .map_or(now_ms, |anchor| anchor.last_ms);
            anchors.push(ScheduleAnchor {
                schedule,
                last_ms: previous.max(processed_ms),
            });
        }
        let due_ms = anchors
            .iter()
            .filter_map(|anchor| next_slot(&anchor.schedule, anchor.last_ms))
            .min()?;
        Some(Self { due_ms, anchors })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Summary of missed and live schedule starts ready at `now_ms`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScheduleDue {
    pub missed_count: u64,
    pub first_missed_ms: Option<u64>,
    pub last_missed_ms: Option<u64>,
    pub live_slot_ms: Option<u64>,
}

/// Looks up an IANA time zone exclusively from the bundled database (zero host file I/O).
pub(crate) fn parse_zone(zone: &str, label: &str) -> Result<TimeZone, String> {
    let trimmed = zone.trim();
    if trimmed.is_empty() {
        return Err(format!("{label} needs an IANA time zone"));
    }
    BUNDLED_TZDB
        .get(trimmed)
        .map_err(|_| format!("{label} has unknown IANA time zone `{trimmed}`"))
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
    if !schedule.at.is_valid() {
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

fn unique_schedules(spec: &Spec) -> Vec<ScheduleSpec> {
    let mut schedules = Vec::new();
    for trigger in &spec.triggers {
        if let Trigger::Schedule(schedule) = trigger
            && !schedules.contains(schedule)
        {
            schedules.push(schedule.clone());
        }
    }
    schedules
}

pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let adjusted_year = i64::from(year) - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let month_i64 = i64::from(month);
    let day_i64 = i64::from(day);
    let day_of_year =
        (153 * (month_i64 + if month_i64 > 2 { -3 } else { 9 }) + 2) / 5 + day_i64 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub(crate) fn civil_from_days(days: i64) -> Option<Date> {
    let shifted = days.checked_add(719_468)?;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let adjusted_year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    let year = adjusted_year + i64::from(month <= 2);
    Date::new(i16::try_from(year).ok()?, month as i8, day as i8).ok()
}

fn date_to_days(date: Date) -> i64 {
    days_from_civil(
        i32::from(date.year()),
        u32::try_from(date.month()).unwrap_or(1),
        u32::try_from(date.day()).unwrap_or(1),
    )
}

fn weekday_for_days(days: i64) -> Weekday {
    match (days + 3).rem_euclid(7) {
        0 => Weekday::Monday,
        1 => Weekday::Tuesday,
        2 => Weekday::Wednesday,
        3 => Weekday::Thursday,
        4 => Weekday::Friday,
        5 => Weekday::Saturday,
        _ => Weekday::Sunday,
    }
}

fn local_slot_ms(date: Date, time: ClockTime, zone: &TimeZone) -> Option<u64> {
    let datetime = DateTime::new(
        date.year(),
        date.month(),
        date.day(),
        time.hour() as i8,
        time.minute() as i8,
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
