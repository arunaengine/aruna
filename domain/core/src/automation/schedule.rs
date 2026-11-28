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

fn hourly_step_minutes(schedule: &ScheduleSpec) -> Option<i64> {
    let hours = schedule.hours?;
    (1..=24).contains(&hours).then_some(i64::from(hours) * 60)
}

fn collect_day_range(
    schedule: &ScheduleSpec,
    zone: &TimeZone,
    first_day: i64,
    last_day: i64,
    after_ms: u64,
    now_ms: u64,
    out: &mut BTreeSet<u64>,
) {
    if first_day > last_day {
        return;
    }
    match schedule.every {
        ScheduleEvery::Day => {
            for day in first_day..=last_day {
                if let Some(date) = civil_from_days(day)
                    && let Some(ms) = local_slot_ms(date, schedule.at, zone)
                    && ms > after_ms
                    && ms <= now_ms
                {
                    out.insert(ms);
                }
            }
        }
        ScheduleEvery::Week => {
            for day in first_day..=last_day {
                if schedule.days.contains(&weekday_for_days(day))
                    && let Some(date) = civil_from_days(day)
                    && let Some(ms) = local_slot_ms(date, schedule.at, zone)
                    && ms > after_ms
                    && ms <= now_ms
                {
                    out.insert(ms);
                }
            }
        }
        ScheduleEvery::Hours => {
            let Some(step_minutes) = hourly_step_minutes(schedule) else {
                return;
            };
            let anchor = i64::from(schedule.at.minutes_since_midnight());
            let low_minute = first_day.saturating_mul(1440);
            let high_minute = last_day.saturating_add(1).saturating_mul(1440) - 1;
            let mut index = (low_minute - anchor).div_euclid(step_minutes);
            if anchor + index * step_minutes < low_minute {
                index += 1;
            }
            let end_index = (high_minute - anchor).div_euclid(step_minutes);
            for slot_index in index..=end_index {
                let total_minutes = anchor + slot_index * step_minutes;
                let day = total_minutes.div_euclid(1440);
                let minute_of_day = total_minutes.rem_euclid(1440) as u16;
                let Some(clock) =
                    ClockTime::new((minute_of_day / 60) as u8, (minute_of_day % 60) as u8)
                else {
                    continue;
                };
                if let Some(date) = civil_from_days(day)
                    && let Some(ms) = local_slot_ms(date, clock, zone)
                    && ms > after_ms
                    && ms <= now_ms
                {
                    out.insert(ms);
                }
            }
        }
    }
}

fn count_interior_slots(schedule: &ScheduleSpec, first_day: i64, last_day: i64) -> u64 {
    if first_day > last_day {
        return 0;
    }
    let total_days = (last_day - first_day + 1) as u64;
    match schedule.every {
        ScheduleEvery::Day => total_days,
        ScheduleEvery::Week => {
            let mut unique_days: Vec<Weekday> = schedule.days.clone();
            unique_days.sort_by_key(|day| day.ordinal());
            unique_days.dedup();
            if unique_days.is_empty() {
                return 0;
            }
            let full_weeks = total_days / 7;
            let remainder = (total_days % 7) as i64;
            let mut extra = 0u64;
            for offset in 0..remainder {
                if unique_days.contains(&weekday_for_days(first_day + offset)) {
                    extra += 1;
                }
            }
            full_weeks
                .saturating_mul(unique_days.len() as u64)
                .saturating_add(extra)
        }
        ScheduleEvery::Hours => {
            let Some(step_minutes) = hourly_step_minutes(schedule) else {
                return 0;
            };
            let anchor = i64::from(schedule.at.minutes_since_midnight());
            let low_minute = first_day.saturating_mul(1440);
            let high_minute = last_day.saturating_add(1).saturating_mul(1440) - 1;
            let mut first_index = (low_minute - anchor).div_euclid(step_minutes);
            if anchor + first_index * step_minutes < low_minute {
                first_index += 1;
            }
            let last_index = (high_minute - anchor).div_euclid(step_minutes);
            if first_index > last_index {
                0
            } else {
                (last_index - first_index + 1) as u64
            }
        }
    }
}

/// Evaluates missed and live slots in `(after_ms, now_ms]` for one schedule in O(1) time.
pub fn schedule_due_single(schedule: &ScheduleSpec, after_ms: u64, now_ms: u64) -> ScheduleDue {
    if after_ms >= now_ms || validate_schedule(schedule).is_err() {
        return ScheduleDue::default();
    }
    let Ok(zone) = parse_zone(&schedule.zone, "schedule trigger") else {
        return ScheduleDue::default();
    };
    let (Some(start_ts), Some(end_ts)) = (
        i64::try_from(after_ms)
            .ok()
            .and_then(|ms| Timestamp::from_millisecond(ms).ok()),
        i64::try_from(now_ms)
            .ok()
            .and_then(|ms| Timestamp::from_millisecond(ms).ok()),
    ) else {
        return ScheduleDue::default();
    };
    let live_cutoff = now_ms.saturating_sub(LIVE_GRACE_MS);
    let cutoff_ts = i64::try_from(live_cutoff)
        .ok()
        .and_then(|ms| Timestamp::from_millisecond(ms).ok())
        .unwrap_or(end_ts);
    let start_day = date_to_days(zone.to_datetime(start_ts).date());
    let cutoff_day = date_to_days(zone.to_datetime(cutoff_ts).date());
    let end_day = date_to_days(zone.to_datetime(end_ts).date());

    if end_day - start_day <= DIRECT_EVAL_DAYS {
        let mut slots = BTreeSet::new();
        collect_day_range(
            schedule,
            &zone,
            start_day - 1,
            end_day + 1,
            after_ms,
            now_ms,
            &mut slots,
        );
        return summarize_slots(&slots, live_cutoff);
    }

    let mut left_slots = BTreeSet::new();
    collect_day_range(
        schedule,
        &zone,
        start_day - 1,
        start_day + BOUNDARY_DAYS,
        after_ms,
        live_cutoff.saturating_sub(1),
        &mut left_slots,
    );
    let mut right_slots = BTreeSet::new();
    collect_day_range(
        schedule,
        &zone,
        cutoff_day - BOUNDARY_DAYS,
        end_day + 1,
        after_ms,
        now_ms,
        &mut right_slots,
    );
    let interior_count = count_interior_slots(
        schedule,
        start_day + BOUNDARY_DAYS + 1,
        cutoff_day - BOUNDARY_DAYS - 1,
    );
    let right_missed: Vec<u64> = right_slots
        .iter()
        .copied()
        .filter(|ms| *ms < live_cutoff)
        .collect();
    let live_slot_ms = right_slots.iter().copied().find(|ms| *ms >= live_cutoff);
    let missed_count = (left_slots.len() as u64)
        .saturating_add(interior_count)
        .saturating_add(right_missed.len() as u64);
    let first_missed_ms = left_slots
        .iter()
        .next()
        .copied()
        .or_else(|| right_missed.first().copied());
    let last_missed_ms = right_missed
        .last()
        .copied()
        .or_else(|| left_slots.iter().next_back().copied());
    ScheduleDue {
        missed_count,
        first_missed_ms,
        last_missed_ms,
        live_slot_ms,
    }
}

fn summarize_slots(slots: &BTreeSet<u64>, live_cutoff: u64) -> ScheduleDue {
    let mut missed_count = 0u64;
    let mut first_missed_ms = None;
    let mut last_missed_ms = None;
    let mut live_slot_ms = None;
    for &ms in slots {
        if ms < live_cutoff {
            missed_count = missed_count.saturating_add(1);
            if first_missed_ms.is_none() {
                first_missed_ms = Some(ms);
            }
            last_missed_ms = Some(ms);
        } else if live_slot_ms.is_none() {
            live_slot_ms = Some(ms);
        }
    }
    ScheduleDue {
        missed_count,
        first_missed_ms,
        last_missed_ms,
        live_slot_ms,
    }
}

/// Evaluates missed and live schedule starts across all triggers in `spec` using `cursor`.
pub fn schedule_due(spec: &Spec, cursor: &ScheduleCursor, now_ms: u64) -> ScheduleDue {
    let schedules = unique_schedules(spec);
    if schedules.is_empty() {
        return ScheduleDue::default();
    }
    let live_cutoff = now_ms.saturating_sub(LIVE_GRACE_MS);
    let all_short = schedules.iter().all(|schedule| {
        let last_ms = cursor
            .anchors
            .iter()
            .find(|anchor| anchor.schedule == *schedule)
            .map_or(now_ms, |anchor| anchor.last_ms);
        now_ms.saturating_sub(last_ms) <= (DIRECT_EVAL_DAYS as u64) * 86_400_000
    });
    if all_short {
        let mut combined = BTreeSet::new();
        for schedule in &schedules {
            let last_ms = cursor
                .anchors
                .iter()
                .find(|anchor| anchor.schedule == *schedule)
                .map_or(now_ms, |anchor| anchor.last_ms);
            if last_ms >= now_ms || validate_schedule(schedule).is_err() {
                continue;
            }
            let Ok(zone) = parse_zone(&schedule.zone, "schedule trigger") else {
                continue;
            };
            let (Some(start_ts), Some(end_ts)) = (
                i64::try_from(last_ms)
                    .ok()
                    .and_then(|ms| Timestamp::from_millisecond(ms).ok()),
                i64::try_from(now_ms)
                    .ok()
                    .and_then(|ms| Timestamp::from_millisecond(ms).ok()),
            ) else {
                continue;
            };
            let start_day = date_to_days(zone.to_datetime(start_ts).date());
            let end_day = date_to_days(zone.to_datetime(end_ts).date());
            collect_day_range(
                schedule,
                &zone,
                start_day - 1,
                end_day + 1,
                last_ms,
                now_ms,
                &mut combined,
            );
        }
        return summarize_slots(&combined, live_cutoff);
    }

    let mut summary = ScheduleDue::default();
    for schedule in &schedules {
        let last_ms = cursor
            .anchors
            .iter()
            .find(|anchor| anchor.schedule == *schedule)
            .map_or(now_ms, |anchor| anchor.last_ms);
        let part = schedule_due_single(schedule, last_ms, now_ms);
        summary.missed_count = summary.missed_count.saturating_add(part.missed_count);
        if let Some(first) = part.first_missed_ms {
            summary.first_missed_ms = Some(
                summary
                    .first_missed_ms
                    .map_or(first, |prev| prev.min(first)),
            );
        }
        if let Some(last) = part.last_missed_ms {
            summary.last_missed_ms =
                Some(summary.last_missed_ms.map_or(last, |prev| prev.max(last)));
        }
        if let Some(live) = part.live_slot_ms {
            summary.live_slot_ms = Some(summary.live_slot_ms.map_or(live, |prev| prev.min(live)));
        }
    }
    summary
}

/// Next scheduled start strictly after `after_ms` across all schedule triggers in `spec`.
pub fn next_start(spec: &Spec, after_ms: u64) -> Option<u64> {
    spec.triggers
        .iter()
        .filter_map(|trigger| match trigger {
            Trigger::Schedule(schedule) => next_slot(schedule, after_ms),
            _ => None,
        })
        .min()
}

/// Next scheduled start strictly after `after_ms` for one schedule trigger.
pub fn next_slot(schedule: &ScheduleSpec, after_ms: u64) -> Option<u64> {
    validate_schedule(schedule).ok()?;
    let zone = parse_zone(&schedule.zone, "schedule trigger").ok()?;
    let timestamp = Timestamp::from_millisecond(i64::try_from(after_ms).ok()?).ok()?;
    let start_day = date_to_days(zone.to_datetime(timestamp).date());
    let mut candidates = BTreeSet::new();
    match schedule.every {
        ScheduleEvery::Day => {
            collect_day_range(
                schedule,
                &zone,
                start_day - 1,
                start_day + 2,
                after_ms,
                u64::MAX,
                &mut candidates,
            );
        }
        ScheduleEvery::Week => {
            collect_day_range(
                schedule,
                &zone,
                start_day - 1,
                start_day + 8,
                after_ms,
                u64::MAX,
                &mut candidates,
            );
        }
        ScheduleEvery::Hours => {
            collect_day_range(
                schedule,
                &zone,
                start_day - 1,
                start_day + 2,
                after_ms,
                u64::MAX,
                &mut candidates,
            );
        }
    }
    candidates.into_iter().next()
}

#[cfg(test)]
#[path = "schedule_tests.rs"]
mod tests;
