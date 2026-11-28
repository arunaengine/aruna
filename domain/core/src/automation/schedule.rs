//! Recurring wall-clock schedule triggers evaluated purely against bundled IANA time zones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::fmt;
use std::sync::LazyLock;

use jiff::civil::{Date, DateTime, Weekday as JiffDay};
use jiff::tz::{TimeZone, TimeZoneDatabase};
use jiff::{Span, Timestamp};
use serde::{Deserialize, Serialize};

use super::record::{Spec, Trigger};
use crate::errors::ConversionError;

/// Grace window for firing a due schedule slot live rather than recording it as missed.
pub const LIVE_GRACE_MS: u64 = 120_000;

const DAY_MS: u64 = 86_400_000;
const DIRECT_DAYS: i64 = 21;
const TRANSITION_MARGIN_MS: u64 = 2 * DAY_MS;
const MAX_PERIOD_DAYS: u64 = 4096;
const EPOCH_DATE: Date = Date::constant(1970, 1, 1);

static BUNDLED_ZONES: LazyLock<TimeZoneDatabase> = LazyLock::new(TimeZoneDatabase::bundled);

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

/// How [`ScheduleCursor::for_spec`] initializes anchors when rebuilding a cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorMode {
    /// Starts every schedule fresh from `now_ms` (activation or resume).
    Reset,
    /// Preserves anchors of unchanged schedules on an already-active automation.
    Preserve,
}

/// Persisted due-time cursor for one active scheduled automation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleCursor {
    /// Earliest next scheduled instant strictly after each anchor's `last_ms`.
    pub due_ms: u64,
    pub anchors: Vec<ScheduleAnchor>,
}

impl ScheduleCursor {
    /// Returns the recorded anchor instant for `schedule`, or `fallback_ms` if absent.
    pub fn anchor_for(&self, schedule: &ScheduleSpec, fallback_ms: u64) -> u64 {
        self.anchors
            .iter()
            .find(|anchor| anchor.schedule == *schedule)
            .map_or(fallback_ms, |anchor| anchor.last_ms)
    }

    /// Builds or updates the schedule cursor for `spec`, preserving existing per-schedule anchors
    /// under [`CursorMode::Preserve`] so unrelated edits never erase already-due slots.
    pub fn for_spec(
        spec: &Spec,
        existing: Option<&Self>,
        mode: CursorMode,
        now_ms: u64,
    ) -> Option<Self> {
        let schedules = unique_schedules(spec);
        if schedules.is_empty() {
            return None;
        }
        let mut anchors = Vec::with_capacity(schedules.len());
        for schedule in schedules {
            let last_ms = match mode {
                CursorMode::Preserve => existing
                    .map(|cursor| cursor.anchor_for(&schedule, now_ms))
                    .unwrap_or(now_ms),
                CursorMode::Reset => now_ms,
            };
            anchors.push(ScheduleAnchor { schedule, last_ms });
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
            let previous = self.anchor_for(&schedule, now_ms);
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
    BUNDLED_ZONES
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

fn day_index(date: Date) -> Option<i64> {
    EPOCH_DATE
        .until(date)
        .ok()
        .map(|span| i64::from(span.get_days()))
}

fn date_for_day(day: i64) -> Option<Date> {
    let span = Span::new().try_days(day).ok()?;
    EPOCH_DATE.checked_add(span).ok()
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
                if let Some(date) = date_for_day(day)
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
                if let Some(date) = date_for_day(day)
                    && schedule.days.contains(&Weekday::from_jiff(date.weekday()))
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
                if let Some(date) = date_for_day(day)
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

struct BoundSchedule<'a> {
    schedule: &'a ScheduleSpec,
    zone: TimeZone,
    last_ms: u64,
}

fn greatest_divisor(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let next = left % right;
        left = right;
        right = next;
    }
    left
}

fn least_multiple(left: u64, right: u64, cap: u64) -> Option<u64> {
    let divisor = greatest_divisor(left, right);
    let scaled = (left / divisor).checked_mul(right)?;
    (scaled <= cap).then_some(scaled)
}

fn schedule_period_days(schedule: &ScheduleSpec) -> u64 {
    match schedule.every {
        ScheduleEvery::Day => 1,
        ScheduleEvery::Week => 7,
        ScheduleEvery::Hours => {
            let hours = u64::from(schedule.hours.unwrap_or(1));
            hours / greatest_divisor(hours, 24)
        }
    }
}

fn collect_window(
    items: &[BoundSchedule<'_>],
    window_start: u64,
    window_end: u64,
    out: &mut BTreeSet<u64>,
) {
    if window_start > window_end {
        return;
    }
    let (Some(start_instant), Some(end_instant)) = (
        i64::try_from(window_start)
            .ok()
            .and_then(|ms| Timestamp::from_millisecond(ms).ok()),
        i64::try_from(window_end)
            .ok()
            .and_then(|ms| Timestamp::from_millisecond(ms).ok()),
    ) else {
        return;
    };
    for item in items {
        if item.last_ms >= window_end {
            continue;
        }
        let after_ms = window_start.saturating_sub(1).max(item.last_ms);
        let Some(start_day) = day_index(item.zone.to_datetime(start_instant).date()) else {
            continue;
        };
        let Some(end_day) = day_index(item.zone.to_datetime(end_instant).date()) else {
            continue;
        };
        collect_day_range(
            item.schedule,
            &item.zone,
            start_day - 1,
            end_day + 1,
            after_ms,
            window_end,
            out,
        );
    }
}

fn merge_windows(mut raw: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    if raw.is_empty() {
        return raw;
    }
    raw.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(raw.len());
    for (start, end) in raw {
        if let Some((_, earlier_end)) = merged.last_mut()
            && start <= earlier_end.saturating_add(1)
        {
            *earlier_end = (*earlier_end).max(end);
        } else {
            merged.push((start, end));
        }
    }
    merged
}

fn add_missed_part(summary: &mut ScheduleDue, count: u64, first: Option<u64>, last: Option<u64>) {
    if count == 0 {
        return;
    }
    summary.missed_count = summary.missed_count.saturating_add(count);
    if let Some(instant) = first {
        summary.first_missed_ms = Some(
            summary
                .first_missed_ms
                .map_or(instant, |earlier| earlier.min(instant)),
        );
    }
    if let Some(instant) = last {
        summary.last_missed_ms = Some(
            summary
                .last_missed_ms
                .map_or(instant, |earlier| earlier.max(instant)),
        );
    }
}

fn count_constant_gap(
    items: &[BoundSchedule<'_>],
    gap_start: u64,
    gap_end: u64,
    summary: &mut ScheduleDue,
) {
    if gap_start > gap_end {
        return;
    }
    let active: Vec<BoundSchedule<'_>> = items
        .iter()
        .filter(|item| item.last_ms < gap_start)
        .map(|item| BoundSchedule {
            schedule: item.schedule,
            zone: item.zone.clone(),
            last_ms: item.last_ms,
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let period_days = active.iter().try_fold(1u64, |combined, item| {
        least_multiple(
            combined,
            schedule_period_days(item.schedule),
            MAX_PERIOD_DAYS,
        )
    });
    let span_ms = gap_end - gap_start + 1;
    if let Some(days) = period_days
        && let Some(period_ms) = days.checked_mul(DAY_MS)
        && span_ms >= period_ms
    {
        let full_periods = span_ms / period_ms;
        let mut period_slots = BTreeSet::new();
        collect_window(
            &active,
            gap_start,
            gap_start + period_ms - 1,
            &mut period_slots,
        );
        let full_count = full_periods.saturating_mul(period_slots.len() as u64);
        let full_first = period_slots.first().copied();
        let full_last = period_slots
            .last()
            .copied()
            .and_then(|ms| (full_periods - 1).checked_mul(period_ms)?.checked_add(ms));
        add_missed_part(summary, full_count, full_first, full_last);

        let remainder_start = gap_start + full_periods * period_ms;
        if remainder_start <= gap_end {
            let mut remainder_slots = BTreeSet::new();
            collect_window(&active, remainder_start, gap_end, &mut remainder_slots);
            add_missed_part(
                summary,
                remainder_slots.len() as u64,
                remainder_slots.first().copied(),
                remainder_slots.last().copied(),
            );
        }
        return;
    }
    let chunk_ms = MAX_PERIOD_DAYS.saturating_mul(DAY_MS);
    let mut cursor_ms = gap_start;
    while cursor_ms <= gap_end {
        let chunk_end = cursor_ms
            .saturating_add(chunk_ms.saturating_sub(1))
            .min(gap_end);
        let mut chunk_slots = BTreeSet::new();
        collect_window(&active, cursor_ms, chunk_end, &mut chunk_slots);
        add_missed_part(
            summary,
            chunk_slots.len() as u64,
            chunk_slots.first().copied(),
            chunk_slots.last().copied(),
        );
        if chunk_end == gap_end {
            break;
        }
        cursor_ms = chunk_end.saturating_add(1);
    }
}

fn evaluate_due(items: &[BoundSchedule<'_>], now_ms: u64) -> ScheduleDue {
    if items.is_empty() {
        return ScheduleDue::default();
    }
    let live_cutoff = now_ms.saturating_sub(LIVE_GRACE_MS);
    let all_short = items
        .iter()
        .all(|item| now_ms.saturating_sub(item.last_ms) <= (DIRECT_DAYS as u64) * DAY_MS);
    if all_short {
        let mut combined = BTreeSet::new();
        let min_start = items
            .iter()
            .map(|item| item.last_ms)
            .min()
            .unwrap_or(now_ms);
        if min_start < now_ms {
            collect_window(items, min_start.saturating_add(1), now_ms, &mut combined);
        }
        return summarize_slots(&combined, live_cutoff);
    }

    let mut summary = ScheduleDue::default();
    let mut live_slots = BTreeSet::new();
    if live_cutoff <= now_ms {
        collect_window(items, live_cutoff.max(1), now_ms, &mut live_slots);
        summary.live_slot_ms = live_slots.first().copied();
    }
    if live_cutoff == 0 {
        return summary;
    }
    let missed_end = live_cutoff - 1;
    let Some(min_start) = items
        .iter()
        .filter(|item| item.last_ms < live_cutoff)
        .map(|item| item.last_ms)
        .min()
    else {
        return summary;
    };
    let range_start = min_start.saturating_add(1);
    if range_start > missed_end {
        return summary;
    }

    let mut raw_windows = Vec::new();
    let mut push_around = |point: u64| {
        let low = point.saturating_sub(TRANSITION_MARGIN_MS).max(range_start);
        let high = point.saturating_add(TRANSITION_MARGIN_MS).min(missed_end);
        if low <= high {
            raw_windows.push((low, high));
        }
    };
    push_around(min_start);
    push_around(missed_end);
    let mut seen_zones = BTreeSet::new();
    let scan_end_ms = missed_end.saturating_add(TRANSITION_MARGIN_MS);
    for item in items {
        if item.last_ms < live_cutoff {
            push_around(item.last_ms);
        }
        if !seen_zones.insert(item.schedule.zone.trim()) {
            continue;
        }
        let scan_start_ms = min_start.saturating_sub(TRANSITION_MARGIN_MS);
        let Ok(start_i64) = i64::try_from(scan_start_ms) else {
            continue;
        };
        let Ok(start_instant) = Timestamp::from_millisecond(start_i64) else {
            continue;
        };
        for transition in item.zone.following(start_instant) {
            let Ok(ms) = u64::try_from(transition.timestamp().as_millisecond()) else {
                break;
            };
            if ms > scan_end_ms {
                break;
            }
            push_around(ms);
        }
    }

    let exact_windows = merge_windows(raw_windows);
    let mut earlier_end = range_start - 1;
    for (window_start, window_end) in exact_windows {
        if earlier_end.saturating_add(1) < window_start {
            count_constant_gap(
                items,
                earlier_end.saturating_add(1),
                window_start - 1,
                &mut summary,
            );
        }
        let mut exact_slots = BTreeSet::new();
        collect_window(items, window_start, window_end, &mut exact_slots);
        add_missed_part(
            &mut summary,
            exact_slots.len() as u64,
            exact_slots.first().copied(),
            exact_slots.last().copied(),
        );
        earlier_end = window_end;
    }
    if earlier_end < missed_end {
        count_constant_gap(
            items,
            earlier_end.saturating_add(1),
            missed_end,
            &mut summary,
        );
    }
    summary
}

/// Evaluates missed and live slots in `(after_ms, now_ms]` for one schedule across all transitions.
pub fn schedule_due_single(schedule: &ScheduleSpec, after_ms: u64, now_ms: u64) -> ScheduleDue {
    if after_ms >= now_ms || validate_schedule(schedule).is_err() {
        return ScheduleDue::default();
    }
    let Ok(zone) = parse_zone(&schedule.zone, "schedule trigger") else {
        return ScheduleDue::default();
    };
    let items = [BoundSchedule {
        schedule,
        zone,
        last_ms: after_ms,
    }];
    evaluate_due(&items, now_ms)
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
    let items: Vec<BoundSchedule<'_>> = schedules
        .iter()
        .filter_map(|schedule| {
            let last_ms = cursor.anchor_for(schedule, now_ms);
            if last_ms >= now_ms || validate_schedule(schedule).is_err() {
                return None;
            }
            let zone = parse_zone(&schedule.zone, "schedule trigger").ok()?;
            Some(BoundSchedule {
                schedule,
                zone,
                last_ms,
            })
        })
        .collect();
    evaluate_due(&items, now_ms)
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
    let start_day = day_index(zone.to_datetime(timestamp).date())?;
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
