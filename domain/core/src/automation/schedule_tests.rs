//! Tests bundled IANA schedule evaluation, DST shifts, midnight-spanning hours and long outages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use jiff::civil::{Date, DateTime};
use jiff::tz::TimeZone;

use super::*;
use crate::automation::record::Spec;

fn utc_ms(year: i16, month: i8, day: i8, hour: i8, minute: i8) -> u64 {
    let datetime = DateTime::new(year, month, day, hour, minute, 0, 0).unwrap();
    let timestamp = TimeZone::UTC.to_timestamp(datetime).unwrap();
    u64::try_from(timestamp.as_millisecond()).unwrap()
}

#[test]
fn validates_schedule_zones() {
    assert_eq!(ClockTime::parse("06:30"), ClockTime::new(6, 30));
    assert_eq!(ClockTime::parse("06:30").unwrap().to_string(), "06:30");
    assert!(ClockTime::parse("24:00").is_none());
    assert!(ClockTime::parse("6:30").is_none());

    let valid = ScheduleSpec {
        every: ScheduleEvery::Week,
        at: ClockTime::new(6, 0).unwrap(),
        days: vec![Weekday::Monday, Weekday::Friday],
        hours: None,
        zone: "Europe/Berlin".into(),
    };
    assert!(validate_schedule(&valid).is_ok());

    let mut bad_zone = valid.clone();
    bad_zone.zone = "Mars/Olympus".into();
    assert!(validate_schedule(&bad_zone).is_err());

    let mut blank_zone = valid;
    blank_zone.zone = "   ".into();
    assert!(validate_schedule(&blank_zone).is_err());
}

#[test]
fn converts_civil_days() {
    for (year, month, day, expected) in [
        (1970i16, 1i8, 1i8, Weekday::Thursday),
        (2000, 2, 29, Weekday::Tuesday),
        (2024, 2, 29, Weekday::Thursday),
        (2026, 3, 29, Weekday::Sunday),
        (2026, 11, 30, Weekday::Monday),
        (2100, 3, 1, Weekday::Monday),
    ] {
        let date = Date::new(year, month, day).unwrap();
        let day_num = day_index(date).unwrap();
        assert_eq!(date_for_day(day_num), Some(date));
        assert_eq!(Weekday::from_jiff(date.weekday()), expected);
    }
}

#[test]
fn handles_dst_transitions() {
    // Europe/Berlin springs forward on 2026-03-29 at 02:00 CET -> 03:00 CEST.
    let schedule = ScheduleSpec {
        every: ScheduleEvery::Day,
        at: ClockTime::new(2, 30).unwrap(),
        days: vec![],
        hours: None,
        zone: "Europe/Berlin".into(),
    };
    let after = utc_ms(2026, 3, 28, 23, 0);
    let now = utc_ms(2026, 3, 29, 1, 31);
    let spring = schedule_due_single(&schedule, after, now);
    assert_eq!(spring.missed_count, 0);
    assert_eq!(spring.live_slot_ms, Some(utc_ms(2026, 3, 29, 1, 30)));

    // Europe/Berlin falls back on 2026-10-25 at 03:00 CEST -> 02:00 CET (one occurrence).
    let after_fall = utc_ms(2026, 10, 24, 23, 0);
    let now_fall = utc_ms(2026, 10, 25, 0, 31);
    let fall = schedule_due_single(&schedule, after_fall, now_fall);
    assert_eq!(fall.missed_count, 0);
    assert_eq!(fall.live_slot_ms, Some(utc_ms(2026, 10, 25, 0, 30)));
    assert_eq!(
        next_slot(&schedule, utc_ms(2026, 10, 25, 0, 30)),
        Some(utc_ms(2026, 10, 26, 1, 30))
    );
}

#[test]
fn carries_hourly_midnight() {
    // Every 5 hours anchored at 00:00 UTC must stay exactly 5 hours apart across midnight.
    let schedule = ScheduleSpec {
        every: ScheduleEvery::Hours,
        at: ClockTime::new(0, 0).unwrap(),
        days: vec![],
        hours: Some(5),
        zone: "UTC".into(),
    };
    let mut current = utc_ms(2026, 6, 1, 18, 0);
    let mut fired = Vec::new();
    for _ in 0..4 {
        let next = next_slot(&schedule, current).unwrap();
        fired.push(next);
        current = next;
    }
    for pair in fired.windows(2) {
        assert_eq!(pair[1] - pair[0], 5 * 3_600_000);
    }
}

#[test]
fn summarizes_long_outage() {
    let schedule = ScheduleSpec {
        every: ScheduleEvery::Day,
        at: ClockTime::new(9, 0).unwrap(),
        days: vec![],
        hours: None,
        zone: "UTC".into(),
    };
    // 2025-01-01 09:00 UTC to 2027-01-01 09:01 UTC = 730 days (729 missed + 1 live).
    let after = utc_ms(2025, 1, 1, 9, 0);
    let now = utc_ms(2027, 1, 1, 9, 1);
    let due = schedule_due_single(&schedule, after, now);
    assert_eq!(due.missed_count, 729);
    assert_eq!(due.first_missed_ms, Some(utc_ms(2025, 1, 2, 9, 0)));
    assert_eq!(due.last_missed_ms, Some(utc_ms(2026, 12, 31, 9, 0)));
    assert_eq!(due.live_slot_ms, Some(utc_ms(2027, 1, 1, 9, 0)));

    // Weekly schedule across a full year (52 Mondays missed + 1 live Monday).
    let weekly = ScheduleSpec {
        every: ScheduleEvery::Week,
        at: ClockTime::new(9, 0).unwrap(),
        days: vec![Weekday::Monday],
        hours: None,
        zone: "UTC".into(),
    };
    let after_mon = utc_ms(2025, 1, 6, 9, 0);
    let now_mon = utc_ms(2026, 1, 5, 9, 1);
    let weekly_due = schedule_due_single(&weekly, after_mon, now_mon);
    assert_eq!(weekly_due.missed_count, 51);
    assert_eq!(weekly_due.first_missed_ms, Some(utc_ms(2025, 1, 13, 9, 0)));
    assert_eq!(weekly_due.last_missed_ms, Some(utc_ms(2025, 12, 29, 9, 0)));
    assert_eq!(weekly_due.live_slot_ms, Some(utc_ms(2026, 1, 5, 9, 0)));
}

#[test]
fn preserves_active_anchors() {
    let first = ScheduleSpec {
        every: ScheduleEvery::Day,
        at: ClockTime::new(8, 0).unwrap(),
        days: vec![],
        hours: None,
        zone: "UTC".into(),
    };
    let second = ScheduleSpec {
        every: ScheduleEvery::Day,
        at: ClockTime::new(9, 0).unwrap(),
        days: vec![],
        hours: None,
        zone: "UTC".into(),
    };
    let mut spec = Spec {
        triggers: vec![Trigger::Schedule(first.clone())],
        ..Spec::default()
    };
    let seeded =
        ScheduleCursor::for_spec(&spec, None, CursorMode::Reset, utc_ms(2026, 6, 1, 7, 0)).unwrap();
    assert_eq!(seeded.due_ms, utc_ms(2026, 6, 1, 8, 0));

    // Approving an edit that adds a second schedule at 08:30 preserves the due 08:00 anchor.
    spec.triggers.push(Trigger::Schedule(second.clone()));
    let updated = ScheduleCursor::for_spec(
        &spec,
        Some(&seeded),
        CursorMode::Preserve,
        utc_ms(2026, 6, 1, 8, 30),
    )
    .unwrap();
    assert_eq!(updated.due_ms, utc_ms(2026, 6, 1, 8, 0));
    assert_eq!(updated.anchor_for(&second, 0), utc_ms(2026, 6, 1, 8, 30));
}

fn step_expected_due(spec: &Spec, after_ms: u64, now_ms: u64) -> ScheduleDue {
    let live_cutoff = now_ms.saturating_sub(LIVE_GRACE_MS);
    let mut cursor_ms = after_ms;
    let mut missed_count = 0u64;
    let mut first_missed_ms = None;
    let mut last_missed_ms = None;
    let mut live_slot_ms = None;
    while let Some(slot_ms) = next_start(spec, cursor_ms) {
        if slot_ms > now_ms {
            break;
        }
        if slot_ms < live_cutoff {
            missed_count += 1;
            if first_missed_ms.is_none() {
                first_missed_ms = Some(slot_ms);
            }
            last_missed_ms = Some(slot_ms);
        } else if live_slot_ms.is_none() {
            live_slot_ms = Some(slot_ms);
        }
        cursor_ms = slot_ms;
    }
    ScheduleDue {
        missed_count,
        first_missed_ms,
        last_missed_ms,
        live_slot_ms,
    }
}

#[test]
fn deduplicates_long_outages() {
    // Daily 09:00 UTC + Monday 09:00 UTC across 30 days (> DIRECT_EVAL_DAYS): 30 distinct misses.
    let daily = ScheduleSpec {
        every: ScheduleEvery::Day,
        at: ClockTime::new(9, 0).unwrap(),
        days: vec![],
        hours: None,
        zone: "UTC".into(),
    };
    let monday = ScheduleSpec {
        every: ScheduleEvery::Week,
        at: ClockTime::new(9, 0).unwrap(),
        days: vec![Weekday::Monday],
        hours: None,
        zone: "UTC".into(),
    };
    let multi_spec = Spec {
        triggers: vec![Trigger::Schedule(daily), Trigger::Schedule(monday)],
        ..Spec::default()
    };
    let start_ms = utc_ms(2026, 3, 1, 0, 0);
    let end_ms = utc_ms(2026, 3, 31, 0, 0);
    let cursor = ScheduleCursor::for_spec(&multi_spec, None, CursorMode::Reset, start_ms).unwrap();
    let expected_multi = step_expected_due(&multi_spec, start_ms, end_ms);
    let actual_multi = schedule_due(&multi_spec, &cursor, end_ms);
    assert_eq!(expected_multi.missed_count, 30);
    assert_eq!(actual_multi, expected_multi);

    // Hourly Europe/Berlin across spring-forward (61 days): 1,463 distinct instants, not 1,464.
    let berlin_hourly = ScheduleSpec {
        every: ScheduleEvery::Hours,
        at: ClockTime::new(0, 0).unwrap(),
        days: vec![],
        hours: Some(1),
        zone: "Europe/Berlin".into(),
    };
    let berlin_spec = Spec {
        triggers: vec![Trigger::Schedule(berlin_hourly.clone())],
        ..Spec::default()
    };
    let spring_start = utc_ms(2026, 3, 1, 0, 0);
    let spring_end = utc_ms(2026, 5, 1, 0, 0);
    let spring_cursor =
        ScheduleCursor::for_spec(&berlin_spec, None, CursorMode::Reset, spring_start).unwrap();
    let expected_spring = step_expected_due(&berlin_spec, spring_start, spring_end);
    assert_eq!(expected_spring.missed_count, 1_463);
    assert_eq!(
        schedule_due(&berlin_spec, &spring_cursor, spring_end),
        expected_spring
    );
    assert_eq!(
        schedule_due_single(&berlin_hourly, spring_start, spring_end),
        expected_spring
    );
}
