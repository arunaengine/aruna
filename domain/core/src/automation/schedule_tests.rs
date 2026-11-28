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
        (1970i16, 1u8, 1u8, Weekday::Thursday),
        (2000, 2, 29, Weekday::Tuesday),
        (2024, 2, 29, Weekday::Thursday),
        (2026, 3, 29, Weekday::Sunday),
        (2026, 11, 30, Weekday::Monday),
        (2100, 3, 1, Weekday::Monday),
    ] {
        let days = days_from_civil(i32::from(year), u32::from(month), u32::from(day));
        assert_eq!(
            civil_from_days(days),
            Some(Date::new(year, month as i8, day as i8).unwrap())
        );
        assert_eq!(weekday_for_days(days), expected);
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
    let seeded = ScheduleCursor::for_spec(&spec, None, false, utc_ms(2026, 6, 1, 7, 0)).unwrap();
    assert_eq!(seeded.due_ms, utc_ms(2026, 6, 1, 8, 0));

    // Approving an edit that adds a second schedule at 08:30 preserves the due 08:00 anchor.
    spec.triggers.push(Trigger::Schedule(second.clone()));
    let updated =
        ScheduleCursor::for_spec(&spec, Some(&seeded), true, utc_ms(2026, 6, 1, 8, 30)).unwrap();
    assert_eq!(updated.due_ms, utc_ms(2026, 6, 1, 8, 0));
    let second_anchor = updated
        .anchors
        .iter()
        .find(|anchor| anchor.schedule == second)
        .unwrap();
    assert_eq!(second_anchor.last_ms, utc_ms(2026, 6, 1, 8, 30));
}
