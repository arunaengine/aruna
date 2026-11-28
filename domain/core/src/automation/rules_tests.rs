//! Tests schedule zones, DST transitions, start condition evaluation and widening checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use ulid::Ulid;

use super::*;
use crate::automation::firing::TriggerPayload;

fn utc_ms(year: i16, month: i8, day: i8, hour: i8, minute: i8) -> u64 {
    let datetime = DateTime::new(year, month, day, hour, minute, 0, 0).unwrap();
    let timestamp = TimeZone::UTC.to_timestamp(datetime).unwrap();
    u64::try_from(timestamp.as_millisecond()).unwrap()
}

#[test]
fn validates_zones() {
    let valid = ScheduleSpec {
        every: ScheduleEvery::Week,
        at: "06:00".into(),
        days: vec![Weekday::Monday, Weekday::Friday],
        hours: None,
        zone: "Europe/Berlin".into(),
    };
    assert!(validate_schedule(&valid).is_ok());

    let mut bad_zone = valid.clone();
    bad_zone.zone = "Mars/Olympus".into();
    assert!(validate_schedule(&bad_zone).is_err());

    let missing_zone = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::Weekdays {
            days: vec![Weekday::Monday],
            zone: String::new(),
        }],
    };
    assert!(validate_conditions(&missing_zone).is_err());
}

#[test]
fn handles_dst_transitions() {
    // Europe/Berlin springs forward on 2026-03-29 at 02:00 CET -> 03:00 CEST.
    let schedule = ScheduleSpec {
        every: ScheduleEvery::Day,
        at: "02:30".into(),
        days: vec![],
        hours: None,
        zone: "Europe/Berlin".into(),
    };
    let after = utc_ms(2026, 3, 28, 23, 0);
    let now = utc_ms(2026, 3, 29, 6, 0);
    let slots = due_slots(&schedule, after, now);
    assert_eq!(slots.len(), 1);
    assert_eq!(slots[0], utc_ms(2026, 3, 29, 1, 30));

    // Europe/Berlin falls back on 2026-10-25 at 03:00 CEST -> 02:00 CET (one occurrence).
    let after_fall = utc_ms(2026, 10, 24, 23, 0);
    let now_fall = utc_ms(2026, 10, 25, 6, 0);
    let fall_slots = due_slots(&schedule, after_fall, now_fall);
    assert_eq!(fall_slots.len(), 1);
    assert_eq!(fall_slots[0], utc_ms(2026, 10, 25, 0, 30));
}

#[test]
fn evaluates_conditions() {
    // 2026-11-30 00:30 Europe/Berlin is 2026-11-29 23:30 UTC (Sunday UTC, Monday Berlin).
    let now = utc_ms(2026, 11, 29, 23, 30);
    let conditions = Conditions {
        mode: ConditionMode::All,
        items: vec![
            Condition::Weekdays {
                days: vec![Weekday::Monday],
                zone: "Europe/Berlin".into(),
            },
            Condition::TimeOfDay {
                from: "22:00".into(),
                to: "06:00".into(),
                zone: "Europe/Berlin".into(),
            },
            Condition::File {
                pattern: Some("*.fastq.gz".into()),
                min_bytes: Some(100),
                max_bytes: Some(5000),
            },
        ],
    };
    let upload = TriggerPayload::DataUploaded {
        bucket: "raw".into(),
        key: "run1/sample.fastq.gz".into(),
        version_id: Ulid::nil(),
        path: "/r/g/raw/run1/sample.fastq.gz".into(),
        size_bytes: 1024,
    };
    assert!(evaluate_conditions(&conditions, now, &upload).ok);

    let too_small = TriggerPayload::DataUploaded {
        bucket: "raw".into(),
        key: "run1/sample.fastq.gz".into(),
        version_id: Ulid::nil(),
        path: "/r/g/raw/run1/sample.fastq.gz".into(),
        size_bytes: 50,
    };
    let outcome = evaluate_conditions(&conditions, now, &too_small);
    assert!(!outcome.ok);
    assert!(outcome.reason.unwrap().contains("less than"));

    // Under Any mode, a non-applicable File condition never satisfies a failed Weekday check.
    let any_conditions = Conditions {
        mode: ConditionMode::Any,
        items: vec![
            Condition::Weekdays {
                days: vec![Weekday::Friday],
                zone: "Europe/Berlin".into(),
            },
            Condition::File {
                pattern: Some("*.bam".into()),
                min_bytes: None,
                max_bytes: None,
            },
        ],
    };
    let schedule_payload = TriggerPayload::Schedule { at_ms: now };
    assert!(!evaluate_conditions(&any_conditions, now, &schedule_payload).ok);
}

#[test]
fn checks_condition_widening() {
    let empty = Conditions::default();
    let one = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::Weekdays {
            days: vec![Weekday::Monday],
            zone: "UTC".into(),
        }],
    };
    let two = Conditions {
        mode: ConditionMode::All,
        items: vec![
            Condition::Weekdays {
                days: vec![Weekday::Monday],
                zone: "UTC".into(),
            },
            Condition::TimeOfDay {
                from: "08:00".into(),
                to: "18:00".into(),
                zone: "UTC".into(),
            },
        ],
    };
    let any_two = Conditions {
        mode: ConditionMode::Any,
        items: two.items.clone(),
    };
    assert!(!conditions_widen(&empty, &one));
    assert!(!conditions_widen(&one, &two));
    assert!(conditions_widen(&two, &one));
    assert!(conditions_widen(&one, &empty));
    assert!(conditions_widen(&two, &any_two));
}
