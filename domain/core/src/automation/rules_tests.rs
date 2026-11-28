//! Tests start condition evaluation, unknown upload sizes, Any mode and order-independent widening.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use ulid::Ulid;

use super::*;
use crate::automation::firing::TriggerPayload;
use crate::automation::schedule::{ClockTime, Weekday};

fn utc_ms(year: i16, month: i8, day: i8, hour: i8, minute: i8) -> u64 {
    let datetime = DateTime::new(year, month, day, hour, minute, 0, 0).unwrap();
    let timestamp = TimeZone::UTC.to_timestamp(datetime).unwrap();
    u64::try_from(timestamp.as_millisecond()).unwrap()
}

#[test]
fn validates_condition_ranges() {
    let missing_zone = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::Weekdays {
            days: vec![Weekday::Monday],
            zone: String::new(),
        }],
    };
    assert!(validate_conditions(&missing_zone).is_err());

    let same_bounds = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::TimeOfDay {
            from: ClockTime::new(8, 0).unwrap(),
            to: ClockTime::new(8, 0).unwrap(),
            zone: "UTC".into(),
        }],
    };
    assert!(validate_conditions(&same_bounds).is_err());

    assert_eq!(format_bytes(512), "512 B");
    assert_eq!(format_bytes(1024), "1.0 KiB");
    assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
    assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GiB");
}

#[test]
fn evaluates_unknown_size() {
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
                from: ClockTime::new(22, 0).unwrap(),
                to: ClockTime::new(6, 0).unwrap(),
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
        size_bytes: Some(1024),
    };
    assert!(evaluate_conditions(&conditions, now, &upload).ok);

    let too_small = TriggerPayload::DataUploaded {
        bucket: "raw".into(),
        key: "run1/sample.fastq.gz".into(),
        version_id: Ulid::nil(),
        path: "/r/g/raw/run1/sample.fastq.gz".into(),
        size_bytes: Some(50),
    };
    let outcome = evaluate_conditions(&conditions, now, &too_small);
    assert!(!outcome.ok);
    assert!(outcome.reason.unwrap().contains("less than"));

    // Unknown upload size fails both min_bytes and max_bytes conditions instead of acting as zero.
    let unknown_size = TriggerPayload::DataUploaded {
        bucket: "raw".into(),
        key: "run1/sample.fastq.gz".into(),
        version_id: Ulid::nil(),
        path: "/r/g/raw/run1/sample.fastq.gz".into(),
        size_bytes: None,
    };
    let max_only = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::File {
            pattern: None,
            min_bytes: None,
            max_bytes: Some(5000),
        }],
    };
    let unavailable = evaluate_conditions(&max_only, now, &unknown_size);
    assert!(!unavailable.ok);
    assert!(unavailable.reason.unwrap().contains("unavailable"));

    // Name-only file condition still holds even when size_bytes is unavailable.
    let name_only = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::File {
            pattern: Some("*.fastq.gz".into()),
            min_bytes: None,
            max_bytes: None,
        }],
    };
    assert!(evaluate_conditions(&name_only, now, &unknown_size).ok);

    // Non-empty Any refuses starts when no condition applies (e.g. file-only on manual/schedule).
    let any_file_only = Conditions {
        mode: ConditionMode::Any,
        items: vec![Condition::File {
            pattern: Some("*.bam".into()),
            min_bytes: None,
            max_bytes: None,
        }],
    };
    let schedule_payload = TriggerPayload::Schedule { at_ms: now };
    let no_match = evaluate_conditions(&any_file_only, now, &schedule_payload);
    assert!(!no_match.ok);
    assert!(no_match.reason.unwrap().contains("applied"));

    // Mixed Any still ignores non-applicable file checks when weekday fails.
    let any_mixed = Conditions {
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
    assert!(!evaluate_conditions(&any_mixed, now, &schedule_payload).ok);
}

#[test]
fn checks_condition_widening() {
    let monday = Condition::Weekdays {
        days: vec![Weekday::Monday],
        zone: "UTC".into(),
    };
    let daytime = Condition::TimeOfDay {
        from: ClockTime::new(8, 0).unwrap(),
        to: ClockTime::new(18, 0).unwrap(),
        zone: "UTC".into(),
    };
    let file_only = Condition::File {
        pattern: Some("*.csv".into()),
        min_bytes: None,
        max_bytes: None,
    };

    let empty = Conditions::default();
    let all_one = Conditions {
        mode: ConditionMode::All,
        items: vec![monday.clone()],
    };
    let all_two = Conditions {
        mode: ConditionMode::All,
        items: vec![monday.clone(), daytime.clone()],
    };
    let all_two_reordered = Conditions {
        mode: ConditionMode::All,
        items: vec![daytime.clone(), monday.clone()],
    };
    let any_two = Conditions {
        mode: ConditionMode::Any,
        items: vec![monday.clone(), daytime.clone()],
    };
    let any_one = Conditions {
        mode: ConditionMode::Any,
        items: vec![monday.clone()],
    };

    assert!(!conditions_widen(&empty, &all_one));
    assert!(!conditions_widen(&all_one, &all_two));
    assert!(!conditions_widen(&all_two, &all_two_reordered));
    assert!(conditions_widen(&all_two, &all_one));
    assert!(conditions_widen(&all_one, &empty));
    assert!(conditions_widen(&all_two, &any_two));

    // Narrowing Any([A, B]) -> Any([A]) or Any([A, B]) -> All([A, B]) does not widen.
    assert!(!conditions_widen(&any_two, &any_one));
    assert!(conditions_widen(&any_one, &any_two));
    assert!(!conditions_widen(&any_two, &all_two));
    assert!(!conditions_widen(&all_one, &any_one));

    // Switching Any([File]) -> All([File]) widens non-file starts and must require approval.
    let any_file = Conditions {
        mode: ConditionMode::Any,
        items: vec![file_only.clone()],
    };
    let all_file = Conditions {
        mode: ConditionMode::All,
        items: vec![file_only],
    };
    assert!(conditions_widen(&any_file, &all_file));
    assert!(!conditions_widen(&all_file, &any_file));
}
