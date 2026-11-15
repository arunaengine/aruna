//! Tests dispatch: subjects from their triggers, a subject of another trigger, the run and its
//! settings, outputs, and the outcome of every kind of error.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::Deserialize;

use super::*;
use crate::bindings::aruna::plugin::types::{
    ErrorKind, Field, HostError, WebhookDelivery, WebhookRequest,
};
use crate::host::fake::install;
use crate::{fail, retry};

fn invocation(trigger: Trigger) -> Invocation {
    Invocation {
        run: "run-1".into(),
        install: "install-1".into(),
        group: "group-1".into(),
        settings: r#"{"limit": 3}"#.into(),
        trigger,
    }
}

fn manual(input: &str) -> Invocation {
    invocation(Trigger::Manual(input.into()))
}

#[test]
fn subjects_from_triggers() {
    let doubled = handle(manual(r#"{"n": 2}"#), |form: Form| {
        Ok(form.get::<u64>("n")? * 2)
    });
    assert_eq!(doubled, Ok(Some("4".into())));
    let tick = |tick: SystemTime| {
        Ok(tick
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis())
    };
    assert_eq!(
        handle(invocation(Trigger::Timer(1500)), tick),
        Ok(Some("1500".into()))
    );
    let delivery = WebhookDelivery {
        delivery_id: Some("d1".into()),
        request: WebhookRequest {
            method: "POST".into(),
            query: String::new(),
            headers: vec![Field {
                name: "X-Kind".into(),
                value: "push".into(),
            }],
            body: br#"{"ref": "main"}"#.to_vec(),
        },
    };
    let pushed = |request: Request| {
        let body: serde_json::Value = request.json()?;
        Ok(format!(
            "{} {} {}",
            request.header("x-kind").unwrap_or_default(),
            body["ref"],
            request.delivery_id.clone().unwrap_or_default()
        ))
    };
    let answered = handle(invocation(Trigger::Webhook(delivery)), pushed);
    assert_eq!(answered, Ok(Some(r#""push \"main\" d1""#.into())));
    let event = WatchEvent {
        kind: "sync_failed".into(),
        path: "raw/".into(),
        resource: "rel-1".into(),
        revision: "job-1".into(),
    };
    let synced = |sync: SyncEvent| Ok((sync.ok, sync.relationship, sync.job));
    let answered = handle(invocation(Trigger::Event(event)), synced);
    assert_eq!(answered, Ok(Some(r#"[false,"rel-1","job-1"]"#.into())));
}

#[test]
fn other_subject_refused() {
    let refused = handle(invocation(Trigger::Timer(1)), |_: Form| Ok(()));
    let failure = refused.unwrap_err();
    assert_eq!(failure.kind, FailureKind::InvalidInput);
    assert!(
        failure.message.contains("takes a form, not a `timer` run"),
        "{}",
        failure.message
    );
    let bad = handle(manual("[1]"), |_: Form| Ok(()));
    assert_eq!(bad.unwrap_err().kind, FailureKind::InvalidInput);
}

#[test]
fn run_and_settings() {
    #[derive(Deserialize)]
    struct Settings {
        limit: u32,
    }
    let seen = handle(manual("{}"), || {
        let settings: Settings = settings()?;
        Ok((run().id, run().install, settings.limit))
    });
    assert_eq!(seen, Ok(Some(r#"["run-1","install-1",3]"#.into())));
    assert_eq!(run(), Run::default(), "a run ends with its handler");
    assert_eq!(handle(manual(""), || Ok(())), Ok(None));
}

#[test]
fn error_outcomes() {
    let ended = |error: Error| {
        handle(manual("{}"), move || Err::<(), _>(error))
            .unwrap_err()
            .kind
    };
    assert_eq!(ended(retry("later")), FailureKind::Retryable);
    assert_eq!(
        ended(Error::Unavailable("busy".into())),
        FailureKind::Retryable
    );
    assert_eq!(ended(fail("no")), FailureKind::Permanent);
    assert_eq!(
        ended(Error::InvalidInput("x".into())),
        FailureKind::InvalidInput
    );
    let refused = HostError {
        kind: ErrorKind::NotGranted,
        message: "the install may not write datasets/run-7".into(),
    };
    let error = Error::from(refused);
    assert_eq!(
        error,
        Error::NotAllowed("the install may not write datasets/run-7".into())
    );
    assert_eq!(ended(error), FailureKind::Permanent);
    assert_eq!(unhandled("timer").kind, FailureKind::InvalidInput);
}

#[test]
fn log_lines() {
    let fake = install(&[]);
    crate::log!("{} done", 3);
    crate::log!(warn, "slow");
    assert_eq!(
        fake.calls(),
        ["log Level::Info 3 done", "log Level::Warn slow"]
    );
}
