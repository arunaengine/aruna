//! Tests state, services and secrets: writes at the version read, a new key, a conflict retried
//! by `update`, removal, whole answers, a status outside 2xx, and readable secrets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::{Value, json};

use super::*;
use crate::host::fake::install;
use crate::{Method, secret, service};

#[test]
fn versions_read_written() {
    let fake = install(&[]);
    assert_eq!(get::<u32>("cursor").unwrap(), None);
    set("cursor", &1).unwrap();
    assert_eq!(get::<u32>("cursor").unwrap(), Some(1));
    set("cursor", &2).unwrap();
    // Another run wrote meanwhile: a write at the version this run read conflicts.
    fake.0
        .borrow_mut()
        .state
        .insert("cursor".into(), (99, b"7".to_vec()));
    assert!(matches!(set("cursor", &3), Err(Error::Conflict(_))));
    forget();
    assert!(
        matches!(set("cursor", &3), Err(Error::Conflict(_))),
        "an unread key is new"
    );
    let counted = update("seen", 0, |seen: u32| seen + 1).unwrap();
    assert_eq!(counted, 1);
    remove("cursor").unwrap();
    assert_eq!(get::<u32>("cursor").unwrap(), None);
    remove("cursor").unwrap();
}

#[test]
fn update_retries_conflicts() {
    let fake = install(&[]);
    set("list", &json!([])).unwrap();
    let mut first = true;
    let written = update("list", json!([]), |mut list: Value| {
        if std::mem::take(&mut first) {
            // Another run changes the value between this read and the write.
            fake.0
                .borrow_mut()
                .state
                .insert("list".into(), (50, b"[\"other\"]".to_vec()));
        }
        list.as_array_mut().unwrap().push("mine".into());
        list
    });
    assert_eq!(written.unwrap(), json!(["other", "mine"]));
}

#[test]
fn services_answer_whole() {
    let fake = install(&[]);
    fake.0
        .borrow_mut()
        .replies
        .insert(String::new(), (201, br#"{"id": "c-1"}"#.to_vec()));
    fake.0
        .borrow_mut()
        .replies
        .insert("down".into(), (500, b"broken".to_vec()));
    let catalog = service("catalog");
    let reply = catalog.post(&json!({ "title": "Run 7" })).unwrap();
    assert_eq!(
        (reply.status, reply.json::<Value>().unwrap()["id"].clone()),
        (201, json!("c-1"))
    );
    assert_eq!(reply.header("content-type"), Some("application/json"));
    assert!(
        fake.calls()[0].ends_with(r#"Post  {"title":"Run 7"}"#),
        "{:?}",
        fake.calls()
    );
    let failed = catalog.get("down");
    assert_eq!(
        failed,
        Err(Error::Service {
            status: 500,
            message: "broken".into()
        })
    );
    let unchecked = catalog
        .request(Method::Get)
        .path("down")
        .check(false)
        .send();
    assert_eq!(unchecked.unwrap().status, 500);
}

#[test]
fn readable_secrets() {
    let fake = install(&[]);
    fake.0
        .borrow_mut()
        .secrets
        .insert("signing".into(), vec![("key".into(), "s3cret".into())]);
    assert_eq!(secret("signing").unwrap()["key"], "s3cret");
    assert!(matches!(secret("other"), Err(Error::NotAllowed(_))));
}
