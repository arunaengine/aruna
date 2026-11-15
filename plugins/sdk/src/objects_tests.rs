//! Tests objects: one by path, listings across pages, small reads of the version given, a marked
//! version refused, and objects as the subject of an upload or a form field.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::json;

use super::*;
use crate::bindings::aruna::plugin::types::{Invocation, Trigger};
use crate::host::fake::install;
use crate::{Form, WatchEvent, handle};

#[test]
fn get_and_read() {
    let fake = install(&[("raw/a.json", br#"{"n": 1}"#, "v1"), ("raw/b", b"x", "m1")]);
    let object = get("raw/a.json").unwrap();
    assert_eq!(
        (object.url.as_str(), object.size, object.marked),
        ("s3://raw/a.json", 8, false)
    );
    assert_eq!(
        object.read_json::<serde_json::Value>().unwrap(),
        json!({"n": 1})
    );
    assert_eq!(fake.calls(), ["info raw/a.json", "read raw/a.json v1"]);
    let marked = get("raw/b").unwrap();
    assert!(marked.marked);
    assert!(matches!(marked.read(), Err(Error::NotAllowed(_))));
    assert!(matches!(get("raw/none"), Err(Error::NotFound(_))));
}

#[test]
fn list_pages() {
    let objects: Vec<(String, Vec<u8>)> =
        (0..5).map(|n| (format!("raw/{n}"), vec![0; n])).collect();
    let refs: Vec<(&str, &[u8], &str)> = (objects.iter())
        .map(|(path, content)| (path.as_str(), content.as_slice(), "v1"))
        .collect();
    let fake = install(&refs);
    let sizes: Vec<u64> = list("raw/").map(|object| object.unwrap().size).collect();
    assert_eq!(sizes, [0, 1, 2, 3, 4]);
    let pages = fake
        .calls()
        .iter()
        .filter(|call| call.starts_with("list"))
        .count();
    assert_eq!(pages, 3, "{:?}", fake.calls());
}

#[test]
fn subjects_load_objects() {
    install(&[("raw/a", b"abc", "v2")]);
    let upload = WatchEvent {
        kind: "data_uploaded".into(),
        path: "/r/g/x/data/n/raw/a".into(),
        resource: "raw/a".into(),
        revision: "v2".into(),
    };
    let invocation = |trigger| Invocation {
        run: "run-1".into(),
        install: "i".into(),
        group: "g".into(),
        settings: "{}".into(),
        trigger,
    };
    let size = |object: Object| Ok(object.size);
    assert_eq!(
        handle(invocation(Trigger::Event(upload)), size),
        Ok(Some("3".into()))
    );
    let form = |form: Form| form.object("file")?.read_text();
    let manual = invocation(Trigger::Manual(r#"{"file": "raw/a"}"#.into()));
    assert_eq!(handle(manual, form), Ok(Some(r#""abc""#.into())));
}
