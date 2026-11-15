//! Tests jobs: a copy answers its job at once, a job read by id with its upload answer and report,
//! outputs across pages, cancel, and the finished job as the subject of `job_finished`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::Value;

use super::*;
use crate::bindings::aruna::plugin::objects_read::ObjectInfo;
use crate::bindings::aruna::plugin::types::{Field, Invocation, Trigger};
use crate::host::fake::{Fake, install};
use crate::{WatchEvent, handle, objects};

fn status(id: &str, state: &str) -> Status {
    Status {
        id: id.into(),
        kind: "upload".into(),
        path: "raw/a".into(),
        state: state.into(),
        message: None,
        bytes: Some(4),
        version: None,
        reply_status: Some(201),
        reply_headers: vec![Field {
            name: "etag".into(),
            value: "e1".into(),
        }],
        reply_body: Some(br#"{"checksum": "md5:1"}"#.to_vec()),
        exit_code: None,
        report: Some(r#"{"reads": 9}"#.into()),
    }
}

fn with_job(fake: &Fake, id: &str, state: &str) {
    let outputs = (0..3).map(|n| ObjectInfo {
        path: format!("out/{n}"),
        size: n,
        version: "v1".into(),
        marked: n == 1,
    });
    fake.0
        .borrow_mut()
        .jobs
        .insert(id.into(), (status(id, state), outputs.collect()));
}

#[test]
fn copy_starts_job() {
    let fake = install(&[]);
    let job = objects::copy("in/a", "out/a")
        .version("v7")
        .start()
        .unwrap();
    assert_eq!(
        (
            job.id.as_str(),
            job.kind.as_str(),
            job.path.as_str(),
            job.state.as_str()
        ),
        ("job-1", "copy", "out/a", "queued")
    );
    assert!(!job.ok);
    assert_eq!(
        fake.calls(),
        ["copy in/a v7 out/a"],
        "a start makes no other call"
    );
}

#[test]
fn job_by_id() {
    let fake = install(&[]);
    with_job(&fake, "j1", "succeeded");
    let mut job = get("j1").unwrap();
    assert!(job.ok);
    let reply = job.reply.clone().unwrap();
    assert_eq!((reply.status, reply.header("ETag")), (201, Some("e1")));
    assert_eq!(reply.json::<Value>().unwrap()["checksum"], "md5:1");
    assert_eq!(job.report_json::<Value>().unwrap().unwrap()["reads"], 9);
    let outputs: Vec<Object> = job.outputs().map(Result::unwrap).collect();
    let marked: Vec<bool> = outputs.iter().map(|output| output.marked).collect();
    assert_eq!(marked, [false, true, false]);
    job.cancel().unwrap();
    with_job(&fake, "j1", "cancelled");
    job.refresh().unwrap();
    assert_eq!((job.state.as_str(), job.ok), ("cancelled", false));
    assert!(matches!(get("j2"), Err(crate::Error::NotFound(_))));
}

#[test]
fn finished_subject() {
    let fake = install(&[]);
    with_job(&fake, "j1", "failed");
    let finished = WatchEvent {
        kind: "job_finished".into(),
        path: "/r/g/x/data/n/raw/a".into(),
        resource: "j1".into(),
        revision: "failed".into(),
    };
    let invocation = Invocation {
        run: "run-1".into(),
        install: "i".into(),
        group: "g".into(),
        settings: "{}".into(),
        trigger: Trigger::Event(finished),
    };
    let ended = |job: Job| Ok((job.id, job.ok));
    assert_eq!(
        handle(invocation, ended),
        Ok(Some(r#"["j1",false]"#.into()))
    );
    assert_eq!(fake.calls(), ["job j1"]);
}
