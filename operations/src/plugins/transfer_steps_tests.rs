//! Tests the transfer decisions as calls and results: the checks before a byte moves, what an
//! answer means, and how a kept reply ends the work.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::StorageError;
use aruna_plugins::broker::ErrorKind;
use serde_json::json;

use super::*;
use crate::plugins::fixtures::{self, is_child, node};
use crate::plugins::rows::tests::child_result;
use crate::plugins::work::SendBoundary;
use crate::plugins::work::tests::proposed;

fn version() -> Ulid {
    Ulid::from_parts(5, 5)
}

struct Parts(RealmConfigDocument, PluginInstall, WorkRow);

fn parts() -> Parts {
    let install = fixtures::install();
    let row = proposed(&install, 1);
    Parts(fixtures::config(&install, json!({})), install, row)
}

impl Parts {
    fn work(&self) -> TransferWork<'_> {
        TransferWork {
            node: (fixtures::realm(), node(3)),
            config: &self.0,
            install: &self.1,
            row: &self.2,
        }
    }
}

fn patterns() -> IoResult {
    child_result(Ok::<_, CallError>(BTreeSet::from(["p".to_string()])))
}

fn object() -> IoResult {
    child_result(Ok::<_, CallError>(("b".to_string(), "k".to_string())))
}

fn request(exposure: ResponseExposure) -> PreparedRequest {
    PreparedRequest {
        outgoing: OutgoingRequest {
            method: "PUT".to_string(),
            url: "https://example.org/a".parse().unwrap(),
            headers: Vec::new(),
        },
        exposure,
        fields: SecretFields::new(Default::default()),
    }
}

fn request_result(exposure: ResponseExposure) -> IoResult {
    child_result(Ok::<_, CallError>(request(exposure)))
}

fn no_version() -> IoResult {
    child_result(Err::<HeadObjectResult, _>(HeadObjectError::NoSuchVersion))
}

fn download(parts: &Parts) -> PrepareDownload {
    let mut prepare = PrepareDownload::new(parts.work(), "in/a", version());
    assert!(is_child(&prepare.first()));
    assert!(is_child(&prepare.next(patterns())));
    assert!(is_child(&prepare.next(object())));
    prepare
}

#[test]
fn download_needs_whole() {
    let parts = parts();
    let mut prepare = download(&parts);
    assert!(is_child(&prepare.next(no_version())));
    let Ok(Next::Done(plan)) = prepare.next(request_result(ResponseExposure::Full)) else {
        panic!("a whole answer may be written");
    };
    assert_eq!((plan.bucket.as_str(), plan.key.as_str()), ("b", "k"));
    let mut narrow = download(&parts);
    narrow.next(no_version()).unwrap();
    let refused = narrow.next(request_result(ResponseExposure::Status));
    assert!(matches!(refused, Err(TransferEnd::Permanent(_))));
}

#[test]
fn download_retries_head() {
    let parts = parts();
    let failed = HeadObjectError::StorageError(StorageError::Timeout);
    let answer = child_result(Err::<HeadObjectResult, _>(failed));
    assert!(matches!(
        download(&parts).next(answer),
        Err(TransferEnd::Retry(_))
    ));
}

#[test]
fn refusal_stops_download() {
    let parts = parts();
    let mut prepare = PrepareDownload::new(parts.work(), "in/a", version());
    prepare.first().unwrap();
    let denied = HostError::new(ErrorKind::Denied, "no");
    let answer = child_result(Err::<BTreeSet<String>, _>(CallError(denied.clone())));
    assert_eq!(prepare.next(answer), Err(TransferEnd::Refused(denied)));
}

#[test]
fn answers_decide_download() {
    assert_eq!(check_answer(200, Some(4), Some(4)), Ok(()));
    let retry = TransferEnd::Retry("the service answered 503".to_string());
    assert_eq!(check_answer(503, None, None), Err(retry));
    let moved = TransferEnd::Permanent("the service answered 302".to_string());
    assert_eq!(check_answer(302, None, None), Err(moved));
    let large = check_answer(200, Some(5), Some(4));
    assert!(matches!(large, Err(TransferEnd::Permanent(_))));
}

#[test]
fn write_failures_classified() {
    let storage = PutObjectError::StorageError(StorageError::Timeout);
    let other = PutObjectError::MissingOutput;
    let bound = Some(4);
    assert!(matches!(
        write_failure(&other, (true, false), bound),
        TransferEnd::Permanent(message) if message.contains("size limit")
    ));
    let broken = write_failure(&other, (false, true), bound);
    assert!(matches!(broken, TransferEnd::Retry(_)));
    let busy = write_failure(&storage, (false, false), bound);
    assert!(matches!(busy, TransferEnd::Retry(_)));
    let refused = write_failure(&other, (false, false), bound);
    assert!(matches!(refused, TransferEnd::Permanent(_)));
}

fn upload(parts: &Parts) -> PrepareUpload {
    let mut prepare = PrepareUpload::new(parts.work(), "out/a", None);
    assert!(is_child(&prepare.first()));
    prepare
}

#[test]
fn upload_runs_checks() {
    let parts = parts();
    let mut prepare = upload(&parts);
    assert!(is_child(&prepare.next(patterns())));
    assert!(is_child(&prepare.next(object())));
    let found = child_result(Ok::<_, CallError>(Some(version())));
    assert!(is_child(&prepare.next(found)));
    let Ok(Next::Done(plan)) = prepare.next(request_result(ResponseExposure::Full)) else {
        panic!("the checked request is the plan");
    };
    assert_eq!(plan.version, version());
}

#[test]
fn upload_source_gone() {
    let parts = parts();
    let mut prepare = upload(&parts);
    prepare.next(patterns()).unwrap();
    prepare.next(object()).unwrap();
    let none = child_result(Ok::<Option<Ulid>, CallError>(None));
    assert_eq!(prepare.next(none), Err(source_gone()));
}

#[test]
fn answered_upload_decides() {
    let mut parts = parts();
    parts.2.boundary = Some(SendBoundary::Sent {
        status: 201,
        reply: None,
        bytes: 3,
        version: version(),
    });
    let done = TransferEnd::Done {
        bytes: 3,
        version: version(),
    };
    assert_eq!(upload(&parts).next(patterns()), Err(done));
}

#[test]
fn sends_end_earlier() {
    assert_eq!(send_end(Send::Now, false), None);
    let undecided = send_end(Send::Undecided, false);
    assert!(matches!(undecided, Some(TransferEnd::Undecided(_))));
    let answered = |status| Send::Answered {
        status,
        reply: None,
        bytes: 3,
        version: version(),
    };
    let retried = send_end(answered(503), true);
    assert!(matches!(retried, Some(TransferEnd::Retry(_))));
    let failed = send_end(answered(503), false);
    assert!(matches!(failed, Some(TransferEnd::Permanent(_))));
}

fn keep(status: u16, body: Option<Vec<u8>>) -> KeepReply {
    let plan = UploadPlan {
        patterns: BTreeSet::new(),
        bucket: "b".to_string(),
        key: "k".to_string(),
        version: version(),
        again: true,
        request: request(ResponseExposure::Full),
    };
    let reply = UploadReply {
        status,
        headers: Vec::new(),
        body,
    };
    KeepReply::new((version(), version()), plan, 3, reply)
}

#[test]
fn reply_kept_whole() {
    assert!(keep(200, Some(b"ok".to_vec())).shown().is_some());
    assert_eq!(keep(200, None).shown(), None);
    let mut kept = keep(200, None);
    assert!(is_child(&kept.first()));
    let stored = child_result(Ok::<Option<WorkRow>, StoreError>(None));
    let done = TransferEnd::Done {
        bytes: 3,
        version: version(),
    };
    assert_eq!(kept.next(stored), Ok(Next::Done(done)));
}

#[test]
fn reply_store_failure() {
    let mut kept = keep(500, None);
    kept.first().unwrap();
    let failed = child_result(Err::<Option<WorkRow>, _>(StoreError("no".into())));
    assert_eq!(kept.next(failed), Err(TransferEnd::Retry("no".into())));
}
