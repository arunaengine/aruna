//! Tests connection calls as calls and results: the exposure check, the stored reply of a call
//! with an intent, and the work request a transfer starts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::injection::OutgoingRequest;
use aruna_plugin_types::connection::Method;

use super::*;
use crate::plugins::fixtures::{self, is_child, node};
use crate::plugins::rows::tests::child_result;
use aruna_core::install_work::RequestContext;
use aruna_test_support::plugins::queued;

fn scope() -> RunScope {
    RunScope {
        realm_id: fixtures::realm(),
        local: node(3),
        run: queued(fixtures::install().install_id, 1),
    }
}

fn ask() -> ConnectionAsk {
    ConnectionAsk {
        connection: "api".into(),
        method: Method::Get,
        path: "records".into(),
        query: Vec::new(),
        headers: Vec::new(),
    }
}

fn intent() -> CallIntent {
    CallIntent {
        ordinal: 0,
        digest: [1; 32],
    }
}

fn ready_request(exposure: ResponseExposure) -> ReadyRequest {
    let context = RequestContext {
        connection: "api".into(),
        base: "https://example.org".into(),
        method: "GET".into(),
        path: "records".into(),
        query: Vec::new(),
        headers: Vec::new(),
    };
    let outgoing = OutgoingRequest {
        method: "GET".into(),
        url: "https://example.org/records".parse().unwrap(),
        headers: Vec::new(),
    };
    ReadyRequest {
        outgoing,
        exposure,
        fields: SecretFields::new(Default::default()),
        context,
        secret: true,
    }
}

fn ready(exposure: ResponseExposure) -> IoResult {
    child_result(Ok::<_, CallError>(ready_request(exposure)))
}

fn send(intent: Option<CallIntent>) -> SendCall {
    let holder = (&scope(), Ulid::from_parts(8, 1));
    let mut send = SendCall::new(holder, ask(), (Vec::new(), intent), 1024);
    assert!(is_child(&send.first()), "the request is checked first");
    send
}

#[test]
fn send_needs_whole() {
    let mut narrowed = send(None);
    let refused = narrowed.next(ready(ResponseExposure::Status));
    assert_eq!(refused.unwrap_err().error.kind, ErrorKind::Denied);
    let mut plain = send(None);
    assert!(
        is_child(&plain.next(ready(ResponseExposure::Full))),
        "then the send"
    );
    assert_eq!(plain.phase, SendPhase::Send);
}

#[test]
fn send_replays_reply() {
    let mut replay = send(Some(intent()));
    assert!(
        is_child(&replay.next(ready(ResponseExposure::Full))),
        "the intent first"
    );
    let stored = whole_text(&Response {
        status: 200,
        headers: Vec::new(),
        body: b"kept".to_vec(),
    });
    let settled = child_result(Ok::<_, StoreError>(Begun::Settled(stored)));
    let Ok(Next::Done(answer)) = replay.next(settled) else {
        panic!("a settled call answers its stored reply");
    };
    assert_eq!((answer.status, answer.body), (200, b"kept".to_vec()));
}

#[test]
fn send_records_outcome() {
    let mut call = send(Some(intent()));
    call.next(ready(ResponseExposure::Full)).unwrap();
    let fresh = child_result(Ok::<_, StoreError>(Begun::Fresh));
    assert!(is_child(&call.next(fresh)), "then the send");
    let lost = Err::<ConnectionAnswer, _>(ConnectionError::Unknown("cut".into()));
    assert!(is_child(&call.next(child_result(lost))), "then the outcome");
    assert_eq!(call.phase, SendPhase::Record);
    let limit = || CallFailure::from(CallError(HostError::new(ErrorKind::Limit, "big")));
    let marked = CallFailure {
        changed: true,
        ..limit()
    };
    let recorded = call.next(child_result(Err::<String, _>(marked)));
    assert_eq!(recorded, Err(limit()), "a late failure is no change");
}

fn transfer(transfer: Transfer) -> TransferCall {
    let place = ((5, Ulid::from_parts(5, 5)), None);
    let holder = (&scope(), Ulid::from_parts(8, 1));
    let mut call = TransferCall::new(holder, ask(), (transfer, intent()), place);
    assert!(is_child(&call.first()));
    call
}

#[test]
fn download_needs_whole() {
    let download = || Transfer::Download {
        target: "/target".into(),
        checksums: Vec::new(),
        max_bytes: None,
        ordinal: 0,
    };
    let mut narrowed = transfer(download());
    let refused = narrowed.next(ready(ResponseExposure::Status));
    assert_eq!(refused.unwrap_err().error.kind, ErrorKind::Denied);
    let mut full = transfer(download());
    assert!(
        is_child(&full.next(ready(ResponseExposure::Full))),
        "the work starts"
    );
    let started = child_result(Ok::<_, CallFailure>("work".to_string()));
    assert_eq!(full.next(started), Ok(Next::Done("work".to_string())));
}

#[test]
fn upload_keeps_context() {
    let upload = Transfer::Upload {
        source: "/source".into(),
        version: None,
        form: None,
        ordinal: 2,
    };
    let request = TransferCall::request(upload, ready_request(ResponseExposure::Status));
    let Ok(WorkRequest::Upload(request)) = request else {
        panic!("an upload needs no whole answer");
    };
    assert_eq!((request.source.as_str(), request.ordinal), ("/source", 2));
    assert_eq!(request.context.connection, "api");
}
