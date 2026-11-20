//! Tests the report keep as calls and results: a small report is collected, a large or missing
//! one is kept as none without reading it, and a failed keep only warns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::BackendError;
use aruna_core::compute::effect::{OutputBody, OutputFailure};
use aruna_core::stream::BackendStream;

use super::*;
use crate::plugins::fixtures::is_child;
use crate::plugins::rows::read_call;
use crate::plugins::rows::tests::child_result;

fn started() -> KeepReport {
    let ids = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
    let mut keep = KeepReport::new(ids, read_call(Vec::new()));
    assert!(matches!(keep.first(), Ok(Next::Call(IoCall::Read(_)))));
    keep
}

fn opened(size: u64) -> IoResult {
    let body = OutputBody {
        size,
        bytes: BackendStream(Box::pin(futures_util::stream::empty())),
        failure: OutputFailure::default(),
    };
    let answer = ComputeAnswer::Output(Ok(body));
    IoResult::Event(Box::new(Event::Compute(Box::new(answer))))
}

#[test]
fn small_report_collected() {
    let mut keep = started();
    let collect = keep.next(opened(6));
    assert!(matches!(collect, Ok(Next::Call(IoCall::Effect(_)))));
    let bytes = Ok(b"mapped".to_vec());
    let collected = Event::Blob(BlobEvent::StreamCollected { bytes });
    assert!(is_child(&keep.next(IoResult::Event(Box::new(collected)))));
    let kept = child_result(Ok::<_, StoreError>(Changed::default()));
    assert_eq!(keep.next(kept), Ok(Next::Done(())));
}

#[test]
fn large_report_skipped() {
    let mut keep = started();
    assert!(is_child(&keep.next(opened(MAX_REPORT as u64 + 1))));
    assert_eq!(keep.phase, ReportPhase::Keep);
    let mut missing = started();
    let answer = ComputeAnswer::Output(Err(BackendError::Unavailable("gone".to_string())));
    let event = IoResult::Event(Box::new(Event::Compute(Box::new(answer))));
    assert!(is_child(&missing.next(event)));
    // A keep that fails leaves the attempt to finish.
    let failed = child_result(Err::<Changed, _>(StoreError("down".to_string())));
    assert_eq!(missing.next(failed), Ok(Next::Done(())));
}
