//! Tests which runs the supervisor tracks, what a revocation cancels and what stopping waits for.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

fn supervisor() -> Supervisor {
    Supervisor::new(RuntimeConfig::default(), Limits::default()).unwrap()
}

fn id(seed: u128) -> Ulid {
    Ulid::from_parts(1, seed)
}

#[tokio::test]
async fn tracks_active_runs() {
    let runs = supervisor();
    runs.admit(id(1), id(9));
    assert_eq!(runs.active(), BTreeMap::from([(id(1), id(9))]));
    runs.finish(id(1));
    assert!(runs.active().is_empty());
}

#[tokio::test]
async fn revoke_cancels_install() {
    let runs = supervisor();
    let (hit, kept) = (runs.admit(id(1), id(8)), runs.admit(id(2), id(9)));
    runs.revoke(|install| (install == id(8)).then_some("paused"));
    assert!(hit.is_cancelled());
    assert!(!kept.is_cancelled());
    assert_eq!(runs.take_reason(id(1)), Some("paused"));
    assert_eq!(runs.take_reason(id(1)), None);
    assert_eq!(runs.take_reason(id(2)), None);
}

#[tokio::test]
async fn settle_waits_attempts() {
    let runs = supervisor();
    let done = Arc::new(AtomicBool::new(false));
    let seen = done.clone();
    runs.spawn(async move {
        tokio::task::yield_now().await;
        seen.store(true, Ordering::SeqCst);
    });
    runs.settle().await;
    assert!(done.load(Ordering::SeqCst));
    assert!(!runs.stopping());
    runs.spawn(async {});
    runs.settle().await;
}

#[tokio::test]
async fn stop_cancels_runs() {
    let runs = supervisor();
    let cancel = runs.admit(id(1), id(9));
    let waited = cancel.clone();
    runs.spawn(async move { waited.cancelled().await });
    runs.stop().await;
    assert!(runs.stopping());
    assert!(cancel.is_cancelled());
}
