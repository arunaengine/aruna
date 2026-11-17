//! Checks that the reservation release refuses an event before it starts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::operation_check::{rejects_events, sample_events};
use ulid::Ulid;

use crate::jobs::lifecycle::reservation::ReleaseExecutionOperation;

#[test]
fn rejects_unstarted_release() {
    let release = || ReleaseExecutionOperation::new(Ulid::from_bytes([5u8; 16]));
    let refusals = rejects_events(release, sample_events, |_| false);
    assert_eq!(refusals.len(), sample_events().len());
}
