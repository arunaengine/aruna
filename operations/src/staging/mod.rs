//! Groups the staging source operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod bucket_guard;
pub mod check_source;
pub mod head_source;
pub mod list_offers;
pub mod list_source;
pub mod native_source;
pub mod offered_directory;
pub mod page;
pub mod read_source;
pub mod reference;
pub mod snapshot;
pub mod snapshot_reuse;

use aruna_core::calls::IoCall;
use aruna_core::effects::Effect;

/// The call for an effect the connector resolver builds: a sub-operation runs as a child.
pub(crate) fn child_call(effect: Effect) -> IoCall {
    match effect {
        Effect::SubOperation(child) => IoCall::Child(child),
        other => IoCall::Effect(Box::new(other)),
    }
}
