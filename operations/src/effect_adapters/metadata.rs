//! Hands metadata effects to the metadata domain, which runs them on the graph store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::pin::Pin;

use aruna_core::events::Event;
use aruna_core::metadata::MetadataEffect;

use crate::driver::DriverContext;

// A boxed future: the metadata effect path reads authorization through the runner again,
// so its type must not contain the runner's own future.
pub(super) fn dispatch_metadata(
    effect: MetadataEffect,
    context: &DriverContext,
) -> Pin<Box<dyn Future<Output = Event> + Send + '_>> {
    Box::pin(crate::metadata::handle::send_metadata_effect(context, effect))
}
