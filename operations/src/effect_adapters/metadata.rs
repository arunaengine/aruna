//! Hands metadata effects to the metadata handle, or answers that the handle is missing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::{Future, ready};
use std::pin::Pin;

use aruna_core::events::Event;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};

use crate::driver::DriverContext;

// A boxed future: the metadata effect path reads authorization through the runner again,
// so its type must not contain the runner's own future.
pub(super) fn dispatch_metadata(
    effect: MetadataEffect,
    context: &DriverContext,
) -> Pin<Box<dyn Future<Output = Event> + Send + '_>> {
    match &context.metadata_handle {
        Some(metadata) => Box::pin(metadata.run_effect(context, effect)),
        None => Box::pin(ready(Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }))),
    }
}
