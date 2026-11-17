//! Hands metadata effects to the metadata domain, which runs them on the graph store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::metadata::MetadataEffect;

use crate::driver::DriverContext;

pub(super) async fn dispatch_metadata(effect: MetadataEffect, context: &DriverContext) -> Event {
    Box::pin(crate::metadata::handle::send_metadata_effect(context, effect)).await
}
