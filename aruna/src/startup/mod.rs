//! Groups the node startup phases: resources, realm, listeners, background and test hooks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod background;
pub mod listeners;
pub mod realm;
pub mod resources;
pub mod test_hooks;

pub use resources::NodeResources;
