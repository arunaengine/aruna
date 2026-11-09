//! Plugin manifests, artifact sections and, with the `runtime` feature, the bounded WASM runtime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod artifact;
pub mod connection;
pub mod manifest;
pub mod permission;
pub mod trigger;

#[cfg(feature = "runtime")]
pub mod broker;
#[cfg(feature = "runtime")]
mod host;
#[cfg(feature = "runtime")]
pub mod limits;
#[cfg(feature = "runtime")]
pub use host::Limit;
