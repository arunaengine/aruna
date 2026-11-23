//! Builds plugin packages and, with the `runtime` feature, runs plugins in the bounded WASM
//! runtime; manifests and artifacts are in `aruna-plugin-contract`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(feature = "build")]
pub mod authored;
#[cfg(feature = "build")]
mod bundle;
pub mod host_state;
#[cfg(feature = "build")]
pub mod package;

#[cfg(feature = "runtime")]
pub mod broker;
#[cfg(feature = "runtime")]
pub mod host;
#[cfg(feature = "runtime")]
mod imports;
#[cfg(feature = "runtime")]
pub mod limits;
#[cfg(feature = "runtime")]
pub mod runtime;
#[cfg(feature = "runtime")]
pub mod supervisor;
#[cfg(feature = "runtime")]
mod values;

// The runtime's calls take this token; plugin test crates name it without depending on tokio-util.
#[cfg(feature = "runtime")]
pub use tokio_util::sync::CancellationToken;
