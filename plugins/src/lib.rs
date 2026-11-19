//! Builds plugin packages and, with the `runtime` feature, runs plugins in the bounded WASM
//! runtime; manifests and artifacts are in `aruna-plugin-types`.
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
mod host;
#[cfg(feature = "runtime")]
mod imports;
#[cfg(feature = "runtime")]
pub mod limits;
#[cfg(feature = "runtime")]
mod runtime;
#[cfg(feature = "runtime")]
mod supervisor;
#[cfg(feature = "runtime")]
mod values;

#[cfg(feature = "runtime")]
pub use host::Limit;
#[cfg(feature = "runtime")]
pub use host::wit::aruna::plugin::types::{
    Failure, FailureKind, Field, Trigger, WatchEvent, WebhookDelivery, WebhookRequest,
};
#[cfg(feature = "runtime")]
pub use runtime::{CompileError, Compiled, Invocation, InvokeError, Runtime};
#[cfg(feature = "runtime")]
pub use supervisor::Supervisor;
#[cfg(feature = "runtime")]
pub use tokio_util::sync::CancellationToken;
