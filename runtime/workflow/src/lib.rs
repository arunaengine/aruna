//! Runs the workflow coordinators of a node: sandboxed coordinator attempts on an executor
//! backend or a local binary, their framed channel, warm preflight workers and run sessions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod channel;
pub mod pool;
pub mod runtime;
pub mod settings;
pub mod transport;

pub use channel::Connection;
pub use pool::PreflightPool;
pub use runtime::WorkflowRuntime;
pub use settings::{ContainerLimits, WorkflowSettings};
pub use transport::{Attempt, ContainerTransport, CoordinatorTransport, LocalBinary};
