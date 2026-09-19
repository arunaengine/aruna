//! Compute executor backends, their registry and the interactive session manager.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod executor;
pub mod registry;
pub mod session;

pub use executor::ExecutorBackend;
pub use executor::config::{
    ApptainerConfig, ComputeConfig, DEFAULT_WORKLOAD_SA, DockerConfig, KubernetesConfig,
};
pub use executor::dispatch_helper;
pub use registry::ExecutorRegistry;
pub use session::{Session, SessionRegistry};
