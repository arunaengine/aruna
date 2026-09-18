//! Groups the source connector storage, validation and resolver modules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod create_connector;
pub mod delete_connector;
pub mod get_connector;
pub mod list_connectors;
pub mod reference_scan;
pub mod replace_connector;
pub mod repository;
pub mod resolver;
pub mod secret_config;
pub mod validation;

pub use resolver::{
    ResolveBindingInput, ResolveBindingOperation, ResolveConnectorInput, ResolveConnectorOperation,
    resolve_binding_effect, resolve_connector_effect,
};
