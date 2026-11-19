//! Groups the staging source operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod check_source;
pub mod granted_read;
pub mod head_source;
pub mod list_source;
pub mod native_source;
pub mod offer;
pub mod offered_directory;
pub mod page;
pub mod read_source;
pub mod reference;
pub mod snapshot;
pub mod snapshot_reuse;

/// Resolves the connector of a source path below its root.
pub(crate) fn resolve_call(
    group_id: aruna_core::types::GroupId,
    connector_id: ulid::Ulid,
    source_path: String,
) -> aruna_core::calls::IoCall {
    let input = crate::connectors::ResolveConnectorInput {
        group_id,
        connector_id,
        source_path,
        allow_root: false,
    };
    aruna_core::calls::IoCall::from(crate::connectors::resolve_connector_effect(input))
}
