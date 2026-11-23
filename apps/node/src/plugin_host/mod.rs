//! Runs plugins on this node: the worker loops, the host calls of a running guest, plugin work
//! and the repair loop. These shells do the I/O; the decisions live in `aruna_ops_work::plugins`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id::NodeId;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};

use aruna_ops_work::document_repository::GetConfigOperation;
use aruna_ops_work::plugins::artifacts::{ArtifactError, FetchArtifact, ServingNode};
use aruna_ops_work::plugins::copies::Assembled;
use aruna_ops_work::plugins::install::InstallError;
use aruna_runner::driver::{DriverContext, drive};

pub mod broker;
pub mod plugin_work;
pub mod repair;
mod transfers;
pub mod worker;

#[cfg(test)]
mod cleanup_tests;
#[cfg(test)]
mod decide_tests;
#[cfg(test)]
mod delivery_tests;
#[cfg(test)]
mod events_tests;
#[cfg(test)]
mod submit_tests;

#[cfg(test)]
#[path = "outbound_tests.rs"]
pub(crate) mod connection_tests;

/// This node as a plugin request sees it; a node without a network serves none.
pub(crate) fn serving_node(ctx: &DriverContext) -> Option<ServingNode> {
    let net = ctx.net_handle.as_ref()?;
    Some((*net.realm_id(), net.node_id()))
}

/// The realm config as this node holds it.
pub async fn read_config(
    ctx: &DriverContext,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, InstallError> {
    Ok(drive(GetConfigOperation::new(realm_id), ctx).await?)
}

/// The artifact with `digest`, from this node or a management node; a copy fetched from
/// another node is kept here.
pub(crate) async fn ready_artifact(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    digest: &str,
    local: Option<NodeId>,
) -> Result<Assembled, ArtifactError> {
    let fetched = drive(FetchArtifact::new(config, digest, local)?, ctx).await?;
    Ok(fetched.copy)
}
