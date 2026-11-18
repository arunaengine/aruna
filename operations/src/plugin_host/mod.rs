//! Runs plugins on this node: the worker loops, the host calls of a running guest, plugin work
//! and the repair loop. These shells do the I/O; the decisions live in `crate::plugins`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;

use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::plugins::artifacts::{ArtifactError, FetchArtifact, ServingNode};
use crate::plugins::copies::Assembled;
use crate::plugins::copy_rows::StoreCopy;
use crate::plugins::install::InstallError;

pub mod broker;
pub(crate) mod peer;
pub mod plugin_work;
pub mod repair;
mod transfers;
pub mod worker;

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
    if fetched.fresh {
        let store = StoreCopy::new(config, digest, &fetched.copy.bytes, unix_timestamp_millis());
        drive(store, ctx).await?;
    }
    Ok(fetched.copy)
}
