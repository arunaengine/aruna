//! Releases the secret a plugin install binds to a slot, to that install's receiver: the use
//! check from the serving node's own view, the audit row, then the value.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::plugins::{ReleaseError, ReleaseRequest, Released};
use aruna_core::structs::identity::realm::RealmConfigDocument;

use super::secret_use::ReleaseSecret;
use crate::driver::{DriverContext, drive};

/// The release a management node serves to `peer`, decided from `config` and its own storage.
pub async fn release(
    ctx: &DriverContext,
    (config, local, peer): (&RealmConfigDocument, NodeId, NodeId),
    request: &ReleaseRequest,
) -> Result<Released, ReleaseError> {
    let nodes = (local, peer);
    let release = ReleaseSecret::new(Arc::new(config.clone()), nodes, request.clone());
    drive(release, ctx).await.map_err(|failure| failure.0)
}

/// The management side of a release request from an infrastructure peer.
pub(crate) async fn serve_release(
    ctx: &DriverContext,
    peer: NodeId,
    request: ReleaseRequest,
) -> Result<Released, ReleaseError> {
    let local = super::artifacts::local_config(ctx).await;
    let local = local.map_err(|error| ReleaseError::Unavailable(format!("{error:?}")))?;
    let (config, local) = local;
    release(ctx, (&config, local, peer), &request).await
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
