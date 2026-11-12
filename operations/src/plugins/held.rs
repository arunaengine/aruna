//! Gets released secrets for the runs of this node: node secrets and a management node's own
//! releases here, others from the management nodes in realm order, reusing held fields per run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::compute::SecretBytes;
use aruna_core::plugin_installs::BoundScope;
use aruna_core::secrets::SecretFields;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use ulid::Ulid;

use super::artifacts::managers;
use super::release::{ReleaseError, ReleaseKeys, ReleaseRequest, Released, release};
use crate::driver::DriverContext;
use crate::realm::mutate_placement::is_management;

/// The fields one run holds, by slot, with the value write id they belong to. They are reused
/// only when a fresh check answers that this value is still current.
#[derive(Default)]
pub struct HeldSecrets {
    held: BTreeMap<String, (Ulid, SecretBytes)>,
}

impl HeldSecrets {
    /// The fields for one use of the request's slot, after a release check through `ask`.
    pub async fn obtain<F: Future<Output = Result<Released, ReleaseError>>>(
        &mut self,
        mut request: ReleaseRequest,
        ask: impl FnOnce(ReleaseRequest) -> F,
    ) -> Result<(Released, SecretFields), ReleaseError> {
        let slot = request.slot.clone();
        request.held = self.held.get(&slot).map(|(value_id, _)| *value_id);
        let mut released = ask(request).await?;
        let bytes = match released.fields.take() {
            Some(bytes) => {
                self.held.insert(slot, (released.value_id, bytes.clone()));
                bytes
            }
            None => match self.held.get(&slot) {
                Some((value_id, bytes)) if *value_id == released.value_id => bytes.clone(),
                _ => {
                    let lost = "the answer named a value this run does not hold";
                    return Err(ReleaseError::Unavailable(lost.into()));
                }
            },
        };
        let fields = SecretFields::decode(&bytes);
        let fields = fields.map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
        Ok((released, fields))
    }
}

/// One release for a run on `local`. A refusal ends the search; an unavailable node is skipped.
pub async fn ask_release(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    (local, keys): (NodeId, &ReleaseKeys),
    request: ReleaseRequest,
) -> Result<Released, ReleaseError> {
    let install = (config.installs.iter()).find(|install| install.install_id == request.install_id);
    let binding = install.and_then(|install| install.secrets.get(&request.slot));
    let node_bound = binding.is_some_and(|binding| binding.scope == BoundScope::Node);
    if node_bound || is_management(config, local) {
        let here = release(ctx, keys, (config, local, local), &request).await;
        if node_bound || !matches!(here, Err(ReleaseError::Unavailable(_))) {
            return here;
        }
    }
    let unreachable = |why: &str| ReleaseError::Unavailable(why.to_string());
    let metadata = ctx.metadata_handle.as_ref();
    let metadata = metadata.ok_or_else(|| unreachable("the metadata transport is not running"))?;
    let mut last = unreachable("the realm has no other management node");
    for node in managers(config).filter(|node| *node != local) {
        match metadata.request_release(node, request.clone()).await {
            Err(ReleaseError::Unavailable(why)) => last = unreachable(&format!("{node}: {why}")),
            answer => return answer,
        }
    }
    Err(last)
}

#[cfg(test)]
#[path = "held_tests.rs"]
mod tests;
