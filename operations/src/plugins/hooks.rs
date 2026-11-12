//! Admits webhook requests on an install's receiver: checks the install and its rate, verifies
//! the request with its bound secret, removes credential headers and queues one run per delivery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Instant;

use aruna_core::NodeId;
use aruna_core::injection::verify_request;
use aruna_core::keyspaces::PLUGIN_HOOKS_KEYSPACE;
use aruna_core::plugin_installs::{BoundScope, InstallState};
use aruna_core::plugin_runs::{HookRequest, RunTrigger, hook_key, hook_time_key};
use aruna_core::secrets::Delivery;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_plugins::manifest::SecretKind;
use aruna_plugins::trigger::TriggerKind;
use ulid::Ulid;

use super::bound::{delivery_clash, select};
use super::dispatch::new_run;
use super::held::{HeldSecrets, ask_release};
use super::install::{pick_version, read_config, read_manifest};
use super::rate::take_hook;
use super::receiver::lost_reason;
use super::release::{ReleaseKeys, ReleaseRequest};
use super::runs::{ADMISSIONS, RunError, admit_in};
use super::store::{Txn, encode, transact};
use crate::driver::DriverContext;

/// Headers removed before a request is stored: hop-by-hop ones and the relay mark. The check
/// itself removes the credential headers.
const DROPPED: &[&str] = &[
    "connection",
    "host",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "x-aruna-relay-hop",
];

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum HookError {
    #[error("no install with a webhook trigger has this id")]
    NotFound,
    #[error("this node does not run the install; its receiver is {0}")]
    Elsewhere(NodeId),
    #[error("the install takes no webhooks now: {0}")]
    Paused(&'static str),
    #[error("the install is over its webhook rate")]
    Rate,
    #[error("the request did not pass its check")]
    Unverified,
    #[error("the request cannot be checked now: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Run(#[from] RunError),
}

/// Checks one webhook request for the install on this receiver and queues its run. Answers
/// `false` for a delivery id the receiver already admitted, which starts nothing.
pub async fn admit_hook(
    ctx: &DriverContext,
    (local, keys): (NodeId, &ReleaseKeys),
    (realm_id, install_id): (RealmId, Ulid),
    mut request: HookRequest,
    now_ms: u64,
) -> Result<bool, HookError> {
    let unavailable = |why: String| HookError::Unavailable(why);
    let config = read_config(ctx, realm_id).await;
    let config = config.map_err(|error| unavailable(error.to_string()))?;
    let install = (config.installs.iter()).find(|install| install.install_id == install_id);
    let install = install.ok_or(HookError::NotFound)?;
    let plugin = pick_version(&config, &install.plugin, Some(&install.digest));
    let manifest = plugin.and_then(|plugin| read_manifest(&plugin));
    let manifest = manifest.map_err(|error| unavailable(error.to_string()))?;
    let hook = manifest.triggers.get(&TriggerKind::Webhook);
    let hook = hook.ok_or(HookError::NotFound)?;
    let both = hook.verify.clone().zip(hook.handler.clone());
    let (slot, handler) = both.ok_or(HookError::NotFound)?;
    if let Some(reason) = lost_reason(&config, install.receiver) {
        return Err(HookError::Paused(reason));
    }
    if install.receiver != local {
        return Err(HookError::Elsewhere(install.receiver));
    }
    if install.state != InstallState::Active {
        return Err(HookError::Paused(install.state.as_str()));
    }
    if !take_hook(install_id, Instant::now()) {
        return Err(HookError::Rate);
    }
    let release = ReleaseRequest {
        install_id,
        slot: slot.clone(),
        delivery: Delivery::Verify,
        target: None,
        held: None,
        run_id: None,
    };
    let ask = |release| ask_release(ctx, &config, (local, keys), release);
    let released = HeldSecrets::default().obtain(release, ask).await;
    let (released, fields) = released.map_err(|error| unavailable(error.to_string()))?;
    // A secret edit can make the checked header the delivery id; nothing starts then.
    if delivery_clash(&manifest, &slot, &released.settings) {
        return Err(unavailable(
            "the delivery id header is the checked header".into(),
        ));
    }
    let (kind, settings) = (&released.kind, &released.settings);
    let passed = verify_request(kind, settings, &fields, &mut request.headers, &request.body);
    let passed = passed.map_err(|error| unavailable(error.to_string()))?;
    (request.headers).retain(|(name, _)| !DROPPED.contains(&name.to_ascii_lowercase().as_str()));
    if !passed {
        return Err(HookError::Unverified);
    }
    let id_header = hook.delivery_id.as_deref();
    request.delivery_id = (request.headers.iter())
        .find(|(name, _)| id_header.is_some_and(|id| name.eq_ignore_ascii_case(id)))
        .map(|(_, value)| value.clone())
        .filter(|value| !value.is_empty());
    let delivery_id = request.delivery_id.clone();
    let run = new_run(install, handler, RunTrigger::Webhook(request), now_ms);
    let _admitting = ADMISSIONS.lock().await;
    let admitted = transact(ctx, async |txn: &Txn<'_>| {
        if let Some(id) = &delivery_id {
            let key = hook_key(install_id, id);
            if txn.read(PLUGIN_HOOKS_KEYSPACE, &key).await?.is_some() {
                return Ok(false);
            }
            let time = hook_time_key(now_ms, install_id, id);
            let rows = vec![(key, encode(&now_ms)?), (time, Vec::new())];
            txn.write(PLUGIN_HOOKS_KEYSPACE, rows).await?;
        }
        admit_in(txn, &run).await?;
        Ok::<_, RunError>(true)
    });
    Ok(admitted.await?)
}

/// Whether the install's verify secret checks `authorization`, so a relay passes it on. A node
/// secret's settings live on the receiver; for one of those the relay drops the header.
pub async fn checks_authorization(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    install_id: Ulid,
) -> bool {
    let install = (config.installs.iter()).find(|install| install.install_id == install_id);
    let Some(install) = install else {
        return false;
    };
    let plugin = pick_version(config, &install.plugin, Some(&install.digest));
    let Ok(manifest) = plugin.and_then(|plugin| read_manifest(&plugin)) else {
        return false;
    };
    let hook = manifest.triggers.get(&TriggerKind::Webhook);
    let Some(slot) = hook.and_then(|hook| hook.verify.as_ref()) else {
        return false;
    };
    let declared = manifest.secrets.get(slot).map(|declared| declared.kind);
    if declared == Some(SecretKind::Bearer) {
        return true;
    }
    let binding = install.secrets.get(slot);
    let Some(binding) = binding.filter(|binding| binding.scope != BoundScope::Node) else {
        return false;
    };
    let selected = select(ctx, config, install, binding).await.ok().flatten();
    let header = selected.and_then(|selected| selected.settings.get("header").cloned());
    header.is_some_and(|header| header.eq_ignore_ascii_case("authorization"))
}

#[cfg(test)]
#[path = "hooks_tests.rs"]
mod tests;
