//! Admits webhook requests on an install's receiver: checks the install and its rate, verifies
//! the request with its bound secret, removes credential headers and queues one run per delivery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Instant;

use aruna_core::NodeId;
use aruna_core::injection::verify_request;
use aruna_core::keyspaces::PLUGIN_HOOKS_KEYSPACE;
use aruna_core::plugin_installs::{BoundScope, InstallState};
use aruna_core::plugin_runs::{HookRequest, PluginRun, RunTrigger, hook_key, hook_time_key};
use aruna_core::secrets::Delivery;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_plugins::manifest::SecretKind;
use aruna_plugins::trigger::TriggerKind;
use ulid::Ulid;

use super::bound::{check_node_binding, delivery_clash, select, vault_kind};
use super::dispatch::new_run;
use super::held::{HeldSecrets, ask_release};
use super::install::{pick_version, read_config, read_manifest};
use super::rate::take_hook;
use super::receiver::lost_reason;
use super::release::{ReleaseKeys, ReleaseRequest};
use super::rows::{Answer, Ask, Plan, Row, RowsOperation, Turn, done, row, values};
use super::runs::{ADMISSIONS, Admission, RunError};
use super::store::encode;
use crate::driver::{DriverContext, drive};

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
    let slot = hook.verify.clone().ok_or(HookError::NotFound)?;
    let handler = TriggerKind::Webhook.as_str().to_string();
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
    let admit = AdmitHook::new(run, (install_id, delivery_id), now_ms)?;
    Ok(drive(RowsOperation::transaction(admit), ctx).await?)
}

/// Queues a webhook's run once per delivery id in one transaction: `false` for a delivery id
/// the receiver already admitted, which starts nothing. The caller holds `ADMISSIONS`.
#[derive(Debug, PartialEq)]
pub struct AdmitHook {
    /// The delivery's rows, written with the run; `None` without a delivery id.
    delivery: Option<(Vec<u8>, Vec<Row>)>,
    admission: Admission,
    phase: HookPhase,
}

#[derive(Debug, PartialEq)]
enum HookPhase {
    Seen,
    Admitting,
    Writing,
}

impl AdmitHook {
    pub fn new(
        run: PluginRun,
        (install_id, delivery_id): (Ulid, Option<String>),
        now_ms: u64,
    ) -> Result<Self, RunError> {
        let delivery = match delivery_id {
            Some(id) => {
                let key = hook_key(install_id, &id);
                let time = hook_time_key(now_ms, install_id, &id);
                let rows = vec![
                    row(PLUGIN_HOOKS_KEYSPACE, key.clone(), encode(&now_ms)?),
                    row(PLUGIN_HOOKS_KEYSPACE, time, Vec::new()),
                ];
                Some((key, rows))
            }
            None => None,
        };
        Ok(Self {
            delivery,
            admission: Admission::new(run),
            phase: HookPhase::Seen,
        })
    }
}

impl Plan for AdmitHook {
    type Output = bool;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<bool>, RunError> {
        match &self.delivery {
            Some((key, _)) => Ok(Turn::Ask(Ask::Read(vec![(PLUGIN_HOOKS_KEYSPACE, key.clone())]))),
            None => {
                self.phase = HookPhase::Admitting;
                Ok(Turn::Ask(self.admission.ask()))
            }
        }
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<bool>, RunError> {
        match self.phase {
            HookPhase::Seen => {
                let [seen] = values::<1>(answer)?;
                if seen.is_some() {
                    return Ok(Turn::Finish(false));
                }
                self.phase = HookPhase::Admitting;
                Ok(Turn::Ask(self.admission.ask()))
            }
            HookPhase::Admitting => {
                let Some(mut rows) = self.admission.take(answer)? else {
                    return Ok(Turn::Ask(self.admission.ask()));
                };
                if let Some((_, delivery)) = self.delivery.take() {
                    rows.splice(0..0, delivery);
                }
                self.phase = HookPhase::Writing;
                Ok(Turn::Ask(Ask::Write(rows)))
            }
            HookPhase::Writing => {
                done(answer)?;
                Ok(Turn::Finish(true))
            }
        }
    }
}

/// Whether a relay from `local` passes `authorization` on: only when the install's verify secret
/// checks that header. A node secret is asked of the receiver; `None` when it cannot be told.
pub async fn checks_authorization(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    (local, install_id): (NodeId, Ulid),
) -> Option<bool> {
    let install = (config.installs.iter()).find(|install| install.install_id == install_id)?;
    let plugin = pick_version(config, &install.plugin, Some(&install.digest));
    let manifest = plugin.and_then(|plugin| read_manifest(&plugin)).ok()?;
    let hook = manifest.triggers.get(&TriggerKind::Webhook);
    let Some(slot) = hook.and_then(|hook| hook.verify.as_ref()) else {
        return Some(false);
    };
    let declared = manifest.secrets.get(slot).map(|declared| declared.kind);
    if declared == Some(SecretKind::Bearer) {
        return Some(true);
    }
    let Some(binding) = install.secrets.get(slot) else {
        return Some(false);
    };
    let header = match binding.scope {
        BoundScope::Node => {
            let kind = declared.and_then(vault_kind)?;
            let place = (local, install.receiver);
            check_node_binding(ctx, place, &binding.name, &kind)
                .await
                .ok()?
        }
        BoundScope::Group | BoundScope::Realm => {
            let selected = select(ctx, config, install, binding).await.ok()??;
            selected.settings.get("header").cloned()
        }
    };
    Some(header.is_some_and(|header| header.eq_ignore_ascii_case("authorization")))
}

#[cfg(test)]
#[path = "hooks_tests.rs"]
mod tests;
