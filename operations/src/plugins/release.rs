//! Releases the secret a plugin install binds to a slot, to that install's receiver: the use
//! check from the serving node's own view, the audit row, then the value.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::compute::SecretBytes;
use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
use aruna_core::plugin_installs::{BoundScope, InstallState, PluginInstall, SecretBinding};
use aruna_core::request_policy::PolicyRequest;
use aruna_core::secrets::{
    Delivery, ResponseExposure, SecretConsumer, SecretKind, SecretName, SecretScope,
    SecretSettings, UseRequest, check_use,
};
use aruna_core::structs::identity::admin_audit::{AdminAuditRecord, SecretAudit, SecretUse};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use url::Url;

use super::account::{owned, read_user};
pub use super::bound::{Refusal, ReleaseError, ReleaseKeys};
use super::bound::{Selected, open, select, slot_kind};
use super::grants::InstallScope;
use super::store::Txn;
use crate::auth::request_policy::PolicyEvaluator;
use crate::driver::DriverContext;

/// What a run asks of the secret bound to one slot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseRequest {
    pub install_id: Ulid,
    pub slot: String,
    pub delivery: Delivery,
    /// The method and URL of the outgoing request, for `inject`.
    pub target: Option<(String, String)>,
    /// The value write id the receiver holds already; its fields are not sent again.
    pub held: Option<Ulid>,
    pub run_id: Option<Ulid>,
}

/// A released secret; `fields` is absent when the receiver holds this value already.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Released {
    pub kind: SecretKind,
    pub settings: SecretSettings,
    pub exposure: ResponseExposure,
    pub value_id: Ulid,
    pub fields: Option<SecretBytes>,
}

pub fn delivery_name(delivery: Delivery) -> &'static str {
    match delivery {
        Delivery::Inject => "inject",
        Delivery::Verify => "verify",
        Delivery::Value => "value",
    }
}

/// The permission path a `secret.use` policy decides on.
pub fn secret_path(
    realm_id: RealmId,
    scope: SecretScope,
    name: &SecretName,
    node: NodeId,
) -> String {
    match scope {
        SecretScope::Group(group_id) => format!("/{realm_id}/g/{group_id}/secrets/{name}"),
        SecretScope::Realm => format!("/{realm_id}/secrets/{name}"),
        SecretScope::Node => format!("/{realm_id}/n/{node}/secrets/{name}"),
        SecretScope::User(user) => format!("/{realm_id}/u/{}/secrets/{name}", user.user_ulid),
    }
}

/// The release a management node serves to `peer`, decided from `config` and its own storage.
pub async fn release(
    ctx: &DriverContext,
    keys: &ReleaseKeys,
    (config, peer): (&RealmConfigDocument, NodeId),
    request: &ReleaseRequest,
) -> Result<Released, ReleaseError> {
    let install = (config.installs.iter())
        .find(|install| install.install_id == request.install_id)
        .ok_or(ReleaseError::Refused(Refusal::Binding))?;
    let binding = install.secrets.get(&request.slot);
    let audit = Audit {
        ctx,
        install,
        binding,
        request,
    };
    if install.receiver != peer {
        return Err(audit.refused(Refusal::Peer, None).await);
    }
    // Node secrets never leave their node; the receiver releases them itself.
    let Some(binding) = binding.filter(|binding| binding.scope != BoundScope::Node) else {
        return Err(audit.refused(Refusal::Binding, None).await);
    };
    let selected = match select(ctx, config, install, binding).await? {
        Some(selected) if slot_kind(config, install, &request.slot)? == selected.kind => selected,
        _ => return Err(audit.refused(Refusal::Entry, None).await),
    };
    let scope = binding.scope(install.group_id);
    let target = parse_target(request)?;
    let consumer = SecretConsumer::Plugin {
        plugin_id: install.plugin.clone(),
        install_id: install.install_id,
    };
    let used = UseRequest {
        consumer: &consumer,
        delivery: request.delivery,
        group: Some(install.group_id),
        target: target.as_ref().map(|(method, url)| (method.as_str(), url)),
    };
    let Ok(rule) = check_use(scope, &selected.rules, &used) else {
        return Err(audit.refused(Refusal::Rule, Some(&selected)).await);
    };
    if !authorized(ctx, install).await? {
        return Err(audit.refused(Refusal::Authority, Some(&selected)).await);
    }
    let path = secret_path(config.realm_id, scope, &binding.name, install.receiver);
    let origin = target
        .as_ref()
        .map(|(_, url)| url.origin().ascii_serialization());
    let params = BTreeMap::from([
        ("consumer".to_string(), install.install_id.to_string()),
        (
            "delivery".to_string(),
            delivery_name(request.delivery).to_string(),
        ),
        ("origin".to_string(), origin.unwrap_or_default()),
        ("kind".to_string(), selected.kind.as_str().to_string()),
    ]);
    let policy = PolicyRequest {
        path,
        permission: "read".to_string(),
        user: install.account.to_string(),
        operation: "secret.use".to_string(),
        params,
        headers: BTreeMap::new(),
        body: None,
        session: None,
    };
    let evaluator = PolicyEvaluator::load(ctx, config.realm_id, Some(install.group_id)).await;
    let evaluator = evaluator.map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
    if evaluator.evaluate(&policy).is_err() {
        return Err(audit.refused(Refusal::Policy, Some(&selected)).await);
    }
    // The row is durable before any value is opened; without it nothing is released.
    audit.write(None, Some(&selected)).await?;
    let fields = match request.held == Some(selected.value_id) {
        true => None,
        false => Some(open(keys, config.realm_id, &selected.sealed)?),
    };
    Ok(Released {
        kind: selected.kind,
        settings: selected.settings,
        exposure: rule.response.clone(),
        value_id: selected.value_id,
        fields,
    })
}

fn parse_target(request: &ReleaseRequest) -> Result<Option<(String, Url)>, ReleaseError> {
    let Some((method, url)) = &request.target else {
        return Ok(None);
    };
    let url = Url::parse(url).map_err(|_| ReleaseError::Refused(Refusal::Rule))?;
    Ok(Some((method.clone(), url)))
}

/// Step 5: the install is active and its account is the active account marked with it.
async fn authorized(ctx: &DriverContext, install: &PluginInstall) -> Result<bool, ReleaseError> {
    if install.state != InstallState::Active {
        return Ok(false);
    }
    let user = read_user(ctx, install.account).await;
    let user = user.map_err(ReleaseError::Unavailable)?;
    let scope = InstallScope {
        realm_id: install.account.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    Ok(user.is_some_and(|user| {
        user.user_id == install.account && owned(&user, &scope) && !user.is_deactivated()
    }))
}

/// Writes the audit row of one use: a release, or a refusal at a step.
struct Audit<'a> {
    ctx: &'a DriverContext,
    install: &'a PluginInstall,
    binding: Option<&'a SecretBinding>,
    request: &'a ReleaseRequest,
}

impl Audit<'_> {
    async fn write(
        &self,
        refusal: Option<Refusal>,
        selected: Option<&Selected>,
    ) -> Result<(), ReleaseError> {
        let Some(binding) = self.binding else {
            return Ok(());
        };
        let secret = SecretAudit {
            scope: binding.scope(self.install.group_id),
            name: binding.name.clone(),
            incarnation: selected.and_then(|selected| selected.incarnation),
            write_id: selected.map(|selected| selected.value_id),
        };
        let used = SecretUse {
            delivery: self.request.delivery,
            run_id: self.request.run_id,
            refused: refusal.map(|refusal| refusal.as_str().to_string()),
        };
        let record = AdminAuditRecord::for_use(self.install, secret, used);
        let unavailable = |error: String| ReleaseError::Unavailable(error);
        let rows = (record
            .entries()
            .map_err(|error| unavailable(error.to_string()))?)
        .into_iter()
        .map(|(_, key, value)| (key.to_vec(), value.to_vec()))
        .collect();
        let written = Txn::direct(self.ctx)
            .write(ADMIN_AUDIT_KEYSPACE, rows)
            .await;
        written.map_err(|error| unavailable(error.to_string()))
    }

    /// Records the refusal, and answers it whether or not the row could be written.
    async fn refused(&self, refusal: Refusal, selected: Option<&Selected>) -> ReleaseError {
        if let Err(error) = self.write(Some(refusal), selected).await {
            tracing::warn!(%error, "Could not record a refused secret use");
        }
        ReleaseError::Refused(refusal)
    }
}

/// The management side of a release request from an infrastructure peer.
pub(crate) async fn serve_release(
    ctx: &DriverContext,
    keys: ReleaseKeys,
    peer: NodeId,
    request: ReleaseRequest,
) -> Result<Released, ReleaseError> {
    let local = super::artifacts::local_config(ctx).await;
    let (config, _) = local.map_err(|error| ReleaseError::Unavailable(format!("{error:?}")))?;
    release(ctx, &keys, (&config, peer), &request).await
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
