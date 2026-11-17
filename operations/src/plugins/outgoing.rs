//! The checked request of a plugin connection, built alike in a run and in a job: a connection
//! the pinned manifest declares, a declared method, a path below its address, the guest's headers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::injection::{OutgoingRequest, inject_secret};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::secrets::{Delivery, ResponseExposure, SecretFields, header_name};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugins::broker::{ErrorKind, HostError};
use aruna_plugins::connection::{Method, relative_path};
use aruna_plugins::manifest::Manifest;
use serde_json::Value;
use ulid::Ulid;
use url::Url;

use super::check::unavailable;
use super::held::ask_release;
use super::release::ReleaseKeys;
use super::work::RequestContext;
use crate::driver::DriverContext;
use aruna_core::plugins::{ReleaseError, ReleaseRequest};

/// Query or header pairs by name and value.
type Pairs<'a> = &'a [(String, String)];

/// The request a run asks for, with the connection's address resolved from the settings the run
/// was admitted with; a work row keeps it, so its job needs no run. No credential header is kept.
pub(crate) fn request_context(
    manifest: &Manifest,
    (connection, method): (&str, Method),
    settings: &str,
    (path, query, headers): (&str, Pairs<'_>, Pairs<'_>),
) -> Result<RequestContext, HostError> {
    let declared = manifest.connections.get(connection);
    let declared = declared.ok_or_else(|| invalid("the manifest has no such connection"))?;
    let settings: Value = serde_json::from_str(settings).unwrap_or(Value::Null);
    let base = declared.base(&settings).map_err(invalid)?;
    Ok(RequestContext {
        connection: connection.to_string(),
        base: base.to_string(),
        method: method.as_str().to_string(),
        path: path.to_string(),
        query: query.to_vec(),
        headers: (headers.iter())
            .filter(|(name, _)| guest_header(name))
            .cloned()
            .collect(),
    })
}

/// The request of `context` checked against `manifest`, the install's current one, and the secret
/// slot the connection injects. Credential headers of the guest never pass.
pub fn outgoing(
    manifest: &Manifest,
    context: &RequestContext,
) -> Result<(OutgoingRequest, Option<String>), HostError> {
    let connection = manifest.connections.get(&context.connection);
    let connection = connection.ok_or_else(|| invalid("the manifest has no such connection"))?;
    let method = method_named(&context.method);
    if !method.is_some_and(|method| connection.methods.contains(&method)) {
        return Err(invalid("the connection does not allow this method"));
    }
    if !relative_path(&context.path) {
        return Err(invalid("the path must stay below the connection's address"));
    }
    let mut url = Url::parse(&context.base).map_err(|_| invalid("the address does not parse"))?;
    let joined = format!("{}/{}", url.path().trim_end_matches('/'), context.path);
    url.set_path(&joined);
    if !context.query.is_empty() {
        url.query_pairs_mut().extend_pairs(context.query.iter());
    }
    let headers = (context.headers.iter())
        .filter(|(name, _)| guest_header(name))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
        .collect();
    let outgoing = OutgoingRequest {
        method: context.method.clone(),
        url,
        headers,
    };
    Ok((outgoing, connection.auth.clone()))
}

/// The realm keys this node releases group and realm secrets with, if it holds them.
pub(crate) fn node_keys(ctx: &DriverContext) -> ReleaseKeys {
    (ctx.metadata_handle.as_ref())
        .map(|handle| ReleaseKeys {
            seal: handle.realm_seal_key(),
            vault: handle.realm_vault_key(),
        })
        .unwrap_or_default()
}

/// Releases the secret of `slot` for one request of a work that run `run_id` started and adds it
/// to `outgoing`; answers the exposure the rule allows now and the fields it added.
pub(crate) async fn release_into(
    ctx: &DriverContext,
    (config, local, keys): (&RealmConfigDocument, NodeId, &ReleaseKeys),
    (install, slot, run_id): (&PluginInstall, &str, Ulid),
    outgoing: &mut OutgoingRequest,
) -> Result<(ResponseExposure, SecretFields), HostError> {
    let request = ReleaseRequest {
        install_id: install.install_id,
        slot: slot.to_string(),
        delivery: Delivery::Inject,
        target: Some((outgoing.method.clone(), outgoing.url.to_string())),
        held: None,
        run_id: Some(run_id),
    };
    let released = ask_release(ctx, config, (local, keys), request).await;
    let mut released = released.map_err(|error| match error {
        ReleaseError::Refused(_) => HostError::new(ErrorKind::Denied, error.to_string()),
        ReleaseError::Unavailable(why) => unavailable(why),
    })?;
    let bytes = released.fields.take();
    let bytes = bytes.ok_or_else(|| unavailable("the release answered no value"))?;
    let fields = SecretFields::decode(&bytes).map_err(|error| unavailable(error.to_string()))?;
    let injected = inject_secret(&released.kind, &released.settings, &fields, outgoing);
    injected.map_err(|error| HostError::new(ErrorKind::Denied, error.to_string()))?;
    Ok((released.exposure, fields))
}

fn method_named(name: &str) -> Option<Method> {
    use Method::{Delete, Get, Head, Patch, Post, Put};
    [Get, Head, Post, Put, Patch, Delete]
        .into_iter()
        .find(|method| method.as_str() == name)
}

/// Whether a guest may set header `name`: a valid name and no credential, which the host owns.
fn guest_header(name: &str) -> bool {
    let credential = ["authorization", "cookie", "proxy-authorization"];
    header_name(name) && !credential.contains(&name.to_ascii_lowercase().as_str())
}

fn invalid(why: &str) -> HostError {
    HostError::new(ErrorKind::Invalid, why)
}

#[cfg(test)]
#[path = "outgoing_tests.rs"]
mod tests;
