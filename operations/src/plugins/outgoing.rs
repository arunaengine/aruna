//! The checked request of a plugin connection, built alike in a run and in a job: a connection
//! the pinned manifest declares, a declared method, a path below its address, the guest's headers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::injection::OutgoingRequest;
use aruna_core::secrets::header_name;
use aruna_plugins::broker::{ErrorKind, HostError};
use aruna_plugins::connection::{Method, relative_path};
use aruna_plugins::manifest::Manifest;
use serde_json::Value;
use url::Url;

use super::work::RequestContext;

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
