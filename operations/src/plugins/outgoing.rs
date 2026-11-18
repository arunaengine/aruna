//! The checked request of a plugin connection, built alike in a run and in a job: a connection
//! the pinned manifest declares, a declared method, a path below its address, the guest's headers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::injection::OutgoingRequest;
use aruna_core::operation::Operation;
use aruna_core::outbound::{
    ConnectionAnswer, ConnectionCall, ConnectionError, OutboundEffect, OutboundEvent,
};
use aruna_core::secrets::header_name;
use aruna_plugins::broker::{ErrorKind, Field, HostError, Response};
use aruna_plugins::connection::{Method, relative_path};
use aruna_plugins::manifest::Manifest;
use base64::Engine;
use serde_json::Value;
use url::Url;

use super::unavailable;
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

/// The manifest's name of a method the guest called.
pub(crate) fn declared_method(
    method: aruna_plugins::broker::Method,
) -> aruna_plugins::connection::Method {
    use aruna_plugins::broker::Method as Called;
    use aruna_plugins::connection::Method;
    match method {
        Called::Get => Method::Get,
        Called::Head => Method::Head,
        Called::Post => Method::Post,
        Called::Put => Method::Put,
        Called::Patch => Method::Patch,
        Called::Delete => Method::Delete,
    }
}

/// The stored form of a whole answer, read back by [`stored_whole`].
pub(crate) fn whole_text(reply: &Response) -> String {
    let headers: Vec<(&str, &str)> = (reply.headers.iter())
        .map(|field| (field.name.as_str(), field.value.as_str()))
        .collect();
    let body = base64::engine::general_purpose::STANDARD.encode(&reply.body);
    serde_json::json!({ "status": reply.status, "headers": headers, "body": body }).to_string()
}

/// The stored reply of a sent request whose answer did not fit the call.
pub(crate) fn too_large(status: u16) -> String {
    serde_json::json!({ "status": status, "too_large": true }).to_string()
}

/// The refusal a stored too-large marker answers with, every time it is replayed.
fn refuse_large(value: &Value) -> Result<(), HostError> {
    if value.get("too_large").and_then(Value::as_bool) != Some(true) {
        return Ok(());
    }
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let message = format!("the answer with status {status} is larger than the call allows");
    Err(HostError::new(ErrorKind::Limit, message))
}

/// A whole answer read back from its stored form; a stored too-large marker is refused.
pub(crate) fn stored_whole(reply: &str) -> Result<Response, HostError> {
    let broken = || unavailable("a stored whole answer does not decode");
    let value: Value = serde_json::from_str(reply).map_err(|_| broken())?;
    refuse_large(&value)?;
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|s| u16::try_from(s).ok());
    let headers: Vec<(String, String)> =
        serde_json::from_value(value.get("headers").cloned().unwrap_or(Value::Null))
            .map_err(|_| broken())?;
    let body = value
        .get("body")
        .and_then(Value::as_str)
        .ok_or_else(broken)?;
    let body = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|_| broken())?;
    Ok(Response {
        status: status.ok_or_else(broken)?,
        headers: (headers.into_iter())
            .map(|(name, value)| Field { name, value })
            .collect(),
        body,
    })
}

fn invalid(why: &str) -> HostError {
    HostError::new(ErrorKind::Invalid, why)
}

/// Sends one request of a plugin connection and answers what the service said.
#[derive(Debug, PartialEq)]
pub(crate) struct SendConnection(Option<ConnectionCall>);

impl SendConnection {
    pub(crate) fn new(request: OutgoingRequest, body: Vec<u8>) -> Self {
        Self(Some(ConnectionCall { request, body }))
    }
}

impl Operation for SendConnection {
    type Output = ConnectionAnswer;
    type Error = ConnectionError;

    fn first(&mut self) -> Result<Next<ConnectionAnswer>, ConnectionError> {
        let call = self.0.take().ok_or(IoError::Unfinished)?;
        let effect = OutboundEffect::Connection(Box::new(call));
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Outbound(
            effect,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ConnectionAnswer>, ConnectionError> {
        let Event::Outbound(OutboundEvent::ConnectionAnswered { result }) = result.event()? else {
            let expected = "OutboundEvent::ConnectionAnswered";
            let got = "another event";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        Ok(Next::Done(result?))
    }
}

#[cfg(test)]
#[path = "outgoing_tests.rs"]
mod tests;
