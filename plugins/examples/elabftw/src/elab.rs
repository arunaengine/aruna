//! eLabFTW API v2 calls below `/api/v2/experiments`, through the install's connection.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::{Value, json};

use crate::host::{Fault, Host, Kind, Method, Reply, Request, Result};
use crate::progress::Listed;
use crate::values::{Experiment, Upload, number};

/// Entries per listing page.
pub const PAGE: u64 = 50;
/// The per-call bound of the host; a request body must stay below it with its headers.
pub const MAX_CALL: usize = 1 << 20;
const HEADROOM: usize = 4096;

/// How a listing is sorted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// Newest `modified_at` first, for the quick pass.
    Changed,
    /// Lowest id first, for the full reconciliation.
    Id,
}

/// The status of a reply as a result: a missing experiment is `NotFound`, a refused write while
/// a user holds exclusive edit mode is worth a retry, other refusals are not.
fn checked(reply: Reply) -> Result<Reply> {
    let status = reply.status;
    if (200..300).contains(&status) {
        return Ok(reply);
    }
    let text = String::from_utf8_lossy(&reply.body)
        .chars()
        .take(200)
        .collect::<String>();
    let kind = match status {
        404 => Kind::NotFound,
        // eLabFTW 6.0 refuses with 400 "This entry is being edited by ..." in exclusive edit mode.
        400 if text.contains("being edited") => Kind::Unavailable,
        401 | 403 => Kind::Denied,
        400..=499 => Kind::Invalid,
        _ => Kind::Unavailable,
    };
    Err(Fault::new(
        kind,
        format!("eLabFTW answered {status}: {text}"),
    ))
}

fn json(reply: &Reply) -> Result<Value> {
    serde_json::from_slice(&reply.body)
        .map_err(|error| Fault::new(Kind::Invalid, format!("eLabFTW answer: {error}")))
}

/// Sends a changing request, refusing one that would not fit the per-call bound.
fn send(
    host: &dyn Host,
    method: Method,
    path: String,
    body: Value,
    key: Option<String>,
) -> Result<Reply> {
    let size = serde_json::to_vec(&body).map_or(0, |bytes| bytes.len());
    if size + HEADROOM > MAX_CALL {
        return Err(Fault::new(
            Kind::Limit,
            format!("eLabFTW write of {size} bytes"),
        ));
    }
    let request = Request {
        key,
        ..Request::send(method, path, body)
    };
    checked(host.elab(&request)?)
}

/// One page of tagged experiments.
pub fn list(host: &dyn Host, tag: &str, order: Order, offset: u64) -> Result<Vec<Listed>> {
    let (order, sort) = match order {
        Order::Changed => ("lastchange", "desc"),
        Order::Id => ("id", "asc"),
    };
    let mut request = Request::get("");
    request.query = [
        ("tags[]", tag.to_string()),
        ("order", order.into()),
        ("sort", sort.into()),
        ("limit", PAGE.to_string()),
        ("offset", offset.to_string()),
    ]
    .map(|(name, value)| (name.to_string(), value))
    .into();
    let entries = json(&checked(host.elab(&request)?)?)?;
    let entries = entries.as_array().map(Vec::as_slice).unwrap_or_default();
    Ok(entries.iter().filter_map(listed).collect())
}

fn listed(entry: &Value) -> Option<Listed> {
    Some(Listed {
        id: number(entry.get("id")?)?,
        modified: entry.get("modified_at")?.as_str()?.to_string(),
        pinned: entry.get("is_pinned").and_then(number).unwrap_or_default() == 1
            || entry.get("is_pinned").and_then(Value::as_bool) == Some(true),
        changed_by: entry
            .get("lastchangeby")
            .and_then(number)
            .unwrap_or_default(),
    })
}

/// Experiments whose extra field `field` holds `value`, through the extended search.
pub fn search(host: &dyn Host, field: &str, value: &str) -> Result<Vec<u64>> {
    let mut request = Request::get("");
    let query = format!("extrafield:\"{field}\":\"{value}\"");
    request.query = vec![
        ("extended".into(), query),
        ("limit".into(), PAGE.to_string()),
    ];
    let entries = json(&checked(host.elab(&request)?)?)?;
    let entries = entries.as_array().map(Vec::as_slice).unwrap_or_default();
    Ok(entries
        .iter()
        .filter_map(|entry| number(entry.get("id")?))
        .collect())
}

/// One experiment; `None` when eLabFTW no longer has it.
pub fn experiment(host: &dyn Host, id: u64) -> Result<Option<Experiment>> {
    match host.elab(&Request::get(id.to_string())).and_then(checked) {
        Ok(reply) => {
            let parsed = Experiment::parse(&json(&reply)?);
            parsed
                .map(Some)
                .ok_or_else(|| Fault::new(Kind::Invalid, "eLabFTW answered no experiment"))
        }
        Err(fault) if fault.kind == Kind::NotFound => Ok(None),
        Err(fault) => Err(fault),
    }
}

pub fn uploads(host: &dyn Host, id: u64) -> Result<Vec<Upload>> {
    let reply = checked(host.elab(&Request::get(format!("{id}/uploads")))?)?;
    let uploads = json(&reply)?;
    let uploads = uploads.as_array().map(Vec::as_slice).unwrap_or_default();
    Ok(uploads.iter().filter_map(Upload::parse).collect())
}

/// Creates an experiment and returns its id from the `Location` header. The key keeps a
/// repeated call from sending a second `POST`.
pub fn create(host: &dyn Host, body: Value, key: String) -> Result<u64> {
    let reply = send(host, Method::Post, String::new(), body, Some(key))?;
    let location = reply.header("location").unwrap_or_default();
    let id = location
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .and_then(|id| id.parse().ok());
    id.ok_or_else(|| Fault::new(Kind::Invalid, "eLabFTW answered no experiment location"))
}

pub fn patch(host: &dyn Host, id: u64, body: Value) -> Result<()> {
    send(host, Method::Patch, id.to_string(), body, None).map(drop)
}

pub fn add_tag(host: &dyn Host, id: u64, tag: &str) -> Result<()> {
    send(
        host,
        Method::Post,
        format!("{id}/tags"),
        json!({ "tag": tag }),
        None,
    )
    .map(drop)
}

pub fn remove_tag(host: &dyn Host, id: u64, tag: u64) -> Result<()> {
    let body = json!({ "action": "unreference" });
    send(host, Method::Patch, format!("{id}/tags/{tag}"), body, None).map(drop)
}
