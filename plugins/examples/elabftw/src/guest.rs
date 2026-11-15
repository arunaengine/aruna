//! The guest side: the host interface behind the sync's host trait, and the three handlers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{SystemTime, UNIX_EPOCH};

use aruna::bindings::aruna::plugin::types::{ErrorKind, Field, HostError};
use aruna::bindings::aruna::plugin::{metadata_create, metadata_read, metadata_write, services};
use aruna::bindings::aruna::plugin::{objects_read, state};
use aruna::{Error, Form, Level, Method as Verb};
use serde_json::Value;

use crate::host::Result;
use crate::host::{Created, Document, Fault, FilePage, Host, Kind, Meter, Method, Reply, Request};
use crate::sync::Sync;
use crate::values::Settings;

/// The timer: the quick pass, failed links and a chunk of the full reconciliation.
fn poll(tick: SystemTime) -> aruna::Result<Value> {
    let millis = tick
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64);
    let settings = aruna::settings::<Settings>()?;
    let (guest, run) = (Guest, aruna::run());
    let meter = Meter::new(&guest);
    Sync::new(&meter, &settings, &run.id, millis)
        .poll()
        .map_err(failure)
}

/// `dataset_created` and `dataset_updated`.
fn changed() -> aruna::Result<()> {
    let run = aruna::run();
    let change = run
        .event
        .clone()
        .ok_or_else(|| Error::InvalidInput("expected a dataset event".into()))?;
    let settings = aruna::settings::<Settings>()?;
    let meter = Meter::new(&Guest);
    let sync = Sync::new(&meter, &settings, &run.id, clock(&run.id));
    (sync.changed(&change.resource, &change.path, &change.revision)).map_err(failure)
}

/// A manual run: `report`, `resolve`, `resolve_create`, `reconcile` or `experiment`.
fn manual(form: Form) -> aruna::Result<Value> {
    let settings = aruna::settings::<Settings>()?;
    let run = aruna::run();
    let meter = Meter::new(&Guest);
    let sync = Sync::new(&meter, &settings, &run.id, clock(&run.id));
    sync.manual(&Value::Object(form.fields().clone()))
        .map_err(failure)
}

aruna::plugin! {
    timer => poll,
    dataset_created => changed,
    dataset_updated => changed,
    manual => manual,
}

/// The run's time from its id, a ULID, for runs without a timer tick.
fn clock(run: &str) -> u64 {
    const DIGITS: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let digits = run
        .chars()
        .take(10)
        .map(|digit| DIGITS.find(digit.to_ascii_uppercase()));
    digits.fold(0, |time, digit| {
        time * 32 + digit.unwrap_or_default() as u64
    })
}

/// Only an unavailable host or eLabFTW is worth a retry of the whole run.
fn failure(fault: Fault) -> Error {
    match fault.kind {
        Kind::Unavailable => Error::Unavailable(fault.message),
        Kind::Invalid => Error::InvalidInput(fault.message),
        _ => aruna::fail(fault.to_string()),
    }
}

fn fault(error: HostError) -> Fault {
    let kind = match error.kind {
        ErrorKind::NotGranted => Kind::NotGranted,
        ErrorKind::Denied => Kind::Denied,
        ErrorKind::Invalid => Kind::Invalid,
        ErrorKind::Limit => Kind::Limit,
        ErrorKind::Conflict => Kind::Conflict,
        ErrorKind::UnknownOutcome => Kind::Unknown,
        ErrorKind::Unavailable => Kind::Unavailable,
        ErrorKind::NotFound => Kind::NotFound,
    };
    Fault::new(kind, error.message)
}

fn text(value: &Value) -> Result<String> {
    serde_json::to_string(value).map_err(|error| Fault::new(Kind::Invalid, error.to_string()))
}

fn parsed(json: &str) -> Result<Value> {
    serde_json::from_str(json).map_err(|error| Fault::new(Kind::Invalid, error.to_string()))
}

/// The host interface itself; the sync keeps its revisions and keys, so it needs no library.
struct Guest;

impl Host for Guest {
    fn elab(&self, request: &Request) -> Result<Reply> {
        let method = match request.method {
            Method::Get => Verb::Get,
            Method::Post => Verb::Post,
            Method::Patch => Verb::Patch,
        };
        let body = match &request.body {
            Some(body) => text(body)?.into_bytes(),
            None => Vec::new(),
        };
        let field = |(name, value): (&str, &str)| Field {
            name: name.into(),
            value: value.into(),
        };
        let query = (request.query.iter())
            .map(|(name, value)| field((name, value)))
            .collect();
        let headers = match body.is_empty() {
            true => Vec::new(),
            false => vec![field(("content-type", "application/json"))],
        };
        let sent = services::Request {
            service: "elab".into(),
            method,
            path: request.path.clone(),
            query,
            headers,
            body,
        };
        let reply = services::send(&sent).map_err(fault)?;
        let headers = (reply.headers.into_iter()).map(|field| (field.name, field.value));
        Ok(Reply {
            status: reply.status,
            headers: headers.collect(),
            body: reply.body,
        })
    }

    fn summary(&self, dataset: &str) -> Result<Document> {
        let doc = metadata_read::summary(dataset).map_err(fault)?;
        Ok(Document {
            revision: doc.revision,
            json: parsed(&doc.json)?,
        })
    }

    fn files(&self, dataset: &str, cursor: Option<&str>) -> Result<FilePage> {
        let page = metadata_read::files(dataset, cursor).map_err(fault)?;
        let files = page.files.iter().map(|file| parsed(file));
        Ok(FilePage {
            files: files.collect::<Result<_>>()?,
            cursor: page.cursor,
        })
    }

    fn patch(&self, dataset: &str, patch: &Value, expected: &str) -> Result<String> {
        metadata_write::patch(dataset, &text(patch)?, expected).map_err(fault)
    }

    fn put_file(&self, dataset: &str, file: &Value, expected: &str) -> Result<String> {
        metadata_write::put_file(dataset, &text(file)?, expected).map_err(fault)
    }

    fn remove_file(&self, dataset: &str, id: &str, expected: &str) -> Result<String> {
        metadata_write::remove_file(dataset, id, expected).map_err(fault)
    }

    // The host names a create by its path, which the sync also uses as its key.
    fn create(&self, path: &str, dataset: &Value, _key: &str) -> Result<Created> {
        let created = metadata_create::create(path, &text(dataset)?).map_err(fault)?;
        Ok(Created {
            resource: created.resource,
            revision: created.revision,
        })
    }

    fn object_size(&self, path: &str) -> Result<u64> {
        objects_read::info(path, None)
            .map(|info| info.size)
            .map_err(fault)
    }

    fn objects(&self, prefix: &str) -> Result<()> {
        objects_read::list(prefix, None).map(drop).map_err(fault)
    }

    fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
        let entry = state::get(key).map_err(fault)?;
        Ok(entry.map(|entry| (entry.version, entry.value)))
    }

    fn put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Result<u64> {
        state::put(key, value, expected).map_err(fault)
    }

    fn log(&self, line: &str) {
        aruna::log(Level::Info, line);
    }
}
