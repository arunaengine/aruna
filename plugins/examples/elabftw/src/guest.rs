//! The guest side: the SDK's host calls behind the sync's host trait, and the three handlers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::task::{Context as Task, Poll, Waker};

use aruna_plugin::{Context, Error, ErrorKind, Event, Level, Method as Verb};
use serde_json::Value;

use crate::host::{
    Created, Document, Fault, FilePage, Host, Kind, Meter, Method, Reply, Request, Result,
};
use crate::sync::Sync;
use crate::values::Settings;

/// The timer: the quick pass, failed links and a chunk of the full reconciliation.
async fn poll(ctx: Context, event: Event) -> aruna_plugin::Result<Value> {
    let (guest, settings) = (Guest::new(&ctx), ctx.settings::<Settings>()?);
    let meter = Meter::new(&guest);
    let sync = Sync::new(&meter, &settings, ctx.run(), clock(ctx.run(), &event));
    sync.poll().map_err(failure)
}

/// `dataset_created` and `dataset_updated`.
async fn changed(ctx: Context, event: Event) -> aruna_plugin::Result<()> {
    let Event::Change(change) = &event else {
        return Err(Error::Input("expected a dataset event".into()));
    };
    let (guest, settings) = (Guest::new(&ctx), ctx.settings::<Settings>()?);
    let meter = Meter::new(&guest);
    let sync = Sync::new(&meter, &settings, ctx.run(), clock(ctx.run(), &event));
    (sync.changed(&change.resource, &change.path, &change.revision)).map_err(failure)
}

/// A manual run: `report`, `resolve`, `resolve_create`, `reconcile` or `experiment`.
async fn manual(ctx: Context, event: Event) -> aruna_plugin::Result<Value> {
    let Event::Manual(input) = &event else {
        return Err(Error::Input("expected manual input".into()));
    };
    let (guest, settings) = (Guest::new(&ctx), ctx.settings::<Settings>()?);
    let meter = Meter::new(&guest);
    let sync = Sync::new(&meter, &settings, ctx.run(), clock(ctx.run(), &event));
    sync.manual(input).map_err(failure)
}

aruna_plugin::export!(poll, changed, manual);

/// The run's time: the timer's tick, else the time in the run id, a ULID.
fn clock(run: &str, event: &Event) -> u64 {
    if let Event::Timer(tick) = event {
        return *tick;
    }
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
        Kind::Unavailable => Error::Host {
            kind: ErrorKind::Unavailable,
            message: fault.message,
        },
        Kind::Invalid => Error::Input(fault.message),
        _ => Error::Failed(fault.to_string()),
    }
}

fn fault(error: Error) -> Fault {
    match error {
        Error::NotGranted {
            permission,
            resource,
        } => Fault::new(
            Kind::NotGranted,
            format!("{permission} is not granted for {resource}"),
        ),
        Error::Host { kind, message } => {
            let kind = match kind {
                ErrorKind::NotGranted => Kind::NotGranted,
                ErrorKind::Denied => Kind::Denied,
                ErrorKind::Invalid => Kind::Invalid,
                ErrorKind::Limit => Kind::Limit,
                ErrorKind::Conflict => Kind::Conflict,
                ErrorKind::UnknownOutcome => Kind::Unknown,
                ErrorKind::Unavailable => Kind::Unavailable,
                ErrorKind::NotFound => Kind::NotFound,
            };
            Fault::new(kind, message)
        }
        Error::Input(message) | Error::Failed(message) => Fault::new(Kind::Invalid, message),
    }
}

/// Every SDK call finishes on its first poll; a pending one is reported, never awaited.
fn ready<T>(future: impl Future<Output = aruna_plugin::Result<T>>) -> Result<T> {
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut Task::from_waker(Waker::noop())) {
        Poll::Ready(result) => result.map_err(fault),
        Poll::Pending => Err(Fault::new(Kind::Unavailable, "a host call did not finish")),
    }
}

struct Guest<'a> {
    ctx: &'a Context,
}

impl<'a> Guest<'a> {
    fn new(ctx: &'a Context) -> Self {
        Self { ctx }
    }
}

impl Host for Guest<'_> {
    fn elab(&self, request: &Request) -> Result<Reply> {
        let method = match request.method {
            Method::Get => Verb::Get,
            Method::Post => Verb::Post,
            Method::Patch => Verb::Patch,
        };
        let body = match &request.body {
            Some(body) => serde_json::to_vec(body)
                .map_err(|error| Fault::new(Kind::Invalid, error.to_string()))?,
            None => Vec::new(),
        };
        let query: Vec<(&str, &str)> = (request.query.iter())
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let json = [("content-type", "application/json")];
        let headers: &[(&str, &str)] = if body.is_empty() { &[] } else { &json };
        let http = self.ctx.http("elab");
        let key = request.key.as_deref();
        let reply = ready(http.send_full_with(method, &request.path, &query, headers, body, key))?;
        let headers = reply
            .headers
            .into_iter()
            .map(|field| (field.name, field.value));
        Ok(Reply {
            status: reply.status,
            headers: headers.collect(),
            body: reply.body,
        })
    }

    fn summary(&self, dataset: &str) -> Result<Document> {
        let doc = ready(self.ctx.metadata().summary(dataset))?;
        Ok(Document {
            revision: doc.revision,
            json: doc.json,
        })
    }

    fn files(&self, dataset: &str, cursor: Option<&str>) -> Result<FilePage> {
        let page = ready(self.ctx.metadata().files(dataset, cursor))?;
        Ok(FilePage {
            files: page.files,
            cursor: page.cursor,
        })
    }

    fn patch(&self, dataset: &str, patch: &Value, expected: &str) -> Result<String> {
        ready(self.ctx.metadata().patch(dataset, patch, expected))
    }

    fn put_file(&self, dataset: &str, file: &Value, expected: &str) -> Result<String> {
        ready(self.ctx.metadata().put_file(dataset, file, expected, None))
    }

    fn remove_file(&self, dataset: &str, id: &str, expected: &str) -> Result<String> {
        ready(self.ctx.metadata().remove_file(dataset, id, expected, None))
    }

    fn create(&self, path: &str, dataset: &Value, key: &str) -> Result<Created> {
        let created = ready(self.ctx.metadata().create(path, dataset, key))?;
        Ok(Created {
            resource: created.resource,
            revision: created.revision,
        })
    }

    fn object_size(&self, path: &str) -> Result<u64> {
        ready(self.ctx.objects().info(path)).map(|info| info.size)
    }

    fn objects(&self, prefix: &str) -> Result<()> {
        ready(self.ctx.objects().list(prefix, None)).map(drop)
    }

    fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
        let entry = ready(self.ctx.state().get(key))?;
        Ok(entry.map(|entry| (entry.version, entry.value)))
    }

    fn put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Result<u64> {
        ready(self.ctx.state().put(key, value.to_vec(), expected))
    }

    fn log(&self, line: &str) {
        self.ctx.log(Level::Info, line);
    }
}
