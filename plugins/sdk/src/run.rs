//! The run a handler serves: its identity and settings, and the dispatch that gives a handler
//! its trigger's subject and keeps what it returns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;
use std::time::{Duration, SystemTime};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::bindings::aruna::plugin::types::{Failure, FailureKind, Invocation, Trigger};
use crate::datasets::{self, Dataset};
use crate::objects::{self, Object};
use crate::{Error, Form, Result, WatchEvent};

/// Who runs the handler, and the raw event of an event trigger.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Run {
    pub id: String,
    pub install: String,
    pub group: String,
    pub event: Option<WatchEvent>,
}

thread_local! {
    static CURRENT: RefCell<(Run, String)> = RefCell::default();
}

/// The run the handler serves; empty outside a handler.
pub fn run() -> Run {
    CURRENT.with(|current| current.borrow().0.clone())
}

/// The install's settings as a typed value.
pub fn settings<T: DeserializeOwned>() -> Result<T> {
    let text = CURRENT.with(|current| current.borrow().1.clone());
    serde_json::from_str(&text).map_err(|error| Error::Fail(format!("settings: {error}")))
}

/// A verified webhook request; authentication headers are already removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// The provider's delivery id, when the manifest names its header.
    pub delivery_id: Option<String>,
}

impl Request {
    /// The value of the first header named `name`, in any case.
    pub fn header(&self, name: &str) -> Option<&str> {
        (self.headers.iter())
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn text(&self) -> Result<&str> {
        std::str::from_utf8(&self.body)
            .map_err(|_| Error::InvalidInput("the body is not UTF-8".into()))
    }

    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body)
            .map_err(|error| Error::InvalidInput(format!("body: {error}")))
    }
}

/// A sync relationship's job that completed or failed, below the source path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncEvent {
    /// Whether the sync completed.
    pub ok: bool,
    pub path: String,
    pub relationship: String,
    pub job: String,
}

/// What a handler takes: its trigger's subject.
pub trait Subject: Sized {
    fn from_trigger(trigger: Trigger) -> Result<Self>;
}

impl Subject for Form {
    fn from_trigger(trigger: Trigger) -> Result<Self> {
        match trigger {
            Trigger::Manual(text) => Form::parse(&text),
            other => Err(mismatch("a form", &other)),
        }
    }
}

/// A timer's tick.
impl Subject for SystemTime {
    fn from_trigger(trigger: Trigger) -> Result<Self> {
        match trigger {
            Trigger::Timer(tick) => Ok(SystemTime::UNIX_EPOCH + Duration::from_millis(tick)),
            other => Err(mismatch("a timer tick", &other)),
        }
    }
}

impl Subject for Request {
    fn from_trigger(trigger: Trigger) -> Result<Self> {
        let Trigger::Webhook(delivery) = trigger else {
            return Err(mismatch("a webhook request", &trigger));
        };
        let request = delivery.request;
        let headers = (request.headers.into_iter())
            .map(|field| (field.name, field.value))
            .collect();
        Ok(Request {
            method: request.method,
            query: request.query,
            headers,
            body: request.body,
            delivery_id: delivery.delivery_id,
        })
    }
}

impl Subject for SyncEvent {
    fn from_trigger(trigger: Trigger) -> Result<Self> {
        match trigger {
            Trigger::Event(event) if event.kind.starts_with("sync_") => Ok(SyncEvent {
                ok: event.kind == "sync_completed",
                path: event.path,
                relationship: event.resource,
                job: event.revision,
            }),
            other => Err(mismatch("a sync event", &other)),
        }
    }
}

/// The created or updated dataset, loaded.
impl Subject for Dataset {
    fn from_trigger(trigger: Trigger) -> Result<Self> {
        match trigger {
            Trigger::Event(event) if event.kind.starts_with("dataset_") => {
                datasets::get(&event.resource)
            }
            other => Err(mismatch("a dataset", &other)),
        }
    }
}

/// The uploaded object, as it is when the run starts.
impl Subject for Object {
    fn from_trigger(trigger: Trigger) -> Result<Self> {
        match trigger {
            Trigger::Event(event) if event.kind == "data_uploaded" => objects::get(&event.resource),
            other => Err(mismatch("an uploaded object", &other)),
        }
    }
}

pub(crate) fn mismatch(wanted: &str, trigger: &Trigger) -> Error {
    let got = trigger_name(trigger);
    Error::InvalidInput(format!("the handler takes {wanted}, not a `{got}` run"))
}

/// A function the plugin registered: of its trigger's subject, or of nothing.
pub trait Handler<Args> {
    fn call(self, trigger: Trigger) -> Result<Option<String>>;
}

impl<F, T> Handler<()> for F
where
    F: FnOnce() -> Result<T>,
    T: Serialize,
{
    fn call(self, _trigger: Trigger) -> Result<Option<String>> {
        answer(self()?)
    }
}

impl<F, S, T> Handler<(S,)> for F
where
    F: FnOnce(S) -> Result<T>,
    S: Subject,
    T: Serialize,
{
    fn call(self, trigger: Trigger) -> Result<Option<String>> {
        answer(self(S::from_trigger(trigger)?)?)
    }
}

/// What the handler returned as the run's JSON output; nothing for `()` or `None`.
fn answer<T: Serialize>(value: T) -> Result<Option<String>> {
    match serde_json::to_value(value)? {
        Value::Null => Ok(None),
        value => Ok(Some(value.to_string())),
    }
}

/// Runs `handler` for one invocation; uncaught errors become the run outcome.
pub fn handle<A, H: Handler<A>>(
    invocation: Invocation,
    handler: H,
) -> Result<Option<String>, Failure> {
    let event = match &invocation.trigger {
        Trigger::Event(event) => Some(event.clone()),
        _ => None,
    };
    let run = Run {
        id: invocation.run,
        install: invocation.install,
        group: invocation.group,
        event,
    };
    CURRENT.with(|current| *current.borrow_mut() = (run, invocation.settings));
    let ended = handler.call(invocation.trigger);
    CURRENT.with(|current| *current.borrow_mut() = Default::default());
    ended.map_err(Error::failure)
}

/// The manifest name of the trigger that started a run.
pub fn trigger_name(trigger: &Trigger) -> &str {
    match trigger {
        Trigger::Manual(_) => "manual",
        Trigger::Webhook(_) => "webhook",
        Trigger::Event(event) => &event.kind,
        Trigger::Timer(_) => "timer",
    }
}

pub fn unhandled(trigger: &str) -> Failure {
    Failure {
        kind: FailureKind::InvalidInput,
        message: format!("this plugin has no handler for the trigger `{trigger}`"),
    }
}

#[cfg(test)]
#[path = "run_tests.rs"]
mod tests;
