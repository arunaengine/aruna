//! Rust SDK for Aruna plugins: handlers are async functions over synchronous host calls, listed
//! once by trigger with [`export!`].
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod context;
mod error;

use std::future::Future;

use serde_json::Value;

pub use context::{
    Context, Created, Document, Entry, FilePage, Http, Level, Listing, Metadata, Method,
    ObjectInfo, ObjectPage, Objects, Response, State,
};
pub use error::{Error, Result};

#[doc(hidden)]
pub mod bindings {
    wit_bindgen::generate!({
        world: "plugin",
        path: "../wit",
        pub_export_macro: true,
        export_macro_name: "export_guest",
        default_bindings_module: "aruna_plugin::bindings",
        additional_derives: [PartialEq, Eq],
        generate_all,
    });
}

pub use bindings::aruna::plugin::types::{ErrorKind, Field, WatchEvent, WebhookRequest};
use bindings::aruna::plugin::types::{Failure, FailureKind, Invocation, Trigger};

/// Why the host started a handler.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Someone ran the plugin by hand, with optional JSON input.
    Manual(Value),
    Webhook {
        delivery_id: Option<String>,
        request: WebhookRequest,
    },
    /// A committed change in the group, such as a new dataset.
    Change(WatchEvent),
    /// A timer tick, in milliseconds since the Unix epoch.
    Timer(u64),
}

/// A new dataset, from a `dataset_created` event or a manual run given a dataset id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetEvent {
    pub dataset: String,
}

/// What a handler accepts as its second argument.
pub trait Input: Sized {
    fn from_event(event: Event) -> Result<Self>;
}

impl Input for Event {
    fn from_event(event: Event) -> Result<Self> {
        Ok(event)
    }
}

impl Input for () {
    fn from_event(_: Event) -> Result<Self> {
        Ok(())
    }
}

impl Input for DatasetEvent {
    fn from_event(event: Event) -> Result<Self> {
        let dataset = match event {
            Event::Change(change) if change.kind == "dataset_created" => Some(change.resource),
            Event::Manual(Value::String(id)) => Some(id),
            Event::Manual(value) => value
                .get("dataset")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        };
        dataset
            .map(|dataset| Self { dataset })
            .ok_or_else(|| Error::Input("expected a dataset event or a dataset id".into()))
    }
}

/// What a handler may return; the host keeps it as the run's bounded output.
pub trait Output {
    fn into_output(self) -> Result<Option<String>>;
}

impl Output for () {
    fn into_output(self) -> Result<Option<String>> {
        Ok(None)
    }
}

impl Output for String {
    fn into_output(self) -> Result<Option<String>> {
        Ok(Some(self))
    }
}

impl Output for Value {
    fn into_output(self) -> Result<Option<String>> {
        Ok(Some(serde_json::to_string(&self)?))
    }
}

/// Lists a plugin's handlers by trigger, such as `export! { dataset_created => announce }`. Each
/// is `async fn(Context, I) -> Result<O>`; `aruna-cli plugin build` checks the triggers against
/// the manifest.
#[macro_export]
macro_rules! export {
    ($($trigger:ident => $handler:path),+ $(,)?) => {
        const _: () = {
            struct Exported;

            impl $crate::bindings::Guest for Exported {
                fn handle(
                    invocation: $crate::bindings::aruna::plugin::types::Invocation,
                ) -> ::std::result::Result<
                    Option<String>,
                    $crate::bindings::aruna::plugin::types::Failure,
                > {
                    match $crate::trigger_name(&invocation.trigger) {
                        $(stringify!($trigger) => $crate::dispatch(invocation, $handler),)+
                        other => Err($crate::unknown(other)),
                    }
                }

                fn triggers() -> Vec<String> {
                    vec![$(stringify!($trigger).to_string()),+]
                }
            }

            $crate::bindings::export_guest!(Exported with_types_in $crate::bindings);
        };
    };
}

/// The manifest name of the trigger that started a run.
#[doc(hidden)]
pub fn trigger_name(trigger: &Trigger) -> &str {
    match trigger {
        Trigger::Manual(_) => "manual",
        Trigger::Webhook(_) => "webhook",
        Trigger::Event(event) => &event.kind,
        Trigger::Timer(_) => "every",
    }
}

#[doc(hidden)]
pub fn dispatch<I, O, F, Fut>(invocation: Invocation, handler: F) -> Result<Option<String>, Failure>
where
    I: Input,
    O: Output,
    F: FnOnce(Context, I) -> Fut,
    Fut: Future<Output = Result<O>>,
{
    let outcome = async {
        let settings = serde_json::from_str(&invocation.settings)
            .map_err(|err| Error::Input(format!("settings: {err}")))?;
        let context = Context {
            run: invocation.run,
            install: invocation.install,
            group: invocation.group,
            settings,
        };
        let input = I::from_event(event(invocation.trigger)?)?;
        handler(context, input).await?.into_output()
    };
    // Every SDK call is synchronous underneath, so one poll runs the handler to its end.
    let mut outcome = std::pin::pin!(outcome);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match outcome.as_mut().poll(&mut context) {
        std::task::Poll::Ready(result) => result.map_err(Error::failure),
        std::task::Poll::Pending => Err(Failure {
            kind: FailureKind::Permanent,
            message: "a handler may only await SDK calls".into(),
        }),
    }
}

#[doc(hidden)]
pub fn unknown(trigger: &str) -> Failure {
    Failure {
        kind: FailureKind::InvalidInput,
        message: format!("this plugin has no handler for the trigger `{trigger}`"),
    }
}

fn event(trigger: Trigger) -> Result<Event> {
    Ok(match trigger {
        Trigger::Manual(text) if text.is_empty() => Event::Manual(Value::Null),
        Trigger::Manual(text) => Event::Manual(
            serde_json::from_str(&text)
                .map_err(|err| Error::Input(format!("manual input: {err}")))?,
        ),
        Trigger::Webhook(delivery) => Event::Webhook {
            delivery_id: delivery.delivery_id,
            request: delivery.request,
        },
        Trigger::Event(change) => Event::Change(change),
        Trigger::Timer(tick) => Event::Timer(tick),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_inputs() {
        let manual = |value: Value| DatasetEvent::from_event(Event::Manual(value));
        let expected = DatasetEvent {
            dataset: "01J".into(),
        };
        assert_eq!(manual(Value::String("01J".into())), Ok(expected.clone()));
        assert_eq!(
            manual(serde_json::json!({ "dataset": "01J" })),
            Ok(expected)
        );
        assert!(manual(Value::Null).is_err());
        let other = WatchEvent {
            kind: "data_uploaded".into(),
            path: "p".into(),
            resource: "r".into(),
            revision: "1".into(),
        };
        assert!(DatasetEvent::from_event(Event::Change(other)).is_err());
    }
}
