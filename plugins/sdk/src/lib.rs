//! The Rust library for Aruna plugins: handlers are plain functions of their trigger's subject,
//! listed by trigger with [`plugin!`], over module functions for every host call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod datasets;
mod form;
mod host;
pub mod jobs;
pub mod objects;
mod outcome;
mod run;
mod service;
pub mod state;

use std::collections::BTreeMap;

pub use bindings::aruna::plugin::log::Level;
pub use bindings::aruna::plugin::services::Method;
pub use bindings::aruna::plugin::types::WatchEvent;
pub use datasets::Dataset;
pub use form::Form;
pub use jobs::Job;
pub use objects::Object;
pub use outcome::{Error, Result, fail, retry};
#[doc(hidden)]
pub use run::{Handler, handle, trigger_name, unhandled};
pub use run::{Request, Run, Subject, SyncEvent, run, settings};
pub use service::{Call, Reply, Service, service};

/// The generated host interface. Code may call it directly; the library is only a convenience.
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

/// The fields of a secret the manifest lists under `[secrets]`, by name; they are the code's
/// data then. Secrets of services are added by the node and never reach the code.
pub fn secret(name: &str) -> Result<BTreeMap<String, String>> {
    let fields = host::with(|host| host.secret(name))?;
    Ok(fields
        .into_iter()
        .map(|field| (field.name, field.value))
        .collect())
}

/// Keeps one line in the run's log: `log!("{n} done")` at info, or `log!(warn, "...")`.
pub fn log(level: Level, message: &str) {
    host::with(|host| host.log(level, message));
}

#[macro_export]
macro_rules! log {
    (debug, $($arg:tt)+) => { $crate::log($crate::Level::Debug, &format!($($arg)+)) };
    (info, $($arg:tt)+) => { $crate::log($crate::Level::Info, &format!($($arg)+)) };
    (warn, $($arg:tt)+) => { $crate::log($crate::Level::Warn, &format!($($arg)+)) };
    (error, $($arg:tt)+) => { $crate::log($crate::Level::Error, &format!($($arg)+)) };
    ($($arg:tt)+) => { $crate::log($crate::Level::Info, &format!($($arg)+)) };
}

/// Lists a plugin's handlers by trigger, such as `plugin! { dataset_created => announce }`. A
/// handler takes its trigger's subject, or nothing, and returns `Result<T>` for any serializable
/// `T`; `aruna-cli plugin build` checks the triggers against the manifest's `on`.
#[macro_export]
macro_rules! plugin {
    ($($trigger:ident => $handler:path),+ $(,)?) => {
        const _: () = {
            struct Plugin;

            impl $crate::bindings::Guest for Plugin {
                fn handle(
                    invocation: $crate::bindings::aruna::plugin::types::Invocation,
                ) -> ::std::result::Result<
                    Option<String>,
                    $crate::bindings::aruna::plugin::types::Failure,
                > {
                    match $crate::trigger_name(&invocation.trigger) {
                        $(stringify!($trigger) => $crate::handle(invocation, $handler),)+
                        other => Err($crate::unhandled(other)),
                    }
                }

                fn triggers() -> Vec<String> {
                    vec![$(stringify!($trigger).to_string()),+]
                }
            }

            $crate::bindings::export_guest!(Plugin with_types_in $crate::bindings);
        };
    };
}
