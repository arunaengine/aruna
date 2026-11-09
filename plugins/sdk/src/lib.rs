//! Rust SDK for Aruna plugins: typed host calls over the generated plugin bindings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod context;
mod error;

pub use context::{
    Context, Document, Entry, Http, Level, Listing, Metadata, Method, ObjectInfo, ObjectPage,
    Objects, Response, State,
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
