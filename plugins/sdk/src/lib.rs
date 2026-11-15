//! Rust SDK for Aruna plugins: the generated host interface, and the first SDK of async handlers
//! over a context, listed by trigger with [`export!`].
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[doc(hidden)]
pub mod legacy;

pub use legacy::*;

/// The generated host interface. Code may call it directly; the SDK is only a convenience.
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
