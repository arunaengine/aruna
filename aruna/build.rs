//! Embeds the tracked demonstration environment file so the startup guard can compare values.
//! A build without that file embeds nothing, so the guard has no shipped value to match.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;

fn main() {
    let shipped = Path::new("..").join(".env");
    println!("cargo:rerun-if-changed=../.env");
    let contents = std::fs::read_to_string(&shipped).unwrap_or_default();
    let out =
        Path::new(&std::env::var("OUT_DIR").expect("cargo sets OUT_DIR")).join("shipped_env.rs");
    std::fs::write(out, format!("const SHIPPED_ENV: &str = {contents:?};\n"))
        .expect("write the shipped environment");
}
