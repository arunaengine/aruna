//! Checks the plugin tests that build examples share: the opt-in, the toolchains, and limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;
use std::process::Command;

use aruna_plugins::limits::Limits;

/// Whether the opt-in is exactly `1` and `program` runs; prints why a language is skipped.
pub fn available(language: &str, program: &str) -> bool {
    if std::env::var("ARUNA_PLUGIN_LANGUAGES").as_deref() != Ok("1") {
        eprintln!("skipping {language}: set ARUNA_PLUGIN_LANGUAGES=1 to build the examples");
        return false;
    }
    let runs = Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success());
    if !runs {
        eprintln!("skipping {language}: `{program}` is not installed");
    }
    runs && (language != "rust" || wasm_target())
}

/// Whether the toolchain has the `wasm32-unknown-unknown` standard library.
fn wasm_target() -> bool {
    let libdir = Command::new("rustc")
        .args([
            "--print",
            "target-libdir",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| PathBuf::from(String::from_utf8_lossy(&out.stdout).trim()));
    let installed = libdir.is_some_and(|dir| dir.is_dir());
    if !installed {
        eprintln!("skipping rust: the wasm32-unknown-unknown target is not installed");
    }
    installed
}

pub fn limits() -> Limits {
    Limits {
        memory: 512 << 20,
        fuel: 1 << 40,
        ..Limits::default()
    }
}
