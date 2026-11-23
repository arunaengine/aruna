//! Checks the plugin tests that build examples share: the toolchains and the limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;
use std::process::Command;

use aruna_wasm_host::limits::Limits;

/// Fails the test unless `program` runs and, for Rust, the wasm target is installed. The tests
/// that build examples are ignored by default, so only a run on purpose reaches this.
pub fn require(language: &str, program: &str) {
    let runs = Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success());
    assert!(runs, "{language}: `{program}` is not installed");
    if language == "rust" {
        assert!(
            wasm_target(),
            "rust: the wasm32-unknown-unknown target is not installed"
        );
    }
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
    libdir.is_some_and(|dir| dir.is_dir())
}

pub fn limits() -> Limits {
    Limits {
        memory: 512 << 20,
        fuel: 1 << 40,
        ..Limits::default()
    }
}
