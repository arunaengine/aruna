//! Tests that the REST runtime builds its client URLs from a public URL.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::RestInterfaceRuntime;

#[test]
fn uses_public_url() {
    let runtime = RestInterfaceRuntime::from_bind_address(
        "0.0.0.0:3000".parse().unwrap(),
        Some("https://api.node-1.v3.aruna-engine.org/"),
    );
    assert_eq!(
        runtime.api_base_url,
        "https://api.node-1.v3.aruna-engine.org/api/v1"
    );
}
