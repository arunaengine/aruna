//! Tests that client URLs are built right from public, wildcard and IPv6 bind addresses.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{RestInterfaceRuntime, client_bind_url, client_host_url};

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

#[test]
fn rewrites_ipv6_url() {
    assert_eq!(
        client_bind_url("[::]:3000".parse().unwrap()),
        "http://[::1]:3000"
    );
}

#[test]
fn normalizes_s3_wildcards() {
    assert_eq!(
        client_host_url("0.0.0.0", "0.0.0.0:1337".parse().unwrap()),
        "http://127.0.0.1:1337"
    );
    assert_eq!(
        client_host_url("::", "[::]:1337".parse().unwrap()),
        "http://[::1]:1337"
    );
}

#[test]
fn preserves_s3_authority() {
    assert_eq!(
        client_host_url("127.0.0.1:1337", "0.0.0.0:9999".parse().unwrap()),
        "http://127.0.0.1:1337"
    );
    assert_eq!(
        client_host_url(
            "s3.node-1.v3.aruna-engine.org",
            "0.0.0.0:1337".parse().unwrap()
        ),
        "http://s3.node-1.v3.aruna-engine.org"
    );
}

#[test]
fn preserves_s3_scheme() {
    assert_eq!(
        client_host_url(
            "https://s3.node-1.v3.aruna-engine.org",
            "0.0.0.0:1337".parse().unwrap()
        ),
        "https://s3.node-1.v3.aruna-engine.org"
    );
    assert_eq!(
        client_host_url(
            "https://s3.node-1.v3.aruna-engine.org/",
            "0.0.0.0:1337".parse().unwrap()
        ),
        "https://s3.node-1.v3.aruna-engine.org"
    );
}
