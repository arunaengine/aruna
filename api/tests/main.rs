//! Builds the API integration test files into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

mod bucket_overview;
mod deletion_routes;
mod git_auth;
mod mcp_transport;
