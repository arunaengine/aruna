//! Groups the S3 gateway parts: auth, checksums, CORS, errors, scopes, server and services.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod auth;
mod browse;
pub mod checksum;
mod cors;
mod error;
pub mod multipart_join;
mod scope;
pub mod server;
pub mod service;
pub mod util;
