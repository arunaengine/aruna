//! Declares the blob crate: storage backends, staging sources, hashing and the blob handle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(clippy::result_large_err)]
#![recursion_limit = "256"]

pub mod arc;
mod autoindex;
pub mod bao_tree;
pub mod blob;
pub mod egress;
pub mod error;
mod framing;
mod fs_source;
mod fs_write;
pub mod git;
pub mod hash;
pub mod invenio;
mod messages;
pub mod opendal;
pub mod s3;
