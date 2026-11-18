//! One object read over the S3 endpoint of another member of a distributed bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::jobs::ByteStream;

/// One GET the operation composed; the blob backend sends it as it is. Its headers carry a
/// signed delegation and its url names the key, so neither is shown in a `Debug` print.
#[derive(PartialEq)]
pub struct MemberGet {
    pub url: String,
    pub headers: Vec<(String, String)>,
}

impl std::fmt::Debug for MemberGet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemberGet")
    }
}

/// What a member answered a [`MemberGet`].
#[derive(Debug, PartialEq)]
pub enum MemberAnswer {
    /// The object's bytes and the version the member names for them.
    Body {
        version: Option<String>,
        stream: ByteStream,
    },
    /// The member refused, with its status and the S3 error code of its answer.
    Refused { status: u16, code: Option<String> },
    /// The member could not be reached or did not start answering in time.
    Silent,
}
