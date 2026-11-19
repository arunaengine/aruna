//! One object read over the S3 endpoint of another member of a distributed bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use crate::jobs::ByteStream;

const VERSION_HEADER: &str = "x-amz-version-id";

/// One GET or HEAD the operation composed; the outbound backend sends it as it is. Its headers
/// carry a signed delegation and its url names the key, so neither shows in a `Debug` print.
#[derive(Clone, PartialEq)]
pub struct MemberFetch {
    pub head: bool,
    pub url: String,
    pub headers: Vec<(String, String)>,
}

impl std::fmt::Debug for MemberFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemberFetch")
    }
}

/// A member's successful answer: its status, its headers with lowercase names and its bytes.
#[derive(Debug, PartialEq)]
pub struct MemberBody {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub stream: ByteStream,
}

impl MemberBody {
    /// The first value of the header `name`, given in lowercase.
    pub fn header(&self, name: &str) -> Option<&str> {
        (self.headers.iter())
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }

    /// The version the member names for these bytes.
    pub fn version(&self) -> Option<Ulid> {
        Ulid::from_string(self.header(VERSION_HEADER)?).ok()
    }
}

/// What a member answered a [`MemberFetch`].
#[derive(Debug, PartialEq)]
pub enum MemberAnswer {
    Body(MemberBody),
    /// The member refused, with its status and the S3 error fields of its answer.
    Refused {
        status: u16,
        code: Option<String>,
        message: String,
    },
    /// The member could not be reached.
    Silent,
    /// The member did not start answering in time.
    Late,
}
