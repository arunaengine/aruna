//! The values a guest's host calls carry, as the node handles them; the runtime converts them to
//! and from the WIT types at the guest boundary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// One header or query pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub value: String,
}

/// Why a host call was refused or failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    NotGranted,
    Denied,
    Invalid,
    Limit,
    Conflict,
    UnknownOutcome,
    Unavailable,
    NotFound,
}

/// A refused or failed host call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostError {
    pub kind: ErrorKind,
    pub message: String,
}

impl HostError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// A metadata document or summary as JSON at its revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Document {
    pub resource: String,
    pub revision: String,
    pub json: String,
}

/// One page of the group's datasets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listing {
    pub resources: Vec<String>,
    pub cursor: Option<String>,
}

/// One page of the dataset's root-linked `File` entities as JSON objects, in `@id` order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilePage {
    pub files: Vec<String>,
    pub cursor: Option<String>,
}

/// A created dataset and its first revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Created {
    pub resource: String,
    pub revision: String,
}

/// One object version; `marked` means a job with a secret wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    pub path: String,
    pub size: u64,
    pub version: String,
    pub marked: bool,
}

/// One page of objects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectPage {
    pub objects: Vec<ObjectInfo>,
    pub cursor: Option<String>,
}

/// The method of a request through a service.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
}

/// Writes `Method::Post` as the WIT type did, since that text is in upload intents, logs and
/// test records.
impl fmt::Debug for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Get => "Method::Get",
            Self::Head => "Method::Head",
            Self::Post => "Method::Post",
            Self::Put => "Method::Put",
            Self::Patch => "Method::Patch",
            Self::Delete => "Method::Delete",
        })
    }
}

/// The status, and the headers and body the code may read of an answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<Field>,
    pub body: Vec<u8>,
}

/// A private state value at its version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub version: u64,
    pub value: Vec<u8>,
}

/// What a job did, read from its row after its record is gone too.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobStatus {
    pub id: String,
    pub kind: String,
    pub path: String,
    pub state: String,
    pub message: Option<String>,
    pub bytes: Option<u64>,
    pub version: Option<String>,
    pub reply_status: Option<u16>,
    pub reply_headers: Vec<Field>,
    pub reply_body: Option<Vec<u8>>,
    pub exit_code: Option<i32>,
    pub report: Option<String>,
}

/// Identity of one outside effect of a run. The ordinal names it; the digest of all arguments
/// checks a rerun asks for the same thing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Intent {
    pub ordinal: u32,
    pub digest: [u8; 32],
}

impl Intent {
    /// Digests length-prefixed parts, so no two argument lists share bytes.
    pub fn new(ordinal: u32, parts: &[&[u8]]) -> Self {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part);
        }
        Self {
            ordinal,
            digest: hasher.finalize().into(),
        }
    }
}

/// Applies a JSON merge patch to `target`, as a dataset patch call does. The SDK's fake host
/// keeps its own copy; `merge_cases.json` holds both to the same cases.
pub fn merge_into(target: &mut Value, patch: Value) {
    let Value::Object(changes) = patch else {
        *target = patch;
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Value::Object(fields) = target else {
        return;
    };
    for (key, value) in changes {
        match value {
            Value::Null => drop(fields.remove(&key)),
            value => merge_into(fields.entry(key).or_insert(Value::Null), value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_text_kept() {
        let methods = [
            (Method::Get, "Method::Get"),
            (Method::Head, "Method::Head"),
            (Method::Post, "Method::Post"),
            (Method::Put, "Method::Put"),
            (Method::Patch, "Method::Patch"),
            (Method::Delete, "Method::Delete"),
        ];
        for (method, text) in methods {
            assert_eq!(format!("{method:?}"), text);
        }
    }
}
