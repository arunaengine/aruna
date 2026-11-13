//! The host calls the sync uses, behind a trait, so the same sync code runs in the guest and
//! natively in tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::Value;

/// Why a host call failed, as the host reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    NotGranted,
    Denied,
    Invalid,
    /// A per-call or per-run bound, such as a reply too large.
    Limit,
    /// A revision or state version moved on.
    Conflict,
    /// The call may or may not have taken effect.
    Unknown,
    /// Not done; try again later.
    Unavailable,
    NotFound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fault {
    pub kind: Kind,
    pub message: String,
}

impl Fault {
    pub fn new(kind: Kind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

pub type Result<T, E = Fault> = std::result::Result<T, E>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Patch,
}

/// A request to eLabFTW below `/api/v2/experiments`; a changing request names itself by `key`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub method: Method,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub body: Option<Value>,
    pub key: Option<String>,
}

impl Request {
    pub fn get(path: impl Into<String>) -> Self {
        Self {
            method: Method::Get,
            path: path.into(),
            query: Vec::new(),
            body: None,
            key: None,
        }
    }

    pub fn send(method: Method, path: impl Into<String>, body: Value) -> Self {
        Self {
            method,
            body: Some(body),
            ..Self::get(path)
        }
    }
}

/// eLabFTW's whole answer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Reply {
    pub fn header(&self, name: &str) -> Option<&str> {
        let found = self
            .headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name));
        found.map(|(_, value)| value.as_str())
    }
}

/// A dataset entity at a revision.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Document {
    pub revision: String,
    pub json: Value,
}

/// A created dataset and its first revision.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Created {
    pub resource: String,
    pub revision: String,
}

/// One page of a dataset's file entities.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FilePage {
    pub files: Vec<Value>,
    pub cursor: Option<String>,
}

/// The host calls of one run. Every call is synchronous; changing calls carry a key where the
/// host keeps one across runs.
pub trait Host {
    fn elab(&self, request: &Request) -> Result<Reply>;
    /// The dataset entity without file links.
    fn summary(&self, dataset: &str) -> Result<Document>;
    fn files(&self, dataset: &str, cursor: Option<&str>) -> Result<FilePage>;
    fn patch(&self, dataset: &str, patch: &Value, expected: &str) -> Result<String>;
    fn put_file(&self, dataset: &str, file: &Value, expected: &str) -> Result<String>;
    fn remove_file(&self, dataset: &str, id: &str, expected: &str) -> Result<String>;
    /// A keyed create: the same key always gives the same document and its first revision.
    fn create(&self, path: &str, dataset: &Value, key: &str) -> Result<Created>;
    fn object_size(&self, path: &str) -> Result<u64>;
    /// Lists the first page below `prefix`; an empty page is fine.
    fn objects(&self, prefix: &str) -> Result<()>;
    fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>>;
    fn put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Result<u64>;
    fn log(&self, line: &str);
    /// Bytes this run has read and written so far, against the run's 16 MiB bound.
    fn spent(&self) -> u64;
}
