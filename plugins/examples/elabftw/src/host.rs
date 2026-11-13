//! The host calls the sync uses, behind a trait, so the same sync code runs in the guest and
//! natively in tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::Cell;

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
    /// This run's budget is used; the work goes on in a later run. The sync's own kind.
    Spent,
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
}

/// Counts every host call of a run and the bytes it moves, for the run's budget.
pub struct Meter<'a> {
    inner: &'a dyn Host,
    calls: Cell<u32>,
    bytes: Cell<u64>,
}

impl<'a> Meter<'a> {
    pub fn new(inner: &'a dyn Host) -> Self {
        Self {
            inner,
            calls: Cell::new(0),
            bytes: Cell::new(0),
        }
    }

    /// Calls made and bytes moved so far.
    pub fn used(&self) -> (u32, u64) {
        (self.calls.get(), self.bytes.get())
    }

    fn count<T>(&self, sent: usize, result: Result<T>, size: impl Fn(&T) -> usize) -> Result<T> {
        let received = (result.as_ref()).map_or_else(|fault| fault.message.len(), size);
        self.calls.set(self.calls.get().saturating_add(1));
        let moved = (sent + received) as u64;
        self.bytes.set(self.bytes.get().saturating_add(moved));
        result
    }
}

fn json_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

impl Host for Meter<'_> {
    fn elab(&self, request: &Request) -> Result<Reply> {
        let query: usize = (request.query.iter())
            .map(|(name, value)| name.len() + value.len())
            .sum();
        let sent = request.path.len() + query + request.body.as_ref().map_or(0, json_len);
        let headers = |reply: &Reply| -> usize {
            (reply.headers.iter())
                .map(|(name, value)| name.len() + value.len())
                .sum()
        };
        let reply = self.inner.elab(request);
        self.count(sent, reply, |reply| reply.body.len() + headers(reply))
    }

    fn summary(&self, dataset: &str) -> Result<Document> {
        let reply = self.inner.summary(dataset);
        self.count(dataset.len(), reply, |doc| json_len(&doc.json))
    }

    fn files(&self, dataset: &str, cursor: Option<&str>) -> Result<FilePage> {
        let sent = dataset.len() + cursor.map_or(0, str::len);
        let reply = self.inner.files(dataset, cursor);
        self.count(sent, reply, |page| page.files.iter().map(json_len).sum())
    }

    fn patch(&self, dataset: &str, patch: &Value, expected: &str) -> Result<String> {
        let reply = self.inner.patch(dataset, patch, expected);
        self.count(json_len(patch) + expected.len(), reply, String::len)
    }

    fn put_file(&self, dataset: &str, file: &Value, expected: &str) -> Result<String> {
        let reply = self.inner.put_file(dataset, file, expected);
        self.count(json_len(file) + expected.len(), reply, String::len)
    }

    fn remove_file(&self, dataset: &str, id: &str, expected: &str) -> Result<String> {
        let reply = self.inner.remove_file(dataset, id, expected);
        self.count(id.len() + expected.len(), reply, String::len)
    }

    fn create(&self, path: &str, dataset: &Value, key: &str) -> Result<Created> {
        let reply = self.inner.create(path, dataset, key);
        let sent = path.len() + json_len(dataset) + key.len();
        self.count(sent, reply, |created| {
            created.resource.len() + created.revision.len()
        })
    }

    fn object_size(&self, path: &str) -> Result<u64> {
        self.count(path.len(), self.inner.object_size(path), |_| 8)
    }

    fn objects(&self, prefix: &str) -> Result<()> {
        self.count(prefix.len(), self.inner.objects(prefix), |_| 0)
    }

    fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
        let reply = self.inner.get(key);
        self.count(key.len(), reply, |entry| {
            entry.as_ref().map_or(0, |(_, value)| value.len())
        })
    }

    fn put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Result<u64> {
        let reply = self.inner.put(key, value, expected);
        self.count(key.len() + value.len(), reply, |_| 8)
    }

    fn log(&self, line: &str) {
        self.count(line.len(), Ok(()), |_| 0).ok();
        self.inner.log(line);
    }
}
