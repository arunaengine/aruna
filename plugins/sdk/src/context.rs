//! Gives a handler its run identity, settings and the host calls its grants allow.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::bindings::aruna::plugin::{
    connections, grants, log, metadata_read, metadata_write, objects_read, state,
};
use crate::error::{Error, Result};

pub use connections::Method;
pub use log::Level;
pub use objects_read::{ObjectInfo, ObjectPage};
pub use state::Entry;

/// What every handler gets: who runs it, the install's settings and the host calls.
#[derive(Debug, Clone)]
pub struct Context {
    pub(crate) run: String,
    pub(crate) install: String,
    pub(crate) group: String,
    pub(crate) settings: Value,
}

impl Context {
    pub fn run(&self) -> &str {
        &self.run
    }

    pub fn install(&self) -> &str {
        &self.install
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    /// The install's settings as a typed value.
    pub fn settings<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_value(self.settings.clone())?)
    }

    pub fn setting(&self, name: &str) -> Option<&Value> {
        self.settings.get(name)
    }

    /// Whether an optional permission is granted for `resource`, before trying the call.
    pub async fn granted(&self, permission: &str, resource: &str) -> bool {
        grants::granted(permission.to_string(), resource.to_string()).await
    }

    pub fn metadata(&self) -> Metadata {
        Metadata
    }

    pub fn objects(&self) -> Objects {
        Objects
    }

    /// The connection the manifest declares under `name`.
    pub fn http(&self, name: &str) -> Http {
        Http {
            name: name.to_string(),
        }
    }

    pub fn state(&self) -> State {
        State
    }

    pub fn log(&self, level: Level, message: &str) {
        log::log(level, message);
    }
}

/// One dataset's metadata at a revision.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    pub resource: String,
    pub revision: String,
    pub json: Value,
}

/// One page of the group's datasets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    pub resources: Vec<String>,
    pub cursor: Option<String>,
}

/// Dataset metadata calls (`metadata.read`, `metadata.write`).
#[derive(Debug, Clone, Copy)]
pub struct Metadata;

impl Metadata {
    pub async fn get(&self, resource: &str) -> Result<Document> {
        let doc = metadata_read::get(resource.to_string())
            .await
            .map_err(|err| Error::host(err, "metadata.read", resource))?;
        Ok(Document {
            resource: doc.resource,
            revision: doc.revision,
            json: serde_json::from_str(&doc.json)?,
        })
    }

    pub async fn list(&self, cursor: Option<&str>) -> Result<Listing> {
        let page = metadata_read::list(cursor.map(str::to_string))
            .await
            .map_err(|err| Error::host(err, "metadata.read", ""))?;
        Ok(Listing {
            resources: page.resources,
            cursor: page.cursor,
        })
    }

    /// Applies `patch` if the metadata is still at `expected`; returns the new revision.
    pub async fn patch(&self, resource: &str, patch: &Value, expected: &str) -> Result<String> {
        self.patch_keyed(resource, patch, expected, None).await
    }

    /// Like [`Metadata::patch`], with a key that names this change across runs.
    pub async fn patch_keyed(
        &self,
        resource: &str,
        patch: &Value,
        expected: &str,
        key: Option<&str>,
    ) -> Result<String> {
        let text = serde_json::to_string(patch)?;
        let reply = metadata_write::patch(
            resource.to_string(),
            text,
            expected.to_string(),
            key.map(str::to_string),
        );
        reply
            .await
            .map_err(|err| Error::host(err, "metadata.write", resource))
    }
}

/// Object information calls (`objects.read`); never object bytes.
#[derive(Debug, Clone, Copy)]
pub struct Objects;

impl Objects {
    pub async fn info(&self, path: &str) -> Result<ObjectInfo> {
        let reply = objects_read::info(path.to_string()).await;
        reply.map_err(|err| Error::host(err, "objects.read", path))
    }

    pub async fn list(&self, prefix: &str, cursor: Option<&str>) -> Result<ObjectPage> {
        let reply = objects_read::list(prefix.to_string(), cursor.map(str::to_string)).await;
        reply.map_err(|err| Error::host(err, "objects.read", prefix))
    }
}

/// The answer of a connection: its status and the response fields the manifest allows.
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    pub status: u16,
    /// Allowed fields by JSON pointer, such as `/id`; the host drops everything else.
    pub fields: serde_json::Map<String, Value>,
}

impl Response {
    /// One allowed response field by its JSON pointer from the manifest, such as `/id`.
    pub fn field(&self, pointer: &str) -> Result<&Value> {
        let missing = || Error::failed(format!("the response has no field `{pointer}`"));
        self.fields.get(pointer).ok_or_else(missing)
    }
}

/// Requests through one declared connection; the host adds its secret.
#[derive(Debug, Clone)]
pub struct Http {
    name: String,
}

impl Http {
    pub async fn get(&self, path: &str) -> Result<Response> {
        self.send(Method::Get, path, Vec::new(), None).await
    }

    pub async fn post(&self, path: &str, body: &impl Serialize) -> Result<Response> {
        self.send(Method::Post, path, serde_json::to_vec(body)?, None)
            .await
    }

    pub async fn put(&self, path: &str, body: &impl Serialize) -> Result<Response> {
        self.send(Method::Put, path, serde_json::to_vec(body)?, None)
            .await
    }

    pub async fn delete(&self, path: &str) -> Result<Response> {
        self.send(Method::Delete, path, Vec::new(), None).await
    }

    /// Sends a request; `key` names a changing request across runs.
    pub async fn send(
        &self,
        method: Method,
        path: &str,
        body: Vec<u8>,
        key: Option<&str>,
    ) -> Result<Response> {
        let json_body = !body.is_empty();
        let headers = json_body
            .then(|| connections::Field {
                name: "content-type".into(),
                value: "application/json".into(),
            })
            .into_iter()
            .collect();
        let request = connections::Request {
            connection: self.name.clone(),
            method,
            path: path.to_string(),
            query: Vec::new(),
            headers,
            body,
            key: key.map(str::to_string),
        };
        let reply = connections::send(request).await;
        let reply = reply.map_err(|err| Error::host(err, "connections.send", &self.name))?;
        let fields = match reply.json {
            Some(text) => serde_json::from_str(&text)?,
            None => serde_json::Map::new(),
        };
        Ok(Response {
            status: reply.status,
            fields,
        })
    }
}

/// Small private values of the install, changed by compare and swap.
#[derive(Debug, Clone, Copy)]
pub struct State;

impl State {
    pub async fn get(&self, key: &str) -> Result<Option<Entry>> {
        let reply = state::get(key.to_string()).await;
        reply.map_err(|err| Error::host(err, "state", key))
    }

    /// Writes `value` if the stored version is still `expected` (`None`: absent).
    pub async fn put(&self, key: &str, value: Vec<u8>, expected: Option<u64>) -> Result<u64> {
        let reply = state::put(key.to_string(), value, expected).await;
        reply.map_err(|err| Error::host(err, "state", key))
    }
}
