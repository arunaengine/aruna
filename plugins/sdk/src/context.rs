//! Gives a handler its run identity, settings and the host calls its grants allow.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::bindings::aruna::plugin::{
    connections, grants, log, metadata_create, metadata_read, metadata_write, objects_read,
    secrets, state,
};
use crate::error::{Error, Result};

pub use connections::{FullResponse, Method};
pub use log::Level;
pub use metadata_create::Created;
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
        grants::granted(permission, resource)
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

    /// The fields of a secret slot granted through `secrets.value`, as name and value pairs.
    pub async fn secret(&self, slot: &str) -> Result<Vec<(String, String)>> {
        let fields = secrets::value(slot).map_err(|err| Error::host(err, "secrets.value", slot))?;
        Ok(fields
            .into_iter()
            .map(|field| (field.name, field.value))
            .collect())
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

/// One page of a dataset's file entities.
#[derive(Debug, Clone, PartialEq)]
pub struct FilePage {
    pub files: Vec<Value>,
    pub cursor: Option<String>,
}

/// Dataset metadata calls (`metadata.read`, `metadata.write`, `metadata.create`).
#[derive(Debug, Clone, Copy)]
pub struct Metadata;

impl Metadata {
    pub async fn get(&self, resource: &str) -> Result<Document> {
        let doc = metadata_read::get(resource)
            .map_err(|err| Error::host(err, "metadata.read", resource))?;
        Ok(Document {
            resource: doc.resource,
            revision: doc.revision,
            json: serde_json::from_str(&doc.json)?,
        })
    }

    pub async fn list(&self, cursor: Option<&str>) -> Result<Listing> {
        let page =
            metadata_read::list(cursor).map_err(|err| Error::host(err, "metadata.read", ""))?;
        Ok(Listing {
            resources: page.resources,
            cursor: page.cursor,
        })
    }

    /// The dataset entity without its file links, cheaper than [`Metadata::get`] for large datasets.
    pub async fn summary(&self, resource: &str) -> Result<Document> {
        let doc = metadata_read::summary(resource)
            .map_err(|err| Error::host(err, "metadata.read", resource))?;
        Ok(Document {
            resource: doc.resource,
            revision: doc.revision,
            json: serde_json::from_str(&doc.json)?,
        })
    }

    /// One page of the dataset's `File` entities, in `@id` order, and the cursor of the next one.
    pub async fn files(&self, resource: &str, cursor: Option<&str>) -> Result<FilePage> {
        let page = metadata_read::files(resource, cursor)
            .map_err(|err| Error::host(err, "metadata.read", resource))?;
        let files = page.files.iter().map(|file| serde_json::from_str(file));
        Ok(FilePage {
            files: files.collect::<std::result::Result<_, _>>()?,
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
        let reply = metadata_write::patch(resource, &text, expected, key);
        reply.map_err(|err| Error::host(err, "metadata.write", resource))
    }

    /// Adds or replaces one `File` entity by `@id` if the dataset is still at `expected`; the
    /// properties it omits go. Returns the new revision.
    pub async fn put_file(
        &self,
        resource: &str,
        file: &Value,
        expected: &str,
        key: Option<&str>,
    ) -> Result<String> {
        let text = serde_json::to_string(file)?;
        let reply = metadata_write::put_file(resource, &text, expected, key);
        reply.map_err(|err| Error::host(err, "metadata.write", resource))
    }

    /// Removes one `File` entity by `@id`, and the dataset's link to it, if the dataset is still
    /// at `expected`. Returns the new revision.
    pub async fn remove_file(
        &self,
        resource: &str,
        id: &str,
        expected: &str,
        key: Option<&str>,
    ) -> Result<String> {
        let reply = metadata_write::remove_file(resource, id, expected, key);
        reply.map_err(|err| Error::host(err, "metadata.write", resource))
    }

    /// Creates a dataset at `path` from one dataset entity; the same `key` always names the same
    /// document. Returns the document and its first revision.
    pub async fn create(&self, path: &str, dataset: &Value, key: &str) -> Result<Created> {
        let text = serde_json::to_string(dataset)?;
        metadata_create::create(path, &text, key)
            .map_err(|err| Error::host(err, "metadata.create", path))
    }
}

/// Object information calls (`objects.read`); never object bytes.
#[derive(Debug, Clone, Copy)]
pub struct Objects;

impl Objects {
    pub async fn info(&self, path: &str) -> Result<ObjectInfo> {
        let reply = objects_read::info(path);
        reply.map_err(|err| Error::host(err, "objects.read", path))
    }

    pub async fn list(&self, prefix: &str, cursor: Option<&str>) -> Result<ObjectPage> {
        let reply = objects_read::list(prefix, cursor);
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
        let reply = connections::send(&request);
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

impl Http {
    /// The whole answer, for a connection whose manifest says `response = "full"`.
    pub async fn send_full(
        &self,
        method: Method,
        path: &str,
        body: Vec<u8>,
        key: Option<&str>,
    ) -> Result<FullResponse> {
        self.send_full_with(method, path, &[], &[], body, key).await
    }

    /// Like [`Http::send_full`], with query pairs and request headers such as `content-type`.
    pub async fn send_full_with(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        headers: &[(&str, &str)],
        body: Vec<u8>,
        key: Option<&str>,
    ) -> Result<FullResponse> {
        let fields = |pairs: &[(&str, &str)]| {
            (pairs.iter())
                .map(|(name, value)| connections::Field {
                    name: name.to_string(),
                    value: value.to_string(),
                })
                .collect()
        };
        let request = connections::Request {
            connection: self.name.clone(),
            method,
            path: path.to_string(),
            query: fields(query),
            headers: fields(headers),
            body,
            key: key.map(str::to_string),
        };
        let reply = connections::send_full(&request);
        reply.map_err(|err| Error::host(err, "connections.send", &self.name))
    }
}

/// Small private values of the install, changed by compare and swap.
#[derive(Debug, Clone, Copy)]
pub struct State;

impl State {
    pub async fn get(&self, key: &str) -> Result<Option<Entry>> {
        let reply = state::get(key);
        reply.map_err(|err| Error::host(err, "state", key))
    }

    /// Writes `value` if the stored version is still `expected` (`None`: absent).
    pub async fn put(&self, key: &str, value: Vec<u8>, expected: Option<u64>) -> Result<u64> {
        let reply = state::put(key, &value, expected);
        reply.map_err(|err| Error::host(err, "state", key))
    }
}
