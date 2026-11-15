//! Requests to the services the manifest declares, answered whole; the node adds their secrets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::bindings::aruna::plugin::jobs::{DownloadRequest, UploadRequest};
use crate::bindings::aruna::plugin::services::{Method, Request};
use crate::bindings::aruna::plugin::types::Field;
use crate::host;
use crate::{Error, Job, Result};

/// The service the manifest declares as `[services.<name>]`.
pub fn service(name: &str) -> Service {
    Service {
        name: name.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Service {
    name: String,
}

impl Service {
    pub fn get(&self, path: &str) -> Result<Reply> {
        self.request(Method::Get).path(path).send()
    }

    pub fn head(&self, path: &str) -> Result<Reply> {
        self.request(Method::Head).path(path).send()
    }

    pub fn delete(&self, path: &str) -> Result<Reply> {
        self.request(Method::Delete).path(path).send()
    }

    /// Posts `body` as JSON to the service's address.
    pub fn post(&self, body: &impl Serialize) -> Result<Reply> {
        self.request(Method::Post).json(body).send()
    }

    pub fn put(&self, body: &impl Serialize) -> Result<Reply> {
        self.request(Method::Put).json(body).send()
    }

    pub fn patch(&self, body: &impl Serialize) -> Result<Reply> {
        self.request(Method::Patch).json(body).send()
    }

    /// A request to build: a path below the address, a query, headers and a body.
    pub fn request(&self, method: Method) -> Call {
        Call {
            request: Request {
                service: self.name.clone(),
                method,
                path: String::new(),
                query: Vec::new(),
                headers: Vec::new(),
                body: Vec::new(),
            },
            check: true,
            invalid: None,
        }
    }

    /// A job that stores the whole answer at `path` as a new version of `to`; it needs write on
    /// `to`.
    pub fn download(&self, path: &str, to: &str) -> Download {
        Download(DownloadRequest {
            service: self.name.clone(),
            path: path.to_string(),
            query: Vec::new(),
            headers: Vec::new(),
            target: to.to_string(),
            size_limit: None,
            checksums: Vec::new(),
        })
    }

    /// A job that sends the object at `source` with PUT unless set otherwise; it needs read on
    /// `source`.
    pub fn upload(&self, source: &str) -> Upload {
        Upload(UploadRequest {
            service: self.name.clone(),
            method: Method::Put,
            path: String::new(),
            query: Vec::new(),
            headers: Vec::new(),
            source: source.to_string(),
            version: None,
            file_field: None,
            fields: Vec::new(),
        })
    }
}

/// A download to start: `service("ena").download("x", "raw/x").size_limit(n).start()?`.
#[derive(Debug, Clone)]
pub struct Download(DownloadRequest);

impl Download {
    pub fn query(mut self, name: &str, value: &str) -> Self {
        self.0.query.push(field(name, value));
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.0.headers.push(field(name, value));
        self
    }

    /// Fails the job when the answer has more bytes.
    pub fn size_limit(mut self, bytes: u64) -> Self {
        self.0.size_limit = Some(bytes);
        self
    }

    /// Fails the job unless the answer has this hex digest; `md5`, `sha1`, `sha256`, `crc32` or
    /// `crc32c`.
    pub fn checksum(mut self, algorithm: &str, hex: &str) -> Self {
        self.0.checksums.push(field(algorithm, hex));
        self
    }

    /// Starts the download; a rerun of the run gets the same job back.
    pub fn start(self) -> Result<Job> {
        let id = host::with(|host| host.job_download(&self.0))?;
        Ok(Job::started(id, "download", &self.0.target))
    }
}

/// An upload to start: `service("zenodo").upload("raw/x").path("files/x").start()?`.
#[derive(Debug, Clone)]
pub struct Upload(UploadRequest);

impl Upload {
    /// A path relative to the service's address.
    pub fn path(mut self, path: &str) -> Self {
        self.0.path = path.to_string();
        self
    }

    /// `Method::Put` or `Method::Post`; a POST is sent at most once.
    pub fn method(mut self, method: Method) -> Self {
        self.0.method = method;
        self
    }

    /// Sends this version of the source instead of its current one.
    pub fn version(mut self, version: &str) -> Self {
        self.0.version = Some(version.to_string());
        self
    }

    /// Sends a form with the object as its file part `name` beside the `field` values.
    pub fn file_field(mut self, name: &str) -> Self {
        self.0.file_field = Some(name.to_string());
        self
    }

    pub fn field(mut self, name: &str, value: &str) -> Self {
        self.0.fields.push(field(name, value));
        self
    }

    pub fn query(mut self, name: &str, value: &str) -> Self {
        self.0.query.push(field(name, value));
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.0.headers.push(field(name, value));
        self
    }

    /// Starts the upload; the service's answer is the job's `reply`.
    pub fn start(self) -> Result<Job> {
        let id = host::with(|host| host.job_upload(&self.0))?;
        Ok(Job::started(id, "upload", &self.0.source))
    }
}

/// One request being built; `send` makes it.
#[derive(Debug, Clone)]
pub struct Call {
    request: Request,
    check: bool,
    invalid: Option<String>,
}

impl Call {
    /// A path relative to the service's address.
    pub fn path(mut self, path: &str) -> Self {
        self.request.path = path.to_string();
        self
    }

    pub fn query(mut self, name: &str, value: &str) -> Self {
        self.request.query.push(field(name, value));
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.request.headers.push(field(name, value));
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.request.body = body.into();
        self
    }

    pub fn json(mut self, body: &impl Serialize) -> Self {
        match serde_json::to_vec(body) {
            Ok(bytes) => self.request.body = bytes,
            Err(error) => self.invalid = Some(error.to_string()),
        }
        self.header("content-type", "application/json")
    }

    /// Whether a status outside 200 to 299 is an error; it is unless turned off.
    pub fn check(mut self, check: bool) -> Self {
        self.check = check;
        self
    }

    pub fn send(self) -> Result<Reply> {
        if let Some(error) = self.invalid {
            return Err(Error::InvalidCall(format!("the body is not JSON: {error}")));
        }
        let response = host::with(|host| host.send(&self.request))?;
        let headers = (response.headers.into_iter())
            .map(|field| (field.name, field.value))
            .collect();
        let reply = Reply {
            status: response.status,
            headers,
            body: response.body,
        };
        match self.check && !(200..300).contains(&reply.status) {
            true => Err(Error::Service {
                status: reply.status,
                message: String::from_utf8_lossy(&reply.body)
                    .chars()
                    .take(200)
                    .collect(),
            }),
            false => Ok(reply),
        }
    }
}

/// A service's whole answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Reply {
    pub fn header(&self, name: &str) -> Option<&str> {
        (self.headers.iter())
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn text(&self) -> Result<String> {
        String::from_utf8(self.body.clone())
            .map_err(|_| Error::Fail("the answer is not UTF-8".into()))
    }

    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.body)?)
    }
}

fn field(name: &str, value: &str) -> Field {
    Field {
        name: name.to_string(),
        value: value.to_string(),
    }
}
