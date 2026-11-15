//! Requests to the services the manifest declares, answered whole; the node adds their secrets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::bindings::aruna::plugin::services::{Method, Request};
use crate::bindings::aruna::plugin::types::Field;
use crate::host;
use crate::{Error, Result};

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
