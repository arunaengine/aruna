//! Declares the broker that runs host calls after the runtime's grant and budget checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::permission::Permission;

pub use crate::host::wit::aruna::plugin::connections::{FullResponse, Method, Response};
pub use crate::host::wit::aruna::plugin::metadata_read::{Document, Listing};
pub use crate::host::wit::aruna::plugin::objects_read::{ObjectInfo, ObjectPage};
pub use crate::host::wit::aruna::plugin::state::Entry;
pub use crate::host::wit::aruna::plugin::types::{ErrorKind, Field, HostError};

/// Identity of the run that issued a host call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallContext {
    pub run: String,
    pub install: String,
    /// The broker must not buffer a reply larger than this.
    pub max_bytes: usize,
}

/// Identity of one outside effect of a run. The ordinal names it, the digest of all
/// arguments checks a rerun asks for the same thing; an author key names it across runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Intent {
    pub ordinal: u32,
    pub digest: [u8; 32],
    pub key: Option<String>,
}

impl Intent {
    /// Digests length-prefixed parts, so no two argument lists share bytes.
    pub fn new(ordinal: u32, key: Option<String>, parts: &[&[u8]]) -> Self {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part);
        }
        Self {
            ordinal,
            digest: hasher.finalize().into(),
            key,
        }
    }
}

/// A revision-checked metadata change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataPatch {
    pub resource: String,
    pub patch: String,
    pub expected: String,
    pub intent: Intent,
}

/// A request through a declared connection; mutating methods carry an intent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendRequest {
    pub connection: String,
    pub method: Method,
    pub path: String,
    pub query: Vec<Field>,
    pub headers: Vec<Field>,
    pub body: Vec<u8>,
    pub intent: Option<Intent>,
}

/// A compare-and-swap write of a private state value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatePut {
    pub key: String,
    pub value: Vec<u8>,
    pub expected: Option<u64>,
    pub intent: Intent,
}

/// Runs host calls under the install's current grants and authority. The runtime has
/// already checked that the permission or connection is granted at all and the budgets.
#[async_trait]
pub trait Broker: Send + Sync {
    /// Whether the permission is granted for this resource now.
    async fn granted(&self, cx: &CallContext, permission: Permission, resource: &str) -> bool;
    async fn metadata_get(&self, cx: &CallContext, resource: &str) -> Result<Document, HostError>;
    async fn metadata_list(
        &self,
        cx: &CallContext,
        cursor: Option<String>,
    ) -> Result<Listing, HostError>;
    async fn metadata_patch(
        &self,
        cx: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError>;
    async fn objects_info(&self, cx: &CallContext, path: &str) -> Result<ObjectInfo, HostError>;
    async fn objects_list(
        &self,
        cx: &CallContext,
        prefix: &str,
        cursor: Option<String>,
    ) -> Result<ObjectPage, HostError>;
    /// Sends through a declared connection; the reply is what the guest may read, already
    /// limited to the exposure, with the fields as an object keyed by JSON pointer.
    async fn connection_send(
        &self,
        cx: &CallContext,
        request: SendRequest,
    ) -> Result<Response, HostError>;
    /// Sends through a connection whose manifest reads whole answers; the reply is the whole
    /// answer within the exposure. A broker that serves none refuses it.
    async fn connection_send_full(
        &self,
        _cx: &CallContext,
        _request: SendRequest,
    ) -> Result<FullResponse, HostError> {
        Err(HostError::new(
            ErrorKind::Unavailable,
            "whole answers are not served here",
        ))
    }
    async fn state_get(&self, cx: &CallContext, key: &str) -> Result<Option<Entry>, HostError>;
    async fn state_put(&self, cx: &CallContext, put: StatePut) -> Result<u64, HostError>;
    /// Records that a write did not finish within its deadline, so its outcome is unknown.
    /// The run's slot is held until this returns or its own deadline passes.
    async fn abandoned(&self, cx: &CallContext, intent: &Intent);
    /// Keeps one guest log line, already cut to its bound; a broker may drop it.
    async fn log(&self, _cx: &CallContext, _level: &str, _message: &str) {}
}

impl HostError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}
