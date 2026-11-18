//! Invenio adapter: the calls and answers of InvenioRDM repository transfers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult};
use aruna_core::repository::LinkFailure;
use aruna_core::repository::remote::{
    InvenioAccess, InvenioError, InvenioMethod, JobStop, RemoteAnswer, RemoteCall, RemoteFailure,
};
use aruna_core::structs::execution::job::ArtifactRef;
use aruna_core::structs::execution::source_access::SourceMetadata;
use serde_json::Value;
use url::Url;

use super::{TransferError, remote_answer, remote_call};

pub(crate) mod export;
pub(crate) mod import;
pub mod query;
pub(crate) mod reference;
pub mod remote;
mod verify;

impl From<InvenioError> for TransferError {
    fn from(error: InvenioError) -> Self {
        match error {
            InvenioError::Transport | InvenioError::Status(429 | 500..=599) => {
                Self::Retryable(error.to_string())
            }
            InvenioError::Status(401 | 403) => Self::Refused(LinkFailure::TokenRejected),
            _ => Self::Permanent(error.to_string()),
        }
    }
}

/// An opened repository connector: builds the calls of one transfer step.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Session {
    access: InvenioAccess,
    stop: Option<JobStop>,
}

impl Session {
    pub(crate) fn new(access: InvenioAccess, stop: Option<JobStop>) -> Self {
        Self { access, stop }
    }

    pub(crate) fn access(&self) -> &InvenioAccess {
        &self.access
    }

    pub(crate) fn endpoint(&self) -> &str {
        self.access.endpoint()
    }

    pub(crate) fn url(&self, segments: &[&str]) -> Result<Url, TransferError> {
        Ok(self.access.url(segments)?)
    }

    pub(crate) fn link(&self, link: &str) -> Result<Url, TransferError> {
        Ok(self.access.link(link)?)
    }

    pub(crate) fn stop(&self) -> Option<JobStop> {
        self.stop.clone()
    }

    fn call(&self, call: RemoteCall) -> IoCall {
        remote_call(call, self.stop.clone())
    }

    fn json(&self, method: InvenioMethod, url: Url, body: Option<Value>) -> IoCall {
        self.call(RemoteCall::InvenioJson {
            access: self.access.clone(),
            method,
            url,
            body,
            if_match: None,
        })
    }

    pub(crate) fn get(&self, url: Url) -> IoCall {
        self.json(InvenioMethod::Get, url, None)
    }

    pub(crate) fn post(&self, url: Url, body: Option<Value>) -> IoCall {
        self.json(InvenioMethod::Post, url, body)
    }

    pub(crate) fn put(&self, url: Url, body: Value) -> IoCall {
        self.json(InvenioMethod::Put, url, Some(body))
    }

    /// Replaces the resource if it still has `revision`.
    pub(crate) fn update(&self, url: Url, body: Value, revision: u64) -> IoCall {
        self.call(RemoteCall::InvenioJson {
            access: self.access.clone(),
            method: InvenioMethod::Put,
            url,
            body: Some(body),
            if_match: Some(revision),
        })
    }

    pub(crate) fn delete(&self, url: Url) -> IoCall {
        self.call(RemoteCall::InvenioDelete {
            access: self.access.clone(),
            url,
        })
    }

    pub(crate) fn head(&self, url: Url) -> IoCall {
        self.call(RemoteCall::InvenioHead {
            access: self.access.clone(),
            url,
        })
    }

    pub(crate) fn upload(
        &self,
        url: Url,
        size: u64,
        artifact: &ArtifactRef,
        range: std::ops::Range<u64>,
    ) -> IoCall {
        self.call(RemoteCall::InvenioUpload {
            access: self.access.clone(),
            url,
            size,
            source: artifact.location.clone(),
            range,
        })
    }
}

/// The answer to a repository call, a refused request kept apart from any other failure.
pub(crate) fn reply(result: IoResult) -> Result<Result<RemoteAnswer, InvenioError>, TransferError> {
    match remote_answer(result)? {
        Ok(answer) => Ok(Ok(answer)),
        Err(RemoteFailure::Invenio(error)) => Ok(Err(error)),
        Err(failure) => Err(failure.into()),
    }
}

fn wrong_answer() -> TransferError {
    TransferError::Retryable("unexpected repository answer".into())
}

/// The JSON body of a call, a refused request kept apart from any other failure.
pub(crate) fn json_reply(result: IoResult) -> Result<Result<Value, InvenioError>, TransferError> {
    match reply(result)? {
        Ok(RemoteAnswer::Json(value)) => Ok(Ok(value)),
        Ok(_) => Err(wrong_answer()),
        Err(error) => Ok(Err(error)),
    }
}

/// The JSON body of a call that must succeed.
pub(crate) fn json(result: IoResult) -> Result<Value, TransferError> {
    Ok(json_reply(result)??)
}

/// The JSON body of a read, `None` when the resource is gone.
pub(crate) fn optional_json(result: IoResult) -> Result<Option<Value>, TransferError> {
    match json_reply(result)? {
        Ok(value) => Ok(Some(value)),
        Err(InvenioError::Status(404 | 410)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// The end of a call that answers nothing.
pub(crate) fn finished(result: IoResult) -> Result<(), TransferError> {
    match reply(result)?? {
        RemoteAnswer::Done => Ok(()),
        _ => Err(wrong_answer()),
    }
}

/// The metadata of a head call.
pub(crate) fn head_metadata(result: IoResult) -> Result<SourceMetadata, TransferError> {
    match reply(result)?? {
        RemoteAnswer::Metadata(metadata) => Ok(metadata),
        _ => Err(wrong_answer()),
    }
}

pub(crate) fn invalid(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}
