//! Decides plugin transfers: every attempt checks the grants, the connection and the secret's
//! rule again before a byte moves, and what the service answered decides how the work ends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::injection::{OutgoingRequest, expose_response};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::secrets::{ResponseExposure, SecretFields};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission as Access};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_plugins::broker::{Field, HostError, Response};
use ulid::Ulid;

use super::check::CallError;
use super::held::{PrepareRequest, PreparedRequest};
use super::objects::{LocalObject, Unmarked};
use super::outgoing::whole_text;
use super::rows::StoreError;
use super::work::{Send, WorkChange, WorkRow, decided, retried, update_work};
use super::work_calls::TransferGrants;
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::put::PutObjectError;

/// The install's account restricted to `patterns` with WRITE, which also reads.
pub fn account(install: &PluginInstall, patterns: &BTreeSet<String>) -> AuthContext {
    let restrictions = (patterns.iter())
        .map(|pattern| PathRestriction {
            pattern: pattern.clone(),
            permission: Access::WRITE,
        })
        .collect();
    AuthContext {
        user_id: install.account,
        realm_id: install.account.realm_id,
        path_restrictions: Some(restrictions),
        session: None,
    }
}

/// How a transfer attempt ends.
#[derive(Debug, PartialEq)]
pub enum TransferEnd {
    Done {
        bytes: u64,
        version: Ulid,
    },
    Refused(HostError),
    Retry(String),
    Permanent(String),
    /// The request may have been sent and may not go twice.
    Undecided(String),
}

impl From<IoError> for TransferEnd {
    fn from(error: IoError) -> Self {
        Self::Retry(error.to_string())
    }
}

impl From<CallError> for TransferEnd {
    fn from(error: CallError) -> Self {
        Self::Refused(error.0)
    }
}

impl From<StoreError> for TransferEnd {
    fn from(error: StoreError) -> Self {
        Self::Retry(error.0)
    }
}

/// The admitted work one attempt moves bytes for.
#[derive(Clone, Copy)]
pub struct TransferWork<'a> {
    pub node: (RealmId, NodeId),
    pub config: &'a RealmConfigDocument,
    pub install: &'a PluginInstall,
    pub row: &'a WorkRow,
}

/// The checks shared by both directions, with the objects they need.
#[derive(Debug, PartialEq)]
struct Admitted {
    node: (RealmId, NodeId),
    config: Arc<RealmConfigDocument>,
    install: PluginInstall,
    row: WorkRow,
    path: String,
}

impl Admitted {
    fn of(work: TransferWork<'_>, path: &str) -> Self {
        Self {
            node: work.node,
            config: Arc::new(work.config.clone()),
            install: work.install.clone(),
            row: work.row.clone(),
            path: path.to_string(),
        }
    }

    fn object(&self) -> IoCall {
        IoCall::child(LocalObject::new(self.node, &self.install, &self.path))
    }

    fn request(&self) -> IoCall {
        let (config, local) = (self.config.clone(), self.node.1);
        IoCall::child(PrepareRequest::new(config, local, &self.install, &self.row))
    }
}

fn granted(result: IoResult) -> Result<BTreeSet<String>, TransferEnd> {
    Ok(result.child::<BTreeSet<String>, CallError>()??)
}

fn located(result: IoResult) -> Result<(String, String), TransferEnd> {
    Ok(result.child::<(String, String), CallError>()??)
}

fn prepared(result: IoResult) -> Result<PreparedRequest, TransferEnd> {
    Ok(result.child::<PreparedRequest, CallError>()??)
}

/// What a download sends and where its answer goes.
#[derive(Debug, PartialEq)]
pub struct DownloadPlan {
    pub patterns: BTreeSet<String>,
    pub bucket: String,
    pub key: String,
    pub outgoing: OutgoingRequest,
}

#[derive(Debug, PartialEq)]
enum DownloadPhase {
    Grants,
    Object(BTreeSet<String>),
    Committed(BTreeSet<String>, String, String),
    Request(BTreeSet<String>, String, String),
}

/// Checks a download. An earlier attempt that committed the version and lost its answer ends
/// the work without a request; only a rule that shows the whole answer lets it into a bucket.
#[derive(Debug, PartialEq)]
pub struct PrepareDownload {
    work: Admitted,
    version: Ulid,
    phase: DownloadPhase,
}

impl PrepareDownload {
    pub fn new(work: TransferWork<'_>, target: &str, version: Ulid) -> Self {
        Self {
            work: Admitted::of(work, target),
            version,
            phase: DownloadPhase::Grants,
        }
    }

    fn committed(&self, result: IoResult) -> Result<(), TransferEnd> {
        match result.child::<HeadObjectResult, HeadObjectError>()? {
            Ok(head) => Err(TransferEnd::Done {
                bytes: head.location.map_or(0, |location| location.blob_size),
                version: self.version,
            }),
            Err(HeadObjectError::NoSuchKey | HeadObjectError::NoSuchVersion) => Ok(()),
            Err(error) => Err(TransferEnd::Retry(error.to_string())),
        }
    }
}

impl Operation for PrepareDownload {
    type Output = DownloadPlan;
    type Error = TransferEnd;

    fn first(&mut self) -> Result<Next<DownloadPlan>, TransferEnd> {
        let work = &self.work;
        let grants = TransferGrants::download(&work.install, &work.row, &work.path);
        Ok(Next::Call(IoCall::child(grants)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DownloadPlan>, TransferEnd> {
        match std::mem::replace(&mut self.phase, DownloadPhase::Grants) {
            DownloadPhase::Grants => {
                self.phase = DownloadPhase::Object(granted(result)?);
                Ok(Next::Call(self.work.object()))
            }
            DownloadPhase::Object(patterns) => {
                let (bucket, key) = located(result)?;
                let head = HeadObjectOperation::new(HeadObjectInput {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: Some(self.version),
                });
                self.phase = DownloadPhase::Committed(patterns, bucket, key);
                Ok(Next::Call(IoCall::child(head)))
            }
            DownloadPhase::Committed(patterns, bucket, key) => {
                self.committed(result)?;
                self.phase = DownloadPhase::Request(patterns, bucket, key);
                Ok(Next::Call(self.work.request()))
            }
            DownloadPhase::Request(patterns, bucket, key) => {
                let request = prepared(result)?;
                if request.exposure != ResponseExposure::Full {
                    let narrow = "this connection does not expose its whole answer";
                    return Err(TransferEnd::Permanent(narrow.to_string()));
                }
                Ok(Next::Done(DownloadPlan {
                    patterns,
                    bucket,
                    key,
                    outgoing: request.outgoing,
                }))
            }
        }
    }
}

fn too_large(bound: u64) -> String {
    format!("the answer is larger than its size limit of {bound} bytes")
}

/// Whether a download's answer is written: a 2xx within the bound. A timeout, a throttle or a
/// server error is tried again.
pub fn check_answer(
    status: u16,
    length: Option<u64>,
    bound: Option<u64>,
) -> Result<(), TransferEnd> {
    match status {
        200..=299 => {}
        408 | 429 | 500..=599 => {
            return Err(TransferEnd::Retry(format!("the service answered {status}")));
        }
        _ => {
            let answered = format!("the service answered {status}");
            return Err(TransferEnd::Permanent(answered));
        }
    }
    match length.zip(bound) {
        Some((length, bound)) if length > bound => Err(TransferEnd::Permanent(too_large(bound))),
        _ => Ok(()),
    }
}

/// How a download ends whose write failed; `over` and `broken` tell what its body did.
pub fn write_failure(
    error: &PutObjectError,
    (over, broken): (bool, bool),
    bound: Option<u64>,
) -> TransferEnd {
    if over {
        return TransferEnd::Permanent(too_large(bound.unwrap_or_default()));
    }
    match (broken, error) {
        (true, _) | (_, PutObjectError::StorageError(_)) => TransferEnd::Retry(error.to_string()),
        _ => TransferEnd::Permanent(error.to_string()),
    }
}

/// What an upload reads and sends.
#[derive(Debug, PartialEq)]
pub struct UploadPlan {
    pub patterns: BTreeSet<String>,
    pub bucket: String,
    pub key: String,
    pub version: Ulid,
    pub again: bool,
    pub request: PreparedRequest,
}

#[derive(Debug, PartialEq)]
enum UploadPhase {
    Grants,
    Object(BTreeSet<String>),
    Readable(BTreeSet<String>, String, String),
    Request(BTreeSet<String>, String, String, Ulid),
}

/// Checks an upload. A request begun or answered before decides without the source, which may
/// be gone by now; the version checked is the version sent.
#[derive(Debug, PartialEq)]
pub struct PrepareUpload {
    work: Admitted,
    version: Option<Ulid>,
    again: bool,
    phase: UploadPhase,
}

impl PrepareUpload {
    pub fn new(work: TransferWork<'_>, source: &str, version: Option<Ulid>) -> Self {
        let context = work.row.context.as_ref();
        Self {
            again: context.is_some_and(|context| context.method == "PUT"),
            work: Admitted::of(work, source),
            version,
            phase: UploadPhase::Grants,
        }
    }
}

impl Operation for PrepareUpload {
    type Output = UploadPlan;
    type Error = TransferEnd;

    fn first(&mut self) -> Result<Next<UploadPlan>, TransferEnd> {
        let work = &self.work;
        let grants = TransferGrants::upload(&work.install, &work.row, &work.path);
        Ok(Next::Call(IoCall::child(grants)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<UploadPlan>, TransferEnd> {
        match std::mem::replace(&mut self.phase, UploadPhase::Grants) {
            UploadPhase::Grants => {
                let patterns = granted(result)?;
                let earlier = decided(self.work.row.boundary.as_ref(), self.again);
                if let Some(end) = earlier.and_then(|send| send_end(send, self.again)) {
                    return Err(end);
                }
                self.phase = UploadPhase::Object(patterns);
                Ok(Next::Call(self.work.object()))
            }
            UploadPhase::Object(patterns) => {
                let (bucket, key) = located(result)?;
                let readable = Unmarked::new((&bucket, &key), self.version);
                self.phase = UploadPhase::Readable(patterns, bucket, key);
                Ok(Next::Call(IoCall::child(readable)))
            }
            UploadPhase::Readable(patterns, bucket, key) => {
                let version = result.child::<Option<Ulid>, CallError>()??;
                let version = version.ok_or_else(source_gone)?;
                self.phase = UploadPhase::Request(patterns, bucket, key, version);
                Ok(Next::Call(self.work.request()))
            }
            UploadPhase::Request(patterns, bucket, key, version) => Ok(Next::Done(UploadPlan {
                patterns,
                bucket,
                key,
                version,
                again: self.again,
                request: prepared(result)?,
            })),
        }
    }
}

/// The end of an upload whose source no longer exists.
pub fn source_gone() -> TransferEnd {
    TransferEnd::Permanent("the object to upload is gone".to_string())
}

/// How an upload ends that an earlier attempt decided: unknown after a begun request that may
/// not go twice, else by the kept answer. `None` when the attempt sends.
pub fn send_end(send: Send, again: bool) -> Option<TransferEnd> {
    match send {
        Send::Now => None,
        Send::Undecided => {
            let unknown = "the upload may have been sent; its outcome is unknown";
            Some(TransferEnd::Undecided(unknown.to_string()))
        }
        Send::Answered {
            status,
            bytes,
            version,
            ..
        } => Some(answered_end(status, again, (bytes, version))),
    }
}

/// How an answered upload ends: a 2xx succeeds; a request that may go twice is tried again
/// after a timeout, a throttle or a server error; anything else fails.
fn answered_end(status: u16, again: bool, (bytes, version): (u64, Ulid)) -> TransferEnd {
    match status {
        200..=299 => TransferEnd::Done { bytes, version },
        _ if again && retried(status) => {
            TransferEnd::Retry(format!("the service answered {status}"))
        }
        _ => TransferEnd::Permanent(format!("the service answered {status}")),
    }
}

/// What the service answered to an upload; `body` is `None` when it was not read whole.
#[derive(Debug, PartialEq)]
pub struct UploadReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

/// Keeps an upload's answer as the rule shows it now, and decides how the work ends. A reply
/// too large for the bound keeps no body.
#[derive(Debug, PartialEq)]
pub struct KeepReply {
    ids: (Ulid, Ulid),
    exposure: ResponseExposure,
    fields: SecretFields,
    sent: (u64, Ulid),
    again: bool,
    reply: UploadReply,
}

impl KeepReply {
    pub fn new(ids: (Ulid, Ulid), plan: UploadPlan, size: u64, reply: UploadReply) -> Self {
        Self {
            ids,
            exposure: plan.request.exposure,
            fields: plan.request.fields,
            sent: (size, plan.version),
            again: plan.again,
            reply,
        }
    }

    fn shown(&self) -> Option<String> {
        let UploadReply {
            status,
            headers,
            body,
        } = &self.reply;
        let body = body.as_ref()?;
        let shown = expose_response(&self.exposure, &self.fields, *status, headers, body);
        let headers = (shown.headers.into_iter()).map(|(name, value)| Field { name, value });
        Some(whole_text(&Response {
            status: shown.status,
            headers: headers.collect(),
            body: shown.body.unwrap_or_default(),
        }))
    }
}

impl Operation for KeepReply {
    type Output = TransferEnd;
    type Error = TransferEnd;

    fn first(&mut self) -> Result<Next<TransferEnd>, TransferEnd> {
        let (bytes, version) = self.sent;
        let change = WorkChange::Sent {
            status: self.reply.status,
            reply: self.shown(),
            bytes,
            version,
        };
        Ok(Next::Call(IoCall::child(update_work(self.ids, change))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TransferEnd>, TransferEnd> {
        result.child::<Option<WorkRow>, StoreError>()??;
        let end = answered_end(self.reply.status, self.again, self.sent);
        Ok(Next::Done(end))
    }
}

#[cfg(test)]
#[path = "transfer_steps_tests.rs"]
mod tests;
