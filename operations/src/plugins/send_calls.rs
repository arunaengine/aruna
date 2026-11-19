//! Runs a plugin connection call as one operation: the checked request with its secret, then the
//! send through the run's call history, or the native transfer work that sends it later.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::injection::expose_response;
use aruna_core::operation::Operation;
use aruna_core::outbound::{ConnectionAnswer, ConnectionError};
use aruna_core::plugin_runs::CallIntent;
use aruna_core::secrets::{ResponseExposure, SecretFields};
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::job::UploadForm;
use aruna_plugin_types::calls::{ErrorKind, Field, HostError, Response};
use ulid::Ulid;

use super::check::{CallError, RunScope};
use super::held::{ReadyConnection, ReadyRequest};
use super::host_calls::{CallEnd, CallFailure, RecordCall, begun_reply};
use super::host_steps::{ConnectionAsk, NativeCall};
use super::intents::{Begun, begin};
use super::outgoing::{SendConnection, stored_whole, too_large, whole_text};
use super::rows::StoreError;
use super::submit::{DownloadRequest, UploadRequest, WorkRequest};
use super::unavailable;
use crate::jobs::records::pages::ServingNode;

/// The refusal of a send whose secret rule an admin narrowed below the whole answer.
const NARROWER: &str = "the secret's use rule no longer lets this plugin read the answer";

fn denied(message: &str) -> CallFailure {
    CallError(HostError::new(ErrorKind::Denied, message)).into()
}

/// The whole answer as the code may see it, with echoes of the secret removed.
pub fn whole(fields: &SecretFields, answer: &ConnectionAnswer) -> Response {
    let shown = expose_response(
        &ResponseExposure::Full,
        fields,
        answer.status,
        &answer.headers,
        &answer.body,
    );
    Response {
        status: shown.status,
        headers: (shown.headers.into_iter())
            .map(|(name, value)| Field { name, value })
            .collect(),
        body: shown.body.unwrap_or_default(),
    }
}

/// A send that has no stored intent: an answer that does not fit is a limit, others unavailable.
fn send_refusal(error: ConnectionError) -> HostError {
    match error {
        ConnectionError::TooLarge { .. } => HostError::new(ErrorKind::Limit, error.to_string()),
        error => unavailable(error.to_string()),
    }
}

#[derive(Debug, PartialEq)]
enum SendPhase {
    Ready,
    Begin,
    Send,
    Record,
}

/// Sends a connection request and answers the whole answer. Install writes every rule of a plugin
/// secret with the whole answer; one an admin narrowed by hand refuses the call instead of
/// filtering it. A call with an intent sends once and answers its stored reply again.
#[derive(Debug, PartialEq)]
pub struct SendCall {
    ready: Option<ReadyConnection>,
    run_id: Ulid,
    body: Vec<u8>,
    intent: Option<CallIntent>,
    max_bytes: usize,
    request: Option<ReadyRequest>,
    phase: SendPhase,
}

impl SendCall {
    pub fn new(
        (scope, holder): (&RunScope, Ulid),
        ask: ConnectionAsk,
        (body, intent): (Vec<u8>, Option<CallIntent>),
        max_bytes: usize,
    ) -> Self {
        Self {
            ready: Some(ReadyConnection::new((scope, holder), ask)),
            run_id: scope.run.run_id,
            body,
            intent,
            max_bytes,
            request: None,
            phase: SendPhase::Ready,
        }
    }

    fn send(&mut self) -> Result<Next<Response>, CallFailure> {
        let request = self.request.as_ref().ok_or(IoError::Unfinished)?;
        let body = std::mem::take(&mut self.body);
        self.phase = SendPhase::Send;
        let send = SendConnection::new(request.outgoing.clone(), body);
        Ok(Next::Call(IoCall::child(send)))
    }

    /// What the call's intent keeps of an answer: the whole answer, or a marker when it does not
    /// fit the call.
    fn outcome(&self, sent: Result<ConnectionAnswer, ConnectionError>) -> Result<String, CallEnd> {
        let fields = (self.request.as_ref()).map(|request| &request.fields);
        let empty = SecretFields::new(Default::default());
        match sent {
            Ok(answer) => {
                let shown = whole(fields.unwrap_or(&empty), &answer);
                let head: usize = (shown.headers.iter())
                    .map(|field| field.name.len() + field.value.len())
                    .sum();
                Ok(match head + shown.body.len() + 2 > self.max_bytes {
                    true => too_large(answer.status),
                    false => whole_text(&shown),
                })
            }
            Err(ConnectionError::TooLarge { status }) => Ok(too_large(status)),
            Err(ConnectionError::NotSent(why)) => Err(CallEnd::None(unavailable(why))),
            Err(ConnectionError::Unknown(why)) => Err(CallEnd::Maybe(why)),
        }
    }
}

impl Operation for SendCall {
    type Output = Response;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<Response>, CallFailure> {
        let ready = self.ready.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(ready)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Response>, CallFailure> {
        match self.phase {
            SendPhase::Ready => {
                let request = result.child::<ReadyRequest, CallError>()??;
                if request.exposure != ResponseExposure::Full {
                    return Err(denied(NARROWER));
                }
                self.request = Some(request);
                let Some(intent) = self.intent.clone() else {
                    return self.send();
                };
                self.phase = SendPhase::Begin;
                Ok(Next::Call(IoCall::child(begin(self.run_id, intent))))
            }
            SendPhase::Begin => {
                let begun = result.child::<Begun, StoreError>()?;
                let begun = begun.map_err(|error| CallError(unavailable(error.0)))?;
                match begun_reply(begun)? {
                    Some(reply) => Ok(Next::Done(stored_whole(&reply)?)),
                    None => self.send(),
                }
            }
            SendPhase::Send => {
                let sent = result.child::<ConnectionAnswer, ConnectionError>()?;
                let Some(intent) = self.intent.clone() else {
                    let request = self.request.as_ref().ok_or(IoError::Unfinished)?;
                    let answer = sent.map_err(|error| CallError(send_refusal(error)))?;
                    return Ok(Next::Done(whole(&request.fields, &answer)));
                };
                let record = RecordCall::new(self.run_id, intent, self.outcome(sent));
                self.phase = SendPhase::Record;
                Ok(Next::Call(IoCall::child(record)))
            }
            SendPhase::Record => {
                let recorded = result.child::<String, CallFailure>()?;
                let reply = recorded.map_err(|failure| CallError(failure.error))?;
                Ok(Next::Done(stored_whole(&reply)?))
            }
        }
    }
}

/// The transfer a run asks for, before its request is checked.
#[derive(Clone, Debug, PartialEq)]
pub enum Transfer {
    /// A download whose whole answer lands in `target`.
    Download {
        target: String,
        checksums: Vec<(ChecksumAlgorithm, Vec<u8>)>,
        max_bytes: Option<u64>,
        ordinal: u32,
    },
    /// An upload of the object at `source`, as one body or a form.
    Upload {
        source: String,
        version: Option<Ulid>,
        form: Option<UploadForm>,
        ordinal: u32,
    },
}

/// Checks a transfer's connection request with its secret released, then starts its native
/// work through the run's call history. A download needs a rule that shows the whole answer.
#[derive(Debug, PartialEq)]
pub struct TransferCall {
    ready: Option<ReadyConnection>,
    scope: RunScope,
    intent: CallIntent,
    transfer: Option<Transfer>,
    place: ((u64, Ulid), ServingNode),
}

impl TransferCall {
    pub fn new(
        (scope, holder): (&RunScope, Ulid),
        ask: ConnectionAsk,
        (transfer, intent): (Transfer, CallIntent),
        place: ((u64, Ulid), ServingNode),
    ) -> Self {
        Self {
            ready: Some(ReadyConnection::new((scope, holder), ask)),
            scope: scope.clone(),
            intent,
            transfer: Some(transfer),
            place,
        }
    }

    fn request(transfer: Transfer, ready: ReadyRequest) -> Result<WorkRequest, CallFailure> {
        Ok(match transfer {
            Transfer::Download {
                target,
                checksums,
                max_bytes,
                ordinal,
            } => {
                // The whole answer lands in a bucket, so the rule must show it whole.
                if ready.exposure != ResponseExposure::Full {
                    return Err(denied("this connection does not expose its whole answer"));
                }
                WorkRequest::Download(DownloadRequest {
                    context: ready.context,
                    target,
                    checksums,
                    max_bytes,
                    secret: ready.secret,
                    ordinal,
                })
            }
            Transfer::Upload {
                source,
                version,
                form,
                ordinal,
            } => WorkRequest::Upload(UploadRequest {
                context: ready.context,
                source,
                version,
                form,
                ordinal,
            }),
        })
    }
}

impl Operation for TransferCall {
    type Output = String;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<String>, CallFailure> {
        let ready = self.ready.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(ready)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, CallFailure> {
        let Some(transfer) = self.transfer.take() else {
            return Ok(Next::Done(result.child::<String, CallFailure>()??));
        };
        let ready = result.child::<ReadyRequest, CallError>()??;
        let request = Self::request(transfer, ready)?;
        let intent = self.intent.clone();
        let native = NativeCall::new(&self.scope, intent, request, self.place);
        Ok(Next::Call(IoCall::child(native)))
    }
}

#[cfg(test)]
#[path = "send_calls_tests.rs"]
mod tests;
