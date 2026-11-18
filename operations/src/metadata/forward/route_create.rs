//! Creates a metadata document here when this node holds its bucket, otherwise at a holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structured_id::StructuredId;
use aruna_core::{MetaResourceId, NodeId};
use tracing::{error, warn};
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::replay::create_record_matches;
use crate::forward::routing::{create_forward_holders, distinct_holders};
use crate::forward::transport::{MetadataWriteError, unexpected_response};
use crate::metadata::call_results::effect_event;
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentResult,
    mint_forward_document,
};
use crate::metadata::create_entry::CreateEntryOperation;
use crate::metadata::protocol::{
    MetadataTransportMessage, WriteAuthError, decode_message, encode_message,
};

/// How long one holder may take to answer a forwarded create, which validates and commits it.
const FORWARD_CREATE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Copy, PartialEq)]
enum RouteStage {
    Local,
    Config,
    Forward,
}

/// Creates locally when the origin holds the bucket, otherwise at a holder of the bucket. A
/// request that definitely was not sent may try the next holder; an ambiguous delivery is
/// terminal, so the create is never replayed.
#[derive(Debug, PartialEq)]
pub struct RouteCreateOperation {
    template: Option<CreateDocumentOperation>,
    config: CreateDocumentConfig,
    commit_message: Option<String>,
    auth_token: Option<AuthToken>,
    local_node: Option<NodeId>,
    stage: RouteStage,
    document_id: Ulid,
    placement: Option<PlacementRef>,
    frame: Vec<u8>,
    holders: VecDeque<NodeId>,
    asked: usize,
    failures: Vec<String>,
}

type RouteTurn = Result<Next<CreateDocumentResult>, MetadataWriteError>;

impl RouteCreateOperation {
    /// `local_node` is this node on the network; `auth_token` vouches for the caller at a
    /// holder.
    pub fn new(
        operation: CreateDocumentOperation,
        local_node: Option<NodeId>,
        auth_token: Option<AuthToken>,
    ) -> Self {
        Self {
            config: operation.config().clone(),
            commit_message: operation.message().map(str::to_owned),
            template: Some(operation),
            auth_token,
            local_node,
            stage: RouteStage::Local,
            document_id: Ulid::nil(),
            placement: None,
            frame: Vec::new(),
            holders: VecDeque::new(),
            asked: 0,
            failures: Vec::new(),
        }
    }

    fn local_answered(&mut self, result: IoResult) -> RouteTurn {
        match result.child::<CreateDocumentResult, CreateDocumentError>()? {
            Ok(created) => Ok(Next::Done(created)),
            Err(CreateDocumentError::HoldsNoBucket) => {
                self.stage = RouteStage::Config;
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.config.actor.realm_id,
                ))))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Mints the forwarded id at the origin with the blind-hash bucket of the subject, so every
    /// candidate holder stamps the same bucket, then asks the holders in rank order.
    fn config_read(&mut self, result: IoResult) -> RouteTurn {
        let realm_config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|_| {
                MetadataWriteError::Undeliverable(
                    "realm placement config is unavailable".to_string(),
                )
            })?;
        let document_id = if self.config.document_id.is_nil() {
            mint_forward_document(
                &realm_config,
                &self.config.actor,
                self.config.group_id,
                &self.config.document_path,
            )?
        } else {
            MetaResourceId::from_bytes(self.config.document_id.to_bytes()).map_err(|error| {
                CreateDocumentError::PlacementBindingUnavailable(format!(
                    "forwarded document id is not a structured id: {error}"
                ))
            })?
        };
        self.document_id = document_id.as_ulid();
        let (placement, holders) =
            create_forward_holders(&realm_config, &self.config, self.document_id).ok_or_else(
                || {
                    MetadataWriteError::Undeliverable(
                        "document id has no resolvable metadata placement".to_string(),
                    )
                },
            )?;
        let config_digest = realm_config
            .digest()
            .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
        let message = MetadataTransportMessage::ForwardCreateDocument {
            auth_token: self.auth_token.clone(),
            config_digest,
            group_id: self.config.group_id,
            document_id: self.document_id,
            document_path: self.config.document_path.clone(),
            public: self.config.public,
            payload: self.config.payload.clone(),
            commit_message: self.commit_message.clone(),
        };
        self.frame = encode_message(&message).map_err(MetadataWriteError::Undeliverable)?;
        self.placement = Some(placement);
        let local_node = self.local_node;
        self.holders = distinct_holders(&holders)
            .into_iter()
            .filter(|holder| Some(*holder) != local_node)
            .collect();
        self.asked = self.holders.len();
        self.stage = RouteStage::Forward;
        self.ask_next()
    }

    fn ask_next(&mut self) -> RouteTurn {
        let Some(peer) = self.holders.front().copied() else {
            let detail = if self.failures.is_empty() {
                "the document's bucket has no reachable holder".to_string()
            } else {
                self.failures.join("; ")
            };
            error!(
                holders = self.asked,
                detail = %detail,
                "Metadata write reached a non-holder and no holder accepted the forward"
            );
            return Err(MetadataWriteError::Undeliverable(detail));
        };
        let request = MetadataRequestEffect {
            peer,
            frame: self.frame.clone(),
            timeout: FORWARD_CREATE_TIMEOUT,
        };
        let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn holder_answered(&mut self, result: IoResult) -> RouteTurn {
        let Some(holder) = self.holders.pop_front() else {
            return Err(MetadataWriteError::Undeliverable(
                "no holder was asked".into(),
            ));
        };
        let Event::Net(NetEvent::MetadataAnswer(answer)) = effect_event(result)? else {
            return Err(MetadataWriteError::Undeliverable(
                "unexpected answer to a forwarded create".into(),
            ));
        };
        let MetadataAnswer { result, .. } = *answer;
        let message = match result.map(|bytes| decode_message(&bytes)) {
            Ok(Ok(message)) => message,
            Err(PeerRequestError::NotSent(error)) => {
                warn!(holder = %holder, error = %error, "Failed to forward a metadata write to holder");
                self.failures.push(format!("{holder}: {error}"));
                return self.ask_next();
            }
            Ok(Err(error)) => return Err(possibly_applied(holder, &error.to_string())),
            Err(PeerRequestError::Failed(error)) => {
                return Err(possibly_applied(holder, &error.to_string()));
            }
            Err(PeerRequestError::TimedOut) => {
                return Err(possibly_applied(holder, "no answer in time"));
            }
        };
        match message {
            MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Unauthorized,
            } => Err(MetadataWriteError::Unauthorized),
            MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
            } => Err(MetadataWriteError::Forbidden),
            MetadataTransportMessage::WriteNotFound => {
                self.failures.push(format!(
                    "{holder}: holder returned not found for a forwarded create"
                ));
                self.ask_next()
            }
            MetadataTransportMessage::ForwardedWriteUnavailable => {
                self.failures
                    .push(format!("{holder}: holder placement view is unavailable"));
                self.ask_next()
            }
            MetadataTransportMessage::MetadataHistoryCapacity => {
                self.failures.push(format!(
                    "{holder}: holder returned metadata history capacity for a non-update"
                ));
                self.ask_next()
            }
            MetadataTransportMessage::Reject(error) => {
                warn!(holder = %holder, error = %error, "Holder rejected a forwarded metadata write");
                Err(MetadataWriteError::Undeliverable(format!(
                    "holder `{holder}` rejected the forwarded metadata write; refusing to replay it: {error}"
                )))
            }
            response => self.created(response),
        }
    }

    /// Checks the record the answering holder returns.
    fn created(&mut self, response: MetadataTransportMessage) -> RouteTurn {
        match response {
            MetadataTransportMessage::ForwardedRecord { record }
                if self.placement.as_ref().is_some_and(|placement| {
                    create_record_matches(&self.config, self.document_id, placement, &record)
                }) =>
            {
                Ok(Next::Done(CreateDocumentResult {
                    event_id: record.last_event_id,
                    record: *record,
                }))
            }
            MetadataTransportMessage::ForwardedRecord { .. } => {
                Err(MetadataWriteError::Undeliverable(
                    "holder returned a metadata create record for another document".to_string(),
                ))
            }
            MetadataTransportMessage::ForwardedProfileValidation { findings } => Err(
                CreateDocumentError::MetadataError(MetadataError::ProfileValidation(findings))
                    .into(),
            ),
            other => Err(unexpected_response(other)),
        }
    }
}

/// A forward that may have reached the holder is never replayed.
fn possibly_applied(holder: NodeId, error: &str) -> MetadataWriteError {
    warn!(holder = %holder, error = %error, "Failed to forward a metadata write to holder");
    MetadataWriteError::Undeliverable(format!(
        "forward to holder `{holder}` may have applied the metadata write before failing; refusing to replay it: {error}"
    ))
}

impl Operation for RouteCreateOperation {
    type Output = CreateDocumentResult;
    type Error = MetadataWriteError;

    fn first(&mut self) -> RouteTurn {
        let template = self
            .template
            .take()
            .ok_or_else(|| MetadataWriteError::Undeliverable("the create was started".into()))?;
        let local = CreateEntryOperation::new(template, self.local_node);
        Ok(Next::Call(IoCall::child(local)))
    }

    fn next(&mut self, result: IoResult) -> RouteTurn {
        match self.stage {
            RouteStage::Local => self.local_answered(result),
            RouteStage::Config => self.config_read(result),
            RouteStage::Forward => self.holder_answered(result),
        }
    }
}

#[cfg(test)]
#[path = "route_create_tests.rs"]
mod tests;
