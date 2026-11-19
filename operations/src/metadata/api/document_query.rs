//! Runs a SPARQL query on one document, at this node or at the first replica that answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::metadata::{AuthToken, MetadataError, MetadataQueryResults};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::warn;

use super::fanout::ensure_query_form;
use super::fanout_types::MetadataFanoutStats;
use super::read_types::{ApiQueryMode, DocumentQueryRequest, MetadataQueryExecution};
use super::readable::RecordReadableOperation;
use super::{MetadataApiError, MetadataReadError, map_query_error, replica_query_nodes};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};
use crate::metadata::local_query::LocalQueryOperation;
use crate::metadata::protocol::{MetadataTransportMessage, encode_message};
use crate::metadata::record_reads::RecordMaterializedOperation;
use crate::metadata::repository::StorageReadError;

/// One replica may open its stream and then answer, each within the transport's own bound.
const HOLDER_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

type QueryTurn = Result<Next<MetadataQueryExecution>, MetadataApiError>;

#[derive(Debug, PartialEq)]
enum QueryStage {
    Record,
    Readable,
    Config,
    /// The local graph is checked for the record's accepted event, then queried.
    Materialized,
    Local,
    Remote(NodeId),
}

/// How this node's own answer counts.
#[derive(Debug, PartialEq)]
enum LocalRole {
    /// The caller asked for this node only.
    Only,
    /// The realm config is unreadable and the caller accepts a partial answer.
    Fallback,
    /// This node is one of the replicas, asked in rank order.
    Replica,
}

/// Queries one live, readable document. A local query answers from this node; otherwise the
/// document's replicas are asked one after the other, this node first when it holds one, and
/// the first answer wins. A refusal is kept for when no replica answers.
#[derive(Debug, PartialEq)]
pub struct DocumentQueryOperation {
    realm_id: RealmId,
    local_node: NodeId,
    request: DocumentQueryRequest,
    stage: QueryStage,
    record: Option<MetadataRegistryRecord>,
    role: LocalRole,
    holders: VecDeque<NodeId>,
    remote_auth: Option<AuthToken>,
    config_digest: [u8; 32],
    stats: MetadataFanoutStats,
    auth_error: Option<MetadataApiError>,
}

impl DocumentQueryOperation {
    pub fn new(realm_id: RealmId, local_node: NodeId, request: DocumentQueryRequest) -> Self {
        Self {
            realm_id,
            local_node,
            request,
            stage: QueryStage::Record,
            record: None,
            role: LocalRole::Replica,
            holders: VecDeque::new(),
            remote_auth: None,
            config_digest: [0; 32],
            stats: MetadataFanoutStats::default(),
            auth_error: None,
        }
    }

    fn record(&self) -> Result<&MetadataRegistryRecord, MetadataApiError> {
        (self.record.as_ref()).ok_or_else(|| MetadataApiError::Internal("no record".to_string()))
    }

    fn materialized(&mut self, role: LocalRole) -> QueryTurn {
        self.role = role;
        self.stage = QueryStage::Materialized;
        let check = RecordMaterializedOperation::new(self.record()?);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn local_query(&mut self) -> QueryTurn {
        let query = LocalQueryOperation::new(
            (self.realm_id, self.request.auth.clone()),
            Some(vec![self.record()?.graph_iri.clone()]),
            self.request.query.clone(),
        );
        self.stage = QueryStage::Local;
        Ok(Next::Call(IoCall::child(query)))
    }

    fn configured(&mut self, config: RealmConfigDocument) -> QueryTurn {
        self.config_digest = config
            .digest()
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let mut holders = replica_query_nodes(Some(&config), self.record()?, self.local_node);
        if let Some(index) = holders.iter().position(|holder| *holder == self.local_node) {
            holders.swap(0, index);
        }
        self.holders = holders.into();
        self.remote_auth = match self.request.bearer_token.as_deref() {
            Some(token) => {
                Some(AuthToken::bearer(token).map_err(|_| MetadataApiError::BadRequest)?)
            }
            None => self.request.auth.clone().map(AuthToken::internal),
        };
        self.next_holder()
    }

    fn next_holder(&mut self) -> QueryTurn {
        let Some(holder) = self.holders.pop_front() else {
            return Err((self.auth_error.take()).unwrap_or(MetadataApiError::ServiceUnavailable));
        };
        self.stats.nodes_queried += 1;
        if holder == self.local_node {
            return self.materialized(LocalRole::Replica);
        }
        let message = MetadataTransportMessage::QueryDocument {
            auth_token: self.remote_auth.clone(),
            config_digest: self.config_digest,
            document_id: self.request.document_id,
            sparql: self.request.query.clone(),
        };
        let frame = encode_message(&message).map_err(MetadataApiError::Internal)?;
        self.stage = QueryStage::Remote(holder);
        Ok(Next::Call(peer_call(holder, frame, HOLDER_QUERY_TIMEOUT)))
    }

    /// The answer of one replica: the first success ends the query.
    fn answered(
        &mut self,
        holder: NodeId,
        result: Result<MetadataQueryResults, MetadataReadError>,
    ) -> QueryTurn {
        match result {
            Ok(results) => {
                // A lagging replica's NotFound must not override an in-hand result.
                let mut fanout_stats = std::mem::take(&mut self.stats);
                fanout_stats.nodes_failed = 0;
                fanout_stats.failed_partitions.clear();
                return Ok(Next::Done(MetadataQueryExecution {
                    results,
                    fanout_stats,
                }));
            }
            Err(MetadataReadError::Unauthorized) => {
                (self.auth_error).get_or_insert(MetadataApiError::Unauthorized);
            }
            Err(MetadataReadError::Forbidden) => {
                (self.auth_error).get_or_insert(MetadataApiError::Forbidden);
            }
            Err(MetadataReadError::NotFound) => {}
            Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => {
                self.stats.nodes_failed += 1;
                self.stats.failed_partitions.push(holder);
                warn!(%holder, "Document query holder unavailable; trying the next replica");
            }
        }
        self.next_holder()
    }

    /// The local answer for the caller's role.
    fn local_answer(
        &mut self,
        result: Result<MetadataQueryResults, MetadataApiError>,
    ) -> QueryTurn {
        let stats = match self.role {
            LocalRole::Replica => {
                let result = result.map_err(|_| MetadataReadError::Unavailable);
                return self.answered(self.local_node, result);
            }
            LocalRole::Only => MetadataFanoutStats {
                nodes_queried: 1,
                ..MetadataFanoutStats::default()
            },
            LocalRole::Fallback => MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 1,
                discovery_failed: true,
                ..MetadataFanoutStats::default()
            },
        };
        Ok(Next::Done(MetadataQueryExecution {
            results: result?,
            fanout_stats: stats,
        }))
    }
}

/// The record's materialization check as an answer: `Ok(false)` waits for the projection.
fn materialized(result: IoResult) -> Result<(), MetadataApiError> {
    match result.child::<bool, StorageReadError>()? {
        Ok(true) => Ok(()),
        Ok(false) => Err(MetadataApiError::ServiceUnavailable),
        Err(error) => Err(MetadataApiError::Internal(error.to_string())),
    }
}

/// The results of a local query, or why there are none.
fn queried(result: IoResult) -> Result<MetadataQueryResults, MetadataApiError> {
    result
        .child::<MetadataQueryResults, MetadataError>()?
        .map_err(map_query_error)
}

impl Operation for DocumentQueryOperation {
    type Output = MetadataQueryExecution;
    type Error = MetadataApiError;

    fn first(&mut self) -> QueryTurn {
        ensure_query_form(&self.request.query)?;
        let record = LiveRecordOperation::new(self.request.document_id);
        Ok(Next::Call(IoCall::child(record)))
    }

    fn next(&mut self, result: IoResult) -> QueryTurn {
        match self.stage {
            QueryStage::Record => {
                let record =
                    result.child::<Option<MetadataRegistryRecord>, LiveRecordsError>()??;
                let record = record.ok_or(MetadataApiError::NotFound)?;
                let auth = self.request.auth.clone();
                let readable = RecordReadableOperation::new(self.realm_id, auth, record.clone());
                self.record = Some(record);
                self.stage = QueryStage::Readable;
                Ok(Next::Call(IoCall::child(readable)))
            }
            QueryStage::Readable => {
                result.child::<(), MetadataApiError>()??;
                if self.request.mode == Some(ApiQueryMode::Local) {
                    return self.materialized(LocalRole::Only);
                }
                self.stage = QueryStage::Config;
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.realm_id,
                ))))
            }
            QueryStage::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(config) => self.configured(config),
                Err(_) if self.request.allow_partial => self.materialized(LocalRole::Fallback),
                Err(_) => Err(MetadataApiError::ServiceUnavailable),
            },
            QueryStage::Materialized => match (materialized(result), &self.role) {
                (Ok(()), _) => self.local_query(),
                (Err(_), LocalRole::Replica) => {
                    self.answered(self.local_node, Err(MetadataReadError::Unavailable))
                }
                (Err(error), _) => Err(error),
            },
            QueryStage::Local => {
                let answer = queried(result);
                self.local_answer(answer)
            }
            QueryStage::Remote(holder) => {
                let answer = match peer_reply(holder, result)? {
                    Ok(MetadataTransportMessage::DocumentQueryResults { result }) => result,
                    _ => Err(MetadataReadError::Unavailable),
                };
                self.answered(holder, answer)
            }
        }
    }
}

#[cfg(test)]
#[path = "document_query_tests.rs"]
mod call_tests;
