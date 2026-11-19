//! Serves the reads a peer forwards for a caller: the caller is checked first, then the read
//! runs as that caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::metadata::{AuthToken, MetadataError, MetadataQueryResults, MetadataSearchHit};
use aruna_core::node_state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use aruna_core::structs::storage::release::HolderAnswer;

use super::ServeNode;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::links::local::{LinksLocalOperation, SetPreviewOperation};
use crate::metadata::api::preflight::local::references_preflight_local;
use crate::metadata::forward::write::read_error;
use crate::metadata::identifier_lookup::{IdentifierLookupOperation, IdentifierQuery};
use crate::metadata::local_query::LocalQueryOperation;
use crate::metadata::read_peer::{PeerReader, ReadPeerOperation, SearchPeerOperation};
use crate::metadata::search_cursor::MAX_PAGINATION_DEPTH;
use crate::metadata::text_search::LocalSearchOperation;
use crate::s3::bucket::local_search::LocalBucketSearch;
use crate::s3::bucket::search::{SearchBucketsError, SearchBucketsInput};
use crate::s3::object::search::{SearchObjectsError, SearchObjectsInput};
use crate::transfer::release::publish::{HolderAnswerOperation, PublishError};
use crate::transfer::search_scan::SearchObjectsOperation;
use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::metadata::api::{
    IdentifierMatch, LinkNodePage, ReferenceNodeExecution, ReferenceNodeRequest, SetPreviewPage,
};
use aruna_core::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage as Message, metadata_read_error,
};
use aruna_core::search::{BucketPage, SearchNodePage};

/// The reply a forwarded read answers with.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ReadReply {
    Identifier,
    Links,
    Owner,
    Preview,
    Query,
    Search,
    Preflight,
    Buckets,
    Objects,
    Release,
}

impl ReadReply {
    /// The reply of `request`, or `None` when it is no forwarded read.
    pub(crate) fn of(request: &Message) -> Option<Self> {
        Some(match request {
            Message::LookupIdentifier { .. } => Self::Identifier,
            Message::DataLinks { .. } => Self::Links,
            Message::BucketOwner { .. } => Self::Owner,
            Message::SetPreview { .. } => Self::Preview,
            Message::QueryGraphs { .. } => Self::Query,
            Message::SearchGraphs { .. } | Message::FilteredSearchGraphs { .. } => Self::Search,
            Message::ReferencePreflight { .. } => Self::Preflight,
            Message::SearchBuckets { .. } => Self::Buckets,
            Message::SearchObjects { .. } => Self::Objects,
            Message::ReleaseHolder { .. } => Self::Release,
            _ => return None,
        })
    }

    // The reader a search check admits; the other reads check the caller as a plain read.
    fn reader(self) -> Option<PeerReader> {
        match self {
            Self::Buckets => Some(PeerReader::Buckets),
            Self::Objects => Some(PeerReader::Objects),
            Self::Release => Some(PeerReader::Release),
            _ => None,
        }
    }

    pub(crate) fn refused(self, error: MetadataReadError) -> Message {
        match self {
            Self::Identifier => Message::IdentifierMatches { result: Err(error) },
            Self::Links => Message::DataLinkResults { result: Err(error) },
            Self::Owner => Message::BucketOwnerResult { result: Err(error) },
            Self::Preview => Message::SetPreviewResults { result: Err(error) },
            Self::Query => Message::QueryResults { result: Err(error) },
            Self::Search => Message::SearchResults { result: Err(error) },
            Self::Preflight => Message::ReferencePreflightResults { result: Err(error) },
            Self::Buckets => Message::BucketSearchResults { result: Err(error) },
            Self::Objects => Message::ObjectSearchResults { result: Err(error) },
            Self::Release => Message::ReleaseHolderResults { result: Err(error) },
        }
    }

    fn answer(self, result: IoResult) -> Result<Message, IoError> {
        Ok(match self {
            Self::Identifier => Message::IdentifierMatches {
                result: (result.child::<Vec<IdentifierMatch>, MetadataApiError>()?)
                    .map_err(unavailable),
            },
            Self::Links => Message::DataLinkResults {
                result: (result.child::<LinkNodePage, MetadataApiError>()?)
                    .map(Box::new)
                    .map_err(read_error),
            },
            Self::Owner => Message::BucketOwnerResult {
                result: (result.child::<BucketInfo, GetBucketError>()?)
                    .map(|info| info.group_id)
                    .map_err(|error| match error {
                        GetBucketError::NotFound => MetadataReadError::NotFound,
                        _ => MetadataReadError::Unavailable,
                    }),
            },
            Self::Preview => Message::SetPreviewResults {
                result: (result.child::<SetPreviewPage, MetadataApiError>()?)
                    .map(Box::new)
                    .map_err(read_error),
            },
            Self::Query => Message::QueryResults {
                result: (result.child::<MetadataQueryResults, MetadataError>()?)
                    .map_err(metadata_read_error),
            },
            Self::Search => Message::SearchResults {
                result: (result.child::<Vec<MetadataSearchHit>, MetadataError>()?)
                    .map_err(metadata_read_error),
            },
            Self::Preflight => Message::ReferencePreflightResults {
                result: (result.child::<ReferenceNodeExecution, MetadataApiError>()?)
                    .map(Box::new)
                    .map_err(read_error),
            },
            Self::Buckets => Message::BucketSearchResults {
                result: (result.child::<BucketPage, SearchBucketsError>()?).map_err(unavailable),
            },
            Self::Objects => Message::ObjectSearchResults {
                result: (result.child::<SearchNodePage, SearchObjectsError>()?)
                    .map_err(unavailable),
            },
            Self::Release => Message::ReleaseHolderResults {
                result: (result.child::<HolderAnswer, PublishError>()?).map_err(
                    |error| match error {
                        PublishError::Forbidden(_) => MetadataReadError::Forbidden,
                        _ => MetadataReadError::Unavailable,
                    },
                ),
            },
        })
    }
}

fn unavailable<E>(_: E) -> MetadataReadError {
    MetadataReadError::Unavailable
}

// Takes the caller's token out of a forwarded read.
fn take_token(request: &mut Message) -> Option<AuthToken> {
    match request {
        Message::LookupIdentifier { auth_token, .. }
        | Message::DataLinks { auth_token, .. }
        | Message::BucketOwner { auth_token, .. }
        | Message::SetPreview { auth_token, .. }
        | Message::QueryGraphs { auth_token, .. }
        | Message::SearchGraphs { auth_token, .. }
        | Message::FilteredSearchGraphs { auth_token, .. }
        | Message::ReferencePreflight { auth_token, .. }
        | Message::SearchBuckets { auth_token, .. }
        | Message::SearchObjects { auth_token, .. }
        | Message::ReleaseHolder { auth_token, .. } => auth_token.take(),
        _ => None,
    }
}

#[derive(Debug, PartialEq)]
enum ReadStage {
    Check,
    /// A preflight reads this node's S3 endpoint before it runs.
    Endpoint(Option<AuthContext>, Box<ReferenceNodeRequest>),
    Read,
}

/// Checks the caller `peer` forwarded, then answers its read; a node without a realm, or a
/// graph read without the graph store, answers unavailable.
#[derive(Debug, PartialEq)]
pub(crate) struct CallerReadOperation {
    peer: NodeId,
    node: ServeNode,
    request: Option<Message>,
    reply: ReadReply,
    stage: ReadStage,
}

type ReadTurn = Result<Next<Message>, IoError>;

impl CallerReadOperation {
    pub(crate) fn new(peer: NodeId, node: ServeNode, request: Message, reply: ReadReply) -> Self {
        Self {
            peer,
            node,
            request: Some(request),
            reply,
            stage: ReadStage::Check,
        }
    }

    fn refused(&self, error: MetadataReadError) -> ReadTurn {
        Ok(Next::Done(self.reply.refused(error)))
    }

    fn call(&mut self, call: IoCall) -> ReadTurn {
        self.stage = ReadStage::Read;
        Ok(Next::Call(call))
    }

    fn run(&mut self, (node, realm_id): (NodeId, RealmId), auth: Option<AuthContext>) -> ReadTurn {
        let request = self.request.take().ok_or(IoError::Unexpected {
            expected: "a forwarded read",
            got: "none",
        })?;
        let call = match (request, auth) {
            (
                Message::LookupIdentifier {
                    kind,
                    value,
                    endpoint,
                    ..
                },
                auth,
            ) => {
                let query = IdentifierQuery {
                    kind,
                    value,
                    endpoint,
                };
                IoCall::child(IdentifierLookupOperation::new(realm_id, auth, query))
            }
            (Message::DataLinks { request, .. }, auth) => {
                IoCall::child(LinksLocalOperation::new(realm_id, auth, *request))
            }
            (Message::BucketOwner { bucket, .. }, Some(_)) => {
                IoCall::child(GetBucketOperation::new(bucket))
            }
            (Message::SetPreview { set_id, limit, .. }, auth) => IoCall::child(
                SetPreviewOperation::new((realm_id, auth), set_id, None, limit),
            ),
            (
                Message::QueryGraphs {
                    graph_iris, sparql, ..
                },
                auth,
            ) => IoCall::child(LocalQueryOperation::new(
                (realm_id, auth),
                graph_iris,
                sparql,
            )),
            (
                Message::SearchGraphs {
                    graph_iris,
                    query,
                    limit,
                    group_id,
                    ..
                },
                auth,
            ) => {
                let ask = (query, limit.clamp(1, MAX_PAGINATION_DEPTH));
                let scope = (graph_iris, group_id);
                IoCall::child(LocalSearchOperation::new(
                    (realm_id, auth),
                    scope,
                    ask,
                    None,
                ))
            }
            (
                Message::FilteredSearchGraphs {
                    graph_iris,
                    query,
                    limit,
                    predicate_iri,
                    object_iri,
                    group_id,
                    ..
                },
                auth,
            ) => {
                let ask = (query, limit.clamp(1, MAX_PAGINATION_DEPTH));
                let scope = (graph_iris, group_id);
                let reference = Some((predicate_iri, object_iri));
                IoCall::child(LocalSearchOperation::new(
                    (realm_id, auth),
                    scope,
                    ask,
                    reference,
                ))
            }
            (Message::ReferencePreflight { request, .. }, auth) => {
                self.stage = ReadStage::Endpoint(auth, request);
                return Ok(Next::Call(IoCall::child(ReadInfoOperation::new(vec![
                    node,
                ]))));
            }
            (
                Message::SearchBuckets {
                    query,
                    key,
                    extras,
                    limit,
                    group_id,
                    start_after,
                    database,
                    ..
                },
                Some(auth),
            ) => IoCall::child(LocalBucketSearch::new(SearchBucketsInput {
                auth,
                realm_id,
                node_id: node,
                query,
                key,
                extras,
                limit,
                group_id,
                start_after: start_after.map(Into::into),
                database,
            })),
            (
                Message::SearchObjects {
                    query,
                    key_match,
                    bucket,
                    limit,
                    start_after,
                    as_of,
                    ..
                },
                Some(auth),
            ) => {
                let input = SearchObjectsInput {
                    auth,
                    realm_id,
                    node_id: node,
                    query,
                    key_match,
                    bucket,
                    limit,
                    start_after,
                    as_of,
                };
                IoCall::child(SearchObjectsOperation::new(input, self.node.indexed))
            }
            (Message::ReleaseHolder { bucket, ask, .. }, Some(auth)) => {
                let answer =
                    HolderAnswerOperation::new((realm_id, node), &auth, (bucket.as_str(), ask));
                IoCall::child(answer)
            }
            // The owner of a bucket and the searches need a known caller.
            _ => return self.refused(MetadataReadError::Unauthorized),
        };
        self.call(call)
    }
}

impl Operation for CallerReadOperation {
    type Output = Message;
    type Error = IoError;

    fn first(&mut self) -> ReadTurn {
        let graph_read = matches!(self.reply, ReadReply::Query | ReadReply::Search);
        let Some((_, realm_id)) = self.node.local else {
            return self.refused(MetadataReadError::Unavailable);
        };
        if graph_read && !self.node.graph {
            return self.refused(MetadataReadError::Unavailable);
        }
        let caller = (self.peer, realm_id);
        let token = self.request.as_mut().and_then(take_token);
        let check = match self.reply.reader() {
            Some(reader) => IoCall::child(SearchPeerOperation::new(caller, token, reader)),
            None => IoCall::child(ReadPeerOperation::new(caller, token, false)),
        };
        Ok(Next::Call(check))
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        let local = self.node.local.ok_or(IoError::Unexpected {
            expected: "this node's realm",
            got: "none",
        })?;
        match std::mem::replace(&mut self.stage, ReadStage::Read) {
            ReadStage::Check => {
                let caller = match self.reply.reader() {
                    Some(_) => (result.child::<AuthContext, MetadataReadError>()?).map(Some),
                    None => result.child::<Option<AuthContext>, MetadataReadError>()?,
                };
                match caller {
                    Ok(auth) => self.run(local, auth),
                    Err(error) => self.refused(error),
                }
            }
            ReadStage::Endpoint(auth, request) => {
                let (node, realm_id) = local;
                let infos = result.child::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>()?;
                let endpoint = (infos.ok())
                    .and_then(|mut infos| infos.remove(&node))
                    .and_then(|info| info.urls.s3);
                let preflight =
                    references_preflight_local((realm_id, node), auth, *request, endpoint);
                self.call(IoCall::child(preflight))
            }
            ReadStage::Read => self.reply.answer(result).map(Next::Done),
        }
    }
}
