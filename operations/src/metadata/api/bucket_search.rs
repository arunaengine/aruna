//! Lists the realm's readable buckets one merged page at a time, each node from where the
//! cursor left it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::RealmId;
use tracing::Span;

use super::bucket_page::{
    BucketCursor, NodePosition, decode_cursor, encode_cursor, merge_pages, retired_nodes,
};
use super::distributed::record_bucket_result;
use super::fanout_types::{MetadataFanoutOperation, MetadataFanoutScope, fanout_bearer};
use super::node_fanout::{
    FanoutParts, NodeCalls, NodeFanoutOperation, child_part, peer_answer, peer_request,
};
use super::search_types::{BucketSearchExecution, BucketSearchRequest};
use super::{ApiQueryMode, MetadataApiError, MetadataReadError, query_fingerprint};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::s3::bucket::local_search::LocalBucketSearch;
use crate::s3::bucket::search::{BucketPage, SearchBucketsError, SearchBucketsInput};

/// How the bucket fanout asks each node, from where the cursor left that node. Nodes the cursor
/// marks done are skipped by the fanout scope, so they are never asked.
#[derive(Debug, PartialEq)]
struct BucketCalls {
    auth_token: Option<AuthToken>,
    input: SearchBucketsInput,
    positions: Vec<NodePosition>,
}

impl BucketCalls {
    fn start(&self, node: NodeId) -> Option<Vec<u8>> {
        NodePosition::of(&self.positions, node).and_then(|entry| entry.after.clone())
    }
}

impl NodeCalls for BucketCalls {
    type Part = BucketPage;

    fn local(&self, node: NodeId) -> Effect {
        let input = SearchBucketsInput {
            node_id: node,
            start_after: self.start(node).map(Into::into),
            ..self.input.clone()
        };
        let search = LocalBucketSearch::new(input);
        Effect::SubOperation(boxed_suboperation(search, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }

    fn remote(&self, node: NodeId, timeout: Duration) -> Effect {
        let input = &self.input;
        let message = MetadataTransportMessage::SearchBuckets {
            auth_token: self.auth_token.clone(),
            query: input.query.clone(),
            key: input.key.clone(),
            extras: input.extras.clone(),
            limit: input.limit,
            group_id: input.group_id,
            start_after: self.start(node),
            database: input.database,
        };
        peer_request(node, &message, timeout)
    }

    fn read(
        &self,
        node: NodeId,
        local: bool,
        event: Event,
    ) -> Result<BucketPage, MetadataReadError> {
        if local {
            return child_part::<BucketPage, SearchBucketsError>(event);
        }
        match peer_answer(node, event)? {
            MetadataTransportMessage::BucketSearchResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    fn record(&self, span: &Span, result: &Result<BucketPage, MetadataReadError>) {
        record_bucket_result(span, result);
    }
}

/// One page of the realm's readable buckets; a node that retired during the page counts as
/// failed, since its part ended early.
#[derive(Debug, PartialEq)]
pub struct BucketSearchOperation {
    realm_id: RealmId,
    local_node: NodeId,
    request: Option<BucketSearchRequest>,
    limit: usize,
    cursor: Option<BucketCursor>,
}

/// The bucket search a caller asked for, across the realm.
pub fn search_buckets_distributed(
    realm_id: RealmId,
    local_node: NodeId,
    request: BucketSearchRequest,
) -> BucketSearchOperation {
    BucketSearchOperation {
        realm_id,
        local_node,
        limit: request.limit.clamp(1, 50),
        request: Some(request),
        cursor: None,
    }
}

impl Operation for BucketSearchOperation {
    type Output = BucketSearchExecution;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<BucketSearchExecution>, MetadataApiError> {
        let request = (self.request.take())
            .ok_or_else(|| MetadataApiError::Internal("search already started".to_string()))?;
        let mode = Some(ApiQueryMode::Distributed);
        let subject = query_fingerprint(&request.query, None, mode, None, None);
        let cursor = decode_cursor(request.cursor.as_deref())?;
        let done = (cursor.positions.iter())
            .filter(|entry| entry.done)
            .filter_map(|entry| NodeId::from_bytes(&entry.node_id).ok())
            .collect();
        let calls = BucketCalls {
            auth_token: fanout_bearer(request.bearer_token.as_deref()),
            input: SearchBucketsInput {
                auth: request.auth,
                realm_id: self.realm_id,
                node_id: self.local_node,
                query: request.query,
                key: request.key,
                extras: request.extras,
                limit: self.limit,
                group_id: request.group_id,
                start_after: None,
                database: request.database,
            },
            positions: cursor.positions.clone(),
        };
        self.cursor = Some(cursor);
        let scope = MetadataFanoutScope::new(mode, request.target_nodes, true)
            .with_subject(subject)
            .with_skip(done);
        let fanout = NodeFanoutOperation::new(
            (self.realm_id, self.local_node),
            scope,
            MetadataFanoutOperation::BucketSearch,
            calls,
        );
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BucketSearchExecution>, MetadataApiError> {
        let (parts, mut stats) = result.child::<FanoutParts<BucketPage>, MetadataApiError>()??;
        let cursor = self.cursor.take().unwrap_or_default();
        let asked = (&stats.failed_partitions[..], &stats.omitted[..]);
        let retired = retired_nodes(&parts);
        let (hits, cursor) = merge_pages(parts, asked, cursor, self.limit);
        // A retired node's part ended early, so the page is partial though its scan is done.
        stats.nodes_failed += retired.len();
        stats.failed_partitions.extend(retired);
        Ok(Next::Done(BucketSearchExecution {
            hits,
            next_cursor: encode_cursor(&cursor),
            fanout_stats: stats,
        }))
    }
}

#[cfg(test)]
mod call_tests {
    //! Drives the bucket search through its cursor, fanout and retired node answers.

    use super::*;
    use aruna_core::UserId;
    use aruna_core::effects::NetEffect;
    use aruna_core::structs::identity::auth::AuthContext;

    fn realm() -> RealmId {
        RealmId::from_bytes([1; 32])
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn request(cursor: Option<String>) -> BucketSearchRequest {
        BucketSearchRequest {
            auth: AuthContext {
                user_id: UserId::nil(realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            bearer_token: None,
            query: "reads".to_string(),
            key: None,
            extras: None,
            limit: 500,
            group_id: None,
            cursor,
            target_nodes: Some(vec![node(1), node(2)]),
            database: false,
        }
    }

    fn fanout(parts: Vec<(NodeId, BucketPage)>) -> IoResult {
        let answer: Result<FanoutParts<BucketPage>, MetadataApiError> =
            Ok((parts, super::super::MetadataFanoutStats::default()));
        IoResult::Child(ChildOutcome::new(answer))
    }

    #[test]
    fn search_asks_realm() {
        let mut search = search_buckets_distributed(realm(), node(1), request(None));
        assert_eq!(search.limit, 50);
        assert!(matches!(search.first(), Ok(Next::Call(IoCall::Child(_)))));
        let Ok(Next::Done(execution)) = search.next(fanout(vec![(node(1), BucketPage::default())]))
        else {
            panic!("expected the merged page");
        };
        assert!(execution.hits.is_empty());
        assert_eq!(execution.next_cursor, None);
    }

    #[test]
    fn bad_cursor_refused() {
        let mut search =
            search_buckets_distributed(realm(), node(1), request(Some("!".to_string())));
        assert!(matches!(
            search.first(),
            Err(MetadataApiError::InvalidCursor(_))
        ));
    }

    #[test]
    fn retired_node_fails() {
        let mut search = search_buckets_distributed(realm(), node(1), request(None));
        search.first().unwrap();
        let retired = BucketPage {
            retired: true,
            ..BucketPage::default()
        };
        let Ok(Next::Done(execution)) = search.next(fanout(vec![(node(2), retired)])) else {
            panic!("expected the merged page");
        };
        assert_eq!(execution.fanout_stats.nodes_failed, 1);
        assert_eq!(execution.fanout_stats.failed_partitions, vec![node(2)]);
    }

    #[test]
    fn remote_asks_peer() {
        let calls = BucketCalls {
            auth_token: None,
            input: SearchBucketsInput {
                auth: request(None).auth,
                realm_id: realm(),
                node_id: node(1),
                query: "reads".to_string(),
                key: None,
                extras: None,
                limit: 5,
                group_id: None,
                start_after: None,
                database: false,
            },
            positions: Vec::new(),
        };
        let Effect::Net(NetEffect::MetadataRequest(request)) =
            calls.remote(node(2), Duration::from_secs(2))
        else {
            panic!("expected a metadata request");
        };
        assert_eq!(
            (request.peer, request.timeout),
            (node(2), Duration::from_secs(2))
        );
    }
}
