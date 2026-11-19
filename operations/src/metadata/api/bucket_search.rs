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
use super::{ApiQueryMode, MetadataApiError, MetadataReadError, map_read_error, query_fingerprint};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::s3::bucket::local_search::LocalBucketSearch;
use crate::s3::bucket::search::{BucketPage, SearchBucketsError, SearchBucketsInput};

/// How the bucket fanout asks each node, from where the cursor left that node. Nodes the cursor
/// marks done are skipped by the fanout scope, so they are never asked.
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
            map_read_error,
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
#[path = "bucket_search_tests.rs"]
mod call_tests;
