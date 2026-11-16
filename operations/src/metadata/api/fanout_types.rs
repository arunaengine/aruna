//! Holds the realm fan-out call types and the bearer and query checks they use.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{ApiQueryMode, Arc, BoxFuture, Instant, MetadataReadError, NodeId};
use futures_util::StreamExt;
use futures_util::future::FutureExt;

pub(super) type MetadataNodeCall<T> =
    Arc<dyn Fn(NodeId) -> BoxFuture<'static, Result<T, MetadataReadError>> + Send + Sync>;

#[derive(Clone, Copy)]
pub(super) enum MetadataFanoutOperation {
    Query,
    Search,
    BucketSearch,
    ObjectSearch,
    ReferencePreflight,
    IdentifierLookup,
}

impl MetadataFanoutOperation {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Search => "search",
            Self::BucketSearch => "bucket_search",
            Self::ObjectSearch => "object_search",
            Self::ReferencePreflight => "reference_preflight",
            Self::IdentifierLookup => "identifier_lookup",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetadataFanoutStats {
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub failed_partitions: Vec<NodeId>,
    pub discovery_failed: bool,
    /// Nodes left out by the fanout cap, for a cursor to ask on a later page.
    pub omitted: Vec<NodeId>,
}

#[derive(Debug, Clone)]
pub(crate) struct RealmNodeDiscovery {
    pub(crate) nodes: Vec<NodeId>,
    pub(crate) failed: bool,
}

#[derive(Debug, Clone)]
pub(super) struct MetadataFanoutScope {
    pub(super) mode: Option<ApiQueryMode>,
    pub(super) target_nodes: Option<Vec<NodeId>>,
    pub(super) allow_partial: bool,
    pub(super) discovery_failed: bool,
    pub(super) subject: Option<[u8; 32]>,
    pub(super) deadline: Option<tokio::time::Instant>,
    /// Discovered nodes not to ask, such as those a cursor says are done.
    pub(super) skip: Vec<NodeId>,
}

impl MetadataFanoutScope {
    pub(super) fn new(
        mode: Option<ApiQueryMode>,
        target_nodes: Option<Vec<NodeId>>,
        allow_partial: bool,
    ) -> Self {
        Self {
            mode,
            target_nodes,
            allow_partial,
            discovery_failed: false,
            subject: None,
            deadline: None,
            skip: Vec::new(),
        }
    }

    pub(super) fn with_skip(mut self, skip: Vec<NodeId>) -> Self {
        self.skip = skip;
        self
    }

    pub(super) fn with_discovery_failed(mut self, discovery_failed: bool) -> Self {
        self.discovery_failed = discovery_failed;
        self
    }

    pub(super) fn with_subject(mut self, subject: [u8; 32]) -> Self {
        self.subject = Some(subject);
        self
    }

    pub(super) fn with_deadline(mut self, deadline: tokio::time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }
}
