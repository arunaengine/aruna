//! Searches local graphs the caller may see and enriches each hit with title, types and text.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::lifecycle::{list_deleted_iris, list_read_records};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use aruna_core::metadata::{MetadataError, MetadataSearchHit};
use aruna_core::search_enrichment::{hit_snippet, hit_title, hit_types};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::telemetry::{record_duration_ms, record_elapsed_ms, record_error};
use aruna_core::types::GroupId;
use aruna_graph::{GraphHandle, Properties, ReadScope};
use futures_util::FutureExt;
use tracing::{Span, debug_span, field};
use ulid::Ulid;

use super::{
    LifecycleVisibilityRefresh, MetadataInner, MetadataVisibilityCache, REGISTRY_CANDIDATE_LIMIT,
    handles,
};
use crate::auth::permission_rules::GroupPermissionRules;
use crate::driver::{DriverContext, drive};
use crate::metadata::live_records::LiveRecordsOperation;
use crate::metadata::query_cache::ScopeDigest;
use crate::metadata::search_cursor::{MAX_PAGINATION_DEPTH, compare_hits, tie_order};

#[tracing::instrument(
    name = "metadata.search.local",
    level = "debug",
    skip(context, inner, graph, auth_context, query, iri_filter),
    fields(
        query_len = query.len() as u64,
        limit = limit as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        registry_records = field::Empty,
        authorized_graphs = field::Empty,
        readable_groups = field::Empty,
        lazy = field::Empty,
        registry_ms = field::Empty,
        authorization_ms = field::Empty,
        craqle_search_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn search_local_graphs(
    context: &DriverContext,
    inner: Arc<MetadataInner>,
    graph: &GraphHandle,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    group_id: Option<GroupId>,
    iri_filter: Option<(String, String)>,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();

    if graph_iris
        .as_ref()
        .is_some_and(|graphs| graphs.len() > REGISTRY_CANDIDATE_LIMIT)
    {
        return Err(MetadataError::Backend(
            "metadata candidate limit exceeded".to_string(),
        ));
    }

    // An IRI filter names its candidates through the backlink index, never the registry.
    let records = match iri_filter {
        Some(_) => Arc::new(Vec::new()),
        None => list_read_records(inner.clone(), graph, &span).await?,
    };
    // Without a candidate filter the request spans the realm, so craqle
    // authorizes the index hits it returns instead of every visible graph.
    let lazy = iri_filter.is_none() && graph_iris.is_none() && group_id.is_none();
    span.record("lazy", lazy);

    let result = if lazy {
        search_realm_scope(inner, graph, auth_context, records, query, limit, &span).await
    } else {
        search_candidate_graphs(
            context,
            inner,
            graph,
            auth_context,
            records,
            graph_iris,
            query,
            limit,
            group_id,
            iri_filter,
            &span,
        )
        .await
    };

    match &result {
        Ok(hits) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(error) => record_error(&span, &error.to_string()),
    }
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    result
}

pub(super) fn record_backend_search(
    span: &Span,
    search_span: &Span,
    started: Instant,
    result: &Result<Vec<MetadataSearchHit>, MetadataError>,
) {
    let elapsed = started.elapsed();
    record_duration_ms(search_span, "elapsed_ms", elapsed);
    record_duration_ms(span, "craqle_search_ms", elapsed);
    match result {
        Ok(hits) => {
            search_span.record("result", "ok");
            search_span.record("hit_count", hits.len() as u64);
        }
        Err(error) => record_error(search_span, &error.to_string()),
    }
}

async fn search_realm_scope(
    inner: Arc<MetadataInner>,
    graph: &GraphHandle,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    query: String,
    limit: usize,
    span: &Span,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let authorization_started = Instant::now();
    let scope = resolve_visibility_scope(&inner, auth_context, records)
        .boxed()
        .await?;
    record_elapsed_ms(span, "authorization_ms", authorization_started);
    span.record("readable_groups", scope.permissions.group_count() as u64);
    if limit == 0 || scope.records.is_empty() {
        return Ok(Vec::new());
    }

    let search_span = debug_span!(
        "metadata.backend.craqle.search",
        lazy = true,
        query_len = query.len() as u64,
        limit = limit as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    );
    let search_started = Instant::now();
    let scope = Arc::new(scope);
    let visible_scope = scope.clone();
    let visible_inner = inner.clone();
    let read_scope = ReadScope::Visible(Arc::new(move |graph_iri: &str| {
        visible_scope.graph_visible(&visible_inner.visibility_cache, graph_iri)
    }));
    let result = graph
        .search(
            read_scope,
            query.clone(),
            limit,
            |graph_iri| scope.record_for_graph(graph_iri).is_some(),
            &search_span,
        )
        .await
        .map(|hits| {
            let mut visible = hits
                .into_iter()
                .filter_map(|(hit, properties)| {
                    let record = scope.record_for_graph(&hit.graph_id)?;
                    Some(hit_from_craqle(hit, record, &properties, &query))
                })
                .collect::<Vec<_>>();
            // The unfiltered entry point returns raw index order; the search cursor's
            // watermark needs the merged score-descending order instead.
            visible.sort_by(compare_hits);
            visible
        });
    record_backend_search(span, &search_span, search_started, &result);
    result
}

// Requests narrowed by graph, group, or IRI filter keep a genuine candidate
// set, so pre-filtering it stays cheaper than post-filtering the whole index.
#[allow(clippy::too_many_arguments)]
async fn search_candidate_graphs(
    context: &DriverContext,
    inner: Arc<MetadataInner>,
    graph: &GraphHandle,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    group_id: Option<GroupId>,
    iri_filter: Option<(String, String)>,
    span: &Span,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let (records, iri_matches) = match iri_filter.as_ref() {
        Some((predicate_iri, object_iri)) => {
            let (found, matches) = super::super::iri_index::lookup_iri_references(
                context,
                predicate_iri,
                object_iri,
                REGISTRY_CANDIDATE_LIMIT,
            )
            .await?;
            (Arc::new(found), Some(matches))
        }
        None => (records, None),
    };

    let authorization_started = Instant::now();
    let allowed_records =
        select_authorized_records(&inner, auth_context, records, graph_iris, group_id).await?;
    record_elapsed_ms(span, "authorization_ms", authorization_started);
    span.record("authorized_graphs", allowed_records.len() as u64);

    if limit == 0 || allowed_records.is_empty() {
        return Ok(Vec::new());
    }

    if query.trim().is_empty()
        && let Some(matches) = iri_matches
    {
        let allowed_graphs = allowed_records
            .iter()
            .map(|record| record.graph_iri.clone())
            .collect::<HashSet<_>>();
        // Every hit scores the same, so the page is cut in watermark tie order
        // before the describes instead of enriching every candidate.
        let mut candidates = allowed_records
            .into_iter()
            .filter_map(|record| {
                let subject_iri = matches.get(&record.document_id)?.first()?.clone();
                Some((record, subject_iri))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|(left, left_subject), (right, right_subject)| {
            tie_order(
                (&left.graph_iri, left_subject),
                (&right.graph_iri, right_subject),
            )
        });
        candidates.truncate(limit);
        let targets = candidates
            .iter()
            .map(|(record, subject_iri)| (record.graph_iri.clone(), subject_iri.clone()))
            .collect::<Vec<_>>();
        let scope = ReadScope::Graphs(allowed_graphs.into_iter().collect());
        let properties = graph.describe(scope, targets, span).await;
        return Ok(candidates
            .into_iter()
            .zip(properties)
            .map(|((record, subject_iri), properties)| MetadataSearchHit {
                document_id: record.document_id.to_string(),
                group_id: record.group_id.to_string(),
                title: hit_title(&properties, &record.document_path, &subject_iri),
                subject_types: hit_types(&properties),
                document_path: record.document_path,
                graph_iri: record.graph_iri,
                subject_iri,
                score: 1.0,
                snippet: None,
            })
            .collect());
    }

    let by_graph: HashMap<_, _> = allowed_records
        .into_iter()
        .map(|record| (record.graph_iri.clone(), record))
        .collect();
    let mut search_graphs = by_graph.keys().cloned().collect::<Vec<_>>();
    search_graphs.sort_unstable();

    let search_span = debug_span!(
        "metadata.backend.craqle.search",
        lazy = false,
        graph_count = search_graphs.len() as u64,
        query_len = query.len() as u64,
        limit = limit as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    );
    let search_started = Instant::now();
    let result = graph
        .search(
            ReadScope::Graphs(search_graphs),
            query.clone(),
            limit,
            |graph_iri| by_graph.contains_key(graph_iri),
            &search_span,
        )
        .await
        .map(|hits| {
            hits.into_iter()
                .filter_map(|(hit, properties)| {
                    let record = by_graph.get(&hit.graph_id)?;
                    Some(hit_from_craqle(hit, record, &properties, &query))
                })
                .collect()
        });
    record_backend_search(span, &search_span, search_started, &result);
    result
}

fn hit_from_craqle(
    hit: craqle::SearchHit,
    record: &MetadataRegistryRecord,
    properties: &Properties,
    query: &str,
) -> MetadataSearchHit {
    MetadataSearchHit {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        title: hit_title(properties, &record.document_path, &hit.subject_iri),
        snippet: hit_snippet(properties, query),
        subject_types: hit_types(properties),
        graph_iri: hit.graph_id,
        subject_iri: hit.subject_iri,
        score: hit.score,
    }
}

pub(super) fn clamp_remote_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_PAGINATION_DEPTH)
}

pub(super) async fn list_visible_graphs(
    context: &DriverContext,
) -> Result<Vec<String>, MetadataError> {
    let (metadata, _) = handles(context)?;
    let inner = &metadata.inner;
    let records = inner
        .visibility_cache
        .registry_records_any()
        .map(|(records, _)| records)
        .ok_or_else(|| {
            MetadataError::Backend("metadata registry snapshot unavailable".to_string())
        })?;
    let records = drive(LiveRecordsOperation::new(records.to_vec(), None), context)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    Ok(records.into_iter().map(|record| record.graph_iri).collect())
}

pub(super) async fn select_authorized_graphs(
    inner: &Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    graph_filter: Option<Vec<String>>,
) -> Result<Vec<String>, MetadataError> {
    Ok(
        select_authorized_records(inner, auth_context, records, graph_filter, None)
            .await?
            .into_iter()
            .map(|record| record.graph_iri)
            .collect(),
    )
}

#[tracing::instrument(
    name = "metadata.authorization.select_records",
    level = "debug",
    skip(inner, auth_context, records, graph_filter),
    fields(
        record_count = records.len() as u64,
        graph_filter_count = graph_filter.as_ref().map_or(0, Vec::len) as u64,
        visible_count = field::Empty,
        deleted_count = field::Empty,
        filtered_count = field::Empty,
        lifecycle_cache_hits = field::Empty,
        lifecycle_cache_misses = field::Empty,
        lifecycle_reads = field::Empty,
        public_count = field::Empty,
        private_checked_count = field::Empty,
        denied_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn select_authorized_records(
    inner: &Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    graph_filter: Option<Vec<String>>,
    group_id: Option<GroupId>,
) -> Result<Vec<MetadataRegistryRecord>, MetadataError> {
    let span = Span::current();
    let started = Instant::now();
    let allowed_graphs = graph_filter.map(|graphs| graphs.into_iter().collect::<HashSet<_>>());
    let record_count = records.len();
    // Filtering first keeps the scope resolution to the groups and lifecycle
    // entries a filtered request can still see.
    let candidates = filter_candidate_records(records, allowed_graphs.as_ref(), group_id);
    let filtered_count = record_count - candidates.len();

    // Boxed so the read paths that already resolve a scope do not nest this
    // future again: the combined depth exceeds the auto-trait recursion limit.
    let scope = resolve_visibility_scope(inner, auth_context, candidates)
        .boxed()
        .await?;
    let selection = select_visible_records(&scope, &inner.visibility_cache);
    let evaluated = scope.records.len() as u64;
    // Lifecycle is resolved once per request, so the cache counters report how
    // the whole request was decided instead of per-record point reads.
    let lifecycle_cached = matches!(&scope.lifecycle_visibility, LifecycleVisibility::Cache(_));
    span.record("visible_count", selection.visible.len() as u64);
    span.record("deleted_count", selection.deleted as u64);
    span.record("filtered_count", filtered_count as u64);
    span.record(
        "lifecycle_cache_hits",
        if lifecycle_cached { evaluated } else { 0 },
    );
    span.record(
        "lifecycle_cache_misses",
        if lifecycle_cached { 0 } else { evaluated },
    );
    span.record("lifecycle_reads", 1u64);
    span.record("public_count", selection.public as u64);
    span.record("private_checked_count", selection.private as u64);
    span.record("denied_count", selection.denied as u64);
    record_elapsed_ms(&span, "elapsed_ms", started);
    Ok(selection.visible)
}

pub(super) fn filter_candidate_records(
    records: Arc<Vec<MetadataRegistryRecord>>,
    allowed_graphs: Option<&HashSet<String>>,
    group_id: Option<GroupId>,
) -> Arc<Vec<MetadataRegistryRecord>> {
    if allowed_graphs.is_none() && group_id.is_none() {
        return records;
    }
    Arc::new(
        records
            .iter()
            .filter(|record| {
                allowed_graphs.is_none_or(|graphs| graphs.contains(&record.graph_iri))
                    && group_id.is_none_or(|group_id| record.group_id == group_id)
            })
            .cloned()
            .collect(),
    )
}

pub(super) struct RecordSelection {
    pub(super) visible: Vec<MetadataRegistryRecord>,
    pub(super) deleted: usize,
    pub(super) public: usize,
    pub(super) private: usize,
    pub(super) denied: usize,
}

// In-memory counterpart of the lazy scope decision: every record is decided
// against the already resolved lifecycle snapshot and group rules.
pub(super) fn select_visible_records(
    scope: &GraphVisibilityScope,
    visibility_cache: &MetadataVisibilityCache,
) -> RecordSelection {
    let mut selection = RecordSelection {
        visible: Vec::new(),
        deleted: 0,
        public: 0,
        private: 0,
        denied: 0,
    };
    for record in scope.records.iter() {
        if scope.record_deleted(visibility_cache, &record.graph_iri) {
            selection.deleted += 1;
            continue;
        }
        if record.public {
            selection.public += 1;
        } else {
            selection.private += 1;
        }
        if scope.permissions.record_visible(record) {
            selection.visible.push(record.clone());
        } else {
            selection.denied += 1;
        }
    }
    selection
}

// All-metadata reads resolve their authorization scope once for query evaluation.
pub(super) enum LocalReadScope<T> {
    Eager(T),
    Lazy(Arc<GraphVisibilityScope>),
}

pub(super) struct GraphVisibilityScope {
    pub(super) records: Arc<Vec<MetadataRegistryRecord>>,
    pub(super) permissions: GroupPermissionRules,
    pub(super) lifecycle_visibility: LifecycleVisibility,
}

pub(super) enum LifecycleVisibility {
    Cache(HashSet<String>),
    FreshDeletedGraphs(HashSet<String>),
}

impl GraphVisibilityScope {
    fn record_for_graph(&self, graph_iri: &str) -> Option<&MetadataRegistryRecord> {
        record_for_graph(&self.records, graph_iri)
    }

    // A tombstone in the request's own snapshot or in the cache hides the graph.
    fn record_deleted(&self, visibility_cache: &MetadataVisibilityCache, graph_iri: &str) -> bool {
        if matches!(
            visibility_cache.lifecycle_deleted_any(graph_iri),
            Some((true, _))
        ) {
            return true;
        }
        match &self.lifecycle_visibility {
            LifecycleVisibility::Cache(deleted_graphs)
            | LifecycleVisibility::FreshDeletedGraphs(deleted_graphs) => {
                deleted_graphs.contains(graph_iri)
            }
        }
    }

    fn record_visible(
        &self,
        visibility_cache: &MetadataVisibilityCache,
        record: &MetadataRegistryRecord,
    ) -> bool {
        // Rules are collected per group, but every record is decided on its own permission
        // path, so per-document grants and denials agree with the decision.
        !self.record_deleted(visibility_cache, &record.graph_iri)
            && self.permissions.record_visible(record)
    }

    // Digest of every graph this scope may evaluate, in registry order.
    pub(super) fn visible_digest(&self, visibility_cache: &MetadataVisibilityCache) -> [u8; 32] {
        let mut digest = ScopeDigest::default();
        for record in self.records.iter() {
            if self.record_visible(visibility_cache, record) {
                digest.push(&record.graph_iri);
            }
        }
        digest.finish()
    }

    // Graphs without a registry record stay invisible (fail closed).
    pub(super) fn graph_visible(
        &self,
        visibility_cache: &MetadataVisibilityCache,
        graph_iri: &str,
    ) -> bool {
        self.record_for_graph(graph_iri)
            .is_some_and(|record| self.record_visible(visibility_cache, record))
    }
}

// Canonical graph IRIs embed the document id (graph_iri_for), enabling an O(log n) lookup in
// the document-id-ordered snapshot; non-canonical IRIs fall back to a scan.
pub(super) fn record_for_graph<'a>(
    records: &'a [MetadataRegistryRecord],
    graph_iri: &str,
) -> Option<&'a MetadataRegistryRecord> {
    if let Some(document_id) = graph_iri
        .rsplit('/')
        .next()
        .and_then(|tail| Ulid::from_string(tail).ok())
        && let Ok(index) = records.binary_search_by(|record| record.document_id.cmp(&document_id))
        && records[index].graph_iri == graph_iri
    {
        return Some(&records[index]);
    }
    records.iter().find(|record| record.graph_iri == graph_iri)
}

pub(super) async fn resolve_visibility_scope(
    inner: &Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
) -> Result<GraphVisibilityScope, MetadataError> {
    let lifecycle_refresh = refresh_visibility(inner, &records).await?;
    let lifecycle_visibility = if lifecycle_refresh.store_accepted {
        LifecycleVisibility::Cache(lifecycle_refresh.deleted_graphs)
    } else {
        LifecycleVisibility::FreshDeletedGraphs(lifecycle_refresh.deleted_graphs)
    };
    let auth_realm = auth_context.as_ref().map(|auth| auth.realm_id);
    let context = DriverContext {
        storage_handle: inner.storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let permissions = GroupPermissionRules::collect(
        &context,
        auth_context.as_ref(),
        records
            .iter()
            .filter(|record| Some(record.realm_id) == auth_realm)
            .map(|record| record.group_id),
    )
    .await;
    // Request policy removes denied records from both eager and SPARQL paths.
    let evaluators = crate::auth::enforcement::PolicyEvaluator::load_bulk(
        &context,
        records
            .iter()
            .map(|record| (record.realm_id, record.group_id)),
    )
    .await
    .map_err(|error| MetadataError::Backend(error.to_string()))?;
    let policy_auth = auth_context.as_ref();
    let records = Arc::new(
        records
            .iter()
            .filter(|record| {
                evaluators
                    .get(&(record.realm_id, record.group_id))
                    .is_some_and(|evaluator| {
                        evaluator
                            .evaluate(&crate::metadata::api::read_types::metadata_read_request(
                                &record.permission_path,
                                policy_auth,
                            ))
                            .is_ok()
                    })
            })
            .cloned()
            .collect(),
    );
    Ok(GraphVisibilityScope {
        records,
        permissions,
        lifecycle_visibility,
    })
}

async fn refresh_visibility(
    inner: &Arc<MetadataInner>,
    records: &[MetadataRegistryRecord],
) -> Result<LifecycleVisibilityRefresh, MetadataError> {
    let fill_generation = inner.visibility_cache.current_generation();
    let (deleted_graphs, _) = list_deleted_iris(inner, REGISTRY_CANDIDATE_LIMIT).await?;
    let store_accepted = inner.visibility_cache.refresh_if_current(
        records.iter().map(|record| {
            (
                record.graph_iri.clone(),
                deleted_graphs.contains(&record.graph_iri),
            )
        }),
        fill_generation,
    );
    Ok(LifecycleVisibilityRefresh {
        deleted_graphs,
        store_accepted,
    })
}

/// Searches the local graphs `auth_context` may read.
#[tracing::instrument(
    name = "metadata.search.local_authorized",
    level = "debug",
    skip(context, auth_context, query),
    fields(
        query_len = query.len() as u64,
        limit = limit as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
    )
)]
pub async fn search_authorized_local(
    context: &DriverContext,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    group_id: Option<GroupId>,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let (metadata, graph) = handles(context)?;
    search_local_graphs(
        context,
        metadata.inner.clone(),
        graph,
        auth_context,
        graph_iris,
        query,
        limit,
        group_id,
        None,
    )
    .await
}

/// Searches the local graphs `auth_context` may read that name `object_iri` by `predicate_iri`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn search_local_filtered(
    context: &DriverContext,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    predicate_iri: String,
    object_iri: String,
    group_id: Option<GroupId>,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let (metadata, graph) = handles(context)?;
    search_local_graphs(
        context,
        metadata.inner.clone(),
        graph,
        auth_context,
        graph_iris,
        query,
        limit,
        group_id,
        Some((predicate_iri, object_iri)),
    )
    .await
}
