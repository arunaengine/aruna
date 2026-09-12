use super::effects::warn_slow_call;
use super::entity_convert::{decode_hit_properties, hit_from_craqle};
use super::lifecycle::{list_deleted_iris, list_read_records};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use aruna_core::metadata::{MetadataError, MetadataSearchHit};
use aruna_core::structs::{AuthContext, MetadataRegistryRecord};
use aruna_core::telemetry::{record_duration_ms, record_elapsed_ms};
use aruna_core::types::GroupId;
use craqle::{
    Action as CraqleAction, AuthorizationError as CraqleAuthError, Authorizer as CraqleAuthorizer,
    CraqleNode, DescribeRequest, GraphId, GraphPolicy, GraphSearchRequest, SearchRequest,
};
use futures_util::FutureExt;
use oxrdf::Term;
use tracing::{Span, debug_span, field, warn};
use ulid::Ulid;

use super::effects::{graph_ids, record_error};
use super::{
    METADATA_ENRICH_TASKS, METADATA_REGISTRY_CANDIDATE_LIMIT, MetadataHandle, MetadataInner,
    MetadataVisibilityCache,
};
use crate::auth::permission_rules::GroupPermissionRules;
use crate::driver::DriverContext;
use crate::metadata::query_cache::ScopeDigest;
use crate::metadata::search_cursor::{METADATA_SEARCH_MAX_PAGINATION_DEPTH, compare_hits};
use crate::metadata::search_enrichment::{hit_title, hit_types};

#[tracing::instrument(
    name = "metadata.search.local",
    level = "debug",
    skip(inner, auth_context, query, iri_filter),
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
    inner: Arc<MetadataInner>,
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
        .is_some_and(|graphs| graphs.len() > METADATA_REGISTRY_CANDIDATE_LIMIT)
    {
        return Err(MetadataError::Backend(
            "metadata candidate limit exceeded".to_string(),
        ));
    }

    let records = list_read_records(inner.clone(), &span).await?;
    // Without a candidate filter the request spans the realm, so craqle
    // authorizes the index hits it returns instead of every visible graph.
    let lazy = iri_filter.is_none() && graph_iris.is_none() && group_id.is_none();
    span.record("lazy", lazy);

    let result = if lazy {
        search_realm_scope(inner, auth_context, records, query, limit, &span).await
    } else {
        search_candidate_graphs(
            inner,
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
    warn_slow_call("search", None, elapsed);
}

async fn search_realm_scope(
    inner: Arc<MetadataInner>,
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
    let result = search_visible_scope(&inner, Arc::new(scope), query, limit, &search_span).await;
    record_backend_search(span, &search_span, search_started, &result);
    result
}

// Craqle post-filters the index hits it returns through the request's already
// resolved scope, so the work scales with the page instead of the realm.
async fn search_visible_scope(
    inner: &Arc<MetadataInner>,
    scope: Arc<GraphVisibilityScope>,
    query: String,
    limit: usize,
    search_span: &Span,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let describe = scope_hit_describe(inner, &scope);
    let mut hits = {
        let task_inner = inner.clone();
        let task_scope = scope.clone();
        let task_query = query.clone();
        let blocking_span = search_span.clone();
        let _permit = inner.craqle_read_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || {
            blocking_span.in_scope(|| {
                let authorizer = ScopeAuthorizer {
                    scope: &task_scope,
                    visibility_cache: &task_inner.visibility_cache,
                };
                task_inner
                    .node
                    .search(
                        &authorizer,
                        SearchRequest {
                            query: &task_query,
                            limit,
                        },
                    )
                    .map_err(|error| MetadataError::Backend(error.to_string()))
            })
        })
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))??
    };
    hits.retain(|hit| scope.record_for_graph(&hit.graph_id).is_some());
    let targets = hits
        .iter()
        .map(|hit| (hit.graph_id.clone(), hit.subject_iri.clone()))
        .collect::<Vec<_>>();
    let properties =
        describe_hits_parallel(&inner.craqle_read_permits, targets, describe, search_span).await;
    let mut visible = hits
        .into_iter()
        .zip(properties)
        .filter_map(|(hit, properties)| {
            let record = scope.record_for_graph(&hit.graph_id)?;
            Some(hit_from_craqle(hit, record, &properties, &query))
        })
        .collect::<Vec<_>>();
    // The unfiltered entry point returns raw index order; the search cursor's
    // watermark needs the merged score-descending order instead.
    visible.sort_by(compare_hits);
    Ok(visible)
}

// Requests narrowed by graph, group, or IRI filter keep a genuine candidate
// set, so pre-filtering it stays cheaper than post-filtering the whole index.
#[allow(clippy::too_many_arguments)]
async fn search_candidate_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    group_id: Option<GroupId>,
    iri_filter: Option<(String, String)>,
    span: &Span,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let iri_matches = match iri_filter.as_ref() {
        Some((predicate_iri, object_iri)) => Some(
            super::super::iri_index::lookup_iri_references(
                &inner.storage_handle,
                records.as_ref(),
                predicate_iri,
                object_iri,
            )
            .await?,
        ),
        None => None,
    };
    let records = match iri_matches.as_ref() {
        Some(matches) => Arc::new(
            records
                .iter()
                .filter(|record| matches.contains_key(&record.document_id))
                .cloned()
                .collect(),
        ),
        None => records,
    };

    let authorization_started = Instant::now();
    let allowed_records =
        select_authorized_records(inner.clone(), auth_context, records, graph_iris, group_id)
            .await?;
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
        let describe = allowed_hit_describe(
            &inner,
            Arc::new(AllowedGraphAuthorizer {
                graph_iris: allowed_graphs,
            }),
        );
        // The page is ordered by document id alone, so it is cut before the
        // describes instead of enriching every candidate.
        let mut candidates = allowed_records
            .into_iter()
            .filter_map(|record| {
                let subject_iri = matches.get(&record.document_id)?.first()?.clone();
                Some((record, subject_iri))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(record, _)| record.document_id);
        candidates.truncate(limit);
        let targets = candidates
            .iter()
            .map(|(record, subject_iri)| (record.graph_iri.clone(), subject_iri.clone()))
            .collect::<Vec<_>>();
        let properties =
            describe_hits_parallel(&inner.craqle_read_permits, targets, describe, span).await;
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
    let allowed_graphs = by_graph.keys().cloned().collect::<HashSet<_>>();
    let mut search_graphs = allowed_graphs.iter().cloned().collect::<Vec<_>>();
    search_graphs.sort_unstable();
    let graph_ids = graph_ids(&search_graphs);

    let search_span = debug_span!(
        "metadata.backend.craqle.search",
        lazy = false,
        graph_count = graph_ids.len() as u64,
        query_len = query.len() as u64,
        limit = limit as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    );
    let search_started = Instant::now();
    let authorizer = Arc::new(AllowedGraphAuthorizer {
        graph_iris: allowed_graphs,
    });
    let result = search_allowed_graphs(
        &inner,
        authorizer,
        by_graph,
        graph_ids,
        &query,
        limit,
        &search_span,
    )
    .await;
    record_backend_search(span, &search_span, search_started, &result);
    result
}

async fn search_allowed_graphs(
    inner: &Arc<MetadataInner>,
    authorizer: Arc<AllowedGraphAuthorizer>,
    by_graph: HashMap<String, MetadataRegistryRecord>,
    graph_ids: Vec<GraphId>,
    query: &str,
    limit: usize,
    search_span: &Span,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let describe = allowed_hit_describe(inner, authorizer.clone());
    let hits = {
        let task_inner = inner.clone();
        let task_query = query.to_string();
        let blocking_span = search_span.clone();
        let _permit = inner.craqle_read_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || {
            blocking_span.in_scope(|| {
                task_inner
                    .node
                    .search_graphs(
                        authorizer.as_ref(),
                        GraphSearchRequest {
                            graphs: &graph_ids,
                            query: &task_query,
                            limit,
                        },
                    )
                    .map_err(|error| MetadataError::Backend(error.to_string()))
            })
        })
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))??
    };
    let hits = hits
        .into_iter()
        .filter(|hit| by_graph.contains_key(&hit.graph_id))
        .take(limit)
        .collect::<Vec<_>>();
    let targets = hits
        .iter()
        .map(|hit| (hit.graph_id.clone(), hit.subject_iri.clone()))
        .collect::<Vec<_>>();
    let properties =
        describe_hits_parallel(&inner.craqle_read_permits, targets, describe, search_span).await;
    Ok(hits
        .into_iter()
        .zip(properties)
        .filter_map(|(hit, properties)| {
            let record = by_graph.get(&hit.graph_id)?;
            Some(hit_from_craqle(hit, record, &properties, query))
        })
        .collect())
}
/// Describes one hit's `(graph_iri, subject_iri)` against a fixed authorizer.
pub(super) type HitDescribe = Arc<dyn Fn(&str, &str) -> Vec<(String, Term)> + Send + Sync>;

/// Enriches hits in parallel, one blocking task per chunk of `targets`.
/// Returned properties align with `targets` by index: chunks are contiguous and
/// concatenated in order, so hit order never depends on task completion order.
pub(super) async fn describe_hits_parallel(
    read_permits: &Arc<tokio::sync::Semaphore>,
    targets: Vec<(String, String)>,
    describe: HitDescribe,
    span: &Span,
) -> Vec<Vec<(String, Term)>> {
    if targets.is_empty() {
        return Vec::new();
    }
    let chunk_size = targets.len().div_ceil(METADATA_ENRICH_TASKS);
    let tasks = targets
        .chunks(chunk_size)
        .map(<[(String, String)]>::to_vec)
        .map(|chunk| {
            let permits = read_permits.clone();
            let describe = describe.clone();
            let span = span.clone();
            async move {
                let chunk_len = chunk.len();
                let _permit = permits.acquire_owned().await.ok();
                tokio::task::spawn_blocking(move || {
                    span.in_scope(|| {
                        chunk
                            .iter()
                            .map(|(graph_iri, subject_iri)| describe(graph_iri, subject_iri))
                            .collect::<Vec<_>>()
                    })
                })
                .await
                .unwrap_or_else(|error| {
                    warn!(%error, "metadata search enrichment task failed");
                    vec![Vec::new(); chunk_len]
                })
            }
        })
        .collect::<Vec<_>>();
    futures_util::future::join_all(tasks)
        .await
        .into_iter()
        .flatten()
        .collect()
}

fn scope_hit_describe(
    inner: &Arc<MetadataInner>,
    scope: &Arc<GraphVisibilityScope>,
) -> HitDescribe {
    let inner = inner.clone();
    let scope = scope.clone();
    Arc::new(move |graph_iri, subject_iri| {
        let authorizer = ScopeAuthorizer {
            scope: &scope,
            visibility_cache: &inner.visibility_cache,
        };
        describe_hit_properties(&inner.node, &authorizer, graph_iri, subject_iri)
    })
}

fn allowed_hit_describe(
    inner: &Arc<MetadataInner>,
    authorizer: Arc<AllowedGraphAuthorizer>,
) -> HitDescribe {
    let inner = inner.clone();
    Arc::new(move |graph_iri, subject_iri| {
        describe_hit_properties(&inner.node, authorizer.as_ref(), graph_iri, subject_iri)
    })
}

// Enrichment is best-effort: a pending or raced projection must never fail the
// search, so fall back to an empty property set.
pub(super) fn describe_hit_properties(
    node: &CraqleNode,
    authorizer: &dyn CraqleAuthorizer,
    graph_iri: &str,
    subject_iri: &str,
) -> Vec<(String, Term)> {
    node.describe_subject(
        authorizer,
        DescribeRequest {
            graph: &GraphId::new(graph_iri),
            subject_id: subject_iri,
        },
    )
    .map(decode_hit_properties)
    .unwrap_or_default()
}

pub(super) fn clamp_remote_limit(limit: usize) -> usize {
    limit.clamp(1, METADATA_SEARCH_MAX_PAGINATION_DEPTH)
}

pub(super) struct AllowedGraphAuthorizer {
    pub(super) graph_iris: HashSet<String>,
}

impl CraqleAuthorizer for AllowedGraphAuthorizer {
    fn authorize(
        &self,
        graph: &GraphId,
        _policy: &GraphPolicy,
        action: CraqleAction,
    ) -> Result<(), CraqleAuthError> {
        if matches!(action, CraqleAction::Read) && self.graph_iris.contains(graph.as_str()) {
            return Ok(());
        }

        Err(CraqleAuthError::PermissionDenied {
            action,
            graph: graph.as_str().to_string(),
        })
    }
}

/// Lazy counterpart of [`AllowedGraphAuthorizer`], answering craqle per hit.
/// Craqle's stored policy is ignored on purpose: the registry record, lifecycle
/// tombstones and collected rules are authoritative, unknown graphs stay invisible.
pub(super) struct ScopeAuthorizer<'a> {
    pub(super) scope: &'a GraphVisibilityScope,
    pub(super) visibility_cache: &'a MetadataVisibilityCache,
}

impl CraqleAuthorizer for ScopeAuthorizer<'_> {
    fn authorize(
        &self,
        graph: &GraphId,
        _policy: &GraphPolicy,
        action: CraqleAction,
    ) -> Result<(), CraqleAuthError> {
        if matches!(action, CraqleAction::Read)
            && self
                .scope
                .graph_visible(self.visibility_cache, graph.as_str())
        {
            return Ok(());
        }

        Err(CraqleAuthError::PermissionDenied {
            action,
            graph: graph.as_str().to_string(),
        })
    }
}

pub(super) async fn list_visible_graphs(
    inner: Arc<MetadataInner>,
) -> Result<Vec<String>, MetadataError> {
    let records = inner
        .visibility_cache
        .registry_records_any()
        .map(|(records, _)| records)
        .ok_or_else(|| {
            MetadataError::Backend("metadata registry snapshot unavailable".to_string())
        })?;
    let records = super::super::api::filter_live_records(&inner.storage_handle, records.as_ref())
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    Ok(records.into_iter().map(|record| record.graph_iri).collect())
}

pub(super) async fn select_authorized_graphs(
    inner: Arc<MetadataInner>,
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
    inner: Arc<MetadataInner>,
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
    let scope = resolve_visibility_scope(&inner, auth_context, candidates)
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
    Lazy(GraphVisibilityScope),
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
        task_handle: None,
        compute_handle: None,
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
    let evaluators = crate::auth::request_policy::PolicyEvaluator::load_bulk(
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
                            .evaluate(&crate::metadata::api::metadata_read_request(
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
    let (deleted_graphs, _) = list_deleted_iris(inner, METADATA_REGISTRY_CANDIDATE_LIMIT).await?;
    let store_accepted = inner.visibility_cache.refresh_lifecycle_deleted(
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

impl MetadataHandle {
    #[tracing::instrument(
        name = "metadata.search.local_authorized",
        level = "debug",
        skip(self, auth_context, query),
        fields(
            query_len = query.len() as u64,
            limit = limit as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        )
    )]
    pub async fn search_authorized_local(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        group_id: Option<GroupId>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        search_local_graphs(
            self.inner.clone(),
            auth_context,
            graph_iris,
            query,
            limit,
            group_id,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_local_filtered(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        predicate_iri: String,
        object_iri: String,
        group_id: Option<GroupId>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        search_local_graphs(
            self.inner.clone(),
            auth_context,
            graph_iris,
            query,
            limit,
            group_id,
            Some((predicate_iri, object_iri)),
        )
        .await
    }
}
