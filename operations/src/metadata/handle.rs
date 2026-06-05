use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::{
    MetadataBatch, MetadataCreateCrateRequest, MetadataDot, MetadataEffect, MetadataError,
    MetadataEvent, MetadataGraphLifecycleRecord, MetadataGraphPolicy, MetadataQuadOp,
    MetadataQueryResults, MetadataRoCratePage, MetadataSearchHit, MetadataUpsertEntityRequest,
};
use aruna_core::storage_entries::metadata_graph_lifecycle_key;
use aruna_core::structs::{AuthContext, MetadataRegistryRecord, Permission};
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use craqle::{
    ActorId, AllowAllAuthorizer, Batch, CraqleError, CraqleIrokleOptions, CraqleNode,
    CraqleOptions, CreateCrateRequest, CreateEntityRequest, GraphId, GraphPolicy, QueryResults,
    RoCrateError, vocab,
};
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use serde_json::Value;
use tokio::time::{sleep, timeout};
use tracing::{Instrument, Span, debug_span, field, warn};

use super::protocol::{MetadataTransportMessage, read_message, write_message};
use super::repository::{iter_all_registry_effect, parse_registry_iter};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};

const METADATA_IO_TIMEOUT: Duration = Duration::from_secs(15);
const METADATA_GRAPH_SYNC_ATTEMPTS: usize = 3;
const METADATA_GRAPH_SYNC_RETRY_AFTER: Duration = Duration::from_millis(250);
const SLOW_METADATA_BACKEND_THRESHOLD: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct MetadataHandle {
    inner: Arc<MetadataInner>,
}

struct MetadataInner {
    node: Arc<CraqleNode>,
    storage_handle: StorageHandle,
    net_handle: Option<NetHandle>,
    irokle_db: Option<fjall::OptimisticTxDatabase>,
}

impl std::fmt::Debug for MetadataHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataHandle").finish_non_exhaustive()
    }
}

impl MetadataHandle {
    pub fn new(
        path: impl AsRef<Path>,
        node_id: NodeId,
        storage_handle: StorageHandle,
        net_handle: Option<NetHandle>,
        irokle_node: Option<irokle::Irokle<irokle::FjallStorage>>,
        irokle_db: Option<fjall::OptimisticTxDatabase>,
    ) -> Result<Self, MetadataError> {
        let actor = ActorId::from_bytes(*node_id.as_bytes());
        let options = CraqleOptions::new().with_actor(actor);
        let options = match irokle_node {
            Some(irokle_node) => options.with_irokle(irokle_node, CraqleIrokleOptions::new()),
            None => options,
        };
        let node = CraqleNode::open_with_options(path, options)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(MetadataInner {
                node: Arc::new(node),
                storage_handle,
                net_handle,
                irokle_db,
            }),
        })
    }

    pub async fn send_metadata_effect(&self, effect: MetadataEffect) -> Event {
        let effect_name = metadata_effect_kind(&effect);
        let graph_iri = effect_graph_iri(&effect);
        if let Some(graph_iri) = graph_iri.as_deref() {
            let span = debug_span!(
                "metadata.graph_lifecycle.read_before_effect",
                effect = effect_name,
                graph_iri,
                deleted = field::Empty,
                elapsed_ms = field::Empty,
            );
            let started = Instant::now();
            let result = graph_lifecycle_record(self.inner.storage_handle.clone(), graph_iri)
                .instrument(span.clone())
                .await;
            match result {
                Ok(Some(record)) if record.is_deleted() => {
                    span.record("deleted", true);
                    record_elapsed(&span, "elapsed_ms", started);
                    match &effect {
                        MetadataEffect::DeleteGraph { .. } => {}
                        MetadataEffect::SyncGraphBestEffort { graph_iri, peers } => {
                            return Event::Metadata(MetadataEvent::GraphSyncScheduled {
                                graph_iri: graph_iri.clone(),
                                peers: peers.clone(),
                            });
                        }
                        MetadataEffect::ContainsGraph { graph_iri } => {
                            return Event::Metadata(MetadataEvent::ContainsGraphResult {
                                graph_iri: graph_iri.clone(),
                                exists: false,
                            });
                        }
                        _ if effect_rejects_deleted_graph(&effect) => {
                            return Event::Metadata(MetadataEvent::Error {
                                graph_iri: Some(graph_iri.to_string()),
                                error: MetadataError::InvalidInput(format!(
                                    "metadata graph `{graph_iri}` is deleted"
                                )),
                            });
                        }
                        _ => {}
                    }
                }
                Ok(_) => {
                    span.record("deleted", false);
                    record_elapsed(&span, "elapsed_ms", started);
                }
                Err(error) => {
                    record_error(&span, &error.to_string());
                    record_elapsed(&span, "elapsed_ms", started);
                    return Event::Metadata(MetadataEvent::Error {
                        graph_iri: Some(graph_iri.to_string()),
                        error,
                    });
                }
            }
        }
        match effect {
            MetadataEffect::SyncGraphBestEffort { graph_iri, peers } => {
                Event::Metadata(self.schedule_graph_sync_best_effort(graph_iri, peers))
            }
            MetadataEffect::QueryGraphs {
                auth_context,
                graph_iris,
                sparql,
            } => Event::Metadata(
                match self
                    .query_authorized_local(auth_context, graph_iris, sparql)
                    .await
                {
                    Ok(results) => MetadataEvent::QueryResult { results },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                },
            ),
            MetadataEffect::SearchGraphs {
                auth_context,
                graph_iris,
                query,
                limit,
            } => Event::Metadata(
                match self
                    .search_authorized_local(auth_context, graph_iris, query, limit)
                    .await
                {
                    Ok(hits) => MetadataEvent::SearchResult { hits },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                },
            ),
            other => {
                let inner = self.inner.clone();
                let span = debug_span!(
                    "metadata.backend.blocking_task",
                    effect = metadata_effect_kind(&other),
                    graph_iri = graph_iri.as_deref().unwrap_or("<none>"),
                    elapsed_ms = field::Empty,
                    result = field::Empty,
                );
                let blocking_span = span.clone();
                let started = Instant::now();
                let metadata_event = match tokio::task::spawn_blocking(move || {
                    blocking_span.in_scope(|| handle_effect(inner, other))
                })
                .await
                {
                    Ok(event) => event,
                    Err(error) => {
                        record_error(&span, &error.to_string());
                        MetadataEvent::Error {
                            graph_iri,
                            error: MetadataError::TaskJoin(error.to_string()),
                        }
                    }
                };
                record_elapsed(&span, "elapsed_ms", started);
                span.record("result", metadata_event_kind(&metadata_event));
                Event::Metadata(metadata_event)
            }
        }
    }

    pub async fn reconcile_irokle(&self) -> Result<usize, MetadataError> {
        let inner = self.inner.clone();
        let applied = tokio::task::spawn_blocking(move || inner.node.reconcile_irokle())
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        self.prune_deleted_graphs().await?;
        Ok(applied)
    }

    pub async fn prune_deleted_graphs(&self) -> Result<usize, MetadataError> {
        let inner = self.inner.clone();
        let graphs = tokio::task::spawn_blocking(move || inner.node.graphs())
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let mut pruned = 0usize;
        for graph in graphs {
            let graph_iri = graph.as_str().to_string();
            let Some(record) =
                graph_lifecycle_record(self.inner.storage_handle.clone(), &graph_iri).await?
            else {
                continue;
            };
            if !record.is_deleted() {
                continue;
            }
            delete_local_graph(self.inner.node.clone(), graph_iri).await?;
            pruned += 1;
        }
        Ok(pruned)
    }

    fn schedule_graph_sync_best_effort(
        &self,
        graph_iri: String,
        mut peers: Vec<NodeId>,
    ) -> MetadataEvent {
        if let Some(net_handle) = self.inner.net_handle.as_ref() {
            peers.retain(|peer| *peer != net_handle.node_id());
        }
        peers.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        peers.dedup();
        if peers.is_empty() {
            return MetadataEvent::GraphSyncScheduled { graph_iri, peers };
        }

        let inner = self.inner.clone();
        let graph_iri_for_task = graph_iri.clone();
        let peers_for_task = peers.clone();
        tokio::spawn(async move {
            for attempt in 1..=METADATA_GRAPH_SYNC_ATTEMPTS {
                match sync_graph_once(
                    inner.clone(),
                    graph_iri_for_task.clone(),
                    peers_for_task.clone(),
                )
                .await
                {
                    Ok(()) => return,
                    Err(error) => {
                        warn!(
                            graph_iri = %graph_iri_for_task,
                            attempt,
                            attempts = METADATA_GRAPH_SYNC_ATTEMPTS,
                            error = ?error,
                            "Metadata graph sync attempt failed"
                        );
                        if attempt < METADATA_GRAPH_SYNC_ATTEMPTS {
                            sleep(METADATA_GRAPH_SYNC_RETRY_AFTER).await;
                        }
                    }
                }
            }
            warn!(
                graph_iri = %graph_iri_for_task,
                peer_count = peers_for_task.len(),
                "Metadata graph sync retries exhausted"
            );
        });

        MetadataEvent::GraphSyncScheduled { graph_iri, peers }
    }

    #[tracing::instrument(
        name = "metadata.remote.inbound",
        level = "debug",
        skip(self, stream),
        fields(
            peer = ?_peer,
            request = field::Empty,
            response = field::Empty,
            read_ms = field::Empty,
            process_ms = field::Empty,
            drain_ms = field::Empty,
            write_ms = field::Empty,
            elapsed_ms = field::Empty,
        )
    )]
    pub async fn handle_inbound_stream(
        &self,
        mut stream: BiStream,
        _peer: NodeId,
    ) -> Result<(), MetadataError> {
        let total_started = Instant::now();
        let read_started = Instant::now();
        let message = read_transport_message(&mut stream).await?;
        let span = Span::current();
        record_elapsed(&span, "read_ms", read_started);
        span.record("request", metadata_transport_message_kind(&message));

        let process_started = Instant::now();
        let response = match message {
            MetadataTransportMessage::QueryGraphs {
                auth_context,
                graph_iris,
                sparql,
            } => {
                match query_local_graphs(self.inner.clone(), auth_context, graph_iris, sparql).await
                {
                    Ok(results) => MetadataTransportMessage::QueryResults { results },
                    Err(error) => MetadataTransportMessage::Reject(error.to_string()),
                }
            }
            MetadataTransportMessage::SearchGraphs {
                auth_context,
                graph_iris,
                query,
                limit,
            } => match search_local_graphs(
                self.inner.clone(),
                auth_context,
                graph_iris,
                query,
                limit,
            )
            .await
            {
                Ok(hits) => MetadataTransportMessage::SearchResults { hits },
                Err(error) => MetadataTransportMessage::Reject(error.to_string()),
            },
            MetadataTransportMessage::QueryResults { .. }
            | MetadataTransportMessage::SearchResults { .. }
            | MetadataTransportMessage::Reject(_) => {
                MetadataTransportMessage::Reject("unexpected metadata control message".to_string())
            }
        };
        record_elapsed(&span, "process_ms", process_started);

        let drain_started = Instant::now();
        drain_request_stream(&mut stream).await?;
        record_elapsed(&span, "drain_ms", drain_started);

        let write_started = Instant::now();
        let _ = write_transport_message(&mut stream, &response).await;
        record_elapsed(&span, "write_ms", write_started);
        close_stream(&mut stream).await;
        record_elapsed(&span, "elapsed_ms", total_started);
        span.record("response", metadata_transport_message_kind(&response));
        Ok(())
    }

    #[tracing::instrument(
        name = "metadata.query.local_authorized",
        level = "debug",
        skip(self, auth_context, sparql),
        fields(
            query_len = sparql.len() as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        )
    )]
    pub async fn query_authorized_local(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataError> {
        query_local_graphs(self.inner.clone(), auth_context, graph_iris, sparql).await
    }

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
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        search_local_graphs(self.inner.clone(), auth_context, graph_iris, query, limit).await
    }

    pub async fn flush_search_updates(&self) -> Result<(), MetadataError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.node.flush_search_updates())
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
            .map_err(metadata_error_from_craqle)
    }

    #[tracing::instrument(
        name = "metadata.query.remote",
        level = "debug",
        skip(self, auth_context, sparql),
        fields(
            peer = ?node_id,
            query_len = sparql.len() as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            row_count = field::Empty,
            triple_count = field::Empty,
        )
    )]
    pub async fn request_remote_query_graphs(
        &self,
        node_id: NodeId,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataError> {
        let started = Instant::now();
        let span = Span::current();
        let Some(net_handle) = self.inner.net_handle.clone() else {
            record_error(&span, "metadata net handle missing");
            return Err(MetadataError::HandleMissing);
        };
        let result = match send_request(
            &net_handle,
            node_id,
            MetadataTransportMessage::QueryGraphs {
                auth_context,
                graph_iris,
                sparql,
            },
        )
        .await?
        {
            MetadataTransportMessage::QueryResults { results } => Ok(results),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected metadata query response: {other:?}"
            ))),
        };
        record_elapsed(&span, "elapsed_ms", started);
        match &result {
            Ok(results) => {
                span.record("result", metadata_query_result_kind(results));
                record_metadata_query_result_counts(&span, results);
            }
            Err(error) => record_error(&span, &error.to_string()),
        }
        result
    }

    #[tracing::instrument(
        name = "metadata.search.remote",
        level = "debug",
        skip(self, auth_context, query),
        fields(
            peer = ?node_id,
            query_len = query.len() as u64,
            limit = limit as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            hit_count = field::Empty,
        )
    )]
    pub async fn request_remote_search_graphs(
        &self,
        node_id: NodeId,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        let started = Instant::now();
        let span = Span::current();
        let Some(net_handle) = self.inner.net_handle.clone() else {
            record_error(&span, "metadata net handle missing");
            return Err(MetadataError::HandleMissing);
        };
        let result = match send_request(
            &net_handle,
            node_id,
            MetadataTransportMessage::SearchGraphs {
                auth_context,
                graph_iris,
                query,
                limit,
            },
        )
        .await?
        {
            MetadataTransportMessage::SearchResults { hits } => Ok(hits),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected metadata search response: {other:?}"
            ))),
        };
        record_elapsed(&span, "elapsed_ms", started);
        match &result {
            Ok(hits) => {
                span.record("result", "ok");
                span.record("hit_count", hits.len() as u64);
            }
            Err(error) => record_error(&span, &error.to_string()),
        }
        result
    }
}

async fn graph_lifecycle_record(
    storage_handle: StorageHandle,
    graph_iri: &str,
) -> Result<Option<MetadataGraphLifecycleRecord>, MetadataError> {
    match storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_graph_lifecycle_key(graph_iri),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes(&bytes)
                    .map_err(|error| MetadataError::Backend(error.to_string()))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataError::Backend(error.to_string()))
        }
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata graph lifecycle read result: {other:?}"
        ))),
    }
}

async fn metadata_graph_deleted(
    storage_handle: StorageHandle,
    graph_iri: &str,
) -> Result<bool, MetadataError> {
    Ok(graph_lifecycle_record(storage_handle, graph_iri)
        .await?
        .map(|record| record.is_deleted())
        .unwrap_or(false))
}

async fn delete_local_graph(node: Arc<CraqleNode>, graph_iri: String) -> Result<(), MetadataError> {
    tokio::task::spawn_blocking(move || node.delete_graph_unchecked(&GraphId::new(&graph_iri)))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(metadata_error_from_craqle)
}

fn effect_rejects_deleted_graph(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::CreateCrate { .. }
            | MetadataEffect::ApplyRoCrate { .. }
            | MetadataEffect::UpsertDataEntity { .. }
            | MetadataEffect::UpsertContextualEntity { .. }
            | MetadataEffect::SetGraphPolicy { .. }
            | MetadataEffect::AddGraphPeer { .. }
            | MetadataEffect::GetGraphPolicy { .. }
            | MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportRoCratePage { .. }
    )
}

#[async_trait]
impl Handle for MetadataHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Metadata(metadata_effect) => self.send_metadata_effect(metadata_effect).await,
            _ => Event::Metadata(MetadataEvent::Error {
                graph_iri: None,
                error: MetadataError::InvalidEffect,
            }),
        }
    }
}

#[tracing::instrument(
    name = "metadata.graph_sync.once",
    level = "debug",
    skip(inner),
    fields(
        graph_iri = %graph_iri,
        peer_count = peers.len() as u64,
        local_peer_setup_ms = field::Empty,
        network_sync_ms = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn sync_graph_once(
    inner: Arc<MetadataInner>,
    graph_iri: String,
    peers: Vec<NodeId>,
) -> Result<(), MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();
    if peers.is_empty() {
        return Ok(());
    }
    if metadata_graph_deleted(inner.storage_handle.clone(), &graph_iri).await? {
        return Ok(());
    }
    let net_handle = inner
        .net_handle
        .clone()
        .ok_or(MetadataError::HandleMissing)?;
    let node = inner.node.clone();
    let graph_iri_for_blocking = graph_iri.clone();
    let peers_for_blocking = peers.clone();
    let setup_span = debug_span!(
        "metadata.backend.craqle.ensure_irokle_topic",
        graph_iri = %graph_iri_for_blocking,
        peer_count = peers_for_blocking.len() as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let blocking_span = setup_span.clone();
    let setup_started = Instant::now();
    let topic_id = tokio::task::spawn_blocking(move || {
        blocking_span.in_scope(|| {
            let graph = GraphId::new(&graph_iri_for_blocking);
            for peer in peers_for_blocking {
                node.add_irokle_peer(&graph, irokle_peer_id(peer))?;
            }
            node.ensure_irokle_topic(&graph)
        })
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?;
    record_elapsed(&setup_span, "elapsed_ms", setup_started);
    record_elapsed(&span, "local_peer_setup_ms", setup_started);
    match &topic_id {
        Ok(_) => {
            setup_span.record("result", "ok");
        }
        Err(error) => record_error(&setup_span, &error.to_string()),
    }
    let topic_id = topic_id.map_err(metadata_error_from_craqle)?;

    let sync_started = Instant::now();
    net_handle
        .sync_irokle_topic_with_peers(topic_id, peers)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed(&span, "network_sync_ms", sync_started);
    record_elapsed(&span, "elapsed_ms", total_started);
    Ok(())
}

fn handle_effect(inner: Arc<MetadataInner>, effect: MetadataEffect) -> MetadataEvent {
    let effect_name = metadata_effect_kind(&effect);
    let auth = AllowAllAuthorizer;
    let graph_iri = effect_graph_iri(&effect);
    let persist_irokle_after_success = metadata_effect_persists_irokle(&effect);
    let node = inner.node.clone();
    let effect_span = debug_span!(
        "metadata.backend.effect",
        effect = effect_name,
        graph_iri = graph_iri.as_deref().unwrap_or("<none>"),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let effect_started = Instant::now();
    let result = effect_span.in_scope(|| match effect {
        MetadataEffect::CreateCrate { request } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.create_crate",
                graph_iri = %request.graph_iri,
                name_len = request.name.len() as u64,
                description_len = request.description.len() as u64,
                public = request.policy.public,
                permission_path_count = request.policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.create_crate(&auth, craqle_create_request(request.clone())));
            record_craqle_call_result(
                &call_span,
                "create_crate",
                Some(&request.graph_iri),
                started,
                &result,
            );
            if let Ok(batch) = &result {
                call_span.record("batch_ops", batch.ops.len() as u64);
            }
            result.map(|batch| MetadataEvent::CreateCrateResult {
                graph_iri: request.graph_iri,
                batch: metadata_batch_from_craqle(batch),
            })
        }
        MetadataEffect::ApplyRoCrate { request } => {
            let graph_iri = request.graph_iri.clone();
            let policy = request.policy;
            let jsonld = request.jsonld;
            let call_span = debug_span!(
                "metadata.backend.craqle.apply_rocrate",
                graph_iri = %graph_iri,
                jsonld_len = jsonld.len() as u64,
                public = policy.public,
                permission_path_count = policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| {
                node.apply_rocrate_document_checked_with_policy(
                    &auth,
                    GraphId::new(&graph_iri),
                    &jsonld,
                    craqle_graph_policy(policy),
                )
            });
            record_craqle_call_result(
                &call_span,
                "apply_rocrate",
                Some(&graph_iri),
                started,
                &result,
            );
            if let Ok(batch) = &result {
                call_span.record("batch_ops", batch.ops.len() as u64);
            }
            result.map(|batch| MetadataEvent::ApplyRoCrateResult {
                graph_iri,
                batch: metadata_batch_from_craqle(batch),
            })
        }
        MetadataEffect::UpsertDataEntity { request } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.upsert_data_entity",
                graph_iri = %request.graph_iri,
                jsonld_len = request.jsonld.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let graph_iri = request.graph_iri.clone();
            let started = Instant::now();
            let result = call_span.in_scope(|| upsert_data_entity(&node, &auth, request));
            let converted = result.map(|batch| {
                call_span.record("batch_ops", batch.ops.len() as u64);
                MetadataEvent::EntityUpsertResult {
                    graph_iri: batch.graph_iri.clone(),
                    batch,
                }
            });
            record_metadata_result(
                &call_span,
                "upsert_data_entity",
                Some(&graph_iri),
                started,
                &converted,
            );
            converted
        }
        MetadataEffect::UpsertContextualEntity { request } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.upsert_contextual_entity",
                graph_iri = %request.graph_iri,
                jsonld_len = request.jsonld.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let graph_iri = request.graph_iri.clone();
            let started = Instant::now();
            let result = call_span.in_scope(|| upsert_contextual_entity(&node, &auth, request));
            let converted = result.map(|batch| {
                call_span.record("batch_ops", batch.ops.len() as u64);
                MetadataEvent::EntityUpsertResult {
                    graph_iri: batch.graph_iri.clone(),
                    batch,
                }
            });
            record_metadata_result(
                &call_span,
                "upsert_contextual_entity",
                Some(&graph_iri),
                started,
                &converted,
            );
            converted
        }
        MetadataEffect::SetGraphPolicy { graph_iri, policy } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.set_graph_policy",
                graph_iri = %graph_iri,
                public = policy.public,
                permission_path_count = policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| {
                    node.import_graph_policy(&GraphId::new(&graph_iri), craqle_graph_policy(policy))
                })
                .map(|_| MetadataEvent::GraphPolicySet {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "set_graph_policy",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::AddGraphPeer { graph_iri, node_id } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.add_graph_peer",
                graph_iri = %graph_iri,
                peer = ?node_id,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| {
                    node.add_irokle_peer(&GraphId::new(&graph_iri), irokle_peer_id(node_id))
                })
                .map(|_| MetadataEvent::GraphPeerAdded {
                    graph_iri: graph_iri.clone(),
                    node_id,
                });
            record_metadata_result(
                &call_span,
                "add_graph_peer",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::GetGraphPolicy { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.get_graph_policy",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.graph_policy(&GraphId::new(&graph_iri)))
                .map(|policy| MetadataEvent::GraphPolicyResult {
                    graph_iri: graph_iri.clone(),
                    policy: metadata_graph_policy_from_craqle(policy),
                });
            record_metadata_result(
                &call_span,
                "get_graph_policy",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ExportRoCrate { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                jsonld_len = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.export_rocrate(&auth, &GraphId::new(&graph_iri)))
                .map(|jsonld| {
                    call_span.record("jsonld_len", jsonld.len() as u64);
                    MetadataEvent::RoCrateExportResult {
                        graph_iri: graph_iri.clone(),
                        jsonld,
                    }
                });
            record_metadata_result(
                &call_span,
                "export_rocrate",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ExportRoCrateSummary { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate_summary",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                jsonld_len = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.export_rocrate_summary(&auth, &GraphId::new(&graph_iri)))
                .map(|jsonld| {
                    call_span.record("jsonld_len", jsonld.len() as u64);
                    MetadataEvent::RoCrateSummaryResult {
                        graph_iri: graph_iri.clone(),
                        jsonld,
                    }
                });
            record_metadata_result(
                &call_span,
                "export_rocrate_summary",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ExportRoCratePage {
            graph_iri,
            offset,
            after,
            limit,
        } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate_page",
                graph_iri = %graph_iri,
                offset = offset.unwrap_or(0) as u64,
                after_present = after.is_some(),
                limit = limit as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                returned_data_entities = field::Empty,
                total_data_entities = field::Empty,
            );
            let started = Instant::now();
            let graph = GraphId::new(&graph_iri);
            let page = call_span.in_scope(|| {
                if let Some(after) = after.as_deref() {
                    node.export_rocrate_page_after(&auth, &graph, Some(after), limit)
                } else {
                    node.export_rocrate_page(&auth, &graph, offset.unwrap_or(0), limit)
                }
            });
            let result = page.map(|page| {
                call_span.record("returned_data_entities", page.returned_data_entities as u64);
                call_span.record("total_data_entities", page.total_data_entities as u64);
                MetadataEvent::RoCratePageResult {
                    graph_iri: graph_iri.clone(),
                    page: metadata_rocrate_page_from_craqle(page),
                }
            });
            record_metadata_result(
                &call_span,
                "export_rocrate_page",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::SearchGraphs { .. }
        | MetadataEffect::QueryGraphs { .. }
        | MetadataEffect::SyncGraphBestEffort { .. } => {
            unreachable!("handled asynchronously")
        }
        MetadataEffect::DeleteGraph { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.delete_graph",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.delete_graph_unchecked(&GraphId::new(&graph_iri)))
                .map(|_| MetadataEvent::GraphDeleted {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "delete_graph",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ListGraphs => {
            let call_span = debug_span!(
                "metadata.backend.craqle.list_graphs",
                elapsed_ms = field::Empty,
                result = field::Empty,
                graph_count = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| node.graphs()).map(|graphs| {
                call_span.record("graph_count", graphs.len() as u64);
                MetadataEvent::GraphListResult {
                    graph_iris: graphs
                        .into_iter()
                        .map(|graph| graph.as_str().to_string())
                        .collect(),
                }
            });
            record_metadata_result(&call_span, "list_graphs", None, started, &result);
            result
        }
        MetadataEffect::ContainsGraph { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.contains_graph",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                exists = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.contains_graph(&GraphId::new(&graph_iri)))
                .map(|exists| {
                    call_span.record("exists", exists);
                    MetadataEvent::ContainsGraphResult {
                        graph_iri: graph_iri.clone(),
                        exists,
                    }
                });
            record_metadata_result(
                &call_span,
                "contains_graph",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
    });

    let persist_error = if persist_irokle_after_success && result.is_ok() {
        persist_irokle_journal(&inner, effect_name, graph_iri.as_deref()).err()
    } else {
        None
    };
    record_elapsed(&effect_span, "elapsed_ms", effect_started);
    let event = match (result, persist_error) {
        (_, Some(error)) => MetadataEvent::Error { graph_iri, error },
        (Ok(event), None) => event,
        (Err(error), None) => MetadataEvent::Error {
            graph_iri,
            error: metadata_error_from_craqle(error),
        },
    };
    effect_span.record("result", metadata_event_kind(&event));
    if let MetadataEvent::Error { error, .. } = &event {
        record_error(&effect_span, &error.to_string());
    }
    event
}

fn persist_irokle_journal(
    inner: &MetadataInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) -> Result<(), MetadataError> {
    let Some(db) = &inner.irokle_db else {
        return Ok(());
    };
    let span = debug_span!(
        "metadata.backend.irokle.persist",
        effect = effect_name,
        graph_iri = graph_iri.unwrap_or("<none>"),
        mode = "sync_data",
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(|| db.persist(fjall::PersistMode::SyncData));
    record_elapsed(&span, "elapsed_ms", started);
    match result {
        Ok(()) => {
            span.record("result", "ok");
            Ok(())
        }
        Err(error) => {
            record_error(&span, &error.to_string());
            Err(MetadataError::Backend(format!(
                "failed to persist irokle journal: {error}"
            )))
        }
    }
}

fn metadata_effect_persists_irokle(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::CreateCrate { .. }
            | MetadataEffect::ApplyRoCrate { .. }
            | MetadataEffect::UpsertDataEntity { .. }
            | MetadataEffect::UpsertContextualEntity { .. }
            | MetadataEffect::SetGraphPolicy { .. }
            | MetadataEffect::AddGraphPeer { .. }
            | MetadataEffect::DeleteGraph { .. }
    )
}

fn upsert_data_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: MetadataUpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    let entity_request = craqle_entity_request(&graph, &request.jsonld)?;
    node.add_data_entity_with(auth, entity_request)
        .map(metadata_batch_from_craqle)
}

fn upsert_contextual_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: MetadataUpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    let entity_request = craqle_entity_request(&graph, &request.jsonld)?;
    node.add_contextual_entity_with(auth, entity_request)
        .map(metadata_batch_from_craqle)
}

fn craqle_entity_request(
    graph: &GraphId,
    jsonld: &str,
) -> Result<CreateEntityRequest, CraqleError> {
    let value: Value = serde_json::from_str(jsonld).map_err(|error| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(error.to_string()))
    })?;
    let object = value.as_object().ok_or_else(|| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity payload must be a JSON object".to_string(),
        ))
    })?;
    if object.contains_key("@graph") || object.contains_key("graph") {
        return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity payload must not contain `@graph`; send a single JSON-LD entity object"
                .to_string(),
        )));
    }

    let entity_id = entity_identifier(object)?;
    let mut entity_types = entity_types(object)?;
    let entity_type = entity_types.remove(0);
    let name = entity_name(object)?;
    let mut additional_triples = Vec::new();
    for extra_type in entity_types {
        additional_triples.push((vocab::rdf_type(), class_term(&extra_type)?));
    }

    for (property, property_value) in object {
        if matches!(
            property.as_str(),
            "@context" | "@id" | "id" | "@type" | "type" | "name"
        ) {
            continue;
        }
        let property = normalize_property(property);
        let predicate = property_named_node(&property)?;
        for object in property_value_terms(&property, property_value)? {
            additional_triples.push((predicate.clone(), object));
        }
    }

    Ok(CreateEntityRequest {
        graph: graph.clone(),
        entity_id,
        entity_type,
        name,
        additional_triples,
    })
}

fn entity_identifier(object: &serde_json::Map<String, Value>) -> Result<String, CraqleError> {
    object
        .get("@id")
        .or_else(|| object.get("id"))
        .and_then(Value::as_str)
        .map(normalize_entity_id)
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define string `@id`".to_string(),
            ))
        })
}

fn entity_types(object: &serde_json::Map<String, Value>) -> Result<Vec<String>, CraqleError> {
    let value = object
        .get("@type")
        .or_else(|| object.get("type"))
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define `@type`".to_string(),
            ))
        })?;
    let mut types = Vec::new();
    match value {
        Value::String(value) => types.push(value.clone()),
        Value::Array(values) => {
            for value in values {
                let Some(value) = value.as_str() else {
                    return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                        "entity `@type` arrays must contain only strings".to_string(),
                    )));
                };
                types.push(value.to_string());
            }
        }
        _ => {
            return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity `@type` must be a string or array of strings".to_string(),
            )));
        }
    }
    if types.is_empty() {
        return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity `@type` must not be empty".to_string(),
        )));
    }
    Ok(types)
}

fn entity_name(object: &serde_json::Map<String, Value>) -> Result<String, CraqleError> {
    object
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define string `name`".to_string(),
            ))
        })
}

fn property_named_node(property: &str) -> Result<NamedNode, CraqleError> {
    match property {
        "@type" | "type" => Ok(vocab::rdf_type()),
        "name" => Ok(vocab::schema_name()),
        "description" => Ok(vocab::schema_description()),
        "keywords" => Ok(vocab::schema_keywords()),
        "datePublished" => Ok(vocab::schema_date_published()),
        "license" => Ok(vocab::schema_license()),
        "about" => Ok(vocab::schema_about()),
        "conformsTo" => Ok(vocab::schema_conforms_to()),
        other if other.contains("://") => Ok(NamedNode::new_unchecked(other)),
        other if other.contains(':') => expand_known_compact_iri(other),
        other => Ok(NamedNode::new_unchecked(format!(
            "http://schema.org/{}",
            normalize_term(other)
        ))),
    }
}

fn property_value_terms(property: &str, value: &Value) -> Result<Vec<Term>, CraqleError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Bool(boolean) => Ok(vec![Term::Literal(Literal::new_typed_literal(
            boolean.to_string(),
            NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
        ))]),
        Value::Number(number) => Ok(vec![number_literal(number)]),
        Value::String(text) => {
            let mapped = normalize_entity_id(text);
            let value = if property_expects_identifier(property) {
                mapped.as_str()
            } else {
                text
            };
            Ok(vec![property_value_term(property, value)?])
        }
        Value::Array(values) => {
            let mut objects = Vec::new();
            for entry in values {
                objects.extend(property_value_terms(property, entry)?);
            }
            Ok(objects)
        }
        Value::Object(object) if is_reference_object(object) => {
            let id = object
                .get("@id")
                .or_else(|| object.get("id"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(format!(
                        "property `{property}` reference object is missing string `@id`"
                    )))
                })?;
            Ok(vec![reference_term(&normalize_entity_id(id))?])
        }
        Value::Object(object) if is_value_object(object) => Ok(vec![value_object_term(object)?]),
        Value::Object(_) => Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            format!(
                "property `{property}` contains an inline nested object; nested entities must be separate top-level entities referenced by `@id`"
            ),
        ))),
    }
}

fn property_value_term(property: &str, value: &str) -> Result<Term, CraqleError> {
    match property {
        "@type" | "type" => class_term(value),
        "license" | "about" | "conformsTo" => {
            if looks_like_identifier(value) {
                reference_term(value)
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(value)))
            }
        }
        _ => Ok(Term::Literal(Literal::new_simple_literal(value))),
    }
}

fn class_term(value: &str) -> Result<Term, CraqleError> {
    let iri = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else if value.contains(':') {
        expand_known_compact_iri(value)?.as_str().to_string()
    } else {
        format!("http://schema.org/{}", normalize_term(value))
    };
    Ok(Term::NamedNode(NamedNode::new_unchecked(iri)))
}

fn reference_term(value: &str) -> Result<Term, CraqleError> {
    if let Some(value) = value.strip_prefix("_:") {
        Ok(Term::BlankNode(BlankNode::new_unchecked(value)))
    } else if value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.contains("://")
    {
        Ok(Term::NamedNode(NamedNode::new_unchecked(value)))
    } else if value.contains(':') {
        Ok(Term::NamedNode(expand_known_compact_iri(value)?))
    } else {
        Err(CraqleError::RoCrate(RoCrateError::UnsupportedTerm(
            value.to_string(),
        )))
    }
}

fn number_literal(number: &serde_json::Number) -> Term {
    let datatype = if number.as_i64().is_some() || number.as_u64().is_some() {
        "http://www.w3.org/2001/XMLSchema#integer"
    } else {
        "http://www.w3.org/2001/XMLSchema#double"
    };
    Term::Literal(Literal::new_typed_literal(
        number.to_string(),
        NamedNode::new_unchecked(datatype),
    ))
}

fn value_object_term(object: &serde_json::Map<String, Value>) -> Result<Term, CraqleError> {
    let value = object
        .get("@value")
        .or_else(|| object.get("value"))
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "value object missing `@value`".to_string(),
            ))
        })?;
    let language = object
        .get("@language")
        .or_else(|| object.get("language"))
        .and_then(Value::as_str);
    let datatype = object
        .get("@type")
        .or_else(|| object.get("type"))
        .and_then(Value::as_str);

    match value {
        Value::String(text) => {
            if let Some(language) = language {
                Ok(Term::Literal(
                    Literal::new_language_tagged_literal_unchecked(text, language),
                ))
            } else if let Some(datatype) = datatype {
                Ok(Term::Literal(Literal::new_typed_literal(
                    text.clone(),
                    datatype_named_node(datatype)?,
                )))
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(text)))
            }
        }
        Value::Bool(boolean) => Ok(Term::Literal(Literal::new_typed_literal(
            boolean.to_string(),
            datatype
                .map(datatype_named_node)
                .transpose()?
                .unwrap_or_else(|| {
                    NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean")
                }),
        ))),
        Value::Number(number) => Ok(Term::Literal(Literal::new_typed_literal(
            number.to_string(),
            datatype
                .map(datatype_named_node)
                .transpose()?
                .unwrap_or_else(|| {
                    if number.as_i64().is_some() || number.as_u64().is_some() {
                        NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer")
                    } else {
                        NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double")
                    }
                }),
        ))),
        Value::Null => Ok(Term::Literal(Literal::new_simple_literal(""))),
        Value::Array(_) | Value::Object(_) => Err(CraqleError::RoCrate(
            RoCrateError::UnsupportedJsonLd("value object `@value` must be scalar".to_string()),
        )),
    }
}

fn datatype_named_node(datatype: &str) -> Result<NamedNode, CraqleError> {
    if datatype.starts_with("http://") || datatype.starts_with("https://") {
        Ok(NamedNode::new_unchecked(datatype))
    } else {
        expand_known_compact_iri(datatype)
    }
}

fn expand_known_compact_iri(value: &str) -> Result<NamedNode, CraqleError> {
    if let Some(local) = value.strip_prefix("schema:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://schema.org/{local}"
        )))
    } else if let Some(local) = value.strip_prefix("rdf:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#{local}"
        )))
    } else if let Some(local) = value.strip_prefix("rdfs:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://www.w3.org/2000/01/rdf-schema#{local}"
        )))
    } else {
        Err(CraqleError::RoCrate(RoCrateError::UnsupportedTerm(
            value.to_string(),
        )))
    }
}

fn normalize_property(property: &str) -> String {
    property
        .strip_prefix("schema:")
        .or_else(|| property.strip_prefix("http://schema.org/"))
        .or_else(|| property.strip_prefix("https://schema.org/"))
        .map(str::to_string)
        .unwrap_or_else(|| property.to_string())
}

fn normalize_term(term: &str) -> String {
    normalize_property(term)
}

fn normalize_entity_id(id: &str) -> String {
    if id == "ro-crate-metadata.json"
        || id.starts_with("./")
        || id.starts_with("../")
        || id.starts_with('#')
        || id.starts_with("_:")
        || id.contains("://")
        || (id.contains(':') && !id.contains('/'))
    {
        id.to_string()
    } else {
        format!("./{id}")
    }
}

fn property_expects_identifier(property: &str) -> bool {
    matches!(property, "license" | "about" | "conformsTo")
}

fn is_reference_object(object: &serde_json::Map<String, Value>) -> bool {
    let has_identifier = object.contains_key("@id") || object.contains_key("id");
    has_identifier
        && object
            .keys()
            .all(|key| matches!(key.as_str(), "@id" | "id" | "@type" | "type"))
}

fn is_value_object(object: &serde_json::Map<String, Value>) -> bool {
    let has_value = object.contains_key("@value") || object.contains_key("value");
    has_value
        && object.keys().all(|key| {
            matches!(
                key.as_str(),
                "@value" | "value" | "@type" | "type" | "@language" | "language"
            )
        })
}

fn looks_like_identifier(value: &str) -> bool {
    value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.starts_with("_:")
        || value.contains("://")
        || (value.contains(':') && !value.contains(' '))
}

fn metadata_error_from_craqle(error: CraqleError) -> MetadataError {
    match error {
        CraqleError::RoCrate(rocrate_error) => match rocrate_error {
            RoCrateError::InvalidGraph(_)
            | RoCrateError::EntityNotFound(_)
            | RoCrateError::UnsupportedJsonLd(_)
            | RoCrateError::UnsupportedTerm(_)
            | RoCrateError::InvalidBatch(_) => {
                MetadataError::InvalidInput(rocrate_error.to_string())
            }
            other => MetadataError::Backend(other.to_string()),
        },
        CraqleError::SyncInputRejected(message) => MetadataError::InvalidInput(message),
        CraqleError::MultiGraphUpdateUnsupported => {
            MetadataError::InvalidInput("unsupported update across multiple graphs".to_string())
        }
        other => MetadataError::Backend(other.to_string()),
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn record_duration(span: &Span, field: &'static str, duration: Duration) {
    span.record(field, duration_ms(duration));
}

fn record_elapsed(span: &Span, field: &'static str, started: Instant) {
    record_duration(span, field, started.elapsed());
}

fn record_error(span: &Span, error: &str) {
    span.record("result", "error");
    span.record("error", field::display(error));
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", field::display(error));
}

fn warn_if_slow_metadata_backend(
    operation: &'static str,
    graph_iri: Option<&str>,
    duration: Duration,
) {
    if duration >= SLOW_METADATA_BACKEND_THRESHOLD {
        warn!(
            event = "metadata.backend.slow_call",
            operation,
            graph_iri = graph_iri.unwrap_or("<none>"),
            duration_ms = duration_ms(duration),
            threshold_ms = duration_ms(SLOW_METADATA_BACKEND_THRESHOLD),
            "Slow metadata backend call"
        );
    }
}

fn record_craqle_call_result<T>(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<T, CraqleError>,
) {
    let duration = started.elapsed();
    record_duration(span, "elapsed_ms", duration);
    match result {
        Ok(_) => {
            span.record("result", "ok");
            span.record("otel.status_code", "OK");
        }
        Err(error) => record_error(span, &error.to_string()),
    }
    warn_if_slow_metadata_backend(operation, graph_iri, duration);
}

fn record_metadata_result(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<MetadataEvent, CraqleError>,
) {
    record_craqle_call_result(span, operation, graph_iri, started, result);
}

fn metadata_effect_kind(effect: &MetadataEffect) -> &'static str {
    match effect {
        MetadataEffect::CreateCrate { .. } => "create_crate",
        MetadataEffect::ApplyRoCrate { .. } => "apply_rocrate",
        MetadataEffect::UpsertDataEntity { .. } => "upsert_data_entity",
        MetadataEffect::UpsertContextualEntity { .. } => "upsert_contextual_entity",
        MetadataEffect::SetGraphPolicy { .. } => "set_graph_policy",
        MetadataEffect::AddGraphPeer { .. } => "add_graph_peer",
        MetadataEffect::SyncGraphBestEffort { .. } => "sync_graph_best_effort",
        MetadataEffect::GetGraphPolicy { .. } => "get_graph_policy",
        MetadataEffect::ExportRoCrate { .. } => "export_rocrate",
        MetadataEffect::ExportRoCrateSummary { .. } => "export_rocrate_summary",
        MetadataEffect::ExportRoCratePage { .. } => "export_rocrate_page",
        MetadataEffect::SearchGraphs { .. } => "search_graphs",
        MetadataEffect::QueryGraphs { .. } => "query_graphs",
        MetadataEffect::DeleteGraph { .. } => "delete_graph",
        MetadataEffect::ListGraphs => "list_graphs",
        MetadataEffect::ContainsGraph { .. } => "contains_graph",
    }
}

fn metadata_event_kind(event: &MetadataEvent) -> &'static str {
    match event {
        MetadataEvent::CreateCrateResult { .. } => "create_crate_result",
        MetadataEvent::ApplyRoCrateResult { .. } => "apply_rocrate_result",
        MetadataEvent::EntityUpsertResult { .. } => "entity_upsert_result",
        MetadataEvent::GraphPolicySet { .. } => "graph_policy_set",
        MetadataEvent::GraphPeerAdded { .. } => "graph_peer_added",
        MetadataEvent::GraphSyncScheduled { .. } => "graph_sync_scheduled",
        MetadataEvent::GraphPolicyResult { .. } => "graph_policy_result",
        MetadataEvent::RoCrateExportResult { .. } => "rocrate_export_result",
        MetadataEvent::RoCrateSummaryResult { .. } => "rocrate_summary_result",
        MetadataEvent::RoCratePageResult { .. } => "rocrate_page_result",
        MetadataEvent::SearchResult { .. } => "search_result",
        MetadataEvent::QueryResult { .. } => "query_result",
        MetadataEvent::GraphDeleted { .. } => "graph_deleted",
        MetadataEvent::GraphListResult { .. } => "graph_list_result",
        MetadataEvent::ContainsGraphResult { .. } => "contains_graph_result",
        MetadataEvent::Error { .. } => "error",
    }
}

fn metadata_query_result_kind(results: &MetadataQueryResults) -> &'static str {
    match results {
        MetadataQueryResults::Solutions(_) => "solutions",
        MetadataQueryResults::Boolean(_) => "boolean",
        MetadataQueryResults::Graph(_) => "graph",
    }
}

fn record_metadata_query_result_counts(span: &Span, results: &MetadataQueryResults) {
    match results {
        MetadataQueryResults::Solutions(rows) => {
            span.record("row_count", rows.len() as u64);
        }
        MetadataQueryResults::Boolean(_) => {
            span.record("row_count", 1u64);
        }
        MetadataQueryResults::Graph(triples) => {
            span.record("triple_count", triples.len() as u64);
        }
    }
}

fn metadata_transport_message_kind(message: &MetadataTransportMessage) -> &'static str {
    match message {
        MetadataTransportMessage::QueryGraphs { .. } => "query_graphs",
        MetadataTransportMessage::QueryResults { .. } => "query_results",
        MetadataTransportMessage::SearchGraphs { .. } => "search_graphs",
        MetadataTransportMessage::SearchResults { .. } => "search_results",
        MetadataTransportMessage::Reject(_) => "reject",
    }
}

fn effect_graph_iri(effect: &MetadataEffect) -> Option<String> {
    match effect {
        MetadataEffect::CreateCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::ApplyRoCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => Some(request.graph_iri.clone()),
        MetadataEffect::SetGraphPolicy { graph_iri, .. }
        | MetadataEffect::AddGraphPeer { graph_iri, .. }
        | MetadataEffect::SyncGraphBestEffort { graph_iri, .. }
        | MetadataEffect::GetGraphPolicy { graph_iri }
        | MetadataEffect::ExportRoCrate { graph_iri }
        | MetadataEffect::ExportRoCrateSummary { graph_iri }
        | MetadataEffect::DeleteGraph { graph_iri }
        | MetadataEffect::ContainsGraph { graph_iri } => Some(graph_iri.clone()),
        MetadataEffect::ExportRoCratePage { graph_iri, .. } => Some(graph_iri.clone()),
        MetadataEffect::SearchGraphs { graph_iris, .. } => graph_iris
            .as_ref()
            .and_then(|graph_iris| graph_iris.first().cloned()),
        MetadataEffect::QueryGraphs { graph_iris, .. } => graph_iris
            .as_ref()
            .and_then(|graph_iris| graph_iris.first().cloned()),
        MetadataEffect::ListGraphs => None,
    }
}

fn graph_ids(graph_iris: &[String]) -> Vec<GraphId> {
    graph_iris
        .iter()
        .map(|graph_iri| GraphId::new(graph_iri))
        .collect()
}

fn craqle_create_request(request: MetadataCreateCrateRequest) -> CreateCrateRequest {
    CreateCrateRequest::new(
        GraphId::new(&request.graph_iri),
        request.name,
        request.description,
        request.date_published,
        request.license,
        craqle_graph_policy(request.policy),
    )
}

fn craqle_graph_policy(policy: MetadataGraphPolicy) -> GraphPolicy {
    GraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

fn irokle_peer_id(node_id: NodeId) -> irokle::PeerId {
    irokle::PeerId::from_bytes(*node_id.as_bytes())
}

fn metadata_graph_policy_from_craqle(policy: GraphPolicy) -> MetadataGraphPolicy {
    MetadataGraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

fn metadata_query_results_from_craqle(results: QueryResults) -> MetadataQueryResults {
    match results {
        QueryResults::Solutions(rows) => MetadataQueryResults::Solutions(
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|(key, value)| (key, value.0))
                        .collect::<BTreeMap<_, _>>()
                })
                .collect(),
        ),
        QueryResults::Boolean(value) => MetadataQueryResults::Boolean(value),
        QueryResults::Graph(triples) => MetadataQueryResults::Graph(
            triples
                .into_iter()
                .map(|(subject, predicate, object)| (subject.0, predicate.0, object.0))
                .collect(),
        ),
    }
}

fn metadata_dot_from_craqle(dot: craqle::Dot) -> MetadataDot {
    MetadataDot {
        actor: *dot.actor.as_bytes(),
        counter: dot.counter,
    }
}

fn metadata_batch_from_craqle(batch: Batch) -> MetadataBatch {
    MetadataBatch {
        graph_iri: batch.graph.as_str().to_string(),
        actor: *batch.actor.as_bytes(),
        counter: batch.counter,
        base_clock: batch.base_clock,
        ops: batch
            .ops
            .into_iter()
            .map(|op| match op {
                craqle::QuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => MetadataQuadOp::Add {
                    subject: subject.0,
                    predicate: predicate.0,
                    object: object.0,
                    dot: metadata_dot_from_craqle(dot),
                },
                craqle::QuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => MetadataQuadOp::Remove {
                    subject: subject.0,
                    predicate: predicate.0,
                    object: object.0,
                    witnessed,
                },
            })
            .collect(),
        timestamp_millis: batch.timestamp.timestamp_millis(),
    }
}

fn metadata_rocrate_page_from_craqle(page: craqle::RoCratePage) -> MetadataRoCratePage {
    MetadataRoCratePage {
        jsonld: page.jsonld,
        total_data_entities: page.total_data_entities,
        returned_data_entities: page.returned_data_entities,
        next_offset: page.next_offset,
        next_cursor: page.next_cursor,
    }
}

fn metadata_search_hit_from_craqle(
    hit: craqle::SearchHit,
    record: &MetadataRegistryRecord,
) -> MetadataSearchHit {
    MetadataSearchHit {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        graph_iri: hit.graph_id,
        subject_iri: hit.subject_iri,
        score: hit.score,
    }
}

#[tracing::instrument(
    name = "metadata.registry.list_local",
    level = "debug",
    skip(storage_handle),
    fields(
        page_count = field::Empty,
        record_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn list_local_registry_records(
    storage_handle: StorageHandle,
) -> Result<Vec<MetadataRegistryRecord>, MetadataError> {
    let started = Instant::now();
    let span = Span::current();
    let mut records = Vec::new();
    let mut start_after = None;
    let mut page_count = 0usize;
    loop {
        let event = storage_handle
            .send_effect(iter_all_registry_effect(start_after.clone(), None))
            .await;
        let (mut page, next_start_after) = parse_registry_iter(event).map_err(|error| {
            MetadataError::Backend(format!("metadata registry iteration failed: {error:?}"))
        })?;
        page_count += 1;
        records.append(&mut page);
        span.record("page_count", page_count as u64);
        span.record("record_count", records.len() as u64);
        if let Some(cursor) = next_start_after {
            start_after = Some(cursor);
        } else {
            record_elapsed(&span, "elapsed_ms", started);
            return Ok(records);
        }
    }
}

#[tracing::instrument(
    name = "metadata.query.local",
    level = "debug",
    skip(inner, auth_context, sparql),
    fields(
        query_len = sparql.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        registry_records = field::Empty,
        authorized_graphs = field::Empty,
        registry_ms = field::Empty,
        authorization_ms = field::Empty,
        craqle_query_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        row_count = field::Empty,
        triple_count = field::Empty,
    )
)]
async fn query_local_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    sparql: String,
) -> Result<MetadataQueryResults, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();

    let registry_started = Instant::now();
    let records = list_local_registry_records(inner.storage_handle.clone()).await?;
    record_elapsed(&span, "registry_ms", registry_started);
    span.record("registry_records", records.len() as u64);

    let authorization_started = Instant::now();
    let allowed = select_authorized_graphs(
        inner.storage_handle.clone(),
        auth_context,
        records,
        graph_iris,
    )
    .await?;
    record_elapsed(&span, "authorization_ms", authorization_started);
    span.record("authorized_graphs", allowed.len() as u64);

    let query_span = debug_span!(
        "metadata.backend.craqle.query_graphs",
        graph_count = allowed.len() as u64,
        query_len = sparql.len() as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        row_count = field::Empty,
        triple_count = field::Empty,
    );
    let blocking_span = query_span.clone();
    let query_started = Instant::now();
    let result = match tokio::task::spawn_blocking(move || {
        blocking_span.in_scope(|| {
            inner
                .node
                .query_graphs(&graph_ids(&allowed), &sparql)
                .map(metadata_query_results_from_craqle)
                .map_err(|error| MetadataError::Backend(error.to_string()))
        })
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err(MetadataError::TaskJoin(error.to_string())),
    };
    let query_elapsed = query_started.elapsed();
    record_duration(&query_span, "elapsed_ms", query_elapsed);
    record_duration(&span, "craqle_query_ms", query_elapsed);
    match &result {
        Ok(results) => {
            query_span.record("result", metadata_query_result_kind(results));
            span.record("result", metadata_query_result_kind(results));
            record_metadata_query_result_counts(&query_span, results);
            record_metadata_query_result_counts(&span, results);
        }
        Err(error) => {
            record_error(&query_span, &error.to_string());
            record_error(&span, &error.to_string());
        }
    }
    warn_if_slow_metadata_backend("query_graphs", None, query_elapsed);
    record_elapsed(&span, "elapsed_ms", total_started);
    result
}

#[tracing::instrument(
    name = "metadata.search.local",
    level = "debug",
    skip(inner, auth_context, query),
    fields(
        query_len = query.len() as u64,
        limit = limit as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        registry_records = field::Empty,
        authorized_graphs = field::Empty,
        registry_ms = field::Empty,
        authorization_ms = field::Empty,
        craqle_search_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    )
)]
async fn search_local_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();

    let registry_started = Instant::now();
    let records = list_local_registry_records(inner.storage_handle.clone()).await?;
    record_elapsed(&span, "registry_ms", registry_started);
    span.record("registry_records", records.len() as u64);

    let authorization_started = Instant::now();
    let allowed_records = select_authorized_records(
        inner.storage_handle.clone(),
        auth_context,
        records,
        graph_iris,
    )
    .await?;
    record_elapsed(&span, "authorization_ms", authorization_started);
    span.record("authorized_graphs", allowed_records.len() as u64);

    let search_span = debug_span!(
        "metadata.backend.craqle.search",
        graph_count = allowed_records.len() as u64,
        query_len = query.len() as u64,
        limit = limit as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    );
    let blocking_span = search_span.clone();
    let search_started = Instant::now();
    let result = match tokio::task::spawn_blocking(move || {
        blocking_span.in_scope(|| {
            let by_graph: HashMap<_, _> = allowed_records
                .into_iter()
                .map(|record| (record.graph_iri.clone(), record))
                .collect();
            inner
                .node
                .search(&AllowAllAuthorizer, &query, limit)
                .map(|hits| {
                    hits.into_iter()
                        .filter_map(|hit| by_graph.get(&hit.graph_id).map(|record| (hit, record)))
                        .map(|(hit, record)| metadata_search_hit_from_craqle(hit, record))
                        .collect::<Vec<_>>()
                })
                .map_err(|error| MetadataError::Backend(error.to_string()))
        })
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err(MetadataError::TaskJoin(error.to_string())),
    };
    let search_elapsed = search_started.elapsed();
    record_duration(&search_span, "elapsed_ms", search_elapsed);
    record_duration(&span, "craqle_search_ms", search_elapsed);
    match &result {
        Ok(hits) => {
            search_span.record("result", "ok");
            span.record("result", "ok");
            search_span.record("hit_count", hits.len() as u64);
            span.record("hit_count", hits.len() as u64);
        }
        Err(error) => {
            record_error(&search_span, &error.to_string());
            record_error(&span, &error.to_string());
        }
    }
    warn_if_slow_metadata_backend("search", None, search_elapsed);
    record_elapsed(&span, "elapsed_ms", total_started);
    result
}

async fn select_authorized_graphs(
    storage_handle: StorageHandle,
    auth_context: Option<AuthContext>,
    records: Vec<MetadataRegistryRecord>,
    graph_filter: Option<Vec<String>>,
) -> Result<Vec<String>, MetadataError> {
    Ok(
        select_authorized_records(storage_handle, auth_context, records, graph_filter)
            .await?
            .into_iter()
            .map(|record| record.graph_iri)
            .collect(),
    )
}

#[tracing::instrument(
    name = "metadata.authorization.select_records",
    level = "debug",
    skip(storage_handle, auth_context, records, graph_filter),
    fields(
        record_count = records.len() as u64,
        graph_filter_count = graph_filter.as_ref().map_or(0, Vec::len) as u64,
        visible_count = field::Empty,
        deleted_count = field::Empty,
        filtered_count = field::Empty,
        public_count = field::Empty,
        private_checked_count = field::Empty,
        denied_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn select_authorized_records(
    storage_handle: StorageHandle,
    auth_context: Option<AuthContext>,
    records: Vec<MetadataRegistryRecord>,
    graph_filter: Option<Vec<String>>,
) -> Result<Vec<MetadataRegistryRecord>, MetadataError> {
    let span = Span::current();
    let started = Instant::now();
    let allowed_graphs = graph_filter.map(|graphs| graphs.into_iter().collect::<HashSet<_>>());
    let mut visible = Vec::new();
    let mut deleted_count = 0usize;
    let mut filtered_count = 0usize;
    let mut public_count = 0usize;
    let mut private_checked_count = 0usize;
    let mut denied_count = 0usize;
    for record in records {
        if metadata_graph_deleted(storage_handle.clone(), &record.graph_iri).await? {
            deleted_count += 1;
            continue;
        }
        if let Some(filter) = allowed_graphs.as_ref()
            && !filter.contains(&record.graph_iri)
        {
            filtered_count += 1;
            continue;
        }
        if record.public {
            public_count += 1;
        } else {
            private_checked_count += 1;
        }
        if can_read_record_locally(storage_handle.clone(), auth_context.clone(), &record).await? {
            visible.push(record);
        } else {
            denied_count += 1;
        }
    }
    span.record("visible_count", visible.len() as u64);
    span.record("deleted_count", deleted_count as u64);
    span.record("filtered_count", filtered_count as u64);
    span.record("public_count", public_count as u64);
    span.record("private_checked_count", private_checked_count as u64);
    span.record("denied_count", denied_count as u64);
    record_elapsed(&span, "elapsed_ms", started);
    Ok(visible)
}

async fn can_read_record_locally(
    storage_handle: StorageHandle,
    auth_context: Option<AuthContext>,
    record: &MetadataRegistryRecord,
) -> Result<bool, MetadataError> {
    if record.public {
        return Ok(true);
    }
    let Some(auth_context) = auth_context else {
        return Ok(false);
    };
    if auth_context.realm_id != record.realm_id {
        return Ok(false);
    }

    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
    };
    drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context,
            path: record.permission_path.clone(),
            required_permission: Permission::READ,
        }),
        &context,
    )
    .await
    .map_err(|error| MetadataError::Backend(error.to_string()))
}

#[tracing::instrument(
    name = "metadata.remote.request",
    level = "debug",
    skip(net_handle, message),
    fields(
        peer = ?node_id,
        request = metadata_transport_message_kind(&message),
        response = field::Empty,
        open_stream_ms = field::Empty,
        write_ms = field::Empty,
        finish_ms = field::Empty,
        read_ms = field::Empty,
        close_ms = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn send_request(
    net_handle: &NetHandle,
    node_id: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataTransportMessage, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();

    let open_started = Instant::now();
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Metadata)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed(&span, "open_stream_ms", open_started);

    let write_started = Instant::now();
    write_transport_message(&mut stream, &message).await?;
    record_elapsed(&span, "write_ms", write_started);

    let finish_started = Instant::now();
    stream
        .0
        .finish()
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed(&span, "finish_ms", finish_started);

    let read_started = Instant::now();
    let response = read_transport_message(&mut stream).await?;
    record_elapsed(&span, "read_ms", read_started);

    let close_started = Instant::now();
    close_stream(&mut stream).await;
    record_elapsed(&span, "close_ms", close_started);
    record_elapsed(&span, "elapsed_ms", total_started);
    span.record("response", metadata_transport_message_kind(&response));
    Ok(response)
}

async fn write_transport_message(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
) -> Result<(), MetadataError> {
    let result: Result<Result<(), String>, tokio::time::error::Elapsed> =
        timeout(METADATA_IO_TIMEOUT, write_message(stream, message)).await;
    result
        .map_err(|_| MetadataError::Backend("timed out writing metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

async fn read_transport_message(
    stream: &mut BiStream,
) -> Result<MetadataTransportMessage, MetadataError> {
    let result: Result<Result<MetadataTransportMessage, String>, tokio::time::error::Elapsed> =
        timeout(METADATA_IO_TIMEOUT, read_message(stream)).await;
    result
        .map_err(|_| MetadataError::Backend("timed out waiting for metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

async fn drain_request_stream(stream: &mut BiStream) -> Result<(), MetadataError> {
    timeout(METADATA_IO_TIMEOUT, stream.1.read_to_end(1))
        .await
        .map_err(|_| {
            MetadataError::Backend("timed out draining metadata request stream".to_string())
        })?
        .map(|_| ())
        .map_err(|error| MetadataError::Backend(error.to_string()))
}
