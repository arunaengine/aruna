use super::*;

impl MetadataHandle {
    pub async fn send_metadata_effect(&self, effect: MetadataEffect) -> Event {
        let started = Instant::now();
        let event = self.send_metadata_effect_inner(effect).await;
        aruna_core::telemetry::record_stage("craqle", started.elapsed());
        event
    }

    async fn send_metadata_effect_inner(&self, effect: MetadataEffect) -> Event {
        let effect_name = metadata_effect_kind(&effect);
        let graph_iri = effect_graph_iri(&effect);
        let _graph_fence = match graph_iri.as_deref() {
            Some(graph_iri) if metadata_effect_mutates_graph(&effect) => {
                match metadata_graph_fence(graph_iri).acquire().await {
                    Ok(permit) => Some(permit),
                    Err(error) => {
                        return Event::Metadata(MetadataEvent::Error {
                            graph_iri: Some(graph_iri.to_string()),
                            error: MetadataError::Backend(format!(
                                "metadata graph fence unavailable: {error}"
                            )),
                        });
                    }
                }
            }
            _ => None,
        };
        if let MetadataEffect::DeleteGraph { graph_iri } = &effect {
            self.inner
                .visibility_cache
                .remove_registry_records_by_graph(graph_iri);
            self.inner
                .visibility_cache
                .remove_lifecycle_entry(graph_iri);
        }
        if let Some(graph_iri) = graph_iri.as_deref()
            && !metadata_effect_skips_lifecycle_read(&effect)
        {
            let span = debug_span!(
                "metadata.graph_lifecycle.read_before_effect",
                effect = effect_name,
                graph_iri,
                deleted = field::Empty,
                elapsed_ms = field::Empty,
            );
            let started = Instant::now();
            let result =
                metadata_graph_deleted(self.inner.clone(), self.lifecycle_storage(), graph_iri)
                    .instrument(span.clone())
                    .await;
            match result {
                Ok(true) => {
                    span.record("deleted", true);
                    record_elapsed_ms(&span, "elapsed_ms", started);
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
                Ok(false) => {
                    span.record("deleted", false);
                    record_elapsed_ms(&span, "elapsed_ms", started);
                }
                Err(error) => {
                    record_error(&span, &error.to_string());
                    record_elapsed_ms(&span, "elapsed_ms", started);
                    return Event::Metadata(MetadataEvent::Error {
                        graph_iri: Some(graph_iri.to_string()),
                        error,
                    });
                }
            }
        }
        match effect {
            MetadataEffect::SyncGraphBestEffort { graph_iri, peers } => {
                Event::Metadata(self.sync_graph_best_effort(graph_iri, peers).await)
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
                    .search_authorized_local(auth_context, graph_iris, query, limit, None)
                    .await
                {
                    Ok(hits) => MetadataEvent::SearchResult { hits },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                },
            ),
            MetadataEffect::ListGraphs => {
                Event::Metadata(match list_visible_graphs(self.inner.clone()).await {
                    Ok(graph_iris) => MetadataEvent::GraphListResult { graph_iris },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                })
            }
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
                // Heavy mutations and cheap reads queue on separate pools so
                // trivial reads never wait behind long materializations.
                let mutates_graph = metadata_effect_mutates_graph(&other);
                let permits = if mutates_graph {
                    self.inner.craqle_permits.clone()
                } else {
                    self.inner.craqle_read_permits.clone()
                };
                let _permit = permits.acquire_owned().await.ok();
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
                record_elapsed_ms(&span, "elapsed_ms", started);
                span.record("result", metadata_event_kind(&metadata_event));
                // Successful mutations invalidate query results through this
                // counter; lifecycle cache changes use their own generation.
                if mutates_graph && !matches!(metadata_event, MetadataEvent::Error { .. }) {
                    self.inner.query_cache.bump_apply();
                }
                Event::Metadata(metadata_event)
            }
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
pub(super) async fn sync_graph_once(
    inner: Arc<MetadataInner>,
    graph_iri: String,
    peers: Vec<NodeId>,
) -> Result<(), MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();
    if peers.is_empty() {
        return Ok(());
    }
    if graph_lifecycle_deleted(inner.storage_handle.clone(), &graph_iri).await? {
        return Ok(());
    }
    let net_handle = inner
        .net_handle
        .clone()
        .ok_or(MetadataError::HandleMissing)?;

    // Deterministic topic id, bound locally only when its genesis is already
    // present. Deriving it never mints a genesis, so concurrent holders cannot
    // fork rival ones for the same graph.
    let setup_started = Instant::now();
    let topic_id = bind_or_derive_graph_topic(&inner, &graph_iri).await?;
    record_elapsed_ms(&span, "local_peer_setup_ms", setup_started);

    let sync_started = Instant::now();
    // Join-before-create: adopt an existing co-holder genesis first (raw sync
    // bootstraps an unknown topic), and only then consider minting one.
    if !document_sync_topic_exists(&net_handle, topic_id)? {
        if let Err(error) = net_handle
            .sync_document_topic_with_peers(topic_id, peers.clone())
            .await
        {
            debug!(%topic_id, error = %error, "graph topic join attempt failed");
        }
        bind_graph_topic(&inner, &graph_iri).await?;
    }
    if !document_sync_topic_exists(&net_handle, topic_id)? {
        ensure_graph_topic_genesis(&inner, &net_handle, &graph_iri, topic_id, &peers).await?;
    }
    add_graph_topic_peers(&inner, &net_handle, &graph_iri, topic_id, &peers).await?;

    net_handle
        .sync_document_topic_with_peers(topic_id, peers)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed_ms(&span, "network_sync_ms", sync_started);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok(())
}

async fn add_graph_topic_peers(
    inner: &Arc<MetadataInner>,
    net_handle: &NetHandle,
    graph_iri: &str,
    topic_id: irokle::TopicId,
    peers: &[NodeId],
) -> Result<(), MetadataError> {
    let sync_node = net_handle.document_sync_node();
    let Some(state) = irokle::Storage::topic_state(sync_node.storage(), &topic_id)
        .map_err(|error| MetadataError::Backend(error.to_string()))?
    else {
        return Ok(());
    };
    let node = inner.node.clone();
    let graph_iri = graph_iri.to_string();
    let peers = peers
        .iter()
        .copied()
        .filter(|peer| !state.members.contains(&document_sync_peer_id(*peer)))
        .collect::<Vec<_>>();
    if peers.is_empty() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || {
        let graph = GraphId::new(&graph_iri);
        for peer in peers {
            node.add_irokle_peer(&graph, document_sync_peer_id(peer))?;
        }
        Ok::<_, CraqleError>(())
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
    .map_err(metadata_error_from_craqle)
}

/// Creates the graph topic genesis under the single-minter discipline: only
/// rank-0 mints, after confirming no co-holder holds a genesis, so a rank-0 move
/// cannot fork a rival. Rank-0 ties only; content materializes on every holder.
async fn ensure_graph_topic_genesis(
    inner: &Arc<MetadataInner>,
    net_handle: &NetHandle,
    graph_iri: &str,
    topic_id: irokle::TopicId,
    peers: &[NodeId],
) -> Result<(), MetadataError> {
    let local = net_handle.node_id();
    let local_is_rank0 = peers.iter().all(|peer| local.as_bytes() < peer.as_bytes());
    if !local_is_rank0 {
        return Ok(());
    }
    let probe = net_handle
        .probe_shard_topic_geneses(vec![topic_id], peers.to_vec())
        .await;
    if probe.known_by_co_holder.contains(&topic_id) {
        if let Err(error) = net_handle
            .sync_document_topic_with_peers(topic_id, peers.to_vec())
            .await
        {
            debug!(%topic_id, error = %error, "graph topic adopt attempt failed");
        }
        bind_graph_topic(inner, graph_iri).await?;
        return Ok(());
    }
    if !probe.unreachable.is_empty() || probe.unconfirmed.contains(&topic_id) {
        return Err(MetadataError::Backend(format!(
            "withholding graph topic {topic_id} genesis: co-holder unreachable or unconfirmed"
        )));
    }
    let mut members: BTreeSet<irokle::PeerId> =
        peers.iter().copied().map(document_sync_peer_id).collect();
    members.insert(document_sync_peer_id(local));
    mint_graph_topic(inner, graph_iri, members).await?;
    Ok(())
}

fn document_sync_topic_exists(
    net_handle: &NetHandle,
    topic_id: irokle::TopicId,
) -> Result<bool, MetadataError> {
    net_handle
        .document_sync_topic_exists(topic_id)
        .map_err(|error| MetadataError::Backend(error.to_string()))
}

async fn bind_or_derive_graph_topic(
    inner: &Arc<MetadataInner>,
    graph_iri: &str,
) -> Result<irokle::TopicId, MetadataError> {
    let node = inner.node.clone();
    let graph_iri = graph_iri.to_string();
    tokio::task::spawn_blocking(move || node.bind_or_derive_irokle_topic(&GraphId::new(&graph_iri)))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(metadata_error_from_craqle)
}

async fn bind_graph_topic(
    inner: &Arc<MetadataInner>,
    graph_iri: &str,
) -> Result<(), MetadataError> {
    let node = inner.node.clone();
    let graph_iri = graph_iri.to_string();
    tokio::task::spawn_blocking(move || node.bind_irokle_topic(&GraphId::new(&graph_iri)))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(metadata_error_from_craqle)?;
    Ok(())
}

async fn mint_graph_topic(
    inner: &Arc<MetadataInner>,
    graph_iri: &str,
    members: BTreeSet<irokle::PeerId>,
) -> Result<irokle::TopicId, MetadataError> {
    let node = inner.node.clone();
    let graph_iri = graph_iri.to_string();
    tokio::task::spawn_blocking(move || node.mint_irokle_topic(&GraphId::new(&graph_iri), members))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(metadata_error_from_craqle)
}
