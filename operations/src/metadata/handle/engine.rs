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

pub(super) fn flush_document_sync_journal(
    inner: &MetadataInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) -> Result<(), MetadataError> {
    let Some(db) = &inner.document_sync_db else {
        return Ok(());
    };
    let span = debug_span!(
        "metadata.backend.document_sync.flush",
        effect = effect_name,
        graph_iri = graph_iri.unwrap_or("<none>"),
        mode = inner.document_sync_persist_policy.label(),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(|| db.persist(inner.document_sync_persist_policy.as_fjall()));
    record_elapsed_ms(&span, "elapsed_ms", started);
    match result {
        Ok(()) => {
            span.record("result", "ok");
            Ok(())
        }
        Err(error) => {
            record_error(&span, &error.to_string());
            Err(MetadataError::Persist(format!(
                "failed to flush document sync journal: {error}"
            )))
        }
    }
}

pub(super) fn flush_metadata_persistence(
    inner: &MetadataInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) -> Result<(), MetadataError> {
    inner
        .node
        .persist_fjall()
        .map_err(|error| MetadataError::Persist(error.to_string()))?;
    flush_document_sync_journal(inner, effect_name, graph_iri)
}

pub(super) fn metadata_effect_persists_document_sync(effect: &MetadataEffect) -> bool {
    match effect {
        MetadataEffect::ValidateCreateCrate { .. } | MetadataEffect::ValidateRoCrate { .. } => {
            false
        }
        MetadataEffect::CreateCrate { request } => {
            metadata_request_persists_document_sync(request.durability)
        }
        MetadataEffect::ApplyRoCrate { request } => {
            metadata_request_persists_document_sync(request.durability)
        }
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => {
            metadata_request_persists_document_sync(request.durability)
        }
        MetadataEffect::SetGraphPolicy { .. }
        | MetadataEffect::AddGraphPeer { .. }
        | MetadataEffect::DeleteGraph { .. } => true,
        MetadataEffect::SyncGraphBestEffort { .. }
        | MetadataEffect::QueryGraphs { .. }
        | MetadataEffect::SearchGraphs { .. }
        | MetadataEffect::GetGraphPolicy { .. }
        | MetadataEffect::ExportRoCrate { .. }
        | MetadataEffect::ExportRoCrateSummary { .. }
        | MetadataEffect::ExportRoCratePage { .. }
        | MetadataEffect::ListGraphs
        | MetadataEffect::ContainsGraph { .. }
        | MetadataEffect::GraphSnapshot { .. }
        // A device runs no document sync, so an installed snapshot has no
        // journal entry to flush.
        | MetadataEffect::InstallSnapshot { .. }
        // A merged batch publishes nothing back to irokle.
        | MetadataEffect::PlanBatch { .. }
        | MetadataEffect::MergeBatch { .. } => false,
    }
}

pub(super) fn metadata_effect_defers_persist(effect: &MetadataEffect) -> bool {
    match effect {
        MetadataEffect::CreateCrate { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        MetadataEffect::ApplyRoCrate { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        _ => false,
    }
}

fn metadata_effect_skips_lifecycle_read(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::ValidateCreateCrate { .. } | MetadataEffect::ValidateRoCrate { .. }
    )
}

pub(super) fn schedule_deferred_metadata_persist(
    inner: Arc<MetadataInner>,
    effect_name: &'static str,
    graph_iri: Option<String>,
) {
    inner
        .deferred_persist_requested
        .store(true, Ordering::Release);
    if inner.deferred_persist_running.swap(true, Ordering::AcqRel) {
        return;
    }

    let worker_inner = inner.clone();
    let worker_graph_iri = graph_iri.clone();
    let spawn_result = thread::Builder::new()
        .name("metadata-deferred-persist".to_string())
        .spawn(move || {
            loop {
                while worker_inner
                    .deferred_persist_requested
                    .swap(false, Ordering::AcqRel)
                {
                    run_deferred_metadata_flush(
                        &worker_inner,
                        effect_name,
                        worker_graph_iri.as_deref(),
                    );
                }

                worker_inner
                    .deferred_persist_running
                    .store(false, Ordering::Release);
                if !worker_inner
                    .deferred_persist_requested
                    .load(Ordering::Acquire)
                {
                    break;
                }
                if worker_inner
                    .deferred_persist_running
                    .swap(true, Ordering::AcqRel)
                {
                    break;
                }
            }
        });

    if let Err(error) = spawn_result {
        inner
            .deferred_persist_running
            .store(false, Ordering::Release);
        warn!(
            event = "metadata.backend.deferred_persist.spawn_failed",
            effect = effect_name,
            error = %error,
            "Failed to spawn deferred metadata persist"
        );
    }
}

fn run_deferred_metadata_flush(
    inner: &MetadataInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) {
    let span = debug_span!(
        "metadata.backend.deferred_persist",
        effect = effect_name,
        graph_iri = graph_iri.unwrap_or("<none>"),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(|| flush_metadata_persistence(inner, effect_name, graph_iri));
    record_elapsed_ms(&span, "elapsed_ms", started);
    match result {
        Ok(()) => {
            span.record("result", "ok");
        }
        Err(error) => {
            record_error(&span, &error.to_string());
            warn!(
                event = "metadata.backend.deferred_persist.failed",
                effect = effect_name,
                graph_iri = graph_iri.unwrap_or("<none>"),
                error = %error,
                "Deferred metadata persist failed"
            );
        }
    }
}

fn metadata_request_persists_document_sync(durability: MetadataRequestDurability) -> bool {
    matches!(durability, MetadataRequestDurability::Durable)
}

pub(super) fn upsert_data_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: MetadataUpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    let actor = request.deterministic_actor.map(ActorId::from_bytes);
    let entity_request = craqle_patch_request(&graph, &request.jsonld)?;
    node.patch_data_with(
        auth,
        entity_request,
        craqle_request_durability(request.durability),
        actor,
    )
    .map(metadata_batch_from_craqle)
}

pub(super) fn upsert_contextual_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: MetadataUpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    let actor = request.deterministic_actor.map(ActorId::from_bytes);
    let entity_request = craqle_patch_request(&graph, &request.jsonld)?;
    node.patch_contextual_with(
        auth,
        entity_request,
        craqle_request_durability(request.durability),
        actor,
    )
    .map(metadata_batch_from_craqle)
}

pub(super) fn craqle_patch_request(
    graph: &GraphId,
    jsonld: &str,
) -> Result<PatchEntityRequest, CraqleError> {
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
    let mut replaced_predicates = Vec::new();
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
        replaced_predicates.push(predicate.clone());
        for object in property_value_terms(&property, property_value)? {
            additional_triples.push((predicate.clone(), object));
        }
    }

    Ok(PatchEntityRequest {
        entity: CreateEntityRequest {
            graph: graph.clone(),
            entity_id,
            entity_type,
            name,
            additional_triples,
        },
        replaced_predicates,
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
        "conformsTo" => Ok(NamedNode::new_unchecked(
            super::super::iri_index::DCTERMS_CONFORMS_TO_IRI,
        )),
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

pub(super) fn metadata_error_from_craqle(error: CraqleError) -> MetadataError {
    match error {
        CraqleError::RoCrate(rocrate_error) => match rocrate_error {
            RoCrateError::Update(craqle::UpdateError::ValidationFailed(violations)) => {
                metadata_violations(violations)
            }
            RoCrateError::Json(_) | RoCrateError::JsonLd(_) => {
                MetadataError::InvalidInput(rocrate_error.to_string())
            }
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
        CraqleError::Update(craqle::UpdateError::ValidationFailed(violations)) => {
            metadata_violations(violations)
        }
        // Backend infrastructure, not the document: an apply that fails on disk
        // or in the search worker must keep retrying instead of being parked.
        error @ (CraqleError::Io(_) | CraqleError::Store(_) | CraqleError::SearchWorker(_)) => {
            MetadataError::Persist(error.to_string())
        }
        other => MetadataError::Backend(other.to_string()),
    }
}

fn metadata_violations(violations: Vec<craqle::CrateViolation>) -> MetadataError {
    MetadataError::Validation(
        violations
            .into_iter()
            .map(|violation| MetadataValidationViolation {
                code: violation.code.to_string(),
                message: violation.message,
                pointer: violation.pointer,
                entity_id: violation.entity_id,
            })
            .collect(),
    )
}

pub(super) fn record_error(span: &Span, error: &str) {
    span.record("result", "error");
    span.record("error", field::display(error));
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", field::display(error));
}

pub(super) fn warn_if_slow_metadata_backend(
    operation: &'static str,
    graph_iri: Option<&str>,
    duration: Duration,
) {
    CRAQLE_LATENCY.record(operation, duration);
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

pub(super) fn record_craqle_call_result<T>(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<T, CraqleError>,
) {
    let duration = started.elapsed();
    record_duration_ms(span, "elapsed_ms", duration);
    match result {
        Ok(_) => {
            span.record("result", "ok");
            span.record("otel.status_code", "OK");
        }
        Err(error) => record_error(span, &error.to_string()),
    }
    warn_if_slow_metadata_backend(operation, graph_iri, duration);
}

pub(super) fn record_metadata_result(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<MetadataEvent, CraqleError>,
) {
    record_craqle_call_result(span, operation, graph_iri, started, result);
}

pub(super) fn metadata_effect_kind(effect: &MetadataEffect) -> &'static str {
    match effect {
        MetadataEffect::ValidateCreateCrate { .. } => "validate_create_crate",
        MetadataEffect::ValidateRoCrate { .. } => "validate_rocrate",
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
        MetadataEffect::PlanBatch { .. } => "plan_batch",
        MetadataEffect::MergeBatch { .. } => "merge_batch",
        MetadataEffect::GraphSnapshot { .. } => "graph_snapshot",
        MetadataEffect::InstallSnapshot { .. } => "install_snapshot",
    }
}

pub(super) fn metadata_event_kind(event: &MetadataEvent) -> &'static str {
    match event {
        MetadataEvent::ValidationResult { .. } => "validation_result",
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
        MetadataEvent::BatchPlanned { .. } => "batch_planned",
        MetadataEvent::BatchMerged { .. } => "batch_merged",
        MetadataEvent::GraphSnapshotResult { .. } => "graph_snapshot_result",
        MetadataEvent::SnapshotInstalled { .. } => "snapshot_installed",
        MetadataEvent::Error { .. } => "error",
    }
}

pub(super) fn record_metadata_query_result_counts(span: &Span, results: &MetadataQueryResults) {
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

pub(crate) fn metadata_read_error(error: MetadataError) -> MetadataReadError {
    match error {
        MetadataError::GraphNotFound => MetadataReadError::NotFound,
        MetadataError::InvalidInput(_)
        | MetadataError::ChannelClosed
        | MetadataError::InvalidEffect
        | MetadataError::HandleMissing
        | MetadataError::TaskJoin(_)
        | MetadataError::Validation(_)
        | MetadataError::ProfileValidation(_)
        | MetadataError::Persist(_)
        | MetadataError::Storage(_)
        | MetadataError::Backend(_) => MetadataReadError::Unavailable,
    }
}

pub(super) async fn config_digest_matches(
    context: &DriverContext,
    realm_id: RealmId,
    expected: &[u8; 32],
) -> bool {
    super::super::api::load_realm_config(context, realm_id)
        .await
        .and_then(|config| config.digest().ok())
        .as_ref()
        == Some(expected)
}

pub(crate) fn transport_message_kind(message: &MetadataTransportMessage) -> &'static str {
    match message {
        MetadataTransportMessage::QueryGraphs { .. } => "query_graphs",
        MetadataTransportMessage::QueryResults { .. } => "query_results",
        MetadataTransportMessage::SearchGraphs { .. } => "search_graphs",
        MetadataTransportMessage::FilteredSearchGraphs { .. } => "filtered_search_graphs",
        MetadataTransportMessage::SearchResults { .. } => "search_results",
        MetadataTransportMessage::SearchBuckets { .. } => "search_buckets",
        MetadataTransportMessage::BucketSearchResults { .. } => "bucket_search_results",
        MetadataTransportMessage::SearchObjects { .. } => "search_objects",
        MetadataTransportMessage::ObjectSearchResults { .. } => "object_search_results",
        MetadataTransportMessage::CreateSyncMirror { .. } => "create_sync_mirror",
        MetadataTransportMessage::DeleteSyncMirror { .. } => "delete_sync_mirror",
        MetadataTransportMessage::SyncMirrorCreated => "sync_mirror_created",
        MetadataTransportMessage::SyncMirrorDeleted => "sync_mirror_deleted",
        MetadataTransportMessage::ForwardCreateDocument { .. } => "forward_create_document",
        MetadataTransportMessage::ForwardUpdateDocument { .. } => "forward_update_document",
        MetadataTransportMessage::ForwardDeleteDocument { .. } => "forward_delete_document",
        MetadataTransportMessage::ForwardReadDocument { .. } => "forward_read_document",
        MetadataTransportMessage::ForwardedRecord { .. } => "forwarded_record",
        MetadataTransportMessage::ForwardedRead { .. } => "forwarded_read",
        MetadataTransportMessage::ForwardPathLookup { .. } => "forward_path_lookup",
        MetadataTransportMessage::ForwardedPathLookup { .. } => "forwarded_path_lookup",
        MetadataTransportMessage::ForwardPathResolution { .. } => "forward_path_resolution",
        MetadataTransportMessage::ForwardedPathResolution { .. } => "forwarded_path_resolution",
        MetadataTransportMessage::ForwardedWriteDenied { .. } => "forwarded_write_denied",
        MetadataTransportMessage::ForwardedWriteNotFound => "forwarded_write_not_found",
        MetadataTransportMessage::ForwardedWriteUnavailable => "forwarded_write_unavailable",
        MetadataTransportMessage::ForwardedDelete => "forwarded_delete",
        MetadataTransportMessage::ForwardExportDocument { .. } => "forward_export_document",
        MetadataTransportMessage::ForwardExportProfile { .. } => "forward_export_profile",
        MetadataTransportMessage::ForwardedExport { .. } => "forwarded_export",
        MetadataTransportMessage::QueryDocument { .. } => "query_document",
        MetadataTransportMessage::DocumentQueryResults { .. } => "document_query_results",
        MetadataTransportMessage::Reject(_) => "reject",
        MetadataTransportMessage::ForwardedUpdateInvalidInput { .. } => {
            "forwarded_update_invalid_input"
        }
        MetadataTransportMessage::ForwardAuditPage { .. } => "forward_audit_page",
        MetadataTransportMessage::ForwardedAuditPage { .. } => "forwarded_audit_page",
        MetadataTransportMessage::ForwardTokenRevocation { .. } => "forward_token_revocation",
        MetadataTransportMessage::ForwardedTokenRevoked => "forwarded_token_revoked",
        MetadataTransportMessage::ForwardedTokenRevocationCapacity => {
            "forwarded_token_revocation_capacity"
        }
        MetadataTransportMessage::ForwardedMetadataHistoryCapacity => {
            "forwarded_metadata_history_capacity"
        }
        MetadataTransportMessage::ForwardPersistentId { .. } => "forward_persistent_id",
        MetadataTransportMessage::ForwardedPersistentId { .. } => "forwarded_persistent_id",
        MetadataTransportMessage::ForwardPlacementPolicy { .. } => "forward_placement_policy",
        MetadataTransportMessage::ForwardedPlacementPolicy { .. } => "forwarded_placement_policy",
        MetadataTransportMessage::ForwardCreatePlacementPolicy { .. } => {
            "forward_create_placement_policy"
        }
        MetadataTransportMessage::ForwardedPlacementPolicyCreated { .. } => {
            "forwarded_placement_policy_created"
        }
        MetadataTransportMessage::ForwardJobRecord { .. } => "forward_job_record",
        MetadataTransportMessage::ForwardedJobRecord { .. } => "forwarded_job_record",
        MetadataTransportMessage::ForwardJobRecordPage { .. } => "forward_job_record_page",
        MetadataTransportMessage::ForwardedJobRecordPage { .. } => "forwarded_job_record_page",
        MetadataTransportMessage::ForwardLaunchOffer { .. } => "forward_launch_offer",
        MetadataTransportMessage::ForwardedLaunchOffer { .. } => "forwarded_launch_offer",
        MetadataTransportMessage::ForwardJobSubmission { .. } => "forward_job_submission",
        MetadataTransportMessage::ForwardedJobSubmission { .. } => "forwarded_job_submission",
        MetadataTransportMessage::ForwardedProfileValidation { .. } => {
            "forwarded_profile_validation"
        }
        MetadataTransportMessage::ForwardProfileValidationStatus { .. } => {
            "forward_profile_validation_status"
        }
        MetadataTransportMessage::ForwardedProfileValidationStatus { .. } => {
            "forwarded_profile_validation_status"
        }
        MetadataTransportMessage::ReferencePreflight { .. } => "reference_preflight",
        MetadataTransportMessage::ReferencePreflightResults { .. } => "reference_preflight_results",
        MetadataTransportMessage::ForwardAdminEvent { .. } => "forward_admin_event",
        MetadataTransportMessage::ForwardedAdminEventQueued => "forwarded_admin_event_queued",
        MetadataTransportMessage::ForwardGroupCreate { .. } => "forward_group_create",
        MetadataTransportMessage::ForwardedGroupCreated { .. } => "forwarded_group_created",
        MetadataTransportMessage::ForwardSyncPull { .. } => "forward_sync_pull",
        MetadataTransportMessage::ForwardedSyncPull { .. } => "forwarded_sync_pull",
        MetadataTransportMessage::ForwardListVersions { .. } => "forward_list_versions",
        MetadataTransportMessage::ForwardedVersions { .. } => "forwarded_versions",
        MetadataTransportMessage::ForwardCreateBucket { .. } => "forward_create_bucket",
        MetadataTransportMessage::ForwardedBucketCreated { .. } => "forwarded_bucket_created",
        MetadataTransportMessage::FetchRealmDocuments { .. } => "fetch_realm_documents",
        MetadataTransportMessage::FetchedRealmDocuments { .. } => "fetched_realm_documents",
        MetadataTransportMessage::FetchGraphState { .. } => "fetch_graph_state",
        MetadataTransportMessage::FetchedGraphState { .. } => "fetched_graph_state",
        MetadataTransportMessage::ForwardApplyBatch { .. } => "forward_apply_batch",
        MetadataTransportMessage::ForwardedApplyBatch { .. } => "forwarded_apply_batch",
        MetadataTransportMessage::ForwardedGroupCreateConflict { .. } => {
            "forwarded_group_create_conflict"
        }
    }
}

pub(super) fn effect_graph_iri(effect: &MetadataEffect) -> Option<String> {
    match effect {
        MetadataEffect::ValidateCreateCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::ValidateRoCrate { request } => Some(request.graph_iri.clone()),
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
        | MetadataEffect::ContainsGraph { graph_iri }
        | MetadataEffect::GraphSnapshot { graph_iri }
        | MetadataEffect::InstallSnapshot { graph_iri, .. }
        | MetadataEffect::PlanBatch { graph_iri, .. }
        | MetadataEffect::MergeBatch { graph_iri, .. } => Some(graph_iri.clone()),
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

pub(super) fn graph_ids(graph_iris: &[String]) -> Vec<GraphId> {
    graph_iris
        .iter()
        .map(|graph_iri| GraphId::new(graph_iri))
        .collect()
}

pub(super) fn craqle_create_request(request: MetadataCreateCrateRequest) -> CreateCrateRequest {
    CreateCrateRequest::new(
        GraphId::new(&request.graph_iri),
        request.name,
        request.description,
        request.date_published,
        request.license,
        craqle_graph_policy(request.policy),
    )
}

pub(super) fn craqle_request_durability(
    durability: MetadataRequestDurability,
) -> CraqleRequestDurability {
    match durability {
        MetadataRequestDurability::Durable => CraqleRequestDurability::Durable,
        MetadataRequestDurability::WalAlreadyDurable => CraqleRequestDurability::WalAlreadyDurable,
    }
}

pub(super) fn craqle_fjall_persist_mode(policy: FjallPersistPolicy) -> CraqleFjallPersistMode {
    match policy {
        FjallPersistPolicy::Buffer => CraqleFjallPersistMode::Buffer,
        FjallPersistPolicy::SyncAll => CraqleFjallPersistMode::SyncAll,
    }
}

pub(super) fn craqle_graph_policy(policy: MetadataGraphPolicy) -> GraphPolicy {
    GraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

pub(super) fn document_sync_peer_id(node_id: NodeId) -> irokle::PeerId {
    irokle::PeerId::from_bytes(*node_id.as_bytes())
}

pub(super) fn metadata_graph_policy_from_craqle(policy: GraphPolicy) -> MetadataGraphPolicy {
    MetadataGraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

pub(super) fn metadata_dot_from_craqle(dot: craqle::Dot) -> MetadataDot {
    MetadataDot {
        actor: *dot.actor.as_bytes(),
        counter: dot.counter,
    }
}

pub(super) fn metadata_batch_from_craqle(batch: Batch) -> MetadataBatch {
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

pub(super) fn to_craqle_batch(batch: &MetadataBatch) -> Result<Batch, CraqleError> {
    let timestamp =
        chrono::DateTime::from_timestamp_millis(batch.timestamp_millis).ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::InvalidBatch(
                "batch timestamp is out of range".to_string(),
            ))
        })?;
    Ok(Batch {
        graph: GraphId::new(&batch.graph_iri),
        actor: ActorId::from_bytes(batch.actor),
        counter: batch.counter,
        base_clock: batch.base_clock.clone(),
        ops: batch
            .ops
            .iter()
            .map(|op| match op {
                MetadataQuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => craqle::QuadOp::Add {
                    subject: craqle::EncodedTerm(subject.clone()),
                    predicate: craqle::EncodedTerm(predicate.clone()),
                    object: craqle::EncodedTerm(object.clone()),
                    dot: craqle::Dot {
                        actor: ActorId::from_bytes(dot.actor),
                        counter: dot.counter,
                    },
                },
                MetadataQuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => craqle::QuadOp::Remove {
                    subject: craqle::EncodedTerm(subject.clone()),
                    predicate: craqle::EncodedTerm(predicate.clone()),
                    object: craqle::EncodedTerm(object.clone()),
                    witnessed: witnessed.clone(),
                },
            })
            .collect(),
        timestamp,
    })
}
