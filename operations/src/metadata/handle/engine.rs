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
