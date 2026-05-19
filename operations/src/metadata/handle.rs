use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::metadata::{
    MetadataBatch, MetadataCreateCrateRequest, MetadataDot, MetadataEffect, MetadataError,
    MetadataEvent, MetadataGraphPolicy, MetadataQuadOp, MetadataQueryResults, MetadataRoCratePage,
    MetadataSearchHit, MetadataUpsertEntityRequest,
};
use aruna_core::structs::{AuthContext, MetadataRegistryRecord, Permission};
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use craqle::{
    ActorId, AllowAllAuthorizer, Batch, CraqleError, CraqleNode, CreateCrateRequest,
    CreateEntityRequest, GraphId, GraphPolicy, QueryResults, RoCrateError, VectorClock, vocab,
};
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use serde_json::Value;
use tokio::time::timeout;
use tracing::warn;
use ulid::Ulid;

use super::protocol::{MetadataTransportMessage, read_message, write_message};
use super::repository::{
    delete_document_index_effect, delete_holders_effect, delete_registry_effect,
    iter_all_registry_effect, parse_registry_iter, parse_registry_read,
    read_registry_by_document_effect, write_document_index_effect, write_holders_effect,
    write_registry_effect,
};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};

const METADATA_IO_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct MetadataHandle {
    inner: Arc<MetadataInner>,
}

struct MetadataInner {
    node: Arc<CraqleNode>,
    storage_handle: StorageHandle,
    net_handle: Option<NetHandle>,
    local_node_id: NodeId,
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
    ) -> Result<Self, MetadataError> {
        let actor = ActorId::from_bytes(*node_id.as_bytes());
        let node = CraqleNode::open_with_actor(path, actor)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(MetadataInner {
                node: Arc::new(node),
                storage_handle,
                net_handle,
                local_node_id: node_id,
            }),
        })
    }

    pub async fn send_metadata_effect(&self, effect: MetadataEffect) -> Event {
        let graph_iri = effect_graph_iri(&effect);
        match effect {
            MetadataEffect::ReplicateBootstrap { record } => {
                Event::Metadata(self.replicate_bootstrap(record).await)
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
            MetadataEffect::ReplicateBatch { record, batch } => {
                Event::Metadata(self.replicate_batch(record, batch).await)
            }
            MetadataEffect::ReplicateDelete { record } => {
                Event::Metadata(self.replicate_delete(record).await)
            }
            MetadataEffect::SyncFromPeer {
                node_id,
                document_id,
                known_clock,
            } => Event::Metadata(self.sync_from_peer(node_id, document_id, known_clock).await),
            other => {
                let inner = self.inner.clone();
                match tokio::task::spawn_blocking(move || handle_effect(inner, other)).await {
                    Ok(event) => Event::Metadata(event),
                    Err(error) => Event::Metadata(MetadataEvent::Error {
                        graph_iri,
                        error: MetadataError::TaskJoin(error.to_string()),
                    }),
                }
            }
        }
    }

    pub async fn handle_inbound_stream(
        &self,
        mut stream: BiStream,
        _peer: NodeId,
    ) -> Result<(), MetadataError> {
        let message = read_transport_message(&mut stream).await?;

        let response = match message {
            MetadataTransportMessage::UpsertRecord { record } => {
                match persist_replica_record(self.inner.clone(), &record).await {
                    Ok(()) => MetadataTransportMessage::Ack,
                    Err(error) => {
                        let _ = cleanup_replica_graph(self.inner.clone(), &record.graph_iri).await;
                        MetadataTransportMessage::Reject(error.to_string())
                    }
                }
            }
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
            MetadataTransportMessage::CatchupFrom {
                document_id,
                known_clock,
            } => match fetch_catchup_data(self.inner.clone(), document_id, known_clock).await {
                Ok((record, batches)) => MetadataTransportMessage::CatchupData { record, batches },
                Err(error) => MetadataTransportMessage::Reject(error.to_string()),
            },
            MetadataTransportMessage::ApplyBatch { batch } => {
                match apply_remote_batch(self.inner.clone(), batch).await {
                    Ok(()) => MetadataTransportMessage::Ack,
                    Err(error) => MetadataTransportMessage::Reject(error.to_string()),
                }
            }
            MetadataTransportMessage::DeleteRecord { record } => {
                match delete_replica_record(self.inner.clone(), record).await {
                    Ok(()) => MetadataTransportMessage::Ack,
                    Err(error) => MetadataTransportMessage::Reject(error.to_string()),
                }
            }
            MetadataTransportMessage::QueryResults { .. }
            | MetadataTransportMessage::SearchResults { .. }
            | MetadataTransportMessage::CatchupData { .. }
            | MetadataTransportMessage::Ack
            | MetadataTransportMessage::Reject(_) => {
                MetadataTransportMessage::Reject("unexpected metadata control message".to_string())
            }
        };

        drain_request_stream(&mut stream).await?;

        let _ = write_transport_message(&mut stream, &response).await;
        close_stream(&mut stream).await;
        Ok(())
    }

    pub async fn query_authorized_local(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataError> {
        query_local_graphs(self.inner.clone(), auth_context, graph_iris, sparql).await
    }

    pub async fn search_authorized_local(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        search_local_graphs(self.inner.clone(), auth_context, graph_iris, query, limit).await
    }

    pub async fn request_remote_query_graphs(
        &self,
        node_id: NodeId,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataError> {
        let Some(net_handle) = self.inner.net_handle.clone() else {
            return Err(MetadataError::HandleMissing);
        };
        match send_request(
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
        }
    }

    pub async fn request_remote_search_graphs(
        &self,
        node_id: NodeId,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        let Some(net_handle) = self.inner.net_handle.clone() else {
            return Err(MetadataError::HandleMissing);
        };
        match send_request(
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
        }
    }

    async fn sync_from_peer(
        &self,
        node_id: NodeId,
        document_id: Ulid,
        known_clock: VectorClock,
    ) -> MetadataEvent {
        let Some(net_handle) = self.inner.net_handle.clone() else {
            return MetadataEvent::Error {
                graph_iri: None,
                error: MetadataError::HandleMissing,
            };
        };

        match send_request(
            &net_handle,
            node_id,
            MetadataTransportMessage::CatchupFrom {
                document_id,
                known_clock,
            },
        )
        .await
        {
            Ok(MetadataTransportMessage::CatchupData { record, batches }) => {
                let graph_iri = record.graph_iri.clone();
                if let Err(error) = persist_replica_record(self.inner.clone(), &record).await {
                    return MetadataEvent::Error {
                        graph_iri: Some(graph_iri),
                        error,
                    };
                }
                for batch in batches {
                    if let Err(error) = apply_remote_batch(self.inner.clone(), batch).await {
                        return MetadataEvent::Error {
                            graph_iri: Some(graph_iri.clone()),
                            error,
                        };
                    }
                }
                MetadataEvent::PeerSyncApplied {
                    document_id,
                    graph_iri,
                }
            }
            Ok(MetadataTransportMessage::Reject(error)) => MetadataEvent::Error {
                graph_iri: None,
                error: MetadataError::Backend(error),
            },
            Ok(other) => MetadataEvent::Error {
                graph_iri: None,
                error: MetadataError::Backend(format!(
                    "unexpected metadata catchup response: {other:?}"
                )),
            },
            Err(error) => MetadataEvent::Error {
                graph_iri: None,
                error,
            },
        }
    }

    async fn replicate_bootstrap(&self, mut record: MetadataRegistryRecord) -> MetadataEvent {
        let graph_iri = record.graph_iri.clone();
        let Some(net_handle) = self.inner.net_handle.clone() else {
            record.holder_node_ids = vec![self.inner.local_node_id];
            return MetadataEvent::BootstrapReplicated {
                graph_iri,
                replicated_node_ids: record.holder_node_ids,
            };
        };

        let batches =
            match export_catchup_batches(self.inner.clone(), &graph_iri, VectorClock::default())
                .await
            {
                Ok(batches) => batches,
                Err(error) => {
                    return MetadataEvent::Error {
                        graph_iri: Some(graph_iri),
                        error,
                    };
                }
            };

        let mut remote_targets = Vec::new();
        let mut seen = HashSet::new();
        for node_id in &record.holder_node_ids {
            if *node_id == self.inner.local_node_id || !seen.insert(*node_id) {
                continue;
            }
            remote_targets.push(*node_id);
        }

        let mut bootstrapped = Vec::new();
        for node_id in remote_targets {
            match send_apply_batches(&net_handle, node_id, &batches).await {
                Ok(()) => bootstrapped.push(node_id),
                Err(error) => {
                    warn!(node_id = %node_id, error = %error, "metadata bootstrap batch sync failed")
                }
            }
        }

        let mut provisional_holders = vec![self.inner.local_node_id];
        provisional_holders.extend(bootstrapped.iter().copied());
        record.holder_node_ids = provisional_holders.clone();

        let mut confirmed = Vec::new();
        for node_id in bootstrapped {
            match send_upsert_record(&net_handle, node_id, record.clone()).await {
                Ok(()) => confirmed.push(node_id),
                Err(error) => {
                    warn!(node_id = %node_id, error = %error, "metadata bootstrap record sync failed")
                }
            }
        }

        if confirmed.len() != provisional_holders.len().saturating_sub(1) {
            let mut corrected = record.clone();
            corrected.holder_node_ids = vec![self.inner.local_node_id];
            corrected.holder_node_ids.extend(confirmed.iter().copied());
            for node_id in &confirmed {
                if let Err(error) =
                    send_upsert_record(&net_handle, *node_id, corrected.clone()).await
                {
                    warn!(node_id = %node_id, error = %error, "metadata holder correction failed");
                }
            }
            return MetadataEvent::BootstrapReplicated {
                graph_iri,
                replicated_node_ids: corrected.holder_node_ids,
            };
        }

        MetadataEvent::BootstrapReplicated {
            graph_iri,
            replicated_node_ids: record.holder_node_ids,
        }
    }

    async fn replicate_delete(&self, record: MetadataRegistryRecord) -> MetadataEvent {
        let graph_iri = record.graph_iri.clone();
        let Some(net_handle) = self.inner.net_handle.clone() else {
            return MetadataEvent::DeleteReplicated {
                graph_iri,
                replicated_node_ids: vec![self.inner.local_node_id],
            };
        };

        let mut replicated = vec![self.inner.local_node_id];
        for node_id in record
            .holder_node_ids
            .iter()
            .copied()
            .filter(|node_id| *node_id != self.inner.local_node_id)
        {
            if send_delete_record(&net_handle, node_id, record.clone())
                .await
                .is_ok()
            {
                replicated.push(node_id);
            }
        }

        MetadataEvent::DeleteReplicated {
            graph_iri,
            replicated_node_ids: replicated,
        }
    }

    async fn replicate_batch(
        &self,
        record: MetadataRegistryRecord,
        batch: MetadataBatch,
    ) -> MetadataEvent {
        let graph_iri = record.graph_iri.clone();
        let Some(net_handle) = self.inner.net_handle.clone() else {
            return MetadataEvent::BatchReplicated {
                graph_iri,
                replicated_node_ids: vec![self.inner.local_node_id],
            };
        };

        let mut replicated = vec![self.inner.local_node_id];
        for node_id in record
            .holder_node_ids
            .iter()
            .copied()
            .filter(|node_id| *node_id != self.inner.local_node_id)
        {
            if send_apply_batch(&net_handle, node_id, batch.clone())
                .await
                .is_ok()
                && send_upsert_record(&net_handle, node_id, record.clone())
                    .await
                    .is_ok()
            {
                replicated.push(node_id);
            }
        }

        MetadataEvent::BatchReplicated {
            graph_iri,
            replicated_node_ids: replicated,
        }
    }
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

fn handle_effect(inner: Arc<MetadataInner>, effect: MetadataEffect) -> MetadataEvent {
    let auth = AllowAllAuthorizer;
    let graph_iri = effect_graph_iri(&effect);
    let node = inner.node.clone();
    let result = match effect {
        MetadataEffect::CreateCrate { request } => node
            .create_crate(&auth, craqle_create_request(request.clone()))
            .map(|batch| MetadataEvent::CreateCrateResult {
                graph_iri: request.graph_iri,
                batch: metadata_batch_from_craqle(batch),
            }),
        MetadataEffect::ApplyRoCrate { request } => node
            .apply_rocrate_document_checked_with_policy(
                &auth,
                GraphId::new(&request.graph_iri),
                &request.jsonld,
                craqle_graph_policy(request.policy),
            )
            .map(|batch| MetadataEvent::ApplyRoCrateResult {
                graph_iri: request.graph_iri,
                batch: metadata_batch_from_craqle(batch),
            }),
        MetadataEffect::UpsertDataEntity { request } => upsert_data_entity(&node, &auth, request)
            .map(|batch| MetadataEvent::EntityUpsertResult {
                graph_iri: batch.graph_iri.clone(),
                batch,
            }),
        MetadataEffect::UpsertContextualEntity { request } => {
            upsert_contextual_entity(&node, &auth, request).map(|batch| {
                MetadataEvent::EntityUpsertResult {
                    graph_iri: batch.graph_iri.clone(),
                    batch,
                }
            })
        }
        MetadataEffect::SetGraphPolicy { graph_iri, policy } => node
            .import_graph_policy(&GraphId::new(&graph_iri), craqle_graph_policy(policy))
            .map(|_| MetadataEvent::GraphPolicySet { graph_iri }),
        MetadataEffect::GetGraphPolicy { graph_iri } => node
            .graph_policy(&GraphId::new(&graph_iri))
            .map(|policy| MetadataEvent::GraphPolicyResult {
                graph_iri,
                policy: metadata_graph_policy_from_craqle(policy),
            }),
        MetadataEffect::ExportRoCrate { graph_iri } => node
            .export_rocrate(&auth, &GraphId::new(&graph_iri))
            .map(|jsonld| MetadataEvent::RoCrateExportResult { graph_iri, jsonld }),
        MetadataEffect::ExportRoCrateSummary { graph_iri } => node
            .export_rocrate_summary(&auth, &GraphId::new(&graph_iri))
            .map(|jsonld| MetadataEvent::RoCrateSummaryResult { graph_iri, jsonld }),
        MetadataEffect::ExportRoCratePage {
            graph_iri,
            offset,
            after,
            limit,
        } => {
            let graph = GraphId::new(&graph_iri);
            let page = if let Some(after) = after.as_deref() {
                node.export_rocrate_page_after(&auth, &graph, Some(after), limit)
            } else {
                node.export_rocrate_page(&auth, &graph, offset.unwrap_or(0), limit)
            };
            page.map(|page| MetadataEvent::RoCratePageResult {
                graph_iri,
                page: metadata_rocrate_page_from_craqle(page),
            })
        }
        MetadataEffect::SearchGraphs { .. } | MetadataEffect::QueryGraphs { .. } => {
            unreachable!("handled asynchronously")
        }
        MetadataEffect::DeleteGraph { graph_iri } => node
            .delete_graph_unchecked(&GraphId::new(&graph_iri))
            .map(|_| MetadataEvent::GraphDeleted { graph_iri }),
        MetadataEffect::ListGraphs => node.graphs().map(|graphs| MetadataEvent::GraphListResult {
            graph_iris: graphs
                .into_iter()
                .map(|graph| graph.as_str().to_string())
                .collect(),
        }),
        MetadataEffect::ContainsGraph { graph_iri } => node
            .contains_graph(&GraphId::new(&graph_iri))
            .map(|exists| MetadataEvent::ContainsGraphResult { graph_iri, exists }),
        MetadataEffect::VectorClock { graph_iri } => node
            .vector_clock(&GraphId::new(&graph_iri))
            .map(|clock| MetadataEvent::VectorClockResult { graph_iri, clock }),
        MetadataEffect::CatchupBatches {
            graph_iri,
            remote_clock,
        } => node
            .catchup_batches(&GraphId::new(&graph_iri), &remote_clock)
            .map(|batches| MetadataEvent::CatchupBatchesResult {
                graph_iri,
                batches: batches
                    .into_iter()
                    .map(metadata_batch_from_craqle)
                    .collect(),
            }),
        MetadataEffect::ReplicateBootstrap { .. }
        | MetadataEffect::ReplicateBatch { .. }
        | MetadataEffect::ReplicateDelete { .. }
        | MetadataEffect::SyncFromPeer { .. } => unreachable!("handled asynchronously"),
        MetadataEffect::ApplyRemoteBatch { batch } => {
            let graph_iri = batch.graph_iri.clone();
            node.apply_remote_batch(craqle_batch(batch))
                .map(|_| MetadataEvent::RemoteBatchApplied { graph_iri })
        }
    };

    result.unwrap_or_else(|error| MetadataEvent::Error {
        graph_iri,
        error: metadata_error_from_craqle(error),
    })
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

fn effect_graph_iri(effect: &MetadataEffect) -> Option<String> {
    match effect {
        MetadataEffect::CreateCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::ApplyRoCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => Some(request.graph_iri.clone()),
        MetadataEffect::SetGraphPolicy { graph_iri, .. }
        | MetadataEffect::GetGraphPolicy { graph_iri }
        | MetadataEffect::ExportRoCrate { graph_iri }
        | MetadataEffect::ExportRoCrateSummary { graph_iri }
        | MetadataEffect::DeleteGraph { graph_iri }
        | MetadataEffect::ContainsGraph { graph_iri }
        | MetadataEffect::VectorClock { graph_iri } => Some(graph_iri.clone()),
        MetadataEffect::ExportRoCratePage { graph_iri, .. } => Some(graph_iri.clone()),
        MetadataEffect::ReplicateBootstrap { record }
        | MetadataEffect::ReplicateBatch { record, .. }
        | MetadataEffect::ReplicateDelete { record } => Some(record.graph_iri.clone()),
        MetadataEffect::SearchGraphs { graph_iris, .. } => graph_iris
            .as_ref()
            .and_then(|graph_iris| graph_iris.first().cloned()),
        MetadataEffect::QueryGraphs { graph_iris, .. } => graph_iris
            .as_ref()
            .and_then(|graph_iris| graph_iris.first().cloned()),
        MetadataEffect::CatchupBatches { graph_iri, .. } => Some(graph_iri.clone()),
        MetadataEffect::ApplyRemoteBatch { batch } => Some(batch.graph_iri.clone()),
        MetadataEffect::SyncFromPeer { .. } | MetadataEffect::ListGraphs => None,
    }
}

async fn fetch_catchup_data(
    inner: Arc<MetadataInner>,
    document_id: Ulid,
    known_clock: VectorClock,
) -> Result<(MetadataRegistryRecord, Vec<MetadataBatch>), MetadataError> {
    let Some(record) =
        read_registry_record_by_document(inner.storage_handle.clone(), document_id).await?
    else {
        return Err(MetadataError::Backend(format!(
            "metadata document not found: {document_id}"
        )));
    };
    let batches = export_catchup_batches(inner, &record.graph_iri, known_clock).await?;
    Ok((record, batches))
}

async fn read_registry_record_by_document(
    storage_handle: StorageHandle,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, MetadataError> {
    let event = storage_handle
        .send_effect(read_registry_by_document_effect(document_id, None))
        .await;
    parse_registry_read(event).map_err(|error| {
        MetadataError::Backend(format!(
            "metadata registry read by document failed: {error:?}"
        ))
    })
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

fn craqle_dot(dot: MetadataDot) -> craqle::Dot {
    craqle::Dot {
        actor: ActorId::from_bytes(dot.actor),
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

fn craqle_batch(batch: MetadataBatch) -> Batch {
    Batch {
        graph: GraphId::new(&batch.graph_iri),
        actor: ActorId::from_bytes(batch.actor),
        counter: batch.counter,
        base_clock: batch.base_clock,
        ops: batch
            .ops
            .into_iter()
            .map(|op| match op {
                MetadataQuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => craqle::QuadOp::Add {
                    subject: craqle::EncodedTerm(subject),
                    predicate: craqle::EncodedTerm(predicate),
                    object: craqle::EncodedTerm(object),
                    dot: craqle_dot(dot),
                },
                MetadataQuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => craqle::QuadOp::Remove {
                    subject: craqle::EncodedTerm(subject),
                    predicate: craqle::EncodedTerm(predicate),
                    object: craqle::EncodedTerm(object),
                    witnessed,
                },
            })
            .collect(),
        timestamp: Utc
            .timestamp_millis_opt(batch.timestamp_millis)
            .single()
            .unwrap_or_else(|| {
                Utc.timestamp_millis_opt(0)
                    .single()
                    .expect("unix epoch exists")
            }),
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

async fn export_catchup_batches(
    inner: Arc<MetadataInner>,
    graph_iri: &str,
    remote_clock: VectorClock,
) -> Result<Vec<MetadataBatch>, MetadataError> {
    let graph_iri = graph_iri.to_string();
    tokio::task::spawn_blocking(move || {
        inner
            .node
            .catchup_batches(&GraphId::new(&graph_iri), &remote_clock)
            .map(|batches| {
                batches
                    .into_iter()
                    .map(metadata_batch_from_craqle)
                    .collect()
            })
            .map_err(|error| MetadataError::Backend(error.to_string()))
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
}

async fn list_local_registry_records(
    storage_handle: StorageHandle,
) -> Result<Vec<MetadataRegistryRecord>, MetadataError> {
    let mut records = Vec::new();
    let mut start_after = None;
    loop {
        let event = storage_handle
            .send_effect(iter_all_registry_effect(start_after.clone(), None))
            .await;
        let (mut page, next_start_after) = parse_registry_iter(event).map_err(|error| {
            MetadataError::Backend(format!("metadata registry iteration failed: {error:?}"))
        })?;
        records.append(&mut page);
        if let Some(cursor) = next_start_after {
            start_after = Some(cursor);
        } else {
            return Ok(records);
        }
    }
}

async fn query_local_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    sparql: String,
) -> Result<MetadataQueryResults, MetadataError> {
    let records = list_local_registry_records(inner.storage_handle.clone()).await?;
    let allowed = select_authorized_graphs(
        inner.storage_handle.clone(),
        auth_context,
        records,
        graph_iris,
    )
    .await?;
    tokio::task::spawn_blocking(move || {
        inner
            .node
            .query_graphs(&graph_ids(&allowed), &sparql)
            .map(metadata_query_results_from_craqle)
            .map_err(|error| MetadataError::Backend(error.to_string()))
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
}

async fn search_local_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let records = list_local_registry_records(inner.storage_handle.clone()).await?;
    let allowed_records = select_authorized_records(
        inner.storage_handle.clone(),
        auth_context,
        records,
        graph_iris,
    )
    .await?;
    tokio::task::spawn_blocking(move || {
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
                    .collect()
            })
            .map_err(|error| MetadataError::Backend(error.to_string()))
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
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

async fn select_authorized_records(
    storage_handle: StorageHandle,
    auth_context: Option<AuthContext>,
    records: Vec<MetadataRegistryRecord>,
    graph_filter: Option<Vec<String>>,
) -> Result<Vec<MetadataRegistryRecord>, MetadataError> {
    let allowed_graphs = graph_filter.map(|graphs| graphs.into_iter().collect::<HashSet<_>>());
    let mut visible = Vec::new();
    for record in records {
        if let Some(filter) = allowed_graphs.as_ref()
            && !filter.contains(&record.graph_iri)
        {
            continue;
        }
        if can_read_record_locally(storage_handle.clone(), auth_context.clone(), &record).await? {
            visible.push(record);
        }
    }
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
        automerge_handle: None,
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

async fn apply_remote_batch(
    inner: Arc<MetadataInner>,
    batch: MetadataBatch,
) -> Result<(), MetadataError> {
    tokio::task::spawn_blocking(move || {
        inner
            .node
            .apply_remote_batch(craqle_batch(batch))
            .map_err(|error| MetadataError::Backend(error.to_string()))
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
}

async fn cleanup_replica_graph(
    inner: Arc<MetadataInner>,
    graph_iri: &str,
) -> Result<(), MetadataError> {
    let graph_iri = graph_iri.to_string();
    tokio::task::spawn_blocking(move || {
        inner
            .node
            .delete_graph_unchecked(&GraphId::new(&graph_iri))
            .map_err(|error| MetadataError::Backend(error.to_string()))
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
}

async fn persist_replica_record(
    inner: Arc<MetadataInner>,
    record: &MetadataRegistryRecord,
) -> Result<(), MetadataError> {
    persist_graph_policy(inner.clone(), record).await?;

    let storage_handle = inner.storage_handle.clone();
    let txn_id = match storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(MetadataError::Backend(error.to_string()));
        }
        other => {
            return Err(MetadataError::Backend(format!(
                "unexpected storage start transaction event: {other:?}"
            )));
        }
    };

    let result = async {
        write_storage_effect(
            &storage_handle,
            write_registry_effect(record, Some(txn_id))
                .map_err(|error| MetadataError::Backend(error.to_string()))?,
            "metadata registry write",
        )
        .await?;
        write_storage_effect(
            &storage_handle,
            write_document_index_effect(record, Some(txn_id))
                .map_err(|error| MetadataError::Backend(error.to_string()))?,
            "metadata document index write",
        )
        .await?;
        write_storage_effect(
            &storage_handle,
            write_holders_effect(record, Some(txn_id))
                .map_err(|error| MetadataError::Backend(error.to_string()))?,
            "metadata holders write",
        )
        .await?;

        match storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(MetadataError::Backend(error.to_string()))
            }
            other => Err(MetadataError::Backend(format!(
                "unexpected storage commit event: {other:?}"
            ))),
        }
    }
    .await;

    if result.is_err() {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
    }
    result
}

async fn persist_graph_policy(
    inner: Arc<MetadataInner>,
    record: &MetadataRegistryRecord,
) -> Result<(), MetadataError> {
    let graph_iri = record.graph_iri.clone();
    let policy = graph_policy_for_record(record);
    tokio::task::spawn_blocking(move || {
        inner
            .node
            .import_graph_policy(&GraphId::new(&graph_iri), craqle_graph_policy(policy))
            .map_err(|error| MetadataError::Backend(error.to_string()))
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
}

fn graph_policy_for_record(record: &MetadataRegistryRecord) -> MetadataGraphPolicy {
    MetadataGraphPolicy {
        public: record.public,
        permission_paths: vec![record.permission_path.clone()],
    }
}

async fn write_storage_effect(
    storage_handle: &StorageHandle,
    effect: Effect,
    label: &str,
) -> Result<(), MetadataError> {
    match storage_handle.send_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataError::Backend(error.to_string()))
        }
        other => Err(MetadataError::Backend(format!(
            "unexpected {label} event: {other:?}"
        ))),
    }
}

async fn send_upsert_record(
    net_handle: &NetHandle,
    node_id: NodeId,
    record: MetadataRegistryRecord,
) -> Result<(), MetadataError> {
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Metadata)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    write_transport_message(
        &mut stream,
        &MetadataTransportMessage::UpsertRecord { record },
    )
    .await?;
    wait_for_request_delivery(&mut stream).await
}

async fn send_apply_batch(
    net_handle: &NetHandle,
    node_id: NodeId,
    batch: MetadataBatch,
) -> Result<(), MetadataError> {
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Metadata)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    write_transport_message(&mut stream, &MetadataTransportMessage::ApplyBatch { batch }).await?;
    wait_for_request_delivery(&mut stream).await
}

async fn send_apply_batches(
    net_handle: &NetHandle,
    node_id: NodeId,
    batches: &[MetadataBatch],
) -> Result<(), MetadataError> {
    for batch in batches {
        send_apply_batch(net_handle, node_id, batch.clone()).await?;
    }
    Ok(())
}

async fn send_request(
    net_handle: &NetHandle,
    node_id: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataTransportMessage, MetadataError> {
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Metadata)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    write_transport_message(&mut stream, &message).await?;
    stream
        .0
        .finish()
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    let response = read_transport_message(&mut stream).await?;
    close_stream(&mut stream).await;
    Ok(response)
}

async fn send_delete_record(
    net_handle: &NetHandle,
    node_id: NodeId,
    record: MetadataRegistryRecord,
) -> Result<(), MetadataError> {
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Metadata)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    write_transport_message(
        &mut stream,
        &MetadataTransportMessage::DeleteRecord { record },
    )
    .await?;
    wait_for_request_delivery(&mut stream).await
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

async fn delete_replica_record(
    inner: Arc<MetadataInner>,
    record: MetadataRegistryRecord,
) -> Result<(), MetadataError> {
    cleanup_replica_graph(inner.clone(), &record.graph_iri).await?;

    let storage_handle = inner.storage_handle.clone();
    let txn_id = match storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(MetadataError::Backend(error.to_string()));
        }
        other => {
            return Err(MetadataError::Backend(format!(
                "unexpected storage start transaction event: {other:?}"
            )));
        }
    };

    let result = async {
        delete_storage_effect(
            &storage_handle,
            delete_registry_effect(record.group_id, record.document_id, Some(txn_id)),
            "metadata registry delete",
        )
        .await?;
        delete_storage_effect(
            &storage_handle,
            delete_document_index_effect(record.document_id, Some(txn_id)),
            "metadata document index delete",
        )
        .await?;
        delete_storage_effect(
            &storage_handle,
            delete_holders_effect(record.group_id, record.document_id, Some(txn_id)),
            "metadata holders delete",
        )
        .await?;

        match storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(MetadataError::Backend(error.to_string()))
            }
            other => Err(MetadataError::Backend(format!(
                "unexpected storage commit event: {other:?}"
            ))),
        }
    }
    .await;

    if result.is_err() {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
    }
    result
}

async fn delete_storage_effect(
    storage_handle: &StorageHandle,
    effect: Effect,
    label: &str,
) -> Result<(), MetadataError> {
    match storage_handle.send_effect(effect).await {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataError::Backend(error.to_string()))
        }
        other => Err(MetadataError::Backend(format!(
            "unexpected {label} event: {other:?}"
        ))),
    }
}

async fn wait_for_request_delivery(stream: &mut BiStream) -> Result<(), MetadataError> {
    stream
        .0
        .finish()
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    match timeout(METADATA_IO_TIMEOUT, stream.0.stopped()).await {
        Ok(Ok(None)) => Ok(()),
        Ok(Ok(Some(code))) => Err(MetadataError::Backend(format!(
            "metadata stream stopped by peer: {code}"
        ))),
        Ok(Err(error)) => Err(MetadataError::Backend(error.to_string())),
        Err(_) => Err(MetadataError::Backend(
            "timed out waiting for metadata request delivery".to_string(),
        )),
    }
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
