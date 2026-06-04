use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncEvent, DocumentSyncTarget, IrokleEvent};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::IROKLE_APPLIED_OPS_KEYSPACE;
use aruna_core::metadata::MetadataGraphLifecycleRecord;
use aruna_core::storage_entries::{
    metadata_graph_lifecycle_key, metadata_graph_lifecycle_write_entry,
    metadata_registry_delete_entries, metadata_registry_write_entries, stale_subject_index_deletes,
    subject_index_writes,
};
use aruna_core::structs::{MetadataRegistryRecord, User};
use aruna_core::types::Value;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use irokle_crate::Event as _;
use irokle_crate::Storage as _;
use irokle_crate::TopicControl;
use irokle_crate::oplog::Oplog;
use irokle_crate::sync::{SyncMessage, SyncRequest};
use irokle_crate::{EventEnvelope, OpId, PeerId, ReplicationPolicy, TopicGenesis, TopicPayload};
use parking_lot::RwLock;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::error::{NetError, Result};
use crate::streams::BiStream;

use ::irokle as irokle_crate;

const IROKLE_PEER_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct IrokleService {
    node: irokle_crate::Irokle<irokle_crate::FjallStorage>,
    net: Arc<irokle_crate::net::IrohNet<irokle_crate::FjallStorage>>,
    storage: StorageHandle,
    default_peers: Arc<RwLock<BTreeSet<PeerId>>>,
    storage_path: PathBuf,
}

impl std::fmt::Debug for IrokleService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrokleService")
            .field("peer_id", &self.node.peer_id())
            .field("storage_path", &self.storage_path)
            .finish()
    }
}

impl IrokleService {
    pub fn open(
        endpoint: iroh::Endpoint,
        storage: StorageHandle,
        storage_path: impl AsRef<Path>,
        peer_nodes: &[NodeId],
        alpns: Vec<Vec<u8>>,
        runtime: irokle_crate::net::IrohRuntimeConfig,
    ) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let default_peers: BTreeSet<PeerId> = peer_nodes.iter().map(node_id_to_peer_id).collect();
        let node = irokle_crate::Irokle::builder()
            .with_iroh_secret_key(endpoint.secret_key())
            .with_peer_whitelist(default_peers.clone())
            .with_fjall_path(&storage_path)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .build()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let net = Arc::new(
            irokle_crate::net::IrohNet::new_with_alpns_and_config(
                endpoint,
                node.clone(),
                alpns,
                runtime,
            )
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
        net.start_configured_resync_loop()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;

        Ok(Self {
            node,
            net,
            storage,
            default_peers: Arc::new(RwLock::new(default_peers)),
            storage_path,
        })
    }

    pub fn node(&self) -> irokle_crate::Irokle<irokle_crate::FjallStorage> {
        self.node.clone()
    }

    pub fn allow_peer_node(&self, node_id: NodeId) -> Result<()> {
        let peer_id = node_id_to_peer_id(&node_id);
        if peer_id == self.node.peer_id() {
            return Ok(());
        }
        self.node
            .add_peer_to_whitelist(peer_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    pub fn add_potential_peer_node(&self, node_id: NodeId) -> Result<()> {
        let peer_id = node_id_to_peer_id(&node_id);
        if peer_id == self.node.peer_id() {
            return Ok(());
        }
        self.allow_peer_node(node_id)?;
        self.default_peers.write().insert(peer_id);
        Ok(())
    }

    pub fn add_potential_peer_nodes(&self, nodes: impl IntoIterator<Item = NodeId>) -> Result<()> {
        for node_id in nodes {
            self.add_potential_peer_node(node_id)?;
        }
        Ok(())
    }

    pub fn refresh_potential_peer_nodes(
        &self,
        nodes: impl IntoIterator<Item = NodeId>,
    ) -> Result<()> {
        let mut peers = BTreeSet::new();
        for node_id in nodes {
            let peer_id = node_id_to_peer_id(&node_id);
            if peer_id == self.node.peer_id() {
                continue;
            }
            peers.insert(peer_id);
        }
        self.node
            .add_peers_to_whitelist(peers.iter().copied())
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        *self.default_peers.write() = peers;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.net.shutdown().await;
    }

    pub async fn sync_topic_with_peers(
        &self,
        topic_id: irokle_crate::TopicId,
        peers: Vec<NodeId>,
    ) -> Result<()> {
        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;
        self.sync_topic(topic_id, sync_peers).await
    }

    pub async fn handle_inbound_stream(&self, stream: BiStream, peer: NodeId) -> Result<usize> {
        let BiStream(send, recv, _) = stream;
        self.net
            .handle_stream(peer, recv, send)
            .await
            .map_err(|error| NetError::Stream(error.to_string()))?;
        self.reconcile_documents().await
    }

    pub async fn publish_document(
        &self,
        event_id: Ulid,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        peers: Vec<NodeId>,
    ) -> IrokleEvent {
        let event = DocumentSyncEvent::Upsert {
            event_id,
            target: target.clone(),
            bytes,
        };
        match self.publish_event(event, peers).await {
            Ok(()) => IrokleEvent::DocumentPublished { target },
            Err(error) => IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            },
        }
    }

    pub async fn delete_document(
        &self,
        event_id: Ulid,
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    ) -> IrokleEvent {
        let event = DocumentSyncEvent::Delete {
            event_id,
            target: target.clone(),
        };
        match self.publish_event(event, peers).await {
            Ok(()) => IrokleEvent::DocumentDeleted { target },
            Err(error) => IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            },
        }
    }

    pub async fn reconcile_documents_event(&self) -> IrokleEvent {
        match self.reconcile_documents().await {
            Ok(applied) => IrokleEvent::DocumentsReconciled { applied },
            Err(error) => IrokleEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn sync_document_event(
        &self,
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    ) -> IrokleEvent {
        let topic_id = target.irokle_topic_id();
        let sync_peers = self.sync_peers(peers);
        if let Err(error) = self.allow_sync_peers(&sync_peers) {
            return IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            };
        }
        match self.has_topic(topic_id) {
            Ok(true) => {
                if let Err(error) = self.sync_topic(topic_id, sync_peers).await {
                    return IrokleEvent::Error {
                        target: Some(target),
                        error: error.to_string(),
                    };
                }
            }
            Ok(false) => {
                if let Err(error) = self.bootstrap_topic_from_peers(topic_id, &sync_peers).await {
                    return IrokleEvent::Error {
                        target: Some(target),
                        error: error.to_string(),
                    };
                }
            }
            Err(error) => {
                return IrokleEvent::Error {
                    target: Some(target),
                    error: error.to_string(),
                };
            }
        }
        match self.reconcile_documents().await {
            Ok(applied) => IrokleEvent::DocumentsReconciled { applied },
            Err(error) => IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            },
        }
    }

    async fn publish_event(&self, event: DocumentSyncEvent, peers: Vec<NodeId>) -> Result<()> {
        let target = event.target().clone();
        let topic_id = target.irokle_topic_id();
        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;
        self.ensure_topic(&target, &sync_peers)?;
        let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
        let envelope = EventEnvelope::encode_event(&event)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let oplog = Oplog::with_storage(self.node.storage().clone());
        oplog
            .create_event_op(topic_id, actor_id, envelope, self.node.signer())
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.net.schedule_topic_recheck(topic_id)?;
        Ok(())
    }

    fn ensure_topic(
        &self,
        target: &DocumentSyncTarget,
        peers: &BTreeSet<PeerId>,
    ) -> Result<irokle_crate::TopicId> {
        let topic_id = target.irokle_topic_id();
        if let Some(state) = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
        {
            if state.event_type_id != DocumentSyncEvent::TYPE_ID {
                return Err(NetError::Bootstrap(format!(
                    "Irokle topic {topic_id} has event type {}, expected {}",
                    state.event_type_id,
                    DocumentSyncEvent::TYPE_ID
                )));
            }
            let missing_peers = peers
                .iter()
                .copied()
                .filter(|peer| !state.members.contains(peer))
                .collect::<Vec<_>>();
            if !missing_peers.is_empty() {
                let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
                let oplog = Oplog::with_storage(self.node.storage().clone());
                for peer in missing_peers {
                    oplog
                        .create_control_op(
                            topic_id,
                            actor_id,
                            TopicControl::AddPeer { peer },
                            self.node.signer(),
                        )
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                }
                self.net.schedule_topic_recheck(topic_id)?;
            }
            return Ok(topic_id);
        }

        let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
        let genesis = TopicGenesis {
            event_type_id: DocumentSyncEvent::TYPE_ID.to_string(),
            initial_peers: peers.clone(),
            replication_policy: ReplicationPolicy::all(),
        };
        let oplog = Oplog::with_storage(self.node.storage().clone());
        oplog
            .create_topic_genesis(topic_id, actor_id, genesis, self.node.signer())
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.net.schedule_topic_recheck(topic_id)?;
        Ok(topic_id)
    }

    fn has_topic(&self, topic_id: irokle_crate::TopicId) -> Result<bool> {
        Ok(self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .is_some())
    }

    fn sync_peers(&self, peers: Vec<NodeId>) -> BTreeSet<PeerId> {
        let mut sync_peers = if peers.is_empty() {
            self.default_peers.read().clone()
        } else {
            peers
                .into_iter()
                .map(|node_id| node_id_to_peer_id(&node_id))
                .collect()
        };
        sync_peers.remove(&self.node.peer_id());
        sync_peers
    }

    fn allow_sync_peers(&self, peers: &BTreeSet<PeerId>) -> Result<()> {
        self.node
            .add_peers_to_whitelist(peers.iter().copied())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    async fn sync_topic(
        &self,
        topic_id: irokle_crate::TopicId,
        peers: BTreeSet<PeerId>,
    ) -> Result<()> {
        let attempted = peers.len();
        if attempted == 0 {
            return Ok(());
        }

        let mut syncs = JoinSet::new();
        let mut successes = 0usize;
        let mut first_error = None;
        for peer in peers {
            let net = self.net.clone();
            syncs.spawn(async move {
                let result = match timeout(
                    IROKLE_PEER_SYNC_TIMEOUT,
                    net.sync_peer_now(peer, topic_id),
                )
                .await
                {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(NetError::Bootstrap(error.to_string())),
                    Err(_) => Err(NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT)),
                };
                (peer, result)
            });
        }
        while let Some(result) = syncs.join_next().await {
            match result {
                Ok((peer, Ok(()))) => {
                    successes += 1;
                    debug!(%peer, %topic_id, "Synced Irokle document topic")
                }
                Ok((peer, Err(error))) => {
                    warn!(%peer, %topic_id, error = %error, "Irokle document sync attempt failed");
                    if first_error.is_none() {
                        first_error = Some(NetError::Bootstrap(error.to_string()));
                    }
                }
                Err(error) => {
                    warn!(error = %error, "Irokle document sync task failed");
                    if first_error.is_none() {
                        first_error = Some(NetError::Bootstrap(error.to_string()));
                    }
                }
            }
        }
        if successes < attempted {
            let detail = first_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "unknown sync error".to_string());
            return Err(NetError::Bootstrap(format!(
                "synced Irokle topic {topic_id} with {successes}/{attempted} peers; {detail}"
            )));
        }
        Ok(())
    }

    async fn bootstrap_topic_from_peers(
        &self,
        topic_id: irokle_crate::TopicId,
        peers: &BTreeSet<PeerId>,
    ) -> Result<()> {
        let mut first_error = None;
        for peer in peers {
            match self.bootstrap_topic_from_peer(topic_id, *peer).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    warn!(%peer, %topic_id, error = %error, "Irokle document bootstrap attempt failed");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        Err(first_error.unwrap_or_else(|| {
            NetError::Bootstrap(format!(
                "no peers available to bootstrap Irokle topic {topic_id}"
            ))
        }))
    }

    async fn bootstrap_topic_from_peer(
        &self,
        topic_id: irokle_crate::TopicId,
        peer: PeerId,
    ) -> Result<()> {
        let peer_addr = peer_id_to_endpoint_addr(peer)?;
        let responses = timeout(
            IROKLE_PEER_SYNC_TIMEOUT,
            self.net.sync_with(
                peer_addr.clone(),
                &[SyncMessage::Open(self.node.sync_open(topic_id))],
            ),
        )
        .await
        .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;
        let summary = responses
            .into_iter()
            .find_map(|response| match response {
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => Some(summary),
                _ => None,
            })
            .ok_or_else(|| {
                NetError::Bootstrap(format!(
                    "peer {peer} did not return an Irokle summary for topic {topic_id}"
                ))
            })?;
        if summary.event_type_id.as_deref() != Some(DocumentSyncEvent::TYPE_ID) {
            return Err(NetError::Bootstrap(format!(
                "peer {peer} advertised Irokle topic {topic_id} with unexpected event type {:?}",
                summary.event_type_id
            )));
        }

        let request = SyncRequest {
            topic_id,
            known: BTreeSet::new(),
            wants: summary.heads,
            actor_range_hints: Vec::new(),
        };
        let responses = timeout(
            IROKLE_PEER_SYNC_TIMEOUT,
            self.net.sync_with(
                peer_addr.clone(),
                &[
                    SyncMessage::Open(self.node.sync_open(topic_id)),
                    SyncMessage::Request(request),
                ],
            ),
        )
        .await
        .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;

        let mut followup = vec![SyncMessage::Open(self.node.sync_open(topic_id))];
        let mut received_data = false;
        for response in responses {
            match response {
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                SyncMessage::Data(data) if data.topic_id == topic_id => {
                    let ack = self
                        .node
                        .receive_sync_data_from(peer, data)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                    received_data = true;
                    followup.push(SyncMessage::Ack(ack));
                }
                other => {
                    return Err(NetError::Bootstrap(format!(
                        "unexpected Irokle bootstrap response: {other:?}"
                    )));
                }
            }
        }
        if received_data {
            self.net.schedule_topic_recheck(topic_id)?;
        }
        if followup.len() > 1 {
            let responses = timeout(
                IROKLE_PEER_SYNC_TIMEOUT,
                self.net.sync_with(peer_addr, &followup),
            )
            .await
            .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
            .map_err(NetError::from)?;
            for response in responses {
                match response {
                    SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                    other => {
                        return Err(NetError::Bootstrap(format!(
                            "unexpected Irokle bootstrap ack response: {other:?}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    async fn reconcile_documents(&self) -> Result<usize> {
        let mut applied = 0usize;
        let topics = self
            .node
            .list_topics()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        for topic in topics {
            if topic.event_type_id != DocumentSyncEvent::TYPE_ID {
                continue;
            }
            let raw = self
                .node
                .raw_topic(topic.topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let ops = raw
                .history()
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            for op in ops {
                let TopicPayload::Event(envelope) = op.signed.body.payload else {
                    continue;
                };
                if self.has_applied(op.id).await? {
                    continue;
                }
                let event = envelope
                    .decode_event::<DocumentSyncEvent>()
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                let event_id = event.event_id();
                let target_topic_id = event.target().irokle_topic_id();
                if target_topic_id != topic.topic_id {
                    warn!(
                        topic_id = %topic.topic_id,
                        %target_topic_id,
                        "Skipping Irokle document event whose target does not match its topic"
                    );
                    self.mark_applied(op.id).await?;
                    continue;
                }
                if self.has_applied_event(event_id).await? {
                    self.mark_applied(op.id).await?;
                    continue;
                }
                self.apply_document_event(event).await?;
                self.mark_applied_event(event_id).await?;
                self.mark_applied(op.id).await?;
                applied += 1;
            }
        }
        Ok(applied)
    }

    async fn apply_document_event(&self, event: DocumentSyncEvent) -> Result<()> {
        match event {
            DocumentSyncEvent::Upsert { target, bytes, .. } => {
                self.apply_upsert(target, bytes).await
            }
            DocumentSyncEvent::Delete { target, .. } => self.apply_delete(target).await,
        }
    }

    async fn apply_upsert(&self, target: DocumentSyncTarget, bytes: Vec<u8>) -> Result<()> {
        if let DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } = target
        {
            let record: MetadataRegistryRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.group_id != group_id || record.document_id != document_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata registry target {group_id}/{document_id} does not match payload {}/{}",
                    record.group_id, record.document_id
                )));
            }
            return self.apply_metadata_registry_upsert(record, bytes).await;
        }
        if let DocumentSyncTarget::MetadataGraphLifecycle { graph_iri } = target {
            let record: MetadataGraphLifecycleRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.graph_iri != graph_iri {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                    record.graph_iri
                )));
            }
            return self.apply_metadata_graph_lifecycle(record, bytes).await;
        }
        if let DocumentSyncTarget::User { user_id } = target {
            let user =
                User::from_bytes(&bytes).map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if user.user_id != user_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated user document id {} does not match payload user id {}",
                    user_id, user.user_id
                )));
            }
            return self.apply_user_upsert(user, bytes).await;
        }
        self.storage_write(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.into(),
        )
        .await
    }

    async fn apply_user_upsert(&self, user: User, primary_bytes: Vec<u8>) -> Result<()> {
        let target = DocumentSyncTarget::User {
            user_id: user.user_id,
        };
        let previous = self
            .storage_read(target.storage_keyspace().to_string(), target.storage_key())
            .await?
            .map(|bytes| User::from_bytes(&bytes))
            .transpose()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;

        let deletes = stale_subject_index_deletes(previous.as_ref(), Some(&user));
        if !deletes.is_empty() {
            self.storage_batch_delete(deletes).await?;
        }

        let mut writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            primary_bytes.into(),
        )];
        writes.extend(subject_index_writes(&user));
        self.storage_batch_write(writes).await?;
        Ok(())
    }

    async fn apply_metadata_registry_upsert(
        &self,
        record: MetadataRegistryRecord,
        primary_bytes: Vec<u8>,
    ) -> Result<()> {
        if self.metadata_graph_deleted(&record.graph_iri).await? {
            return self
                .storage_batch_delete(metadata_registry_delete_entries(
                    record.group_id,
                    record.document_id,
                ))
                .await;
        }
        let mut entries = metadata_registry_write_entries(&record)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if let Some((_, _, value)) = entries.first_mut() {
            *value = primary_bytes.into();
        }
        self.storage_batch_write(entries).await
    }

    async fn apply_metadata_graph_lifecycle(
        &self,
        record: MetadataGraphLifecycleRecord,
        primary_bytes: Vec<u8>,
    ) -> Result<()> {
        let (key_space, key, _) = metadata_graph_lifecycle_write_entry(&record)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.storage_write(key_space, key, primary_bytes.into())
            .await?;
        if record.is_deleted() {
            self.storage_batch_delete(metadata_registry_delete_entries(
                record.group_id,
                record.document_id,
            ))
            .await?;
        }
        Ok(())
    }

    async fn metadata_graph_deleted(&self, graph_iri: &str) -> Result<bool> {
        let value = self
            .storage_read(
                aruna_core::keyspaces::METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                metadata_graph_lifecycle_key(graph_iri),
            )
            .await?;
        let Some(value) = value else {
            return Ok(false);
        };
        let record: MetadataGraphLifecycleRecord =
            postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
        Ok(record.is_deleted())
    }

    async fn apply_delete(&self, target: DocumentSyncTarget) -> Result<()> {
        if let DocumentSyncTarget::MetadataGraphLifecycle { .. } = target {
            return Ok(());
        }
        if let DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } = target
        {
            return self
                .storage_batch_delete(metadata_registry_delete_entries(group_id, document_id))
                .await;
        }
        if let DocumentSyncTarget::User { user_id } = target {
            let target = DocumentSyncTarget::User { user_id };
            let previous = self
                .storage_read(target.storage_keyspace().to_string(), target.storage_key())
                .await?
                .map(|bytes| User::from_bytes(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let mut deletes = vec![(target.storage_keyspace().to_string(), target.storage_key())];
            if let Some(previous) = previous {
                deletes.extend(stale_subject_index_deletes(Some(&previous), None));
            }
            return self.storage_batch_delete(deletes).await;
        }
        self.storage_delete(target.storage_keyspace().to_string(), target.storage_key())
            .await
    }

    async fn storage_read(&self, key_space: String, key: ByteView) -> Result<Option<Value>> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle read: {other:?}"
            ))),
        }
    }

    async fn has_applied(&self, op_id: OpId) -> Result<bool> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Read {
                key_space: IROKLE_APPLIED_OPS_KEYSPACE.to_string(),
                key: ByteView::from(op_id.as_bytes().to_vec()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while reading applied irokle op: {other:?}"
            ))),
        }
    }

    async fn mark_applied(&self, op_id: OpId) -> Result<()> {
        self.storage_write(
            IROKLE_APPLIED_OPS_KEYSPACE.to_string(),
            ByteView::from(op_id.as_bytes().to_vec()),
            ByteView::from(vec![1u8]),
        )
        .await
    }

    async fn has_applied_event(&self, event_id: Ulid) -> Result<bool> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Read {
                key_space: IROKLE_APPLIED_OPS_KEYSPACE.to_string(),
                key: applied_event_key(event_id),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while reading applied document sync event: {other:?}"
            ))),
        }
    }

    async fn mark_applied_event(&self, event_id: Ulid) -> Result<()> {
        self.storage_write(
            IROKLE_APPLIED_OPS_KEYSPACE.to_string(),
            applied_event_key(event_id),
            ByteView::from(vec![1u8]),
        )
        .await
    }

    async fn storage_write(&self, key_space: String, key: ByteView, value: Value) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle write: {other:?}"
            ))),
        }
    }

    async fn storage_batch_write(&self, writes: Vec<(String, ByteView, Value)>) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle batch write: {other:?}"
            ))),
        }
    }

    async fn storage_delete(&self, key_space: String, key: ByteView) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Delete {
                key_space,
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle delete: {other:?}"
            ))),
        }
    }

    async fn storage_batch_delete(&self, deletes: Vec<(String, ByteView)>) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle batch delete: {other:?}"
            ))),
        }
    }
}

fn node_id_to_peer_id(node_id: &NodeId) -> PeerId {
    PeerId::from_bytes(*node_id.as_bytes())
}

fn applied_event_key(event_id: Ulid) -> ByteView {
    let mut key = b"document-sync-event/".to_vec();
    key.extend_from_slice(&event_id.to_bytes());
    ByteView::from(key)
}

fn peer_id_to_endpoint_addr(peer_id: PeerId) -> Result<iroh::EndpointAddr> {
    let endpoint_id = iroh::EndpointId::from_bytes(peer_id.as_bytes())
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(iroh::EndpointAddr::from(endpoint_id))
}
