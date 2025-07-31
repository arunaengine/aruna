use crate::{
    error::ArunaMetadataError,
    logerr,
    models::{
        requests::Request,
        structs::{PolicyResult, TaskPayload, TypedDoc},
    },
    network::network_trait::{Network, REPLICATION_POLICY},
    persistence::{
        persistor::{
            Persistor,
            tables::{GROUPS_DB_NAME, USER_DB_NAME},
        },
        search::generic::Search,
    },
};
use aruna_permission::Path;
use aruna_storage::storage::store::Store;
use aruna_task::{Task, TaskHandler, error::ArunaTaskError, task_trait::TaskExecutor};
use automerge::sync::Message;
use iroh::NodeAddr;
use std::sync::Arc;
use tokio::{sync::Notify, task::JoinSet};
use tracing::{Instrument, error, trace};
use ulid::Ulid;

pub struct Controller<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    pub network: Arc<N>,
    pub persistence: Arc<Persistor<St, Se>>,
    pub executor_idx: u8,
    pub task_handler: TaskHandler<St>,
}

impl<St, Se, N> Clone for Controller<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    fn clone(&self) -> Self {
        Controller {
            network: self.network.clone(),
            persistence: self.persistence.clone(),
            executor_idx: self.executor_idx.clone(),
            task_handler: self.task_handler.clone(),
        }
    }
}

impl<St, Se, N> Controller<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    #[tracing::instrument(level = "trace", skip(persistence, network, task_handler))]
    pub async fn new(
        persistence: Arc<Persistor<St, Se>>,
        network: Arc<N>,
        task_handler: TaskHandler<St>,
    ) -> Self {
        let mut controller = Self {
            persistence,
            network,
            executor_idx: 0,
            task_handler: task_handler.clone(),
        };
        controller.executor_idx = controller
            .task_handler
            .add_executor(controller.clone_box())
            .await;
        controller
    }
    #[tracing::instrument(level = "trace", skip(self, request, token))]
    pub async fn request<R: Request<St, Se, N> + Send>(
        &self,
        mut request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaMetadataError> {
        match &mut request.run_policy(&token, self).await? {
            PolicyResult::Deny(reason) => Err(ArunaMetadataError::Forbidden(reason.to_string())),
            PolicyResult::Forward => request.forward(&token, self).await,
            PolicyResult::Modify | PolicyResult::Accept => {
                match request.sync_or_forward(&token, self).await? {
                    Some(response) => Ok(response),
                    None => {
                        let user_identity = request.authorize(token, self).await?;
                        request.run_request(user_identity, self).await
                    }
                }
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, token))]
    pub async fn sync_user(&self, token: String) -> Result<(), ArunaMetadataError> {
        let identity = self
            .persistence
            .get_identity(token)
            .await
            .map_err(logerr!())?;

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(identity.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        let nodes = self.network.find_verified(subject_hash).await?;

        if !nodes.contains(&self.network.get_addr().await?) {
            let doc = self
                .persistence
                .get_or_create_doc(identity.to_bytes(), USER_DB_NAME)
                .await?;

            let path = Path::builder()
                .realm_id(self.network.get_realm_key().await?)
                .build()
                .map_err(|e| crate::error::ArunaMetadataError::ServerError(e.to_string()))?;

            self.sync_loop(
                crate::models::structs::TypedDoc::User(doc),
                *subject_hash,
                identity.to_bytes(),
                path,
                nodes.into_iter(),
                true,
            )
            .await?;
            self.network.store(subject_hash).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, group_id))]
    pub async fn sync_group(&self, group_id: Ulid) -> Result<(), ArunaMetadataError> {
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(group_id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        let nodes = self.network.find_verified(subject_hash).await?;

        if !nodes.contains(&self.network.get_addr().await?) {
            trace!("Syncing group {}", group_id.to_string());
            let doc = self
                .persistence
                .get_or_create_doc(group_id.to_bytes().to_vec(), GROUPS_DB_NAME)
                .await?;

            let path = Path::builder()
                .realm_id(self.network.get_realm_key().await?)
                .group(group_id)
                .build()
                .map_err(|e| crate::error::ArunaMetadataError::ServerError(e.to_string()))?;

            self.sync_loop(
                crate::models::structs::TypedDoc::Group(doc),
                *subject_hash,
                group_id.to_bytes().to_vec(),
                path,
                nodes.into_iter(),
                true,
            )
            .await?;
            self.network.store(subject_hash).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, doc, nodes))]
    pub async fn sync_loop(
        &self,
        doc: TypedDoc,
        subject_hash: [u8; 32],
        doc_id: Vec<u8>,
        path: Path,
        nodes: impl Iterator<Item = NodeAddr>,
        wait: bool,
    ) -> Result<(), ArunaMetadataError> {
        let distribution_strategy = match doc {
            TypedDoc::Resource(_) => {
                aruna_task::DistributionStrategy::LimitedRealm(REPLICATION_POLICY as u32)
            }
            TypedDoc::Group(_) | TypedDoc::User(_) => aruna_task::DistributionStrategy::AllInRealm,
        };
        let retry_strategy = aruna_task::RetryStrategy::Forever;
        let notify = match wait {
            true => Some(Arc::new(Notify::new())),
            false => None,
        };
        let payload = TaskPayload::Sync {
            doc: doc.into(),
            subject_hash,
            doc_id,
            path,
            nodes: nodes.collect(),
        };
        self.task_handler
            .register_task(
                distribution_strategy,
                retry_strategy,
                self.executor_idx,
                postcard::to_allocvec(&payload).map_err(logerr!())?,
                notify.clone(),
            )
            .await
            .map_err(logerr!())?;
        if let Some(notify) = notify {
            trace!("Waiting ...");
            notify.notified().await
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, doc, nodes))]
    async fn distribute_sync(
        &self,
        doc: TypedDoc,
        subject_hash: [u8; 32],
        doc_id: Vec<u8>,
        path: Path,
        nodes: impl Iterator<Item = NodeAddr>,
    ) -> JoinSet<Result<(), ArunaMetadataError>> {
        let mut tasks = tokio::task::JoinSet::new();
        for node in nodes {
            trace!("Syncing to node {}", node.node_id);
            let inner_doc = doc.get_inner();
            let network = self.network.clone();
            let persistence = self.persistence.clone();
            let doc = doc.clone();
            let doc_id = doc_id.clone();
            let path = path.clone();
            tasks.spawn(
                async move {
                    let mut stream = network.create_stream(node.node_id).await?;
                    let mut document = inner_doc;
                    'sync: loop {
                        let sync_message = persistence
                            .generate_sync_message(doc_id.clone(), &mut document, node.node_id)
                            .await?;

                        let recv_message = network
                            .sync(
                                &mut stream,
                                match &doc {
                                    TypedDoc::Resource(_) => {
                                        crate::network::network_trait::ReplicationSubject::Object(
                                            sync_message.clone(),
                                        )
                                    }
                                    TypedDoc::Group(_) => {
                                        crate::network::network_trait::ReplicationSubject::Group(
                                            sync_message.clone(),
                                        )
                                    }
                                    TypedDoc::User(_) => {
                                        crate::network::network_trait::ReplicationSubject::User(
                                            sync_message.clone(),
                                        )
                                    }
                                },
                                &subject_hash,
                                doc_id.clone(),
                                path.clone(),
                                node.clone(),
                            )
                            .await?;

                        if let Some(response) = &recv_message {
                            persistence
                                .receive_sync_message(
                                    doc_id.clone(),
                                    &mut document,
                                    Message::decode(response)?,
                                    node.node_id,
                                )
                                .await?;
                        }
                        if sync_message.is_none() && recv_message.is_none() {
                            break 'sync;
                        }
                    }

                    match &doc {
                        TypedDoc::Resource(_) => {
                            persistence
                                .handle_object_merges(path, document.save())
                                .await?
                        }
                        TypedDoc::Group(_) => {
                            persistence.handle_group_merges(document.save()).await?
                        }
                        TypedDoc::User(_) => {
                            persistence.handle_user_merges(document.save()).await?
                        }
                    }
                    network.finish_stream(stream).await?;

                    Ok::<(), ArunaMetadataError>(())
                }
                .in_current_span(),
            );
        }
        tasks
    }
}

#[async_trait::async_trait]
impl<St, Se, N> TaskExecutor for Controller<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    #[tracing::instrument(level = "trace", skip(self, task))]
    async fn execute(&self, task: Task) -> Result<(), ArunaTaskError> {
        trace!("Execution metadata task");
        let res: TaskPayload = postcard::from_bytes(&task.payload).map_err(logerr!())?;

        match res {
            TaskPayload::Sync {
                doc,
                subject_hash,
                doc_id,
                path,
                nodes,
            } => {
                let doc: TypedDoc = doc.try_into().map_err(|err: ArunaMetadataError| {
                    error!("{err}");
                    ArunaTaskError::ExecutionError(err.to_string())
                })?;
                for x in self
                    .distribute_sync(doc, subject_hash, doc_id, path, nodes.into_iter())
                    .await
                    .join_all()
                    .await
                {
                    x.map_err(|err| {
                        error!("{err}");
                        ArunaTaskError::ExecutionError(err.to_string())
                    })?;
                }
            }
        }

        Ok(())
    }
    fn clone_box(&self) -> Box<dyn TaskExecutor + 'static> {
        Box::new(self.clone())
    }
}
