use super::request::Request;
use crate::{
    error::ArunaMetadataError,
    models::models::TypedDoc,
    network::network_trait::Network,
    persistence::{
        persistence::{Persistor, tables::USER_DB_NAME},
        search::search::Search,
    },
};
use aruna_permission::{Path, UserIdentity};
use aruna_storage::storage::store::Store;
use automerge::sync::Message;
use iroh::NodeAddr;
use std::sync::Arc;

pub struct Controller<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    pub network: Arc<N>,
    pub persistence: Arc<Persistor<St, Se>>,
}

impl<St, Se, N> Controller<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    #[tracing::instrument(level = "trace", skip(persistence, network))]
    pub fn new(persistence: Arc<Persistor<St, Se>>, network: Arc<N>) -> Self {
        let controller = Self {
            persistence,
            network,
        };
        controller
    }
    #[tracing::instrument(level = "trace", skip(self, request, token))]
    pub async fn request<R: Request<St, Se, N>>(
        &self,
        request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaMetadataError> {
        match request.forward_or_return(&token, self).await? {
            Some(response) => Ok(response),
            None => {
                let user_identity = request.authorize(token, self).await?;
                let result = request.run_request(user_identity, self).await?;
                Ok(result)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, token))]
    pub async fn get_or_sync_user(
        &self,
        token: String,
    ) -> Result<UserIdentity, ArunaMetadataError> {
        let identity = self.persistence.get_identity(token).await?;

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
                crate::models::models::TypedDoc::User(doc),
                *subject_hash,
                identity.to_bytes(),
                path,
                nodes.into_iter(),
            )
            .await?;
        }
        Ok(identity)
    }

    #[tracing::instrument(level = "trace", skip(self, doc, nodes))]
    pub async fn sync_loop(
        &self,
        doc: TypedDoc,
        subject_hash: [u8; 32],
        doc_id: Vec<u8>,
        path: Path,
        nodes: impl Iterator<Item = NodeAddr>,
    ) -> Result<(), ArunaMetadataError> {
        for node in nodes {
            let inner_doc = doc.get_inner();
            let network = self.network.clone();
            let persistence = self.persistence.clone();
            let doc = doc.clone();
            let doc_id = doc_id.clone();
            let path = path.clone();
            tokio::spawn(async move {
                let mut stream = network.create_stream(node.node_id).await?;
                let mut document = inner_doc;
                'sync: loop {
                    let sync_message = persistence
                        .generate_sync_message(doc_id.clone(), &mut document, node.node_id.clone())
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
                                node.node_id.clone(),
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
                    TypedDoc::Group(_) => persistence.handle_group_merges(document.save()).await?,
                    TypedDoc::User(_) => persistence.handle_user_merges(document.save()).await?,
                }
                network.finish_stream(stream).await?;

                Ok::<(), ArunaMetadataError>(())
            });
        }
        Ok(())
    }
}
