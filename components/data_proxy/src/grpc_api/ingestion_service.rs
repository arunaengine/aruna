use crate::{
    auth::auth_helpers::get_token_from_md,
    caching::cache::Cache,
    data_backends::storage_backend::StorageBackend,
    helpers::bucket_path_from_pathstring,
    structs::{FileFormat, ObjectLocation, TypedRelation},
};
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_ingestion_service_server::DataproxyIngestionService,
    ingest_existing_object_request::{Collection, Dataset},
    IngestExistingObjectRequest, IngestExistingObjectResponse,
};
use diesel_ulid::DieselUlid;
use std::{str::FromStr, sync::Arc};
use tracing::error;

#[derive(Clone)]
pub struct DataproxyIngestionServiceImpl {
    pub cache: Arc<Cache>,
    pub backend: Arc<Box<dyn StorageBackend>>,
    admin_ids: Vec<DieselUlid>,
}

impl DataproxyIngestionServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
    pub fn new(
        cache: Arc<Cache>,
        backend: Arc<Box<dyn StorageBackend>>,
        admin_ids: Vec<DieselUlid>,
    ) -> Self {
        Self {
            cache,
            backend,
            admin_ids,
        }
    }
}

#[tonic::async_trait]
impl DataproxyIngestionService for DataproxyIngestionServiceImpl {
    async fn ingest_existing_object(
        &self,
        request: tonic::Request<IngestExistingObjectRequest>,
    ) -> Result<tonic::Response<IngestExistingObjectResponse>, tonic::Status> {
        let user_id = if let Some(a) = self.cache.auth.read().await.as_ref() {
            let token = get_token_from_md(request.metadata()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

            let (u, _, pk) = a.check_permissions(&token).map_err(|_| {
                error!(error = "Unable to authenticate user, check permissions");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            if pk.is_proxy {
                error!(error = "Proxy token is not allowed to ingest objects");
                return Err(tonic::Status::unauthenticated(
                    "Proxy token is not allowed to ingest objects",
                ));
            }

            if !self.admin_ids.contains(&u) {
                error!(error = "Only admins are allowed to ingest objects");
                return Err(tonic::Status::unauthenticated("Invalid permissions"));
            }
            u
        } else {
            error!(error = "Unable to authenticate user, cache is empty");
            return Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ));
        };

        let request = request.into_inner();
        let bucket = DieselUlid::from_str(&request.project_id)
            .map_err(|_| tonic::Status::invalid_argument("Unable to parse project_id"))?;

        let (project, _) = self
            .cache
            .get_resource_cloned(&bucket, true)
            .await
            .map_err(|_| {
                error!(error = "Unable to find project");
                tonic::Status::not_found("Unable to find project")
            })?;

        let Some(created_by) = project.created_by else {
            error!(error = "Unable to find project owner");
            return Err(tonic::Status::not_found("Unable to find project owner"));
        };
        if created_by != user_id {
            error!(error = "User is not the owner of the project");
            return Err(tonic::Status::unauthenticated("Invalid permissions"));
        }

        let user_token = if let Some(element) = self.cache.auth.read().await.as_ref() {
            element
                .sign_impersonating_token(user_id.to_string(), None::<String>)
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    tonic::Status::unauthenticated(e.to_string())
                })?
        } else {
            error!(error = "Unable to authenticate user, cache is empty");
            return Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ));
        };

        let (bucket, key) = bucket_path_from_pathstring(&request.path).map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            tonic::Status::invalid_argument("Invalid path")
        })?;

        let mut location = ObjectLocation {
            id: DieselUlid::generate(),
            bucket,
            key,
            file_format: FileFormat::Raw,
            ..Default::default()
        };

        let head = self
            .backend
            .head_object(location.clone())
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::internal(e.to_string())
            })?;

        location.raw_content_len = head;
        location.disk_content_len = head;

        // Collection
        let mut maybe_collection = None;
        if let Some(col) = request.collection {
            match col {
                Collection::CollectionId(id) => {
                    let collection_id = DieselUlid::from_str(&id).map_err(|_| {
                        tonic::Status::invalid_argument("Unable to parse collection_id")
                    })?;

                    let (collection, _) = self
                        .cache
                        .get_resource_cloned(&collection_id, true)
                        .await
                        .map_err(|_| {
                            error!(error = "Unable to find collection");
                            tonic::Status::not_found("Unable to find collection")
                        })?;
                    if let Some(parents) = &collection.parents {
                        if !parents.contains(&TypedRelation::Project(project.id)) {
                            error!(error = "Collection is not part of the project");
                            return Err(tonic::Status::not_found(
                                "Collection is not part of the project",
                            ));
                        }
                    } else {
                        error!(error = "Collection is not part of the project");
                        return Err(tonic::Status::not_found(
                            "Collection is not part of the project",
                        ));
                    }
                    maybe_collection = Some(collection);
                }
                Collection::CollectionResource(ingest_res) => {
                    if let Some(grpc_handler) = self.cache.aruna_client.read().await.as_ref() {
                        maybe_collection = Some(
                            grpc_handler
                                .create_collection_ingestion(ingest_res, project.id, &user_token)
                                .await
                                .map_err(|e| {
                                    error!(error = ?e, msg = e.to_string());
                                    tonic::Status::internal(e.to_string())
                                })?,
                        );
                    }
                }
            }
        }

        // Collection
        let mut maybe_dataset = None;
        if let Some(ds) = request.dataset {
            match ds {
                Dataset::DatasetId(id) => {
                    let dataset_id = DieselUlid::from_str(&id).map_err(|_| {
                        tonic::Status::invalid_argument("Unable to parse collection_id")
                    })?;

                    let (dataset, _) = self
                        .cache
                        .get_resource_cloned(&dataset_id, true)
                        .await
                        .map_err(|_| {
                            error!(error = "Unable to find collection");
                            tonic::Status::not_found("Unable to find collection")
                        })?;
                    if let Some(parents) = &dataset.parents {
                        if !parents.contains(&TypedRelation::Project(project.id)) {
                            error!(error = "Collection is not part of the project");
                            return Err(tonic::Status::not_found(
                                "Collection is not part of the project",
                            ));
                        }
                    } else {
                        error!(error = "Collection is not part of the project");
                        return Err(tonic::Status::not_found(
                            "Collection is not part of the project",
                        ));
                    }
                    maybe_dataset = Some(dataset);
                }
                Dataset::DatasetResource(ingest_res) => {
                    if let Some(grpc_handler) = self.cache.aruna_client.read().await.as_ref() {
                        let parent = if let Some(c) = &maybe_collection {
                            TypedRelation::Collection(c.id)
                        } else {
                            TypedRelation::Project(project.id)
                        };

                        maybe_dataset = Some(
                            grpc_handler
                                .create_dataset_ingest(ingest_res, parent, &user_token)
                                .await
                                .map_err(|e| {
                                    error!(error = ?e, msg = e.to_string());
                                    tonic::Status::internal(e.to_string())
                                })?,
                        );
                    }
                }
            }
        }

        let parent = if let Some(d) = maybe_dataset {
            TypedRelation::Dataset(d.id)
        } else if let Some(collection) = maybe_collection {
            TypedRelation::Collection(collection.id)
        } else {
            TypedRelation::Project(project.id)
        };

        if let Some(grpc_handler) = self.cache.aruna_client.read().await.as_ref() {
            if let Some(object) = request.object {
                let object = grpc_handler
                    .create_and_finish_ingest(
                        object,
                        location.disk_content_len,
                        parent,
                        &user_token,
                    )
                    .await
                    .map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::internal(e.to_string())
                    })?;

                self.cache
                    .add_location_with_binding(object.id, location)
                    .await
                    .map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::internal(e.to_string())
                    })?;

                return Ok(tonic::Response::new(IngestExistingObjectResponse {
                    object_id: object.id.to_string(),
                }));
            }
        }
        error!(error = "Unable to find aruna client");
        return Err(tonic::Status::internal("Unable to find aruna client"));
    }
}
