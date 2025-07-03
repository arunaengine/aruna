use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::crud::CrudDb;
use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::license_dsl::{License, ALL_RIGHTS_RESERVED};
use crate::database::dsls::object_dsl::{
    Author, EndpointInfo, ExternalRelations, Hashes, KeyValues, Object,
};
use crate::database::enums::{
    DbPermissionLevel, ObjectStatus, ObjectType, ReplicationStatus, ReplicationType,
};
use crate::utils::conversions::relations::ContextContainer;
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::{relation::Relation as RelationEnum, Relation};
use aruna_rust_api::api::storage::{
    models::v2::{ExternalRelation, KeyValue},
    services::v2::{
        CreateCollectionRequest, CreateDatasetRequest, CreateObjectRequest, CreateProjectRequest,
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use lazy_static::lazy_static;
use postgres_types::Json;
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::Client;

pub enum CreateRequest {
    Project(CreateProjectRequest, String),
    Collection(CreateCollectionRequest),
    Dataset(CreateDatasetRequest),
    Object(CreateObjectRequest),
}

#[derive(Clone)]
pub enum Parent {
    Project(String),
    Collection(String),
    Dataset(String),
}

lazy_static! {
    pub static ref PROJECT_SCHEMA: Regex =
        Regex::new(r"^[a-z0-9\-]+$").expect("Regex must be valid");
    pub static ref S3_KEY_SCHEMA: Regex =
        Regex::new(r"^[a-zA-Z0-9\-\!\_\.\*\'\(\)]+$").expect("Regex must be valid");

    //TODO: Improve to match more sophisticated cases
    pub static ref OBJECT_SCHEMA: Regex =
        Regex::new(r"^[a-zA-Z0-9\-\!\_\.\*\'\(\)\/\s]+$").expect("Regex must be valid");
}

impl Parent {
    pub fn get_id(&self) -> Result<DieselUlid> {
        match self {
            Parent::Project(id) | Parent::Collection(id) | Parent::Dataset(id) => {
                Ok(DieselUlid::from_str(id.as_str())?)
            }
        }
    }

    pub fn get_type(&self) -> ObjectType {
        match self {
            Parent::Project(_) => ObjectType::PROJECT,
            Parent::Collection(_) => ObjectType::COLLECTION,
            Parent::Dataset(_) => ObjectType::DATASET,
        }
    }

    pub fn get_context(&self) -> Result<Context> {
        Ok(Context::res_ctx(
            self.get_id()?,
            DbPermissionLevel::APPEND,
            true,
        ))
    }
}

impl CreateRequest {
    pub fn get_name(&self) -> Result<String> {
        match self {
            CreateRequest::Project(request, _) => {
                let name = request.name.to_string();
                if !PROJECT_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid project name"))
                } else {
                    Ok(name)
                }
            }
            CreateRequest::Collection(request) => {
                let name = request.name.to_string();
                if !S3_KEY_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid collection name"))
                } else {
                    Ok(name)
                }
            }
            CreateRequest::Dataset(request) => {
                let name = request.name.to_string();
                if !S3_KEY_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid dataset name"))
                } else {
                    Ok(name)
                }
            }
            CreateRequest::Object(request) => {
                let full_name = request.name.to_string();
                let mut name_parts = full_name.split("/").collect::<Vec<&str>>();
                let name = name_parts
                    .pop()
                    .ok_or_else(|| anyhow!("Invalid object name"))?;

                for part in name_parts {
                    if !S3_KEY_SCHEMA.is_match(part) {
                        return Err(anyhow!("Invalid path name"));
                    }
                }

                if !OBJECT_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid object name"))
                } else {
                    Ok(full_name.to_string())
                }
            }
        }
    }

    pub fn get_description(&self) -> String {
        match self {
            CreateRequest::Project(request, _) => request.description.to_string(),
            CreateRequest::Collection(request) => request.description.to_string(),
            CreateRequest::Dataset(request) => request.description.to_string(),
            CreateRequest::Object(request) => request.description.to_string(),
        }
    }

    pub fn get_key_values(&self) -> &Vec<KeyValue> {
        match self {
            CreateRequest::Project(request, _) => &request.key_values,
            CreateRequest::Collection(request) => &request.key_values,
            CreateRequest::Dataset(request) => &request.key_values,
            CreateRequest::Object(request) => &request.key_values,
        }
    }

    pub fn get_relation_contexts(&self) -> Result<Vec<Context>, tonic::Status> {
        let container: ContextContainer = match self {
            CreateRequest::Project(req, _) => req.relations.clone().try_into()?,
            CreateRequest::Collection(req) => req.relations.clone().try_into()?,
            CreateRequest::Dataset(req) => req.relations.clone().try_into()?,
            CreateRequest::Object(req) => req.relations.clone().try_into()?,
        };
        Ok(container.0)
    }

    pub async fn get_internal_relations(
        &self,
        id: DieselUlid,
        transaction_client: &Client,
    ) -> Result<Vec<InternalRelation>> {
        match self {
            CreateRequest::Project(req, _) => {
                let mut relations = Vec::new();
                for ir in req.relations.iter().filter_map(filter_relations) {
                    relations.push(InternalRelation::from_api(ir, id, transaction_client).await?)
                }
                Ok(relations)
            }
            CreateRequest::Collection(req) => {
                let mut relations = Vec::new();
                for ir in req.relations.iter().filter_map(filter_relations) {
                    relations.push(InternalRelation::from_api(ir, id, transaction_client).await?)
                }
                Ok(relations)
            }
            CreateRequest::Dataset(req) => {
                let mut relations = Vec::new();
                for ir in req.relations.iter().filter_map(filter_relations) {
                    relations.push(InternalRelation::from_api(ir, id, transaction_client).await?)
                }
                Ok(relations)
            }
            CreateRequest::Object(req) => {
                let mut relations = Vec::new();
                for ir in req.relations.iter().filter_map(filter_relations) {
                    relations.push(InternalRelation::from_api(ir, id, transaction_client).await?)
                }
                Ok(relations)
            }
        }
    }
    pub fn get_external_relations(&self) -> Vec<ExternalRelation> {
        match self {
            CreateRequest::Project(request, _) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(RelationEnum::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),
            CreateRequest::Collection(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(RelationEnum::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),
            CreateRequest::Dataset(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(RelationEnum::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),

            CreateRequest::Object(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(RelationEnum::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),
        }
    }

    pub fn get_data_class(&self) -> i32 {
        match self {
            CreateRequest::Project(request, _) => request.data_class,
            CreateRequest::Collection(request) => request.data_class,
            CreateRequest::Dataset(request) => request.data_class,
            CreateRequest::Object(request) => request.data_class,
        }
    }

    pub fn get_hashes(&self) -> Option<Vec<Hash>> {
        match self {
            CreateRequest::Object(request) => Some(request.hashes.clone()),
            _ => None,
        }
    }

    pub fn get_type(&self) -> ObjectType {
        match self {
            CreateRequest::Project(..) => ObjectType::PROJECT,
            CreateRequest::Collection(_) => ObjectType::COLLECTION,
            CreateRequest::Dataset(_) => ObjectType::DATASET,
            CreateRequest::Object(_) => ObjectType::OBJECT,
        }
    }

    pub fn is_dynamic(&self) -> bool {
        !matches!(self, CreateRequest::Object(_))
    }

    pub fn get_status(&self) -> ObjectStatus {
        match self {
            CreateRequest::Project(..)
            | CreateRequest::Collection(_)
            | CreateRequest::Dataset(_) => ObjectStatus::AVAILABLE,
            CreateRequest::Object(_) => ObjectStatus::INITIALIZING,
        }
    }

    pub fn get_parent(&self) -> Option<Parent> {
        match self {
            CreateRequest::Project(..) => None,
            CreateRequest::Collection(request) => Some(request.parent.clone()?.into()),
            CreateRequest::Dataset(request) => Some(request.parent.clone()?.into()),
            CreateRequest::Object(request) => Some(request.parent.clone()?.into()),
        }
    }

    pub async fn get_endpoint(
        &self,
        cache: Arc<Cache>,
        db_client: &Client,
    ) -> Result<DashMap<DieselUlid, EndpointInfo, RandomState>> {
        match self {
            CreateRequest::Project(req, default_endpoint) => {
                if req.preferred_endpoint.is_empty() {
                    Ok(DashMap::from_iter([(
                        DieselUlid::from_str(default_endpoint)?,
                        EndpointInfo {
                            replication: ReplicationType::FullSync, // at least one full sync endpoint is needed for projects
                            status: None,
                        },
                    )]))
                } else {
                    // Checks if endpoints exists
                    let endpoint_id = DieselUlid::from_str(&req.preferred_endpoint)?;
                    match Endpoint::get(endpoint_id, db_client).await? {
                        Some(_) => Ok(DashMap::from_iter([(
                            endpoint_id,
                            EndpointInfo {
                                replication: ReplicationType::FullSync,
                                status: None,
                            }, // at least one full sync endpoint is needed for projects
                        )])),
                        None => Err(anyhow!("Endpoint does not exist")),
                    }
                }
            }
            CreateRequest::Object(_) => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent found"))?;
                let parent = cache
                    .get_object(&parent.get_id()?)
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                Ok(DashMap::from_iter(
                    parent
                        .object
                        .endpoints
                        .0
                        .into_iter()
                        .filter_map(|(id, info)| {
                            if let ReplicationType::PartialSync(inheritance) = info.replication {
                                if inheritance {
                                    Some((
                                        id,
                                        EndpointInfo {
                                            replication: info.replication, // If not cloned, this could deadlock, right?
                                            status: Some(ReplicationStatus::Waiting),
                                        },
                                    ))
                                } else {
                                    None
                                }
                            } else {
                                Some((
                                    id,
                                    EndpointInfo {
                                        replication: info.replication, // If not cloned, this could deadlock, right?
                                        status: Some(ReplicationStatus::Waiting),
                                    },
                                ))
                            }
                        }),
                ))
            }
            _ => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent found"))?;
                let parent = cache
                    .get_object(&parent.get_id()?)
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                let filtered_endpoints =
                    parent
                        .object
                        .endpoints
                        .0
                        .into_iter()
                        .filter_map(|(id, info)| {
                            if let ReplicationType::PartialSync(inheritance) = info.replication {
                                if inheritance {
                                    Some((
                                        id,
                                        EndpointInfo {
                                            replication: info.replication, // If not cloned, this could deadlock, right?
                                            status: None,
                                        },
                                    ))
                                } else {
                                    None
                                }
                            } else {
                                Some((
                                    id,
                                    EndpointInfo {
                                        replication: info.replication, // If not cloned, this could deadlock, right?
                                        status: None,
                                    },
                                ))
                            }
                        });
                Ok(DashMap::from_iter(filtered_endpoints))
            }
        }
    }

    fn get_title(&self) -> String {
        match self {
            CreateRequest::Project(req, _) => req.title.clone(),
            CreateRequest::Collection(req) => req.title.clone(),
            CreateRequest::Dataset(req) => req.title.clone(),
            CreateRequest::Object(req) => req.title.clone(),
        }
    }

    fn get_authors(&self) -> Result<Json<Vec<Author>>> {
        Ok(match self {
            CreateRequest::Project(req, _) => Json(
                req.authors
                    .iter()
                    .map(|author| author.clone().try_into())
                    .collect::<Result<Vec<Author>>>()?,
            ),
            CreateRequest::Collection(req) => Json(
                req.authors
                    .iter()
                    .map(|author| author.clone().try_into())
                    .collect::<Result<Vec<Author>>>()?,
            ),
            CreateRequest::Dataset(req) => Json(
                req.authors
                    .iter()
                    .map(|author| author.clone().try_into())
                    .collect::<Result<Vec<Author>>>()?,
            ),
            CreateRequest::Object(req) => Json(
                req.authors
                    .iter()
                    .map(|author| author.clone().try_into())
                    .collect::<Result<Vec<Author>>>()?,
            ),
        })
    }
    pub async fn as_new_db_object(
        &self,
        user_id: DieselUlid,
        client: &Client,
        cache: Arc<Cache>,
    ) -> Result<Object> {
        // Conversions
        let id = DieselUlid::generate();
        let key_values: KeyValues = self.get_key_values().try_into()?;
        let external_relations: ExternalRelations = (&self.get_external_relations()).try_into()?;
        let data_class = self.get_data_class().try_into()?;
        let hashes: Hashes = match self.get_hashes() {
            Some(h) => h.try_into()?,
            None => Hashes(Vec::new()),
        };
        let (metadata_license, data_license) = self.get_licenses(client).await?;
        let endpoints = self.get_endpoint(cache, client).await?;
        let name = self.get_name()?;

        Ok(Object {
            id,
            revision_number: 0,
            name,
            title: self.get_title(),
            description: self.get_description(),
            created_at: None,
            content_len: 0,
            created_by: user_id,
            authors: self.get_authors()?,
            count: 1,
            key_values: Json(key_values),
            object_status: self.get_status(),
            data_class,
            object_type: self.get_type(),
            external_relations: Json(external_relations),
            hashes: Json(hashes),
            dynamic: self.is_dynamic(),
            endpoints: Json(endpoints),
            metadata_license,
            data_license,
        })
    }

    pub async fn get_licenses(&self, client: &Client) -> Result<(String, String)> {
        // Either retrieve license from request or parent
        match &self {
            // Projects must specify licenses
            CreateRequest::Project(req, _) => {
                let data_tag = if req.default_data_license_tag.is_empty() {
                    ALL_RIGHTS_RESERVED.to_string()
                } else {
                    req.default_data_license_tag.clone()
                };
                let meta_tag = if req.metadata_license_tag.is_empty() {
                    ALL_RIGHTS_RESERVED.to_string()
                } else {
                    req.metadata_license_tag.clone()
                };
                if License::get(data_tag.clone(), client).await?.is_some()
                    && License::get(meta_tag.clone(), client).await?.is_some()
                {
                    Ok((meta_tag.to_string(), data_tag.to_string()))
                } else {
                    Err(anyhow!("Invalid license: License not found"))
                }
            }
            CreateRequest::Collection(req) => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent specified"))?
                    .get_id()?;
                let data_tag = req.default_data_license_tag.clone();
                let meta_tag = req.metadata_license_tag.clone();
                CreateRequest::check_license(data_tag, meta_tag, parent, client).await
            }
            CreateRequest::Dataset(req) => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent specified"))?
                    .get_id()?;
                let data_tag = req.default_data_license_tag.clone();
                let meta_tag = req.metadata_license_tag.clone();
                CreateRequest::check_license(data_tag, meta_tag, parent, client).await
            }
            CreateRequest::Object(req) => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent specified"))?
                    .get_id()?;
                let data_tag = if req.data_license_tag.is_empty() {
                    None
                } else {
                    Some(req.data_license_tag.clone())
                };
                let meta_tag = if req.metadata_license_tag.is_empty() {
                    None
                } else {
                    Some(req.metadata_license_tag.clone())
                };
                CreateRequest::check_license(data_tag, meta_tag, parent, client).await
            }
        }
    }

    // Checks if licenses are specified
    // and if not tries to retrieve parent licenses
    async fn check_license(
        data: Option<String>,
        meta: Option<String>,
        parent: DieselUlid,
        client: &Client,
    ) -> Result<(String, String)> {
        match (meta, data) {
            // both not specified -> get parent licenses
            (None, None) => {
                let parent = Object::get(parent, client)
                    .await?
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                Ok((parent.metadata_license, parent.data_license))
            }
            (None, Some(data)) => {
                let parent = Object::get(parent, client)
                    .await?
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                if License::get(data.clone(), client).await?.is_some() {
                    Ok((parent.metadata_license, data.to_string()))
                } else {
                    Err(anyhow!("License invalid"))
                }
            }
            (Some(meta), None) => {
                let parent = Object::get(parent, client)
                    .await?
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                if License::get(meta.clone(), client).await?.is_some() {
                    Ok((meta.to_string(), parent.data_license))
                } else {
                    Err(anyhow!("License invalid"))
                }
            }
            (Some(meta), Some(data)) => {
                if License::get(data.clone(), client).await?.is_some()
                    && License::get(meta.clone(), client).await?.is_some()
                {
                    Ok((meta.to_string(), data.to_string()))
                } else {
                    Err(anyhow!("Licenses invalid"))
                }
            }
        }
    }
}

fn filter_relations(
    relation: &Relation,
) -> Option<&aruna_rust_api::api::storage::models::v2::InternalRelation> {
    match &relation.relation {
        Some(RelationEnum::Internal(internal)) => Some(internal),
        _ => None,
    }
}
