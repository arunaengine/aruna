use crate::caching::cache::Cache;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::{DbPermissionLevel, ObjectType};
use crate::grpc::users::UserServiceImpl;
use crate::{auth::structs::Context, database::enums::ObjectMapping};
use anyhow::{anyhow, Result as AnyhowResult};
use aruna_rust_api::api::storage::models::v2::relation::Relation as RelationEnum;
use aruna_rust_api::api::storage::models::v2::{
    generic_resource, Collection, DataEndpoint, Dataset, Object, Project, Relation,
    ReplicationStatus, User,
};
use base64::{engine::general_purpose, Engine};
use chrono::{DateTime, NaiveDateTime};
use diesel_ulid::DieselUlid;
use rusty_ulid::DecodingError;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::{Result, Status};
use xxhash_rust::xxh3::xxh3_128;

use super::conversions::relations::from_db_internal_relation;

pub fn from_prost_time(prost_stamp: Option<prost_wkt_types::Timestamp>) -> Option<NaiveDateTime> {
    DateTime::from_timestamp(prost_stamp.as_ref()?.seconds, prost_stamp?.nanos as u32)
        .map(|e| e.naive_utc())
}

pub fn type_name_of<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

pub trait IntoGenericInner<T> {
    fn into_inner(self) -> Result<T, Status>;
}

impl IntoGenericInner<Project> for generic_resource::Resource {
    fn into_inner(self) -> Result<Project, Status> {
        match self {
            generic_resource::Resource::Project(project) => Ok(project),
            _ => Err(Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Collection> for generic_resource::Resource {
    fn into_inner(self) -> Result<Collection> {
        match self {
            generic_resource::Resource::Collection(collection) => Ok(collection),
            _ => Err(Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Dataset> for generic_resource::Resource {
    fn into_inner(self) -> Result<Dataset> {
        match self {
            generic_resource::Resource::Dataset(dataset) => Ok(dataset),
            _ => Err(Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Object> for generic_resource::Resource {
    fn into_inner(self) -> Result<Object> {
        match self {
            generic_resource::Resource::Object(object) => Ok(object),
            _ => Err(Status::invalid_argument("Invalid conversion")),
        }
    }
}

impl IntoGenericInner<DbPermissionLevel> for ObjectMapping<DbPermissionLevel> {
    fn into_inner(self) -> Result<DbPermissionLevel, Status> {
        match self {
            ObjectMapping::PROJECT(perm) => Ok(perm),
            ObjectMapping::COLLECTION(perm) => Ok(perm),
            ObjectMapping::DATASET(perm) => Ok(perm),
            ObjectMapping::OBJECT(perm) => Ok(perm),
        }
    }
}

impl UserServiceImpl {
    pub async fn match_ctx(
        &self,
        tuple: (Option<DieselUlid>, Context),
        token: String,
    ) -> Result<DieselUlid> {
        match tuple {
            (Some(id), ctx) => {
                tonic_auth!(
                    self.authorizer.check_permissions(&token, vec![ctx]).await,
                    "Unauthorized"
                );
                Ok(id)
            }

            (None, ctx) => {
                let user_id = tonic_auth!(
                    self.authorizer.check_permissions(&token, vec![ctx]).await,
                    "Unauthorized"
                );
                Ok(user_id)
            }
        }
    }
}

pub fn generic_object_without_rules(object: ObjectWithRelations) -> generic_resource::Resource {
    let object_with_relations = object;
    let (inbound, outbound) = (
        object_with_relations
            .inbound
            .0
            .iter()
            .chain(object_with_relations.inbound_belongs_to.0.iter())
            .map(|r| r.clone())
            .collect::<Vec<InternalRelation>>(),
        object_with_relations
            .outbound
            .0
            .iter()
            .chain(object_with_relations.outbound_belongs_to.0.iter())
            .map(|r| r.clone())
            .collect::<Vec<InternalRelation>>(),
    );

    let mut inbound = inbound
        .into_iter()
        .map(|r| from_db_internal_relation(r, true))
        .collect::<Vec<_>>();

    let mut outbound = outbound
        .into_iter()
        .map(|r| from_db_internal_relation(r, false))
        .collect::<Vec<_>>();
    let mut relations: Vec<Relation> = object_with_relations
        .object
        .external_relations
        .0
         .0
        .into_iter()
        .map(|r| Relation {
            relation: Some(RelationEnum::External(r.1.into())),
        })
        .collect();
    relations.append(&mut inbound);
    relations.append(&mut outbound);
    let stats = None;

    match object_with_relations.object.object_type {
        ObjectType::PROJECT => generic_resource::Resource::Project(Project {
            id: object_with_relations.object.id.to_string(),
            name: object_with_relations.object.name,
            title: object_with_relations.object.title.to_string(),
            description: object_with_relations.object.description,
            created_at: object_with_relations.object.created_at.map(|t| t.into()),
            stats,
            created_by: object_with_relations.object.created_by.to_string(),
            authors: object_with_relations
                .object
                .authors
                .0
                .into_iter()
                .map(|a| a.into())
                .collect(),
            data_class: object_with_relations.object.data_class.into(),
            dynamic: object_with_relations.object.dynamic,
            key_values: object_with_relations.object.key_values.0.into(),
            status: object_with_relations.object.object_status.into(),
            relations,
            endpoints: object_with_relations
                .object
                .endpoints
                .0
                .iter()
                .map(|e| DataEndpoint {
                    id: e.key().to_string(),
                    variant: Some(e.replication.into()),
                    status: None,
                })
                .collect(),
            metadata_license_tag: object_with_relations.object.metadata_license,
            default_data_license_tag: object_with_relations.object.data_license,
            rule_bindings: Vec::new(),
        }),
        ObjectType::COLLECTION => generic_resource::Resource::Collection(Collection {
            id: object_with_relations.object.id.to_string(),
            name: object_with_relations.object.name,
            title: object_with_relations.object.title.to_string(),
            description: object_with_relations.object.description,
            created_at: object_with_relations.object.created_at.map(|t| t.into()),
            stats,
            created_by: object_with_relations.object.created_by.to_string(),
            authors: object_with_relations
                .object
                .authors
                .0
                .into_iter()
                .map(|a| a.into())
                .collect(),
            data_class: object_with_relations.object.data_class.into(),
            dynamic: object_with_relations.object.dynamic,
            key_values: object_with_relations.object.key_values.0.into(),
            status: object_with_relations.object.object_status.into(),
            relations,
            endpoints: object_with_relations
                .object
                .endpoints
                .0
                .iter()
                .map(|e| DataEndpoint {
                    id: e.key().to_string(),
                    variant: Some(e.replication.into()),
                    status: None,
                })
                .collect(),
            metadata_license_tag: object_with_relations.object.metadata_license,
            default_data_license_tag: object_with_relations.object.data_license,
            rule_bindings: Vec::new(),
        }),
        ObjectType::DATASET => generic_resource::Resource::Dataset(Dataset {
            id: object_with_relations.object.id.to_string(),
            name: object_with_relations.object.name,
            title: object_with_relations.object.title.to_string(),
            description: object_with_relations.object.description,
            created_at: object_with_relations.object.created_at.map(|t| t.into()),
            stats,
            created_by: object_with_relations.object.created_by.to_string(),
            authors: object_with_relations
                .object
                .authors
                .0
                .into_iter()
                .map(|a| a.into())
                .collect(),
            data_class: object_with_relations.object.data_class.into(),
            dynamic: object_with_relations.object.dynamic,
            key_values: object_with_relations.object.key_values.0.into(),
            status: object_with_relations.object.object_status.into(),
            relations,
            endpoints: object_with_relations
                .object
                .endpoints
                .0
                .iter()
                .map(|e| DataEndpoint {
                    id: e.key().to_string(),
                    variant: Some(e.replication.into()),
                    status: None,
                })
                .collect(),
            metadata_license_tag: object_with_relations.object.metadata_license,
            default_data_license_tag: object_with_relations.object.data_license,
            rule_bindings: Vec::new(),
        }),
        ObjectType::OBJECT => generic_resource::Resource::Object(Object {
            id: object_with_relations.object.id.to_string(),
            content_len: object_with_relations.object.content_len,
            name: object_with_relations.object.name,
            title: object_with_relations.object.title.to_string(),
            description: object_with_relations.object.description,
            created_at: object_with_relations.object.created_at.map(|t| t.into()),
            created_by: object_with_relations.object.created_by.to_string(),
            authors: object_with_relations
                .object
                .authors
                .0
                .into_iter()
                .map(|a| a.into())
                .collect(),
            data_class: object_with_relations.object.data_class.into(),
            dynamic: object_with_relations.object.dynamic,
            hashes: object_with_relations.object.hashes.0.into(),
            key_values: object_with_relations.object.key_values.0.into(),
            status: object_with_relations.object.object_status.into(),
            relations,
            endpoints: object_with_relations
                .object
                .endpoints
                .0
                .iter()
                .map(|e| DataEndpoint {
                    id: e.key().to_string(),
                    variant: Some(e.replication.into()),
                    status: e.status.map(|s| ReplicationStatus::from(s) as i32),
                })
                .collect(),
            metadata_license_tag: object_with_relations.object.metadata_license,
            data_license_tag: object_with_relations.object.data_license,
            rule_bindings: Vec::new(),
        }),
    }
}

///ToDo: Rust Doc
pub fn checksum_resource(gen_res: generic_resource::Resource) -> anyhow::Result<String> {
    match gen_res {
        generic_resource::Resource::Project(mut proj) => {
            proj.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(
                    xxh3_128(&bincode::serde::encode_to_vec(
                        &proj,
                        bincode::config::legacy(),
                    )?)
                    .to_be_bytes(),
                )
                .to_string())
        }
        generic_resource::Resource::Collection(mut col) => {
            col.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(
                    xxh3_128(&bincode::serde::encode_to_vec(
                        &col,
                        bincode::config::legacy(),
                    )?)
                    .to_be_bytes(),
                )
                .to_string())
        }
        generic_resource::Resource::Dataset(mut ds) => {
            ds.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(
                    xxh3_128(&bincode::serde::encode_to_vec(
                        &ds,
                        bincode::config::legacy(),
                    )?)
                    .to_be_bytes(),
                )
                .to_string())
        }
        generic_resource::Resource::Object(obj) => Ok(general_purpose::STANDARD_NO_PAD
            .encode(
                xxh3_128(&bincode::serde::encode_to_vec(
                    &obj,
                    bincode::config::legacy(),
                )?)
                .to_be_bytes(),
            )
            .to_string()),
    }
}

///ToDo: Rust Doc
pub fn checksum_user(user: &User) -> anyhow::Result<String> {
    Ok(general_purpose::STANDARD_NO_PAD
        .encode(
            xxh3_128(&bincode::serde::encode_to_vec(
                &user.attributes,
                bincode::config::legacy(),
            )?)
            .to_be_bytes(),
        )
        .to_string())
}

pub fn get_id_and_ctx(ids: Vec<String>) -> Result<(Vec<DieselUlid>, Vec<Context>)> {
    let zipped = tonic_invalid!(
        ids.iter()
            .map(
                |id| -> std::result::Result<(DieselUlid, Context), DecodingError> {
                    let id = DieselUlid::from_str(id)?;
                    let ctx = Context::res_ctx(id, DbPermissionLevel::READ, true);
                    Ok((id, ctx))
                },
            )
            .collect::<std::result::Result<Vec<(DieselUlid, Context)>, DecodingError>>(),
        "Invalid ids"
    );
    let (ids, ctxs) = zipped.into_iter().unzip();
    Ok((ids, ctxs))
}

pub fn query(cache: &Arc<Cache>, id: &DieselUlid) -> Result<generic_resource::Resource, Status> {
    cache
        .get_protobuf_object(id)
        .ok_or_else(|| Status::not_found("Resource not found"))
}

pub fn get_token_from_md(md: &MetadataMap) -> AnyhowResult<String> {
    let token_string = md
        .get("Authorization")
        .ok_or(anyhow!("Metadata token not found"))?
        .to_str()?;

    let split = token_string.split(' ').collect::<Vec<_>>();

    if split.len() != 2 {
        log::debug!(
            "Could not get token from metadata: Wrong length, expected: 2, got: {:?}",
            split.len()
        );
        return Err(anyhow!("Authorization flow error"));
    }

    if split[0] != "Bearer" {
        log::debug!(
            "Could not get token from metadata: Invalid token type, expected: Bearer, got: {:?}",
            split[0]
        );

        return Err(anyhow!("Authorization flow error"));
    }

    if split[1].is_empty() {
        log::debug!(
            "Could not get token from metadata: Invalid token length, expected: >0, got: {:?}",
            split[1].len()
        );

        return Err(anyhow!("Authorization flow error"));
    }

    Ok(split[1].to_string())
}
