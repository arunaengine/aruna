use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::license_dsl::License;
use crate::database::dsls::object_dsl::{
    Author, EndpointInfo, Hashes, KeyValue as DBKeyValue, KeyValueVariant, KeyValues, Object,
    ObjectWithRelations,
};
use crate::database::enums::{DataClass, ObjectType, ReplicationStatus};
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::services::v2::update_object_request::Parent as UpdateParent;
use aruna_rust_api::api::storage::services::v2::{
    SetObjectHashesRequest, UpdateCollectionAuthorsRequest, UpdateCollectionDataClassRequest,
    UpdateCollectionDescriptionRequest, UpdateCollectionKeyValuesRequest,
    UpdateCollectionLicensesRequest, UpdateCollectionNameRequest, UpdateCollectionTitleRequest,
    UpdateDatasetAuthorsRequest, UpdateDatasetDataClassRequest, UpdateDatasetDescriptionRequest,
    UpdateDatasetKeyValuesRequest, UpdateDatasetLicensesRequest, UpdateDatasetNameRequest,
    UpdateDatasetTitleRequest, UpdateObjectAuthorsRequest, UpdateObjectRequest,
    UpdateObjectTitleRequest, UpdateProjectAuthorsRequest, UpdateProjectDataClassRequest,
    UpdateProjectDescriptionRequest, UpdateProjectKeyValuesRequest, UpdateProjectLicensesRequest,
    UpdateProjectNameRequest, UpdateProjectTitleRequest,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;
use tokio_postgres::Client;

use super::create_request_types::{PROJECT_SCHEMA, S3_KEY_SCHEMA};

pub struct UpdateObject(pub UpdateObjectRequest);

pub enum DataClassUpdate {
    Project(UpdateProjectDataClassRequest),
    Collection(UpdateCollectionDataClassRequest),
    Dataset(UpdateDatasetDataClassRequest),
}
pub enum DescriptionUpdate {
    Project(UpdateProjectDescriptionRequest),
    Collection(UpdateCollectionDescriptionRequest),
    Dataset(UpdateDatasetDescriptionRequest),
}
pub enum KeyValueUpdate {
    Project(UpdateProjectKeyValuesRequest),
    Collection(UpdateCollectionKeyValuesRequest),
    Dataset(UpdateDatasetKeyValuesRequest),
}

pub enum NameUpdate {
    Project(UpdateProjectNameRequest),
    Collection(UpdateCollectionNameRequest),
    Dataset(UpdateDatasetNameRequest),
}

pub enum LicenseUpdate {
    Project(UpdateProjectLicensesRequest),
    Collection(UpdateCollectionLicensesRequest),
    Dataset(UpdateDatasetLicensesRequest),
}

pub enum UpdateTitle {
    Project(UpdateProjectTitleRequest),
    Collection(UpdateCollectionTitleRequest),
    Dataset(UpdateDatasetTitleRequest),
    Object(UpdateObjectTitleRequest),
}

pub enum UpdateAuthor {
    Project(UpdateProjectAuthorsRequest),
    Collection(UpdateCollectionAuthorsRequest),
    Dataset(UpdateDatasetAuthorsRequest),
    Object(UpdateObjectAuthorsRequest),
}

pub struct SetHashes(pub SetObjectHashesRequest);

impl DataClassUpdate {
    pub fn get_dataclass(&self) -> Result<DataClass> {
        let class = match self {
            DataClassUpdate::Project(req) => req.data_class.try_into()?,
            DataClassUpdate::Collection(req) => req.data_class.try_into()?,
            DataClassUpdate::Dataset(req) => req.data_class.try_into()?,
        };
        Ok(class)
    }
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match self {
            DataClassUpdate::Project(req) => DieselUlid::from_str(&req.project_id)?,
            DataClassUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            DataClassUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        };
        Ok(id)
    }
}

impl NameUpdate {
    pub fn get_name(&self) -> Result<String> {
        match self {
            NameUpdate::Project(req) => {
                let name = req.name.to_string();
                if !PROJECT_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid project name"))
                } else {
                    Ok(name)
                }
            }
            NameUpdate::Collection(req) => {
                let name = req.name.to_string();
                if !S3_KEY_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid collection name"))
                } else {
                    Ok(name)
                }
            }
            NameUpdate::Dataset(req) => {
                let name = req.name.to_string();
                if !S3_KEY_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid dataset name"))
                } else {
                    Ok(name)
                }
            }
        }
    }
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match self {
            NameUpdate::Project(req) => DieselUlid::from_str(&req.project_id)?,
            NameUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            NameUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        };
        Ok(id)
    }
}

impl DescriptionUpdate {
    pub fn get_description(&self) -> String {
        match self {
            DescriptionUpdate::Project(req) => req.description.to_string(),
            DescriptionUpdate::Collection(req) => req.description.to_string(),
            DescriptionUpdate::Dataset(req) => req.description.to_string(),
        }
    }
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match self {
            DescriptionUpdate::Project(req) => DieselUlid::from_str(&req.project_id)?,
            DescriptionUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            DescriptionUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        };
        Ok(id)
    }
}

impl KeyValueUpdate {
    pub fn get_keyvals(&self) -> Result<(KeyValues, KeyValues)> {
        let (add, rm) = match self {
            KeyValueUpdate::Project(req) => (&req.add_key_values, &req.remove_key_values),
            KeyValueUpdate::Collection(req) => (&req.add_key_values, &req.remove_key_values),
            KeyValueUpdate::Dataset(req) => (&req.add_key_values, &req.remove_key_values),
        };
        Ok((add.try_into()?, rm.try_into()?))
    }
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match self {
            KeyValueUpdate::Project(req) => DieselUlid::from_str(&req.project_id)?,
            KeyValueUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            KeyValueUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        };
        Ok(id)
    }
}

impl LicenseUpdate {
    pub fn get_id(&self) -> Result<DieselUlid> {
        match self {
            LicenseUpdate::Project(req) => DieselUlid::from_str(&req.project_id),
            LicenseUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id),
            LicenseUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id),
        }
        .map_err(|_| anyhow!("Invalid id"))
    }

    pub async fn get_licenses(&self, old: &Object, client: &Client) -> Result<(String, String)> {
        match self {
            LicenseUpdate::Project(req) => {
                let meta = if req.metadata_license_tag.is_empty() {
                    old.metadata_license.clone()
                } else if License::get(req.metadata_license_tag.clone(), client)
                    .await?
                    .is_some()
                {
                    req.metadata_license_tag.clone()
                } else {
                    return Err(anyhow!("Invalid metadata license tag"));
                };
                let data = if req.default_data_license_tag.is_empty() {
                    old.data_license.clone()
                } else if License::get(req.default_data_license_tag.clone(), client)
                    .await?
                    .is_some()
                {
                    req.default_data_license_tag.clone()
                } else {
                    return Err(anyhow!("Invalid default data license tag"));
                };

                Ok((meta, data))
            }
            LicenseUpdate::Collection(req) => {
                let meta = if req.metadata_license_tag.is_empty() {
                    old.metadata_license.clone()
                } else if License::get(req.metadata_license_tag.clone(), client)
                    .await?
                    .is_some()
                {
                    req.metadata_license_tag.clone()
                } else {
                    return Err(anyhow!("Invalid metadata license tag"));
                };
                let data = if req.default_data_license_tag.is_empty() {
                    old.data_license.clone()
                } else if License::get(req.default_data_license_tag.clone(), client)
                    .await?
                    .is_some()
                {
                    req.default_data_license_tag.clone()
                } else {
                    return Err(anyhow!("Invalid default data license tag"));
                };

                Ok((meta, data))
            }
            LicenseUpdate::Dataset(req) => {
                let meta = if req.metadata_license_tag.is_empty() {
                    old.metadata_license.clone()
                } else if License::get(req.metadata_license_tag.clone(), client)
                    .await?
                    .is_some()
                {
                    req.metadata_license_tag.clone()
                } else {
                    return Err(anyhow!("Invalid metadata license tag"));
                };
                let data = if req.default_data_license_tag.is_empty() {
                    old.data_license.clone()
                } else if License::get(req.default_data_license_tag.clone(), client)
                    .await?
                    .is_some()
                {
                    req.default_data_license_tag.clone()
                } else {
                    return Err(anyhow!("Invalid default data license tag"));
                };

                Ok((meta, data))
            }
        }
    }
}

impl UpdateObject {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.object_id)?)
    }
    pub fn get_description(&self, old: Object) -> String {
        match self.0.description.clone() {
            Some(d) => d,
            None => old.description,
        }
    }
    pub fn get_add_keyvals(&self, old: Object) -> Result<KeyValues> {
        Ok(match self.0.add_key_values.is_empty() {
            false => {
                let kv = &self.0.add_key_values;
                if kv
                    .iter()
                    .map(|kv| kv.variant)
                    .contains(&(KeyValueVariant::HOOK_STATUS as i32))
                {
                    return Err(anyhow!("Cannot add hook status without triggering hooks"));
                }
                kv.try_into()?
            }
            true => old.key_values.0,
        })
    }
    pub fn get_hashes(&self, old: Object) -> Result<Hashes> {
        Ok(match self.0.hashes.is_empty() {
            false => self.0.hashes.clone().try_into()?,
            true => old.hashes.0,
        })
    }
    pub fn get_all_kvs(&self, old: Object) -> Result<KeyValues> {
        let rm_kv = &self.0.remove_key_values;
        let add_kv = &self.0.add_key_values;
        let remove_kv: KeyValues = rm_kv.try_into()?;
        let mut add_kv: KeyValues = add_kv.try_into()?;
        if add_kv
            .0
            .iter()
            .map(|kv| &kv.variant)
            .contains(&KeyValueVariant::HOOK_STATUS)
        {
            return Err(anyhow!("Cannot add hook status without triggering hooks"));
        }
        let mut key_values: Vec<DBKeyValue> = old
            .key_values
            .0
             .0
            .into_iter()
            .filter(|l| !remove_kv.0.contains(l))
            .collect();
        key_values.append(&mut add_kv.0);
        Ok(KeyValues(key_values))
    }
    pub fn get_name(&self, old: Object) -> String {
        match self.0.name.clone() {
            Some(n) => n,
            None => old.name,
        }
    }
    pub fn get_dataclass(
        &self,
        old: Object,
        is_service_account: bool,
    ) -> Result<(DataClass, bool)> {
        let new = self.0.data_class;
        let old_converted: i32 = old.data_class.clone().into();
        if is_service_account {
            return if (new != 0) || (new != 4) {
                Err(anyhow!("Workspaces need to be claimed for status updates"))
            } else {
                Ok((DataClass::WORKSPACE, false))
            };
        } else if new == 0 {
            return Ok((old.data_class, false));
        } else if old_converted < new {
            // True because triggers new revision
            return Ok((new.try_into()?, true));
        }
        // False because triggers no new revision
        Ok((new.try_into()?, false))
    }
    pub fn get_endpoints(
        &self,
        old: Object,
        new_revision: bool,
    ) -> Result<DashMap<DieselUlid, EndpointInfo, RandomState>> {
        let eps = if new_revision {
            let new = old.endpoints.0.into_iter().map(|(id, info)| {
                (
                    id,
                    EndpointInfo {
                        replication: info.replication, // If not cloned, this could deadlock, right?
                        status: Some(ReplicationStatus::Waiting),
                    },
                )
            });
            DashMap::from_iter(new)
        } else {
            old.endpoints.0
        };
        Ok(eps)
    }
    pub fn add_parent_relation(
        object_id: DieselUlid,
        parent: UpdateParent,
        name: String,
    ) -> Result<InternalRelation> {
        let (parent_id, parent_type) = match parent {
            UpdateParent::ProjectId(p) => (DieselUlid::from_str(&p)?, ObjectType::PROJECT),
            UpdateParent::CollectionId(p) => (DieselUlid::from_str(&p)?, ObjectType::COLLECTION),
            UpdateParent::DatasetId(p) => (DieselUlid::from_str(&p)?, ObjectType::DATASET),
        };
        Ok(InternalRelation {
            id: DieselUlid::generate(),
            origin_pid: parent_id,
            origin_type: parent_type,
            relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
            target_pid: object_id,
            target_type: ObjectType::OBJECT,
            target_name: name,
        })
    }

    pub fn get_parent_id(parent: &UpdateParent) -> Result<DieselUlid> {
        match parent {
            UpdateParent::ProjectId(id) => Ok(DieselUlid::from_str(id)?),
            UpdateParent::CollectionId(id) => Ok(DieselUlid::from_str(id)?),
            UpdateParent::DatasetId(id) => Ok(DieselUlid::from_str(id)?),
        }
    }

    pub fn parent_relation_changed(
        old_object: &ObjectWithRelations,
        parent: &UpdateParent,
    ) -> Result<bool> {
        let requested_parent_id = Self::get_parent_id(parent)?;
        Ok(!old_object
            .inbound_belongs_to
            .0
            .contains_key(&requested_parent_id))
    }

    pub fn get_parent_affected_ids(
        old_object: &ObjectWithRelations,
        parent: &UpdateParent,
    ) -> Result<Vec<DieselUlid>> {
        let requested_parent_id = Self::get_parent_id(parent)?;
        let mut affected = vec![requested_parent_id];

        affected.extend(old_object.inbound_belongs_to.0.iter().filter_map(|entry| {
            let parent_id = *entry.key();
            (parent_id != requested_parent_id).then_some(parent_id)
        }));

        Ok(affected)
    }

    pub fn get_all_relations(
        old_object: ObjectWithRelations,
        new_object: Object,
    ) -> Vec<(InternalRelation, (DieselUlid, DieselUlid))> {
        let mut relations: Vec<(InternalRelation, (DieselUlid, DieselUlid))> = old_object
            .inbound_belongs_to
            .0
            .into_iter()
            .map(|ir| {
                (
                    InternalRelation {
                        id: DieselUlid::generate(),
                        origin_pid: ir.1.origin_pid,
                        origin_type: ir.1.origin_type,
                        relation_name: ir.1.relation_name,
                        target_pid: new_object.id,
                        target_type: new_object.object_type,
                        target_name: new_object.name.clone(),
                    },
                    (ir.1.id, ir.1.origin_pid),
                )
            })
            .collect();
        relations.append(
            &mut old_object
                .inbound
                .0
                .into_iter()
                .map(|ir| {
                    (
                        InternalRelation {
                            id: DieselUlid::generate(),
                            origin_pid: ir.1.origin_pid,
                            origin_type: ir.1.origin_type,
                            relation_name: ir.1.relation_name,
                            target_pid: new_object.id,
                            target_type: new_object.object_type,
                            target_name: new_object.name.clone(),
                        },
                        (ir.1.id, ir.1.origin_pid),
                    )
                })
                .collect(),
        );
        relations.append(
            &mut old_object
                .outbound_belongs_to
                .0
                .into_iter()
                .map(|ir| {
                    (
                        InternalRelation {
                            id: DieselUlid::generate(),
                            origin_pid: new_object.id,
                            origin_type: new_object.object_type,
                            relation_name: ir.1.relation_name,
                            target_pid: ir.1.target_pid,
                            target_type: ir.1.target_type,
                            target_name: ir.1.target_name,
                        },
                        (ir.1.id, ir.1.target_pid),
                    )
                })
                .collect(),
        );
        relations.append(
            &mut old_object
                .outbound
                .0
                .into_iter()
                .map(|ir| {
                    (
                        InternalRelation {
                            id: DieselUlid::generate(),
                            origin_pid: new_object.id,
                            origin_type: new_object.object_type,
                            relation_name: ir.1.relation_name,
                            target_pid: ir.1.target_pid,
                            target_type: ir.1.target_type,
                            target_name: ir.1.target_name,
                        },
                        (ir.1.id, ir.1.target_pid),
                    )
                })
                .collect(),
        );
        relations
    }
    pub async fn get_license(&self, old: &Object, client: &Client) -> Result<(String, String)> {
        let metadata_license = match self.0.metadata_license_tag.clone() {
            None => old.metadata_license.clone(),
            Some(meta) => {
                if UpdateObject::check_license(&meta, client).await? {
                    meta
                } else {
                    return Err(anyhow!("License does not exist"));
                }
            }
        };
        let data_license = match self.0.data_license_tag.clone() {
            None => old.data_license.clone(),
            Some(data) => {
                if UpdateObject::check_license(&data, client).await? {
                    data
                } else {
                    return Err(anyhow!("License does not exist"));
                }
            }
        };
        Ok((metadata_license, data_license))
    }

    async fn check_license(tag: &str, client: &Client) -> Result<bool> {
        Ok(License::get(tag.to_string(), client).await?.is_some())
    }
}

impl UpdateTitle {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(match self {
            UpdateTitle::Project(req) => &req.project_id,
            UpdateTitle::Collection(req) => &req.collection_id,
            UpdateTitle::Dataset(req) => &req.dataset_id,
            UpdateTitle::Object(req) => &req.object_id,
        })?)
    }

    pub fn get_title(&self) -> String {
        match self {
            UpdateTitle::Project(req) => req.title.to_string(),
            UpdateTitle::Collection(req) => req.title.to_string(),
            UpdateTitle::Dataset(req) => req.title.to_string(),
            UpdateTitle::Object(req) => req.title.to_string(),
        }
    }
}
impl UpdateAuthor {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(match self {
            UpdateAuthor::Project(req) => &req.project_id,
            UpdateAuthor::Collection(req) => &req.collection_id,
            UpdateAuthor::Dataset(req) => &req.dataset_id,
            UpdateAuthor::Object(req) => &req.object_id,
        })?)
    }

    pub fn get_authors(&self) -> Result<(Vec<Author>, Vec<Author>)> {
        match self {
            UpdateAuthor::Project(req) => {
                let remove = req
                    .clone()
                    .remove_authors
                    .into_iter()
                    .map(|a| a.try_into())
                    .collect::<Result<Vec<Author>>>()?;
                let add = req
                    .clone()
                    .add_authors
                    .into_iter()
                    .map(|a| a.try_into())
                    .collect::<Result<Vec<Author>>>()?;
                Ok((remove, add))
            }
            UpdateAuthor::Collection(req) => {
                let remove = req
                    .clone()
                    .remove_authors
                    .into_iter()
                    .map(|a| a.try_into())
                    .collect::<Result<Vec<Author>>>()?;
                let add = req
                    .clone()
                    .add_authors
                    .into_iter()
                    .map(|a| a.try_into())
                    .collect::<Result<Vec<Author>>>()?;
                Ok((remove, add))
            }
            UpdateAuthor::Dataset(req) => {
                let remove = req
                    .clone()
                    .remove_authors
                    .into_iter()
                    .map(|a| a.try_into())
                    .collect::<Result<Vec<Author>>>()?;
                let add = req
                    .clone()
                    .add_authors
                    .into_iter()
                    .map(|a| a.try_into())
                    .collect::<Result<Vec<Author>>>()?;
                Ok((remove, add))
            }
            UpdateAuthor::Object(req) => {
                let remove = req
                    .clone()
                    .remove_authors
                    .into_iter()
                    .map(|a| a.try_into())
                    .collect::<Result<Vec<Author>>>()?;
                let add = req
                    .clone()
                    .add_authors
                    .into_iter()
                    .map(|a| a.try_into())
                    .collect::<Result<Vec<Author>>>()?;
                Ok((remove, add))
            }
        }
    }
}

impl SetHashes {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.object_id)?)
    }
    pub fn get_hashes(&self) -> Result<Hashes> {
        self.0.hashes.clone().try_into()
    }
}
