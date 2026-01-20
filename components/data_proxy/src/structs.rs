use anyhow::Result;
use anyhow::{anyhow, bail};
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation, DataClass, InternalRelationVariant, KeyValue, Object as GrpcObject,
    PermissionLevel, Project, RelationDirection, Status, User as GrpcUser,
};
use aruna_rust_api::api::storage::models::v2::{Collection, DataEndpoint};
use aruna_rust_api::api::storage::models::v2::{Dataset, ResourceVariant};
use aruna_rust_api::api::storage::models::v2::{Hash, Permission};
use aruna_rust_api::api::storage::models::v2::{Hashalgorithm, Pubkey};
use aruna_rust_api::api::storage::services::v2::create_collection_request;
use aruna_rust_api::api::storage::services::v2::create_dataset_request;
use aruna_rust_api::api::storage::services::v2::create_object_request;
use aruna_rust_api::api::storage::services::v2::CreateCollectionRequest;
use aruna_rust_api::api::storage::services::v2::CreateDatasetRequest;
use aruna_rust_api::api::storage::services::v2::CreateObjectRequest;
use aruna_rust_api::api::storage::services::v2::CreateProjectRequest;
use aruna_rust_api::api::storage::services::v2::UpdateObjectRequest;
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel_ulid::DieselUlid;
use http::{HeaderValue, Method};
use pithos_lib::helpers::structs::{EncryptionKey, FileContext};
use rand::RngCore;
use s3s::dto::{CORSRule as S3SCORSRule, GetBucketCorsOutput};
use s3s::dto::{CreateBucketInput, Part};
use s3s::{s3_error, S3Error};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use tracing::{debug, error};

use crate::auth::auth::AuthHandler;
use crate::helpers::IntoOption;
use crate::s3_frontend::utils::checksum::{base64_to_hex, hex_to_base64};
use crate::CONFIG;

/* ----- Constants ----- */
pub const ALL_RIGHTS_RESERVED: &str = "AllRightsReserved";

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Bundle {
    pub id: DieselUlid,
    pub owner_access_key: String,
    pub ids: Vec<DieselUlid>,
    pub expires_at: Option<DateTime<Utc>>,
    pub once: bool,
}
impl Bundle {
    pub fn is_default(&self) -> bool {
        self.id == DieselUlid::default()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum DbPermissionLevel {
    Deny,
    None,
    Read,
    Append,
    Write,
    Admin,
}

impl Display for DbPermissionLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            DbPermissionLevel::Deny => "deny",
            DbPermissionLevel::None => "none",
            DbPermissionLevel::Read => "read",
            DbPermissionLevel::Append => "append",
            DbPermissionLevel::Write => "write",
            DbPermissionLevel::Admin => "admin",
        };
        write!(f, "{str}")
    }
}

impl From<&Method> for DbPermissionLevel {
    #[tracing::instrument(level = "trace", skip(method))]
    fn from(method: &Method) -> Self {
        match *method {
            Method::GET | Method::OPTIONS | Method::HEAD => DbPermissionLevel::Read,
            Method::POST | Method::PUT => DbPermissionLevel::Append,
            Method::DELETE => DbPermissionLevel::Write,
            _ => DbPermissionLevel::Admin,
        }
    }
}

impl From<PermissionLevel> for DbPermissionLevel {
    #[tracing::instrument(level = "trace", skip(level))]
    fn from(level: PermissionLevel) -> Self {
        match level {
            PermissionLevel::Unspecified | PermissionLevel::None => DbPermissionLevel::None,
            PermissionLevel::Read => DbPermissionLevel::Read,
            PermissionLevel::Append => DbPermissionLevel::Append,
            PermissionLevel::Write => DbPermissionLevel::Write,
            PermissionLevel::Admin => DbPermissionLevel::Admin,
        }
    }
}

//impl From<Option<Permission

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum ObjectType {
    Project,
    Collection,
    Dataset,
    #[default]
    Object,
}

impl TryFrom<ResourceVariant> for ObjectType {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: ResourceVariant) -> Result<Self> {
        match value {
            ResourceVariant::Project => Ok(ObjectType::Project),
            ResourceVariant::Collection => Ok(ObjectType::Collection),
            ResourceVariant::Dataset => Ok(ObjectType::Dataset),
            ResourceVariant::Object => Ok(ObjectType::Object),
            _ => Err(anyhow!("Invalid resource variant")),
        }
    }
}

impl TryFrom<&ResourceVariant> for ObjectType {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: &ResourceVariant) -> Result<Self> {
        match *value {
            ResourceVariant::Project => Ok(ObjectType::Project),
            ResourceVariant::Collection => Ok(ObjectType::Collection),
            ResourceVariant::Dataset => Ok(ObjectType::Dataset),
            ResourceVariant::Object => Ok(ObjectType::Object),
            _ => Err(anyhow!("Invalid resource variant")),
        }
    }
}

#[derive(Hash, Debug, Clone, PartialEq, Serialize, Deserialize, Eq, PartialOrd, Ord)]
pub enum TypedRelation {
    Project(DieselUlid),
    Collection(DieselUlid),
    Dataset(DieselUlid),
    Object(DieselUlid),
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub enum FileFormat {
    #[default]
    Raw,
    RawEncrypted([u8; 32]),
    RawCompressed,
    RawEncryptedCompressed([u8; 32]),
    Pithos([u8; 32]),
    Pseudo, // Zero-byte single part uploads
}

impl FileFormat {
    pub fn from_bools(
        allow_pithos: bool,
        allow_encryption: bool,
        allow_compression: bool,
    ) -> Result<Self> {
        let mut enc_key = [0u8; 32];
        rand::rng().fill_bytes(&mut enc_key);

        Ok(match (allow_pithos, allow_encryption, allow_compression) {
            (true, _, _) => FileFormat::Pithos(enc_key),
            (false, true, false) => FileFormat::RawEncrypted(enc_key),
            (false, false, true) => FileFormat::RawCompressed,
            (false, true, true) => FileFormat::RawEncryptedCompressed(enc_key),
            _ => FileFormat::Raw,
        })
    }

    pub fn is_encrypted(&self) -> bool {
        matches!(
            self,
            FileFormat::RawEncrypted(_)
                | FileFormat::RawEncryptedCompressed(_)
                | FileFormat::Pithos(_)
        )
    }

    pub fn is_compressed(&self) -> bool {
        matches!(
            self,
            FileFormat::RawCompressed
                | FileFormat::RawEncryptedCompressed(_)
                | FileFormat::Pithos(_)
        )
    }

    pub fn get_encryption_key(&self) -> Option<[u8; 32]> {
        match self {
            FileFormat::RawEncrypted(key)
            | FileFormat::RawEncryptedCompressed(key)
            | FileFormat::Pithos(key) => Some(*key),
            _ => None,
        }
    }

    pub fn get_encryption_key_as_enc_key(&self) -> Vec<EncryptionKey> {
        let Some(key) = self.get_encryption_key() else {
            return vec![];
        };
        vec![EncryptionKey::new_same_key(key)]
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectLocation {
    pub id: DieselUlid, // Not the object_id
    pub bucket: String,
    pub key: String,
    pub upload_id: Option<String>,
    pub file_format: FileFormat,
    pub raw_content_len: i64,
    pub disk_content_len: i64,
    pub disk_hash: Option<String>,
    pub is_temporary: bool,
    pub ref_count: u32, // Number of objects that reference this location
}

impl ObjectLocation {
    pub fn get_encryption_key(&self) -> Option<[u8; 32]> {
        self.file_format.get_encryption_key()
    }

    pub fn count_blocks(&self) -> usize {
        match &self.file_format {
            FileFormat::Raw => {
                if (self.raw_content_len as usize).is_multiple_of(65536) {
                    self.raw_content_len as usize / 65536
                } else {
                    (self.raw_content_len as usize / 65536) + 1
                }
            }
            FileFormat::RawCompressed => {
                if (self.disk_content_len as usize).is_multiple_of(65536) {
                    self.disk_content_len as usize / 65536
                } else {
                    (self.disk_content_len as usize / 65536) + 1
                }
            }
            FileFormat::RawEncrypted(_) =>
            // 109 is the overhead for a new key in footer
            {
                if (self.raw_content_len as usize + 109).is_multiple_of(65536 + 28) {
                    (self.raw_content_len as usize + 109) / (65536 + 28)
                } else {
                    ((self.raw_content_len as usize + 109) / (65536 + 28)) + 1
                }
            }
            FileFormat::RawEncryptedCompressed(_) | FileFormat::Pithos(_) => {
                // 109 is the overhead for a new key in footer
                if (self.disk_content_len as usize + 109).is_multiple_of(65536 + 28) {
                    (self.disk_content_len as usize + 109) / (65536 + 28)
                } else {
                    ((self.disk_content_len as usize + 109) / (65536 + 28)) + 1
                }
            }
            FileFormat::Pseudo => 0,
        }
    }

    pub fn is_pithos(&self) -> bool {
        matches!(self.file_format, FileFormat::Pithos(_))
    }

    pub fn is_compressed(&self) -> bool {
        matches!(
            self.file_format,
            FileFormat::RawCompressed
                | FileFormat::RawEncryptedCompressed(_)
                | FileFormat::Pithos(_)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct LocationBinding {
    pub object_id: DieselUlid,
    pub location_id: DieselUlid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum VersionVariant {
    HasVersion(DieselUlid), // a(latest) -> has_version -> b (DieselUlid)
    IsVersion(DieselUlid),  // a(not_latest) -> is_version -> b (DieselUlid / latest)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Object {
    pub id: DieselUlid,
    pub name: String,
    pub title: String,
    pub key_values: Vec<KeyValue>,
    pub object_status: Status,
    pub data_class: DataClass,
    pub object_type: ObjectType,
    pub hashes: HashMap<String, String>,
    pub metadata_license: String,
    pub data_license: String,
    pub dynamic: bool,
    pub children: Option<HashSet<TypedRelation>>,
    pub parents: Option<HashSet<TypedRelation>>,
    pub versions: Option<HashSet<VersionVariant>>,
    pub synced: bool,
    pub endpoints: Vec<Endpoint>, // TODO
    pub created_at: Option<NaiveDateTime>,
    pub created_by: Option<DieselUlid>,
}

impl Object {
    pub fn initialize_now(
        name: String,
        object_type: ObjectType,
        parent: Option<TypedRelation>,
    ) -> Self {
        let object_status = if object_type == ObjectType::Object {
            Status::Initializing
        } else {
            Status::Available
        };

        Self {
            id: DieselUlid::generate(),
            name,
            object_status,
            data_class: DataClass::Private,
            object_type,
            hashes: HashMap::default(),
            metadata_license: ALL_RIGHTS_RESERVED.to_string(),
            data_license: ALL_RIGHTS_RESERVED.to_string(),
            dynamic: true,
            parents: parent.map(|p| HashSet::from([p])),
            // object with gRPC response
            created_at: Some(Utc::now().naive_utc()),
            ..Default::default()
        }
    }

    pub fn get_file_context(
        &self,
        location: Option<ObjectLocation>,
        expected_size: Option<i64>,
    ) -> Result<FileContext> {
        let location_size = expected_size
            .or_else(|| location.as_ref().map(|l| l.raw_content_len))
            .unwrap_or_default();

        // TODO: Maybe hashes
        Ok(FileContext {
            idx: 0,
            file_path: self.name.clone(),
            compressed_size: location_size as u64,
            decompressed_size: location
                .as_ref()
                .map(|l| l.raw_content_len as u64)
                .unwrap_or_default(),
            compression: location
                .as_ref()
                .map(|l| l.is_compressed())
                .unwrap_or_default(),
            encryption_key: location
                .as_ref()
                .map(|l| {
                    if let Some(key) = l.get_encryption_key() {
                        EncryptionKey::new_same_key(key)
                    } else {
                        EncryptionKey::default()
                    }
                })
                .unwrap_or_default(),
            recipients_pubkeys: vec![CONFIG.proxy.get_public_key_x25519()?],
            ..Default::default()
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct User {
    pub user_id: DieselUlid,
    pub personal_permissions: HashMap<DieselUlid, DbPermissionLevel>,
    pub tokens: HashMap<DieselUlid, HashMap<DieselUlid, DbPermissionLevel>>,
    pub is_service_account: bool,
    pub attributes: HashMap<String, String>,
}

impl User {
    pub fn compare_permissions(
        &self,        // GRPC user
        other: &User, // database user
    ) -> (
        Vec<(String, HashMap<DieselUlid, DbPermissionLevel>)>,
        Vec<String>,
    ) {
        let mut to_update = vec![];
        let mut to_delete = vec![];
        for (k, v) in &self.personal_permissions {
            if let Some(ov) = other.personal_permissions.get(k) {
                if v != ov {
                    to_update.push((self.user_id.to_string(), self.personal_permissions.clone()));
                }
            } else {
                to_update.push((self.user_id.to_string(), self.personal_permissions.clone()));
            }
        }
        for (k, v) in &self.tokens {
            if let Some(ov) = other.tokens.get(k) {
                if v != ov {
                    to_update.push((k.to_string(), v.clone()));
                }
            }
        }

        for k in other.tokens.keys() {
            if !self.tokens.contains_key(k) {
                to_delete.push(k.to_string());
            }
        }

        (to_update, to_delete)
    }
}

pub fn perm_convert(perms: Vec<Permission>) -> HashMap<DieselUlid, DbPermissionLevel> {
    perms
        .iter()
        .filter_map(|p| {
            if let Some(id) = &p.resource_id {
                match id {
                    ResourceId::ProjectId(id)
                    | ResourceId::CollectionId(id)
                    | ResourceId::DatasetId(id)
                    | ResourceId::ObjectId(id) => Some((
                        DieselUlid::from_str(id).ok()?,
                        DbPermissionLevel::from(p.permission_level()),
                    )),
                }
            } else {
                None
            }
        })
        .collect()
}

// TODO: FIX this
impl TryFrom<GrpcUser> for User {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GrpcUser) -> Result<Self> {
        let attributes = if let Some(attr) = &value.attributes {
            let mut map = attr
                .data_proxy_attributes
                .iter()
                .map(|e| (e.attribute_name.to_string(), e.attribute_value.to_string()))
                .collect::<HashMap<String, String>>();
            map.extend(
                attr.custom_attributes
                    .iter()
                    .map(|e| (e.attribute_name.to_string(), e.attribute_value.to_string())),
            );
            map
        } else {
            HashMap::default()
        };

        let is_service_account = value
            .attributes
            .as_ref()
            .map(|e| e.service_account)
            .unwrap_or_default();

        let personal_permissions = perm_convert(
            value
                .attributes
                .as_ref()
                .map(|e| e.personal_permissions.clone())
                .unwrap_or_default(),
        );

        Ok(User {
            user_id: DieselUlid::from_str(&value.id)?,
            personal_permissions: personal_permissions.clone(),
            tokens: value
                .attributes
                .as_ref()
                .ok_or_else(|| {
                    error!("No tokens found");
                    anyhow!("No tokens found")
                })?
                .tokens
                .iter()
                .map(|t| {
                    Ok((
                        DieselUlid::from_str(&t.id)?,
                        if let Some(perm) = &t.permission {
                            match &perm.resource_id {
                                Some(ResourceId::ProjectId(id))
                                | Some(ResourceId::CollectionId(id))
                                | Some(ResourceId::DatasetId(id))
                                | Some(ResourceId::ObjectId(id)) => HashMap::from([(
                                    DieselUlid::from_str(id)?,
                                    DbPermissionLevel::from(perm.permission_level()),
                                )]),
                                None => personal_permissions.clone(),
                            }
                        } else {
                            Err(anyhow!("No permission found"))?
                        },
                    ))
                })
                .collect::<Result<HashMap<DieselUlid, HashMap<DieselUlid, DbPermissionLevel>>>>()?,
            attributes,
            is_service_account,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessKeyPermissions {
    pub access_key: String,
    pub user_id: DieselUlid,
    pub secret: String,
    pub is_service_account: bool,
    pub permissions: HashMap<DieselUlid, DbPermissionLevel>,
}

// TODO! ENDPOINTS
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Endpoint {
    pub id: DieselUlid,
    pub variant: SyncVariant,
    pub status: Option<SyncStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SyncVariant {
    FullSync,
    PartialSync(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SyncStatus {
    Waiting,
    Running,
    Finished,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartETag {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PubKey {
    pub id: i16,
    pub key: String,
    pub is_proxy: bool,
}

impl From<Pubkey> for PubKey {
    #[tracing::instrument(level = "trace", skip(value))]
    fn from(value: Pubkey) -> Self {
        Self {
            id: value.id as i16,
            key: value.key,
            is_proxy: value.location.contains("proxy"),
        }
    }
}

impl TypedRelation {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_id(&self) -> DieselUlid {
        match self {
            TypedRelation::Project(i)
            | TypedRelation::Collection(i)
            | TypedRelation::Dataset(i)
            | TypedRelation::Object(i) => *i,
        }
    }
}

impl TryFrom<&Relation> for TypedRelation {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: &Relation) -> Result<Self> {
        match value {
            Relation::External(_) => {
                error!("invalid external rel");
                Err(anyhow!("Invalid External rel"))
            }
            Relation::Internal(int) => {
                let resource_id = DieselUlid::from_str(&int.resource_id)?;

                match int.resource_variant() {
                    ResourceVariant::Unspecified => {
                        error!("Invalid target");
                        Err(anyhow!("Invalid target"))
                    }
                    ResourceVariant::Project => Ok(Self::Project(resource_id)),
                    ResourceVariant::Collection => Ok(Self::Collection(resource_id)),
                    ResourceVariant::Dataset => Ok(Self::Dataset(resource_id)),
                    ResourceVariant::Object => Ok(Self::Object(resource_id)),
                }
            }
        }
    }
}
impl TryInto<create_collection_request::Parent> for TypedRelation {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<create_collection_request::Parent> {
        match self {
            TypedRelation::Project(i) => {
                Ok(create_collection_request::Parent::ProjectId(i.to_string()))
            }
            _ => {
                error!("Invalid");
                Err(anyhow!("Invalid"))
            }
        }
    }
}

impl TryInto<create_dataset_request::Parent> for TypedRelation {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<create_dataset_request::Parent> {
        match self {
            TypedRelation::Project(i) => {
                Ok(create_dataset_request::Parent::ProjectId(i.to_string()))
            }
            TypedRelation::Collection(i) => {
                Ok(create_dataset_request::Parent::CollectionId(i.to_string()))
            }
            _ => {
                error!("Invalid");
                Err(anyhow!("Invalid"))
            }
        }
    }
}

impl TryInto<create_object_request::Parent> for TypedRelation {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<create_object_request::Parent> {
        match self {
            TypedRelation::Project(i) => {
                Ok(create_object_request::Parent::ProjectId(i.to_string()))
            }
            TypedRelation::Collection(i) => {
                Ok(create_object_request::Parent::CollectionId(i.to_string()))
            }
            TypedRelation::Dataset(i) => {
                Ok(create_object_request::Parent::DatasetId(i.to_string()))
            }
            _ => {
                error!("Invalid");
                Err(anyhow!("Invalid "))
            }
        }
    }
}

impl TryFrom<Resource> for Object {
    type Error = anyhow::Error;

    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: Resource) -> std::result::Result<Self, Self::Error> {
        match value {
            Resource::Project(p) => Object::try_from(p),
            Resource::Collection(c) => Object::try_from(c),
            Resource::Dataset(d) => Object::try_from(d),
            Resource::Object(o) => Object::try_from(o),
        }
        .map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })
    }
}

impl TryFrom<&DataEndpoint> for Endpoint {
    type Error = anyhow::Error;

    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: &DataEndpoint) -> Result<Self> {
        Ok(Endpoint {
            id: DieselUlid::from_str(&value.id)?,
            variant: match value.variant.as_ref().ok_or_else(|| {
                error!(error = "No endpoint sync variant found");
                anyhow!("No endpoint sync variant found")
            })? {
                aruna_rust_api::api::storage::models::v2::data_endpoint::Variant::FullSync(
                    aruna_rust_api::api::storage::models::v2::FullSync { .. },
                ) => SyncVariant::FullSync,
                aruna_rust_api::api::storage::models::v2::data_endpoint::Variant::PartialSync(
                    inheritance,
                ) => SyncVariant::PartialSync(*inheritance),
            },
            status: match value.status() {
                aruna_rust_api::api::storage::models::v2::ReplicationStatus::Unspecified => None,
                aruna_rust_api::api::storage::models::v2::ReplicationStatus::Waiting => {
                    Some(SyncStatus::Waiting)
                }
                aruna_rust_api::api::storage::models::v2::ReplicationStatus::Running => {
                    Some(SyncStatus::Running)
                }
                aruna_rust_api::api::storage::models::v2::ReplicationStatus::Finished => {
                    Some(SyncStatus::Finished)
                }
                aruna_rust_api::api::storage::models::v2::ReplicationStatus::Error => {
                    Some(SyncStatus::Error)
                }
            },
        })
    }
}

pub fn from_prost_time(prost_stamp: Option<prost_wkt_types::Timestamp>) -> Option<NaiveDateTime> {
    DateTime::from_timestamp(prost_stamp.as_ref()?.seconds, prost_stamp?.nanos as u32)
        .map(|e| e.naive_utc())
}

impl TryFrom<Project> for Object {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: Project) -> Result<Self, Self::Error> {
        let mut inbound = HashSet::default();
        let mut outbound = HashSet::default();
        let mut version = HashSet::default();

        for rel in value.relations.iter() {
            if let Some(rel) = &rel.relation {
                match rel {
                    Relation::Internal(var) => match var.defined_variant() {
                        InternalRelationVariant::BelongsTo => match var.direction() {
                            RelationDirection::Inbound => {
                                inbound.insert(TypedRelation::try_from(rel)?);
                            }
                            RelationDirection::Outbound => {
                                outbound.insert(TypedRelation::try_from(rel)?);
                            }
                            RelationDirection::Unspecified => continue,
                        },
                        InternalRelationVariant::Version => match var.direction() {
                            RelationDirection::Inbound => {
                                version.insert(VersionVariant::HasVersion(DieselUlid::from_str(
                                    &var.resource_id,
                                )?));
                            }
                            RelationDirection::Outbound => {
                                version.insert(VersionVariant::IsVersion(DieselUlid::from_str(
                                    &var.resource_id,
                                )?));
                            }
                            RelationDirection::Unspecified => continue,
                        },
                        _ => continue,
                    },
                    _ => continue,
                }
            }
        }

        let inbounds = inbound.into_option();
        let outbounds = outbound.into_option();
        let versions = version.into_option();

        Ok(Object {
            id: DieselUlid::from_str(&value.id).inspect_err(|&e| {
                error!(error = ?e, msg = e.to_string());
            })?,
            name: value.name.to_string(),
            title: value.title.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::Project,
            hashes: HashMap::default(),
            metadata_license: value.metadata_license_tag,
            data_license: value.default_data_license_tag,
            dynamic: value.dynamic,
            parents: inbounds,
            children: outbounds,
            versions,
            synced: false,
            endpoints: value
                .endpoints
                .iter()
                .map(Endpoint::try_from)
                .collect::<Result<Vec<Endpoint>>>()?,
            created_at: from_prost_time(value.created_at),
            created_by: Some(DieselUlid::from_str(&value.created_by)?),
        })
    }
}

impl TryFrom<Collection> for Object {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: Collection) -> Result<Self, Self::Error> {
        let mut inbound = HashSet::default();
        let mut outbound = HashSet::default();
        let mut version = HashSet::default();

        for rel in value.relations.iter() {
            if let Some(rel) = &rel.relation {
                match rel {
                    Relation::Internal(var) => match var.defined_variant() {
                        InternalRelationVariant::BelongsTo => match var.direction() {
                            RelationDirection::Inbound => {
                                inbound.insert(TypedRelation::try_from(rel)?);
                            }
                            RelationDirection::Outbound => {
                                outbound.insert(TypedRelation::try_from(rel)?);
                            }
                            RelationDirection::Unspecified => continue,
                        },
                        InternalRelationVariant::Version => match var.direction() {
                            RelationDirection::Inbound => {
                                version.insert(VersionVariant::HasVersion(DieselUlid::from_str(
                                    &var.resource_id,
                                )?));
                            }
                            RelationDirection::Outbound => {
                                version.insert(VersionVariant::IsVersion(DieselUlid::from_str(
                                    &var.resource_id,
                                )?));
                            }
                            RelationDirection::Unspecified => continue,
                        },
                        _ => continue,
                    },
                    _ => continue,
                }
            }
        }

        let inbounds = inbound.into_option();
        let outbounds = outbound.into_option();
        let versions = version.into_option();

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            title: value.title.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::Collection,
            hashes: HashMap::default(),
            metadata_license: value.metadata_license_tag,
            data_license: value.default_data_license_tag,
            dynamic: value.dynamic,
            parents: inbounds,
            children: outbounds,
            versions,
            synced: false,
            endpoints: value
                .endpoints
                .iter()
                .map(Endpoint::try_from)
                .collect::<Result<Vec<Endpoint>>>()?,
            created_at: from_prost_time(value.created_at),
            created_by: Some(DieselUlid::from_str(&value.created_by)?),
        })
    }
}

impl TryFrom<Dataset> for Object {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: Dataset) -> Result<Self, Self::Error> {
        let mut inbound = HashSet::default();
        let mut outbound = HashSet::default();
        let mut version = HashSet::default();

        for rel in value.relations.iter() {
            if let Some(rel) = &rel.relation {
                match rel {
                    Relation::Internal(var) => match var.defined_variant() {
                        InternalRelationVariant::BelongsTo => match var.direction() {
                            RelationDirection::Inbound => {
                                inbound.insert(TypedRelation::try_from(rel)?);
                            }
                            RelationDirection::Outbound => {
                                outbound.insert(TypedRelation::try_from(rel)?);
                            }
                            RelationDirection::Unspecified => continue,
                        },
                        InternalRelationVariant::Version => match var.direction() {
                            RelationDirection::Inbound => {
                                version.insert(VersionVariant::HasVersion(DieselUlid::from_str(
                                    &var.resource_id,
                                )?));
                            }
                            RelationDirection::Outbound => {
                                version.insert(VersionVariant::IsVersion(DieselUlid::from_str(
                                    &var.resource_id,
                                )?));
                            }
                            RelationDirection::Unspecified => continue,
                        },
                        _ => continue,
                    },
                    _ => continue,
                }
            }
        }

        let inbounds = inbound.into_option();
        let outbounds = outbound.into_option();
        let versions = version.into_option();

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            title: value.title.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::Dataset,
            hashes: HashMap::default(),
            metadata_license: value.metadata_license_tag,
            data_license: value.default_data_license_tag,
            dynamic: value.dynamic,
            parents: inbounds,
            children: outbounds,
            versions,
            synced: false,
            endpoints: value
                .endpoints
                .iter()
                .map(Endpoint::try_from)
                .collect::<Result<Vec<Endpoint>>>()?,
            created_at: from_prost_time(value.created_at),
            created_by: Some(DieselUlid::from_str(&value.created_by)?),
        })
    }
}

impl TryFrom<GrpcObject> for Object {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GrpcObject) -> Result<Self, Self::Error> {
        let mut inbound = HashSet::default();
        let mut outbound = HashSet::default();
        let mut version = HashSet::default();

        for rel in value.relations.iter() {
            if let Some(rel) = &rel.relation {
                match rel {
                    Relation::Internal(var) => match var.defined_variant() {
                        InternalRelationVariant::BelongsTo => match var.direction() {
                            RelationDirection::Inbound => {
                                inbound.insert(TypedRelation::try_from(rel)?);
                            }
                            RelationDirection::Outbound => {
                                outbound.insert(TypedRelation::try_from(rel)?);
                            }
                            RelationDirection::Unspecified => continue,
                        },
                        InternalRelationVariant::Version => match var.direction() {
                            RelationDirection::Inbound => {
                                version.insert(VersionVariant::HasVersion(DieselUlid::from_str(
                                    &var.resource_id,
                                )?));
                            }
                            RelationDirection::Outbound => {
                                version.insert(VersionVariant::IsVersion(DieselUlid::from_str(
                                    &var.resource_id,
                                )?));
                            }
                            RelationDirection::Unspecified => continue,
                        },
                        _ => continue,
                    },
                    _ => continue,
                }
            }
        }

        let inbounds = inbound.into_option();
        let outbounds = outbound.into_option();
        let versions = version.into_option();

        // Convert hashes to proxy internal format
        let mut hashes = HashMap::new();
        for h in value.hashes.iter() {
            let b64_hash = hex_to_base64(&h.hash)?;
            match Hashalgorithm::try_from(h.alg)? {
                Hashalgorithm::Md5 => {
                    hashes.insert("md5".to_string(), b64_hash);
                }
                Hashalgorithm::Sha256 => {
                    hashes.insert("sha256".to_string(), b64_hash);
                }
                _ => {
                    // Ignore unspecified hashes
                }
            }
        }

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            title: value.title.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::Object,
            hashes,
            metadata_license: value.metadata_license_tag,
            data_license: value.data_license_tag,
            dynamic: value.dynamic,
            parents: inbounds,
            children: outbounds,
            versions,
            synced: false,
            endpoints: value
                .endpoints
                .iter()
                .map(Endpoint::try_from)
                .collect::<Result<Vec<Endpoint>>>()?,
            created_at: from_prost_time(value.created_at),
            created_by: Some(DieselUlid::from_str(&value.created_by)?),
        })
    }
}

impl From<Object> for CreateProjectRequest {
    #[tracing::instrument(level = "trace", skip(value))]
    fn from(value: Object) -> Self {
        CreateProjectRequest {
            name: value.name,
            description: "".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: value.data_class.into(),
            preferred_endpoint: "".to_string(), // Gets endpoint id from grpc_query_handler::create_project()
            metadata_license_tag: value.metadata_license,
            default_data_license_tag: value.data_license,
            ..Default::default()
        }
    }
}

impl From<Object> for CreateCollectionRequest {
    #[tracing::instrument(level = "trace", skip(value))]
    fn from(value: Object) -> Self {
        CreateCollectionRequest {
            name: value.name,
            title: value.title,
            description: "".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: value.data_class.into(),
            parent: value
                .parents
                .and_then(|x| x.iter().next().map(|y| y.clone().try_into().ok()))
                .flatten(),
            metadata_license_tag: Some(value.metadata_license),
            default_data_license_tag: Some(value.data_license),
            authors: vec![],
        }
    }
}

impl From<Object> for CreateDatasetRequest {
    #[tracing::instrument(level = "trace", skip(value))]
    fn from(value: Object) -> Self {
        CreateDatasetRequest {
            name: value.name,
            description: "".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: value.data_class.into(),
            parent: value
                .parents
                .and_then(|x| x.iter().next().map(|y| y.clone().try_into().ok()))
                .flatten(),
            metadata_license_tag: Some(value.metadata_license),
            default_data_license_tag: Some(value.data_license),
            title: value.title,
            authors: vec![],
        }
    }
}

impl From<CreateBucketInput> for Object {
    #[tracing::instrument(level = "trace", skip(value))]
    fn from(value: CreateBucketInput) -> Self {
        Object {
            id: DieselUlid::generate(),
            name: value.bucket,
            object_status: Status::Available,
            data_class: DataClass::Private,
            object_type: ObjectType::Project,
            metadata_license: ALL_RIGHTS_RESERVED.to_string(), // Default for now
            data_license: ALL_RIGHTS_RESERVED.to_string(),     // Default for now
            endpoints: vec![],
            created_at: Some(Utc::now().naive_utc()), // Now for default
            ..Default::default()
        }
    }
}

impl From<Object> for CreateObjectRequest {
    #[tracing::instrument(level = "trace", skip(value))]
    fn from(value: Object) -> Self {
        CreateObjectRequest {
            name: value.name,
            title: value.title,
            description: "".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: value.data_class.into(),
            parent: value
                .parents
                .and_then(|x| x.iter().next().map(|y| y.clone().try_into().ok()))
                .flatten(),
            hashes: vec![],
            metadata_license_tag: value.metadata_license,
            data_license_tag: value.data_license,
            authors: vec![],
        }
    }
}

impl From<Object> for UpdateObjectRequest {
    #[tracing::instrument(level = "trace", skip(value))]
    fn from(value: Object) -> Self {
        UpdateObjectRequest {
            object_id: value.id.to_string(),
            name: None,
            description: None,
            add_key_values: vec![],
            remove_key_values: vec![],
            data_class: value.data_class as i32,
            hashes: vec![],
            force_revision: false,
            parent: None,
            metadata_license_tag: Some(value.metadata_license),
            data_license_tag: Some(value.data_license),
        }
    }
}

impl Object {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_hashes(&self) -> Vec<Hash> {
        self.hashes
            .iter()
            .map(|(k, v)| {
                let alg = if k == "md5" {
                    Hashalgorithm::Md5 as i32
                } else if k == "sha256" {
                    Hashalgorithm::Sha256 as i32
                } else {
                    Hashalgorithm::Unspecified as i32
                };

                Hash {
                    alg,
                    hash: v.to_string(),
                }
            })
            .collect()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_api_safe_hashes(&self) -> Result<Vec<Hash>> {
        self.hashes
            .iter()
            .filter(|(k, _)| *k == "md5" || *k == "sha256")
            .map(|(k, v)| {
                let alg = if k == "md5" {
                    Hashalgorithm::Md5
                } else {
                    Hashalgorithm::Sha256
                };

                Ok(Hash {
                    alg: alg as i32,
                    hash: base64_to_hex(v)?,
                })
            })
            .collect()
    }

    #[tracing::instrument(level = "trace", skip(self, ep_id))]
    pub fn is_partial_sync(&self, ep_id: &DieselUlid) -> bool {
        self.endpoints
            .iter()
            .find(|ep| &ep.id == ep_id)
            .map(|ep| matches!(ep.variant, SyncVariant::PartialSync(_)))
            .unwrap_or(false)
    }

    #[tracing::instrument(level = "trace", skip(self, ep_id))]
    pub fn fail_partial_sync(&self, ep_id: &DieselUlid) -> Result<(), S3Error> {
        if self.is_partial_sync(ep_id) {
            error!("Rejecting request: Object partial synced");
            return Err(s3_error!(InvalidObjectState, "Object partial synced"));
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn project_get_headers(
        &self,
        request_method: &Method,
        header: &http::HeaderMap<HeaderValue>,
    ) -> Option<HashMap<String, String>> {
        if self.object_type != ObjectType::Project {
            error!("Invalid object type");
            return None;
        }

        let request_origin = header.get(hyper::header::ORIGIN)?.to_str().ok()?;

        let request_headers = header
            .get(hyper::header::ACCESS_CONTROL_REQUEST_HEADERS)
            .map(|x| {
                x.to_str()
                    .unwrap_or("")
                    .split(',')
                    .map(|e| e.trim().to_string())
                    .collect::<Vec<String>>()
            });

        let key = self
            .key_values
            .iter()
            .find_map(|e| {
                if e.key == "app.aruna-storage.org/cors" {
                    match serde_json::from_str::<CORSConfiguration>(&e.value) {
                        Ok(config) => Some(config.into_headers(
                            request_origin.to_string(),
                            request_method.to_string(),
                            request_headers.clone(),
                        )),
                        _ => {
                            error!("Invalid cors config");
                            None
                        }
                    }
                } else {
                    debug!("No cors config");
                    None
                }
            })
            .flatten();

        key
    }
}

#[derive(Clone, Debug, Default)]
pub enum ResourceState {
    Found {
        object: Object,
    },
    Missing {
        name: String,
        variant: ResourceVariant,
    },
    #[default]
    None,
}

impl ResourceState {
    pub fn is_missing(&self) -> bool {
        matches!(self, ResourceState::Missing { .. })
    }

    pub fn _is_missing_or_none(&self) -> bool {
        matches!(self, ResourceState::Missing { .. } | ResourceState::None)
    }

    pub fn new_found(object: Object) -> Self {
        Self::Found { object }
    }

    pub fn new_missing(name: String, variant: ResourceVariant) -> Self {
        Self::Missing { name, variant }
    }

    pub fn as_ref(&self) -> Option<&Object> {
        match self {
            ResourceState::Found { object } => Some(object),
            _ => None,
        }
    }

    pub fn _as_name(&self) -> Option<String> {
        match self {
            ResourceState::Missing { name, .. } => Some(name.clone()),
            ResourceState::Found { object } => Some(object.name.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NewOrExistingObject {
    Missing(Object),
    Existing(Object),
    None,
}

#[derive(Debug, Clone)]
pub struct ResourceStates {
    objects: [ResourceState; 4],
}

impl Default for ResourceStates {
    fn default() -> Self {
        Self {
            objects: [
                ResourceState::None,
                ResourceState::None,
                ResourceState::None,
                ResourceState::None,
            ],
        }
    }
}

impl ResourceStates {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn validate(&self, allow_create_project: bool) -> Result<()> {
        let project = if allow_create_project && self.objects[0].is_missing() {
            false
        } else {
            self.objects[0].is_missing()
        };

        match (
            project,
            self.objects[1].is_missing(),
            self.objects[2].is_missing(),
            self.objects[3].is_missing(),
        ) {
            (false, true, true, true)
            | (false, true, false, true)
            | (false, false, true, true)
            | (false, false, false, true)
            | (false, false, false, false) => {}
            _ => {
                bail!("Invalid resource state")
            }
        }
        Ok(())
    }

    pub fn disallow_missing(&self) -> Result<(), S3Error> {
        if self.objects.iter().any(|x| x.is_missing()) {
            return Err(s3_error!(NoSuchKey, "Resource not found"));
        }
        Ok(())
    }

    pub fn get_missing_collection(&self, parent: Option<TypedRelation>) -> Option<Object> {
        if let ResourceState::Missing { name, variant } = &self.objects[1] {
            Some(Object::initialize_now(
                name.clone(),
                variant.try_into().ok()?,
                parent,
            ))
        } else {
            None
        }
    }

    pub fn get_missing_dataset(&self, parent: Option<TypedRelation>) -> Option<Object> {
        if let ResourceState::Missing { name, variant } = &self.objects[2] {
            Some(Object::initialize_now(
                name.clone(),
                variant.try_into().ok()?,
                parent,
            ))
        } else {
            None
        }
    }

    pub fn get_missing_object(&self, parent: Option<TypedRelation>) -> Option<Object> {
        if let ResourceState::Missing { name, variant } = &self.objects[3] {
            Some(Object::initialize_now(
                name.clone(),
                variant.try_into().ok()?,
                parent,
            ))
        } else {
            None
        }
    }

    pub fn set_project(&mut self, project: Object) {
        self.objects[0] = ResourceState::new_found(project);
    }

    pub fn set_collection(&mut self, collection: Object) {
        self.objects[1] = ResourceState::new_found(collection);
    }

    pub fn set_dataset(&mut self, dataset: Object) {
        self.objects[2] = ResourceState::new_found(dataset);
    }

    pub fn set_object(&mut self, object: Object) {
        self.objects[3] = ResourceState::new_found(object);
    }

    pub fn get_project(&self) -> Option<&Object> {
        self.objects[0].as_ref()
    }

    pub fn get_collection(&self) -> Option<&Object> {
        self.objects[1].as_ref()
    }

    pub fn get_dataset(&self) -> Option<&Object> {
        self.objects[2].as_ref()
    }

    pub fn get_object(&self) -> Option<&Object> {
        self.objects[3].as_ref()
    }

    pub fn get_project_or_missing(&self) -> Option<Object> {
        match &self.objects[0] {
            x @ ResourceState::Found { .. } => x.as_ref().cloned(),
            ResourceState::Missing { name, .. } => Some(Object::initialize_now(
                name.to_string(),
                ObjectType::Project,
                None,
            )),
            _ => None,
        }
    }

    pub fn get_collection_or_missing(&self) -> Option<Object> {
        match &self.objects[1] {
            x @ ResourceState::Found { .. } => x.as_ref().cloned(),
            ResourceState::Missing { name, .. } => Some(Object::initialize_now(
                name.to_string(),
                ObjectType::Collection,
                None,
            )),
            _ => None,
        }
    }

    pub fn get_dataset_or_missing(&self) -> Option<Object> {
        match &self.objects[2] {
            x @ ResourceState::Found { .. } => x.as_ref().cloned(),
            ResourceState::Missing { name, .. } => Some(Object::initialize_now(
                name.to_string(),
                ObjectType::Dataset,
                None,
            )),
            _ => None,
        }
    }

    pub fn get_object_or_missing(&self) -> Option<Object> {
        match &self.objects[3] {
            x @ ResourceState::Found { .. } => x.as_ref().cloned(),
            ResourceState::Missing { name, .. } => Some(Object::initialize_now(
                name.to_string(),
                ObjectType::Object,
                None,
            )),
            _ => None,
        }
    }

    pub fn set_missing(&mut self, idx: usize, len: usize, name: String) -> Result<()> {
        match (idx, len) {
            (0, _) => self.objects[0] = ResourceState::new_missing(name, ResourceVariant::Project),
            (1, 2) | (2, 3) | (3, 4) => {
                self.objects[3] = ResourceState::new_missing(name, ResourceVariant::Object)
            }
            (1, 3) | (1, 4) => {
                if matches!(self.objects[1], ResourceState::None) {
                    self.objects[1] = ResourceState::new_missing(name, ResourceVariant::Collection)
                }
            }
            (2, 4) => {
                if matches!(self.objects[2], ResourceState::None) {
                    self.objects[2] = ResourceState::new_missing(name, ResourceVariant::Dataset)
                }
            }
            (a, b) => bail!("Invalid index {}, {}", a, b),
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn require_project(&self) -> Result<&Object, S3Error> {
        self.objects[0].as_ref().ok_or_else(|| {
            error!("Project not found");
            s3_error!(NoSuchKey, "Project not found")
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn require_collection(&self) -> Result<&Object, S3Error> {
        self.objects[1].as_ref().ok_or_else(|| {
            error!("Collection not found");
            s3_error!(NoSuchKey, "Collection not found")
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn require_dataset(&self) -> Result<&Object, S3Error> {
        self.objects[2].as_ref().ok_or_else(|| {
            error!("Dataset not found");
            s3_error!(NoSuchKey, "Dataset not found")
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn require_object(&self) -> Result<&Object, S3Error> {
        self.objects[3].as_ref().ok_or_else(|| {
            error!("Object not found");
            s3_error!(NoSuchKey, "Object not found")
        })
    }

    #[tracing::instrument(level = "trace", skip(self, key_info, perm))]
    pub fn check_permissions(
        &self,
        key_info: &AccessKeyPermissions,
        perm: DbPermissionLevel,
        allow_public: bool,
    ) -> Result<(), S3Error> {
        // Check from bottom to top Object -> Dataset -> Collection -> Project
        for res in self.objects.iter().rev() {
            let ResourceState::Found { object } = res else {
                continue;
            };
            if allow_public && object.data_class == DataClass::Public {
                return Ok(());
            }
            if let Some(q_perm) = key_info.permissions.get(&object.id) {
                if q_perm >= &perm {
                    return Ok(());
                }
            }
        }
        error!("Insufficient permissions");
        Err(s3_error!(AccessDenied, "Access Denied"))
    }

    #[tracing::instrument(level = "trace", skip(self, ep_id))]
    pub fn fail_partial_sync(&self, ep_id: &DieselUlid) -> Result<(), S3Error> {
        for res in self.objects.iter().rev() {
            let ResourceState::Found { object } = res else {
                continue;
            };
            object.fail_partial_sync(ep_id)?;
            break; // Only check the last Found object
        }
        Ok(())
    }

    pub fn as_slice(&self) -> [Option<(DieselUlid, String)>; 4] {
        [
            self.objects[0].as_ref().map(|x| (x.id, x.name.clone())),
            self.objects[1].as_ref().map(|x| (x.id, x.name.clone())),
            self.objects[2].as_ref().map(|x| (x.id, x.name.clone())),
            self.objects[3].as_ref().map(|x| (x.id, x.name.clone())),
        ]
    }

    pub fn to_new_or_existing(
        &self,
    ) -> Result<
        (
            NewOrExistingObject,
            NewOrExistingObject,
            NewOrExistingObject,
            NewOrExistingObject,
            [Option<(DieselUlid, String)>; 4],
        ),
        S3Error,
    > {
        let project = self.require_project()?;

        let project_tag = Some((project.id, project.name.clone()));

        let collection = match &self.objects[1] {
            ResourceState::None => NewOrExistingObject::None,
            ResourceState::Found { object } => NewOrExistingObject::Existing(object.clone()),
            ResourceState::Missing { .. } => {
                let collection = self
                    .get_missing_collection(Some(TypedRelation::Project(project.id)))
                    .ok_or_else(|| {
                        error!("Collection not found");
                        s3_error!(NoSuchKey, "Collection not found")
                    })?;
                NewOrExistingObject::Missing(collection)
            }
        };

        let collection_tag = match collection {
            NewOrExistingObject::Existing(ref collection)
            | NewOrExistingObject::Missing(ref collection) => {
                Some((collection.id, collection.name.clone()))
            }
            _ => None,
        };

        let dataset = match &self.objects[2] {
            ResourceState::None => NewOrExistingObject::None,
            ResourceState::Found { object } => NewOrExistingObject::Existing(object.clone()),
            ResourceState::Missing { .. } => {
                let relation = match collection {
                    NewOrExistingObject::Existing(ref collection)
                    | NewOrExistingObject::Missing(ref collection) => {
                        Some(TypedRelation::Collection(collection.id))
                    }
                    _ => Some(TypedRelation::Project(project.id)),
                };
                let dataset = self.get_missing_dataset(relation).ok_or_else(|| {
                    error!("Dataset not found");
                    s3_error!(NoSuchKey, "Dataset not found")
                })?;
                NewOrExistingObject::Missing(dataset)
            }
        };

        let dataset_tag = match dataset {
            NewOrExistingObject::Existing(ref dataset)
            | NewOrExistingObject::Missing(ref dataset) => Some((dataset.id, dataset.name.clone())),
            _ => None,
        };

        let object = match &self.objects[3] {
            ResourceState::None => NewOrExistingObject::None,
            ResourceState::Found { object } => NewOrExistingObject::Existing(object.clone()),
            ResourceState::Missing { .. } => {
                let relation = match dataset {
                    NewOrExistingObject::Existing(ref dataset)
                    | NewOrExistingObject::Missing(ref dataset) => {
                        Some(TypedRelation::Dataset(dataset.id))
                    }
                    _ => match collection {
                        NewOrExistingObject::Existing(ref collection)
                        | NewOrExistingObject::Missing(ref collection) => {
                            Some(TypedRelation::Collection(collection.id))
                        }
                        _ => Some(TypedRelation::Project(project.id)),
                    },
                };
                let object = self.get_missing_object(relation).ok_or_else(|| {
                    error!("Object not found");
                    s3_error!(NoSuchKey, "Object not found")
                })?;
                NewOrExistingObject::Missing(object)
            }
        };

        let object_tag = match object {
            NewOrExistingObject::Existing(ref object)
            | NewOrExistingObject::Missing(ref object) => Some((object.id, object.name.clone())),
            _ => None,
        };

        Ok((
            NewOrExistingObject::Existing(project.clone()),
            collection,
            dataset,
            object,
            [project_tag, collection_tag, dataset_tag, object_tag],
        ))
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ObjectsState {
    Regular {
        states: ResourceStates,
        location: Option<ObjectLocation>,
    },
    Objects {
        root: Object,
        filename: String,
    },
    Bundle {
        bundle: Bundle,
        filename: String,
    },
}

impl Default for ObjectsState {
    fn default() -> Self {
        Self::Regular {
            states: ResourceStates::default(),
            location: None,
        }
    }
}

impl ObjectsState {
    pub fn new_regular(states: ResourceStates, location: Option<ObjectLocation>) -> Self {
        Self::Regular { states, location }
    }
    pub fn new_objects(root: Object, filename: String) -> Self {
        Self::Objects { root, filename }
    }
    pub fn new_bundle(bundle: Bundle, filename: String) -> Self {
        Self::Bundle { bundle, filename }
    }
    pub fn extract_object(&self) -> Result<(Object, Option<ObjectLocation>), S3Error> {
        match self {
            ObjectsState::Regular { states, location } => {
                Ok((states.require_object()?.clone(), location.clone()))
            }
            _ => Err(s3_error!(InvalidRequest, "Object not found")),
        }
    }

    pub fn try_slice(&self) -> Result<[Option<(DieselUlid, String)>; 4], S3Error> {
        match self {
            ObjectsState::Regular { states, .. } => Ok(states.as_slice()),
            _ => Err(s3_error!(InvalidRequest, "Object not found")),
        }
    }

    pub fn require_regular(self) -> Result<(ResourceStates, Option<ObjectLocation>), S3Error> {
        match self {
            ObjectsState::Regular { states, location } => Ok((states, location)),
            _ => Err(s3_error!(InvalidRequest, "Object not found")),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub enum UserState {
    #[default]
    Anonymous,
    Personal {
        user_id: DieselUlid,
    },
    Token {
        access_key: String,
        user_id: DieselUlid,
    },
}

impl UserState {
    pub fn get_user_id(&self) -> Option<DieselUlid> {
        match self {
            UserState::Token { user_id, .. } => Some(*user_id),
            UserState::Personal { user_id } => Some(*user_id),
            _ => None,
        }
    }

    pub fn sign_impersonating_token(&self, auth_handler: Option<&AuthHandler>) -> Option<String> {
        match auth_handler {
            Some(auth_handler) => match self {
                UserState::Token {
                    access_key,
                    user_id,
                } => Some(
                    auth_handler
                        .sign_impersonating_token(user_id.to_string(), Some(access_key))
                        .ok()?,
                ),
                UserState::Personal { user_id } => Some(
                    auth_handler
                        .sign_impersonating_token(user_id.to_string(), None::<String>)
                        .ok()?,
                ),
                _ => None,
            },
            None => None,
        }
    }
}

impl From<Option<AccessKeyPermissions>> for UserState {
    fn from(val: Option<AccessKeyPermissions>) -> Self {
        match val {
            Some(perm) => {
                if perm.access_key == perm.user_id.to_string() {
                    UserState::Personal {
                        user_id: perm.user_id,
                    }
                } else {
                    UserState::Token {
                        access_key: perm.access_key,
                        user_id: perm.user_id,
                    }
                }
            }
            None => UserState::Anonymous,
        }
    }
}

#[derive(Default, Clone)]
pub struct CheckAccessResult {
    pub objects_state: ObjectsState,
    pub user_state: UserState,
    pub headers: Option<HashMap<String, String>>,
}

impl CheckAccessResult {
    #[tracing::instrument(level = "trace", skip(objects_state, user_state, headers))]
    pub fn new(
        objects_state: ObjectsState,
        user_state: UserState,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            objects_state,
            user_state,
            headers,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CORSRule {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_headers: Option<Vec<String>>,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expose_headers: Option<Vec<String>>,
    pub max_age_seconds: i32,
}

impl From<S3SCORSRule> for CORSRule {
    fn from(value: S3SCORSRule) -> Self {
        Self {
            id: value.id,
            allowed_headers: value.allowed_headers,
            allowed_methods: value.allowed_methods,
            allowed_origins: value.allowed_origins,
            expose_headers: value.expose_headers,
            max_age_seconds: value.max_age_seconds.unwrap_or_default(),
        }
    }
}

impl From<CORSRule> for S3SCORSRule {
    fn from(val: CORSRule) -> Self {
        S3SCORSRule {
            id: val.id,
            allowed_headers: val.allowed_headers,
            allowed_methods: val.allowed_methods,
            allowed_origins: val.allowed_origins,
            expose_headers: val.expose_headers,
            max_age_seconds: Some(val.max_age_seconds),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CORSConfiguration(pub Vec<CORSRule>);

impl From<CORSConfiguration> for GetBucketCorsOutput {
    fn from(val: CORSConfiguration) -> Self {
        let cors_rule = val
            .0
            .iter()
            .map(|x| CORSRule::into(x.clone()))
            .collect::<Vec<S3SCORSRule>>();
        if cors_rule.is_empty() {
            GetBucketCorsOutput { cors_rules: None }
        } else {
            GetBucketCorsOutput {
                cors_rules: Some(cors_rule),
            }
        }
    }
}

impl CORSConfiguration {
    #[tracing::instrument]
    pub fn into_headers(
        self,
        origin: String,
        method: String,
        header: Option<Vec<String>>,
    ) -> Option<HashMap<String, String>> {
        for cors_rule in self.0 {
            if (cors_rule.allowed_origins.contains(&origin)
                || cors_rule.allowed_origins.contains(&"*".to_string()))
                && cors_rule.allowed_methods.contains(&method)
            {
                let mut headers = HashMap::new();
                if !cors_rule.allowed_origins.is_empty() {
                    if !cors_rule.allowed_origins.contains(&"*".to_string()) {
                        headers.insert("Vary".to_string(), "Origin".to_string());
                    }
                    // Only 'Access-Control-Allow-Origin' header with single origin is allowed in response
                    headers.insert("Access-Control-Allow-Origin".to_string(), origin);
                }
                if !cors_rule.allowed_methods.is_empty() {
                    headers.insert(
                        "Access-Control-Allow-Methods".to_string(),
                        cors_rule.allowed_methods.join(", "),
                    );
                }
                if let Some(head) = cors_rule.allowed_headers {
                    headers.insert("Access-Control-Allow-Headers".to_string(), head.join(", "));
                }
                if let Some(head) = cors_rule.expose_headers {
                    headers.insert("Access-Control-Expose-Headers".to_string(), head.join(", "));
                }

                if cors_rule.max_age_seconds == 0 {
                    headers.insert(
                        "Access-Control-Max-Age".to_string(),
                        cors_rule.max_age_seconds.to_string(),
                    );
                }

                if let Some(expected_headers) = header {
                    for header in expected_headers {
                        if !headers.contains_key(&header) {
                            return None;
                        }
                    }
                }
                return Some(headers);
            }
        }
        None
    }
}

// This is similar to TypedRelation but distinct to indicate that it is not a relation
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypedId {
    Project(DieselUlid),
    Collection(DieselUlid),
    Dataset(DieselUlid),
    Object(DieselUlid),
    Unknown(DieselUlid),
}

impl TypedId {
    pub fn get_id(&self) -> DieselUlid {
        match self {
            TypedId::Project(id) => *id,
            TypedId::Collection(id) => *id,
            TypedId::Dataset(id) => *id,
            TypedId::Object(id) => *id,
            TypedId::Unknown(id) => *id,
        }
    }
}

impl From<&Object> for TypedId {
    fn from(value: &Object) -> Self {
        match value.object_type {
            ObjectType::Project => TypedId::Project(value.id),
            ObjectType::Collection => TypedId::Collection(value.id),
            ObjectType::Dataset => TypedId::Dataset(value.id),
            ObjectType::Object => TypedId::Object(value.id),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UploadPart {
    pub id: DieselUlid,
    pub object_id: DieselUlid,
    pub upload_id: String,
    pub part_number: u64,
    pub raw_size: u64,
    pub size: u64,
    pub checksums: Option<HashMap<String, String>>,
}

impl From<UploadPart> for Part {
    fn from(val: UploadPart) -> Self {
        Part {
            e_tag: None,         //TODO?
            last_modified: None, //TODO?
            part_number: Some(val.part_number as i32),
            size: Some(val.size as i64),
            ..Default::default() //Checksums
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::database::database::Database;
    use crate::database::persistence::WithGenericBytes;
    use crate::structs::UploadPart;
    use diesel_ulid::DieselUlid;
    use std::collections::HashMap;

    #[test]
    fn test_resource_strings_cmp() {}

    #[test]
    fn upload_part_marshalling() {
        // Create a test instance of UploadPart with checksums
        let mut checksums = HashMap::new();
        checksums.insert("md5".to_string(), "abc123".to_string());
        checksums.insert("sha256".to_string(), "def456".to_string());
        checksums.insert("crc32".to_string(), "hC5Uvw==".to_string());
        checksums.insert("crc32c".to_string(), "p+pydw====".to_string());
        checksums.insert("crc64nvme".to_string(), "N4vzZ9TJUr8=".to_string());

        let mut original = UploadPart {
            id: DieselUlid::generate(),
            object_id: DieselUlid::generate(),
            upload_id: "upload-123".to_string(),
            part_number: 1,
            raw_size: 1024,
            size: 512,
            checksums: Some(checksums),
        };

        // Serialize / Deserialize
        let json = serde_json::to_string(&original).expect("Failed to serialize");
        dbg!(&json);
        let deserialized: UploadPart = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(original, deserialized);

        // Set checksums to None and try again
        original.checksums = None;

        // Serialize / Deserialize
        let json = serde_json::to_string(&original).expect("Failed to serialize");
        dbg!(&json);
        let deserialized: UploadPart = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(original, deserialized);
    }

    #[tokio::test]
    async fn upload_part_persistence() {
        // Create a test instance of UploadPart with checksums
        let mut checksums = HashMap::new();
        checksums.insert("md5".to_string(), "abc123".to_string());
        checksums.insert("sha256".to_string(), "def456".to_string());
        checksums.insert("crc32".to_string(), "hC5Uvw==".to_string());
        checksums.insert("crc32c".to_string(), "p+pydw====".to_string());
        checksums.insert("crc64nvme".to_string(), "N4vzZ9TJUr8=".to_string());

        let mut original = UploadPart {
            id: DieselUlid::generate(),
            object_id: DieselUlid::generate(),
            upload_id: "upload-123".to_string(),
            part_number: 1,
            raw_size: 1024,
            size: 512,
            checksums: Some(checksums),
        };

        let db = Database::new().await.unwrap();
        let client = db.get_client().await.unwrap();

        original.upsert(&client).await.unwrap();

        let fetched = UploadPart::get_opt(&original.id, &client)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(original, fetched);

        // Remove checksums, upsert in database and re-fetch
        original.checksums = None;
        original.upsert(&client).await.unwrap();

        let fetched = UploadPart::get_opt(&original.id, &client)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(original, fetched);
    }
}
