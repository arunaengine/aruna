use chrono::{DateTime, NaiveDateTime, Utc};
use jsonwebtoken::DecodingKey;
use obkv::KvReaderU16;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::fmt::Display;
use ulid::Ulid;
use utoipa::ToSchema;

use crate::{
    constants::{
        field_names::{DELETED_FIELD, LICENSE_FIELD, TAG_FIELD, VARIANT_FIELD},
        relation_types::*,
    },
    error::ArunaError,
    storage::obkv_ext::{FieldIterator, ParseError},
};

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct IssuerKey {
    pub key_id: String,
    pub issuer_name: String,
    pub issuer_endpoint: Option<String>,
    pub issuer_type: IssuerType,
    pub decoding_key: DecodingKey,
    pub x25519_pubkey: [u8; 32],
    pub audiences: Vec<String>,
}

pub type EdgeType = u32;

// Constants for the models

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
#[repr(u8)]
pub enum ResourceVariant {
    Project,
    Folder,
    #[default]
    Object,
}

impl TryFrom<u8> for ResourceVariant {
    type Error = ArunaError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => ResourceVariant::Project,
            1 => ResourceVariant::Folder,
            2 => ResourceVariant::Object,
            _ => {
                return Err(ArunaError::ConversionError {
                    from: format!("{}u8", value),
                    to: "models::ResourceVariant".to_string(),
                })
            }
        })
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum GenericNode {
    Resource(Resource),
    User(User),
    ServiceAccount(ServiceAccount),
    Group(Group),
    Realm(Realm),
    Component(Component),
    License(License),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeVariant {
    ResourceProject = 0,
    ResourceFolder = 1,
    ResourceObject = 2,
    User = 3,
    ServiceAccount = 4,
    Group = 5,
    Realm = 6,
    Component = 7,
    License = 8,
}

impl TryFrom<u8> for NodeVariant {
    type Error = ArunaError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => NodeVariant::ResourceProject,
            1 => NodeVariant::ResourceFolder,
            2 => NodeVariant::ResourceObject,
            3 => NodeVariant::User,
            4 => NodeVariant::ServiceAccount,
            5 => NodeVariant::Group,
            6 => NodeVariant::Realm,
            7 => NodeVariant::Component,
            _ => {
                return Err(ArunaError::ConversionError {
                    from: format!("{}u8", value),
                    to: "models::NodeVariant".to_string(),
                })
            }
        })
    }
}

impl TryFrom<serde_json::Number> for NodeVariant {
    type Error = ArunaError;

    fn try_from(value: serde_json::Number) -> Result<Self, Self::Error> {
        value.as_u64().map_or_else(
            || {
                Err(ArunaError::ConversionError {
                    from: "serde_json::Number".to_string(),
                    to: "models::NodeVariant".to_string(),
                })
            },
            |v| {
                Ok(match v {
                    0 => NodeVariant::ResourceProject,
                    1 => NodeVariant::ResourceFolder,
                    2 => NodeVariant::ResourceObject,
                    3 => NodeVariant::User,
                    4 => NodeVariant::ServiceAccount,
                    5 => NodeVariant::Group,
                    6 => NodeVariant::Realm,
                    7 => NodeVariant::Component,
                    8 => NodeVariant::License,
                    _ => {
                        return Err(ArunaError::ConversionError {
                            from: format!("{}u64", v),
                            to: "models::NodeVariant".to_string(),
                        })
                    }
                })
            },
        )
    }
}

pub trait Node: for<'a> TryFrom<&'a KvReaderU16, Error = ParseError>
where
    for<'a> &'a Self: TryInto<serde_json::Map<String, Value>, Error = ArunaError>,
{
    fn get_id(&self) -> Ulid;
    fn get_variant(&self) -> NodeVariant;
}

// Helper fuction to convert a struct to serde_json::Map<String, Value>
pub fn into_serde_json_map<T: Serialize>(
    value: T,
    variant: NodeVariant,
) -> Result<serde_json::Map<String, Value>, ArunaError> {
    let value = serde_json::to_value(value).map_err(|e| {
        tracing::error!(?e, "Error converting to serde_json::Value");
        ArunaError::ConversionError {
            from: "models::Node".to_string(),
            to: "serde_json::Map<String, Value>".to_string(),
        }
    })?;
    match value {
        Value::Object(mut map) => {
            map.insert(
                VARIANT_FIELD.to_string(),
                Value::Number(Number::from(variant.clone() as u64)),
            );
            map.insert(DELETED_FIELD.to_string(), Value::Bool(false));

            match variant {
                NodeVariant::ResourceFolder
                | NodeVariant::ResourceObject
                | NodeVariant::ResourceProject => {
                    if let Some(license) = map.remove("license_id") {
                        map.insert(LICENSE_FIELD.to_string(), license);
                    }
                    if let Some(title) = map.remove("title") {
                        map.insert(TAG_FIELD.to_string(), title);
                    }
                }
                _ => {}
            }
            Ok(map)
        }
        _ => Err(ArunaError::ConversionError {
            from: "models::Node".to_string(),
            to: "serde_json::Map<String, Value>".to_string(),
        }),
    }
}

impl Node for Resource {
    fn get_id(&self) -> Ulid {
        self.id
    }
    fn get_variant(&self) -> NodeVariant {
        match self.variant {
            ResourceVariant::Project => NodeVariant::ResourceProject,
            ResourceVariant::Folder => NodeVariant::ResourceFolder,
            ResourceVariant::Object => NodeVariant::ResourceObject,
        }
    }
}

impl TryFrom<&Resource> for serde_json::Map<String, Value> {
    type Error = ArunaError;
    fn try_from(r: &Resource) -> Result<Self, Self::Error> {
        Ok(match r.variant {
            ResourceVariant::Project => into_serde_json_map(r, NodeVariant::ResourceProject)?,
            ResourceVariant::Folder => into_serde_json_map(r, NodeVariant::ResourceFolder)?,
            ResourceVariant::Object => into_serde_json_map(r, NodeVariant::ResourceObject)?,
        })
    }
}

// Implement TryFrom for Resource
impl TryFrom<&KvReaderU16> for Resource {
    type Error = ParseError;

    fn try_from(obkv: &KvReaderU16) -> Result<Self, Self::Error> {
        let mut obkv = FieldIterator::new(obkv);

        let id: Ulid = obkv.get_required_field(0)?;
        let variant: u8 = obkv.get_required_field(1)?;

        let variant = match variant {
            0 => ResourceVariant::Project,
            1 => ResourceVariant::Folder,
            2 => ResourceVariant::Object,
            _ => {
                return Err(ParseError(format!(
                    "Invalid variant for Resource: {}",
                    variant
                )))
            }
        };
        let deleted: bool = obkv.get_required_field(2)?;
        if deleted {
            Ok(Resource {
                id,
                variant,
                deleted,
                ..Default::default()
            })
        } else {
            Ok(Resource {
                id,
                variant,
                deleted,
                name: obkv.get_required_field(3)?,
                description: obkv.get_field(4)?,
                revision: 0,
                labels: obkv.get_field(5)?,
                identifiers: obkv.get_field(6)?,
                content_len: obkv.get_field(7)?,
                count: obkv.get_field(8)?,
                visibility: obkv.get_field(9)?,
                created_at: obkv.get_field(10)?,
                last_modified: obkv.get_field(11)?,
                authors: obkv.get_field(12)?,
                locked: obkv.get_field(13)?,
                license_id: obkv.get_field(14)?,
                hashes: obkv.get_field(15)?,
                location: obkv.get_field(16)?,
                title: obkv.get_field(23)?,
            })
        }
    }
}

impl Node for User {
    fn get_id(&self) -> Ulid {
        self.id
    }
    fn get_variant(&self) -> NodeVariant {
        NodeVariant::User
    }
}

impl TryFrom<&User> for serde_json::Map<String, Value> {
    type Error = ArunaError;
    fn try_from(u: &User) -> Result<Self, Self::Error> {
        into_serde_json_map(u, NodeVariant::User)
    }
}

// Implement TryFrom for User
impl TryFrom<&KvReaderU16> for User {
    type Error = ParseError;

    fn try_from(obkv: &KvReaderU16) -> Result<Self, Self::Error> {
        let mut obkv = FieldIterator::new(obkv);
        // Get the required id
        let id: Ulid = obkv.get_required_field(0)?;
        // Get and double check the variant
        let variant: u8 = obkv.get_required_field(1)?;
        if variant != NodeVariant::User as u8 {
            return Err(ParseError(format!("Invalid variant for User: {}", variant)));
        }

        let deleted: bool = obkv.get_required_field(2)?;
        if deleted {
            Ok(User {
                id,
                deleted,
                ..Default::default()
            })
        } else {
            Ok(User {
                id,
                deleted,
                identifiers: obkv.get_field(6)?,
                first_name: obkv.get_required_field(19)?,
                last_name: obkv.get_required_field(20)?,
                email: obkv.get_required_field(21)?,
                global_admin: obkv.get_field(22)?,
            })
        }
    }
}

impl Node for ServiceAccount {
    fn get_id(&self) -> Ulid {
        self.id
    }
    fn get_variant(&self) -> NodeVariant {
        NodeVariant::ServiceAccount
    }
}

impl TryFrom<&ServiceAccount> for serde_json::Map<String, Value> {
    type Error = ArunaError;
    fn try_from(sa: &ServiceAccount) -> Result<Self, Self::Error> {
        into_serde_json_map(sa, NodeVariant::ServiceAccount)
    }
}

// Implement TryFrom for ServiceAccount
impl TryFrom<&KvReaderU16> for ServiceAccount {
    type Error = ParseError;

    fn try_from(obkv: &KvReaderU16) -> Result<Self, Self::Error> {
        let mut obkv = FieldIterator::new(obkv);
        // Get the required id
        let id: Ulid = obkv.get_required_field(0)?;
        // Get and double check the variant
        let variant: u8 = obkv.get_required_field(1)?;
        if variant != NodeVariant::ServiceAccount as u8 {
            return Err(ParseError(format!("Invalid variant for User: {}", variant)));
        }
        let deleted: bool = obkv.get_required_field(2)?;
        if deleted {
            Ok(ServiceAccount {
                id,
                deleted,
                ..Default::default()
            })
        } else {
            Ok(ServiceAccount {
                id,
                deleted,
                name: obkv.get_field(3)?,
            })
        }
    }
}

impl Node for Group {
    fn get_id(&self) -> Ulid {
        self.id
    }
    fn get_variant(&self) -> NodeVariant {
        NodeVariant::Group
    }
}

impl TryFrom<&Group> for serde_json::Map<String, Value> {
    type Error = ArunaError;
    fn try_from(g: &Group) -> Result<Self, Self::Error> {
        into_serde_json_map(g, NodeVariant::Group)
    }
}

// Implement TryFrom for Group
impl TryFrom<&KvReaderU16> for Group {
    type Error = ParseError;

    fn try_from(obkv: &KvReaderU16) -> Result<Self, Self::Error> {
        let mut obkv = FieldIterator::new(obkv);
        // Get the required id
        let id: Ulid = obkv.get_required_field(0)?;
        // Get and double check the variant
        let variant: u8 = obkv.get_required_field(1)?;
        if variant != NodeVariant::Group as u8 {
            return Err(ParseError(format!("Invalid variant for User: {}", variant)));
        }
        let deleted: bool = obkv.get_required_field(2)?;
        if deleted {
            Ok(Group {
                id,
                deleted,
                ..Default::default()
            })
        } else {
            Ok(Group {
                id,
                deleted,
                name: obkv.get_required_field(3)?,
                description: obkv.get_field(4)?,
            })
        }
    }
}

impl Node for Realm {
    fn get_id(&self) -> Ulid {
        self.id
    }
    fn get_variant(&self) -> NodeVariant {
        NodeVariant::Realm
    }
}

impl TryFrom<&Realm> for serde_json::Map<String, Value> {
    type Error = ArunaError;
    fn try_from(r: &Realm) -> Result<Self, Self::Error> {
        into_serde_json_map(r, NodeVariant::Realm)
    }
}

// Implement TryFrom for Group
impl TryFrom<&KvReaderU16> for Realm {
    type Error = ParseError;

    fn try_from(obkv: &KvReaderU16) -> Result<Self, Self::Error> {
        let mut obkv = FieldIterator::new(obkv);
        // Get the required id
        let id: Ulid = obkv.get_required_field(0)?;
        // Get and double check the variant
        let variant: u8 = obkv.get_required_field(1)?;
        if variant != NodeVariant::Realm as u8 {
            return Err(ParseError(format!("Invalid variant for User: {}", variant)));
        }
        let deleted: bool = obkv.get_required_field(2)?;
        if deleted {
            Ok(Realm {
                id,
                deleted,
                ..Default::default()
            })
        } else {
            Ok(Realm {
                id,
                deleted,
                name: obkv.get_required_field(3)?,
                description: obkv.get_field(4)?,
                tag: obkv.get_field(23)?,
            })
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum NodeVariantIdx {
    Resource(u32),
    User(u32),
    ServiceAccount(u32),
    Group(u32),
    Realm(u32),
    License(u32),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
    pub locked: bool,
}

// TODO: Decide how hooks are going to be implemented
pub enum HookExecutionState {
    Pending,
    Running,
    Finished,
    Error,
}

// TODO: Decide how hooks are going to be implemented
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct HookRunStatus {
    hook_id: Ulid,
    run_id: Ulid,
    revision: u64,
    status: String,
    last_updated: DateTime<Utc>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, Default, ToSchema,
)]
pub enum VisibilityClass {
    Public,
    PublicMetadata,
    #[default]
    Private,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Author {
    pub id: Ulid,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub identifier: String,
}

// ArunaGraph Nodes
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct Realm {
    pub id: Ulid,
    pub tag: String, // -> Region
    pub name: String,
    pub description: String,
    pub deleted: bool,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct Group {
    pub id: Ulid,
    pub name: String,
    pub description: String,
    pub deleted: bool,
    // TODO: OIDC mapping ?
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Constraints {}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub enum TokenType {
    #[default]
    Aruna,
    S3,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Token {
    pub id: u16,
    pub user_id: Ulid,
    pub name: String,
    pub expires_at: DateTime<Utc>,
    pub token_type: TokenType,
    pub scope: Scope,
    pub constraints: Option<Constraints>,
    pub default_realm: Option<Ulid>,
    pub default_group: Option<Ulid>,
    pub component_id: Option<Ulid>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub enum Scope {
    #[default]
    Personal,
    Ressource {
        resource_id: Ulid,
        permission: Permission,
    },
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct S3Credential {
    pub access_key: String,
    pub token_info: Token,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct User {
    pub id: Ulid,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub identifiers: String, // TODO: Vec<String>?
    // TODO: OIDC mapping ?
    // pub oidc_mappings: Vec<OidcMapping>,
    pub global_admin: bool,
    pub deleted: bool,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct OidcMapping {
    pub provider: String,
    pub id: String,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct ServiceAccount {
    pub id: Ulid,
    pub name: String,
    pub deleted: bool,
    // TODO: More fields?
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct Resource {
    pub id: Ulid,
    pub name: String,
    pub title: String,
    pub description: String,
    pub revision: u64, // This should not be part of the index
    pub variant: ResourceVariant,
    pub labels: Vec<KeyValue>,
    //pub hook_status: Vec<KeyValue>, // TODO: Hooks ? Not part of the index
    pub identifiers: Vec<String>,
    pub content_len: u64,
    pub count: u64,
    pub visibility: VisibilityClass,
    pub created_at: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
    pub authors: Vec<Author>,
    pub license_id: Ulid,
    pub locked: bool,
    pub deleted: bool,
    // TODO:
    pub location: Vec<DataLocation>, // Part of index ?
    pub hashes: Vec<Hash>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RelationInfo {
    pub idx: EdgeType,
    pub forward_type: String,  // A --- HasPart---> B
    pub backward_type: String, // A <---PartOf--- B
    pub internal: bool,        // only for internal use
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Relation {
    pub from_id: Ulid,
    pub to_id: Ulid,
    pub relation_type: String,
}

pub type Source = u32;
pub type Target = u32;

#[derive(Deserialize, Serialize, Debug)]
pub struct RawRelation {
    pub source: Source,
    pub target: Target,
    pub edge_type: EdgeType,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct ServerInfo {
    pub node_id: Ulid,
    pub node_serial: u32,
    pub url: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct ServerState {
    pub node_id: Ulid,
    pub status: String,
}

pub struct PubKey {
    pub key_serial: u32,
    pub node_id: Ulid,
    pub key: String,
    pub decoding_key: DecodingKey,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Hash {
    pub algorithm: HashAlgorithm,
    pub value: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum SyncingStatus {
    Pending,
    Running,
    Finished,
    Error,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct DataLocation {
    pub endpoint_id: Ulid,
    pub status: SyncingStatus,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum ResourceStatus {
    Initializing, // Resource initialized but no data provided
    Validating,   // Validating the resource
    Available,
    Frozen,
    Unavailable,
    Error,
    Deleted,
}

pub enum ResourceEndpointStatus {
    Pending,
    Running,
    Finished,
    Error,
}

pub enum ResourceEndpointVariant {
    Dataproxy,
    Compute,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum HashAlgorithm {
    Sha256,
    MD5,
}

impl Display for HashAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            HashAlgorithm::Sha256 => "Sha256",
            HashAlgorithm::MD5 => "MD5",
        };
        write!(f, "{}", name)
    }
}

#[repr(u32)]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum Permission {
    None = 2,
    Read = 3,
    Append = 4,
    Write = 5,
    Admin = 6,
}

impl TryFrom<u32> for Permission {
    type Error = ArunaError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Ok(match value {
            PERMISSION_NONE => Permission::None,
            PERMISSION_READ => Permission::Read,
            PERMISSION_APPEND => Permission::Append,
            PERMISSION_WRITE => Permission::Write,
            PERMISSION_ADMIN => Permission::Admin,
            _ => {
                return Err(ArunaError::ConversionError {
                    from: format!("{}u32", value),
                    to: "models::Permission".to_string(),
                })
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IssuerType {
    SERVER,
    DATAPROXY,
    OIDC,
}

// #[derive(Clone, Serialize, Deserialize)]
// pub struct Issuer {
//     pub issuer_name: String,
//     pub pubkey_endpoint: Option<String>,
//     pub audiences: Vec<String>,
//     pub issuer_type: IssuerType,
// }

impl IssuerKey {
    pub async fn fetch_jwks(
        endpoint: &str,
    ) -> Result<(Vec<(String, DecodingKey)>, NaiveDateTime), ArunaError> {
        let client = reqwest::Client::new();
        let res = client.get(endpoint).send().await.map_err(|e| {
            tracing::error!(?e, "Error fetching JWK from endpoint");
            ArunaError::Unauthorized
        })?;
        let jwks: jsonwebtoken::jwk::JwkSet = res.json().await.map_err(|e| {
            tracing::error!(?e, "Error serializing JWK from endpoint");
            ArunaError::Unauthorized
        })?;

        Ok((
            jwks.keys
                .iter()
                .filter_map(|jwk| {
                    let key = DecodingKey::from_jwk(jwk).ok()?;
                    Some((jwk.common.clone().key_id?, key))
                })
                .collect::<Vec<_>>(),
            Utc::now().naive_utc(),
        ))
    }
}

pub type UserType = u8;
pub type TokenIdx = u16;

/// This contains claims for ArunaTokens
/// containing 3 mandatory and 2 optional fields.
///
/// - iss: Token issuer
/// - sub: User_ID or subject
/// - exp: When this token expires (by default very large number)
/// - tid: UUID from the specific token
#[derive(Debug, Serialize, Deserialize)]
pub struct ArunaTokenClaims {
    pub iss: String, // 'aruna', 'data', 'compute' or oidc issuer
    pub sub: String, // User or ServiceAccount ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<Audience>, // Audience;
    pub exp: u64,    // Expiration timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<(UserType, TokenIdx)>, // Optional info for aruna tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>, // Optional scope
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(untagged)]
pub enum Audience {
    String(String),
    Vec(Vec<String>),
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
#[repr(u8)]
pub enum ComponentType {
    Server,
    #[default]
    Data,
    Compute,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum Endpoint {
    S3(Url),
    Json(Url),
    Grpc(Url),
    Consensus(Url),
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct Component {
    pub id: Ulid,
    pub name: String,
    pub description: String,
    pub component_type: ComponentType,
    pub endpoints: Vec<Endpoint>,
    pub public: bool,
    pub deleted: bool,
}

impl Node for Component {
    fn get_id(&self) -> Ulid {
        self.id
    }
    fn get_variant(&self) -> NodeVariant {
        NodeVariant::Component
    }
}

impl TryFrom<&Component> for serde_json::Map<String, Value> {
    type Error = ArunaError;
    fn try_from(u: &Component) -> Result<Self, Self::Error> {
        into_serde_json_map(u, NodeVariant::Component)
    }
}

// Implement TryFrom for User
impl TryFrom<&KvReaderU16> for Component {
    type Error = ParseError;

    fn try_from(obkv: &KvReaderU16) -> Result<Self, Self::Error> {
        let mut obkv = FieldIterator::new(obkv);
        // Get the required id
        let id: Ulid = obkv.get_required_field(0)?;
        // Get and double check the variant
        let variant: u8 = obkv.get_required_field(1)?;
        if variant != NodeVariant::Component as u8 {
            return Err(ParseError(format!(
                "Invalid variant for Component: {}",
                variant
            )));
        }

        let deleted: bool = obkv.get_required_field(2)?;
        if deleted {
            Ok(Component {
                id,
                deleted,
                ..Default::default()
            })
        } else {
            Ok(Component {
                id,
                deleted,
                name: obkv.get_field(3)?,
                description: obkv.get_field(4)?,
                component_type: obkv.get_field(24)?,
                endpoints: obkv.get_field(25)?,
                public: obkv.get_field(26)?,
            })
        }
    }
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, ToSchema)]
pub struct Subscriber {
    pub id: Ulid,
    pub owner: Ulid,
    pub target_idx: u32,
    pub cascade: bool,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, ToSchema, Default)]
pub struct License {
    pub id: Ulid,
    pub name: String,
    pub description: String,
    pub terms: String,
    pub deleted: bool,
}

impl Node for License {
    fn get_id(&self) -> Ulid {
        self.id
    }
    fn get_variant(&self) -> NodeVariant {
        NodeVariant::License
    }
}

impl TryFrom<&License> for serde_json::Map<String, Value> {
    type Error = ArunaError;
    fn try_from(u: &License) -> Result<Self, Self::Error> {
        into_serde_json_map(u, NodeVariant::License)
    }
}

// Implement TryFrom for User
impl TryFrom<&KvReaderU16> for License {
    type Error = ParseError;

    fn try_from(obkv: &KvReaderU16) -> Result<Self, Self::Error> {
        let mut obkv = FieldIterator::new(obkv);
        // Get the required id
        let id: Ulid = obkv.get_required_field(0)?;
        // Get and double check the variant
        let variant: u8 = obkv.get_required_field(1)?;
        if variant != NodeVariant::License as u8 {
            return Err(ParseError(format!(
                "Invalid variant for License: {}",
                variant
            )));
        }
        let deleted: bool = obkv.get_required_field(2)?;
        if deleted {
            Ok(License {
                id,
                deleted,
                ..Default::default()
            })
        } else {
            Ok(License {
                id,
                deleted,
                name: obkv.get_field(3)?,
                description: obkv.get_field(4)?,
                terms: obkv.get_field(27)?,
            })
        }
    }
}
