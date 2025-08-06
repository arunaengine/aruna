use super::conversions::autosurgeon_bytes;
use super::conversions::autosurgeon_date_time;
use super::conversions::autosurgeon_ulid;
use super::conversions::autosurgeon_user_identity;
use aruna_permission::Path;
use aruna_permission::UserIdentity;
use aruna_permission::manager::AddUserPrepare;
use aruna_permission::manager::CreateGroupPrepare;
use automerge::AutoCommit;
use autosurgeon::{Hydrate, Reconcile};
use chrono::{DateTime, Utc};
use iroh::NodeAddr;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;
use utoipa::ToSchema;

// TODO: Decide what essential fields are needed
// and put rest into labels/hashmap
#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Default,
    Reconcile,
    Hydrate,
    Hash,
)]
pub struct Resource {
    // Needed
    #[autosurgeon(with = "autosurgeon_ulid")]
    #[key]
    pub id: Ulid,
    pub name: String,
    #[autosurgeon(with = "autosurgeon_date_time")]
    pub created_at: DateTime<Utc>,
    #[autosurgeon(with = "autosurgeon_date_time")]
    pub last_modified: DateTime<Utc>,
    pub visibility: VisibilityClass,
    pub variant: ResourceVariant,
    pub deleted: bool,
    // JSON where the rest is defined
    pub labels: Vec<KeyValue>, // rename to tags
    // pub metadata: Vec<KeyValue>,
    // pub data: enum {Url, ContentHash, MetadataId+CommitHash}

    // Optional
    pub title: String,
    pub description: String,
    pub revision: u64, // This should not be part of the index
    pub identifiers: Vec<String>,
    pub content_len: u64,
    pub count: u64,
    pub authors: Vec<Author>,
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub license_id: Ulid,
    pub locked: bool,
    pub location: Vec<String>, // Part of index ?
    pub hashes: Vec<Hash>,
}

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Default,
    Reconcile,
    Hydrate,
    Hash,
)]
pub enum ResourceVariant {
    #[default]
    Project,
    Folder,
    Object,
}

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Reconcile,
    Hydrate,
    Hash,
)]
pub struct KeyValue {
    #[key]
    pub key: String,
    pub value: String,
}

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Reconcile,
    Hydrate,
    Hash,
)]
pub struct Author {
    pub first: String,
    pub last: String,
    pub id: String,
}

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Default,
    Reconcile,
    Hydrate,
    Hash,
)]
pub enum VisibilityClass {
    Public,
    Private,
    #[default]
    Invisible,
}

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Reconcile,
    Hydrate,
    Hash,
)]
pub struct Hash {
    pub algorithm: HashAlgorithm,
    pub value: String,
}

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Reconcile,
    Hydrate,
    Hash,
)]
pub enum HashAlgorithm {
    Sha256,
    MD5,
}

#[repr(u32)]
#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Reconcile,
    Hydrate,
)]
pub enum Permission {
    None = 2,
    Read = 3,
    Append = 4,
    Write = 5,
    Admin = 6,
}

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Default,
    Reconcile,
    Hydrate,
)]
pub enum Direction {
    Incoming,
    Outgoing,
    #[default]
    All,
}

// TODO: UserIdentities
#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Reconcile,
    Hydrate,
)]
pub struct User {
    #[autosurgeon(with = "autosurgeon_user_identity")]
    #[schema(value_type=String)]
    #[key]
    pub id: UserIdentity,
    #[autosurgeon(with = "autosurgeon_bytes")]
    pub realm_key: [u8; 32],
    pub name: String,
    //pub user_identity: UserIdentity,
}

#[derive(Clone)]
pub enum TypedDoc {
    Resource(AutoCommit),
    Group(AutoCommit),
    User(AutoCommit),
}

impl TypedDoc {
    pub fn get_inner(&self) -> AutoCommit {
        match self {
            TypedDoc::Resource(x) => x,
            TypedDoc::Group(x) => x,
            TypedDoc::User(x) => x,
        }
        .clone()
    }
}

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Default,
    Reconcile,
    Hydrate,
)]
pub struct Group {
    #[autosurgeon(with = "autosurgeon_ulid")]
    #[key]
    pub id: Ulid,
    #[autosurgeon(with = "autosurgeon_bytes")]
    pub realm_key: [u8; 32],
    pub name: String,
    pub roles: Vec<String>,
    pub members: BTreeMap<String, Vec<String>>, // UserIndentity to role mappings
}

pub enum HandleHelper {
    AddGroup(CreateGroupPrepare),
    AddUser(AddUserPrepare),
}

#[derive(Clone)]
pub enum PolicyResult {
    Deny(String),
    Accept,
    Forward,
    Modify,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TypedSavedDoc {
    Resource(Vec<u8>),
    Group(Vec<u8>),
    User(Vec<u8>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TaskPayload {
    Sync {
        doc: TypedSavedDoc,
        subject_hash: [u8; 32],
        doc_id: Vec<u8>,
        path: Path,
        nodes: Vec<NodeAddr>,
    },
}
