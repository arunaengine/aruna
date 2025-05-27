use std::collections::BTreeMap;
use std::collections::HashMap;

use super::conversions::autosurgeon_date_time;
use super::conversions::autosurgeon_ulid;
use autosurgeon::{Hydrate, Reconcile};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;

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
pub struct Resource {
    #[autosurgeon(with = "autosurgeon_ulid")]
    #[key]
    pub id: Ulid,
    pub name: String,
    pub title: String,
    pub description: String,
    pub revision: u64, // This should not be part of the index
    pub variant: ResourceVariant,
    pub labels: Vec<KeyValue>,
    pub identifiers: Vec<String>,
    pub content_len: u64,
    pub count: u64,
    pub visibility: VisibilityClass,
    #[autosurgeon(with = "autosurgeon_date_time")]
    pub created_at: DateTime<Utc>,
    #[autosurgeon(with = "autosurgeon_date_time")]
    pub last_modified: DateTime<Utc>,
    pub authors: Vec<Author>,
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub license_id: Ulid,
    pub locked: bool,
    pub deleted: bool,
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
pub struct User {
    #[autosurgeon(with = "autosurgeon_ulid")]
    #[key]
    pub id: Ulid,
    pub name: String,
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
    pub name: String,
    pub roles: Vec<String>,
    pub members: BTreeMap<String, Vec<String>>, // User to role mappings
}
