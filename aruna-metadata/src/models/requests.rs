use std::collections::BTreeMap;

use crate::error::ArunaMetadataError;

use super::models::{Author, Group, KeyValue, Resource, User, VisibilityClass};
use aruna_permission::UserIdentity;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct CreateResourceRequest {
    pub name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub variant: CreateResourceVariant,
    #[serde(default)]
    pub labels: Vec<KeyValue>,
    #[serde(default)]
    pub identifiers: Vec<String>,
    #[serde(default)]
    pub visibility: VisibilityClass,
    #[serde(default)]
    pub authors: Vec<Author>,
    #[serde(default)]
    pub license_id: Option<Ulid>,
    pub parent_id: Ulid,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub enum CreateResourceVariant {
    #[default]
    Folder,
    Object,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateResourceResponse {
    pub resource: Resource,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct CreateProjectRequest {
    pub name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub labels: Vec<KeyValue>,
    #[serde(default)]
    pub identifiers: Vec<String>,
    #[serde(default)]
    pub visibility: VisibilityClass,
    #[serde(default)]
    pub authors: Vec<Author>,
    #[serde(default)]
    pub license_id: Option<Ulid>,
    pub group_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateProjectResponse {
    pub resource: Resource,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetResourceRequest {
    pub id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetResourceResponse {
    pub resource: Resource,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct SearchRequest {
    pub query: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchResponse {
    pub resources: Vec<Resource>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddUserRequest {
    pub name: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddUserResponse {
    pub user: User,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGroupRequest {
    pub name: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGroupResponse {
    pub group: Group,
}

// TODO: Add Roles and add resources(?)
// #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
// pub struct AddRolesToGroupRequest {
//     pub group_id: Ulid,
//     pub roles: BTreeMap<String, String>, // role + permission
// }
//
// #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
// pub struct AddRolesToGroupResponse {}
//
// #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
// pub struct AddResourcesToGroupRequest {
//     pub group_id: Ulid,
//     pub resources: Vec<Ulid>,
// }
//
// #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
// pub struct AddResourcesToGroupResponse {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddUserToGroupRequest {
    pub group_id: Ulid,
    pub user_roles: BTreeMap<String, Vec<String>>, // useridentity + roles
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddUserToGroupResponse {}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceNameRequest {
    pub id: Ulid,
    pub name: String,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceNameResponse {
    pub resource: Resource,
}
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceTitleRequest {
    pub id: Ulid,
    pub title: String,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceTitleResponse {
    pub resource: Resource,
}
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceDescriptionRequest {
    pub id: Ulid,
    pub description: String,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceDescriptionResponse {
    pub resource: Resource,
}
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceVisibilityRequest {
    pub id: Ulid,
    pub visibility: VisibilityClass,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceVisibilityResponse {
    pub resource: Resource,
}
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceLicenseRequest {
    pub id: Ulid,
    pub license_id: Ulid,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceLicenseResponse {
    pub resource: Resource,
}
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceLabelsRequest {
    pub id: Ulid,
    pub labels_to_add: Vec<KeyValue>,
    pub labels_to_remove: Vec<KeyValue>,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceLabelsResponse {
    pub resource: Resource,
}
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceIdentifiersRequest {
    pub id: Ulid,
    pub ids_to_add: Vec<String>,
    pub ids_to_remove: Vec<String>,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceIdentifiersResponse {
    pub resource: Resource,
}
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceAuthorsRequest {
    pub id: Ulid,
    pub authors_to_add: Vec<Author>,
    pub authors_to_remove: Vec<Author>,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceAuthorsResponse {
    pub resource: Resource,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum ResourceUpdateRequests {
    Name(UpdateResourceNameRequest),
    Title(UpdateResourceTitleRequest),
    Description(UpdateResourceDescriptionRequest),
    Visibility(UpdateResourceVisibilityRequest),
    License(UpdateResourceLicenseRequest),
    Labels(UpdateResourceLabelsRequest),
    Identifiers(UpdateResourceIdentifiersRequest),
    Authors(UpdateResourceAuthorsRequest),
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum ResourceUpdateResponses {
    Name(UpdateResourceNameResponse),
    Title(UpdateResourceTitleResponse),
    Description(UpdateResourceDescriptionResponse),
    Visibility(UpdateResourceVisibilityResponse),
    License(UpdateResourceLicenseResponse),
    Labels(UpdateResourceLabelsResponse),
    Identifiers(UpdateResourceIdentifiersResponse),
    Authors(UpdateResourceAuthorsResponse),
}
pub trait GetInner {
    fn get_id(&self) -> Ulid;
}
impl GetInner for ResourceUpdateRequests {
    fn get_id(&self) -> Ulid {
        match self {
            ResourceUpdateRequests::Name(req) => req.id,
            ResourceUpdateRequests::Title(req) => req.id,
            ResourceUpdateRequests::Description(req) => req.id,
            ResourceUpdateRequests::Visibility(req) => req.id,
            ResourceUpdateRequests::License(req) => req.id,
            ResourceUpdateRequests::Labels(req) => req.id,
            ResourceUpdateRequests::Identifiers(req) => req.id,
            ResourceUpdateRequests::Authors(req) => req.id,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum ForwardRequest {
    GetResource(GetResourceRequest),
    UpdateResource(ResourceUpdateRequests),
    Search(SearchRequest),
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum ForwardResponse {
    GetResource(Result<GetResourceResponse, ArunaMetadataError>),
    UpdateResource(Result<ResourceUpdateResponses, ArunaMetadataError>),
    Search(Result<SearchResponse, ArunaMetadataError>),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams)]
pub struct GetUserRequest {
    #[schema(value_type=String)]
    #[param(value_type=String)]
    pub id: UserIdentity,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUserResponse {
    pub user: User,
}
