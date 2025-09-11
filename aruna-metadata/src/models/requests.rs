use super::structs::{Author, Group, KeyValue, Resource, User, VisibilityClass};
use crate::error::ArunaMetadataError;
use crate::models::structs::{Change, Data};
use crate::transactions::controller::Controller;
use crate::{
    models::structs::PolicyResult, network::network_trait::Network,
    persistence::search::generic::Search,
};
use aruna_permission::UserIdentity;
use aruna_storage::storage::store::Store;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

#[async_trait::async_trait]
pub trait Request<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response;
    type AuthContext;

    async fn authorize(
        &self,
        token: Option<String>,
        controller: &Controller<St, Se, N>,
    ) -> Result<Self::AuthContext, crate::error::ArunaMetadataError>;

    async fn run_request(
        self,
        auth_result: Self::AuthContext,
        controller: &Controller<St, Se, N>,
    ) -> Result<Self::Response, ArunaMetadataError>;

    async fn run_policy(
        &mut self,
        _token: &Option<String>,
        _controller: &Controller<St, Se, N>,
    ) -> Result<PolicyResult, ArunaMetadataError> {
        Ok(PolicyResult::Accept)
    }

    /// Syncs the prerequisites of the request to the node.
    /// It returns an Option<Response> with either the Self::Response of the forwarded request
    /// or None for local execution.
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, ArunaMetadataError>;

    /// Forwards the request
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &Controller<St, Se, N>,
    ) -> Result<Self::Response, ArunaMetadataError>;
}

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
    pub token: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGroupRequest {
    pub name: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGroupResponse {
    pub group: Group,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetGroupRequest {
    pub id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupResponse {
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

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceDataRequest {
    pub id: Ulid,
    pub data_to_add: Vec<Data>,
    pub data_to_remove: Vec<Data>,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceDataResponse {
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
    Data(UpdateResourceDataRequest),
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
    Data(UpdateResourceDataResponse),
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
            ResourceUpdateRequests::Data(req) => req.id,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum ForwardRequest {
    // User requests
    AddUser(AddUserRequest),
    GetUser(GetUserRequest),
    CreateToken(CreateTokenRequest),

    // Group requests
    AddGroup(AddGroupRequest),
    AddUserToGroup(AddUserToGroupRequest),
    GetGroup(GetGroupRequest),

    // Resource requests
    CreateProject(CreateProjectRequest),
    CreateResource(CreateResourceRequest),
    GetResource(GetResourceRequest),
    GetResourceHistory(GetResourceHistoryRequest),
    UpdateResource(ResourceUpdateRequests),
    Search(SearchRequest),
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum ForwardResponse {
    // User requests
    AddUser(Result<AddUserResponse, ArunaMetadataError>),
    GetUser(Result<GetUserResponse, ArunaMetadataError>),
    CreateToken(Result<CreateTokenResponse, ArunaMetadataError>),

    // Group requests
    AddGroup(Result<AddGroupResponse, ArunaMetadataError>),
    AddUserToGroup(Result<AddUserToGroupResponse, ArunaMetadataError>),
    GetGroup(Result<GetGroupResponse, ArunaMetadataError>),

    // Resource requests
    CreateProject(Result<CreateProjectResponse, ArunaMetadataError>),
    CreateResource(Result<CreateResourceResponse, ArunaMetadataError>),
    GetResource(Result<GetResourceResponse, ArunaMetadataError>),
    GetResourceHistory(Result<GetResourceHistoryResponse, ArunaMetadataError>),
    UpdateResource(Result<ResourceUpdateResponses, ArunaMetadataError>),
    Search(Result<SearchResponse, ArunaMetadataError>),
}
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetUserRequestOuter {
    pub id: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct GetUserRequest {
    // TODO: Fix, this is not working
    //#[schema(value_type=String)]
    //#[param(value_type=String)]
    pub id: UserIdentity,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUserResponse {
    pub user: User,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTokenRequest {
    pub expiration_hours: Option<u64>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTokenResponse {
    pub token: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetInfoRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetInfoResponse {
    pub realm_id: String,
    pub node_id: String,
    pub node_addr: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams)]
pub struct GetResourceHistoryRequest {
    pub id: Ulid,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetResourceHistoryResponse {
    pub history: Vec<Change>,
}
