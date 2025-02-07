use chrono::Utc;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

use super::models::{
    Author, Component, ComponentType, Endpoint, GenericNode, Group, Hash, KeyValue, Permission,
    Realm, Relation, RelationInfo, Resource, ResourceVariant, S3Credential, Scope, Token, User,
    VisibilityClass,
};

fn default_license_tag() -> String {
    "CC-BY-SA-4.0".to_string()
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
    pub variant: ResourceVariant,
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
pub struct BatchResource {
    pub name: String,
    pub parent: Parent,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub variant: ResourceVariant,
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
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Hash)]
pub enum Parent {
    ID(Ulid),
    Idx(u32),
}

impl Default for Parent {
    fn default() -> Self {
        Parent::Idx(0)
    }
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct CreateResourceBatchRequest {
    pub resources: Vec<BatchResource>,
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
    #[serde(default)]
    pub group_id: Ulid,
    #[serde(default)]
    pub realm_id: Ulid,
    // TODO: Default endpoints?
    #[serde(default)]
    pub data_endpoint: Option<Ulid>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRealmRequest {
    pub tag: String,
    pub name: String,
    pub description: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupRequest {
    pub name: String,
    pub description: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupResponse {
    pub group: Group,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetResourcesRequest {
    pub ids: Vec<Ulid>,
}

// Read responses
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetResourcesResponse {
    pub resources: Vec<Resource>,
    // pub relations: Vec<Relation>, // TODO: remove and move into its own request
}

// Write responses
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateProjectResponse {
    pub resource: Resource,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateResourceResponse {
    pub resource: Resource,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateResourceBatchResponse {
    pub resources: Vec<Resource>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRealmResponse {
    pub realm: Realm,
    pub admin_group_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGroupRequest {
    pub realm_id: Ulid,
    pub group_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGroupResponse {}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetRealmRequest {
    pub id: Ulid,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetRealmResponse {
    pub realm: Realm,
    // pub groups: Vec<Ulid>, // TODO: remove and move into its own request
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetGroupRequest {
    pub id: Ulid,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetGroupResponse {
    pub group: Group,
    // pub members: Vec<Ulid>, // TODO: remove and move into its own request?
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserRequest {
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub identifier: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserResponse {
    pub user: User,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTokenRequest {
    pub name: String,
    #[serde(default)]
    pub expires_at: Option<chrono::DateTime<Utc>>,
    #[serde(default)]
    pub scope: Scope,
    //pub constraints: Vec<Constraint>,
    pub realm_id: Option<Ulid>,
    pub group_id: Option<Ulid>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTokenResponse {
    pub token: Token,
    pub secret: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsRequest {
    pub name: String,
    pub realm_id: Ulid,
    pub group_id: Ulid,
    pub component_id: Ulid,
    #[serde(default)]
    pub scope: Scope,
    #[serde(default)]
    pub expires_at: Option<chrono::DateTime<Utc>>,
    //pub constraints: Vec<Constraint>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsResponse {
    pub token: Token,
    pub component_id: Ulid,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetTokensRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetTokensResponse {
    pub tokens: Vec<Token>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetS3CredentialsRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetS3CredentialsResponse {
    pub tokens: Vec<S3Credential>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct SearchRequest {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchResponse {
    pub expected_hits: usize,
    pub resources: Vec<GenericNode>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddUserRequest {
    pub group_id: Ulid,
    pub user_id: Ulid,
    pub permission: Permission,
}

// Is needed to parse permissions correctly in query parameters
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct PermissionWrapper {
    pub permission: Permission,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddUserResponse {}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub enum Direction {
    Incoming,
    Outgoing,
    #[default]
    All,
}

fn default_page_size() -> usize {
    100
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetRelationsRequest {
    #[serde(default)]
    pub node: Ulid,
    #[serde(default)]
    pub direction: Direction, // wrapper type for petgraph::Direction enum
    #[serde(default)]
    pub filter: Vec<u32>, // Filter with Strings for directions or idx for rel idx?
    #[serde(default)]
    pub last_entry: Option<usize>, // usize
    #[serde(default = "default_page_size")]
    pub page_size: usize, // Max value 1000? Default 100
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationsResponse {
    pub relations: Vec<Relation>,
    pub offset: Option<usize>,
    pub total_hits: u32,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationInfosRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationInfosResponse {
    pub relation_infos: Vec<RelationInfo>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetUsersFromGroupRequest {
    pub group_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUsersFromGroupResponse {
    pub users: Vec<User>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetGroupsFromRealmRequest {
    pub realm_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupsFromRealmResponse {
    pub groups: Vec<Group>,
}

// Issuer and auth requests
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddOidcProviderRequest {
    pub issuer_name: String,
    pub issuer_endpoint: String,
    pub audiences: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddOidcProviderResponse {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRealmsFromUserRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRealmsFromUserResponse {
    pub realms: Vec<Realm>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupsFromUserRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupsFromUserResponse {
    pub groups: Vec<(Group, Permission)>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetStatsRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetStatsResponse {
    pub resources: usize,
    pub projects: usize,
    pub users: usize,
    pub storage: usize, // in bytes
    pub realms: usize,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetRealmComponentsRequest {
    pub realm_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRealmComponentsResponse {
    pub components: Vec<Component>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUserRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUserResponse {
    pub user: User,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetEventsRequest {
    pub subscriber_id: Ulid,
    #[serde(default)]
    pub acknowledge_from: Option<Ulid>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetEventsResponse {
    // EventId, Event
    pub events: Vec<serde_json::Map<String, serde_json::Value>>,
}

// User ask to access a group
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UserAccessGroupRequest {
    pub group_id: Ulid,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UserAccessGroupResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GroupAccessRealmRequest {
    pub group_id: Ulid,
    pub realm_id: Ulid,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GroupAccessRealmRequestHelper {
    pub group_id: Ulid,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GroupAccessRealmResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRelationRequest {
    pub source: Ulid,
    pub target: Ulid,
    pub variant: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRelationResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRelationVariantRequest {
    pub forward_type: String,
    pub backward_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRelationVariantResponse {
    pub idx: u32,
}

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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateComponentRequest {
    pub name: String,
    pub description: String,
    pub component_type: ComponentType,
    pub endpoints: Vec<Endpoint>,
    pub pubkey: String,
    pub public: bool,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateComponentResponse {
    pub component: Component,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddComponentToRealmRequest {
    pub realm_id: Ulid,
    pub component_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddComponentToRealmResponse {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterDataRequest {
    #[serde(default)]
    pub object_id: Ulid,
    pub component_id: Ulid,
    pub hashes: Vec<Hash>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterDataResponse {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AuthorizeRequest {
    pub id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AuthorizeResponse {
    pub allowed: bool,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteRequest {
    pub id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateLicenseRequest {
    pub name: String,
    pub description: String,
    pub license_terms: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateLicenseResponse {
    pub license_id: Ulid,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetLicensesRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetLicensesResponse {
    pub licenses: Vec<super::models::License>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetLicenseRequest {
    pub id: Ulid,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetLicenseResponse {
    pub license: super::models::License,
}
