use super::{
    JsonPayload, McpServer, empty_extras, explained, internal_error, parse_ulid, request_auth,
    server_error, tool_extras,
};
use aruna_core::structs::{AuthContext, Group, Permission, Role};
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::list_groups::ListGroupOperation;
use aruna_operations::read_realm_authorization::ReadRealmAuthorizationOperation;
use aruna_operations::read_user_document::{ReadUserDocumentError, ReadUserDocumentOperation};
use aruna_operations::request_policy::PolicyRequestExtras;
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RoleOutput {
    pub role_id: String,
    pub name: String,
    pub permissions: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct GroupOutput {
    pub group_id: String,
    pub display_name: String,
    pub roles: Vec<RoleOutput>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct WhoamiOutput {
    pub user_id: String,
    pub display_name: Option<String>,
    pub realm_roles: Vec<RoleOutput>,
    pub groups: Vec<GroupOutput>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct GroupsOutput {
    pub groups: Vec<GroupOutput>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct GroupIdInput {
    /// The group's bare 26-character ULID, for example
    /// `01JZ8Y6T0K4W7M2N9Q5R3S8V1X`. Call `list_groups` or `whoami` for the ids
    /// the caller belongs to, or read `group_id` from a `search` hit. This is
    /// not the `<ulid>@<realm>` form a user id uses.
    pub group_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct MemberGroup {
    pub group_id: Ulid,
    pub output: GroupOutput,
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::context_router()
}

#[tool_router(router = context_router)]
impl McpServer {
    #[tool(
        description = "Describe the authenticated caller: user id, display name when one is set, realm roles, and every group the caller belongs to with the roles that assign them. Use this first in a session to learn which group ids are available, and list_groups when only the group list is needed. Returns user_id in `<ulid>@<realm>` form while every group_id is a bare 26-character ULID. Takes no arguments and never returns tokens or credentials.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn whoami(
        &self,
        Extension(parts): Extension<http::request::Parts>,
    ) -> Result<Json<WhoamiOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        authorize_read(self, &auth, empty_extras("whoami")).await?;
        let user = drive(
            ReadUserDocumentOperation::new(auth.user_id),
            &self.state.get_ctx(),
        )
        .await
        .map_err(map_user_error)?;
        let groups = member_groups(self, &auth).await?;
        let realm = drive(
            ReadRealmAuthorizationOperation::new(self.state.get_realm_id()),
            &self.state.get_ctx(),
        )
        .await
        .map_err(internal_error)?;
        let mut realm_roles = realm
            .into_iter()
            .flat_map(|document| document.roles.into_values())
            .filter(|role| role.assigned_users.contains(&auth.user_id))
            .map(map_role)
            .collect::<Vec<_>>();
        realm_roles.sort_by(|left, right| left.role_id.cmp(&right.role_id));

        Ok(Json(WhoamiOutput {
            user_id: auth.user_id.to_string(),
            display_name: (!user.name.is_empty()).then_some(user.name),
            realm_roles,
            groups: groups.into_iter().map(|group| group.output).collect(),
        }))
    }

    #[tool(
        description = "List the groups the caller belongs to, each with group_id, display_name, and the caller's roles in that group. Call this to obtain the group_id that create_dataset, run_script, and the group filters require. Groups the caller is not a member of are not listed; read one of those with get_group. Takes no arguments.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_groups(
        &self,
        Extension(parts): Extension<http::request::Parts>,
    ) -> Result<Json<GroupsOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        authorize_read(self, &auth, empty_extras("list_groups")).await?;
        let groups = member_groups(self, &auth).await?;
        Ok(Json(GroupsOutput {
            groups: groups.into_iter().map(|group| group.output).collect(),
        }))
    }

    #[tool(
        description = "Read one realm group by its ULID and return display_name, group_id, realm_id, and the full role list. Roles list their assigned users only when the caller is a member of that group. Use list_groups for the caller's own groups and this tool for a group id that came from another tool's output. Returns Not found when no group in this realm carries that id.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn get_group(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<GroupIdInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        authorize_read(self, &auth, tool_extras("get_group", &input)?).await?;
        group_argument(&input.group_id)?;
        let response =
            crate::routes::groups::run_get_group(&self.state, Some(auth), &input.group_id)
                .await
                .map_err(group_error)?;
        json_output(response)
    }

    #[tool(
        description = "List every member of a group with the roles that assign them, returning member ids in `<ulid>@<realm>` form together with display names when known. The caller must be a member of that group; a non-member receives Forbidden. Use get_group when only the role names and the group's display name are needed. Call list_groups first for a valid group_id.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_group_members(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<GroupIdInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        authorize_read(self, &auth, tool_extras("list_group_members", &input)?).await?;
        group_argument(&input.group_id)?;
        let response =
            crate::routes::groups::run_group_members(&self.state, Some(auth), &input.group_id)
                .await
                .map_err(group_error)?;
        json_output(response)
    }

    #[tool(
        description = "Read one group's storage usage on this node next to the realm-wide totals, its dataset, Profile, and process-run counts, and its quota status. The caller must be a member of that group; a non-member receives Forbidden. Use get_realm_info for the realm's quota configuration rather than one group's consumption. Call list_groups first for a valid group_id.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn get_group_usage(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<GroupIdInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        authorize_read(self, &auth, tool_extras("get_group_usage", &input)?).await?;
        group_argument(&input.group_id)?;
        let response =
            crate::routes::groups::run_group_usage(&self.state, Some(auth), &input.group_id)
                .await
                .map_err(group_error)?;
        json_output(response)
    }

    #[tool(
        description = "Describe the realm this node serves: realm id, description, OIDC providers, metadata replication, quota configuration, member nodes, and the endpoints this node exposes. Takes no arguments. Use get_node_info for the answering node's own version and service health, and get_group_usage for one group's consumption against the quota reported here.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn get_realm_info(
        &self,
        Extension(parts): Extension<http::request::Parts>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        authorize_read(self, &auth, empty_extras("get_realm_info")).await?;
        let response = crate::routes::info::run_realm_info(&self.state, Some(auth))
            .await
            .map_err(server_error)?;
        json_output(response)
    }

    #[tool(
        description = "Describe the node answering this request: api version, capabilities, peer addresses, and network, blob, and database service status. Takes no arguments. Use get_realm_info for realm-wide configuration and the list of member nodes. Buckets, objects, and jobs are node-local, so this identifies the node those tools act on.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn get_node_info(
        &self,
        Extension(parts): Extension<http::request::Parts>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        authorize_read(self, &auth, empty_extras("get_node_info")).await?;
        let response = crate::routes::info::run_node_info(&self.state, Some(auth)).await;
        json_output(response)
    }
}

pub(crate) fn group_argument(group_id: &str) -> Result<Ulid, CallToolResult> {
    parse_ulid(
        "group_id",
        group_id,
        "call list_groups for the ids the caller may use",
    )
}

/// The group routes answer a tool caller with bare "Not found" or "Forbidden".
fn group_error(error: crate::error::ServerError) -> CallToolResult {
    match error {
        crate::error::ServerError::NotFound => explained(
            error,
            "no group in this realm carries that group_id; call list_groups for valid ids",
        ),
        crate::error::ServerError::Forbidden => explained(
            error,
            "the caller may not read that group; list_group_members and get_group_usage require membership",
        ),
        error => server_error(error),
    }
}

fn json_output<T: Serialize>(response: T) -> Result<Json<JsonPayload>, CallToolResult> {
    Ok(Json(JsonPayload(
        serde_json::to_value(response).map_err(internal_error)?,
    )))
}

pub(crate) async fn member_groups(
    server: &McpServer,
    auth: &AuthContext,
) -> Result<Vec<MemberGroup>, CallToolResult> {
    let groups = drive(ListGroupOperation::new(), &server.state.get_ctx())
        .await
        .map_err(internal_error)?;
    let mut memberships = Vec::new();
    for Group { group_id, .. } in groups {
        let (group, authorization) = drive(
            GetGroupOperation::new(GetGroupConfig { group_id }),
            &server.state.get_ctx(),
        )
        .await
        .map_err(internal_error)?;
        let mut roles = authorization
            .roles
            .into_values()
            .filter(|role| role.assigned_users.contains(&auth.user_id))
            .map(map_role)
            .collect::<Vec<_>>();
        if roles.is_empty() {
            continue;
        }
        roles.sort_by(|left, right| left.role_id.cmp(&right.role_id));
        memberships.push(MemberGroup {
            group_id,
            output: GroupOutput {
                group_id: group_id.to_string(),
                display_name: group.display_name,
                roles,
            },
        });
    }
    memberships.sort_by_key(|membership| membership.group_id);
    Ok(memberships)
}

/// Directory reads carry no permission path in REST either, so the gate is the
/// realm deny-policy layer and the membership rule each route enforces itself.
async fn authorize_read(
    server: &McpServer,
    auth: &AuthContext,
    extras: PolicyRequestExtras,
) -> Result<(), CallToolResult> {
    super::authorize_self(&server.state, auth, Permission::READ, extras)
        .await
        .map_err(server_error)
}

fn map_role(role: Role) -> RoleOutput {
    RoleOutput {
        role_id: role.role_id.to_string(),
        name: role.name,
        permissions: role
            .permissions
            .into_iter()
            .map(|(path, permission)| (path, permission.to_string()))
            .collect(),
    }
}

fn map_user_error(error: ReadUserDocumentError) -> CallToolResult {
    match error {
        ReadUserDocumentError::NotFound => server_error(crate::error::ServerError::NotFound),
        ReadUserDocumentError::StorageError(error) => internal_error(error),
        ReadUserDocumentError::ConversionError(error) => internal_error(error),
        ReadUserDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        } => internal_error(format!(
            "unexpected user read event in {state}: expected {expected}, got {got}"
        )),
        ReadUserDocumentError::NotFinished => internal_error("user read did not finish"),
    }
}
