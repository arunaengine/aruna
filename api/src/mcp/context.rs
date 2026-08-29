use super::{McpServer, authorize_tool, empty_extras, internal_error, request_auth, server_error};
use aruna_core::structs::{AuthContext, Group, Permission, Role};
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::list_groups::ListGroupOperation;
use aruna_operations::read_realm_authorization::ReadRealmAuthorizationOperation;
use aruna_operations::read_user_document::{ReadUserDocumentError, ReadUserDocumentOperation};
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::Serialize;
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
        description = "Describe the authenticated Aruna user and their assigned realm and group roles without returning credentials",
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
        let user = drive(
            ReadUserDocumentOperation::new(auth.user_id),
            &self.state.get_ctx(),
        )
        .await
        .map_err(map_user_error)?;
        let groups = member_groups(self, &auth).await?;
        authorize_context(self, &auth, "whoami").await?;
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
        description = "List the authenticated user's Aruna groups and assigned roles",
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
        let groups = member_groups(self, &auth).await?;
        for group in &groups {
            authorize_group(self, &auth, group.group_id, "list_groups").await?;
        }
        Ok(Json(GroupsOutput {
            groups: groups.into_iter().map(|group| group.output).collect(),
        }))
    }
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
    memberships.sort_by(|left, right| left.group_id.cmp(&right.group_id));
    Ok(memberships)
}

async fn authorize_context(
    server: &McpServer,
    auth: &AuthContext,
    tool: &str,
) -> Result<(), CallToolResult> {
    super::authorize_self(&server.state, auth, Permission::READ, empty_extras(tool))
        .await
        .map_err(server_error)
}

async fn authorize_group(
    server: &McpServer,
    auth: &AuthContext,
    group_id: Ulid,
    tool: &str,
) -> Result<(), CallToolResult> {
    authorize_tool(
        &server.state,
        auth,
        group_path(server, group_id),
        Permission::READ,
        empty_extras(tool),
    )
    .await
    .map_err(server_error)
}

fn group_path(server: &McpServer, group_id: Ulid) -> String {
    format!("/{}/g/{group_id}/data/**", server.state.get_realm_id())
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
