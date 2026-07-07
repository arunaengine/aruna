pub mod client;
pub mod dispatch;
pub mod incoming;
pub mod protocol;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{Actor, AuthContext, PlacementRef, RealmConfigDocument};
use aruna_core::types::{GroupId, UserId};

use crate::add_group_role::{AddGroupRoleConfig, AddGroupRoleError, AddGroupRoleOperation};
use crate::add_user_to_group::{AddUserToGroupError, AddUserToGroupInput, AddUserToGroupOperation};
use crate::auth::{NodeBearerValidationState, validate_aruna_bearer_token};
use crate::driver::{DriverContext, drive};
use crate::ensure_canonical_user_token_subject::EnsureCanonicalUserTokenSubjectOperation;
use crate::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use crate::get_user::{GetUserError, GetUserInput, GetUserOperation};
use crate::placement::{placement_ref_for_target, resolve_shard_holders};
use crate::read_user_document::{ReadUserDocumentError, ReadUserDocumentOperation};
use crate::remove_group_role::{
    RemoveGroupRoleConfig, RemoveGroupRoleError, RemoveGroupRoleOperation,
};
use crate::remove_user_from_group::{
    RemoveUserFromGroupError, RemoveUserFromGroupInput, RemoveUserFromGroupOperation,
};
use crate::routing::protocol::{
    GroupCall, GroupReply, HolderProxyResponse, ProxiedCall, ProxiedReply, UserCall, UserReply,
};
use crate::update_user::{UpdateUserError, UpdateUserInput, UpdateUserOperation};
use aruna_core::errors::AuthorizationError;

/// Canonical [`DocumentSyncTarget`] a proxied call resolves holders against.
/// `None` for call domains not yet routed through the holder proxy.
pub(crate) fn proxied_call_target(call: &ProxiedCall) -> Option<DocumentSyncTarget> {
    match call {
        ProxiedCall::Group(group_call) => Some(DocumentSyncTarget::Group {
            group_id: group_call_group_id(group_call),
        }),
        ProxiedCall::User(user_call) => {
            user_call_user_id(user_call).map(|user_id| DocumentSyncTarget::User { user_id })
        }
        ProxiedCall::Metadata(_) | ProxiedCall::Notification(_) => None,
    }
}

/// The group subject shard every [`GroupCall`] resolves holders against.
fn group_call_group_id(call: &GroupCall) -> GroupId {
    match call {
        GroupCall::Get { group_id }
        | GroupCall::AddMember { group_id, .. }
        | GroupCall::RemoveMember { group_id, .. }
        | GroupCall::AddRole { group_id, .. }
        | GroupCall::RemoveRole { group_id, .. } => *group_id,
    }
}

/// The user subject shard a [`UserCall`] resolves holders against. For the
/// by-id calls the id is a string the target re-parses; a syntactically invalid
/// id yields `None`, surfacing as an internal error rather than a mis-route.
fn user_call_user_id(call: &UserCall) -> Option<UserId> {
    match call {
        UserCall::Get { user_id } | UserCall::Update { user_id, .. } => {
            UserId::from_string(user_id).ok()
        }
        UserCall::ReadDocument { user_id } | UserCall::EnsureCanonicalTokenSubject { user_id } => {
            Some(*user_id)
        }
    }
}

/// Resolves the rank-ordered holder set for a proxied call's subject shard,
/// reusing the placement helpers so holder resolution is identical everywhere.
/// `None` when the call domain is unroutable.
pub(crate) fn resolve_call_holders(
    config: &RealmConfigDocument,
    call: &ProxiedCall,
) -> Option<(PlacementRef, Vec<NodeId>)> {
    let target = proxied_call_target(call)?;
    let placement = placement_ref_for_target(config, &target, None);
    let holders = resolve_shard_holders(config, &placement);
    Some((placement, holders))
}

/// Validates a forwarded bearer against this node's trust configuration and
/// rebuilds the auth context. `None` token yields `Ok(None)`; per-call
/// requirements (e.g. bearer-required reads) are enforced in [`serve_local`].
pub(crate) async fn validate_proxy_bearer(
    context: &DriverContext,
    token: Option<&str>,
) -> Result<Option<AuthContext>, String> {
    let Some(token) = token else {
        return Ok(None);
    };
    let state = NodeBearerValidationState::new(context.storage_handle.clone());
    validate_aruna_bearer_token(&state, token)
        .await
        .map(Some)
        .map_err(|error| format!("invalid bearer token: {error}"))
}

/// Drives the local operation backing a proxied call. Shared by the inbound
/// handler (after the loop guard) and the dispatch local arm, so the routed and
/// direct code paths are identical.
pub(crate) async fn serve_local(
    context: &DriverContext,
    call: ProxiedCall,
    auth: Option<AuthContext>,
) -> HolderProxyResponse {
    match call {
        ProxiedCall::Group(group_call) => serve_group_call(context, group_call, auth).await,
        ProxiedCall::User(user_call) => serve_user_call(context, user_call, auth).await,
        ProxiedCall::Metadata(_) | ProxiedCall::Notification(_) => {
            HolderProxyResponse::Rejected("proxied call domain not yet supported".into())
        }
    }
}

fn ok_group(reply: GroupReply) -> HolderProxyResponse {
    HolderProxyResponse::Ok(ProxiedReply::Group(Box::new(reply)))
}

fn ok_user(reply: UserReply) -> HolderProxyResponse {
    HolderProxyResponse::Ok(ProxiedReply::User(Box::new(reply)))
}

/// Actor stamped onto a routed write: the holder is the authoritative writer,
/// so `node_id` is the serving node's, while the identity comes from the
/// validated bearer.
fn local_actor(context: &DriverContext, auth: &AuthContext) -> Option<Actor> {
    let net_handle = context.net_handle.as_ref()?;
    Some(Actor {
        node_id: net_handle.node_id(),
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    })
}

async fn serve_group_call(
    context: &DriverContext,
    call: GroupCall,
    auth: Option<AuthContext>,
) -> HolderProxyResponse {
    let Some(auth) = auth else {
        return HolderProxyResponse::Rejected("group operation requires a bearer token".into());
    };
    match call {
        GroupCall::Get { group_id } => {
            match drive(GetGroupOperation::new(GetGroupConfig { group_id }), context).await {
                Ok((group, authorization)) => ok_group(GroupReply::Document {
                    group,
                    authorization,
                }),
                Err(GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound) => {
                    HolderProxyResponse::NotFound
                }
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
        GroupCall::AddMember {
            group_id,
            user_id,
            role_ids,
        } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::Rejected("holder has no net handle".into());
            };
            match drive(
                AddUserToGroupOperation::new(AddUserToGroupInput {
                    actor,
                    group_id,
                    user_id,
                    role_ids,
                }),
                context,
            )
            .await
            {
                Ok(authorization) => ok_group(GroupReply::Authorization(authorization)),
                Err(AddUserToGroupError::RoleNotFound | AddUserToGroupError::AuthDocNotFound) => {
                    HolderProxyResponse::NotFound
                }
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
        GroupCall::RemoveMember {
            group_id,
            user_id,
            role_ids,
        } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::Rejected("holder has no net handle".into());
            };
            match drive(
                RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
                    actor,
                    group_id,
                    user_id,
                    role_ids,
                }),
                context,
            )
            .await
            {
                Ok(_) => ok_group(GroupReply::Ack),
                Err(
                    RemoveUserFromGroupError::RoleNotFound
                    | RemoveUserFromGroupError::AuthDocNotFound,
                ) => HolderProxyResponse::NotFound,
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
        GroupCall::AddRole { group_id, role } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::Rejected("holder has no net handle".into());
            };
            let realm_id = auth.realm_id;
            match drive(
                AddGroupRoleOperation::new(AddGroupRoleConfig {
                    auth_context: auth,
                    actor,
                    realm_id,
                    group_id,
                    role: *role,
                }),
                context,
            )
            .await
            {
                Ok((_, authorization)) => ok_group(GroupReply::Authorization(authorization)),
                Err(
                    AddGroupRoleError::GroupNotFound
                    | AddGroupRoleError::CheckPermissionsError(
                        AuthorizationError::GroupNotFound | AuthorizationError::AuthDocNotFound,
                    ),
                ) => HolderProxyResponse::NotFound,
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
        GroupCall::RemoveRole { group_id, role_id } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::Rejected("holder has no net handle".into());
            };
            let realm_id = auth.realm_id;
            match drive(
                RemoveGroupRoleOperation::new(RemoveGroupRoleConfig {
                    auth_context: auth,
                    actor,
                    realm_id,
                    group_id,
                    role_id,
                }),
                context,
            )
            .await
            {
                Ok(_) => ok_group(GroupReply::Ack),
                Err(RemoveGroupRoleError::RoleNotFound | RemoveGroupRoleError::AuthDocNotFound) => {
                    HolderProxyResponse::NotFound
                }
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
    }
}

async fn serve_user_call(
    context: &DriverContext,
    call: UserCall,
    auth: Option<AuthContext>,
) -> HolderProxyResponse {
    let Some(auth) = auth else {
        return HolderProxyResponse::Rejected("user operation requires a bearer token".into());
    };
    match call {
        UserCall::Get { user_id } => {
            let self_realm_id = auth.realm_id;
            match drive(
                GetUserOperation::new(GetUserInput {
                    auth_context: auth,
                    self_realm_id,
                    user_id,
                }),
                context,
            )
            .await
            {
                Ok(user) => ok_user(UserReply::User(user)),
                Err(GetUserError::UserNotFound) => HolderProxyResponse::NotFound,
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
        UserCall::Update {
            user_id,
            name,
            set_attributes,
            remove_attributes,
        } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::Rejected("holder has no net handle".into());
            };
            let self_realm_id = auth.realm_id;
            match drive(
                UpdateUserOperation::new(UpdateUserInput {
                    actor,
                    auth_context: auth,
                    self_realm_id,
                    user_id,
                    name,
                    set_attributes,
                    remove_attributes,
                }),
                context,
            )
            .await
            {
                Ok(user) => ok_user(UserReply::User(user)),
                Err(UpdateUserError::UserNotFound) => HolderProxyResponse::NotFound,
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
        // `user_id` is a routing hint only; the served identity is the validated
        // bearer subject, never the wire claim.
        UserCall::ReadDocument { .. } => {
            match drive(ReadUserDocumentOperation::new(auth.user_id), context).await {
                Ok(user) => ok_user(UserReply::User(user)),
                Err(ReadUserDocumentError::NotFound) => HolderProxyResponse::NotFound,
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
        UserCall::EnsureCanonicalTokenSubject { .. } => {
            match drive(
                EnsureCanonicalUserTokenSubjectOperation::new(auth.user_id),
                context,
            )
            .await
            {
                Ok(()) => ok_user(UserReply::TokenSubjectEnsured),
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
    }
}
