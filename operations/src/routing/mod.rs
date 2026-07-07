pub mod client;
pub mod dispatch;
pub mod incoming;
mod metadata;
mod notification;
pub mod protocol;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{Actor, AuthContext, PlacementRef, RealmConfigDocument};
use aruna_core::types::{GroupId, UserId};

use crate::add_group_role::{AddGroupRoleConfig, AddGroupRoleError, AddGroupRoleOperation};
use crate::add_user_to_group::{AddUserToGroupError, AddUserToGroupInput, AddUserToGroupOperation};
use crate::auth::{NodeBearerValidationState, validate_aruna_bearer_token};
use crate::driver::{DriverContext, drive};
use crate::ensure_canonical_user_token_subject::{
    EnsureCanonicalUserTokenSubjectError, EnsureCanonicalUserTokenSubjectOperation,
};
use crate::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::get_user::{GetUserError, GetUserInput, GetUserOperation};
use crate::notifications::placement::resolve_inbox_holder;
use crate::placement::{placement_ref_for_target, resolve_shard_holders};
use crate::read_user_document::{ReadUserDocumentError, ReadUserDocumentOperation};
use crate::remove_group_role::{
    RemoveGroupRoleConfig, RemoveGroupRoleError, RemoveGroupRoleOperation,
};
use crate::remove_user_from_group::{
    RemoveUserFromGroupError, RemoveUserFromGroupInput, RemoveUserFromGroupOperation,
};
use crate::routing::protocol::{
    GroupCall, GroupReply, HolderProxyResponse, MetadataCall, NotificationCall, ProxiedCall,
    ProxiedReply, UserCall, UserReply,
};
use crate::update_user::{UpdateUserError, UpdateUserInput, UpdateUserOperation};
use aruna_core::errors::AuthorizationError;
use ulid::Ulid;

/// Strategy-selection key for a metadata by-id mutation, loaded from the local
/// registry record. `Create` carries `(group_id, document_path)` on the wire, but
/// `Update`/`Delete` do not — loading them from the everywhere-placed registry
/// record makes by-id mutations resolve the same strategy the create resolved.
#[derive(Debug, Clone)]
pub(crate) struct MetadataStrategyKey {
    pub group_id: GroupId,
    pub document_path: String,
}

/// Outcome of loading a metadata strategy key: `NotFound` when the record is
/// absent (mapped to `NotFound`/`NotHolder` by the caller), `Storage` on a read
/// failure.
pub(crate) enum MetadataStrategyKeyError {
    NotFound,
    Storage(String),
}

/// Loads the `(group_id, document_path)` strategy key for a by-id metadata
/// mutation from the local registry record; every other call yields `None`.
/// Registry records are everywhere-placed, so the origin and the target both load
/// this locally and resolve the identical holder set.
pub(crate) async fn load_metadata_strategy_key(
    context: &DriverContext,
    call: &ProxiedCall,
) -> Result<Option<MetadataStrategyKey>, MetadataStrategyKeyError> {
    let document_id = match call {
        ProxiedCall::Metadata(
            MetadataCall::Update { document_id, .. } | MetadataCall::Delete { document_id },
        ) => *document_id,
        _ => return Ok(None),
    };
    match load_metadata_record_by_document(context, document_id).await {
        Ok(Some(record)) => Ok(Some(MetadataStrategyKey {
            group_id: record.group_id,
            document_path: record.document_path,
        })),
        Ok(None) => Err(MetadataStrategyKeyError::NotFound),
        Err(error) => Err(MetadataStrategyKeyError::Storage(format!(
            "metadata registry read failed: {error:?}"
        ))),
    }
}

/// Canonical [`DocumentSyncTarget`] a proxied call resolves holders against.
/// `None` for call domains not yet routed through the holder proxy. For a by-id
/// metadata mutation the group comes from `metadata_key`, loaded from the record.
pub(crate) fn proxied_call_target(
    call: &ProxiedCall,
    metadata_key: Option<&MetadataStrategyKey>,
) -> Option<DocumentSyncTarget> {
    match call {
        ProxiedCall::Group(group_call) => Some(DocumentSyncTarget::Group {
            group_id: group_call_group_id(group_call),
        }),
        ProxiedCall::User(user_call) => {
            user_call_user_id(user_call).map(|user_id| DocumentSyncTarget::User { user_id })
        }
        ProxiedCall::Metadata(metadata_call) => Some(DocumentSyncTarget::MetadataRegistry {
            group_id: metadata_key
                .map(|key| key.group_id)
                .unwrap_or_else(|| metadata_call_group_id(metadata_call)),
            document_id: metadata_call_document_id(metadata_call),
        }),
        ProxiedCall::Notification(_) => None,
    }
}

/// The metadata document a [`MetadataCall`] resolves holders against.
fn metadata_call_document_id(call: &MetadataCall) -> Ulid {
    match call {
        MetadataCall::Create { document_id, .. }
        | MetadataCall::Delete { document_id }
        | MetadataCall::Update { document_id, .. } => *document_id,
    }
}

/// The owning group when the call carries it on the wire. Only `Create` knows
/// the group up front; by-id mutations supply it through the loaded
/// [`MetadataStrategyKey`] instead, so this nil fallback is unreachable for them.
fn metadata_call_group_id(call: &MetadataCall) -> GroupId {
    match call {
        MetadataCall::Create { group_id, .. } => *group_id,
        MetadataCall::Delete { .. } | MetadataCall::Update { .. } => Ulid::nil(),
    }
}

/// The metadata path a call resolves strategy selection with. `Create` carries it
/// on the wire; by-id mutations supply it through the loaded strategy key so
/// path-prefix bindings (e.g. `profiles/`) steer them onto the create's strategy.
fn proxied_call_metadata_path<'a>(
    call: &'a ProxiedCall,
    metadata_key: Option<&'a MetadataStrategyKey>,
) -> Option<&'a str> {
    if let Some(key) = metadata_key {
        return Some(key.document_path.as_str());
    }
    match call {
        ProxiedCall::Metadata(MetadataCall::Create { document_path, .. }) => {
            Some(document_path.as_str())
        }
        _ => None,
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
    metadata_key: Option<&MetadataStrategyKey>,
) -> Option<(PlacementRef, Vec<NodeId>)> {
    // The notification inbox is not a `DocumentSyncTarget`; its holder comes from
    // the replica-1 inbox resolver rather than shard placement.
    if let ProxiedCall::Notification(notification_call) = call {
        let recipient = notification_call_recipient(notification_call);
        let holders = match resolve_inbox_holder(&recipient, config) {
            Ok(Some(holder)) => vec![holder],
            Ok(None) | Err(_) => Vec::new(),
        };
        return Some((PlacementRef::NIL, holders));
    }
    let target = proxied_call_target(call, metadata_key)?;
    let placement = placement_ref_for_target(
        config,
        &target,
        proxied_call_metadata_path(call, metadata_key),
    );
    let holders = resolve_shard_holders(config, &placement);
    Some((placement, holders))
}

/// The inbox owner a [`NotificationCall`] resolves its holder against.
pub(crate) fn notification_call_recipient(call: &NotificationCall) -> UserId {
    match call {
        NotificationCall::List { recipient, .. }
        | NotificationCall::UnreadCount { recipient }
        | NotificationCall::MarkRead { recipient, .. }
        | NotificationCall::CreateWatch { recipient, .. }
        | NotificationCall::ListWatches { recipient }
        | NotificationCall::DeleteWatch { recipient, .. } => *recipient,
    }
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

/// Refusal for serve arms whose REST routes require unrestricted tokens: the
/// holder re-enforces the gate so a path-restricted (delegated) token cannot
/// reach those operations by arriving over the proxy instead of REST.
pub(crate) fn reject_path_restricted(auth: &AuthContext) -> Option<HolderProxyResponse> {
    if auth.path_restrictions.is_some() {
        return Some(HolderProxyResponse::forbidden(
            "operation requires an unrestricted token",
        ));
    }
    None
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
        ProxiedCall::Metadata(metadata_call) => {
            metadata::serve_metadata_call(context, metadata_call, auth).await
        }
        ProxiedCall::Notification(notification_call) => {
            notification::serve_notification_call(context, notification_call, auth).await
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
        return HolderProxyResponse::forbidden("group operation requires a bearer token");
    };
    // Membership and role mutations mint their permission checks from the caller
    // identity (REST gates them with `require_unrestricted`); reads stay open.
    if !matches!(call, GroupCall::Get { .. })
        && let Some(rejection) = reject_path_restricted(&auth)
    {
        return rejection;
    }
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
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
        GroupCall::AddMember {
            group_id,
            user_id,
            role_ids,
        } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::internal("holder has no net handle");
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
                Err(
                    error @ (AddUserToGroupError::Unauthorized
                    | AddUserToGroupError::CheckPermissionsError(_)),
                ) => HolderProxyResponse::forbidden(error.to_string()),
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
        GroupCall::RemoveMember {
            group_id,
            user_id,
            role_ids,
        } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::internal("holder has no net handle");
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
                Err(RemoveUserFromGroupError::LastAdmin) => {
                    HolderProxyResponse::conflict("cannot remove the last admin of a group")
                }
                Err(
                    error @ (RemoveUserFromGroupError::Unauthorized
                    | RemoveUserFromGroupError::CheckPermissionsError(_)),
                ) => HolderProxyResponse::forbidden(error.to_string()),
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
        GroupCall::AddRole { group_id, role } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::internal("holder has no net handle");
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
                Err(
                    error @ (AddGroupRoleError::Unauthorized
                    | AddGroupRoleError::CheckPermissionsError(_)),
                ) => HolderProxyResponse::forbidden(error.to_string()),
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
        GroupCall::RemoveRole { group_id, role_id } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::internal("holder has no net handle");
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
                Err(
                    RemoveGroupRoleError::RoleNotFound
                    | RemoveGroupRoleError::AuthDocNotFound
                    | RemoveGroupRoleError::GroupNotFound,
                ) => HolderProxyResponse::NotFound,
                Err(RemoveGroupRoleError::AdminRoleUndeletable) => {
                    HolderProxyResponse::conflict("the admin role of a group cannot be deleted")
                }
                Err(
                    error @ (RemoveGroupRoleError::Unauthorized
                    | RemoveGroupRoleError::CheckPermissionsError(_)),
                ) => HolderProxyResponse::forbidden(error.to_string()),
                Err(other) => HolderProxyResponse::internal(other.to_string()),
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
        return HolderProxyResponse::forbidden("user operation requires a bearer token");
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
                Err(error @ (GetUserError::Unauthorized | GetUserError::AuthorizationError(_))) => {
                    HolderProxyResponse::forbidden(error.to_string())
                }
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
        UserCall::Update {
            user_id,
            name,
            set_attributes,
            remove_attributes,
        } => {
            let Some(actor) = local_actor(context, &auth) else {
                return HolderProxyResponse::internal("holder has no net handle");
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
                Err(
                    error @ (UpdateUserError::InvalidUserName
                    | UpdateUserError::InvalidAttributeKey(_)
                    | UpdateUserError::InvalidAttributeValue(_)
                    | UpdateUserError::TooManyAttributes),
                ) => HolderProxyResponse::bad_request(error.to_string()),
                Err(
                    error
                    @ (UpdateUserError::Unauthorized | UpdateUserError::AuthorizationError(_)),
                ) => HolderProxyResponse::forbidden(error.to_string()),
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
        // `user_id` is a routing hint only; the served identity is the validated
        // bearer subject, never the wire claim.
        UserCall::ReadDocument { .. } => {
            match drive(ReadUserDocumentOperation::new(auth.user_id), context).await {
                Ok(user) => ok_user(UserReply::User(user)),
                Err(ReadUserDocumentError::NotFound) => HolderProxyResponse::NotFound,
                Err(other) => HolderProxyResponse::internal(other.to_string()),
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
                Err(
                    error @ (EnsureCanonicalUserTokenSubjectError::Unauthorized
                    | EnsureCanonicalUserTokenSubjectError::Forbidden),
                ) => HolderProxyResponse::forbidden(error.to_string()),
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::protocol::{MetadataCall, MetadataCreatePayload};
    use aruna_core::structs::{
        BindingScope, DEFAULT_SHARD_COUNT, DocumentClass, PlacementStrategy, RealmId,
        RealmNodeKind, StrategyBinding,
    };
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn strategy(name: &str, replica_count: Option<u32>) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id: Ulid::new(),
            name: name.to_string(),
            replica_count,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: DEFAULT_SHARD_COUNT,
        }
    }

    fn create_call(path: &str) -> ProxiedCall {
        ProxiedCall::Metadata(MetadataCall::Create {
            group_id: Ulid::nil(),
            document_id: Ulid::new(),
            document_path: path.to_string(),
            public: true,
            payload: MetadataCreatePayload::RoCrate {
                jsonld: "{}".to_string(),
            },
        })
    }

    // A `profiles/` MetadataPathPrefix binding steers a profile create onto its
    // bound (replica-1) strategy's holder, while any other path falls to the
    // everywhere class binding — proving the routing layer threads the create
    // path into holder resolution.
    #[test]
    fn metadata_path_prefix_binding_steers_create_holders() {
        let everywhere = strategy("everywhere", None);
        let profiles = strategy("profiles", Some(1));
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
        config.default_strategy_id = Some(everywhere.strategy_id);
        config.strategy_bindings = vec![
            StrategyBinding {
                scope: BindingScope::Class(DocumentClass::MetadataRegistry),
                strategy_id: everywhere.strategy_id,
            },
            StrategyBinding {
                scope: BindingScope::MetadataPathPrefix("profiles/".to_string()),
                strategy_id: profiles.strategy_id,
            },
        ];
        config.strategies = vec![everywhere, profiles];
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }

        let (_, profile_holders) =
            resolve_call_holders(&config, &create_call("profiles/team"), None)
                .expect("profile create resolves holders");
        let (_, dataset_holders) =
            resolve_call_holders(&config, &create_call("datasets/run"), None)
                .expect("dataset create resolves holders");

        assert_eq!(
            profile_holders.len(),
            1,
            "profiles pin to the replica-1 strategy"
        );
        assert_eq!(
            dataset_holders.len(),
            4,
            "other paths ride the everywhere strategy"
        );
    }
}
