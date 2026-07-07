pub mod incoming;
pub mod protocol;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{AuthContext, PlacementRef, RealmConfigDocument};

use crate::auth::{NodeBearerValidationState, validate_aruna_bearer_token};
use crate::driver::{DriverContext, drive};
use crate::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use crate::placement::{placement_ref_for_target, resolve_shard_holders};
use crate::routing::protocol::{
    GroupCall, GroupReply, HolderProxyResponse, ProxiedCall, ProxiedReply,
};

/// Canonical [`DocumentSyncTarget`] a proxied call resolves holders against.
/// `None` for call domains not yet routed through the holder proxy.
pub(crate) fn proxied_call_target(call: &ProxiedCall) -> Option<DocumentSyncTarget> {
    match call {
        ProxiedCall::Group(GroupCall::Get { group_id }) => Some(DocumentSyncTarget::Group {
            group_id: *group_id,
        }),
        ProxiedCall::User(_) | ProxiedCall::Metadata(_) | ProxiedCall::Notification(_) => None,
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
        ProxiedCall::Group(GroupCall::Get { group_id }) => {
            if auth.is_none() {
                return HolderProxyResponse::Rejected("group read requires a bearer token".into());
            }
            match drive(GetGroupOperation::new(GetGroupConfig { group_id }), context).await {
                Ok((group, authorization)) => {
                    HolderProxyResponse::Ok(ProxiedReply::Group(Box::new(GroupReply::Document {
                        group,
                        authorization,
                    })))
                }
                Err(GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound) => {
                    HolderProxyResponse::NotFound
                }
                Err(other) => HolderProxyResponse::Rejected(other.to_string()),
            }
        }
        ProxiedCall::User(_) | ProxiedCall::Metadata(_) | ProxiedCall::Notification(_) => {
            HolderProxyResponse::Rejected("proxied call domain not yet supported".into())
        }
    }
}
