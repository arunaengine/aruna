//! Enforcement seam for the deny-only CEL request policies. Called at the
//! request choke points after authorization allowed the action and before it
//! executes; policy compile and evaluation failures deny (fail-closed).

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use aruna_core::request_policy::{PolicyDecision, PolicyRequest, evaluate_policies};
use aruna_core::structs::RealmId;
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error, PartialEq)]
pub enum PolicyEnforcementError {
    #[error("request denied by policy `{name}`: {reason}")]
    Denied { name: String, reason: String },
}

/// Evaluates the realm's deny policies against one request. An absent realm
/// config carries no policies and allows; a stored policy that cannot be
/// compiled or evaluated denies.
pub async fn enforce_policies(
    context: &DriverContext,
    realm_id: RealmId,
    request: &PolicyRequest,
) -> Result<(), PolicyEnforcementError> {
    let Ok(config) = drive(GetRealmConfigOperation::new(realm_id), context).await else {
        return Ok(());
    };
    if config.deny_policies.is_empty() {
        return Ok(());
    }
    match evaluate_policies(&config.deny_policies, request) {
        PolicyDecision::Allowed => Ok(()),
        PolicyDecision::Denied {
            policy_id,
            name,
            reason,
        } => {
            warn!(
                policy_id = %policy_id,
                policy = %name,
                path = %request.path,
                permission = %request.permission,
                "Request denied by realm policy"
            );
            Err(PolicyEnforcementError::Denied { name, reason })
        }
    }
}

/// Builds the policy request for one authorized action.
pub fn policy_request(
    path: &str,
    permission: &aruna_core::structs::Permission,
    user: Option<&aruna_core::UserId>,
) -> PolicyRequest {
    PolicyRequest {
        path: path.to_string(),
        permission: match permission {
            aruna_core::structs::Permission::READ => "read".to_string(),
            aruna_core::structs::Permission::WRITE => "write".to_string(),
            aruna_core::structs::Permission::DENY => "deny".to_string(),
        },
        user: user
            .filter(|user| !user.is_nil())
            .map(|user| user.to_string())
            .unwrap_or_default(),
    }
}
