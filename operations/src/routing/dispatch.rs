use aruna_core::NodeId;
use aruna_core::structs::{RealmConfigDocument, RealmId};
use thiserror::Error;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use crate::routing::client::{HolderProxySendError, send_holder_proxy_request};
use crate::routing::protocol::{
    HolderProxyRequest, HolderProxyResponse, ProxiedCall, ProxiedReply, ProxyBearerToken,
    RejectKind,
};
use crate::routing::{
    MetadataStrategyKey, MetadataStrategyKeyError, load_metadata_strategy_key,
    resolve_call_holders, serve_local, validate_proxy_bearer,
};

/// Outcome of routing a proxied call. Designed so the api layer maps
/// `NotFound` → 404, `Unavailable` → 503 (+Retry-After), `Unauthorized` → 403,
/// `Conflict` → 409, `BadRequest` → 400, and `Internal` → 500. Holder rejections
/// carry a typed [`RejectKind`] so a domain error keeps its status across the proxy.
#[derive(Debug, Error)]
pub enum HolderRoutingError {
    #[error("resource not found")]
    NotFound,
    #[error("no reachable holder for the requested resource")]
    Unavailable,
    #[error("holder rejected the request: {0}")]
    Unauthorized(String),
    #[error("holder rejected the request: {0}")]
    Conflict(String),
    #[error("holder rejected the request: {0}")]
    BadRequest(String),
    #[error("{0}")]
    Internal(String),
}

/// Maps a holder's typed rejection onto the routing error the api layer maps to a
/// status. `Unavailable` drops its reason to match the general 503 path.
fn rejection_to_routing_error(kind: RejectKind, reason: String) -> HolderRoutingError {
    match kind {
        RejectKind::Forbidden => HolderRoutingError::Unauthorized(reason),
        RejectKind::Conflict => HolderRoutingError::Conflict(reason),
        RejectKind::BadRequest => HolderRoutingError::BadRequest(reason),
        RejectKind::Unavailable => HolderRoutingError::Unavailable,
        RejectKind::Internal => HolderRoutingError::Internal(reason),
    }
}

/// What the origin does after a send failure, keyed on the call's write-vs-read
/// class. Reads retry the next holder on any failure. Mutations retry only on a
/// connect failure — the request provably never reached the holder; once bytes
/// may have been delivered, retrying elsewhere risks a double apply, so the
/// caller gets a retryable `Unavailable` instead.
#[derive(Debug, PartialEq, Eq)]
enum SendFailureAction {
    NextHolder,
    Unavailable,
}

fn send_failure_action(error: &HolderProxySendError, is_mutation: bool) -> SendFailureAction {
    match error {
        HolderProxySendError::Connect(_) => SendFailureAction::NextHolder,
        HolderProxySendError::Io(_) if is_mutation => SendFailureAction::Unavailable,
        HolderProxySendError::Io(_) => SendFailureAction::NextHolder,
    }
}

/// Routes a user-authorized call to a current holder of its subject shard.
///
/// Serves locally when this node holds the shard; otherwise forwards to remote
/// holders in resolver rank order over [`Alpn::HolderProxy`](aruna_core::alpn::Alpn::HolderProxy)
/// with `hop = 0`. Send failures fall through to the next holder under the
/// [`send_failure_action`] policy; a `NotHolder` from every reachable holder
/// refreshes the realm config once and retries; a `NotFound` or `Rejected` from
/// a reachable holder is definitive.
pub async fn dispatch_holder_call(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
    call: ProxiedCall,
) -> Result<ProxiedReply, HolderRoutingError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(HolderRoutingError::Unavailable)?;
    let realm_id = *net_handle.realm_id();

    // Forwarded token is capped before it ever hits the wire.
    let forwarded_bearer = bearer
        .map(ProxyBearerToken::new)
        .transpose()
        .map_err(|error| HolderRoutingError::Internal(error.to_string()))?;

    // A by-id metadata mutation resolves its strategy from the local registry
    // record, so update/delete route to the same holder the create resolved. The
    // record is everywhere-placed; an absent one is an authoritative NotFound.
    let metadata_key = match load_metadata_strategy_key(context, &call).await {
        Ok(key) => key,
        Err(MetadataStrategyKeyError::NotFound) => return Err(HolderRoutingError::NotFound),
        Err(MetadataStrategyKeyError::Storage(reason)) => {
            return Err(HolderRoutingError::Internal(reason));
        }
    };

    let config = get_realm_config(context, realm_id).await?;
    match run_pass(
        context,
        local_node_id,
        realm_id,
        bearer,
        forwarded_bearer.as_ref(),
        &call,
        metadata_key.as_ref(),
        &config,
    )
    .await?
    {
        PassOutcome::Done(reply) => return Ok(reply),
        PassOutcome::StaleView => {}
    }

    // A stale holder view: refresh the realm config once and retry.
    let config = get_realm_config(context, realm_id).await?;
    match run_pass(
        context,
        local_node_id,
        realm_id,
        bearer,
        forwarded_bearer.as_ref(),
        &call,
        metadata_key.as_ref(),
        &config,
    )
    .await?
    {
        PassOutcome::Done(reply) => Ok(reply),
        PassOutcome::StaleView => Err(HolderRoutingError::Unavailable),
    }
}

enum PassOutcome {
    Done(ProxiedReply),
    StaleView,
}

#[allow(clippy::too_many_arguments)]
async fn run_pass(
    context: &DriverContext,
    local_node_id: NodeId,
    realm_id: RealmId,
    bearer: Option<&str>,
    forwarded_bearer: Option<&ProxyBearerToken>,
    call: &ProxiedCall,
    metadata_key: Option<&MetadataStrategyKey>,
    config: &RealmConfigDocument,
) -> Result<PassOutcome, HolderRoutingError> {
    let Some((_placement, holders)) = resolve_call_holders(config, call, metadata_key) else {
        return Err(HolderRoutingError::Internal(
            "proxied call domain not yet supported".to_string(),
        ));
    };
    if holders.is_empty() {
        return Ok(PassOutcome::StaleView);
    }

    // Local node holds the shard: serve directly, same path as the inbound handler.
    if holders.contains(&local_node_id) {
        let auth = validate_proxy_bearer(context, bearer)
            .await
            .map_err(HolderRoutingError::Unauthorized)?;
        return map_local_response(serve_local(context, call.clone(), auth).await);
    }

    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(HolderRoutingError::Unavailable)?;

    let is_mutation = call.is_mutation();
    let mut saw_not_holder = false;
    for holder in holders {
        if holder == local_node_id {
            continue;
        }
        let request = HolderProxyRequest {
            realm_id,
            bearer: forwarded_bearer.cloned(),
            hop: 0,
            call: call.clone(),
        };
        match send_holder_proxy_request(net_handle, holder, request).await {
            Ok(HolderProxyResponse::Ok(reply)) => return Ok(PassOutcome::Done(reply)),
            Ok(HolderProxyResponse::NotFound) => return Err(HolderRoutingError::NotFound),
            Ok(HolderProxyResponse::Rejected { kind, reason }) => {
                return Err(rejection_to_routing_error(kind, reason));
            }
            Ok(HolderProxyResponse::NotHolder) => saw_not_holder = true,
            Err(error) => match send_failure_action(&error, is_mutation) {
                SendFailureAction::NextHolder => continue,
                SendFailureAction::Unavailable => return Err(HolderRoutingError::Unavailable),
            },
        }
    }

    if saw_not_holder {
        Ok(PassOutcome::StaleView)
    } else {
        Err(HolderRoutingError::Unavailable)
    }
}

fn map_local_response(response: HolderProxyResponse) -> Result<PassOutcome, HolderRoutingError> {
    match response {
        HolderProxyResponse::Ok(reply) => Ok(PassOutcome::Done(reply)),
        HolderProxyResponse::NotFound => Err(HolderRoutingError::NotFound),
        HolderProxyResponse::Rejected { kind, reason } => {
            Err(rejection_to_routing_error(kind, reason))
        }
        // The local node was resolved into the holder set, so it cannot answer
        // `NotHolder`; treat any such inconsistency as an internal error.
        HolderProxyResponse::NotHolder => Err(HolderRoutingError::Internal(
            "local holder reported NotHolder".to_string(),
        )),
    }
}

async fn get_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, HolderRoutingError> {
    drive(GetRealmConfigOperation::new(realm_id), context)
        .await
        .map_err(|error| match error {
            GetRealmConfigError::DocumentNotFound => HolderRoutingError::Unavailable,
            other => HolderRoutingError::Internal(other.to_string()),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connect() -> HolderProxySendError {
        HolderProxySendError::Connect("dial failed".to_string())
    }

    fn io() -> HolderProxySendError {
        HolderProxySendError::Io("timed out waiting for holder proxy response".to_string())
    }

    // Reads retry the next holder on every failure; mutations retry only when
    // the request provably never reached the holder.
    #[test]
    fn send_failure_action_guards_mutations_after_delivery() {
        assert_eq!(
            send_failure_action(&connect(), false),
            SendFailureAction::NextHolder
        );
        assert_eq!(
            send_failure_action(&io(), false),
            SendFailureAction::NextHolder
        );
        assert_eq!(
            send_failure_action(&connect(), true),
            SendFailureAction::NextHolder
        );
        assert_eq!(
            send_failure_action(&io(), true),
            SendFailureAction::Unavailable
        );
    }
}
