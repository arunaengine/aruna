use aruna_core::NodeId;
use aruna_core::structs::{RealmConfigDocument, RealmId};
use thiserror::Error;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use crate::routing::client::send_holder_proxy_request;
use crate::routing::protocol::{
    HolderProxyRequest, HolderProxyResponse, ProxiedCall, ProxiedReply, ProxyBearerToken,
};
use crate::routing::{resolve_call_holders, serve_local, validate_proxy_bearer};

/// Outcome of routing a proxied call. Designed so the api layer maps
/// `NotFound` → 404, `Unavailable` → 503 (+Retry-After), `Remote` → 502,
/// `Unauthorized` → 401/403, and `Internal` → 500.
#[derive(Debug, Error)]
pub enum HolderRoutingError {
    #[error("resource not found")]
    NotFound,
    #[error("no reachable holder for the requested resource")]
    Unavailable,
    #[error("holder proxy error: {0}")]
    Remote(String),
    #[error("holder rejected the request: {0}")]
    Unauthorized(String),
    #[error("{0}")]
    Internal(String),
}

/// Routes a user-authorized call to a current holder of its subject shard.
///
/// Serves locally when this node holds the shard; otherwise forwards to remote
/// holders in resolver rank order over [`Alpn::HolderProxy`](aruna_core::alpn::Alpn::HolderProxy)
/// with `hop = 0`. Connect/I/O failures fall through to the next holder; a
/// `NotHolder` from every reachable holder refreshes the realm config once and
/// retries; a `NotFound` or `Rejected` from a reachable holder is definitive.
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

    let config = get_realm_config(context, realm_id).await?;
    match run_pass(
        context,
        local_node_id,
        realm_id,
        bearer,
        forwarded_bearer.as_ref(),
        &call,
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

async fn run_pass(
    context: &DriverContext,
    local_node_id: NodeId,
    realm_id: RealmId,
    bearer: Option<&str>,
    forwarded_bearer: Option<&ProxyBearerToken>,
    call: &ProxiedCall,
    config: &RealmConfigDocument,
) -> Result<PassOutcome, HolderRoutingError> {
    let Some((_placement, holders)) = resolve_call_holders(config, call) else {
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
            Ok(HolderProxyResponse::Rejected(reason)) => {
                return Err(HolderRoutingError::Unauthorized(reason));
            }
            Ok(HolderProxyResponse::NotHolder) => saw_not_holder = true,
            Err(_unreachable) => continue,
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
        HolderProxyResponse::Rejected(reason) => Err(HolderRoutingError::Unauthorized(reason)),
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
