//! Seeds and refreshes device replicas by joining a holder's graph snapshot as an OR-Set.
//! Unpublished local edits survive, and a repeated refresh changes nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;
use rand::seq::SliceRandom;
use tracing::debug;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::protocol::{GraphState, MetadataTransportMessage};
use crate::realm::mutate_placement::node_kind;
use aruna_core::placement::read_holder_sets;

use super::install_graph::InstallGraphOperation;
use super::replica::{ReplicaRecord, ReplicaState};
use super::replica_store::{list_replicas, read_replica, store_replica};
use crate::document_repository::GetConfigOperation;
use crate::realm::load_config::config_unavailable;

/// Replicas one pass refreshes. The rest follow on the next beat, so a large
/// working set converges instead of blocking one pass.
const MAX_REFRESH_PASS: usize = 32;

/// Bounds one document's exchange with the realm.
const REFRESH_BUDGET: Duration = Duration::from_secs(10);

/// Who this device is and whom it may ask.
struct RefreshPlan {
    realm_id: RealmId,
    owner: UserId,
    config: RealmConfigDocument,
}

/// What one refresh achieved. Silence is not a refusal: the replica keeps
/// serving what it already holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshOutcome {
    Installed,
    Refused(SyncRefusal),
    Unreachable,
}

/// Refreshes the least recently synced selected replicas, and answers how many
/// of them a holder served.
pub async fn refresh_replicas(context: &Arc<DriverContext>) -> usize {
    let Some(plan) = refresh_plan(context).await else {
        return 0;
    };
    let Some(mut replicas) = list_replicas(context).await else {
        return 0;
    };
    replicas.retain(|replica| replica.selected);
    replicas.sort_by_key(|replica| replica.last_synced_ms.unwrap_or(0));
    let mut refreshed = 0usize;
    for replica in replicas.into_iter().take(MAX_REFRESH_PASS) {
        if refresh_with_plan(context, &plan, replica).await == RefreshOutcome::Installed {
            refreshed += 1;
        }
    }
    refreshed
}

/// Refreshes one replica now.
pub async fn refresh_replica(context: &Arc<DriverContext>, document_id: Ulid) -> RefreshOutcome {
    let Some(plan) = refresh_plan(context).await else {
        return RefreshOutcome::Unreachable;
    };
    let Some(replica) = read_replica(context, document_id).await else {
        return RefreshOutcome::Unreachable;
    };
    refresh_with_plan(context, &plan, replica).await
}

async fn refresh_plan(context: &Arc<DriverContext>) -> Option<RefreshPlan> {
    let net_handle = context.net_handle.as_ref()?;
    context.metadata_handle.as_ref()?;
    let realm_id = *net_handle.realm_id();
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()?;
    let owner = node_kind(&config, net_handle.node_id()).and_then(|kind| kind.owner())?;
    Some(RefreshPlan {
        realm_id,
        owner,
        config,
    })
}

async fn refresh_with_plan(
    context: &Arc<DriverContext>,
    plan: &RefreshPlan,
    mut replica: ReplicaRecord,
) -> RefreshOutcome {
    let document_id = replica.document_id;
    let Ok(answer) =
        tokio::time::timeout(REFRESH_BUDGET, ask_holders(context, plan, document_id)).await
    else {
        return RefreshOutcome::Unreachable;
    };
    match answer {
        Some(Ok(state)) => {
            let install = InstallGraphOperation::new(replica, *state, unix_timestamp_millis());
            match drive(install, context).await {
                Ok(true) => RefreshOutcome::Installed,
                _ => RefreshOutcome::Unreachable,
            }
        }
        // An authoritative refusal is what the owner has to see; silence only
        // means the realm is out of reach and the replica keeps serving.
        Some(Err(refusal)) => {
            replica.state = ReplicaState::Failed;
            replica.last_error = Some(refusal_reason(&refusal));
            store_replica(context, &replica).await;
            RefreshOutcome::Refused(refusal)
        }
        None => RefreshOutcome::Unreachable,
    }
}

/// What the owner is told about a refusal. The variant alone is the message;
/// an invalid one carries the holder's own words.
pub fn refusal_reason(refusal: &SyncRefusal) -> String {
    match refusal {
        SyncRefusal::Unauthorized => "the realm did not accept this device".to_string(),
        SyncRefusal::Forbidden => "the owner may not read this document".to_string(),
        SyncRefusal::NotFound => "the realm does not know this document".to_string(),
        SyncRefusal::Invalid(reason) => reason.clone(),
        SyncRefusal::Unavailable => "no holder could serve this document".to_string(),
        SyncRefusal::Coordinator(node) => format!("node {node} coordinates this key"),
    }
}

/// Asks the document's holders in turn, stopping at the first that answers.
/// `None` means nobody answered at all.
async fn ask_holders(
    context: &Arc<DriverContext>,
    plan: &RefreshPlan,
    document_id: Ulid,
) -> Option<Result<Box<GraphState>, SyncRefusal>> {
    let metadata = context.metadata_handle.as_ref()?;
    let auth = AuthContext {
        user_id: plan.owner,
        realm_id: plan.realm_id,
        path_restrictions: None,
        session: None,
    };
    for holder in holders_for(plan, document_id) {
        let message = MetadataTransportMessage::FetchGraphState {
            auth_token: AuthToken::internal(auth.clone()),
            document_id,
        };
        match metadata.request_forwarded_write(holder, message).await {
            Ok(MetadataTransportMessage::FetchedGraphState { result: Ok(state) }) => {
                return Some(Ok(state));
            }
            Ok(MetadataTransportMessage::FetchedGraphState {
                result: Err(refusal),
            }) => {
                // An authorization verdict is the same at every holder.
                if matches!(refusal, SyncRefusal::Unauthorized | SyncRefusal::Forbidden) {
                    return Some(Err(refusal));
                }
                debug!(%document_id, refusal = ?refusal, "A holder refused the graph state");
            }
            Ok(other) => {
                debug!(
                    %document_id,
                    message = %crate::metadata::transport_message_kind(&other),
                    "Unexpected answer to a graph state fetch"
                );
            }
            Err(error) => {
                debug!(%document_id, error = %error, "Could not reach a holder for the graph state");
            }
        }
    }
    None
}

/// The document's holders, in a different order every time so one lagging
/// holder never owns this device's view.
fn holders_for(plan: &RefreshPlan, document_id: Ulid) -> Vec<NodeId> {
    let Ok(placement) = resolve_metadata_id(&plan.config, plan.realm_id, None, document_id) else {
        return Vec::new();
    };
    let mut holders = read_holder_sets(&plan.config, &placement).unwrap_or_default();
    holders.shuffle(&mut rand::rng());
    holders
}
