//! Seeding and refreshing the replicas this device keeps.
//!
//! A device holds no bucket, so nothing pushes a metadata document to it. It
//! asks a holder for the document's graph state and joins the snapshot into its
//! own replica: an OR-Set union, so a refresh never drops an edit this device
//! has not published yet and repeating it changes nothing.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::metadata::{MetadataAuthToken, MetadataEffect, MetadataEvent};
use aruna_core::structs::{AuthContext, RealmConfigDocument, RealmId, SyncRefusal};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use rand::seq::SliceRandom;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::create_metadata_document::resolve_metadata_id;
use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::metadata::protocol::{GraphState, MetadataTransportMessage};
use crate::mutate_realm_placement::node_kind;
use crate::placement::read_holder_sets;

use super::replica::{ReplicaRecord, ReplicaState, list_replicas, read_replica, store_replica};

/// Replicas one pass refreshes. The rest follow on the next beat, so a large
/// working set converges instead of blocking one pass.
pub const MAX_REFRESH_PASS: usize = 32;

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
    let config = load_realm_config(context, realm_id).await?;
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
        Some(Ok(state)) => match install_state(context, &mut replica, *state).await {
            true => RefreshOutcome::Installed,
            false => RefreshOutcome::Unreachable,
        },
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
    };
    for holder in holders_for(plan, document_id) {
        let message = MetadataTransportMessage::FetchGraphState {
            auth_token: MetadataAuthToken::internal(auth.clone()),
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

/// Joins one holder's answer into the local replica.
async fn install_state(
    context: &Arc<DriverContext>,
    replica: &mut ReplicaRecord,
    state: GraphState,
) -> bool {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return false;
    };
    let graph_iri = state.record.graph_iri.clone();
    let clock = state.snapshot.clock.clone();
    match metadata
        .send_metadata_effect(MetadataEffect::InstallSnapshot {
            graph_iri: graph_iri.clone(),
            snapshot: Box::new(state.snapshot),
        })
        .await
    {
        Event::Metadata(MetadataEvent::SnapshotInstalled { .. }) => {}
        other => {
            warn!(document_id = %replica.document_id, event = ?other, "Could not install a graph snapshot");
            return false;
        }
    }
    replica.local_clock.merge(&clock);
    replica.realm_clock = clock;
    replica.last_synced_ms = Some(unix_timestamp_millis());
    replica.displayed_jsonld = state.displayed_jsonld;
    replica.dataset_digest = state.dataset_digest;
    replica.group_id = state.record.group_id;
    replica.document_path = state.record.document_path.clone();
    replica.graph_iri = graph_iri;
    replica.record = Some(Box::new(state.record));
    replica.last_error = None;
    replica.findings = state.findings;
    replica.state = match state.findings {
        0 => ReplicaState::Synced,
        _ => ReplicaState::Invalid,
    };
    store_replica(context, replica).await
}
