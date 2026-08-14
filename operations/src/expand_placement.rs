//! Machine-initiated placement expansion at onboarding.
//!
//! A joining node no longer siphons buckets by merely existing: holder sets are
//! pinned to an activated candidate map, so a below-RF realm needs an explicit
//! transition to reach RF. This issues one - but only for buckets whose target
//! set contains their current set. Nothing moves off any node, the old holders
//! stay write authority throughout, and the grace release is a no-op, yet the
//! full barrier/pull/verify/proof machinery still runs. Weight changes,
//! removals, and drains are never auto-issued.

use aruna_core::errors::StorageError;
use aruna_core::structs::{Actor, CandidatePlacementMap, RealmConfigDocument, TransitionLimits};
use aruna_core::util::unix_timestamp_millis;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::mutate_realm_placement::{
    MutateRealmPlacementConfig, MutateRealmPlacementError, MutateRealmPlacementOperation,
    RealmPlacementMutation,
};
use crate::placement::transition::{TransitionRequest, expansion_buckets, plan_transition};

/// Publishes the realm's first candidate map and hands every strategy's
/// activations to the reducer.
///
/// While no map exists, resolution runs over the live view - the bootstrap
/// bridge, which is exactly right for the single node that created the realm
/// and must be closed before a second node is registered, or the join itself
/// would move buckets. A literal activation written into a document is not
/// enough: only activations initialized through the reducer advance when a
/// transition completes.
pub async fn ensure_activated_map(
    context: &DriverContext,
    actor: &Actor,
) -> Result<RealmConfigDocument, MutateRealmPlacementError> {
    let mut config = read_config(context, actor).await?;
    let epoch = match config.newest_map_epoch() {
        Some(epoch) => epoch,
        None => {
            let (epoch, map) = next_map(&config);
            config = mutate(
                context,
                actor,
                RealmPlacementMutation::PublishCandidateMap(map),
            )
            .await?;
            epoch
        }
    };
    // Bucket zero stands for the strategy: activations are initialized for all
    // of its buckets in one reduced event.
    let uninitialized: Vec<Ulid> = config
        .strategies
        .iter()
        .map(|strategy| strategy.strategy_id)
        .filter(|strategy_id| config.activation(strategy_id, 0).is_none())
        .collect();
    for strategy_id in uninitialized {
        config = mutate(
            context,
            actor,
            RealmPlacementMutation::InitializeActivations {
                strategy_id,
                candidate_map_epoch: epoch,
            },
        )
        .await?;
    }
    Ok(config)
}

/// Publishes the current view and starts a transition onto it for every bucket
/// whose holder set only grows. Returns the transitions it started; an empty
/// result means the view already matches the newest map or nothing expands.
pub async fn expand_realm_placement(
    context: &DriverContext,
    actor: &Actor,
) -> Result<Vec<Ulid>, MutateRealmPlacementError> {
    let config = ensure_activated_map(context, actor).await?;
    let (next_epoch, map) = next_map(&config);
    // The newest epoch is the durable pending expansion target when it already
    // freezes the current view; equality never short-circuits the transition
    // work below, or a join during an active expansion would be dropped.
    let reuse = config
        .newest_map_epoch()
        .and_then(|epoch| config.candidate_map(epoch))
        .is_some_and(|newest| {
            newest.nodes == map.nodes
                && newest.selectors == map.selectors
                && newest.shard_overrides == map.shard_overrides
        });
    let (epoch, mut config) = if reuse {
        (config.newest_map_epoch().unwrap_or(next_epoch), config)
    } else {
        let config = mutate(
            context,
            actor,
            RealmPlacementMutation::PublishCandidateMap(map),
        )
        .await?;
        (next_epoch, config)
    };

    let strategy_ids: Vec<Ulid> = config
        .strategies
        .iter()
        .map(|strategy| strategy.strategy_id)
        .collect();
    let mut started = Vec::new();
    for strategy_id in strategy_ids {
        if config.placement_transitions.iter().any(|transition| {
            transition.plan.strategy_id == strategy_id && !transition.is_terminal()
        }) {
            continue;
        }
        let buckets = expansion_buckets(&config, strategy_id, epoch).map_err(invalid)?;
        if buckets.is_empty() {
            continue;
        }
        let transition_id = Ulid::generate();
        let plan = plan_transition(
            &config,
            TransitionRequest {
                transition_id,
                strategy_id,
                buckets,
                target_map_epoch: epoch,
                // Expansion moves nothing off a holder, so every bucket may run
                // at once: there is no window in which authority is in doubt.
                limits: TransitionLimits {
                    max_incomplete_buckets: u32::MAX,
                    ..TransitionLimits::default()
                },
                created_by: actor.node_id,
                created_at_ms: unix_timestamp_millis(),
            },
        )
        .map_err(invalid)?;
        config = mutate(
            context,
            actor,
            RealmPlacementMutation::StartTransition(plan),
        )
        .await?;
        started.push(transition_id);
    }
    Ok(started)
}

/// The next epoch and the map that freezes the current view into it.
fn next_map(config: &RealmConfigDocument) -> (u64, CandidatePlacementMap) {
    let epoch = config.newest_map_epoch().unwrap_or(0) + 1;
    (epoch, config.freeze_map(epoch))
}

fn invalid(error: crate::placement::transition::TransitionPlanError) -> MutateRealmPlacementError {
    MutateRealmPlacementError::InvalidInput(error.to_string())
}

async fn read_config(
    context: &DriverContext,
    actor: &Actor,
) -> Result<RealmConfigDocument, MutateRealmPlacementError> {
    drive(GetRealmConfigOperation::new(actor.realm_id), context)
        .await
        .map_err(|_| MutateRealmPlacementError::RealmConfigNotFound)
}

const MUTATION_CONFLICT_RETRIES: usize = 10;

/// Drives one placement mutation, re-driving on SSI conflict: the node's own
/// reconciler submits transition steps against the same realm config document
/// concurrently, so bounded interference is expected, not an error.
pub(crate) async fn mutate(
    context: &DriverContext,
    actor: &Actor,
    mutation: RealmPlacementMutation,
) -> Result<RealmConfigDocument, MutateRealmPlacementError> {
    let mut attempts = 0;
    loop {
        let result = drive(
            MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
                actor: actor.clone(),
                mutation: mutation.clone(),
            }),
            context,
        )
        .await;
        match result {
            Err(MutateRealmPlacementError::StorageError(StorageError::TransactionConflict))
                if attempts < MUTATION_CONFLICT_RETRIES =>
            {
                attempts += 1;
            }
            other => return other,
        }
    }
}
