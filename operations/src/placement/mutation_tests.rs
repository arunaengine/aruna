//! Tests placement mutation re-drives: racing writers all land.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::*;
use crate::document_repository::GetConfigOperation;
use crate::driver::drive;
use crate::realm::mutate_placement::tests::{actor, context, seed_config, strategy};
use aruna_core::structs::identity::realm::RealmId;
use tempfile::tempdir;

#[tokio::test]
async fn concurrent_mutations_land() {
    // Writers racing on one document conflict at commit; each re-drives
    // its own mutation, so all of them land instead of one failing.
    let root = tempdir().unwrap();
    let context = context(root.path().to_str().unwrap());
    let actor = actor(RealmId::from_bytes([21; 32]));
    seed_config(&context, &actor).await;
    let ids: Vec<Ulid> = (1..=6u8).map(|seed| Ulid::from_bytes([seed; 16])).collect();
    let mut tasks = Vec::new();
    for strategy_id in ids.clone() {
        let context = context.clone();
        let actor = actor.clone();
        tasks.push(tokio::spawn(async move {
            drive(
                PlacementMutationOperation::new(
                    MutatePlacementConfig {
                        actor,
                        mutation: RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
                    },
                    None,
                    None,
                ),
                &context,
            )
            .await
        }));
    }
    for task in tasks {
        task.await.unwrap().expect("every mutation lands");
    }
    let config = drive(GetConfigOperation::new(actor.realm_id), &context)
        .await
        .unwrap();
    for strategy_id in ids {
        assert!(config.strategy(&strategy_id).is_some());
    }
}

fn mutation_config(mutation: RealmPlacementMutation) -> MutatePlacementConfig {
    MutatePlacementConfig {
        actor: actor(RealmId::from_bytes([21; 32])),
        mutation,
    }
}

fn outcome(result: Result<RealmConfigDocument, MutatePlacementError>) -> IoResult {
    IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
}

fn conflict() -> IoResult {
    outcome(Err(MutatePlacementError::StorageError(
        StorageError::TransactionConflict,
    )))
}

fn upsert() -> RealmPlacementMutation {
    RealmPlacementMutation::UpsertStrategy(strategy(Ulid::from_bytes([1; 16])))
}

#[test]
fn conflicts_then_fail() {
    let mut operation = PlacementMutationOperation::new(mutation_config(upsert()), None, None);
    operation.first().unwrap();
    for _ in 0..CONFLICT_ATTEMPTS {
        assert!(matches!(
            operation.next(conflict()),
            Ok(Next::Call(IoCall::Delay(_)))
        ));
        assert!(matches!(
            operation.next(IoResult::Woke),
            Ok(Next::Call(IoCall::Child(_)))
        ));
    }
    assert_eq!(
        operation.next(conflict()),
        Err(MutatePlacementError::StorageError(
            StorageError::TransactionConflict
        ))
    );
}

#[test]
fn plain_mutation_finishes() {
    let config = RealmConfigDocument::new(RealmId::from_bytes([21; 32]), Vec::new(), 2);
    let mut operation = PlacementMutationOperation::new(mutation_config(upsert()), None, None);
    operation.first().unwrap();
    assert_eq!(
        operation.next(outcome(Ok(config.clone()))),
        Ok(Next::Done(config))
    );
}
