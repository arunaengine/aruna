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
            drive_placement_mutation(
                MutatePlacementConfig {
                    actor,
                    mutation: RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
                },
                None,
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
