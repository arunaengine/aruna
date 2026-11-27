//! Tests who may read and replace the realm feature policy and what the effective view shows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::portal::get_portal_features;
use crate::tests::realms::{admin_state, state_auth};
use aruna_core::user::id::UserId;

fn caller(state: &ServerState, user: UserId) -> RealmAuth {
    RealmAuth(state_auth(state, user).expect("a signed-in caller"))
}

fn policy(group: Ulid) -> FeaturePolicy {
    let runs = FeatureSetting::Scoped {
        default: false,
        groups: BTreeMap::from([(group.to_string(), true)]),
    };
    let features = [("runs", runs), ("shares", FeatureSetting::ForcedOff)];
    FeaturePolicy {
        features: (features.into_iter())
            .map(|(name, setting)| (name.to_string(), setting))
            .collect(),
    }
}

async fn effective(
    state: &Arc<ServerState>,
    user: UserId,
    group: Option<Ulid>,
) -> ServerResult<EffectiveFeatures> {
    let query = EffectiveQuery {
        group_id: group.map(|group| group.to_string()),
    };
    let result = effective_features(State(state.clone()), caller(state, user), Query(query)).await;
    result.map(|Json(body)| body)
}

#[tokio::test]
async fn admin_replaces_policy() {
    let (_dir, state, admin) = admin_state().await;
    let group = Ulid::generate();
    let Json(stored) = set_features(
        State(state.clone()),
        caller(&state, admin),
        Json(policy(group)),
    )
    .await
    .unwrap();
    assert_eq!(stored, policy(group));
    let Json(read) = get_features(State(state.clone()), caller(&state, admin))
        .await
        .unwrap();
    assert_eq!(read, policy(group));

    // The older portal key follows the realm `runs` default.
    let Json(portal) = get_portal_features(State(state.clone()), caller(&state, admin))
        .await
        .unwrap();
    assert_eq!(portal.features.get("compute"), Some(&false));
}

#[tokio::test]
async fn member_cannot_read() {
    // The raw policy lists every group's exception, so only realm config admins see it.
    let (_dir, state, _admin) = admin_state().await;
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let error = get_features(State(state.clone()), caller(&state, member))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
    let error = set_features(
        State(state.clone()),
        caller(&state, member),
        Json(policy(Ulid::generate())),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
}

#[tokio::test]
async fn refuses_unknown_feature() {
    let (_dir, state, admin) = admin_state().await;
    let mut body = policy(Ulid::generate());
    body.features
        .insert("compute".to_string(), FeatureSetting::ForcedOff);
    let error = set_features(State(state.clone()), caller(&state, admin), Json(body))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::BadRequestReason(_)));
}

#[tokio::test]
async fn effective_resolves_groups() {
    let (_dir, state, admin) = admin_state().await;
    let group = Ulid::generate();
    let Json(_) = set_features(
        State(state.clone()),
        caller(&state, admin),
        Json(policy(group)),
    )
    .await
    .unwrap();

    let realm = effective(&state, admin, None).await.unwrap();
    assert_eq!(realm.features.len(), Feature::ALL.len());
    let runs = &realm.features["runs"];
    assert_eq!(
        (runs.state.as_str(), runs.reason.as_str()),
        ("off", "realm")
    );
    let workflows = &realm.features["workflows"];
    assert_eq!(workflows.state, "off");
    let notebooks = &realm.features["notebooks"];
    assert_eq!(
        (notebooks.reason.as_str(), notebooks.dependency.as_deref()),
        ("dependency", Some("runs"))
    );

    let scoped = effective(&state, admin, Some(group)).await.unwrap();
    assert_eq!(scoped.group_id, Some(group.to_string()));
    let runs = &scoped.features["runs"];
    assert_eq!((runs.state.as_str(), runs.reason.as_str()), ("on", "group"));
    let shares = &scoped.features["shares"];
    assert_eq!(
        (shares.state.as_str(), shares.reason.as_str()),
        ("off", "realm")
    );
    let applets = &scoped.features["applets"];
    assert_eq!(
        (applets.state.as_str(), applets.reason.as_str()),
        ("off", "node")
    );
}

#[tokio::test]
async fn stranger_cannot_probe() {
    // A caller outside the group learns nothing about its exceptions.
    let (_dir, state, _admin) = admin_state().await;
    let stranger = UserId::local(Ulid::generate(), state.get_realm_id());
    let error = effective(&state, stranger, Some(Ulid::generate()))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
    let realm = effective(&state, stranger, None).await.unwrap();
    assert_eq!(realm.features["vault"].state, "on");
}

#[test]
fn unreadable_stays_unknown() {
    let off = std::collections::BTreeSet::from([Feature::Runs]);
    let shares = node_only(&off, Feature::Shares);
    assert_eq!(
        (shares.state.as_str(), shares.reason.as_str()),
        ("unknown", "unreadable")
    );
    assert_eq!(node_only(&off, Feature::Runs).reason, "node");
    let workflows = node_only(&off, Feature::Workflows);
    assert_eq!(
        (workflows.state.as_str(), workflows.dependency.as_deref()),
        ("off", Some("runs"))
    );
}
