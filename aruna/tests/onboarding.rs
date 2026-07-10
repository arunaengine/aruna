mod shared;

use aruna::config::{PersistedNodeIdentity, StartupMode};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::onboarding::{OnboardingMode, OnboardingPhase};
use aruna_core::structs::{Actor, User};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::node_info::read_node_info_document;
use aruna_operations::placement::build_view;
use aruna_operations::register_or_get_oidc_user::{
    RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation,
};
use byteview::ByteView;
use shared::{
    TestResult, create_onboarding_secret_via_http, spawn_joiner_node, spawn_seed_node,
    wait_for_realm_nodes,
};
use tokio::time::{Duration, sleep};

async fn read_user(
    context: &aruna_operations::driver::DriverContext,
    user_id: UserId,
) -> Option<User> {
    match context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: aruna_core::USER_KEYSPACE.to_string(),
            key: ByteView::from(user_id.to_bytes()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => Some(User::from_bytes(&bytes).unwrap()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => None,
        other => panic!("unexpected user read result: {other:?}"),
    }
}

#[tokio::test]
async fn onboarding_bootstraps_joiner_over_http_and_syncs_core_documents() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    sleep(Duration::from_millis(50)).await;
    let _user = drive(
        RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: Actor {
                node_id: seed.net.node_id(),
                user_id: seed.user_id,
                realm_id: seed.realm_id,
            },
            user_id: seed.user_id,
            name: "Seed Admin".to_string(),
            subject_id: seed.user_id.to_string(),
            issuer: format!("issuer_of_{}", seed.user_id),
        }),
        seed.context.as_ref(),
    )
    .await?;
    let onboarding_secret = create_onboarding_secret_via_http(&seed, OnboardingMode::Local).await?;
    let expected_user = read_user(seed.context.as_ref(), seed.user_id)
        .await
        .expect("seed user should exist");

    let joiner = spawn_joiner_node(&seed, onboarding_secret).await?;

    assert!(matches!(
        joiner.config.startup_mode,
        StartupMode::JoinRealm {
            phase: OnboardingPhase::Bootstrapped
        }
    ));
    assert_eq!(
        read_user(joiner.context.as_ref(), seed.user_id)
            .await
            .expect("joiner user should be bootstrapped"),
        expected_user
    );
    assert_eq!(
        read_node_info_document(&joiner.context.storage_handle, seed.net.node_id())
            .await
            .unwrap()
            .expect("issuer node info should be fetched from the onboarding ticket"),
        read_node_info_document(&seed.context.storage_handle, seed.net.node_id())
            .await
            .unwrap()
            .expect("seed fixture should publish issuer node info")
    );
    let realm_config = drive(
        GetRealmConfigOperation::new(joiner.config.realm_id),
        joiner.context.as_ref(),
    )
    .await?;
    let selector_labels = build_view(&realm_config)
        .nodes
        .into_iter()
        .find(|node| node.node_id == joiner.config.node_id)
        .expect("joiner should be in the placement view")
        .labels;
    let joiner_info =
        read_node_info_document(&joiner.context.storage_handle, joiner.config.node_id)
            .await?
            .expect("joiner startup should seed its node info after fetching realm config");
    assert_eq!(joiner_info.labels, selector_labels);
    assert_eq!(joiner_info.labels.get("fixture").unwrap(), "joiner");
    assert_eq!(
        joiner_info.urls.api.as_deref(),
        Some("https://api.joiner.example.test")
    );
    assert_eq!(
        joiner_info.urls.s3.as_deref(),
        Some("https://s3.joiner.example.test")
    );
    assert_eq!(joiner_info.utilization.documents_held, None);
    assert_eq!(joiner_info.utilization.load_permille, None);

    wait_for_realm_nodes(
        &[seed.context.as_ref(), joiner.context.as_ref()],
        &joiner.config.realm_id,
        2,
    )
    .await?;

    joiner.shutdown().await;
    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn server_onboarding_bootstraps_joiner_over_http_and_completes() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    sleep(Duration::from_millis(50)).await;
    let onboarding_secret =
        create_onboarding_secret_via_http(&seed, OnboardingMode::Server).await?;

    let joiner = spawn_joiner_node(&seed, onboarding_secret).await?;

    assert!(matches!(
        joiner.config.startup_mode,
        StartupMode::JoinRealm {
            phase: OnboardingPhase::Bootstrapped
        }
    ));
    assert!(matches!(
        joiner.config.node_state.identity,
        PersistedNodeIdentity::Server { .. }
    ));

    wait_for_realm_nodes(
        &[seed.context.as_ref(), joiner.context.as_ref()],
        &joiner.config.realm_id,
        2,
    )
    .await?;

    joiner.shutdown().await;
    seed.shutdown().await;
    Ok(())
}
