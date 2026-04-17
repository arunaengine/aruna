mod shared;

use aruna::config::StartupMode;
use aruna_core::onboarding::{OnboardingMode, OnboardingPhase};
use shared::{
    TestResult, create_onboarding_secret_via_http, spawn_joiner_node, spawn_seed_node,
    wait_for_realm_nodes,
};
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn onboarding_bootstraps_joiner_over_http_and_syncs_core_documents() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    sleep(Duration::from_millis(50)).await;
    let onboarding_secret = create_onboarding_secret_via_http(&seed, OnboardingMode::Local).await?;

    let joiner = spawn_joiner_node(&seed, onboarding_secret).await?;

    assert!(matches!(
        joiner.config.startup_mode,
        StartupMode::JoinRealm {
            phase: OnboardingPhase::Bootstrapped
        }
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
