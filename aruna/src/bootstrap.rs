use crate::config::PersistedNodeState;
use aruna_api::server_state::{
    INITIAL_LOCAL_ONBOARDING_SECRET_KEY, load_persisted_state, persist_state,
};
use aruna_core::effects::{Effect, GossipEffect, NetEffect, StorageEffect};
use aruna_core::errors::GossipError;
use aruna_core::events::{Event, GossipEvent, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::{OnboardingMode, OnboardingSecret, OnboardingSyncTicket};
use aruna_core::{NodeId, TopicId};
use aruna_operations::announce::AnnounceTopicOperation;
use aruna_operations::create_onboarding_secret::{
    CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::outgoing_automerge::OutgoingAutomergeOperation;
use byteview::ByteView;
use rand::RngCore;

pub async fn realm_bootstrap_exists(
    driver_ctx: &DriverContext,
    realm_id: &aruna_core::structs::RealmId,
) -> Result<bool, Box<dyn std::error::Error>> {
    let key = ByteView::from(*realm_id.as_bytes());

    for key_space in [AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE] {
        match driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.clone(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {}
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => return Ok(false),
            Event::Storage(StorageEvent::Error { error }) => return Err(Box::new(error)),
            other => return Err(format!("unexpected storage event: {other:?}").into()),
        }
    }

    Ok(true)
}

pub async fn announce_core_documents(
    driver_ctx: &DriverContext,
    node_id: NodeId,
    realm_id: &aruna_core::structs::RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    for topic in [TopicId::realm(*realm_id), TopicId::users(*realm_id)] {
        drive(AnnounceTopicOperation::new(topic, node_id), driver_ctx).await?;
    }

    Ok(())
}

async fn subscribe_topic(
    driver_ctx: &DriverContext,
    topic: TopicId,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(net_handle) = driver_ctx.net_handle.as_ref() else {
        return Err("net handle unavailable".into());
    };

    match net_handle
        .send_effect(Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
            topic,
        })))
        .await
    {
        Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. })) => Ok(()),
        Event::Net(NetEvent::Gossip(GossipEvent::Error {
            error: GossipError::AlreadySubscribed,
        })) => Ok(()),
        Event::Net(NetEvent::Gossip(GossipEvent::Error { error })) => Err(error.to_string().into()),
        Event::Net(NetEvent::Error(error)) => Err(format!("{error:?}").into()),
        other => Err(format!("unexpected gossip subscribe result: {other:?}").into()),
    }
}

pub async fn fetch_core_onboarding_documents(
    driver_ctx: &DriverContext,
    node_state: &PersistedNodeState,
    realm_id: &aruna_core::structs::RealmId,
    bootstrap_peer: Option<NodeId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_peer = bootstrap_peer.ok_or("missing bootstrap peer")?;
    let onboarding_sync_ticket = node_state
        .onboarding_sync_ticket
        .as_deref()
        .ok_or("missing onboarding sync ticket")?;
    let onboarding_sync_ticket = OnboardingSyncTicket::decode(onboarding_sync_ticket)?;

    for topic in [TopicId::realm(*realm_id), TopicId::users(*realm_id)] {
        subscribe_topic(driver_ctx, topic).await?;
    }

    for document in onboarding_sync_ticket.payload.documents.clone() {
        drive(
            OutgoingAutomergeOperation::new_with_auth(
                bootstrap_peer,
                document,
                Some(onboarding_sync_ticket.clone().into_auth_proof()),
            ),
            driver_ctx,
        )
        .await?;
    }

    Ok(())
}

pub async fn ensure_initial_local_onboarding_secret(
    driver_ctx: &DriverContext,
    seed_url: String,
) -> Result<OnboardingSecret, Box<dyn std::error::Error>> {
    if let Some(secret) =
        load_persisted_state::<OnboardingSecret>(driver_ctx, INITIAL_LOCAL_ONBOARDING_SECRET_KEY)
            .await
    {
        return Ok(secret);
    }

    let mut secret_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut secret_bytes);
    let onboarding_secret = OnboardingSecret {
        seed_url,
        enrollment_id: ulid::Ulid::new(),
        secret: secret_bytes,
        mode: OnboardingMode::Local,
    };
    let record = aruna_core::onboarding::OnboardingSecretRecord {
        enrollment_id: onboarding_secret.enrollment_id,
        secret_hash: blake3::hash(&onboarding_secret.secret).to_string(),
        mode: OnboardingMode::Local,
        expires_at: u64::MAX,
        consumed: false,
    };

    drive(
        CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput { record }),
        driver_ctx,
    )
    .await?;
    persist_state(
        driver_ctx,
        INITIAL_LOCAL_ONBOARDING_SECRET_KEY,
        &onboarding_secret,
    )
    .await;
    Ok(onboarding_secret)
}
