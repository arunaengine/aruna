use crate::config::PersistedNodeState;
use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, GossipEffect, NetEffect, StorageEffect};
use aruna_core::errors::GossipError;
use aruna_core::events::{Event, GossipEvent, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::OnboardingSyncTicket;
use aruna_core::{NodeId, TopicId};
use aruna_operations::announce::AnnounceTopicOperation;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::outgoing_automerge::OutgoingAutomergeOperation;
use byteview::ByteView;

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
    drive(
        AnnounceTopicOperation::new(TopicId::realm(realm_id.clone()), node_id),
        driver_ctx,
    )
    .await?;

    Ok(())
}

pub async fn fetch_core_onboarding_documents(
    driver_ctx: &DriverContext,
    node_state: &PersistedNodeState,
    realm_id: &aruna_core::structs::RealmId,
    bootstrap_peer: Option<NodeId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(net_handle) = driver_ctx.net_handle.as_ref() else {
        return Err("net handle unavailable".into());
    };
    let bootstrap_peer = bootstrap_peer.ok_or("missing bootstrap peer")?;
    let onboarding_sync_ticket = node_state
        .onboarding_sync_ticket
        .as_deref()
        .ok_or("missing onboarding sync ticket")?;
    let onboarding_sync_ticket = OnboardingSyncTicket::decode(onboarding_sync_ticket)?;

    match net_handle
        .send_effect(Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
            topic: TopicId::realm(realm_id.clone()),
        })))
        .await
    {
        Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. })) => {}
        Event::Net(NetEvent::Gossip(GossipEvent::Error {
            error: GossipError::AlreadySubscribed,
        })) => {}
        Event::Net(NetEvent::Gossip(GossipEvent::Error { error })) => {
            return Err(error.to_string().into());
        }
        Event::Net(NetEvent::Error(error)) => return Err(format!("{error:?}").into()),
        other => return Err(format!("unexpected gossip subscribe result: {other:?}").into()),
    }

    for document in [
        AutomergeDocumentVariant::RealmAuthorization {
            realm_id: realm_id.clone(),
        },
        AutomergeDocumentVariant::RealmConfig {
            realm_id: realm_id.clone(),
        },
    ] {
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
