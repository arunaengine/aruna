//! Tests that realm peers reload only after a stored realm config change, one reload at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::test_support::make_secret;
use crate::tests::test_net_handle;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmNodeKind;
use std::future::{Future, poll_fn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

fn config_with(handle: &NetHandle, peer: NodeId) -> RealmConfigDocument {
    let mut document = RealmConfigDocument::default_for_realm(*handle.realm_id(), Vec::new());
    document.ensure_node(handle.node_id(), RealmNodeKind::Management);
    document.ensure_node(peer, RealmNodeKind::Server);
    document
}

async fn store_config(handle: &NetHandle, bytes: Vec<u8>) {
    let target = DocumentTarget::RealmConfig {
        realm_id: *handle.realm_id(),
    };
    let event = handle
        .inner
        .storage
        .send_storage_effect(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: bytes.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

fn encoded(handle: &NetHandle, document: &RealmConfigDocument) -> Vec<u8> {
    let actor = Actor {
        node_id: handle.node_id(),
        user_id: UserId::nil(*handle.realm_id()),
        realm_id: *handle.realm_id(),
    };
    document.to_bytes(&actor).unwrap()
}

#[tokio::test]
async fn reload_skips_unchanged() -> Result<()> {
    let (handle, _dir) = test_net_handle().await?;
    let peer_a = make_secret(21).public();
    let peer_b = make_secret(22).public();
    let stored = config_with(&handle, peer_a);
    store_config(&handle, encoded(&handle, &stored)).await;

    assert_eq!(handle.reload_if_changed().await?, Some(vec![peer_a]));
    // A stored row exists, so `None` means no storage read happened.
    assert_eq!(handle.reload_if_changed().await?, None);

    // A supplied document may be older than storage; the next check reads storage again.
    handle
        .refresh_document_peers(&config_with(&handle, peer_b))
        .await?;
    assert_eq!(handle.realm_peers().await, vec![peer_b]);
    assert_eq!(handle.reload_if_changed().await?, Some(vec![peer_a]));
    assert_eq!(handle.realm_peers().await, vec![peer_a]);

    // A failed reload records nothing, so each later check retries.
    store_config(&handle, b"not-a-config".to_vec()).await;
    assert!(handle.reload_if_changed().await.is_err());
    assert!(handle.reload_if_changed().await.is_err());
    store_config(&handle, encoded(&handle, &config_with(&handle, peer_b))).await;
    assert_eq!(handle.reload_if_changed().await?, Some(vec![peer_b]));

    handle.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn reload_waits_turn() -> Result<()> {
    let (handle, _dir) = test_net_handle().await?;
    let peer_a = make_secret(23).public();
    let peer_b = make_secret(24).public();
    store_config(&handle, encoded(&handle, &config_with(&handle, peer_a))).await;
    assert_eq!(handle.reload_if_changed().await?, Some(vec![peer_a]));

    // A reload cancelled while it waits leaves nothing behind.
    let guard = handle.inner.config_applied.lock().await;
    let mut cancelled = Box::pin(handle.reload_if_changed());
    assert!(poll_once(&mut cancelled).await.is_pending());
    drop(cancelled);

    // A waiting reload reads storage only after the running one applied its peers.
    let mut waiting = Box::pin(handle.reload_if_changed());
    assert!(poll_once(&mut waiting).await.is_pending());
    store_config(&handle, encoded(&handle, &config_with(&handle, peer_b))).await;
    drop(guard);
    assert_eq!(waiting.await?, Some(vec![peer_b]));
    assert_eq!(handle.realm_peers().await, vec![peer_b]);

    handle.shutdown().await;
    Ok(())
}

async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(std::pin::Pin::new(&mut *future).poll(cx))).await
}

#[tokio::test]
async fn stale_read_repaired() -> Result<()> {
    let dir = tempfile::tempdir().map_err(|e| NetError::Io(e.to_string()))?;
    let real = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
        .map_err(|e| NetError::Io(e.to_string()))?;
    // The proxy shares the real worker's change count and holds one realm config reply.
    let (proxy, receivers) = real.proxy_channels();
    drop(receivers.bulk);
    let armed = Arc::new(AtomicBool::new(false));
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let forward = {
        let armed = armed.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut held_tx = Some(held_tx);
            let mut release_rx = Some(release_rx);
            let mut held = None;
            while let Ok((effect, response, _span, _queued, in_flight)) =
                receivers.foreground.recv()
            {
                let gated = matches!(
                    &effect,
                    StorageEffect::Read { key_space, .. } if key_space == REALM_CONFIG_KEYSPACE
                ) && armed.swap(false, Ordering::AcqRel);
                let Event::Storage(event) = runtime.block_on(real.send_storage_effect(effect))
                else {
                    unreachable!("storage proxy only handles storage events");
                };
                if !gated {
                    let _ = response.send(event);
                    continue;
                }
                let release = release_rx.take().unwrap();
                held = Some(std::thread::spawn(move || {
                    release.recv().unwrap();
                    let _ = response.send(event);
                    drop(in_flight);
                }));
                let _ = held_tx.take().unwrap().send(());
            }
            if let Some(held) = held {
                held.join().unwrap();
            }
        })
    };
    let handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        proxy,
    )
    .await?;
    let peer_a = make_secret(25).public();
    let peer_b = make_secret(26).public();
    let peer_c = make_secret(27).public();
    store_config(&handle, encoded(&handle, &config_with(&handle, peer_a))).await;
    assert_eq!(handle.reload_if_changed().await?, Some(vec![peer_a]));

    // This reload reads B, and C commits before B is applied.
    store_config(&handle, encoded(&handle, &config_with(&handle, peer_b))).await;
    armed.store(true, Ordering::Release);
    let stale = tokio::spawn({
        let handle = handle.clone();
        async move { handle.reload_if_changed().await }
    });
    held_rx.await.unwrap();
    store_config(&handle, encoded(&handle, &config_with(&handle, peer_c))).await;
    release_tx.send(()).unwrap();
    assert_eq!(stale.await.unwrap()?, Some(vec![peer_b]));

    // The count moved past the stale read, so the next check applies C.
    assert_eq!(handle.reload_if_changed().await?, Some(vec![peer_c]));
    assert_eq!(handle.realm_peers().await, vec![peer_c]);

    handle.shutdown().await;
    drop(handle);
    forward.join().unwrap();
    Ok(())
}
