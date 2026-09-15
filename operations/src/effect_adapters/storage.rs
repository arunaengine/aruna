//! Storage adapter: one storage effect plus the peer refresh a successful
//! realm-config write or transaction commit triggers. The bounded refresh runs
//! after the event and never replaces it: a failure warns but does not fail.

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use std::time::Duration;
use tracing::warn;

use crate::driver::DriverContext;

const PEER_REFRESH_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) async fn dispatch_storage(effect: StorageEffect, context: &DriverContext) -> Event {
    let realm_config_write = match &effect {
        StorageEffect::Write {
            key_space,
            value,
            txn_id: None,
            ..
        } if key_space == REALM_CONFIG_KEYSPACE => Some(value.clone()),
        _ => None,
    };
    let refresh_after_commit = matches!(&effect, StorageEffect::CommitTransaction { .. });
    let event = Box::pin(context.storage_handle.send_storage_effect(effect)).await;
    if let Some(net_handle) = context.net_handle.as_ref() {
        match (&event, realm_config_write) {
            (Event::Storage(StorageEvent::WriteResult { .. }), Some(bytes)) => {
                match tokio::time::timeout(
                    PEER_REFRESH_TIMEOUT,
                    net_handle.refresh_encoded_peers(&bytes),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        warn!(error = %error, "Failed to refresh realm peers from written realm config");
                    }
                    Err(_) => {
                        warn!(
                            timeout_ms = PEER_REFRESH_TIMEOUT.as_millis() as u64,
                            "Timed out refreshing realm peers from written realm config"
                        );
                    }
                }
            }
            (Event::Storage(StorageEvent::TransactionCommitted { .. }), _)
                if refresh_after_commit =>
            {
                match tokio::time::timeout(PEER_REFRESH_TIMEOUT, net_handle.reload_realm_peers())
                    .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        warn!(error = %error, "Failed to refresh realm peers after storage commit");
                    }
                    Err(_) => {
                        warn!(
                            timeout_ms = PEER_REFRESH_TIMEOUT.as_millis() as u64,
                            "Timed out refreshing realm peers after storage commit"
                        );
                    }
                }
            }
            _ => {}
        }
    }
    event
}

#[cfg(test)]
mod tests {
    use super::dispatch_storage;
    use crate::driver::{DriverContext, managed_effect};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::types::TxnId;
    use aruna_storage::storage;
    use byteview::ByteView;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    #[derive(Debug, PartialEq)]
    struct CommitOutcome {
        state: u8,
        failed: bool,
        txn_id: Option<TxnId>,
    }

    impl Operation for CommitOutcome {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            if let Some(txn_id) = self.txn_id {
                self.state = 3;
                return smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                    txn_id
                },)];
            }
            self.state = 1;
            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match (event, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.txn_id = Some(txn_id);
                    self.state = 2;
                    smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"commit-outcome"),
                        value: ByteView::from(*b"committed"),
                        txn_id: Some(txn_id),
                    })]
                }
                (Event::Storage(StorageEvent::WriteResult { .. }), 2) => {
                    self.state = 3;
                    smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.expect("transaction id recorded"),
                    })]
                }
                (Event::Storage(StorageEvent::TransactionCommitted { .. }), 3) => {
                    self.state = 4;
                    smallvec::smallvec![]
                }
                (Event::Storage(StorageEvent::Error { .. }), 3) => {
                    self.failed = true;
                    self.state = 4;
                    smallvec::smallvec![]
                }
                _ => smallvec::smallvec![],
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 4
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            if self.failed { Err(()) } else { Ok(()) }
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            self.failed = true;
            self.state = 4;
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn commit_refresh_survives() {
        // Real time: the proxy actor answers from an OS thread, and paused-time
        // auto-advance would fire the storage request timeout before it can.
        assert!(managed_effect(&Effect::Storage(
            StorageEffect::CommitTransaction {
                txn_id: ulid::Ulid::generate(),
            },
        )));
        assert!(managed_effect(&Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(*b"realm"),
            value: ByteView::from(*b"config"),
            txn_id: None,
        })));
        let directory = tempdir().unwrap();
        let direct = storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let (storage_handle, receivers) = storage::StorageHandle::new();
        let receiver = receivers.foreground;
        drop(receivers.bulk);
        let committed = Arc::new(AtomicBool::new(false));
        let committed_for_actor = committed.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (actor_done_tx, actor_done_rx) = std::sync::mpsc::channel();
        let actor = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut started_tx = Some(started_tx);
            let mut done_tx = Some(done_tx);
            while let Ok((effect, response, _span, _queued, _in_flight)) = receiver.recv() {
                let gated = committed_for_actor.load(Ordering::Acquire)
                    && matches!(
                        &effect,
                        StorageEffect::Read { key_space, .. }
                            if key_space == REALM_CONFIG_KEYSPACE
                    );
                if gated {
                    if let Some(sender) = started_tx.take() {
                        let _ = sender.send(());
                    }
                    release_rx.recv().unwrap();
                    committed_for_actor.store(false, Ordering::Release);
                }
                let committed_effect = matches!(&effect, StorageEffect::CommitTransaction { .. });
                let Event::Storage(event) = runtime.block_on(direct.send_storage_effect(effect))
                else {
                    unreachable!("storage proxy only handles storage events");
                };
                let committed_event =
                    committed_effect && matches!(&event, StorageEvent::TransactionCommitted { .. });
                if committed_event {
                    committed_for_actor.store(true, Ordering::Release);
                }
                let _ = response.send(event);
                if gated && let Some(sender) = done_tx.take() {
                    let _ = sender.send(());
                }
            }
            let _ = actor_done_tx.send(());
        });
        let net_handle = aruna_net::NetHandle::new(
            aruna_net::NetConfig {
                discovery_method: aruna_net::DiscoveryMethod::None,
                relay_method: aruna_net::RelayMethod::None,
                ..aruna_net::NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let txn_id = match storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            other => panic!("unexpected transaction start: {other:?}"),
        };
        match storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "default".to_string(),
                key: ByteView::from(*b"commit-outcome"),
                value: ByteView::from(*b"committed"),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected transaction write: {other:?}"),
        }
        let task_context = context.clone();
        let mut task = tokio::spawn(async move {
            crate::driver::drive_until(
                CommitOutcome {
                    state: 0,
                    failed: false,
                    txn_id: Some(txn_id),
                },
                &task_context,
                tokio::time::Instant::now() + std::time::Duration::from_millis(100),
            )
            .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            tokio::select! {
                started = started_rx => started.unwrap(),
                result = &mut task => panic!("drive finished before commit refresh: {result:?}"),
            }
        })
        .await
        .expect("commit refresh did not start");
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(30), &mut task)
                .await
                .expect("commit refresh did not honor its own timeout")
                .unwrap()
                .is_ok()
        );

        committed.store(false, Ordering::Release);
        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), done_rx)
            .await
            .expect("released refresh did not finish")
            .unwrap();
        net_handle.shutdown().await;
        drop(context);
        drop(net_handle);
        drop(storage_handle);
        actor_done_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("storage proxy did not stop");
        actor.join().unwrap();
    }

    #[tokio::test]
    async fn missing_handle_result() {
        // Reduced-capability nodes must still see the storage result, not a
        // dropped effect; the peer link is only a post-write side channel.
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let event = dispatch_storage(
            StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"missing"),
                txn_id: None,
            },
            &context,
        )
        .await;

        assert!(matches!(
            event,
            Event::Storage(StorageEvent::ReadResult { value: None, .. })
        ));
    }
}
