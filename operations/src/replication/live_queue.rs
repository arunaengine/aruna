//! Queues live replication after a committed write and announces the upload to watchers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::id_mint::IdMint;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, schedule_watch_drain, watch_event_entry,
    watch_resource_path,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use tracing::warn;
use ulid::Ulid;

use super::queue::{BlobQueueError, LiveVersionInput, LiveVersionOperation, QueueBlobResult};

/// A committed put, as its live replication and its watchers need it.
#[derive(Clone, Debug, PartialEq)]
pub struct CompletePutInput {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub auth: AuthContext,
    pub group_id: GroupId,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub size_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PutStep {
    Queue,
    Holders,
    Emit,
}

/// Best effort after a committed put: queues live replication, then queues the watch event when
/// anyone watches. Every failure only warns, since the durable obligation remains for repair.
#[derive(Debug, PartialEq)]
pub struct CompletePutOperation {
    now_ms: u64,
    ids: IdMint,
    input: CompletePutInput,
    step: PutStep,
}

impl CompletePutOperation {
    pub fn new(input: CompletePutInput) -> Self {
        Self {
            now_ms: 0,
            ids: IdMint::default(),
            input,
            step: PutStep::Queue,
        }
    }

    fn path(&self) -> String {
        let input = &self.input;
        watch_resource_path(input.group_id, input.node_id, &input.bucket, &input.key)
    }

    fn ask_holders(&mut self) -> Result<Next<()>, IoError> {
        self.step = PutStep::Holders;
        let holders = Effect::Net(NetEffect::WatchHolders {
            realm_id: self.input.realm_id,
            path: self.path(),
            kind: WatchEventKind::DataUploaded,
        });
        Ok(Next::Call(holders.into()))
    }

    fn emit(&mut self, event: Event) -> Result<Next<()>, IoError> {
        let route = match event {
            Event::Net(NetEvent::WatchHolders(route)) => route,
            other => {
                return Err(IoError::Unexpected {
                    expected: "NetEvent::WatchHolders",
                    got: other.label(),
                });
            }
        };
        if route.is_none() {
            return Ok(Next::Done(()));
        }
        let event_id = self.ids.mint(self.now_ms);
        let input = &self.input;
        let event = WatchEvent {
            event_id,
            realm_id: input.realm_id,
            kind: WatchEventKind::DataUploaded,
            path: self.path(),
            actor: input.auth.user_id,
            occurred_at_ms: self.now_ms,
            detail: WatchEventDetail::DataUploaded {
                group_id: input.group_id,
                node_id: input.node_id,
                bucket: input.bucket.clone(),
                key: input.key.clone(),
                size_bytes: input.size_bytes,
            },
        };
        self.step = PutStep::Emit;
        match watch_event_entry(&event) {
            Ok(entry) => Ok(Next::Call(IoCall::Write(vec![entry]))),
            Err(error) => {
                warn!(%error, "The upload watch event could not be queued");
                Ok(Next::Done(()))
            }
        }
    }
}

impl Operation for CompletePutOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let input = &self.input;
        let queue = QueueLiveOperation::new(LiveVersionInput {
            local_node_id: input.node_id,
            auth_context: input.auth.clone(),
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            version_id: input.version_id,
            delete_marker: false,
        });
        Ok(Next::Call(IoCall::child(queue)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match self.step {
            PutStep::Queue => {
                // The queue step only warns, so its outcome changes nothing here.
                let _ = result.child::<(), IoError>()?;
                self.ask_holders()
            }
            PutStep::Holders => self.emit(result.event()?),
            PutStep::Emit => {
                if let Err(error) = result.done() {
                    warn!(%error, "The upload watch event could not be queued");
                }
                Ok(Next::Then {
                    output: (),
                    calls: vec![IoCall::from(schedule_watch_drain())],
                })
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

/// Queues continuous sync for one committed version or delete marker and wakes the drain. A
/// failure only warns, since the durable obligation remains for repair.
#[derive(Debug, PartialEq)]
pub struct QueueLiveOperation {
    input: LiveVersionInput,
}

impl QueueLiveOperation {
    pub fn new(input: LiveVersionInput) -> Self {
        Self { input }
    }
}

impl Operation for QueueLiveOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let queue = LiveVersionOperation::new(self.input.clone());
        Ok(Next::Call(IoCall::child(queue)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let input = &self.input;
        let (bucket, key, version_id) = (&input.bucket, &input.key, input.version_id);
        match result.child::<QueueBlobResult, BlobQueueError>()? {
            Ok(queued) if queued.queued > 0 && !queued.scheduled => {
                warn!(bucket, key, version_id = %version_id, queued = queued.queued, "Live replication jobs persisted but drain scheduling was not acknowledged");
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    error = %error,
                    bucket,
                    key,
                    version_id = %version_id,
                    delete_marker = input.delete_marker,
                    "Failed to queue live replication after committed write; durable obligation remains for repair"
                );
            }
        }
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::errors::StorageError;

    fn realm() -> RealmId {
        RealmId::from_bytes([1; 32])
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn input() -> CompletePutInput {
        CompletePutInput {
            realm_id: realm(),
            node_id: node(1),
            auth: AuthContext {
                user_id: UserId::nil(realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            group_id: Ulid::from_bytes([3; 16]),
            bucket: "bucket".to_string(),
            key: "file".to_string(),
            version_id: Ulid::from_bytes([4; 16]),
            size_bytes: 9,
        }
    }

    fn done() -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(())))
    }

    fn holders(route: Option<(NodeId, Vec<NodeId>)>) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::WatchHolders(route))))
    }

    /// A put whose queue step ended, waiting for the matching watch holders.
    fn asking() -> CompletePutOperation {
        let mut operation = CompletePutOperation::new(input());
        let queued = operation.first();
        assert!(matches!(queued, Ok(Next::Call(IoCall::Child(_)))));
        let expected = Effect::Net(NetEffect::WatchHolders {
            realm_id: realm(),
            path: operation.path(),
            kind: WatchEventKind::DataUploaded,
        });
        assert_eq!(operation.next(done()), Ok(Next::Call(expected.into())));
        operation
    }

    // A watched put queues its event, then arms the drain after the write.
    #[test]
    fn put_queues_watch() {
        let mut operation = asking();
        let turn = operation.next(holders(Some((node(1), vec![node(2)]))));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
        let armed = Ok(Next::Then {
            output: (),
            calls: vec![IoCall::from(schedule_watch_drain())],
        });
        assert_eq!(operation.next(IoResult::Done), armed);
    }

    #[test]
    fn netless_skips_emit() {
        let mut operation = asking();
        assert_eq!(operation.next(holders(None)), Ok(Next::Done(())));
    }

    fn queueing() -> QueueLiveOperation {
        let input = input();
        let mut operation = QueueLiveOperation::new(LiveVersionInput {
            local_node_id: input.node_id,
            auth_context: input.auth,
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
            delete_marker: true,
        });
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation
    }

    #[test]
    fn queue_failure_warns() {
        // The committed write stands, so the failure ends in a warning only.
        let failed = Err::<QueueBlobResult, _>(BlobQueueError::Storage(StorageError::Timeout));
        let turn = queueing().next(IoResult::Child(ChildOutcome::new(failed)));
        assert_eq!(turn, Ok(Next::Done(())));
    }

    #[test]
    fn queue_ends_done() {
        let queued = Ok::<_, BlobQueueError>(QueueBlobResult {
            queued: 1,
            scheduled: true,
        });
        let turn = queueing().next(IoResult::Child(ChildOutcome::new(queued)));
        assert_eq!(turn, Ok(Next::Done(())));
    }
}
