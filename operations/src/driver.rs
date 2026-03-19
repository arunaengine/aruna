use aruna_core::effects::Effect;
use aruna_core::events::{Event, NetEvent, SubOperationEvent};
use aruna_core::handle::Handle;
use aruna_core::operation::{Operation, SubOperation};
use aruna_net::NetHandle;
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;

use crate::automerge::AutomergeHandle;

#[derive(Debug)]
pub struct DriverContext {
    pub storage_handle: storage::StorageHandle,
    pub net_handle: Option<NetHandle>,
    pub automerge_handle: Option<AutomergeHandle>,
    pub task_handle: Option<TaskHandle>,
}

const MAX_SUBOP_DEPTH: usize = 32;

async fn dispatch_effect(effect: Effect, context: &DriverContext, depth: usize) -> Event {
    match effect {
        Effect::Storage(storage_effect) => {
            context
                .storage_handle
                .send_storage_effect(storage_effect)
                .await
        }
        Effect::Net(net_effect) => {
            if let Some(net_handle) = &context.net_handle {
                net_handle.send_effect(Effect::Net(net_effect)).await
            } else {
                Event::Net(NetEvent::Error(aruna_core::events::NetError::ChannelClosed))
            }
        }
        Effect::Automerge(automerge_effect) => {
            if let Some(automerge_handle) = &context.automerge_handle {
                automerge_handle
                    .send_effect(Effect::Automerge(automerge_effect))
                    .await
            } else {
                Event::Automerge(aruna_core::automerge::AutomergeEvent::SyncRejected {
                    sync_id: ulid::Ulid::new(),
                    document: None,
                    error: aruna_core::automerge::AutomergeSyncError::Network(
                        "automerge handle unavailable".to_string(),
                    ),
                })
            }
        }
        Effect::SubOperation(sub_operation) => {
            if depth >= MAX_SUBOP_DEPTH {
                Event::SubOperation(SubOperationEvent::DepthLimitExceeded {
                    max_depth: MAX_SUBOP_DEPTH,
                })
            } else {
                drive_suboperation(sub_operation, context, depth + 1).await
            }
        }
        Effect::Task(task_effect) => {
            if let Some(task_handle) = &context.task_handle {
                task_handle.send_effect(Effect::Task(task_effect)).await
            } else {
                Event::Task(aruna_core::task::TaskEvent::Error {
                    key: None,
                    message: "task handle unavailable".to_string(),
                })
            }
        }
        Effect::Search() => {
            tracing::warn!("Search effect is not handled by driver yet");
            Event::Search()
        }
        Effect::Stream() => {
            tracing::warn!("Top-level stream effect is not handled by driver yet");
            Event::Stream()
        }
    }
}

fn drive_suboperation<'a>(
    mut operation: Box<dyn SubOperation>,
    context: &'a DriverContext,
    depth: usize,
) -> Pin<Box<dyn Future<Output = Event> + Send + 'a>> {
    Box::pin(async move {
        let mut queue: VecDeque<_> = operation.start().into_iter().collect();

        while !operation.is_complete() {
            while let Some(effect) = queue.pop_front() {
                let event = dispatch_effect(effect, context, depth).await;
                queue.extend(operation.step(event));
            }

            if queue.is_empty() && !operation.is_complete() {
                queue.extend(operation.abort());
                if queue.is_empty() {
                    break;
                }
            }
        }

        operation.finalize()
    })
}

pub async fn drive<O: Operation>(
    mut operation: O,
    context: &DriverContext,
) -> Result<O::Output, O::Error> {
    let mut queue: VecDeque<_> = operation.start().into_iter().collect();

    while !operation.is_complete() {
        while let Some(effect) = queue.pop_front() {
            let event = dispatch_effect(effect, context, 0).await;
            queue.extend(operation.step(event));
        }

        if queue.is_empty() && !operation.is_complete() {
            queue.extend(operation.abort());
            if queue.is_empty() {
                break;
            }
        }
    }
    operation.finalize()
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use aruna_core::{
        automerge::AutomergeDocumentVariant,
        effects::{Effect, StorageEffect},
        events::{Event, StorageEvent, SubOperationEvent},
        operation::{Operation, boxed_suboperation},
        task::{TaskEffect, TaskKey},
    };
    use aruna_storage::storage;
    use byteview::ByteView;
    use std::convert::Infallible;
    use tempfile::tempdir;

    #[derive(Debug, PartialEq)]
    pub struct TestOperation {
        pub state: u8,
        pub txn_id: Option<aruna_core::types::TxnId>,
    }

    impl TestOperation {
        pub fn new() -> Self {
            TestOperation {
                state: 0,
                txn_id: None,
            }
        }
    }

    impl Operation for TestOperation {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;

            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, events: aruna_core::events::Event) -> aruna_core::types::Effects {
            match (events, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.state = 2;
                    self.txn_id = Some(txn_id);
                    eprintln!("Transaction started with id {:?}", txn_id);
                    smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"key1"),
                        value: ByteView::from(*b"value1"),
                        txn_id: Some(txn_id),
                    })]
                }
                (Event::Storage(StorageEvent::WriteResult { key: _ }), 2) => {
                    self.state = 3;
                    eprintln!("Write completed, committing transaction.");
                    smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.unwrap(),
                    })]
                }
                (Event::Storage(StorageEvent::TransactionCommitted { txn_id: _ }), 3) => {
                    self.state = 4;
                    eprintln!("Transaction committed, reading back value.");
                    smallvec::smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"key1"),
                        txn_id: None,
                    })]
                }
                (Event::Storage(StorageEvent::ReadResult { key, value }), 4) => {
                    self.state = 5;

                    eprintln!("Read key: {:?}, value: {:?}", key, value);
                    assert_eq!(key, ByteView::from(*b"key1"));
                    assert_eq!(value, Some(ByteView::from(*b"value1")));
                    self.state = 6;
                    smallvec::smallvec![]
                }

                a => {
                    eprintln!("Unexpected event/state combination {:?}", a);
                    smallvec::smallvec![]
                }
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 6
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(())
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    pub async fn test_driver() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            automerge_handle: None,
            task_handle: None,
        };

        let operation = TestOperation::new();
        let result = drive(operation, &context).await;
        assert!(result.is_ok());
    }

    #[derive(Debug, PartialEq)]
    struct EffectOrderOperation {
        observed: Vec<&'static str>,
    }

    impl EffectOrderOperation {
        fn new() -> Self {
            Self {
                observed: Vec::new(),
            }
        }
    }

    impl Operation for EffectOrderOperation {
        type Output = Vec<&'static str>;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![
                Effect::Task(TaskEffect::CancelTimer {
                    key: TaskKey::AutomergeAnnounce(AutomergeDocumentVariant::GroupAuthorization {
                        group_id: ulid::Ulid::from_bytes([0u8; 16]),
                    }),
                }),
                Effect::Search()
            ]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match event {
                Event::Task(_) => self.observed.push("task"),
                Event::Search() => self.observed.push("search"),
                _ => {}
            }
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed.len() == 2
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.observed)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn test_driver_preserves_effect_order_fifo() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            automerge_handle: None,
            task_handle: None,
        };

        let operation = EffectOrderOperation::new();
        let observed = drive(operation, &context)
            .await
            .expect("drive should succeed");
        assert_eq!(observed, vec!["task", "search"]);
    }

    #[derive(Debug, PartialEq)]
    struct RecursiveSubOperation {
        observed: Option<Event>,
    }

    impl RecursiveSubOperation {
        fn new() -> Self {
            Self { observed: None }
        }
    }

    impl Operation for RecursiveSubOperation {
        type Output = Event;
        type Error = Infallible;

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::SubOperation(boxed_suboperation(
                RecursiveSubOperation::new(),
                |result| match result {
                    Ok(event) => event,
                    Err(never) => match never {},
                },
            ))]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            self.observed = Some(event);
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed.is_some()
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self
                .observed
                .expect("recursive suboperation should produce an event"))
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn test_suboperation_depth_limit_is_enforced() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            automerge_handle: None,
            task_handle: None,
        };

        let event = drive(RecursiveSubOperation::new(), &context)
            .await
            .expect("recursive suboperation should resolve to depth-limit event");

        assert!(matches!(
            event,
            Event::SubOperation(SubOperationEvent::DepthLimitExceeded { max_depth })
                if max_depth == super::MAX_SUBOP_DEPTH
        ));
    }
}
