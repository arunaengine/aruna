use aruna_blob::blob::BlobHandle;
use aruna_core::effects::Effect;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, NetEvent, SubOperationEvent};
use aruna_core::handle::Handle;
use aruna_core::operation::{Operation, SubOperation};
use aruna_net::NetHandle;
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use std::any::{type_name, type_name_of_val};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use tracing::{Instrument, debug_span, error, trace};

use crate::metadata::MetadataHandle;
use aruna_core::events::NetError;
use aruna_core::metadata::{MetadataError, MetadataEvent};
use aruna_core::task::TaskEvent;

#[derive(Clone, Debug)]
pub struct DriverContext {
    pub storage_handle: storage::StorageHandle,
    pub net_handle: Option<NetHandle>,
    pub blob_handle: Option<BlobHandle>,
    pub metadata_handle: Option<MetadataHandle>,
    pub task_handle: Option<TaskHandle>,
}

const MAX_SUBOP_DEPTH: usize = 32;

#[tracing::instrument(
    name = "operation.effect",
    level = "debug",
    skip(effect, context),
    fields(depth, effect = effect_kind(&effect))
)]
async fn dispatch_effect(effect: Effect, context: &DriverContext, depth: usize) -> Event {
    let effect_name = effect_kind(&effect);
    if depth == 0 {
        tracing::debug!(
            effect = effect_name,
            "Dispatching top-level operation effect"
        );
    }
    trace!(
        event = "operation.effect.dispatch",
        depth,
        effect = effect_name,
        "Dispatching operation effect"
    );

    let event = match effect {
        Effect::Blob(blob_effect) => {
            if let Some(blob_handle) = &context.blob_handle {
                blob_handle.send_blob_effect(blob_effect).await
            } else {
                Event::Blob(BlobEvent::Error(BlobError::HandleMissing))
            }
        }
        Effect::StagingSource(staging_source_effect) => {
            if let Some(blob_handle) = &context.blob_handle {
                blob_handle
                    .send_staging_source_effect(staging_source_effect)
                    .await
            } else {
                Event::StagingSource(aruna_core::events::StagingSourceEvent::Error {
                    error: aruna_core::errors::StagingSourceError::HandleMissing,
                })
            }
        }
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
                Event::Net(NetEvent::Error(NetError::ChannelClosed))
            }
        }
        Effect::Metadata(metadata_effect) => {
            if let Some(metadata_handle) = &context.metadata_handle {
                metadata_handle
                    .send_effect(Effect::Metadata(metadata_effect))
                    .await
            } else {
                Event::Metadata(MetadataEvent::Error {
                    graph_iri: None,
                    error: MetadataError::HandleMissing,
                })
            }
        }
        Effect::SubOperation(sub_operation) => {
            if depth >= MAX_SUBOP_DEPTH {
                Event::SubOperation(SubOperationEvent::DepthLimitExceeded {
                    max_depth: MAX_SUBOP_DEPTH,
                })
            } else {
                let context = context.clone();
                tokio::spawn(
                    async move { drive_suboperation(sub_operation, &context, depth + 1).await },
                )
                .await
                .expect("suboperation task panicked or was cancelled")
            }
        }
        Effect::Task(task_effect) => {
            if let Some(task_handle) = &context.task_handle {
                task_handle.send_effect(Effect::Task(task_effect)).await
            } else {
                Event::Task(TaskEvent::Error {
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
    };

    trace!(
        event = "operation.effect.result",
        depth,
        effect = effect_name,
        result = event_kind(&event),
        "Received operation event"
    );
    if depth == 0 {
        tracing::debug!(
            effect = effect_name,
            result = event_kind(&event),
            "Received top-level operation event"
        );
    }

    event
}

fn drive_suboperation<'a>(
    mut operation: Box<dyn SubOperation>,
    context: &'a DriverContext,
    depth: usize,
) -> Pin<Box<dyn Future<Output = Event> + Send + 'a>> {
    let operation_name = type_name_of_val(&*operation).to_string();
    Box::pin(async move {
        let span = debug_span!("suboperation", operation = %operation_name, depth);
        async move {
            trace!(
                event = "suboperation.started",
                operation = %operation_name,
                depth,
                "Starting suboperation"
            );
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

            trace!(
                event = "suboperation.completed",
                operation = %operation_name,
                depth,
                "Completed suboperation"
            );
            operation.finalize()
        }
        .instrument(span)
        .await
    })
}

#[tracing::instrument(
    name = "operation",
    level = "debug",
    skip(operation, context),
    fields(operation = type_name::<O>())
)]
pub async fn drive<O: Operation>(
    mut operation: O,
    context: &DriverContext,
) -> Result<O::Output, O::Error> {
    let operation_name = type_name::<O>();

    trace!(
        event = "operation.started",
        operation = %operation_name,
        "Starting operation"
    );

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
    let result = operation.finalize();
    match &result {
        Ok(_) => trace!(
            event = "operation.completed",
            operation = %operation_name,
            "Completed operation"
        ),
        Err(error) => error!(
            event = "operation.failed",
            operation = %operation_name,
            error = ?error,
            "Operation failed"
        ),
    }
    result
}

fn effect_kind(effect: &Effect) -> &'static str {
    match effect {
        Effect::Blob(_) => "blob",
        Effect::StagingSource(_) => "staging_source",
        Effect::Storage(_) => "storage",
        Effect::Net(_) => "net",
        Effect::Metadata(_) => "metadata",
        Effect::SubOperation(_) => "suboperation",
        Effect::Task(_) => "task",
        Effect::Search() => "search",
        Effect::Stream() => "stream",
    }
}

fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::Blob(_) => "blob",
        Event::StagingSource(_) => "staging_source",
        Event::Storage(_) => "storage",
        Event::Net(_) => "net",
        Event::Metadata(_) => "metadata",
        Event::SubOperation(_) => "suboperation",
        Event::Task(_) => "task",
        Event::Search() => "search",
        Event::Stream() => "stream",
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use aruna_core::{
        effects::{Effect, StagingSourceEffect, StorageEffect},
        events::{Event, StagingSourceEvent, StorageEvent, SubOperationEvent},
        operation::{Operation, boxed_suboperation},
        structs::{ResolvedSourceAccess, SourceConnectorKind},
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
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
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
                    key: TaskKey::RealmPresence {
                        realm_id: aruna_core::structs::RealmId::from_bytes([0u8; 32]),
                        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                    },
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
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let operation = EffectOrderOperation::new();
        let observed = drive(operation, &context)
            .await
            .expect("drive should succeed");
        assert_eq!(observed, vec!["task", "search"]);
    }

    #[derive(Debug, PartialEq)]
    struct StagingSourceDispatchOperation {
        observed_staging_source: bool,
    }

    impl StagingSourceDispatchOperation {
        fn new() -> Self {
            Self {
                observed_staging_source: false,
            }
        }
    }

    impl Operation for StagingSourceDispatchOperation {
        type Output = bool;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::StagingSource(StagingSourceEffect::Head {
                access: ResolvedSourceAccess::OpenDal {
                    kind: SourceConnectorKind::Http,
                    config: std::collections::HashMap::from([(
                        "endpoint".to_string(),
                        "https://missing.example.org".to_string(),
                    )]),
                    path: "file.txt".to_string(),
                    version: None,
                },
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            self.observed_staging_source = matches!(
                event,
                Event::StagingSource(StagingSourceEvent::Error { .. })
            );
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed_staging_source
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.observed_staging_source)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn test_driver_dispatches_staging_source_effect_via_blob_handle() {
        let temp_dir = tempdir().unwrap();
        let temp_root = temp_dir.path().to_str().unwrap().to_string();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(&temp_root).unwrap();
        let net_handle =
            aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage_handle.clone())
                .await
                .unwrap();
        let blob_handle = aruna_blob::blob::BlobHandler::new(
            aruna_core::structs::BackendConfig {
                backend_type: aruna_core::structs::Backend::FileSystem,
                root: blob_root,
                service_config: std::collections::HashMap::new(),
                bucket_prefix: Some("aruna-test-".to_string()),
                max_bucket_size: Some(1),
                multipart_bucket: Some("uploaded-parts".to_string()),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle,
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
        };

        let observed = drive(StagingSourceDispatchOperation::new(), &context)
            .await
            .expect("staging source effect should be dispatched");
        assert!(observed);
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
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
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
