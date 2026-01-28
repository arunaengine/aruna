use aruna_core::operation::Operation;
use aruna_storage::storage;

pub struct DriverContext {
    pub storage_handle: storage::StorageHandle,
}

pub async fn drive<O: Operation>(
    mut operation: O,
    context: &DriverContext,
) -> Result<O::Output, O::Error> {
    let mut queue = operation.start();

    while !operation.is_complete() {
        while let Some(effect) = queue.pop() {
            match effect {
                aruna_core::effects::Effect::Storage(storage_effect) => {
                    let event = context.storage_handle.send_effect(storage_effect).await;
                    queue.extend(operation.step(event));
                }
                aruna_core::effects::Effect::Network() => todo!(),
                aruna_core::effects::Effect::Task() => todo!(),
                aruna_core::effects::Effect::Search() => todo!(),
                aruna_core::effects::Effect::Stream() => todo!(),
            }
        }
    }
    operation.finalize()
}

#[cfg(test)]
mod test {
    use aruna_core::{
        effects::{Effect, StorageEffect},
        events::{Event, StorageEvent},
        operation::Operation,
    };
    use aruna_storage::storage;
    use byteview::ByteView;

    use crate::driver::{DriverContext, drive};

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
        let storage_handle = storage::FjallStorage::new("/tmp/aruna_test_db").unwrap();

        let context = DriverContext { storage_handle };

        let operation = TestOperation::new();
        let result = drive(operation, &context).await;
        assert!(result.is_ok());
    }
}
