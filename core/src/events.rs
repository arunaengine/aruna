use crate::{
    errors::StorageError,
    types::{Key, TxnId, Value},
};

#[derive(Debug)]
pub enum Event {
    Storage(StorageEvent),
    Network(),
    Task(),
    Search(),
    Stream(),
}

#[derive(Debug)]
pub enum StorageEvent {
    TransactionStarted { txn_id: TxnId },
    TransactionCommitted { txn_id: TxnId },
    TransactionAborted { txn_id: TxnId },
    ReadResult { key: Key, value: Option<Value> },
    IterResult { values: Vec<(Key, Value)> },
    WriteResult { key: Key },
    DeleteResult { key: Key },
    Error { error: StorageError },
}
