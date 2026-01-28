use crate::types::{Key, KeySpace, TxnId, Value};

pub enum Effect {
    Storage(StorageEffect),
    Network(),
    Task(),
    Search(),
    Stream(),
}

pub enum StorageEffect {
    StartTransaction {
        read: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
    },
    Read {
        key_space: KeySpace,
        key: Key,
        txn_id: Option<TxnId>,
    },
    Write {
        key_space: KeySpace,
        key: Key,
        value: Value,
        txn_id: Option<TxnId>,
    },
    Delete {
        key_space: KeySpace,
        key: Key,
        txn_id: Option<TxnId>,
    },
    AbortTransaction {
        txn_id: TxnId,
    },
}
