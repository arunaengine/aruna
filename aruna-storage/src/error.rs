#[derive(thiserror::Error, Debug)]
pub enum ArunaStorageError {
    #[error("LMDB error: {0}")]
    HeedError(#[from] heed::Error),
    #[error("Fjall error: {0}")]
    FjallError(#[from] fjall::Error),
    #[error("Redb error: {0}")]
    RedbError(#[from] redb::Error),
    #[error("Redb commit error: {0}")]
    RedbCommitError(#[from] redb::CommitError),
    #[error("Redb database error: {0}")]
    RedbDatabaseError(#[from] redb::DatabaseError),
    #[error("Redb transaction error: {0}")]
    RedbTransactionError(#[from] redb::TransactionError),
    #[error("Redb table error: {0}")]
    RedbTableError(#[from] redb::TableError),
    #[error("Redb storage error: {0}")]
    RedbStorageError(#[from] redb::StorageError),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Database error: {0}")]
    DatabaseError(String),
}
