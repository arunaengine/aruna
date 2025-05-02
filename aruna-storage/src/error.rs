#[derive(thiserror::Error, Debug)]
pub enum ArunaStorageError {
    #[error("LMDB error: {0}")]
    HeedError(#[from] heed::Error),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Database error: {0}")]
    DatabaseError(String),
}
