//! The Fjall-backed storage handle, its worker, and transaction ownership.
mod compaction;
pub mod errors;
pub mod storage;

pub use storage::{FjallPersistPolicy, FjallStorage, StorageHandle};
