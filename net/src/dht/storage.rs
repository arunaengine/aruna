use aruna_core::id::NodeId;
use aruna_core::util::unix_timestamp_secs;
use serde::{Deserialize, Serialize};
pub const CLEANUP_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEntry {
    pub publisher: NodeId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    pub signature: Option<iroh::Signature>,
}

pub fn decode_entries(bytes: &[u8]) -> Vec<StoredEntry> {
    postcard::from_bytes(bytes).unwrap_or_default()
}

pub fn encode_entries(entries: &[StoredEntry]) -> Option<Vec<u8>> {
    postcard::to_allocvec(entries).ok()
}

pub fn live_entries(entries: Vec<StoredEntry>, now: u64) -> Vec<StoredEntry> {
    entries
        .into_iter()
        .filter(|entry| entry.expires_at > now)
        .collect()
}

pub fn merge_entry(
    entries: Vec<StoredEntry>,
    new_entry: StoredEntry,
    now: u64,
) -> Vec<StoredEntry> {
    let mut filtered: Vec<StoredEntry> = entries
        .into_iter()
        .filter(|entry| entry.expires_at > now && entry.publisher != new_entry.publisher)
        .collect();
    filtered.push(new_entry);
    filtered
}

pub fn now_unix_secs() -> u64 {
    unix_timestamp_secs()
}
