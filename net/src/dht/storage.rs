use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::util::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const DHT_KEYSPACE: &str = "dht";
const CLEANUP_PAGE_SIZE: usize = 256;

/// Serde helper for NodeId (iroh::PublicKey)
mod node_id_serde {
    use super::*;

    pub fn serialize<S: Serializer>(key: &NodeId, s: S) -> Result<S::Ok, S::Error> {
        key.as_bytes().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<NodeId, D::Error> {
        let bytes: [u8; 32] = Deserialize::deserialize(d)?;
        NodeId::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

/// Serde helper for Option<[u8; 64]> (Ed25519 signature)
mod opt_signature_serde {
    use super::*;

    pub fn serialize<S: Serializer>(sig: &Option<[u8; 64]>, s: S) -> Result<S::Ok, S::Error> {
        match sig {
            Some(bytes) => {
                let vec: Vec<u8> = bytes.to_vec();
                Some(vec).serialize(s)
            }
            None => None::<Vec<u8>>.serialize(s),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<[u8; 64]>, D::Error> {
        let opt: Option<Vec<u8>> = Deserialize::deserialize(d)?;
        match opt {
            Some(vec) => {
                if vec.len() != 64 {
                    return Err(serde::de::Error::custom("signature must be 64 bytes"));
                }
                let mut arr = [0u8; 64];
                arr.copy_from_slice(&vec);
                Ok(Some(arr))
            }
            None => Ok(None),
        }
    }
}

/// A value stored in the DHT
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEntry {
    #[serde(with = "node_id_serde")]
    pub publisher: NodeId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    /// Optional Ed25519 signature for publisher verification
    #[serde(with = "opt_signature_serde")]
    pub signature: Option<[u8; 64]>,
}

pub struct DhtStorage {
    storage: StorageHandle,
    write_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for DhtStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtStorage").finish()
    }
}

impl DhtStorage {
    pub fn new(storage: StorageHandle) -> Self {
        Self {
            storage,
            write_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Get all non-expired entries for a key
    pub async fn get(&self, key: &DhtKeyId) -> Vec<StoredEntry> {
        let effect = Effect::Storage(StorageEffect::Read {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            txn_id: None,
        });

        match self.storage.send_effect(effect).await {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(data), ..
            }) => {
                let entries: Vec<StoredEntry> = postcard::from_bytes(&data).unwrap_or_default();
                let now = unix_timestamp_secs();
                entries.into_iter().filter(|e| e.expires_at > now).collect()
            }
            _ => vec![],
        }
    }

    /// Store a value, deduplicating by publisher
    pub async fn put(&self, key: &DhtKeyId, entry: StoredEntry) {
        let _guard = self.write_lock.lock().await;
        let mut entries = self.get(key).await;
        let now = unix_timestamp_secs();

        // Remove expired and entries from same publisher
        entries.retain(|e| e.expires_at > now && e.publisher != entry.publisher);
        entries.push(entry);

        let Ok(data) = postcard::to_allocvec(&entries) else {
            return; // Serialization failed - skip
        };

        let effect = Effect::Storage(StorageEffect::Write {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            value: ByteView::from(data),
            txn_id: None,
        });

        let _ = self.storage.send_effect(effect).await;
    }

    /// Remove expired entries for a key
    pub async fn cleanup(&self, key: &DhtKeyId) {
        let _guard = self.write_lock.lock().await;
        let entries = self.get(key).await; // Already filters expired
        if entries.is_empty() {
            // Delete the key entirely
            let effect = Effect::Storage(StorageEffect::Delete {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                txn_id: None,
            });
            let _ = self.storage.send_effect(effect).await;
        } else {
            // Re-store cleaned entries
            let Ok(data) = postcard::to_allocvec(&entries) else {
                return;
            };
            let effect = Effect::Storage(StorageEffect::Write {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                value: ByteView::from(data),
                txn_id: None,
            });
            let _ = self.storage.send_effect(effect).await;
        }
    }

    /// Remove expired entries from all DHT keys
    pub async fn cleanup_all(&self) {
        let _guard = self.write_lock.lock().await;
        let now = unix_timestamp_secs();
        let mut start_after = None;

        loop {
            let effect = Effect::Storage(StorageEffect::Iter {
                key_space: DHT_KEYSPACE.to_string(),
                prefix: None,
                start_after: start_after.clone(),
                limit: CLEANUP_PAGE_SIZE,
                txn_id: None,
            });

            let Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) = self.storage.send_effect(effect).await
            else {
                return;
            };

            for (key, value) in values {
                let stored_entries: Vec<StoredEntry> =
                    postcard::from_bytes(&value).unwrap_or_default();
                let original_len = stored_entries.len();

                let valid_entries: Vec<StoredEntry> = stored_entries
                    .into_iter()
                    .filter(|e| e.expires_at > now)
                    .collect();

                if valid_entries.is_empty() {
                    let delete_effect = Effect::Storage(StorageEffect::Delete {
                        key_space: DHT_KEYSPACE.to_string(),
                        key,
                        txn_id: None,
                    });
                    let _ = self.storage.send_effect(delete_effect).await;
                } else if valid_entries.len() < original_len {
                    let Ok(data) = postcard::to_allocvec(&valid_entries) else {
                        continue;
                    };
                    let write_effect = Effect::Storage(StorageEffect::Write {
                        key_space: DHT_KEYSPACE.to_string(),
                        key,
                        value: ByteView::from(data),
                        txn_id: None,
                    });
                    let _ = self.storage.send_effect(write_effect).await;
                }
            }

            if let Some(next) = next_start_after {
                start_after = Some(next);
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_node(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    #[tokio::test]
    async fn test_concurrent_puts_preserve_publishers() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage = aruna_storage::FjallStorage::open(
            temp_dir
                .path()
                .to_str()
                .expect("temp path must be valid utf8"),
        )
        .expect("open storage");

        let dht_storage = Arc::new(DhtStorage::new(storage));
        let key = DhtKeyId::from_data(b"concurrent-put");
        let task_count = 16usize;
        let barrier = Arc::new(tokio::sync::Barrier::new(task_count));

        let mut tasks = Vec::with_capacity(task_count);
        for i in 0..task_count {
            let dht_storage = dht_storage.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let mut signature = [0u8; 64];
                signature[0] = i as u8;
                let entry = StoredEntry {
                    publisher: make_node((i + 1) as u8),
                    value: vec![i as u8],
                    expires_at: unix_timestamp_secs() + 120,
                    signature: Some(signature),
                };

                barrier.wait().await;
                dht_storage.put(&key, entry).await;
            }));
        }

        for task in tasks {
            task.await.expect("task join");
        }

        let mut entries = dht_storage.get(&key).await;
        entries.sort_by_key(|entry| entry.value[0]);

        assert_eq!(entries.len(), task_count);
        for (idx, entry) in entries.iter().enumerate() {
            assert_eq!(entry.value, vec![idx as u8]);
        }
    }
}
