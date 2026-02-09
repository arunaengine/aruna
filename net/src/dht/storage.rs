use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::util::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const DHT_KEYSPACE: &str = "dht";

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
}

impl std::fmt::Debug for DhtStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtStorage").finish()
    }
}

impl DhtStorage {
    pub fn new(storage: StorageHandle) -> Self {
        Self { storage }
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
        // Scan all keys in the DHT keyspace
        let effect = Effect::Storage(StorageEffect::Scan {
            key_space: DHT_KEYSPACE.to_string(),
            prefix: None,
        });

        let Event::Storage(StorageEvent::ScanResult { entries }) =
            self.storage.send_effect(effect).await
        else {
            return;
        };

        let now = unix_timestamp_secs();

        for (key, value) in entries {
            // Deserialize the stored entries
            let stored_entries: Vec<StoredEntry> =
                postcard::from_bytes(&value).unwrap_or_default();

            // Filter out expired entries
            let valid_entries: Vec<StoredEntry> = stored_entries
                .into_iter()
                .filter(|e| e.expires_at > now)
                .collect();

            if valid_entries.is_empty() {
                // Delete the key entirely
                let delete_effect = Effect::Storage(StorageEffect::Delete {
                    key_space: DHT_KEYSPACE.to_string(),
                    key,
                    txn_id: None,
                });
                let _ = self.storage.send_effect(delete_effect).await;
            } else if valid_entries.len()
                < postcard::from_bytes::<Vec<StoredEntry>>(&value)
                    .map(|v| v.len())
                    .unwrap_or(0)
            {
                // Some entries were removed, re-store the cleaned list
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
    }
}
