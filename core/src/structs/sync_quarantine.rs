use crate::NodeId;
use crate::errors::ConversionError;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// A replicated sync event that failed permanent validation, retained for
/// inspection instead of being silently dropped (#338). The topic still advances
/// past it; this is the durable record on top of the accept/reject classification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncQuarantineRecord {
    /// Raw sync-topic bytes the event arrived on.
    pub topic: Vec<u8>,
    pub event_id: Ulid,
    pub origin_node_id: NodeId,
    pub reason: String,
    pub quarantined_at_ms: u64,
    /// The postcard-encoded rejected event, for out-of-band inspection.
    pub event_bytes: Vec<u8>,
}

impl SyncQuarantineRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Key: `topic || event_id`. Re-delivery of the same poison event overwrites its
/// own row, so the store is bounded by the count of distinct invalid events.
pub fn sync_quarantine_key(topic: &[u8], event_id: Ulid) -> Vec<u8> {
    let mut key = Vec::with_capacity(topic.len() + 16);
    key.extend_from_slice(topic);
    key.extend_from_slice(&event_id.to_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrips_and_key_is_prefixed_by_topic() {
        let record = SyncQuarantineRecord {
            topic: vec![7u8; 32],
            event_id: Ulid::from_bytes([1; 16]),
            origin_node_id: NodeId::from_bytes(&[1u8; 32]).unwrap(),
            reason: "unauthorized".to_string(),
            quarantined_at_ms: 42,
            event_bytes: vec![9, 9, 9],
        };
        assert_eq!(
            SyncQuarantineRecord::from_bytes(&record.to_bytes().unwrap()).unwrap(),
            record
        );
        let key = sync_quarantine_key(&record.topic, record.event_id);
        assert!(key.starts_with(&record.topic));
        assert_eq!(key.len(), record.topic.len() + 16);
    }
}
