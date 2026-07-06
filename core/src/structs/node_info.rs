use crate::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Derived read-only label carrying a node's `RealmNode.kind`; writes are rejected.
pub const KIND_LABEL_KEY: &str = "aruna.io/kind";

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodeInfoDocument {
    pub node_id: NodeId,
    pub labels: BTreeMap<String, String>,
    pub urls: NodeUrls,
    pub utilization: NodeUtilization,
    pub updated_at_ms: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodeUrls {
    pub api: Option<String>,
    pub s3: Option<String>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodeUtilization {
    pub storage_bytes_used: u64,
    pub documents_held: u64,
    pub load_permille: u32,
    pub heartbeat_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn node_info_document_round_trips() {
        let document = NodeInfoDocument {
            node_id: node(1),
            labels: BTreeMap::from([(KIND_LABEL_KEY.to_string(), "Server".to_string())]),
            urls: NodeUrls {
                api: Some("https://api.example".to_string()),
                s3: None,
            },
            utilization: NodeUtilization {
                storage_bytes_used: 1_024,
                documents_held: 7,
                load_permille: 250,
                heartbeat_at_ms: 1_700_000_000_000,
            },
            updated_at_ms: 1_700_000_000_500,
        };
        let bytes = postcard::to_allocvec(&document).unwrap();
        assert_eq!(
            postcard::from_bytes::<NodeInfoDocument>(&bytes).unwrap(),
            document
        );
    }
}
