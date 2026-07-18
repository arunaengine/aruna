/// Application Layer Protocol Negotiation identifiers for Aruna streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Alpn {
    /// DHT RPC protocol
    Dht,
    /// BAO content streaming protocol
    Bao,
    /// Durable document sync protocol
    DocumentSync,
    /// Metadata bootstrap protocol
    Metadata,
    /// Lazy Aruna-native reference reads
    NativeReference,
    /// Notification delivery protocol
    Notification,
    /// Shard holder-manifest exchange protocol
    Shard,
}

impl Alpn {
    pub const fn as_bytes(&self) -> &'static [u8] {
        match self {
            Alpn::Dht => b"aruna/dht/2",
            Alpn::Bao => b"aruna/bao/1",
            Alpn::DocumentSync => irokle::net::IROKLE_SYNC_ALPN,
            Alpn::Metadata => b"aruna/metadata/1",
            Alpn::NativeReference => b"aruna/native/1",
            Alpn::Notification => b"aruna/notification/1",
            Alpn::Shard => b"aruna/shard/1",
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"aruna/dht/2" => Some(Alpn::Dht),
            b"aruna/bao/1" => Some(Alpn::Bao),
            irokle::net::IROKLE_SYNC_ALPN => Some(Alpn::DocumentSync),
            b"aruna/metadata/1" => Some(Alpn::Metadata),
            b"aruna/native/1" => Some(Alpn::NativeReference),
            b"aruna/notification/1" => Some(Alpn::Notification),
            b"aruna/shard/1" => Some(Alpn::Shard),
            _ => None,
        }
    }
}

impl std::fmt::Display for Alpn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Alpn::Dht => write!(f, "aruna/dht/2"),
            Alpn::Bao => write!(f, "aruna/bao/1"),
            Alpn::DocumentSync => match std::str::from_utf8(irokle::net::IROKLE_SYNC_ALPN) {
                Ok(value) => write!(f, "{value}"),
                Err(_) => write!(f, "<invalid-document-sync-alpn>"),
            },
            Alpn::Metadata => write!(f, "aruna/metadata/1"),
            Alpn::NativeReference => write!(f, "aruna/native/1"),
            Alpn::Notification => write!(f, "aruna/notification/1"),
            Alpn::Shard => write!(f, "aruna/shard/1"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpn_roundtrip() {
        assert_eq!(Alpn::from_bytes(Alpn::Dht.as_bytes()), Some(Alpn::Dht));
        assert_eq!(Alpn::from_bytes(Alpn::Bao.as_bytes()), Some(Alpn::Bao));
        assert_eq!(
            Alpn::from_bytes(Alpn::DocumentSync.as_bytes()),
            Some(Alpn::DocumentSync)
        );
        assert_eq!(
            Alpn::from_bytes(Alpn::Metadata.as_bytes()),
            Some(Alpn::Metadata)
        );
        assert_eq!(
            Alpn::from_bytes(Alpn::NativeReference.as_bytes()),
            Some(Alpn::NativeReference)
        );
        assert_eq!(
            Alpn::from_bytes(Alpn::Notification.as_bytes()),
            Some(Alpn::Notification)
        );
        assert_eq!(Alpn::from_bytes(Alpn::Shard.as_bytes()), Some(Alpn::Shard));
    }

    #[test]
    fn test_alpn_unknown() {
        assert_eq!(Alpn::from_bytes(b"unknown"), None);
    }
}
