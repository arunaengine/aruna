//! Application Layer Protocol Negotiation identifiers for Aruna streams.
//!
//! The version suffix is the whole compatibility contract: a peer whose frames
//! differ never negotiates the ALPN, so it fails the connection instead of
//! decoding foreign bytes. There is no fallback ALPN and no downgrade.

use crate::structs::RealmNodeKind;

/// Which side of a connection is being judged. The same table answers all
/// three, so a protocol that is one-directional for a node kind says so in one
/// place instead of in three checks that can drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlpnRole {
    /// A remote key dialing this node.
    PeerInbound,
    /// This node accepting the protocol for itself.
    LocalServe,
    /// This node dialing the protocol at someone else.
    LocalDial,
}

impl AlpnRole {
    /// Every role, so the matrix contract test covers all of them.
    pub const ALL: [AlpnRole; 3] = [
        AlpnRole::PeerInbound,
        AlpnRole::LocalServe,
        AlpnRole::LocalDial,
    ];
}

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
    /// Placement-routed durable job control protocol
    JobControl,
}

impl Alpn {
    /// Every protocol, so a matrix or dispatch review can iterate all of them.
    pub const ALL: [Alpn; 8] = [
        Alpn::Dht,
        Alpn::Bao,
        Alpn::DocumentSync,
        Alpn::Metadata,
        Alpn::NativeReference,
        Alpn::Notification,
        Alpn::Shard,
        Alpn::JobControl,
    ];

    /// The ALPN x node-kind allow matrix, consulted on both sides of every
    /// connection. `None` is a key the realm config does not name: it keeps the
    /// pre-matrix provisional behaviour and is bounded by admission instead.
    ///
    /// A `User` device speaks the read and forward surface only. Document sync
    /// and shard exchange are realm infrastructure a device never touches in
    /// any direction; job control it dials for its owner but never serves.
    pub const fn permits(&self, kind: Option<&RealmNodeKind>, role: AlpnRole) -> bool {
        match kind {
            None | Some(RealmNodeKind::Management) | Some(RealmNodeKind::Server) => true,
            Some(RealmNodeKind::User { .. }) => match self {
                Alpn::Dht
                | Alpn::Bao
                | Alpn::Metadata
                | Alpn::NativeReference
                | Alpn::Notification => true,
                Alpn::DocumentSync | Alpn::Shard => false,
                Alpn::JobControl => match role {
                    AlpnRole::PeerInbound | AlpnRole::LocalDial => true,
                    AlpnRole::LocalServe => false,
                },
            },
        }
    }

    pub const fn as_bytes(&self) -> &'static [u8] {
        match self {
            Alpn::Dht => b"aruna/dht/2",
            Alpn::Bao => b"aruna/bao/2",
            Alpn::DocumentSync => irokle::net::IROKLE_SYNC_ALPN,
            Alpn::Metadata => b"aruna/metadata/2",
            Alpn::NativeReference => b"aruna/native/1",
            Alpn::Notification => b"aruna/notification/1",
            Alpn::Shard => b"aruna/shard/1",
            Alpn::JobControl => b"aruna/job-control/2",
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"aruna/dht/2" => Some(Alpn::Dht),
            b"aruna/bao/2" => Some(Alpn::Bao),
            irokle::net::IROKLE_SYNC_ALPN => Some(Alpn::DocumentSync),
            b"aruna/metadata/2" => Some(Alpn::Metadata),
            b"aruna/native/1" => Some(Alpn::NativeReference),
            b"aruna/notification/1" => Some(Alpn::Notification),
            b"aruna/shard/1" => Some(Alpn::Shard),
            b"aruna/job-control/2" => Some(Alpn::JobControl),
            _ => None,
        }
    }
}

impl std::fmt::Display for Alpn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Alpn::Dht => write!(f, "aruna/dht/2"),
            Alpn::Bao => write!(f, "aruna/bao/2"),
            Alpn::DocumentSync => match std::str::from_utf8(irokle::net::IROKLE_SYNC_ALPN) {
                Ok(value) => write!(f, "{value}"),
                Err(_) => write!(f, "<invalid-document-sync-alpn>"),
            },
            Alpn::Metadata => write!(f, "aruna/metadata/2"),
            Alpn::NativeReference => write!(f, "aruna/native/1"),
            Alpn::Notification => write!(f, "aruna/notification/1"),
            Alpn::Shard => write!(f, "aruna/shard/1"),
            Alpn::JobControl => write!(f, "aruna/job-control/2"),
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
        assert_eq!(
            Alpn::from_bytes(Alpn::JobControl.as_bytes()),
            Some(Alpn::JobControl)
        );
    }

    #[test]
    fn all_is_complete() {
        // `ALL` drives the accept-matrix contract test; a missing or duplicated
        // entry would leave a protocol unpinned.
        let mut seen = std::collections::BTreeSet::new();
        for alpn in Alpn::ALL {
            assert_eq!(Alpn::from_bytes(alpn.as_bytes()), Some(alpn));
            assert!(seen.insert(alpn));
        }
        assert_eq!(seen.len(), Alpn::ALL.len());
    }

    #[test]
    fn test_alpn_unknown() {
        assert_eq!(Alpn::from_bytes(b"unknown"), None);
    }

    #[test]
    fn refuses_predecessor_alpns() {
        // An old peer must fail ALPN negotiation instead of decoding new frames
        // as if they were its own.
        for predecessor in [
            b"aruna/bao/1".as_slice(),
            b"aruna/metadata/1".as_slice(),
            b"aruna/job-control/1".as_slice(),
        ] {
            assert_eq!(Alpn::from_bytes(predecessor), None);
        }
    }

    #[test]
    fn advertises_display_bytes() {
        // Accept lists use `as_bytes` and diagnostics use `Display`; a peer that
        // dials the displayed name must reach the same protocol.
        for alpn in [
            Alpn::Dht,
            Alpn::Bao,
            Alpn::Metadata,
            Alpn::NativeReference,
            Alpn::Notification,
            Alpn::Shard,
            Alpn::JobControl,
        ] {
            assert_eq!(alpn.to_string().as_bytes(), alpn.as_bytes());
        }
    }
}
