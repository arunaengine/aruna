/// Application Layer Protocol Negotiation identifiers for Aruna streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Alpn {
    /// BAO content streaming protocol
    Bao,
    /// Automerge CRDT sync protocol
    Automerge,
}

impl Alpn {
    pub const fn as_bytes(&self) -> &'static [u8] {
        match self {
            Alpn::Bao => b"aruna/bao/1",
            Alpn::Automerge => b"aruna/automerge/1",
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"aruna/bao/1" => Some(Alpn::Bao),
            b"aruna/automerge/1" => Some(Alpn::Automerge),
            _ => None,
        }
    }
}

impl std::fmt::Display for Alpn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Alpn::Bao => write!(f, "aruna/bao/1"),
            Alpn::Automerge => write!(f, "aruna/automerge/1"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpn_roundtrip() {
        assert_eq!(Alpn::from_bytes(Alpn::Bao.as_bytes()), Some(Alpn::Bao));
        assert_eq!(
            Alpn::from_bytes(Alpn::Automerge.as_bytes()),
            Some(Alpn::Automerge)
        );
    }

    #[test]
    fn test_alpn_unknown() {
        assert_eq!(Alpn::from_bytes(b"unknown"), None);
    }
}
