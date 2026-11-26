//! RFC 9289 RPC-over-TLS: TLS 1.3 only, `sunrpc` ALPN, a client certificate whose SPKI digest
//! names an enrolled host, and a registry that closes a host's live connections on revocation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

/// The ALPN protocol id RFC 9289 assigns to RPC-over-TLS.
pub const SUNRPC_ALPN: &[u8] = b"sunrpc";

/// The server certificate and key, and whether a loopback peer may skip TLS.
#[derive(Clone)]
pub struct NfsTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    /// Allows cleartext RPC only from a loopback address, for local tests and tools.
    pub loopback_cleartext: bool,
}

impl std::fmt::Debug for NfsTlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NfsTlsConfig")
            .field("loopback_cleartext", &self.loopback_cleartext)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TlsSetupError {
    #[error("the NFS server certificate does not parse: {0}")]
    Certificate(String),
    #[error("the NFS server key does not parse: {0}")]
    Key(String),
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
}

/// The TLS 1.3 server configuration: `sunrpc` ALPN and a mandatory client certificate.
pub fn server_config(config: &NfsTlsConfig) -> Result<Arc<ServerConfig>, TlsSetupError> {
    let certificates = CertificateDer::pem_slice_iter(&config.cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TlsSetupError::Certificate(error.to_string()))?;
    if certificates.is_empty() {
        return Err(TlsSetupError::Certificate("no certificate".to_string()));
    }
    let key = PrivateKeyDer::from_pem_slice(&config.key_pem)
        .map_err(|error| TlsSetupError::Key(error.to_string()))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = Arc::new(HostCertVerifier {
        algorithms: provider.signature_verification_algorithms,
    });
    let mut server = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificates, key)?;
    server.alpn_protocols = vec![SUNRPC_ALPN.to_vec()];
    Ok(Arc::new(server))
}

/// Accepts any well-formed client certificate and checks its handshake signature, which
/// proves possession of the key. The SPKI digest is matched to an enrolled host and the
/// peer network right after the handshake, where the async host lookup can run.
#[derive(Debug)]
pub struct HostCertVerifier {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for HostCertVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        if fingerprint(end_entity).is_none() {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::BadEncoding,
            ));
        }
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General(
            "TLS 1.2 is not accepted".to_string(),
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// One DER element: its tag, the whole encoding, its content and what follows it.
struct Element<'a> {
    tag: u8,
    whole: &'a [u8],
    content: &'a [u8],
    rest: &'a [u8],
}

/// Reads one definite-length DER element; long lengths take at most four bytes.
fn element(input: &[u8]) -> Option<Element<'_>> {
    let (&tag, after) = input.split_first()?;
    let (&first, mut after) = after.split_first()?;
    let length = if first < 0x80 {
        usize::from(first)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 || after.len() < count {
            return None;
        }
        let mut length = 0usize;
        for byte in &after[..count] {
            length = (length << 8) | usize::from(*byte);
        }
        after = &after[count..];
        length
    };
    if after.len() < length {
        return None;
    }
    let header = input.len() - after.len();
    Some(Element {
        tag,
        whole: &input[..header + length],
        content: &after[..length],
        rest: &after[length..],
    })
}

const SEQUENCE_TAG: u8 = 0x30;
const VERSION_TAG: u8 = 0xa0;
const INTEGER_TAG: u8 = 0x02;

/// The DER `SubjectPublicKeyInfo` of an X.509 certificate.
pub fn public_key_info(certificate: &[u8]) -> Option<&[u8]> {
    let outer = element(certificate).filter(|outer| outer.tag == SEQUENCE_TAG)?;
    let tbs = element(outer.content).filter(|tbs| tbs.tag == SEQUENCE_TAG)?;
    let mut field = element(tbs.content)?;
    if field.tag == VERSION_TAG {
        field = element(field.rest)?;
    }
    if field.tag != INTEGER_TAG {
        return None;
    }
    // Skips signature, issuer, validity and subject, which are all sequences.
    for _ in 0..4 {
        field = element(field.rest).filter(|next| next.tag == SEQUENCE_TAG)?;
    }
    let key = element(field.rest).filter(|key| key.tag == SEQUENCE_TAG)?;
    Some(key.whole)
}

/// The SHA-256 digest of the certificate's SPKI, which enrollment pins.
pub fn fingerprint(certificate: &[u8]) -> Option<[u8; 32]> {
    public_key_info(certificate).map(|key| Sha256::digest(key).into())
}

/// The established connections of each enrolled host, so revocation or key rotation closes
/// them at once instead of at their next request.
#[derive(Default)]
pub struct ConnectionRegistry {
    next: AtomicU64,
    hosts: Mutex<HashMap<Ulid, HashMap<u64, CancellationToken>>>,
}

impl ConnectionRegistry {
    /// Records a connection of `host_id`; its token fires when the host is closed.
    pub fn register(&self, host_id: Ulid) -> (u64, CancellationToken) {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let token = CancellationToken::new();
        self.hosts
            .lock()
            .entry(host_id)
            .or_default()
            .insert(id, token.clone());
        (id, token)
    }

    pub fn release(&self, host_id: Ulid, id: u64) {
        let mut hosts = self.hosts.lock();
        if let Some(connections) = hosts.get_mut(&host_id) {
            connections.remove(&id);
            if connections.is_empty() {
                hosts.remove(&host_id);
            }
        }
    }

    /// Closes every live connection of `host_id`.
    pub fn close_host(&self, host_id: Ulid) {
        let connections = self.hosts.lock().remove(&host_id).unwrap_or_default();
        for token in connections.into_values() {
            token.cancel();
        }
    }

    /// Closes every live connection, when a missed revocation could leave one admitted.
    pub fn close_all(&self) {
        let hosts = std::mem::take(&mut *self.hosts.lock());
        for token in hosts.into_values().flat_map(HashMap::into_values) {
            token.cancel();
        }
    }

    pub fn count(&self, host_id: Ulid) -> usize {
        self.hosts.lock().get(&host_id).map_or(0, HashMap::len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, KeyPair, PublicKeyData};

    // The extracted SPKI equals the key's own encoding, so the pin names exactly that key.
    #[test]
    fn extracts_public_key() {
        let key = KeyPair::generate().expect("key");
        let params = CertificateParams::new(vec!["client.test".to_string()]).expect("params");
        let certificate = params.self_signed(&key).expect("certificate");
        let spki = public_key_info(certificate.der()).expect("spki");
        assert_eq!(spki, key.subject_public_key_info().as_slice());
        let expected: [u8; 32] = Sha256::digest(key.subject_public_key_info()).into();
        assert_eq!(fingerprint(certificate.der()), Some(expected));
    }

    #[test]
    fn rejects_malformed_certificates() {
        assert_eq!(public_key_info(&[]), None);
        assert_eq!(public_key_info(&[0x30, 0x84, 0xff, 0xff, 0xff, 0xff]), None);
        assert_eq!(public_key_info(&[0x30, 0x80, 0x00, 0x00]), None);
        assert_eq!(public_key_info(&[0x30, 0x03, 0x02, 0x01, 0x01]), None);
    }

    #[test]
    fn closing_host_cancels() {
        let registry = ConnectionRegistry::default();
        let host = Ulid::from_parts(1, 1);
        let other = Ulid::from_parts(1, 2);
        let (_, first) = registry.register(host);
        let (second_id, second) = registry.register(host);
        let (_, kept) = registry.register(other);
        registry.release(host, second_id);
        registry.close_host(host);
        assert!(first.is_cancelled());
        assert!(
            !second.is_cancelled(),
            "a released connection is no longer tracked"
        );
        assert!(!kept.is_cancelled());
        assert_eq!(registry.count(host), 0);
        assert_eq!(registry.count(other), 1);
    }
}
