//! Checks PEM certificate pairs and serves the active pair, which only a checked pair replaces.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, TrustAnchor, UnixTime};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tracing::{error, info};

use super::{HostRoute, HostRoutes};

/// Why a certificate pair was refused. The text never carries the validation
/// time, so a rejection of the same files reads the same at every check.
#[derive(Debug, thiserror::Error)]
pub enum CertificateError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the {file} file holds an unreadable PEM block: {message}")]
    Parse { file: &'static str, message: String },
    #[error("the {file} file holds no PEM certificate")]
    NoCertificate { file: &'static str },
    #[error("the key file must hold exactly one private key, found {count}")]
    KeyCount { count: usize },
    #[error("the private key does not fit the certificate: {0}")]
    Key(String),
    #[error("the certificate chain is not valid: {0}")]
    Chain(String),
    #[error("the certificate does not cover {host}")]
    Name { host: String },
}

/// The result of one reload check.
#[derive(Debug)]
pub enum ReloadOutcome {
    /// A new checked pair is served from now on.
    Replaced,
    /// The files still hold the active pair.
    Unchanged,
    /// The files were refused and the active pair stays. `logged` is false
    /// when the same files failed for the same cause at the last check.
    Rejected {
        error: CertificateError,
        logged: bool,
    },
}

/// Checks one pair: PEM parsing, key match, a complete chain valid at `now`
/// to a Mozilla root or a CA in `ca_pem`, and every exact route host.
pub fn check_pair(
    certificate_pem: &[u8],
    key_pem: &[u8],
    ca_pem: Option<&[u8]>,
    routes: &HostRoutes,
    now: UnixTime,
) -> Result<CertifiedKey, CertificateError> {
    let chain = certificates(certificate_pem, "certificate")?;
    let key = private_key(key_pem)?;
    let ca = ca_pem
        .map(|pem| certificates(pem, "CA"))
        .transpose()?
        .unwrap_or_default();
    let provider = rustls::crypto::ring::default_provider();
    let pair = CertifiedKey::from_der(chain.clone(), key, &provider)
        .map_err(|error| CertificateError::Key(error.to_string()))?;
    let leaf = check_chain(&chain, &ca, &provider, now)?;
    for host in routes.hosts() {
        let name = ServerName::try_from(host).map_err(|_| CertificateError::Name {
            host: host.to_string(),
        })?;
        leaf.verify_is_valid_for_subject_name(&name)
            .map_err(|_| CertificateError::Name {
                host: host.to_string(),
            })?;
    }
    Ok(pair)
}

fn certificates(
    pem: &[u8],
    file: &'static str,
) -> Result<Vec<CertificateDer<'static>>, CertificateError> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| CertificateError::Parse {
            file,
            message: error.to_string(),
        })?;
    match certificates.is_empty() {
        true => Err(CertificateError::NoCertificate { file }),
        false => Ok(certificates),
    }
}

fn private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, CertificateError> {
    let keys = PrivateKeyDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| CertificateError::Parse {
            file: "key",
            message: error.to_string(),
        })?;
    let count = keys.len();
    match (keys.into_iter().next(), count) {
        (Some(key), 1) => Ok(key),
        _ => Err(CertificateError::KeyCount { count }),
    }
}

/// Verifies the leaf with the rest of the file as intermediates, so a missing,
/// expired or not yet valid link fails here instead of at the clients.
fn check_chain<'a>(
    chain: &'a [CertificateDer<'static>],
    ca: &[CertificateDer<'static>],
    provider: &CryptoProvider,
    now: UnixTime,
) -> Result<webpki::EndEntityCert<'a>, CertificateError> {
    let chain_error = |error: webpki::Error| CertificateError::Chain(stable_cause(&error));
    let mut anchors: Vec<TrustAnchor<'_>> = webpki_roots::TLS_SERVER_ROOTS.to_vec();
    for certificate in ca {
        anchors.push(
            webpki::anchor_from_trusted_cert(certificate).map_err(|error| {
                CertificateError::Parse {
                    file: "CA",
                    message: stable_cause(&error),
                }
            })?,
        );
    }
    let (leaf, intermediates) = chain.split_first().ok_or(CertificateError::NoCertificate {
        file: "certificate",
    })?;
    let leaf = webpki::EndEntityCert::try_from(leaf).map_err(chain_error)?;
    leaf.verify_for_usage(
        provider.signature_verification_algorithms.all,
        &anchors,
        intermediates,
        now,
        webpki::KeyUsage::server_auth(),
        None,
        None,
    )
    .map_err(chain_error)?;
    Ok(leaf)
}

/// The cause without the validation time that webpki puts into date errors.
fn stable_cause(error: &webpki::Error) -> String {
    match error {
        webpki::Error::CertExpired { not_after, .. } => {
            format!("a certificate expired at Unix time {}", not_after.as_secs())
        }
        webpki::Error::CertNotValidYet { not_before, .. } => format!(
            "a certificate is not valid before Unix time {}",
            not_before.as_secs()
        ),
        other => format!("{other:?}"),
    }
}

/// The three files as read at one check, kept only as a digest.
fn read_files(
    certificate: &Path,
    key: &Path,
    ca: Option<&Path>,
) -> Result<(Vec<u8>, Vec<u8>, Option<Vec<u8>>), CertificateError> {
    let read = |path: &Path| {
        std::fs::read(path).map_err(|source| CertificateError::Read {
            path: path.to_path_buf(),
            source,
        })
    };
    Ok((read(certificate)?, read(key)?, ca.map(read).transpose()?))
}

fn digest(certificate: &[u8], key: &[u8], ca: Option<&[u8]>) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    for part in [Some(certificate), Some(key), ca] {
        let part = part.unwrap_or_default();
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    hasher.finalize()
}

/// The digest of the active files and the last logged rejection.
struct ReloadState {
    active: blake3::Hash,
    logged: Option<(Option<blake3::Hash>, String)>,
}

/// Serves the active certificate pair to every routed handshake.
pub struct CertificateStore {
    certificate: PathBuf,
    key: PathBuf,
    ca: Option<PathBuf>,
    routes: Arc<HostRoutes>,
    active: RwLock<Arc<CertifiedKey>>,
    state: Mutex<ReloadState>,
}

impl std::fmt::Debug for CertificateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateStore")
            .field("certificate", &self.certificate)
            .field("key", &self.key)
            .field("ca", &self.ca)
            .finish_non_exhaustive()
    }
}

impl CertificateStore {
    /// Reads and checks the files once; any failure is returned, never a fallback.
    pub fn load(
        certificate: PathBuf,
        key: PathBuf,
        ca: Option<PathBuf>,
        routes: Arc<HostRoutes>,
        now: UnixTime,
    ) -> Result<Self, CertificateError> {
        let (certificate_pem, key_pem, ca_pem) = read_files(&certificate, &key, ca.as_deref())?;
        let pair = check_pair(&certificate_pem, &key_pem, ca_pem.as_deref(), &routes, now)?;
        if !covers_buckets(&pair, &routes) {
            info!(
                certificate = %certificate.display(),
                "The certificate has no wildcard for S3 bucket hosts; virtual-hosted buckets need path style"
            );
        }
        Ok(Self {
            active: RwLock::new(Arc::new(pair)),
            state: Mutex::new(ReloadState {
                active: digest(&certificate_pem, &key_pem, ca_pem.as_deref()),
                logged: None,
            }),
            certificate,
            key,
            ca,
            routes,
        })
    }

    pub fn routes(&self) -> &Arc<HostRoutes> {
        &self.routes
    }

    /// Checks the files again. Refused files are checked at every call, since a
    /// renewed pair may become valid later; only the error log is deduplicated.
    pub fn reload(&self, now: UnixTime) -> ReloadOutcome {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let checked = match read_files(&self.certificate, &self.key, self.ca.as_deref()) {
            Err(error) => Err((None, error)),
            Ok((certificate, key, ca)) => {
                let files = digest(&certificate, &key, ca.as_deref());
                if files == state.active {
                    return ReloadOutcome::Unchanged;
                }
                check_pair(&certificate, &key, ca.as_deref(), &self.routes, now)
                    .map(|pair| (files, pair))
                    .map_err(|error| (Some(files), error))
            }
        };
        match checked {
            Ok((files, pair)) => {
                *self.active.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(pair);
                state.active = files;
                state.logged = None;
                info!(certificate = %self.certificate.display(), "Loaded the new certificate pair");
                ReloadOutcome::Replaced
            }
            Err((files, error)) => {
                let cause = (files, error.to_string());
                let logged = state.logged.as_ref() != Some(&cause);
                if logged {
                    error!(
                        certificate = %self.certificate.display(),
                        key = %self.key.display(),
                        error = %error,
                        "Refused the new certificate pair; the active pair stays"
                    );
                    state.logged = Some(cause);
                }
                ReloadOutcome::Rejected { error, logged }
            }
        }
    }

    /// The pair served right now.
    pub fn active(&self) -> Arc<CertifiedKey> {
        self.active
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ResolvesServerCert for CertificateStore {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.active())
    }
}

/// Whether the leaf covers a one-label subdomain of the S3 host, as a wildcard does.
fn covers_buckets(pair: &CertifiedKey, routes: &HostRoutes) -> bool {
    let Some(s3) = routes.host(HostRoute::S3) else {
        return true;
    };
    let probe = format!("bucket.{s3}");
    let Ok(name) = ServerName::try_from(probe.as_str()) else {
        return false;
    };
    pair.end_entity_cert()
        .ok()
        .and_then(|leaf| webpki::EndEntityCert::try_from(leaf).ok())
        .is_some_and(|leaf| leaf.verify_is_valid_for_subject_name(&name).is_ok())
}
