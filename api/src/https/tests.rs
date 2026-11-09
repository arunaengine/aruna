//! Tests native HTTPS: certificate checks and reload with generated test certificates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    date_time_ymd,
};
use rustls::pki_types::UnixTime;

use super::HostRoutes;
use super::certificate::{CertificateError, CertificateStore, ReloadOutcome, check_pair};

/// 2026-01-01T00:00:00Z, inside every default validity window below.
const NOW: u64 = 1_767_225_600;
const DAY: u64 = 86_400;

pub(super) fn at(seconds: u64) -> UnixTime {
    UnixTime::since_unix_epoch(Duration::from_secs(seconds))
}

pub(super) fn routes() -> HostRoutes {
    let url = |value: &str| url::Url::parse(value).expect("test origin");
    HostRoutes::new(
        &url("https://api.test"),
        Some(&url("https://s3.test")),
        Some(&url("https://portal.test")),
    )
}

/// A test certificate authority that signs intermediates and leaves.
pub(super) struct Authority {
    pub(super) pem: String,
    issuer: Issuer<'static, KeyPair>,
}

/// A validity window in years, from the first January of `from` to that of `until`.
#[derive(Clone, Copy)]
pub(super) struct Window(pub(super) i32, pub(super) i32);

const VALID: Window = Window(2020, 2040);

fn ca_params(window: Window) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.not_before = date_time_ymd(window.0, 1, 1);
    params.not_after = date_time_ymd(window.1, 1, 1);
    params
}

impl Authority {
    pub(super) fn root() -> Self {
        let key = KeyPair::generate().expect("CA key");
        let params = ca_params(VALID);
        let pem = params.self_signed(&key).expect("CA certificate").pem();
        Self {
            pem,
            issuer: Issuer::new(params, key),
        }
    }

    fn intermediate(&self, window: Window) -> Self {
        let key = KeyPair::generate().expect("intermediate key");
        let params = ca_params(window);
        let pem = params
            .signed_by(&key, &self.issuer)
            .expect("intermediate certificate")
            .pem();
        Self {
            pem,
            issuer: Issuer::new(params, key),
        }
    }

    pub(super) fn leaf(&self, names: &[&str], window: Window, key: &KeyPair) -> String {
        leaf_params(names, window)
            .signed_by(key, &self.issuer)
            .expect("leaf certificate")
            .pem()
    }
}

fn leaf_params(names: &[&str], window: Window) -> CertificateParams {
    let names: Vec<String> = names.iter().map(ToString::to_string).collect();
    let mut params = CertificateParams::new(names).expect("leaf params");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.not_before = date_time_ymd(window.0, 1, 1);
    params.not_after = date_time_ymd(window.1, 1, 1);
    params
}

pub(super) const NAMES: [&str; 4] = ["api.test", "s3.test", "*.s3.test", "portal.test"];

/// A leaf for every route host with its key, signed by `authority`.
pub(super) fn pair(authority: &Authority) -> (String, String) {
    let key = KeyPair::generate().expect("leaf key");
    (authority.leaf(&NAMES, VALID, &key), key.serialize_pem())
}

fn check(certificate: &str, key: &str, ca: &str) -> Result<(), CertificateError> {
    check_pair(
        certificate.as_bytes(),
        key.as_bytes(),
        Some(ca.as_bytes()),
        &routes(),
        at(NOW),
    )
    .map(|_| ())
}

/// The three PEM files of a store in a unique folder.
pub(super) struct Files {
    _dir: tempfile::TempDir,
    pub(super) certificate: PathBuf,
    pub(super) key: PathBuf,
    pub(super) ca: PathBuf,
}

impl Files {
    pub(super) fn new(certificate: &str, key: &str, ca: &str) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let files = Self {
            certificate: dir.path().join("fullchain.pem"),
            key: dir.path().join("privkey.pem"),
            ca: dir.path().join("ca.pem"),
            _dir: dir,
        };
        files.write(&files.certificate, certificate);
        files.write(&files.key, key);
        files.write(&files.ca, ca);
        files
    }

    /// Replaces a file atomically: a temporary name, then a rename.
    pub(super) fn write(&self, path: &Path, content: &str) {
        let temporary = path.with_extension("new");
        std::fs::write(&temporary, content).expect("write PEM");
        std::fs::rename(&temporary, path).expect("rename PEM");
    }

    pub(super) fn store(&self, now: UnixTime) -> Result<CertificateStore, CertificateError> {
        CertificateStore::load(
            self.certificate.clone(),
            self.key.clone(),
            Some(self.ca.clone()),
            Arc::new(routes()),
            now,
        )
    }
}

fn leaf_der(store: &CertificateStore) -> Vec<u8> {
    store
        .active()
        .end_entity_cert()
        .expect("active leaf")
        .to_vec()
}

#[test]
fn good_pair_loads() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    check(&certificate, &key, &root.pem).expect("a complete pair is accepted");
    let files = Files::new(&certificate, &key, &root.pem);
    assert!(files.store(at(NOW)).is_ok());
}

#[test]
fn refuses_bad_pem() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    assert!(matches!(
        check("", &key, &root.pem),
        Err(CertificateError::NoCertificate {
            file: "certificate"
        })
    ));
    assert!(matches!(
        check(&certificate, "", &root.pem),
        Err(CertificateError::KeyCount { count: 0 })
    ));
    let broken = "-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n";
    assert!(matches!(
        check(broken, &key, &root.pem),
        Err(CertificateError::Parse { .. })
    ));
    let two_keys = format!("{key}{key}");
    assert!(matches!(
        check(&certificate, &two_keys, &root.pem),
        Err(CertificateError::KeyCount { count: 2 })
    ));
}

#[test]
fn refuses_wrong_key() {
    let root = Authority::root();
    let (certificate, _) = pair(&root);
    let other = KeyPair::generate().expect("other key").serialize_pem();
    assert!(matches!(
        check(&certificate, &other, &root.pem),
        Err(CertificateError::Key(_))
    ));
}

#[test]
fn refuses_uncovered_host() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let certificate = root.leaf(&["api.test", "s3.test"], VALID, &key);
    match check(&certificate, &key.serialize_pem(), &root.pem) {
        Err(CertificateError::Name { host }) => assert_eq!(host, "portal.test"),
        other => panic!("expected a name error, got {other:?}"),
    }
}

#[test]
fn refuses_dated_certificates() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let pem = key.serialize_pem();
    let expired = root.leaf(&NAMES, Window(2020, 2021), &key);
    let future = root.leaf(&NAMES, Window(2030, 2031), &key);
    for certificate in [expired, future] {
        assert!(matches!(
            check(&certificate, &pem, &root.pem),
            Err(CertificateError::Chain(_))
        ));
    }
    let old = root.intermediate(Window(2020, 2021));
    let leaf = old.leaf(&NAMES, VALID, &key);
    let chain = format!("{leaf}{}", old.pem);
    assert!(matches!(
        check(&chain, &pem, &root.pem),
        Err(CertificateError::Chain(_))
    ));
}

#[test]
fn refuses_incomplete_chain() {
    let root = Authority::root();
    let middle = root.intermediate(VALID);
    let key = KeyPair::generate().expect("leaf key");
    let pem = key.serialize_pem();
    let leaf = middle.leaf(&NAMES, VALID, &key);
    assert!(matches!(
        check(&leaf, &pem, &root.pem),
        Err(CertificateError::Chain(_))
    ));
    let chain = format!("{leaf}{}", middle.pem);
    check(&chain, &pem, &root.pem).expect("leaf and intermediate complete the chain");
}

#[test]
fn self_signed_listed() {
    let key = KeyPair::generate().expect("key");
    let certificate = leaf_params(&NAMES, VALID)
        .self_signed(&key)
        .expect("self-signed")
        .pem();
    let pem = key.serialize_pem();
    let unrelated = Authority::root();
    assert!(matches!(
        check(&certificate, &pem, &unrelated.pem),
        Err(CertificateError::Chain(_))
    ));
    check(&certificate, &pem, &certificate).expect("a listed self-signed certificate passes");
}

#[test]
fn reload_same_key() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let first = root.leaf(&NAMES, VALID, &key);
    let files = Files::new(&first, &key.serialize_pem(), &root.pem);
    let store = files.store(at(NOW)).expect("store loads");
    let before = leaf_der(&store);

    files.write(&files.certificate, &root.leaf(&NAMES, VALID, &key));
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Replaced));
    assert_ne!(leaf_der(&store), before, "the renewed leaf is served");
}

#[test]
fn reload_new_key() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    let files = Files::new(&certificate, &key, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");
    let before = leaf_der(&store);

    let (certificate, key) = pair(&root);
    files.write(&files.key, &key);
    files.write(&files.certificate, &certificate);
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Replaced));
    assert_ne!(leaf_der(&store), before);
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Unchanged));
}

#[test]
fn reload_keeps_active() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let pem = key.serialize_pem();
    let files = Files::new(&root.leaf(&NAMES, VALID, &key), &pem, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");
    let before = leaf_der(&store);

    let middle = root.intermediate(VALID);
    let refused = [
        root.leaf(&NAMES, Window(2020, 2021), &key),
        root.leaf(&NAMES, Window(2030, 2031), &key),
        middle.leaf(&NAMES, VALID, &key),
    ];
    for certificate in refused {
        files.write(&files.certificate, &certificate);
        assert!(matches!(
            store.reload(at(NOW)),
            ReloadOutcome::Rejected {
                error: CertificateError::Chain(_),
                logged: true
            }
        ));
        assert_eq!(leaf_der(&store), before, "a refused pair never replaces");
    }
}

#[test]
fn waits_for_validity() {
    // A renewal installed before its start date: two refusals at different
    // times log once, and the same bytes load once the window has started.
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let pem = key.serialize_pem();
    let files = Files::new(&root.leaf(&NAMES, VALID, &key), &pem, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");

    files.write(
        &files.certificate,
        &root.leaf(&NAMES, Window(2027, 2040), &key),
    );
    match store.reload(at(NOW)) {
        ReloadOutcome::Rejected {
            error: CertificateError::Chain(cause),
            logged: true,
        } => assert!(cause.contains("not valid before"), "{cause}"),
        other => panic!("expected a logged refusal, got {other:?}"),
    }
    let second = store.reload(at(NOW + 30 * DAY));
    assert!(
        matches!(second, ReloadOutcome::Rejected { logged: false, .. }),
        "the same files and cause must not log again: {second:?}"
    );
    assert!(matches!(
        store.reload(at(NOW + 400 * DAY)),
        ReloadOutcome::Replaced
    ));
}

#[test]
fn ca_change_applies() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    let files = Files::new(&certificate, &key, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");

    let private = Authority::root();
    let (certificate, key) = pair(&private);
    files.write(&files.key, &key);
    files.write(&files.certificate, &certificate);
    assert!(matches!(
        store.reload(at(NOW)),
        ReloadOutcome::Rejected { .. }
    ));
    files.write(&files.ca, &private.pem);
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Replaced));
}

#[test]
fn rename_order_recovers() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    let files = Files::new(&certificate, &key, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");

    let (certificate, key) = pair(&root);
    files.write(&files.certificate, &certificate);
    assert!(matches!(
        store.reload(at(NOW)),
        ReloadOutcome::Rejected {
            error: CertificateError::Key(_),
            ..
        }
    ));
    files.write(&files.key, &key);
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Replaced));
}

#[test]
fn startup_refuses_expired() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let expired = root.leaf(&NAMES, Window(2020, 2021), &key);
    let files = Files::new(&expired, &key.serialize_pem(), &root.pem);
    assert!(matches!(
        files.store(at(NOW)),
        Err(CertificateError::Chain(_))
    ));
    let missing = CertificateStore::load(
        files.certificate.with_extension("missing"),
        files.key.clone(),
        None,
        Arc::new(routes()),
        at(NOW),
    );
    assert!(matches!(missing, Err(CertificateError::Read { .. })));
}
