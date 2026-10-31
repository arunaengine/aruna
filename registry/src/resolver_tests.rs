// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::admin::{AdminState, RealmStatus};
use crate::store::tests::{realm_id, registration};
use crate::store::{Entry, Store};
use aruna_core::egress::EgressPolicy;
use aruna_core::{BucketId, PlacementHandle, StructuredId};
use axum::extract::State;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

const BASE: &str = "https://realm.example.org";
const API: &str = "https://realm.example.org/api/v1";
/// A v2 style id: random entropy, so it parses as a structured id with some placement handle.
const LEGACY: &str = "01H8Y3R7Z5X9C2V4B6N8M0Q1W3";

/// A resolver whose registry holds the test realm at `base` as its default realm, approved by
/// the registry admin when `approved` is set.
fn resolver(base: &str, approved: bool) -> (TempDir, Resolver, u64) {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(RegistryState::new(
        Store::open(dir.path()).unwrap(),
        EgressGuard::new(EgressPolicy::strict()).unwrap(),
    ));
    let now = unix_timestamp_secs();
    let entry = Entry {
        registration: registration(base, now),
        received_at: now,
        verified: false,
        problems: Vec::new(),
    };
    state.store.register(&realm_id(), &entry, now).unwrap();
    if approved {
        set_status(&state, RealmStatus::Approved, now);
    }
    (dir, Resolver::new(state, Some(realm_id()), None), now)
}

fn set_status(state: &RegistryState, status: RealmStatus, now: u64) {
    let next = AdminState {
        status,
        ..AdminState::default()
    };
    state
        .store
        .update_admin(&realm_id(), next, "test", now)
        .unwrap();
}

/// A document id as a v3 realm mints it, on the metadata placement handle.
fn minted() -> Ulid {
    let handle = PlacementHandle::new(1).unwrap();
    MetaResourceId::from_parts(1_790_000_000_000, handle, BucketId::new(0).unwrap(), 9)
        .unwrap()
        .as_ulid()
}

/// Fake sites: each URL says what the map holds, anything else is absent; asks are counted.
/// Realm nodes answer the route check with `routes`, counted apart.
struct Sites {
    said: HashMap<String, Holding>,
    asked: AtomicUsize,
    routes: Holding,
    checks: AtomicUsize,
}

impl Sites {
    fn new(said: &[(&str, Holding)]) -> Self {
        Self::serving(said, Holding::Holds)
    }

    /// Sites whose realm nodes predate the w3id route.
    fn old(said: &[(&str, Holding)]) -> Self {
        Self::serving(said, Holding::Absent)
    }

    fn serving(said: &[(&str, Holding)], routes: Holding) -> Self {
        Self {
            said: said
                .iter()
                .map(|(url, holding)| ((*url).to_string(), *holding))
                .collect(),
            asked: AtomicUsize::new(0),
            routes,
            checks: AtomicUsize::new(0),
        }
    }

    fn probe(&self, url: &Url, source: Source) -> Holding {
        if source == Source::Routes {
            assert_eq!(url.path(), format!("/api/v1/w3id/data/{ROUTE_CHECK}"));
            self.checks.fetch_add(1, Ordering::SeqCst);
            return self.routes;
        }
        self.asked.fetch_add(1, Ordering::SeqCst);
        let holding = self.said.get(url.as_str()).copied();
        holding.unwrap_or(Holding::Absent)
    }

    async fn ask(&self, resolver: &Resolver, path: &str, now: u64) -> Answer {
        self.ask_from(resolver, path, now, IpAddr::from([192, 0, 2, 1]))
            .await
    }

    async fn ask_from(&self, resolver: &Resolver, path: &str, now: u64, client: IpAddr) -> Answer {
        let probe = |url: Url, source| {
            let holding = self.probe(&url, source);
            async move { holding }
        };
        resolver.answer(parse_path(path), now, client, probe).await
    }

    async fn owned(&self, resolver: &Resolver, rest: &str, now: u64) -> Answer {
        let probe = |url: Url, source| {
            let holding = self.probe(&url, source);
            async move { holding }
        };
        resolver
            .owned(rest, now, IpAddr::from([192, 0, 2, 1]), &probe)
            .await
    }

    fn checks(&self) -> usize {
        self.checks.load(Ordering::SeqCst)
    }

    fn asked(&self) -> usize {
        self.asked.load(Ordering::SeqCst)
    }
}

fn node_id() -> aruna_core::NodeId {
    let key = ed25519_dalek::SigningKey::from_bytes(&[4; 32]).verifying_key();
    aruna_core::NodeId::from_bytes(key.as_bytes()).unwrap()
}

fn redirect(target: &str) -> Answer {
    Answer::Redirect(target.to_string())
}

#[test]
fn parses_every_pattern() {
    let id = Ulid::from_string("01JMETADATA0123456789ABCDE").unwrap();
    let realm = realm_id();
    let hash = "ab".repeat(32);
    let key = node_id();
    let arn = format!("arn:aruna:{realm}:{key}:s3/reads/a%20b%2Bc@{id}");
    let cases = [
        ("", Shape::Home),
        ("terms/Release", Shape::Home),
        ("invenio/metadata", Shape::Home),
        ("profiles/repository/zenodo", Shape::Home),
        ("01JMETADATA0123456789ABCDE", Shape::Document(id)),
        ("01jmetadata0123456789abcde", Shape::Document(id)),
        ("profile/01JMETADATA0123456789ABCDE", Shape::Profile(id)),
        ("dev/01JMETADATA0123456789ABCDE", Shape::Dev(id)),
        (&format!("data/{hash}"), Shape::Data(None, hash.clone())),
        (
            &format!("data/{realm}/{id}"),
            Shape::Data(Some(realm), format!("{realm}/{id}")),
        ),
        (
            &format!("data/{realm}/{id}/"),
            Shape::Data(Some(realm), format!("{realm}/{id}/")),
        ),
        (
            &format!("data/{arn}"),
            Shape::Data(Some(realm), arn.clone()),
        ),
        // w3id.org passes keys decoded once and leaves `+` raw; both come out canonical.
        (
            &format!("data/arn:aruna:{realm}:{key}:s3/reads/a%20b+c@{id}"),
            Shape::Data(Some(realm), arn.clone()),
        ),
    ];
    for (path, shape) in cases {
        assert_eq!(parse_path(path), shape, "{path}");
    }
    for unknown in [
        "x",
        "8ZZZZZZZZZZZZZZZZZZZZZZZZZ",
        "01JMETADATA0123456789ABCDE/x",
        "profile/x",
        "dev/",
        "data/",
        "data/not-an-id",
        &format!("data/{realm}/not-a-ulid"),
        &format!("data/arn:aruna:{realm}:{key}:s3/reads/a@not-a-ulid"),
        "../x",
        "https://evil.example",
        // The node route and the owned namespace prefix never resolve, so nothing loops.
        "w3id/01JMETADATA0123456789ABCDE",
        &format!("realm/{realm}/01JMETADATA0123456789ABCDE"),
    ] {
        assert_eq!(parse_path(unknown), Shape::Unknown, "{unknown}");
    }
}

#[test]
fn decides_by_priority() {
    use Holding::{Absent, Holds, Unavailable};
    let id = minted();
    let realm = |holding| ("realm".to_string(), Source::Realm, holding);
    let other = |holding| ("other".to_string(), Source::Realm, holding);
    let prod = |holding| (format!("{}{id}", V2.page), Source::Legacy, holding);
    let dev = |holding| (format!("{}{id}", V2_DEV.page), Source::Legacy, holding);
    let document = Shape::Document(id);
    let prod_page = redirect(&format!("{}{id}", V2.page));
    let dev_page = redirect(&format!("{}{id}", V2_DEV.page));
    let cases = [
        (vec![realm(Holds), prod(Holds)], redirect("realm")),
        (vec![realm(Holds), other(Holds)], Answer::Conflict),
        (vec![realm(Holds), other(Unavailable)], redirect("realm")),
        (
            vec![realm(Absent), prod(Holds), dev(Holds)],
            prod_page.clone(),
        ),
        (
            vec![realm(Absent), prod(Absent), dev(Holds)],
            dev_page.clone(),
        ),
        (vec![realm(Unavailable), dev(Holds)], dev_page.clone()),
        // Nobody knows the id: the page the old w3id rule named, unless a realm may hold it.
        (
            vec![realm(Absent), prod(Absent), dev(Absent)],
            prod_page.clone(),
        ),
        (vec![realm(Absent), prod(Unavailable)], prod_page.clone()),
        (vec![prod(Absent)], prod_page),
        (vec![realm(Unavailable), prod(Absent)], Answer::Unavailable),
    ];
    for (answers, expected) in cases {
        assert_eq!(decide(&document, &answers), expected, "{answers:?}");
    }
    // A dev id goes to v2 dev unless a realm holds it, also while a realm is unreachable.
    let dev_id = Shape::Dev(id);
    assert_eq!(decide(&dev_id, &[realm(Holds)]), redirect("realm"));
    assert_eq!(decide(&dev_id, &[realm(Unavailable)]), dev_page);
    assert_eq!(decide(&dev_id, &[]), dev_page);
    let profile = Shape::Profile(id);
    assert_eq!(decide(&profile, &[realm(Holds)]), redirect("realm"));
    assert!(matches!(
        decide(&profile, &[realm(Absent)]),
        Answer::NotFound(_)
    ));
    assert_eq!(decide(&profile, &[realm(Unavailable)]), Answer::Unavailable);
}

#[test]
fn maps_site_statuses() {
    let cases = [
        (Source::Realm, 302, Holding::Holds),
        (Source::Realm, 307, Holding::Holds),
        (Source::Realm, 410, Holding::Holds),
        (Source::Realm, 404, Holding::Absent),
        (Source::Realm, 200, Holding::Unavailable),
        (Source::Realm, 503, Holding::Unavailable),
        (Source::Legacy, 200, Holding::Holds),
        (Source::Legacy, 400, Holding::Holds),
        (Source::Legacy, 404, Holding::Absent),
        (Source::Legacy, 302, Holding::Unavailable),
        (Source::Legacy, 502, Holding::Unavailable),
        (Source::Routes, 307, Holding::Holds),
        (Source::Routes, 404, Holding::Absent),
        (Source::Routes, 302, Holding::Unavailable),
        (Source::Routes, 503, Holding::Unavailable),
    ];
    for (source, status, expected) in cases {
        let status = StatusCode::from_u16(status).unwrap();
        assert_eq!(holding(source, status), expected, "{source:?} {status}");
    }
}

#[tokio::test]
async fn realm_hit_cached() {
    let (_dir, resolver, now) = resolver(BASE, true);
    let id = minted();
    let target = format!("{API}/w3id/{id}");
    let sites = Sites::new(&[(&target, Holding::Holds)]);
    let path = id.to_string();
    assert_eq!(sites.ask(&resolver, &path, now).await, redirect(&target));
    // The realm and both v2 sites are asked once; the answer is then kept briefly.
    assert_eq!(sites.asked(), 3);
    assert_eq!(
        sites.ask(&resolver, &path, now + 1).await,
        redirect(&target)
    );
    assert_eq!(sites.asked(), 3);
    let later = now + CACHE_SECS;
    assert_eq!(sites.ask(&resolver, &path, later).await, redirect(&target));
    assert_eq!(sites.asked(), 6);
}

#[tokio::test]
async fn kept_realms_rechecked() {
    let (_dir, resolver, now) = resolver(BASE, true);
    let id = minted();
    let path = id.to_string();
    let sites = Sites::new(&[(&format!("{API}/w3id/{id}"), Holding::Holds)]);
    assert_eq!(
        sites.ask(&resolver, &path, now).await,
        redirect(&format!("{API}/w3id/{id}"))
    );
    // A new API URL applies to the kept hit at once, without asking anyone again.
    let moved = Entry {
        registration: registration("https://moved.example.org", now + 1),
        received_at: now,
        verified: false,
        problems: Vec::new(),
    };
    let store = &resolver.registry.store;
    store.register(&realm_id(), &moved, now).unwrap();
    let target = format!("https://moved.example.org/api/v1/w3id/{id}");
    assert_eq!(
        sites.ask(&resolver, &path, now + 1).await,
        redirect(&target)
    );
    assert_eq!(sites.asked(), 3);
    // The new URL's routes are checked again, since another node may answer there.
    assert_eq!(sites.checks(), 2);
    // A withdrawn realm is never a kept target: the id is guessed again without it.
    store.withdraw(&realm_id(), now + 2, now).unwrap();
    let page = redirect(&format!("{}{id}", V2.page));
    assert_eq!(sites.ask(&resolver, &path, now + 2).await, page);
    assert_eq!(sites.asked(), 5);
}

#[tokio::test]
async fn expired_lease_rechecked() {
    let (_dir, resolver, now) = resolver(BASE, true);
    let leased = now - crate::store::LEASE_SECS + 10;
    let entry = Entry {
        registration: registration(BASE, now + 1),
        received_at: leased,
        verified: false,
        problems: Vec::new(),
    };
    resolver
        .registry
        .store
        .register(&realm_id(), &entry, now)
        .unwrap();
    let id = minted();
    let path = id.to_string();
    let sites = Sites::new(&[(&format!("{API}/w3id/{id}"), Holding::Holds)]);
    assert_eq!(
        sites.ask(&resolver, &path, now).await,
        redirect(&format!("{API}/w3id/{id}"))
    );
    // The lease ends while the guess is still kept; the kept realm is no target any more.
    let page = redirect(&format!("{}{id}", V2.page));
    assert_eq!(sites.ask(&resolver, &path, now + 20).await, page);
    assert_eq!(sites.asked(), 5);
}

#[tokio::test]
async fn legacy_ids_guessed() {
    let dev_api = format!("{}{LEGACY}", V2_DEV.api);
    let prod_api = format!("{}{LEGACY}", V2.api);
    let dev_page = redirect(&format!("{}{LEGACY}", V2_DEV.page));
    let prod_page = redirect(&format!("{}{LEGACY}", V2.page));
    let cases = [
        (vec![(dev_api.as_str(), Holding::Holds)], dev_page),
        (
            vec![
                (dev_api.as_str(), Holding::Holds),
                (prod_api.as_str(), Holding::Holds),
            ],
            prod_page.clone(),
        ),
        // Unknown everywhere: the old rule's page, which explains itself.
        (Vec::new(), prod_page),
    ];
    for (said, expected) in cases {
        // A fresh resolver each time, so no case reads another's cached answer.
        let (_dir, resolver, now) = resolver(BASE, true);
        let sites = Sites::new(&said);
        assert_eq!(
            sites.ask(&resolver, LEGACY, now).await,
            expected,
            "{said:?}"
        );
        // The realm's w3id 404 is confirmed on its PID route before v2 is trusted.
        assert_eq!(sites.asked(), 4);
    }
}

#[tokio::test]
async fn reserved_handle_skips() {
    // A ULID on the reserved placement handle cannot be a v3 id, so no realm is asked.
    let (_dir, resolver, now) = resolver(BASE, true);
    let id = Ulid::from_parts(1_700_000_000_000, 0x1234_5678);
    assert!(MetaResourceId::from_str(&id.to_string()).is_err());
    let sites = Sites::new(&[(&format!("{API}/w3id/{id}"), Holding::Holds)]);
    let answer = sites.ask(&resolver, &id.to_string(), now).await;
    assert_eq!(answer, redirect(&format!("{}{id}", V2.page)));
    assert_eq!(sites.asked(), 2);
}

#[tokio::test]
async fn failures_never_cached() {
    let (_dir, resolver, now) = resolver(BASE, true);
    let id = minted();
    let target = format!("{API}/w3id/{id}");
    let sites = Sites::new(&[(&target, Holding::Unavailable)]);
    let path = id.to_string();
    assert_eq!(sites.ask(&resolver, &path, now).await, Answer::Unavailable);
    assert_eq!(sites.ask(&resolver, &path, now).await, Answer::Unavailable);
    assert_eq!(sites.asked(), 6);
    // A guess made while a site was unreachable is not kept either.
    let prod = format!("{}{id}", V2.api);
    let partial = Sites::new(&[(&prod, Holding::Unavailable)]);
    let page = redirect(&format!("{}{id}", V2.page));
    assert_eq!(partial.ask(&resolver, &path, now).await, page);
    assert_eq!(partial.ask(&resolver, &path, now).await, page);
    // Each guess asks the realm's w3id and PID routes and both v2 sites.
    assert_eq!(partial.asked(), 8);
}

#[tokio::test]
async fn dev_and_profiles() {
    let (_dir, resolver, now) = resolver(BASE, true);
    let id = minted();
    let held = format!("{API}/w3id/{id}");
    let sites = Sites::new(&[(&held, Holding::Holds)]);
    let dev = format!("dev/{id}");
    assert_eq!(sites.ask(&resolver, &dev, now).await, redirect(&held));
    // Only realms are asked for a dev id; v2 dev is the answer when none holds it.
    assert_eq!(sites.asked(), 1);
    let down = Sites::new(&[(&format!("{API}/w3id/{LEGACY}"), Holding::Unavailable)]);
    let legacy = format!("dev/{LEGACY}");
    let page = redirect(&format!("{}{LEGACY}", V2_DEV.page));
    assert_eq!(down.ask(&resolver, &legacy, now).await, page);

    let profile = format!("{API}/w3id/profile/{id}");
    let sites = Sites::new(&[(&profile, Holding::Holds)]);
    let path = format!("profile/{id}");
    assert_eq!(sites.ask(&resolver, &path, now).await, redirect(&profile));
    assert_eq!(sites.asked(), 1);
    let missing = format!("profile/{LEGACY}");
    assert!(matches!(
        sites.ask(&resolver, &missing, now).await,
        Answer::NotFound(_)
    ));
}

#[tokio::test]
async fn unapproved_never_asked() {
    let (_dir, resolver, now) = resolver(BASE, false);
    let id = minted();
    let sites = Sites::new(&[(&format!("{API}/w3id/{id}"), Holding::Holds)]);
    let answer = sites.ask(&resolver, &id.to_string(), now).await;
    assert_eq!(answer, redirect(&format!("{}{id}", V2.page)));
    assert_eq!(sites.asked(), 2);
    let realm = realm_id();
    let global = format!("data/{realm}/{id}");
    for path in [global, format!("data/{}", "ab".repeat(32))] {
        let answer = sites.ask(&resolver, &path, now).await;
        assert!(matches!(answer, Answer::NotFound(_)), "{path}");
    }
}

#[tokio::test]
async fn retired_realms_gone() {
    // Ids naming a retired realm explain themselves; guesses no longer ask it.
    let (_dir, resolver, now) = resolver(BASE, true);
    set_status(&resolver.registry, RealmStatus::Retired, now);
    let realm = realm_id();
    let id = minted();
    let sites = Sites::new(&[(&format!("{API}/w3id/{id}"), Holding::Holds)]);
    for path in [
        format!("data/{realm}/{id}"),
        format!("data/{}", "ab".repeat(32)),
    ] {
        let answer = sites.ask(&resolver, &path, now).await;
        assert!(matches!(answer, Answer::Gone(_)), "{path}: {answer:?}");
    }
    let answer = sites.ask(&resolver, &id.to_string(), now).await;
    assert_eq!(answer, redirect(&format!("{}{id}", V2.page)));
    assert_eq!(sites.asked(), 2);
    let response = respond(Answer::Gone("realm Realm is retired".to_string()));
    assert_eq!(response.status(), StatusCode::GONE);
}

#[tokio::test]
async fn old_nodes_use_pid() {
    // Nodes without the w3id route are asked and reached through their PID routes.
    let (_dir, resolver, now) = resolver(BASE, true);
    let id = minted();
    let pid = format!("{API}/pid/{id}");
    let sites = Sites::old(&[(&pid, Holding::Holds)]);
    assert_eq!(
        sites.ask(&resolver, &id.to_string(), now).await,
        redirect(&pid)
    );
    assert_eq!((sites.asked(), sites.checks()), (3, 1));
    // The kept hit and the other shapes use the same routes; the check itself is kept too.
    assert_eq!(
        sites.ask(&resolver, &id.to_string(), now + 1).await,
        redirect(&pid)
    );
    let profile = format!("profile/{id}");
    let target = redirect(&format!("{API}/profile/{id}"));
    let profiles = Sites::old(&[(&format!("{API}/profile/{id}"), Holding::Holds)]);
    assert_eq!(profiles.ask(&resolver, &profile, now + 1).await, target);
    let hash = "ab".repeat(32);
    let data = sites.ask(&resolver, &format!("data/{hash}"), now + 1).await;
    assert_eq!(data, redirect(&format!("{API}/resolve/{hash}")));
    assert_eq!(sites.checks() + profiles.checks(), 1);
    let later = now + ROUTES_SECS;
    sites.ask(&resolver, &format!("data/{hash}"), later).await;
    assert_eq!(sites.checks(), 2);
}

#[tokio::test]
async fn mixed_backends_fall_back() {
    // A node without the w3id route behind a checked address: its PID route still decides.
    let (_dir, resolver, now) = resolver(BASE, true);
    let id = minted();
    let pid = format!("{API}/pid/{id}");
    let sites = Sites::new(&[(&pid, Holding::Holds)]);
    assert_eq!(
        sites.ask(&resolver, &id.to_string(), now).await,
        redirect(&pid)
    );
    assert_eq!((sites.asked(), sites.checks()), (4, 1));
    // The kept hit stays on the PID route; the dropped route check runs again for others.
    assert_eq!(
        sites.ask(&resolver, &id.to_string(), now).await,
        redirect(&pid)
    );
    assert_eq!((sites.asked(), sites.checks()), (4, 1));
    sites
        .ask(&resolver, &format!("data/{}", "ab".repeat(32)), now)
        .await;
    assert_eq!(sites.checks(), 2);
}

#[tokio::test]
async fn old_nodes_never_absent() {
    // An older node answers 503 for ids it cannot place, which must not send v3 ids to v2.
    let (_dir, resolver, now) = resolver(BASE, true);
    let pid = format!("{API}/pid/{LEGACY}");
    let sites = Sites::old(&[(&pid, Holding::Unavailable)]);
    assert_eq!(sites.ask(&resolver, LEGACY, now).await, Answer::Unavailable);
    // A v2 site that knows the id still wins, as it would on newer nodes.
    let dev_api = format!("{}{LEGACY}", V2_DEV.api);
    let known = Sites::old(&[(&pid, Holding::Unavailable), (&dev_api, Holding::Holds)]);
    let page = redirect(&format!("{}{LEGACY}", V2_DEV.page));
    assert_eq!(known.ask(&resolver, LEGACY, now).await, page);
}

#[tokio::test]
async fn unclear_check_uses_pid() {
    // An unreachable route check picks the routes every node serves and waits before the next.
    let (_dir, resolver, now) = resolver(BASE, true);
    let sites = Sites::serving(&[], Holding::Unavailable);
    let hash = "ab".repeat(32);
    let path = format!("data/{hash}");
    let target = redirect(&format!("{API}/resolve/{hash}"));
    for at in [now, now + 1, now + RETRY_SECS - 1] {
        assert_eq!(sites.ask(&resolver, &path, at).await, target);
    }
    assert_eq!(sites.checks(), 1);
    assert_eq!(sites.ask(&resolver, &path, now + RETRY_SECS).await, target);
    assert_eq!(sites.checks(), 2);
}

#[tokio::test]
async fn checks_run_once() {
    // Concurrent lookups share one route check; the others use the older routes meanwhile.
    let (_dir, resolver, now) = resolver(BASE, true);
    let entry = resolver
        .registry
        .store
        .entry(&realm_id(), now)
        .unwrap()
        .unwrap();
    let release = tokio::sync::Notify::new();
    let checks = AtomicUsize::new(0);
    let probe = |_: Url, _| {
        checks.fetch_add(1, Ordering::SeqCst);
        let release = &release;
        async move {
            release.notified().await;
            Holding::Holds
        }
    };
    let (first, second, ()) = tokio::join!(
        resolver.routes(&entry, now, &probe),
        resolver.routes(&entry, now, &probe),
        async { release.notify_one() },
    );
    // Whichever call starts the check gets its answer; the other does not wait for it.
    let mut got = [first, second];
    got.sort_by_key(|routes| *routes == Routes::Legacy);
    assert_eq!(got, [Routes::W3id, Routes::Legacy]);
    assert_eq!(checks.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.routes(&entry, now, &probe).await, Routes::W3id);
    assert_eq!(checks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn direct_lookups_charged() {
    // Data ids cost no guess, but one client still cannot send unlimited lookups.
    let (_dir, resolver, now) = resolver(BASE, true);
    let sites = Sites::new(&[]);
    let busy = IpAddr::from([198, 51, 100, 1]);
    let path = format!("data/{}", "ab".repeat(32));
    for _ in 0..CLIENT_BURST {
        let answer = sites.ask_from(&resolver, &path, now, busy).await;
        assert!(matches!(answer, Answer::Redirect(_)));
    }
    assert_eq!(
        sites.ask_from(&resolver, &path, now, busy).await,
        Answer::Busy
    );
    let other = IpAddr::from([198, 51, 100, 2]);
    assert!(matches!(
        sites.ask_from(&resolver, &path, now, other).await,
        Answer::Redirect(_)
    ));
    let probe = |_: Url, _| async { Holding::Holds };
    let rest = format!("{}/{}", realm_id(), minted());
    assert_eq!(resolver.owned(&rest, now, busy, &probe).await, Answer::Busy);
}

#[tokio::test]
async fn data_ids_direct() {
    let (_dir, resolver, now) = resolver(BASE, true);
    let sites = Sites::new(&[]);
    let realm = realm_id();
    let id = minted();
    let hash = "ab".repeat(32);
    let key = node_id();
    let arn = format!("arn:aruna:{realm}:{key}:s3/reads/a%20b@{id}");
    let cases = [
        (format!("data/{hash}"), format!("{API}/w3id/data/{hash}")),
        (
            format!("data/{realm}/{id}"),
            format!("{API}/w3id/data/{realm}/{id}"),
        ),
        (
            format!("data/arn:aruna:{realm}:{key}:s3/reads/a b@{id}"),
            format!("{API}/w3id/data/{arn}"),
        ),
    ];
    for (path, target) in cases {
        assert_eq!(sites.ask(&resolver, &path, now).await, redirect(&target));
    }
    assert_eq!(sites.asked(), 0);
}

#[tokio::test]
async fn w3id_host_refused() {
    // A realm API on w3id.org would send the redirect back to this resolver.
    let (_dir, resolver, now) = resolver("https://w3id.org/aruna", true);
    let path = format!("data/{}", "ab".repeat(32));
    let answer = Sites::new(&[]).ask(&resolver, &path, now).await;
    assert!(matches!(answer, Answer::NotFound(_)), "{answer:?}");
}

#[tokio::test]
async fn clients_limited_separately() {
    // One client runs out of its share long before the shared budget, so others still resolve.
    let (_dir, resolver, now) = resolver(BASE, true);
    let sites = Sites::new(&[]);
    let fresh = |nonce| {
        let handle = PlacementHandle::new(1).unwrap();
        let bucket = BucketId::new(0).unwrap();
        let id = MetaResourceId::from_parts(1, handle, bucket, nonce).unwrap();
        id.as_ulid().to_string()
    };
    let busy = IpAddr::from([198, 51, 100, 1]);
    let ask = |nonce, at, client| {
        let path = fresh(nonce);
        let sites = &sites;
        let resolver = &resolver;
        async move { sites.ask_from(resolver, &path, at, client).await }
    };
    for nonce in 0..u64::from(CLIENT_BURST) {
        assert!(
            matches!(ask(nonce, now, busy).await, Answer::Redirect(_)),
            "{nonce}"
        );
    }
    assert_eq!(ask(100, now, busy).await, Answer::Busy);
    let other = IpAddr::from([198, 51, 100, 2]);
    assert!(matches!(ask(101, now, other).await, Answer::Redirect(_)));
    // The share refills with request time: one guess per second at 60 per minute.
    assert!(matches!(ask(102, now + 1, busy).await, Answer::Redirect(_)));
    assert_eq!(ask(103, now + 1, busy).await, Answer::Busy);
}

#[tokio::test]
async fn shared_budget_bounds() {
    // Many clients together stop at the shared burst, whatever their own shares say.
    let (_dir, resolver, now) = resolver(BASE, true);
    let sites = Sites::new(&[]);
    for nonce in 0..u64::from(GUESS_BURST) {
        let handle = PlacementHandle::new(1).unwrap();
        let id = MetaResourceId::from_parts(1, handle, BucketId::new(0).unwrap(), nonce).unwrap();
        let client = IpAddr::from([198, 51, 100, nonce as u8]);
        let answer = sites
            .ask_from(&resolver, &id.as_ulid().to_string(), now, client)
            .await;
        assert!(matches!(answer, Answer::Redirect(_)), "{nonce}");
    }
    let client = IpAddr::from([203, 0, 113, 1]);
    assert_eq!(
        sites.ask_from(&resolver, LEGACY, now, client).await,
        Answer::Busy
    );
}

#[test]
fn client_from_ingress() {
    let peer = IpAddr::from([10, 0, 0, 7]);
    let header = HeaderName::from_static("x-forwarded-for");
    let mut headers = HeaderMap::new();
    assert_eq!(client_address(&headers, Some(&header), peer), peer);
    headers.append(&header, HeaderValue::from_static("203.0.113.5"));
    // A client may send its own value first; the proxy appends the real one last.
    headers.append(&header, HeaderValue::from_static("192.0.2.4, 198.51.100.7"));
    let last = IpAddr::from([198, 51, 100, 7]);
    assert_eq!(client_address(&headers, Some(&header), peer), last);
    assert_eq!(client_address(&headers, None, peer), peer);
    let mut bad = HeaderMap::new();
    bad.insert(&header, HeaderValue::from_static("unknown"));
    assert_eq!(client_address(&bad, Some(&header), peer), peer);
}

#[tokio::test]
async fn handlers_set_headers() {
    let (_dir, resolver, _) = resolver(BASE, true);
    let resolver = Arc::new(resolver);
    let realm = realm_id();
    let id = minted();
    let shared = |path: &str| {
        let uri: Uri = path.parse().unwrap();
        let peer = ConnectInfo(SocketAddr::from(([192, 0, 2, 9], 4000)));
        resolve_shared(State(resolver.clone()), peer, HeaderMap::new(), uri)
    };
    for path in ["/aruna", "/aruna/", "/aruna/terms/Release"] {
        let response = shared(path).await;
        assert_eq!(response.status(), StatusCode::FOUND, "{path}");
        assert_eq!(response.headers()[header::LOCATION], HOME);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    }
    let unknown = shared("/aruna/x").await;
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    assert_eq!(unknown.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");

    let sites = Sites::new(&[]);
    let now = unix_timestamp_secs();
    let answer = sites
        .owned(&resolver, &format!("{realm}/profile/{id}"), now)
        .await;
    assert_eq!(answer, redirect(&format!("{API}/w3id/profile/{id}")));
    let other = aruna_core::structs::identity::realm::RealmId::from_bytes([3; 32]);
    for rest in [
        format!("{realm}/terms/Release"),
        format!("{other}/{id}"),
        format!("not-a-realm/{id}"),
        realm.to_string(),
    ] {
        let answer = sites.owned(&resolver, &rest, now).await;
        assert!(matches!(answer, Answer::NotFound(_)), "{rest}: {answer:?}");
    }
}

#[tokio::test]
async fn probes_follow_nothing() {
    use axum::routing::get;
    let followed = Arc::new(AtomicUsize::new(0));
    let counter = followed.clone();
    let status = |code: u16| move || async move { StatusCode::from_u16(code).unwrap() };
    let router = Router::new()
        .route(
            "/moved",
            get(|| async { (StatusCode::FOUND, [(header::LOCATION, "/target")]) }),
        )
        .route(
            "/target",
            get(move || async move {
                counter.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }),
        )
        .route("/gone", get(status(410)))
        .route("/down", get(status(503)))
        .route("/ok", get(status(200)))
        .route("/private", get(status(400)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let egress = EgressGuard::new(EgressPolicy::loopback()).unwrap();
    let url = |path: &str| Url::parse(&format!("{base}{path}")).unwrap();
    let cases = [
        ("/moved", Source::Realm, Holding::Holds),
        ("/gone", Source::Realm, Holding::Holds),
        ("/missing", Source::Realm, Holding::Absent),
        ("/down", Source::Realm, Holding::Unavailable),
        ("/ok", Source::Legacy, Holding::Holds),
        ("/private", Source::Legacy, Holding::Holds),
        ("/missing", Source::Legacy, Holding::Absent),
    ];
    for (path, source, expected) in cases {
        assert_eq!(probe(&egress, url(path), source).await, expected, "{path}");
    }
    assert_eq!(followed.load(Ordering::SeqCst), 0);
    // The production policy refuses loopback, which counts as unreachable.
    let strict = EgressGuard::new(EgressPolicy::strict()).unwrap();
    let refused = probe(&strict, url("/ok"), Source::Legacy).await;
    assert_eq!(refused, Holding::Unavailable);
}
