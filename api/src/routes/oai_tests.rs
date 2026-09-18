//! Tests OAI-PMH paging, resumption tokens and how denied or private records are hidden.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::{
    MetadataAuditOperation, MetadataAuditRecord,
};
use aruna_core::types::{Key, Value};
use aruna_operations::metadata::repository::create_outbox_entries;
use aruna_operations::metadata::visibility_index::{CANDIDATE_BUDGET, rebuild_index};
use std::collections::{HashMap, HashSet};

struct Fixture {
    state: Arc<ServerState>,
    ctx: Arc<DriverContext>,
    realm_id: RealmId,
    group_id: Ulid,
    _dir: tempfile::TempDir,
}

fn actor(realm_id: RealmId) -> Actor {
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
        user_id: aruna_core::UserId::local(Ulid::from_bytes([4; 16]), realm_id),
        realm_id,
    }
}

async fn fixture(limits: RoCrateLimits) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let storage_handle =
        aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let realm_id = RealmId::from_bytes(
        *ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .as_bytes(),
    );
    let node_id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
    let ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let state = ServerState::new(
        Arc::clone(&ctx),
        realm_id,
        node_id,
        aruna_core::structs::identity::auth::NodeCapabilities::user_node(realm_id).unwrap(),
        false,
        None,
        aruna_operations::jobs::runtime::JobsRuntime::new(),
    )
    .await
    .with_rocrate_limits(limits);
    let group_id = Ulid::from_bytes([9; 16]);
    let fixture = Fixture {
        state: Arc::new(state),
        ctx,
        realm_id,
        group_id,
        _dir: dir,
    };
    seed_scopes(&fixture).await;
    fixture
}

async fn store(ctx: &DriverContext, writes: Vec<(String, Key, Value)>) {
    let event = ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

async fn seed_scopes(fixture: &Fixture) {
    let realm_id = fixture.realm_id;
    let config = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    let target = aruna_core::document::DocumentTarget::RealmConfig { realm_id };
    store(
        &fixture.ctx,
        vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            config.to_bytes(&actor(realm_id)).unwrap().into(),
        )],
    )
    .await;
    seed_group(fixture, fixture.group_id, Vec::new()).await;
}

async fn seed_group(fixture: &Fixture, group_id: Ulid, policies: Vec<RequestPolicy>) {
    let realm_id = fixture.realm_id;
    let group = Group {
        display_name: "g".to_string(),
        group_id,
        realm_id,
        roles: HashSet::new(),
        owner: actor(realm_id).user_id,
    };
    let auth = GroupAuthorizationDocument {
        group_id,
        roles: HashMap::new(),
        policies,
    };
    store(
        &fixture.ctx,
        vec![
            (
                GROUP_KEYSPACE.to_string(),
                byteview::ByteView::from(group_id.to_bytes().to_vec()),
                group.to_bytes(&actor(realm_id)).unwrap().into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                byteview::ByteView::from(group_id.to_bytes().to_vec()),
                postcard::to_allocvec(&auth).unwrap().into(),
            ),
        ],
    )
    .await;
}

fn deny_read() -> RequestPolicy {
    RequestPolicy {
        policy_id: Ulid::from_bytes([6u8; 16]),
        name: "oai".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: "operation == 'metadata.read'".to_string(),
        enabled: true,
    }
}

fn registry_record(fixture: &Fixture, index: u64, public: bool) -> MetadataRegistryRecord {
    record_in(fixture, fixture.group_id, index, public)
}

fn record_in(
    fixture: &Fixture,
    group_id: Ulid,
    index: u64,
    public: bool,
) -> MetadataRegistryRecord {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&index.to_be_bytes());
    let document_id = Ulid::from_bytes(bytes);
    let path = format!("doc/{index}");
    MetadataRegistryRecord {
        realm_id: fixture.realm_id,
        group_id,
        document_id,
        document_path: path.clone(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &fixture.realm_id,
            group_id,
            &path,
            document_id,
        ),
        placement: PlacementRef {
            strategy_id: Ulid::nil(),
            shard: 0,
        },
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1_000 + index,
        establishing_event_id: Ulid::from_bytes([8; 16]),
        last_event_id: Ulid::from_bytes([9; 16]),
    }
}

async fn store_record(fixture: &Fixture, record: &MetadataRegistryRecord) {
    let audit = MetadataAuditRecord {
        realm_id: record.realm_id,
        group_id: record.group_id,
        document_id: record.document_id,
        graph_iri: record.graph_iri.clone(),
        user_id: Default::default(),
        node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
        operation: MetadataAuditOperation::Create,
        occurred_at_ms: record.updated_at_ms,
        details: None,
    };
    let writes = create_outbox_entries(record, &audit, Ulid::generate(), None).unwrap();
    store(&fixture.ctx, writes).await;
}

async fn seed_records(fixture: &Fixture, count: u64, public: bool) {
    for index in 0..count {
        store_record(fixture, &registry_record(fixture, index, public)).await;
    }
    rebuild_index(&fixture.ctx).await.unwrap();
}

fn list_params(token: Option<&str>) -> OaiParams {
    OaiParams {
        verb: Some("ListIdentifiers".to_string()),
        metadata_prefix: token.is_none().then(|| METADATA_PREFIX.to_string()),
        resumption_token: token.map(|token| token.to_string()),
        ..OaiParams::default()
    }
}

fn token_from(body: &str) -> Option<String> {
    let start = body.find("<resumptionToken>")? + "<resumptionToken>".len();
    let end = body[start..].find("</resumptionToken>")? + start;
    Some(body[start..end].to_string())
}

async fn list_page(fixture: &Fixture, token: Option<&str>) -> Result<String, OaiFault> {
    list(
        &fixture.state,
        &fixture.ctx,
        fixture.realm_id,
        &list_params(token),
        false,
    )
    .await
}

// a full page emits no resumption token
#[tokio::test]
async fn full_page_ends() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, PAGE_SIZE as u64, true).await;
    let body = list_page(&fixture, None).await.unwrap();
    assert_eq!(body.matches("<header>").count(), PAGE_SIZE);
    assert!(token_from(&body).is_none());
    assert!(!body.contains("<resumptionToken"));
}

// a short page emits no resumption token
#[tokio::test]
async fn short_page_ends() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, PAGE_SIZE as u64 - 1, true).await;
    let body = list_page(&fixture, None).await.unwrap();
    assert_eq!(body.matches("<header>").count(), PAGE_SIZE - 1);
    assert!(!body.contains("<resumptionToken"));
}

// 101 visible records: the lookahead exists, so a token is issued, and the
// token-driven final page carries the empty terminal element.
#[tokio::test]
async fn overflow_carries_token() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, PAGE_SIZE as u64 + 1, true).await;
    let first = list_page(&fixture, None).await.unwrap();
    assert_eq!(first.matches("<header>").count(), PAGE_SIZE);
    let token = token_from(&first).expect("a token continues the list");

    let second = list_page(&fixture, Some(&token)).await.unwrap();
    assert_eq!(second.matches("<header>").count(), 1);
    assert!(second.contains("<resumptionToken />"));
}

// Leading candidates denied after publication must continue the enumeration
// through a token; reporting noRecordsMatch would drop the later group.
#[tokio::test]
async fn denied_prefix_continues() {
    let fixture = fixture(RoCrateLimits::default()).await;
    let denied = Ulid::from_bytes([31; 16]);
    seed_group(&fixture, denied, Vec::new()).await;
    let tail = CANDIDATE_BUDGET as u64 + 8;
    for index in 0..tail {
        store_record(&fixture, &record_in(&fixture, denied, index, true)).await;
    }
    for index in tail..tail + 3 {
        store_record(&fixture, &registry_record(&fixture, index, true)).await;
    }
    rebuild_index(&fixture.ctx).await.unwrap();
    seed_group(&fixture, denied, vec![deny_read()]).await;

    let first = list_page(&fixture, None).await.unwrap();
    assert_eq!(first.matches("<header>").count(), 0);
    let token = token_from(&first).expect("a denied batch continues the list");

    let second = list_page(&fixture, Some(&token)).await.unwrap();
    assert_eq!(second.matches("<header>").count(), 3);
    assert!(second.contains("<resumptionToken />"));
}

// The oldest visible datestamp must survive a denied prefix longer than one
// candidate budget.
#[tokio::test]
async fn earliest_skips_denied() {
    let fixture = fixture(RoCrateLimits::default()).await;
    let denied = Ulid::from_bytes([32; 16]);
    seed_group(&fixture, denied, Vec::new()).await;
    let tail = CANDIDATE_BUDGET as u64 + 8;
    for index in 0..tail {
        store_record(&fixture, &record_in(&fixture, denied, index, true)).await;
    }
    store_record(&fixture, &registry_record(&fixture, tail, true)).await;
    rebuild_index(&fixture.ctx).await.unwrap();
    seed_group(&fixture, denied, vec![deny_read()]).await;

    let body = identify(&fixture.ctx, "https://example.test/api/v1/oai")
        .await
        .unwrap();
    let earliest = format_from(1_000 + tail).unwrap();
    assert!(body.contains(&format!(
        "<earliestDatestamp>{earliest}</earliestDatestamp>"
    )));
}

// A continuation whose remaining records disappeared must close the sequence
// with the empty terminal token, never with a protocol error.
#[tokio::test]
async fn empty_continuation_terminates() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, PAGE_SIZE as u64 + 1, true).await;
    let first = list_page(&fixture, None).await.unwrap();
    let token = token_from(&first).expect("a token continues the list");

    // The only record left in the window turns private mid-sequence.
    store_record(
        &fixture,
        &registry_record(&fixture, PAGE_SIZE as u64, false),
    )
    .await;
    rebuild_index(&fixture.ctx).await.unwrap();

    let second = list_page(&fixture, Some(&token)).await.unwrap();
    assert_eq!(
        second,
        "<ListIdentifiers><resumptionToken /></ListIdentifiers>"
    );
}

// A sequence started before later writes must terminate on the window frozen
// at its first page, even across a generation flip.
#[tokio::test]
async fn frozen_window_ends() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, PAGE_SIZE as u64 + 1, true).await;
    let first = list_page(&fixture, None).await.unwrap();
    assert_eq!(first.matches("<header>").count(), PAGE_SIZE);
    let token = token_from(&first).expect("a token continues the list");

    let later = decode_token(&token).unwrap().until_ms + 1;
    let mut created = registry_record(&fixture, 500, true);
    created.updated_at_ms = later;
    store_record(&fixture, &created).await;
    let mut moved = registry_record(&fixture, 0, true);
    moved.updated_at_ms = later + 1;
    store_record(&fixture, &moved).await;
    rebuild_index(&fixture.ctx).await.unwrap();

    let second = list_page(&fixture, Some(&token)).await.unwrap();
    assert_eq!(second.matches("<header>").count(), 1);
    assert!(second.contains("<resumptionToken />"));
    assert!(!second.contains(&created.graph_iri));
    assert!(!second.contains(&moved.graph_iri));

    // A fresh request over a window that covers them does see both.
    let params = OaiParams {
        verb: Some("ListIdentifiers".to_string()),
        metadata_prefix: Some(METADATA_PREFIX.to_string()),
        from: Some(format_from(later).unwrap()),
        until: Some(format_from(later + 60_000).unwrap()),
        ..OaiParams::default()
    };
    let fresh = list(
        &fixture.state,
        &fixture.ctx,
        fixture.realm_id,
        &params,
        false,
    )
    .await
    .unwrap();
    assert!(fresh.contains(&created.graph_iri));
    assert!(fresh.contains(&moved.graph_iri));
}

#[tokio::test]
async fn private_never_lists() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, 5, false).await;
    assert!(matches!(
        list_page(&fixture, None).await,
        Err(OaiFault::Protocol {
            code: "noRecordsMatch",
            ..
        })
    ));
}

#[tokio::test]
async fn unbuilt_index_unavailable() {
    let fixture = fixture(RoCrateLimits::default()).await;
    assert!(matches!(
        list_page(&fixture, None).await,
        Err(OaiFault::Unavailable)
    ));
}

// The page stops before the record that would breach the aggregate budget and
// hands the caller a token at the last emitted cursor.
#[tokio::test]
async fn budget_stops_early() {
    let limits = RoCrateLimits {
        metadata_bytes: 100,
        ..RoCrateLimits::default()
    };
    let fixture = fixture(limits).await;
    seed_records(&fixture, 20, true).await;
    let body = list_page(&fixture, None).await.unwrap();
    let emitted = body.matches("<header>").count();
    assert!(emitted > 0 && emitted < 20);
    assert!(body.len() <= 100 * XML_ESCAPE_FACTOR as usize + "</ListIdentifiers>".len() + 200);
    assert!(token_from(&body).is_some());
}

#[tokio::test]
async fn oversized_record_unavailable() {
    let limits = RoCrateLimits {
        metadata_bytes: 1,
        ..RoCrateLimits::default()
    };
    let fixture = fixture(limits).await;
    seed_records(&fixture, 3, true).await;
    assert!(matches!(
        list_page(&fixture, None).await,
        Err(OaiFault::Unavailable)
    ));
}

// A record whose graph cannot be exported must fail the request, never fall
// back to a synthesized title-only crosswalk.
#[tokio::test]
async fn export_failure_faults() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, 1, true).await;
    let record = registry_record(&fixture, 0, true);
    let rendered = render_record(&fixture.state, &fixture.ctx, fixture.realm_id, &record).await;
    assert!(matches!(rendered, Err(OaiFault::Unavailable)));
}

#[tokio::test]
async fn record_guards_realm() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, 1, true).await;
    let record = registry_record(&fixture, 0, true);
    let params = OaiParams {
        verb: Some("GetRecord".to_string()),
        metadata_prefix: Some(METADATA_PREFIX.to_string()),
        identifier: Some(record.graph_iri.clone()),
        ..OaiParams::default()
    };
    let other = RealmId::from_bytes([2; 32]);
    assert!(matches!(
        get_record(&fixture.state, &fixture.ctx, other, &params).await,
        Err(OaiFault::Protocol {
            code: "idDoesNotExist",
            ..
        })
    ));
}

#[tokio::test]
async fn record_hides_private() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, 1, false).await;
    let record = registry_record(&fixture, 0, false);
    let params = OaiParams {
        verb: Some("GetRecord".to_string()),
        metadata_prefix: Some(METADATA_PREFIX.to_string()),
        identifier: Some(record.graph_iri.clone()),
        ..OaiParams::default()
    };
    assert!(matches!(
        get_record(&fixture.state, &fixture.ctx, fixture.realm_id, &params).await,
        Err(OaiFault::Protocol {
            code: "idDoesNotExist",
            ..
        })
    ));
}

// Identify reports the earliest visible datestamp
#[tokio::test]
async fn identify_uses_earliest() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, 3, true).await;
    let body = identify(&fixture.ctx, "https://example.test/api/v1/oai")
        .await
        .unwrap();
    assert!(body.contains("<baseURL>https://example.test/api/v1/oai</baseURL>"));
    let earliest = format_from(1_000).unwrap();
    assert!(body.contains(&format!(
        "<earliestDatestamp>{earliest}</earliestDatestamp>"
    )));
}

async fn call(fixture: &Fixture, request: axum::http::Request<Body>) -> (StatusCode, String) {
    use tower::ServiceExt;
    let (app, _) = router().split_for_parts();
    let response = app
        .with_state(Arc::clone(&fixture.state))
        .oneshot(request)
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn peer_request(builder: axum::http::request::Builder, body: Body) -> axum::http::Request<Body> {
    let mut request = builder.body(body).unwrap();
    request.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:9000".parse::<std::net::SocketAddr>().unwrap(),
    ));
    request
}

/// Structural stand-in for XSD validation: declaration, root namespaces,
/// element order, and a payload that is either the verb or an error.
fn assert_envelope(body: &str, payload: &str) {
    assert!(body.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"));
    assert!(body.contains(OAI_PMH_OPEN));
    assert!(body.ends_with("</OAI-PMH>"));
    let date = body.find("<responseDate>").expect("responseDate");
    let request = body.find("<request").expect("request");
    assert!(date < request);
    let payload_at = body
        .find(&format!("<{payload}"))
        .unwrap_or_else(|| panic!("missing {payload} in {body}"));
    assert!(request < payload_at);
}

// every verb answers inside a well-shaped envelope
#[tokio::test]
async fn verb_envelopes_shaped() {
    let fixture = fixture(RoCrateLimits::default()).await;
    seed_records(&fixture, 2, true).await;
    let record = registry_record(&fixture, 0, true);
    let cases: [(&str, &str); 7] = [
        ("verb=Identify", "Identify"),
        ("verb=ListMetadataFormats", "ListMetadataFormats"),
        ("verb=ListSets", "error"),
        (
            "verb=ListIdentifiers&metadataPrefix=oai_dc",
            "ListIdentifiers",
        ),
        ("verb=ListRecords&metadataPrefix=marc", "error"),
        (
            "verb=GetRecord&metadataPrefix=oai_dc&identifier=nope",
            "error",
        ),
        ("verb=Bogus", "error"),
    ];
    for (query, payload) in cases {
        let (status, body) = call(
            &fixture,
            peer_request(
                axum::http::Request::builder()
                    .method("GET")
                    .uri(format!("/oai?{query}")),
                Body::empty(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{query}");
        assert_envelope(&body, payload);
    }
    let (_, body) = call(
        &fixture,
        peer_request(
            axum::http::Request::builder().method("GET").uri(format!(
                "/oai?verb=GetRecord&metadataPrefix=oai_dc&identifier={}",
                record.graph_iri
            )),
            Body::empty(),
        ),
    )
    .await;
    // The graph is unreadable in this harness, so the routed export fails
    // closed instead of rendering a fallback record.
    assert!(body.is_empty() || body.contains("service error"));
}

#[tokio::test]
async fn get_post_agree() {
    let fixture = fixture(RoCrateLimits::default()).await;
    let (get_status, get_body) = call(
        &fixture,
        peer_request(
            axum::http::Request::builder()
                .method("GET")
                .uri("/oai?verb=ListMetadataFormats"),
            Body::empty(),
        ),
    )
    .await;
    let (post_status, post_body) = call(
        &fixture,
        peer_request(
            axum::http::Request::builder()
                .method("POST")
                .uri("/oai")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded"),
            Body::from("verb=ListMetadataFormats"),
        ),
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(post_status, StatusCode::OK);
    assert!(get_body.contains("<ListMetadataFormats>"));
    // Only the responseDate differs between the two transports.
    let strip = |body: &str| {
        let start = body.find("<responseDate>").unwrap();
        let end = body.find("</responseDate>").unwrap();
        format!("{}{}", &body[..start], &body[end..])
    };
    assert_eq!(strip(&get_body), strip(&post_body));
}

// POST rejects media types other than form-urlencoded
#[tokio::test]
async fn post_rejects_media() {
    let fixture = fixture(RoCrateLimits::default()).await;
    let (status, body) = call(
        &fixture,
        peer_request(
            axum::http::Request::builder()
                .method("POST")
                .uri("/oai")
                .header(header::CONTENT_TYPE, "application/json"),
            Body::from("{}"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("code=\"badArgument\""));
}

// A repeated argument must surface as a protocol error inside the envelope,
// not as a bare transport 400.
#[tokio::test]
async fn repeat_stays_enveloped() {
    let fixture = fixture(RoCrateLimits::default()).await;
    let (status, body) = call(
        &fixture,
        peer_request(
            axum::http::Request::builder()
                .method("GET")
                .uri("/oai?verb=ListRecords&metadataPrefix=oai_dc&metadataPrefix=oai_dc"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("code=\"badArgument\""));
    assert!(body.contains("xsi:schemaLocation="));
}

// The advertised endpoint comes from the configured public URL, never from a
// Host header an untrusted caller controls.
#[tokio::test]
async fn base_url_configured() {
    let fixture = fixture(RoCrateLimits::default()).await;
    fixture
        .state
        .register_rest_public(
            "127.0.0.1:8080".parse().unwrap(),
            Some("https://public.test"),
        )
        .await;
    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, HeaderValue::from_static("evil.test"));
    headers.insert(
        "x-forwarded-host",
        HeaderValue::from_static("forwarded.test"),
    );
    let url = base_url(&fixture.state, "127.0.0.1".parse().unwrap(), &headers).await;
    assert_eq!(url, "https://public.test/api/v1/oai");
}

#[tokio::test]
async fn url_ignores_host() {
    let fixture = fixture(RoCrateLimits::default()).await;
    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, HeaderValue::from_static("evil.test"));
    let url = base_url(&fixture.state, "127.0.0.1".parse().unwrap(), &headers).await;
    assert_eq!(url, BASE_URL_FALLBACK);
}

fn params(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn fault_code(fault: OaiFault) -> String {
    match fault {
        OaiFault::Protocol { code, .. } => code.to_string(),
        OaiFault::Unavailable => "unavailable".to_string(),
        OaiFault::Internal => "internal".to_string(),
    }
}

// the OAI identifier round-trips through the graph IRI
#[test]
fn identifier_round_trips() {
    let id = Ulid::from_bytes([7; 16]);
    let iri = format!("{GRAPH_IRI_PREFIX}{id}");
    assert_eq!(parse_identifier(&iri), Some(id));
    assert_eq!(parse_identifier("oai:other:1"), None);
}

#[test]
fn token_round_trips() {
    let cursor = vec![1, 2, 3, 4];
    let encoded = encode_token(42, cursor.clone());
    let decoded = decode_token(&encoded).unwrap();
    assert_eq!(decoded.until_ms, 42);
    assert_eq!(decoded.prefix, METADATA_PREFIX);
    assert_eq!(decoded.cursor, cursor);
    assert!(decode_token("!!!not base64!!!").is_none());
}

#[test]
fn escapes_xml_specials() {
    assert_eq!(
        escape_xml("a & b < c > \"d\" 'e'"),
        "a &amp; b &lt; c &gt; &quot;d&quot; &apos;e&apos;"
    );
}

// strips the characters XML forbids
#[test]
fn strips_illegal_xml() {
    let value = "a\u{0}b\u{8}c\u{1F}d\u{B}e\tf\nh";
    assert_eq!(escape_xml(value), "abcde\tf\nh");
    assert_eq!(escape_xml("\u{FFFE}\u{FFFF}ok"), "ok");
}

#[test]
fn duplicate_argument_faults() {
    let error = validate_pairs(&params(&[
        ("verb", "GetRecord"),
        ("identifier", "a"),
        ("identifier", "b"),
    ]))
    .err()
    .unwrap();
    assert_eq!(fault_code(error), "badArgument");
}

#[test]
fn unknown_argument_faults() {
    let error = validate_pairs(&params(&[("verb", "Identify"), ("nonsense", "1")]))
        .err()
        .unwrap();
    assert_eq!(fault_code(error), "badArgument");
}

#[test]
fn illegal_verb_faults() {
    assert_eq!(
        fault_code(validate_pairs(&params(&[("verb", "Nope")])).err().unwrap()),
        "badVerb"
    );
    assert_eq!(
        fault_code(validate_pairs(&params(&[])).err().unwrap()),
        "badVerb"
    );
}

#[test]
fn verb_matrix_rejects() {
    // Identify takes no selective arguments; GetRecord takes no window.
    assert_eq!(
        fault_code(
            validate_pairs(&params(&[
                ("verb", "Identify"),
                ("metadataPrefix", "oai_dc")
            ]))
            .err()
            .unwrap()
        ),
        "badArgument"
    );
    assert_eq!(
        fault_code(
            validate_pairs(&params(&[
                ("verb", "GetRecord"),
                ("identifier", "x"),
                ("metadataPrefix", "oai_dc"),
                ("from", "2026-01-01"),
            ]))
            .err()
            .unwrap()
        ),
        "badArgument"
    );
}

#[test]
fn missing_required_faults() {
    assert_eq!(
        fault_code(
            validate_pairs(&params(&[("verb", "ListRecords")]))
                .err()
                .unwrap()
        ),
        "badArgument"
    );
    assert_eq!(
        fault_code(
            validate_pairs(&params(&[
                ("verb", "GetRecord"),
                ("metadataPrefix", "oai_dc")
            ]))
            .err()
            .unwrap()
        ),
        "badArgument"
    );
}

// a resumption token alone needs no metadataPrefix
#[test]
fn token_skips_prefix() {
    let parsed = validate_pairs(&params(&[
        ("verb", "ListRecords"),
        ("resumptionToken", "abc"),
    ]))
    .unwrap();
    assert_eq!(parsed.resumption_token.as_deref(), Some("abc"));
}

#[test]
fn form_keeps_duplicates() {
    let pairs = parse_pairs(b"verb=ListRecords&from=a&from=b");
    assert_eq!(pairs.len(), 3);
    assert_eq!(
        fault_code(validate_pairs(&pairs).err().unwrap()),
        "badArgument"
    );
}

#[test]
fn until_covers_second() {
    let params = OaiParams {
        verb: Some("ListRecords".to_string()),
        metadata_prefix: Some(METADATA_PREFIX.to_string()),
        until: Some("2026-01-02T03:04:05Z".to_string()),
        ..OaiParams::default()
    };
    let (from_ms, until_ms, _) = resolve_window(&params).unwrap();
    assert_eq!(from_ms, 0);
    let second = chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
        .unwrap()
        .timestamp_millis() as u64;
    assert_eq!(until_ms, second + 999);
    // .000, .001 and .999 of that second all fall inside the bound.
    assert!(second < until_ms && second + 999 <= until_ms);
}

#[test]
fn until_covers_day() {
    let params = OaiParams {
        verb: Some("ListRecords".to_string()),
        metadata_prefix: Some(METADATA_PREFIX.to_string()),
        from: Some("2026-01-02".to_string()),
        until: Some("2026-01-02".to_string()),
        ..OaiParams::default()
    };
    let (from_ms, until_ms, _) = resolve_window(&params).unwrap();
    assert_eq!(until_ms - from_ms, 86_400_000 - 1);
}

#[test]
fn mixed_granularity_faults() {
    let params = OaiParams {
        verb: Some("ListRecords".to_string()),
        metadata_prefix: Some(METADATA_PREFIX.to_string()),
        from: Some("2026-01-02".to_string()),
        until: Some("2026-01-03T00:00:00Z".to_string()),
        ..OaiParams::default()
    };
    assert_eq!(
        fault_code(resolve_window(&params).err().unwrap()),
        "badArgument"
    );
}

#[test]
fn omitted_until_freezes() {
    let params = OaiParams {
        verb: Some("ListRecords".to_string()),
        metadata_prefix: Some(METADATA_PREFIX.to_string()),
        ..OaiParams::default()
    };
    let before = current_time_ms();
    let (from_ms, until_ms, cursor) = resolve_window(&params).unwrap();
    let after = current_time_ms();
    assert_eq!(from_ms, 0);
    assert!(cursor.is_none());
    assert_ne!(until_ms, u64::MAX);
    assert!(until_ms >= before && until_ms <= after);
}

// A continuation carries the frozen bound through unchanged.
#[test]
fn token_preserves_until() {
    let params = OaiParams {
        verb: Some("ListRecords".to_string()),
        resumption_token: Some(encode_token(1_700_000_000_999, vec![1, 2, 3])),
        ..OaiParams::default()
    };
    let (_, until_ms, cursor) = resolve_window(&params).unwrap();
    assert_eq!(until_ms, 1_700_000_000_999);
    assert_eq!(cursor.unwrap().as_ref(), &[1, 2, 3]);
}

#[test]
fn token_excludes_args() {
    let params = OaiParams {
        verb: Some("ListRecords".to_string()),
        metadata_prefix: Some(METADATA_PREFIX.to_string()),
        resumption_token: Some("t".to_string()),
        ..OaiParams::default()
    };
    assert_eq!(
        fault_code(resolve_window(&params).err().unwrap()),
        "badArgument"
    );
}

#[test]
fn envelope_declares_schema() {
    let response = respond(
        "https://example.test/api/v1/oai",
        &OaiParams {
            verb: Some("Identify".to_string()),
            ..OaiParams::default()
        },
        Ok("<Identify />".to_string()),
    );
    assert_eq!(response.status(), StatusCode::OK);
    assert!(OAI_PMH_OPEN.contains("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""));
    assert!(OAI_PMH_OPEN.contains("xsi:schemaLocation="));
    assert!(OAI_PMH_OPEN.contains("OAI-PMH.xsd"));
    assert!(OAI_DC_OPEN.contains("xmlns:xsi="));
    assert!(OAI_DC_OPEN.contains("oai_dc.xsd"));
}

// the form content type is checked
#[test]
fn form_type_checked() {
    let mut headers = HeaderMap::new();
    assert!(!is_form_encoded(&headers));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded; charset=utf-8"),
    );
    assert!(is_form_encoded(&headers));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    assert!(!is_form_encoded(&headers));
}
