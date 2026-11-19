//! Drives the authorized and pinned creates through their children without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn auth(realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_parts(8, 8), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn payload() -> CreateDocumentPayload {
    CreateDocumentPayload::RoCrate {
        jsonld: "{}".to_string(),
    }
}

fn extras() -> PolicyRequestExtras {
    PolicyRequestExtras::operation("metadata.create")
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn record(document_id: Ulid) -> MetadataRegistryRecord {
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: "datasets/new".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: String::new(),
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(4, 4),
        last_event_id: Ulid::from_parts(4, 4),
    }
}

fn create(path: &str) -> AuthorizedCreateOperation {
    let document = DocumentCreate {
        group_id: Ulid::from_parts(2, 2),
        path: path.to_string(),
        public: false,
        payload: payload(),
        commit_message: None,
    };
    let node = (realm(), node(1), true);
    AuthorizedCreateOperation::new(node, (auth(realm()), extras(), None), document)
}

fn pinned(target: NodeId) -> PinnedCreateOperation {
    let actor = Actor {
        node_id: node(1),
        user_id: auth(realm()).user_id,
        realm_id: realm(),
    };
    let create = PinnedCreate {
        group_id: Ulid::from_parts(2, 2),
        path: "datasets/new".to_string(),
        document_id: Ulid::from_parts(3, 3),
        target,
        payload: payload(),
        message: None,
    };
    PinnedCreateOperation::new((actor, Some(node(1))), (auth(realm()), extras()), create)
}

#[test]
fn empty_path_refused() {
    assert_eq!(create("//").first(), Err(CreateAuthorizedError::EmptyPath));
}

#[test]
fn unreadable_config_unavailable() {
    let mut create = create("datasets/new");
    assert!(matches!(create.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    let unavailable = CreateAuthorizedError::Api(MetadataApiError::ServiceUnavailable);
    assert_eq!(create.next(missing), Err(unavailable));
}

#[test]
fn routed_create_emits() {
    let mut create = create("datasets/new");
    create.stage = AuthorizedStage::Route;
    let document_id = Ulid::from_parts(3, 3);
    let created = CreateDocumentResult {
        record: record(document_id),
        event_id: Ulid::from_parts(4, 4),
    };
    let Ok(Next::Then { output, calls }) = create.next(child(Ok::<_, MetadataWriteError>(created)))
    else {
        panic!("expected the record with its watch event");
    };
    assert_eq!(output, record(document_id));
    assert!(matches!(calls.as_slice(), [IoCall::Child(_)]));
}

#[test]
fn foreign_pin_refused() {
    let mut create = pinned(node(1));
    create.auth = auth(RealmId::from_bytes([9; 32]));
    assert_eq!(create.first(), Err(CreateAuthorizedError::Forbidden));
}

#[test]
fn pinned_denied_stops() {
    let mut create = pinned(node(1));
    assert!(matches!(create.first(), Ok(Next::Call(IoCall::Child(_)))));
    let denied = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    let refused = CreateAuthorizedError::Authorize(AuthorizeError::PermissionDenied);
    assert_eq!(create.next(denied), Err(refused));
}

#[test]
fn pinned_target_routes() {
    // This node creates its own pinned document; another target needs the realm config to
    // forward it.
    let mut local = pinned(node(1));
    local.first().unwrap();
    local.next(child(Ok::<_, AuthorizeError>(()))).unwrap();
    assert_eq!(local.stage, PinnedStage::Create);

    let mut remote = pinned(node(2));
    remote.first().unwrap();
    remote.next(child(Ok::<_, AuthorizeError>(()))).unwrap();
    assert_eq!(remote.stage, PinnedStage::Config);
}

#[test]
fn forward_checks_record() {
    let config = CreateDocumentConfig {
        actor: Actor {
            node_id: node(1),
            user_id: auth(realm()).user_id,
            realm_id: realm(),
        },
        group_id: Ulid::from_parts(2, 2),
        document_id: Ulid::from_parts(3, 3),
        document_path: "datasets/new".to_string(),
        public: false,
        payload: payload(),
    };
    let realm_config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    let mut forward =
        ForwardCreateOperation::new((Some(node(1)), realm_config), (config, None), (None, None));
    forward.placement = Some(aruna_core::structs::placement::record::PlacementRef::NIL);
    let other = MetadataTransportMessage::ForwardedRecord {
        record: Box::new(record(Ulid::from_parts(9, 9))),
    };
    let answer = child(Ok::<_, HolderWriteError>(other));
    assert!(matches!(
        forward.next(answer),
        Err(MetadataWriteError::Undeliverable(_))
    ));
}
