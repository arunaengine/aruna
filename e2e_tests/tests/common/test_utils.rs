use aruna_rust_api::api::storage::{
    models::v2::{Collection, DataClass as ApiDataClass, Dataset, PermissionLevel, Project},
    services::v2::{
        authorization_service_server::AuthorizationService,
        collection_service_server::CollectionService, create_collection_request,
        create_dataset_request, dataset_service_server::DatasetService,
        project_service_server::ProjectService, CreateAuthorizationRequest,
        CreateCollectionRequest, CreateDatasetRequest, CreateProjectRequest,
        DeleteAuthorizationRequest, GetCollectionRequest, GetDatasetRequest,
        UpdateAuthorizationRequest,
    },
};
use aruna_server::database::dsls::object_dsl::Author;
use aruna_server::{
    database::{
        dsls::{
            internal_relation_dsl::InternalRelation,
            license_dsl::ALL_RIGHTS_RESERVED,
            object_dsl::{EndpointInfo, ExternalRelations, Hashes, KeyValues, Object},
        },
        enums::{
            DataClass, DbPermissionLevel, ObjectMapping, ObjectStatus, ObjectType,
            ReplicationStatus, ReplicationType,
        },
    },
    grpc::{
        authorization::AuthorizationServiceImpl, collections::CollectionServiceImpl,
        datasets::DatasetServiceImpl, projects::ProjectServiceImpl,
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use rand::{distr::Alphanumeric, rng as thread_rng, seq::IteratorRandom, Rng};
use tonic::{
    metadata::{AsciiMetadataKey, AsciiMetadataValue},
    Request,
};

#[allow(dead_code)]
pub fn choose_and_remove(raw: &mut Vec<String>) -> Option<String> {
    let i = (0..raw.len()).choose(&mut thread_rng())?;
    Some(raw.swap_remove(i))
}

/* ----- Begin Testing Constants ---------- */
#[allow(dead_code)]
pub const ADMIN_OIDC_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJocS1BcGJfWS15RzJ1YktjSDFmTGN4UmltZ3YzSlBSelRQUENKbEtpOW9zIn0.eyJleHAiOjE3ODUyMzk0MjQsImlhdCI6MTY5ODgzOTQyNCwiYXV0aF90aW1lIjoxNjk4ODM5NDI0LCJqdGkiOiI5ZjJlMjdhYi04MDIzLTQ1MTctYTE3Yi1jNDY2OGRlZTk2MzAiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjE5OTgvcmVhbG1zL3Rlc3QiLCJhdWQiOiJ0ZXN0LWxvbmciLCJzdWIiOiIxNGYwZTdiZi0wOTQ3LTRhYTEtYThjZC0zMzdkZGVmZjQ1NzMiLCJ0eXAiOiJJRCIsImF6cCI6InRlc3QtbG9uZyIsIm5vbmNlIjoiREFrX3BTZjYxVEpPYnpRWDhwN0JQUSIsInNlc3Npb25fc3RhdGUiOiJiYTkxYmZkMi0wNmY2LTRjYTMtOTFlYS0wYmQ1ZmQxNzZkZjIiLCJhdF9oYXNoIjoiX3pkYXhxMHlucDRvajk1UmhiRG5VdyIsImFjciI6IjEiLCJzaWQiOiJiYTkxYmZkMi0wNmY2LTRjYTMtOTFlYS0wYmQ1ZmQxNzZkZjIiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicHJlZmVycmVkX3VzZXJuYW1lIjoiYXJ1bmFhZG1pbiIsImdpdmVuX25hbWUiOiIiLCJmYW1pbHlfbmFtZSI6IiIsImVtYWlsIjoiYWRtaW5AdGVzdC5jb20ifQ.sV0qo32b4tl7Y984_hW8Pc8a8trkmNg_6MKb7l3aacEH6eC1633JsI8D6qMPw22y4Lf5sb3XOCY_LZQpIKWs7TmkaSlv-9I2Ioi9kZRHpoNd75PnYJDFi6NrK7byJ5IeE167UskEqVTNfCkhkWFUzjogDRaHL-oscb-aTG35tqR-9DcVWUb5wuyKYbJQyRVetiQIKdo-ExNgqad1ScVPdhX9ktRJRZvWSeP7AHV2NpoM3x0WojAWXNIkhWoNksUJclaR25PcTlQmAh43QvICxpaiCCKTOcNSf-wBLGzTvxvFijYjYPgfyXCThFzOJkBC-qhrpVRXQh_nVcLmXJPxCQ";
#[allow(dead_code)]
pub const ADMIN_USER_ULID: &str = "01H819G3ZMK5DC9Q5PD18N9SXB";
#[allow(dead_code)]
pub const USER1_OIDC_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJocS1BcGJfWS15RzJ1YktjSDFmTGN4UmltZ3YzSlBSelRQUENKbEtpOW9zIn0.eyJleHAiOjE3ODUyMzk1OTksImlhdCI6MTY5ODgzOTU5OSwiYXV0aF90aW1lIjoxNjk4ODM5NTk5LCJqdGkiOiJmZjkyMzEwNC1hZGNkLTRjOTEtYjdjNi03MWM1ODMxNjlhYzciLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjE5OTgvcmVhbG1zL3Rlc3QiLCJhdWQiOiJ0ZXN0LWxvbmciLCJzdWIiOiI4ZGJlZTAwOS1hM2U4LTQ2NjQtODg1Ni0xNDE3M2Q5YWJkNWIiLCJ0eXAiOiJJRCIsImF6cCI6InRlc3QtbG9uZyIsIm5vbmNlIjoiQ21NRWxIM3JQSVF2dENBTFVSQWlPZyIsInNlc3Npb25fc3RhdGUiOiIyY2FmNGE0Ni1mZDYxLTQ2MWEtODIwZS1jMTM0YmY4ZjU0ZTYiLCJhdF9oYXNoIjoiMXRRYjhETWRaNjJVaW9MTl9tRkQxZyIsImFjciI6IjEiLCJzaWQiOiIyY2FmNGE0Ni1mZDYxLTQ2MWEtODIwZS1jMTM0YmY4ZjU0ZTYiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicHJlZmVycmVkX3VzZXJuYW1lIjoicmVndWxhciIsImdpdmVuX25hbWUiOiIiLCJmYW1pbHlfbmFtZSI6IiIsImVtYWlsIjoicmVndWxhckB0ZXN0LmNvbSJ9.dc759HTpLgcMT8exZPWpgO9k3O5eQy0KKkqVRj6LQZAIq9CcK-rEHs6P6QiT3vWq8CKLQkBcYPTY4zniKQ78spip9b1OrNdvQ5K9aHuCsZHvaH72tOXQGCsMXKwV_WX6EkRn75A1y4nqJ0H3GCcrNzJTLeh32dcUcxHZtHxcBp3SKpTeq6e-hXYP1XSK73KfSsDj5-zYcaVHWR-av7Q7YcxBul4P2bfOPQRDZNIqkHa7cZGD6nMpLb5WFB-mGHqEB3V4dmvF4Wu9CJScyiVkleG-aSRLXzGDQMtk8iRbCM-xQpr-JvwvKvQXeas5B6ifiMO8GRq8DOPf5m9rCAwEVw";
#[allow(dead_code)]
pub const USER1_ULID: &str = "01H8KWYY5MTAH1YZGPYVS7PQWD";
#[allow(dead_code)]
pub const USER2_OIDC_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJocS1BcGJfWS15RzJ1YktjSDFmTGN4UmltZ3YzSlBSelRQUENKbEtpOW9zIn0.eyJleHAiOjE3ODUyMzk3NDgsImlhdCI6MTY5ODgzOTc0OCwiYXV0aF90aW1lIjoxNjk4ODM5NzQ4LCJqdGkiOiI1Nzc0MmIzNS1iMGQ5LTRkNTUtYjMzYi02ODkxMjFlM2ZhNGMiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjE5OTgvcmVhbG1zL3Rlc3QiLCJhdWQiOiJ0ZXN0LWxvbmciLCJzdWIiOiIyMWU4N2M2MC1iMDVjLTQwYjEtYWM1Yy05ODJiNTJhYjI4NjUiLCJ0eXAiOiJJRCIsImF6cCI6InRlc3QtbG9uZyIsIm5vbmNlIjoiZlM0aGJXcVE2VlI3X3NYc0wyYlRiZyIsInNlc3Npb25fc3RhdGUiOiIwMDA4MTQ5Mi0wODkwLTRmZTYtYjc1OS05Y2VmYmM1MTQ0MTMiLCJhdF9oYXNoIjoiU1JvZ0ZPV0NIMjI2THp4OExRZDBUZyIsImFjciI6IjEiLCJzaWQiOiIwMDA4MTQ5Mi0wODkwLTRmZTYtYjc1OS05Y2VmYmM1MTQ0MTMiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsInByZWZlcnJlZF91c2VybmFtZSI6InJlZ3VsYXIyIiwiZ2l2ZW5fbmFtZSI6IiIsImZhbWlseV9uYW1lIjoiIn0.DHI4fbRLn_7Ul-Vzjz5P7vaFNCOz75W_2OkWKzWF8Ipv3F7ett14-tuRo5fewngCV0JKRiSlnsfPxVkLQIh4659egQcjpZbXvgboMKtzfrUO7AMN-S9vZF143r_zwgllEHqVyAyPqymgLzCMrfk0z2r_nqVCY4qLXV-pSbETL6o_ZWhNEAjIAkD1PzhVpqD2k-OfdVzMFj8Ov7_p5xo3Pl1iNVCtK-4QpT5OLhkHb8ycbcJyuC0XRmm4Es31jtDSjuriKCP5iJl4W4GcXF9SGU9ftGRSxxumv5_p0z9u47nYf2-BcEo4H-1offDZ8MDxpg2QZ4Wh5FPBakNqjdgMww";
#[allow(dead_code)]
pub const USER2_ULID: &str = "01HBG7TQS8HTV3M2PKFKMMBSJ5";
#[allow(dead_code)]
pub const DEFAULT_ENDPOINT_ULID: &str = "01H81W0ZMB54YEP5711Q2BK46V";
#[allow(dead_code)]
pub const INVALID_OIDC_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjEyNjc0Mzk0ODQsImlhdCI6MTI2NzQwMzQ4NCwiYXV0aF90aW1lIjoxNjY3NDAzNDg0LCJqdGkiOiI2NzRkNDZiNy1kZGFkLTQ1ZmYtYjU4YS01MjFkYjk2M2QyNjYiLCJpc3MiOiJsb2NhbGhvc3QudGVzdCIsImF1ZCI6ImFydW5hIiwic3ViIjoiMzk4OTM3ODEtMzIwZS00ZGJmLWJlMzktYzA2ZDhiMjhlODk3IiwidHlwIjoiSUQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJyZWd1bGFyLXVzZXItZXhwaXJlZCJ9.M3oVEybyUAytlCxJ7C8SsIo9_2uK1VC0dfUhgWtGW96Y8SmKqRnIyd-9SywsFHfzlUvNESKanW8ftnrNimfibneLFmDvW5t9FFuh3TkGuMUIpe-YIStBqycv1Vs8cbVdluEHQnwRobAvwBFMiHQt15DsBKoOD1IQXKIb0cbgOSL9QDwhHsu5_h8P10THoqaNrq14C6rX9WarzHIa0rruoujoKVWvo4yH8CEtOMFRcNNYWzFXnKIXqHVJzMhUjiuOn4KO6PuL3_8eYmmJJp22Xb0K8h3C8ukGpwrWbhWGw4vWot9pXEWpoBgTTDwF4PTWP7AJzTkeWu_nc1ak3uxp8uVy31CZa4tOxHfNkP19Ala4r7BUfUis195wU7eWECzm4G5V-GQkI8DDK_vWLV-4Nf_YtEAOma4yn5FeJQqZ2WaUAueU24TLKrTGm_lRrIVQ-w_xUNnW9V6iRvztkeFcFVNbbm59JDmEWffzbYa5B99h87LH-CaCxoYdy4rN6-PZCA5E-dIq-qIprVkuUpQats72qkQ4_WqgaQU45aJS0StcB2cXlri919YR6so1culkv7pqD4Dh_L_OR5N7UNmZHoeBlUeE6MSnWRJx7NfZNe4QcbKgaT2zqaDvyrIsxvLhIYMKOqpMQPPqR9OV7ylPxnnykvg-A_8shXXgaR0qkgQ";
/* ----- End Testing Constants ---------- */

#[allow(dead_code)]
pub fn new_object(user_id: DieselUlid, object_id: DieselUlid, object_type: ObjectType) -> Object {
    Object {
        id: object_id,
        revision_number: 0,
        name: object_id.to_string(),
        title: "title-test".to_string(),
        description: "b".to_string(),
        count: 1,
        created_at: None,
        content_len: if let ObjectType::OBJECT = object_type {
            1337
        } else {
            0
        },
        created_by: user_id,
        key_values: Json(KeyValues(vec![])),
        object_status: ObjectStatus::AVAILABLE,
        data_class: DataClass::PUBLIC,
        object_type,
        external_relations: Json(ExternalRelations(DashMap::default())),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(DashMap::from_iter([(
            DieselUlid::generate(),
            EndpointInfo {
                replication: ReplicationType::FullSync,
                status: Some(ReplicationStatus::Waiting),
            },
        )])),
        data_license: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license: ALL_RIGHTS_RESERVED.to_string(),
        authors: Json(vec![Author {
            first_name: "Jane".to_string(),
            last_name: "Doe".to_string(),
            email: Some("jane.doe@test.org".to_string()),
            orcid: None,
            user_id: None,
        }]),
    }
}

#[allow(dead_code)]
pub fn new_internal_relation(origin: &Object, target: &Object) -> InternalRelation {
    InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: origin.id,
        origin_type: origin.object_type,
        target_pid: target.id,
        target_type: target.object_type,
        relation_name: "BELONGS_TO".to_string(),
        target_name: target.name.to_string(),
    }
}

#[allow(dead_code)]
pub fn object_from_mapping(
    user_id: DieselUlid,
    object_mapping: ObjectMapping<DieselUlid>,
) -> Object {
    let (id, object_type) = match object_mapping {
        ObjectMapping::PROJECT(id) => (id, ObjectType::PROJECT),
        ObjectMapping::COLLECTION(id) => (id, ObjectType::COLLECTION),
        ObjectMapping::DATASET(id) => (id, ObjectType::DATASET),
        ObjectMapping::OBJECT(id) => (id, ObjectType::OBJECT),
    };
    Object {
        id,
        revision_number: 0,
        name: "a".to_string(),
        description: "b".to_string(),
        title: "title-test".to_string(),
        count: 1,
        created_at: None,
        content_len: if let ObjectType::OBJECT = object_type {
            1337
        } else {
            0
        },
        created_by: user_id,
        key_values: Json(KeyValues(vec![])),
        object_status: ObjectStatus::AVAILABLE,
        data_class: DataClass::PRIVATE,
        object_type,
        external_relations: Json(ExternalRelations(DashMap::default())),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(DashMap::default()),
        data_license: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license: ALL_RIGHTS_RESERVED.to_string(),
        authors: Json(vec![Author {
            first_name: "Jane".to_string(),
            last_name: "Doe".to_string(),
            email: Some("jane.doe@test.org".to_string()),
            orcid: None,
            user_id: None,
        }]),
    }
}

#[allow(dead_code)]
pub fn add_token<T>(mut req: Request<T>, token: &str) -> Request<T> {
    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes(b"Authorization").unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    req
}

#[allow(dead_code)]
pub fn rand_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/* ----- Resource create convenience functions ---------- */
#[allow(dead_code)]
pub async fn fast_track_grpc_get_collection(
    collection_service: &CollectionServiceImpl,
    token: &str,
    collection_id: &str,
) -> Collection {
    // Create request with token
    let get_request = add_token(
        Request::new(GetCollectionRequest {
            collection_id: collection_id.to_string(),
        }),
        token,
    );

    // Fetch collection vie gRPC service
    collection_service
        .get_collection(get_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap()
}

#[allow(dead_code)]
pub async fn fast_track_grpc_get_dataset(
    dataset_service: &DatasetServiceImpl,
    token: &str,
    datset_id: &str,
) -> Dataset {
    // Create request with token
    let get_request = add_token(
        Request::new(GetDatasetRequest {
            dataset_id: datset_id.to_string(),
        }),
        token,
    );

    // Fetch collection vie gRPC service
    dataset_service
        .get_dataset(get_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap()
}

#[allow(dead_code)]
pub async fn fast_track_grpc_project_create(
    project_service: &ProjectServiceImpl,
    token: &str,
) -> Project {
    // Create request with token
    let project_name = rand_string(32).to_lowercase();

    let create_request = CreateProjectRequest {
        name: project_name.to_string(),
        title: "title-test".to_string(),
        description: "".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: ApiDataClass::Private as i32,
        preferred_endpoint: "".to_string(),
        default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![aruna_rust_api::api::storage::models::v2::Author {
            first_name: "Jane".to_string(),
            last_name: "Doe".to_string(),
            email: Some("jane.doe@test.org".to_string()),
            orcid: None,
            id: None,
        }],
    };

    let grpc_request = add_token(Request::new(create_request), token);

    // Create project via gRPC service
    let create_response = project_service
        .create_project(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_project = create_response.project.unwrap();

    assert!(!proto_project.id.is_empty());
    assert_eq!(proto_project.name, project_name);

    proto_project
}

#[allow(dead_code)]
pub async fn fast_track_grpc_collection_create(
    collection_service: &CollectionServiceImpl,
    token: &str,
    parent: create_collection_request::Parent,
) -> Collection {
    // Create request with token
    let collection_name = rand_string(32);

    //ToDo: Fetch parent for license inheritance?

    let create_request = CreateCollectionRequest {
        name: collection_name.to_string(),
        title: "title-test".to_string(),
        description: "".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: ApiDataClass::Private as i32,
        parent: Some(parent),
        default_data_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        metadata_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        authors: vec![aruna_rust_api::api::storage::models::v2::Author {
            first_name: "Jane".to_string(),
            last_name: "Doe".to_string(),
            email: Some("jane.doe@test.org".to_string()),
            orcid: None,
            id: None,
        }],
    };

    let grpc_request = add_token(Request::new(create_request), token);

    // Create project via gRPC service
    let create_response = collection_service
        .create_collection(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_collection = create_response.collection.unwrap();

    assert!(!proto_collection.id.is_empty());
    assert_eq!(proto_collection.name, collection_name);

    proto_collection
}

#[allow(dead_code)]
pub async fn fast_track_grpc_dataset_create(
    dataset_service: &DatasetServiceImpl,
    token: &str,
    parent: create_dataset_request::Parent,
) -> Dataset {
    // Create request with token
    let dataset_name = rand_string(32);

    //ToDo: Fetch parent for license inheritance?

    let create_request = CreateDatasetRequest {
        name: dataset_name.to_string(),
        description: "".to_string(),
        title: "".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: ApiDataClass::Private as i32,
        parent: Some(parent),
        default_data_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        metadata_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        authors: vec![aruna_rust_api::api::storage::models::v2::Author {
            first_name: "Jane".to_string(),
            last_name: "Doe".to_string(),
            email: Some("jane.doe@test.org".to_string()),
            orcid: None,
            id: None,
        }],
    };

    let grpc_request = add_token(Request::new(create_request), token);

    // Create project via gRPC service
    let create_response = dataset_service
        .create_dataset(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_dataset = create_response.dataset.unwrap();

    assert!(!proto_dataset.id.is_empty());
    assert_eq!(proto_dataset.name, dataset_name);

    proto_dataset
}

#[allow(dead_code)]
pub async fn fast_track_grpc_permission_add(
    auth_service: &AuthorizationServiceImpl,
    token: &str,
    user_ulid: &DieselUlid,
    resource_ulid: &DieselUlid,
    permission_level: DbPermissionLevel,
) {
    // Create request with token
    let inner_request = CreateAuthorizationRequest {
        resource_id: resource_ulid.to_string(),
        user_id: user_ulid.to_string(),
        permission_level: PermissionLevel::from(permission_level) as i32,
    };
    let grpc_request = add_token(Request::new(inner_request), token);

    // Add permission to user
    auth_service
        .create_authorization(grpc_request)
        .await
        .unwrap();
}

#[allow(dead_code)]
pub async fn fast_track_grpc_permission_update(
    auth_service: &AuthorizationServiceImpl,
    token: &str,
    user_ulid: &DieselUlid,
    resource_ulid: &DieselUlid,
    permission_level: DbPermissionLevel,
) {
    // Create request with token
    let inner_request = UpdateAuthorizationRequest {
        resource_id: resource_ulid.to_string(),
        user_id: user_ulid.to_string(),
        permission_level: PermissionLevel::from(permission_level) as i32,
    };
    let grpc_request = add_token(Request::new(inner_request), token);

    // Add permission to user
    auth_service
        .update_authorization(grpc_request)
        .await
        .unwrap();
}

#[allow(dead_code)]
pub async fn fast_track_grpc_permission_delete(
    auth_service: &AuthorizationServiceImpl,
    token: &str,
    user_ulid: &DieselUlid,
    resource_ulid: &DieselUlid,
) {
    // Create request with token
    let inner_request = DeleteAuthorizationRequest {
        resource_id: resource_ulid.to_string(),
        user_id: user_ulid.to_string(),
    };
    let grpc_request = add_token(Request::new(inner_request), token);

    // Add permission to user
    auth_service
        .delete_authorization(grpc_request)
        .await
        .unwrap();
}
