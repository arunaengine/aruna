use super::*;
use crate::error::ServerError;
use crate::tests::fixtures::routes::{
    seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state as build_state,
    test_storage,
};
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::NodeCapabilities;
use aruna_core::structs::RealmId;
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, PathRestriction, Permission,
    RealmAuthorizationDocument, RealmConfigDocument, group_permission_path,
};
use std::sync::Arc;
use tempfile::TempDir;
use ulid::Ulid;

async fn test_state() -> (TempDir, Arc<ServerState>, AuthContext) {
    let (storage_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let state = Arc::new(
        build_state(
            Arc::new(test_context(storage)),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );
    let auth = AuthContext {
        user_id: UserId::new(Ulid::from_bytes([3u8; 16]), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    (storage_dir, state, auth)
}

fn test_auth_context(path_restrictions: Option<Vec<PathRestriction>>) -> AuthContext {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    AuthContext {
        user_id: UserId::new(Ulid::from_bytes([9u8; 16]), realm_id),
        realm_id,
        path_restrictions,
        session: None,
    }
}

/// State whose caller holds group write, plus a credential of another member.
async fn revoke_state() -> (TempDir, Arc<ServerState>, AuthContext, String) {
    let (dir, state, auth) = test_state().await;
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let group_id = Ulid::from_bytes([4u8; 16]);
    let owner = UserId::new(Ulid::from_bytes([5u8; 16]), realm_id);
    let actor = Actor {
        node_id,
        user_id: auth.user_id,
        realm_id,
    };
    seed_realm_config(&state.get_ctx(), realm_id, &actor).await;
    seed_realm_auth(&state.get_ctx(), realm_id, &actor).await;
    seed_group_docs(
        &state.get_ctx(),
        realm_id,
        &actor,
        group_id,
        "credential-group",
        auth.user_id,
    )
    .await;

    let (access_key_id, _, _) = drive(
        CreateUserAccessOperation::new(
            CreateUserAccessConfig {
                user_identity: owner,
                group_id,
                expiry: SystemTime::now() + Duration::from_secs(3600),
                path_restrictions: None,
                issued_by: *node_id.as_bytes(),
            },
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .unwrap()
    .unwrap();

    (dir, state, auth, access_key_id)
}

/// Group whose only role for the caller carries the given permissions.
async fn scoped_state(
    permissions: Vec<(String, Permission)>,
) -> (TempDir, Arc<ServerState>, AuthContext, Ulid) {
    use std::collections::{HashMap, HashSet};
    let (dir, state, auth) = test_state().await;
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let group_id = Ulid::from_bytes([7u8; 16]);
    let actor = Actor {
        node_id,
        user_id: auth.user_id,
        realm_id,
    };
    let role_id = Ulid::from_bytes([8u8; 16]);
    let group_auth = GroupAuthorizationDocument {
        group_id,
        roles: HashMap::from([(
            role_id,
            aruna_core::structs::Role {
                role_id,
                name: "scoped".to_string(),
                permissions: permissions.into_iter().collect(),
                assigned_users: HashSet::from([auth.user_id]),
            },
        )]),
        policies: Vec::new(),
    };
    let group = Group {
        display_name: "scoped-group".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner: auth.user_id,
    };
    for (key_space, key, value) in [
        (
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        ),
        (
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        ),
    ] {
        state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
    }

    (dir, state, auth, group_id)
}

async fn create_credential(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    group_id: Ulid,
    path_restrictions: Option<Vec<CreateS3PathRestriction>>,
) -> ServerResult<(StatusCode, Json<CreateS3CredentialsResponse>)> {
    create_s3_credentials(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(CreateS3CredentialsRequest {
            group_id: group_id.to_string(),
            expires_in_seconds: None,
            path_restrictions,
        }),
    )
    .await
}

#[tokio::test]
async fn viewer_takes_credential() {
    // Read on the group data root is enough: the credential cannot widen it.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::from_bytes([7u8; 16]);
    let (_dir, state, auth, group_id) = scoped_state(vec![(
        format!("/{realm_id}/g/{group_id}/data/**"),
        Permission::READ,
    )])
    .await;

    assert!(
        create_credential(&state, &auth, group_id, None)
            .await
            .is_ok()
    );
    let (_, Json(response)) = create_credential(
        &state,
        &auth,
        group_id,
        Some(vec![CreateS3PathRestriction {
            pattern: "shared/**".to_string(),
            permission: "READ".to_string(),
        }]),
    )
    .await
    .unwrap();
    assert!(!response.access_secret.is_empty());
}

#[tokio::test]
async fn viewer_cannot_write() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::from_bytes([7u8; 16]);
    let (_dir, state, auth, group_id) = scoped_state(vec![(
        format!("/{realm_id}/g/{group_id}/data/**"),
        Permission::READ,
    )])
    .await;

    let error = create_credential(
        &state,
        &auth,
        group_id,
        Some(vec![CreateS3PathRestriction {
            pattern: "shared/**".to_string(),
            permission: "WRITE".to_string(),
        }]),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
}

#[tokio::test]
async fn subpath_takes_credential() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::from_bytes([7u8; 16]);
    let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let group_root = group_permission_path(realm_id, group_id, node_id);
    let (_dir, state, auth, group_id) = scoped_state(vec![(
        format!("{group_root}/study/imaging/**"),
        Permission::READ,
    )])
    .await;

    assert!(
        create_credential(&state, &auth, group_id, None)
            .await
            .is_ok()
    );
    assert!(
        create_credential(
            &state,
            &auth,
            group_id,
            Some(vec![CreateS3PathRestriction {
                pattern: "study/imaging/**".to_string(),
                permission: "READ".to_string(),
            }]),
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn outsider_refused() {
    let (_dir, state, auth, group_id) = scoped_state(vec![(
        "/other/g/group/data/**".to_string(),
        Permission::WRITE,
    )])
    .await;

    assert!(matches!(
        create_credential(&state, &auth, group_id, None).await,
        Err(ServerError::Forbidden)
    ));
}

#[tokio::test]
async fn writer_cannot_revoke() {
    // Group write must not reach the S3 credential of another member.
    let (_dir, state, auth, access_key_id) = revoke_state().await;

    let error = revoke_s3_credentials(
        State(state.clone()),
        Extension(Some(auth)),
        Path(access_key_id.clone()),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ServerError::Forbidden));
    let credential = drive(GetUserAccessOperation::new(access_key_id), &state.get_ctx())
        .await
        .unwrap();
    assert!(!credential.is_revoked());
}

#[test]
fn group_root_canonical() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::from_bytes([2u8; 16]);
    let node_id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();

    let group_root = group_permission_path(realm_id, group_id, node_id);
    assert_eq!(
        group_root,
        format!("/{realm_id}/g/{group_id}/data/{node_id}")
    );
}

#[test]
fn permission_case_insensitive() {
    assert_eq!(
        parse_permission("read").unwrap(),
        aruna_core::structs::Permission::READ
    );
    assert_eq!(
        parse_permission("WRITE").unwrap(),
        aruna_core::structs::Permission::WRITE
    );
    assert_eq!(
        parse_permission("Deny").unwrap(),
        aruna_core::structs::Permission::DENY
    );
}

#[test]
fn delegation_accepts_descendants() {
    assert_eq!(
        DelegationScope::parse_supported("/root/path"),
        Some(DelegationScope::exact("/root/path".to_string()))
    );
    assert_eq!(
        DelegationScope::parse_supported("/root/path/**"),
        Some(DelegationScope::descendants("/root/path".to_string()))
    );
    assert_eq!(DelegationScope::parse_supported("/root/*/path"), None);
    assert_eq!(DelegationScope::parse_supported("/root/**/path"), None);
    assert_eq!(DelegationScope::parse_supported("relative/path"), None);
}

#[test]
fn exact_scope_preserved() {
    let scope = DelegationScope::exact("/realm/g/group/data/node/object".to_string());
    assert_eq!(
        scope.intersect_group_root("/realm/g/group/data/node"),
        Some(DelegationScope::exact(
            "/realm/g/group/data/node/object".to_string()
        ))
    );
}

#[test]
fn descendant_scope_narrowed() {
    let scope = DelegationScope::descendants("/realm/g/group/data".to_string());
    assert_eq!(
        scope.intersect_group_root("/realm/g/group/data/node"),
        Some(DelegationScope::descendants(
            "/realm/g/group/data/node".to_string()
        ))
    );
}

#[test]
fn relative_paths_normalized() {
    let group_root = "/realm/g/group/data/node";

    assert_eq!(
        normalize_requested_restrictions(
            Some(vec![CreateS3PathRestriction {
                pattern: "nested/path".to_string(),
                permission: "WRITE".to_string(),
            }]),
            group_root,
        )
        .unwrap(),
        Some(vec![NormalizedRestriction {
            scope: DelegationScope::exact("/realm/g/group/data/node/nested/path".to_string()),
            permission: Permission::WRITE,
        }])
    );
}

#[test]
fn empty_path_normalized() {
    let group_root = "/realm/g/group/data/node";

    assert_eq!(
        normalize_requested_restrictions(
            Some(vec![CreateS3PathRestriction {
                pattern: String::new(),
                permission: "READ".to_string(),
            }]),
            group_root,
        )
        .unwrap(),
        Some(vec![NormalizedRestriction {
            scope: DelegationScope::exact(group_root.to_string()),
            permission: Permission::READ,
        }])
    );
}

#[test]
fn external_path_rejected() {
    let err = normalize_requested_restrictions(
        Some(vec![CreateS3PathRestriction {
            pattern: "/realm/g/other/data/node/object".to_string(),
            permission: "WRITE".to_string(),
        }]),
        "/realm/g/group/data/node",
    )
    .unwrap_err();

    assert!(matches!(err, ServerError::Forbidden));
}

#[test]
fn wildcards_are_rejected() {
    let err = normalize_requested_restrictions(
        Some(vec![CreateS3PathRestriction {
            pattern: "nested/*/path".to_string(),
            permission: "WRITE".to_string(),
        }]),
        "/realm/g/group/data/node",
    )
    .unwrap_err();

    assert!(matches!(err, ServerError::BadRequest));
}

#[test]
fn unrelated_groups_filtered() {
    let auth = test_auth_context(Some(vec![PathRestriction {
        pattern: "/realm/g/other/data/node/**".to_string(),
        permission: Permission::WRITE,
    }]));

    assert_eq!(
        normalize_auth_restrictions(&auth, "/realm/g/group/data/node").unwrap(),
        Some(Vec::new())
    );
}

#[tokio::test]
async fn list_rejects_scope() {
    let (_storage_dir, state, auth) = test_state().await;
    for restrictions in [
        Some(vec![PathRestriction {
            pattern: "/restricted/**".to_string(),
            permission: Permission::READ,
        }]),
        Some(Vec::new()),
    ] {
        let mut restricted = auth.clone();
        restricted.path_restrictions = restrictions;
        let error = list_s3_credentials(State(state.clone()), Extension(Some(restricted)))
            .await
            .unwrap_err();
        assert!(matches!(error, ServerError::Forbidden));
    }

    let (status, Json(response)) = list_s3_credentials(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert!(response.credentials.is_empty());
}

#[test]
fn broad_scope_narrowed() {
    let auth = test_auth_context(Some(vec![PathRestriction {
        pattern: "/realm/g/group/data/**".to_string(),
        permission: Permission::WRITE,
    }]));

    assert_eq!(
        normalize_auth_restrictions(&auth, "/realm/g/group/data/node").unwrap(),
        Some(vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/realm/g/group/data/node".to_string()),
            permission: Permission::WRITE,
        }])
    );
}

#[test]
fn auth_wildcards_rejected() {
    let auth = test_auth_context(Some(vec![PathRestriction {
        pattern: "/realm/g/group/**/node".to_string(),
        permission: Permission::WRITE,
    }]));

    let err = normalize_auth_restrictions(&auth, "/realm/g/group/data/node").unwrap_err();
    assert!(matches!(err, ServerError::Forbidden));
}

#[test]
fn auth_restrictions_inherited() {
    let auth = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/allowed".to_string()),
        permission: Permission::WRITE,
    }];

    assert_eq!(
        merge_effective_restrictions(Some(&auth), None),
        Some(auth.clone())
    );
}

#[test]
fn request_restrictions_used() {
    let requested = vec![NormalizedRestriction {
        scope: DelegationScope::exact("/group/object".to_string()),
        permission: Permission::READ,
    }];

    assert_eq!(
        merge_effective_restrictions(None, Some(&requested)),
        Some(requested.clone())
    );
}

#[test]
fn requested_allow_used() {
    let auth = vec![NormalizedRestriction {
        scope: DelegationScope::exact("/group/object".to_string()),
        permission: Permission::WRITE,
    }];
    let requested = auth.clone();

    assert_eq!(
        merge_effective_restrictions(Some(&auth), Some(&requested)),
        Some(requested)
    );
}

#[test]
fn requested_read_preserved() {
    let auth = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/allowed".to_string()),
        permission: Permission::WRITE,
    }];
    let requested = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/allowed".to_string()),
        permission: Permission::READ,
    }];

    assert_eq!(
        merge_effective_restrictions(Some(&auth), Some(&requested)),
        Some(requested)
    );
}

#[test]
fn requested_write_preserved() {
    let auth = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/allowed".to_string()),
        permission: Permission::READ,
    }];
    let requested = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/allowed".to_string()),
        permission: Permission::WRITE,
    }];

    assert_eq!(
        merge_effective_restrictions(Some(&auth), Some(&requested)),
        Some(requested)
    );
}

#[test]
fn auth_denies_preserved() {
    let auth = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/blocked".to_string()),
        permission: Permission::DENY,
    }];
    let requested = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/allowed".to_string()),
        permission: Permission::WRITE,
    }];

    assert_eq!(
        merge_effective_restrictions(Some(&auth), Some(&requested)),
        Some(vec![requested[0].clone(), auth[0].clone(),])
    );
}

#[test]
fn requested_denies_preserved() {
    let auth = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/allowed".to_string()),
        permission: Permission::WRITE,
    }];
    let requested = vec![NormalizedRestriction {
        scope: DelegationScope::descendants("/group/blocked".to_string()),
        permission: Permission::DENY,
    }];

    assert_eq!(
        merge_effective_restrictions(Some(&auth), Some(&requested)),
        Some(vec![auth[0].clone(), requested[0].clone(),])
    );
}
