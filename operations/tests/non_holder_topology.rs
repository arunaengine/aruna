//! Holder and non-holder coverage on a realm sized above the replication factor.
//!
//! Every other multi-node fixture in this workspace runs at `node_count <= RF`,
//! where every node holds every subject and non-holder behaviour is
//! unobservable. These tests run five nodes at RF three and assert the
//! not-a-holder precondition through the placement resolver before exercising
//! the path, so a regression to universal holdership fails the fixture instead
//! of quietly voiding the test.

mod topology;

use std::collections::{HashMap, HashSet};

use aruna_core::UserId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::{
    Actor, AuthContext, Permission, RealmAuthorizationDocument, RealmId, Role,
};
use aruna_operations::add_group_role::{AddGroupRoleConfig, AddGroupRoleOperation};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::delete_metadata_document::DeleteMetadataDocumentOperation;
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::placement::PlacementResolutionContext;
use aruna_operations::read_user_document::ReadUserDocumentOperation;
use aruna_operations::register_or_get_oidc_user::{
    RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation,
};
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentMutation, UpdateMetadataDocumentOperation,
};
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const NODE_COUNT: usize = 5;
const REPLICATION_FACTOR: u32 = 3;

// The fixture itself is the deliverable: without this proof every test below
// degrades silently into the all-nodes-hold-everything case.
#[tokio::test]
async fn fixture_proves_nonholders() -> TestResult<()> {
    let realm_id = RealmId([90u8; 32]);
    let realm = Topology::spawn(realm_id, NODE_COUNT, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([11; 16]);
    let document_id = Ulid::from_bytes([12; 16]);
    let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
    let context = PlacementResolutionContext {
        group_id: Some(group_id),
        metadata_path: Some("datasets/proof"),
    };

    let holders = realm.holders(&target, context);
    let non_holders = realm.non_holder_ids(&target, context);
    assert_eq!(holders.len(), REPLICATION_FACTOR as usize);
    assert_eq!(non_holders.len(), NODE_COUNT - REPLICATION_FACTOR as usize);
    for node_id in &non_holders {
        realm.assert_not_holder(*node_id, &target, context);
    }
    for node_id in &holders {
        realm.assert_holder(*node_id, &target, context);
    }

    // Placement is a pure function of the replicated realm config, so the proof
    // is exact rather than probabilistic: every node must derive the same set.
    for view in realm.holder_views(&target, context).await? {
        assert_eq!(view, holders, "holder set diverged across nodes");
    }

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn create_off_holders() -> TestResult<()> {
    let realm_id = RealmId([91u8; 32]);
    let realm = Topology::spawn(realm_id, NODE_COUNT, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([21; 16]);
    let document_id = Ulid::from_bytes([22; 16]);
    let document_path = "datasets/created-off-holders";
    let (target, context) = metadata_subject(group_id, document_id, document_path);

    let origin = realm.non_holder(&target, context);
    let holders = realm.assert_not_holder(origin.node_id(), &target, context);

    create_document(&realm, origin, group_id, document_id, document_path).await?;

    // The write must land on every real holder even though the node that
    // accepted it holds nothing for the document.
    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let view = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        origin.context.as_ref(),
    )
    .await?;
    let mut recorded = view.record.holder_node_ids.clone();
    recorded.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    assert_eq!(
        recorded, holders,
        "origin recorded a holder set that is not the planned placement"
    );
    assert!(
        !recorded.contains(&origin.node_id()),
        "non-holder origin recorded itself as a holder"
    );

    realm.shutdown().await;
    Ok(())
}

// A node that neither authored the document nor holds it has no copy and no
// forwarding path on main: the miss is definitive, not an unavailability error.
#[tokio::test]
async fn read_misses_nonholder() -> TestResult<()> {
    let realm_id = RealmId([92u8; 32]);
    let realm = Topology::spawn(realm_id, NODE_COUNT, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([31; 16]);
    let document_id = Ulid::from_bytes([32; 16]);
    let document_path = "datasets/read-off-holders";
    let (target, context) = metadata_subject(group_id, document_id, document_path);

    let holders = realm.holders(&target, context);
    let origin = realm.holder(&target, context);
    realm.assert_holder(origin.node_id(), &target, context);

    create_document(&realm, origin, group_id, document_id, document_path).await?;
    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let bystander = realm.non_holder(&target, context);
    let result = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        bystander.context.as_ref(),
    )
    .await;
    assert!(
        result.is_err(),
        "non-holder served a document it never received"
    );

    realm.shutdown().await;
    Ok(())
}

// #398's fail-loud invariant: a node that holds nothing for the document and
// did not author it must reject the mutation rather than accept a write it can
// never publish. Silent acceptance here is the data-loss shape.
#[tokio::test]
async fn bystander_writes_fail() -> TestResult<()> {
    let realm_id = RealmId([97u8; 32]);
    let realm = Topology::spawn(realm_id, NODE_COUNT, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([81; 16]);
    let document_id = Ulid::from_bytes([82; 16]);
    let document_path = "datasets/bystander-writes";
    let (target, context) = metadata_subject(group_id, document_id, document_path);

    let holders = realm.holders(&target, context);
    let origin = realm.holder(&target, context);
    create_document(&realm, origin, group_id, document_id, document_path).await?;
    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let bystander = realm.non_holder(&target, context);
    realm.assert_not_holder(bystander.node_id(), &target, context);
    let actor = realm.actor(
        bystander,
        UserId::local(Ulid::from_bytes([83; 16]), realm_id),
    );

    let updated = drive(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: actor.clone(),
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./ghost.txt","@type":"File","name":"ghost.txt"}"#.to_string(),
            },
        }),
        bystander.context.as_ref(),
    )
    .await;
    assert!(
        updated.is_err(),
        "non-holder accepted an update it cannot publish"
    );

    let deleted = drive(
        DeleteMetadataDocumentOperation::new(actor, group_id, document_id),
        bystander.context.as_ref(),
    )
    .await;
    assert!(
        deleted.is_err(),
        "non-holder accepted a delete it cannot publish"
    );

    // The rejected writes must not have disturbed the real holders.
    for holder in &holders {
        let node = realm.find(*holder);
        let view = drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            node.context.as_ref(),
        )
        .await?;
        assert!(!view.jsonld.contains("ghost.txt"));
    }

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn mutate_off_holders() -> TestResult<()> {
    let realm_id = RealmId([93u8; 32]);
    let realm = Topology::spawn(realm_id, NODE_COUNT, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([41; 16]);
    let document_id = Ulid::from_bytes([42; 16]);
    let document_path = "datasets/mutated-off-holders";
    let (target, context) = metadata_subject(group_id, document_id, document_path);

    let origin = realm.non_holder(&target, context);
    let holders = realm.assert_not_holder(origin.node_id(), &target, context);

    create_document(&realm, origin, group_id, document_id, document_path).await?;
    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    drive(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: realm.actor(origin, UserId::local(Ulid::from_bytes([43; 16]), realm_id)),
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./off-holder.txt","@type":"File","name":"off-holder.txt"}"#
                    .to_string(),
            },
        }),
        origin.context.as_ref(),
    )
    .await?;

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("update reaches holder", node.node_id(), || async {
            drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                node.context.as_ref(),
            )
            .await
            .is_ok_and(|view| view.jsonld.contains("off-holder.txt"))
        })
        .await?;
    }

    drive(
        DeleteMetadataDocumentOperation::new(
            realm.actor(origin, UserId::local(Ulid::from_bytes([44; 16]), realm_id)),
            group_id,
            document_id,
        ),
        origin.context.as_ref(),
    )
    .await?;

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("delete reaches holder", node.node_id(), || async {
            !document_present(node, group_id, document_id).await
        })
        .await?;
    }

    realm.shutdown().await;
    Ok(())
}

// Group documents are admin-class: they sync as reducer operations over the
// group topic and never take a placement. A node the resolver would call a
// non-holder must still accept the write and serve the group.
#[tokio::test]
async fn group_write_nonholder() -> TestResult<()> {
    let realm_id = RealmId([94u8; 32]);
    let realm = Topology::spawn(realm_id, NODE_COUNT, REPLICATION_FACTOR).await?;
    let owner = UserId::local(Ulid::from_bytes([51; 16]), realm_id);
    seed_realm_authorization(&realm, owner).await?;

    let (group, _) = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: realm.actor(realm.node(0), owner),
            display_name: "off-holder group".to_string(),
            owner_cap: None,
        }),
        realm.node(0).context.as_ref(),
    )
    .await?;
    let group_id = group.group_id;

    let target = DocumentSyncTarget::GroupAuthorization { group_id };
    let context = PlacementResolutionContext {
        group_id: Some(group_id),
        metadata_path: None,
    };
    let writer = realm.non_holder(&target, context);
    realm.assert_not_holder(writer.node_id(), &target, context);

    // Presence of the document is not enough: the reducer must also have applied
    // the owner's admin assignment before the write is authorized.
    for node in &realm.nodes {
        wait_until("group reaches node", node.node_id(), || async {
            drive(
                GetGroupOperation::new(GetGroupConfig { group_id }),
                node.context.as_ref(),
            )
            .await
            .is_ok_and(|(_, auth)| {
                auth.roles
                    .values()
                    .any(|role| role.name == "admin" && role.assigned_users.contains(&owner))
            })
        })
        .await?;
    }

    let role_id = Ulid::from_bytes([52; 16]);
    drive(
        AddGroupRoleOperation::new(AddGroupRoleConfig {
            auth_context: AuthContext {
                user_id: owner,
                realm_id,
                path_restrictions: None,
            },
            actor: realm.actor(writer, owner),
            realm_id,
            group_id,
            role: Role {
                role_id,
                name: "auditor".to_string(),
                permissions: HashMap::from([(
                    format!("/{realm_id}/g/{group_id}/**"),
                    Permission::READ,
                )]),
                assigned_users: HashSet::new(),
            },
        }),
        writer.context.as_ref(),
    )
    .await?;

    // The regression this guards: a group write accepted on a node that holds
    // nothing for the group and then never published anywhere.
    for node in &realm.nodes {
        wait_until("role reaches node", node.node_id(), || async {
            drive(
                GetGroupOperation::new(GetGroupConfig { group_id }),
                node.context.as_ref(),
            )
            .await
            .is_ok_and(|(_, auth)| auth.roles.values().any(|role| role.name == "auditor"))
        })
        .await?;
    }

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn user_write_nonholder() -> TestResult<()> {
    let realm_id = RealmId([95u8; 32]);
    let realm = Topology::spawn(realm_id, NODE_COUNT, REPLICATION_FACTOR).await?;

    let user_id = UserId::local(Ulid::from_bytes([61; 16]), realm_id);
    let target = DocumentSyncTarget::User { user_id };
    let context = PlacementResolutionContext::default();

    let origin = realm.non_holder(&target, context);
    realm.assert_not_holder(origin.node_id(), &target, context);

    drive(
        RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: realm.actor(origin, user_id),
            issuer: "https://issuer.test".to_string(),
            subject_id: "off-holder-subject".to_string(),
            name: "Off Holder".to_string(),
            user_id,
        }),
        origin.context.as_ref(),
    )
    .await?;

    for node in &realm.nodes {
        wait_until("user reaches node", node.node_id(), || async {
            drive(
                ReadUserDocumentOperation::new(user_id),
                node.context.as_ref(),
            )
            .await
            .is_ok_and(|user| user.name == "Off Holder")
        })
        .await?;
    }

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn permission_check_nonholder() -> TestResult<()> {
    let realm_id = RealmId([96u8; 32]);
    let realm = Topology::spawn(realm_id, NODE_COUNT, REPLICATION_FACTOR).await?;
    let owner = UserId::local(Ulid::from_bytes([71; 16]), realm_id);
    seed_realm_authorization(&realm, owner).await?;

    let (group, _) = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: realm.actor(realm.node(0), owner),
            display_name: "authorized group".to_string(),
            owner_cap: None,
        }),
        realm.node(0).context.as_ref(),
    )
    .await?;
    let group_id = group.group_id;

    let target = DocumentSyncTarget::GroupAuthorization { group_id };
    let context = PlacementResolutionContext {
        group_id: Some(group_id),
        metadata_path: None,
    };
    let non_holders = realm.non_holder_ids(&target, context);
    assert!(!non_holders.is_empty());

    let path = format!("/{realm_id}/g/{group_id}/meta/document");
    for node_id in non_holders {
        let node = realm.find(node_id);
        realm.assert_not_holder(node_id, &target, context);
        wait_until("group auth reaches node", node_id, || {
            let path = path.clone();
            async move {
                drive(
                    CheckPermissionsOperation::new(CheckPermissionsConfig {
                        auth_context: AuthContext {
                            user_id: owner,
                            realm_id,
                            path_restrictions: None,
                        },
                        path,
                        required_permission: Permission::READ,
                    }),
                    node.context.as_ref(),
                )
                .await
                .is_ok_and(|granted| granted)
            }
        })
        .await?;
    }

    realm.shutdown().await;
    Ok(())
}

fn metadata_subject(
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> (DocumentSyncTarget, PlacementResolutionContext<'_>) {
    (
        DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
        PlacementResolutionContext {
            group_id: Some(group_id),
            metadata_path: Some(document_path),
        },
    )
}

async fn create_document(
    realm: &Topology,
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> TestResult<()> {
    drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: realm.actor(
                node,
                UserId::local(Ulid::from_bytes([9; 16]), realm.realm_id),
            ),
            group_id,
            document_id,
            document_path: document_path.to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Non-holder Dataset".to_string(),
                description: "Written on a node outside the holder set".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        node.context.as_ref(),
    )
    .await?;
    replay_metadata_event_log(node.context.as_ref()).await?;
    Ok(())
}

async fn document_present(node: &TestNode, group_id: Ulid, document_id: Ulid) -> bool {
    drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        node.context.as_ref(),
    )
    .await
    .is_ok()
}

async fn seed_realm_authorization(realm: &Topology, owner: UserId) -> TestResult<()> {
    let document = RealmAuthorizationDocument::new_default_realm_doc(realm.realm_id);
    for node in &realm.nodes {
        let actor = Actor {
            node_id: node.node_id(),
            user_id: owner,
            realm_id: realm.realm_id,
        };
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: realm.realm_id.as_bytes().to_vec().into(),
                value: document.to_bytes(&actor)?.into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => return Err(format!("unexpected realm auth write event: {other:?}").into()),
        }
    }
    Ok(())
}
