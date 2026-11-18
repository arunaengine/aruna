//! Creates an immutable placement policy document on the holders its policy id resolves to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, PolicySignEvent};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::done;
use aruna_core::storage_entries::{shard_manifest_entry, sync_revision_entry};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, PolicyAuthorityError, PolicyPublication, PolicyPublicationClaim,
    placement_policy_change, placement_policy_target, policy_authority_path,
};
use aruna_core::structs::placement::policy::{
    PlacementPolicy, PlacementPolicyError, VerifiedPolicy,
};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::Value;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::sync::outbox_record::{new_outbox_record, outbox_write_entry, schedule_drain_effect};
use aruna_core::placement::{PlacementResolveError, fence, holds_placement, plan_target_placement};

#[derive(Debug, Clone, PartialEq)]
pub struct CreatePolicyConfig {
    pub actor: Actor,
    pub auth_context: AuthContext,
    /// Definition and its immutable id. Republishing the same id with the same
    /// bytes is a no-op; different bytes are refused.
    pub policy: PlacementPolicy,
    pub created_at_ms: u64,
}

/// Publishes one immutable policy document to the holders its policy id
/// resolves to. The document's holders are a replication detail only: nothing
/// here consults the policy's own selectors, which govern data residency.
#[derive(Debug, PartialEq)]
pub struct CreatePolicyOperation {
    config: CreatePolicyConfig,
    authorized: bool,
}

/// Everything the write needs once this node's key has signed the publication.
#[derive(Debug, Clone, PartialEq)]
struct PendingPublication {
    policy: VerifiedPolicy,
    claim: PolicyPublicationClaim,
    placement: PlacementRef,
    holders: Vec<NodeId>,
    generation: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreatePolicyError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Authority(#[from] PolicyAuthorityError),
    /// This node could not sign the publication, so nothing is published: an
    /// unsigned policy would carry no provenance a fetcher could verify.
    #[error("policy publication could not be signed: {0}")]
    PublicationUnavailable(String),
    /// Realm-wide rules need realm-configuration write; a group-owned rule
    /// needs admin write on the group it names.
    #[error("caller may not publish a rule under this ownership")]
    Unauthorized,
    #[error("realm config document missing")]
    RealmConfigMissing,
    #[error("no placement strategy governs policy documents")]
    PlacementUnavailable,
    #[error(transparent)]
    PlacementResolve(#[from] PlacementResolveError),
    /// This node holds no replica of the policy's bucket, so it may not commit
    /// the document; the caller forwards to one of `holders`.
    #[error("node holds no replica of the policy bucket")]
    NotHolder { holders: Vec<NodeId> },
    #[error("policy bucket cut over mid-write; retry the create")]
    PlacementFenced,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for CreatePolicyError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "CreatePolicy".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

type PolicyTurn = Result<Next<PlacementPolicyDocument>, CreatePolicyError>;

impl CreatePolicyOperation {
    pub fn new(config: CreatePolicyConfig) -> Self {
        Self {
            config,
            authorized: false,
        }
    }
}

impl Operation for CreatePolicyOperation {
    type Output = PlacementPolicyDocument;
    type Error = CreatePolicyError;

    fn first(&mut self) -> PolicyTurn {
        VerifiedPolicy::verify(self.config.policy.clone())?;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path: policy_authority_path(
                self.config.actor.realm_id,
                self.config.policy.owner_group_id,
            ),
            required_permission: Permission::WRITE,
        };
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            auth_config,
        ))))
    }

    fn next(&mut self, result: IoResult) -> PolicyTurn {
        if self.authorized {
            return result
                .child::<PlacementPolicyDocument, CreatePolicyError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                let write = PolicyWrite::new(self.config.clone());
                Ok(Next::Call(IoCall::child(write)))
            }
            Ok(false) => Err(CreatePolicyError::Unauthorized),
            Err(error) => {
                warn!(error = %error, "Placement policy authorization check failed");
                Err(CreatePolicyError::Unauthorized)
            }
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            CreatePolicyError::Unauthorized
                | CreatePolicyError::NotHolder { .. }
                | CreatePolicyError::Policy(_)
        )
    }
}

/// What the policy write transaction waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Read,
    Fence,
    Sign,
    Write,
}

/// Plans, fences, signs and writes the policy document in one write transaction.
#[derive(Debug, PartialEq)]
struct PolicyWrite {
    config: CreatePolicyConfig,
    stage: Stage,
    pending: Option<Box<PendingPublication>>,
    document: Option<PlacementPolicyDocument>,
}

impl PolicyWrite {
    fn new(config: CreatePolicyConfig) -> Self {
        Self {
            config,
            stage: Stage::Read,
            pending: None,
            document: None,
        }
    }

    fn policy_id(&self) -> Ulid {
        self.config.policy.policy_id
    }

    fn target(&self) -> DocumentTarget {
        placement_policy_target(self.policy_id())
    }

    /// Plans the document's bucket and either finishes an idempotent replay or
    /// moves on to the write fence.
    fn plan_write(
        &mut self,
        config_value: Option<Value>,
        policy_value: Option<Value>,
    ) -> PolicyTurn {
        let Some(config_value) = config_value else {
            return Err(CreatePolicyError::RealmConfigMissing);
        };
        let config = RealmConfigDocument::from_bytes(&config_value)?;
        let verified = VerifiedPolicy::verify(self.config.policy.clone())?;

        if let Some(policy_value) = policy_value {
            let existing = PlacementPolicyDocument::from_bytes(&policy_value)?;
            if existing.policy_ref()? != verified.policy_ref() {
                return Err(PlacementPolicyError::PolicyIdReuse {
                    policy_id: self.policy_id(),
                }
                .into());
            }
            return Ok(Self::committed(existing));
        }

        let target = self.target();
        let plan = plan_target_placement(&config, &target, Default::default())?
            .ok_or(CreatePolicyError::PlacementUnavailable)?;
        if !holds_placement(&config, &plan.placement, self.config.actor.node_id) {
            return Err(CreatePolicyError::NotHolder {
                holders: plan.holders,
            });
        }
        // The signed claim names the config the realm-admin check ran against,
        // so the publication epoch stays auditable after membership changes.
        let claim = PolicyPublicationClaim::new(
            self.config.actor.realm_id,
            &verified,
            self.config.actor.node_id,
            self.config.auth_context.user_id,
            Ulid::generate(),
            self.config.created_at_ms,
            config.digest()?,
        );
        let generation = fence::write_generation(&config, &plan.placement).unwrap_or_default();
        let pending = Box::new(PendingPublication {
            policy: verified,
            claim,
            placement: plan.placement,
            holders: plan.holders,
            generation,
        });
        if generation == 0 {
            return Ok(self.sign(pending));
        }
        let (key_space, key) = fence::fence_read(&self.config.actor.realm_id, &plan.placement);
        self.stage = Stage::Fence;
        self.pending = Some(pending);
        Ok(Next::Call(IoCall::Read(vec![(key_space, key)])))
    }

    fn sign(&mut self, pending: Box<PendingPublication>) -> Next<PlacementPolicyDocument> {
        let claim = Box::new(pending.claim.clone());
        self.stage = Stage::Sign;
        self.pending = Some(pending);
        let sign = Effect::Net(NetEffect::PolicySign(claim));
        Next::Call(IoCall::Effect(Box::new(sign)))
    }

    fn pending(&mut self) -> Result<Box<PendingPublication>, CreatePolicyError> {
        self.pending.take().ok_or(CreatePolicyError::NotFinished)
    }

    /// Binds the signed publication to the planned definition before it is
    /// written, so a signature that does not authenticate never leaves the node.
    fn bind_publication(
        &mut self,
        pending: PendingPublication,
        publication: PolicyPublication,
    ) -> PolicyTurn {
        let document =
            PlacementPolicyDocument::new(self.config.actor.realm_id, &pending.policy, publication);
        document.verify_publication()?;
        self.emit_write(
            document,
            pending.placement,
            pending.holders,
            pending.generation,
        )
    }

    /// Row, sync sidecar, shard-manifest entry, and outbox publish in one
    /// transaction, so an accepted policy is either durable and replicated or
    /// not created at all.
    fn emit_write(
        &mut self,
        document: PlacementPolicyDocument,
        placement: PlacementRef,
        holders: Vec<NodeId>,
        generation: u64,
    ) -> PolicyTurn {
        let target = self.target();
        let change = placement_policy_change(&document, placement);
        let bytes = document.to_bytes()?;
        let mut writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.clone().into(),
        )];
        writes.push(sync_revision_entry(&target, &change)?);
        if let Some(entry) = shard_manifest_entry(&target, &change)? {
            writes.push(entry);
        }
        let record = new_outbox_record(
            self.config.actor.node_id,
            target,
            holders,
            DocumentOutboxEvent::Upsert { bytes, change },
            placement,
            // Shard topics are join-only: the bucket's rank-0 holder mints the
            // genesis, so no publisher of a policy document claims it.
            false,
        )
        .fenced_at(generation);
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);

        self.document = Some(document);
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn committed(document: PlacementPolicyDocument) -> Next<PlacementPolicyDocument> {
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Next::Then {
            output: document,
            calls,
        }
    }

    /// Checks the group deletion fence an owned rule reads, then plans the write.
    fn read(&mut self, values: Vec<Option<Value>>) -> PolicyTurn {
        let [config_value, policy_value, tail @ ..] = values.as_slice() else {
            return Err(CreatePolicyError::UnexpectedEvent {
                state: "Read".to_string(),
                expected: "realm config and policy row",
                got: "a batch read with other rows",
            });
        };
        match (self.config.policy.owner_group_id, tail) {
            (Some(_), [value]) => {
                aruna_core::structs::identity::group_delete::check_group_write(value.as_deref())?;
            }
            (None, []) => {}
            _ => {
                return Err(CreatePolicyError::UnexpectedEvent {
                    state: "Read".to_string(),
                    expected: "group deletion fence",
                    got: "a batch read with other rows",
                });
            }
        }
        self.plan_write(config_value.clone(), policy_value.clone())
    }
}

impl Operation for PolicyWrite {
    type Output = PlacementPolicyDocument;
    type Error = CreatePolicyError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> PolicyTurn {
        let config_target = DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        };
        let target = self.target();
        let mut reads = vec![
            (
                config_target.storage_keyspace().to_string(),
                config_target.storage_key(),
            ),
            (target.storage_keyspace().to_string(), target.storage_key()),
        ];
        if let Some(group_id) = self.config.policy.owner_group_id {
            reads.push(crate::groups::fence::group_fence_key(group_id));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> PolicyTurn {
        match self.stage {
            Stage::Read => {
                let IoResult::Values(values) = result else {
                    return Err(IoError::Unexpected {
                        expected: "IoResult::Values",
                        got: result.label(),
                    }
                    .into());
                };
                self.read(values)
            }
            Stage::Fence => {
                let [value] = aruna_core::rows::values::<1>(result)?;
                let pending = self.pending()?;
                if !fence::admits(value.as_ref(), pending.generation) {
                    return Err(CreatePolicyError::PlacementFenced);
                }
                Ok(self.sign(pending))
            }
            Stage::Sign => {
                let pending = self.pending()?;
                let IoResult::Event(event) = result else {
                    return Err(IoError::Unexpected {
                        expected: "the policy publication signature",
                        got: result.label(),
                    }
                    .into());
                };
                match *event {
                    Event::Net(NetEvent::PolicySign(PolicySignEvent::Signed(publication))) => {
                        self.bind_publication(*pending, *publication)
                    }
                    Event::Net(NetEvent::PolicySign(PolicySignEvent::Unavailable(reason))) => {
                        Err(CreatePolicyError::PublicationUnavailable(reason))
                    }
                    other => Err(CreatePolicyError::UnexpectedEvent {
                        state: "Sign".to_string(),
                        expected: "policy publication signature",
                        got: other.label(),
                    }),
                }
            }
            Stage::Write => {
                done(result)?;
                let document = self.document.take().ok_or(CreatePolicyError::NotFinished)?;
                Ok(Self::committed(document))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::placement::policy::read::{PolicySource, ReadPolicyConfig, ReadPolicyOperation};
    use crate::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::handle::Handle;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::PlacementSelector;
    use aruna_core::structs::placement::policy::document::verify_policy_authority;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;

    fn selector(location: &str) -> PlacementSelector {
        PlacementSelector {
            node_id: None,
            location: Some(location.to_string()),
            labels: Vec::new(),
            executor_kind: None,
        }
    }

    fn policy(location: &str) -> PlacementPolicy {
        PlacementPolicy::new(
            Ulid::from_bytes([8u8; 16]),
            "residency".to_string(),
            vec![selector(location)],
        )
        .expect("policy is valid")
    }

    fn config(actor: &Actor, policy: PlacementPolicy) -> CreatePolicyConfig {
        CreatePolicyConfig {
            actor: actor.clone(),
            auth_context: AuthContext {
                user_id: actor.user_id,
                realm_id: actor.realm_id,
                path_restrictions: None,
                session: None,
            },
            policy,
            created_at_ms: 1_700_000_000_000,
        }
    }

    /// A real net handle is part of the fixture: the publication is signed with
    /// this node's key, so a policy cannot be created without one.
    async fn setup() -> (tempfile::TempDir, DriverContext, Actor) {
        let dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(dir.path().to_str().expect("path")).expect("storage");
        let secret = iroh::SecretKey::from_bytes(&[3u8; 32]);
        let realm_id = RealmId([21u8; 32]);
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                secret_key: Some(secret.clone()),
                realm_id,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let context = DriverContext {
            storage_handle: storage,
            net_handle: Some(net_handle),
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        let actor = Actor {
            node_id: secret.public(),
            user_id: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
            realm_id,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "policy realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &context,
        )
        .await
        .expect("realm is created");
        drive(
            ClaimInitialOperation::new(ClaimInitialInput {
                actor: actor.clone(),
            }),
            &context,
        )
        .await
        .expect("admin is claimed");
        (dir, context, actor)
    }

    #[tokio::test]
    async fn stores_policy_document() {
        // The created document is readable on its holder through the ordinary
        // read path, without any catalog entry.
        let (_dir, context, actor) = setup().await;
        let document = drive(
            CreatePolicyOperation::new(config(&actor, policy("eu-west"))),
            &context,
        )
        .await
        .expect("policy is created");
        let policy_ref = document.policy_ref().expect("document verifies");

        let (read, source) = drive(
            ReadPolicyOperation::new(ReadPolicyConfig {
                realm_id: actor.realm_id,
                policy_ref,
                local_node_id: actor.node_id,
            }),
            &context,
        )
        .await
        .expect("policy resolves");
        assert_eq!(read.policy.policy_ref(), policy_ref);
        assert_eq!(source, PolicySource::Local);
    }

    #[tokio::test]
    async fn publishes_admin_authority() {
        // What creation emits is exactly what an independent verifier accepts:
        // the publication names this node and the realm admin who authorized it.
        let (_dir, context, actor) = setup().await;
        let document = drive(
            CreatePolicyOperation::new(config(&actor, policy("eu-west"))),
            &context,
        )
        .await
        .expect("policy is created");

        assert_eq!(document.publication.publisher, actor.node_id);
        assert_eq!(document.publication.created_by, actor.user_id);
        let realm_config = read_document(
            &context,
            DocumentTarget::RealmConfig {
                realm_id: actor.realm_id,
            },
        )
        .await;
        let realm_auth = read_document(
            &context,
            DocumentTarget::RealmAuthorization {
                realm_id: actor.realm_id,
            },
        )
        .await;
        assert_eq!(
            verify_policy_authority(
                &document,
                &RealmConfigDocument::from_bytes(&realm_config).expect("config decodes"),
                &aruna_core::structs::identity::realm::RealmAuthorizationDocument::from_bytes(
                    &realm_auth
                )
                .expect("authorization decodes"),
                None,
            ),
            Ok(())
        );
    }

    async fn read_document(context: &DriverContext, target: DocumentTarget) -> Value {
        let event = context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                txn_id: None,
            }))
            .await;
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.expect("row exists"),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn replay_is_idempotent() {
        // The same id with the same bytes is a no-op, not a conflict.
        let (_dir, context, actor) = setup().await;
        let first = drive(
            CreatePolicyOperation::new(config(&actor, policy("eu-west"))),
            &context,
        )
        .await
        .expect("policy is created");
        let second = drive(
            CreatePolicyOperation::new(config(&actor, policy("eu-west"))),
            &context,
        )
        .await
        .expect("replay is accepted");
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn rejects_id_reuse() {
        // A changed definition must mint a new id; reusing this one is refused
        // and leaves the stored rule untouched.
        let (_dir, context, actor) = setup().await;
        let created = drive(
            CreatePolicyOperation::new(config(&actor, policy("eu-west"))),
            &context,
        )
        .await
        .expect("policy is created");
        let reused = drive(
            CreatePolicyOperation::new(config(&actor, policy("us-east"))),
            &context,
        )
        .await;
        assert_eq!(
            reused,
            Err(CreatePolicyError::Policy(
                PlacementPolicyError::PolicyIdReuse {
                    policy_id: Ulid::from_bytes([8u8; 16])
                }
            ))
        );

        let (read, _) = drive(
            ReadPolicyOperation::new(ReadPolicyConfig {
                realm_id: actor.realm_id,
                policy_ref: created.policy_ref().expect("document verifies"),
                local_node_id: actor.node_id,
            }),
            &context,
        )
        .await
        .expect("policy resolves");
        assert_eq!(read.policy.policy(), &created.policy);
    }

    /// A realm role granting the admin user write on one group's admin path,
    /// so a group-owned publication has an authority to check against.
    async fn seed_group_admin(context: &DriverContext, actor: &Actor, group_id: Ulid) {
        let target = DocumentTarget::GroupAuthorization { group_id };
        let document = aruna_core::structs::identity::group::GroupAuthorizationDocument {
            group_id,
            roles: std::collections::HashMap::from([(
                Ulid::from_bytes([4u8; 16]),
                aruna_core::structs::identity::auth::Role {
                    role_id: Ulid::from_bytes([4u8; 16]),
                    name: "group_admin".to_string(),
                    permissions: std::collections::HashMap::from([(
                        format!("/{}/g/{group_id}/**", actor.realm_id),
                        Permission::WRITE,
                    )]),
                    assigned_users: std::collections::HashSet::from([actor.user_id]),
                },
            )]),
            policies: Vec::new(),
        };
        let _ = context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document.to_bytes(actor).expect("document encodes").into(),
                txn_id: None,
            }))
            .await;
    }

    #[tokio::test]
    async fn publishes_group_owner() {
        // A group administrator publishes a rule its own group owns, and an
        // independent verifier accepts it against that group's roles.
        let (_dir, context, actor) = setup().await;
        let group_id = Ulid::from_bytes([7u8; 16]);
        seed_group_admin(&context, &actor, group_id).await;
        let owned = policy("eu-west")
            .owned_by(group_id)
            .expect("owner is valid");

        let document = drive(CreatePolicyOperation::new(config(&actor, owned)), &context)
            .await
            .expect("policy is created");

        assert_eq!(document.policy.owner_group_id, Some(group_id));
        let realm_config = read_document(
            &context,
            DocumentTarget::RealmConfig {
                realm_id: actor.realm_id,
            },
        )
        .await;
        let realm_auth = read_document(
            &context,
            DocumentTarget::RealmAuthorization {
                realm_id: actor.realm_id,
            },
        )
        .await;
        let group_auth =
            read_document(&context, DocumentTarget::GroupAuthorization { group_id }).await;
        let group_auth =
            aruna_core::structs::identity::group::GroupAuthorizationDocument::from_bytes(
                &group_auth,
            )
            .expect("group authorization decodes");
        assert_eq!(
            verify_policy_authority(
                &document,
                &RealmConfigDocument::from_bytes(&realm_config).expect("config decodes"),
                &aruna_core::structs::identity::realm::RealmAuthorizationDocument::from_bytes(
                    &realm_auth
                )
                .expect("authorization decodes"),
                Some(&group_auth),
            ),
            Ok(())
        );
    }

    #[tokio::test]
    async fn denies_foreign_group() {
        // Administering one group is no authority over another group's rules.
        let (_dir, context, actor) = setup().await;
        seed_group_admin(&context, &actor, Ulid::from_bytes([7u8; 16])).await;
        let owned = policy("eu-west")
            .owned_by(Ulid::from_bytes([8u8; 16]))
            .expect("owner is valid");

        let denied = drive(CreatePolicyOperation::new(config(&actor, owned)), &context).await;

        assert_eq!(denied, Err(CreatePolicyError::Unauthorized));
    }

    #[tokio::test]
    async fn denies_non_admin() {
        // Publishing a residency rule is a realm-configuration change.
        let (_dir, context, actor) = setup().await;
        let outsider = Actor {
            user_id: UserId::local(Ulid::from_bytes([6u8; 16]), actor.realm_id),
            ..actor.clone()
        };
        let denied = drive(
            CreatePolicyOperation::new(config(&outsider, policy("eu-west"))),
            &context,
        )
        .await;
        assert_eq!(denied, Err(CreatePolicyError::Unauthorized));
    }
}
