//! Creation of an immutable placement-policy document on one of its holders.

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, PolicySignEvent, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{document_sync_revision_write_entry, shard_manifest_write_entry};
use aruna_core::structs::{
    Actor, AuthContext, Permission, PlacementPolicy, PlacementPolicyDocument, PlacementPolicyError,
    PlacementRef, PolicyAuthorityError, PolicyPublication, PolicyPublicationClaim,
    RealmConfigDocument, VerifiedPolicy, placement_policy_change, placement_policy_target,
    policy_admin_path,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::{PlacementResolveError, fence, holds_placement, plan_target_placement};

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
/// resolves to. The document's holders are a replication fact only: nothing
/// here consults the policy's own selectors, which govern data residency.
#[derive(Debug, PartialEq)]
pub struct CreatePolicyOperation {
    config: CreatePolicyConfig,
    txn_id: Option<TxnId>,
    state: CreatePolicyState,
    output: Option<Result<PlacementPolicyDocument, CreatePolicyError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CreatePolicyState {
    Init,
    Authorize,
    StartTransaction,
    ReadConfig,
    ReadFence { pending: Box<PendingPublication> },
    Sign { pending: Box<PendingPublication> },
    Write,
    Commit,
    ScheduleDrain,
    Finish,
    Error,
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
    #[error("caller may not administer the realm configuration")]
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
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl CreatePolicyOperation {
    pub fn new(config: CreatePolicyConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: CreatePolicyState::Init,
            output: None,
        }
    }

    fn policy_id(&self) -> Ulid {
        self.config.policy.policy_id
    }

    fn target(&self) -> DocumentSyncTarget {
        placement_policy_target(self.policy_id())
    }

    fn emit_read_config(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = CreatePolicyState::ReadConfig;
        let config_target = DocumentSyncTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        };
        let target = self.target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    config_target.storage_keyspace().to_string(),
                    config_target.storage_key(),
                ),
                (target.storage_keyspace().to_string(), target.storage_key()),
            ],
            txn_id: Some(txn_id),
        })]
    }

    /// Plans the document's bucket and either finishes an idempotent replay or
    /// moves on to the write fence.
    fn plan_write(
        &mut self,
        config_value: Option<Value>,
        policy_value: Option<Value>,
    ) -> Result<Effects, CreatePolicyError> {
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
            self.output = Some(Ok(existing));
            return Ok(self.emit_commit());
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
            return Ok(self.emit_sign(pending));
        }
        let (key_space, key) = fence::fence_read(&self.config.actor.realm_id, &plan.placement);
        let txn_id = self.txn_id.ok_or(CreatePolicyError::MissingTransaction)?;
        self.state = CreatePolicyState::ReadFence { pending };
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space,
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_sign(&mut self, pending: Box<PendingPublication>) -> Effects {
        let claim = pending.claim;
        self.state = CreatePolicyState::Sign { pending };
        smallvec![Effect::Net(NetEffect::PolicySign(Box::new(claim)))]
    }

    /// Binds the signed publication to the planned definition before it is
    /// written, so a signature that does not authenticate never leaves the node.
    fn seal(
        &mut self,
        pending: PendingPublication,
        publication: PolicyPublication,
    ) -> Result<Effects, CreatePolicyError> {
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
    ) -> Result<Effects, CreatePolicyError> {
        let txn_id = self.txn_id.ok_or(CreatePolicyError::MissingTransaction)?;
        let target = self.target();
        let change = placement_policy_change(&document, placement);
        let bytes = document.to_bytes()?;
        let mut writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.clone().into(),
        )];
        writes.push(document_sync_revision_write_entry(&target, &change)?);
        if let Some(entry) = shard_manifest_write_entry(&target, &change)? {
            writes.push(entry);
        }
        let record = new_outbox_record(
            self.config.actor.node_id,
            target,
            holders,
            DocumentSyncOutboxEvent::Upsert { bytes, change },
            placement,
            // Shard topics are join-only: the bucket's rank-0 holder mints the
            // genesis, so no publisher of a policy document claims it.
            false,
        )
        .fenced_at(generation);
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);

        self.output = Some(Ok(document));
        self.state = CreatePolicyState::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(CreatePolicyError::MissingTransaction);
        };
        self.state = CreatePolicyState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: CreatePolicyError) -> Effects {
        let cleanup = self.abort();
        self.state = CreatePolicyState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(CreatePolicyError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for CreatePolicyOperation {
    type Output = PlacementPolicyDocument;
    type Error = CreatePolicyError;

    fn start(&mut self) -> Effects {
        if let Err(error) = VerifiedPolicy::verify(self.config.policy.clone()) {
            return self.fail(error.into());
        }
        self.state = CreatePolicyState::Authorize;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path: policy_admin_path(self.config.actor.realm_id),
            required_permission: Permission::WRITE,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(auth_config),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            CreatePolicyState::Authorize => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = CreatePolicyState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(CreatePolicyError::Unauthorized),
                        Err(error) => {
                            warn!(error = %error, "Placement policy authorization check failed");
                            self.fail(CreatePolicyError::Unauthorized)
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            CreatePolicyState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_config(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            CreatePolicyState::ReadConfig => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, config_value), (_, policy_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "realm config and policy row",
                            format!("{values:?}"),
                        );
                    };
                    match self.plan_write(config_value.clone(), policy_value.clone()) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            CreatePolicyState::ReadFence { pending } => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    if !fence::admits(value.as_ref(), pending.generation) {
                        return self.fail(CreatePolicyError::PlacementFenced);
                    }
                    self.emit_sign(pending)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("placement fence read", format!("{other:?}")),
            },
            CreatePolicyState::Sign { pending } => match event {
                Event::Net(NetEvent::PolicySign(PolicySignEvent::Signed(publication))) => {
                    match self.seal(*pending, *publication) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Net(NetEvent::PolicySign(PolicySignEvent::Unavailable(reason))) => {
                    self.fail(CreatePolicyError::PublicationUnavailable(reason))
                }
                other => {
                    self.unexpected_event("policy publication signature", format!("{other:?}"))
                }
            },
            CreatePolicyState::Write => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.emit_commit(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            CreatePolicyState::Commit => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = CreatePolicyState::ScheduleDrain;
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            CreatePolicyState::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = CreatePolicyState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule policy outbox drain; durable outbox remains retryable");
                    self.state = CreatePolicyState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event("outbox drain schedule", format!("{other:?}")),
            },
            CreatePolicyState::Init | CreatePolicyState::Finish | CreatePolicyState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreatePolicyState::Finish | CreatePolicyState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(CreatePolicyError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use crate::placement_policy::read::{PolicySource, ReadPolicyConfig, ReadPolicyOperation};
    use aruna_core::structs::{PlacementSelector, RealmId};
    use aruna_core::types::UserId;
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
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
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
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
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
