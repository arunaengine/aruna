//! Creates a group with its default roles, its authorization document and its owner index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::admin_documents::{
    AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, OWNER_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{conflict_write_entries, reducer_state_entry};
use aruna_core::structs::identity::auth::{Actor, Role};
use aruna_core::structs::identity::group::{
    Group, GroupAuthorizationDocument, owner_group_key, owner_group_prefix,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::dataset_location::DatasetLocation;
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use thiserror::Error;
use tracing::trace;
use ulid::Ulid;

use super::CONFLICT_RETRIES;
use crate::groups::update_group::{MAX_NAME_LEN, normalize_group_name};
use aruna_core::placement::target_placement_ref;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateGroupConfig {
    pub actor: Actor,
    pub display_name: String,
    /// Maximum number of groups the actor may own; checked inside the write
    /// transaction so concurrent creates cannot slip past. None = unlimited
    /// (realm admins are exempt).
    pub owner_cap: Option<u32>,
}

/// The created group and its authorization document.
pub type CreatedGroup = (Group, GroupAuthorizationDocument);

type CreateTurn = Result<Next<CreatedGroup>, CreateGroupError>;

/// Checks the name, then creates the group in a write transaction that reruns after a lost
/// conflict with a fresh group id.
#[derive(Debug, PartialEq)]
pub struct CreateGroupOperation {
    config: CreateGroupConfig,
}

impl CreateGroupOperation {
    pub fn new(config: CreateGroupConfig) -> Self {
        CreateGroupOperation { config }
    }
}

impl Operation for CreateGroupOperation {
    type Output = CreatedGroup;
    type Error = CreateGroupError;

    fn first(&mut self) -> CreateTurn {
        let trimmed = normalize_group_name(&self.config.display_name)
            .ok_or(CreateGroupError::InvalidDisplayName)?;
        self.config.display_name = trimmed;
        let config = self.config.clone();
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || GroupCreate::new(config.clone());
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> CreateTurn {
        result
            .child::<CreatedGroup, CreateGroupError>()?
            .map(Next::Done)
    }
}

/// What the create transaction waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Config,
    CountOwned,
    Write,
    Fence,
}

/// Reads the realm config and the owner's groups against the cap, then writes the group, its
/// owner index, its authorization document and the seeded admin events, and takes the bucket
/// fence, in one write transaction.
#[derive(Debug, PartialEq)]
struct GroupCreate {
    config: CreateGroupConfig,
    group: Option<Group>,
    auth_doc: Option<GroupAuthorizationDocument>,
    realm_config: Option<RealmConfigDocument>,
    /// Bucket the group's authorization rows publish onto, read inside the write transaction.
    fence: aruna_core::placement::fence::WriteFence,
    stage: Stage,
}

impl GroupCreate {
    fn new(config: CreateGroupConfig) -> Self {
        Self {
            config,
            group: None,
            auth_doc: None,
            realm_config: None,
            fence: Default::default(),
            stage: Stage::Config,
        }
    }

    fn handle_config(&mut self, value: Option<Value>) -> CreateTurn {
        self.realm_config = value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?;
        match self.config.owner_cap {
            Some(0) => Err(CreateGroupError::GroupLimitReached { limit: 0 }),
            Some(cap) => {
                self.stage = Stage::CountOwned;
                Ok(Next::Call(IoCall::Page {
                    space: OWNER_INDEX_KEYSPACE.to_string(),
                    prefix: owner_group_prefix(self.config.actor.user_id).into(),
                    after: None,
                    limit: cap as usize,
                }))
            }
            None => self.write(),
        }
    }

    /// A new group with its default roles, owned by the actor.
    fn prepare(&mut self) -> Group {
        let group_id = Ulid::generate();
        let auth_doc = GroupAuthorizationDocument::default_group_doc(
            self.config.actor.user_id,
            self.config.actor.realm_id,
            group_id,
        );
        let group = Group {
            roles: auth_doc.roles.keys().copied().collect(),
            display_name: self.config.display_name.clone(),
            group_id,
            realm_id: self.config.actor.realm_id,
            owner: self.config.actor.user_id,
        };
        self.auth_doc = Some(auth_doc);
        self.group = Some(group.clone());
        group
    }

    /// The group row, its owner index entry, its authorization document and the seeded admin
    /// state and events.
    fn writes(&mut self) -> Result<Vec<(String, Key, Value)>, CreateGroupError> {
        let group = self.prepare();
        trace!(
            event = "group.create.started",
            group_id = %group.group_id,
            realm_id = %group.realm_id,
            user_id = %self.config.actor.user_id,
            "Creating group"
        );
        self.resolve_fence();
        let auth_doc = self
            .auth_doc
            .as_ref()
            .ok_or(CreateGroupError::DocNotFound)?;
        let key: ByteView = group.group_id.to_bytes().into();
        let mut writes = vec![
            (
                GROUP_KEYSPACE.to_string(),
                key.clone(),
                group.to_bytes(&self.config.actor)?.into(),
            ),
            (
                OWNER_INDEX_KEYSPACE.to_string(),
                owner_group_key(self.config.actor.user_id, group.group_id).into(),
                ByteView::from(Vec::new()),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                key,
                auth_doc.to_bytes(&self.config.actor)?.into(),
            ),
        ];
        writes.extend(self.reducer_seed_writes()?);
        Ok(writes)
    }

    fn write(&mut self) -> CreateTurn {
        let writes = self.writes()?;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn reducer_seed_writes(&self) -> Result<Vec<(String, Key, Value)>, CreateGroupError> {
        let group_id = self
            .group
            .as_ref()
            .ok_or(CreateGroupError::GroupNotFound)?
            .group_id;
        let auth_doc = self
            .auth_doc
            .as_ref()
            .ok_or(CreateGroupError::DocNotFound)?;
        let target = AdminDocumentTarget::Group { group_id };
        let mut reducer_state = AdminDocumentState::new(target);
        let mut admin_events = Vec::new();
        let roles = sorted_roles(auth_doc);

        admin_events.push(reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::GroupCreated {
                realm_id: self.config.actor.realm_id,
                display_name: self.config.display_name.clone(),
                owner: self.config.actor.user_id,
            },
        )?);
        admin_events.push(reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::GroupLocationSet {
                location: Some(DatasetLocation::group_default(group_id)),
            },
        )?);

        for role in &roles {
            admin_events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::GroupRoleCreated {
                    role: AdminRoleDefinition::from(*role),
                },
            )?);
        }

        let admin_role = roles
            .iter()
            .find(|role| role.name == "admin")
            .ok_or(CreateGroupError::AdminNotFound)?;
        for user_id in crate::sorted_user_ids(&admin_role.assigned_users) {
            admin_events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::GroupAssignmentAdded {
                    role_id: admin_role.role_id,
                    user_id,
                },
            )?);
        }

        let document_target = DocumentTarget::GroupAuthorization { group_id };
        let placement = self
            .realm_config
            .as_ref()
            .map(|config| target_placement_ref(config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        let realm_id = self.config.actor.realm_id;
        let mut writes = vec![reducer_state_entry(&reducer_state)?];
        for event in admin_events {
            let record = new_identified_record(
                event.event_id,
                self.config.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event),
                placement,
                true,
            )
            .fenced_at(self.fence.generation(&realm_id, &placement));
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&reducer_state)?);

        Ok(writes)
    }

    /// The group's authorization bucket and the generation it resolves at.
    fn resolve_fence(&mut self) {
        let (Some(config), Some(group)) = (self.realm_config.as_ref(), self.group.as_ref()) else {
            return;
        };
        let target = DocumentTarget::GroupAuthorization {
            group_id: group.group_id,
        };
        let placement = target_placement_ref(config, &target, Default::default());
        self.fence
            .add(self.config.actor.realm_id, config, [placement]);
    }

    /// The fence joins this transaction's read set, so a departing holder's close conflicts an
    /// uncommitted write; a failed drain timer leaves the durable outbox retryable.
    fn fence_or_finish(&mut self) -> CreateTurn {
        if !self.fence.is_empty() {
            self.stage = Stage::Fence;
            return Ok(Next::Call(IoCall::Read(self.fence.reads())));
        }
        self.finish()
    }

    fn finish(&mut self) -> CreateTurn {
        let (Some(group), Some(auth)) = (self.group.clone(), self.auth_doc.clone()) else {
            return Err(CreateGroupError::GroupNotFound);
        };
        trace!(
            event = "group.create.completed",
            group_id = %group.group_id,
            realm_id = %group.realm_id,
            "Created group and queued admin document operations"
        );
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then {
            output: (group, auth),
            calls,
        })
    }
}

impl Operation for GroupCreate {
    type Output = CreatedGroup;
    type Error = CreateGroupError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        CONFLICT_RETRIES as u32
    }

    fn first(&mut self) -> CreateTurn {
        Ok(Next::Call(IoCall::Read(vec![(
            REALM_CONFIG_KEYSPACE.to_string(),
            ByteView::from(*self.config.actor.realm_id.as_bytes()),
        )])))
    }

    fn next(&mut self, result: IoResult) -> CreateTurn {
        match self.stage {
            Stage::Config => {
                let [value] = values::<1>(result)?;
                self.handle_config(value)
            }
            Stage::CountOwned => {
                let (rows, _) = result.page()?;
                let cap = self.config.owner_cap.unwrap_or(u32::MAX);
                if rows.len() >= cap as usize {
                    return Err(CreateGroupError::GroupLimitReached { limit: cap });
                }
                self.write()
            }
            Stage::Write => {
                done(result)?;
                self.fence_or_finish()
            }
            Stage::Fence => {
                let values = result.values()?;
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(CreateGroupError::PlacementFenced);
                }
                self.finish()
            }
        }
    }
}

fn sorted_roles(auth_doc: &GroupAuthorizationDocument) -> Vec<&Role> {
    let mut roles: Vec<_> = auth_doc.roles.values().collect();
    roles.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.role_id.cmp(&right.role_id))
    });
    roles
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("No auth doc found")]
    DocNotFound,
    #[error("No admin role found")]
    AdminNotFound,
    #[error("No group found")]
    GroupNotFound,
    #[error("the group's bucket cut over to a new holder set; retry the create")]
    PlacementFenced,
    #[error("owned group limit reached ({limit})")]
    GroupLimitReached { limit: u32 },
    #[error("group name must be non-empty and at most {MAX_NAME_LEN} bytes")]
    InvalidDisplayName,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for CreateGroupError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "CreateGroup".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use crate::groups::create_group::{
        CreateGroupConfig, CreateGroupError, CreateGroupOperation, GroupCreate,
    };
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
    };
    use aruna_core::calls::{IoCall, IoError, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::reducer::AdminDocumentState;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::{Key, KeySpace, Value};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use std::collections::{BTreeSet, HashSet};
    use tempfile::tempdir;
    use ulid::Ulid;

    fn actor(realm_id: RealmId, node_seed: u8, user_seed: u8) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[node_seed; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([user_seed; 16]), realm_id),
            realm_id,
        }
    }

    fn config(actor: Actor) -> CreateGroupConfig {
        CreateGroupConfig {
            actor,
            display_name: "Test group".to_string(),
            owner_cap: None,
        }
    }

    fn write_values<'a>(writes: &'a [(KeySpace, Key, Value)], keyspace: &str) -> Vec<&'a Value> {
        writes
            .iter()
            .filter(|(candidate, _, _)| candidate == keyspace)
            .map(|(_, _, value)| value)
            .collect()
    }

    #[test]
    fn retries_commit_conflict() {
        // A background writer's conflict reruns the whole write; only an exhausted budget surfaces
        // the conflict to the caller.
        let conflict = CreateGroupError::from(IoError::Conflict);
        assert!(GroupCreate::is_conflict(&conflict));
        assert_eq!(
            GroupCreate::conflict_retries(),
            super::CONFLICT_RETRIES as u32
        );
        let write = GroupCreate::new(config(actor(RealmId([3; 32]), 1, 2)));
        assert_eq!(write.transaction(), TransactionMode::Write);
    }

    /// A realm whose buckets are activated at generation one, so a create
    /// resolves a generation and takes the group bucket's fence.
    fn activated_config(
        actor: &Actor,
    ) -> aruna_core::structs::identity::realm::RealmConfigDocument {
        let mut config = aruna_core::structs::identity::realm::RealmConfigDocument::new(
            actor.realm_id,
            Vec::new(),
            3,
        );
        config.ensure_node(
            actor.node_id,
            aruna_core::structs::identity::realm::RealmNodeKind::Server,
        );
        config
            .strategies
            .push(aruna_core::structs::placement::record::PlacementStrategy {
                strategy_id: Ulid::from_bytes([5; 16]),
                name: "default".to_string(),
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
                shard_count: 16,
            });
        config.default_strategy_id = Some(config.strategies[0].strategy_id);
        config.snapshot_candidate_map();
        config
    }

    /// Runs a create to the bucket fence read and answers it with `closed`.
    fn step_to_fence(
        write: &mut GroupCreate,
        actor: &Actor,
        closed: Option<u64>,
    ) -> Result<Next<(Group, GroupAuthorizationDocument)>, CreateGroupError> {
        let config = activated_config(actor);
        write.first().unwrap();
        let stored = IoResult::Values(vec![Some(config.to_bytes(actor).unwrap().into())]);
        assert!(matches!(
            write.next(stored),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        let Ok(Next::Call(IoCall::Read(reads))) = write.next(IoResult::Done) else {
            panic!("the transaction reads the bucket fence");
        };
        let values = reads
            .iter()
            .map(|_| closed.map(|generation| Value::from(generation.to_be_bytes().to_vec())))
            .collect();
        write.next(IoResult::Values(values))
    }

    #[test]
    fn create_takes_fence() {
        // An admitted create stamps the generation it resolved at onto every
        // admin row, so the drain can bound the predecessor set.
        let realm_id = RealmId::from_bytes([2; 32]);
        let actor = actor(realm_id, 3, 4);
        let mut write = GroupCreate::new(config(actor.clone()));

        let finished = step_to_fence(&mut write, &actor, None);
        assert!(matches!(finished, Ok(Next::Then { .. })));
        let writes = write.reducer_seed_writes().unwrap();
        let records = write_values(&writes, SYNC_OUTBOX_KEYSPACE)
            .into_iter()
            .map(|value| postcard::from_bytes::<DocumentOutboxRecord>(value.as_ref()).unwrap())
            .collect::<Vec<_>>();
        assert!(!records.is_empty(), "an admitted create publishes");
        for record in records {
            assert_eq!(record.generation, 1, "row for {:?}", record.placement);
        }
    }

    #[test]
    fn fence_rejects_create() {
        // The departing holder closed generation one: the create must not
        // commit a row the drained bucket can no longer publish.
        let realm_id = RealmId::from_bytes([2; 32]);
        let actor = actor(realm_id, 3, 4);
        let mut write = GroupCreate::new(config(actor.clone()));

        let fenced = step_to_fence(&mut write, &actor, Some(1));
        assert_eq!(fenced.err(), Some(CreateGroupError::PlacementFenced));
    }

    #[test]
    fn seeds_reducer_outbox() {
        let realm_id = RealmId::from_bytes([2; 32]);
        let actor = actor(realm_id, 3, 4);
        let mut write = GroupCreate::new(config(actor.clone()));
        let writes = write.writes().unwrap();
        let group = write.group.clone().unwrap();
        let auth_doc = write.auth_doc.clone().unwrap();
        let stored_group =
            Group::from_bytes(write_values(&writes, GROUP_KEYSPACE)[0].as_ref()).unwrap();
        assert_eq!(
            stored_group.roles,
            auth_doc.roles.keys().copied().collect::<HashSet<_>>()
        );
        assert!(!stored_group.roles.is_empty());

        let target = AdminDocumentTarget::Group {
            group_id: group.group_id,
        };
        let reducer_state = postcard::from_bytes::<AdminDocumentState>(
            write_values(&writes, DOCUMENT_STATE_KEYSPACE)[0].as_ref(),
        )
        .unwrap();
        let outbox_records = write_values(&writes, SYNC_OUTBOX_KEYSPACE)
            .into_iter()
            .map(|value| postcard::from_bytes::<DocumentOutboxRecord>(value.as_ref()).unwrap())
            .collect::<Vec<_>>();
        let stored_auth = GroupAuthorizationDocument::from_bytes(
            write_values(&writes, AUTH_KEYSPACE)[0].as_ref(),
        )
        .unwrap();

        assert_eq!(stored_auth, auth_doc);
        assert_eq!(reducer_state.target, target);
        assert_eq!(
            reducer_state.materialized_group_roles(),
            auth_doc.roles.keys().copied().collect::<BTreeSet<_>>()
        );
        assert_eq!(
            reducer_state.materialized_group_name().as_deref(),
            Some(group.display_name.as_str())
        );
        assert_eq!(
            reducer_state.materialized_group_realm(),
            Some(group.realm_id)
        );
        assert!(reducer_state.conflicts.is_empty());

        let admin_role = auth_doc
            .roles
            .values()
            .find(|role| role.name == "admin")
            .unwrap();
        assert!(
            reducer_state.materialized_group_assignments()[&admin_role.role_id]
                .contains(&actor.user_id)
        );
        assert_eq!(
            reducer_state.group_location(),
            Some(
                aruna_core::structs::storage::dataset_location::DatasetLocation::group_default(
                    group.group_id
                )
            )
        );

        // Creation, the default dataset location, each role and the admin assignment.
        assert_eq!(outbox_records.len(), auth_doc.roles.len() + 3);
        assert!(outbox_records.iter().all(|record| {
            record.target
                == (DocumentTarget::GroupAuthorization {
                    group_id: group.group_id,
                })
        }));
        // create_group originates the group authorization document, so every
        // outbox record it writes is allowed to mint the topic genesis.
        assert!(outbox_records.iter().all(|record| record.allow_genesis));
        let events = outbox_records
            .iter()
            .map(|record| match &record.event {
                DocumentOutboxEvent::AdminOperation { event, .. } => event.as_ref(),
                other => panic!("unexpected outbox event: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            &events.first().unwrap().op,
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name,
                owner,
            } if *realm_id == group.realm_id
                && display_name == &group.display_name
                && *owner == group.owner
        ));
        assert!(matches!(
            &events[1].op,
            AdminDocumentOperation::GroupLocationSet { location: Some(_) }
        ));
        let role_names = events[2..=auth_doc.roles.len() + 1]
            .iter()
            .map(|event| match &event.op {
                AdminDocumentOperation::GroupRoleCreated { role } => role.name.as_str(),
                other => panic!("unexpected role seed event: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(role_names, vec!["admin", "user", "viewer"]);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.target, target);
            assert_eq!(event.origin_seq, index as u64 + 1);
            assert_eq!(event.observed.sequence_for(&actor.node_id), index as u64);
        }
        assert!(matches!(
            &events.last().unwrap().op,
            AdminDocumentOperation::GroupAssignmentAdded { role_id, user_id }
                if *role_id == admin_role.role_id && *user_id == actor.user_id
        ));
        assert!(
            events[2..=auth_doc.roles.len() + 1]
                .iter()
                .all(|event| matches!(
                    &event.op,
                    AdminDocumentOperation::GroupRoleCreated { role }
                        if auth_doc.roles.get(&role.role_id).is_some_and(|source| {
                            role == &AdminRoleDefinition::from(source)
                        })
                ))
        );
    }

    #[test]
    fn schedule_finishes() {
        // Without a realm config nothing is fenced: the write commits and wakes the drain.
        let realm_id = RealmId::from_bytes([5; 32]);
        let mut write = GroupCreate::new(config(actor(realm_id, 6, 7)));
        write.first().unwrap();
        write.next(IoResult::Values(vec![None])).unwrap();
        let Ok(Next::Then { output, calls }) = write.next(IoResult::Done) else {
            panic!("expected the group with the drain wake");
        };
        assert_eq!(output.0.display_name, "Test group");
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    #[tokio::test]
    pub async fn test_group_creation() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
            outbound_handle: None,
        };

        let realm_id = aruna_core::structs::identity::realm::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let group_config = CreateGroupConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            display_name: "Test group".to_string(),
            owner_cap: None,
        };
        let group_operation = CreateGroupOperation::new(group_config.clone());
        let result = drive(group_operation, &context).await.unwrap();
        assert_eq!(result.0.display_name, group_config.display_name);
        assert_eq!(result.0.realm_id, group_config.actor.realm_id);
        assert_eq!(
            result.0.roles,
            result.1.roles.keys().copied().collect::<HashSet<_>>()
        );
        assert_eq!(result.0.roles.len(), 3);
        assert!(
            result
                .0
                .roles
                .iter()
                .all(|r| result.1.roles.iter().any(|(id, _)| id == r))
        );
        assert!(result.1.roles.iter().any(|(_id, role)| {
            role.name == "admin"
                && role
                    .assigned_users
                    .iter()
                    .any(|user| user == &group_config.actor.user_id)
        }));
        assert!(
            result
                .1
                .roles
                .iter()
                .any(|(_id, role)| { role.name == "user" })
        );
        assert!(
            result
                .1
                .roles
                .iter()
                .any(|(_id, role)| { role.name == "viewer" })
        );

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn owner_cap_blocks() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
            outbound_handle: None,
        };

        let realm_id = aruna_core::structs::identity::realm::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };

        let capped = |name: &str| CreateGroupConfig {
            actor: actor.clone(),
            display_name: name.to_string(),
            owner_cap: Some(1),
        };

        drive(CreateGroupOperation::new(capped("first")), &context)
            .await
            .unwrap();
        let second = drive(CreateGroupOperation::new(capped("second")), &context).await;
        assert!(matches!(
            second,
            Err(crate::groups::create_group::CreateGroupError::GroupLimitReached { limit: 1 })
        ));

        // Uncapped (realm admin) creation still works past the limit.
        drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: actor.clone(),
                display_name: "third".to_string(),
                owner_cap: None,
            }),
            &context,
        )
        .await
        .unwrap();

        net_handle.shutdown().await;
    }
}
