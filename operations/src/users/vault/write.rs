//! Appends one vault revision or public key record on a holder of the user's vault placement.
//! The rows, their sync sidecars and the outbox publish commit in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, VaultSignEvent};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done, event, values};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::vault::{
    MAX_KEY_RECORDS, MAX_PREDECESSORS, MAX_VAULT_HEADS, UserKeyRecord, VaultRecordError,
    VaultRecords, VaultRevision, head_rows, record_rows, user_record_prefix,
};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::{Key, KeySpace, Value};
use aruna_core::vault_format::key_fingerprint;
use aruna_core::{NodeId, UserId};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::sync::outbox_record::{new_outbox_record, outbox_write_entry, schedule_drain_effect};
use aruna_core::placement::{PlacementResolveError, fence, holds_placement, plan_target_placement};

/// One change a user makes to their own vault records.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultChange {
    /// A new head replacing `predecessors`, the heads the client read.
    Save {
        payload: String,
        predecessors: Vec<Ulid>,
    },
    /// Replaces every current head with a delete marker.
    Delete,
    PublishKey {
        key_id: String,
        public_key: [u8; 32],
        has_recovery: bool,
    },
}

/// Shows the payload size only, so no formatted message carries vault ciphertext.
impl std::fmt::Debug for VaultChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Save {
                payload,
                predecessors,
            } => formatter
                .debug_struct("Save")
                .field("payload_bytes", &payload.len())
                .field("predecessors", predecessors)
                .finish(),
            Self::Delete => formatter.write_str("Delete"),
            Self::PublishKey { key_id, .. } => formatter
                .debug_struct("PublishKey")
                .field("key_id", key_id)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppendVaultConfig {
    pub node_id: NodeId,
    /// The realm that stores the vault: the serving realm, also for a user of another realm.
    pub realm_id: RealmId,
    pub user_id: UserId,
    /// Chosen where the request arrived, so a forwarded change keeps its id.
    pub record_id: Ulid,
    pub change: VaultChange,
    pub now_ms: u64,
}

/// The heads after a save or delete, or the published key record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultAppended {
    Heads(Vec<VaultRevision>),
    Key(Box<UserKeyRecord>),
}

/// Why a holder refused a forwarded vault change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultRefusal {
    Invalid,
    TooManyKeys,
}

#[derive(Debug, Error, PartialEq)]
pub enum AppendVaultError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Record(#[from] VaultRecordError),
    #[error(transparent)]
    PlacementResolve(#[from] PlacementResolveError),
    #[error("realm config document missing")]
    RealmConfigMissing,
    #[error("no placement strategy governs user vaults")]
    PlacementUnavailable,
    /// This node holds no replica of the user's vault; the caller forwards to `holders`.
    #[error("node holds no replica of the user's vault")]
    NotHolder { holders: Vec<NodeId> },
    #[error("vault placement cut over mid-write; retry")]
    PlacementFenced,
    #[error("the user has published the most key records allowed")]
    TooManyKeys,
    #[error("vault record not signed: {0}")]
    Unsigned(String),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct Planned {
    placement: PlacementRef,
    holders: Vec<NodeId>,
    generation: u64,
}

/// Records built in the transaction, waiting for this node's signature. `heads` are the
/// other current heads a save keeps.
#[derive(Debug, Clone, PartialEq)]
struct Unsigned {
    planned: Box<Planned>,
    records: VaultRecords,
    heads: Vec<VaultRevision>,
}

fn record_target(config: &AppendVaultConfig) -> DocumentTarget {
    let AppendVaultConfig {
        realm_id,
        user_id,
        record_id,
        ..
    } = *config;
    match config.change {
        VaultChange::PublishKey { .. } => DocumentTarget::UserKey {
            realm_id,
            user_id,
            record_id,
        },
        VaultChange::Save { .. } | VaultChange::Delete => DocumentTarget::VaultRevision {
            realm_id,
            user_id,
            revision_id: record_id,
        },
    }
}

/// Whether `signed` is `unsigned` with valid origin signatures added.
fn same_records(unsigned: &VaultRecords, signed: &VaultRecords) -> bool {
    match (unsigned, signed) {
        (VaultRecords::Heads(left), VaultRecords::Heads(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    right.origin_signed()
                        && *left
                            == VaultRevision {
                                signature: None,
                                ..right.clone()
                            }
                })
        }
        (VaultRecords::Keys(left), VaultRecords::Keys(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    right.origin_signed()
                        && *left
                            == UserKeyRecord {
                                signature: None,
                                ..right.clone()
                            }
                })
        }
        _ => false,
    }
}

#[derive(Debug, PartialEq)]
enum Stage {
    Config,
    Fence(Box<Planned>),
    Rows(Box<Planned>),
    Sign(Box<Unsigned>),
    Delete(Vec<(KeySpace, Key, Value)>),
    Write,
}

type AppendTurn = Result<Next<VaultAppended>, AppendVaultError>;

impl From<IoError> for AppendVaultError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "AppendVault".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

/// Adds one immutable vault or key record for the user on a holder of the user's
/// vault placement. A non-holder fails with `NotHolder`, so the caller forwards.
#[derive(Debug, PartialEq)]
pub struct AppendVaultOperation {
    config: AppendVaultConfig,
    stage: Stage,
    rows: PrefixPages,
    output: Option<VaultAppended>,
    delete_heads: Vec<Ulid>,
    delete_markers: Vec<VaultRevision>,
    delete_live: bool,
}

impl AppendVaultOperation {
    pub fn new(config: AppendVaultConfig) -> Self {
        let limit = match config.change {
            VaultChange::PublishKey { .. } => MAX_KEY_RECORDS,
            VaultChange::Save { .. } | VaultChange::Delete => MAX_VAULT_HEADS,
        };
        let space = record_target(&config).storage_keyspace();
        let rows = PrefixPages::new(space, user_record_prefix(config.user_id), limit);
        Self {
            config,
            stage: Stage::Config,
            rows,
            output: None,
            delete_heads: Vec::new(),
            delete_markers: Vec::new(),
            delete_live: false,
        }
    }

    fn target(&self) -> DocumentTarget {
        record_target(&self.config)
    }

    fn plan(&mut self, value: Option<Value>) -> AppendTurn {
        let value = value.ok_or(AppendVaultError::RealmConfigMissing)?;
        let config = RealmConfigDocument::from_bytes(&value)?;
        let plan = plan_target_placement(&config, &self.target(), Default::default())?
            .ok_or(AppendVaultError::PlacementUnavailable)?;
        if !holds_placement(&config, &plan.placement, self.config.node_id) {
            return Err(AppendVaultError::NotHolder {
                holders: plan.holders,
            });
        }
        let planned = Box::new(Planned {
            placement: plan.placement,
            holders: plan.holders,
            generation: fence::write_generation(&config, &plan.placement).unwrap_or_default(),
        });
        if planned.generation == 0 {
            return self.read_rows(planned);
        }
        let read = fence::fence_read(&self.config.realm_id, &planned.placement);
        self.stage = Stage::Fence(planned);
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn read_rows(&mut self, planned: Box<Planned>) -> AppendTurn {
        self.stage = Stage::Rows(planned);
        Ok(Next::Call(self.rows.call()))
    }

    /// Builds the record from the rows read in this transaction and asks for its signature.
    fn append(&mut self, planned: Box<Planned>, rows: Vec<(Key, Value)>) -> AppendTurn {
        let AppendVaultConfig {
            node_id,
            realm_id,
            user_id,
            record_id,
            now_ms,
            ..
        } = self.config;
        match self.config.change.clone() {
            VaultChange::PublishKey {
                key_id,
                public_key,
                has_recovery,
            } => {
                if rows.len() >= MAX_KEY_RECORDS {
                    return Err(AppendVaultError::TooManyKeys);
                }
                let record = UserKeyRecord {
                    realm_id,
                    user_id,
                    record_id,
                    key_id,
                    public_key,
                    fingerprint: key_fingerprint(&public_key),
                    has_recovery,
                    node_id,
                    placement: planned.placement,
                    created_at_ms: now_ms,
                    signature: None,
                };
                record.validate()?;
                self.sign(planned, VaultRecords::Keys(vec![record]), Vec::new())
            }
            VaultChange::Save {
                payload,
                predecessors,
            } => {
                let mut heads = rows
                    .iter()
                    .map(|(_, value)| VaultRevision::from_bytes(value))
                    .collect::<Result<Vec<_>, _>>()?;
                let record = VaultRevision {
                    realm_id,
                    user_id,
                    revision_id: record_id,
                    predecessors,
                    payload: Some(payload),
                    node_id,
                    placement: planned.placement,
                    created_at_ms: now_ms,
                    signature: None,
                };
                record.validate()?;
                heads.retain(|head| !record.predecessors.contains(&head.revision_id));
                self.sign(planned, VaultRecords::Heads(vec![record]), heads)
            }
            VaultChange::Delete => self.delete_heads(planned),
        }
    }

    /// Chains delete markers so each names at most `MAX_PREDECESSORS` heads.
    fn delete_heads(&mut self, planned: Box<Planned>) -> AppendTurn {
        if !self.delete_live {
            let markers = std::mem::take(&mut self.delete_markers);
            return Ok(Next::Done(VaultAppended::Heads(markers)));
        }
        let mut heads = std::mem::take(&mut self.delete_heads)
            .into_iter()
            .peekable();
        let mut previous = None;
        let mut records = Vec::new();
        loop {
            let mut predecessors: Vec<_> = previous.into_iter().collect();
            predecessors.extend(heads.by_ref().take(MAX_PREDECESSORS - predecessors.len()));
            let final_head = heads.peek().is_none();
            let record = VaultRevision {
                realm_id: self.config.realm_id,
                user_id: self.config.user_id,
                revision_id: if final_head {
                    self.config.record_id
                } else {
                    Ulid::generate()
                },
                predecessors,
                payload: None,
                node_id: self.config.node_id,
                placement: planned.placement,
                created_at_ms: self.config.now_ms,
                signature: None,
            };
            record.validate()?;
            previous = Some(record.revision_id);
            records.push(record);
            if final_head {
                return self.sign(planned, VaultRecords::Heads(records), Vec::new());
            }
        }
    }

    fn sign(
        &mut self,
        planned: Box<Planned>,
        records: VaultRecords,
        heads: Vec<VaultRevision>,
    ) -> AppendTurn {
        let effect = Effect::Net(NetEffect::VaultSign(Box::new(records.clone())));
        self.stage = Stage::Sign(Box::new(Unsigned {
            planned,
            records,
            heads,
        }));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    /// Emits the rows and outbox entries of the signed records. The adapter may only add
    /// signatures, so any other difference fails the write.
    fn write_signed(&mut self, unsigned: Unsigned, signed: VaultRecords) -> AppendTurn {
        let Unsigned {
            planned,
            records,
            mut heads,
        } = unsigned;
        if !same_records(&records, &signed) {
            return Err(AppendVaultError::Unsigned(
                "the signed records differ".to_string(),
            ));
        }
        let mut writes = Vec::new();
        let mut deletes = Vec::new();
        let outbox = |target, bytes, change| -> Result<(KeySpace, Key, Value), AppendVaultError> {
            let record = new_outbox_record(
                self.config.node_id,
                target,
                planned.holders.clone(),
                DocumentOutboxEvent::Upsert { bytes, change },
                planned.placement,
                false,
            )
            .fenced_at(planned.generation);
            Ok(outbox_write_entry(&record).map_err(ConversionError::from)?)
        };
        let output = match signed {
            VaultRecords::Keys(keys) => {
                let [record] = <[UserKeyRecord; 1]>::try_from(keys)
                    .map_err(|_| AppendVaultError::Unsigned("one key record".to_string()))?;
                let (bytes, change) = (record.to_bytes()?, record.sync_change());
                let rows = record_rows(&record.target(), &bytes, &change)?;
                writes.extend(rows);
                writes.push(outbox(record.target(), bytes, change)?);
                VaultAppended::Key(Box::new(record))
            }
            VaultRecords::Heads(records) => {
                let last = records.len().saturating_sub(1);
                let mut newest = None;
                for (index, record) in records.into_iter().enumerate() {
                    let (bytes, change) = (record.to_bytes()?, record.sync_change());
                    let (mut rows, retired) = head_rows(&record, &bytes)?;
                    // A delete chain keeps only its final marker as a head.
                    if index != last {
                        rows.retain(|(key_space, _, _)| {
                            key_space != aruna_core::keyspaces::VAULT_REVISION_KEYSPACE
                        });
                    }
                    writes.extend(rows);
                    writes.push(outbox(record.target(), bytes, change)?);
                    deletes.extend(retired);
                    newest = Some(record);
                }
                let record = newest.ok_or(AppendVaultError::Unsigned("no record".to_string()))?;
                if matches!(self.config.change, VaultChange::Delete) {
                    VaultAppended::Heads(vec![record])
                } else {
                    heads.push(record);
                    heads.sort_by_key(|head| head.revision_id);
                    VaultAppended::Heads(heads)
                }
            }
        };
        self.output = Some(output);
        if deletes.is_empty() {
            self.stage = Stage::Write;
            return Ok(Next::Call(IoCall::Write(writes)));
        }
        self.stage = Stage::Delete(writes);
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    /// Notes the heads a delete retires; a save or key publish reads one page only.
    fn take_rows(&mut self, planned: Box<Planned>, result: IoResult) -> AppendTurn {
        let rows = self.rows.take(result)?;
        if !matches!(self.config.change, VaultChange::Delete) {
            return self.append(planned, rows);
        }
        for (_, value) in &rows {
            let head = VaultRevision::from_bytes(value)?;
            self.delete_heads.push(head.revision_id);
            self.delete_live |= head.payload.is_some();
            if head.payload.is_none() && self.delete_markers.len() < MAX_VAULT_HEADS {
                self.delete_markers.push(head);
            }
        }
        if !self.rows.done() {
            return self.read_rows(planned);
        }
        self.append(planned, rows)
    }
}

impl Operation for AppendVaultOperation {
    type Output = VaultAppended;
    type Error = AppendVaultError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> AppendTurn {
        if let VaultChange::Save { payload, .. } = &self.config.change
            && payload.len() > aruna_core::structs::identity::user::vault::MAX_VAULT_BYTES
        {
            return Err(VaultRecordError::TooLarge.into());
        }
        let target = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
        )])))
    }

    fn next(&mut self, result: IoResult) -> AppendTurn {
        match std::mem::replace(&mut self.stage, Stage::Config) {
            Stage::Config => {
                let [value] = values::<1>(result)?;
                self.plan(value)
            }
            Stage::Fence(planned) => {
                let [value] = values::<1>(result)?;
                if !fence::admits(value.as_ref(), planned.generation) {
                    return Err(AppendVaultError::PlacementFenced);
                }
                self.read_rows(planned)
            }
            Stage::Rows(planned) => self.take_rows(planned, result),
            Stage::Sign(unsigned) => match event(result)? {
                Event::Net(NetEvent::VaultSign(VaultSignEvent::Signed(signed))) => {
                    self.write_signed(*unsigned, *signed)
                }
                Event::Net(NetEvent::VaultSign(VaultSignEvent::Unavailable(reason))) => {
                    Err(AppendVaultError::Unsigned(reason))
                }
                other => Err(AppendVaultError::UnexpectedEvent {
                    state: "sign".to_string(),
                    expected: "Event::Net(NetEvent::VaultSign)",
                    got: other.label(),
                }),
            },
            Stage::Delete(writes) => {
                done(result)?;
                self.stage = Stage::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
            Stage::Write => {
                done(result)?;
                let output = self.output.take().ok_or(AppendVaultError::NotFinished)?;
                Ok(Next::Then {
                    output,
                    calls: vec![IoCall::Effect(Box::new(schedule_drain_effect()))],
                })
            }
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            AppendVaultError::NotHolder { .. }
                | AppendVaultError::Record(_)
                | AppendVaultError::TooManyKeys
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::{VAULT_RETIRED_KEYSPACE, VAULT_REVISION_KEYSPACE};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    use aruna_core::structs::identity::user::vault::user_record_key;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
    }

    /// Four servers and two vault replicas, so the realm has holders and non-holders.
    fn config_bytes() -> (Value, Vec<NodeId>) {
        let mut config = RealmConfigDocument::new(user().realm_id, Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let target = DocumentTarget::VaultRevision {
            realm_id: user().realm_id,
            user_id: user(),
            revision_id: Ulid::nil(),
        };
        let holders = plan_target_placement(&config, &target, Default::default())
            .unwrap()
            .unwrap()
            .holders;
        let actor = Actor {
            node_id: node(1),
            user_id: user(),
            realm_id: user().realm_id,
        };
        (config.to_bytes(&actor).unwrap().into(), holders)
    }

    fn operation(node_id: NodeId, change: VaultChange) -> AppendVaultOperation {
        AppendVaultOperation::new(AppendVaultConfig {
            node_id,
            realm_id: user().realm_id,
            user_id: user(),
            record_id: Ulid::from_bytes([9; 16]),
            change,
            now_ms: 5,
        })
    }

    /// Runs the operation up to the row read.
    fn read_rows(operation: &mut AppendVaultOperation, config: Value) -> AppendTurn {
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        operation.next(IoResult::Values(vec![Some(config)]))
    }

    fn head(seed: u8, payload: Option<&str>) -> (Key, Value) {
        let revision = VaultRevision {
            realm_id: user().realm_id,
            user_id: user(),
            revision_id: Ulid::from_bytes([seed; 16]),
            predecessors: Vec::new(),
            payload: payload.map(str::to_string),
            node_id: node(1),
            placement: PlacementRef::NIL,
            created_at_ms: 1,
            signature: None,
        };
        (
            revision.target().storage_key(),
            revision.to_bytes().unwrap().into(),
        )
    }

    fn rows(operation: &mut AppendVaultOperation, rows: Vec<(Key, Value)>) -> AppendTurn {
        operation.next(IoResult::Page { rows, next: None })
    }

    fn sign_request(turn: &AppendTurn) -> VaultRecords {
        match turn {
            Ok(Next::Call(IoCall::Effect(effect))) => match &**effect {
                Effect::Net(NetEffect::VaultSign(records)) => (**records).clone(),
                _ => panic!("expected a signing request"),
            },
            _ => panic!("expected a signing request"),
        }
    }

    fn answer_sign(operation: &mut AppendVaultOperation, event: VaultSignEvent) -> AppendTurn {
        let event = Event::Net(NetEvent::VaultSign(event));
        operation.next(IoResult::Event(Box::new(event)))
    }

    /// Answers the signing request as the adapter of the named node would.
    fn signed(operation: &mut AppendVaultOperation, turn: AppendTurn) -> AppendTurn {
        let mut records = sign_request(&turn);
        let sign = |node_id: NodeId, bytes: Vec<u8>| {
            (1..=4)
                .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]))
                .find(|secret| secret.public() == node_id)
                .unwrap()
                .sign(&bytes)
        };
        match &mut records {
            VaultRecords::Heads(heads) => heads.iter_mut().for_each(|head| {
                head.signature = Some(sign(head.node_id, head.signing_bytes().unwrap()));
            }),
            VaultRecords::Keys(keys) => keys.iter_mut().for_each(|key| {
                key.signature = Some(sign(key.node_id, key.signing_bytes().unwrap()));
            }),
        }
        answer_sign(operation, VaultSignEvent::Signed(Box::new(records)))
    }

    fn written(turn: AppendTurn) -> Vec<(KeySpace, Key, Value)> {
        match turn {
            Ok(Next::Call(IoCall::Write(writes))) => writes,
            _ => panic!("expected a write"),
        }
    }

    fn deleted(turn: AppendTurn) -> Vec<(KeySpace, Key)> {
        match turn {
            Ok(Next::Call(IoCall::Delete(deletes))) => deletes,
            _ => panic!("expected a delete"),
        }
    }

    #[test]
    fn saves_on_holder() {
        let (config, holders) = config_bytes();
        let previous = Ulid::from_bytes([1; 16]);
        let mut operation = operation(
            holders[0],
            VaultChange::Save {
                payload: "sealed".to_string(),
                predecessors: vec![previous],
            },
        );
        let turn = read_rows(&mut operation, config);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        let turn = rows(
            &mut operation,
            vec![head(1, Some("old")), head(2, Some("other"))],
        );
        let turn = signed(&mut operation, turn);
        let previous_row = user_record_key(user(), previous);
        assert_eq!(
            deleted(turn),
            vec![(VAULT_REVISION_KEYSPACE.to_string(), previous_row.clone())]
        );
        let writes: Vec<_> = written(operation.next(IoResult::Done))
            .into_iter()
            .map(|(key_space, key, _)| (key_space, key))
            .collect();
        let own_row = user_record_key(user(), Ulid::from_bytes([9; 16]));
        assert!(writes.contains(&(VAULT_REVISION_KEYSPACE.to_string(), own_row)));
        assert!(writes.contains(&(VAULT_RETIRED_KEYSPACE.to_string(), previous_row)));
        assert!(
            writes
                .iter()
                .any(|(key_space, _)| key_space == aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE),
            "the record is queued for the other holders in the same transaction"
        );
        let Ok(Next::Then {
            output: VaultAppended::Heads(heads),
            calls,
        }) = operation.next(IoResult::Done)
        else {
            panic!("a save answers heads with the drain wake");
        };
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
        let ids: Vec<_> = heads.iter().map(|head| head.revision_id).collect();
        assert_eq!(ids, [Ulid::from_bytes([2; 16]), Ulid::from_bytes([9; 16])]);
        assert!(heads[1].origin_signed());
    }

    #[test]
    fn refuses_unsigned_records() {
        // Changed records, a forged signature or a refusal abort the transaction unwritten.
        let (config, holders) = config_bytes();
        let change = VaultChange::PublishKey {
            key_id: "key-1".to_string(),
            public_key: [4; 32],
            has_recovery: false,
        };
        for altered in [Some(true), Some(false), None] {
            let mut operation = operation(holders[0], change.clone());
            read_rows(&mut operation, config.clone()).unwrap();
            let turn = rows(&mut operation, Vec::new());
            let VaultRecords::Keys(mut keys) = sign_request(&turn) else {
                panic!("expected a key record");
            };
            if altered == Some(true) {
                keys[0].user_id = UserId::local(Ulid::from_bytes([8; 16]), keys[0].realm_id);
            }
            keys[0].signature = Some(iroh::SecretKey::from_bytes(&[1; 32]).sign(b"x"));
            let event = match altered {
                Some(_) => VaultSignEvent::Signed(Box::new(VaultRecords::Keys(keys))),
                None => VaultSignEvent::Unavailable("no key".to_string()),
            };
            assert!(matches!(
                answer_sign(&mut operation, event),
                Err(AppendVaultError::Unsigned(_))
            ));
        }
    }

    #[test]
    fn refuses_non_holder() {
        let (config, holders) = config_bytes();
        let outsider = (1..=4)
            .map(node)
            .find(|node| !holders.contains(node))
            .unwrap();
        let mut operation = operation(outsider, VaultChange::Delete);
        assert_eq!(
            read_rows(&mut operation, config),
            Err(AppendVaultError::NotHolder { holders })
        );
    }

    #[test]
    fn deletes_live_heads() {
        let (config, holders) = config_bytes();
        let mut operation = operation(holders[0], VaultChange::Delete);
        read_rows(&mut operation, config.clone()).unwrap();
        // Only delete markers left: nothing to write.
        let turn = rows(&mut operation, vec![head(1, None)]);
        assert!(matches!(turn, Ok(Next::Done(VaultAppended::Heads(heads))) if heads.len() == 1));

        let mut operation = self::operation(holders[0], VaultChange::Delete);
        read_rows(&mut operation, config).unwrap();
        let turn = rows(&mut operation, vec![head(1, None), head(2, Some("sealed"))]);
        assert_eq!(deleted(signed(&mut operation, turn)).len(), 2);
    }

    #[test]
    fn deletes_snapshot_heads() {
        use aruna_core::document::DocumentOutboxRecord;
        use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
        use std::collections::{BTreeMap, BTreeSet};

        for count in [33, 65] {
            let (config, holders) = config_bytes();
            let mut operation = operation(holders[0], VaultChange::Delete);
            operation.config.record_id = Ulid::from_bytes([250; 16]);
            let original: Vec<_> = (1..=count)
                .map(|id| head(id, (id == count).then_some("sealed")))
                .collect();
            read_rows(&mut operation, config).unwrap();
            let Stage::Rows(planned) = &mut operation.stage else {
                panic!("expected scan");
            };
            planned.generation = 7;
            let mut turn = Err(AppendVaultError::NotFinished);
            for (index, page) in original.chunks(MAX_VAULT_HEADS).enumerate() {
                let next = (index * MAX_VAULT_HEADS + page.len() < original.len())
                    .then(|| page.last().unwrap().0.clone());
                turn = operation.next(IoResult::Page {
                    rows: page.to_vec(),
                    next: next.clone(),
                });
                if let Some(next) = next {
                    assert!(matches!(&turn,
                        Ok(Next::Call(IoCall::Page { after: Some(after), limit, .. }))
                            if after == &next && *limit == MAX_VAULT_HEADS));
                }
            }
            let deletes = deleted(signed(&mut operation, turn));
            assert!(original.iter().all(|(key, _)| {
                deletes.contains(&(VAULT_REVISION_KEYSPACE.to_string(), key.clone()))
            }));
            let writes = written(operation.next(IoResult::Done));
            let outbox: Vec<_> = writes
                .iter()
                .filter(|(space, _, _)| space == SYNC_OUTBOX_KEYSPACE)
                .map(|(_, _, bytes)| postcard::from_bytes::<DocumentOutboxRecord>(bytes).unwrap())
                .collect();
            assert_eq!(outbox.len(), if count == 33 { 2 } else { 3 });
            let mut covered = BTreeSet::new();
            let mut markers = BTreeSet::new();
            for entry in &outbox {
                assert_eq!(entry.generation, 7);
                let DocumentOutboxEvent::Upsert { bytes, change } = &entry.event else {
                    panic!("expected upsert");
                };
                let marker = VaultRevision::from_bytes(bytes).unwrap();
                assert_eq!(marker.validate(), Ok(()));
                assert!(marker.origin_signed());
                assert!(marker.payload.is_none());
                assert_eq!(entry.target, marker.target());
                assert_eq!(*change, marker.sync_change());
                assert!(
                    writes.contains(
                        &aruna_core::storage_entries::sync_revision_entry(&entry.target, change)
                            .unwrap()
                    )
                );
                assert!(
                    writes.contains(
                        &aruna_core::storage_entries::shard_manifest_entry(&entry.target, change)
                            .unwrap()
                            .unwrap()
                    )
                );
                covered.extend(marker.predecessors);
                markers.insert(marker.revision_id);
            }
            let final_id = operation.config.record_id;
            assert!(
                markers
                    .iter()
                    .filter(|id| **id != final_id)
                    .all(|id| covered.contains(id))
            );
            for (key, value) in &original {
                assert!(covered.contains(&VaultRevision::from_bytes(value).unwrap().revision_id));
                assert!(
                    writes.iter().any(
                        |(space, retired, _)| space == VAULT_RETIRED_KEYSPACE && retired == key
                    )
                );
            }
            let concurrent = head(251, Some("later"));
            let mut live: BTreeMap<_, _> = original.into_iter().collect();
            live.insert(concurrent.0.clone(), concurrent.1.clone());
            for (_, key) in &deletes {
                live.remove(key);
            }
            for (space, key, value) in &writes {
                if space == VAULT_REVISION_KEYSPACE {
                    live.insert(key.clone(), value.clone());
                }
            }
            assert_eq!(live.len(), 2);
            assert_eq!(live.get(&concurrent.0), Some(&concurrent.1));
            assert!(
                VaultRevision::from_bytes(&live[&user_record_key(user(), final_id)])
                    .unwrap()
                    .payload
                    .is_none()
            );
            assert!(matches!(
                operation.next(IoResult::Done),
                Ok(Next::Then { .. })
            ));
        }
    }

    #[test]
    fn bounds_key_records() {
        let (config, holders) = config_bytes();
        let change = VaultChange::PublishKey {
            key_id: "key-1".to_string(),
            public_key: [4; 32],
            has_recovery: false,
        };
        let mut operation = operation(holders[0], change);
        read_rows(&mut operation, config).unwrap();
        let full = (0..MAX_KEY_RECORDS)
            .map(|index| (Key::from(vec![index as u8]), Value::from(&[][..])))
            .collect();
        assert_eq!(
            rows(&mut operation, full),
            Err(AppendVaultError::TooManyKeys)
        );
    }

    #[test]
    fn rejects_unexpected_event() {
        let (_, holders) = config_bytes();
        let mut operation = operation(holders[0], VaultChange::Delete);
        operation.first().unwrap();
        assert!(matches!(
            rows(&mut operation, Vec::new()),
            Err(AppendVaultError::UnexpectedEvent { .. })
        ));
    }
}
