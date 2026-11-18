//! Reads and writes where a group's data is stored, and gathers its routing inputs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::{RecordReadError, index_prefix};
use crate::groups::fence::GroupWriteOperation;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_INDEX_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE,
    STORAGE_ROUTING_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::{PrefixPages, values};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::discovery::DiscoveryRules;
use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, GroupStorageRouting, RoutingError, RoutingTarget, validate_tenant_target,
};
use aruna_core::types::{GroupId, Key, Value};
use std::collections::BTreeSet;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

const BACKEND_PAGE_SIZE: usize = 128;

pub(crate) fn routing_key(group_id: GroupId) -> Key {
    group_id.to_bytes().to_vec().into()
}

fn routing_read(group_id: GroupId) -> IoCall {
    IoCall::Read(vec![(
        STORAGE_ROUTING_KEYSPACE.to_string(),
        routing_key(group_id),
    )])
}

/// What the inputs load reads next.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputsStage {
    Default,
    Backends,
    Imports,
    Mappings,
}

#[derive(Debug, Error, PartialEq)]
pub enum GroupInputsError {
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("group routing inputs never completed")]
    Incomplete,
    #[error("the group has more than {0} links to read at once")]
    TooManyRules(usize),
}

impl From<IoError> for GroupInputsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Read(RecordReadError::Storage(error)),
            _ => Self::Read(RecordReadError::Unexpected),
        }
    }
}

type InputsTurn = Result<Next<GroupRoutingInputs>, GroupInputsError>;

/// Loads one group's routing inputs: its default write target and the ids of
/// the backends it registered. The scan is prefixed by group, so a write never
/// pays for another tenant's backends.
#[derive(Debug, PartialEq)]
pub struct GroupInputsOperation {
    group_id: GroupId,
    stage: InputsStage,
    inputs: GroupRoutingInputs,
    pages: PrefixPages,
    /// Links and mapping rows read at most; more fail instead of being cut.
    cap: usize,
    read_rules: usize,
}

impl GroupInputsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            stage: InputsStage::Default,
            inputs: GroupRoutingInputs::default(),
            pages: PrefixPages::new(
                BACKEND_INDEX_KEYSPACE,
                index_prefix(group_id),
                BACKEND_PAGE_SIZE,
            ),
            cap: usize::MAX,
            read_rules: 0,
        }
    }

    /// Fails with `TooManyRules` instead of reading more than `cap` links or mapping rows.
    pub fn capped(mut self, cap: usize) -> Self {
        self.cap = cap;
        self
    }

    fn count_rules(&mut self, rows: usize) -> Result<(), GroupInputsError> {
        self.read_rules += rows;
        match self.read_rules > self.cap {
            true => Err(GroupInputsError::TooManyRules(self.cap)),
            false => Ok(()),
        }
    }

    /// The next page of the current scan, or the first page of the next one.
    fn advance(&mut self, next: InputsStage, space: &str) -> InputsTurn {
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        // The import and discovery rules are keyed by group first, so one prefix holds them.
        let prefix = Key::from(self.group_id.to_bytes().to_vec());
        self.pages = PrefixPages::new(space, prefix, BACKEND_PAGE_SIZE);
        self.stage = next;
        Ok(Next::Call(self.pages.call()))
    }

    fn backends(&mut self, rows: Vec<(Key, Value)>) -> InputsTurn {
        for (_, value) in rows {
            let record = GroupStorage::from_bytes(value.as_ref()).map_err(RecordReadError::from)?;
            if record.group_id != self.group_id || record.disabled {
                continue;
            }
            let folders = record.reserved_folders();
            self.inputs.containers.insert(record.backend_id, folders);
            self.inputs.backend_ids.insert(record.backend_id);
            if record.kind == GroupBackendKind::Fs {
                self.inputs.folder_ids.insert(record.backend_id);
            }
        }
        self.advance(InputsStage::Imports, BACKEND_IMPORT_KEYSPACE)
    }

    /// A rule whose write backend is disabled or gone keeps Aruna's own layout.
    fn imports(&mut self, rows: Vec<(Key, Value)>) -> InputsTurn {
        self.count_rules(rows.len())?;
        for (_, value) in rows {
            let rule = BackendImport::from_bytes(value.as_ref()).map_err(RecordReadError::from)?;
            if self.inputs.backend_ids.contains(&rule.write_backend()) {
                self.inputs.imports.push(rule);
            }
        }
        self.advance(InputsStage::Mappings, IMPORT_DISCOVERY_KEYSPACE)
    }

    fn mappings(&mut self, rows: Vec<(Key, Value)>) -> InputsTurn {
        self.count_rules(rows.len())?;
        for (key, value) in rows {
            let id = key.get(16..32).and_then(|id| <[u8; 16]>::try_from(id).ok());
            let id = id.ok_or(RecordReadError::Unexpected)?;
            let rules = DiscoveryRules::from_bytes(&value).map_err(RecordReadError::from)?;
            self.inputs
                .mappings
                .insert(Ulid::from_bytes(id), rules.rules);
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        Ok(Next::Done(std::mem::take(&mut self.inputs)))
    }
}

impl Operation for GroupInputsOperation {
    type Output = GroupRoutingInputs;
    type Error = GroupInputsError;

    fn first(&mut self) -> InputsTurn {
        Ok(Next::Call(routing_read(self.group_id)))
    }

    fn next(&mut self, result: IoResult) -> InputsTurn {
        if self.stage == InputsStage::Default {
            let [value] = values::<1>(result)?;
            let record = value
                .map(|value| GroupStorageRouting::from_bytes(value.as_ref()))
                .transpose()
                .map_err(RecordReadError::from)?;
            self.inputs.default_target = record.and_then(|it| it.default_target);
            self.stage = InputsStage::Backends;
            return Ok(Next::Call(self.pages.call()));
        }
        let rows = self.pages.take(result)?;
        match self.stage {
            InputsStage::Default | InputsStage::Backends => self.backends(rows),
            InputsStage::Imports => self.imports(rows),
            InputsStage::Mappings => self.mappings(rows),
        }
    }
}

/// Refuses a target naming a local folder backend; only import rules route to one.
pub(crate) fn refuse_folder(
    target: &RoutingTarget,
    folders: &BTreeSet<Ulid>,
) -> Result<(), RoutingError> {
    match target {
        RoutingTarget::Backend(BackendRef::Group(id)) if folders.contains(id) => {
            Err(RoutingError::FolderTarget(*id))
        }
        _ => Ok(()),
    }
}

/// Emits the sub-operation that loads a group's routing inputs.
pub fn load_group_inputs(group_id: GroupId) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        GroupInputsOperation::new(group_id),
        |result| {
            Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
                result: result.map_err(|error| error.to_string()),
            })
        },
    ))
}

/// What the default write waits for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutStage {
    LoadInputs,
    WriteRecord,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutGroupError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    InvalidTarget(#[from] RoutingError),
    #[error("could not load the group's storage backends: {0}")]
    InputsUnavailable(String),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    /// The default bucket could not be found, created or written to.
    #[error(transparent)]
    Unusable(aruna_core::errors::BlobError),
    #[error("unexpected event in state {0}")]
    Unexpected(&'static str),
    #[error("group routing write did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

impl From<IoError> for PutGroupError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, .. } => Self::Unexpected(expected),
            _ => Self::NotFinished,
        }
    }
}

type PutTurn = Result<Next<GroupStorageRouting>, PutGroupError>;

/// Writes the group's default write target. Clearing it is a write with
/// `default_target = None`, so the record always records who decided last.
#[derive(Debug, PartialEq)]
pub struct PutGroupOperation {
    record: GroupStorageRouting,
    stage: PutStage,
}

impl PutGroupOperation {
    pub fn new(
        group_id: GroupId,
        default_target: Option<RoutingTarget>,
        updated_by: UserId,
        updated_at: SystemTime,
    ) -> Self {
        Self {
            record: GroupStorageRouting {
                group_id,
                default_target,
                updated_at,
                updated_by,
            },
            stage: PutStage::LoadInputs,
        }
    }

    /// The guarded write of the routing record.
    fn record_write(&self) -> Result<GroupWriteOperation, PutGroupError> {
        let value = self.record.to_bytes()?;
        let row = (
            STORAGE_ROUTING_KEYSPACE.to_string(),
            routing_key(self.record.group_id),
            value.into(),
        );
        Ok(GroupWriteOperation::new(self.record.group_id, vec![row]))
    }

    fn write_record(&mut self, owned: &BTreeSet<Ulid>, folders: &BTreeSet<Ulid>) -> PutTurn {
        if let Some(target) = self.record.default_target.as_ref() {
            validate_tenant_target(target, owned).and_then(|()| refuse_folder(target, folders))?;
        }
        self.stage = PutStage::WriteRecord;
        Ok(Next::Call(IoCall::child(self.record_write()?)))
    }
}

impl Operation for PutGroupOperation {
    type Output = GroupStorageRouting;
    type Error = PutGroupError;

    /// A `Group` target is checked against the ids this group registered, so the record can
    /// never name another tenant's backend.
    fn first(&mut self) -> PutTurn {
        let load = GroupInputsOperation::new(self.record.group_id);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> PutTurn {
        match self.stage {
            PutStage::LoadInputs => {
                let inputs = result
                    .child::<GroupRoutingInputs, GroupInputsError>()?
                    .map_err(|error| PutGroupError::InputsUnavailable(error.to_string()))?;
                self.write_record(&inputs.backend_ids, &inputs.folder_ids)
            }
            PutStage::WriteRecord => {
                result.child::<(), GroupWriteError>()??;
                Ok(Next::Done(self.record.clone()))
            }
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GroupRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("group routing read did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

impl From<IoError> for GroupRoutingError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            _ => Self::NotFinished,
        }
    }
}

/// Reads the group's default write target. An absent record is no default, not
/// an error: most groups never set one.
#[derive(Debug, PartialEq)]
pub struct GroupRoutingOperation {
    group_id: GroupId,
}

impl GroupRoutingOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self { group_id }
    }
}

impl Operation for GroupRoutingOperation {
    type Output = Option<GroupStorageRouting>;
    type Error = GroupRoutingError;

    fn first(&mut self) -> Result<Next<Self::Output>, GroupRoutingError> {
        Ok(Next::Call(routing_read(self.group_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GroupRoutingError> {
        let [value] = values::<1>(result)?;
        let record = value
            .map(|value| GroupStorageRouting::from_bytes(value.as_ref()))
            .transpose()?;
        Ok(Next::Done(record))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{
        GroupInputsError, GroupInputsOperation, GroupRoutingError, GroupRoutingOperation,
        PutGroupError, PutGroupOperation, PutTurn, RecordReadError, routing_key,
    };
    use crate::groups::backends::{index_key, index_prefix};
    use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
    use aruna_core::keyspaces::{
        BACKEND_IMPORT_KEYSPACE, BACKEND_INDEX_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE,
        STORAGE_ROUTING_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::backend_import::BackendImport;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::structs::storage::routing::{
        GroupRoutingInputs, GroupStorageRouting, RoutingError, RoutingTarget,
    };
    use aruna_core::types::{Key, Value};
    use std::collections::{BTreeSet, HashMap};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn group() -> Ulid {
        Ulid::from_bytes([2u8; 16])
    }

    /// Answers the inputs child with these inputs.
    fn loaded_inputs(operation: &mut PutGroupOperation, inputs: GroupRoutingInputs) -> PutTurn {
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let loaded = Ok::<_, GroupInputsError>(inputs);
        operation.next(IoResult::Child(ChildOutcome::new(loaded)))
    }

    /// Answers the inputs child with the ids the group owns.
    fn loaded(operation: &mut PutGroupOperation, owned: BTreeSet<Ulid>) -> PutTurn {
        let inputs = GroupRoutingInputs {
            default_target: None,
            backend_ids: owned,
            ..Default::default()
        };
        loaded_inputs(operation, inputs)
    }

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    fn backend(backend_id: Ulid, group_id: Ulid) -> GroupStorage {
        GroupStorage {
            backend_id,
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::B2,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: false,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
    }

    fn record(target: Option<RoutingTarget>) -> GroupStorageRouting {
        GroupStorageRouting {
            group_id: group(),
            default_target: target,
            updated_at: SystemTime::UNIX_EPOCH,
            updated_by: aruna_core::UserId::default(),
        }
    }

    #[test]
    fn writes_group_default() {
        let target = RoutingTarget::Class("cold".to_string());
        let mut operation = PutGroupOperation::new(
            group(),
            Some(target.clone()),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );

        let write = loaded(&mut operation, BTreeSet::new());
        assert!(matches!(write, Ok(Next::Call(IoCall::Child(_)))));
        let mut write = operation.record_write().unwrap();
        write.first().unwrap();
        let Ok(Next::Call(IoCall::Write(writes))) = write.next(IoResult::Values(vec![None])) else {
            panic!("expected one record write");
        };
        let [(key_space, key, value)] = writes.as_slice() else {
            panic!("expected one record");
        };
        assert_eq!(key_space, STORAGE_ROUTING_KEYSPACE);
        assert_eq!(key, &routing_key(group()));
        assert_eq!(
            GroupStorageRouting::from_bytes(value.as_ref()).unwrap(),
            record(Some(target))
        );
    }

    #[test]
    fn rejects_operator_target() {
        let mut operation = PutGroupOperation::new(
            group(),
            Some(RoutingTarget::Backend(BackendRef::Node("cold".to_string()))),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );

        assert!(matches!(
            loaded(&mut operation, BTreeSet::new()),
            Err(PutGroupError::InvalidTarget(
                RoutingError::OperatorBackendTarget
            ))
        ));
    }

    #[test]
    fn rejects_unexpected_event() {
        let mut operation = PutGroupOperation::new(
            group(),
            None,
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );
        loaded(&mut operation, BTreeSet::new()).unwrap();

        // The write child answers; a plain write result does not.
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(PutGroupError::Unexpected(_))
        ));
    }

    #[test]
    fn rejects_folder_default() {
        // A local folder serves imports only, so it never becomes the group default.
        let folder = Ulid::from_bytes([4u8; 16]);
        let mut operation = PutGroupOperation::new(
            group(),
            Some(RoutingTarget::Backend(BackendRef::Group(folder))),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );
        let inputs = GroupRoutingInputs {
            backend_ids: BTreeSet::from([folder]),
            folder_ids: BTreeSet::from([folder]),
            ..Default::default()
        };
        assert_eq!(
            loaded_inputs(&mut operation, inputs).err(),
            Some(PutGroupError::InvalidTarget(RoutingError::FolderTarget(
                folder
            )))
        );
    }

    #[test]
    fn rejects_foreign_backend() {
        // A default naming a backend this group does not own must not be stored.
        let foreign = Ulid::from_bytes([9u8; 16]);
        let mut operation = PutGroupOperation::new(
            group(),
            Some(RoutingTarget::Backend(BackendRef::Group(foreign))),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );

        let owned = BTreeSet::from([Ulid::from_bytes([4u8; 16])]);
        assert_eq!(
            loaded(&mut operation, owned).err(),
            Some(PutGroupError::InvalidTarget(RoutingError::ForeignBackend(
                foreign
            )))
        );
    }

    #[test]
    fn loads_own_backends() {
        // The scan is prefixed by group, and a disabled backend cannot be routed to.
        let mine = Ulid::from_bytes([4u8; 16]);
        let disabled = Ulid::from_bytes([5u8; 16]);
        let mut operation = GroupInputsOperation::new(group());
        let read = vec![(STORAGE_ROUTING_KEYSPACE.to_string(), routing_key(group()))];
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(read))));

        let stored = record(Some(RoutingTarget::Class("cold".to_string())));
        let scan = operation.next(IoResult::Values(vec![Some(
            stored.to_bytes().unwrap().into(),
        )]));
        let Ok(Next::Call(IoCall::Page { space, prefix, .. })) = scan else {
            panic!("expected a prefixed backend scan");
        };
        assert_eq!(space, BACKEND_INDEX_KEYSPACE);
        assert_eq!(prefix, index_prefix(group()));

        let mut leaving = backend(disabled, group());
        leaving.disabled = true;
        let scan = operation.next(page(vec![
            (
                index_key(group(), mine),
                backend(mine, group()).to_bytes().unwrap().into(),
            ),
            (
                index_key(group(), disabled),
                leaving.to_bytes().unwrap().into(),
            ),
        ]));
        let Ok(Next::Call(IoCall::Page { space, .. })) = scan else {
            panic!("expected the import scan");
        };
        assert_eq!(space, BACKEND_IMPORT_KEYSPACE);

        // A rule that writes to a disabled backend keeps Aruna's own layout.
        let rule = |import: u8, backend_id| BackendImport {
            import_id: Ulid::from_bytes([import; 16]),
            group_id: group(),
            backend_id,
            backend_prefix: String::new(),
            bucket: "b".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: "{dir}{id}_{name}".to_string(),
        };
        let (kept, dropped) = (rule(8, mine), rule(9, disabled));
        let scan = operation.next(page(
            [&kept, &dropped]
                .into_iter()
                .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
                .collect(),
        ));
        // The mapping rules of the group's links load last, keyed like the links.
        let Ok(Next::Call(IoCall::Page { space, .. })) = scan else {
            panic!("expected the mapping scan");
        };
        assert_eq!(space, IMPORT_DISCOVERY_KEYSPACE);
        let mapping = aruna_core::structs::storage::discovery::DiscoveryRules {
            rules: vec![aruna_core::structs::storage::discovery::DiscoveryRule {
                pattern: "lars/{name}".to_string(),
                folder: "foobar/".to_string(),
            }],
        };
        let done = operation.next(page(vec![(kept.key(), mapping.to_bytes().unwrap().into())]));
        let Ok(Next::Done(inputs)) = done else {
            panic!("expected the inputs");
        };
        assert_eq!(inputs.mappings.get(&kept.import_id), Some(&mapping.rules));
        assert_eq!(
            inputs.default_target,
            Some(RoutingTarget::Class("cold".to_string()))
        );
        assert_eq!(inputs.backend_ids, BTreeSet::from([mine]));
        assert_eq!(inputs.imports, vec![kept]);
        assert!(!inputs.containers.contains_key(&disabled));
    }

    #[test]
    fn reads_absent_record() {
        let mut operation = GroupRoutingOperation::new(group());
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
    }

    #[test]
    fn reads_stored_target() {
        let stored = record(Some(RoutingTarget::Class("cold".to_string())));
        let mut operation = GroupRoutingOperation::new(group());
        operation.first().unwrap();
        let value = IoResult::Values(vec![Some(stored.to_bytes().unwrap().into())]);
        assert_eq!(operation.next(value), Ok(Next::Done(Some(stored))));
    }

    #[test]
    fn stray_answers_fail() {
        // An answer of the wrong kind fails each read instead of being taken as data.
        let mut inputs = GroupInputsOperation::new(group());
        inputs.first().unwrap();
        assert_eq!(
            inputs.next(IoResult::Done),
            Err(GroupInputsError::Read(RecordReadError::Unexpected))
        );
        let mut get = GroupRoutingOperation::new(group());
        get.first().unwrap();
        assert_eq!(
            get.next(IoResult::Done),
            Err(GroupRoutingError::NotFinished)
        );
    }
}
