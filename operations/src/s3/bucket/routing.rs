//! Writes and reads the storage routing rules that a bucket record carries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::storage_routing::{GroupInputsError, GroupInputsOperation, refuse_folder};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, RoutingError, RoutingTarget, StorageRoutingRule, validate_tenant_rules,
};
use aruna_core::types::GroupId;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutRoutingState {
    LoadInputs,
    ReadBucket,
    WriteBucket,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    InvalidRules(#[from] RoutingError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("could not load the group's storage backends: {0}")]
    InputsUnavailable(String),
    #[error("The bucket changed owner while the rules were written")]
    GroupMismatch,
    #[error("PutBucketRouting failed: {0}")]
    Io(IoError),
}

impl From<IoError> for PutRoutingError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

type PutStep = Result<Next<Vec<StorageRoutingRule>>, PutRoutingError>;

#[derive(Debug, PartialEq)]
pub struct PutRoutingOperation {
    bucket: String,
    group_id: GroupId,
    rules: Vec<StorageRoutingRule>,
    state: PutRoutingState,
}

impl PutRoutingOperation {
    pub fn new(bucket: String, group_id: GroupId, rules: Vec<StorageRoutingRule>) -> Self {
        Self {
            bucket,
            group_id,
            rules,
            state: PutRoutingState::LoadInputs,
        }
    }

    /// Checks the rules against the group's backends, then reads the bucket in the transaction.
    fn inputs_loaded(&mut self, inputs: GroupRoutingInputs) -> PutStep {
        validate_tenant_rules(&self.rules, &inputs.backend_ids)?;
        // A folder backend takes only the rule its import rule wrote for this bucket.
        let imported = |rule: &StorageRoutingRule| {
            inputs.imports.iter().any(|import| {
                import.bucket == self.bucket
                    && import.key_prefix == rule.key_prefix
                    && !rule.exact
                    && rule.target
                        == RoutingTarget::Backend(BackendRef::Group(import.write_backend()))
            })
        };
        let refused = self.rules.iter().find_map(|rule| {
            let refused = refuse_folder(&rule.target, &inputs.folder_ids).err();
            refused.filter(|_| !imported(rule))
        });
        if let Some(error) = refused {
            return Err(error.into());
        }
        self.state = PutRoutingState::ReadBucket;
        Ok(Next::Call(read_row(
            S3_BUCKET_KEYSPACE,
            self.bucket.as_bytes(),
        )))
    }

    fn bucket_read(&mut self, value: Option<aruna_core::types::Value>) -> PutStep {
        let value = value.ok_or(PutRoutingError::NoSuchBucket)?;
        let mut info = BucketInfo::from_bytes(value.as_ref())?;
        // The owned backend ids were loaded for the authorized group, so
        // the record has to still belong to it.
        if info.group_id != self.group_id {
            return Err(PutRoutingError::GroupMismatch);
        }
        info.storage_routing = self.rules.clone();
        let row = (
            S3_BUCKET_KEYSPACE.to_string(),
            self.bucket.as_bytes().to_vec().into(),
            info.to_bytes()?.into(),
        );
        self.state = PutRoutingState::WriteBucket;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for PutRoutingOperation {
    type Output = Vec<StorageRoutingRule>;
    type Error = PutRoutingError;

    fn first(&mut self) -> PutStep {
        // A `Group` target is checked against the ids the bucket's own group
        // registered, so a rule can never name another tenant's backend.
        let load = GroupInputsOperation::new(self.group_id);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> PutStep {
        match self.state {
            PutRoutingState::LoadInputs => {
                let loaded = result.child::<GroupRoutingInputs, GroupInputsError>()?;
                let inputs = loaded
                    .map_err(|error| PutRoutingError::InputsUnavailable(error.to_string()))?;
                self.inputs_loaded(inputs)
            }
            PutRoutingState::ReadBucket => self.bucket_read(result.value()?),
            PutRoutingState::WriteBucket => {
                result.done()?;
                Ok(Next::Done(self.rules.clone()))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GetRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("GetBucketRouting failed: {0}")]
    Io(IoError),
}

impl From<IoError> for GetRoutingError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct GetRoutingOperation {
    bucket: String,
}

impl GetRoutingOperation {
    pub fn new(bucket: String) -> Self {
        Self { bucket }
    }
}

impl Operation for GetRoutingOperation {
    type Output = Vec<StorageRoutingRule>;
    type Error = GetRoutingError;

    fn first(&mut self) -> Result<Next<Self::Output>, GetRoutingError> {
        Ok(Next::Call(read_row(
            S3_BUCKET_KEYSPACE,
            self.bucket.as_bytes(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GetRoutingError> {
        let value = result.value()?.ok_or(GetRoutingError::NoSuchBucket)?;
        let info = BucketInfo::from_bytes(value.as_ref())?;
        Ok(Next::Done(info.storage_routing))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{
        GetRoutingError, GetRoutingOperation, PutRoutingError, PutRoutingOperation, PutStep,
    };
    use crate::groups::storage_routing::GroupInputsError;
    use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::routing::{
        GroupRoutingInputs, RoutingError, RoutingTarget, StorageRoutingRule,
    };
    use std::collections::BTreeSet;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn rule(target: RoutingTarget) -> StorageRoutingRule {
        StorageRoutingRule {
            key_prefix: "archive/".to_string(),
            exact: false,
            target,
        }
    }

    fn group() -> Ulid {
        Ulid::from_bytes([1u8; 16])
    }

    fn inputs_answer(inputs: GroupRoutingInputs) -> IoResult {
        let loaded: Result<_, GroupInputsError> = Ok(inputs);
        IoResult::Child(ChildOutcome::new(loaded))
    }

    /// Replays the loader child with the ids the group owns.
    fn loaded(operation: &mut PutRoutingOperation, owned: BTreeSet<Ulid>) -> PutStep {
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.next(inputs_answer(GroupRoutingInputs {
            default_target: None,
            backend_ids: owned,
            ..Default::default()
        }))
    }

    fn bucket() -> BucketInfo {
        BucketInfo {
            group_id: group(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn read(info: Option<&BucketInfo>) -> IoResult {
        IoResult::Values(vec![info.map(|info| info.to_bytes().unwrap().into())])
    }

    fn reads_bucket(turn: &PutStep) -> bool {
        matches!(turn, Ok(Next::Call(IoCall::Read(_))))
    }

    #[test]
    fn writes_bucket_rules() {
        let rules = vec![rule(RoutingTarget::Class("cold".to_string()))];
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules.clone());
        assert!(operation.transaction());
        assert!(reads_bucket(&loaded(&mut operation, BTreeSet::new())));

        let turn = operation.next(read(Some(&bucket())));

        let Ok(Next::Call(IoCall::Write(writes))) = &turn else {
            panic!("expected one record write, got {turn:?}")
        };
        let written = BucketInfo::from_bytes(writes[0].2.as_ref()).unwrap();
        assert_eq!(written.storage_routing, rules);
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(rules)));
    }

    #[test]
    fn rejects_operator_target() {
        // Tenant rules must not bind an operator backend name.
        let rules = vec![rule(RoutingTarget::Backend(BackendRef::Node(
            "cold".to_string(),
        )))];
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules);

        assert_eq!(
            loaded(&mut operation, BTreeSet::new()),
            Err(PutRoutingError::InvalidRules(
                RoutingError::OperatorBackendTarget
            ))
        );
    }

    #[test]
    fn rejects_foreign_backend() {
        // A rule naming a backend this group does not own must not be stored.
        let foreign = Ulid::from_bytes([9u8; 16]);
        let rules = vec![rule(RoutingTarget::Backend(BackendRef::Group(foreign)))];
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules);

        let turn = loaded(
            &mut operation,
            BTreeSet::from([Ulid::from_bytes([4u8; 16])]),
        );

        assert_eq!(
            turn,
            Err(PutRoutingError::InvalidRules(RoutingError::ForeignBackend(
                foreign
            )))
        );
    }

    #[test]
    fn folder_imports_only() {
        // A local folder takes only the routing rule its import rule wrote for this bucket.
        let folder = Ulid::from_bytes([4u8; 16]);
        let target = RoutingTarget::Backend(BackendRef::Group(folder));
        let import = aruna_core::structs::storage::backend_import::BackendImport {
            import_id: Ulid::from_bytes([5u8; 16]),
            group_id: group(),
            backend_id: folder,
            backend_prefix: String::new(),
            bucket: "b".to_string(),
            key_prefix: "archive/".to_string(),
            schedule: None,
            created_by: aruna_core::UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: String::new(),
        };
        let run = |imports| {
            let rules = vec![rule(target.clone())];
            let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules);
            let _ = operation.first();
            operation.next(inputs_answer(GroupRoutingInputs {
                backend_ids: BTreeSet::from([folder]),
                folder_ids: BTreeSet::from([folder]),
                imports,
                ..Default::default()
            }))
        };

        assert_eq!(
            run(Vec::new()),
            Err(PutRoutingError::InvalidRules(RoutingError::FolderTarget(
                folder
            )))
        );
        assert!(reads_bucket(&run(vec![import])));
    }

    #[test]
    fn accepts_owned_backend() {
        let owned = Ulid::from_bytes([4u8; 16]);
        let rules = vec![rule(RoutingTarget::Backend(BackendRef::Group(owned)))];
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules);

        assert!(reads_bucket(&loaded(
            &mut operation,
            BTreeSet::from([owned])
        )));
    }

    #[test]
    fn rejects_foreign_bucket() {
        // The loaded ids belong to the authorized group, so a record that moved
        // to another group must not take its rules.
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), Vec::new());
        let _ = loaded(&mut operation, BTreeSet::new());
        let mut moved = bucket();
        moved.group_id = Ulid::from_bytes([2u8; 16]);

        let turn = operation.next(read(Some(&moved)));

        assert_eq!(turn, Err(PutRoutingError::GroupMismatch));
    }

    #[test]
    fn missing_bucket_fails() {
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), Vec::new());
        let _ = loaded(&mut operation, BTreeSet::new());

        assert_eq!(
            operation.next(read(None)),
            Err(PutRoutingError::NoSuchBucket)
        );
    }

    #[test]
    fn rejects_unexpected_result() {
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), Vec::new());
        let _ = loaded(&mut operation, BTreeSet::new());

        assert!(matches!(
            operation.next(IoResult::Done),
            Err(PutRoutingError::Io(_))
        ));
    }

    #[test]
    fn reads_stored_rules() {
        let rules = vec![rule(RoutingTarget::Class("cold".to_string()))];
        let mut info = bucket();
        info.storage_routing = rules.clone();
        let mut operation = GetRoutingOperation::new("b".to_string());
        let _ = operation.first();

        assert_eq!(operation.next(read(Some(&info))), Ok(Next::Done(rules)));
    }

    #[test]
    fn missing_bucket_errors() {
        let mut operation = GetRoutingOperation::new("b".to_string());
        let _ = operation.first();

        assert_eq!(
            operation.next(read(None)),
            Err(GetRoutingError::NoSuchBucket)
        );
    }
}
