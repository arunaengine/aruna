use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{GROUP_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    ArunaArn, AuthContext, BucketInfo, Group, Permission, RealmId, blob_bucket_permission_path,
};
use aruna_core::types::{Effects, GroupId, Key, Value};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct SearchBucketsInput {
    pub auth: AuthContext,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub query: String,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSearchHit {
    pub arn: String,
    pub bucket: String,
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub group_name: Option<String>,
    pub created_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq)]
struct BucketCandidate {
    bucket: String,
    info: BucketInfo,
}

#[derive(Clone, Debug, PartialEq)]
enum SearchBucketsState {
    Init,
    ScanBuckets,
    CheckPermission,
    ReadGroup,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SearchBucketsError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("bucket search did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
pub struct SearchBucketsOperation {
    input: SearchBucketsInput,
    state: SearchBucketsState,
    candidates: VecDeque<BucketCandidate>,
    current: Option<BucketCandidate>,
    hits: Vec<BucketSearchHit>,
    next_start_after: Option<Key>,
    output: Option<Result<Vec<BucketSearchHit>, SearchBucketsError>>,
}

impl SearchBucketsOperation {
    const MAX_LIMIT: usize = 50;
    const SCAN_LIMIT: usize = 1_000;

    pub fn new(mut input: SearchBucketsInput) -> Self {
        input.limit = input.limit.clamp(1, Self::MAX_LIMIT);
        input.query = input.query.to_lowercase();
        Self {
            input,
            state: SearchBucketsState::Init,
            candidates: VecDeque::new(),
            current: None,
            hits: Vec::new(),
            next_start_after: None,
            output: None,
        }
    }

    fn fail(&mut self, error: SearchBucketsError) -> Effects {
        self.state = SearchBucketsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(SearchBucketsError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn emit_bucket_scan(&mut self) -> Effects {
        self.state = SearchBucketsState::ScanBuckets;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            prefix: None,
            start: self.next_start_after.clone().map(IterStart::After),
            limit: Self::SCAN_LIMIT,
            txn_id: None,
        })]
    }

    fn handle_bucket_scan(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected("Event::Storage(StorageEvent::IterResult)", got);
        };
        if let Err(error) = self.collect_candidates(values) {
            return self.fail(error);
        }
        self.next_start_after = next_start_after;
        self.continue_search()
    }

    fn collect_candidates(&mut self, values: Vec<(Key, Value)>) -> Result<(), SearchBucketsError> {
        for (key, value) in values {
            let bucket = String::from_utf8(key.to_vec()).map_err(ConversionError::from)?;
            if bucket.starts_with("ws-") || !bucket.to_lowercase().contains(&self.input.query) {
                continue;
            }
            self.candidates.push_back(BucketCandidate {
                bucket,
                info: BucketInfo::from_bytes(value.as_ref())?,
            });
        }
        Ok(())
    }

    fn continue_search(&mut self) -> Effects {
        if self.hits.len() >= self.input.limit {
            return self.finish();
        }
        if let Some(candidate) = self.candidates.pop_front() {
            self.current = Some(candidate);
            return self.emit_permission_check();
        }
        if self.next_start_after.is_some() {
            return self.emit_bucket_scan();
        }
        self.finish()
    }

    fn emit_permission_check(&mut self) -> Effects {
        let Some(candidate) = self.current.as_ref() else {
            return self.fail(SearchBucketsError::NotFinished);
        };
        self.state = SearchBucketsState::CheckPermission;
        let path = blob_bucket_permission_path(
            self.input.realm_id,
            candidate.info.group_id,
            self.input.node_id,
            &candidate.bucket,
        );
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.input.auth.clone(),
                path,
                required_permission: Permission::READ,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn handle_permission(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected(
                "Event::SubOperation(SubOperationEvent::AuthorizationResult)",
                got,
            );
        };
        if !matches!(allowed, Ok(true)) {
            self.current = None;
            return self.continue_search();
        }
        let Some(candidate) = self.current.as_ref() else {
            return self.fail(SearchBucketsError::NotFinished);
        };
        self.state = SearchBucketsState::ReadGroup;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_KEYSPACE.to_string(),
            key: candidate.info.group_id.to_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn handle_group_read(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("Event::Storage(StorageEvent::ReadResult)", got);
        };
        let group_name = match value {
            Some(value) => match Group::from_bytes(value.as_ref()) {
                Ok(group) => Some(group.display_name),
                Err(error) => return self.fail(error.into()),
            },
            None => None,
        };
        let Some(candidate) = self.current.take() else {
            return self.fail(SearchBucketsError::NotFinished);
        };
        let arn =
            match ArunaArn::s3_bucket(self.input.realm_id, self.input.node_id, &candidate.bucket) {
                Ok(arn) => arn.to_string(),
                Err(error) => return self.fail(error.into()),
            };
        self.hits.push(BucketSearchHit {
            arn,
            bucket: candidate.bucket,
            node_id: self.input.node_id,
            group_id: candidate.info.group_id,
            group_name,
            created_at: candidate.info.created_at,
        });
        self.continue_search()
    }

    fn finish(&mut self) -> Effects {
        self.state = SearchBucketsState::Finish;
        self.output = Some(Ok(std::mem::take(&mut self.hits)));
        smallvec![]
    }
}

impl Operation for SearchBucketsOperation {
    type Output = Vec<BucketSearchHit>;
    type Error = SearchBucketsError;

    fn start(&mut self) -> Effects {
        self.emit_bucket_scan()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            SearchBucketsState::ScanBuckets => self.handle_bucket_scan(event),
            SearchBucketsState::CheckPermission => self.handle_permission(event),
            SearchBucketsState::ReadGroup => self.handle_group_read(event),
            SearchBucketsState::Init | SearchBucketsState::Finish | SearchBucketsState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SearchBucketsState::Finish | SearchBucketsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SearchBucketsError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, RealmAuthorizationDocument, Role,
    };
    use aruna_core::{UserId, structs::BucketInfo};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    use super::*;
    use crate::driver::{DriverContext, drive};

    async fn write_value(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn filters_permissions() {
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let owner = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let outsider = UserId::local(Ulid::from_bytes([6u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        write_value(
            &context,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::new_default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;

        let public_group = Ulid::from_bytes([7u8; 16]);
        let private_group = Ulid::from_bytes([8u8; 16]);
        for (group_id, name, public_bucket) in [
            (public_group, "Public Group", Some("data-public")),
            (private_group, "Private Group", Some("data-private")),
        ] {
            let mut auth =
                GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
            if let Some(bucket) = public_bucket.filter(|_| group_id == public_group) {
                let role_id = Ulid::r#gen();
                auth.roles.insert(
                    role_id,
                    Role {
                        role_id,
                        name: "public-reader".to_string(),
                        permissions: HashMap::from([(
                            blob_bucket_permission_path(realm_id, group_id, node_id, bucket),
                            Permission::READ,
                        )]),
                        assigned_users: HashSet::from([UserId::nil(realm_id)]),
                    },
                );
            }
            write_value(
                &context,
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                auth.to_bytes(&actor).unwrap(),
            )
            .await;
            let group = Group {
                display_name: name.to_string(),
                group_id,
                realm_id,
                roles: auth.roles.keys().copied().collect(),
                owner,
            };
            write_value(
                &context,
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            )
            .await;
        }

        for (bucket, group_id) in [
            ("data-public", public_group),
            ("data-private", private_group),
            ("ws-data-temporary", public_group),
        ] {
            write_value(
                &context,
                S3_BUCKET_KEYSPACE,
                bucket.as_bytes().to_vec(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::UNIX_EPOCH,
                    created_by: owner,
                    cors_configuration: None,
                }
                .to_bytes()
                .unwrap(),
            )
            .await;
        }

        let hits = drive(
            SearchBucketsOperation::new(SearchBucketsInput {
                auth: AuthContext {
                    user_id: outsider,
                    realm_id,
                    path_restrictions: None,
                },
                realm_id,
                node_id,
                query: "DATA".to_string(),
                limit: 50,
            }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].bucket, "data-public");
        assert_eq!(hits[0].group_name.as_deref(), Some("Public Group"));
        assert_eq!(
            hits[0].arn,
            ArunaArn::s3_bucket(realm_id, node_id, "data-public")
                .unwrap()
                .to_string()
        );
    }
}
