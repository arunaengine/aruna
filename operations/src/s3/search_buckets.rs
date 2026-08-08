use std::collections::{HashMap, VecDeque};
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
use crate::driver::{DriverContext, drive};
use crate::request_policy::{
    PolicyEnforcementError, PolicyEvaluator, PolicyRequestExtras, policy_request_with,
};

#[derive(Clone, Debug, PartialEq)]
pub struct SearchBucketsInput {
    pub auth: AuthContext,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub query: String,
    pub limit: usize,
    /// Resumes the raw bucket scan after this key so a filtering caller can fill
    /// one page across several drives.
    pub start_after: Option<Key>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchBucketsOutput {
    pub hits: Vec<BucketSearchHit>,
    /// Scan position after the last decided bucket, or `None` once the keyspace
    /// is exhausted.
    pub next_start_after: Option<Key>,
    /// Number of raw bucket rows consumed while producing this page.
    pub scanned_rows: usize,
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
    #[error(transparent)]
    Policy(#[from] PolicyEnforcementError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("bucket search did not finish")]
    NotFinished,
    #[error("bucket search scan budget exhausted")]
    Unavailable,
}

#[derive(Debug, PartialEq)]
pub struct SearchBucketsOperation {
    input: SearchBucketsInput,
    state: SearchBucketsState,
    candidates: VecDeque<BucketCandidate>,
    current: Option<BucketCandidate>,
    hits: Vec<BucketSearchHit>,
    next_start_after: Option<Key>,
    last_decided: Option<Key>,
    max_scan_rows: usize,
    scanned_rows: usize,
    output: Option<Result<SearchBucketsOutput, SearchBucketsError>>,
}

impl SearchBucketsOperation {
    const MAX_LIMIT: usize = 50;
    const SCAN_LIMIT: usize = 1_000;
    const MAX_SCAN_ROWS: usize = 1_024;

    pub fn new(input: SearchBucketsInput) -> Self {
        Self::with_budget(input, Self::MAX_SCAN_ROWS)
    }

    fn with_budget(mut input: SearchBucketsInput, max_scan_rows: usize) -> Self {
        input.limit = input.limit.clamp(1, Self::MAX_LIMIT);
        input.query = input.query.to_lowercase();
        let next_start_after = input.start_after.clone();
        Self {
            input,
            state: SearchBucketsState::Init,
            candidates: VecDeque::new(),
            current: None,
            hits: Vec::new(),
            next_start_after,
            last_decided: None,
            max_scan_rows: max_scan_rows.min(Self::MAX_SCAN_ROWS),
            scanned_rows: 0,
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
        let Some(remaining) = self.max_scan_rows.checked_sub(self.scanned_rows) else {
            return self.fail(SearchBucketsError::Unavailable);
        };
        if remaining == 0 {
            return self.fail(SearchBucketsError::Unavailable);
        }
        self.state = SearchBucketsState::ScanBuckets;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            prefix: None,
            start: self.next_start_after.clone().map(IterStart::After),
            limit: Self::SCAN_LIMIT.min(remaining),
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
        let remaining = self
            .max_scan_rows
            .checked_sub(self.scanned_rows)
            .ok_or(SearchBucketsError::Unavailable)?;
        if values.len() > remaining {
            return Err(SearchBucketsError::Unavailable);
        }
        self.scanned_rows += values.len();
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
            self.last_decided = Some(candidate.bucket.as_bytes().into());
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
        // Candidates left in the queue were scanned past but never decided, so
        // the continuation is the last decided bucket rather than the batch end.
        let next_start_after = if self.candidates.is_empty() {
            self.next_start_after.take()
        } else {
            self.last_decided.take()
        };
        self.output = Some(Ok(SearchBucketsOutput {
            hits: std::mem::take(&mut self.hits),
            next_start_after,
            scanned_rows: self.scanned_rows,
        }));
        smallvec![]
    }
}

/// Runs one node's bucket search, dropping hits denied by realm/group policies.
/// Policy state is read once per candidate group; unreadable groups stay invisible,
/// and scans continue so hidden buckets cannot shorten the page.
pub async fn search_local_buckets(
    context: &DriverContext,
    input: SearchBucketsInput,
) -> Result<Vec<BucketSearchHit>, SearchBucketsError> {
    search_with_budget(context, input, SearchBucketsOperation::MAX_SCAN_ROWS).await
}

async fn search_with_budget(
    context: &DriverContext,
    input: SearchBucketsInput,
    max_scan_rows: usize,
) -> Result<Vec<BucketSearchHit>, SearchBucketsError> {
    let limit = input.limit.clamp(1, SearchBucketsOperation::MAX_LIMIT);
    let mut evaluators: HashMap<(RealmId, GroupId), PolicyEvaluator> = HashMap::new();
    let mut visible: Vec<BucketSearchHit> = Vec::with_capacity(limit);
    let mut start_after = input.start_after.clone();
    let mut scanned_rows = 0;
    loop {
        let remaining = max_scan_rows
            .min(SearchBucketsOperation::MAX_SCAN_ROWS)
            .checked_sub(scanned_rows)
            .ok_or(SearchBucketsError::Unavailable)?;
        if remaining == 0 {
            return Err(SearchBucketsError::Unavailable);
        }
        let output = drive(
            SearchBucketsOperation::with_budget(
                SearchBucketsInput {
                    start_after,
                    ..input.clone()
                },
                remaining,
            ),
            context,
        )
        .await?;
        scanned_rows += output.scanned_rows;
        let pending = output
            .hits
            .iter()
            .map(|hit| hit.group_id)
            .filter(|group_id| !evaluators.contains_key(&(input.realm_id, *group_id)))
            .map(|group_id| (input.realm_id, group_id))
            .collect::<Vec<_>>();
        evaluators.extend(PolicyEvaluator::load_bulk(context, pending).await?);
        for hit in output.hits {
            if !policy_allows(&evaluators, &input, &hit) {
                continue;
            }
            visible.push(hit);
            if visible.len() >= limit {
                return Ok(visible);
            }
        }
        match output.next_start_after {
            Some(key) => start_after = Some(key),
            None => return Ok(visible),
        }
    }
}

/// Evaluates the loaded policies for one hit on the path the RBAC check used,
/// under the S3 bucket-read action so one policy covers both read surfaces. A
/// group with no loaded evaluator fails closed.
fn policy_allows(
    evaluators: &HashMap<(RealmId, GroupId), PolicyEvaluator>,
    input: &SearchBucketsInput,
    hit: &BucketSearchHit,
) -> bool {
    let path = blob_bucket_permission_path(input.realm_id, hit.group_id, hit.node_id, &hit.bucket);
    let request = policy_request_with(
        &path,
        &Permission::READ,
        Some(&input.auth.user_id),
        PolicyRequestExtras::operation("s3.ListBuckets"),
    );
    evaluators
        .get(&(input.realm_id, hit.group_id))
        .is_some_and(|evaluator| evaluator.evaluate(&request).is_ok())
}

impl Operation for SearchBucketsOperation {
    type Output = SearchBucketsOutput;
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

    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, RealmAuthorizationDocument, RealmConfigDocument, Role,
    };
    use aruna_core::{UserId, structs::BucketInfo};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    use super::*;

    fn scan_input(query: &str, limit: usize) -> SearchBucketsInput {
        let realm_id = RealmId::from_bytes([9u8; 32]);
        SearchBucketsInput {
            auth: AuthContext {
                user_id: UserId::nil(realm_id),
                realm_id,
                path_restrictions: None,
            },
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[10u8; 32]).public(),
            query: query.to_string(),
            limit,
            start_after: None,
        }
    }

    fn bucket_entry(bucket: &str, group_id: GroupId) -> (Key, Value) {
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(realm_id),
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
        };
        (
            bucket.as_bytes().to_vec().into(),
            info.to_bytes().unwrap().into(),
        )
    }

    #[test]
    fn scan_budget_fails() {
        let mut operation = SearchBucketsOperation::new(scan_input("missing", 1));
        operation.start();
        let mut remaining = SearchBucketsOperation::MAX_SCAN_ROWS;
        let mut page = 0;
        while remaining > 0 {
            let page_len = SearchBucketsOperation::SCAN_LIMIT.min(remaining);
            let values = (0..page_len)
                .map(|row| {
                    (
                        format!("other-{page:02}-{row:04}").into_bytes().into(),
                        Vec::<u8>::new().into(),
                    )
                })
                .collect();
            operation.step(Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after: Some(format!("cursor-{page}").into()),
            }));
            remaining -= page_len;
            page += 1;
        }

        assert_eq!(operation.finalize(), Err(SearchBucketsError::Unavailable));
    }

    #[test]
    fn cursor_after_hit() {
        let group_id = Ulid::from_bytes([11u8; 16]);
        let mut operation = SearchBucketsOperation::new(scan_input("data", 1));
        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                bucket_entry("data-a", group_id),
                bucket_entry("data-b", group_id),
            ],
            next_start_after: Some("data-b".as_bytes().to_vec().into()),
        }));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));

        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: group_id.to_bytes().to_vec().into(),
            value: None,
        }));

        let output = operation.finalize().unwrap();
        assert_eq!(output.hits.len(), 1);
        assert_eq!(output.hits[0].bucket, "data-a");
        assert_eq!(
            output.next_start_after,
            Some("data-a".as_bytes().to_vec().into())
        );
        assert_eq!(output.scanned_rows, 2);
    }

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
        // Bulk policy loading fails closed without the realm config document.
        write_value(
            &context,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
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
                let role_id = Ulid::generate();
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
                    replication: None,
                    storage_routing: Vec::new(),
                }
                .to_bytes()
                .unwrap(),
            )
            .await;
        }

        let hits = search_local_buckets(
            &context,
            SearchBucketsInput {
                auth: AuthContext {
                    user_id: outsider,
                    realm_id,
                    path_restrictions: None,
                },
                realm_id,
                node_id,
                query: "DATA".to_string(),
                limit: 50,
                start_after: None,
            },
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
