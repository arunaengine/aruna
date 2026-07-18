use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{SYNC_RELATIONSHIP_IN_KEYSPACE, SYNC_RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{SyncRelationship, sync_relationship_key, sync_relationship_prefix};
use aruna_core::types::{Effects, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const RELATIONSHIP_PAGE_SIZE: usize = 128;
const CREATE_RELATIONSHIP_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRelationshipDirection {
    Outgoing,
    Incoming,
}

impl SyncRelationshipDirection {
    fn keyspace(self) -> &'static str {
        match self {
            Self::Outgoing => SYNC_RELATIONSHIP_OUT_KEYSPACE,
            Self::Incoming => SYNC_RELATIONSHIP_IN_KEYSPACE,
        }
    }

    fn bucket(self, relationship: &SyncRelationship) -> Option<&str> {
        match self {
            Self::Outgoing => relationship.source.bucket(),
            Self::Incoming => relationship.target.bucket(),
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum SyncRelationshipError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("sync relationship not found")]
    NotFound,
    #[error("sync relationship already exists")]
    Duplicate,
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    UnexpectedEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
    #[error("sync relationship operation did not finish")]
    NotFinished,
}

pub async fn create_sync_relationship(
    storage: &StorageHandle,
    relationship: SyncRelationship,
) -> Result<SyncRelationship, SyncRelationshipError> {
    let bucket = relationship.source.bucket().ok_or_else(|| {
        ConversionError::FromStrError("sync relationship endpoint is not an S3 ARN".to_string())
    })?;
    let prefix = ByteView::from(sync_relationship_prefix(bucket));
    let key = storage_key(SyncRelationshipDirection::Outgoing, &relationship)?;
    let value = ByteView::from(relationship.to_bytes()?);

    for attempt in 0..CREATE_RELATIONSHIP_ATTEMPTS {
        let txn_id = match start_create_txn(storage).await {
            Err(SyncRelationshipError::Storage(StorageError::TransactionConflict))
                if attempt + 1 < CREATE_RELATIONSHIP_ATTEMPTS =>
            {
                continue;
            }
            result => result?,
        };
        if let Err(error) =
            create_relationship_once(storage, &relationship, &prefix, &key, &value, txn_id).await
        {
            abort_create_txn(storage, txn_id).await;
            return Err(error);
        }

        match storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => return Ok(relationship),
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) if attempt + 1 < CREATE_RELATIONSHIP_ATTEMPTS => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            received => {
                abort_create_txn(storage, txn_id).await;
                return Err(SyncRelationshipError::UnexpectedEvent {
                    state: "CommittingCreate",
                    expected: "storage transaction committed",
                    received,
                });
            }
        }
    }

    Err(StorageError::TransactionConflict.into())
}

async fn create_relationship_once(
    storage: &StorageHandle,
    relationship: &SyncRelationship,
    prefix: &Key,
    key: &Key,
    value: &Value,
    txn_id: TxnId,
) -> Result<(), SyncRelationshipError> {
    let mut start = None;
    loop {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start.map(IterStart::After),
                limit: RELATIONSHIP_PAGE_SIZE,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                if parse_values(SyncRelationshipDirection::Outgoing, values)?
                    .iter()
                    .any(|existing| same_create_identity(existing, relationship))
                {
                    return Err(SyncRelationshipError::Duplicate);
                }
                let Some(next_start_after) = next_start_after else {
                    break;
                };
                start = Some(next_start_after);
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            received => {
                return Err(SyncRelationshipError::UnexpectedEvent {
                    state: "ScanningCreate",
                    expected: "storage iteration result",
                    received,
                });
            }
        }
    }

    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
            key: key.clone(),
            value: value.clone(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        received => Err(SyncRelationshipError::UnexpectedEvent {
            state: "WritingCreate",
            expected: "storage write result",
            received,
        }),
    }
}

async fn start_create_txn(storage: &StorageHandle) -> Result<TxnId, SyncRelationshipError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        received => Err(SyncRelationshipError::UnexpectedEvent {
            state: "StartingCreate",
            expected: "storage transaction started",
            received,
        }),
    }
}

async fn abort_create_txn(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

fn same_create_identity(left: &SyncRelationship, right: &SyncRelationship) -> bool {
    left.source == right.source && left.target == right.target && left.mode == right.mode
}

fn storage_key(
    direction: SyncRelationshipDirection,
    relationship: &SyncRelationship,
) -> Result<Key, ConversionError> {
    let bucket = direction.bucket(relationship).ok_or_else(|| {
        ConversionError::FromStrError("sync relationship endpoint is not an S3 ARN".to_string())
    })?;
    Ok(ByteView::from(sync_relationship_key(
        bucket,
        relationship.id,
    )))
}

fn iter_effect(
    direction: SyncRelationshipDirection,
    bucket: Option<&str>,
    start_after: Option<Key>,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: direction.keyspace().to_string(),
        prefix: bucket.map(sync_relationship_prefix).map(ByteView::from),
        start: start_after.map(IterStart::After),
        limit: RELATIONSHIP_PAGE_SIZE,
        txn_id: None,
    })
}

fn parse_values(
    direction: SyncRelationshipDirection,
    values: Vec<(Key, Value)>,
) -> Result<Vec<SyncRelationship>, ConversionError> {
    values
        .into_iter()
        .map(|(key, value)| {
            let relationship = SyncRelationship::from_bytes(value.as_ref())?;
            if storage_key(direction, &relationship)? != key {
                return Err(ConversionError::FromStrError(
                    "sync relationship key does not match payload".to_string(),
                ));
            }
            Ok(relationship)
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreState {
    Init,
    Writing,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct StoreSyncRelationshipOperation {
    relationship: SyncRelationship,
    direction: SyncRelationshipDirection,
    state: StoreState,
    output: Option<Result<SyncRelationship, SyncRelationshipError>>,
}

impl StoreSyncRelationshipOperation {
    pub fn new(relationship: SyncRelationship, direction: SyncRelationshipDirection) -> Self {
        Self {
            relationship,
            direction,
            state: StoreState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: SyncRelationshipError) -> Effects {
        self.state = StoreState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for StoreSyncRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        let value = match self.relationship.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        let key = match storage_key(self.direction, &self.relationship) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };

        self.state = StoreState::Writing;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: self.direction.keyspace().to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state {
            StoreState::Writing => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.state = StoreState::Finish;
                    self.output = Some(Ok(self.relationship.clone()));
                    smallvec![]
                }
                received => self.fail(SyncRelationshipError::UnexpectedEvent {
                    state: "Writing",
                    expected: "storage write result",
                    received,
                }),
            },
            StoreState::Init => self.fail(SyncRelationshipError::UnexpectedEvent {
                state: "Init",
                expected: "operation start",
                received: event,
            }),
            StoreState::Finish | StoreState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, StoreState::Finish | StoreState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SyncRelationshipError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListState {
    Init,
    Listing,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ListSyncRelationshipsOperation {
    direction: SyncRelationshipDirection,
    bucket: Option<String>,
    state: ListState,
    relationships: Vec<SyncRelationship>,
    output: Option<Result<Vec<SyncRelationship>, SyncRelationshipError>>,
}

impl ListSyncRelationshipsOperation {
    pub fn new(direction: SyncRelationshipDirection, bucket: Option<String>) -> Self {
        Self {
            direction,
            bucket,
            state: ListState::Init,
            relationships: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: SyncRelationshipError) -> Effects {
        self.state = ListState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ListSyncRelationshipsOperation {
    type Output = Vec<SyncRelationship>;
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        self.state = ListState::Listing;
        smallvec![iter_effect(self.direction, self.bucket.as_deref(), None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state {
            ListState::Listing => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return self.fail(SyncRelationshipError::UnexpectedEvent {
                        state: "Listing",
                        expected: "storage iteration result",
                        received: event,
                    });
                };

                let relationships = match parse_values(self.direction, values) {
                    Ok(relationships) => relationships,
                    Err(error) => return self.fail(error.into()),
                };
                self.relationships.extend(relationships);

                if let Some(start_after) = next_start_after {
                    return smallvec![iter_effect(
                        self.direction,
                        self.bucket.as_deref(),
                        Some(start_after)
                    )];
                }

                self.state = ListState::Finish;
                self.output = Some(Ok(std::mem::take(&mut self.relationships)));
                smallvec![]
            }
            ListState::Init => self.fail(SyncRelationshipError::UnexpectedEvent {
                state: "Init",
                expected: "operation start",
                received: event,
            }),
            ListState::Finish | ListState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListState::Finish | ListState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SyncRelationshipError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetState {
    Init,
    Listing,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct GetSyncRelationshipOperation {
    id: Ulid,
    direction: SyncRelationshipDirection,
    state: GetState,
    output: Option<Result<SyncRelationship, SyncRelationshipError>>,
}

impl GetSyncRelationshipOperation {
    pub fn new(id: Ulid, direction: SyncRelationshipDirection) -> Self {
        Self {
            id,
            direction,
            state: GetState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: SyncRelationshipError) -> Effects {
        self.state = GetState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for GetSyncRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        self.state = GetState::Listing;
        smallvec![iter_effect(self.direction, None, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state {
            GetState::Listing => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return self.fail(SyncRelationshipError::UnexpectedEvent {
                        state: "Listing",
                        expected: "storage iteration result",
                        received: event,
                    });
                };

                let relationships = match parse_values(self.direction, values) {
                    Ok(relationships) => relationships,
                    Err(error) => return self.fail(error.into()),
                };
                if let Some(relationship) = relationships
                    .into_iter()
                    .find(|relationship| relationship.id == self.id)
                {
                    self.state = GetState::Finish;
                    self.output = Some(Ok(relationship));
                    return smallvec![];
                }

                if let Some(start_after) = next_start_after {
                    return smallvec![iter_effect(self.direction, None, Some(start_after))];
                }

                self.fail(SyncRelationshipError::NotFound)
            }
            GetState::Init => self.fail(SyncRelationshipError::UnexpectedEvent {
                state: "Init",
                expected: "operation start",
                received: event,
            }),
            GetState::Finish | GetState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetState::Finish | GetState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SyncRelationshipError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeleteState {
    Init,
    Deleting,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct DeleteSyncRelationshipOperation {
    relationship: SyncRelationship,
    direction: SyncRelationshipDirection,
    state: DeleteState,
    output: Option<Result<(), SyncRelationshipError>>,
}

impl DeleteSyncRelationshipOperation {
    pub fn new(relationship: SyncRelationship, direction: SyncRelationshipDirection) -> Self {
        Self {
            relationship,
            direction,
            state: DeleteState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: SyncRelationshipError) -> Effects {
        self.state = DeleteState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for DeleteSyncRelationshipOperation {
    type Output = ();
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.relationship.validate() {
            return self.fail(error.into());
        }
        let key = match storage_key(self.direction, &self.relationship) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };

        self.state = DeleteState::Deleting;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: self.direction.keyspace().to_string(),
            key,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state {
            DeleteState::Deleting => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.state = DeleteState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                received => self.fail(SyncRelationshipError::UnexpectedEvent {
                    state: "Deleting",
                    expected: "storage delete result",
                    received,
                }),
            },
            DeleteState::Init => self.fail(SyncRelationshipError::UnexpectedEvent {
                state: "Init",
                expected: "operation start",
                received: event,
            }),
            DeleteState::Finish | DeleteState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, DeleteState::Finish | DeleteState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SyncRelationshipError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::{ArunaArn, RealmId, SyncMode, SyncState, SyncStatusSnapshot};
    use aruna_core::{NodeId, UserId};
    use aruna_storage::storage::FjallStorage;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};

    fn test_context() -> (TempDir, DriverContext) {
        let tempdir = tempdir().unwrap();
        let storage_handle = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (tempdir, context)
    }

    fn test_node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn relationship(id: u8, source: &str, target: &str) -> SyncRelationship {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut id_bytes = [0u8; 16];
        id_bytes[15] = id;
        SyncRelationship {
            id: Ulid::from_bytes(id_bytes),
            source: ArunaArn::s3_object_prefix(realm_id, test_node(2), source, "selected/")
                .unwrap(),
            target: ArunaArn::s3_object_prefix(realm_id, test_node(3), target, "replica/").unwrap(),
            mode: SyncMode::Continuous,
            replicate_deletes: true,
            created_by: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    #[tokio::test]
    async fn outgoing_crud() {
        let (_tempdir, context) = test_context();
        let first = relationship(1, "source-a", "target-a");
        let second = relationship(2, "source-b", "target-a");

        assert_eq!(
            drive(
                StoreSyncRelationshipOperation::new(
                    first.clone(),
                    SyncRelationshipDirection::Outgoing,
                ),
                &context,
            )
            .await
            .unwrap(),
            first
        );
        drive(
            StoreSyncRelationshipOperation::new(
                second.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &context,
        )
        .await
        .unwrap();

        let listed = drive(
            ListSyncRelationshipsOperation::new(
                SyncRelationshipDirection::Outgoing,
                Some("source-a".to_string()),
            ),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(listed, vec![first.clone()]);

        let fetched = drive(
            GetSyncRelationshipOperation::new(first.id, SyncRelationshipDirection::Outgoing),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(fetched, first);

        drive(
            DeleteSyncRelationshipOperation::new(
                first.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(
            drive(
                GetSyncRelationshipOperation::new(first.id, SyncRelationshipDirection::Outgoing,),
                &context,
            )
            .await,
            Err(SyncRelationshipError::NotFound)
        );

        let remaining = drive(
            ListSyncRelationshipsOperation::new(SyncRelationshipDirection::Outgoing, None),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(remaining, vec![second]);
    }

    #[tokio::test]
    async fn incoming_crud() {
        let (_tempdir, context) = test_context();
        let relationships = (1..=129)
            .map(|id| relationship(id, "source", "target"))
            .collect::<Vec<_>>();

        for relationship in &relationships {
            drive(
                StoreSyncRelationshipOperation::new(
                    relationship.clone(),
                    SyncRelationshipDirection::Incoming,
                ),
                &context,
            )
            .await
            .unwrap();
        }

        let listed = drive(
            ListSyncRelationshipsOperation::new(
                SyncRelationshipDirection::Incoming,
                Some("target".to_string()),
            ),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(listed, relationships);

        let last = relationships.last().unwrap();
        let fetched = drive(
            GetSyncRelationshipOperation::new(last.id, SyncRelationshipDirection::Incoming),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(&fetched, last);

        drive(
            DeleteSyncRelationshipOperation::new(last.clone(), SyncRelationshipDirection::Incoming),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(
            drive(
                GetSyncRelationshipOperation::new(last.id, SyncRelationshipDirection::Incoming,),
                &context,
            )
            .await,
            Err(SyncRelationshipError::NotFound)
        );
    }

    #[tokio::test]
    async fn create_is_atomic() {
        // Every duplicate race must commit one row and reject the losing transaction.
        let (_tempdir, context) = test_context();

        for attempt in 0..16u8 {
            let source = format!("source-{attempt}");
            let target = format!("target-{attempt}");
            let first = relationship(attempt * 2 + 1, &source, &target);
            let second = relationship(attempt * 2 + 2, &source, &target);

            let (first_result, second_result) = tokio::join!(
                create_sync_relationship(&context.storage_handle, first),
                create_sync_relationship(&context.storage_handle, second),
            );
            assert!(matches!(
                (&first_result, &second_result),
                (Ok(_), Err(SyncRelationshipError::Duplicate))
                    | (Err(SyncRelationshipError::Duplicate), Ok(_))
            ));

            let stored = drive(
                ListSyncRelationshipsOperation::new(
                    SyncRelationshipDirection::Outgoing,
                    Some(source),
                ),
                &context,
            )
            .await
            .unwrap();
            assert_eq!(stored.len(), 1);
        }
    }
}
