use aruna_core::credential_seal::CredentialSealKey;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ASSISTANT_PROVIDER_KEYSPACE, ASSISTANT_PROVIDER_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AssistantHeaders, AssistantProvider, AssistantProviderSecret, AssistantSecretError,
};
use aruna_core::types::{Effects, Key, TxnId, UserId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

fn owner_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, ConversionError> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    let index: BTreeSet<String> = postcard::from_bytes(value.as_ref())?;
    for provider_id in &index {
        Ulid::from_string(provider_id)?;
    }
    Ok(index)
}

fn encode_index(index: &BTreeSet<String>) -> Result<Value, ConversionError> {
    Ok(ByteView::from(postcard::to_allocvec(index)?))
}

#[derive(Debug, Error)]
pub enum ProviderStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Secret(#[from] AssistantSecretError),
    #[error("provider not found")]
    NotFound,
    #[error("provider id collision")]
    IdCollision,
    #[error("provider owner index is inconsistent")]
    IndexInconsistent,
    #[error("provider operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

fn unexpected(
    state: &impl std::fmt::Debug,
    expected: &'static str,
    event: &Event,
) -> ProviderStoreError {
    ProviderStoreError::UnexpectedEvent {
        state: format!("{state:?}"),
        expected,
        got: format!("{event:?}"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CreateProviderState {
    Init,
    StartTransaction,
    ReadOwnerIndex,
    ReadProvider { index: BTreeSet<String> },
    WriteProvider,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct CreateProviderOperation {
    provider: AssistantProvider,
    pending_secret: AssistantProviderSecret,
    pending_headers: AssistantHeaders,
    seal_key: CredentialSealKey,
    txn_id: Option<TxnId>,
    state: CreateProviderState,
    output: Option<Result<AssistantProvider, String>>,
}

impl CreateProviderOperation {
    pub fn new(
        provider: AssistantProvider,
        secret: AssistantProviderSecret,
        headers: AssistantHeaders,
        seal_key: CredentialSealKey,
    ) -> Self {
        Self {
            provider,
            pending_secret: secret,
            pending_headers: headers,
            seal_key,
            txn_id: None,
            state: CreateProviderState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ProviderStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = CreateProviderState::Error;
        self.output = Some(Err(error.to_string()));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = CreateProviderState::ReadOwnerIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: ASSISTANT_PROVIDER_OWNER_KEYSPACE.to_string(),
            key: owner_key(self.provider.user_id),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_index(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "owner index read", &event));
        };
        let mut index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.fail(error.into()),
        };
        if !index.insert(self.provider.provider_id.clone()) {
            return self.fail(ProviderStoreError::IdCollision);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = CreateProviderState::ReadProvider {
            index: index.clone(),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: ASSISTANT_PROVIDER_KEYSPACE.to_string(),
            key: self.provider.provider_id.as_bytes().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_provider(&mut self, event: Event, index: BTreeSet<String>) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "provider read", &event));
        };
        if value.is_some() {
            return self.fail(ProviderStoreError::IdCollision);
        }
        if let Err(error) = self
            .provider
            .seal_secret(&self.seal_key, &self.pending_secret)
            .and_then(|_| {
                self.provider
                    .seal_headers(&self.seal_key, &self.pending_headers)
            })
        {
            return self.fail(error.into());
        }
        let provider_bytes = match self.provider.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        let index_bytes = match encode_index(&index) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = CreateProviderState::WriteProvider;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    ASSISTANT_PROVIDER_KEYSPACE.to_string(),
                    self.provider.provider_id.as_bytes().into(),
                    provider_bytes.into(),
                ),
                (
                    ASSISTANT_PROVIDER_OWNER_KEYSPACE.to_string(),
                    owner_key(self.provider.user_id),
                    index_bytes,
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "provider write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.output = Some(Ok(self.provider.clone()));
        self.state = CreateProviderState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = CreateProviderState::Finish;
        smallvec![]
    }
}

impl Operation for CreateProviderOperation {
    type Output = AssistantProvider;
    type Error = ProviderStoreError;

    fn start(&mut self) -> Effects {
        if let Err(error) = Ulid::from_string(&self.provider.provider_id) {
            return self.fail(ConversionError::from(error).into());
        }
        self.state = CreateProviderState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state.clone() {
            CreateProviderState::Init => self.start(),
            CreateProviderState::StartTransaction => self.handle_started(event),
            CreateProviderState::ReadOwnerIndex => self.handle_index(event),
            CreateProviderState::ReadProvider { index } => self.handle_provider(event, index),
            CreateProviderState::WriteProvider => self.handle_written(event),
            CreateProviderState::CommitTransaction => self.handle_committed(event),
            CreateProviderState::Finish | CreateProviderState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateProviderState::Finish | CreateProviderState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(provider)) => Ok(provider),
            Some(Err(error)) if error == ProviderStoreError::IdCollision.to_string() => {
                Err(ProviderStoreError::IdCollision)
            }
            Some(Err(error)) => Err(ProviderStoreError::UnexpectedEvent {
                state: "Error".to_string(),
                expected: "successful provider write",
                got: error,
            }),
            None => Err(ProviderStoreError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadProviderState {
    Init,
    Read,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct GetProviderOperation {
    provider_id: String,
    user_id: UserId,
    state: ReadProviderState,
    output: Option<Result<Option<AssistantProvider>, String>>,
}

impl GetProviderOperation {
    pub fn new(provider_id: String, user_id: UserId) -> Self {
        Self {
            provider_id,
            user_id,
            state: ReadProviderState::Init,
            output: None,
        }
    }
}

impl Operation for GetProviderOperation {
    type Output = Option<AssistantProvider>;
    type Error = ProviderStoreError;

    fn start(&mut self) -> Effects {
        if let Err(error) = Ulid::from_string(&self.provider_id) {
            self.state = ReadProviderState::Error;
            self.output = Some(Err(error.to_string()));
            return smallvec![];
        }
        self.state = ReadProviderState::Read;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: ASSISTANT_PROVIDER_KEYSPACE.to_string(),
            key: self.provider_id.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. })
                if self.state == ReadProviderState::Read =>
            {
                let provider = value
                    .as_ref()
                    .map(|value| AssistantProvider::from_bytes(value.as_ref()))
                    .transpose();
                match provider {
                    Ok(Some(provider)) if provider.user_id == self.user_id => {
                        self.output = Some(Ok(Some(provider)));
                        self.state = ReadProviderState::Finish;
                    }
                    Ok(_) => {
                        self.output = Some(Ok(None));
                        self.state = ReadProviderState::Finish;
                    }
                    Err(error) => {
                        self.output = Some(Err(error.to_string()));
                        self.state = ReadProviderState::Error;
                    }
                }
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.output = Some(Err(error.to_string()));
                self.state = ReadProviderState::Error;
                smallvec![]
            }
            _ => {
                self.output = Some(Err("unexpected provider read event".to_string()));
                self.state = ReadProviderState::Error;
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReadProviderState::Finish | ReadProviderState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(provider)) => Ok(provider),
            Some(Err(error)) => Err(ProviderStoreError::UnexpectedEvent {
                state: "Error".to_string(),
                expected: "provider read",
                got: error,
            }),
            None => Err(ProviderStoreError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ListProviderState {
    Init,
    StartTransaction,
    ReadOwnerIndex,
    ReadProviders,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ListProviderOperation {
    user_id: UserId,
    provider_ids: Vec<String>,
    providers: Vec<AssistantProvider>,
    txn_id: Option<TxnId>,
    state: ListProviderState,
    output: Option<Result<Vec<AssistantProvider>, String>>,
}

impl ListProviderOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            provider_ids: Vec::new(),
            providers: Vec::new(),
            txn_id: None,
            state: ListProviderState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: impl ToString) -> Effects {
        let cleanup = self.abort();
        self.output = Some(Err(error.to_string()));
        self.state = ListProviderState::Error;
        cleanup
    }
}

impl Operation for ListProviderOperation {
    type Output = Vec<AssistantProvider>;
    type Error = ProviderStoreError;

    fn start(&mut self) -> Effects {
        self.state = ListProviderState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error);
        }
        match self.state {
            ListProviderState::Init => self.start(),
            ListProviderState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail("unexpected provider transaction event");
                };
                self.txn_id = Some(txn_id);
                self.state = ListProviderState::ReadOwnerIndex;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ASSISTANT_PROVIDER_OWNER_KEYSPACE.to_string(),
                    key: owner_key(self.user_id),
                    txn_id: Some(txn_id),
                })]
            }
            ListProviderState::ReadOwnerIndex => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail("unexpected provider index event");
                };
                let index = match decode_index(value.as_ref()) {
                    Ok(index) => index,
                    Err(error) => return self.fail(error),
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StorageError::TransactionNotFound);
                };
                if index.is_empty() {
                    self.output = Some(Ok(Vec::new()));
                    self.state = ListProviderState::CommitTransaction;
                    return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
                }
                self.provider_ids = index.into_iter().collect();
                self.state = ListProviderState::ReadProviders;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: self
                        .provider_ids
                        .iter()
                        .map(|id| (
                            ASSISTANT_PROVIDER_KEYSPACE.to_string(),
                            id.as_bytes().into()
                        ))
                        .collect(),
                    txn_id: Some(txn_id),
                })]
            }
            ListProviderState::ReadProviders => {
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return self.fail("unexpected provider records event");
                };
                if values.len() != self.provider_ids.len() {
                    return self.fail(ProviderStoreError::IndexInconsistent);
                }
                for (key, value) in values {
                    let Some(value) = value else {
                        return self.fail(ProviderStoreError::IndexInconsistent);
                    };
                    let provider = match AssistantProvider::from_bytes(value.as_ref()) {
                        Ok(provider) => provider,
                        Err(error) => return self.fail(error),
                    };
                    if provider.user_id != self.user_id
                        || provider.provider_id.as_bytes() != key.as_ref()
                        || !self.provider_ids.contains(&provider.provider_id)
                    {
                        return self.fail(ProviderStoreError::IndexInconsistent);
                    }
                    self.providers.push(provider);
                }
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StorageError::TransactionNotFound);
                };
                self.providers.sort_by_key(|provider| provider.created_at);
                self.output = Some(Ok(std::mem::take(&mut self.providers)));
                self.state = ListProviderState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            ListProviderState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail("unexpected provider commit event");
                };
                self.txn_id = None;
                self.state = ListProviderState::Finish;
                smallvec![]
            }
            ListProviderState::Finish | ListProviderState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListProviderState::Finish | ListProviderState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(providers)) => Ok(providers),
            Some(Err(error)) => Err(ProviderStoreError::UnexpectedEvent {
                state: "Error".to_string(),
                expected: "provider list",
                got: error,
            }),
            None => Err(ProviderStoreError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum UpdateProviderState {
    Init,
    StartTransaction,
    ReadProvider,
    WriteProvider,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct UpdateProviderOperation {
    provider: AssistantProvider,
    user_id: UserId,
    txn_id: Option<TxnId>,
    state: UpdateProviderState,
    output: Option<Result<AssistantProvider, String>>,
}

impl UpdateProviderOperation {
    pub fn new(provider: AssistantProvider, user_id: UserId) -> Self {
        Self {
            provider,
            user_id,
            txn_id: None,
            state: UpdateProviderState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: impl ToString) -> Effects {
        let cleanup = self.abort();
        self.output = Some(Err(error.to_string()));
        self.state = UpdateProviderState::Error;
        cleanup
    }
}

impl Operation for UpdateProviderOperation {
    type Output = AssistantProvider;
    type Error = ProviderStoreError;

    fn start(&mut self) -> Effects {
        self.state = UpdateProviderState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error);
        }
        match self.state {
            UpdateProviderState::Init => self.start(),
            UpdateProviderState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail("unexpected provider transaction event");
                };
                self.txn_id = Some(txn_id);
                self.state = UpdateProviderState::ReadProvider;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ASSISTANT_PROVIDER_KEYSPACE.to_string(),
                    key: self.provider.provider_id.as_bytes().into(),
                    txn_id: Some(txn_id),
                })]
            }
            UpdateProviderState::ReadProvider => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail("unexpected provider read event");
                };
                let Some(value) = value else {
                    return self.fail(ProviderStoreError::NotFound);
                };
                let current = match AssistantProvider::from_bytes(value.as_ref()) {
                    Ok(provider) => provider,
                    Err(error) => return self.fail(error),
                };
                if current.user_id != self.user_id
                    || self.provider.user_id != self.user_id
                    || current.provider_id != self.provider.provider_id
                    || current.kind != self.provider.kind
                    || current.created_at != self.provider.created_at
                {
                    return self.fail(ProviderStoreError::NotFound);
                }
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StorageError::TransactionNotFound);
                };
                let bytes = match self.provider.to_bytes() {
                    Ok(bytes) => bytes,
                    Err(error) => return self.fail(error),
                };
                self.state = UpdateProviderState::WriteProvider;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: ASSISTANT_PROVIDER_KEYSPACE.to_string(),
                    key: self.provider.provider_id.as_bytes().into(),
                    value: bytes.into(),
                    txn_id: Some(txn_id),
                })]
            }
            UpdateProviderState::WriteProvider => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail("unexpected provider write event");
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StorageError::TransactionNotFound);
                };
                self.output = Some(Ok(self.provider.clone()));
                self.state = UpdateProviderState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            UpdateProviderState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail("unexpected provider commit event");
                };
                self.txn_id = None;
                self.state = UpdateProviderState::Finish;
                smallvec![]
            }
            UpdateProviderState::Finish | UpdateProviderState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            UpdateProviderState::Finish | UpdateProviderState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(provider)) => Ok(provider),
            Some(Err(error)) if error == ProviderStoreError::NotFound.to_string() => {
                Err(ProviderStoreError::NotFound)
            }
            Some(Err(error)) => Err(ProviderStoreError::UnexpectedEvent {
                state: "Error".to_string(),
                expected: "provider update",
                got: error,
            }),
            None => Err(ProviderStoreError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DeleteProviderState {
    Init,
    StartTransaction,
    ReadProvider,
    ReadOwnerIndex,
    DeleteProvider,
    WriteOwnerIndex,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct DeleteProviderOperation {
    provider_id: String,
    user_id: UserId,
    txn_id: Option<TxnId>,
    state: DeleteProviderState,
    output: Option<Result<(), String>>,
}

impl DeleteProviderOperation {
    pub fn new(provider_id: String, user_id: UserId) -> Self {
        Self {
            provider_id,
            user_id,
            txn_id: None,
            state: DeleteProviderState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: impl ToString) -> Effects {
        let cleanup = self.abort();
        self.output = Some(Err(error.to_string()));
        self.state = DeleteProviderState::Error;
        cleanup
    }
}

impl Operation for DeleteProviderOperation {
    type Output = ();
    type Error = ProviderStoreError;

    fn start(&mut self) -> Effects {
        if Ulid::from_string(&self.provider_id).is_err() {
            self.output = Some(Err(ProviderStoreError::NotFound.to_string()));
            self.state = DeleteProviderState::Error;
            return smallvec![];
        }
        self.state = DeleteProviderState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error);
        }
        match self.state {
            DeleteProviderState::Init => self.start(),
            DeleteProviderState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail("unexpected provider transaction event");
                };
                self.txn_id = Some(txn_id);
                self.state = DeleteProviderState::ReadProvider;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ASSISTANT_PROVIDER_KEYSPACE.to_string(),
                    key: self.provider_id.as_bytes().into(),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteProviderState::ReadProvider => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail("unexpected provider read event");
                };
                let Some(value) = value else {
                    return self.fail(ProviderStoreError::NotFound);
                };
                let provider = match AssistantProvider::from_bytes(value.as_ref()) {
                    Ok(provider) => provider,
                    Err(error) => return self.fail(error),
                };
                if provider.user_id != self.user_id {
                    return self.fail(ProviderStoreError::NotFound);
                }
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StorageError::TransactionNotFound);
                };
                self.state = DeleteProviderState::ReadOwnerIndex;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ASSISTANT_PROVIDER_OWNER_KEYSPACE.to_string(),
                    key: owner_key(self.user_id),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteProviderState::ReadOwnerIndex => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail("unexpected provider index event");
                };
                let mut index = match decode_index(value.as_ref()) {
                    Ok(index) => index,
                    Err(error) => return self.fail(error),
                };
                if !index.remove(&self.provider_id) {
                    return self.fail(ProviderStoreError::IndexInconsistent);
                }
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StorageError::TransactionNotFound);
                };
                let bytes = match encode_index(&index) {
                    Ok(bytes) => bytes,
                    Err(error) => return self.fail(error),
                };
                self.state = DeleteProviderState::DeleteProvider;
                self.output = Some(Ok(()));
                smallvec![
                    Effect::Storage(StorageEffect::Delete {
                        key_space: ASSISTANT_PROVIDER_KEYSPACE.to_string(),
                        key: self.provider_id.as_bytes().into(),
                        txn_id: Some(txn_id),
                    }),
                    Effect::Storage(StorageEffect::Write {
                        key_space: ASSISTANT_PROVIDER_OWNER_KEYSPACE.to_string(),
                        key: owner_key(self.user_id),
                        value: bytes,
                        txn_id: Some(txn_id),
                    }),
                ]
            }
            DeleteProviderState::DeleteProvider => {
                let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
                    return self.fail("unexpected provider delete event");
                };
                self.state = DeleteProviderState::WriteOwnerIndex;
                smallvec![]
            }
            DeleteProviderState::WriteOwnerIndex => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail("unexpected provider index write event");
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StorageError::TransactionNotFound);
                };
                self.state = DeleteProviderState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            DeleteProviderState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail("unexpected provider commit event");
                };
                self.txn_id = None;
                self.state = DeleteProviderState::Finish;
                smallvec![]
            }
            DeleteProviderState::Finish | DeleteProviderState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteProviderState::Finish | DeleteProviderState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(())) if self.state == DeleteProviderState::Finish => Ok(()),
            Some(Err(error)) if error == ProviderStoreError::NotFound.to_string() => {
                Err(ProviderStoreError::NotFound)
            }
            Some(Err(error)) => Err(ProviderStoreError::UnexpectedEvent {
                state: "Error".to_string(),
                expected: "provider delete",
                got: error,
            }),
            _ => Err(ProviderStoreError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}
