//! Publishes queued authoring intents once the realm is reachable again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{DEVICE_FENCE_KEYSPACE, DEVICE_INTAKE_KEYSPACE};
use aruna_core::metadata::{AuthToken, MetadataActor, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::rows::{done, outside_answer, outside_call, read_row, value};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{NodeRealm, RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structured_id::StructuredId;
use aruna_core::types::{Key, Value};
use tracing::{info, warn};
use ulid::Ulid;

use super::edit::{DeviceEditError, read_actor, reject_edit};
use super::publish_queue::{
    ClaimError, ClaimStateOperation, MAX_PUBLISH_ATTEMPTS, PUBLISH_PAGE_SIZE, PublishEntry,
    PublishKind, PublishState, entry_with_state, publish_entry, publish_key,
};
use super::replica::{ReplicaRecord, SelectionError};
use super::replica_store::{read_replica, store_replica};
use super::retry::{exhausted, retry_due_at};
use super::selection::track_created;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::transport::MetadataWriteError;
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentPayload,
    CreateDocumentResult, mint_forward_document,
};
use crate::metadata::forward::route_create::RouteCreateOperation;
use crate::metadata::forward::routed_write::ApplyBatchOperation;
use crate::metadata::update_document::UpdateDocumentError;
use crate::realm::load_config::config_unavailable;

/// Delay before a deferred pass looks for the realm again.
pub const PUBLISH_DEFER_AFTER: Duration = Duration::from_secs(15);

/// Delay between passes while entries are still due.
pub const PUBLISH_CONTINUE_AFTER: Duration = Duration::from_millis(250);

/// What one drain pass achieved, so the caller knows how soon to look again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The pass could not complete; earlier pages may already be forwarded.
    Deferred,
    /// At least one entry asked for another pass; it does not prove entries remain.
    Recheck,
    /// No entry asked for another pass.
    Idle,
}

fn queue_page(after: Option<Key>, limit: usize) -> IoCall {
    outside_call(IoCall::Page {
        space: DEVICE_INTAKE_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after,
        limit,
    })
}

type QueueRows = (Vec<(Key, Value)>, Option<Key>);

fn queue_rows(result: IoResult) -> Result<QueueRows, IoError> {
    outside_answer(result)?.page()
}

/// What the pending call of a drain pass answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DrainStep {
    Config,
    Page,
    Forward,
}

/// Forwards every due entry of the whole queue, oldest first. The scan pages past entries it
/// skips, so published and parked ones cannot starve the tail.
pub fn drain_publish_queue(ids: NodeRealm) -> DrainPublishOperation {
    DrainPublishOperation {
        ids,
        step: DrainStep::Config,
        now_ms: 0,
        config: None,
        due: VecDeque::new(),
        after: None,
        rechecked: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct DrainPublishOperation {
    ids: NodeRealm,
    step: DrainStep,
    now_ms: u64,
    config: Option<RealmConfigDocument>,
    due: VecDeque<PublishEntry>,
    after: Option<Key>,
    rechecked: bool,
}

type DrainTurn = Result<Next<DrainOutcome>, IoError>;

impl DrainPublishOperation {
    fn page(&mut self) -> DrainTurn {
        self.step = DrainStep::Page;
        let call = queue_page(self.after.take(), PUBLISH_PAGE_SIZE);
        Ok(Next::Call(call))
    }

    fn paged(&mut self, (rows, next): QueueRows) -> DrainTurn {
        let now_ms = self.now_ms;
        self.due = (rows.iter())
            .filter_map(|(_, bytes)| PublishEntry::from_bytes(bytes).ok())
            .filter(|entry| entry.is_due(now_ms))
            .collect();
        self.after = next;
        self.advance()
    }

    /// Forwards the next due entry. A failed claim still asks for another pass: the entry may
    /// have advanced rather than the queue being empty.
    fn advance(&mut self) -> DrainTurn {
        if let Some(entry) = self.due.pop_front() {
            let (node_id, realm_id) = self.ids.ok_or(IoError::Unfinished)?;
            let config = self.config.clone().ok_or(IoError::Unfinished)?;
            let forward = ForwardEntryOperation::new((realm_id, node_id), config, entry);
            self.rechecked = true;
            self.step = DrainStep::Forward;
            return Ok(Next::Call(IoCall::child(forward)));
        }
        if self.after.is_some() {
            return self.page();
        }
        Ok(Next::Done(match self.rechecked {
            true => DrainOutcome::Recheck,
            false => DrainOutcome::Idle,
        }))
    }
}

impl Operation for DrainPublishOperation {
    type Output = DrainOutcome;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> DrainTurn {
        let Some((_, realm_id)) = self.ids else {
            return Ok(Next::Done(DrainOutcome::Deferred));
        };
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> DrainTurn {
        match self.step {
            DrainStep::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(config) => {
                    self.config = Some(config);
                    self.page()
                }
                Err(error) => {
                    config_unavailable(&error);
                    Ok(Next::Done(DrainOutcome::Deferred))
                }
            },
            DrainStep::Page => match queue_rows(result) {
                Ok(page) => self.paged(page),
                Err(error) => {
                    warn!(%error, "Failed to scan the device publish queue");
                    Ok(Next::Done(DrainOutcome::Deferred))
                }
            },
            DrainStep::Forward => {
                result.child::<(), IoError>()?.ok();
                self.advance()
            }
        }
    }
}

/// What the pending call of forwarding one entry answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ForwardStep {
    Retry,
    Claim,
    Fence,
    Refused,
    Apply,
    Reject,
    ReadReplica,
    StoreReplica,
    Create,
    Track,
    Stored,
}

/// Claims one due entry and forwards it. The entry is `Publishing` by then, so the owner cannot
/// delete it underneath the forward; the outcome is stored as its next state.
#[derive(Debug, PartialEq)]
pub(super) struct ForwardEntryOperation {
    realm_id: RealmId,
    node_id: NodeId,
    config: RealmConfigDocument,
    entry: PublishEntry,
    step: ForwardStep,
    now_ms: u64,
    document_id: Ulid,
    attempts: u32,
    token: Option<AuthToken>,
    state: Option<PublishState>,
}

type ForwardTurn = Result<Next<()>, IoError>;

impl ForwardEntryOperation {
    fn new(
        (realm_id, node_id): (RealmId, NodeId),
        config: RealmConfigDocument,
        entry: PublishEntry,
    ) -> Self {
        Self {
            realm_id,
            node_id,
            config,
            attempts: entry.attempts().saturating_add(1),
            entry,
            step: ForwardStep::Claim,
            now_ms: 0,
            document_id: Ulid::nil(),
            token: None,
            state: None,
        }
    }

    fn actor(&self) -> Actor {
        Actor {
            node_id: self.node_id,
            user_id: self.entry.owner,
            realm_id: self.realm_id,
        }
    }

    fn claim(&mut self, next: PublishState, step: ForwardStep) -> ForwardTurn {
        self.step = step;
        let claim = ClaimStateOperation::new(self.entry.clone(), next);
        Ok(Next::Call(IoCall::child(claim)))
    }

    /// The token of one replay, with the issue time of the token that authored it. An entry
    /// without a fence row replays as issued at zero.
    fn replay_token(&self, fence: Option<Value>) -> Option<AuthToken> {
        let issued_at = match fence {
            Some(bytes) => u64::from_be_bytes(bytes.as_ref().try_into().ok()?),
            None => 0,
        };
        let auth = AuthContext {
            user_id: self.entry.owner,
            realm_id: self.realm_id,
            path_restrictions: None,
            session: None,
        };
        Some(AuthToken::Replay {
            auth,
            issued_at,
            authored_at_ms: self.entry.created_at_ms,
        })
    }

    fn retry(&self, reason: String) -> PublishState {
        publishing_retry(self.now_ms, self.document_id, self.attempts, reason)
    }

    /// Stores the state this forward ended in.
    fn store(&mut self, state: PublishState) -> ForwardTurn {
        let draft_id = self.entry.draft_id;
        self.step = ForwardStep::Stored;
        match publish_entry(&entry_with_state(&self.entry, state)) {
            Ok(row) => Ok(Next::Call(outside_call(IoCall::Write(vec![row])))),
            Err(error) => {
                warn!(%error, %draft_id, "Failed to encode a queued draft");
                Ok(Next::Done(()))
            }
        }
    }

    fn fenced(&mut self, fence: Result<Option<Value>, IoError>) -> ForwardTurn {
        let token = fence.ok().and_then(|fence| self.replay_token(fence));
        let Some(token) = token else {
            let state = self.retry("replay fence unreadable".to_string());
            return self.store(state);
        };
        if matches!(self.entry.kind, PublishKind::Edit { .. }) {
            self.token = Some(token);
            self.step = ForwardStep::Refused;
            let actor = read_actor(self.document_id);
            return Ok(Next::Call(IoCall::child(actor)));
        }
        let create = CreateDocumentOperation::new_generated_id(CreateDocumentConfig {
            actor: self.actor(),
            group_id: self.entry.group_id,
            document_id: self.document_id,
            document_path: self.entry.document_path.clone(),
            public: self.entry.public,
            payload: CreateDocumentPayload::RoCrate {
                jsonld: self.entry.jsonld.clone(),
            },
        });
        let route = RouteCreateOperation::new(create, Some(self.node_id), Some(token));
        self.step = ForwardStep::Create;
        Ok(Next::Call(IoCall::child(route)))
    }

    /// Forwards the edit the owner already applied locally, so a holder that accepts it
    /// converges with this device.
    fn apply(&mut self, actor: Result<Option<MetadataActor>, DeviceEditError>) -> ForwardTurn {
        let PublishKind::Edit {
            document_id,
            batch,
            authored,
        } = &self.entry.kind
        else {
            let state = PublishState::Failed {
                reason: "not an edit".to_string(),
                retryable: false,
                document_id: None,
            };
            return self.store(state);
        };
        let document_id = *document_id;
        let refused = (actor.ok().flatten())
            .is_some_and(|current| current.refused(batch.actor, batch.counter));
        if refused {
            let state = PublishState::Failed {
                reason: "an earlier edit it builds on was refused".to_string(),
                retryable: false,
                document_id: Some(document_id),
            };
            return self.store(state);
        }
        let token = self.token.take().ok_or(IoError::Unfinished)?;
        let apply = ApplyBatchOperation::new(
            (self.realm_id, document_id),
            (batch.clone(), authored.clone()),
            token,
        );
        self.step = ForwardStep::Apply;
        Ok(Next::Call(IoCall::child(apply)))
    }

    fn applied(
        &mut self,
        outcome: Result<MetadataRegistryRecord, MetadataWriteError>,
    ) -> ForwardTurn {
        let document_id = self.document_id;
        match outcome {
            Ok(_) => {
                let draft_id = self.entry.draft_id;
                info!(%draft_id, %document_id, "Published an offline edit");
                self.state = Some(PublishState::Published { document_id });
                self.step = ForwardStep::ReadReplica;
                Ok(Next::Call(IoCall::child(read_replica(document_id))))
            }
            // Later edits must neither reuse this actor nor wait for this dot.
            Err(error) if permanent(&error) => {
                let PublishKind::Edit { batch, .. } = &self.entry.kind else {
                    return Err(IoError::Unfinished);
                };
                let reject = reject_edit(document_id, (batch.actor, batch.counter));
                self.state = Some(PublishState::Failed {
                    reason: error.to_string(),
                    retryable: false,
                    document_id: Some(document_id),
                });
                self.step = ForwardStep::Reject;
                Ok(Next::Call(IoCall::child(reject)))
            }
            Err(error) => {
                let state = self.retry(error.to_string());
                self.store(state)
            }
        }
    }

    fn created(
        &mut self,
        outcome: Result<CreateDocumentResult, MetadataWriteError>,
    ) -> ForwardTurn {
        let document_id = self.document_id;
        match outcome {
            // The id was minted for this entry alone, so an existing document under it is this
            // entry's own earlier forward.
            Ok(_) | Err(MetadataWriteError::Create(CreateDocumentError::DocumentAlreadyExists)) => {
                let draft_id = self.entry.draft_id;
                info!(%draft_id, %document_id, "Published a queued draft");
                self.state = Some(PublishState::Published { document_id });
                self.step = ForwardStep::Track;
                let entry = &self.entry;
                let path = entry.document_path.clone();
                let track = track_created(document_id, entry.group_id, path);
                Ok(Next::Call(IoCall::child(track)))
            }
            Err(error) if permanent(&error) => self.store(PublishState::Failed {
                reason: error.to_string(),
                retryable: false,
                document_id: None,
            }),
            // An unknown outcome keeps the minted id, so the retry is the same create.
            Err(error) => {
                let state = self.retry(error.to_string());
                self.store(state)
            }
        }
    }

    fn settled(&mut self) -> ForwardTurn {
        let state = self.state.take().ok_or(IoError::Unfinished)?;
        self.store(state)
    }
}

impl Operation for ForwardEntryOperation {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    /// Mints the entry's realm document id when it has none and claims it as `Publishing`. The
    /// minted id is stored before the forward, so a crash re-forwards the same id and the
    /// holder's create fence dedups it.
    fn first(&mut self) -> ForwardTurn {
        let entry = &self.entry;
        self.document_id = match (&entry.kind, &entry.state) {
            // An edit already names its document; nothing is minted for it.
            (PublishKind::Edit { document_id, .. }, _) => *document_id,
            (_, PublishState::Publishing { document_id, .. }) => *document_id,
            _ => {
                let actor = self.actor();
                let (group, path) = (entry.group_id, &entry.document_path);
                match mint_forward_document(&self.config, &actor, group, path) {
                    Ok(minted) => minted.as_ulid(),
                    // Placement or configuration is not settled yet; try again later.
                    Err(error) => {
                        let next = retry_state(self.now_ms, self.attempts, error.to_string());
                        return self.claim(next, ForwardStep::Retry);
                    }
                }
            }
        };
        let next = PublishState::Publishing {
            document_id: self.document_id,
            due_at_ms: self.now_ms,
            attempts: self.attempts,
        };
        self.claim(next, ForwardStep::Claim)
    }

    fn next(&mut self, result: IoResult) -> ForwardTurn {
        match self.step {
            ForwardStep::Retry => {
                result.child::<bool, ClaimError>()?.ok();
                Ok(Next::Done(()))
            }
            // An uncommitted claim stops the forward: the entry was deleted or advanced.
            ForwardStep::Claim => {
                if !result.child::<bool, ClaimError>()?.unwrap_or(false) {
                    return Ok(Next::Done(()));
                }
                self.step = ForwardStep::Fence;
                let key = publish_key(self.entry.draft_id);
                Ok(Next::Call(outside_call(read_row(
                    DEVICE_FENCE_KEYSPACE,
                    key,
                ))))
            }
            ForwardStep::Fence => self.fenced(outside_answer(result).and_then(value)),
            ForwardStep::Refused => {
                let actor = result.child::<Option<MetadataActor>, DeviceEditError>()?;
                self.apply(actor)
            }
            ForwardStep::Apply => {
                self.applied(result.child::<MetadataRegistryRecord, MetadataWriteError>()?)
            }
            ForwardStep::Reject => {
                if !matches!(result.child::<bool, IoError>()?, Ok(true)) {
                    let document_id = self.document_id;
                    warn!(%document_id, "Could not record a refused offline edit");
                }
                self.settled()
            }
            // Records that the realm has seen one of this replica's edits.
            ForwardStep::ReadReplica => {
                let replica = result.child::<Option<ReplicaRecord>, IoError>()?;
                let Ok(Some(mut replica)) = replica else {
                    return self.settled();
                };
                replica.pending_edits = replica.pending_edits.saturating_sub(1);
                self.step = ForwardStep::StoreReplica;
                Ok(Next::Call(IoCall::child(store_replica(&replica))))
            }
            ForwardStep::StoreReplica => {
                result.child::<(), IoError>()?.ok();
                self.settled()
            }
            ForwardStep::Create => {
                self.created(result.child::<CreateDocumentResult, MetadataWriteError>()?)
            }
            // Keeps a document this device created readable and editable here.
            ForwardStep::Track => {
                if let Err(error) = result.child::<(), SelectionError>()? {
                    let document_id = self.document_id;
                    warn!(%document_id, %error, "Failed to track a created document");
                }
                self.settled()
            }
            ForwardStep::Stored => {
                if let Err(error) = outside_answer(result).and_then(done) {
                    let draft_id = self.entry.draft_id;
                    warn!(%error, %draft_id, "Failed to store a queued draft");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

/// Authorization, target and payload verdicts do not improve by waiting.
fn permanent(error: &MetadataWriteError) -> bool {
    matches!(
        error,
        MetadataWriteError::Unauthorized
            | MetadataWriteError::Forbidden
            | MetadataWriteError::NotFound
            | MetadataWriteError::Update(UpdateDocumentError::MetadataError(
                MetadataError::InvalidInput(_)
            ))
    )
}

/// Backoff before the entry is minted: nothing has been forwarded yet.
fn retry_state(now_ms: u64, attempts: u32, reason: String) -> PublishState {
    if exhausted(attempts, MAX_PUBLISH_ATTEMPTS) {
        return PublishState::Failed {
            reason,
            retryable: true,
            document_id: None,
        };
    }
    PublishState::Pending {
        due_at_ms: retry_due_at(now_ms, attempts),
        attempts,
        last_error: Some(reason),
    }
}

/// Backoff after a forward whose outcome is unknown. The minted id is kept so the next attempt
/// is the same create rather than a second document.
fn publishing_retry(now_ms: u64, document_id: Ulid, attempts: u32, reason: String) -> PublishState {
    if exhausted(attempts, MAX_PUBLISH_ATTEMPTS) {
        return PublishState::Failed {
            reason: format!("{reason} (document id {document_id})"),
            retryable: true,
            document_id: Some(document_id),
        };
    }
    PublishState::Publishing {
        document_id,
        due_at_ms: retry_due_at(now_ms, attempts),
        attempts,
    }
}

/// Whether the publish queue holds any entry, so a starting node re-arms the drain.
pub fn publish_pending() -> PublishPendingOperation {
    PublishPendingOperation
}

#[derive(Debug, PartialEq)]
pub struct PublishPendingOperation;

impl Operation for PublishPendingOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(queue_page(None, 1)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(!queue_rows(result)?.0.is_empty()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::delete_draft::DeleteDraftOperation;
    use crate::device::enqueue_draft::{EnqueueDraftInput, EnqueueDraftOperation};
    use crate::device::inspect_draft::{InspectDraftError, InspectDraftOperation};
    use crate::driver::{DriverContext, drive};
    use crate::tests::device::{context, read_value};
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::identity::realm::RealmId;
    use std::sync::Arc;

    fn entry() -> PublishEntry {
        PublishEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([6u8; 32])),
            Ulid::generate(),
            "/notes".to_string(),
            false,
            "{}".to_string(),
        )
    }

    async fn store(context: &Arc<DriverContext>, entry: &PublishEntry) {
        let (key_space, key, value) = publish_entry(entry).unwrap();
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await;
    }

    async fn claim_state(
        context: &Arc<DriverContext>,
        entry: &PublishEntry,
        next: PublishState,
    ) -> bool {
        let claim = ClaimStateOperation::new(entry.clone(), next);
        drive(claim, context).await.unwrap_or(false)
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([6u8; 32])
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[2; 32]).public()
    }

    fn forwarding(entry: PublishEntry) -> ForwardEntryOperation {
        let config = RealmConfigDocument::new(realm(), Vec::new(), 1);
        ForwardEntryOperation::new((realm(), node()), config, entry)
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn publishing() -> PublishState {
        PublishState::Publishing {
            document_id: Ulid::from_bytes([4u8; 16]),
            due_at_ms: 1,
            attempts: 1,
        }
    }

    #[tokio::test]
    async fn defers_without_realm() {
        // No net handle means the realm is unreachable: nothing may be touched.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        store(&context, &entry).await;
        assert_eq!(
            drive(drain_publish_queue(context.node_and_realm()), &context).await,
            Ok(DrainOutcome::Deferred)
        );
    }

    #[test]
    fn keeps_minted_id() {
        // A forward with an unknown outcome must never mint a second id.
        let document_id = Ulid::generate();
        let PublishState::Publishing {
            document_id: kept,
            due_at_ms,
            attempts,
        } = publishing_retry(5, document_id, 1, "unreachable".to_string())
        else {
            panic!("a retryable forward stays in publishing");
        };
        assert_eq!(kept, document_id);
        assert_eq!(attempts, 1);
        assert!(due_at_ms > 0);
    }

    #[test]
    fn parks_exhausted_entries() {
        let document_id = Ulid::generate();
        assert!(matches!(
            publishing_retry(5, document_id, MAX_PUBLISH_ATTEMPTS, "unreachable".to_string()),
            PublishState::Failed {
                retryable: true,
                document_id: Some(kept),
                ..
            } if kept == document_id
        ));
        assert!(matches!(
            retry_state(5, MAX_PUBLISH_ATTEMPTS, "no placement".to_string()),
            PublishState::Failed {
                retryable: true,
                document_id: None,
                ..
            }
        ));
    }

    #[test]
    fn backs_off_unminted() {
        let PublishState::Pending {
            attempts,
            last_error,
            ..
        } = retry_state(5, 2, "no placement".to_string())
        else {
            panic!("an unminted entry stays pending");
        };
        assert_eq!(attempts, 2);
        assert_eq!(last_error.as_deref(), Some("no placement"));
    }

    #[tokio::test]
    async fn skips_deleted_entry() {
        // The owner's delete committed while the page was in flight: the entry
        // must stay gone instead of coming back as publishing.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        store(&context, &entry).await;
        drive(DeleteDraftOperation::new(entry.draft_id), context.as_ref())
            .await
            .unwrap();

        assert!(!claim_state(&context, &entry, publishing()).await);
        assert_eq!(
            drive(InspectDraftOperation::new(entry.draft_id), context.as_ref()).await,
            Err(InspectDraftError::NotFound)
        );
    }

    // A replay carries the issue time stored with its entry; a deleted entry keeps no fence.
    #[tokio::test]
    async fn replays_stored_fence() {
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        drive(
            EnqueueDraftOperation::new(EnqueueDraftInput {
                entry: entry.clone(),
            })
            .with_fence(42),
            context.as_ref(),
        )
        .await
        .unwrap();
        let key = crate::device::publish_queue::publish_key(entry.draft_id);
        let fence = read_value(&context, DEVICE_FENCE_KEYSPACE, key.clone(), None).await;
        let forward = forwarding(entry.clone());
        assert!(matches!(
            forward.replay_token(fence),
            Some(AuthToken::Replay { issued_at: 42, .. })
        ));

        drive(DeleteDraftOperation::new(entry.draft_id), context.as_ref())
            .await
            .unwrap();
        let fence = read_value(&context, DEVICE_FENCE_KEYSPACE, key, None).await;
        assert!(matches!(
            forward.replay_token(fence),
            Some(AuthToken::Replay { issued_at: 0, .. })
        ));
    }

    #[tokio::test]
    async fn skips_advanced_entry() {
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        let advanced = PublishEntry {
            state: PublishState::Failed {
                reason: "parked".to_string(),
                retryable: true,
                document_id: None,
            },
            ..entry.clone()
        };
        store(&context, &advanced).await;

        assert!(!claim_state(&context, &entry, publishing()).await);
        assert_eq!(
            drive(InspectDraftOperation::new(entry.draft_id), context.as_ref())
                .await
                .unwrap()
                .state,
            advanced.state
        );
    }

    #[tokio::test]
    async fn claims_unchanged_entry() {
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        store(&context, &entry).await;

        assert!(claim_state(&context, &entry, publishing()).await);
        assert_eq!(
            drive(InspectDraftOperation::new(entry.draft_id), context.as_ref())
                .await
                .unwrap()
                .state,
            publishing()
        );
    }

    #[test]
    fn classifies_permanent_errors() {
        assert!(permanent(&MetadataWriteError::Forbidden));
        assert!(!permanent(&MetadataWriteError::Undeliverable(
            "no holder".to_string()
        )));
    }

    // Without a network identity the queue is left alone.
    #[test]
    fn drain_needs_ids() {
        let mut drain = drain_publish_queue(None);
        assert_eq!(drain.first(), Ok(Next::Done(DrainOutcome::Deferred)));
    }

    // A scan failure defers the pass instead of reporting an idle queue.
    #[test]
    fn scan_failure_defers() {
        let mut drain = drain_publish_queue(Some((node(), realm())));
        drain.first().unwrap();
        let config = RealmConfigDocument::new(realm(), Vec::new(), 1);
        drain.next(child(Ok::<_, GetConfigError>(config))).unwrap();
        let failed = child(Err::<IoResult, _>(IoError::Unfinished));
        assert_eq!(drain.next(failed), Ok(Next::Done(DrainOutcome::Deferred)));
    }

    // Only due entries are forwarded; a page without any leaves the queue idle.
    #[test]
    fn future_entries_wait() {
        let mut drain = drain_publish_queue(Some((node(), realm())));
        drain.first().unwrap();
        let config = RealmConfigDocument::new(realm(), Vec::new(), 1);
        drain.next(child(Ok::<_, GetConfigError>(config))).unwrap();
        let mut later = entry();
        later.state = PublishState::Pending {
            due_at_ms: u64::MAX,
            attempts: 0,
            last_error: None,
        };
        let (_, key, value) = publish_entry(&later).unwrap();
        let page = IoResult::Page {
            rows: vec![(key, value)],
            next: None,
        };
        assert_eq!(drain.next(page), Ok(Next::Done(DrainOutcome::Idle)));
    }

    // A claim the store refused ends the forward before anything is sent.
    #[test]
    fn lost_claim_stops() {
        let mut forward = forwarding(entry());
        forward.step = ForwardStep::Claim;
        let lost = child(Ok::<_, ClaimError>(false));
        assert_eq!(forward.next(lost), Ok(Next::Done(())));
    }

    // An unreadable fence retries the same document instead of forwarding without one.
    #[test]
    fn broken_fence_retries() {
        let mut forward = forwarding(entry());
        forward.document_id = Ulid::from_bytes([4; 16]);
        forward.step = ForwardStep::Fence;
        let bad = IoResult::Values(vec![Some(Value::from(vec![1u8]))]);
        assert!(matches!(forward.next(bad), Ok(Next::Call(_))));
        assert_eq!(forward.step, ForwardStep::Stored);
    }

    // An empty queue needs no timer at start.
    #[test]
    fn empty_queue_pending() {
        let mut pending = publish_pending();
        pending.first().unwrap();
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(pending.next(page), Ok(Next::Done(false)));
    }
}
