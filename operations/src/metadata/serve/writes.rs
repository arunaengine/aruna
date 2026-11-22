//! Serves the document writes a peer forwards to a holder: create, update and delete, each for
//! the forwarded caller under its WRITE on the document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::metadata::{MetadataEffect, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::warn;

use super::ServeNode;
use super::paths::{config_call, digest_matches, read_config};
use crate::auth::error::AuthorizeError;
use crate::forward::authorize::{
    ForwardAuthError, ForwardedCallerOperation, forward_auth_error, write_check,
};
use crate::forward::transport::{forwarded_unavailable, reject};
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentResult,
};
use crate::metadata::create_entry::CreateEntryOperation;
use crate::metadata::delete_document::{DeleteDocumentError, DeleteDocumentOperation};
use crate::metadata::deletion_guard::metadata_call;
use crate::metadata::forward::replay::{
    CreateRows, CreateRowsOperation, HeldRecordError, HeldRecordOperation, replay_answer,
};
use crate::metadata::persistent_id::forward::pid_authority_node;
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentOperation,
};
use crate::metadata::update_entry::UpdateEntryOperation;
use aruna_core::metadata::protocol::MetadataTransportMessage as Message;

#[derive(Debug, PartialEq)]
enum WriteStage {
    Config,
    Caller(Box<RealmConfigDocument>),
    /// A create's WRITE check, then its replay read.
    CreateCheck,
    Replay,
    Create,
    /// A create lost the race against a concurrent delivery and reads the winner.
    Raced,
    Held(Box<RealmConfigDocument>),
    Check(Box<MetadataRegistryRecord>),
    Update,
    Delete(ulid::Ulid),
}

/// Applies a forwarded create, update or delete on this holder and answers its outcome.
#[derive(Debug, PartialEq)]
pub(crate) struct WriteServeOperation {
    peer: NodeId,
    node: ServeNode,
    digest: [u8; 32],
    request: Option<Message>,
    auth: Option<AuthContext>,
    create: Option<(CreateDocumentConfig, Option<String>)>,
    stage: WriteStage,
}

type WriteTurn = Result<Next<Message>, IoError>;

fn done(reply: Message) -> WriteTurn {
    Ok(Next::Done(reply))
}

// The answer of a create already applied here, or `None` when it was not.
fn replayed(result: IoResult, config: &CreateDocumentConfig) -> Result<Option<Message>, IoError> {
    Ok(match result.child::<Option<CreateRows>, IoError>()? {
        Ok(rows) => match rows.map(|rows| replay_answer(config, rows)).transpose() {
            Ok(reply) => reply,
            Err(error) => Some(reject(error)),
        },
        Err(error) => Some(reject(format!(
            "metadata create replay read failed: {error}"
        ))),
    })
}

impl WriteServeOperation {
    pub(crate) fn new(
        (peer, node): (NodeId, ServeNode),
        digest: [u8; 32],
        request: Message,
    ) -> Self {
        Self {
            peer,
            node,
            digest,
            request: Some(request),
            auth: None,
            create: None,
            stage: WriteStage::Config,
        }
    }

    fn unavailable(&self) -> WriteTurn {
        match self.request.as_ref() {
            Some(request) => done(forwarded_unavailable(request)),
            None => done(Message::ForwardedWriteUnavailable),
        }
    }

    fn call(&mut self, stage: WriteStage, call: IoCall) -> WriteTurn {
        self.stage = stage;
        Ok(Next::Call(call))
    }

    fn actor(&self, (node, realm_id): (NodeId, RealmId)) -> Result<Actor, IoError> {
        let auth = self.auth.as_ref().ok_or(IoError::Unexpected {
            expected: "a checked caller",
            got: "none",
        })?;
        Ok(Actor {
            node_id: node,
            user_id: auth.user_id,
            realm_id,
        })
    }

    fn write_check(&mut self, stage: WriteStage, path: &str) -> WriteTurn {
        let Some(auth) = self.auth.as_ref() else {
            return self.unavailable();
        };
        let check = write_check(auth, path);
        self.call(stage, IoCall::child(check))
    }

    // Starts the write the caller check admitted.
    fn accepted(&mut self, local: (NodeId, RealmId), config: RealmConfigDocument) -> WriteTurn {
        let (node, realm_id) = local;
        match self.request.take() {
            Some(Message::ForwardCreateDocument {
                group_id,
                document_id,
                document_path,
                public,
                payload,
                commit_message,
                ..
            }) => {
                let normalized = MetadataRegistryRecord::normalize_document_path(&document_path);
                if normalized.is_empty() {
                    return done(reject(
                        "forwarded metadata create has an empty document path",
                    ));
                }
                let path = MetadataRegistryRecord::permission_path_for(
                    &realm_id,
                    group_id,
                    &normalized,
                    document_id,
                );
                let config = CreateDocumentConfig {
                    actor: self.actor(local)?,
                    group_id,
                    document_id,
                    document_path,
                    public,
                    payload,
                };
                self.create = Some((config, commit_message));
                self.write_check(WriteStage::CreateCheck, &path)
            }
            Some(
                request @ (Message::ForwardUpdateDocument { document_id, .. }
                | Message::ForwardDeleteDocument { document_id, .. }),
            ) => {
                let held = HeldRecordOperation::new(config.clone(), node, document_id);
                self.request = Some(request);
                self.call(WriteStage::Held(Box::new(config)), IoCall::child(held))
            }
            _ => done(reject("unexpected forwarded metadata message")),
        }
    }

    // Runs the update or delete the WRITE check admitted on `record`.
    fn checked(&mut self, local: (NodeId, RealmId), record: MetadataRegistryRecord) -> WriteTurn {
        let actor = self.actor(local)?;
        match self.request.take() {
            Some(Message::ForwardUpdateDocument {
                document_id,
                public,
                mutation,
                expected_revision,
                commit_message,
                ..
            }) => {
                let update = UpdateDocumentOperation::new(UpdateDocumentConfig {
                    actor,
                    group_id: record.group_id,
                    document_id,
                    public: public.unwrap_or(record.public),
                    mutation,
                    expected_revision,
                })
                .with_message(commit_message);
                let entry = UpdateEntryOperation::new(update, Some(local.0));
                self.call(WriteStage::Update, IoCall::child(entry))
            }
            Some(Message::ForwardDeleteDocument { document_id, .. }) => {
                let delete = DeleteDocumentOperation::new(actor, record.group_id, document_id);
                self.call(
                    WriteStage::Delete(document_id),
                    IoCall::retry_with(move || delete.fresh_copy(), &document_id.to_bytes()),
                )
            }
            _ => done(reject("unexpected forwarded metadata message")),
        }
    }

    fn run_create(&mut self, local_node: NodeId) -> WriteTurn {
        let Some((config, message)) = self.create.as_ref() else {
            return self.unavailable();
        };
        let operation =
            CreateDocumentOperation::new_forwarded(config.clone()).with_message(message.clone());
        let entry = CreateEntryOperation::new(operation, Some(local_node));
        self.call(WriteStage::Create, IoCall::child(entry))
    }

    fn replay_read(&mut self, stage: WriteStage) -> WriteTurn {
        let Some((config, _)) = self.create.as_ref() else {
            return self.unavailable();
        };
        let rows = CreateRowsOperation::new(config.document_id);
        self.call(stage, IoCall::child(rows))
    }
}

impl Operation for WriteServeOperation {
    type Output = Message;
    type Error = IoError;

    fn first(&mut self) -> WriteTurn {
        match self.node.local {
            Some((_, realm_id)) => self.call(WriteStage::Config, config_call(realm_id)),
            None => self.unavailable(),
        }
    }

    fn next(&mut self, result: IoResult) -> WriteTurn {
        let local = self.node.local.ok_or(IoError::Unexpected {
            expected: "this node's realm",
            got: "none",
        })?;
        let (node, realm_id) = local;
        match std::mem::replace(&mut self.stage, WriteStage::Config) {
            WriteStage::Config => {
                let config = match read_config(result)? {
                    Some(config) if digest_matches(Some(&config), &self.digest) => config,
                    _ => return self.unavailable(),
                };
                let Some(request) = self.request.as_ref() else {
                    return self.unavailable();
                };
                let caller = ForwardedCallerOperation::new(self.peer, realm_id, request);
                self.call(WriteStage::Caller(Box::new(config)), IoCall::child(caller))
            }
            WriteStage::Caller(config) => match result.child::<AuthContext, ForwardAuthError>()? {
                Ok(auth) => {
                    self.auth = Some(auth);
                    self.accepted(local, *config)
                }
                Err(error) => done(forward_auth_error(error)),
            },
            WriteStage::CreateCheck => match result.child::<(), AuthorizeError>()? {
                Ok(()) => self.replay_read(WriteStage::Replay),
                Err(error) => done(forward_auth_error(ForwardAuthError::from(error))),
            },
            WriteStage::Replay => {
                let Some((config, _)) = self.create.as_ref() else {
                    return self.unavailable();
                };
                match replayed(result, config)? {
                    Some(reply) => done(reply),
                    None => self.run_create(node),
                }
            }
            WriteStage::Create => {
                match result.child::<CreateDocumentResult, CreateDocumentError>()? {
                    Ok(created) => done(Message::ForwardedRecord {
                        record: Box::new(created.record),
                    }),
                    // Lost the race against a concurrent delivery of the same forward: the
                    // winner's record is the answer, not an error.
                    Err(CreateDocumentError::DocumentAlreadyExists) => {
                        self.replay_read(WriteStage::Raced)
                    }
                    Err(CreateDocumentError::MetadataError(MetadataError::ProfileValidation(
                        findings,
                    ))) => done(Message::ForwardedProfileValidation { findings }),
                    Err(error) => {
                        done(reject(format!("forwarded metadata create failed: {error}")))
                    }
                }
            }
            WriteStage::Raced => {
                let Some((config, _)) = self.create.as_ref() else {
                    return self.unavailable();
                };
                let document_id = config.document_id;
                match replayed(result, config)? {
                    Some(reply) => done(reply),
                    None => done(reject(format!(
                        "forwarded metadata create for `{document_id}` raced a delete"
                    ))),
                }
            }
            WriteStage::Held(config) => {
                let document_id = match self.request.as_ref() {
                    Some(
                        Message::ForwardUpdateDocument { document_id, .. }
                        | Message::ForwardDeleteDocument { document_id, .. },
                    ) => *document_id,
                    _ => return self.unavailable(),
                };
                let delete = matches!(self.request, Some(Message::ForwardDeleteDocument { .. }));
                let record = match result.child::<MetadataRegistryRecord, HeldRecordError>()? {
                    Ok(record) => record,
                    Err(HeldRecordError::NotFound) => return done(Message::WriteNotFound),
                    Err(HeldRecordError::Unavailable(error)) => {
                        let write = if delete { "delete" } else { "update" };
                        warn!(%document_id, %error, "Forwarded metadata {write} is unavailable");
                        return done(Message::ForwardedWriteUnavailable);
                    }
                };
                if delete && pid_authority_node(&config, realm_id, document_id) != Some(node) {
                    return done(Message::ForwardedWriteUnavailable);
                }
                let path = record.permission_path.clone();
                self.write_check(WriteStage::Check(Box::new(record)), &path)
            }
            WriteStage::Check(record) => match result.child::<(), AuthorizeError>()? {
                Ok(()) => self.checked(local, *record),
                Err(error) => done(forward_auth_error(ForwardAuthError::from(error))),
            },
            WriteStage::Update => done(
                match result.child::<MetadataRegistryRecord, UpdateDocumentError>()? {
                    Ok(record) => Message::ForwardedRecord {
                        record: Box::new(record),
                    },
                    Err(UpdateDocumentError::RawLimit) => Message::MetadataHistoryCapacity,
                    Err(UpdateDocumentError::RevisionConflict { expected, current }) => {
                        Message::ForwardedRevisionConflict { expected, current }
                    }
                    Err(UpdateDocumentError::GraphPending { expected }) => {
                        Message::ForwardedGraphPending { expected }
                    }
                    Err(UpdateDocumentError::MetadataError(MetadataError::InvalidInput(
                        message,
                    ))) => Message::UpdateInvalidInput { message },
                    Err(UpdateDocumentError::MetadataError(MetadataError::ProfileValidation(
                        findings,
                    ))) => Message::ForwardedProfileValidation { findings },
                    Err(error) => reject(format!("forwarded metadata update failed: {error}")),
                },
            ),
            WriteStage::Delete(document_id) => match result.child::<(), DeleteDocumentError>()? {
                // The registry cache drops the row only after the delete committed.
                Ok(()) => {
                    let uncache = MetadataEffect::UncacheRecord { document_id };
                    Ok(Next::Then {
                        output: Message::ForwardedDelete,
                        calls: vec![metadata_call(uncache)],
                    })
                }
                Err(error) => done(reject(format!("forwarded metadata delete failed: {error}"))),
            },
        }
    }
}
