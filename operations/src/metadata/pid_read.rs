//! Reads a document's persistent identifier mapping from the node that owns it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::routing::{ForwardHoldersOperation, HolderWriteError};
use crate::forward::transport::{MetadataWriteError, write_error};
use crate::metadata::api::MetadataApiError;
use crate::metadata::id_mapping::{parse_mapping_read, read_mapping_effect};
use crate::metadata::persistent_id::forward::{pid_authority_node, pid_error};
use crate::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, PersistentIdOutcome, PersistentIdRequest,
};
use crate::realm::load_config::config_unavailable;

#[derive(Debug, PartialEq)]
enum PidStep {
    Config,
    Local,
    Authority,
}

/// Reads the mapping here when this node has no network or is the document's PID authority,
/// otherwise at the authority.
#[derive(Debug, PartialEq)]
pub struct ReadPidOperation {
    local_node: Option<NodeId>,
    realm_id: RealmId,
    document_id: Ulid,
    step: PidStep,
}

type PidTurn = Result<Next<Option<PersistentIdMapping>>, MetadataApiError>;

impl ReadPidOperation {
    /// `local_node` is this node on the network, if it has one.
    pub fn new(local_node: Option<NodeId>, realm_id: RealmId, document_id: Ulid) -> Self {
        Self {
            local_node,
            realm_id,
            document_id,
            step: PidStep::Config,
        }
    }

    fn read_local(&mut self) -> PidTurn {
        self.step = PidStep::Local;
        let read = read_mapping_effect(self.document_id, None);
        Ok(Next::Call(IoCall::Effect(Box::new(read))))
    }

    fn configured(&mut self, config: RealmConfigDocument) -> PidTurn {
        let authority = pid_authority_node(&config, self.realm_id, self.document_id)
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        if Some(authority) == self.local_node {
            return self.read_local();
        }
        let config_digest = config
            .digest()
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let message = MetadataTransportMessage::ForwardPersistentId {
            auth_token: None,
            config_digest,
            document_id: self.document_id,
            request: PersistentIdRequest::Status,
        };
        let forward =
            ForwardHoldersOperation::new(self.local_node, &[authority], &message, None, false);
        self.step = PidStep::Authority;
        Ok(Next::Call(IoCall::child(forward)))
    }
}

fn answered(response: MetadataTransportMessage) -> PidTurn {
    let MetadataTransportMessage::ForwardedPersistentId { result } = response else {
        return Err(MetadataApiError::ServiceUnavailable);
    };
    match result {
        Ok(PersistentIdOutcome::Status(mapping)) => Ok(Next::Done(mapping.map(|it| *it))),
        Ok(_) => Err(MetadataApiError::ServiceUnavailable),
        Err(MetadataReadError::Unauthorized) => Err(MetadataApiError::Unauthorized),
        Err(MetadataReadError::Forbidden) => Err(MetadataApiError::Forbidden),
        Err(MetadataReadError::NotFound) => Err(MetadataApiError::NotFound),
        Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => {
            Err(MetadataApiError::ServiceUnavailable)
        }
    }
}

impl Operation for ReadPidOperation {
    type Output = Option<PersistentIdMapping>;
    type Error = MetadataApiError;

    fn first(&mut self) -> PidTurn {
        if self.local_node.is_none() {
            return self.read_local();
        }
        let config = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> PidTurn {
        match self.step {
            PidStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| {
                    config_unavailable(&error);
                    MetadataApiError::ServiceUnavailable
                })?;
                self.configured(config)
            }
            PidStep::Local => {
                let mapping = parse_mapping_read(result.event()?).map_err(pid_error)?;
                Ok(Next::Done(mapping))
            }
            PidStep::Authority => {
                let response = result.child::<MetadataTransportMessage, HolderWriteError>()?;
                answered(response.map_err(|error| write_error(MetadataWriteError::from(error)))?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn read(local_node: Option<NodeId>) -> ReadPidOperation {
        let realm_id = RealmId::from_bytes([1; 32]);
        ReadPidOperation::new(local_node, realm_id, Ulid::from_parts(3, 3))
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    // A node without a network reads its own store.
    #[test]
    fn offline_reads_locally() {
        let mut read = read(None);
        let effect = read_mapping_effect(Ulid::from_parts(3, 3), None);
        assert_eq!(
            read.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(effect))))
        );
        assert_eq!(read.step, PidStep::Local);
    }

    // A networked node first finds the authority in the realm config.
    #[test]
    fn missing_config_unavailable() {
        let mut read = read(Some(node()));
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
        let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
        let answer = IoResult::Child(ChildOutcome::new(missing));
        assert_eq!(read.next(answer), Err(MetadataApiError::ServiceUnavailable));
    }

    // The authority's status answer is the mapping, and any other answer is an outage.
    #[test]
    fn authority_answers_status() {
        let status = MetadataTransportMessage::ForwardedPersistentId {
            result: Ok(PersistentIdOutcome::Status(None)),
        };
        assert_eq!(answered(status), Ok(Next::Done(None)));
        let other = MetadataTransportMessage::ForwardedDelete;
        assert_eq!(answered(other), Err(MetadataApiError::ServiceUnavailable));
    }
}
