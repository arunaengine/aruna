//! Gathers a group's audit trail across the realm and serves this node's audit page to peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::audit::{AuditPageRequest, AuditPageResponse, validate_request};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tokio::sync::OwnedSemaphorePermit;

use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::audit::{
    AUDIT_INBOUND_ADMISSION, AUDIT_OUTBOUND_ADMISSION, AUDIT_PAGE_LIMIT, AUDIT_PAGE_SIZE,
    AuditAggregate, ListAuditError, ListAuditOperation, ListAuditRequest, LocalPageOperation,
    decode_cursor,
};
use crate::metadata::protocol::MetadataReadError;
use crate::realm::load_config::config_unavailable;

/// A permit for one realm-wide audit read, or `None` while this node is saturated. The caller
/// holds it while the read runs.
pub fn outbound_admission() -> Option<OwnedSemaphorePermit> {
    AUDIT_OUTBOUND_ADMISSION.clone().try_acquire_owned().ok()
}

/// A permit for serving one peer's audit page, or `None` while this node is saturated.
pub(crate) fn inbound_admission() -> Option<OwnedSemaphorePermit> {
    AUDIT_INBOUND_ADMISSION.clone().try_acquire_owned().ok()
}

#[derive(Debug, PartialEq)]
enum ListStage {
    Config,
    Gather { partial: bool },
}

/// Gathers the group's audit trail from every eligible realm node within `wait` and merges the
/// pages. This node reads its own keyspace when the realm still authorizes it; the other nodes
/// are asked over the metadata control transport. Membership and its digest are eventual
/// candidates, which each peer revalidates.
#[derive(Debug, PartialEq)]
pub struct AuditListOperation {
    realm_id: RealmId,
    local_node: NodeId,
    auth_token: Option<AuthToken>,
    request: ListAuditRequest,
    wait: Duration,
    stage: ListStage,
}

impl AuditListOperation {
    pub fn new(
        (realm_id, local_node): (RealmId, NodeId),
        auth_token: Option<AuthToken>,
        request: ListAuditRequest,
        wait: Duration,
    ) -> Self {
        Self {
            realm_id,
            local_node,
            auth_token,
            request,
            wait,
            stage: ListStage::Config,
        }
    }

    fn gather(
        &mut self,
        config: Option<RealmConfigDocument>,
    ) -> Result<Next<AuditAggregate>, ListAuditError> {
        let limit = self
            .request
            .limit
            .unwrap_or(AUDIT_PAGE_SIZE)
            .clamp(1, AUDIT_PAGE_LIMIT);
        let mut partial = config.is_none();
        let mut config_digest = [0u8; 32];
        let mut digest_ready = false;
        let mut include_local = false;
        let mut user_origin = false;
        if let Some(config) = &config {
            match config.digest() {
                Ok(digest) => {
                    config_digest = digest;
                    digest_ready = true;
                }
                Err(_) => partial = true,
            }
            let local = self.local_node.to_string();
            match config.nodes.iter().find(|node| node.node_id == local) {
                Some(node) => {
                    let current_local_authorized = node.kind.is_sync_eligible();
                    user_origin = !current_local_authorized;
                    include_local = self.request.local_authorized && current_local_authorized;
                    partial |= self.request.local_authorized != current_local_authorized;
                }
                None => partial = true,
            }
        }
        if self.request.cursor.is_some() && !digest_ready {
            return Err(ListAuditError::InvalidCursor);
        }
        if user_origin && self.auth_token.is_none() {
            return Err(ListAuditError::Unauthorized);
        }
        let request = &self.request;
        let start_after = (request.cursor.as_deref())
            .map(|cursor| {
                decode_cursor(
                    cursor,
                    self.realm_id,
                    config_digest,
                    request.group_id,
                    request.document_id,
                )
            })
            .transpose()?;
        let mut invalid_peer = false;
        let local_node = self.local_node;
        let peer_nodes: Vec<NodeId> = (config.iter())
            .flat_map(|config| config.nodes.iter())
            .filter(|node| node.kind.is_sync_eligible())
            .filter_map(|node| match node.node_id.parse::<NodeId>() {
                Ok(node) if node != local_node && digest_ready => Some(node),
                Ok(_) => None,
                Err(_) => {
                    invalid_peer = true;
                    None
                }
            })
            .collect();
        let operation = ListAuditOperation::new(
            self.realm_id,
            local_node,
            include_local,
            request.group_id,
            request.document_id,
            peer_nodes,
            start_after,
            limit,
            self.auth_token.clone(),
            config_digest,
        );
        self.stage = ListStage::Gather {
            partial: partial || invalid_peer,
        };
        // The group deadline marks late nodes missing, so the run itself needs no deadline.
        Ok(Next::Call(IoCall::child(operation.within(self.wait))))
    }
}

impl Operation for AuditListOperation {
    type Output = AuditAggregate;
    type Error = ListAuditError;

    fn first(&mut self) -> Result<Next<AuditAggregate>, ListAuditError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuditAggregate>, ListAuditError> {
        match self.stage {
            ListStage::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.gather(config.inspect_err(config_unavailable).ok())
            }
            ListStage::Gather { partial } => {
                let mut aggregate = result.child::<AuditAggregate, ListAuditError>()??;
                aggregate.partial |= partial;
                if aggregate.partial {
                    aggregate.next_cursor = None;
                }
                Ok(Next::Done(aggregate))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ServeStage {
    Config,
    Authorize,
    Page,
}

/// Serves a peer's request for this node's local audit page after re-checking the caller's
/// group-admin authority. `auth` is the caller the peer's forwarded token stands for.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeAuditOperation {
    realm_id: RealmId,
    local_node: NodeId,
    auth: AuthContext,
    request: AuditPageRequest,
    stage: ServeStage,
}

impl ServeAuditOperation {
    pub(crate) fn new(
        (realm_id, local_node): (RealmId, NodeId),
        auth: AuthContext,
        request: AuditPageRequest,
    ) -> Self {
        Self {
            realm_id,
            local_node,
            auth,
            request,
            stage: ServeStage::Config,
        }
    }
}

impl Operation for ServeAuditOperation {
    type Output = AuditPageResponse;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<AuditPageResponse>, MetadataReadError> {
        if self.request.realm_id != self.realm_id {
            return Err(MetadataReadError::Unavailable);
        }
        validate_request(&self.request).map_err(|_| MetadataReadError::Unavailable)?;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuditPageResponse>, MetadataReadError> {
        let unavailable = |_| MetadataReadError::Unavailable;
        match self.stage {
            ServeStage::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()
                    .map_err(unavailable)?;
                let config = config
                    .inspect_err(config_unavailable)
                    .map_err(|_| MetadataReadError::Unavailable)?;
                if config.digest().ok() != Some(self.request.config_digest)
                    || !audit_member(&config, self.realm_id, self.local_node)
                {
                    return Err(MetadataReadError::Unavailable);
                }
                let path = format!("/{}/g/{}/admin", self.realm_id, self.request.group_id);
                let auth = &self.auth;
                let authorize = AuthorizeOperation::new(
                    auth.realm_id,
                    auth,
                    &path,
                    &Permission::WRITE,
                    PolicyRequestExtras::rest(),
                );
                self.stage = ServeStage::Authorize;
                Ok(Next::Call(IoCall::child(authorize)))
            }
            ServeStage::Authorize => {
                let checked = result.child::<(), AuthorizeError>().map_err(unavailable)?;
                checked.map_err(admin_refusal)?;
                let request = &self.request;
                let page = LocalPageOperation::new(
                    self.realm_id,
                    request.group_id,
                    request.document_id,
                    request.start_after.clone(),
                    request.limit.clamp(1, AUDIT_PAGE_LIMIT),
                );
                self.stage = ServeStage::Page;
                Ok(Next::Call(IoCall::child(page)))
            }
            ServeStage::Page => {
                let page = result
                    .child::<AuditPageResponse, ListAuditError>()
                    .map_err(unavailable)?;
                page.map(Next::Done)
                    .map_err(|_| MetadataReadError::Unavailable)
            }
        }
    }
}

/// A refused group-admin check as the audit channel reports it: a denial is forbidden, a
/// failed check leaves the page unavailable.
pub(super) fn admin_refusal(error: AuthorizeError) -> MetadataReadError {
    match error {
        AuthorizeError::PermissionDenied | AuthorizeError::Policy(_) => {
            MetadataReadError::Forbidden
        }
        _ => MetadataReadError::Unavailable,
    }
}

pub(super) fn audit_member(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    node_id: NodeId,
) -> bool {
    config.realm_id == realm_id
        && config
            .nodes
            .iter()
            .any(|node| node.node_id == node_id.to_string() && node.kind.is_sync_eligible())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId([1; 32])
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn request(cursor: Option<String>) -> ListAuditRequest {
        ListAuditRequest {
            group_id: Ulid::from_bytes([2; 16]),
            document_id: None,
            cursor,
            limit: Some(10),
            local_authorized: true,
        }
    }

    fn list(cursor: Option<String>) -> AuditListOperation {
        AuditListOperation::new(
            (realm(), node()),
            None,
            request(cursor),
            Duration::from_secs(1),
        )
    }

    fn missing_config() -> IoResult {
        let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
        IoResult::Child(ChildOutcome::new(missing))
    }

    #[test]
    fn cursor_needs_config() {
        let mut list = list(Some("cursor".to_string()));
        assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(
            list.next(missing_config()),
            Err(ListAuditError::InvalidCursor)
        );
    }

    #[test]
    fn missing_config_partial() {
        let mut list = list(None);
        list.first().unwrap();
        assert!(matches!(
            list.next(missing_config()),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(list.stage, ListStage::Gather { partial: true });
    }

    #[test]
    fn foreign_realm_unserved() {
        let auth = AuthContext {
            user_id: UserId::nil(realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        };
        let request = AuditPageRequest {
            auth_token: None,
            config_digest: [0; 32],
            realm_id: RealmId([9; 32]),
            group_id: Ulid::from_bytes([2; 16]),
            document_id: None,
            start_after: None,
            limit: 10,
        };
        let mut serve = ServeAuditOperation::new((realm(), node()), auth, request);
        assert_eq!(serve.first(), Err(MetadataReadError::Unavailable));
    }

    #[test]
    fn admin_denial_forbidden() {
        assert_eq!(
            admin_refusal(AuthorizeError::PermissionDenied),
            MetadataReadError::Forbidden
        );
        let failed = AuthorizeError::CheckFailed("down".to_string());
        assert_eq!(admin_refusal(failed), MetadataReadError::Unavailable);
    }
}
