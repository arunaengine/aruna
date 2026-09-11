use super::*;

impl MetadataHandle {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn apply_sync_mirror(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        auth_token: Option<MetadataAuthToken>,
        relationship: SyncRelationship,
        source_group_id: Option<GroupId>,
        delete: bool,
        extras: PolicyRequestExtras,
    ) -> MetadataTransportMessage {
        let Some(net_handle) = self.inner.net_handle.as_ref() else {
            return MetadataTransportMessage::Reject("mirror_internal".to_string());
        };
        let auth = match authorize_remote_metadata_peer(
            &self.inner.auth_validation,
            &self.inner.storage_handle,
            peer,
            Some(*net_handle.realm_id()),
            auth_token,
            true,
        )
        .await
        {
            Ok(Some(auth)) => auth,
            Ok(None) => {
                return MetadataTransportMessage::Reject("access_denied".to_string());
            }
            Err(_) => return MetadataTransportMessage::Reject("access_denied".to_string()),
        };

        let Some(source_bucket) = relationship.source.bucket() else {
            return MetadataTransportMessage::Reject("invalid_relationship".to_string());
        };
        let Some(target_bucket) = relationship.target.bucket() else {
            return MetadataTransportMessage::Reject("invalid_relationship".to_string());
        };
        if !valid_sync_request(
            &relationship,
            source_bucket,
            target_bucket,
            *net_handle.realm_id(),
            auth.user_id,
            delete,
        ) {
            return MetadataTransportMessage::Reject("invalid_relationship".to_string());
        }

        let local_node = net_handle.node_id();
        let (local_bucket, direction) =
            if relationship.source.node_id == peer && relationship.target.node_id == local_node {
                (target_bucket, SyncRelationshipDirection::Incoming)
            } else if delete
                && relationship.target.node_id == peer
                && relationship.source.node_id == local_node
            {
                (source_bucket, SyncRelationshipDirection::Outgoing)
            } else {
                return MetadataTransportMessage::Reject("invalid_relationship".to_string());
            };

        if delete {
            let stored = match drive(
                GetSyncRelationshipOperation::new(relationship.id, direction),
                context.as_ref(),
            )
            .await
            {
                Ok(stored) => stored,
                Err(SyncRelationshipError::NotFound) => {
                    return MetadataTransportMessage::SyncMirrorDeleted;
                }
                Err(_) => {
                    return MetadataTransportMessage::Reject("mirror_internal".to_string());
                }
            };
            if !sync_identity_matches(&stored, &relationship) {
                return MetadataTransportMessage::Reject("invalid_relationship".to_string());
            }
            // A still-present bucket must pass the same RBAC and policy boundary
            // the origin ran; a gone bucket leaves nothing to authorize against.
            match drive(
                GetBucketInfoOperation::new(local_bucket.to_string()),
                context.as_ref(),
            )
            .await
            {
                Ok(Some(Ok(bucket_info))) => {
                    let path = blob_bucket_permission_path(
                        *net_handle.realm_id(),
                        bucket_info.group_id,
                        local_node,
                        local_bucket,
                    );
                    match authorize(
                        context.as_ref(),
                        *net_handle.realm_id(),
                        &auth,
                        &path,
                        &Permission::WRITE,
                        extras,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(AuthorizeError::CheckFailed(_) | AuthorizeError::Storage(_)) => {
                            return MetadataTransportMessage::Reject("mirror_internal".to_string());
                        }
                        Err(_) => {
                            return MetadataTransportMessage::Reject("access_denied".to_string());
                        }
                    }
                }
                Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => {}
                Ok(Some(Err(_))) | Err(_) => {
                    return MetadataTransportMessage::Reject("mirror_internal".to_string());
                }
            }
            // Outgoing reference relationships are detached instead of
            // deleted so the peer's retained reference records stay readable.
            let removed = match direction {
                SyncRelationshipDirection::Outgoing => {
                    remove_outgoing_relationship(context.as_ref(), stored).await
                }
                SyncRelationshipDirection::Incoming => {
                    drive(
                        DeleteSyncRelationshipOperation::new(stored, direction),
                        context.as_ref(),
                    )
                    .await
                }
            };
            return match removed {
                Ok(()) => MetadataTransportMessage::SyncMirrorDeleted,
                Err(_) => MetadataTransportMessage::Reject("mirror_internal".to_string()),
            };
        }

        let (group_id, create_bucket) = match drive(
            GetBucketInfoOperation::new(local_bucket.to_string()),
            context.as_ref(),
        )
        .await
        {
            Ok(Some(Ok(bucket_info))) => (bucket_info.group_id, false),
            Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => {
                let Some(source_group_id) = source_group_id else {
                    return MetadataTransportMessage::Reject("invalid_relationship".to_string());
                };
                (source_group_id, true)
            }
            Ok(Some(Err(_))) => {
                return MetadataTransportMessage::Reject("mirror_internal".to_string());
            }
            Err(_) => return MetadataTransportMessage::Reject("mirror_internal".to_string()),
        };
        let path = blob_bucket_permission_path(
            *net_handle.realm_id(),
            group_id,
            net_handle.node_id(),
            local_bucket,
        );
        match authorize(
            context.as_ref(),
            *net_handle.realm_id(),
            &auth,
            &path,
            &Permission::WRITE,
            extras,
        )
        .await
        {
            Ok(()) => {}
            Err(AuthorizeError::CheckFailed(_) | AuthorizeError::Storage(_)) => {
                return MetadataTransportMessage::Reject("mirror_internal".to_string());
            }
            Err(_) => return MetadataTransportMessage::Reject("access_denied".to_string()),
        }
        if create_bucket
            && create_sync_bucket(context.as_ref(), local_bucket, group_id, &relationship)
                .await
                .is_err()
        {
            return MetadataTransportMessage::Reject("mirror_internal".to_string());
        }

        match drive(
            StoreSyncRelationshipOperation::new(relationship, direction),
            context.as_ref(),
        )
        .await
        {
            Ok(_) => MetadataTransportMessage::SyncMirrorCreated,
            Err(_) => MetadataTransportMessage::Reject("mirror_internal".to_string()),
        }
    }
    /// Validates a forwarded caller's bearer token and confirms the forwarding
    /// peer belongs to the token's realm, exactly as the query/search paths do.
    pub(crate) async fn authorize_remote_peer(
        &self,
        peer: NodeId,
        auth_token: Option<MetadataAuthToken>,
    ) -> Result<Option<AuthContext>, MetadataError> {
        authorize_remote_metadata_peer(
            &self.inner.auth_validation,
            &self.inner.storage_handle,
            peer,
            self.inner.net_handle.as_ref().map(|net| *net.realm_id()),
            auth_token,
            true,
        )
        .await
    }

    pub(crate) async fn authorize_read_peer(
        &self,
        peer: NodeId,
        auth_token: Option<MetadataAuthToken>,
        require_trusted: bool,
    ) -> Result<Option<AuthContext>, MetadataReadError> {
        let auth = match auth_token {
            Some(token @ MetadataAuthToken::Bearer(_)) => {
                Some(self.authorize_write_peer(peer, Some(token)).await.map_err(
                    |error| match error {
                        MetadataWritePeerError::Unauthorized => MetadataReadError::Unauthorized,
                        MetadataWritePeerError::Unavailable(_) => MetadataReadError::Unavailable,
                    },
                )?)
            }
            token => self
                .authorize_remote_peer(peer, token)
                .await
                .map_err(|_| MetadataReadError::Unavailable)?,
        };
        let realm_id = self
            .inner
            .net_handle
            .as_ref()
            .map(|net| *net.realm_id())
            .ok_or(MetadataReadError::Unavailable)?;
        if auth.as_ref().is_some_and(|auth| auth.realm_id != realm_id) {
            return Err(MetadataReadError::Forbidden);
        }
        if require_trusted {
            ensure_remote_metadata_peer_is_configured_for_realm(
                &self.inner.storage_handle,
                peer,
                realm_id,
                PeerTrust::Vouched(None),
            )
            .await
            .map_err(|_| MetadataReadError::Unavailable)?;
        }
        self.note_peer_contact(peer);
        Ok(auth)
    }

    pub(crate) async fn authorize_write_peer(
        &self,
        peer: NodeId,
        auth_token: Option<MetadataAuthToken>,
    ) -> Result<AuthContext, MetadataWritePeerError> {
        let Some(auth_token) = auth_token else {
            return Err(MetadataWritePeerError::Unauthorized);
        };
        let MetadataAuthToken::Bearer(token) = auth_token else {
            let auth = self
                .authorize_remote_peer(peer, Some(auth_token))
                .await
                .map_err(MetadataWritePeerError::Unavailable)?
                .ok_or(MetadataWritePeerError::Unauthorized)?;
            self.note_peer_contact(peer);
            return Ok(auth);
        };
        let auth = validate_aruna_bearer_token(&self.inner.auth_validation, token.as_str())
            .await
            .map_err(|_| MetadataWritePeerError::Unauthorized)?;
        let local_realm_id = self
            .inner
            .net_handle
            .as_ref()
            .map(|net| *net.realm_id())
            .ok_or_else(|| {
                MetadataWritePeerError::Unavailable(MetadataError::InvalidInput(
                    "forwarded metadata auth requires a local serving realm".to_string(),
                ))
            })?;
        if auth.realm_id == local_realm_id {
            ensure_remote_metadata_peer_is_configured_for_realm(
                &self.inner.storage_handle,
                peer,
                auth.realm_id,
                PeerTrust::Member,
            )
            .await
            .map_err(MetadataWritePeerError::Unavailable)?;
        }
        self.note_peer_contact(peer);
        Ok(auth)
    }

    /// This node's own liveness observation, taken where the peer identity is
    /// authorized. Never realm state: it is neither replicated nor published.
    fn note_peer_contact(&self, peer: NodeId) {
        self.inner.peer_contacts.note(peer, unix_timestamp_millis());
    }

    /// When this node last saw each authorized peer.
    pub fn peer_contacts(&self) -> PeerContacts {
        self.inner.peer_contacts.clone()
    }

    pub(crate) async fn claims_for_revocation(
        &self,
        token: &str,
    ) -> Result<TokenClaims, ArunaBearerTokenError> {
        decode_aruna_bearer_token(
            &RevocationBlindValidation(&self.inner.auth_validation),
            token,
        )
        .await
    }

    #[tracing::instrument(
        name = "metadata.forward.remote",
        level = "debug",
        skip(self, message),
        fields(
            peer = ?node_id,
            request = transport_message_kind(&message),
            elapsed_ms = field::Empty,
        )
    )]
    pub(crate) async fn request_forwarded_write(
        &self,
        node_id: NodeId,
        message: MetadataTransportMessage,
    ) -> Result<MetadataTransportMessage, MetadataRequestError> {
        let started = Instant::now();
        let span = Span::current();
        let result = send_remote_metadata_request(&self.inner, &span, node_id, message).await;
        record_elapsed_ms(&span, "elapsed_ms", started);
        if let Err(error) = &result {
            record_error(&span, &error.to_string());
        }
        result
    }
}
