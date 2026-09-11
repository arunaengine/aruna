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
}
