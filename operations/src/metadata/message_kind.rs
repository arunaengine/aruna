//! Names metadata transport messages for logs and says whether a failed request was sent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::protocol::MetadataTransportMessage;

/// Whether a failed metadata request may have reached its peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataRequestDelivery {
    DefinitelyNotSent,
    PossiblySent,
}

/// A stable label of `message` for logs and traces.
pub(crate) fn transport_message_kind(message: &MetadataTransportMessage) -> &'static str {
    match message {
        MetadataTransportMessage::QueryGraphs { .. } => "query_graphs",
        MetadataTransportMessage::QueryResults { .. } => "query_results",
        MetadataTransportMessage::SearchGraphs { .. } => "search_graphs",
        MetadataTransportMessage::FilteredSearchGraphs { .. } => "filtered_search_graphs",
        MetadataTransportMessage::SearchResults { .. } => "search_results",
        MetadataTransportMessage::SearchBuckets { .. } => "search_buckets",
        MetadataTransportMessage::BucketSearchResults { .. } => "bucket_search_results",
        MetadataTransportMessage::ReleaseHolder { .. } => "release_holder",
        MetadataTransportMessage::ReleaseHolderResults { .. } => "release_holder_results",
        MetadataTransportMessage::SearchObjects { .. } => "search_objects",
        MetadataTransportMessage::ObjectSearchResults { .. } => "object_search_results",
        MetadataTransportMessage::CreateSyncMirror { .. } => "create_sync_mirror",
        MetadataTransportMessage::DeleteSyncMirror { .. } => "delete_sync_mirror",
        MetadataTransportMessage::SyncMirrorCreated => "sync_mirror_created",
        MetadataTransportMessage::SyncMirrorDeleted => "sync_mirror_deleted",
        MetadataTransportMessage::ForwardCreateDocument { .. } => "forward_create_document",
        MetadataTransportMessage::ForwardUpdateDocument { .. } => "forward_update_document",
        MetadataTransportMessage::ForwardDeleteDocument { .. } => "forward_delete_document",
        MetadataTransportMessage::ForwardReadDocument { .. } => "forward_read_document",
        MetadataTransportMessage::ForwardedRecord { .. } => "forwarded_record",
        MetadataTransportMessage::ForwardedRead { .. } => "forwarded_read",
        MetadataTransportMessage::ForwardPathLookup { .. } => "forward_path_lookup",
        MetadataTransportMessage::ForwardedPathLookup { .. } => "forwarded_path_lookup",
        MetadataTransportMessage::ForwardPathResolution { .. } => "forward_path_resolution",
        MetadataTransportMessage::ForwardedPathResolution { .. } => "forwarded_path_resolution",
        MetadataTransportMessage::ForwardedWriteDenied { .. } => "forwarded_write_denied",
        MetadataTransportMessage::WriteNotFound => "forwarded_write_not_found",
        MetadataTransportMessage::ForwardedWriteUnavailable => "forwarded_write_unavailable",
        MetadataTransportMessage::ForwardedDelete => "forwarded_delete",
        MetadataTransportMessage::ForwardExportDocument { .. } => "forward_export_document",
        MetadataTransportMessage::ForwardExportProfile { .. } => "forward_export_profile",
        MetadataTransportMessage::ForwardedExport { .. } => "forwarded_export",
        MetadataTransportMessage::QueryDocument { .. } => "query_document",
        MetadataTransportMessage::DocumentQueryResults { .. } => "document_query_results",
        MetadataTransportMessage::Reject(_) => "reject",
        MetadataTransportMessage::UpdateInvalidInput { .. } => "forwarded_update_invalid_input",
        MetadataTransportMessage::ForwardAuditPage { .. } => "forward_audit_page",
        MetadataTransportMessage::ForwardedAuditPage { .. } => "forwarded_audit_page",
        MetadataTransportMessage::ForwardTokenRevocation { .. } => "forward_token_revocation",
        MetadataTransportMessage::ForwardedTokenRevoked => "forwarded_token_revoked",
        MetadataTransportMessage::TokenRevocationCapacity => "forwarded_token_revocation_capacity",
        MetadataTransportMessage::MetadataHistoryCapacity => "forwarded_metadata_history_capacity",
        MetadataTransportMessage::ForwardPersistentId { .. } => "forward_persistent_id",
        MetadataTransportMessage::ForwardedPersistentId { .. } => "forwarded_persistent_id",
        MetadataTransportMessage::ForwardPlacementPolicy { .. } => "forward_placement_policy",
        MetadataTransportMessage::ForwardedPlacementPolicy { .. } => "forwarded_placement_policy",
        MetadataTransportMessage::ForwardCreatePolicy { .. } => "forward_create_placement_policy",
        MetadataTransportMessage::PlacementPolicyCreated { .. } => {
            "forwarded_placement_policy_created"
        }
        MetadataTransportMessage::ForwardJobRecord { .. } => "forward_job_record",
        MetadataTransportMessage::ForwardedJobRecord { .. } => "forwarded_job_record",
        MetadataTransportMessage::ForwardRecordPage { .. } => "forward_job_record_page",
        MetadataTransportMessage::ForwardedRecordPage { .. } => "forwarded_job_record_page",
        MetadataTransportMessage::ForwardLaunchOffer { .. } => "forward_launch_offer",
        MetadataTransportMessage::ForwardedLaunchOffer { .. } => "forwarded_launch_offer",
        MetadataTransportMessage::ForwardJobSubmission { .. } => "forward_job_submission",
        MetadataTransportMessage::ForwardedJobSubmission { .. } => "forwarded_job_submission",
        MetadataTransportMessage::ForwardedProfileValidation { .. } => {
            "forwarded_profile_validation"
        }
        MetadataTransportMessage::ForwardValidationStatus { .. } => {
            "forward_profile_validation_status"
        }
        MetadataTransportMessage::ForwardedValidationStatus { .. } => {
            "forwarded_profile_validation_status"
        }
        MetadataTransportMessage::ReferencePreflight { .. } => "reference_preflight",
        MetadataTransportMessage::ReferencePreflightResults { .. } => "reference_preflight_results",
        MetadataTransportMessage::ForwardAdminEvent { .. } => "forward_admin_event",
        MetadataTransportMessage::AdminEventQueued => "forwarded_admin_event_queued",
        MetadataTransportMessage::ForwardGroupCreate { .. } => "forward_group_create",
        MetadataTransportMessage::ForwardedGroupCreated { .. } => "forwarded_group_created",
        MetadataTransportMessage::GroupDeletion { .. } => "group_deletion",
        MetadataTransportMessage::GroupDeletionResult { .. } => "group_deletion_result",
        MetadataTransportMessage::LookupIdentifier { .. } => "lookup_identifier",
        MetadataTransportMessage::IdentifierMatches { .. } => "identifier_matches",
        MetadataTransportMessage::ForwardedRevisionConflict { .. } => "forwarded_revision_conflict",
        MetadataTransportMessage::ForwardVaultChange { .. } => "forward_vault_change",
        MetadataTransportMessage::ForwardedVaultChange { .. } => "forwarded_vault_change",
        MetadataTransportMessage::FetchVaultRecords { .. } => "fetch_vault_records",
        MetadataTransportMessage::FetchedVaultRecords { .. } => "fetched_vault_records",
        MetadataTransportMessage::ForwardAdmitMember { .. } => "forward_admit_member",
        MetadataTransportMessage::AdmittedMember { .. } => "admitted_member",
        MetadataTransportMessage::ForwardDistributionRequest { .. } => {
            "forward_distribution_request"
        }
        MetadataTransportMessage::SubmitDistributionStatement { .. } => {
            "submit_distribution_statement"
        }
        MetadataTransportMessage::DistributionAccepted { .. } => "distribution_accepted",
        MetadataTransportMessage::ForwardSyncPull { .. } => "forward_sync_pull",
        MetadataTransportMessage::ForwardedSyncPull { .. } => "forwarded_sync_pull",
        MetadataTransportMessage::ForwardPullStatus { .. } => "forward_pull_status",
        MetadataTransportMessage::ForwardListVersions { .. } => "forward_list_versions",
        MetadataTransportMessage::ForwardedVersions { .. } => "forwarded_versions",
        MetadataTransportMessage::ForwardCreateBucket { .. } => "forward_create_bucket",
        MetadataTransportMessage::ForwardedBucketCreated { .. } => "forwarded_bucket_created",
        MetadataTransportMessage::ForwardCheckBucket { .. } => "forward_check_bucket",
        MetadataTransportMessage::ForwardedBucketChecked { .. } => "forwarded_bucket_checked",
        MetadataTransportMessage::ForwardNewBucket { .. } => "forward_new_bucket",
        MetadataTransportMessage::ForwardedNewBucket { .. } => "forwarded_new_bucket",
        MetadataTransportMessage::FetchRealmDocuments { .. } => "fetch_realm_documents",
        MetadataTransportMessage::FetchedRealmDocuments { .. } => "fetched_realm_documents",
        MetadataTransportMessage::FetchGraphState { .. } => "fetch_graph_state",
        MetadataTransportMessage::FetchedGraphState { .. } => "fetched_graph_state",
        MetadataTransportMessage::ForwardApplyBatch { .. } => "forward_apply_batch",
        MetadataTransportMessage::ForwardedApplyBatch { .. } => "forwarded_apply_batch",
        MetadataTransportMessage::GroupCreateConflict { .. } => "forwarded_group_create_conflict",
        MetadataTransportMessage::DataLinks { .. } => "data_links",
        MetadataTransportMessage::DataLinkResults { .. } => "data_link_results",
        MetadataTransportMessage::BucketOwner { .. } => "bucket_owner",
        MetadataTransportMessage::BucketOwnerResult { .. } => "bucket_owner_result",
        MetadataTransportMessage::SetPreview { .. } => "set_preview",
        MetadataTransportMessage::SetPreviewResults { .. } => "set_preview_results",
        MetadataTransportMessage::StagingSpec { .. } => "staging_spec",
        MetadataTransportMessage::StagingSpecResult { .. } => "staging_spec_result",
        MetadataTransportMessage::ReferencePage { .. } => "reference_page",
        MetadataTransportMessage::ReferencePageAnswer { .. } => "reference_page_answer",
        MetadataTransportMessage::HandRealmKey { .. } => "hand_realm_key",
        MetadataTransportMessage::RealmKeyStored { .. } => "realm_key_stored",
        MetadataTransportMessage::PromotionCheck => "promotion_check",
        MetadataTransportMessage::PromotionReady { .. } => "promotion_ready",
        MetadataTransportMessage::JobInputPage { .. } => "job_input_page",
        MetadataTransportMessage::InputPageResult { .. } => "job_input_page_result",
        MetadataTransportMessage::ResolveJobInputs { .. } => "resolve_job_inputs",
        MetadataTransportMessage::ResolvedJobInputs { .. } => "resolved_job_inputs",
        MetadataTransportMessage::RealmSecret { .. } => "realm_secret",
        MetadataTransportMessage::RealmSecretResult { .. } => "realm_secret_result",
        MetadataTransportMessage::OpenSetting { .. } => "open_setting",
        MetadataTransportMessage::OpenSettingResult { .. } => "open_setting_result",
        MetadataTransportMessage::StoreArtifact { .. } => "store_artifact",
        MetadataTransportMessage::ArtifactStored { .. } => "artifact_stored",
        MetadataTransportMessage::FetchArtifact { .. } => "fetch_artifact",
        MetadataTransportMessage::FetchedArtifact { .. } => "fetched_artifact",
        MetadataTransportMessage::ProbeClock => "probe_clock",
        MetadataTransportMessage::ClockProbed { .. } => "clock_probed",
        MetadataTransportMessage::ForwardEvents { .. } => "forward_events",
        MetadataTransportMessage::EventsDecided { .. } => "events_decided",
        MetadataTransportMessage::ReleaseSecret { .. } => "release_secret",
        MetadataTransportMessage::SecretReleased { .. } => "secret_released",
        MetadataTransportMessage::CheckNodeSecret { .. } => "check_node_secret",
        MetadataTransportMessage::NodeSecretChecked { .. } => "node_secret_checked",
        MetadataTransportMessage::ForwardedGraphPending { .. } => "forwarded_graph_pending",
        MetadataTransportMessage::ExportAnswer { .. } => "export_answer",
    }
}
