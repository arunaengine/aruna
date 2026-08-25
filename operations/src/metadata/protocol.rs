use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::admin_documents::{AdminDocumentClock, AdminDocumentEvent};
use aruna_core::audit::{AuditPageRequest, AuditPageResponse, MAX_AUDIT_PAGE_BYTES};
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{FetchCursor, JobRecordFrame, LaunchFrame, PageLimit, ReceiptFrame};
use aruna_core::events::{JobRecordPage, JobRecordRejection, LaunchDecline};
use aruna_core::metadata::{
    MetadataProfileValidationFinding, MetadataProfileValidationStatus, MetadataQueryResults,
    MetadataSearchHit,
};
use aruna_core::structs::{
    Group, GroupAuthorizationDocument, MetadataRegistryRecord, PathClaimRecord,
    PersistentIdFailure, PersistentIdMapping, PlacementPolicy, PlacementPolicyDocument,
    PlacementPolicyRef, PlacementRef, SubmissionId, SyncListCursor, SyncPageLimit, SyncPullAck,
    SyncRefusal, SyncRelationship, SyncVersionPage, VersionedObjectArn,
};
use aruna_core::types::{GroupId, UserId};
use aruna_net::streams::BiStream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use ulid::Ulid;

use crate::create_metadata_document::CreateMetadataDocumentPayload;
use crate::jobs::lifecycle::ids::SubmissionRequest;
use crate::jobs::lifecycle::ingress::{SubmissionAck, SubmissionRefusal};
use crate::metadata::api::{
    MetadataReferencePreflightNodeExecution, MetadataReferencePreflightNodeRequest,
    MetadataRoCrateExportView,
};
use crate::request_policy::PolicyRequestExtras;
use crate::s3::search_buckets::BucketSearchHit;
use crate::s3::search_objects::{ObjectKeyMatch, ObjectSearchNodePage};
use crate::update_metadata_document::UpdateMetadataDocumentMutation;

pub use aruna_core::metadata::{MetadataAuthToken, MetadataAuthTokenError};

pub(crate) const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const AUDIT_FRAME_OVERHEAD: usize = 256;
pub(crate) const METADATA_INBOUND_FRAME_BYTES: usize = 64 * 1024 * 1024;
const STANDARD_FRAME: u8 = 0;
const AUDIT_FRAME: u8 = 1;
const POSTCARD_VARIANT_BYTES: usize = 5;
const AUDIT_REQUEST_VARIANT: u32 = 31;
const AUDIT_RESPONSE_VARIANT: u32 = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPathCandidate {
    pub claim: PathClaimRecord,
    pub record: Option<MetadataRegistryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPathWinner {
    pub realm_id: aruna_core::structs::RealmId,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub document_path: String,
    pub graph_iri: String,
    pub public: bool,
    pub replicas: usize,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPathResolution {
    pub winner: MetadataPathWinner,
    pub conflicts: Vec<Ulid>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataTransportMessage {
    QueryGraphs {
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    },
    QueryResults {
        result: Result<MetadataQueryResults, MetadataReadError>,
    },
    SearchGraphs {
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        group_id: Option<GroupId>,
    },
    SearchResults {
        result: Result<Vec<MetadataSearchHit>, MetadataReadError>,
    },
    /// A metadata write that arrived at a node holding none of the document's
    /// bucket, forwarded to a holder. The payloads mirror the HTTP handlers'
    /// deconstructed request; `auth_token` carries the caller's authority so the
    /// holder re-runs the same permission checks the origin would have run.
    ForwardCreateDocument {
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        group_id: GroupId,
        document_id: Ulid,
        document_path: String,
        public: bool,
        payload: CreateMetadataDocumentPayload,
    },
    ForwardUpdateDocument {
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
        /// `None` leaves the holder's current visibility untouched: the origin's
        /// record copy may be stale, so only an explicit request value travels.
        public: Option<bool>,
        mutation: UpdateMetadataDocumentMutation,
    },
    ForwardDeleteDocument {
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
    },
    ForwardedRecord {
        record: Box<MetadataRegistryRecord>,
    },
    ForwardedDelete,
    Reject(String),
    /// A permanent update validation failure. Appended after `Reject` so the
    /// postcard discriminants of existing control messages remain stable.
    ForwardedUpdateInvalidInput {
        message: String,
    },
    FilteredSearchGraphs {
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        predicate_iri: String,
        object_iri: String,
        group_id: Option<GroupId>,
    },
    SearchBuckets {
        auth_token: Option<MetadataAuthToken>,
        query: String,
        limit: usize,
    },
    BucketSearchResults {
        result: Result<Vec<BucketSearchHit>, MetadataReadError>,
    },
    CreateSyncMirror {
        auth_token: Option<MetadataAuthToken>,
        source_group_id: GroupId,
        relationship: Box<SyncRelationship>,
        extras: PolicyRequestExtras,
    },
    DeleteSyncMirror {
        auth_token: Option<MetadataAuthToken>,
        relationship: Box<SyncRelationship>,
        extras: PolicyRequestExtras,
    },
    SyncMirrorCreated,
    SyncMirrorDeleted,
    ForwardReadDocument {
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
    },
    ForwardedRead {
        result: Result<Box<MetadataRegistryRecord>, MetadataReadError>,
    },
    ForwardPathLookup {
        auth_token: Option<MetadataAuthToken>,
        group_id: GroupId,
        document_path: String,
        config_digest: [u8; 32],
    },
    ForwardedPathLookup {
        result: Result<Vec<MetadataPathCandidate>, MetadataReadError>,
    },
    ForwardedWriteDenied {
        error: MetadataWriteAuthError,
    },
    ForwardPathResolution {
        auth_token: Option<MetadataAuthToken>,
        group_id: GroupId,
        document_path: String,
        config_digest: [u8; 32],
    },
    ForwardedPathResolution {
        result: Result<Box<MetadataPathResolution>, MetadataReadError>,
    },
    ForwardedWriteNotFound,
    ForwardedWriteUnavailable,
    /// An RO-Crate export forwarded to a holder with the caller's bearer or
    /// peer-attested internal principal for another READ check.
    ForwardExportDocument {
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
        view: MetadataRoCrateExportView,
        metadata_bytes: u64,
        limit: Option<usize>,
        offset: Option<usize>,
        after: Option<String>,
    },
    ForwardedExport {
        result: Result<u64, MetadataReadError>,
    },
    QueryDocument {
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
        sparql: String,
    },
    DocumentQueryResults {
        result: Result<MetadataQueryResults, MetadataReadError>,
    },
    /// A request for one node's local page of a group's audit trail. Appended
    /// last so existing control-message discriminants stay stable.
    ForwardAuditPage {
        request: AuditPageRequest,
    },
    ForwardedAuditPage {
        result: Result<AuditPageResponse, MetadataReadError>,
    },
    /// A User-kind node forwards a bearer-token revocation to a node that may
    /// publish realm administration events.
    ForwardTokenRevocation {
        auth_token: MetadataAuthToken,
        token: String,
    },
    ForwardedTokenRevoked,
    ForwardedTokenRevocationCapacity,
    ForwardedMetadataHistoryCapacity,
    /// A PID mapping transition or landing resolution routed to a document
    /// holder, which is the mapping's authority. Appended last so the postcard
    /// variant indices the frame classifier depends on stay stable.
    ForwardPersistentId {
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
        request: PersistentIdRequest,
    },
    ForwardedPersistentId {
        result: Result<PersistentIdOutcome, MetadataReadError>,
    },
    /// One immutable policy document asked of a holder the requester resolved
    /// from the policy id. Appended last so existing variant indices stay stable.
    ForwardPlacementPolicy {
        policy_ref: PlacementPolicyRef,
    },
    /// `Ok(None)` means this holder has no such document; it never means the
    /// policy does not exist.
    ForwardedPlacementPolicy {
        result: Result<Option<Box<PlacementPolicyDocument>>, MetadataReadError>,
    },
    /// A realm-admin publication routed to a holder of the policy's bucket,
    /// because only a holder may commit the immutable document. Appended last so
    /// existing variant indices stay stable.
    ForwardCreatePlacementPolicy {
        auth_token: Option<MetadataAuthToken>,
        policy: Box<PlacementPolicy>,
        created_at_ms: u64,
    },
    /// The document the holder committed, or the identical one it already had.
    ForwardedPlacementPolicyCreated {
        document: Box<PlacementPolicyDocument>,
    },
    /// One immutable job-family record offered to a holder of the family
    /// placement. The frame is bounded at decode and the envelope keeps its own
    /// publisher, so the authenticated peer is only the relay. Appended last so
    /// existing variant indices stay stable.
    ForwardJobRecord {
        placement: PlacementRef,
        record: Box<JobRecordFrame>,
    },
    /// `Ok` means the holder durably accepted or already had the record.
    ForwardedJobRecord {
        result: Result<(), JobRecordRejection>,
    },
    /// A bounded page of one submission's immutable records read from a holder.
    ForwardJobRecordPage {
        placement: PlacementRef,
        submission_id: SubmissionId,
        /// `None` reads every request family of the submission.
        request_digest: Option<[u8; 32]>,
        cursor: Option<FetchCursor>,
        limit: PageLimit,
    },
    ForwardedJobRecordPage {
        result: Result<JobRecordPageReply, JobRecordRejection>,
    },
    /// One scheduler's launch offer to an execution target. It carries no
    /// caller token: the target verifies the signed launch itself.
    ForwardLaunchOffer {
        launch: Box<LaunchFrame>,
    },
    ForwardedLaunchOffer {
        result: Result<Box<ReceiptFrame>, LaunchDecline>,
    },
    /// One complete external submission forwarded a single hop to an observed
    /// family holder, with the identity the ingress preassigned. The holder
    /// revalidates the caller and recomputes that identity before it commits,
    /// and never forwards it again. Appended after the earlier variants so
    /// their indices stay stable.
    ForwardJobSubmission {
        auth_token: MetadataAuthToken,
        submission_id: SubmissionId,
        request: Box<SubmissionRequest>,
    },
    ForwardedJobSubmission {
        result: Result<SubmissionAck, SubmissionRefusal>,
    },
    /// Authenticated live-head object inventory search. These variants are
    /// appended so every pre-existing transport discriminant remains stable.
    SearchObjects {
        auth_token: Option<MetadataAuthToken>,
        query: String,
        key_match: ObjectKeyMatch,
        bucket: Option<String>,
        limit: usize,
        start_after: Option<Vec<u8>>,
        as_of: SystemTime,
    },
    ObjectSearchResults {
        result: Result<ObjectSearchNodePage, MetadataReadError>,
    },
    /// A structured Profile gate rejection returned by a holder.
    /// Appended after the object-search variants so every existing postcard
    /// discriminant, including theirs, remains stable.
    ForwardedProfileValidation {
        findings: Vec<MetadataProfileValidationFinding>,
    },
    /// Read or deterministically recompute a document's revision-bound Profile
    /// status on a holder, under the caller's READ authority.
    ForwardProfileValidationStatus {
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
        revalidate: bool,
    },
    ForwardedProfileValidationStatus {
        result: Result<Box<MetadataProfileValidationStatus>, MetadataReadError>,
    },
    /// One node's exact-IRI backlink and location-impact partition. Appended after
    /// the existing tail variants so all prior postcard discriminants remain stable.
    ReferencePreflight {
        auth_token: Option<MetadataAuthToken>,
        request: Box<MetadataReferencePreflightNodeRequest>,
    },
    ReferencePreflightResults {
        result: Result<Box<MetadataReferencePreflightNodeExecution>, MetadataReadError>,
    },
    /// An administrative event whose origin holds none of the target's shard,
    /// handed to a holder that relays the exact origin-signed envelope. It
    /// deliberately carries no caller token: the envelope's origin signature is
    /// the authority, and every receiver re-authorizes against the origin.
    /// Appended after the preflight variants so all prior postcard discriminants
    /// remain stable.
    ForwardAdminEvent {
        target: DocumentSyncTarget,
        event: Box<AdminDocumentEvent>,
        placement: PlacementRef,
        origin_signature: iroh::Signature,
    },
    ForwardedAdminEventQueued,
    /// A User node cannot originate a realm administrative event, so its local
    /// group create travels to a sync-eligible ingress under the caller's own
    /// token. That ingress authorizes the caller and originates the event.
    ForwardGroupCreate {
        auth_token: Option<MetadataAuthToken>,
        display_name: String,
    },
    ForwardedGroupCreated {
        group: Box<Group>,
        authorization: Box<GroupAuthorizationDocument>,
    },
    ForwardedGroupCreateConflict {
        reason: String,
    },
    /// One device version a synced folder asks its realm node to pull and
    /// commit as the owner. The realm node reads the exact version from the
    /// device and writes its own copy, so the device never pushes. Appended
    /// after the earlier variants so their indices stay stable.
    ForwardSyncPull {
        auth_token: MetadataAuthToken,
        source: Box<VersionedObjectArn>,
        blake3: Option<[u8; 32]>,
        size: u64,
        target_bucket: String,
        target_key: String,
        /// A local delete asks for a delete marker instead of a version.
        deleted: bool,
    },
    ForwardedSyncPull {
        result: Result<SyncPullAck, SyncRefusal>,
    },
    /// Bounded listing of the current heads under one bucket prefix, served as
    /// a routed read with the requesting owner's authority.
    ForwardListVersions {
        auth_token: MetadataAuthToken,
        bucket: String,
        prefix: String,
        cursor: Option<SyncListCursor>,
        limit: SyncPageLimit,
    },
    ForwardedVersions {
        result: Result<SyncVersionPage, SyncRefusal>,
    },
    /// The realm-wide documents a device asks a realm node for. A device takes
    /// no part in document sync, so this routed read is how the configuration
    /// it is judged by - revocations included - reaches it.
    FetchRealmDocuments {
        auth_token: MetadataAuthToken,
    },
    FetchedRealmDocuments {
        result: Result<RealmDocuments, SyncRefusal>,
    },
}

/// The realm-wide documents as the serving node stores them. The copies a
/// device installs from them are never published again: they are a read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmDocuments {
    pub realm_config: Vec<u8>,
    /// Absent while the realm has no authorization document yet.
    pub realm_authorization: Option<Vec<u8>>,
    /// What the serving node had applied when it made this copy. A device
    /// refuses a copy that has seen less than the one it already holds.
    pub clock: AdminDocumentClock,
}

/// One page of immutable records plus the cursor of the next one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecordPageReply {
    pub page: JobRecordPage,
    pub next: Option<FetchCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistentIdRequest {
    Mint {
        minted_by: UserId,
        minted_at_ms: u64,
    },
    Withdraw {
        withdrawn_by: UserId,
        reason: String,
        withdrawn_at_ms: u64,
    },
    Fail {
        failure: PersistentIdFailure,
    },
    /// Queue the mint job on the authority, so one document has one dedup row and
    /// one execution however many ingress nodes accept the request.
    SubmitMint {
        minted_by: UserId,
        retention_ms: u64,
    },
    /// Unauthenticated landing resolution: the authority applies the same
    /// per-record anonymous readability check a single-record OAI read applies.
    Resolve {
        pid: String,
    },
    /// Trusted realm-peer read used by the authenticated typed status route.
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistentIdOutcome {
    Mapping {
        mapping: Box<PersistentIdMapping>,
        /// Whether this call performed the transition rather than observing one.
        changed: bool,
    },
    Resolution(PersistentIdResolution),
    Status(Option<Box<PersistentIdMapping>>),
    /// The authority's mint job: its id is owned by the authority, and `created`
    /// is false for a caller that joined the job another submitter opened.
    Submission {
        job_id: aruna_core::structs::JobId,
        created: bool,
    },
}

/// What a landing request resolves to. `Gone` outranks `Missing`: a withdrawn PID
/// is a permanent tombstone even for a document that is not anonymously visible,
/// which discloses only that a once-public PID is gone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistentIdResolution {
    Redirect,
    Gone { pid: String },
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataReadError {
    Unauthorized,
    Forbidden,
    NotFound,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataWriteAuthError {
    Unauthorized,
    Forbidden,
}

pub async fn write_message(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
) -> Result<(), String> {
    let bytes = encode_message(message)?;
    write_encoded_message(stream, frame_class(message), &bytes).await
}

pub(crate) fn encode_message(message: &MetadataTransportMessage) -> Result<Vec<u8>, String> {
    if frame_class(message) == AUDIT_FRAME {
        let size =
            postcard::experimental::serialized_size(message).map_err(|err| err.to_string())?;
        if size > audit_frame_cap() {
            return Err("metadata message exceeds maximum size".to_string());
        }
    }
    let bytes = postcard::to_allocvec(message).map_err(|err| err.to_string())?;
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err("metadata message exceeds maximum size".to_string());
    }

    Ok(bytes)
}

pub(crate) async fn write_encoded_message(
    stream: &mut BiStream,
    class: u8,
    bytes: &[u8],
) -> Result<(), String> {
    if class != STANDARD_FRAME && class != AUDIT_FRAME {
        return Err("invalid metadata frame class".to_string());
    }
    stream
        .0
        .write_all(&[class])
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(bytes)
        .await
        .map_err(|err| err.to_string())?;
    stream.0.flush().await.map_err(|err| err.to_string())?;
    Ok(())
}

pub async fn read_message(stream: &mut BiStream) -> Result<MetadataTransportMessage, String> {
    read_message_cap(&mut stream.1, MAX_MESSAGE_SIZE).await
}

pub(crate) async fn read_message_budget<R>(
    reader: &mut R,
    max_size: usize,
    byte_budget: &Arc<Semaphore>,
) -> Result<(MetadataTransportMessage, OwnedSemaphorePermit), String>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let (message, permit) = read_message_inner(reader, max_size, Some(byte_budget)).await?;
    permit
        .ok_or_else(|| "metadata inbound frame budget missing".to_string())
        .map(|permit| (message, permit))
}

pub(crate) async fn read_message_cap<R>(
    reader: &mut R,
    max_size: usize,
) -> Result<MetadataTransportMessage, String>
where
    R: AsyncRead + Unpin + ?Sized,
{
    read_message_inner(reader, max_size, None)
        .await
        .map(|(message, _)| message)
}

async fn read_message_inner<R>(
    reader: &mut R,
    max_size: usize,
    byte_budget: Option<&Arc<Semaphore>>,
) -> Result<(MetadataTransportMessage, Option<OwnedSemaphorePermit>), String>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut class_buf = [0u8; 1];
    reader
        .read_exact(&mut class_buf)
        .await
        .map_err(|err| err.to_string())?;
    let class = class_buf[0];
    if class != STANDARD_FRAME && class != AUDIT_FRAME {
        return Err("invalid metadata frame class".to_string());
    }

    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| err.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size.min(MAX_MESSAGE_SIZE) {
        return Err("metadata frame exceeds maximum size".to_string());
    }
    if class == AUDIT_FRAME && len > audit_frame_cap() {
        return Err("metadata audit frame exceeds maximum size".to_string());
    }

    let permit = match byte_budget {
        Some(byte_budget) => {
            let permits = u32::try_from(len)
                .map_err(|_| "metadata frame length is unsupported".to_string())?;
            Some(
                byte_budget
                    .clone()
                    .try_acquire_many_owned(permits)
                    .map_err(|_| "metadata inbound frame budget unavailable".to_string())?,
            )
        }
        None => None,
    };

    let mut variant = [0u8; POSTCARD_VARIANT_BYTES];
    let variant_len = len.min(POSTCARD_VARIANT_BYTES);
    let mut prefix_len = 0;
    if variant_len > 0 {
        reader
            .read_exact(&mut variant[..1])
            .await
            .map_err(|err| err.to_string())?;
        prefix_len = 1;
        while prefix_len < variant_len && variant[prefix_len - 1] & 0x80 != 0 {
            reader
                .read_exact(&mut variant[prefix_len..prefix_len + 1])
                .await
                .map_err(|err| err.to_string())?;
            prefix_len += 1;
        }
        let variant = parse_variant(&variant[..prefix_len])
            .map_err(|_| "metadata frame variant is invalid".to_string())?;
        if len > audit_frame_cap() && variant.is_some_and(is_audit_variant) {
            return Err("metadata audit frame exceeds maximum size".to_string());
        }
    }

    let mut bytes = vec![0u8; len];
    bytes[..prefix_len].copy_from_slice(&variant[..prefix_len]);
    reader
        .read_exact(&mut bytes[prefix_len..])
        .await
        .map_err(|err| err.to_string())?;
    let message = postcard::from_bytes(&bytes).map_err(|err| err.to_string())?;
    let decoded_class = frame_class(&message);
    if decoded_class != class {
        return Err("metadata frame class does not match message".to_string());
    }
    if decoded_class == AUDIT_FRAME && len > audit_frame_cap() {
        return Err("metadata audit frame exceeds maximum size".to_string());
    }
    Ok((message, permit))
}

fn audit_frame_cap() -> usize {
    MAX_AUDIT_PAGE_BYTES.saturating_add(AUDIT_FRAME_OVERHEAD)
}

fn parse_variant(prefix: &[u8]) -> Result<Option<u32>, ()> {
    let Some(&last) = prefix.last() else {
        return Ok(None);
    };
    if last & 0x80 != 0 || (prefix.len() == POSTCARD_VARIANT_BYTES && last > 0x0f) {
        return Err(());
    }
    postcard::from_bytes::<u32>(prefix)
        .map(Some)
        .map_err(|_| ())
}

fn is_audit_variant(variant: u32) -> bool {
    variant == AUDIT_REQUEST_VARIANT || variant == AUDIT_RESPONSE_VARIANT
}

pub(crate) fn response_cap(message: &MetadataTransportMessage) -> usize {
    match message {
        MetadataTransportMessage::ForwardAuditPage { .. } => audit_frame_cap(),
        _ => MAX_MESSAGE_SIZE,
    }
}

pub(crate) fn frame_class(message: &MetadataTransportMessage) -> u8 {
    match message {
        MetadataTransportMessage::ForwardAuditPage { .. }
        | MetadataTransportMessage::ForwardedAuditPage { .. } => AUDIT_FRAME,
        _ => STANDARD_FRAME,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use aruna_core::audit::{AuditPageEntry, AuditPageRequest};
    use aruna_core::metadata::MAX_METADATA_BEARER_TOKEN_LEN;
    use aruna_core::structs::{
        AuthContext, MetadataAuditOperation, MetadataAuditRecord, PathRestriction, Permission,
        RealmId,
    };
    use aruna_core::types::UserId;
    use tokio::sync::Semaphore;

    #[test]
    fn transport_messages_use_auth_token_fields() {
        assert_has_auth_token_field(MetadataTransportMessage::QueryGraphs {
            auth_token: Some(MetadataAuthToken::bearer("query-token").unwrap()),
            graph_iris: None,
            sparql: "ASK {}".to_string(),
        });
        assert_has_auth_token_field(MetadataTransportMessage::SearchGraphs {
            auth_token: Some(MetadataAuthToken::bearer("search-token").unwrap()),
            graph_iris: None,
            query: "dataset".to_string(),
            limit: 10,
            group_id: None,
        });
        assert_has_auth_token_field(MetadataTransportMessage::FilteredSearchGraphs {
            auth_token: Some(MetadataAuthToken::bearer("filtered-search-token").unwrap()),
            graph_iris: None,
            query: String::new(),
            limit: 10,
            predicate_iri: "http://schema.org/conformsTo".to_string(),
            object_iri: "https://example.com/profile".to_string(),
            group_id: None,
        });
        assert_has_auth_token_field(MetadataTransportMessage::SearchBuckets {
            auth_token: Some(MetadataAuthToken::bearer("bucket-token").unwrap()),
            query: "dataset".to_string(),
            limit: 10,
        });
        assert_has_auth_token_field(MetadataTransportMessage::SearchObjects {
            auth_token: Some(MetadataAuthToken::bearer("object-token").unwrap()),
            query: "reads".to_string(),
            key_match: ObjectKeyMatch::Substring,
            bucket: Some("data".to_string()),
            limit: 10,
            start_after: None,
            as_of: SystemTime::UNIX_EPOCH,
        });
    }

    #[test]
    fn forwarded_writes_carry_authority() {
        // The holder applies a forwarded write under the caller's own token, so
        // every forward variant must carry one: a tokenless forward would be an
        // unauthenticated internal write path.
        assert_has_auth_token_field(MetadataTransportMessage::ForwardCreateDocument {
            auth_token: Some(MetadataAuthToken::bearer("create-token").unwrap()),
            config_digest: [0; 32],
            group_id: Ulid::nil(),
            document_id: Ulid::nil(),
            document_path: "datasets/forwarded".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::RoCrate {
                jsonld: "{}".to_string(),
            },
        });
        assert_has_auth_token_field(MetadataTransportMessage::ForwardUpdateDocument {
            auth_token: Some(MetadataAuthToken::bearer("update-token").unwrap()),
            config_digest: [0; 32],
            document_id: Ulid::nil(),
            public: None,
            mutation: UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: "{}".to_string(),
            },
        });
        assert_has_auth_token_field(MetadataTransportMessage::ForwardDeleteDocument {
            auth_token: Some(MetadataAuthToken::bearer("delete-token").unwrap()),
            config_digest: [0; 32],
            document_id: Ulid::nil(),
        });
        assert_has_auth_token_field(MetadataTransportMessage::ForwardExportDocument {
            auth_token: Some(MetadataAuthToken::bearer("export-token").unwrap()),
            config_digest: [0; 32],
            document_id: Ulid::nil(),
            view: MetadataRoCrateExportView::Raw,
            metadata_bytes: 16 * 1024 * 1024,
            limit: None,
            offset: None,
            after: None,
        });
        assert_has_auth_token_field(MetadataTransportMessage::ForwardTokenRevocation {
            auth_token: MetadataAuthToken::bearer("revoke-token").unwrap(),
            token: "target-token".to_string(),
        });
    }

    #[test]
    fn forwarded_create_round_trips() {
        let message = MetadataTransportMessage::ForwardCreateDocument {
            auth_token: Some(MetadataAuthToken::bearer("create-token").unwrap()),
            config_digest: [0; 32],
            group_id: Ulid::from_bytes([3u8; 16]),
            document_id: Ulid::from_bytes([4u8; 16]),
            document_path: "datasets/forwarded".to_string(),
            public: false,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Forwarded".to_string(),
                description: "Placed by a holder".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
        };
        let bytes = postcard::to_allocvec(&message).unwrap();

        assert_eq!(
            postcard::from_bytes::<MetadataTransportMessage>(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn token_revoke_roundtrip() {
        for response in [
            MetadataTransportMessage::ForwardedTokenRevoked,
            MetadataTransportMessage::ForwardedTokenRevocationCapacity,
        ] {
            let bytes = postcard::to_allocvec(&response).unwrap();

            assert_eq!(
                postcard::from_bytes::<MetadataTransportMessage>(&bytes).unwrap(),
                response
            );
        }
    }

    #[test]
    fn history_capacity_roundtrip() {
        let message = MetadataTransportMessage::ForwardedMetadataHistoryCapacity;
        let bytes = postcard::to_allocvec(&message).unwrap();

        assert_eq!(
            postcard::from_bytes::<MetadataTransportMessage>(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn path_lookup_roundtrip() {
        let message = MetadataTransportMessage::ForwardPathLookup {
            auth_token: Some(MetadataAuthToken::bearer("path-token").unwrap()),
            group_id: Ulid::from_bytes([3u8; 16]),
            document_path: "datasets/private".to_string(),
            config_digest: [4u8; 32],
        };
        let bytes = postcard::to_allocvec(&message).unwrap();

        assert_eq!(
            postcard::from_bytes::<MetadataTransportMessage>(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn path_resolution_roundtrip() {
        let message = MetadataTransportMessage::ForwardPathResolution {
            auth_token: Some(MetadataAuthToken::bearer("path-token").unwrap()),
            group_id: Ulid::from_bytes([3u8; 16]),
            document_path: "datasets/private".to_string(),
            config_digest: [4u8; 32],
        };
        let bytes = postcard::to_allocvec(&message).unwrap();

        assert_eq!(
            postcard::from_bytes::<MetadataTransportMessage>(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn read_errors_roundtrip() {
        for error in [
            MetadataReadError::Unauthorized,
            MetadataReadError::Forbidden,
            MetadataReadError::NotFound,
            MetadataReadError::Unavailable,
        ] {
            let query = MetadataTransportMessage::QueryResults { result: Err(error) };
            let search = MetadataTransportMessage::SearchResults { result: Err(error) };
            let buckets = MetadataTransportMessage::BucketSearchResults { result: Err(error) };
            let objects = MetadataTransportMessage::ObjectSearchResults { result: Err(error) };

            for message in [query, search, buckets, objects] {
                let bytes = postcard::to_allocvec(&message).unwrap();
                assert_eq!(
                    postcard::from_bytes::<MetadataTransportMessage>(&bytes).unwrap(),
                    message
                );
            }
        }
    }

    #[test]
    fn legacy_reject_stable() {
        assert_eq!(
            postcard::to_allocvec(&MetadataTransportMessage::Reject(String::new())).unwrap(),
            vec![9, 0]
        );
        assert_eq!(
            postcard::to_allocvec(&MetadataTransportMessage::ForwardedUpdateInvalidInput {
                message: String::new(),
            })
            .unwrap(),
            vec![10, 0]
        );
        assert_eq!(
            postcard::to_allocvec(&MetadataTransportMessage::SearchBuckets {
                auth_token: None,
                query: String::new(),
                limit: 0,
            })
            .unwrap(),
            vec![12, 0, 0, 0]
        );
    }

    #[test]
    fn oversized_bearer_tokens_are_rejected() {
        let oversized = "x".repeat(MAX_METADATA_BEARER_TOKEN_LEN + 1);

        assert!(MetadataAuthToken::bearer(oversized).is_err());
    }

    #[tokio::test]
    async fn audit_frame_rejects() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let length = (audit_frame_cap() + 1) as u32;
        writer.write_all(&[AUDIT_FRAME]).await.unwrap();
        writer.write_all(&length.to_be_bytes()).await.unwrap();

        let error = read_message_cap(&mut reader, audit_frame_cap())
            .await
            .unwrap_err();
        assert_eq!(error, "metadata frame exceeds maximum size");
    }

    #[tokio::test]
    async fn audit_rejects_early() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let length = (audit_frame_cap() + 1) as u32;
        writer.write_all(&[AUDIT_FRAME]).await.unwrap();
        writer.write_all(&length.to_be_bytes()).await.unwrap();
        let budget = Arc::new(Semaphore::new(1));

        let error = read_message_budget(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap_err();
        assert_eq!(error, "metadata audit frame exceeds maximum size");
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test]
    async fn audit_wire_cap() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let length = (audit_frame_cap() + 1) as u32;
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer.write_all(&length.to_be_bytes()).await.unwrap();
        writer
            .write_all(&[AUDIT_REQUEST_VARIANT as u8])
            .await
            .unwrap();
        let budget = Arc::new(Semaphore::new(METADATA_INBOUND_FRAME_BYTES));

        let error = read_message_budget(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap_err();
        assert_eq!(error, "metadata audit frame exceeds maximum size");
        assert_eq!(budget.available_permits(), METADATA_INBOUND_FRAME_BYTES);
    }

    #[test]
    fn audit_variants_match() {
        let messages = [
            (
                MetadataTransportMessage::ForwardAuditPage {
                    request: AuditPageRequest {
                        auth_token: None,
                        config_digest: [0; 32],
                        realm_id: RealmId([0; 32]),
                        group_id: Ulid::nil(),
                        document_id: None,
                        start_after: None,
                        limit: 1,
                    },
                },
                AUDIT_REQUEST_VARIANT,
            ),
            (
                MetadataTransportMessage::ForwardedAuditPage {
                    result: Ok(AuditPageResponse {
                        records: Vec::new(),
                        next_start_after: None,
                    }),
                },
                AUDIT_RESPONSE_VARIANT,
            ),
        ];

        for (message, expected) in messages {
            let bytes = postcard::to_allocvec(&message).unwrap();
            assert_eq!(postcard::from_bytes::<u32>(&bytes).unwrap(), expected);
            assert_eq!(frame_class(&message), AUDIT_FRAME);
        }
    }

    #[tokio::test]
    async fn audit_wire_varint() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let length = (audit_frame_cap() + 1) as u32;
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer.write_all(&length.to_be_bytes()).await.unwrap();
        writer.write_all(&[0x9f, 0]).await.unwrap();
        let budget = Arc::new(Semaphore::new(METADATA_INBOUND_FRAME_BYTES));

        let error = read_message_budget(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap_err();
        assert_eq!(error, "metadata audit frame exceeds maximum size");
        assert_eq!(budget.available_permits(), METADATA_INBOUND_FRAME_BYTES);
    }

    #[tokio::test]
    async fn audit_variant_limit() {
        for prefix in [
            vec![0x80; POSTCARD_VARIANT_BYTES],
            vec![0x9f, 0x80, 0x80, 0x80, 0x80, 0],
        ] {
            let (mut writer, mut reader) = tokio::io::duplex(16);
            let length = (audit_frame_cap() + 1) as u32;
            writer.write_all(&[STANDARD_FRAME]).await.unwrap();
            writer.write_all(&length.to_be_bytes()).await.unwrap();
            writer.write_all(&prefix).await.unwrap();
            let budget = Arc::new(Semaphore::new(METADATA_INBOUND_FRAME_BYTES));

            let error = read_message_budget(&mut reader, MAX_MESSAGE_SIZE, &budget)
                .await
                .unwrap_err();
            assert_eq!(error, "metadata frame variant is invalid");
            assert_eq!(budget.available_permits(), METADATA_INBOUND_FRAME_BYTES);
        }
    }

    #[tokio::test]
    async fn budget_releases() {
        let message = MetadataTransportMessage::Reject(String::new());
        let bytes = postcard::to_allocvec(&message).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 5);
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        writer.write_all(&bytes).await.unwrap();
        let budget = Arc::new(Semaphore::new(32));

        let (decoded, permit) = read_message_budget(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap();
        assert_eq!(decoded, message);
        assert_eq!(budget.available_permits(), 32 - bytes.len());
        drop(permit);
        assert_eq!(budget.available_permits(), 32);
    }

    #[tokio::test]
    async fn budget_rejects() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&[STANDARD_FRAME, 0, 0, 0, 8])
            .await
            .unwrap();
        let budget = Arc::new(Semaphore::new(4));

        let error = read_message_budget(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap_err();
        assert_eq!(error, "metadata inbound frame budget unavailable");
        assert_eq!(budget.available_permits(), 4);
    }

    #[tokio::test]
    async fn budget_cancels() {
        let (mut writer, reader) = tokio::io::duplex(16);
        writer
            .write_all(&[STANDARD_FRAME, 0, 0, 0, 8])
            .await
            .unwrap();
        let budget = Arc::new(Semaphore::new(8));
        let task_budget = budget.clone();
        let task = tokio::spawn(async move {
            let mut reader = reader;
            read_message_budget(&mut reader, MAX_MESSAGE_SIZE, &task_budget).await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.available_permits() == 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(budget.available_permits(), 8);
    }

    #[tokio::test]
    async fn audit_frame_reads() {
        let message = MetadataTransportMessage::ForwardedAuditPage {
            result: Ok(AuditPageResponse {
                records: Vec::new(),
                next_start_after: None,
            }),
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        assert!(bytes.len() <= audit_frame_cap());
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 5);
        writer.write_all(&[AUDIT_FRAME]).await.unwrap();
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        writer.write_all(&bytes).await.unwrap();

        assert_eq!(
            read_message_cap(&mut reader, audit_frame_cap())
                .await
                .unwrap(),
            message
        );
    }

    #[tokio::test]
    async fn audit_cap_decoded() {
        let realm_id = RealmId([1u8; 32]);
        let message = MetadataTransportMessage::ForwardedAuditPage {
            result: Ok(AuditPageResponse {
                records: vec![AuditPageEntry {
                    key: vec![0u8; aruna_core::audit::AUDIT_KEY_BYTES],
                    record: MetadataAuditRecord {
                        realm_id,
                        group_id: Ulid::from_bytes([2u8; 16]),
                        document_id: Ulid::from_bytes([3u8; 16]),
                        graph_iri: "x".repeat(MAX_AUDIT_PAGE_BYTES + AUDIT_FRAME_OVERHEAD),
                        user_id: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
                        node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
                        operation: MetadataAuditOperation::Create,
                        occurred_at_ms: 0,
                        details: None,
                    },
                }],
                next_start_after: None,
            }),
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        assert!(bytes.len() > audit_frame_cap());
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 5);
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        writer.write_all(&bytes).await.unwrap();

        assert_eq!(
            read_message_cap(&mut reader, MAX_MESSAGE_SIZE)
                .await
                .unwrap_err(),
            "metadata audit frame exceeds maximum size"
        );
    }

    #[tokio::test]
    async fn frame_class_matches() {
        let message = MetadataTransportMessage::ForwardedAuditPage {
            result: Ok(AuditPageResponse {
                records: Vec::new(),
                next_start_after: None,
            }),
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 5);
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        writer.write_all(&bytes).await.unwrap();

        assert_eq!(
            read_message_cap(&mut reader, MAX_MESSAGE_SIZE)
                .await
                .unwrap_err(),
            "metadata frame class does not match message"
        );
    }

    #[test]
    fn bearer_auth_token_round_trips_through_postcard() {
        let token = MetadataAuthToken::bearer("bearer-token").unwrap();
        let bytes = postcard::to_allocvec(&token).unwrap();

        let decoded = postcard::from_bytes::<MetadataAuthToken>(&bytes).unwrap();

        assert_eq!(decoded, token);
        let MetadataAuthToken::Bearer(bearer) = decoded else {
            panic!("expected bearer token");
        };
        assert_eq!(bearer.as_str(), "bearer-token");
    }

    #[test]
    fn internal_auth_roundtrip() {
        let realm_id = RealmId([7; 32]);
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([8; 16]), realm_id),
            realm_id,
            path_restrictions: Some(vec![PathRestriction {
                pattern: format!("/{realm_id}/g/**"),
                permission: Permission::READ,
            }]),
        };
        let token = MetadataAuthToken::internal(auth);
        let bytes = postcard::to_allocvec(&token).unwrap();

        assert_eq!(
            postcard::from_bytes::<MetadataAuthToken>(&bytes).unwrap(),
            token
        );
    }

    #[test]
    fn oversized_bearer_tokens_are_rejected_on_decode() {
        #[derive(Serialize)]
        enum RawAuthToken {
            Bearer(String),
        }

        #[derive(Serialize)]
        enum RawTransportMessage {
            QueryGraphs {
                auth_token: Option<RawAuthToken>,
                graph_iris: Option<Vec<String>>,
                sparql: String,
            },
        }

        let bytes = postcard::to_allocvec(&RawTransportMessage::QueryGraphs {
            auth_token: Some(RawAuthToken::Bearer(
                "x".repeat(MAX_METADATA_BEARER_TOKEN_LEN + 1),
            )),
            graph_iris: None,
            sparql: "ASK {}".to_string(),
        })
        .unwrap();

        assert!(postcard::from_bytes::<MetadataTransportMessage>(&bytes).is_err());
    }

    #[test]
    fn rejects_large_audit() {
        let message = MetadataTransportMessage::ForwardedAuditPage {
            result: Ok(AuditPageResponse {
                records: vec![AuditPageEntry {
                    key: vec![0u8; aruna_core::audit::AUDIT_KEY_BYTES],
                    record: MetadataAuditRecord {
                        realm_id: RealmId([1u8; 32]),
                        group_id: Ulid::from_bytes([2u8; 16]),
                        document_id: Ulid::from_bytes([3u8; 16]),
                        graph_iri: "x".repeat(MAX_AUDIT_PAGE_BYTES + AUDIT_FRAME_OVERHEAD),
                        user_id: UserId::local(Ulid::from_bytes([4u8; 16]), RealmId([1u8; 32])),
                        node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
                        operation: MetadataAuditOperation::Create,
                        occurred_at_ms: 0,
                        details: None,
                    },
                }],
                next_start_after: None,
            }),
        };

        assert!(encode_message(&message).is_err());
    }

    fn assert_has_auth_token_field(message: MetadataTransportMessage) {
        let value = serde_json::to_value(message).unwrap();
        let fields = value
            .as_object()
            .and_then(|variants| variants.values().next())
            .and_then(|variant| variant.as_object())
            .unwrap();

        assert!(fields.contains_key("auth_token"));
        assert!(!fields.contains_key("auth_context"));
    }
}
