//! Takes in external submissions and the requests other nodes send about them. Each entry point
//! gathers what only this node knows and drives the operation that decides.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::jobs::{InputResolveRequest, SubmissionRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::structs::execution::job::{
    CapturedInput, ExecutionSpec, InputSelection, SubmissionId, WorkspaceMode,
};
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use super::admit_forwarded::AdmitForwardedOperation;
use super::admit_here::{AcceptedSubmission, placement_failure};
use super::ids::{SubmissionRequest, SubmissionScope};
use super::serve_inputs::ServeInputsOperation;
use super::submit_external::{ExternalSubmission, IngressNode, SubmitExternalOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::submit::SubmitJobError;
use crate::metadata::protocol::MetadataTransportMessage;

/// This node as the submission operations see it; `None` without a network.
fn ingress_node(context: &DriverContext) -> Option<IngressNode> {
    let (local, realm_id) = context.node_and_realm()?;
    Some(IngressNode {
        local,
        realm_id,
        now_ms: unix_timestamp_millis(),
        fresh: context.fresh_heartbeats(),
    })
}

/// Resolves inputs this node holds for an ingress; see [`ServeInputsOperation`].
pub async fn serve_input_resolve(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    request: InputResolveRequest,
) -> Result<Vec<(InputSelection, CapturedInput)>, SubmissionRefusal> {
    let node = ingress_node(context).ok_or(SubmissionRefusal::Unavailable)?;
    let caller = node.input_caller(request.created_by);
    let serve = ServeInputsOperation::new(peer, auth_token, caller, request, node.now_ms);
    drive(serve, context.as_ref()).await
}

/// Normalizes, authorizes, and admits one external execution request.
#[allow(clippy::too_many_arguments)]
pub async fn submit_external_job(
    context: &DriverContext,
    spec: ExecutionSpec,
    created_by: UserId,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<String>,
    retention_ms: u64,
    auth_token: Option<AuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    let scope = match idempotency_key {
        Some(key) => SubmissionScope::Keyed(key),
        None => SubmissionScope::Unkeyed(Ulid::generate()),
    };
    let submission = ExternalSubmission {
        spec,
        created_by,
        scope,
        workspace_mode,
        workspace_bucket,
        retention_ms,
        auth_token,
    };
    let submit = SubmitExternalOperation::new(submission, ingress_node(context));
    drive(submit, context).await.map_err(placement_failure)
}

/// Serves one forwarded submission; see [`AdmitForwardedOperation`].
pub async fn serve_submission(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    submission_id: SubmissionId,
    request: SubmissionRequest,
) -> MetadataTransportMessage {
    let node = ingress_node(context);
    let admit = AdmitForwardedOperation::new(peer, auth_token, submission_id, request, node);
    let result = drive(admit, context.as_ref()).await;
    MetadataTransportMessage::ForwardedJobSubmission { result }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::lifecycle::ids::store_workspace;
    use crate::jobs::lifecycle::input_walk::RangeHolderOperation;
    use crate::jobs::lifecycle::submission_rules::{
        admitted_description, answers_input, batch_len, exact_version, pin_outputs,
        reference_content, reference_shape, refusal_of, resolve_answer, resolver_admits,
        sources_match,
    };
    use crate::tests::records::captured_input;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::execution::job::OutputDestination;
    use aruna_core::structs::execution::job::{
        InputContent, InputMode, InputSelection, InputSource, OutputSelection, WorkspaceOutput,
    };
    use aruna_core::structs::identity::auth::{AuthContext, Permission};
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
    use aruna_core::structs::storage::dataset_description::DatasetDescription;
    use aruna_core::structs::storage::dataset_description::DescriptionError;
    use aruna_core::structs::storage::replication::VersionedObjectArn;

    use crate::tests::records::{node, payload};

    #[test]
    fn unavailable_dataset_retries() {
        // Only a dataset that gives no description for good leaves the capture empty.
        let described = DatasetDescription {
            title: Some("Bakta".to_string()),
            ..DatasetDescription::default()
        };
        assert_eq!(admitted_description(Ok(described.clone())), Ok(described));
        for definite in [
            DescriptionError::Unreadable,
            DescriptionError::Invalid("not a Dataset".to_string()),
        ] {
            assert_eq!(admitted_description(Err(definite)), Ok(Default::default()));
        }
        assert!(admitted_description(Err(DescriptionError::Unavailable)).is_err());
    }

    // A plain s3:// input of a distributed bucket is captured at the member that coordinates
    // its key, not refused because this member holds no copy.
    #[tokio::test]
    async fn routes_range_holder() {
        use crate::distribution::test_record::{binding, record, secret, store_record};
        use crate::distribution::test_store::{context, open};
        let (_dir, handle) = open();
        let (snapshot, bytes) = record(true);
        store_record(&handle, snapshot.set_id(), bytes).await;
        let bucket = aruna_core::structs::storage::blob::BucketInfo {
            group_id: Ulid::from_bytes([90; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(RealmId::from_bytes([9; 32])),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: Some(binding(&snapshot, 1, 5)),
        };
        let write = StorageEffect::Write {
            key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
            key: b"refseq".to_vec().into(),
            value: bucket.to_bytes().unwrap().into(),
            txn_id: None,
        };
        handle.send_storage_effect(write).await;
        let context = context(&handle);
        let input = |key: &str| InputSelection {
            source: InputSource::S3 {
                bucket: "refseq".to_string(),
                key: key.to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: key.to_string(),
            mode: InputMode::Snapshot,
            container_path: Some(format!("/in/{key}")),
            name: None,
            description: None,
        };
        let local = secret(1).public();

        let holder = |key| drive(RangeHolderOperation::new(input(key), local), &context);
        let lower = holder("a").await.unwrap();
        let upper = holder("x").await.unwrap();

        assert_eq!(lower.source_node_id, None);
        assert_eq!(upper.source_node_id, Some(secret(2).public()));
    }

    fn s3_input(bucket: &str, key: &str, version: Option<&str>) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: version.map(str::to_string),
            },
            source_node_id: None,
            dest_key: "in.txt".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        }
    }

    #[test]
    fn pins_holder_outputs() {
        // The admitting holder owns every destination: a device-supplied node id
        // is overwritten and the workspace outputs resolve to the holder.
        let mut spec = payload();
        spec.file_outputs.push(OutputSelection {
            container_path: "/out/result.txt".to_string(),
            path_prefix: None,
            destination_node_id: Some(node(2)),
            destination: OutputDestination::S3 {
                bucket: "results".to_string(),
                key: "result.txt".to_string(),
            },
            name: None,
            description: None,
        });
        spec.workspace_outputs.push(WorkspaceOutput {
            container_path: "/out/extra.txt".to_string(),
            dest_key: "extra.txt".to_string(),
        });
        store_workspace(
            &mut spec,
            WorkspaceMode::Existing,
            Some("results".to_string()),
        )
        .expect("workspace stored");

        pin_outputs(&mut spec, node(1));

        assert!(spec.workspace_outputs.is_empty());
        assert_eq!(spec.file_outputs.len(), 2);
        for output in &spec.file_outputs {
            assert_eq!(output.destination_node_id, Some(node(1)));
        }
    }

    #[test]
    fn keeps_reference_shape() {
        // A device holds none of the objects it names, so a well-formed absent
        // reference passes while a malformed one is still refused here.
        let mut spec = payload();
        spec.inputs.push(s3_input("bucket", "key", None));
        assert!(reference_shape(&spec).is_ok());

        let mut versioned = payload();
        versioned.inputs.push(s3_input(
            "bucket",
            "key",
            Some(&Ulid::generate().to_string()),
        ));
        assert!(reference_shape(&versioned).is_ok());

        let mut empty_key = payload();
        empty_key.inputs.push(s3_input("bucket", "  ", None));
        assert!(matches!(
            reference_shape(&empty_key),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));

        let mut bad_version = payload();
        bad_version
            .inputs
            .push(s3_input("bucket", "key", Some("not-a-ulid")));
        assert!(matches!(
            reference_shape(&bad_version),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
    }

    fn reference_head(
        kind: aruna_core::structs::execution::source_connector::SourceConnectorKind,
        etag: Option<&str>,
        digested: bool,
    ) -> crate::s3::object::head::HeadObjectResult {
        use aruna_core::structs::checksum::ChecksumAlgorithm;
        use aruna_core::structs::execution::source_access::{
            DigestOrigin, DigestScope, SourceDigest, SourceMetadata,
        };
        use aruna_core::structs::execution::staging::{
            PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
        };
        let mut digests = std::collections::BTreeMap::new();
        if digested {
            digests.insert(
                ChecksumAlgorithm::Sha256,
                SourceDigest {
                    digest: vec![1; 32],
                    scope: DigestScope::FullObject,
                    origin: DigestOrigin::Manifest,
                },
            );
        }
        crate::s3::object::head::HeadObjectResult {
            location: None,
            metadata: Default::default(),
            source_metadata: Some(SourceMetadata {
                content_length: 9,
                content_type: None,
                etag: etag.map(str::to_string),
                last_modified: None,
                source_version: None,
                digests,
            }),
            last_refresh: None,
            version_created_at: None,
            version_id: None,
            resolved_version_id: None,
            checksum_type:
                aruna_core::structs::storage::multipart::MultipartChecksumType::FullObject,
            composite_hashes: Default::default(),
            part_count: None,
            source_policies: Vec::new(),
            source_binding: Some(VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind,
                    public_config: Default::default(),
                    source_path: "db/bakta.db".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                    namespace: None,
                    root: None,
                },
                connector_id: None,
                mirrors: Vec::new(),
            }),
        }
    }

    #[test]
    fn binds_reference_reads() {
        // A strong ETag on an S3 source binds every range; any other reference is staged whole,
        // bound to its recorded observation.
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        let bound = reference_content(
            "s3://db/a",
            &reference_head(SourceConnectorKind::S3, Some("\"e1\""), false),
        );
        let (content, pin) = bound.expect("range bound reference");
        assert_eq!(
            content,
            InputContent::Reference {
                etag: Some("\"e1\"".to_string()),
                range_bound: true
            }
        );
        assert_eq!(pin.expect("pin").path, "db/bakta.db");
        let weak = reference_head(SourceConnectorKind::S3, Some("W/\"e1\""), true);
        let (content, _) = reference_content("s3://db/a", &weak).expect("digest bound");
        assert!(matches!(
            content,
            InputContent::Reference {
                range_bound: false,
                ..
            }
        ));
        let http = reference_head(SourceConnectorKind::Http, Some("\"e1\""), false);
        let (content, _) = reference_content("s3://db/a", &http).expect("observation bound");
        assert!(matches!(
            content,
            InputContent::Reference {
                range_bound: false,
                ..
            }
        ));
        let mut stored = reference_head(SourceConnectorKind::S3, Some("\"e1\""), false);
        stored.source_binding = None;
        assert!(reference_content("s3://db/a", &stored).is_err());
    }

    #[test]
    fn pages_resolve_messages() {
        // Answers past one transport message split into pages that each encode within it, and
        // an answer that stops early is taken as a prefix, leaving the rest to ask again.
        let node_id = node(2);
        let long = "k".repeat(64 * 1024);
        let version = Ulid::from_parts(1, 1).to_string();
        let answers: Vec<(InputSelection, CapturedInput)> = (0..120u32)
            .map(|index| {
                let mut input = s3_input("db", &format!("{index}/{long}"), Some(&version));
                input.dest_key = format!("{index}/{long}");
                input.source_node_id = Some(node_id);
                let captured = captured_input(&input, node_id);
                (input, captured)
            })
            .collect();
        let inputs: Vec<InputSelection> = answers.iter().map(|(input, _)| input.clone()).collect();
        let encode = |answers: &[(InputSelection, CapturedInput)]| {
            let result = Ok(answers.to_vec());
            crate::metadata::protocol::encode_message(
                &MetadataTransportMessage::ResolvedJobInputs { result },
            )
        };
        assert!(encode(&answers).is_err());
        let mut done = 0;
        let mut pages = 0;
        while done < answers.len() {
            let page = &answers[done..][..batch_len(&answers[done..])];
            assert!(!page.is_empty() && encode(page).is_ok());
            let taken = resolve_answer(Ok(Ok(page.to_vec())), &inputs[done..], node_id).unwrap();
            assert_eq!(taken.len(), page.len());
            done += page.len();
            pages += 1;
        }
        assert!(pages > 1);
        assert!(resolve_answer(Ok(Ok(Vec::new())), &inputs, node_id).is_err());
    }

    #[test]
    fn answers_pins() {
        // A source node may change an input only behind a link that names the asked version,
        // and must pin what it answers to the version it captured.
        let node_id = node(2);
        let version = Ulid::from_parts(1, 1);
        let mut asked = s3_input("db", "bakta.db", Some(&version.to_string()));
        asked.source_node_id = Some(node_id);
        let plain = (asked.clone(), captured_input(&asked, node_id));
        assert!(answers_input(&asked, &plain, node_id));
        let target_version = Ulid::from_parts(2, 2);
        let mut target = s3_input("raw", "bakta-v6.db", Some(&target_version.to_string()));
        target.source_node_id = Some(node_id);
        let mut linked = captured_input(&asked, node_id);
        linked.version_id = target_version;
        assert!(!answers_input(
            &asked,
            &(target.clone(), linked.clone()),
            node_id
        ));
        linked.link = Some(VersionedObjectArn {
            realm_id: RealmId::from_bytes([1; 32]),
            node_id,
            bucket: "db".to_string(),
            key: "bakta.db".to_string(),
            version,
        });
        assert!(answers_input(
            &asked,
            &(target.clone(), linked.clone()),
            node_id
        ));
        let mut repinned = target.clone();
        repinned.source = s3_input(
            "raw",
            "bakta-v6.db",
            Some(&Ulid::from_parts(3, 3).to_string()),
        )
        .source;
        assert!(!answers_input(&asked, &(repinned, linked.clone()), node_id));
        let mut stale = linked;
        stale.link.as_mut().expect("link").version = Ulid::from_parts(9, 9);
        assert!(!answers_input(&asked, &(target, stale), node_id));

        // An input routed to its range coordinator without a version is answered pinned there,
        // never unpinned and never as another key.
        let mut unpinned = s3_input("db", "bakta.db", None);
        unpinned.source_node_id = Some(node_id);
        assert!(answers_input(&unpinned, &plain, node_id));
        let bare = (unpinned.clone(), captured_input(&asked, node_id));
        assert!(!answers_input(&unpinned, &bare, node_id));
        let mut other = asked.clone();
        other.source = s3_input("db", "other.db", Some(&version.to_string())).source;
        let moved = (other.clone(), captured_input(&other, node_id));
        assert!(!answers_input(&unpinned, &moved, node_id));
    }

    // A node named by the caller still needs an exact version; a range coordinator does not.
    #[test]
    fn node_needs_version() {
        assert!(exact_version(&s3_input("db", "a", None)).is_err());
        let version = Ulid::from_parts(1, 1).to_string();
        assert!(exact_version(&s3_input("db", "a", Some(&version))).is_ok());
    }

    #[test]
    fn refuses_restricted_resolver() {
        // A bearer narrowed to some paths never gets unrestricted reads or grants resolved.
        let realm_id = RealmId::from_bytes([1; 32]);
        let peer = node(2);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 1);
        config
            .nodes
            .push(aruna_core::structs::identity::realm::RealmNode {
                node_id: peer.to_string(),
                kind: aruna_core::structs::identity::realm::RealmNodeKind::Server,
            });
        let user = UserId::local(Ulid::from_parts(3, 3), realm_id);
        let open = AuthContext {
            user_id: user,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        assert_eq!(resolver_admits(&open, user, &config, peer), Ok(()));
        let restricted = AuthContext {
            path_restrictions: Some(vec![aruna_core::structs::identity::auth::PathRestriction {
                pattern: "/x/**".to_string(),
                permission: Permission::READ,
            }]),
            ..open.clone()
        };
        assert_eq!(
            resolver_admits(&restricted, user, &config, peer),
            Err(SubmissionRefusal::Unauthorized)
        );
        let other = UserId::local(Ulid::from_parts(4, 4), realm_id);
        assert!(resolver_admits(&open, other, &config, peer).is_err());
        assert!(resolver_admits(&open, user, &config, node(9)).is_err());
    }

    #[test]
    fn checks_capture_sources() {
        // A capture must come from the node its input names, or from ingress without one.
        let (ingress, other) = (node(1), node(2));
        let unnamed = s3_input("a", "x", None);
        let mut named = s3_input("b", "y", Some(&Ulid::from_parts(1, 1).to_string()));
        named.dest_key = "named".to_string();
        named.source_node_id = Some(other);
        let inputs = vec![unnamed.clone(), named.clone()];
        let honest = vec![
            captured_input(&unnamed, ingress),
            captured_input(&named, other),
        ];
        assert!(sources_match(&inputs, &honest, ingress));
        let forged = vec![
            captured_input(&unnamed, ingress),
            captured_input(&named, ingress),
        ];
        assert!(!sources_match(&inputs, &forged, ingress));
        let moved = vec![
            captured_input(&unnamed, other),
            captured_input(&named, other),
        ];
        assert!(!sources_match(&inputs, &moved, ingress));
    }

    #[test]
    fn maps_holder_refusal() {
        // A holder that could not read is retried at the next holder; one that
        // read the request and refused it ends the ingress definitively.
        assert_eq!(
            refusal_of(SubmitJobError::PlacementUnavailable("offline".to_string())),
            SubmissionRefusal::Unavailable
        );
        assert_eq!(
            refusal_of(SubmitJobError::InvalidWorkspace("absent".to_string())),
            SubmissionRefusal::Invalid {
                reason: "invalid workspace: absent".to_string()
            }
        );
    }
}
