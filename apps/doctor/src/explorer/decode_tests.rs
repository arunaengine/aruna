//! Tests decoding raw key and value bytes per keyspace, including the raw hex fallback.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::present::{DecodedField, DecodedValue, JsonCacheEntry, JsonPlacementDocument};
use super::super::present::{JsonGraphKey, JsonLogKey, JsonQuadKey, JsonStoredOp};
use super::{
    BATCH_ENCODING_TAG, CraqleQuadOp, CraqleStoredBatch, CraqleStoredMeta, CraqleTermId,
    DOT_ENCODING_TAG, GRAPH_META_PREFIX, LOG_BATCH_PREFIX, decode_entry, raw_field,
};
use aruna_core::compute::quota::{ComputeDepartureReport, JobReservationRecord};
use aruna_core::dht_protocol::StoredEntry;
use aruna_core::id::DhtKeyId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    COMPUTE_DEPARTURE_KEYSPACE, CRAQLE_GRAPHS_KEYSPACE, CRAQLE_LOG_KEYSPACE, CRAQLE_QUADS_KEYSPACE,
    CRAQLE_TERMS_KEYSPACE, DHT_KEYSPACE, FAMILY_ALIAS_KEYSPACE, FAMILY_CONFLICT_KEYSPACE,
    FAMILY_OUTBOX_KEYSPACE, FAMILY_PENDING_KEYSPACE, FAMILY_PROJECTION_KEYSPACE,
    FAMILY_RECORD_KEYSPACE, JOB_RESERVATION_KEYSPACE, NODE_STATE_KEYSPACE,
    OBJECT_METADATA_KEYSPACE, ONBOARDING_KEYSPACE, OUTPUT_RECORD_KEYSPACE, PATHS_INDEX_KEYSPACE,
    PLACEMENT_POLICY_KEYSPACE, PLAN_EXPLAIN_KEYSPACE, POLICY_CACHE_KEYSPACE, REALM_CONFIG_KEYSPACE,
    S3_BUCKET_KEYSPACE, SYNC_PLACEMENT_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE,
    WITNESS_DEADLINE_KEYSPACE,
};
use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
use aruna_core::structs::execution::job::family::{JobFamilyId, JobRecordEnvelope};
use aruna_core::structs::execution::job::record_rows::{
    ConflictRecord, OutboxEntry, PROJECTION_CACHE_VERSION, PendingNeed, PendingRecord,
    ProjectionCache,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::node_state::{
    BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::attachment::{
    BULK_INTENT_KEYSPACE, BULK_RUN_KEYSPACE, POLICY_MUTATION_KEYSPACE, PolicyBulkRun, PolicyIntent,
    PolicyIntentOutcome, PolicyMutationParams, PolicyMutationRecord, PolicyRefMode, PolicyStatus,
};
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, PolicyPublication, placement_policy_key,
};
use aruna_core::structs::placement::policy::{PlacementPolicy, PlacementPolicyRef};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CurrentVersionPointer, HashIndex,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::format::StoredFormat;
use aruna_core::structs::storage::multipart::{
    MultipartChecksumType, MultipartObjectKey, MultipartObjectPart, MultipartObjectSummary,
    MultipartPart, MultipartPartKey, MultipartUpload, MultipartUploadStatus,
};
use aruna_ops_identity::placement::policy::cache::PolicyCacheEntry;
use aruna_ops_work::jobs::lifecycle::witness::arm::{WitnessDeadline, WitnessExplain};
use chrono::{DateTime, Utc};
use craqle::{
    ActorId as CraqleActorId, Dot as CraqleDot, GraphPolicy as CraqleGraphPolicy,
    VectorClock as CraqleVectorClock,
};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::SystemTime;
use ulid::Ulid;

#[test]
fn decodes_bucket_record() {
    let realm_id = RealmId::from_bytes([4_u8; 32]);
    let info = BucketInfo {
        group_id: Ulid::from_bytes([7_u8; 16]),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: aruna_core::user::id::UserId::local(Ulid::from_bytes([8_u8; 16]), realm_id),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 7,
        compression: Compression::Off,
        distribution: None,
    };

    let decoded = decode_entry(
        S3_BUCKET_KEYSPACE,
        b"primary-bucket",
        &info.to_bytes().unwrap(),
    );
    assert_eq!(
        decoded.key,
        DecodedField::Utf8 {
            value: "primary-bucket".to_string()
        }
    );
    match decoded.value {
        DecodedValue::BucketInfo { data } => assert_eq!(data, info),
        other => panic!("expected bucket info, got {other:?}"),
    }
}

fn test_family() -> JobFamilyId {
    JobFamilyId {
        submission_id: aruna_core::structs::execution::job::family::SubmissionId([9u8; 32]),
        request_digest: [8u8; 32],
    }
}

fn test_node() -> aruna_core::id::NodeId {
    iroh::SecretKey::from_bytes(&[6u8; 32]).public()
}

fn test_envelope() -> JobRecordEnvelope {
    let family = test_family();
    JobRecordEnvelope::sign(
        RealmId::from_bytes([2u8; 32]),
        aruna_core::structs::execution::job::family::JobFamilyRecord::Claim(
            aruna_core::structs::execution::job::family::SubmissionClaim {
                submission_id: family.submission_id,
                job_id: aruna_core::structs::execution::job::identity::JobId::from_bytes(
                    Ulid::from_bytes([5u8; 16]).to_bytes(),
                ),
                request_digest: family.request_digest,
                spec_digest: [7u8; 32],
                committing_node_id: test_node(),
                accepted_at_ms: 11,
            },
        ),
        &iroh::SecretKey::from_bytes(&[6u8; 32]),
    )
    .unwrap()
}

#[test]
fn decodes_family_rows() {
    // Every append-only family keyspace must decode: an operator inspecting
    // a stuck submission cannot be told the rows are opaque bytes.
    let envelope = test_envelope();
    let key = envelope.key();
    let key_bytes = key.to_bytes();

    let record = decode_entry(
        FAMILY_RECORD_KEYSPACE,
        &key_bytes,
        &postcard::to_allocvec(&envelope).unwrap(),
    );
    assert!(matches!(
        record.key,
        DecodedField::JobRecordKey { .. } // the signed identity, not raw bytes
    ));
    assert!(matches!(record.value, DecodedValue::JobFamilyRecord { .. }));

    let pending = PendingRecord {
        envelope: envelope.clone(),
        need: PendingNeed::Evidence(
            aruna_core::structs::execution::job::family::JobRecordKind::Spec,
        ),
        first_seen_ms: 12,
        attempts: 2,
    };
    let decoded = decode_entry(
        FAMILY_PENDING_KEYSPACE,
        &key_bytes,
        &postcard::to_allocvec(&pending).unwrap(),
    );
    match decoded.value {
        DecodedValue::JobPendingRecord { need, attempts, .. } => {
            assert_eq!(need, "evidence:Spec");
            assert_eq!(attempts, 2);
        }
        other => panic!("expected a pending record, got {other:?}"),
    }

    let conflict = ConflictRecord {
        envelope: envelope.clone(),
        retained: [3u8; 32],
        observed_at_ms: 13,
        relayed_by: Some(test_node()),
    };
    let mut conflict_key = key_bytes.to_vec();
    conflict_key.extend_from_slice(&[3u8; 32]);
    let decoded = decode_entry(
        FAMILY_CONFLICT_KEYSPACE,
        &conflict_key,
        &postcard::to_allocvec(&conflict).unwrap(),
    );
    assert!(matches!(decoded.key, DecodedField::JobConflictKey { .. }));
    assert!(matches!(
        decoded.value,
        DecodedValue::JobConflictRecord { .. }
    ));

    let mut alias_key = Ulid::from_bytes([5u8; 16]).to_bytes().to_vec();
    alias_key.extend_from_slice(&test_family().to_bytes());
    let decoded = decode_entry(FAMILY_ALIAS_KEYSPACE, &alias_key, &key_bytes);
    assert!(matches!(decoded.key, DecodedField::JobAliasKey { .. }));
    assert!(matches!(decoded.value, DecodedValue::JobAliasTarget { .. }));

    let cache = ProjectionCache {
        version: PROJECTION_CACHE_VERSION,
        revision: 4,
        stale: true,
        projection: None,
    };
    let decoded = decode_entry(
        FAMILY_PROJECTION_KEYSPACE,
        &test_family().to_bytes(),
        &postcard::to_allocvec(&cache).unwrap(),
    );
    assert!(matches!(decoded.key, DecodedField::JobFamilyKey { .. }));
    assert_eq!(
        decoded.value,
        DecodedValue::JobProjectionCache {
            revision: 4,
            stale: true,
            projected: false,
        }
    );

    let outbox = OutboxEntry {
        queued_at_ms: 14,
        delivered: Vec::new(),
        next_holder: 0,
        rejections: 0,
    };
    let decoded = decode_entry(
        FAMILY_OUTBOX_KEYSPACE,
        &key_bytes,
        &postcard::to_allocvec(&outbox).unwrap(),
    );
    assert_eq!(decoded.value, DecodedValue::JobOutboxEntry { data: outbox });
}

#[test]
fn decodes_compute_rows() {
    // Reservations, witness deadlines, plan explains, and the departure row
    // are the operator's only local evidence about scheduled work.
    let execution_id = Ulid::from_bytes([1u8; 16]);
    let reservation = JobReservationRecord {
        execution_id,
        job_id: aruna_core::structs::execution::job::identity::JobId::from_bytes(
            Ulid::from_bytes([5u8; 16]).to_bytes(),
        ),
        logical_job_id: aruna_core::structs::execution::job::identity::JobId::from_bytes([5u8; 16]),
        resources: aruna_core::structs::execution::job::family::EffectiveResources {
            cpu_cores: 2,
            ram_bytes: 1024,
            disk_bytes: 2048,
            max_walltime_ms: 60_000,
            preemptible: false,
        },
        created_at_ms: 21,
        subject_generation: 3,
        subject_digest: [9u8; 32],
        config_clock: Default::default(),
        backend: "default".to_string(),
        config_generation: 0,
        config_digest: [0u8; 32],
    };
    let decoded = decode_entry(
        JOB_RESERVATION_KEYSPACE,
        &execution_id.to_bytes(),
        &postcard::to_allocvec(&reservation).unwrap(),
    );
    assert_eq!(
        decoded.key,
        DecodedField::Ulid {
            value: execution_id.to_string()
        }
    );
    assert!(matches!(decoded.value, DecodedValue::JobReservation { .. }));

    let deadline = WitnessDeadline {
        due_at_ms: 22,
        rank: 1,
    };
    let mut deadline_key = 22u64.to_be_bytes().to_vec();
    deadline_key.extend_from_slice(&test_family().to_bytes());
    let decoded = decode_entry(
        WITNESS_DEADLINE_KEYSPACE,
        &deadline_key,
        &postcard::to_allocvec(&deadline).unwrap(),
    );
    assert!(matches!(decoded.key, DecodedField::JobFamilyKey { .. }));
    assert_eq!(
        decoded.value,
        DecodedValue::JobWitnessDeadline { data: deadline }
    );

    let explain = WitnessExplain {
        sequence: 3,
        plan: aruna_core::scheduling::ExecutionPlan {
            selected: None,
            retryable: true,
            alternatives: Vec::new(),
            rejected: Vec::new(),
            omitted: 0,
        },
        declined: Vec::new(),
        deferred: Vec::new(),
        answer: None,
        overlapping: false,
        stored_at_ms: 23,
    };
    let mut explain_key = test_family().to_bytes().to_vec();
    explain_key.extend_from_slice(test_node().as_bytes());
    let decoded = decode_entry(
        PLAN_EXPLAIN_KEYSPACE,
        &explain_key,
        &postcard::to_allocvec(&explain).unwrap(),
    );
    assert!(matches!(decoded.key, DecodedField::JobExplainKey { .. }));
    assert_eq!(
        decoded.value,
        DecodedValue::JobPlanExplain {
            sequence: 3,
            selected: None,
            alternatives: 0,
            rejected: 0,
            overlapping: false,
            stored_at_ms: 23,
        }
    );

    let report = ComputeDepartureReport {
        departed_at_ms: 24,
        membership_generation: 5,
        unresolved: vec![execution_id],
        truncated: false,
    };
    let decoded = decode_entry(
        COMPUTE_DEPARTURE_KEYSPACE,
        b"departure",
        &postcard::to_allocvec(&report).unwrap(),
    );
    assert_eq!(
        decoded.key,
        DecodedField::Utf8 {
            value: "departure".to_string()
        }
    );
    assert_eq!(
        decoded.value,
        DecodedValue::ComputeDepartureReport { data: report }
    );
}

#[test]
fn decodes_output_key() {
    // Output records share the attempt-control key, so the epoch has to
    // survive decoding for a stuck attempt to be found at all.
    let job_id = Ulid::from_bytes([3_u8; 16]);
    let mut key = job_id.to_bytes().to_vec();
    key.extend_from_slice(&7_u64.to_be_bytes());

    let decoded = decode_entry(OUTPUT_RECORD_KEYSPACE, &key, b"not-an-envelope");

    assert_eq!(
        decoded.key,
        DecodedField::AttemptKey {
            job_id: job_id.to_string(),
            attempt_epoch: 7,
        }
    );
    assert!(matches!(
        decoded.value,
        DecodedValue::Raw {
            decode_error: Some(_),
            ..
        }
    ));
}

fn policy_document(policy_id: Ulid, realm_id: RealmId) -> PlacementPolicyDocument {
    let secret = iroh::SecretKey::from_bytes(&[11_u8; 32]);
    PlacementPolicyDocument {
        realm_id,
        policy: PlacementPolicy {
            policy_id,
            name: "eu-only".to_string(),
            owner_group_id: None,
            allowed: Vec::new(),
        },
        publication: PolicyPublication {
            publisher: secret.public(),
            created_by: aruna_core::user::id::UserId::local(
                Ulid::from_bytes([15_u8; 16]),
                realm_id,
            ),
            created_at_ms: 42,
            event_id: Ulid::from_bytes([12_u8; 16]),
            config_digest: [13_u8; 32],
            signature: secret.sign(b"publication"),
        },
    }
}

#[test]
fn decodes_policy_document() {
    let realm_id = RealmId::from_bytes([10_u8; 32]);
    let policy_id = Ulid::from_bytes([14_u8; 16]);
    let document = policy_document(policy_id, realm_id);

    let decoded = decode_entry(
        PLACEMENT_POLICY_KEYSPACE,
        &placement_policy_key(policy_id),
        &document.to_bytes().unwrap(),
    );

    assert_eq!(
        decoded.key,
        DecodedField::Ulid {
            value: policy_id.to_string()
        }
    );
    assert_eq!(
        decoded.value,
        DecodedValue::PlacementPolicyDocument {
            data: JsonPlacementDocument(document)
        }
    );
}

#[test]
fn decodes_policy_cache() {
    let policy_id = Ulid::from_bytes([15_u8; 16]);
    let policy_ref = PlacementPolicyRef {
        policy_id,
        digest: [16_u8; 32],
    };
    let entry = PolicyCacheEntry::Unavailable {
        stored_at_ms: 5,
        expires_at_ms: 15,
    };
    let mut key = policy_id.to_bytes().to_vec();
    key.extend_from_slice(&policy_ref.digest);

    let decoded = decode_entry(POLICY_CACHE_KEYSPACE, &key, &entry.to_bytes().unwrap());

    assert_eq!(
        decoded.key,
        DecodedField::PolicyCacheKey {
            policy_id: policy_id.to_string(),
            digest: hex::encode([16_u8; 32]),
        }
    );
    assert_eq!(
        decoded.value,
        DecodedValue::PolicyCacheEntry {
            data: JsonCacheEntry(entry)
        }
    );
}

#[test]
fn decodes_policy_mutation() {
    let realm_id = RealmId::from_bytes([17_u8; 32]);
    let mutation_id = Ulid::from_bytes([18_u8; 16]);
    let record = PolicyMutationRecord {
        mutation_id,
        params: PolicyMutationParams {
            bucket: "bucket".to_string(),
            key: "a.tar".to_string(),
            expected_head: CurrentVersionPointer::new(Ulid::from_bytes([19_u8; 16])),
            bucket_identity: (
                Ulid::from_bytes([20_u8; 16]),
                std::time::SystemTime::UNIX_EPOCH,
                aruna_core::user::id::UserId::local(Ulid::from_bytes([22_u8; 16]), realm_id),
            ),
            target_refs: Vec::new(),
            mode: PolicyRefMode::Union,
        },
        successor_version_id: Ulid::from_bytes([21_u8; 16]),
        effective_refs: Vec::new(),
        materialized: true,
    };

    let decoded = decode_entry(
        POLICY_MUTATION_KEYSPACE,
        &PolicyMutationRecord::key(mutation_id).unwrap(),
        &record.to_bytes().unwrap(),
    );

    assert_eq!(
        decoded.key,
        DecodedField::Ulid {
            value: mutation_id.to_string()
        }
    );
    assert_eq!(
        decoded.value,
        DecodedValue::PolicyMutationRecord { data: record }
    );
}

#[test]
fn decodes_bulk_run() {
    let realm_id = RealmId::from_bytes([22_u8; 32]);
    let operation_id = Ulid::from_bytes([23_u8; 16]);
    let run = PolicyBulkRun {
        operation_id,
        bucket: "bucket".to_string(),
        bucket_identity: (
            Ulid::from_bytes([24_u8; 16]),
            std::time::SystemTime::UNIX_EPOCH,
            aruna_core::user::id::UserId::local(Ulid::from_bytes([25_u8; 16]), realm_id),
        ),
        generation: 3,
        target_refs: Vec::new(),
        status: PolicyStatus::Active,
    };

    let decoded = decode_entry(
        BULK_RUN_KEYSPACE,
        &PolicyBulkRun::key(operation_id).unwrap(),
        &run.to_bytes().unwrap(),
    );

    assert_eq!(
        decoded.key,
        DecodedField::Ulid {
            value: operation_id.to_string()
        }
    );
    assert_eq!(decoded.value, DecodedValue::PolicyBulkRun { data: run });
}

#[test]
fn decodes_bulk_intent() {
    let operation_id = Ulid::from_bytes([25_u8; 16]);
    let intent = PolicyIntent {
        operation_id,
        key: "a.tar".to_string(),
        observed_head: CurrentVersionPointer::new(Ulid::from_bytes([26_u8; 16])),
        successor_version_id: Ulid::from_bytes([27_u8; 16]),
        outcome: PolicyIntentOutcome::Planned,
    };

    let decoded = decode_entry(
        BULK_INTENT_KEYSPACE,
        &intent.key().to_bytes().unwrap(),
        &intent.to_bytes().unwrap(),
    );

    assert_eq!(
        decoded.key,
        DecodedField::PolicyIntentKey {
            operation_id: operation_id.to_string(),
            key: "a.tar".to_string(),
        }
    );
    assert_eq!(decoded.value, DecodedValue::PolicyIntent { data: intent });
}
#[test]
fn unknown_keyspace_raw() {
    let entry = decode_entry("unknown", b"\x01\x02", b"\x03\x04");
    assert_eq!(entry.key, raw_field(b"\x01\x02"));
    match entry.value {
        DecodedValue::Raw { hex, .. } => assert_eq!(hex, "0304"),
        other => panic!("expected raw fallback, got {other:?}"),
    }
}

#[test]
fn decodes_realm_key() {
    let realm_id = RealmId::from_bytes([5_u8; 32]);
    let entry = decode_entry(REALM_CONFIG_KEYSPACE, realm_id.as_bytes(), b"not-a-config");
    assert_eq!(
        entry.key,
        DecodedField::RealmId {
            value: realm_id.to_string()
        }
    );
}

#[test]
fn decodes_realm_config() {
    let realm_id = RealmId::from_bytes([1_u8; 32]);
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[3_u8; 32]).public(),
        user_id: aruna_core::user::id::UserId::local(Ulid::from_bytes([4_u8; 16]), realm_id),
        realm_id,
    };
    let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    realm_config.description = "Explorer Realm".to_string();

    let decoded = decode_entry(
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes(),
        &realm_config.to_bytes(&actor).unwrap(),
    );
    match decoded.value {
        DecodedValue::RealmConfigDocument { data } => {
            assert_eq!(data.0.description, "Explorer Realm")
        }
        other => panic!("expected realm config, got {other:?}"),
    }

    let fallback = decode_entry(REALM_CONFIG_KEYSPACE, realm_id.as_bytes(), b"broken");
    match fallback.value {
        DecodedValue::Raw {
            decode_error: Some(_),
            ..
        } => {}
        other => panic!("expected raw decode fallback, got {other:?}"),
    }
}

#[test]
fn decodes_onboarding_secret() {
    let record = OnboardingSecretRecord {
        enrollment_id: Ulid::from_bytes([6_u8; 16]),
        secret_hash: "hash123".to_string(),
        mode: OnboardingMode::Server,
        purpose: OnboardingPurpose::NodeEnrollment,
        expires_at: 1234,
        claimed_node_id: None,
    };
    let value = postcard::to_allocvec(&record).unwrap();

    let decoded = decode_entry(ONBOARDING_KEYSPACE, b"secret:test", &value);
    assert_eq!(
        decoded.key,
        DecodedField::Utf8 {
            value: "secret:test".to_string()
        }
    );
    match decoded.value {
        DecodedValue::OnboardingSecretRecord { data } => assert_eq!(data, record),
        other => panic!("expected onboarding secret record, got {other:?}"),
    }
}

#[test]
fn decodes_node_state() {
    let realm_id = RealmId::from_bytes([4_u8; 32]);
    let state = PersistedNodeState {
        boot_origin: BootOrigin::Onboarded,
        status: PersistedNodeStatus::PendingOnboarding,
        realm_id,
        net_secret_key: [11_u8; 32],
        onboarding_phase: None,
        onboarding_sync_ticket: Some("ticket".to_string()),
        identity: PersistedNodeIdentity::User {
            owner: aruna_core::user::id::UserId::nil(realm_id),
        },
    };
    let value = postcard::to_allocvec(&state).unwrap();

    let decoded = decode_entry(NODE_STATE_KEYSPACE, b"node_state", &value);
    assert_eq!(
        decoded.key,
        DecodedField::Utf8 {
            value: "node_state".to_string()
        }
    );
    match decoded.value {
        DecodedValue::NodeState { data } => assert_eq!(data.0, state),
        other => panic!("expected node state, got {other:?}"),
    }
}

#[test]
fn decodes_pending_placement() {
    let realm_id = RealmId::from_bytes([4_u8; 32]);
    let placement_ref = aruna_core::structs::placement::record::PlacementRef {
        strategy_id: ulid::Ulid::from_bytes([9_u8; 16]),
        shard: 5,
    };
    let selected_peer = iroh::SecretKey::from_bytes(&[7_u8; 32]).public();
    let authoritative_node_id = iroh::SecretKey::from_bytes(&[6_u8; 32]).public();
    let placement = aruna_ops_identity::sync::shard_placement::new_placement(
        realm_id,
        placement_ref,
        authoritative_node_id,
        vec![selected_peer],
        1,
    );
    let value = postcard::to_allocvec(&placement).unwrap();
    let key = aruna_ops_identity::sync::shard_placement::placement_key(realm_id, &placement_ref);

    let decoded = decode_entry(SYNC_PLACEMENT_KEYSPACE, key.as_ref(), &value);
    assert_eq!(
        decoded.key,
        DecodedField::Raw {
            hex: hex::encode(key.as_ref())
        }
    );
    match decoded.value {
        DecodedValue::PendingDocumentPlacement { data } => {
            assert_eq!(data.0.realm_id, realm_id);
            assert_eq!(data.0.placement, placement_ref);
            assert_eq!(data.0.authoritative_node_id, authoritative_node_id);
            assert_eq!(data.0.selected_peers, vec![selected_peer]);
        }
        other => panic!("expected pending topic placement, got {other:?}"),
    }
}

#[test]
fn decodes_dht_entry() {
    let key = DhtKeyId::from_bytes([6_u8; 32]);
    let realm_id = RealmId::from_bytes([7_u8; 32]);
    let publisher_secret = iroh::SecretKey::from_bytes(&[5_u8; 32]);
    let publisher = publisher_secret.public();
    let revision = 1;
    let signed = aruna_core::dht_protocol::signed_record_bytes(
        &key,
        &publisher,
        &realm_id,
        &[1, 2, 3, 4],
        42,
        revision,
    );
    let entries = vec![StoredEntry {
        publisher,
        realm_id,
        value: vec![1, 2, 3, 4],
        expires_at: 42,
        revision,
        signature: publisher_secret.sign(&signed),
        retain_until: 42,
    }];
    let value = postcard::to_allocvec(&entries).unwrap();

    let decoded = decode_entry(DHT_KEYSPACE, key.as_bytes(), &value);
    assert_eq!(
        decoded.key,
        DecodedField::DhtKeyId {
            value: key.to_string()
        }
    );
    match decoded.value {
        DecodedValue::DhtEntries { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0].0.publisher, publisher);
            assert_eq!(data[0].0.realm_id, realm_id);
            assert_eq!(data[0].0.value, vec![1, 2, 3, 4]);
        }
        other => panic!("expected dht entries, got {other:?}"),
    }
}

#[test]
fn decodes_upload_entry() {
    let realm_id = RealmId::from_bytes([3_u8; 32]);
    let created_by = aruna_core::user::id::UserId::local(Ulid::from_bytes([10_u8; 16]), realm_id);
    let upload = MultipartUpload {
        backend: BackendRef::node_default(),
        storage_class: None,
        upload_id: Ulid::from_bytes([7_u8; 16]),
        bucket: "bucket-a".to_string(),
        key: "parts/big.bin".to_string(),
        group_id: Ulid::from_bytes([8_u8; 16]),
        created_by,
        created_at: SystemTime::UNIX_EPOCH,
        status: MultipartUploadStatus::Open,
        checksum_hint: None,
        metadata: HashMap::new(),
        placement_policies: Vec::new(),
        subject_generation: 0,
        completing_since_ms: None,
        backend_upload: None,
        encryption: None,
    };

    let decoded = decode_entry(
        UPLOAD_KEYSPACE,
        &upload.upload_id.to_bytes(),
        &upload.to_bytes().unwrap(),
    );
    assert_eq!(
        decoded.key,
        DecodedField::Ulid {
            value: upload.upload_id.to_string()
        }
    );
    match decoded.value {
        DecodedValue::MultipartUpload { data } => assert_eq!(data, upload),
        other => panic!("expected multipart upload, got {other:?}"),
    }
}

#[test]
fn decodes_upload_part() {
    let realm_id = RealmId::from_bytes([9_u8; 32]);
    let created_by = aruna_core::user::id::UserId::local(Ulid::from_bytes([11_u8; 16]), realm_id);
    let key = MultipartPartKey::new(Ulid::from_bytes([2_u8; 16]), 5);
    let part = MultipartPart {
        part_number: 5,
        location: BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "blob-bucket".to_string(),
            backend_path: "multipart/part-5.bin".to_string(),
            ulid: Ulid::from_bytes([4_u8; 16]),
            format: StoredFormat::default(),
            created_by,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: true,
            blob_size: 42,
            hashes: HashMap::from([("md5".to_string(), vec![1_u8; 16])]),
        },
        created_at: SystemTime::UNIX_EPOCH,
        backend_etag: None,
        piece: None,
    };

    let decoded = decode_entry(
        UPLOAD_PART_KEYSPACE,
        &key.to_bytes().unwrap(),
        &part.to_bytes().unwrap(),
    );
    assert_eq!(
        decoded.key,
        DecodedField::MultipartPartKey { value: key.clone() }
    );
    match decoded.value {
        DecodedValue::MultipartPart { data } => assert_eq!(data, part),
        other => panic!("expected multipart upload part, got {other:?}"),
    }
}

#[test]
fn decodes_object_summary() {
    let key = MultipartObjectKey::summary(Ulid::from_bytes([1_u8; 16]));
    let summary = MultipartObjectSummary {
        checksum_type: MultipartChecksumType::Composite,
        part_count: 3,
        composite_hashes: Default::default(),
    };

    let decoded = decode_entry(
        OBJECT_METADATA_KEYSPACE,
        &key.to_bytes().unwrap(),
        &summary.to_bytes().unwrap(),
    );
    assert_eq!(
        decoded.key,
        DecodedField::MultipartObjectKey { value: key.clone() }
    );
    match decoded.value {
        DecodedValue::MultipartObjectSummary { data } => assert_eq!(data, summary),
        other => panic!("expected multipart object summary, got {other:?}"),
    }
}

#[test]
fn decodes_object_part() {
    let key = MultipartObjectKey::part(Ulid::from_bytes([5_u8; 16]), 2);
    let part = MultipartObjectPart {
        part_number: 2,
        size: 64,
        hashes: HashMap::from([
            ("blake3".to_string(), vec![7_u8; 32]),
            ("md5".to_string(), vec![8_u8; 16]),
        ]),
    };

    let decoded = decode_entry(
        OBJECT_METADATA_KEYSPACE,
        &key.to_bytes().unwrap(),
        &part.to_bytes().unwrap(),
    );
    assert_eq!(
        decoded.key,
        DecodedField::MultipartObjectKey { value: key.clone() }
    );
    match decoded.value {
        DecodedValue::MultipartObjectPart { data } => assert_eq!(data, part),
        other => panic!("expected multipart object part, got {other:?}"),
    }
}

#[test]
fn invalid_metadata_raw() {
    let key = MultipartObjectKey::summary(Ulid::from_bytes([3_u8; 16]));
    let decoded = decode_entry(
        OBJECT_METADATA_KEYSPACE,
        &key.to_bytes().unwrap(),
        b"broken",
    );
    match decoded.value {
        DecodedValue::Raw {
            decode_error: Some(_),
            ..
        } => {}
        other => panic!("expected raw decode fallback, got {other:?}"),
    }
}

#[test]
fn decodes_term_entry() {
    let term_id = 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10_u128;
    let decoded = decode_entry(
        CRAQLE_TERMS_KEYSPACE,
        &term_id.to_be_bytes(),
        b"<https://example.org/dataset>",
    );

    assert_eq!(
        decoded.key,
        DecodedField::CraqleTermId {
            value: format!("{term_id:032x}")
        }
    );
    match decoded.value {
        DecodedValue::CraqleTerm { data } => {
            assert_eq!(data, "<https://example.org/dataset>")
        }
        other => panic!("expected craqle term, got {other:?}"),
    }
}

#[test]
fn decodes_quad_entry() {
    let graph = 1_u128;
    let subject = 2_u128;
    let predicate = 3_u128;
    let object = 4_u128;
    let actor = CraqleActorId::from_bytes([8_u8; 32]);

    let mut key = Vec::new();
    key.extend_from_slice(&graph.to_be_bytes());
    key.extend_from_slice(&subject.to_be_bytes());
    key.extend_from_slice(&predicate.to_be_bytes());
    key.extend_from_slice(&object.to_be_bytes());

    let mut value = vec![DOT_ENCODING_TAG];
    value.extend_from_slice(actor.as_bytes());
    value.extend_from_slice(&7_u64.to_be_bytes());

    let decoded = decode_entry(CRAQLE_QUADS_KEYSPACE, &key, &value);
    assert_eq!(
        decoded.key,
        DecodedField::CraqleQuadKey {
            value: JsonQuadKey {
                graph: format!("{graph:032x}"),
                subject: format!("{subject:032x}"),
                predicate: format!("{predicate:032x}"),
                object: format!("{object:032x}"),
            }
        }
    );
    match decoded.value {
        DecodedValue::CraqleQuadDots { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0].actor, actor.to_string());
            assert_eq!(data[0].counter, 7);
        }
        other => panic!("expected craqle quad dots, got {other:?}"),
    }
}

#[test]
fn decodes_graph_meta() {
    let graph = 9_u128;
    let actor = CraqleActorId::from_bytes([5_u8; 32]);
    let mut key = vec![GRAPH_META_PREFIX];
    key.extend_from_slice(&graph.to_be_bytes());

    let value = postcard::to_allocvec(&CraqleStoredMeta {
        policy: CraqleGraphPolicy {
            public: true,
            permission_paths: vec!["/b".to_string(), "/a".to_string(), "/a".to_string()],
        },
        clock: CraqleVectorClock(BTreeMap::from([(actor, 11_u64)])),
    })
    .unwrap();

    let decoded = decode_entry(CRAQLE_GRAPHS_KEYSPACE, &key, &value);
    match decoded.key {
        DecodedField::CraqleGraphKey { value } => assert_eq!(
            value,
            JsonGraphKey::Meta {
                graph: format!("{graph:032x}")
            }
        ),
        other => panic!("expected craqle graph key, got {other:?}"),
    }
    match decoded.value {
        DecodedValue::CraqleGraphMeta { data } => {
            assert_eq!(data.graph, format!("{graph:032x}"));
            assert!(data.policy.public);
            assert_eq!(data.policy.permission_paths, vec!["/a", "/b"]);
            assert_eq!(data.clock.entries.len(), 1);
            assert_eq!(data.clock.entries[0].actor, actor.to_string());
            assert_eq!(data.clock.entries[0].counter, 11);
        }
        other => panic!("expected craqle graph meta, got {other:?}"),
    }
}

#[test]
fn decodes_log_batch() {
    let graph = 12_u128;
    let subject = 13_u128;
    let predicate = 14_u128;
    let object = 15_u128;
    let actor = CraqleActorId::from_bytes([3_u8; 32]);

    let mut key = vec![LOG_BATCH_PREFIX];
    key.extend_from_slice(&graph.to_be_bytes());
    key.extend_from_slice(actor.as_bytes());
    key.extend_from_slice(&17_u64.to_be_bytes());

    let batch = CraqleStoredBatch {
        actor,
        counter: 17,
        base_clock: CraqleVectorClock(BTreeMap::from([(actor, 16_u64)])),
        ops: vec![CraqleQuadOp::Add {
            subject: CraqleTermId(subject),
            predicate: CraqleTermId(predicate),
            object: CraqleTermId(object),
            dot: CraqleDot { actor, counter: 17 },
        }],
        timestamp: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
    };

    let mut value = vec![BATCH_ENCODING_TAG];
    value.extend_from_slice(&postcard::to_allocvec(&batch).unwrap());

    let decoded = decode_entry(CRAQLE_LOG_KEYSPACE, &key, &value);
    match decoded.key {
        DecodedField::CraqleLogKey { value } => assert_eq!(
            value,
            JsonLogKey::Batch {
                graph: format!("{graph:032x}"),
                actor: actor.to_string(),
                counter: 17,
            }
        ),
        other => panic!("expected craqle log key, got {other:?}"),
    }
    match decoded.value {
        DecodedValue::CraqleLogBatch { data } => {
            assert_eq!(data.graph, format!("{graph:032x}"));
            assert_eq!(data.actor, actor.to_string());
            assert_eq!(data.counter, 17);
            assert_eq!(data.base_clock.entries.len(), 1);
            assert_eq!(data.base_clock.entries[0].counter, 16);
            assert_eq!(data.ops.len(), 1);
            match &data.ops[0] {
                JsonStoredOp::Add {
                    subject: got_subject,
                    predicate: got_predicate,
                    object: got_object,
                    dot,
                } => {
                    assert_eq!(got_subject, &format!("{subject:032x}"));
                    assert_eq!(got_predicate, &format!("{predicate:032x}"));
                    assert_eq!(got_object, &format!("{object:032x}"));
                    assert_eq!(dot.actor, actor.to_string());
                    assert_eq!(dot.counter, 17);
                }
                other => panic!("expected add op, got {other:?}"),
            }
        }
        other => panic!("expected craqle log batch, got {other:?}"),
    }
}

#[test]
fn invalid_node_raw() {
    let decoded = decode_entry(NODE_STATE_KEYSPACE, b"node_state", b"broken");
    match decoded.value {
        DecodedValue::Raw {
            decode_error: Some(_),
            ..
        } => {}
        other => panic!("expected raw decode fallback, got {other:?}"),
    }
}

#[test]
fn decodes_blob_keyspaces() {
    let realm_id = RealmId::from_bytes([1_u8; 32]);
    let group_id = Ulid::from_bytes([2_u8; 16]);
    let node_id = iroh::SecretKey::from_bytes(&[3_u8; 32]).public();
    let created_by = aruna_core::user::id::UserId::local(Ulid::from_bytes([6_u8; 16]), realm_id);
    let head_key = BlobHeadKey::new("bucket", "path/file.txt");
    let head_value =
        aruna_core::structs::storage::blob::CurrentVersionPointer::new_with_generation(
            Ulid::from_bytes([4_u8; 16]),
            7,
        )
        .to_bytes()
        .unwrap();
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "blob-bucket".to_string(),
        backend_path: "path/blob.bin".to_string(),
        ulid: Ulid::from_bytes([5_u8; 16]),
        format: StoredFormat::default(),
        created_by,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 11,
        hashes: HashMap::from([("blake3".to_string(), vec![9_u8; 32])]),
    };
    let version = BlobVersion::materialized(
        [9_u8; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        created_by,
        None,
    );
    let hash_path_key = HashIndex::new(
        [9_u8; 32],
        Ulid::from_bytes([4_u8; 16]),
        realm_id,
        group_id,
        node_id,
        "bucket",
        "path/file.txt",
    );

    let decoded_head = decode_entry(
        BLOB_HEAD_KEYSPACE,
        &head_key.to_bytes().unwrap(),
        &head_value,
    );
    assert_eq!(
        decoded_head.key,
        DecodedField::BlobHeadKey { value: head_key }
    );
    match decoded_head.value {
        DecodedValue::CurrentVersionPointer { data } => {
            assert_eq!(data.version_id, Ulid::from_bytes([4_u8; 16]));
            assert_eq!(data.generation, 7);
        }
        other => panic!("expected current version pointer, got {other:?}"),
    }

    let location_key =
        BlobLocationKey::new([9_u8; 32], EncodingClass::Raw, location.backend.clone());
    let decoded_location = decode_entry(
        BLOB_LOCATIONS_KEYSPACE,
        &location_key.to_bytes(),
        &location.to_bytes().unwrap(),
    );
    assert_eq!(
        decoded_location.key,
        DecodedField::BlobLocationKey {
            blake3: hex::encode([9_u8; 32]),
            backend: location.backend.to_string(),
        }
    );
    match decoded_location.value {
        DecodedValue::BackendLocation { data } => assert_eq!(data, location),
        other => panic!("expected backend location, got {other:?}"),
    }

    let version_key = aruna_core::structs::storage::blob::VersionKey::new(
        "bucket",
        "path/file.txt",
        Ulid::from_bytes([6_u8; 16]),
    );
    let decoded_version = decode_entry(
        BLOB_VERSIONS_KEYSPACE,
        &version_key.to_bytes().unwrap(),
        &version.to_bytes().unwrap(),
    );
    match decoded_version.value {
        DecodedValue::BlobVersion { data } => assert_eq!(data, version),
        other => panic!("expected blob version, got {other:?}"),
    }

    let decoded_index = decode_entry(
        PATHS_INDEX_KEYSPACE,
        &hash_path_key.to_bytes().unwrap(),
        &[],
    );
    assert_eq!(
        decoded_index.key,
        DecodedField::HashIndex {
            value: hash_path_key.clone()
        }
    );
    match decoded_index.value {
        DecodedValue::Raw {
            hex,
            decode_error: None,
        } => assert_eq!(hex, ""),
        other => panic!("expected raw marker value, got {other:?}"),
    }
    assert_eq!(
        hash_path_key.permission_path(),
        format!(
            "/{realm_id}/g/{group_id}/data/{}/bucket/path/file.txt",
            node_id
        )
    );
}
