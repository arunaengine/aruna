mod shared;

use aruna_api::routes::credentials::CreateS3PathRestriction;
use aruna_api::routes::groups::AddGroupMemberRequest;
use aruna_api::routes::info::{RealmGroupQuotaOverride, RealmQuotaConfig};
use aruna_api::routes::sync::{
    ApiReferenceHandling, ApiSyncMode, CreateSyncRequest, SyncDetailResponse,
    SyncRelationshipResponse, SyncSourceRequest, SyncTargetRequest,
};
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_REPLICATION_JOB_KEYSPACE, BLOB_VERSIONS_KEYSPACE, SYNC_RELATIONSHIP_IN_KEYSPACE,
    SYNC_RELATIONSHIP_OUT_KEYSPACE, USAGE_STATS_KEYSPACE,
};
use aruna_core::structs::{
    BlobVersion, BlobVersionState, SourceConnectorKind, StagingStrategy, SyncRelationship,
    SyncState, UsageCounters, VersionKey, sync_relationship_key,
};
use aruna_operations::driver::DriverContext;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ReplicationConfiguration,
    ReplicationRule, ReplicationRuleStatus,
};
use reqwest::StatusCode;
use shared::{
    JoinerNode, SeedNode, TestResult, bucket_arn, create_bearer_token, create_group_via_http,
    create_onboarding_secret_via_http, create_s3_credentials_via_http,
    create_s3_credentials_with_restrictions_via_http, s3_client, spawn_full_joiner_node,
    spawn_full_seed_node, wait_for_group_via_http, wait_for_realm_nodes, wait_until,
};
use std::time::Duration;
use ulid::Ulid;

fn error_code<T, E>(result: &Result<T, aws_sdk_s3::error::SdkError<E>>) -> Option<String>
where
    E: ProvideErrorMetadata,
{
    result
        .as_ref()
        .err()
        .and_then(|err| err.as_service_error().and_then(|inner| inner.code()))
        .map(ToOwned::to_owned)
}

fn build_replication_configuration(
    destination_arn: String,
    rule_id: &str,
    replicate_delete_markers: bool,
) -> TestResult<ReplicationConfiguration> {
    let mut rule = ReplicationRule::builder()
        .id(rule_id)
        .priority(1)
        .status(ReplicationRuleStatus::Enabled)
        .destination(
            Destination::builder()
                .bucket(destination_arn)
                .build()
                .map_err(|err| std::io::Error::other(err.to_string()))?,
        );

    if replicate_delete_markers {
        rule = rule.delete_marker_replication(
            DeleteMarkerReplication::builder()
                .status(DeleteMarkerReplicationStatus::Enabled)
                .build(),
        );
    }

    ReplicationConfiguration::builder()
        .role("arn:aruna:replication-role")
        .rules(
            rule.build()
                .map_err(|err| std::io::Error::other(err.to_string()))?,
        )
        .build()
        .map_err(|err| std::io::Error::other(err.to_string()).into())
}

struct ReplicationHarness {
    seed: SeedNode,
    joiner: JoinerNode,
    seed_token: String,
    group_id: String,
    seed_client: S3Client,
    joiner_client: S3Client,
}

impl ReplicationHarness {
    async fn new(group_name: &str) -> TestResult<Self> {
        let seed = spawn_full_seed_node().await?;
        let onboarding_secret =
            create_onboarding_secret_via_http(&seed, aruna_core::onboarding::OnboardingMode::Local)
                .await?;
        let joiner = spawn_full_joiner_node(&seed, onboarding_secret).await?;

        let seed_s3 = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let joiner_s3 = joiner
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("joiner node did not start S3 server"))?;

        wait_for_realm_nodes(
            &[seed.context.as_ref(), joiner.context.as_ref()],
            &seed.realm_id,
            2,
        )
        .await?;

        let seed_token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;

        let group = create_group_via_http(&seed.base_url, &seed_token, group_name).await?;
        wait_for_group_via_http(&joiner.base_url, &seed_token, &group.group_id).await?;

        let seed_credentials =
            create_s3_credentials_via_http(&seed.base_url, &seed_token, &group.group_id).await?;
        let joiner_credentials =
            create_s3_credentials_via_http(&joiner.base_url, &seed_token, &group.group_id).await?;

        let seed_client = s3_client(seed_s3, &seed_credentials);
        let joiner_client = s3_client(joiner_s3, &joiner_credentials);

        Ok(Self {
            seed,
            joiner,
            seed_token,
            group_id: group.group_id,
            seed_client,
            joiner_client,
        })
    }

    fn destination_arn(&self, bucket: &str) -> String {
        bucket_arn(&self.seed.realm_id, self.joiner.config.node_id, bucket)
    }

    async fn create_seed_scoped_client(
        &self,
        path_restrictions: Vec<CreateS3PathRestriction>,
    ) -> TestResult<S3Client> {
        let seed_s3 = self
            .seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let credentials = create_s3_credentials_with_restrictions_via_http(
            &self.seed.base_url,
            &self.seed_token,
            &self.group_id,
            Some(path_restrictions),
        )
        .await?;
        Ok(s3_client(seed_s3, &credentials))
    }

    async fn create_buckets(&self, bucket: &str, create_destination: bool) -> TestResult<()> {
        self.seed_client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await?;
        if create_destination {
            self.joiner_client
                .create_bucket()
                .bucket(bucket)
                .send()
                .await?;
        }
        Ok(())
    }

    async fn create_bucket_pair(&self, source: &str, target: &str) -> TestResult<()> {
        self.seed_client
            .create_bucket()
            .bucket(source)
            .send()
            .await?;
        self.joiner_client
            .create_bucket()
            .bucket(target)
            .send()
            .await?;
        Ok(())
    }

    async fn post_sync(
        &self,
        base_url: &str,
        bearer_token: &str,
        request: CreateSyncRequest,
    ) -> TestResult<SyncRelationshipResponse> {
        let response = reqwest::Client::new()
            .post(format!("{}/api/v1/data/sync-relationships", base_url))
            .bearer_auth(bearer_token)
            .json(&request)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if status != StatusCode::CREATED {
            return Err(std::io::Error::other(format!(
                "sync relationship creation returned {status}: {body}"
            ))
            .into());
        }
        Ok(serde_json::from_str(&body)?)
    }

    async fn get_sync(
        &self,
        base_url: &str,
        bearer_token: &str,
        relationship_id: &str,
    ) -> TestResult<SyncDetailResponse> {
        let response = reqwest::Client::new()
            .get(format!(
                "{base_url}/api/v1/data/sync-relationships/{relationship_id}"
            ))
            .bearer_auth(bearer_token)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if status != StatusCode::OK {
            return Err(std::io::Error::other(format!(
                "sync relationship status returned {status}: {body}"
            ))
            .into());
        }
        Ok(serde_json::from_str(&body)?)
    }

    async fn add_member(&self, user_id: UserId) -> TestResult<String> {
        let member_token = create_bearer_token(
            self.seed.context.as_ref(),
            user_id,
            self.seed.realm_id,
            self.seed.capabilities.clone(),
        )
        .await?;
        let response = reqwest::Client::new()
            .post(format!(
                "{}/api/v1/groups/{}/members",
                self.seed.base_url, self.group_id
            ))
            .bearer_auth(&self.seed_token)
            .json(&AddGroupMemberRequest {
                user_id: user_id.to_string(),
                role_ids: None,
            })
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(std::io::Error::other(format!(
                "adding sync creator returned {}",
                response.status()
            ))
            .into());
        }

        wait_until(
            "sync creator membership convergence",
            Duration::from_secs(10),
            Duration::from_millis(200),
            || {
                let base_url = self.joiner.base_url.clone();
                let group_id = self.group_id.clone();
                let member_token = member_token.clone();
                async move {
                    reqwest::Client::new()
                        .get(format!("{base_url}/api/v1/groups/{group_id}/members"))
                        .bearer_auth(member_token)
                        .send()
                        .await
                        .is_ok_and(|response| response.status() == StatusCode::OK)
                }
            },
        )
        .await?;
        Ok(member_token)
    }

    async fn remove_member(&self, user_id: UserId) -> TestResult<()> {
        let response = reqwest::Client::new()
            .delete(format!(
                "{}/api/v1/groups/{}/members/{user_id}",
                self.seed.base_url, self.group_id
            ))
            .bearer_auth(&self.seed_token)
            .send()
            .await?;
        if response.status() != StatusCode::NO_CONTENT {
            return Err(std::io::Error::other(format!(
                "removing sync creator returned {}",
                response.status()
            ))
            .into());
        }
        Ok(())
    }

    async fn configure_replication(
        &self,
        bucket: &str,
        rule_id: &str,
        replicate_delete_markers: bool,
    ) -> TestResult<()> {
        let replication_configuration = build_replication_configuration(
            self.destination_arn(bucket),
            rule_id,
            replicate_delete_markers,
        )?;
        self.seed_client
            .put_bucket_replication()
            .bucket(bucket)
            .replication_configuration(replication_configuration)
            .send()
            .await?;
        Ok(())
    }

    async fn assert_replication_destination(
        &self,
        bucket: &str,
        expected_destination_arn: &str,
    ) -> TestResult<()> {
        let replication = self
            .seed_client
            .get_bucket_replication()
            .bucket(bucket)
            .send()
            .await?;
        let replication = replication.replication_configuration().ok_or_else(|| {
            std::io::Error::other("missing replication configuration in response")
        })?;
        let rule = replication
            .rules()
            .first()
            .ok_or_else(|| std::io::Error::other("missing replication rule in response"))?;
        let rule_destination = rule
            .destination()
            .map(|destination| destination.bucket())
            .ok_or_else(|| std::io::Error::other("missing replication destination bucket ARN"))?;
        assert_eq!(rule_destination, expected_destination_arn);
        Ok(())
    }

    async fn wait_for_object(&self, bucket: &str, key: &str) -> TestResult<()> {
        wait_until(
            "replicated object availability",
            Duration::from_secs(20),
            Duration::from_millis(200),
            || {
                let joiner_client = self.joiner_client.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                async move {
                    joiner_client
                        .head_object()
                        .bucket(bucket)
                        .key(key)
                        .send()
                        .await
                        .is_ok()
                }
            },
        )
        .await
    }

    async fn wait_seed_object(&self, bucket: &str, key: &str) -> TestResult<()> {
        wait_until(
            "chained object availability",
            Duration::from_secs(20),
            Duration::from_millis(200),
            || {
                let seed_client = self.seed_client.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                async move {
                    seed_client
                        .head_object()
                        .bucket(bucket)
                        .key(key)
                        .send()
                        .await
                        .is_ok()
                }
            },
        )
        .await
    }

    async fn assert_object_matches(&self, bucket: &str, key: &str, body: &[u8]) -> TestResult<()> {
        let head_output = self
            .joiner_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        assert_eq!(head_output.content_length(), Some(body.len() as i64));

        let get_output = self
            .joiner_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        let replicated_bytes = get_output.body.collect().await?.into_bytes().to_vec();
        assert_eq!(replicated_bytes, body);
        Ok(())
    }

    async fn assert_object_never_appears(
        &self,
        bucket: &str,
        key: &str,
        polls: usize,
        interval: Duration,
    ) -> TestResult<()> {
        for _ in 0..polls {
            let head_result = self
                .joiner_client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await;
            assert!(head_result.is_err());
            tokio::time::sleep(interval).await;
        }
        Ok(())
    }

    async fn shutdown(self) {
        self.joiner.shutdown().await;
        self.seed.shutdown().await;
    }
}

async fn keyspace_empty(context: &DriverContext, keyspace: &str) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: keyspace.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::IterResult { values, .. }) if values.is_empty()
    )
}

#[tokio::test]
async fn continuous_remaps_prefix() -> TestResult<()> {
    let harness = ReplicationHarness::new("continuous-remap-group").await?;

    let result = async {
        let source_bucket = "continuous-remap-source";
        let target_bucket = "continuous-remap-target";
        let source_key = "selected/nested/object.txt";
        let target_key = "replica/nested/object.txt";
        let excluded_key = "other/not-replicated.txt";
        let excluded_target = "replica/other/not-replicated.txt";
        let body = b"continuous prefix remapping".to_vec();

        harness
            .create_bucket_pair(source_bucket, target_bucket)
            .await?;
        let relationship = harness
            .post_sync(
                &harness.seed.base_url,
                &harness.seed_token,
                CreateSyncRequest {
                    source: SyncSourceRequest {
                        bucket: source_bucket.to_string(),
                        prefix: Some("selected/".to_string()),
                    },
                    target: SyncTargetRequest {
                        node_id: harness.joiner.config.node_id.to_string(),
                        bucket: target_bucket.to_string(),
                        prefix: Some("replica/".to_string()),
                    },
                    mode: ApiSyncMode::Continuous,
                    reference_handling: ApiReferenceHandling::Materialize,
                    replicate_deletes: true,
                },
            )
            .await?;
        let source_arn = aruna_core::structs::ArunaArn::parse(&relationship.source)?;
        let target_arn = aruna_core::structs::ArunaArn::parse(&relationship.target)?;
        assert_eq!(source_arn.bucket(), Some(source_bucket));
        assert_eq!(source_arn.key_prefix(), Some("selected/"));
        assert_eq!(target_arn.bucket(), Some(target_bucket));
        assert_eq!(target_arn.key_prefix(), Some("replica/"));

        let put_output = harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(source_key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        let source_version_id = put_output
            .version_id()
            .ok_or_else(|| std::io::Error::other("source put did not return version id"))?
            .to_string();
        harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(excluded_key)
            .body(ByteStream::from_static(b"must stay on source"))
            .send()
            .await?;

        harness.wait_for_object(target_bucket, target_key).await?;
        harness
            .assert_object_matches(target_bucket, target_key, &body)
            .await?;
        harness
            .assert_object_never_appears(target_bucket, source_key, 10, Duration::from_millis(200))
            .await?;
        harness
            .assert_object_never_appears(
                target_bucket,
                excluded_key,
                10,
                Duration::from_millis(200),
            )
            .await?;
        harness
            .assert_object_never_appears(
                target_bucket,
                excluded_target,
                10,
                Duration::from_millis(200),
            )
            .await?;

        let delete_output = harness
            .seed_client
            .delete_object()
            .bucket(source_bucket)
            .key(source_key)
            .send()
            .await?;
        assert_eq!(delete_output.delete_marker(), Some(true));
        let delete_version_id = delete_output
            .version_id()
            .ok_or_else(|| std::io::Error::other("source delete did not return version id"))?
            .to_string();

        wait_until(
            "remapped delete marker visibility",
            Duration::from_secs(20),
            Duration::from_millis(200),
            || {
                let joiner_client = harness.joiner_client.clone();
                async move {
                    joiner_client
                        .head_object()
                        .bucket(target_bucket)
                        .key(target_key)
                        .send()
                        .await
                        .is_err()
                }
            },
        )
        .await?;

        let marker_get = harness
            .joiner_client
            .get_object()
            .bucket(target_bucket)
            .key(target_key)
            .version_id(delete_version_id)
            .send()
            .await;
        assert_eq!(error_code(&marker_get).as_deref(), Some("MethodNotAllowed"));

        let prior_version = harness
            .joiner_client
            .get_object()
            .bucket(target_bucket)
            .key(target_key)
            .version_id(source_version_id)
            .send()
            .await?;
        let prior_bytes = prior_version.body.collect().await?.into_bytes().to_vec();
        assert_eq!(prior_bytes, body);
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn once_syncs_prefix() -> TestResult<()> {
    let harness = ReplicationHarness::new("once-prefix-group").await?;

    let result = async {
        let source_bucket = "once-prefix-source";
        let target_bucket = "once-prefix-target";
        let source_key = "selected/archive.bin";
        let target_key = "snapshot/archive.bin";
        let excluded_key = "other/not-selected.bin";
        let excluded_target = "snapshot/other/not-selected.bin";
        let body = b"pre-existing once sync".to_vec();

        harness
            .create_bucket_pair(source_bucket, target_bucket)
            .await?;
        harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(source_key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(excluded_key)
            .body(ByteStream::from_static(b"must not be copied"))
            .send()
            .await?;

        let relationship = harness
            .post_sync(
                &harness.seed.base_url,
                &harness.seed_token,
                CreateSyncRequest {
                    source: SyncSourceRequest {
                        bucket: source_bucket.to_string(),
                        prefix: Some("selected/".to_string()),
                    },
                    target: SyncTargetRequest {
                        node_id: harness.joiner.config.node_id.to_string(),
                        bucket: target_bucket.to_string(),
                        prefix: Some("snapshot/".to_string()),
                    },
                    mode: ApiSyncMode::Once,
                    reference_handling: ApiReferenceHandling::Materialize,
                    replicate_deletes: false,
                },
            )
            .await?;
        assert_eq!(relationship.mode, ApiSyncMode::Once);

        harness.wait_for_object(target_bucket, target_key).await?;
        harness
            .assert_object_matches(target_bucket, target_key, &body)
            .await?;
        harness
            .assert_object_never_appears(target_bucket, source_key, 10, Duration::from_millis(200))
            .await?;
        harness
            .assert_object_never_appears(
                target_bucket,
                excluded_key,
                10,
                Duration::from_millis(200),
            )
            .await?;
        harness
            .assert_object_never_appears(
                target_bucket,
                excluded_target,
                10,
                Duration::from_millis(200),
            )
            .await?;
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn reference_syncs_lazily() -> TestResult<()> {
    async fn read_value(
        context: &DriverContext,
        keyspace: &str,
        key: Vec<u8>,
    ) -> TestResult<Option<Vec<u8>>> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: keyspace.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                Ok(value.map(|value| value.as_ref().to_vec()))
            }
            event => Err(std::io::Error::other(format!(
                "unexpected storage read event: {event:?}"
            ))
            .into()),
        }
    }

    async fn read_usage(context: &DriverContext) -> TestResult<UsageCounters> {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 4096,
                txn_id: None,
            })
            .await
        else {
            return Err(std::io::Error::other("unexpected usage iteration event").into());
        };
        let mut usage = UsageCounters::default();
        for (key, value) in values {
            if key.as_ref().starts_with(b"global/") {
                usage.add(&UsageCounters::from_bytes(value.as_ref())?)?;
            }
        }
        Ok(usage)
    }

    let harness = ReplicationHarness::new("reference-sync-group").await?;

    let result = async {
        let source_bucket = "reference-sync-source";
        let target_bucket = "reference-sync-target";
        let key = "archive/large-object.bin";
        let body = vec![0x5au8; 512 * 1024];

        harness
            .create_bucket_pair(source_bucket, target_bucket)
            .await?;
        let initial_usage = read_usage(harness.joiner.context.as_ref()).await?;
        assert_eq!(initial_usage.stored_blobs, 0);
        assert_eq!(initial_usage.stored_bytes, 0);
        assert_eq!(initial_usage.logical_bytes, 0);

        let put_output = harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        let source_version_id = put_output
            .version_id()
            .ok_or_else(|| std::io::Error::other("source put did not return version id"))?
            .parse::<Ulid>()?;
        let source_version = read_value(
            harness.seed.context.as_ref(),
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(source_bucket, key, source_version_id).to_bytes()?,
        )
        .await?
        .ok_or_else(|| std::io::Error::other("missing source version record"))?;
        let source_hash = BlobVersion::from_bytes(&source_version)?
            .blob_hash()
            .copied()
            .ok_or_else(|| std::io::Error::other("source version is not materialized"))?;

        let relationship = harness
            .post_sync(
                &harness.seed.base_url,
                &harness.seed_token,
                CreateSyncRequest {
                    source: SyncSourceRequest {
                        bucket: source_bucket.to_string(),
                        prefix: None,
                    },
                    target: SyncTargetRequest {
                        node_id: harness.joiner.config.node_id.to_string(),
                        bucket: target_bucket.to_string(),
                        prefix: None,
                    },
                    mode: ApiSyncMode::Reference,
                    reference_handling: ApiReferenceHandling::Preserve,
                    replicate_deletes: true,
                },
            )
            .await?;
        assert_eq!(relationship.mode, ApiSyncMode::Reference);

        harness.wait_for_object(target_bucket, key).await?;
        let target_version = read_value(
            harness.joiner.context.as_ref(),
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(target_bucket, key, source_version_id).to_bytes()?,
        )
        .await?
        .ok_or_else(|| std::io::Error::other("missing target reference version"))?;
        let target_version = BlobVersion::from_bytes(&target_version)?;
        let BlobVersionState::Reference { source, .. } = &target_version.state else {
            return Err(std::io::Error::other("target version is not a reference").into());
        };
        assert_eq!(source.strategy, StagingStrategy::Reference);
        assert_eq!(source.connector_id, None);
        assert_eq!(source.descriptor.kind, SourceConnectorKind::ArunaNative);
        assert_eq!(
            source.descriptor.origin_node_id,
            Some(harness.seed.net.node_id())
        );
        assert_eq!(
            source.descriptor.source_path,
            format!("{source_bucket}/{key}")
        );
        assert!(
            read_value(
                harness.joiner.context.as_ref(),
                BLOB_LOCATIONS_KEYSPACE,
                source_hash.to_vec(),
            )
            .await?
            .is_none()
        );

        harness
            .assert_object_matches(target_bucket, key, &body)
            .await?;
        assert!(
            read_value(
                harness.joiner.context.as_ref(),
                BLOB_LOCATIONS_KEYSPACE,
                source_hash.to_vec(),
            )
            .await?
            .is_none()
        );
        let reference_usage = read_usage(harness.joiner.context.as_ref()).await?;
        assert_eq!(reference_usage.stored_blobs, 0);
        assert_eq!(reference_usage.stored_bytes, 0);
        assert_eq!(reference_usage.logical_bytes, 0);
        assert_eq!(reference_usage.referenced_bytes, body.len() as u64);

        let delete_output = harness
            .seed_client
            .delete_object()
            .bucket(source_bucket)
            .key(key)
            .send()
            .await?;
        assert_eq!(delete_output.delete_marker(), Some(true));
        let delete_version_id = delete_output
            .version_id()
            .ok_or_else(|| std::io::Error::other("source delete did not return version id"))?
            .parse::<Ulid>()?;
        wait_until(
            "reference delete marker",
            Duration::from_secs(20),
            Duration::from_millis(200),
            || {
                let context = harness.joiner.context.clone();
                async move {
                    read_value(
                        context.as_ref(),
                        BLOB_VERSIONS_KEYSPACE,
                        VersionKey::new(target_bucket, key, delete_version_id)
                            .to_bytes()
                            .unwrap(),
                    )
                    .await
                    .ok()
                    .flatten()
                    .and_then(|bytes| BlobVersion::from_bytes(&bytes).ok())
                    .is_some_and(|version| version.is_deleted())
                }
            },
        )
        .await?;

        let target_head = harness
            .joiner_client
            .head_object()
            .bucket(target_bucket)
            .key(key)
            .send()
            .await;
        assert!(target_head.is_err());
        let delete_version = read_value(
            harness.joiner.context.as_ref(),
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(target_bucket, key, delete_version_id).to_bytes()?,
        )
        .await?
        .ok_or_else(|| std::io::Error::other("missing target delete marker"))?;
        assert!(BlobVersion::from_bytes(&delete_version)?.is_deleted());

        // Deleting the relationship must keep already synchronized reference
        // objects on the target readable via a detached serving stub.
        let retained_key = "archive/retained-object.bin";
        let retained_body = vec![0xa5u8; 256 * 1024];
        harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(retained_key)
            .body(ByteStream::from(retained_body.clone()))
            .send()
            .await?;
        harness.wait_for_object(target_bucket, retained_key).await?;

        let delete_response = reqwest::Client::new()
            .delete(format!(
                "{}/api/v1/data/sync-relationships/{}",
                harness.seed.base_url, relationship.id
            ))
            .bearer_auth(&harness.seed_token)
            .send()
            .await?;
        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

        let relationship_id = relationship.id.parse::<Ulid>()?;
        let stub = read_value(
            harness.seed.context.as_ref(),
            SYNC_RELATIONSHIP_OUT_KEYSPACE,
            sync_relationship_key(source_bucket, relationship_id),
        )
        .await?
        .ok_or_else(|| std::io::Error::other("missing detached serving stub"))?;
        assert_eq!(
            SyncRelationship::from_bytes(&stub)?.state,
            SyncState::Detached
        );
        wait_until(
            "incoming mirror removal",
            Duration::from_secs(10),
            Duration::from_millis(200),
            || {
                let context = harness.joiner.context.clone();
                async move {
                    read_value(
                        context.as_ref(),
                        SYNC_RELATIONSHIP_IN_KEYSPACE,
                        sync_relationship_key(target_bucket, relationship_id),
                    )
                    .await
                    .is_ok_and(|value| value.is_none())
                }
            },
        )
        .await?;
        let status_after_delete = reqwest::Client::new()
            .get(format!(
                "{}/api/v1/data/sync-relationships/{}",
                harness.seed.base_url, relationship.id
            ))
            .bearer_auth(&harness.seed_token)
            .send()
            .await?;
        assert_eq!(status_after_delete.status(), StatusCode::NOT_FOUND);

        harness
            .assert_object_matches(target_bucket, retained_key, &retained_body)
            .await?;
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn quota_surfaces_failure() -> TestResult<()> {
    let harness = ReplicationHarness::new("quota-failure-group").await?;

    let result = async {
        let source_bucket = "quota-failure-source";
        let target_bucket = "quota-failure-target";
        let key = "large/object.bin";
        let body = b"larger than the target quota".to_vec();

        harness
            .create_bucket_pair(source_bucket, target_bucket)
            .await?;
        harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await?;

        let quota_response = reqwest::Client::new()
            .put(format!("{}/api/v1/info/realm/quota", harness.seed.base_url))
            .bearer_auth(&harness.seed_token)
            .json(&RealmQuotaConfig {
                default_group_quota_bytes: None,
                grace_factor_percent: 100,
                warn_threshold_percent: 85,
                group_overrides: vec![RealmGroupQuotaOverride {
                    group_id: harness.group_id.clone(),
                    quota_bytes: Some(1),
                    grace_factor_percent: Some(100),
                }],
                max_groups_per_user: Some(3),
                user_group_cap_overrides: Vec::new(),
                max_devices_per_user: None,
            })
            .send()
            .await?;
        assert_eq!(quota_response.status(), StatusCode::OK);

        let relationship = harness
            .post_sync(
                &harness.seed.base_url,
                &harness.seed_token,
                CreateSyncRequest {
                    source: SyncSourceRequest {
                        bucket: source_bucket.to_string(),
                        prefix: None,
                    },
                    target: SyncTargetRequest {
                        node_id: harness.joiner.config.node_id.to_string(),
                        bucket: target_bucket.to_string(),
                        prefix: None,
                    },
                    mode: ApiSyncMode::Once,
                    reference_handling: ApiReferenceHandling::Materialize,
                    replicate_deletes: false,
                },
            )
            .await?;

        wait_until(
            "quota failure relationship status",
            Duration::from_secs(20),
            Duration::from_millis(200),
            || {
                let harness = &harness;
                let relationship_id = relationship.id.clone();
                async move {
                    harness
                        .get_sync(
                            &harness.seed.base_url,
                            &harness.seed_token,
                            &relationship_id,
                        )
                        .await
                        .is_ok_and(|detail| {
                            detail
                                .last_error
                                .as_deref()
                                .is_some_and(|error| error.contains("quota"))
                        })
                }
            },
        )
        .await?;
        let detail = harness
            .get_sync(
                &harness.seed.base_url,
                &harness.seed_token,
                &relationship.id,
            )
            .await?;
        assert!(detail.pending_jobs > 0);
        assert!(
            detail
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("quota"))
        );
        let target_head = harness
            .joiner_client
            .head_object()
            .bucket(target_bucket)
            .key(key)
            .send()
            .await;
        assert!(target_head.is_err());
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn permission_rechecks_creator() -> TestResult<()> {
    let harness = ReplicationHarness::new("permission-recheck-group").await?;

    let result = async {
        let source_bucket = "permission-recheck-source";
        let target_bucket = "permission-recheck-target";
        let key = "after-removal/object.txt";
        let creator_id = UserId::local(ulid::Ulid::generate(), harness.seed.realm_id);

        harness
            .create_bucket_pair(source_bucket, target_bucket)
            .await?;
        let creator_token = harness.add_member(creator_id).await?;
        let relationship = harness
            .post_sync(
                &harness.seed.base_url,
                &creator_token,
                CreateSyncRequest {
                    source: SyncSourceRequest {
                        bucket: source_bucket.to_string(),
                        prefix: None,
                    },
                    target: SyncTargetRequest {
                        node_id: harness.joiner.config.node_id.to_string(),
                        bucket: target_bucket.to_string(),
                        prefix: None,
                    },
                    mode: ApiSyncMode::Continuous,
                    reference_handling: ApiReferenceHandling::Materialize,
                    replicate_deletes: true,
                },
            )
            .await?;

        harness.remove_member(creator_id).await?;
        harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(key)
            .body(ByteStream::from_static(b"must be rejected at drain"))
            .send()
            .await?;

        wait_until(
            "creator permission failure status",
            Duration::from_secs(20),
            Duration::from_millis(200),
            || {
                let harness = &harness;
                let relationship_id = relationship.id.clone();
                let creator_token = creator_token.clone();
                async move {
                    harness
                        .get_sync(&harness.seed.base_url, &creator_token, &relationship_id)
                        .await
                        .is_ok_and(|detail| {
                            detail.relationship.state == "failed"
                                && detail
                                    .last_error
                                    .as_deref()
                                    .is_some_and(|error| error.contains("access_denied"))
                        })
                }
            },
        )
        .await?;
        let detail = harness
            .get_sync(&harness.seed.base_url, &creator_token, &relationship.id)
            .await?;
        assert_eq!(detail.relationship.state, "failed");
        assert_eq!(
            detail.relationship.failure_reason.as_deref(),
            Some("access_denied")
        );
        assert_eq!(detail.pending_jobs, 0);
        let target_head = harness
            .joiner_client
            .head_object()
            .bucket(target_bucket)
            .key(key)
            .send()
            .await;
        assert!(target_head.is_err());
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn chain_blocks_cycle() -> TestResult<()> {
    let harness = ReplicationHarness::new("chain-cycle-group").await?;

    let result = async {
        let source_bucket = "chain-cycle-source";
        let relay_bucket = "chain-cycle-relay";
        let sink_bucket = "chain-cycle-sink";
        let source_key = "input/object.txt";
        let relay_key = "relay/object.txt";
        let sink_key = "final/object.txt";
        let body = b"two-hop chained sync".to_vec();

        harness
            .seed_client
            .create_bucket()
            .bucket(source_bucket)
            .send()
            .await?;
        harness
            .joiner_client
            .create_bucket()
            .bucket(relay_bucket)
            .send()
            .await?;
        harness
            .seed_client
            .create_bucket()
            .bucket(sink_bucket)
            .send()
            .await?;

        let chained = harness
            .post_sync(
                &harness.joiner.base_url,
                &harness.seed_token,
                CreateSyncRequest {
                    source: SyncSourceRequest {
                        bucket: relay_bucket.to_string(),
                        prefix: Some("relay/".to_string()),
                    },
                    target: SyncTargetRequest {
                        node_id: harness.seed.net.node_id().to_string(),
                        bucket: sink_bucket.to_string(),
                        prefix: Some("final/".to_string()),
                    },
                    mode: ApiSyncMode::Continuous,
                    reference_handling: ApiReferenceHandling::Materialize,
                    replicate_deletes: true,
                },
            )
            .await?;
        let reverse = harness
            .post_sync(
                &harness.joiner.base_url,
                &harness.seed_token,
                CreateSyncRequest {
                    source: SyncSourceRequest {
                        bucket: relay_bucket.to_string(),
                        prefix: Some("relay/".to_string()),
                    },
                    target: SyncTargetRequest {
                        node_id: harness.seed.net.node_id().to_string(),
                        bucket: source_bucket.to_string(),
                        prefix: Some("input/".to_string()),
                    },
                    mode: ApiSyncMode::Continuous,
                    reference_handling: ApiReferenceHandling::Materialize,
                    replicate_deletes: true,
                },
            )
            .await?;
        harness
            .post_sync(
                &harness.seed.base_url,
                &harness.seed_token,
                CreateSyncRequest {
                    source: SyncSourceRequest {
                        bucket: source_bucket.to_string(),
                        prefix: Some("input/".to_string()),
                    },
                    target: SyncTargetRequest {
                        node_id: harness.joiner.config.node_id.to_string(),
                        bucket: relay_bucket.to_string(),
                        prefix: Some("relay/".to_string()),
                    },
                    mode: ApiSyncMode::Continuous,
                    reference_handling: ApiReferenceHandling::Materialize,
                    replicate_deletes: true,
                },
            )
            .await?;

        harness
            .seed_client
            .put_object()
            .bucket(source_bucket)
            .key(source_key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        harness.wait_for_object(relay_bucket, relay_key).await?;
        harness.wait_seed_object(sink_bucket, sink_key).await?;

        let sink_output = harness
            .seed_client
            .get_object()
            .bucket(sink_bucket)
            .key(sink_key)
            .send()
            .await?;
        let sink_bytes = sink_output.body.collect().await?.into_bytes().to_vec();
        assert_eq!(sink_bytes, body);

        wait_until(
            "chained replication queue drain",
            Duration::from_secs(20),
            Duration::from_millis(200),
            || async {
                keyspace_empty(
                    harness.seed.context.as_ref(),
                    BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
                )
                .await
                    && keyspace_empty(
                        harness.joiner.context.as_ref(),
                        BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
                    )
                    .await
                    && keyspace_empty(harness.seed.context.as_ref(), BLOB_REPLICATION_JOB_KEYSPACE)
                        .await
                    && keyspace_empty(
                        harness.joiner.context.as_ref(),
                        BLOB_REPLICATION_JOB_KEYSPACE,
                    )
                    .await
            },
        )
        .await?;

        let chained_detail = harness
            .get_sync(&harness.joiner.base_url, &harness.seed_token, &chained.id)
            .await?;
        assert!(chained_detail.last_synced_at.is_some());
        assert!(chained_detail.relationship.status.counters.versions_synced > 0);

        let reverse_detail = harness
            .get_sync(&harness.joiner.base_url, &harness.seed_token, &reverse.id)
            .await?;
        assert!(reverse_detail.last_synced_at.is_none());
        assert_eq!(
            reverse_detail.relationship.status.counters.versions_synced,
            0
        );
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn replication_live_put_reaches_joined_node() -> TestResult<()> {
    let harness = ReplicationHarness::new("replication-live-put-e2e-group").await?;

    let result = async {
        let bucket = "replication-live-put-e2e";
        let key = "nested/path/object.txt";
        let body = b"replication hello world".to_vec();

        harness.create_buckets(bucket, true).await?;
        let destination_arn = harness.destination_arn(bucket);
        harness
            .configure_replication(bucket, "replication-e2e", true)
            .await?;
        harness
            .assert_replication_destination(bucket, &destination_arn)
            .await?;

        let put_output = harness
            .seed_client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        let _source_version_id = put_output
            .version_id()
            .ok_or_else(|| std::io::Error::other("source put did not return version id"))?
            .to_string();

        harness.wait_for_object(bucket, key).await?;
        harness.assert_object_matches(bucket, key, &body).await
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn replication_delete_marker_reaches_joined_node() -> TestResult<()> {
    let harness = ReplicationHarness::new("replication-delete-marker-e2e-group").await?;

    let result = async {
        let bucket = "replication-delete-marker-e2e";
        let key = "nested/path/deleted-object.txt";
        let body = b"delete marker replication hello world".to_vec();

        harness.create_buckets(bucket, true).await?;
        harness
            .configure_replication(bucket, "replication-delete-marker-e2e", true)
            .await?;

        let put_output = harness
            .seed_client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        let source_version_id = put_output
            .version_id()
            .ok_or_else(|| std::io::Error::other("source put did not return version id"))?
            .to_string();

        harness.wait_for_object(bucket, key).await?;

        let delete_output = harness
            .seed_client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        assert_eq!(delete_output.delete_marker(), Some(true));
        let delete_marker_version_id = delete_output
            .version_id()
            .ok_or_else(|| {
                std::io::Error::other("delete object did not return delete-marker version id")
            })?
            .to_string();

        wait_until(
            "replicated delete marker visibility",
            Duration::from_secs(20),
            Duration::from_millis(200),
            || {
                let joiner_client = harness.joiner_client.clone();
                async move {
                    joiner_client
                        .head_object()
                        .bucket(bucket)
                        .key(key)
                        .send()
                        .await
                        .is_err()
                }
            },
        )
        .await?;

        let current_head_error = harness
            .joiner_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        assert!(current_head_error.is_err());

        let current_get_error = harness
            .joiner_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        assert!(current_get_error.is_err());

        let delete_marker_get_error = harness
            .joiner_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(delete_marker_version_id)
            .send()
            .await;
        assert_eq!(
            error_code(&delete_marker_get_error).as_deref(),
            Some("MethodNotAllowed")
        );

        let prior_version = harness
            .joiner_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(source_version_id)
            .send()
            .await?;
        let prior_bytes = prior_version.body.collect().await?.into_bytes().to_vec();
        assert_eq!(prior_bytes, body);
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn creates_destination_bucket() -> TestResult<()> {
    // Incoming replication now auto-creates a missing destination bucket, so a
    // target-side pre-create is no longer required for delivery.
    let harness = ReplicationHarness::new("replication-missing-destination-group").await?;

    let result = async {
        let bucket = "replication-missing-destination";
        let key = "objects/auto-created.txt";
        let body = b"missing destination bucket".to_vec();

        harness.create_buckets(bucket, false).await?;
        harness
            .configure_replication(bucket, "replication-missing-destination", true)
            .await?;

        harness
            .seed_client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;

        harness.wait_for_object(bucket, key).await?;
        harness.assert_object_matches(bucket, key, &body).await?;

        let joiner_buckets = harness.joiner_client.list_buckets().send().await?;
        assert!(
            joiner_buckets
                .buckets()
                .iter()
                .any(|entry| entry.name().unwrap_or_default() == bucket)
        );
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}

#[tokio::test]
async fn replication_honors_scoped_credential_path_restrictions() -> TestResult<()> {
    let harness = ReplicationHarness::new("replication-scoped-credential-group").await?;

    let result = async {
        let bucket = "replication-scoped-credentials";
        let scoped_key = "scoped/blocked.txt";
        let control_key = "control/replicates.txt";
        let scoped_body = b"scoped credentials should stay local".to_vec();
        let control_body = b"unrestricted credentials still replicate".to_vec();

        harness.create_buckets(bucket, true).await?;
        harness
            .configure_replication(bucket, "replication-scoped-credential", true)
            .await?;

        let scoped_client = harness
            .create_seed_scoped_client(vec![CreateS3PathRestriction {
                pattern: format!("{bucket}/scoped/**"),
                permission: "WRITE".to_string(),
            }])
            .await?;

        scoped_client
            .put_object()
            .bucket(bucket)
            .key(scoped_key)
            .body(ByteStream::from(scoped_body.clone()))
            .send()
            .await?;

        let source_scoped = harness
            .seed_client
            .get_object()
            .bucket(bucket)
            .key(scoped_key)
            .send()
            .await?;
        let source_scoped_bytes = source_scoped.body.collect().await?.into_bytes().to_vec();
        assert_eq!(source_scoped_bytes, scoped_body);

        harness
            .seed_client
            .put_object()
            .bucket(bucket)
            .key(control_key)
            .body(ByteStream::from(control_body.clone()))
            .send()
            .await?;

        harness.wait_for_object(bucket, control_key).await?;
        harness
            .assert_object_matches(bucket, control_key, &control_body)
            .await?;

        harness
            .assert_object_never_appears(bucket, scoped_key, 10, Duration::from_millis(200))
            .await?;

        let scoped_destination_get = harness
            .joiner_client
            .get_object()
            .bucket(bucket)
            .key(scoped_key)
            .send()
            .await;
        assert!(scoped_destination_get.is_err());
        Ok(())
    }
    .await;

    harness.shutdown().await;
    result
}
