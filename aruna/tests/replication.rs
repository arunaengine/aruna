mod shared;

use aruna_api::routes::credentials::CreateS3PathRestriction;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ReplicationConfiguration,
    ReplicationRule, ReplicationRuleStatus,
};
use shared::{
    JoinerNode, SeedNode, TestResult, bucket_arn, create_bearer_token, create_group_via_http,
    create_onboarding_secret_via_http, create_s3_credentials_via_http,
    create_s3_credentials_with_restrictions_via_http, s3_client, spawn_full_joiner_node,
    spawn_full_seed_node, wait_for_group_via_http, wait_for_realm_nodes, wait_until,
};
use std::time::Duration;

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
async fn replication_does_not_auto_create_destination_bucket() -> TestResult<()> {
    let harness = ReplicationHarness::new("replication-missing-destination-group").await?;

    let result = async {
        let bucket = "replication-missing-destination";
        let key = "objects/never-replicated.txt";
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
            .body(ByteStream::from(body))
            .send()
            .await?;

        harness
            .assert_object_never_appears(bucket, key, 15, Duration::from_millis(200))
            .await?;

        let joiner_buckets = harness.joiner_client.list_buckets().send().await?;
        assert!(
            joiner_buckets
                .buckets()
                .iter()
                .all(|entry| entry.name().unwrap_or_default() != bucket)
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
