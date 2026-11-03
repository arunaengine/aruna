//! Tests S3 rejections: forged and revoked credentials, encryption headers, protocol errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::shared;

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, ServerSideEncryption};
use shared::{
    S3Credentials, SeedNode, TestResult, create_bearer_token, create_group_http,
    create_s3_credentials, revoke_s3_credentials, s3_client, s3_client_once, spawn_complete_seed,
};

fn service_error_code<T, E>(result: &Result<T, aws_sdk_s3::error::SdkError<E>>) -> Option<String>
where
    E: ProvideErrorMetadata,
{
    result
        .as_ref()
        .err()
        .and_then(|err| err.as_service_error().and_then(|inner| inner.code()))
        .map(ToOwned::to_owned)
}

async fn security_setup(group_name: &str) -> TestResult<(SeedNode, String, S3Credentials)> {
    let seed = spawn_complete_seed().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_http(&seed.base_url, &admin_token, group_name).await?;
    let credentials = create_s3_credentials(&seed.base_url, &admin_token, &group.group_id).await?;
    Ok((seed, admin_token, credentials))
}

fn client(seed: &SeedNode, credentials: &S3Credentials) -> TestResult<S3Client> {
    let endpoint = seed
        .s3
        .as_ref()
        .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
    Ok(s3_client(endpoint, credentials))
}

fn no_retry_client(seed: &SeedNode, credentials: &S3Credentials) -> TestResult<S3Client> {
    let endpoint = seed
        .s3
        .as_ref()
        .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
    Ok(s3_client_once(endpoint, credentials))
}

#[tokio::test]
async fn forged_credentials_rejected() -> TestResult<()> {
    let (seed, _admin_token, credentials) = security_setup("s3-sec-forged").await?;

    let result = async {
        // Correct access key, tampered secret -> signature verification fails.
        let tampered = S3Credentials {
            access_key_id: credentials.access_key_id.clone(),
            access_secret: format!("{}tampered", credentials.access_secret),
        };
        let bad_secret = client(&seed, &tampered)?.list_buckets().send().await;
        assert_eq!(
            service_error_code(&bad_secret).as_deref(),
            Some("SignatureDoesNotMatch"),
            "tampered secret must be rejected"
        );

        // Unknown access key -> invalid access key id.
        let unknown = S3Credentials {
            access_key_id: "unknown-access-key".into(),
            access_secret: "irrelevant".into(),
        };
        let bad_key = client(&seed, &unknown)?.list_buckets().send().await;
        assert_eq!(
            service_error_code(&bad_key).as_deref(),
            Some("InvalidAccessKeyId"),
            "unknown access key must be rejected"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

/// Signs `Expect` as aws-sdk-go v1 does on a retry, then drops it as Envoy does.
#[derive(Debug)]
struct DroppedExpect;

impl Intercept for DroppedExpect {
    fn name(&self) -> &'static str {
        "DroppedExpect"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        context
            .request_mut()
            .headers_mut()
            .insert("expect", "100-continue");
        Ok(())
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        context.request_mut().headers_mut().remove("expect");
        Ok(())
    }
}

#[tokio::test]
async fn dropped_expect_accepted() -> TestResult<()> {
    let (seed, _admin_token, credentials) = security_setup("s3-sec-expect").await?;

    let result = async {
        let listed = no_retry_client(&seed, &credentials)?
            .list_buckets()
            .customize()
            .interceptor(DroppedExpect)
            .send()
            .await;
        assert!(
            listed.is_ok(),
            "a signed Expect removed by a proxy must still verify: {:?}",
            service_error_code(&listed)
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn revoked_credentials_rejected() -> TestResult<()> {
    let (seed, admin_token, credentials) = security_setup("s3-sec-revoked").await?;

    let result = async {
        // The credential works before revocation.
        client(&seed, &credentials)?.list_buckets().send().await?;

        revoke_s3_credentials(&seed.base_url, &admin_token, &credentials.access_key_id).await?;

        let revoked = client(&seed, &credentials)?.list_buckets().send().await;
        assert_eq!(
            service_error_code(&revoked).as_deref(),
            Some("InvalidAccessKeyId"),
            "deleted credential must be denied"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn sse_headers_rejected() -> TestResult<()> {
    let (seed, _admin_token, credentials) = security_setup("s3-sec-sse").await?;
    let bucket = "s3-sec-sse";

    let result = async {
        let sse_client = no_retry_client(&seed, &credentials)?;
        sse_client.create_bucket().bucket(bucket).send().await?;
        sse_client
            .put_object()
            .bucket(bucket)
            .key("source.txt")
            .body(ByteStream::from_static(b"plain body"))
            .send()
            .await?;

        // SSE-S3 on PutObject into a plain bucket.
        let put_sse = sse_client
            .put_object()
            .bucket(bucket)
            .key("sse.txt")
            .body(ByteStream::from_static(b"body"))
            .server_side_encryption(ServerSideEncryption::Aes256)
            .send()
            .await;
        assert_eq!(
            service_error_code(&put_sse).as_deref(),
            Some("InvalidArgument")
        );

        // SSE-C on PutObject.
        let put_ssec = sse_client
            .put_object()
            .bucket(bucket)
            .key("ssec.txt")
            .body(ByteStream::from_static(b"body"))
            .sse_customer_algorithm("AES256")
            .sse_customer_key("01234567890123456789012345678901")
            .send()
            .await;
        assert_eq!(
            service_error_code(&put_ssec).as_deref(),
            Some("NotImplemented")
        );

        // SSE-C on CreateMultipartUpload.
        let create_ssec = sse_client
            .create_multipart_upload()
            .bucket(bucket)
            .key("mpu.txt")
            .sse_customer_algorithm("AES256")
            .sse_customer_key("01234567890123456789012345678901")
            .send()
            .await;
        assert_eq!(
            service_error_code(&create_ssec).as_deref(),
            Some("NotImplemented")
        );

        // SSE-S3 on CopyObject into a plain bucket.
        let copy_sse = sse_client
            .copy_object()
            .bucket(bucket)
            .key("copy.txt")
            .copy_source(format!("{bucket}/source.txt"))
            .server_side_encryption(ServerSideEncryption::Aes256)
            .send()
            .await;
        assert_eq!(
            service_error_code(&copy_sse).as_deref(),
            Some("InvalidArgument")
        );

        // SSE-C on a read path (GetObject) is rejected, not silently ignored.
        let get_ssec = sse_client
            .get_object()
            .bucket(bucket)
            .key("source.txt")
            .sse_customer_algorithm("AES256")
            .sse_customer_key("01234567890123456789012345678901")
            .send()
            .await;
        assert_eq!(
            service_error_code(&get_ssec).as_deref(),
            Some("NotImplemented")
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn protocol_errors_mapped() -> TestResult<()> {
    let (seed, _admin_token, credentials) = security_setup("s3-sec-protocol").await?;
    let bucket = "s3-sec-protocol";

    let result = async {
        let client = no_retry_client(&seed, &credentials)?;
        client.create_bucket().bucket(bucket).send().await?;

        // Completing a multipart upload with no parts must be a client error.
        let created = client
            .create_multipart_upload()
            .bucket(bucket)
            .key("mpu.txt")
            .send()
            .await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| std::io::Error::other("missing upload id"))?
            .to_string();
        let empty = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key("mpu.txt")
            .upload_id(&upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().build())
            .send()
            .await;
        assert_eq!(
            service_error_code(&empty).as_deref(),
            Some("InvalidRequest"),
            "completing without parts must be a 4xx, not InternalError"
        );

        // Uploading a part to an unknown upload must be a client error.
        let unknown_part = client
            .upload_part()
            .bucket(bucket)
            .key("mpu.txt")
            .upload_id("01ARZ3NDEKTSV4RRFFQ69G5FAV")
            .part_number(1)
            .body(ByteStream::from_static(b"body"))
            .send()
            .await;
        assert_eq!(
            service_error_code(&unknown_part).as_deref(),
            Some("NoSuchUpload"),
            "unknown upload must be NoSuchUpload"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

// The credential a batch run inside its bucket gets reads and writes that bucket through the
// gateway's own S3 auth path, and nothing else.
#[tokio::test]
async fn workspace_credential_writes() -> TestResult<()> {
    use aruna_core::structs::execution::job::{
        ComputeResources, ExecutionSpec, JobId, JobPayload, JobRecord, WorkspaceMode,
    };
    use aruna_operations::jobs::workflow::workspace::mint_workspace_credential;

    let seed = spawn_complete_seed().await?;
    let result = async {
        let bearer = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&seed.base_url, &bearer, "workspace-credential").await?;
        let owner = create_s3_credentials(&seed.base_url, &bearer, &group.group_id).await?;
        let owner = client(&seed, &owner)?;
        for bucket in ["ws-run", "ws-other"] {
            owner.create_bucket().bucket(bucket).send().await?;
        }
        let spec = ExecutionSpec {
            group_id: ulid::Ulid::from_string(&group.group_id)?,
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            workdir: None,
            env: Default::default(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };
        let node_id = seed.net.node_id();
        let mut record = JobRecord::new(
            JobId::from_bytes([0x5A; 16]),
            JobPayload::Execution(spec.clone()),
            seed.user_id,
            node_id,
            1,
            1,
            None,
        );
        record.workspace_mode = WorkspaceMode::Existing;
        record.workspace_bucket = Some("ws-run".to_string());
        let minted = mint_workspace_credential(
            seed.context.as_ref(),
            &spec,
            &record,
            node_id,
            ("ws-run", None),
        )
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
        let run = client(
            &seed,
            &S3Credentials {
                access_key_id: minted.access_key,
                access_secret: minted.secret,
            },
        )?;

        run.put_object()
            .bucket("ws-run")
            .key("probe/probe.txt")
            .body(ByteStream::from_static(b"probe"))
            .send()
            .await?;
        let listed = run.list_objects_v2().bucket("ws-run").send().await?;
        let keys: Vec<_> = listed.contents().iter().filter_map(|o| o.key()).collect();
        assert_eq!(keys, ["probe/probe.txt"]);
        let read = run
            .get_object()
            .bucket("ws-run")
            .key("probe/probe.txt")
            .send()
            .await?;
        assert_eq!(read.body.collect().await?.into_bytes().as_ref(), b"probe");

        let other = run
            .put_object()
            .bucket("ws-other")
            .key("probe.txt")
            .body(ByteStream::from_static(b"probe"))
            .send()
            .await;
        assert_eq!(service_error_code(&other).as_deref(), Some("AccessDenied"));
        Ok(())
    }
    .await;
    seed.shutdown().await;
    result
}
