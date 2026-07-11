mod shared;

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, ServerSideEncryption};
use shared::{
    S3Credentials, SeedNode, TestResult, create_bearer_token, create_group_via_http,
    create_s3_credentials_via_http, revoke_s3_credentials_via_http, s3_client, s3_client_no_retry,
    spawn_full_seed_node,
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
    let seed = spawn_full_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, group_name).await?;
    let credentials =
        create_s3_credentials_via_http(&seed.base_url, &admin_token, &group.group_id).await?;
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
    Ok(s3_client_no_retry(endpoint, credentials))
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

#[tokio::test]
async fn revoked_credentials_rejected() -> TestResult<()> {
    let (seed, admin_token, credentials) = security_setup("s3-sec-revoked").await?;

    let result = async {
        // The credential works before revocation.
        client(&seed, &credentials)?.list_buckets().send().await?;

        revoke_s3_credentials_via_http(&seed.base_url, &admin_token, &credentials.access_key_id)
            .await?;

        let revoked = client(&seed, &credentials)?.list_buckets().send().await;
        assert_eq!(
            service_error_code(&revoked).as_deref(),
            Some("AccessDenied"),
            "revoked credential must be denied"
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

        // SSE-S3 on PutObject.
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
            Some("NotImplemented")
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

        // SSE-S3 on CopyObject.
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
            Some("NotImplemented")
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
