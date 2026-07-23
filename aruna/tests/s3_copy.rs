mod shared;

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, MetadataDirective};
use shared::{
    SeedNode, TestResult, create_bearer_token, create_group_via_http,
    create_s3_credentials_via_http, s3_client, spawn_full_seed_node,
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

async fn s3_setup(group_name: &str) -> TestResult<(SeedNode, S3Client)> {
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
    let endpoint = seed
        .s3
        .as_ref()
        .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
    let client = s3_client(endpoint, &credentials);
    Ok((seed, client))
}

#[tokio::test]
async fn copy_object_same_bucket_preserves_bytes_and_etag() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-copy-same-bucket").await?;

    let result = async {
        let bucket = "s3-copy-same-bucket";
        let body = b"copy object identical bytes".to_vec();
        client.create_bucket().bucket(bucket).send().await?;

        let source_put = client
            .put_object()
            .bucket(bucket)
            .key("source.txt")
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        let source_etag = source_put.e_tag().map(ToOwned::to_owned);
        assert!(source_etag.is_some());

        let copy = client
            .copy_object()
            .bucket(bucket)
            .key("dest.txt")
            .copy_source(format!("{bucket}/source.txt"))
            .send()
            .await?;
        let copy_etag = copy
            .copy_object_result()
            .and_then(|result| result.e_tag())
            .map(ToOwned::to_owned);
        assert_eq!(copy_etag, source_etag);

        let dest = client
            .get_object()
            .bucket(bucket)
            .key("dest.txt")
            .send()
            .await?;
        let dest_etag = dest.e_tag().map(ToOwned::to_owned);
        let dest_bytes = dest.body.collect().await?.into_bytes().to_vec();
        assert_eq!(dest_etag, source_etag);
        assert_eq!(dest_bytes, body);
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn copy_object_with_version_id_source_copies_old_version() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-copy-version-source").await?;

    let result = async {
        let bucket = "s3-copy-version-source";
        let old_body = b"copy the old version bytes".to_vec();
        let new_body = b"the current version has different bytes".to_vec();
        client.create_bucket().bucket(bucket).send().await?;

        let old_put = client
            .put_object()
            .bucket(bucket)
            .key("obj.txt")
            .body(ByteStream::from(old_body.clone()))
            .send()
            .await?;
        let old_version = old_put
            .version_id()
            .ok_or_else(|| std::io::Error::other("first put missing version id"))?
            .to_string();

        client
            .put_object()
            .bucket(bucket)
            .key("obj.txt")
            .body(ByteStream::from(new_body.clone()))
            .send()
            .await?;

        let copy = client
            .copy_object()
            .bucket(bucket)
            .key("dest.txt")
            .copy_source(format!("{bucket}/obj.txt?versionId={old_version}"))
            .send()
            .await?;
        assert_eq!(copy.copy_source_version_id(), Some(old_version.as_str()));

        let dest = client
            .get_object()
            .bucket(bucket)
            .key("dest.txt")
            .send()
            .await?;
        let dest_bytes = dest.body.collect().await?.into_bytes().to_vec();
        assert_eq!(dest_bytes, old_body);
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn metadata_roundtrip() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-copy-metadata-replace").await?;

    let result = async {
        let bucket = "s3-copy-metadata-replace";
        let mtime = "1753272000.123456789";
        let md5 = "XUFAKrxLKna5cZ2REBfFkg==";
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key("source.txt")
            .metadata("mtime", mtime)
            .metadata("md5chksum", md5)
            .body(ByteStream::from_static(b"metadata replacement source"))
            .send()
            .await?;

        let head = client
            .head_object()
            .bucket(bucket)
            .key("source.txt")
            .send()
            .await?;
        assert_eq!(
            head.metadata()
                .and_then(|metadata| metadata.get("mtime"))
                .map(String::as_str),
            Some(mtime)
        );
        assert_eq!(
            head.metadata()
                .and_then(|metadata| metadata.get("md5chksum"))
                .map(String::as_str),
            Some(md5)
        );

        let get = client
            .get_object()
            .bucket(bucket)
            .key("source.txt")
            .send()
            .await?;
        assert_eq!(
            get.metadata()
                .and_then(|metadata| metadata.get("mtime"))
                .map(String::as_str),
            Some(mtime)
        );

        client
            .copy_object()
            .bucket(bucket)
            .key("dest.txt")
            .copy_source(format!("{bucket}/source.txt"))
            .send()
            .await?;
        let copied = client
            .head_object()
            .bucket(bucket)
            .key("dest.txt")
            .send()
            .await?;
        assert_eq!(
            copied
                .metadata()
                .and_then(|metadata| metadata.get("mtime"))
                .map(String::as_str),
            Some(mtime)
        );
        assert_eq!(
            copied
                .metadata()
                .and_then(|metadata| metadata.get("md5chksum"))
                .map(String::as_str),
            Some(md5)
        );

        let updated_mtime = "1753273000.987654321";
        client
            .copy_object()
            .bucket(bucket)
            .key("dest.txt")
            .copy_source(format!("{bucket}/dest.txt"))
            .metadata_directive(MetadataDirective::Replace)
            .metadata("mtime", updated_mtime)
            .send()
            .await?;
        let replaced = client
            .head_object()
            .bucket(bucket)
            .key("dest.txt")
            .send()
            .await?;
        assert_eq!(
            replaced
                .metadata()
                .and_then(|metadata| metadata.get("mtime"))
                .map(String::as_str),
            Some(updated_mtime)
        );
        assert!(
            replaced
                .metadata()
                .is_none_or(|metadata| !metadata.contains_key("md5chksum"))
        );

        let multipart = client
            .create_multipart_upload()
            .bucket(bucket)
            .key("multipart.txt")
            .metadata("mtime", mtime)
            .metadata("md5chksum", md5)
            .send()
            .await?;
        let upload_id = multipart
            .upload_id()
            .ok_or_else(|| std::io::Error::other("multipart upload missing upload id"))?;
        let part = client
            .upload_part()
            .bucket(bucket)
            .key("multipart.txt")
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"multipart metadata"))
            .send()
            .await?;
        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key("multipart.txt")
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().unwrap_or_default())
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await?;
        let multipart_head = client
            .head_object()
            .bucket(bucket)
            .key("multipart.txt")
            .send()
            .await?;
        assert_eq!(
            multipart_head
                .metadata()
                .and_then(|metadata| metadata.get("mtime"))
                .map(String::as_str),
            Some(mtime)
        );
        assert_eq!(
            multipart_head
                .metadata()
                .and_then(|metadata| metadata.get("md5chksum"))
                .map(String::as_str),
            Some(md5)
        );

        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn copy_object_if_none_match_matching_source_etag_fails() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-copy-precondition").await?;

    let result = async {
        let bucket = "s3-copy-precondition";
        client.create_bucket().bucket(bucket).send().await?;

        let source_put = client
            .put_object()
            .bucket(bucket)
            .key("src.txt")
            .body(ByteStream::from_static(b"precondition source bytes"))
            .send()
            .await?;
        let source_etag = source_put
            .e_tag()
            .ok_or_else(|| std::io::Error::other("source put missing etag"))?
            .to_string();

        let copy = client
            .copy_object()
            .bucket(bucket)
            .key("dest.txt")
            .copy_source(format!("{bucket}/src.txt"))
            .copy_source_if_none_match(source_etag)
            .send()
            .await;
        assert_eq!(
            service_error_code(&copy).as_deref(),
            Some("PreconditionFailed")
        );

        // The precondition failure must not create a destination object.
        let dest = client
            .get_object()
            .bucket(bucket)
            .key("dest.txt")
            .send()
            .await;
        assert_eq!(service_error_code(&dest).as_deref(), Some("NoSuchKey"));
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn upload_part_copy_range_assembles_expected_object() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-copy-upload-part").await?;

    let result = async {
        let bucket = "s3-copy-upload-part";
        let source_body = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ".to_vec();
        let first_part = vec![b'f'; 5 * 1024 * 1024];
        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("source.txt")
            .body(ByteStream::from(source_body.clone()))
            .send()
            .await?;

        let created = client
            .create_multipart_upload()
            .bucket(bucket)
            .key("assembled.txt")
            .send()
            .await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| std::io::Error::other("create multipart missing upload id"))?
            .to_string();

        let part_one = client
            .upload_part()
            .bucket(bucket)
            .key("assembled.txt")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(first_part.clone()))
            .send()
            .await?;

        let part_two = client
            .upload_part_copy()
            .bucket(bucket)
            .key("assembled.txt")
            .upload_id(&upload_id)
            .part_number(2)
            .copy_source(format!("{bucket}/source.txt"))
            .copy_source_range("bytes=0-4")
            .send()
            .await?;
        let part_two_etag = part_two
            .copy_part_result()
            .and_then(|result| result.e_tag())
            .map(ToOwned::to_owned);
        assert!(part_two_etag.is_some());

        let completed = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .set_e_tag(part_one.e_tag().map(ToOwned::to_owned))
                    .build(),
            )
            .parts(
                CompletedPart::builder()
                    .part_number(2)
                    .set_e_tag(part_two_etag)
                    .build(),
            )
            .build();
        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key("assembled.txt")
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await?;

        let assembled = client
            .get_object()
            .bucket(bucket)
            .key("assembled.txt")
            .send()
            .await?;
        let assembled_bytes = assembled.body.collect().await?.into_bytes().to_vec();
        let mut expected = first_part.clone();
        expected.extend_from_slice(&source_body[0..=4]);
        assert_eq!(assembled_bytes, expected);
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn self_copy_without_replace_directive_fails() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-copy-self").await?;

    let result = async {
        let bucket = "s3-copy-self";
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key("obj.txt")
            .body(ByteStream::from_static(b"self copy body"))
            .send()
            .await?;

        let copy = client
            .copy_object()
            .bucket(bucket)
            .key("obj.txt")
            .copy_source(format!("{bucket}/obj.txt"))
            .send()
            .await;
        assert_eq!(service_error_code(&copy).as_deref(), Some("InvalidRequest"));
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}
