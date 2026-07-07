mod shared;

use aruna_api::routes::credentials::CreateS3PathRestriction;
use aruna_core::structs::{Permission, blob_group_permission_path};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    VersioningConfiguration,
};
use shared::{
    SeedNode, TestResult, create_bearer_token, create_group_via_http,
    create_s3_credentials_via_http, create_s3_credentials_with_restrictions_via_http, s3_client,
    spawn_full_seed_node,
};
use ulid::Ulid;

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
async fn head_bucket_reports_existing_and_missing_buckets() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-head-bucket").await?;

    let result = async {
        let bucket = "s3-ops-head-bucket";
        client.create_bucket().bucket(bucket).send().await?;

        client.head_bucket().bucket(bucket).send().await?;

        let missing = client
            .head_bucket()
            .bucket("s3-ops-head-bucket-missing")
            .send()
            .await;
        let error = missing
            .expect_err("head_bucket on missing bucket must fail")
            .into_service_error();
        assert!(error.is_not_found(), "expected NotFound, got {error:?}");
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn bucket_versioning_reports_enabled_and_rejects_suspended() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-versioning").await?;

    let result = async {
        let bucket = "s3-ops-versioning";
        client.create_bucket().bucket(bucket).send().await?;

        let versioning = client.get_bucket_versioning().bucket(bucket).send().await?;
        assert_eq!(versioning.status(), Some(&BucketVersioningStatus::Enabled));

        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;

        let suspended = client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await;
        assert_eq!(
            service_error_code(&suspended).as_deref(),
            Some("NotImplemented")
        );
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn delete_objects_marks_existing_and_missing_keys() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-delete-batch").await?;

    let result = async {
        let bucket = "s3-ops-delete-batch";
        client.create_bucket().bucket(bucket).send().await?;

        for key in ["keep/a.txt", "keep/b.txt"] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"delete batch body"))
                .send()
                .await?;
        }

        let delete = Delete::builder()
            .objects(ObjectIdentifier::builder().key("keep/a.txt").build()?)
            .objects(ObjectIdentifier::builder().key("keep/b.txt").build()?)
            .objects(
                ObjectIdentifier::builder()
                    .key("keep/missing.txt")
                    .build()?,
            )
            .quiet(false)
            .build()?;
        let output = client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;

        assert!(output.errors().is_empty(), "unexpected errors: {output:?}");
        assert_eq!(output.deleted().len(), 3);
        for deleted in output.deleted() {
            assert_eq!(deleted.delete_marker(), Some(true));
            assert!(deleted.delete_marker_version_id().is_some());
        }

        // A delete marker now shadows the previously existing key.
        let head = client
            .head_object()
            .bucket(bucket)
            .key("keep/a.txt")
            .send()
            .await;
        assert!(head.is_err());
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn delete_objects_quiet_mode_omits_deleted_entries() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-delete-quiet").await?;

    let result = async {
        let bucket = "s3-ops-delete-quiet";
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key("quiet.txt")
            .body(ByteStream::from_static(b"quiet body"))
            .send()
            .await?;

        let delete = Delete::builder()
            .objects(ObjectIdentifier::builder().key("quiet.txt").build()?)
            .quiet(true)
            .build()?;
        let output = client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;

        assert!(output.deleted().is_empty(), "quiet mode must omit deleted");
        assert!(output.errors().is_empty());
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn delete_objects_authorizes_each_entry_for_prefix_scoped_token() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, "s3-ops-delete-scoped").await?;
    let full_credentials =
        create_s3_credentials_via_http(&seed.base_url, &admin_token, &group.group_id).await?;
    let endpoint = seed
        .s3
        .as_ref()
        .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
    let full_client = s3_client(endpoint, &full_credentials);

    let result = async {
        let bucket = "s3-ops-delete-scoped";
        full_client.create_bucket().bucket(bucket).send().await?;
        for key in ["scoped/in.txt", "other/out.txt"] {
            full_client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"scoped delete body"))
                .send()
                .await?;
        }

        // Mint a credential scoped to the "scoped/" prefix only.
        let group_root =
            blob_group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
        let scoped_credentials = create_s3_credentials_with_restrictions_via_http(
            &seed.base_url,
            &admin_token,
            &group.group_id,
            Some(vec![CreateS3PathRestriction {
                pattern: format!("{group_root}/{bucket}/scoped/**"),
                permission: Permission::WRITE.to_string(),
            }]),
        )
        .await?;
        let scoped_client = s3_client(endpoint, &scoped_credentials);

        let delete = Delete::builder()
            .objects(ObjectIdentifier::builder().key("scoped/in.txt").build()?)
            .objects(ObjectIdentifier::builder().key("other/out.txt").build()?)
            .quiet(false)
            .build()?;
        let output = scoped_client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;

        // In-scope key deleted; out-of-scope key rejected per entry, not whole request.
        let deleted_keys: Vec<&str> = output.deleted().iter().filter_map(|d| d.key()).collect();
        assert_eq!(deleted_keys, vec!["scoped/in.txt"]);

        assert_eq!(output.errors().len(), 1);
        let error = &output.errors()[0];
        assert_eq!(error.key(), Some("other/out.txt"));
        assert_eq!(error.code(), Some("AccessDenied"));

        // The out-of-scope object survives; the in-scope object is now shadowed.
        full_client
            .head_object()
            .bucket(bucket)
            .key("other/out.txt")
            .send()
            .await?;
        assert!(
            full_client
                .head_object()
                .bucket(bucket)
                .key("scoped/in.txt")
                .send()
                .await
                .is_err()
        );
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn delete_objects_reports_invalid_version_id_per_key() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-delete-badversion").await?;

    let result = async {
        let bucket = "s3-ops-delete-badversion";
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key("good.txt")
            .body(ByteStream::from_static(b"good body"))
            .send()
            .await?;

        let delete = Delete::builder()
            .objects(ObjectIdentifier::builder().key("good.txt").build()?)
            .objects(
                ObjectIdentifier::builder()
                    .key("bad.txt")
                    .version_id("not-a-ulid")
                    .build()?,
            )
            .quiet(false)
            .build()?;
        let output = client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;

        assert_eq!(output.deleted().len(), 1);
        assert_eq!(output.deleted()[0].key(), Some("good.txt"));

        assert_eq!(output.errors().len(), 1);
        let error = &output.errors()[0];
        assert_eq!(error.key(), Some("bad.txt"));
        assert_eq!(error.code(), Some("NoSuchVersion"));
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn list_object_versions_orders_versions_and_delete_marker() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-versions-order").await?;

    let result = async {
        let bucket = "s3-ops-versions-order";
        let key = "versioned.txt";
        let first_body = b"first version body".to_vec();
        let second_body = b"second version body which is longer".to_vec();
        client.create_bucket().bucket(bucket).send().await?;

        let first_put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(first_body.clone()))
            .send()
            .await?;
        let first_version = Ulid::from_string(
            first_put
                .version_id()
                .ok_or_else(|| std::io::Error::other("first put missing version id"))?,
        )?;

        let second_put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(second_body.clone()))
            .send()
            .await?;
        let second_version = Ulid::from_string(
            second_put
                .version_id()
                .ok_or_else(|| std::io::Error::other("second put missing version id"))?,
        )?;

        let delete = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        assert_eq!(delete.delete_marker(), Some(true));
        let marker_version = Ulid::from_string(
            delete
                .version_id()
                .ok_or_else(|| std::io::Error::other("delete missing marker version id"))?,
        )?;

        let listing = client.list_object_versions().bucket(bucket).send().await?;

        assert_eq!(listing.versions().len(), 2);
        assert_eq!(listing.delete_markers().len(), 1);

        // Versions are returned newest-first, neither is the current version.
        let newest = &listing.versions()[0];
        let oldest = &listing.versions()[1];
        assert_eq!(
            Ulid::from_string(newest.version_id().unwrap())?,
            second_version
        );
        assert_eq!(
            Ulid::from_string(oldest.version_id().unwrap())?,
            first_version
        );
        assert_eq!(newest.is_latest(), Some(false));
        assert_eq!(oldest.is_latest(), Some(false));
        assert_eq!(newest.size(), Some(second_body.len() as i64));
        assert_eq!(oldest.size(), Some(first_body.len() as i64));

        // The delete marker is the newest entry overall and is current.
        let marker = &listing.delete_markers()[0];
        assert_eq!(
            Ulid::from_string(marker.version_id().unwrap())?,
            marker_version
        );
        assert_eq!(marker.is_latest(), Some(true));
        assert!(marker_version > second_version);
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn list_object_versions_honors_prefix_and_delimiter() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-versions-prefix").await?;

    let result = async {
        let bucket = "s3-ops-versions-prefix";
        client.create_bucket().bucket(bucket).send().await?;
        for key in ["a/1.txt", "a/2.txt", "b/1.txt"] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"prefix body"))
                .send()
                .await?;
        }

        let grouped = client
            .list_object_versions()
            .bucket(bucket)
            .delimiter("/")
            .send()
            .await?;
        let mut prefixes: Vec<&str> = grouped
            .common_prefixes()
            .iter()
            .filter_map(|prefix| prefix.prefix())
            .collect();
        prefixes.sort();
        assert_eq!(prefixes, vec!["a/", "b/"]);
        assert!(grouped.versions().is_empty());

        let scoped = client
            .list_object_versions()
            .bucket(bucket)
            .prefix("a/")
            .delimiter("/")
            .send()
            .await?;
        assert!(scoped.common_prefixes().is_empty());
        let mut keys: Vec<&str> = scoped
            .versions()
            .iter()
            .filter_map(|version| version.key())
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["a/1.txt", "a/2.txt"]);
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn list_object_versions_paginates_with_max_keys() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-versions-page").await?;

    let result = async {
        let bucket = "s3-ops-versions-page";
        client.create_bucket().bucket(bucket).send().await?;
        for key in ["obj-1", "obj-2", "obj-3"] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"page body"))
                .send()
                .await?;
        }

        let page_one = client
            .list_object_versions()
            .bucket(bucket)
            .max_keys(2)
            .send()
            .await?;
        assert_eq!(page_one.is_truncated(), Some(true));
        let page_one_keys: Vec<&str> = page_one
            .versions()
            .iter()
            .filter_map(|version| version.key())
            .collect();
        assert_eq!(page_one_keys, vec!["obj-1", "obj-2"]);

        let next_key = page_one.next_key_marker().map(ToOwned::to_owned);
        let next_version = page_one.next_version_id_marker().map(ToOwned::to_owned);
        assert!(next_key.is_some());

        let mut request = client.list_object_versions().bucket(bucket).max_keys(2);
        if let Some(key_marker) = next_key {
            request = request.key_marker(key_marker);
        }
        if let Some(version_marker) = next_version {
            request = request.version_id_marker(version_marker);
        }
        let page_two = request.send().await?;
        assert_eq!(page_two.is_truncated(), Some(false));
        let page_two_keys: Vec<&str> = page_two
            .versions()
            .iter()
            .filter_map(|version| version.key())
            .collect();
        assert_eq!(page_two_keys, vec!["obj-3"]);
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn multipart_listing_before_and_after_complete() -> TestResult<()> {
    let (seed, client) = s3_setup("s3-ops-multipart-listing").await?;

    let result = async {
        let bucket = "s3-ops-multipart-listing";
        let key = "assembled.bin";
        let part_one = b"multipart-part-one-".to_vec();
        let part_two = b"multipart-part-two".to_vec();
        client.create_bucket().bucket(bucket).send().await?;

        let created = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| std::io::Error::other("create multipart missing upload id"))?
            .to_string();

        let uploaded_one = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(part_one.clone()))
            .send()
            .await?;
        let uploaded_two = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(2)
            .body(ByteStream::from(part_two.clone()))
            .send()
            .await?;

        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await?;
        assert_eq!(uploads.uploads().len(), 1);
        let open_upload = &uploads.uploads()[0];
        assert_eq!(open_upload.key(), Some(key));
        assert_eq!(open_upload.upload_id(), Some(upload_id.as_str()));

        let parts = client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await?;
        assert_eq!(parts.parts().len(), 2);
        let listed_one = &parts.parts()[0];
        let listed_two = &parts.parts()[1];
        assert_eq!(listed_one.part_number(), Some(1));
        assert_eq!(listed_two.part_number(), Some(2));
        assert_eq!(listed_one.size(), Some(part_one.len() as i64));
        assert_eq!(listed_two.size(), Some(part_two.len() as i64));
        assert_eq!(listed_one.e_tag(), uploaded_one.e_tag());
        assert_eq!(listed_two.e_tag(), uploaded_two.e_tag());

        let completed = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .set_e_tag(uploaded_one.e_tag().map(ToOwned::to_owned))
                    .build(),
            )
            .parts(
                CompletedPart::builder()
                    .part_number(2)
                    .set_e_tag(uploaded_two.e_tag().map(ToOwned::to_owned))
                    .build(),
            )
            .build();
        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await?;

        // The completed object is assembled from both parts.
        let object = client.get_object().bucket(bucket).key(key).send().await?;
        let bytes = object.body.collect().await?.into_bytes().to_vec();
        let mut expected = part_one.clone();
        expected.extend_from_slice(&part_two);
        assert_eq!(bytes, expected);

        // The open upload is gone from the listing.
        let uploads_after = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await?;
        assert!(uploads_after.uploads().is_empty());

        // ListParts on a completed upload reports NoSuchUpload (matches AWS).
        let parts_after = client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        assert_eq!(
            service_error_code(&parts_after).as_deref(),
            Some("NoSuchUpload")
        );
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}
