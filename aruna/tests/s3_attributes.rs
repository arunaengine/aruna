mod shared;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
    ObjectAttributes,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use sha2::{Digest, Sha256};
use shared::{
    TestResult, create_bearer_token, create_group_via_http, create_s3_credentials_via_http,
    s3_client, spawn_full_seed_node,
};

fn sha256_base64(bytes: &[u8]) -> String {
    STANDARD.encode(Sha256::digest(bytes))
}

#[tokio::test]
async fn get_object_attributes_reports_simple_object() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;

    let result = async {
        let bearer = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_via_http(&seed.base_url, &bearer, "s3-attributes-simple").await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let credentials =
            create_s3_credentials_via_http(&seed.base_url, &bearer, &group.group_id).await?;
        let s3 = s3_client(endpoint, &credentials);

        let bucket = "s3-attributes-simple";
        let key = "docs/simple.txt";
        let body = b"attributes simple object body".to_vec();

        s3.create_bucket().bucket(bucket).send().await?;
        let put = s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        let put_etag = put
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();

        let attrs = s3
            .get_object_attributes()
            .bucket(bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await?;

        assert_eq!(attrs.object_size(), Some(body.len() as i64));
        let attr_etag = attrs
            .e_tag()
            .ok_or_else(|| std::io::Error::other("get_object_attributes returned no etag"))?
            .trim_matches('"')
            .to_string();
        assert_eq!(attr_etag, put_etag);
        assert!(attrs.object_parts().is_none());

        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn get_object_attributes_reports_composite_multipart_object() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;

    let result = async {
        let bearer = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group =
            create_group_via_http(&seed.base_url, &bearer, "s3-attributes-multipart").await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let credentials =
            create_s3_credentials_via_http(&seed.base_url, &bearer, &group.group_id).await?;
        let s3 = s3_client(endpoint, &credentials);

        let bucket = "s3-attributes-multipart";
        let key = "docs/multipart.bin";
        let part_one = vec![7u8; 6 * 1024];
        let part_two = vec![9u8; 4 * 1024];
        let total_size = (part_one.len() + part_two.len()) as i64;
        // AWS composite form: base64(sha256(part digests concatenated)) + "-<partCount>".
        let composite_sha256 = {
            let mut combined = Sha256::digest(&part_one).to_vec();
            combined.extend_from_slice(&Sha256::digest(&part_two));
            format!("{}-2", sha256_base64(&combined))
        };

        s3.create_bucket().bucket(bucket).send().await?;
        let create = s3
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_type(ChecksumType::Composite)
            .send()
            .await?;
        let upload_id = create
            .upload_id()
            .ok_or_else(|| std::io::Error::other("create_multipart_upload returned no upload id"))?
            .to_string();

        let mut completed_parts = Vec::new();
        for (index, chunk) in [part_one, part_two].into_iter().enumerate() {
            let part_number = (index + 1) as i32;
            let checksum = sha256_base64(&chunk);
            let uploaded = s3
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .checksum_sha256(checksum.clone())
                .body(ByteStream::from(chunk))
                .send()
                .await?;
            completed_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(uploaded.e_tag().unwrap_or_default())
                    .checksum_sha256(
                        uploaded
                            .checksum_sha256()
                            .map(|value| value.to_string())
                            .unwrap_or(checksum),
                    )
                    .build(),
            );
        }

        // The SDK-computed object-level checksum arrives in the suffixed form.
        let complete = s3
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .checksum_type(ChecksumType::Composite)
            .checksum_sha256(&composite_sha256)
            .send()
            .await?;
        assert_eq!(complete.checksum_sha256(), Some(composite_sha256.as_str()));

        let attrs = s3
            .get_object_attributes()
            .bucket(bucket)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectParts)
            .object_attributes(ObjectAttributes::Checksum)
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await?;

        assert_eq!(attrs.object_size(), Some(total_size));
        let object_parts = attrs.object_parts().ok_or_else(|| {
            std::io::Error::other("get_object_attributes returned no object parts")
        })?;
        assert_eq!(object_parts.total_parts_count(), Some(2));
        assert_eq!(object_parts.parts().len(), 2);
        for (index, part) in object_parts.parts().iter().enumerate() {
            assert_eq!(part.part_number(), Some((index + 1) as i32));
            assert!(
                part.checksum_sha256().is_some(),
                "part {} missing sha256 checksum",
                index + 1
            );
        }

        let checksum = attrs
            .checksum()
            .ok_or_else(|| std::io::Error::other("get_object_attributes returned no checksum"))?;
        assert_eq!(checksum.checksum_sha256(), Some(composite_sha256.as_str()));
        assert_eq!(checksum.checksum_type(), Some(&ChecksumType::Composite));

        let head = s3
            .head_object()
            .bucket(bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;
        assert_eq!(head.checksum_sha256(), Some(composite_sha256.as_str()));
        assert_eq!(head.checksum_type(), Some(&ChecksumType::Composite));
        assert_eq!(head.content_length(), Some(total_size));

        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}
