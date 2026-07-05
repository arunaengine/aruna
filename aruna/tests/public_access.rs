mod shared;

use aruna_core::structs::blob_bucket_permission_path;
use aws_sdk_s3::primitives::ByteStream;
use reqwest::StatusCode;
use serde_json::json;
use shared::{
    TestResult, create_bearer_token, create_group_via_http, create_s3_credentials_via_http,
    s3_client, spawn_full_seed_node,
};
use ulid::Ulid;

const PUBLIC_BODY: &[u8] = b"public profile artifact bytes";
const PRIVATE_BODY: &[u8] = b"private bytes";

/// A public role (assigned to the Everyone principal via `public: true`)
/// grants anonymous READ on exactly the paths it names — S3 GETs and DRS
/// lookups/downloads succeed without credentials, while writes and everything
/// outside the granted path stay denied.
#[tokio::test]
async fn public_role_grants_anonymous_read_and_nothing_else() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;

    let result = async {
        let bearer_token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;

        let group =
            create_group_via_http(&seed.base_url, &bearer_token, "public-access-e2e").await?;
        let s3_endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let credentials =
            create_s3_credentials_via_http(&seed.base_url, &bearer_token, &group.group_id).await?;
        let s3 = s3_client(s3_endpoint, &credentials);

        let bucket = "public-profiles";
        let key = "profiles/demo/mode.json";
        s3.create_bucket().bucket(bucket).send().await?;
        s3.put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(PUBLIC_BODY))
            .send()
            .await?;
        s3.create_bucket().bucket("private-bucket").send().await?;
        s3.put_object()
            .bucket("private-bucket")
            .key("secret.txt")
            .body(ByteStream::from_static(PRIVATE_BODY))
            .send()
            .await?;

        let http = reqwest::Client::new();
        let object_url = format!("{}/{bucket}/{key}", s3_endpoint.endpoint_url);

        // Before any public role exists, anonymous reads are denied.
        let denied = http.get(&object_url).send().await?;
        assert_eq!(
            denied.status(),
            StatusCode::FORBIDDEN,
            "anonymous GET must be denied before a public role exists"
        );

        // Anonymous DRS lookups are equally denied (403, not 401 — the object
        // exists on this node but nothing grants the Everyone principal READ).
        let public_hash = hex::encode(blake3::hash(PUBLIC_BODY).as_bytes());
        let drs_object_id = format!(
            "arn:aruna:{}:{}:ch/{}",
            seed.realm_id,
            seed.net.node_id(),
            public_hash
        );
        let drs_object_url = format!(
            "{}/api/v1/ga4gh/drs/v1/objects/{}",
            seed.base_url, drs_object_id
        );
        let drs_denied = http.get(&drs_object_url).send().await?;
        assert_eq!(
            drs_denied.status(),
            StatusCode::FORBIDDEN,
            "anonymous DRS lookup must be denied before a public role exists"
        );

        // Mint a public-read role scoped to the public bucket.
        let group_ulid = Ulid::from_string(&group.group_id)?;
        let public_path = format!(
            "{}/**",
            blob_bucket_permission_path(seed.realm_id, group_ulid, seed.net.node_id(), bucket)
        );
        let permissions = std::collections::HashMap::from([(public_path.clone(), "read")]);
        let created = http
            .post(format!(
                "{}/api/v1/groups/{}/roles",
                seed.base_url, group.group_id
            ))
            .bearer_auth(&bearer_token)
            .json(&json!({
                "name": "public-read-profiles",
                "permissions": permissions,
                "public": true,
            }))
            .send()
            .await?;
        let created_status = created.status();
        let created_body = created.text().await?;
        assert_eq!(
            created_status,
            StatusCode::CREATED,
            "creating the public role failed: {created_body}"
        );
        assert!(
            created_body.contains("\"public\":true"),
            "role response should mark the role public: {created_body}"
        );

        // Anonymous read of the public object now succeeds.
        let allowed = http.get(&object_url).send().await?;
        assert_eq!(allowed.status(), StatusCode::OK);
        assert_eq!(allowed.bytes().await?.as_ref(), PUBLIC_BODY);

        // Anonymous writes stay denied — public roles never grant more than
        // the anonymous read path allows.
        let put = http
            .put(&object_url)
            .body("overwrite attempt")
            .send()
            .await?;
        assert_eq!(
            put.status(),
            StatusCode::FORBIDDEN,
            "anonymous writes must be denied even on public buckets"
        );

        // Objects outside the granted path stay private.
        let private = http
            .get(format!(
                "{}/private-bucket/secret.txt",
                s3_endpoint.endpoint_url
            ))
            .send()
            .await?;
        assert_eq!(
            private.status(),
            StatusCode::FORBIDDEN,
            "the public role must not leak other buckets"
        );

        // Authenticated requests inherit public grants: signed access is never
        // weaker than unsigned access (the signing key belongs to the same
        // group here, but the grant flows through the public role's path).
        let signed = s3.get_object().bucket(bucket).key(key).send().await?;
        let signed_body = signed.body.collect().await?.into_bytes();
        assert_eq!(signed_body.as_ref(), PUBLIC_BODY);

        // Anonymous DRS lookup and download now resolve through the public role.
        let drs_allowed = http.get(&drs_object_url).send().await?;
        let drs_status = drs_allowed.status();
        let drs_body = drs_allowed.text().await?;
        assert_eq!(
            drs_status,
            StatusCode::OK,
            "anonymous DRS lookup should succeed via the public role: {drs_body}"
        );

        let download = http
            .get(format!(
                "{}/api/v1/ga4gh/drs/v1/download?object_id={}",
                seed.base_url, drs_object_id
            ))
            .send()
            .await?;
        assert_eq!(download.status(), StatusCode::OK);
        assert_eq!(download.bytes().await?.as_ref(), PUBLIC_BODY);

        // A private object stays unreachable via anonymous DRS.
        let private_hash = hex::encode(blake3::hash(PRIVATE_BODY).as_bytes());
        let private_drs = http
            .get(format!(
                "{}/api/v1/ga4gh/drs/v1/objects/arn:aruna:{}:{}:ch/{}",
                seed.base_url,
                seed.realm_id,
                seed.net.node_id(),
                private_hash
            ))
            .send()
            .await?;
        assert_eq!(
            private_drs.status(),
            StatusCode::FORBIDDEN,
            "anonymous DRS must not resolve private objects"
        );

        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}
