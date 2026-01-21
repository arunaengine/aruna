use crate::common::aruna_client::{
    create_api_token, create_project, create_s3_credentials, get_object, get_pubkeys, get_user,
};
use crate::common::init;
use crate::common::init::init_test_environment;
use crate::common::init_proxy::{
    generate_config, init_data_proxy, PROXY_01_DECODING_KEY, PROXY_01_DEFAULT_ULID,
    PROXY_01_ENCODING_KEY, PROXY_01_GRPC_PORT, PROXY_01_S3_PORT,
};
use crate::common::init_server::{init_server, Component};
use crate::common::test_utils::USER1_OIDC_TOKEN;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::{Credentials, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

#[tokio::test]
async fn upload_download() {
    // Init
    let infra = init_test_environment(true).await;
    let (_server_handle, server_port) = init_server(&infra).await.unwrap();

    // Check if server is correctly initiated
    let keys = get_pubkeys(server_port).await.pubkeys;
    assert_eq!(keys.len(), 3);

    let (dbname, _) = init::init_database(&infra.postgres, Component::Proxy).await;
    let proxy_config = generate_config(
        DieselUlid::from_str(PROXY_01_DEFAULT_ULID).unwrap(),
        PROXY_01_ENCODING_KEY,
        PROXY_01_DECODING_KEY,
        1337,
        PROXY_01_GRPC_PORT,
        PROXY_01_S3_PORT,
        server_port,
        dbname,
    );
    let _proxy_handle = init_data_proxy(proxy_config).await.unwrap();

    // Create Aruna token for user
    let api_token = create_api_token(server_port, USER1_OIDC_TOKEN)
        .await
        .token_secret;
    dbg!(&api_token);

    let user = get_user(server_port, &api_token).await.user.unwrap();
    assert_eq!(user.display_name, "test-user");

    // Create S3 credentials in proxy for user
    let s3_credentials =
        create_s3_credentials(&api_token, server_port, &PROXY_01_DEFAULT_ULID).await;

    // Create Project via Aruna API
    let project = create_project(server_port, &api_token).await;

    // Init S3 client
    let creds = Credentials::new(
        s3_credentials.s3_access_key,
        s3_credentials.s3_secret_key,
        None,
        None,
        "ARUNA_SERVER", // Endpoint name?
    );
    let base_config = aws_config::defaults(BehaviorVersion::v2025_01_17())
        .credentials_provider(creds)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&base_config)
        .region(Region::new("regionOne"))
        .endpoint_url(s3_credentials.s3_endpoint_url)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .build();
    let s3_client = Client::from_conf(s3_config);

    let mut counter = 0;
    loop {
        if counter >= 5 {
            break;
        }
        let buckets = s3_client.list_buckets().send().await.unwrap().buckets;
        if let Some(bucket_vec) = buckets {
            for bucket in bucket_vec {
                if bucket.name.unwrap_or_default() == project.name {
                    // Project readily synced to endpoint
                    break;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(2u64.pow(counter))).await;
        counter += 1;
    }

    // Upload single part file via S3
    let body = ByteStream::from_static("This is some pretty important content.".as_bytes());
    let object_ulid = s3_client
        .put_object()
        .bucket(project.name)
        .key("SRR33138449.7mb.fastq")
        .body(body)
        .send()
        .await
        .unwrap()
        .e_tag
        .unwrap()
        .replace("-", "");

    // Get Object to validate upload
    let object = get_object(server_port, &api_token, &object_ulid)
        .await
        .object
        .unwrap();

    assert_eq!(object.id, object_ulid);
    for hash in object.hashes {
        match hash.alg {
            1 => assert_eq!(hash.hash, "fa8449a915bdf6d71e1140442ad93fbe"), //MD5
            2 => assert_eq!(
                hash.hash,
                "7521e2b83fa99c5351a29289902e0da82224c319dfc0c06573b0464717fb6579"
            ), //SHA256
            _ => panic!("Unexpected hash alg: {}", hash.alg),
        }
    }

    // Download object in tempdir and check if content is same
}
