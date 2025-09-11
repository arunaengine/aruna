pub mod commons;

#[cfg(test)]
mod tests {
    use crate::commons::{
        fetch_user_token, init_test_nodes, register_oidc_user,
        register_user_with_group_and_credentials, upload_data,
    };
    use aruna_data::api_json::requests::{CreateS3CredentialsRequest, CreateS3CredentialsResponse};
    use aruna_data::util::s3::create_s3_client;
    use aws_sdk_s3::types::{
        CompletedMultipartUpload, CompletedPart, Destination, ReplicationConfiguration,
        ReplicationRule,
    };
    use blake3::Hasher;
    use std::time::Duration;
    use ulid::Ulid;

    const OFFSET: u16 = 0;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_credentials() {
        let test_nodes = init_test_nodes(1, OFFSET, vec![]).await.unwrap();
        let node = test_nodes.node_services.first().unwrap();
        let node_controller = node.openapi_data_endpoint.0.clone();

        // Register dummy user and create token
        let user_id = register_oidc_user(
            "Hans",
            &node_controller.io_handler.store,
            node_controller.io_handler.token_handler.clone(),
        )
        .unwrap();
        let user_token =
            fetch_user_token(&user_id, node_controller.io_handler.token_handler.clone()).unwrap();

        // Create simple http client and base request
        let client = reqwest::Client::new();
        let base_request = client.post(&format!(
            "http://{}/api/v3/users/credentials",
            node.openapi_data_endpoint.1
        ));

        // Unauthorized request
        let unauthorized_response = base_request
            .try_clone()
            .unwrap()
            .json(&CreateS3CredentialsRequest {
                group_id: Ulid::new().to_string(),
            })
            .send()
            .await
            .unwrap();
        assert!(unauthorized_response.status().is_client_error());
        assert_eq!(unauthorized_response.status().as_u16(), 401);

        // Invalid request
        let invalid_response = base_request
            .try_clone()
            .unwrap()
            .json(&CreateS3CredentialsRequest {
                group_id: "InvalidGroupUlid".to_string(),
            })
            .bearer_auth(user_token.clone())
            .send()
            .await
            .unwrap();
        assert!(invalid_response.status().is_client_error());
        assert_eq!(invalid_response.status().as_u16(), 400);
        assert_eq!(
            invalid_response.text().await.unwrap(),
            "\"Invalid parameter Ulid: invalid length\""
        );

        // Valid request
        base_request
            .try_clone()
            .unwrap()
            .json(&CreateS3CredentialsRequest {
                group_id: Ulid::new().to_string(),
            })
            .bearer_auth(user_token)
            .send()
            .await
            .unwrap()
            .json::<CreateS3CredentialsResponse>()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_put_object() {
        let test_nodes = init_test_nodes(1, OFFSET, vec![]).await.unwrap();
        let node = test_nodes.node_services.first().unwrap();
        let node_controller = node.openapi_data_endpoint.0.clone();

        // Register dummy user and create token
        let group_id = Ulid::new();
        let (_, _, creds) = register_user_with_group_and_credentials(
            "Hans",
            group_id,
            test_nodes.realm_key.to_bytes(),
            &node_controller.io_handler.store,
            node_controller.io_handler.token_handler.clone(),
            node_controller.io_handler.permission_manager.clone(),
            node_controller.clone(),
        )
        .await
        .unwrap();

        // Create S3 client and upload some data
        let client = create_s3_client(
            &format!("http://{}", node.s3_endpoint),
            None,
            &creds.access_key_id.to_string(),
            &creds.secret_access_key,
            true,
        )
        .await
        .unwrap();

        // Create bucket
        let _resp = client
            .create_bucket()
            .bucket("some-project")
            .send()
            .await
            .unwrap();

        // Put object
        let body_content = "This is some dummy content";
        let body = aws_sdk_s3::primitives::ByteStream::from_static(body_content.as_bytes());
        let resp = client
            .put_object()
            .bucket("some-project")
            .key("subdir/content.txt")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.e_tag,
            Some("f82323ba75cec986a7abd74c97795c2c".to_string())
        );
        assert_eq!(
            resp.checksum_sha256,
            Some("913ed33e2e8642b4be2c3608d44c8ac44cd571f241d847e472f8dfb78ebf99e6".to_string())
        );

        let mut response = client
            .get_object()
            .bucket("some-project")
            .key("subdir/content.txt")
            .send()
            .await
            .unwrap();

        let mut content = vec![];
        while let Some(bytes) = response.body.try_next().await.unwrap() {
            content.extend_from_slice(bytes.as_ref());
        }
        assert_eq!(
            String::from_utf8(content).unwrap(),
            "This is some dummy content"
        );

        // Find file hash with Kademlia at node
        let blake3_hash = Hasher::new().update(body_content.as_bytes()).finalize();
        let result = node_controller.network.find(blake3_hash).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.first().unwrap().addr,
            node_controller.network.get_node_addr()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multipart_upload() {
        //TODO
        let test_nodes = init_test_nodes(1, OFFSET, vec![]).await.unwrap();
        let node = test_nodes.node_services.first().unwrap();
        let node_controller = node.openapi_data_endpoint.0.clone();

        // Register dummy user and create token
        let group_id = Ulid::new();
        let (_, _, creds) = register_user_with_group_and_credentials(
            "Hans",
            group_id,
            test_nodes.realm_key.to_bytes(),
            &node_controller.io_handler.store,
            node_controller.io_handler.token_handler.clone(),
            node_controller.io_handler.permission_manager.clone(),
            node_controller.clone(),
        )
        .await
        .unwrap();

        // Create S3 client and upload some data
        let client = create_s3_client(
            &format!("http://{}", node.s3_endpoint),
            None,
            &creds.access_key_id.to_string(),
            &creds.secret_access_key,
            true,
        )
        .await
        .unwrap();

        let bucket = "multipart-bucket";

        // Create bucket
        let _resp = client.create_bucket().bucket(bucket).send().await.unwrap();

        let key = "multipartsubdir/content.bytes";

        // Put object
        let resp = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = resp.upload_id.unwrap();
        let mut finish = CompletedMultipartUpload::builder();
        let mut object_before = Vec::new();

        for i in 0..10 {
            let part = vec![u8::MAX; 512];
            object_before.extend(part.iter());
            let response = client
                .upload_part()
                .upload_id(upload_id.clone())
                .bucket(bucket)
                .key(key)
                .part_number(i)
                .body(part.into())
                .send()
                .await
                .unwrap();
            finish = finish.parts(
                CompletedPart::builder()
                    .part_number(i)
                    .e_tag(response.e_tag.unwrap())
                    .build(),
            );
        }

        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(finish.build())
            .send()
            .await
            .unwrap();

        let mut response = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let mut object_after = vec![];
        while let Some(bytes) = response.body.try_next().await.unwrap() {
            object_after.extend_from_slice(bytes.as_ref());
        }
        let hash_before = blake3::hash(&object_before);
        let hash_after = blake3::hash(&object_after);
        assert_eq!(hash_before, hash_after);

        // Find file hash with Kademlia at node
        let result = node_controller.network.find(hash_before).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.first().unwrap().addr,
            node_controller.network.get_node_addr()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_object() {
        // Initialize multiple nodes with a user
        let test_nodes = init_test_nodes(2, OFFSET, vec![]).await.unwrap();
        let node = test_nodes.node_services.first().unwrap();
        let node_controller = node.openapi_data_endpoint.0.clone();

        //TODO: Put object to specific node
        // Register dummy user and create token
        let group_id = Ulid::new();
        let (_, _, creds) = register_user_with_group_and_credentials(
            "Horst",
            group_id,
            test_nodes.realm_key.to_bytes(),
            &node_controller.io_handler.store,
            node_controller.io_handler.token_handler.clone(),
            node_controller.io_handler.permission_manager.clone(),
            node_controller.clone(),
        )
        .await
        .unwrap();

        // Create S3 client and upload some data
        let client = create_s3_client(
            &format!("http://{}", node.s3_endpoint),
            None,
            &creds.access_key_id.to_string(),
            &creds.secret_access_key,
            true,
        )
        .await
        .unwrap();

        let bucket = "other-bucket";

        // Create bucket
        let _resp = client.create_bucket().bucket(bucket).send().await.unwrap();

        let key = "dummy.txt";
        let content_hash = upload_data(
            &client,
            "other-bucket",
            "dummy.txt",
            "Some other content".as_bytes(),
        )
        .await
        .unwrap();

        // Get object with invalid request
        let invalid_client =
            create_s3_client(&format!("http://{}", node.s3_endpoint), None, "", "", true)
                .await
                .unwrap();

        let err_res = invalid_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        assert!(err_res.is_err());

        //TODO: Get object with invalid permissions
        // Get object with valid request from specific node
        let mut response = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let mut content = vec![];
        let mut hasher = Hasher::new();
        while let Some(bytes) = response.body.try_next().await.unwrap() {
            hasher.update(bytes.as_ref());
            content.extend_from_slice(bytes.as_ref());
        }
        let hash = hasher.finalize();
        assert_eq!(String::from_utf8(content).unwrap(), "Some other content");
        assert_eq!(content_hash, hash.to_string());

        //TODO: Get object with valid request from other node in same realm

        //TODO: Get object with valid request from other node in other realm (should fail)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replicate_object() {
        // Initialize multiple nodes with a user
        let mut test_nodes = init_test_nodes(2, OFFSET, vec![]).await.unwrap();
        let node = test_nodes.node_services.pop().unwrap();
        let node_controller = node.openapi_data_endpoint.0.clone();

        //TODO: Put object to specific node
        // Register dummy user and create token
        let group_id = Ulid::new();
        let (_, _, creds) = register_user_with_group_and_credentials(
            "Horst",
            group_id,
            test_nodes.realm_key.to_bytes(),
            &node_controller.io_handler.store,
            node_controller.io_handler.token_handler.clone(),
            node_controller.io_handler.permission_manager.clone(),
            node_controller.clone(),
        )
        .await
        .unwrap();

        // Create S3 client and upload some data
        let client = create_s3_client(
            &format!("http://{}", node.s3_endpoint),
            None,
            &creds.access_key_id.to_string(),
            &creds.secret_access_key,
            true,
        )
        .await
        .unwrap();

        let bucket = "replication-bucket";

        // Create bucket
        let _resp = client.create_bucket().bucket(bucket).send().await.unwrap();

        let key = "dummy.txt";
        let content_hash = upload_data(
            &client,
            bucket,
            "dummy.txt",
            "Some other content".as_bytes(),
        )
        .await
        .unwrap();

        // Create S3 client and upload some data
        let other_node = test_nodes.node_services.pop().unwrap();
        assert_ne!(
            other_node.openapi_data_endpoint.0.network.get_node_addr(),
            node.openapi_data_endpoint.0.network.get_node_addr()
        );

        let other_node_controller = other_node.openapi_data_endpoint.0.clone();
        let node_id = other_node_controller
            .network
            .get_node_addr()
            .node_id
            .to_string();

        let _resp = client
            .put_bucket_replication()
            .bucket(bucket)
            .replication_configuration(
                ReplicationConfiguration::builder()
                    .role(String::new())
                    .rules(
                        ReplicationRule::builder()
                            .id("MyNewReplicationRule".to_string())
                            .destination(
                                Destination::builder()
                                    .bucket(format!("arn:aws:s3:{}:account_id:{}", node_id, bucket))
                                    .build()
                                    .unwrap(),
                            )
                            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        let (_, _, creds) = register_user_with_group_and_credentials(
            "Horst",
            group_id,
            test_nodes.realm_key.to_bytes(),
            &other_node_controller.io_handler.store,
            other_node_controller.io_handler.token_handler.clone(),
            other_node_controller.io_handler.permission_manager.clone(),
            other_node_controller.clone(),
        )
        .await
        .unwrap();

        let other_client = create_s3_client(
            &format!("http://{}", other_node.s3_endpoint),
            None,
            &creds.access_key_id.to_string(),
            &creds.secret_access_key,
            true,
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_secs(10)).await;

        let mut response = other_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let mut content = vec![];
        let mut hasher = Hasher::new();
        while let Some(bytes) = response.body.try_next().await.unwrap() {
            hasher.update(bytes.as_ref());
            content.extend_from_slice(bytes.as_ref());
        }
        let hash = hasher.finalize();
        assert_eq!(String::from_utf8(content).unwrap(), "Some other content");
        assert_eq!(content_hash, hash.to_string());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_data() {
        // TODO
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_location_stats() {
        // TODO
    }
}
