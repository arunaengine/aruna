pub mod commons;

#[cfg(test)]
mod tests {
    use crate::commons::{
        fetch_user_token, init_test_nodes, register_oidc_user,
        register_user_with_group_and_credentials, upload_data,
    };
    use aruna_data::api_json::requests::{CreateS3CredentialsRequest, CreateS3CredentialsResponse};
    use aruna_data::util::s3::create_s3_client;
    use blake3::Hasher;
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
            node_controller.io_handler.store.as_ref(),
            node_controller.token_handler.clone(),
        )
        .unwrap();
        let user_token = fetch_user_token(&user_id, node_controller.token_handler.clone()).unwrap();

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
        let test_nodes = init_test_nodes(1, OFFSET + 10, vec![]).await.unwrap();
        let node = test_nodes.node_services.first().unwrap();
        let node_controller = node.openapi_data_endpoint.0.clone();

        // Register dummy user and create token
        let group_id = Ulid::new();
        let (_, _, creds) = register_user_with_group_and_credentials(
            "Hans",
            group_id,
            test_nodes.realm_key.to_bytes(),
            node_controller.io_handler.store.as_ref(),
            node_controller.token_handler.clone(),
            node_controller.permission_manager.clone(),
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
        let result = node_controller
            .io_handler
            .kademlia
            .find_value(*blake3_hash.as_bytes())
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.first().unwrap().addr,
            node_controller.io_handler.get_node_addr()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multipart_upload() {
        //TODO
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_object() {}

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replicate_object() {}

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_data() {}

    #[tokio::test(flavor = "multi_thread")]
    async fn test_location_stats() {}
}
