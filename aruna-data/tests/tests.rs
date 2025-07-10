pub mod commons;

#[cfg(test)]
mod tests {
    use crate::commons::{fetch_user_token, init_test_nodes, register_oidc_user};
    use aruna_data::api_json::requests::{CreateS3CredentialsRequest, CreateS3CredentialsResponse};
    use ulid::Ulid;

    const OFFSET: u16 = 100;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_credentials() {
        let test_nodes = init_test_nodes(1, OFFSET).await.unwrap();
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
        let unathorized_response = base_request
            .try_clone()
            .unwrap()
            .json(&CreateS3CredentialsRequest {
                group_id: Ulid::new().to_string(),
            })
            .send()
            .await
            .unwrap();
        assert!(unathorized_response.status().is_client_error());
        assert_eq!(unathorized_response.status().as_u16(), 401);

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_put_object() {}

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multipart_upload() {}

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_object() {}

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replicate_object() {}

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_data() {}

    #[tokio::test(flavor = "multi_thread")]
    async fn test_location_stats() {}
}
