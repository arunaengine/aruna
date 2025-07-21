pub mod commons;

mod sync_tests {
    use crate::commons::{
        Server, TestConfig, create_user_with_token, init_server,
    };
    use aruna_metadata::{
        models::{
            requests::{
                AddGroupRequest, CreateProjectRequest, CreateProjectResponse,
                GetResourceResponse, SearchResponse,
            },
            structs::Resource,
        },
        transactions::request::Request,
    };
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    const OFFSET: u16 = 100;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sync() {
        let signing_key = SigningKey::generate(&mut OsRng);

        let ref test @ Server {
            ref controller,
            ref path,
            ref addr,
        } = init_server(
            TestConfig {
                socket_addr: "127.0.0.1",
                path: "/dev/shm/sync_tests",
                p2p_port: 60000,
                api_port: 9080,
            },
            signing_key.clone(),
            OFFSET,
            None,
        )
        .await
        .unwrap();

        let client = reqwest::Client::new();

        let (user_identity, user_token) = create_user_with_token(&test, "sync_user".to_string())
            .await
            .unwrap();

        let request = AddGroupRequest {
            name: "sync_group".to_string(),
        };
        let response = request
            .run_request(user_identity.clone(), &controller)
            .await
            .unwrap();

        let group = response.group.id;

        let create_resource = CreateProjectRequest {
            name: format!("sync_project"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id: group,
            ..Default::default()
        };

        let project: Resource = client
            .post(format!("{path}/resources/project"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token.to_string()).as_ref(),
            )
            .json(&create_resource)
            .send()
            .await
            .unwrap()
            .json::<CreateProjectResponse>()
            .await
            .unwrap()
            .resource;

        let project_id = project.id;

        let Server { path, .. } = init_server(
            TestConfig {
                socket_addr: "127.0.0.1",
                path: "/dev/shm/sync_tests",
                p2p_port: 60000,
                api_port: 9080,
            },
            signing_key,
            OFFSET,
            Some(addr.clone()),
        )
        .await
        .unwrap();

        let response: GetResourceResponse = client
            .get(format!("{path}/resources"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token.to_string()).as_ref(),
            )
            .query(&[("id".to_string(), project_id.to_string())])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resource, project);

        let response: SearchResponse = client
            .get(format!("{path}/info/search"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token.to_string()).as_ref(),
            )
            .query(&[("query", "name:sync_project")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resources.len(), 1);
        assert_eq!(response.resources.first().unwrap(), &project);
    }
}
