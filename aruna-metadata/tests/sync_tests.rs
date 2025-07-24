pub mod commons;

mod sync_tests {
    use crate::commons::{Server, TestConfig, create_user_with_token, init_server};
    use aruna_metadata::{
        models::requests::{
            AddGroupRequest, AddUserToGroupRequest, AddUserToGroupResponse, CreateProjectRequest,
            CreateProjectResponse, Request,
        },
        network::network_trait::Network,
    };
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use std::collections::BTreeMap;
    use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};
    const OFFSET: u16 = 100;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sync_group() {
        let signing_key = SigningKey::generate(&mut OsRng);

        let ref test @ Server {
            controller: ref first_controller,
            ref addr,
            ..
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
            .run_request(user_identity.clone(), &first_controller)
            .await
            .unwrap();

        let group_id = response.group.id;
        let group = response.group;

        // Test if group gets synced when creating project
        let Server {
            path, controller, ..
        } = init_server(
            TestConfig {
                socket_addr: "127.0.0.1",
                path: "/dev/shm/sync_tests",
                p2p_port: 60000,
                api_port: 9080,
            },
            signing_key.clone(),
            OFFSET,
            Some(addr.clone()),
        )
        .await
        .unwrap();
        controller.network.update_realm().await.unwrap();

        // Group not yet synced
        assert!(
            !controller
                .persistence
                .check_is_group(group_id)
                .await
                .unwrap()
        );

        let create_resource = CreateProjectRequest {
            name: format!("other_project"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id,
            ..Default::default()
        };

        let _ = client
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
            .unwrap();

        // Group is synced when needed
        assert!(
            controller
                .persistence
                .check_is_group(group_id)
                .await
                .unwrap()
        );
        assert_eq!(
            controller.persistence.get_group(group_id).await.unwrap(),
            group
        );

        // Test if group gets synced when adding user
        let Server {
            path, controller, ..
        } = init_server(
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
        controller.network.update_realm().await.unwrap();

        let logging_env_filter = EnvFilter::try_from_default_env()
            .unwrap_or("none".into())
            .add_directive("aruna_metadata=trace".parse().unwrap())
            .add_directive("aruna_permission=trace".parse().unwrap())
            .add_directive("casbin=trace".parse().unwrap());
        //.add_directive("tower_http=info".parse().unwrap())
        //.add_directive("aruna_net=info".parse().unwrap());

        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_file(true)
            .with_line_number(true)
            .with_filter(logging_env_filter);
        tracing_subscriber::registry().with(fmt_layer).init();

        // Group not yet synced
        assert!(
            !controller
                .persistence
                .check_is_group(group_id)
                .await
                .unwrap()
        );

        let mut user_roles = BTreeMap::new();
        user_roles.insert(user_identity.to_string(), vec!["another_role".to_string()]);

        let add_user = AddUserToGroupRequest {
            group_id,
            user_roles,
        };

        let _: AddUserToGroupResponse = client
            .post(format!("{path}/groups/user"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token.to_string()).as_ref(),
            )
            .json(&add_user)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // Group is synced when needed
        assert!(
            controller
                .persistence
                .check_is_group(group_id)
                .await
                .unwrap()
        );
        assert_eq!(
            controller.persistence.get_group(group_id).await.unwrap(),
            first_controller
                .persistence
                .get_group(group_id)
                .await
                .unwrap(),
        );
    }
}
