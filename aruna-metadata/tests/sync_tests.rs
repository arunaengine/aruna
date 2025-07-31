pub mod commons;

mod sync_tests {
    use crate::commons::{Server, create_user_with_token, init_server};
    use aruna_metadata::{
        models::requests::{
            AddGroupRequest, AddUserToGroupRequest, AddUserToGroupResponse, CreateProjectRequest,
            CreateProjectResponse, CreateResourceRequest, CreateResourceResponse,
            Request,
        },
        network::network_trait::Network,
    };
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use std::collections::BTreeMap;
    const OFFSET: u16 = 100;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sync_group() {
        let signing_key = SigningKey::generate(&mut OsRng);

        let ref test @ Server {
            controller: ref first_controller,
            ref addr,
            ..
        } = init_server(signing_key.clone(), OFFSET, None, false)
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
        } = init_server(signing_key.clone(), OFFSET, Some(addr.clone()), false)
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
        } = init_server(signing_key, OFFSET, Some(addr.clone()), false)
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sync_paths() {
        let signing_key = SigningKey::generate(&mut OsRng);

        let ref test @ Server {
            controller: ref first_controller,
            ref addr,
            path: ref first_path,
            ..
        } = init_server(signing_key.clone(), OFFSET, None, false)
            .await
            .unwrap();

        let client = reqwest::Client::new();

        let (user_identity, user_token) =
            create_user_with_token(&test, "sync_paths_user".to_string())
                .await
                .unwrap();

        let request = AddGroupRequest {
            name: "sync_paths_group".to_string(),
        };
        let response = request
            .run_request(user_identity.clone(), &first_controller)
            .await
            .unwrap();

        let group_id = response.group.id;

        let create_resource = CreateProjectRequest {
            name: format!("sync_paths_project"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id,
            ..Default::default()
        };

        let response = client
            .post(format!("{first_path}/resources/project"))
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

        let project_id = response.resource.id;

        // Test if group gets synced when creating project
        let Server {
            path: second_path,
            controller,
            ..
        } = init_server(signing_key.clone(), OFFSET, Some(addr.clone()), false)
            .await
            .unwrap();
        controller.network.update_realm().await.unwrap();

        let create_resource = CreateResourceRequest {
            name: format!("other_project"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            parent_id: project_id,
            ..Default::default()
        };

        let response = client
            .post(format!("{second_path}/resources"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token.to_string()).as_ref(),
            )
            .json(&create_resource)
            .send()
            .await
            .unwrap()
            .json::<CreateResourceResponse>()
            .await
            .unwrap();

        let object_id = response.resource.id;

        assert!(matches!(
            controller.persistence.get_resource(project_id).await,
            Err(aruna_metadata::error::ArunaMetadataError::NotFound(_))
        ));

        let _ = client
            .get(format!("{first_path}/resources"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token.to_string()).as_ref(),
            )
            .query(&[("id", object_id)])
            .send()
            .await
            .unwrap()
            .json::<CreateResourceResponse>()
            .await
            .unwrap();
    }
}
