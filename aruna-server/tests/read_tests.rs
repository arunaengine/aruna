mod common;

#[cfg(test)]
mod read_tests {

    use crate::common::{init_test, ADMIN_TOKEN, REGULAR_TOKEN};
    use aruna_rust_api::v3::aruna::api::v3::{
        AddGroupRequest, CreateGroupRequest, CreateProjectRequest, CreateRealmRequest,
        CreateResourceRequest as GrpcCreateResourceRequest, GetRealmRequest, Realm,
        ResourceVariant, VisibilityClass,
    };
    use aruna_server::models::{
        models::Permission,
        requests::{
            BatchResource, CreateGroupRequest as ModelsCreateGroupRequest,
            CreateGroupResponse as ModelsCreateGroupResponse,
            CreateProjectRequest as ModelsCreateProject, CreateProjectResponse,
            CreateResourceBatchRequest, CreateResourceBatchResponse, GetEventsResponse,
            GetGroupsFromUserResponse, GetRealmsFromUserResponse, GetRelationsRequest,
            GetRelationsResponse, GetResourcesResponse, GroupAccessRealmResponse, SearchResponse,
            UserAccessGroupResponse,
        },
    };
    use serde_json::json;
    use ulid::Ulid;
    pub const OFFSET: u16 = 100;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_realm() {
        let mut clients = init_test(OFFSET).await;

        let create_request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };

        let response = clients
            .realm_client
            .create_realm(create_request.clone())
            .await
            .unwrap()
            .into_inner();

        let id = response.realm.unwrap().id;
        let group_id = response.admin_group_id;

        let request = GetRealmRequest { id };

        let realm = clients
            .realm_client
            .get_realm(request)
            .await
            .unwrap()
            .into_inner()
            .realm
            .unwrap();

        assert_eq!(&realm.name, &create_request.name);
        assert_eq!(&realm.tag, &create_request.tag);
        assert_eq!(&realm.description, &create_request.description);

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/users/groups", clients.rest_endpoint);

        let response: GetGroupsFromUserResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(response
            .groups
            .iter()
            .any(|(g, p)| g.id == Ulid::from_string(&group_id).unwrap() && p == &Permission::Admin))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search() {
        // Setup
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id,
            visibility: 1,
            ..Default::default()
        };
        let parent_id = Ulid::from_string(
            &clients
                .resource_client
                .create_project(request.clone())
                .await
                .unwrap()
                .into_inner()
                .resource
                .unwrap()
                .id,
        )
        .unwrap();

        // Check if linking works
        let mut resources = Vec::new();
        for i in 0..50 {
            if i == 0 {
                resources.push(BatchResource {
                    name: format!("TestObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::ID(parent_id),
                    variant: aruna_server::models::models::ResourceVariant::Folder,
                    ..Default::default()
                });
            } else {
                if i % 2 != 0 {
                    resources.push(BatchResource {
                        name: format!("TestObjectNo{i}"),
                        parent: aruna_server::models::requests::Parent::Idx(i - 1),
                        visibility: aruna_server::models::models::VisibilityClass::Public,
                        variant: aruna_server::models::models::ResourceVariant::Folder,
                        ..Default::default()
                    });
                } else {
                    resources.push(BatchResource {
                        name: format!("TestObjectNo{i}"),
                        parent: aruna_server::models::requests::Parent::Idx(i - 1),
                        visibility: aruna_server::models::models::VisibilityClass::Private,
                        variant: aruna_server::models::models::ResourceVariant::Folder,
                        ..Default::default()
                    });
                }
            }
        }
        let request = CreateResourceBatchRequest { resources };

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources/batch", clients.rest_endpoint);

        let _response: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let url = format!("{}/api/v3/info/search", clients.rest_endpoint);

        let response: SearchResponse = client
            .get(url.clone())
            .query(&[("query", "TestObject")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.expected_hits, 25);

        let response: SearchResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .query(&[("query", "TestObject")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.expected_hits, 50);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search_permissions() {
        //
        // Setup for search permissions
        //
        let mut clients = init_test(OFFSET).await;
        let client = reqwest::Client::new();

        // 1. Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // 2. Create project for user 1
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id: realm_id.clone(),
            visibility: 1,
            ..Default::default()
        };
        let parent_id = Ulid::from_string(
            &clients
                .resource_client
                .create_project(request.clone())
                .await
                .unwrap()
                .into_inner()
                .resource
                .unwrap()
                .id,
        )
        .unwrap();

        // 3. Create resources for user 1
        let mut resources = Vec::new();
        for i in 0..5 {
            resources.push(BatchResource {
                name: format!("PUBLIC_TEST_U1_{i}"),
                parent: aruna_server::models::requests::Parent::ID(parent_id),
                visibility: aruna_server::models::models::VisibilityClass::Public,
                ..Default::default()
            });
        }
        for i in 5..10 {
            resources.push(BatchResource {
                name: format!("PRIVATE_TEST_U1_{i}"),
                parent: aruna_server::models::requests::Parent::ID(parent_id),
                visibility: aruna_server::models::models::VisibilityClass::Private,
                ..Default::default()
            });
        }
        let request = CreateResourceBatchRequest { resources };
        let url = format!("{}/api/v3/resources/batch", clients.rest_endpoint);
        let _batch_response1: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // 5.Create resources for user 2
        // 5.1. Create group
        let request = ModelsCreateGroupRequest {
            name: "SecondGroup".to_string(),
            description: String::new(),
        };
        let url = format!("{}/api/v3/groups", clients.rest_endpoint);
        let response: ModelsCreateGroupResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // 5.2. Add group to realm
        let request = AddGroupRequest {
            realm_id: realm_id.clone(),
            group_id: response.group.id.to_string(),
        };
        let _response = clients
            .realm_client
            .add_group(request)
            .await
            .unwrap()
            .into_inner();

        // 5.3 Create Project
        let request = ModelsCreateProject {
            name: "TestProjectNo2".to_string(),
            group_id: response.group.id,
            realm_id: Ulid::from_string(&realm_id).unwrap(),
            visibility: aruna_server::models::models::VisibilityClass::Public,
            ..Default::default()
        };
        let url = format!("{}/api/v3/resources/projects", clients.rest_endpoint);
        let response: CreateProjectResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // 5.4. Create resources for user 1
        let mut resources = Vec::new();
        for i in 0..5 {
            resources.push(BatchResource {
                name: format!("PUBLIC_TEST_U2_{i}"),
                parent: aruna_server::models::requests::Parent::ID(response.resource.id),
                visibility: aruna_server::models::models::VisibilityClass::Public,
                variant: aruna_server::models::models::ResourceVariant::Folder,
                ..Default::default()
            });
        }
        for i in 5..10 {
            resources.push(BatchResource {
                name: format!("PRIVATE_TEST_U2_{i}"),
                parent: aruna_server::models::requests::Parent::ID(response.resource.id),
                visibility: aruna_server::models::models::VisibilityClass::Private,
                variant: aruna_server::models::models::ResourceVariant::Folder,
                ..Default::default()
            });
        }
        let request = CreateResourceBatchRequest { resources };
        let url = format!("{}/api/v3/resources/batch", clients.rest_endpoint);
        let _batch_response2: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        //
        // Test search
        //
        let url = format!("{}/api/v3/info/search", clients.rest_endpoint);

        let response: SearchResponse = client
            .get(url.clone())
            .query(&[("query", "PUBLIC")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.expected_hits, 10);

        let response: SearchResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .query(&[("query", "PRIVATE")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(&response.expected_hits, &5);
        assert!(response.resources.iter().all(|r| {
            match r {
                aruna_server::models::models::GenericNode::Resource(n) => n.name.contains("U1"),
                _ => false,
            }
        }));
        assert!(response.resources.iter().all(|r| {
            match r {
                aruna_server::models::models::GenericNode::Resource(n) => !n.name.contains("U2"),
                _ => false,
            }
        }));

        let url = format!("{}/api/v3/info/search", clients.rest_endpoint);
        let response: SearchResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .query(&[("query", "PRIVATE")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(&response.expected_hits, &5);
        assert!(response.resources.iter().all(|r| {
            match r {
                aruna_server::models::models::GenericNode::Resource(n) => !n.name.contains("U1"),
                _ => false,
            }
        }));
        assert!(response.resources.iter().all(|r| {
            match r {
                aruna_server::models::models::GenericNode::Resource(n) => n.name.contains("U2"),
                _ => false,
            }
        }))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_user_realms() {
        let mut clients = init_test(OFFSET).await;

        let mut ids = Vec::new();
        for i in 0..5 {
            let response = clients
                .realm_client
                .create_realm(CreateRealmRequest {
                    tag: format!("test_user_realm{i}"),
                    name: format!("TestUserRealm{i}"),
                    description: String::new(),
                })
                .await
                .unwrap()
                .into_inner();
            ids.push(Ulid::from_string(&response.realm.unwrap().id).unwrap());
        }

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/users/realms", clients.rest_endpoint);

        let response: GetRealmsFromUserResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        for realm in response.realms {
            assert!(ids.contains(&realm.id))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_user_groups() {
        let mut clients = init_test(OFFSET).await;

        let mut ids = Vec::new();
        for i in 0..5 {
            let response = clients
                .group_client
                .create_group(CreateGroupRequest {
                    name: format!("TestUserRealm{i}"),
                    description: String::new(),
                })
                .await
                .unwrap()
                .into_inner();
            ids.push(Ulid::from_string(&response.group.unwrap().id).unwrap());
        }

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/users/groups", clients.rest_endpoint);

        let response: GetGroupsFromUserResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        for (group, permission) in response.groups {
            assert!(ids.contains(&group.id));
            assert_eq!(permission, Permission::Admin);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_relations() {
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id,
            visibility: 1,
            ..Default::default()
        };
        let parent_id = clients
            .resource_client
            .create_project(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap()
            .id;

        let mut resources = Vec::new();
        for i in 0..10 {
            // Create resource
            let request = GrpcCreateResourceRequest {
                name: format!("TestResource{i}"),
                parent_id: parent_id.clone(),
                visibility: 1,
                variant: 2,
                ..Default::default()
            };
            resources.push(
                clients
                    .resource_client
                    .create_resource(request.clone())
                    .await
                    .unwrap()
                    .into_inner()
                    .resource
                    .unwrap(),
            );
        }

        let _get_relations = GetRelationsRequest {
            node: Ulid::from_string(&parent_id).unwrap(),
            direction: aruna_server::models::requests::Direction::Outgoing,
            filter: vec![0],
            offset: None,
            page_size: 1000,
        };

        let _client = reqwest::Client::new();
        let _url = format!(
            "{}/api/v3/resources/{parent_id}/relations",
            clients.rest_endpoint
        );

        // TODO: Test pagination, filters and permissions
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_subscriptions() {
        let mut clients = init_test(OFFSET).await;

        // Create realm with admin user
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create group with regular user
        let request = ModelsCreateGroupRequest {
            name: "SecondGroup".to_string(),
            description: String::new(),
        };
        let rest_client = reqwest::Client::new();
        let url = format!("{}/api/v3/groups", clients.rest_endpoint);
        let response: ModelsCreateGroupResponse = rest_client
            .post(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let group_id = response.group.id.to_string();

        // Request realm access
        let rest_client = reqwest::Client::new();
        let url = format!(
            "{}/api/v3/realms/{}/access",
            clients.rest_endpoint, realm_id
        );
        let _response: GroupAccessRealmResponse = rest_client
            .post(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .query(&[("group_id", group_id.clone())])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // Check subscribed events
        let url = format!("{}/api/v3/info/events", clients.rest_endpoint);
        let response: GetEventsResponse = rest_client
            .get(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .query(&[("subscriber_id", "01JER6JBQ39EDVR2M7A6Z9512B")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response.events.iter().any(|map| map
            .values()
            .into_iter()
            .any(|v| v["type"] == "GroupAccessRealmTx")));

        // Request group access
        let url = format!("{}/api/v3/groups/{}/join", clients.rest_endpoint, group_id);
        let _response: UserAccessGroupResponse = rest_client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let url = format!("{}/api/v3/info/events", clients.rest_endpoint);
        let response: GetEventsResponse = rest_client
            .get(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .query(&[("subscriber_id", "01JER6Q2MEX5SS7GQCSSDFJJVG")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response.events.iter().any(|map| map
            .values()
            .into_iter()
            .any(|v| v["type"] == "UserAccessGroupTx")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_public_resources() {
        let mut clients = init_test(OFFSET).await;
        let create_request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };

        let response = clients
            .realm_client
            .create_realm(create_request.clone())
            .await
            .unwrap()
            .into_inner();

        let realm_id = response.realm.unwrap().id;
        let group_id = response.admin_group_id;

        let project_id = clients
            .resource_client
            .create_project(tonic::Request::new(CreateProjectRequest {
                name: "test123".to_string(),
                title: "title123".to_string(),
                group_id,
                realm_id,
                visibility: VisibilityClass::Public as i32,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap()
            .id;

        let mut objects = vec![];
        for i in 0..5 {
            objects.push(
                clients
                    .resource_client
                    .create_resource(tonic::Request::new(GrpcCreateResourceRequest {
                        name: format!("test{i}"),
                        title: format!("test{i}"),
                        parent_id: project_id.clone(),
                        visibility: VisibilityClass::Public as i32,
                        variant: ResourceVariant::Object as i32,
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner()
                    .resource
                    .unwrap()
                    .id,
            );
        }

        // Test with no token
        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources", clients.rest_endpoint);
        let _response: GetResourcesResponse = client
            .get(url)
            .query(
                &objects
                    .iter()
                    .map(|id| ("ids", id.as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let url = format!(
            "{}/api/v3/resources/{}/relations",
            clients.rest_endpoint, project_id
        );
        let _response: GetRelationsResponse =
            client.get(url).send().await.unwrap().json().await.unwrap();

        // Test with unauth token
        let url = format!("{}/api/v3/resources", clients.rest_endpoint);
        let _response: GetResourcesResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .query(
                &objects
                    .iter()
                    .map(|id| ("ids", id.as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let url = format!(
            "{}/api/v3/resources/{}/relations",
            clients.rest_endpoint, project_id
        );
        let _response: GetRelationsResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", REGULAR_TOKEN))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_events() {
        // Setup
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let create_request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };

        let response = clients
            .realm_client
            .create_realm(create_request.clone())
            .await
            .unwrap()
            .into_inner();

        let realm_id = response.realm.unwrap().id;
        let group_id = response.admin_group_id;
        let group_ulid = Ulid::from_string(&group_id).unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id,
            realm_id,
            visibility: 1,
            ..Default::default()
        };
        let parent_id = Ulid::from_string(
            &clients
                .resource_client
                .create_project(request.clone())
                .await
                .unwrap()
                .into_inner()
                .resource
                .unwrap()
                .id,
        )
        .unwrap();

        // Create resources
        let mut resources = Vec::new();
        for i in 0..50 {
            resources.push(BatchResource {
                name: format!("TestEventsObject{i}"),
                parent: aruna_server::models::requests::Parent::ID(parent_id),
                variant: aruna_server::models::models::ResourceVariant::Folder,
                ..Default::default()
            });
        }
        let request = CreateResourceBatchRequest { resources };

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources/batch", clients.rest_endpoint);
        let _response: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let url = format!("{}/api/v3/info/events", clients.rest_endpoint);
        let group_events_response: GetEventsResponse = client
            .get(url.clone())
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .query(&[("subscriber_id", group_ulid)])
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
 
        // Ugly json parsing to skip over "ULID": Object {} part
        let mut events = group_events_response
            .events
            .into_iter()
            .map(|inner| inner.values().cloned().collect::<Vec<_>>())
            .flatten();

        // Find CreateProject
        assert_eq!(
            events.next().unwrap()["type"].clone(),
            "CreateProjectRequestTx"
        );

        // Find CreateBatch
        assert_eq!(
            events.next().unwrap()["type"].clone(),
            "CreateResourceBatchRequestTx"
        )
    }
}
