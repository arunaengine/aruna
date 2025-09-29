pub mod commons;

#[cfg(test)]
mod api_tests {
    use crate::commons::{
        Server, TestServers, create_user_with_token, init_lmdb_servers, init_server,
    };
    use aruna_metadata::{
        models::{
            requests::{
                AddGroupRequest, AddGroupResponse, AddUserToGroupRequest, AddUserToGroupResponse,
                CreateProjectRequest, CreateProjectResponse, CreateResourceRequest,
                GetGroupResponse, GetResourceHistoryResponse, GetResourceResponse, GetUserResponse,
                Request, ResourceUpdateRequests, SearchResponse, UpdateResourceAuthorsRequest,
                UpdateResourceDataRequest, UpdateResourceDescriptionRequest,
                UpdateResourceIdentifiersRequest, UpdateResourceLabelsRequest,
                UpdateResourceLicenseRequest, UpdateResourceNameRequest,
                UpdateResourceTitleRequest, UpdateResourceVisibilityRequest,
            },
            structs::{Author, Data, KeyValue, Resource},
        },
        network::network_trait::Network,
    };
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use std::{collections::BTreeMap, time::Duration};
    use ulid::Ulid;
    const OFFSET: u16 = 0;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_user_creation() {
        let TestServers {
            addr_server_pairs: ref servers,
            ..
        } = init_lmdb_servers(OFFSET).await.unwrap();

        let first_node = servers.iter().next().unwrap();

        let pubkey = first_node.controller.network.get_realm_key().await.unwrap();

        let (user1_identity, token1) =
            create_user_with_token(first_node, "create_user_test1".to_string())
                .await
                .unwrap();
        assert_eq!(user1_identity.realm_key, pubkey);
        let (user2_identity, token2) =
            create_user_with_token(first_node, "create_user_test2".to_string())
                .await
                .unwrap();
        assert_eq!(user2_identity.realm_key, pubkey);

        let client = reqwest::Client::new();

        for Server { path, .. } in servers.iter() {
            assert!(
                client
                    .get(format!("{path}/users"))
                    .header::<&str, &str>(
                        "Authorization",
                        format!("Bearer {}", token1.to_string()).as_ref(),
                    )
                    .query(&[("id", user1_identity.to_string())])
                    .send()
                    .await
                    .unwrap()
                    .json::<GetUserResponse>()
                    .await
                    .is_ok()
            );
            assert!(
                client
                    .get(format!("{path}/users"))
                    .header::<&str, &str>(
                        "Authorization",
                        format!("Bearer {}", token2.to_string()).as_ref(),
                    )
                    .query(&[("id", user2_identity.to_string())])
                    .send()
                    .await
                    .unwrap()
                    .json::<GetUserResponse>()
                    .await
                    .is_ok()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_group_creation() {
        // TODO: Fix group sync issue
        // Init
        let TestServers {
            addr_server_pairs: ref servers,
            ..
        } = init_lmdb_servers(OFFSET).await.unwrap();
        let client = reqwest::Client::new();

        let first_node = servers.iter().next().unwrap();
        let (user_identity1, user_token1) =
            create_user_with_token(first_node, "group_test_user".to_string())
                .await
                .unwrap();

        let (user_identity2, user_token2) =
            create_user_with_token(first_node, "group_test_user_to_add".to_string())
                .await
                .unwrap();

        // Test
        let first_url = first_node.path.clone();
        let request = AddGroupRequest {
            name: "group_test_group".to_string(),
        };

        let response = client
            .post(format!("{first_url}/groups"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token1.to_string()).as_ref(),
            )
            .json(&request)
            .send()
            .await
            .unwrap()
            .json::<AddGroupResponse>()
            .await
            .unwrap();
        let group_id = response.group.id;

        let request = AddUserToGroupRequest {
            group_id,
            user_roles: BTreeMap::from([(user_identity2.to_string(), vec!["reader".to_string()])]),
        };
        let _: AddUserToGroupResponse = client
            .post(format!("{first_url}/groups/user"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token1.to_string()).as_ref(),
            )
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let response: GetGroupResponse = client
            .get(format!("{first_url}/groups"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user_token1.to_string()).as_ref(),
            )
            .query(&[("id".to_string(), group_id.to_string())])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let group = response.group;

        std::thread::sleep(Duration::from_secs(10));

        for Server {
            controller, path, ..
        } in servers.iter()
        {
            let _: GetUserResponse = client
                .get(format!("{path}/users"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user_token1.to_string()).as_ref(),
                )
                .query(&[("id".to_string(), user_identity1.to_string())])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let _: GetUserResponse = client
                .get(format!("{path}/users"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user_token2.to_string()).as_ref(),
                )
                .query(&[("id".to_string(), user_identity2.to_string())])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let user1_groups = controller
                .persistence
                .get_user_groups(&user_identity1)
                .await
                .unwrap();
            let user2_groups = controller
                .persistence
                .get_user_groups(&user_identity2)
                .await
                .unwrap();
            assert_eq!(user1_groups, user2_groups);
            assert!(user1_groups.contains(&group_id));
            assert!(user2_groups.contains(&group_id));
            assert_eq!(
                client
                    .get(format!("{first_url}/groups"))
                    .header::<&str, &str>(
                        "Authorization",
                        format!("Bearer {}", user_token1.to_string()).as_ref(),
                    )
                    .query(&[("id".to_string(), group_id.to_string())])
                    .send()
                    .await
                    .unwrap()
                    .json::<GetGroupResponse>()
                    .await
                    .unwrap()
                    .group,
                group
            )
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create() {
        let TestServers {
            addr_server_pairs: ref servers,
            ..
        } = init_lmdb_servers(OFFSET).await.unwrap();

        let ref first_node @ Server {
            controller,
            path: first_url,
            ..
        } = servers.first().unwrap();
        let client = reqwest::Client::new();

        let (user1_identity, user1_token) =
            create_user_with_token(&first_node, "create_resource_test1".to_string())
                .await
                .unwrap();
        let (user2_identity, user2_token) =
            create_user_with_token(&first_node, "create_resource_test2".to_string())
                .await
                .unwrap();

        let request = AddGroupRequest {
            name: "create_test_group1".to_string(),
        };
        let response = request
            .run_request(user1_identity.clone(), controller)
            .await
            .unwrap();

        let group1 = response.group.id;

        let request = AddGroupRequest {
            name: "create_test_group2".to_string(),
        };
        let response = request
            .run_request(user2_identity.clone(), controller)
            .await
            .unwrap();
        let group2 = response.group.id;

        let create_resource = CreateProjectRequest {
            name: format!("test_resource_from_user1"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id: group1,
            ..Default::default()
        };

        let object1: Resource = client
            .post(format!("{first_url}/resources/project"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user1_token.to_string()).as_ref(),
            )
            .json(&create_resource)
            .send()
            .await
            .unwrap()
            .json::<CreateProjectResponse>()
            .await
            .unwrap()
            .resource;
        let object_id1 = object1.id;

        let create_resource = CreateProjectRequest {
            name: format!("test_resource_from_user2"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id: group2,
            ..Default::default()
        };
        let object2: Resource = client
            .post(format!("{first_url}/resources/project"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user2_token.to_string()).as_ref(),
            )
            .json(&create_resource)
            .send()
            .await
            .unwrap()
            .json::<CreateProjectResponse>()
            .await
            .unwrap()
            .resource;
        let object_id2 = object2.id;

        for Server { path: base_url, .. } in servers.iter() {
            let response: GetResourceResponse = client
                .get(format!("{base_url}/resources"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user1_token.to_string()).as_ref(),
                )
                .query(&[("id".to_string(), object_id1.to_string())])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response.resource, object1);
            let response = client
                .get(format!("{base_url}/resources"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user2_token.to_string()).as_ref(),
                )
                .query(&[("id".to_string(), object_id1.to_string())])
                .send()
                .await
                .unwrap()
                .json::<GetResourceResponse>()
                .await;
            assert!(response.is_err());

            let response: GetResourceResponse = client
                .get(format!("{base_url}/resources"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user2_token.to_string()).as_ref(),
                )
                .query(&[("id".to_string(), object_id2.to_string())])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response.resource, object2);

            let response = client
                .get(format!("{base_url}/resources"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user1_token.to_string()).as_ref(),
                )
                .query(&[("id".to_string(), object_id2.to_string())])
                .send()
                .await
                .unwrap()
                .json::<GetResourceResponse>()
                .await;
            assert!(response.is_err());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search_authorization() {
        let TestServers {
            addr_server_pairs: ref servers,
            ..
        } = init_lmdb_servers(OFFSET).await.unwrap();

        let ref test @ Server {
            controller,
            path: first_url,
            ..
        } = servers.first().unwrap();
        let client = reqwest::Client::new();

        let (user1_identity, user1_token) =
            create_user_with_token(&test, "search_user1".to_string())
                .await
                .unwrap();
        let (user2_identity, user2_token) =
            create_user_with_token(&test, "search_user2".to_string())
                .await
                .unwrap();

        let request = AddGroupRequest {
            name: "search_group1".to_string(),
        };
        let response = request
            .run_request(user1_identity.clone(), controller)
            .await
            .unwrap();

        let group1 = response.group.id;
        let request = AddGroupRequest {
            name: "search_group2".to_string(),
        };
        let response = request
            .run_request(user2_identity.clone(), controller)
            .await
            .unwrap();
        let group2 = response.group.id;

        let create_resource = CreateProjectRequest {
            name: format!("test_search"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id: group1,
            ..Default::default()
        };
        let object1: Resource = client
            .post(format!("{first_url}/resources/project"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user1_token.to_string()).as_ref(),
            )
            .json(&create_resource)
            .send()
            .await
            .unwrap()
            .json::<CreateProjectResponse>()
            .await
            .unwrap()
            .resource;
        let parent_project1 = object1.id;

        let create_resource = CreateProjectRequest {
            name: format!("test_search"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id: group2,
            ..Default::default()
        };
        let object2: Resource = client
            .post(format!("{first_url}/resources/project"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user2_token.to_string()).as_ref(),
            )
            .json(&create_resource)
            .send()
            .await
            .unwrap()
            .json::<CreateProjectResponse>()
            .await
            .unwrap()
            .resource;
        let parent_project2 = object2.id;

        let mut user1_ids = Vec::new();
        user1_ids.push(parent_project1);
        let mut user2_ids = Vec::new();
        user2_ids.push(parent_project2);
        for i in 0..1000 {
            if i > 499 {
                let create_resource = CreateResourceRequest {
                    name: format!("test_search"),
                    visibility: aruna_metadata::models::structs::VisibilityClass::Private,
                    parent_id: parent_project1,
                    ..Default::default()
                };
                let r: Resource = client
                    .post(format!("{first_url}/resources"))
                    .header::<&str, &str>(
                        "Authorization",
                        format!("Bearer {}", user1_token.to_string()).as_ref(),
                    )
                    .json(&create_resource)
                    .send()
                    .await
                    .unwrap()
                    .json::<CreateProjectResponse>()
                    .await
                    .unwrap()
                    .resource;
                user1_ids.push(r.id);
            } else {
                let create_resource = CreateResourceRequest {
                    name: format!("test_search"),
                    visibility: aruna_metadata::models::structs::VisibilityClass::Private,
                    parent_id: parent_project2,
                    ..Default::default()
                };
                let r: Resource = client
                    .post(format!("{first_url}/resources"))
                    .header::<&str, &str>(
                        "Authorization",
                        format!("Bearer {}", user2_token.to_string()).as_ref(),
                    )
                    .json(&create_resource)
                    .send()
                    .await
                    .unwrap()
                    .json::<CreateProjectResponse>()
                    .await
                    .unwrap()
                    .resource;
                user2_ids.push(r.id);
            }
        }

        std::thread::sleep(Duration::from_secs(10));

        for Server { path: base_url, .. } in servers.iter() {
            let response: SearchResponse = client
                .get(format!("{base_url}/info/search"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user1_token.to_string()).as_ref(),
                )
                .query(&[("query", "name:test_search")])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response.resources.len(), 501);
            assert!(response.resources.iter().all(|r| user1_ids.contains(&r.id)));
            let response: SearchResponse = client
                .get(format!("{base_url}/info/search"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user2_token.to_string()).as_ref(),
                )
                .query(&[("query", "name:test_search")])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response.resources.len(), 501);
            assert!(response.resources.iter().all(|r| user2_ids.contains(&r.id)));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search_queries() {
        let TestServers {
            addr_server_pairs: ref servers,
            ..
        } = init_lmdb_servers(OFFSET).await.unwrap();

        let ref test @ Server {
            controller,
            path: first_url,
            ..
        } = servers.first().unwrap();
        let client = reqwest::Client::new();

        let (user_identity, user_token) =
            create_user_with_token(&test, "search_query_user".to_string())
                .await
                .unwrap();
        let request = AddGroupRequest {
            name: "search_query_group".to_string(),
        };
        let response = request
            .run_request(user_identity.clone(), controller)
            .await
            .unwrap();

        let group = response.group.id;

        let create_resource = CreateProjectRequest {
            name: format!("project_test"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id: group,
            ..Default::default()
        };
        let object: Resource = client
            .post(format!("{first_url}/resources/project"))
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
        let parent_project = object.id;

        for i in 0..10 {
            let create_resource = if i == 2 {
                CreateResourceRequest {
                    name: format!("test_search_{i}"),
                    visibility: aruna_metadata::models::structs::VisibilityClass::Private,
                    parent_id: parent_project,
                    description: "A unique description for a unique object".to_string(),
                    ..Default::default()
                }
            } else {
                CreateResourceRequest {
                    name: format!("test_search_{i}"),
                    visibility: aruna_metadata::models::structs::VisibilityClass::Private,
                    parent_id: parent_project,
                    ..Default::default()
                }
            };
            let _: Resource = client
                .post(format!("{first_url}/resources"))
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
        }

        std::thread::sleep(Duration::from_secs(10));

        for Server { path: base_url, .. } in servers.iter() {
            let response: SearchResponse = client
                .get(format!("{base_url}/info/search"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user_token.to_string()).as_ref(),
                )
                .query(&[("query", "name:.*search.*")])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response.resources.len(), 10);

            let response: SearchResponse = client
                .get(format!("{base_url}/info/search"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user_token.to_string()).as_ref(),
                )
                .query(&[("query", "name:.*1")])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response.resources.len(), 1);
            assert_eq!(
                response.resources.first().unwrap().name,
                "test_search_1".to_string()
            );

            let response: SearchResponse = client
                .get(format!("{base_url}/info/search"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user_token.to_string()).as_ref(),
                )
                .query(&[("query", "description:unique")])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response.resources.len(), 1);
            assert_eq!(
                response.resources.first().unwrap().name,
                "test_search_2".to_string()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_resource_updates() {
        let realm_key = SigningKey::generate(&mut OsRng);
        let ref node @ Server {
            ref controller,
            ref path,
            ..
        } = init_server(realm_key.clone(), OFFSET, None, false)
            .await
            .unwrap();

        let client = reqwest::Client::new();

        let (user1_identity, user1_token) =
            create_user_with_token(&node, "create_resource_test1".to_string())
                .await
                .unwrap();
        let (user2_identity, user2_token) =
            create_user_with_token(&node, "create_resource_test2".to_string())
                .await
                .unwrap();

        let request = AddGroupRequest {
            name: "create_test_group1".to_string(),
        };
        let response = request
            .run_request(user1_identity.clone(), &controller)
            .await
            .unwrap();

        let group_id = response.group.id;

        let request = AddUserToGroupRequest {
            group_id,
            user_roles: BTreeMap::from([(user2_identity.to_string(), vec!["member".to_string()])]),
        };
        request
            .run_request(user1_identity.clone(), &controller)
            .await
            .unwrap();

        let create_resource = CreateProjectRequest {
            name: format!("test_resource_from_user1"),
            visibility: aruna_metadata::models::structs::VisibilityClass::Private,
            group_id,
            ..Default::default()
        };

        let project: Resource = client
            .post(format!("{path}/resources/project"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user1_token.to_string()).as_ref(),
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

        let update_resource_name = UpdateResourceNameRequest {
            id: project_id,
            name: "a new project name".to_string(),
        };

        let update_resource_title = UpdateResourceTitleRequest {
            id: project_id,
            title: "A new title".to_string(),
        };

        let update_resource_description = UpdateResourceDescriptionRequest {
            id: project_id,
            description: "A new description".to_string(),
        };

        let update_resource_visibility = UpdateResourceVisibilityRequest {
            id: project_id,
            visibility: aruna_metadata::models::structs::VisibilityClass::Public,
        };

        let update_resource_license = UpdateResourceLicenseRequest {
            id: project_id,
            license_id: Ulid::new(),
        };

        let update_resource_labels = UpdateResourceLabelsRequest {
            id: project_id,
            labels_to_add: vec![KeyValue {
                key: "a new key".to_string(),
                value: "a new value".to_string(),
            }],
            labels_to_remove: vec![],
        };

        let update_resource_ids = UpdateResourceIdentifiersRequest {
            id: project_id,
            ids_to_add: vec!["https://a.doi.org/1203971450".to_string()],
            ids_to_remove: vec![],
        };

        let update_resource_authors = UpdateResourceAuthorsRequest {
            id: project_id,
            authors_to_add: vec![Author {
                first: "Jane".to_string(),
                last: "Doe".to_string(),
                id: "https://www.albumoftheyear.org/album/4323-converge-jane-doe.php".to_string(),
            }],
            authors_to_remove: vec![],
        };

        let update_resource_data = UpdateResourceDataRequest {
            id: project_id,
            data_to_add: vec![Data::Link("https://this.is.a.link.to.data.org".to_string())],
            data_to_remove: vec![],
        };

        let requests = vec![
            ResourceUpdateRequests::Name(update_resource_name),
            ResourceUpdateRequests::Title(update_resource_title),
            ResourceUpdateRequests::Description(update_resource_description),
            ResourceUpdateRequests::Visibility(update_resource_visibility),
            ResourceUpdateRequests::License(update_resource_license),
            ResourceUpdateRequests::Labels(update_resource_labels),
            ResourceUpdateRequests::Identifiers(update_resource_ids),
            ResourceUpdateRequests::Authors(update_resource_authors),
            ResourceUpdateRequests::Data(update_resource_data),
        ];

        for (idx, request) in requests.into_iter().enumerate() {
            if idx % 2 == 0 {
                controller
                    .request(request, Some(user1_token.clone()))
                    .await
                    .unwrap();
            } else {
                controller
                    .request(request, Some(user2_token.clone()))
                    .await
                    .unwrap();
            }
        }

        let response: GetResourceHistoryResponse = client
            .get(format!("{path}/resources/history"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user1_token.to_string()).as_ref(),
            )
            .query(&[("id".to_string(), project_id.to_string())])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.history.len(), 10);
    }
}
