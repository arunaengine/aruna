pub mod commons;

#[cfg(test)]
mod api_tests {
    use crate::commons::{Server, TestServers, create_user_with_token, init_lmdb_servers};
    use aruna_metadata::{
        models::{
            requests::{
                AddGroupRequest, AddGroupResponse, AddUserToGroupRequest, AddUserToGroupResponse,
                CreateProjectRequest, CreateProjectResponse, CreateResourceRequest,
                GetGroupResponse, GetResourceResponse, GetUserResponse, Request, SearchResponse,
            },
            structs::Resource,
        },
        network::network_trait::Network,
    };
    use std::{collections::BTreeMap, time::Duration};
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

        for Server {
            path, ..
        } in servers.iter()
        {
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
                    .await.is_ok()
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
                    .await.is_ok()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_group_creation() {
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
}
