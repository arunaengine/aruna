pub mod commons;

#[cfg(test)]
mod tests {
    use crate::commons::{TestServers, create_user_with_token, init_lmdb_servers};
    use aruna_metadata::{
        models::{
            models::Resource,
            requests::{
                AddGroupRequest, CreateProjectRequest, CreateProjectResponse,
                CreateResourceRequest, GetResourceResponse, SearchResponse,
            },
        },
        network::network_trait::Network,
        transactions::request::Request,
    };
    use std::time::Duration;
    const OFFSET: u16 = 0;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_user_creation() {
        let ref test @ TestServers {
            addr_server_pairs: ref servers,
            ..
        } = init_lmdb_servers(OFFSET).await.unwrap();

        let (user1_identity, _) = create_user_with_token(&test, "create_user_test1".to_string())
            .await
            .unwrap();
        let (user2_identity, _) = create_user_with_token(&test, "create_user_test2".to_string())
            .await
            .unwrap();

        std::thread::sleep(Duration::from_secs(5));

        for (controller, _) in servers.iter() {
            assert!(
                controller
                    .persistence
                    .get_user(&user1_identity)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                controller
                    .persistence
                    .get_user(&user2_identity)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create() {
        let ref test @ TestServers {
            addr_server_pairs: ref servers,
            ..
        } = init_lmdb_servers(OFFSET).await.unwrap();

        let (controller, first_url) = servers.first().unwrap();
        println!("{first_url}");
        println!("{:?}", controller.network.get_addr().await.unwrap());
        println!(
            "{:?}",
            servers.iter().map(|(_, url)| url).collect::<Vec<&String>>()
        );
        let client = reqwest::Client::new();

        let (user1_identity, user1_token) =
            create_user_with_token(&test, "create_resource_test1".to_string())
                .await
                .unwrap();
        let (user2_identity, user2_token) =
            create_user_with_token(&test, "create_resource_test2".to_string())
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
            name: "gcreate_test_group2".to_string(),
        };
        let response = request
            .run_request(user2_identity.clone(), controller)
            .await
            .unwrap();
        let group2 = response.group.id;

        let create_resource = CreateProjectRequest {
            name: format!("test_resource_from_user1"),
            visibility: aruna_metadata::models::models::VisibilityClass::Private,
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
            visibility: aruna_metadata::models::models::VisibilityClass::Private,
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

        std::thread::sleep(Duration::from_secs(5));

        for (_, base_url) in servers.iter() {
            println!("{base_url}");
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
    async fn test_search() {
        let ref test @ TestServers {
            addr_server_pairs: ref servers,
            ..
        } = init_lmdb_servers(OFFSET).await.unwrap();

        let (controller, first_url) = servers.first().unwrap();
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
            visibility: aruna_metadata::models::models::VisibilityClass::Private,
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
            visibility: aruna_metadata::models::models::VisibilityClass::Private,
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

        std::thread::sleep(Duration::from_secs(5));

        let mut user1_ids = Vec::new();
        user1_ids.push(parent_project1);
        let mut user2_ids = Vec::new();
        user2_ids.push(parent_project2);
        for i in 0..1000 {
            if i > 499 {
                let create_resource = CreateResourceRequest {
                    name: format!("test_search"),
                    visibility: aruna_metadata::models::models::VisibilityClass::Private,
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
                    visibility: aruna_metadata::models::models::VisibilityClass::Private,
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

        std::thread::sleep(Duration::from_secs(20));

        for (_, base_url) in servers.iter() {
            println!("{base_url}");
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
}
