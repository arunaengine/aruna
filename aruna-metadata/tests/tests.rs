pub mod commons;

#[cfg(test)]
mod tests {
    use crate::commons::{TestServers, create_user_with_token, init_lmdb_servers};
    use aruna_metadata::{
        models::{
            models::Resource,
            requests::{
                AddGroupRequest, CreateProjectRequest, CreateProjectResponse,
                GetResourceResponse,
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

        let (user1_identity, _) = create_user_with_token(&test, "bench_user1".to_string())
            .await
            .unwrap();
        let user1 = user1_identity.user_ulid;
        let (user2_identity, _) = create_user_with_token(&test, "bench_user2".to_string())
            .await
            .unwrap();
        let user2 = user2_identity.user_ulid;

        for (controller, _) in servers.iter() {
            println!("{}", controller.network.get_addr().await.unwrap().node_id);
            assert!(
                controller
                    .persistence
                    .get_user(&user1)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                controller
                    .persistence
                    .get_user(&user2)
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
        let client = reqwest::Client::new();

        let (user1_identity, user1_token) =
            create_user_with_token(&test, "bench_user1".to_string())
                .await
                .unwrap();
        let (user2_identity, user2_token) =
            create_user_with_token(&test, "bench_user2".to_string())
                .await
                .unwrap();

        std::thread::sleep(Duration::from_secs(5));

        let request = AddGroupRequest {
            name: "group1".to_string(),
        };
        let response = request
            .run_request(user1_identity.clone(), controller)
            .await
            .unwrap();
        let group1 = response.group.id;
        let request = AddGroupRequest {
            name: "group1".to_string(),
        };
        let response = request
            .run_request(user2_identity.clone(), controller)
            .await
            .unwrap();
        let group2 = response.group.id;

        println!(
            "
User1 ID:       {}
User1 GROUP:    {}
User2 ID:       {}
User2 GROUP:    {}
",
            user1_identity.user_ulid.to_string(),
            group1.to_string(),
            user2_identity.user_ulid.to_string(),
            group2.to_string()
        );


        std::thread::sleep(Duration::from_secs(5));

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

        std::thread::sleep(Duration::from_secs(10));

        for (_, base_url) in servers.iter() {
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
            println!(
                "Response :
{:?}",
                response
            );
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
            println!(
                "Response :
{:?}",
                response
            );
            assert!(response.is_err());
        }
    }
}
