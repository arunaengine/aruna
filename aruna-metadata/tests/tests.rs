pub mod commons;

#[cfg(test)]
mod tests {
    use crate::commons::init_lmdb_servers;
    use aruna_metadata::models::{
        models::Resource,
        requests::{
            AddUserRequest, AddUserResponse, CreateResourceRequest, CreateResourceResponse,
            GetResourceResponse,
        },
    };
    const OFFSET: u16 = 0;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create() {

        let servers = init_lmdb_servers(OFFSET).await.unwrap();
        println!("Init servers");

        let (_, first_url) = servers.first().unwrap();

        let client = reqwest::Client::new();

        let request = AddUserRequest {
            name: "bench_user1".to_string(),
        };
        let response: AddUserResponse = client
            .post(format!("{first_url}/users"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let user1 = response.user.id;
        println!("{}", user1.to_string());

        let request = AddUserRequest {
            name: "bench_user2".to_string(),
        };

        let response: AddUserResponse = client
            .post(format!("{first_url}/users"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let user2 = response.user.id;
        println!("{}", user2.to_string());

        for (controller, _) in servers.iter() {
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

        let create_resource = CreateResourceRequest {
            name: format!("test_resource_from_user1"),
            visibility: aruna_metadata::models::models::VisibilityClass::Private,
            ..Default::default()
        };
        let object1: Resource = client
            .post(format!("{first_url}/resources"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user1.to_string()).as_ref(),
            )
            .json(&create_resource)
            .send()
            .await
            .unwrap()
            .json::<CreateResourceResponse>()
            .await
            .unwrap()
            .resource;
        let object_id1 = object1.id;

        let create_resource = CreateResourceRequest {
            name: format!("test_resource_from_user2"),
            visibility: aruna_metadata::models::models::VisibilityClass::Private,
            ..Default::default()
        };
        let object2: Resource = client
            .post(format!("{first_url}/resources"))
            .header::<&str, &str>(
                "Authorization",
                format!("Bearer {}", user2.to_string()).as_ref(),
            )
            .json(&create_resource)
            .send()
            .await
            .unwrap()
            .json::<CreateResourceResponse>()
            .await
            .unwrap()
            .resource;
        let object_id2 = object2.id;

        for (_, base_url) in servers.iter() {
            let response: GetResourceResponse = client
                .get(format!("{base_url}/resources"))
                .header::<&str, &str>(
                    "Authorization",
                    format!("Bearer {}", user1.to_string()).as_ref(),
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
                    format!("Bearer {}", user2.to_string()).as_ref(),
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
                    format!("Bearer {}", user2.to_string()).as_ref(),
                )
                .query(&[("id".to_string(), object_id2.to_string())])
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
                    format!("Bearer {}", user1.to_string()).as_ref(),
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
