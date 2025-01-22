mod common;

#[cfg(test)]
mod update_tests {

    use aruna_rust_api::v3::aruna::api::v3::{
        Author, CreateProjectRequest, CreateRealmRequest, CreateResourceRequest, KeyValue, Realm,
    };
    use aruna_server::models::models::{Author as ModelAuthor, KeyValue as ModelKeyValue};
    use aruna_server::models::requests::{
        AddUserResponse, UpdateResourceAuthorsRequest, UpdateResourceAuthorsResponse,
        UpdateResourceDescriptionRequest, UpdateResourceDescriptionResponse,
        UpdateResourceIdentifiersRequest, UpdateResourceIdentifiersResponse,
        UpdateResourceLabelsRequest, UpdateResourceLabelsResponse, UpdateResourceLicenseRequest,
        UpdateResourceLicenseResponse, UpdateResourceNameRequest, UpdateResourceNameResponse,
        UpdateResourceTitleRequest, UpdateResourceTitleResponse, UpdateResourceVisibilityRequest,
        UpdateResourceVisibilityResponse,
    };
    use ulid::Ulid;

    use crate::common::{init_test, ADMIN_TOKEN};
    pub const OFFSET: u16 = 200;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_updates() {
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
        let _project_id = Ulid::from_string(&parent_id).unwrap();

        // Create resource
        let request = CreateResourceRequest {
            name: "TestResource".to_string(),
            parent_id,
            visibility: 1,
            variant: 2,
            title: "TitleTest".to_string(),
            description: "DescriptionTest".to_string(),
            labels: vec![
                KeyValue {
                    key: "test".to_string(),
                    value: "value".to_string(),
                    locked: false,
                },
                KeyValue {
                    key: "another_test".to_string(),
                    value: "another_value".to_string(),
                    locked: false,
                },
            ],
            identifiers: vec!["ORCID".to_string(), "WHATEVER".to_string()],
            authors: vec![
                Author {
                    id: "01JDVSKJPCMEBWZNG8PYKZEBFQ".to_string(),
                    first_name: "Jane".to_string(),
                    last_name: "Doe".to_string(),
                    email: "converge@test.org".to_string(),
                    orcid: "THIS_IS_AN_ORCID".to_string(),
                },
                Author {
                    id: "01JDVSKTFKR4J1DZQT11231R6Q".to_string(),
                    first_name: "John".to_string(),
                    last_name: "Doe".to_string(),
                    email: "test@test.org".to_string(),
                    orcid: "THIS_IS_ANOTHER_ORCID".to_string(),
                },
            ],
            license_tag: "CC0".to_string(),
        };
        let resource = clients
            .resource_client
            .create_resource(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap();
        let resource_id = Ulid::from_string(&resource.id).unwrap();
        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources/name", clients.rest_endpoint);
        let update_name = UpdateResourceNameRequest {
            id: resource_id,
            name: "ReleaseName".to_string(),
        };
        let response: UpdateResourceNameResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_name)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resource.name, update_name.name);

        let url = format!("{}/api/v3/resources/title", clients.rest_endpoint);
        let update_title = UpdateResourceTitleRequest {
            id: resource_id,
            title: "ReleaseTitle".to_string(),
        };
        let response: UpdateResourceTitleResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_title)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resource.title, update_title.title);

        let url = format!("{}/api/v3/resources/description", clients.rest_endpoint);
        let update_desc = UpdateResourceDescriptionRequest {
            id: resource_id,
            description: "ReleaseDescription".to_string(),
        };
        let response: UpdateResourceDescriptionResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_desc)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resource.description, update_desc.description);

        let url = format!("{}/api/v3/resources/visibility", clients.rest_endpoint);
        let update_visi = UpdateResourceVisibilityRequest {
            id: resource_id,
            visibility: aruna_server::models::models::VisibilityClass::Public,
        };
        let response: UpdateResourceVisibilityResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_visi)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resource.visibility, update_visi.visibility);

        let url = format!("{}/api/v3/resources/license", clients.rest_endpoint);
        let update_license = UpdateResourceLicenseRequest {
            id: resource_id,
            license_id: Ulid::new(),
        };
        let response: UpdateResourceLicenseResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_license)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resource.license_id, update_license.license_id);

        let url = format!("{}/api/v3/resources/labels", clients.rest_endpoint);
        let update_labels = UpdateResourceLabelsRequest {
            id: resource_id,
            labels_to_add: vec![ModelKeyValue {
                key: "NewKey".to_string(),
                value: "NewValue".to_string(),
                locked: false,
            }],
            labels_to_remove: vec![ModelKeyValue {
                key: "another_test".to_string(),
                value: "another_value".to_string(),
                locked: false,
            }],
        };
        let response: UpdateResourceLabelsResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_labels)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response
            .resource
            .labels
            .contains(&update_labels.labels_to_add[0]));
        assert!(!response
            .resource
            .labels
            .contains(&update_labels.labels_to_remove[0]));

        let url = format!("{}/api/v3/resources/identifiers", clients.rest_endpoint);
        let update_ids = UpdateResourceIdentifiersRequest {
            id: resource_id,
            ids_to_add: vec!["NewId".to_string()],
            ids_to_remove: vec!["WHATEVER".to_string()],
        };
        let response: UpdateResourceIdentifiersResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_ids)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response
            .resource
            .identifiers
            .contains(&update_ids.ids_to_add[0]));
        assert!(!response
            .resource
            .identifiers
            .contains(&update_ids.ids_to_remove[0]));

        let url = format!("{}/api/v3/resources/authors", clients.rest_endpoint);
        let update_authors = UpdateResourceAuthorsRequest {
            id: resource_id,
            authors_to_add: vec![ModelAuthor {
                id: Ulid::new(),
                first_name: "Herribert".to_string(),
                last_name: "Harribert".to_string(),
                email: "herri@harri.bert".to_string(),
                identifier: "AnotherId".to_string(),
            }],
            authors_to_remove: vec![ModelAuthor {
                id: Ulid::from_string("01JDVSKTFKR4J1DZQT11231R6Q").unwrap(),
                first_name: "John".to_string(),
                last_name: "Doe".to_string(),
                email: "test@test.org".to_string(),
                identifier: "THIS_IS_ANOTHER_ORCID".to_string(),
            }],
        };
        let response: UpdateResourceAuthorsResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_authors)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response
            .resource
            .authors
            .contains(&update_authors.authors_to_add[0]));
        assert!(!response
            .resource
            .authors
            .contains(&update_authors.authors_to_remove[0]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_user() {
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
        let group_id = Ulid::from_string(&response.admin_group_id).unwrap();

        let url = format!(
            "{}/api/v3/groups/{}/user/01JER6Q2MEX5SS7GQCSSDFJJVG",
            clients.rest_endpoint, group_id
        );
        println!("{url}");

        let client = reqwest::Client::new();
        let _: AddUserResponse = client
            .patch(url)
            .query(&[("permission", "Read")])
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    }
}
