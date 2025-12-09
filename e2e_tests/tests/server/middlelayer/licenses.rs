use crate::common::{init, init_server};
use aruna_rust_api::api::storage::services::v2::CreateLicenseRequest;
use aruna_server::database::dsls::license_dsl::License;
use itertools::Itertools;

#[tokio::test]
async fn create_and_read_license() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    // Create & Get License
    let request = CreateLicenseRequest {
        tag: "middlelayer_license_test".to_string(),
        name: "middlelayer license test".to_string(),
        text: "Tests create license function in middlelayer".to_string(),
        url: "test.org/middelayer-test-license".to_string(),
    };
    let tag = db_handler.create_license(request.clone()).await.unwrap();
    assert_eq!(tag, request.tag);
    let license = db_handler.get_license(tag).await.unwrap();
    assert_eq!(license.tag, request.tag);
    assert_eq!(license.name, request.name);
    assert_eq!(license.text, request.text);
    assert_eq!(license.url, request.url);

    // List Licenses
    let dummy_one_req = CreateLicenseRequest {
        tag: "dummy_one".to_string(),
        name: "middlelayer license test".to_string(),
        text: "Tests create license function in middlelayer".to_string(),
        url: "test.org/middelayer-test-license".to_string(),
    };
    let dummy_two_req = CreateLicenseRequest {
        tag: "dummy_two".to_string(),
        name: "middlelayer license test".to_string(),
        text: "Tests create license function in middlelayer".to_string(),
        url: "test.org/middelayer-test-license".to_string(),
    };
    db_handler
        .create_license(dummy_one_req.clone())
        .await
        .unwrap();
    db_handler
        .create_license(dummy_two_req.clone())
        .await
        .unwrap();
    let all = db_handler.list_licenses().await.unwrap();
    let dummy_one: License = dummy_one_req.into();
    let dummy_two: License = dummy_two_req.into();
    assert!(all.iter().contains(&dummy_one));
    assert!(all.iter().contains(&dummy_two));
}
