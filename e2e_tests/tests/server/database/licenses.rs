use aruna_server::database::{crud::CrudDb, dsls::license_dsl::License};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;

use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::test_utils::USER1_ULID;
use crate::common::{infra, test_utils};

#[tokio::test]
async fn create_and_get_licenses() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do licenses stuff on database
    let client = db.get_client().await.unwrap();

    // Basic Create and Get
    let mut license: License = License {
        tag: "test_license".to_string(),
        name: "test license".to_string(),
        text: "this is a test license".to_string(),
        url: "test.org/test-license".to_string(),
    };
    license.create(&client).await.unwrap();

    let get_license = License::get(license.tag.clone(), &client)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(license, get_license);

    // Licenses must have unique tags!
    let mut err_license: License = License {
        tag: "test_license".to_string(),
        name: "error test license".to_string(),
        text: "this is a test license that cannot be created".to_string(),
        url: "test.org/error-test-license".to_string(),
    };
    assert!(err_license.create(&client).await.is_err());

    // This should work because different tag, rest stays the same
    let mut ok_license: License = License {
        tag: "ok_test_license".to_string(),
        name: "test license".to_string(),
        text: "this is a test license".to_string(),
        url: "test.org/test-license".to_string(),
    };
    assert!(ok_license.create(&client).await.is_ok());

    let all = License::all(&client).await.unwrap();

    assert!(all.iter().contains(&get_license));
    assert!(all.iter().contains(&ok_license));
    assert!(!all.iter().contains(&err_license));
}

#[tokio::test]
async fn objects_and_licenses() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do licenses stuff on database
    let client = db.get_client().await.unwrap();

    // Test if object creation fails when license is not valid
    let mut license: License = License {
        tag: "another_test_license".to_string(),
        name: "another test license".to_string(),
        text: "this is another test license".to_string(),
        url: "test.org/another_test_license".to_string(),
    };
    let object_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let mut object = test_utils::new_object(
        user_id,
        object_id,
        aruna_server::database::enums::ObjectType::PROJECT,
    );
    object.data_license.clone_from(&license.tag);
    object.metadata_license.clone_from(&license.tag);

    assert!(object.create(&client).await.is_err());
    // Test if it works after creating the license
    license.create(&client).await.unwrap();
    assert!(object.create(&client).await.is_ok());
}
