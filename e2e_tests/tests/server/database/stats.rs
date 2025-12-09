use std::str::FromStr;
use std::time::Duration;

use aruna_server::database::dsls::internal_relation_dsl::InternalRelation;
use aruna_server::database::dsls::stats_dsl::{get_last_refresh, ObjectStats};
use aruna_server::database::{
    dsls::{object_dsl::Object, stats_dsl::refresh_stats_view as refresh_stats},
    enums::ObjectType,
};
use chrono::Utc;
use diesel_ulid::DieselUlid;

use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::test_utils::USER1_ULID;
use crate::common::{infra, test_utils};

#[tokio::test]
async fn general_object_stats_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do stats stuff on database
    let client = db.get_client().await.unwrap();

    // Save timestamp for evaluation at the end
    let timestamp = Utc::now().timestamp_millis();

    // Create dummy hierarchy
    let random_user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let project =
        test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let collection = test_utils::new_object(
        random_user_id,
        DieselUlid::generate(),
        ObjectType::COLLECTION,
    );
    let object_1 =
        test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::OBJECT);
    let object_2 =
        test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::OBJECT);

    let proj_obj = test_utils::new_internal_relation(&project, &object_1);
    let proj_coll = test_utils::new_internal_relation(&project, &collection);
    let coll_obj2 = test_utils::new_internal_relation(&collection, &object_2);

    Object::batch_create(
        &vec![
            project.clone(),
            collection.clone(),
            object_1.clone(),
            object_2.clone(),
        ],
        &client,
    )
    .await
    .unwrap();

    InternalRelation::batch_create(&vec![proj_obj, proj_coll, coll_obj2], &client)
        .await
        .unwrap();

    // Assert that refresh can be started
    // Needs the loop as the first Materialized View refresh, which also creates the table,
    // can fail with a "Restarting a DDL transaction not supported" error.
    while refresh_stats(&client).await.is_err() {
        // Test will timeout if refresh start fails long enough
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Assert Project exists in stats table after refresh
    while ObjectStats::get_object_stats(&project.id, &client)
        .await
        .unwrap()
        .size
        == 0
    {
        // Will timeout if nothing happens ...
    }

    let project_stats = ObjectStats::get_object_stats(&project.id, &client)
        .await
        .unwrap();
    assert_eq!(project_stats.count, 2);
    assert_eq!(
        project_stats.size,
        object_1.content_len + object_2.content_len
    );

    let collection_stats = ObjectStats::get_object_stats(&collection.id, &client)
        .await
        .unwrap();
    assert_eq!(collection_stats.count, 1);
    assert_eq!(collection_stats.size, object_2.content_len);

    // Assert that get_all_stats().len() > 0
    let all_stats = ObjectStats::get_all_stats(&client).await.unwrap();
    assert!(!all_stats.is_empty());

    // Assert that last timestamp is greater than timestamp at the start of the test
    let last_timestamp = get_last_refresh(&client)
        .await
        .unwrap()
        .and_utc()
        .timestamp_millis();
    assert!(last_timestamp > timestamp)
}
