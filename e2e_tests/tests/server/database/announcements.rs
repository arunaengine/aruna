use aruna_rust_api::api::storage::models::v2::PageRequest;
use aruna_server::database::{crud::CrudDb, dsls::info_dsl::Announcement};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use rand::seq::IndexedRandom;
use tokio_postgres::GenericClient;

use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::{infra, test_utils};

#[tokio::test]
async fn create_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do announcements stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    // Invalid announcement type
    let mut ann = Announcement {
        id: DieselUlid::generate(),
        announcement_type: "INVALID".to_string(),
        title: "Announcement Title".to_string(),
        teaser: "Announcement Teaser".to_string(),
        image_url: "https://announcement_image_url/dummy.webp".to_string(),
        content: "Announcement Content".to_string(),
        created_by: "The Aruna Team".to_string(),
        created_at: chrono::Utc::now().naive_local(),
        modified_by: "The Aruna Team".to_string(),
        modified_at: chrono::Utc::now().naive_local(),
    };
    let res = ann.create(client).await;

    assert!(res.is_err());

    ann.announcement_type = "RELEASE".to_string();
    ann.create(client).await.unwrap();

    assert_eq!(
        ann,
        Announcement::get(ann.id, client).await.unwrap().unwrap()
    )
}

#[tokio::test]
async fn get_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do announcements stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let mut types = vec![
        "ORGA".to_string(),
        "ORGA".to_string(),
        "RELEASE".to_string(),
        "RELEASE".to_string(),
        "MAINTENANCE".to_string(),
        "MAINTENANCE".to_string(),
        "UPDATE".to_string(),
        "UPDATE".to_string(),
        "BLOG".to_string(),
        "BLOG".to_string(),
    ];

    let mut announcements = vec![];
    while let Some(a_type) = test_utils::choose_and_remove(&mut types) {
        announcements.push(
            Announcement {
                id: DieselUlid::generate(),
                announcement_type: a_type.to_string(),
                title: format!("Announcement Title {}", announcements.len() + 1),
                teaser: format!("Announcement Teaser {}", announcements.len() + 1),
                image_url: format!(
                    "https://announcement_image_url/{}.webp",
                    announcements.len() + 1
                ),
                content: format!("Announcement Content {}", announcements.len() + 1),
                created_by: "The Aruna Team".to_string(),
                created_at: chrono::Utc::now().naive_local(),
                modified_by: "The Aruna Team".to_string(),
                modified_at: chrono::Utc::now().naive_local(),
            }
            .upsert(client)
            .await
            .unwrap(),
        );
        std::thread::sleep(std::time::Duration::from_millis(10))
    }

    // Try get announcement with non existent id
    assert!(Announcement::get(DieselUlid::generate(), client)
        .await
        .unwrap()
        .is_none());

    // Get any from the created announcements
    let get_announcement = Announcement::get(announcements.first().unwrap().id, client)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_announcement, *announcements.first().unwrap());

    // Get all announcements (paginated)
    let all = Announcement::all(client).await.unwrap();
    for a in &announcements {
        assert!(all.contains(a))
    }

    let mut page = PageRequest {
        start_after: "".to_string(),
        page_size: 2,
    };
    let page_01 = Announcement::all_paginated(client, Some(page.clone()))
        .await
        .unwrap();
    let page_01_positions = page_01
        .iter()
        .map(|page_a| all.iter().position(|a| page_a == a).unwrap())
        .collect_vec();

    assert_eq!(page_01.len(), 2);
    assert!(page_01.iter().all(|a| all.contains(a)));

    page.start_after = page_01.last().unwrap().id.to_string();
    let page_02 = Announcement::all_paginated(client, Some(page.clone()))
        .await
        .unwrap();
    let page_02_positions = page_02
        .iter()
        .map(|page_a| all.iter().position(|a| page_a == a).unwrap())
        .collect_vec();

    assert_eq!(page_02.len(), 2);
    assert!(page_02.iter().all(|a| all.contains(a)));
    assert_eq!(
        page_01_positions
            .iter()
            .zip(&page_02_positions)
            .filter(|&(a, b)| a < b)
            .count(),
        2
    );

    // Get announcements filtered by id
    let ids = announcements
        .choose_multiple(&mut rand::rng(), 2)
        .map(|a| a.id)
        .collect_vec();
    let filtered_anns = Announcement::get_by_ids(client, &ids, None).await.unwrap();

    assert_eq!(filtered_anns.len(), 2);
    for a in &filtered_anns {
        assert!(announcements.contains(a))
    }

    // Get announcements by type (paginated)
    let mut all_typed = vec![];
    for ann in all {
        if &ann.announcement_type == "BLOG" {
            all_typed.push(ann)
        }
    }

    let all_typed_get = Announcement::get_by_type(client, "BLOG".to_string(), None)
        .await
        .unwrap();

    for type_ann in all_typed_get {
        assert!(all_typed.contains(&type_ann))
    }

    page.start_after = "".to_string();
    page.page_size = 1;
    let get_typed_page_01 =
        Announcement::get_by_type(client, "BLOG".to_string(), Some(page.clone()))
            .await
            .unwrap();

    assert_eq!(get_typed_page_01.len(), 1);
    assert!(all_typed.contains(get_typed_page_01.first().unwrap()));

    page.start_after = get_typed_page_01.last().unwrap().id.to_string();
    let get_typed_page_02 =
        Announcement::get_by_type(client, "BLOG".to_string(), Some(page.clone()))
            .await
            .unwrap();

    assert_eq!(get_typed_page_02.len(), 1);
    assert!(
        all_typed
            .iter()
            .position(|a| a == get_typed_page_01.first().unwrap())
            .unwrap()
            < all_typed
                .iter()
                .position(|a| a == get_typed_page_02.first().unwrap())
                .unwrap()
    );
}

#[tokio::test]
async fn upsert_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do announcements stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    // Invalid announcement type
    let mut ann = Announcement {
        id: DieselUlid::generate(),
        announcement_type: "ORGA".to_string(),
        title: "Original Announcement Title".to_string(),
        teaser: "Original Announcement Teaser".to_string(),
        image_url: "https://announcement_image_url/example.webp".to_string(),
        content: "Original Announcement Content".to_string(),
        created_by: "The Aruna Team".to_string(),
        created_at: chrono::Utc::now().naive_local(),
        modified_by: "The Aruna Team".to_string(),
        modified_at: chrono::Utc::now().naive_local(),
    };
    ann.create(client).await.unwrap();

    // Try to set invalid and valid announcement type
    ann.announcement_type = "INVALID".to_string();
    assert!(ann.create(client).await.is_err());
    ann.announcement_type = "BLOG".to_string();

    let type_updated = ann.upsert(client).await.unwrap();

    assert_eq!(ann.id, type_updated.id);
    assert_eq!(&type_updated.announcement_type, "BLOG");

    // Update announcement title
    ann.title = "Updated Announcement Title".to_string();
    let title_updated = ann.upsert(client).await.unwrap();

    assert_eq!(ann.id, title_updated.id);
    assert_eq!(&title_updated.title, "Updated Announcement Title");

    // Update announcement teaser and content together
    ann.teaser = "Updated Announcement Teaser".to_string();
    ann.content = "Updated Announcement Content".to_string();

    let teaser_content_updated = ann.upsert(client).await.unwrap();

    assert_eq!(ann.id, teaser_content_updated.id);
    assert_eq!(
        &teaser_content_updated.teaser,
        "Updated Announcement Teaser"
    );
    assert_eq!(
        &teaser_content_updated.content,
        "Updated Announcement Content"
    );
}

#[tokio::test]
async fn delete_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do announcements stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    // Create some dummy announcements
    let ann_futures = (0..5)
        .map(|_| async {
            Announcement {
                id: DieselUlid::generate(),
                announcement_type: "ORGA".to_string(),
                title: "Announcement Title".to_string(),
                teaser: "Announcement Teaser".to_string(),
                image_url: "https://announcement_image_url/{}.webp".to_string(),
                content: "Announcement Content {}".to_string(),
                created_by: "The Aruna Team".to_string(),
                created_at: chrono::Utc::now().naive_local(),
                modified_by: "The Aruna Team".to_string(),
                modified_at: chrono::Utc::now().naive_local(),
            }
            .upsert(client)
            .await
            .unwrap()
        })
        .collect_vec();
    let announcements = futures::future::join_all(ann_futures).await;

    // Delete the first three of the announcements
    Announcement::batch_delete(
        client,
        &announcements[..3].iter().map(|a| a.id).collect_vec(),
    )
    .await
    .unwrap();

    for a in &announcements[..3] {
        assert!(Announcement::get(a.id, client).await.unwrap().is_none())
    }
    for a in &announcements[3..] {
        assert!(Announcement::get(a.id, client).await.unwrap().is_some())
    }
}
