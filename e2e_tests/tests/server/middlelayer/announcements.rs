use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{AnnouncementType, PageRequest},
    services::v2::{
        Announcement as ProtoAnnouncement, GetAnnouncementsByTypeRequest, GetAnnouncementsRequest,
        SetAnnouncementsRequest,
    },
};
use aruna_server::database::{crud::CrudDb, dsls::info_dsl::Announcement};
use diesel_ulid::DieselUlid;
use itertools::{enumerate, Itertools};

use crate::common::{
    init, init_server,
    test_utils::{self, choose_and_remove},
};

#[tokio::test]
async fn get_announcement() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();

    // Create an announcement in the database
    let original = Announcement {
        id: DieselUlid::generate(),
        announcement_type: "RELEASE".to_string(),
        title: "middlelayer get_announcement()".to_string(),
        teaser: "Announcement Teaser".to_string(),
        image_url: "https://announcement_image_url/dummy.webp".to_string(),
        content: "Announcement Content".to_string(),
        created_by: "The Aruna Team".to_string(),
        created_at: chrono::Utc::now().naive_local(),
        modified_by: "The Aruna Team".to_string(),
        modified_at: chrono::Utc::now().naive_local(),
    }
    .upsert(&client)
    .await
    .unwrap();

    // Error: get non-existent announcement
    assert!(db_handler
        .get_announcement(DieselUlid::generate())
        .await
        .is_err());

    // Get the created announcement
    let get_ann = db_handler.get_announcement(original.id).await.unwrap();

    assert_eq!(ProtoAnnouncement::from(original), get_ann)
}

#[tokio::test]
async fn get_announcements() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();

    // Create some announcements
    let mut announcements = vec![];
    for idx in 0..5 {
        announcements.push(
            Announcement {
                id: DieselUlid::generate(),
                announcement_type: "ORGA".to_string(),
                title: format!("middlelayer get_announcements({idx})"),
                teaser: "Announcement Teaser".to_string(),
                image_url: "https://announcement_image_url/some_dummy.webp".to_string(),
                content: "Announcement Content {}".to_string(),
                created_by: "The Aruna Team".to_string(),
                created_at: chrono::Utc::now().naive_local(),
                modified_by: "The Aruna Team".to_string(),
                modified_at: chrono::Utc::now().naive_local(),
            }
            .upsert(&client)
            .await
            .unwrap(),
        );
        std::thread::sleep(std::time::Duration::from_millis(10))
    }

    let mut request = GetAnnouncementsRequest {
        announcement_ids: vec![],
        page: None,
    };

    // Get all announcements
    let all_announcements = db_handler.get_announcements(request.clone()).await.unwrap();
    assert!(all_announcements.len() >= announcements.len());

    for a in &announcements {
        assert!(all_announcements.contains(&ProtoAnnouncement::from(a.clone())))
    }

    // Get first, third and last of the created announcements by id
    request.announcement_ids = vec![
        announcements.first().unwrap().id.to_string(),
        announcements.get(2).unwrap().id.to_string(),
        announcements.last().unwrap().id.to_string(),
    ];
    let anns = db_handler.get_announcements(request.clone()).await.unwrap();
    assert_eq!(anns.len(), 3);

    // Get pages with two announcements
    let mut page = PageRequest {
        start_after: "".to_string(),
        page_size: 2,
    };
    request.announcement_ids = vec![];
    request.page = Some(page.clone());

    let page_01 = db_handler.get_announcements(request.clone()).await.unwrap();
    let page_01_positions = page_01
        .iter()
        .map(|page_a| all_announcements.iter().position(|a| page_a == a).unwrap())
        .collect_vec();

    assert_eq!(page_01.len(), 2);
    assert!(page_01.iter().all(|a| all_announcements.contains(a)));

    page.start_after = page_01.last().unwrap().announcement_id.clone();
    request.page = Some(page);
    let page_02 = db_handler.get_announcements(request.clone()).await.unwrap();
    let page_02_positions = page_02
        .iter()
        .map(|page_a| all_announcements.iter().position(|a| page_a == a).unwrap())
        .collect_vec();

    assert_eq!(page_02.len(), 2);
    assert!(page_02.iter().all(|a| all_announcements.contains(a)));
    assert_eq!(
        page_01_positions
            .iter()
            .zip(&page_02_positions)
            .filter(|&(a, b)| a < b)
            .count(),
        2
    );
}

#[tokio::test]
async fn get_announcements_by_type() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();

    let mut types = vec![
        "ORGA".to_string(),
        "ORGA".to_string(),
        "RELEASE".to_string(),
        "RELEASE".to_string(),
    ];

    let mut announcements = vec![];
    while let Some(a_type) = test_utils::choose_and_remove(&mut types) {
        announcements.push(
            Announcement {
                id: DieselUlid::generate(),
                announcement_type: a_type.to_string(),
                title: format!(
                    "middlelayer get_announcements_by_type({})",
                    announcements.len() + 1
                ),
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
            .upsert(&client)
            .await
            .unwrap(),
        )
    }

    // Get announcements by type
    let mut request = GetAnnouncementsByTypeRequest {
        announcement_type: AnnouncementType::Orga as i32,
        page: None,
    };
    let typed_anns = db_handler
        .get_announcements_by_type(request.clone())
        .await
        .unwrap();

    assert!(typed_anns.len() >= 2);
    for ta in &typed_anns {
        assert_eq!(ta.announcement_type(), AnnouncementType::Orga)
    }

    // Get announcements by type paginated
    let mut page = PageRequest {
        start_after: "".to_string(),
        page_size: 1,
    };
    request.page = Some(page.clone());

    let page_01 = db_handler
        .get_announcements_by_type(request.clone())
        .await
        .unwrap();
    assert_eq!(page_01.len(), 1);
    assert_eq!(
        page_01.first().unwrap().announcement_type,
        AnnouncementType::Orga as i32
    );

    page.start_after = page_01.first().unwrap().announcement_id.clone();
    request.page = Some(page);
    let page_02 = db_handler
        .get_announcements_by_type(request.clone())
        .await
        .unwrap();
    assert_eq!(page_02.len(), 1);
    assert_eq!(
        page_02.first().unwrap().announcement_type,
        AnnouncementType::Orga as i32
    );
    assert_ne!(
        page_01.first().unwrap().announcement_id,
        page_02.first().unwrap().announcement_id
    );
}

#[tokio::test]
async fn set_announcements() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();

    let admin_ulid = DieselUlid::from_str(test_utils::ADMIN_USER_ULID).unwrap();
    let announcements = vec![
        ProtoAnnouncement {
            announcement_id: "".to_string(),
            announcement_type: AnnouncementType::Release as i32,
            title: "middlelayer set_announcements(1)".to_string(),
            teaser: "Some teaser".to_string(),
            image_url: "".to_string(),
            content: "Some content".to_string(),
            created_by: "The Aruna Team".to_string(),
            created_at: None,
            modified_by: "The Aruna Team".to_string(),
            modified_at: None,
        },
        ProtoAnnouncement {
            announcement_id: "".to_string(),
            announcement_type: AnnouncementType::Blog as i32,
            title: "middlelayer set_announcements(2)".to_string(),
            teaser: "Some teaser".to_string(),
            image_url: "".to_string(),
            content: "Some content".to_string(),
            created_by: "The Aruna Team".to_string(),
            created_at: None,
            modified_by: "The Aruna Team".to_string(),
            modified_at: None,
        },
        ProtoAnnouncement {
            announcement_id: "".to_string(),
            announcement_type: AnnouncementType::Maintenance as i32,
            title: "middlelayer set_announcements(3)".to_string(),
            teaser: "Some teaser".to_string(),
            image_url: "".to_string(),
            content: "Some content".to_string(),
            created_by: "The Aruna Team".to_string(),
            created_at: None,
            modified_by: "The Aruna Team".to_string(),
            modified_at: None,
        },
    ];

    let mut request = SetAnnouncementsRequest {
        announcements_upsert: announcements.clone(),
        announcements_delete: vec![],
    };

    // Insert the announcements
    let inserted = db_handler
        .set_announcements(admin_ulid, request.clone())
        .await
        .unwrap();

    for (idx, a) in enumerate(&announcements) {
        let insert = inserted.get(idx).unwrap();
        assert!(!insert.announcement_id.is_empty());
        assert_eq!(a.announcement_type, insert.announcement_type);
        assert_eq!(a.title, insert.title);
        assert_eq!(a.teaser, insert.teaser);
        assert_eq!(a.image_url, insert.image_url);
        assert_eq!(a.content, insert.content);
        assert_eq!(a.created_by, insert.created_by);
        assert_eq!(a.modified_by, insert.modified_by);
        assert_ne!(a.created_at, insert.created_at);
        assert_ne!(a.modified_at, insert.modified_at);
    }

    // Update one of the announcements
    let mut to_update = inserted.first().unwrap().clone();
    to_update.image_url = "http://example.org/franz_forster.webp".to_string();
    request.announcements_upsert = vec![to_update];

    let updated = db_handler
        .set_announcements(admin_ulid, request.clone())
        .await
        .unwrap();
    assert_eq!(
        updated.first().unwrap().image_url,
        "http://example.org/franz_forster.webp"
    );
    assert_eq!(
        inserted.first().unwrap().announcement_id,
        updated.first().unwrap().announcement_id
    );
    assert_ne!(
        inserted.first().unwrap().modified_at,
        updated.first().unwrap().modified_at
    );

    // Delete the announcements in random order
    request.announcements_upsert = vec![];
    let mut ids = inserted
        .iter()
        .map(|a| a.announcement_id.clone())
        .collect_vec();
    let mut deleted_ids = vec![];

    while let Some(id) = choose_and_remove(&mut ids) {
        request.announcements_delete = vec![id.clone()];
        db_handler
            .set_announcements(admin_ulid, request.clone())
            .await
            .unwrap();
        deleted_ids.push(id);

        // Check undeleted
        for id in &ids {
            assert!(
                Announcement::get(DieselUlid::from_str(id).unwrap(), &client)
                    .await
                    .unwrap()
                    .is_some()
            )
        }

        // Check deleted
        for id in &deleted_ids {
            assert!(
                Announcement::get(DieselUlid::from_str(id).unwrap(), &client)
                    .await
                    .unwrap()
                    .is_none()
            )
        }
    }
}
