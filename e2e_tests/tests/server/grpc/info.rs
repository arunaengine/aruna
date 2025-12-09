use aruna_rust_api::api::storage::models::v2::PageRequest;
use aruna_rust_api::api::storage::services::v2::storage_status_service_server::StorageStatusService;
use aruna_rust_api::api::storage::services::v2::{
    GetAnnouncementRequest, GetAnnouncementsByTypeRequest, GetAnnouncementsRequest,
};
use aruna_rust_api::api::storage::{
    models::v2::AnnouncementType,
    services::v2::{Announcement as ProtoAnnouncement, SetAnnouncementsRequest},
};
use chrono::{DateTime, Utc};
use diesel_ulid::DieselUlid;
use itertools::{enumerate, Itertools};
use tonic::Request;

use crate::common;
use crate::common::test_utils::ADMIN_OIDC_TOKEN;
use crate::common::{
    init_server::init_storage_status_service,
    test_utils::{self, add_token},
};
use prost_wkt_types::Timestamp;

#[tokio::test]
async fn set_announcement() {
    // Init StorageStatusService
    let postgres = common::infra::postgres(false).await;
    let nats = common::infra::nats(false).await;
    let info_service = init_storage_status_service(&postgres, &nats).await;

    let mut inner_request = SetAnnouncementsRequest {
        announcements_upsert: vec![
            ProtoAnnouncement {
                announcement_id: "".to_string(),
                announcement_type: AnnouncementType::Release as i32,
                title: "gRPC set_announcement(1)".to_string(),
                teaser: "Some teaser".to_string(),
                image_url: "".to_string(),
                content: "".to_string(),
                created_by: "The Aruna Team".to_string(),
                created_at: None,
                modified_by: "The Aruna Team".to_string(),
                modified_at: None,
            },
            ProtoAnnouncement {
                announcement_id: "".to_string(),
                announcement_type: AnnouncementType::Blog as i32,
                title: "gRPC set_announcement(2)".to_string(),
                teaser: "Some teaser".to_string(),
                image_url: "".to_string(),
                content: "".to_string(),
                created_by: "The Aruna Team".to_string(),
                created_at: None,
                modified_by: "The Aruna Team".to_string(),
                modified_at: None,
            },
        ],
        announcements_delete: vec![],
    };

    // Set announcements without token
    let grpc_request = Request::new(inner_request.clone());
    assert!(info_service.set_announcements(grpc_request).await.is_err());

    // Set announcements with invalid token
    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::INVALID_OIDC_TOKEN,
    );
    assert!(info_service.set_announcements(grpc_request).await.is_err());

    // Set announcements without sufficient permissions
    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::USER1_OIDC_TOKEN,
    );
    assert!(info_service.set_announcements(grpc_request).await.is_err());

    // Set announcements
    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::ADMIN_OIDC_TOKEN,
    );
    let inserted_announcements = info_service
        .set_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    // Update announcements
    let mut update_announcement = inserted_announcements.first().unwrap().clone();
    update_announcement.announcement_type = AnnouncementType::Update as i32;
    update_announcement.content = "Lorem Ipsum Dolor".to_string();
    inner_request.announcements_upsert = vec![update_announcement];

    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::ADMIN_OIDC_TOKEN,
    );

    let updated_announcements = info_service
        .set_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    let original = inserted_announcements.first().unwrap();
    let updated = updated_announcements.first().unwrap();

    assert_eq!(original.announcement_id, updated.announcement_id);
    assert_ne!(original.announcement_type, updated.announcement_type);
    assert_ne!(original.content, updated.content);
    assert_ne!(original.modified_at, updated.modified_at);

    // Delete announcements
    let delete_ids = inserted_announcements
        .iter()
        .map(|a| a.announcement_id.clone())
        .collect_vec();
    inner_request.announcements_upsert = vec![];
    inner_request.announcements_delete = delete_ids;

    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::ADMIN_OIDC_TOKEN,
    );

    info_service.set_announcements(grpc_request).await.unwrap();
}

#[tokio::test]
async fn get_announcement() {
    // Init StorageStatusService
    let postgres = common::infra::postgres(false).await;
    let nats = common::infra::nats(false).await;
    let info_service = init_storage_status_service(&postgres, &nats).await;

    let set_request = SetAnnouncementsRequest {
        announcements_upsert: vec![ProtoAnnouncement {
            announcement_id: "".to_string(),
            announcement_type: AnnouncementType::Blog as i32,
            title: "gRPC get_announcement(1)".to_string(),
            teaser: "Some teaser".to_string(),
            image_url: "".to_string(),
            content: "".to_string(),
            created_by: "The Aruna Team".to_string(),
            created_at: None,
            modified_by: "The Aruna Team".to_string(),
            modified_at: None,
        }],
        announcements_delete: vec![],
    };

    // Insert announcement
    let grpc_request = add_token(Request::new(set_request), test_utils::ADMIN_OIDC_TOKEN);
    let inserted_announcements = info_service
        .set_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;
    let inserted_announcement = inserted_announcements.first().unwrap();

    // Get non-existent announcement
    let mut get_request = GetAnnouncementRequest {
        announcement_id: DieselUlid::generate().to_string(),
    };
    let grpc_request = Request::new(get_request.clone());

    assert!(info_service.get_announcement(grpc_request).await.is_err());

    // Get announcement without token
    get_request.announcement_id = inserted_announcement.announcement_id.to_string();
    let grpc_request = Request::new(get_request.clone());

    let get_announcement = info_service
        .get_announcement(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcement
        .unwrap();

    assert_eq!(inserted_announcement, &get_announcement);

    // Get announcement with token
    let get_request = GetAnnouncementRequest {
        announcement_id: inserted_announcement.announcement_id.to_string(),
    };
    let grpc_request = add_token(Request::new(get_request.clone()), ADMIN_OIDC_TOKEN);

    let get_announcement = info_service
        .get_announcement(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcement
        .unwrap();

    assert_eq!(inserted_announcement, &get_announcement);
}

#[tokio::test]
async fn get_announcements() {
    // Init StorageStatusService
    let postgres = common::infra::postgres(false).await;
    let nats = common::infra::nats(false).await;
    let info_service = init_storage_status_service(&postgres, &nats).await;

    // Insert announcements
    let announcement_types = vec![
        "ANNOUNCEMENT_TYPE_RELEASE",
        "ANNOUNCEMENT_TYPE_BLOG",
        "ANNOUNCEMENT_TYPE_UPDATE",
        "ANNOUNCEMENT_TYPE_BLOG",
    ];
    let set_request = SetAnnouncementsRequest {
        announcements_upsert: enumerate(announcement_types)
            .map(|(idx, a_type)| ProtoAnnouncement {
                announcement_id: "".to_string(),
                announcement_type: AnnouncementType::from_str_name(a_type).unwrap() as i32,
                title: format!("gRPC get_announcements({idx})"),
                teaser: "Some teaser".to_string(),
                image_url: "".to_string(),
                content: "".to_string(),
                created_by: "The Aruna Team".to_string(),
                created_at: Some(Timestamp::from(
                    DateTime::from_timestamp_millis(Utc::now().timestamp_millis() + idx as i64)
                        .unwrap(),
                )),
                modified_by: "The Aruna Team".to_string(),
                modified_at: None,
            })
            .collect_vec(),
        announcements_delete: vec![],
    };
    let grpc_request = add_token(Request::new(set_request), test_utils::ADMIN_OIDC_TOKEN);
    let mut inserted_announcements = info_service
        .set_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;
    inserted_announcements.reverse(); // Reverse to sort descending for created_at

    // Get all announcements
    let mut get_request = GetAnnouncementsRequest {
        announcement_ids: vec![],
        page: None,
    };
    let grpc_request = Request::new(get_request.clone());
    let all_announcements = info_service
        .get_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    for a in &inserted_announcements {
        assert!(all_announcements.contains(a))
    }

    // Get all announcements paginated
    let mut page = PageRequest {
        start_after: "".to_string(),
        page_size: 2,
    };
    get_request.page = Some(page.clone());

    let grpc_request = Request::new(get_request.clone());
    let all_page_01 = info_service
        .get_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;
    let all_page_01_positions = all_page_01
        .iter()
        .map(|page_a| all_announcements.iter().position(|a| page_a == a).unwrap())
        .collect_vec();

    assert_eq!(all_page_01.len(), 2);
    assert!(all_page_01.iter().all(|a| all_announcements.contains(a)));

    page.start_after = all_page_01.first().unwrap().announcement_id.clone();
    get_request.page = Some(page.clone());

    let grpc_request = Request::new(get_request.clone());
    let all_page_02 = info_service
        .get_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;
    let all_page_02_positions = all_page_02
        .iter()
        .map(|page_a| all_announcements.iter().position(|a| page_a == a).unwrap())
        .collect_vec();

    assert_eq!(all_page_02.len(), 2);
    assert!(all_page_02.iter().all(|a| all_announcements.contains(a)));
    assert_eq!(
        all_page_01_positions
            .iter()
            .zip(&all_page_02_positions)
            .filter(|&(a, b)| a < b)
            .count(),
        2
    );

    // Get announcements by id
    get_request.announcement_ids = inserted_announcements[..2]
        .iter()
        .map(|a| a.announcement_id.clone())
        .collect_vec();
    get_request.page = None;
    let grpc_request = Request::new(get_request.clone());
    let id_announcements = info_service
        .get_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    assert_eq!(id_announcements.len(), 2);
    assert_eq!(id_announcements.as_slice(), &inserted_announcements[..2]);

    // Get announcements by id paginated
    get_request.announcement_ids = inserted_announcements
        .iter()
        .map(|a| a.announcement_id.clone())
        .collect_vec();
    page.start_after = "".to_string();
    get_request.page = Some(page.clone());
    let grpc_request = Request::new(get_request.clone());
    let id_announcements_page_01 = info_service
        .get_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;
    let id_page_01_positions = id_announcements_page_01
        .iter()
        .map(|page_a| all_announcements.iter().position(|a| page_a == a).unwrap())
        .collect_vec();

    assert_eq!(id_announcements_page_01.len(), 2);
    assert!(id_announcements_page_01
        .iter()
        .all(|a| inserted_announcements.contains(a)));

    page.start_after = id_announcements_page_01
        .last()
        .unwrap()
        .announcement_id
        .to_string();
    get_request.page = Some(page.clone());
    let grpc_request = Request::new(get_request.clone());
    let id_announcements_page_02 = info_service
        .get_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;
    let id_page_02_positions = id_announcements_page_02
        .iter()
        .map(|page_a| all_announcements.iter().position(|a| page_a == a).unwrap())
        .collect_vec();

    assert_eq!(id_announcements_page_02.len(), 2);
    assert!(id_announcements_page_02
        .iter()
        .all(|a| inserted_announcements.contains(a)));
    assert_eq!(
        id_page_01_positions
            .iter()
            .zip(&id_page_02_positions)
            .filter(|&(a, b)| a < b)
            .count(),
        2
    );
}

#[tokio::test]
async fn get_announcements_by_type() {
    // Init StorageStatusService
    let postgres = common::infra::postgres(false).await;
    let nats = common::infra::nats(false).await;
    let info_service = init_storage_status_service(&postgres, &nats).await;

    // Insert announcements
    let mut types = vec![
        "ANNOUNCEMENT_TYPE_ORGA".to_string(),
        "ANNOUNCEMENT_TYPE_ORGA".to_string(),
        "ANNOUNCEMENT_TYPE_ORGA".to_string(),
        "ANNOUNCEMENT_TYPE_RELEASE".to_string(),
        "ANNOUNCEMENT_TYPE_RELEASE".to_string(),
        "ANNOUNCEMENT_TYPE_RELEASE".to_string(),
    ];

    let mut announcements = vec![];
    while let Some(a_type) = test_utils::choose_and_remove(&mut types) {
        announcements.push(ProtoAnnouncement {
            announcement_id: "".to_string(),
            announcement_type: AnnouncementType::from_str_name(&a_type).unwrap() as i32,
            title: format!(
                "gRPC get_announcements_by_type({})",
                announcements.len() + 1
            ),
            teaser: "Some teaser".to_string(),
            image_url: "".to_string(),
            content: "".to_string(),
            created_by: "The Aruna Team".to_string(),
            created_at: None,
            modified_by: "The Aruna Team".to_string(),
            modified_at: None,
        })
    }
    let grpc_request = add_token(
        Request::new(SetAnnouncementsRequest {
            announcements_upsert: announcements,
            announcements_delete: vec![],
        }),
        test_utils::ADMIN_OIDC_TOKEN,
    );
    let inserted_announcements = info_service
        .set_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    // Get announcements by type
    let mut type_request = GetAnnouncementsByTypeRequest {
        announcement_type: AnnouncementType::Orga as i32,
        page: None,
    };
    let grpc_request = Request::new(type_request.clone());
    let all_type_announcements = info_service
        .get_announcements_by_type(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    assert!(all_type_announcements.len() >= 3);
    for a in &inserted_announcements
        .iter()
        .filter(|a| a.announcement_type() == AnnouncementType::Orga)
        .collect_vec()
    {
        assert!(all_type_announcements.contains(a))
    }
    for a in all_type_announcements {
        assert_eq!(a.announcement_type(), AnnouncementType::Orga);
    }

    // Get announcements by type paginated
    let mut page = PageRequest {
        start_after: "".to_string(),
        page_size: 2,
    };
    type_request.announcement_type = AnnouncementType::Release as i32;
    type_request.page = Some(page.clone());

    let grpc_request = Request::new(type_request.clone());
    let type_announcements_page_01 = info_service
        .get_announcements_by_type(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    assert_eq!(type_announcements_page_01.len(), 2);
    for a in &type_announcements_page_01 {
        assert_eq!(a.announcement_type(), AnnouncementType::Release)
    }

    page.start_after = type_announcements_page_01
        .last()
        .unwrap()
        .announcement_id
        .clone();
    type_request.page = Some(page.clone());

    let grpc_request = Request::new(type_request.clone());
    let type_announcements_page_02 = info_service
        .get_announcements_by_type(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    assert!(!type_announcements_page_02.is_empty());
    for a in type_announcements_page_02 {
        assert_eq!(a.announcement_type(), AnnouncementType::Release)
    }
}
