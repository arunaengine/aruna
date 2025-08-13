use aruna_rust_api::api::storage::models::v2::generic_resource;
use aruna_server::database::dsls::object_dsl::Author;
use aruna_server::{
    database::{
        dsls::object_dsl::{KeyValue, KeyValueVariant},
        enums::{DataClass, ObjectStatus, ObjectType},
    },
    search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes, ObjectDocument},
};
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use rand::{seq::IteratorRandom, rng as thread_rng, Rng};

mod common;

#[tokio::test]
async fn search_test() {
    // Create Meilisearch client
    let meilisearch_client =
        MeilisearchClient::new("http://localhost:7700", Some("MASTER_KEY")).unwrap();

    // Create index
    meilisearch_client
        .get_or_create_index("objects", Some("id"))
        .await
        .unwrap();

    // Generate random index objects
    let index_documents = (0..3)
        .map(|_| generate_random_object_document())
        .collect::<Vec<_>>();

    // Put objects in index
    meilisearch_client
        .add_or_update_stuff(index_documents.as_slice(), MeilisearchIndexes::OBJECT)
        .await
        .unwrap()
        .wait_for_completion(&meilisearch_client.client, None, None)
        .await
        .unwrap();

    // List index for and check if index contains all created documents
    let all_documents = meilisearch_client.list_index("objects").await.unwrap();

    index_documents
        .iter()
        .for_each(|doc| assert!(all_documents.contains(doc)));

    // Query some specific stuff without filter/sorting
    let mut specific_document = index_documents.first().unwrap().to_owned();
    let document_id = specific_document.id.to_string();
    let search_query = format!("\"{document_id}\""); // Exact search with quotation marks

    let (hits, estimated_total) = meilisearch_client
        .query_generic_stuff::<ObjectDocument>("objects", &search_query, "", 1000, 0)
        .await
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(estimated_total, 1);

    // Query some stuff with broken filter
    let mut query_filter = r#"resource_status IN [AVAILABLE, "ERROR"]"#;
    let result = meilisearch_client
        .query_generic_stuff::<ObjectDocument>("objects", "whatev", query_filter, 1000, 0)
        .await;
    assert!(result.is_err()); // resource_status is not in the list of filterable attributes

    // Update an index document
    specific_document.data_class = DataClass::PRIVATE;
    meilisearch_client
        .add_or_update_stuff(&[specific_document], MeilisearchIndexes::OBJECT)
        .await
        .unwrap()
        .wait_for_completion(&meilisearch_client.client, None, None)
        .await
        .unwrap();

    // Query updated document by unique dataclass
    query_filter = r#"data_class = PRIVATE"#;
    let (hits, estimated_total) = meilisearch_client
        .query_generic_stuff::<ObjectDocument>("objects", "ChatGPT", query_filter, 1000, 0)
        .await
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(estimated_total, 1);

    // Remove some index document
    meilisearch_client
        .delete_stuff(&[document_id], MeilisearchIndexes::OBJECT)
        .await
        .unwrap()
        .wait_for_completion(&meilisearch_client.client, None, None)
        .await
        .unwrap();

    let (hits, estimated_total) = meilisearch_client
        .query_generic_stuff::<ObjectDocument>("objects", &search_query, "", 1000, 0)
        .await
        .unwrap();

    assert_eq!(hits.len(), 0);
    assert_eq!(estimated_total, 0); // specific document is not in search index anymore

    // Convert all index documents to proto representation
    for doc in all_documents {
        let _ = generic_resource::Resource::from(doc);
    }
}

fn generate_random_object_document() -> ObjectDocument {
    let mut rng = thread_rng();
    let name_parts = vec![
        "rna", "hyper", "breaking", "sequence", "mapper", "stuff", "creature", "bacteria",
        "science", "dna", "cdna",
    ];
    let project_name = name_parts
        .into_iter()
        .choose_multiple(&mut rng, 2)
        .to_vec()
        .join("-");
    let object_type = ObjectType::try_from(rng.random_range(1..5)).unwrap();
    let rand_count = match object_type {
        ObjectType::OBJECT => 1,
        _ => rng.random_range(2..123),
    };
    let rand_size = rng.random_range(123..123456789);
    let hook_run_success = rng.random_bool(0.5).to_string();
    let created_at = format!("{}-01-01 23:59:59", rng.random_range(2001..2023));

    ObjectDocument {
        id: DieselUlid::generate(),
        object_type,
        object_type_id: object_type as u8,
        status: ObjectStatus::try_from(rng.random_range(1..6)).unwrap(),
        name: project_name,
        title: "this is a project_title".to_string(),
        description: "ChatGPT should create some hallucinated description of this project."
            .to_string(),
        authors: vec![Author {
            first_name: "A".to_string(),
            last_name: "B".to_string(),
            email: Some("C".to_string()),
            orcid: None,
            user_id: None,
        }],
        count: rand_count,
        size: rand_size,
        labels: vec![
            KeyValue {
                key: "validated".to_string(),
                value: hook_run_success.clone(),
                variant: KeyValueVariant::LABEL,
            },
            KeyValue {
                key: "submitted".to_string(),
                value: hook_run_success,
                variant: KeyValueVariant::LABEL,
            },
            KeyValue {
                key: "validate_and_submit".to_string(),
                value: "fastq;ENA".to_string(),
                variant: KeyValueVariant::HOOK,
            },
        ],
        data_class: DataClass::PUBLIC,
        created_at: NaiveDateTime::parse_from_str(&created_at, "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp(),
        dynamic: rng.random_bool(0.5).to_string().parse::<bool>().unwrap(),
        metadata_license: "AllRightsReserved".to_string(),
        data_license: "AllRightsReserved".to_string(),
    }
}
