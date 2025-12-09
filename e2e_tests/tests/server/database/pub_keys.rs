use crate::common::infra;
use crate::common::init::init_database;
use crate::common::init_server::Component;
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::pub_key_dsl::PubKey;
use tokio_postgres::GenericClient;

#[tokio::test]
async fn test_crud() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do pubkeys stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let mut key_one = PubKey {
        id: 1001,
        proxy: None,
        pubkey: "MCowBQYDK2VwAyEAZ+mKlzCFRvR1bfSt1jrW9OSiO6Jf/zOQI9K5JtfeR7o=".to_string(),
    };

    key_one.create(client).await.unwrap();

    let key = PubKey::get(1001i16, client).await.unwrap();
    assert!(key.is_some());
    let mut key_two = PubKey {
        id: 2001,
        proxy: None,
        pubkey: "MCowBQYDK2VwAyEAK6xkhtaRnJGxt/t2o/xVYb4XS/vlDLRDEayUGpUs2c0=".to_string(),
    };
    let mut key_three = PubKey {
        id: 3001,
        proxy: None,
        pubkey: "MCowBQYDK2VwAyEAFbz/lgotH+LhybhaVCcdz2k/gKR/IeTZt+3/7Tl70ro=".to_string(),
    };
    key_two.create(client).await.unwrap();
    key_three.create(client).await.unwrap();

    let comp = [&key_one, &key_two, &key_three];
    let all = PubKey::all(client).await.unwrap();

    assert!(comp.iter().all(|pk| all.contains(pk)));

    key_one.delete(client).await.unwrap();
    key_two.delete(client).await.unwrap();
    key_three.delete(client).await.unwrap();

    let rest = PubKey::all(client).await.unwrap();

    assert!([&key_one, &key_two, &key_three]
        .iter()
        .all(|k| !rest.contains(k)))
}

#[tokio::test]
async fn test_pub_key_serial_auto_incerement() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do pubkeys stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    // Generate random strings as key dummy
    let dummy_pubkey_001 = "MCowBQYDK2VwAyEAw6nwkNVZyJyYytGxLTfa9yQpPJNR616iq7G9BzjT8wM="; //gen_rand_string();
    let dummy_pubkey_002 = "MCowBQYDK2VwAyEAQPP30yBtHJ4IRRtNjxBr4+p4HzpE0EWLMMN/sHpWnT4="; //gen_rand_string();

    // Persist dummy keys in database with auto serial increment
    let dummy_key_001 = PubKey::create_or_get_without_id(None, dummy_pubkey_001, client)
        .await
        .unwrap();

    // Persist dummy keys in database with auto serial increment
    let dummy_key_002 = PubKey::create_or_get_without_id(None, dummy_pubkey_002, client)
        .await
        .unwrap();

    assert!(dummy_key_002.id > dummy_key_001.id);

    // Fetch pubkey by its key
    for key in [dummy_key_001, dummy_key_002] {
        let fetched_key = PubKey::get_by_key(&key.pubkey, client)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(fetched_key, key)
    }
}
