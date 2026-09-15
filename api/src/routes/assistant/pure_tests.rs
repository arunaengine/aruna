use super::*;
use aruna_core::UserId;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::structs::identity::realm::RealmId;

#[test]
fn server_rejects_private() {
    assert!(validate_url_mode("http://127.0.0.1:11434", false).is_err());
    assert!(validate_url_mode("https://localhost", false).is_err());
    assert!(validate_url_mode("https://service.local", false).is_err());
}

#[test]
fn device_accepts_private() {
    assert_eq!(
        validate_url_mode("http://127.0.0.1:11434", true).unwrap(),
        "http://127.0.0.1:11434"
    );
}

#[test]
fn response_hides_secrets() {
    let realm_id = RealmId::from_bytes([3; 32]);
    let mut provider = AssistantProvider {
        provider_id: Ulid::from_bytes([4; 16]).to_string(),
        user_id: UserId::local(Ulid::from_bytes([5; 16]), realm_id),
        kind: AssistantProviderKind::Openai,
        label: "OpenAI".to_string(),
        base_url: "https://api.openai.com".to_string(),
        headers: EncryptedS3Secret::empty(),
        secret: EncryptedS3Secret::empty(),
        models: Vec::new(),
        default_model: None,
        created_at: 1,
        status: AssistantProviderStatus::Ready,
        token_obtained_at: None,
        login_expires_at: None,
        login_interval_seconds: None,
    };
    let key = CredentialEncryptionKey::derive(&[7; 32]);
    provider
        .encrypt_secret(
            &key,
            &AssistantProviderSecret {
                api_key: Some(Secret::new("secret-key")),
                account_id: Some(Secret::new("secret-account")),
                ..AssistantProviderSecret::empty()
            },
        )
        .unwrap();
    let body = serde_json::to_string(&provider_summary(&provider)).unwrap();

    assert!(!body.contains("secret-key"));
    assert!(!body.contains("secret-account"));
}

#[test]
fn efforts_by_family() {
    assert_eq!(
        reasoning_efforts(AssistantProviderKind::Chatgpt, "gpt-5.6-sol"),
        ["minimal", "low", "medium", "high", "xhigh", "max", "ultra"]
    );
    assert_eq!(
        reasoning_efforts(AssistantProviderKind::Chatgpt, "gpt-5.5"),
        ["minimal", "low", "medium", "high", "xhigh"]
    );
    assert_eq!(
        reasoning_efforts(AssistantProviderKind::Openai, "gpt-5"),
        ["low", "medium", "high"]
    );
    assert_eq!(
        reasoning_efforts(AssistantProviderKind::Anthropic, "claude-sonnet-4"),
        ["off", "low", "medium", "high"]
    );
    assert!(reasoning_efforts(AssistantProviderKind::Chatgpt, "o3-mini").is_empty());
    assert!(reasoning_efforts(AssistantProviderKind::Anthropic, "claude-3-5-haiku").is_empty());
}

#[test]
fn efforts_round_trip() {
    let response = ProviderModelsResponse {
        models: vec![
            ProviderModel {
                id: "gpt-5.6-sol".to_string(),
                display_name: None,
                static_model: true,
                reasoning_efforts: vec!["minimal".to_string(), "xhigh".to_string()],
            },
            ProviderModel {
                id: "text-embedding-3-small".to_string(),
                display_name: None,
                static_model: false,
                reasoning_efforts: Vec::new(),
            },
        ],
    };
    let json = serde_json::to_value(&response).unwrap();
    assert_eq!(
        json["models"][0]["reasoning_efforts"],
        serde_json::json!(["minimal", "xhigh"])
    );
    assert!(json["models"][1].get("reasoning_efforts").is_none());
    let parsed: ProviderModelsResponse = serde_json::from_value(json).unwrap();
    assert_eq!(parsed.models[0].reasoning_efforts, ["minimal", "xhigh"]);
    assert!(parsed.models[1].reasoning_efforts.is_empty());
}

#[test]
fn kind_parsing() {
    assert!(matches!(
        parse_provider_kind("anthropic"),
        Ok(AssistantProviderKind::Anthropic)
    ));
    assert!(matches!(
        parse_provider_kind("openai_compatible"),
        Ok(AssistantProviderKind::OpenaiCompatible)
    ));
    assert!(parse_provider_kind("bogus").is_err());
}

#[test]
fn url_rejects_credentials() {
    assert!(validate_url_mode("http://user:pass@host", true).is_err());
    assert!(validate_url_mode("http://host/?q=1", true).is_err());
    assert!(validate_url_mode("http://host/#frag", true).is_err());
    assert!(validate_url_mode("ftp://host", true).is_err());
    // A server node accepts a public https domain and trims the slash.
    assert_eq!(
        validate_url_mode("https://api.example.com/", false).unwrap(),
        "https://api.example.com"
    );
}

#[test]
fn headers_reject_auth() {
    let ok =
        headers_from_input(BTreeMap::from([("x-trace".to_string(), "1".to_string())])).unwrap();
    assert!(ok.0.contains_key("x-trace"));
    assert!(
        headers_from_input(BTreeMap::from([(
            "authorization".to_string(),
            "Bearer x".to_string()
        )]))
        .is_err()
    );
    assert!(
        headers_from_input(BTreeMap::from([(
            "bad header".to_string(),
            "v".to_string()
        )]))
        .is_err()
    );
    let too_many = (0..65)
        .map(|index| (format!("x-h-{index}"), "v".to_string()))
        .collect();
    assert!(headers_from_input(too_many).is_err());
}

#[test]
fn forbidden_header_names() {
    assert!(forbidden_header(&HeaderName::from_static("authorization")));
    assert!(forbidden_header(&HeaderName::from_static("content-length")));
    assert!(!forbidden_header(&HeaderName::from_static("x-custom")));
}

#[test]
fn store_error_mapping() {
    use aruna_operations::assistant::provider::ProviderStoreError;
    assert!(matches!(
        map_store_error(ProviderStoreError::NotFound),
        ServerError::NotFound
    ));
    assert!(matches!(
        map_store_error(ProviderStoreError::IdCollision),
        ServerError::Conflict(_)
    ));
    assert!(matches!(
        map_store_error(ProviderStoreError::Stale),
        ServerError::Conflict(_)
    ));
    assert!(matches!(
        map_store_error(ProviderStoreError::NotFinished),
        ServerError::InternalError(_)
    ));
}

#[test]
fn drops_display_names() {
    let ids = model_ids(vec![
        ModelInput {
            id: "gpt-5".to_string(),
            display_name: Some("GPT 5".to_string()),
        },
        ModelInput {
            id: "o3".to_string(),
            display_name: None,
        },
    ]);
    assert_eq!(ids, ["gpt-5", "o3"]);
}
