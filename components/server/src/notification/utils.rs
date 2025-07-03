use crate::caching::structs::CachedRule;
use anyhow::anyhow;
use aruna_rust_api::api::notification::services::v2::{announcement_event::EventVariant, Reply};
use base64::{engine::general_purpose, Engine};
use diesel_ulid::DieselUlid;
use hmac::{Hmac, Mac};
use rand::{distributions::Alphanumeric, Rng};
use sha2::Sha256;
use xxhash_rust::xxh3::xxh3_128;

use crate::database::dsls::rule_dsl::Rule;
use crate::database::{dsls::object_dsl::Hierarchy, enums::ObjectType};

use super::handler::EventType;

// ------------------------------------------- //
// ----- Subject Generation ------------------ //
// ------------------------------------------- //
///ToDo: Rust Doc
pub fn generate_resource_subject(
    resource_id: &str,
    resource_variant: ObjectType,
    include_sub_resources: bool,
) -> String {
    // No one cares about the specific graph hierarchy anymore
    let base_subject = match resource_variant {
        ObjectType::PROJECT => format!("AOS.RESOURCE._.{resource_id}."),
        ObjectType::COLLECTION => format!("AOS.RESOURCE._.*._.{resource_id}."),
        ObjectType::DATASET => format!("AOS.RESOURCE._.*._.*._.{resource_id}."),
        ObjectType::OBJECT => format!("AOS.RESOURCE._.*._.*._.*._.{resource_id}."),
    };

    if include_sub_resources {
        format!("{base_subject}>")
    } else {
        format!("{base_subject}_")
    }
}

///ToDo: Rust Doc
pub fn generate_resource_message_subject(
    resource_id: &str,
    resource_variant: ObjectType,
) -> String {
    // No one cares about the specific graph anymore
    match resource_variant {
        ObjectType::PROJECT => format!("AOS.RESOURCE._.{resource_id}._"),
        ObjectType::COLLECTION => format!("AOS.RESOURCE._.*._.{resource_id}._"),
        ObjectType::DATASET => format!("AOS.RESOURCE._.*._.*._.{resource_id}._"),
        ObjectType::OBJECT => format!("AOS.RESOURCE._.*._.*._.*._.{resource_id}._"),
    }
}

///ToDo: Rust Doc
pub fn generate_resource_message_subjects(hierarchies: Vec<Hierarchy>) -> Vec<String> {
    let mut subjects = vec![];
    for hierarchy in hierarchies {
        subjects.push(format!(
            "AOS.RESOURCE._.{}._.{}._.{}._.{}._",
            hierarchy.project_id,
            hierarchy.collection_id.unwrap_or_else(|| "*".to_string()),
            hierarchy.dataset_id.unwrap_or_else(|| "*".to_string()),
            hierarchy.object_id.unwrap_or_else(|| "*".to_string()),
        ))
    }

    subjects
}

///ToDo: Rust Doc
pub fn generate_user_subject(user_id: &str) -> String {
    format!("AOS.USER.{user_id}.>")
}

///ToDo: Rust Doc
pub fn generate_user_message_subject(user_id: &str) -> String {
    format!("AOS.USER.{user_id}._")
}

///ToDo: Rust Doc
pub fn generate_announcement_subject() -> String {
    "AOS.ANNOUNCEMENT".to_string()
}

///ToDo: Rust Doc
pub fn generate_announcement_message_subject(event_variant: &EventVariant) -> String {
    match event_variant {
        EventVariant::NewDataProxyId(_) => "AOS.ANNOUNCEMENT.DATAPROXY.NEW".to_string(),
        EventVariant::RemoveDataProxyId(_) => "AOS.ANNOUNCEMENT.DATAPROXY.DELETE".to_string(),
        EventVariant::UpdateDataProxyId(_) => "AOS.ANNOUNCEMENT.DATAPROXY.UPDATE".to_string(),
        EventVariant::NewPubkey(_) => "AOS.ANNOUNCEMENT.PUBKEY.NEW".to_string(),
        EventVariant::RemovePubkey(_) => "AOS.ANNOUNCEMENT.PUBKEY.DELETE".to_string(),
        EventVariant::Downtime(_) => "AOS.ANNOUNCEMENT.DOWNTIME".to_string(),
        EventVariant::Version(_) => "AOS.ANNOUNCEMENT.VERSION".to_string(),
    }
}

///ToDo: Rust Doc
pub fn generate_endpoint_subject(endpoint_id: &DieselUlid) -> String {
    format!("AOS.ENDPOINT.{endpoint_id}")
}

///ToDo: Rust Doc
pub fn parse_event_consumer_subject(subject: &str) -> anyhow::Result<EventType> {
    // Evaluate general message variant
    if subject.starts_with("AOS.RESOURCE") {
        let include_subresources = subject.ends_with('>');
        let placeholder_count = subject[13..].matches('*').count();
        let resource_id = subject[13..].split('.').collect::<Vec<_>>()[match placeholder_count {
            0 => 1,
            1 => 3,
            2 => 5,
            3 => 7,
            _ => return Err(anyhow::anyhow!("Invalid number of placeholders in subject")),
        }]
        .to_string();

        Ok(EventType::Resource((
            resource_id,
            match placeholder_count {
                0 => ObjectType::PROJECT,
                1 => ObjectType::COLLECTION,
                2 => ObjectType::DATASET,
                3 => ObjectType::OBJECT,
                _ => return Err(anyhow::anyhow!("Could not determine resource type")),
            },
            include_subresources,
        )))
    } else if subject.starts_with("AOS.USER") {
        // Parse user_id
        let user_id = subject.split('.').collect::<Vec<_>>()[2];
        Ok(EventType::User(user_id.to_string()))
    } else if subject.starts_with("AOS.ANNOUNCEMENT") {
        // Variant does not matter at this moment
        Ok(EventType::Announcement(None))
    } else if subject.starts_with("AOS.>") || subject.starts_with("AOS.ENDPOINT") {
        Ok(EventType::All)
    } else {
        Err(anyhow::anyhow!("Invalid consumer subject"))
    }
}

// ------------------------------------------- //
// ----- Reply Validation -------------------- //
// ------------------------------------------- //
type HmacSha256 = Hmac<Sha256>;

///ToDo: Rust Doc
pub fn calculate_base64_xxhash(payload: &[u8]) -> String {
    general_purpose::STANDARD.encode(xxh3_128(payload).to_le_bytes())
}

///ToDo: Rust Doc
pub fn calculate_reply_hmac(reply_subject: &str, secret: String) -> Reply {
    // Generate random salt value
    let salt = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();

    // Calculate hmac
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(format!("{reply_subject}-{salt}").as_bytes());

    // Encode hmac in base64
    let base64_hmac = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    // Return reply
    Reply {
        reply: reply_subject.to_string(),
        salt,
        hmac: base64_hmac,
    }
}

///ToDo: Rust Doc
pub fn validate_reply_msg(reply: Reply, secret: String) -> anyhow::Result<bool> {
    // Calculate hmac
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(hmac256) => hmac256,
        Err(_) => return Err(anyhow::anyhow!("Invalid key length for hmac")),
    };
    mac.update(format!("{}-{}", reply.reply, reply.salt).as_bytes());

    // Encode updated hmac in base64
    let base64_hmac = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    // Check if hmacs are equal
    Ok(base64_hmac == reply.hmac)
}

/// Builds CachedRule for cache update
/// expects:
///     - Rule
/// results:
///     - CachedRule with compiled expression from expression field
pub fn build_rule(rule: Rule) -> anyhow::Result<CachedRule> {
    let compiled = cel_parser::parse(&rule.rule_expressions).map_err(|e| anyhow!(e.to_string()))?;
    Ok(CachedRule { rule, compiled })
}

#[cfg(test)]
mod tests {
    use crate::{
        database::enums::ObjectType,
        notification::{handler::EventType, utils::parse_event_consumer_subject},
    };
    use diesel_ulid::DieselUlid;

    #[test]
    fn test_consumer_subject_parser() {
        /* ----- Resources ----- */
        let project_ulid = DieselUlid::generate();
        let project_subject = format!("AOS.RESOURCE._.{project_ulid}._");

        let event_type = parse_event_consumer_subject(&project_subject).unwrap();
        assert_eq!(
            event_type,
            EventType::Resource((project_ulid.to_string(), ObjectType::PROJECT, false))
        );

        let collection_ulid = DieselUlid::generate();
        let collection_subject = format!("AOS.RESOURCE._.*._.{collection_ulid}._");

        let event_type = parse_event_consumer_subject(&collection_subject).unwrap();
        assert_eq!(
            event_type,
            EventType::Resource((collection_ulid.to_string(), ObjectType::COLLECTION, false))
        );

        let dataset_ulid = DieselUlid::generate();
        let dataset_subject = format!("AOS.RESOURCE._.*._.*._.{dataset_ulid}._");

        let event_type = parse_event_consumer_subject(&dataset_subject).unwrap();
        assert_eq!(
            event_type,
            EventType::Resource((dataset_ulid.to_string(), ObjectType::DATASET, false))
        );

        let object_ulid = DieselUlid::generate();
        let object_subject = format!("AOS.RESOURCE._.*._.*._.*._.{object_ulid}._");

        let event_type = parse_event_consumer_subject(&object_subject).unwrap();
        assert_eq!(
            event_type,
            EventType::Resource((object_ulid.to_string(), ObjectType::OBJECT, false))
        );

        /* ----- User ----- */
        let user_ulid = DieselUlid::generate();
        let user_subject = format!("AOS.USER.{user_ulid}.>");

        let event_type = parse_event_consumer_subject(&user_subject).unwrap();
        assert_eq!(event_type, EventType::User(user_ulid.to_string()));

        /* ----- Announcement ----- */
        let announcement_subjects = vec![
            "AOS.ANNOUNCEMENT.DATAPROXY.NEW",
            "AOS.ANNOUNCEMENT.DATAPROXY.DELETE",
            "AOS.ANNOUNCEMENT.DATAPROXY.UPDATE",
            "AOS.ANNOUNCEMENT.PUBKEY.NEW",
            "AOS.ANNOUNCEMENT.PUBKEY.DELETE",
            "AOS.ANNOUNCEMENT.DOWNTIME",
            "AOS.ANNOUNCEMENT.VERSION",
        ];

        for subject in announcement_subjects {
            let event_type = parse_event_consumer_subject(subject).unwrap();
            assert_eq!(event_type, EventType::Announcement(None));
        }
    }
}
