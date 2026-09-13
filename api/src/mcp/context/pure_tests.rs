use super::*;
use std::collections::{HashMap, HashSet};

fn body(result: CallToolResult) -> serde_json::Value {
    assert_eq!(result.is_error, Some(true));
    result
        .structured_content
        .expect("a tool error carries the structured body")
}

#[test]
fn group_argument_reasons() {
    let text = body(group_argument("bad").unwrap_err());
    assert_eq!(text["code"], "Bad request");
    assert!(
        text["error"]
            .as_str()
            .unwrap_or_default()
            .contains("list_groups")
    );
    assert!(group_argument(&Ulid::generate().to_string()).is_ok());
}

#[test]
fn group_error_maps() {
    let missing = body(group_error(crate::error::ServerError::NotFound));
    assert!(
        missing["error"]
            .as_str()
            .unwrap_or_default()
            .contains("list_groups")
    );
    let forbidden = body(group_error(crate::error::ServerError::Forbidden));
    assert!(
        forbidden["error"]
            .as_str()
            .unwrap_or_default()
            .contains("membership")
    );
}

#[test]
fn map_role_permissions() {
    let role_id = Ulid::generate();
    let role = Role {
        role_id,
        name: "writer".to_string(),
        permissions: HashMap::from([("/realm/g/x/**".to_string(), Permission::WRITE)]),
        assigned_users: HashSet::new(),
    };
    let output = map_role(role);
    assert_eq!(output.role_id, role_id.to_string());
    assert_eq!(output.name, "writer");
    assert_eq!(
        output.permissions.get("/realm/g/x/**").map(String::as_str),
        Some("Write")
    );
}

#[test]
fn maps_user_error() {
    assert_eq!(
        body(map_user_error(ReadUserDocumentError::NotFound))["code"],
        "Not found"
    );
    assert_eq!(
        body(map_user_error(ReadUserDocumentError::NotFinished))["code"],
        "Internal error"
    );
}
