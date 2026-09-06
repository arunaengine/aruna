//! Line-delimited JSON the node and the session helper exchange over the
//! channel. The node forwards cell code unchanged and never interprets it.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One request the node sends to the helper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum HelperRequest {
    Execute {
        id: u64,
        cell_id: String,
        code: String,
    },
    Interrupt {
        id: u64,
    },
    Status {
        id: u64,
    },
    List {
        id: u64,
        path: String,
    },
    Read {
        id: u64,
        path: String,
        offset: u64,
        limit: u64,
    },
}

impl HelperRequest {
    pub fn id(&self) -> u64 {
        match self {
            HelperRequest::Execute { id, .. }
            | HelperRequest::Interrupt { id }
            | HelperRequest::Status { id }
            | HelperRequest::List { id, .. }
            | HelperRequest::Read { id, .. } => *id,
        }
    }
}

/// One event the helper sends back. A reply answers `list`, `read` and
/// `status` by request id.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HelperEvent {
    Output {
        cell_id: String,
        output: Value,
    },
    Cell {
        cell_id: String,
        state: String,
        #[serde(default)]
        execution_count: Option<u32>,
    },
    Kernel {
        state: String,
    },
    Reply {
        id: u64,
        #[serde(flatten)]
        body: Map<String, Value>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_carries_op() {
        let line = serde_json::to_string(&HelperRequest::Execute {
            id: 7,
            cell_id: "c1".to_string(),
            code: "print(1)".to_string(),
        })
        .expect("request serializes");
        let parsed: Value = serde_json::from_str(&line).expect("line parses");
        assert_eq!(parsed["op"], "execute");
        assert_eq!(parsed["id"], 7);
        assert_eq!(parsed["cell_id"], "c1");
    }

    #[test]
    fn reply_keeps_body() {
        // A reply's payload is helper owned, so unknown fields must survive.
        let event: HelperEvent =
            serde_json::from_str(r#"{"kind":"reply","id":3,"entries":[],"path":"."}"#)
                .expect("reply parses");
        let HelperEvent::Reply { id, body } = event else {
            panic!("expected a reply");
        };
        assert_eq!(id, 3);
        assert_eq!(body["path"], ".");
        assert!(body.contains_key("entries"));
    }

    #[test]
    fn rejects_unknown_kind() {
        assert!(serde_json::from_str::<HelperEvent>(r#"{"kind":"nope"}"#).is_err());
    }
}
