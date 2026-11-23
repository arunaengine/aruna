//! Tests the checked connection request: the address from the saved settings, the path below it,
//! the query, declared methods only, and no credential header of the guest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use serde_json::json;

fn manifest() -> Manifest {
    let manifest = json!({
        "name": "fetcher",
        "version": "0.1.0",
        "settings": { "ena_url": { "title": "ENA", "type": "url" } },
        "connections": {
            "ena": { "url": "{ena_url}/api", "methods": ["GET"], "auth": "token" },
            "public": { "url": "https://public.example.org/v1/" }
        }
    });
    serde_json::from_value(manifest).unwrap()
}

fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    (pairs.iter())
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

#[test]
fn builds_request() {
    let manifest = manifest();
    let settings = r#"{"ena_url":"https://ena.example.org"}"#;
    let (query, headers) = (
        pairs(&[("run", "SRR1")]),
        pairs(&[("Accept", "text/plain")]),
    );
    let parts = ("files/x", &query[..], &headers[..]);
    let context = request_context(&manifest, ("ena", Method::Get), settings, parts).unwrap();
    assert_eq!(context.base, "https://ena.example.org/api");
    let (request, slot) = outgoing(&manifest, &context).unwrap();
    assert_eq!(request.method, "GET");
    let url = "https://ena.example.org/api/files/x?run=SRR1";
    assert_eq!(request.url.as_str(), url);
    assert_eq!(request.headers, pairs(&[("accept", "text/plain")]));
    assert_eq!(slot.as_deref(), Some("token"));
}

#[test]
fn refuses_undeclared() {
    let manifest = manifest();
    let no_settings = "{}";
    let parts = ("x", &[][..], &[][..]);
    let unset = request_context(&manifest, ("ena", Method::Get), no_settings, parts);
    assert_eq!(
        unset.unwrap_err().kind,
        ErrorKind::Invalid,
        "the address has no setting"
    );
    let missing = request_context(&manifest, ("other", Method::Get), no_settings, parts);
    assert_eq!(missing.unwrap_err().kind, ErrorKind::Invalid);
    let context = request_context(&manifest, ("public", Method::Get), no_settings, parts).unwrap();
    let mut posted = context.clone();
    posted.method = "POST".into();
    assert_eq!(
        outgoing(&manifest, &posted).unwrap_err().kind,
        ErrorKind::Invalid
    );
    let mut escaped = context.clone();
    escaped.path = "../admin".into();
    assert_eq!(
        outgoing(&manifest, &escaped).unwrap_err().kind,
        ErrorKind::Invalid
    );
    // A context kept from an earlier manifest is checked against the current one.
    let mut gone = context;
    gone.connection = "removed".into();
    assert_eq!(
        outgoing(&manifest, &gone).unwrap_err().kind,
        ErrorKind::Invalid
    );
}

#[test]
fn drops_credentials() {
    let manifest = manifest();
    let headers = pairs(&[
        ("Authorization", "Bearer x"),
        ("Cookie", "c=1"),
        ("X-Run", "7"),
    ]);
    let parts = ("", &[][..], &headers[..]);
    let context = request_context(&manifest, ("public", Method::Get), "{}", parts).unwrap();
    assert_eq!(context.headers, pairs(&[("X-Run", "7")]));
    let (request, slot) = outgoing(&manifest, &context).unwrap();
    assert_eq!(request.headers, pairs(&[("x-run", "7")]));
    assert_eq!(request.url.as_str(), "https://public.example.org/v1/");
    assert_eq!(slot, None);
}

#[test]
fn stored_answer_reads() {
    let reply = Response {
        status: 201,
        headers: vec![Field {
            name: "x-run".into(),
            value: "7".into(),
        }],
        body: b"done".to_vec(),
    };
    let read = stored_whole(&whole_text(&reply)).unwrap();
    assert_eq!((read.status, read.body), (201, b"done".to_vec()));
    assert_eq!(read.headers[0].value, "7");
    let large = stored_whole(&too_large(413)).unwrap_err();
    assert_eq!(large.kind, ErrorKind::Limit);
    assert!(stored_whole("{}").is_err());
}

mod connection_calls {
    use aruna_core::outbound::ConnectionAnswer;

    use super::*;

    fn send() -> SendConnection {
        let request = OutgoingRequest {
            method: "GET".to_string(),
            url: "https://example.org/a".parse().unwrap(),
            headers: Vec::new(),
        };
        SendConnection::new(request, Vec::new())
    }

    fn answered(result: Result<ConnectionAnswer, ConnectionError>) -> IoResult {
        let event = Event::Outbound(OutboundEvent::ConnectionAnswered { result });
        IoResult::Event(Box::new(event))
    }

    #[test]
    fn send_answers_status() {
        let mut call = send();
        let Ok(Next::Call(IoCall::Effect(effect))) = call.first() else {
            panic!("a send is one outbound effect");
        };
        assert!(matches!(
            *effect,
            Effect::Outbound(OutboundEffect::Connection(_))
        ));
        let answer = ConnectionAnswer {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
        };
        let done = call.next(answered(Ok(answer.clone())));
        assert_eq!(done, Ok(Next::Done(answer)));
    }

    #[test]
    fn send_reports_failure() {
        let mut call = send();
        call.first().unwrap();
        let failed = ConnectionError::NotSent("no".to_string());
        assert_eq!(call.next(answered(Err(failed.clone()))), Err(failed));
        let other = call.next(IoResult::Done);
        assert!(matches!(other, Err(ConnectionError::Unknown(_))));
    }
}
