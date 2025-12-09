-- Insert global admin and normal user
INSERT INTO users (id, display_name, attributes, active) 
VALUES
(
    '018A0298-0FF4-995A-C4DC-B6685154E7AB', --01H819G3ZMK5DC9Q5PD18N9SXB
    'test-admin',
    '{"global_admin": true, "service_account": false, "tokens": {"01HV1NYGX710QGH24NZQS9MQ3J": {"name": "regular-token", "pub_key": 1, "object_id": null, "created_at": "2024-04-09T14:59:44.167166599", "expires_at": "2034-04-07T14:59:44", "user_rights": "NONE"}}, "trusted_endpoints": {}, "custom_attributes": [], "permissions": {}, "external_ids": [{ "external_id": "14f0e7bf-0947-4aa1-a8cd-337ddeff4573", "oidc_name":"http://localhost:1998/realms/test"}], "pubkey": "", "data_proxy_attribute": []}',
    true
);

INSERT INTO users (id, display_name, attributes, active) 
VALUES
(
    '018A27CF-78B4-D2A2-1F7E-16F6F27B5F8D', --01H8KWYY5MTAH1YZGPYVS7PQWD
    'test-user',
    '{"global_admin": false, "service_account": false, "tokens": {}, "trusted_endpoints": {}, "custom_attributes": [], "permissions": {}, "external_ids": [{ "external_id": "8dbee009-a3e8-4664-8856-14173d9abd5b", "oidc_name":"http://localhost:1998/realms/test"}], "pubkey": "", "data_proxy_attribute": []}',
    true
);

INSERT INTO users (id, display_name, attributes, active) 
VALUES
(
    '018AE07D-5F28-8EB6-3A0A-D37CE945E645', --01HBG7TQS8HTV3M2PKFKMMBSJ5
    'another-test-user',
    '{"global_admin": true, "service_account": false, "tokens": {}, "trusted_endpoints": {}, "custom_attributes": [], "permissions": {}, "external_ids": [{ "external_id": "21e87c60-b05c-40b1-ac5c-982b52ab2865", "oidc_name":"http://localhost:1998/realms/test"}], "pubkey": "", "data_proxy_attribute": []}',
    true
);


INSERT INTO endpoints(id, name, host_config, endpoint_variant, is_public, status) 
VALUES 
(
    '018a03c0-7e8b-293c-eb14-e10dc4b990db', --01H81W0ZMB54YEP5711Q2BK46V
    'default_endpoint',
    '[
	{"url": "http://localhost:50052", "is_primary": true, "ssl": false, "public": true, "feature": "GRPC"},
	{"url": "http://localhost:1337", "is_primary": true, "ssl": false, "public": true, "feature": "S3"}
    ]',
    'PERSISTENT',
    't',
    'AVAILABLE' 
);

INSERT INTO endpoints(id, name, host_config, endpoint_variant, is_public, status) VALUES (
    '018C1164-BDFC-08B5-F0FB-9D189419E848', --01HG8P9FFW12TZ1YWX32A1KT28
    'replication_endpoint',
    '[
	{"url": "http://localhost:50055", "is_primary": true, "ssl": false, "public": true, "feature": "GRPC"},
	{"url": "http://localhost:1338", "is_primary": true, "ssl": false, "public": true, "feature": "S3"}
    ]',
    'PERSISTENT',
    't',
    'AVAILABLE' 
);

INSERT INTO pub_keys(id, proxy, pubkey) VALUES (1337, '018a03c0-7e8b-293c-eb14-e10dc4b990db', 'MCowBQYDK2VwAyEAnouQBh4GHPCD/k85VIzPyCdOijVg2qlzt2TELwTMy4c=');
INSERT INTO pub_keys(id, proxy, pubkey) VALUES (1338, '018C1164-BDFC-08B5-F0FB-9D189419E848', 'MCowBQYDK2VwAyEAN77yqfv4hAdHTxrG576xqOgpqS8LuZUSjYXyJiUwfws=');

INSERT INTO licenses(tag, name, text, url) VALUES ('AllRightsReserved', 'All rights reserved', 'All rights reserved', 'license.test.org');

INSERT INTO identity_providers (
    issuer_name,
    jwks_endpoint,
    audiences
) VALUES ('http://localhost:1998/realms/test', 'http://localhost:1998/realms/test/protocol/openid-connect/certs', '{"test", "test-long"}');
