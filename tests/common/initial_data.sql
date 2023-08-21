INSERT INTO relation_types (relation_name) VALUES ('BELONGS_TO'), ('VERSION'), ('METADATA'), ('ORIGIN'), ('POLICY');

INSERT INTO users (id, display_name, external_id, attributes, active) 
VALUES
(
    '018A0298-0FF4-995A-C4DC-B6685154E7AB', --01H819G3ZMK5DC9Q5PD18N9SXB
    'test-admin',
    'df5b0209-60e0-4a3b-806d-bbfc99d9e152',
    '{"global_admin": true, "service_account": false, "tokens": {}, "trusted_endpoints": {}, "custom_attributes": [], "permissions": {}}',
    true
);

INSERT INTO pub_keys();

INSERT INTO endpoints(id, name, host_config, endpoint_variant, is_public, status) VALUES (
    '018a03c0-7e8b-293c-eb14-e10dc4b990db', --01H81W0ZMB54YEP5711Q2BK46V
    'default_endpoint',
    '[{"url": "http://localhost:50052", "is_primary": true, "ssl": false, "public": true, "feature": "PROXY"}]',
    'PERSISTENT',
    't',
    'AVAILABLE' 
);


INSERT INTO pub_keys(id, proxy, pubkey) VALUES (1337, '018a03c0-7e8b-293c-eb14-e10dc4b990db', 'MCowBQYDK2VwAyEAnouQBh4GHPCD/k85VIzPyCdOijVg2qlzt2TELwTMy4c=');