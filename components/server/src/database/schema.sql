/* ----- Type ENUMs ------------------------------------------------ */
-- All ENUM types have to be created before their usage in a table
DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ObjectStatus') THEN
            CREATE TYPE "ObjectStatus" AS ENUM (
                'INITIALIZING',
                'VALIDATING',
                'AVAILABLE',
    	        'UNAVAILABLE',
                'ERROR',
                'DELETED'
            );
        END IF;
    END
$$;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'DataClass') THEN
            CREATE TYPE "DataClass" AS ENUM ('PUBLIC', 'PRIVATE', 'WORKSPACE', 'CONFIDENTIAL');
        END IF;
    END
$$;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'PermissionLevel') THEN
            CREATE TYPE "PermissionLevel" AS ENUM ('DENY', 'NONE', 'READ', 'APPEND', 'WRITE', 'ADMIN');
        END IF;
    END
$$;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ObjectType') THEN
            CREATE TYPE "ObjectType" AS ENUM (
                'PROJECT',
                'COLLECTION',
                'DATASET',
                'OBJECT'
            );
        END IF;
    END
$$;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'HashType') THEN
            CREATE TYPE "HashType" AS ENUM (
                'MD5',
                'SHA256'
            );
        END IF;
    END
$$;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'EndpointStatus') THEN
            CREATE TYPE "EndpointStatus" AS ENUM (
                'INITIALIZING',
                'AVAILABLE',
                'DEGRADED',
                'UNAVAILABLE',
                'MAINTENANCE'
            );
        END IF;
    END
$$;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'EndpointVariant') THEN
            CREATE TYPE "EndpointVariant" AS ENUM (
                'PERSISTENT',
                'VOLATILE'
                );
        END IF;
    END
$$;

/*
DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'TriggerType') THEN
            CREATE TYPE "TriggerType" AS ENUM (
                'HOOK_ADDED',
                'RESOURCE_CREATED',
                'LABEL_ADDED',
                'STATIC_LABEL_ADDED',
                'HOOK_STATUS_CHANGED',
                'OBJECT_FINISHED'
                );
        END IF;
    END
$$;
*/

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'PersistentNotificationVariant') THEN
            CREATE TYPE "PersistentNotificationVariant" AS ENUM (
                'ACCESS_REQUESTED',
                'PERMISSION_GRANTED',
                'PERMISSION_REVOKED',
                'PERMISSION_UPDATED',
                'ANNOUNCEMENT'
                );
        END IF;
    END
$$;

/* ----- Authorization --------------------------------------------- */
-- Table with users imported from some aai
-- Join table to map users to multiple identity providers

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    first_name TEXT NOT NULL DEFAULT '',
    last_name TEXT NOT NULL DEFAULT '',
    email VARCHAR(511) NOT NULL DEFAULT '',
    attributes JSONB NOT NULL,
    active BOOL NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS identity_providers (
    issuer_name VARCHAR(255) PRIMARY KEY,
    jwks_endpoint VARCHAR(255),
    audiences VARCHAR(255)[] NOT NULL
);

/* ----- Licenses -------------------------------------- */
-- Table for licenses
CREATE TABLE IF NOT EXISTS licenses (
    tag VARCHAR(511) PRIMARY KEY NOT NULL, -- Common license abbreviation
    name VARCHAR(511) NOT NULL,            -- Full name of the license
    text TEXT NOT NULL,                    -- Full license text
    url VARCHAR(2047) NOT NULL             -- URL to full license text
);

/* ----- Object Service -------------------------------------------- */
-- Table with objects which represent individual data blobs
CREATE TABLE IF NOT EXISTS objects (
    id UUID NOT NULL PRIMARY KEY, -- The unique per object id
    revision_number INT NOT NULL,
    name VARCHAR(511) NOT NULL,          -- Filename or subpath
    title VARCHAR(511) NOT NULL,          -- Filename or subpath
    description VARCHAR(1023) NOT NULL,                 
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by UUID NOT NULL REFERENCES users(id),
    authors JSONB NOT NULL DEFAULT '[]',
    content_len BIGINT NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    key_values JSONB NOT NULL,
    object_status "ObjectStatus" NOT NULL DEFAULT 'INITIALIZING',
    data_class "DataClass" NOT NULL DEFAULT 'PRIVATE',
    object_type "ObjectType" NOT NULL DEFAULT 'PROJECT',
    external_relations JSONB NOT NULL,
    hashes JSONB NOT NULL DEFAULT '{}',
    dynamic BOOL NOT NULL DEFAULT TRUE,
    endpoints JSONB NOT NULL DEFAULT '{}',
    metadata_license VARCHAR(511) NOT NULL REFERENCES licenses(tag),
    data_license VARCHAR(511) NOT NULL REFERENCES licenses(tag),
    UNIQUE(id, object_type)
);
CREATE INDEX IF NOT EXISTS objects_pk_idx ON objects (id);

-- Table with endpoints
CREATE TABLE IF NOT EXISTS endpoints (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    host_config JSONB NOT NULL,
    endpoint_variant "EndpointVariant" NOT NULL DEFAULT 'PERSISTENT',
    documentation_object UUID REFERENCES objects(id),
    is_public BOOL NOT NULL DEFAULT TRUE,
    status "EndpointStatus" NOT NULL DEFAULT 'AVAILABLE',
    UNIQUE(name)

);

/* ----- Object Relations ------------------------------------------ */
-- Table to store custom relation types
CREATE TABLE IF NOT EXISTS relation_types (
    --id SMALLSERIAL PRIMARY KEY NOT NULL,
    relation_name VARCHAR(511) PRIMARY KEY NOT NULL 
);

-- Table to store all internal relations between objects
CREATE TABLE IF NOT EXISTS internal_relations (
    id UUID PRIMARY KEY NOT NULL,
    origin_pid UUID NOT NULL,
    origin_type "ObjectType" NOT NULL,
    relation_name VARCHAR(511) REFERENCES relation_types(relation_name),
    target_pid UUID REFERENCES objects(id) ON DELETE CASCADE,
    target_type "ObjectType" NOT NULL,
    target_name VARCHAR(511) NOT NULL,
    FOREIGN KEY (origin_pid, origin_type) REFERENCES objects(id, object_type) ON DELETE CASCADE,
    FOREIGN KEY (target_pid, target_type) REFERENCES objects(id, object_type) ON DELETE CASCADE,
    UNIQUE(origin_pid, relation_name, target_pid)
    --UNIQUE(origin_pid, relation_name, target_name)
);

CREATE INDEX IF NOT EXISTS origin_pid_idx ON internal_relations (origin_pid);
CREATE INDEX IF NOT EXISTS target_pid_idx ON internal_relations (target_pid);

-- Table for available pubkeys
CREATE TABLE IF NOT EXISTS pub_keys (
    id SMALLSERIAL PRIMARY KEY, -- This is a serial to make jwt tokens smaller
    proxy UUID REFERENCES endpoints(id) ON DELETE CASCADE,
    pubkey TEXT NOT NULL,
    UNIQUE(pubkey)
);

-- Create Materialized View for hierarchy object stats
CREATE MATERIALIZED VIEW IF NOT EXISTS object_stats AS
    /*+ indexscan(ir) set(yb_bnl_batch_size 1024) */
WITH RECURSIVE stats AS (
    SELECT o.id, o.object_type, o.content_len, ir.origin_pid, ir.target_pid
    FROM objects o
             INNER JOIN internal_relations ir ON o.id = ir.target_pid
    WHERE o.object_type = 'OBJECT' AND ir.relation_name = 'BELONGS_TO'
    UNION ALL
    SELECT o2.id, o2.object_type, stats.content_len+o2.content_len, ir2.origin_pid, ir2.target_pid
    FROM stats, objects o2
                    RIGHT JOIN internal_relations ir2 ON o2.id = ir2.target_pid
    WHERE o2.id = stats.origin_pid AND ir2.relation_name = 'BELONGS_TO'
)
SELECT origin_pid, count(origin_pid), sum(content_len)::BIGINT as size, now()::TIMESTAMP as last_refresh
FROM stats
GROUP BY origin_pid;
-- Create unique index for concurrent refreshs
CREATE UNIQUE INDEX IF NOT EXISTS object_stats_id_idx ON object_stats (origin_pid);


/* ----- Notification Service -------------------------------------- */
-- Table for the notification service to persist consumer
CREATE TABLE IF NOT EXISTS stream_consumers (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    config JSONB NOT NULL
);

-- Table for persistent notifications which are delivered outside the message broker
CREATE TABLE IF NOT EXISTS persistent_notifications (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    notification_variant "PersistentNotificationVariant" NOT NULL,
    message TEXT NOT NULL,
    refs JSONB NOT NULL
);

/* ----- Hooks -------------------------------------- */
-- Table for persisting hooks 
CREATE TABLE IF NOT EXISTS hooks (
    id UUID PRIMARY KEY NOT NULL,
    name VARCHAR(511) NOT NULL,
    description VARCHAR(1023) NOT NULL,
    project_ids UUID[],
    owner UUID REFERENCES users(id) ON DELETE CASCADE,
    trigger JSONB NOT NULL,
    /*
    trigger_key VARCHAR(511) NOT NULL,
    trigger_value VARCHAR(511) NOT NULL,
     */
    timeout TIMESTAMP NOT NULL,
    hook JSONB NOT NULL
);

/* ----- Workspaces -------------------------------------- */
-- Table for workspace templates
CREATE TABLE IF NOT EXISTS workspaces (
    id UUID PRIMARY KEY NOT NULL,
    name VARCHAR(511) NOT NULL,
    description VARCHAR(1023) NOT NULL,
    owner UUID REFERENCES users(id) ON DELETE CASCADE,
    prefix VARCHAR(511) NOT NULL,
    hook_ids JSONB,
    endpoint_ids JSONB,
    rules JSONB,
    UNIQUE(name)
);

/* ----- Object rules ------------------------------------- */
CREATE TABLE IF NOT EXISTS rules (
    id UUID PRIMARY KEY NOT NULL,
    rule_expressions VARCHAR(2047) NOT NULL,
    description VARCHAR(511),
    owner_id UUID REFERENCES users(id) ON DELETE CASCADE,
    is_public BOOL NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS rule_bindings (
    rule_id UUID REFERENCES rules(id) ON DELETE CASCADE,
    origin_id UUID REFERENCES objects(id) ON DELETE CASCADE,
    object_id UUID REFERENCES objects(id) ON DELETE CASCADE,
    cascading BOOL NOT NULL DEFAULT TRUE,
    PRIMARY KEY(rule_id, origin_id, object_id)
);

/* ----- Announcements ------------------------------------- */
CREATE TABLE IF NOT EXISTS announcements (
    id UUID PRIMARY KEY NOT NULL,
    announcement_type VARCHAR(128) NOT NULL,
    title VARCHAR(256) NOT NULL,
    teaser VARCHAR(2048) NOT NULL DEFAULT '',
    image_url VARCHAR(4096) NOT NULL DEFAULT '', -- Should be sufficient?
    content text NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    modified_by VARCHAR(128) NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_announcement_type CHECK (announcement_type IN ('ORGA', 'RELEASE', 'UPDATE', 'MAINTENANCE', 'BLOG'))
);

-- Insert predefined relation types
INSERT INTO relation_types (relation_name) VALUES ('BELONGS_TO'), ('VERSION'), ('METADATA'), ('ORIGIN'), ('POLICY'), ('DELETED') ON CONFLICT (relation_name) DO NOTHING;
-- Create partial unique index for BELONGS_TO relations only
CREATE UNIQUE INDEX IF NOT EXISTS belongs_to_idx ON internal_relations (origin_pid, relation_name, target_name) WHERE relation_name = ('BELONGS_TO');
