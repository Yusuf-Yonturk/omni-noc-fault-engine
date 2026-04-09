-- ============================================================
-- V1__init_schema.sql (correlation-engine)
-- Same schema as alarm-ingestion-service — Flyway baseline only.
-- This service READS from the shared schema, does not own it.
-- ============================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS raw_alarms (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system    VARCHAR(100),
    raw_payload      JSONB       NOT NULL,
    received_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    processing_status VARCHAR(50) NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE IF NOT EXISTS normalized_alarms (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_alarm_id     UUID,
    alarm_type       VARCHAR(100) NOT NULL,
    severity         VARCHAR(20)  NOT NULL,
    site_id          VARCHAR(100),
    region           VARCHAR(100),
    device_type      VARCHAR(100),
    device_id        VARCHAR(100),
    service_affected VARCHAR(200),
    description      TEXT,
    occurred_at      TIMESTAMP   NOT NULL,
    correlated_at    TIMESTAMP,
    incident_id      UUID,
    is_root_cause    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS incidents (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    title                       VARCHAR(500) NOT NULL,
    probable_root_cause         TEXT,
    status                      VARCHAR(50)  NOT NULL DEFAULT 'OPEN',
    severity                    VARCHAR(20)  NOT NULL,
    region                      VARCHAR(100),
    site_id                     VARCHAR(100),
    service_impact_count        INTEGER      NOT NULL DEFAULT 0,
    first_seen_at               TIMESTAMP    NOT NULL,
    last_seen_at                TIMESTAMP,
    assigned_team               VARCHAR(200),
    acknowledged_by             VARCHAR(200),
    acknowledged_at             TIMESTAMP,
    resolved_at                 TIMESTAMP,
    closed_at                   TIMESTAMP,
    improvement_ticket_created  BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at                  TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS incident_alarm_map (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id   UUID        NOT NULL,
    alarm_id      UUID        NOT NULL,
    relation_type VARCHAR(50) NOT NULL DEFAULT 'CORRELATED',
    mapped_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS affected_services (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id           UUID        NOT NULL,
    service_name          VARCHAR(200),
    service_type          VARCHAR(100),
    customer_impact_count INTEGER     NOT NULL DEFAULT 0,
    impact_level          VARCHAR(50),
    detected_at           TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS escalation_history (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id           UUID        NOT NULL,
    escalated_to          VARCHAR(200),
    escalation_level      VARCHAR(50),
    reason                TEXT,
    notification_channel  VARCHAR(100),
    escalated_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
    acknowledged_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type  VARCHAR(100),
    entity_id    UUID,
    action       VARCHAR(100),
    old_value    TEXT,
    new_value    TEXT,
    performed_by VARCHAR(200) NOT NULL DEFAULT 'SYSTEM',
    performed_at TIMESTAMP   NOT NULL DEFAULT NOW()
);
