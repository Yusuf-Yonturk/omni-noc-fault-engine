-- ============================================================
-- V1__init_schema.sql
-- Telco NOC Platform - Initial Database Schema
-- ============================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- raw_alarms: stores every incoming alarm payload verbatim
-- ============================================================
CREATE TABLE raw_alarms (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system    VARCHAR(100),
    raw_payload      JSONB       NOT NULL,
    received_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    processing_status VARCHAR(50) NOT NULL DEFAULT 'PENDING'
                        CHECK (processing_status IN ('PENDING','PROCESSING','PROCESSED','FAILED'))
);

CREATE INDEX idx_raw_alarms_status         ON raw_alarms (processing_status);
CREATE INDEX idx_raw_alarms_received_at    ON raw_alarms (received_at DESC);
CREATE INDEX idx_raw_alarms_source_system  ON raw_alarms (source_system);
CREATE INDEX idx_raw_alarms_payload_gin    ON raw_alarms USING GIN (raw_payload);

-- ============================================================
-- normalized_alarms: canonical alarm representation
-- ============================================================
CREATE TABLE normalized_alarms (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_alarm_id     UUID        REFERENCES raw_alarms(id) ON DELETE SET NULL,
    alarm_type       VARCHAR(100) NOT NULL
                        CHECK (alarm_type IN (
                            'SITE_DOWN','FIBER_CUT','PACKET_LOSS_HIGH','SERVICE_DEGRADED',
                            'CPU_HIGH','POWER_LOSS','LINK_DOWN','CUSTOMER_COMPLAINT_SPIKE',
                            'TRANSMISSION_FAILURE','SERVICE_LATENCY'
                        )),
    severity         VARCHAR(20)  NOT NULL
                        CHECK (severity IN ('CRITICAL','MAJOR','MINOR','WARNING','INFO')),
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

CREATE INDEX idx_norm_alarms_alarm_type    ON normalized_alarms (alarm_type);
CREATE INDEX idx_norm_alarms_severity      ON normalized_alarms (severity);
CREATE INDEX idx_norm_alarms_site_id       ON normalized_alarms (site_id);
CREATE INDEX idx_norm_alarms_region        ON normalized_alarms (region);
CREATE INDEX idx_norm_alarms_incident_id   ON normalized_alarms (incident_id);
CREATE INDEX idx_norm_alarms_occurred_at   ON normalized_alarms (occurred_at DESC);
CREATE INDEX idx_norm_alarms_device_id     ON normalized_alarms (device_id);

-- ============================================================
-- incidents: correlated incident records
-- ============================================================
CREATE TABLE incidents (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    title                       VARCHAR(500) NOT NULL,
    probable_root_cause         TEXT,
    status                      VARCHAR(50)  NOT NULL DEFAULT 'OPEN'
                                    CHECK (status IN (
                                        'OPEN','ACKNOWLEDGED','INVESTIGATING',
                                        'MITIGATED','RESOLVED','CLOSED'
                                    )),
    severity                    VARCHAR(20)  NOT NULL
                                    CHECK (severity IN ('CRITICAL','MAJOR','MINOR','WARNING','INFO')),
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

CREATE INDEX idx_incidents_status        ON incidents (status);
CREATE INDEX idx_incidents_severity      ON incidents (severity);
CREATE INDEX idx_incidents_region        ON incidents (region);
CREATE INDEX idx_incidents_site_id       ON incidents (site_id);
CREATE INDEX idx_incidents_first_seen_at ON incidents (first_seen_at DESC);
CREATE INDEX idx_incidents_assigned_team ON incidents (assigned_team);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trg_incidents_updated_at
    BEFORE UPDATE ON incidents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- incident_alarm_map: many-to-many alarm ↔ incident
-- ============================================================
CREATE TABLE incident_alarm_map (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id   UUID        NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    alarm_id      UUID        NOT NULL REFERENCES normalized_alarms(id) ON DELETE CASCADE,
    relation_type VARCHAR(50) NOT NULL DEFAULT 'CORRELATED'
                    CHECK (relation_type IN ('ROOT_CAUSE','CORRELATED','CHILD')),
    mapped_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
    UNIQUE (incident_id, alarm_id)
);

CREATE INDEX idx_iam_incident_id ON incident_alarm_map (incident_id);
CREATE INDEX idx_iam_alarm_id    ON incident_alarm_map (alarm_id);

-- ============================================================
-- affected_services: services impacted by an incident
-- ============================================================
CREATE TABLE affected_services (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id           UUID        NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    service_name          VARCHAR(200),
    service_type          VARCHAR(100),
    customer_impact_count INTEGER     NOT NULL DEFAULT 0,
    impact_level          VARCHAR(50)
                            CHECK (impact_level IN ('CRITICAL','HIGH','MEDIUM','LOW','NONE')),
    detected_at           TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_affected_services_incident_id ON affected_services (incident_id);

-- ============================================================
-- escalation_history: audit trail of all escalation actions
-- ============================================================
CREATE TABLE escalation_history (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id           UUID        NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    escalated_to          VARCHAR(200),
    escalation_level      VARCHAR(50),
    reason                TEXT,
    notification_channel  VARCHAR(100),
    escalated_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
    acknowledged_at       TIMESTAMP
);

CREATE INDEX idx_escalation_incident_id  ON escalation_history (incident_id);
CREATE INDEX idx_escalation_escalated_at ON escalation_history (escalated_at DESC);

-- ============================================================
-- audit_logs: full audit trail for all entity state changes
-- ============================================================
CREATE TABLE audit_logs (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type  VARCHAR(100),
    entity_id    UUID,
    action       VARCHAR(100),
    old_value    TEXT,
    new_value    TEXT,
    performed_by VARCHAR(200) NOT NULL DEFAULT 'SYSTEM',
    performed_at TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_entity_id   ON audit_logs (entity_id);
CREATE INDEX idx_audit_logs_entity_type ON audit_logs (entity_type);
CREATE INDEX idx_audit_logs_performed_at ON audit_logs (performed_at DESC);

-- ============================================================
-- Add FK: normalized_alarms.incident_id → incidents.id
-- (deferred to after incidents table exists)
-- ============================================================
ALTER TABLE normalized_alarms
    ADD CONSTRAINT fk_norm_alarms_incident
    FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE SET NULL;
